from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial, reduce
from itertools import count
import math
import operator
from pprint import pp
from pipe import map, traverse, where
from typing import Any, ClassVar, Optional, Tuple

from subgrounds.query import Argument, Document, InputValue, Selection, Query, VariableDefinition
import subgrounds.client as client
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.utils import extract_data, union

DEFAULT_NUM_ENTITIES = 100
PAGE_SIZE = 900

"""
Cannonical query form

```graphql
query itemsQuery($first: BigInt!, $last: Any!) {
  items(
    orderBy: FIELD,
    orderDirection: desc,
    first: $first,
    where: {
      FIELD_lt: $last
    }
  ) {
    field1
    field2
  }
}
```

If `orderBy` is not provided, then `FIELD` defaults to `id`.

If `skip` is provided, then use the skip value for the first query, then drop it.

"""


@dataclass(frozen=True)
class PaginationNode:
  node_idx: int
  filter_field: str       # name of the node's filter field, e.g.: if `filter_name` is `timestamp_gt`, then `filter_field` is `timestamp`
  
  first_value: int        # initial `first_varname` value
  skip_value: int         # initial `skip_varname` value
  filter_value: Any       # initial `filter_varname` value
  filter_value_type: TypeRef.T

  key_path: list[str]
  inner: list[PaginationNode] = field(default_factory=list)

  def variable_definitions(self: PaginationNode) -> list[VariableDefinition]:
    vardefs = [
      VariableDefinition(f'first{self.node_idx}', TypeRef.Named('Int')),
      VariableDefinition(f'skip{self.node_idx}', TypeRef.Named('Int')),
      VariableDefinition(f'lastOrderingValue{self.node_idx}', self.filter_value_type),
    ]

    return list(self.inner | map(PaginationNode.variable_definitions) | traverse) + vardefs


def preprocess_selection(
  schema: SchemaMeta,
  select: Selection,
  key_path: list[str],
  counter: count[int]
) -> Tuple[Selection, PaginationNode]:
  """ Returns a tuple `(select_, node)` where `select_` is the same selection
  tree as `select` except it has been normalized for pagination and `node` is 
  a PaginationNode tree containing all pagination metadata for each selection in
  `select` yielding a list of entities.
  
  A normalized selection is of the form:
  ```graphql
  items(
    first: $firstN,
    skip: $skipN,
    orderBy: FIELD,
    orderDirection: ORD
    where: {
      FIELD_ORD: $lastOrderingValueN
    }
  )
  ```

  Args:
      schema (SchemaMeta): _description_
      select (Selection): _description_
      key_path (list[str]): _description_
      counter (count[int]): _description_

  Returns:
      Tuple[Selection, PaginationNode]: _description_
  """

  # 'Folding' function to recursively apply `preprocess_selection` to `select`'s inner selections
  def fold(
    acc: Tuple[list[Selection], list[PaginationNode]],
    select_: Selection
  ) -> Tuple[list[Selection], list[PaginationNode]]:
    new_select, pagination_node = preprocess_selection(schema, select_, [*key_path, select.key], counter)
    return ([*acc[0], new_select], [*acc[1], pagination_node])

  # Compute nested nromalized selections and pagination nodes
  acc0: Tuple[list[Selection], list[PaginationNode]] = ([], [])
  new_selections, pagination_nodes = reduce(fold, select.selection, acc0)

  if select.fmeta.type_.is_list:
    # Add id to selection if not already present
    try:
      next(new_selections | where(lambda select: select.fmeta.name == 'id'))
    except StopIteration:
      new_selections.append(Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))))

    n = next(counter)

    # Starting point for new arguments: all arguments not important for pagination
    new_args = list(select.arguments | where(lambda arg: arg.name not in ['first', 'skip', 'where', 'orderBy', 'orderDirection']))

    # Set `first` argument
    first_arg: Argument | None = select.get_argument('first', recurse=False)
    if first_arg is None:
      first_arg_value = 100
    else:
      first_arg_value = first_arg.value.value
    new_args.append(Argument(name='first', value=InputValue.Variable(f'first{n}')))

    # Set `skip` argument
    skip_arg: Argument | None = select.get_argument('skip', recurse=False)
    if skip_arg is None:
      skip_arg_value = 0
    else:
      skip_arg_value = skip_arg.value.value
    new_args.append(Argument(name='skip', value=InputValue.Variable(f'skip{n}')))

    # Check if `orderBy` argument is provided. If not, set the `orderBy` argument to `id`
    order_by_arg: Argument | None = select.get_argument('orderBy', recurse=False)
    if order_by_arg is None:
      order_by_arg = Argument(name='orderBy', value=InputValue.Enum('id'))
      order_by_val = 'id'
    else:
      order_by_val = order_by_arg.value.value

      # Add `order_by_val` field to selection if not already present
      try:
        next(new_selections | where(lambda select: select.fmeta.name == order_by_val))
      except StopIteration:
        select_type: TypeMeta.ObjectMeta = schema.type_of_typeref(select.fmeta.type_)
        new_selections.append(Selection(fmeta=TypeMeta.FieldMeta(order_by_val, '', [], select_type.type_of_field(order_by_val))))

    new_args.append(order_by_arg)

    # Check if `orderDirection` argument is provided. If not, set it to `asc`.
    order_direction_arg: Argument | None = select.get_argument('orderDirection', recurse=False)
    if order_direction_arg is None:
      order_direction_arg = Argument(name='orderDirection', value=InputValue.Enum('asc'))
      order_direction_val = 'asc'
    else:
      order_direction_val = order_direction_arg.value.value
    new_args.append(order_direction_arg)

    # Check if `where` argument is provided. If not, set it to `where: {filtering_arg: $lastOrderingValueN}`
    # where `filtering_arg` depends on the previous values of `orderBy` and `orderDirection`.
    # E.g.: if `orderBy` is `foo` and `orderDirection` is `asc`, then `filtering_arg` will be `foo_gt`
    filtering_arg = '{}_{}'.format(order_by_val, 'gt' if order_direction_val == 'asc' else 'lt')

    where_arg: Argument | None = select.get_argument('where', recurse=False)
    if where_arg is None:
      where_arg = Argument(name='where', value=InputValue.Object({
        filtering_arg: InputValue.Variable(f'lastOrderingValue{n}')
      }))
      filter_value = None
    else:
      if filtering_arg in where_arg.value.value:
        filter_value = where_arg.value.value[filtering_arg].value
      else:
        filter_value = None

      where_arg = Argument(name='where', value=InputValue.Object(
        where_arg.value.value | {filtering_arg: InputValue.Variable(f'lastOrderingValue{n}')}
      ))

    new_args.append(where_arg)

    # Find type of filter argument
    t: TypeRef.T = select.fmeta.type_of_arg('where')
    where_arg_type: TypeMeta.InputObjectMeta = schema.type_of_typeref(t)
    filtering_arg_type: TypeRef.T = where_arg_type.type_of_input_field(filtering_arg)

    return (
      Selection(
        fmeta=select.fmeta,
        alias=select.alias,
        arguments=new_args,
        selection=new_selections
      ),
      PaginationNode(
        node_idx=n,
        filter_field=order_by_val,
        
        first_value=first_arg_value,
        skip_value=skip_arg_value,
        filter_value=filter_value,
        filter_value_type=filtering_arg_type,

        key_path=[*key_path, select.alias if select.alias is not None else select.fmeta.name],
        inner=list(pagination_nodes | traverse)
      )
    )
  else:
    # If selection does not return a list of entities, leave it unchanged
    return (
      Selection(
        fmeta=select.fmeta,
        alias=select.alias,
        arguments=select.arguments,
        selection=new_selections
      ),
      list(pagination_nodes | traverse)
    )


def preprocess_document(
  schema: SchemaMeta,
  document: Document,
) -> Tuple[Document, list[PaginationNode]]:
  match document:
    case Document(url, None, fragments, variables) as doc:
      return (doc, [])

    case Document(url, query, fragments, variables) as doc:
      counter = count(0)

      def fold(
        acc: Tuple[list[Selection], list[PaginationNode]],
        select: Selection
      ) -> Tuple[list[Selection], list[PaginationNode]]:
        new_select, pagination_node = preprocess_selection(schema, select, [], counter)
        return ([*acc[0], new_select], [*acc[1], pagination_node])
      
      acc0: Tuple[list[Selection], list[PaginationNode]] = ([], [])
      new_selections, pagination_nodes = reduce(fold, query.selection, acc0)

      variable_defs = list(
        pagination_nodes
        | traverse
        | map(PaginationNode.variable_definitions)
        | traverse
      )

      return (
        Document(
          url=url,
          query=Query(
            name=query.name,
            selection=new_selections,
            variables=union(
              query.variables,
              variable_defs,
              key=lambda vardef: vardef.name,
              combine=lambda _, x: x
            )
          ),
          fragments=fragments,
          variables=variables
        ),
        list(pagination_nodes | traverse)
      )


def flatten(l: list[list | dict]) -> list[dict]:
  match l[0]:
    case list():
      return reduce(lambda acc, l: acc + flatten(l), l, [])
    case _:
      return l


@dataclass
class ArgGeneratorFSM:
  PAGE_SIZE: ClassVar[int] = 900

  page_node: PaginationNode

  inner: list[ArgGeneratorFSM]
  inner_idx: int = 0

  filter_value: Any = None
  queried_entities: int = 0
  stop: bool = False
  page_count: int = 0
  keys: set[str] = field(default_factory=set)

  def __init__(self, page_node: PaginationNode) -> None:
    self.page_node = page_node
    self.inner = list(page_node.inner | map(ArgGeneratorFSM))
    self.reset()

  @property
  def is_leaf(self):
    return len(self.inner) == 0

  def update(self, data: dict) -> None:
    """ Moves 'self' cursor forward according to previous response data `data`

    Args:
        data (dict): Previous response data

    Raises:
        StopIteration: _description_
    """
    # Current node step
    index_field_data = list(extract_data([*self.page_node.key_path, self.page_node.filter_field], data) | traverse)
    num_entities = len(index_field_data)
    filter_value = index_field_data[-1] if len(index_field_data) > 0 else None
    # print('node {}: num_entities = {}, filter_value = {}'.format('_'.join(self.page_node.key_path), num_entities, filter_value))

    id_data = list(extract_data([*self.page_node.key_path, 'id'], data) | traverse)
    for key in id_data:
      if key not in self.keys:
        self.keys.add(key)

    self.page_count = self.page_count + 1
    self.queried_entities = len(self.keys)

    if filter_value:
      self.filter_value = filter_value

    if (
      (self.is_leaf and num_entities < ArgGeneratorFSM.PAGE_SIZE)
      or (not self.is_leaf and num_entities == 0)
      or (self.queried_entities == self.page_node.first_value)
    ):
      raise StopIteration(self)

  def step(self, data: dict) -> None:
    """ Updates either 'self' cursor or inner state machine depending on
    whether the inner state machine has reached its limit

    Args:
        data (dict): _description_
    """
    if self.is_leaf:
      self.update(data)
    else:
      try:
        self.inner[self.inner_idx].step(data)
      except StopIteration:
        if self.inner_idx < len(self.inner) - 1:
          self.inner_idx = self.inner_idx + 1
        else:
          self.update(data)
          self.inner_idx = 0

        self.inner[self.inner_idx].reset()

  def args(self) -> dict:
    """ Returns the pagination arguments for the current state of the state machine

    Returns:
        dict: _description_
    """
    if self.is_leaf:
      return {
        # `first`
        f'first{self.page_node.node_idx}': self.page_node.first_value - self.queried_entities
        if self.page_node.first_value - self.queried_entities < ArgGeneratorFSM.PAGE_SIZE
        else ArgGeneratorFSM.PAGE_SIZE,

        # `skip`
        f'skip{self.page_node.node_idx}': self.page_node.skip_value if self.page_count == 0 else 0,

        # `filter`
        f'lastOrderingValue{self.page_node.node_idx}': self.filter_value
      }
    else:
      args = {
        # `first`
        f'first{self.page_node.node_idx}': 1,

        # `skip`
        f'skip{self.page_node.node_idx}': self.page_node.skip_value if self.page_count == 0 else 0,

        # `filter`
        f'lastOrderingValue{self.page_node.node_idx}': self.filter_value
      }

      inner_args = self.inner[self.inner_idx].args()
      return args | inner_args

  def reset(self):
    """ Reset state machine
    """
    self.inner_idx = 0
    self.filter_value = self.page_node.filter_value
    self.queried_entities = 0
    self.stop = False
    self.page_count = 0
    self.keys = set()


def trim_document(document: Document, pagination_args: dict[str, int]) -> Document:
  """ Returns a new Document containing only the selection subtrees of `document`
  whose arguments are present in `pagination_args`.

  Args:
      document (Document): _description_
      pagination_args (dict[str, int]): _description_

  Returns:
      Query: _description_
  """

  def trim_where_input_object(input_object: InputValue.Object) -> InputValue.Object:
    def f(keyval):
      (key, value) = keyval
      match value:
        case InputValue.Variable(name) if name in pagination_args and pagination_args[name] is None:
          return None
        case _:
          return (key, value)

    return InputValue.Object(dict(
      list(input_object.value.items())
      | map(f)
      | where(lambda keyval: keyval is not None)
    ))

  def trim_selection(selection: Selection) -> Optional[Selection]:
    try:
      # Check if pagination node by checking for `first` argument
      arg = next(selection.arguments | where(lambda arg: arg.name == 'first'))

      # Return selection if argument in current page variables
      if arg.value.name in pagination_args:
        return Selection(
          selection.fmeta,
          selection.alias,
          list(
            selection.arguments
            | map(lambda arg: Argument(name=arg.name, value=trim_where_input_object(arg.value)) if arg.name == 'where' else arg)
          ),
          list(
            selection.selection
            | map(trim_selection)
            | where(lambda val: val is not None)
          )
        )
      else:
        return None
    except StopIteration:
      # If does not contain `first` argument, then not a pagination node
      return Selection(
        selection.fmeta,
        selection.alias,
        selection.arguments,
        list(
          selection.selection
          | map(trim_selection)
          | where(lambda val: val is not None)
        )
      )

  return Document(
    url=document.url,
    query=Query(
      name=document.query.name,
      selection=list(
        document.query.selection
        | map(trim_selection)
        | where(lambda val: val is not None)
      ),
      variables=list(
        document.query.variables
        | where(lambda vardef: vardef.name in pagination_args)
      )
      # variables=[VariableDefinition(key, TypeRef.Named('Int')) for key in pagination_args] + document.query.variables
    ),
    fragments=document.fragments,
    variables=document.variables | pagination_args
  )


def merge(data1, data2):
  match (data1, data2):
    case (list(), list()):
      return union(
        data1,
        data2,
        lambda data: data['id'],
        combine=merge
      )
      # return data1 + data2

    case (dict(), dict()):
      data = {}
      for key in data1:
        if key in data2:
          data[key] = merge(data1[key], data2[key])
        else:
          data[key] = data1[key]

      for key in data2:
        if key not in data:
          data[key] = data2[key]

      return data

    case (val1, _):
      return val1


def paginate(schema: SchemaMeta, doc: Document) -> dict[str, Any]:
  new_doc, pagination_nodes = preprocess_document(schema, doc)

  if pagination_nodes == []:
    return client.query(doc.url, doc.graphql, variables=doc.variables)
  else:
    data = {}
    for page_node in pagination_nodes:
      arg_gen = ArgGeneratorFSM(page_node)

      while True:
        try:
          args = arg_gen.args()
          trimmed_doc = trim_document(new_doc, args)
          page_data = client.query(trimmed_doc.url, trimmed_doc.graphql, variables=trimmed_doc.variables | args)
          data = merge(data, page_data)
          arg_gen.step(page_data)
        except StopIteration:
          break
    
    return data