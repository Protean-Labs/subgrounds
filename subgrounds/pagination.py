from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial, reduce
from itertools import count
import math
import operator
from pipe import map, traverse, where
from typing import Any, Optional, Tuple

from subgrounds.query import Argument, Document, InputValue, Selection, Query, VariableDefinition
import subgrounds.client as client
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.utils import extract_data, union

DEFAULT_NUM_ENTITIES = 100
PAGE_SIZE = 900


@dataclass(frozen=True)
class PaginationNode:
  first_name: str
  skip_name: str
  key_path: list[str]
  first_value: Optional[InputValue.T] = None
  skip_value: Optional[InputValue.T] = None
  inner: list[PaginationNode] = field(default_factory=list)

  def args(self, page_size: int, inner_size: int = 0) -> list[dict[str, int]]:
    num_entities = self.first_value.value if self.first_value is not None else DEFAULT_NUM_ENTITIES
    num_pages = math.ceil(num_entities / page_size)
    skip0 = self.skip_value.value if self.skip_value is not None else 0

    return [
      # If we are at the last page and there is a remainder
      {self.first_name: num_entities % page_size, self.skip_name: skip0 + i * page_size}
      if (i == num_pages - 1 and num_entities % page_size != 0)

      # If we are not at the last page or there is no remainder
      else {self.first_name: page_size, self.skip_name: skip0 + i * page_size}
      for i in range(0, num_pages)
    ]


def preprocess_document(
  document: Document,
) -> Tuple[Document, list[PaginationNode]]:
  counter = count(0)

  def preprocess_selection(
    select: Selection,
    key_path: list[str] = []
  ) -> Tuple[Selection, PaginationNode]:
    def preprocess_arg(arg: Argument, n: int) -> Argument:
      match arg.name:
        case 'first' | 'skip' as argname:
          return Argument(name=argname, value=InputValue.Variable(f'{argname}{n}'))
        case _:
          return arg

    def fold(
      acc: Tuple[list[Selection], list[PaginationNode]],
      select_: Selection
    ) -> Tuple[list[Selection], list[PaginationNode]]:
      new_select, pagination_node = preprocess_selection(select_, [*key_path, select.alias if select.alias is not None else select.fmeta.name])
      return ([*acc[0], new_select], [*acc[1], pagination_node])

    new_selections, pagination_nodes = reduce(fold, select.selection, ([], []))

    if (
      select.fmeta.type_.is_list
      and select.fmeta.has_arg('first')
      and select.fmeta.has_arg('skip')
    ):
      # Add id to selection if not already present
      try:
        next(new_selections | where(lambda select: select.fmeta.name == 'id'))
      except StopIteration:
        new_selections.append(Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))))

      n = next(counter)
      new_arguments = list(select.arguments | map(partial(preprocess_arg, n=n)))

      try:
        first_arg_value = next(select.arguments | where(lambda arg: arg.name == 'first')).value
      except StopIteration:
        first_arg_value = None
        new_arguments.append(Argument(name='first', value=InputValue.Variable(f'first{n}')))

      try:
        skip_arg_value = next(select.arguments | where(lambda arg: arg.name == 'skip')).value
      except StopIteration:
        skip_arg_value = None
        new_arguments.append(Argument(name='skip', value=InputValue.Variable(f'skip{n}')))

      return (
        Selection(
          fmeta=select.fmeta,
          alias=select.alias,
          arguments=new_arguments,
          selection=new_selections
        ),
        PaginationNode(
          first_name=f'first{n}',
          skip_name=f'skip{n}',
          key_path=[*key_path, select.alias if select.alias is not None else select.fmeta.name],
          first_value=first_arg_value,
          skip_value=skip_arg_value,
          inner=list(pagination_nodes | traverse)
        )
      )
    else:
      return (
        Selection(
          fmeta=select.fmeta,
          alias=select.alias,
          arguments=select.arguments,
          selection=new_selections
        ),
        list(pagination_nodes | traverse)
      )

  def f(
    item: Document | Query | Selection
  ) -> Tuple[Document | Query | Selection, PaginationNode]:
    match item:
      case Document(url, query, fragments, variables) as doc:
        if query is None:
          return (doc, [])
        else:
          new_query, pagination_nodes = f(query)
          return (
            Document(
              url=url,
              query=new_query,
              fragments=fragments,
              variables=variables
            ),
            list(pagination_nodes | traverse)
          )

      case Query(name, selection, variables):
        def fold(
          acc: Tuple[list[Selection], list[PaginationNode]],
          select: Selection
        ) -> Tuple[list[Selection], list[PaginationNode]]:
          new_select, pagination_node = f(select)
          return ([*acc[0], new_select], [*acc[1], pagination_node])

        new_selections, pagination_nodes = reduce(fold, selection, ([], []))
        return (
          Query(
            name=name,
            selection=new_selections,
            variables=variables
          ),
          list(pagination_nodes | traverse)
        )

      case Selection() as selection:
        return preprocess_selection(selection)

  return f(document)


def flatten(l: list[list | dict]) -> list[dict]:
  match l[0]:
    case list():
      return reduce(lambda acc, l: acc + flatten(l), l, [])
    case _:
      return l


def pagination_args(node: PaginationNode | list[PaginationNode], page_size: int = PAGE_SIZE) -> list[dict]:
  if type(node) == list:
    return flatten(list(node | map(partial(pagination_args, page_size=page_size))))
  else:
    match node.inner:
      case []:
        return node.args(page_size)

      case l:
        return flatten(list(
          range(0, node.first_value.value if node.first_value is not None else DEFAULT_NUM_ENTITIES)
          | map(lambda i: list(
            l | map(lambda nested_pagination: list(
              pagination_args(nested_pagination, page_size)
              | map(lambda nested_args: {node.first_name: 1, node.skip_name: i} | nested_args)
            ))
          ))
        ))


class Empty(Exception):
  pass


class PaginationArgsGenerator:
  nodes: list[PaginationNode]
  skip_key_path: Optional[list[str]] = None

  def skip(self, key_path: list[str]):
    self.skip_key_path = key_path

  def check_empty(self):
    if self.skip_key_path is not None:
      path = self.skip_key_path
      self.skip_key_path = None
      raise Empty(path)

  def generator(
    self: PaginationArgsGenerator,
    node: PaginationNode | list[PaginationNode],
    page_size: int = PAGE_SIZE,
    args_acc: dict = {},
    nodes_acc: list[PaginationNode] = []
  ) -> list[dict]:
    # print(f'node = {node}, args_acc = {args_acc}')
    if type(node) == list:
      for node in node:
        yield from self.generator(node, page_size, args_acc, nodes_acc)

    else:
      try:
        match node.inner:
          case []:
            for args in node.args(page_size):
              self.check_empty()
              yield ([*nodes_acc, node], args_acc | args)

          case l:
            for i in range(0, node.first_value.value if node.first_value is not None else DEFAULT_NUM_ENTITIES):
              self.check_empty()
              yield from self.generator(l, page_size, args_acc | {node.first_name: 1, node.skip_name: i}, [*nodes_acc, node])

      except Empty as empty:
        path = empty.args[0]
        if node.key_path != path:
          raise empty


def trim_document(document: Document, pagination_args: dict[str, int]) -> Query:
  def trim_selection(selection: Selection) -> Optional[Selection]:
    try:
      arg = next(selection.arguments | where(lambda arg: arg.name == 'first'))
      if arg.value.name in pagination_args:
        return Selection(
          selection.fmeta,
          selection.alias,
          selection.arguments,
          list(selection.selection | map(trim_selection) | where(lambda val: val is not None))
        )
      else:
        return None
    except StopIteration:
      return Selection(
        selection.fmeta,
        selection.alias,
        selection.arguments,
        list(selection.selection | map(trim_selection) | where(lambda val: val is not None))
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
      variables=[VariableDefinition(key, TypeRef.Named('Int')) for key in pagination_args] + document.query.variables
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

# TODO: If selection of pagination node returns nothing, skip rest of node
def paginate(doc: Document) -> dict[str, Any]:
  new_doc, pagination_nodes = preprocess_document(doc)

  if pagination_nodes == []:
    return client.query(doc.url, doc.graphql, variables=doc.variables)
  else:
    gen = PaginationArgsGenerator()

    def fold(data: dict, nodesargs: Tuple[list[PaginationNode], dict]) -> dict:
      nodes, args = nodesargs
      trimmed_doc = trim_document(new_doc, args)
      new_data = client.query(trimmed_doc.url, trimmed_doc.graphql, variables=trimmed_doc.variables | args)

      for node in nodes:
        if extract_data(node.key_path, new_data) == []:
          gen.skip(node.key_path)
          break

      return merge(data, new_data)

    argsgen = gen.generator(pagination_nodes, page_size=PAGE_SIZE)

    return reduce(fold, argsgen, {})
