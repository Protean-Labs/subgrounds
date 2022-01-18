from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import partial, reduce
from typing import Any, Callable, Optional, Tuple
from pipe import map, traverse, where, take
import math

from subgrounds.schema import (
  TypeMeta,
  SchemaMeta,
  TypeRef,
  typeref_of_input_field
)
from subgrounds.utils import filter_none, identity, rel_complement, union
import subgrounds.client as client


# ================================================================
# Query definitions, data structures and types
# ================================================================
class InputValue:
  @dataclass(frozen=True)
  class T(ABC):
    @property
    @abstractmethod
    def graphql_string(self) -> str:
      """ Returns a GraphQL string representation of the input value

      Returns:
        str: The GraphQL string representation of the input value
      """
      pass

    @property
    def is_variable(self) -> bool:
      """ Returns True i.f.f. the input value is of type Variable

      Returns:
        bool: True i.f.f. the input value is of type Variable, otherwise False
      """
      return False

    @property
    def is_number(self) -> bool:
      """ Returns True i.f.f. the input value is of type Float or Int

      Returns:
        bool: True i.f.f. the input value is of type Float or Int, otherwise False
      """
      return False

  @dataclass(frozen=True)
  class Null(T):
    @property
    def graphql_string(self) -> str:
      return "null"

  @dataclass(frozen=True)
  class Int(T):
    value: int

    @property
    def graphql_string(self) -> str:
      return str(self.value)

    @property
    def is_number(self) -> bool:
      return True

  @dataclass(frozen=True)
  class Float(T):
    value: float

    @property
    def graphql_string(self) -> str:
      return str(self.value)

    @property
    def is_number(self) -> bool:
      return True

  @dataclass(frozen=True)
  class String(T):
    value: str

    @property
    def graphql_string(self) -> str:
      return f"\"{self.value}\""

  @dataclass(frozen=True)
  class Boolean(T):
    value: bool

    @property
    def graphql_string(self) -> str:
      return str(self.value).lower()

  @dataclass(frozen=True)
  class Enum(T):
    value: str

    @property
    def graphql_string(self) -> str:
      return self.value

  @dataclass(frozen=True)
  class Variable(T):
    name: str

    @property
    def graphql_string(self) -> str:
      return f'${self.name}'

    @property
    def is_variable(self) -> bool:
      return True

  @dataclass(frozen=True)
  class List(T):
    value: list[InputValue.T]

    @property
    def graphql_string(self) -> str:
      return f"[{', '.join([val.graphql_string for val in self.value])}]"

  @dataclass(frozen=True)
  class Object(T):
    value: dict[str, InputValue.T]

    @property
    def graphql_string(self) -> str:
      return f"{{{', '.join([f'{key}: {value.graphql_string}' for key, value in self.value.items()])}}}"


@dataclass(frozen=True)
class VariableDefinition:
  name: str
  type_: TypeRef.T
  default: Optional[InputValue.T] = None

  @property
  def graphql_string(self) -> str:
    if self.default is None:
      return f'${self.name}: {TypeRef.graphql_string(self.type_)}'
    else:
      return f'${self.name}: {TypeRef.graphql_string(self.type_)} = {self.default.graphql_string}'


@dataclass(frozen=True)
class Argument:
  name: str
  value: InputValue.T

  @property
  def graphql_string(self) -> str:
    return f"{self.name}: {self.value.graphql_string}"


@dataclass(frozen=True)
class Selection:
  """ 

  Raises:
    Exception: [description]

  Returns:
    [type]: [description]
  """
  fmeta: TypeMeta.FieldMeta
  alias: Optional[str] = None
  arguments: list[Argument] = field(default_factory=list)
  selection: list[Selection] = field(default_factory=list)

  @property
  def args_graphql_string(self) -> str:
    if self.arguments:
      return f'({", ".join([arg.graphql_string for arg in self.arguments])})'
    else:
      return ""

  def graphql_string(self, level: int = 0) -> str:
    indent = "  " * level

    match (self.selection):
      case None | []:
        return f"{indent}{self.fmeta.name}{self.args_graphql_string}"
      case inner_selection:
        inner_str = "\n".join(
          [f.graphql_string(level=level + 1) for f in inner_selection]
        )
        return f"{indent}{self.fmeta.name}{self.args_graphql_string} {{\n{inner_str}\n{indent}}}"

  @staticmethod
  def add_selections(select: Selection, new_selections: list[Selection]) -> Selection:
    return Selection(
      fmeta=select.fmeta,
      alias=select.alias,
      selection=union(
        select.selection,
        new_selections,
        key=lambda select: select.fmeta.name,
        combine=Selection.combine
      )
    )

  @staticmethod
  def add_selection(select: Selection, new_selection: Selection) -> Selection:
    return Selection.add_selections(select, [new_selection])

  @staticmethod
  def remove_selections(select: Selection, selections_to_remove: list[Selection]) -> Selection:
    def combine(select: Selection, selection_to_remove: Selection) -> Optional[Selection]:
      if selection_to_remove.selection == []:
        return None
      else:
        return Selection.remove_selections(select, selection_to_remove.selection)

    return Selection(
      fmeta=select.fmeta,
      alias=select.alias,
      arguments=select.arguments,
      selection=filter_none(union(
        select.selection,
        selections_to_remove,
        key=lambda s: s.fmeta.name,
        combine=combine
      ))
    )

  @staticmethod
  def remove_selection(select: Selection, selection_to_remove: Selection) -> Selection:
    return Selection.remove_selections(select, [selection_to_remove])

  @staticmethod
  def combine(select: Selection, other: Selection) -> Selection:
    if select.fmeta != other.fmeta:
      raise Exception(f"Selection.combine: {select.fmeta} != {other.fmeta}")

    return Selection(
      fmeta=select.fmeta,
      alias=select.alias,
      arguments=select.arguments,
      selection=filter_none(union(
        select.selection,
        other.selection,
        key=lambda select: select.fmeta.name,
        combine=Selection.combine
      ))
    )

  @staticmethod
  def contains(select: Selection, other: Selection) -> bool:
    if (select.fmeta == other.fmeta and rel_complement(other.selection, select.selection, key=lambda s: s.fmeta.name) == []):
      return all(
        other.selection
        | map(lambda s: Selection.contains(next(filter(lambda s_: s.fmeta.name == s_.fmeta.name, select.selection)), s))
      )
    else:
      return False

  @staticmethod
  def contains_argument(select: Selection, arg_name: str) -> bool:
    try:
      next(filter(lambda arg: arg.name == arg_name, select.arguments))
      return True
    except StopIteration:
      return any(select.selection | map(partial(Selection.contains_argument, arg_name=arg_name)))

  @staticmethod
  def get_argument(select: Selection, arg_name: str) -> Optional[Argument]:
    try:
      return next(filter(lambda arg: arg.name == arg_name, select.arguments))
    except StopIteration:
      return next(select.selection | map(partial(Selection.contains_argument, arg_name=arg_name)))

  @staticmethod
  def substitute_arg(select: Selection, arg_name: str, replacement: Argument | list[Argument]) -> Selection:
    return Selection(
      fmeta=select.fmeta,
      arguments=list(
        select.arguments
        | map(lambda arg: replacement if arg.name == arg_name else arg)
        | traverse
      ),
      selection=list(
        select.selection
        | map(partial(Selection.substitute_arg, arg_name=arg_name, replacement=replacement))
      )
    )

  @staticmethod
  def select(select: Selection, other: Selection) -> Selection:
    if other.selection == []:
      return select
    else:
      return Selection(
        fmeta=select.fmeta,
        alias=select.alias,
        arguments=select.arguments,
        selection=list(
          other.selection
          | map(lambda s: next(
            select.selection
            | where(lambda s_: s_.fmeta.name == s.fmeta.name)
            | map(lambda s_: Selection.select(s_, s))
            | take(1)
          ))
        )
      )

  # TODO: Function to recover an approximate selection from a JSON data object
  @staticmethod
  def of_json(data: dict) -> Selection:
    pass


@dataclass(frozen=True)
class Query:
  name: Optional[str] = None
  selection: list[Selection] = field(default_factory=list)

  # Variables as query arguments, not the values of those variables
  # NOTE: Temporarily add the values with the definitions
  variables: list[VariableDefinition] = field(default_factory=list)

  @property
  def graphql_string(self) -> str:
    """ Returns a string containing a GraphQL query matching the current query

    Returns:
      str: The string containing the GraphQL query
    """
    selection_str = "\n".join(
      [select.graphql_string(level=1) for select in self.selection]
    )

    if len(self.variables) > 0:
      args_str = f'({", ".join([vardef.graphql_string for vardef in self.variables])})'
    else:
      args_str = ""

    return f"""query{args_str} {{\n{selection_str}\n}}"""

  @staticmethod
  def add_selections(query: Query, new_selections: list[Selection]) -> Query:
    """ Returns a new Query containing all selections in 'query' along with
    the new selections in `new_selections`

    Args:
      query (Query): The query to which new selections are to be added
      new_selections (list[Selection]): The new selections to be added to the query

    Returns:
      Query: A new `Query` objects containing all selections
    """
    return Query(
      name=query.name,
      selection=union(
        query.selection,
        new_selections,
        key=lambda select: select.fmeta.name,
        combine=Selection.combine
      )
    )

  @staticmethod
  def add_selection(query: Query, new_selection: Selection) -> Query:
    """ Same as `add_selections`, but for a single `new_selection`.

    Args:
      query (Query): The query to which new selections are to be added
      new_selection (Selection): The new selection to be added to the query

    Returns:
      Query: A new `Query` objects containing all selections
    """
    return Query.add_selections(query, [new_selection])

  @staticmethod
  def remove_selections(query: Query, selections_to_remove: list[Selection]) -> Query:
    """ Returns a new `Query` object containing all selections in `query` minus the selections
    sepcified in `selections_to_remove`.

    Note: Selections in `selections_to_remove` do not need to be "full" selections (i.e.: a selections all the way to
    leaves of the GraphQL schema).

    Args:
      query (Query): The query to which selections have to be removed
      selections_to_remove (list[Selection]): The selections to remove from the query

    Returns:
      Query: A new `Query` object containing the original query selections without the
      selections in `selections_to_remove`
    """
    def combine(select: Selection, selection_to_remove: Selection) -> Optional[Selection]:
      if selection_to_remove.selection == []:
        return None
      else:
        return Selection.remove_selections(select, selection_to_remove.selection)

    return Query(
      name=query.name,
      selection=filter_none(union(
        query.selection,
        selections_to_remove,
        key=lambda s: s.fmeta.name,
        combine=combine
      )),
      variables=query.variables
    )

  @staticmethod
  def remove_selection(query: Query, selection_to_remove: Selection) -> Query:
    """ Same as `remove_selections` but for a single selection

    Note: `selection_to_remove` does not need to be a "full" selection (i.e.: a selection all the way to
    leaves of the GraphQL schema).

    Example:
    ```python
    expected = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [])

    og_selection = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])

    selection_to_remove = Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [])

    new_selection = Selection.remove_selection(og_selection, selection_to_remove)
    self.assertEqual(new_selection, expected)
    ```

    Args:
        query (Query): The query to which a selection has to be removed
        selection_to_remove (Selection): The selection to remove from the query

    Returns:
      Query: A new `Query` object containing the original query selections without the
      selection `selection_to_remove`
    """
    return Query.remove_selections(query, [selection_to_remove])

  @staticmethod
  def remove(query: Query, other: Query) -> Query:
    """ Same as `remove_selections` but takes another `Query` object as argument
    instead of a list of selections

    Note: `other` does not need to include "full" selections (i.e.: selections all the way to
    leaves of the GraphQL schema).

    Args:
        query (Query): The query for which selections are to be removed
        other (Query): A query containing selections that will be removed from `query`

    Returns:
      Query: A new `Query` object containing the original query selections without the
      selections in `other`
    """
    return reduce(Query.remove_selection, other.selection, query)

  @staticmethod
  def combine(query: Query, other: Query) -> Query:
    """ Returns a new `Query` object containing the selections of both `query` and `other`

    Args:
      query (Query): A `Query` object
      other (Query): Another `Query` object

    Returns:
      Query: A new `Query` object containing the selections of both `query` and `other`
    """
    return Query(
      name=query.name,
      selection=union(
        query.selection,
        other.selection,
        key=lambda select: select.fmeta.name,
        combine=Selection.combine
      )
    )

  @staticmethod
  def transform(
    query: Query,
    variable_f: Callable[[VariableDefinition], VariableDefinition] = identity,
    selection_f: Callable[[Selection], Selection] = identity
  ) -> Query:
    return Query(
      name=query.name,
      selection=list(query.selection | map(selection_f) | traverse),
      variables=list(query.variables | map(variable_f) | traverse)
    )

  @staticmethod
  def contains_selection(query: Query, selection: Selection) -> bool:
    """ Returns True i.f.f. the `selection` is present in `query`

    Args:
      query (Query): A query object
      selection (Selection): The selection to be found (or not) in `query`

    Returns:
      bool: True if the `selection` is present in `query`, otherwise False
    """
    return any(
      query.selection
      | map(lambda select: Selection.contains(select, selection))
    )

  @staticmethod
  def contains_argument(query: Query, arg_name: str) -> bool:
    return any(query.selection | map(partial(Selection.contains_argument, arg_name=arg_name)))

  @staticmethod
  def get_argument(query: Selection, arg_name: str) -> Optional[Argument]:
    return next(query.selection | map(partial(Selection.get_argument, arg_name=arg_name)))

  @staticmethod
  def substitute_arg(query: Query, arg_name: str, replacement: Argument | list[Argument]) -> Query:
    return Query(
      name=query.name,
      selection=list(
        query.selection
        | map(partial(Selection.substitute_arg, arg_name=arg_name, replacement=replacement))
      ),
      variables=query.variables
    )

  @staticmethod
  def contains(query: Query, other: Query) -> bool:
    """ Returns True i.f.f. all selections in `other` are contained in `query`. In other words,
    returns true i.f.f. `other` is a subset of `query`.

    Note: `other` does not need to include "full" selections (i.e.: selections all the way to
    leaves of the GraphQL schema).

    Args:
      query (Query): The query that is to be checked
      other (Query): The query that has to be in `query`

    Returns:
      bool: True i.f.f. all selections in `other` are contained in `query`, otherwise False
    """
    return all(other.selection | map(partial(Query.contains_selection, query)))

  @staticmethod
  def select(query: Query, other: Query) -> Query:
    """ Returns a new Query

    Args:
        query (Query): [description]
        other (Query): [description]

    Returns:
        Query: [description]
    """
    return Query(
      name=query.name,
      selection=list(
        other.selection
        | map(lambda s: next(
          query.selection
          | where(lambda s_: s_.fmeta.name == s.fmeta.name)
          | map(lambda s_: Selection.select(s_, s))
          | take(1)
        ))
      ),
      variables=query.variables
    )


@dataclass(frozen=True)
class Fragment:
  name: str
  type_: TypeRef.T
  selection: list[Selection] = field(default_factory=list)

  # Variables as fragment arguments, not the values of those variables
  variables: list[VariableDefinition] = field(default_factory=list)

  @property
  def graphql_string(self):
    selection_str = "\n".join(
      [select.graphql_string(level=1) for select in self.selection]
    )
    return f"""fragment {self.name} on {TypeRef.root_type_name(self.type_)} {{\n{selection_str}\n}}"""

  @staticmethod
  def combine(frag: Fragment, other: Fragment) -> Fragment:
    pass

  @staticmethod
  def transform(frag: Fragment, f: Callable[[Selection], Selection]) -> Fragment:
    return Fragment(
      name=frag.name,
      type_=frag.type_,
      selection=list(frag.selection | map(f))
    )


@dataclass(frozen=True)
class Document:
  url: str
  query: Optional[Query]
  fragments: list[Fragment] = field(default_factory=list)

  # A list of variable assignments. For non-repeating queries
  # the list would be of length 1 (i.e.: only one set of query variable assignments)
  variables: list[dict[str, Any]] = field(default_factory=list)

  @property
  def graphql_string(self):
    return '\n'.join([self.query.graphql_string, *list(self.fragments | map(lambda frag: frag.graphql_string))])

  @staticmethod
  def mk_single_query(url: str, query: Query) -> Document:
    return Document(url, [query])

  @staticmethod
  def combine(doc: Document, other: Document) -> Document:
    return Document(
      url=doc.url,
      query=doc.query.combine(other.query),
      fragments=union(
        doc.fragments,
        other.fragments,
        key=lambda frag: frag.name,
        combine=Fragment.combine
      )
    )

  @staticmethod
  def transform(
    doc: Document,
    query_f: Callable[[Query], Query] = identity,
    fragment_f: Callable[[Fragment], Fragment] = identity
  ) -> Document:
    return Document(
      url=doc.url,
      query=query_f(doc.query),
      fragments=list(doc.fragments | map(fragment_f)),
      variables=doc.variables
    )


@dataclass(frozen=True)
class DataRequest:
  documents: list[Document] = field(default_factory=list)

  @staticmethod
  def combine(req: DataRequest, other: DataRequest) -> None:
    return DataRequest(
      documents=union(
        req.documents,
        other.documents,
        key=lambda doc: doc.url,
        combine=Document.combine
      )
    )

  @staticmethod
  def transform(req: DataRequest, f: Callable[[Document], Document]) -> DataRequest:
    return DataRequest(
      documents=list(req.documents | map(f))
    )

  @staticmethod
  def single_query(url: str, query: Query) -> DataRequest:
    return DataRequest([
      Document(url, query)
    ])

  @staticmethod
  def single_document(doc: Document) -> DataRequest:
    return DataRequest([doc])


def execute(request: DataRequest) -> list:
  def f(doc: Document) -> dict:
    match doc.variables:
      case []:
        return client.query(doc.url, doc.graphql_string)
      case [args]:
        return client.query(doc.url, doc.graphql_string, args)
      case args_list:
        return client.repeat(doc.url, doc.graphql_string, args_list)

  return list(request.documents | map(f))

  
# ================================================================
# Utility functions
# ================================================================
def input_value_of_string(type_: TypeRef.T, value: str) -> InputValue:
  match type_:
    case TypeRef.Named("ID"):
      return InputValue.String(value)

    case TypeRef.Named("Int"):
      return InputValue.Int(int(value))
    case TypeRef.Named("BigInt"):
      return InputValue.String(value)

    case (TypeRef.Named("Float")):
      return InputValue.Float(float(value))
    case (TypeRef.Named("BigDecimal")):
      return InputValue.String(value)

    case (TypeRef.Named("Boolean")):
      return InputValue.Boolean(bool(value))

    case (TypeRef.Named("String" | "Bytes")):
      return InputValue.String(value)
    case (TypeRef.Named()):
      return InputValue.Enum(value)

    case type_:
      raise TypeError(f"input_value_of_string: invalid type {type_}")


def input_value_of_value(type_: TypeRef.T, value: Any) -> InputValue:
  match type_:
    case (TypeRef.Named("ID"), _, str()):
      return InputValue.String(str(value))

    case TypeRef.Named("Int"):
      return InputValue.Int(int(value))
    case TypeRef.Named("BigInt"):
      return InputValue.String(str(value))

    case (TypeRef.Named("Float")):
      return InputValue.Float(float(value))
    case (TypeRef.Named("BigDecimal")):
      return InputValue.String(str(value))

    case (TypeRef.Named("Boolean")):
      return InputValue.Boolean(bool(value))

    case (TypeRef.Named("String" | "Bytes")):
      return InputValue.String(str(value))
    case (TypeRef.Named()):
      return InputValue.Enum(str(value))

    case type_:
      raise TypeError(f"input_value_of_value: invalid type {type_}")


def input_value_of_argument(
  schema: SchemaMeta,
  meta: TypeMeta,
  value: Any
) -> InputValue:
  def fmt_value(type_ref: TypeRef.T, value: Any, non_null=False):
    match (type_ref, schema.type_map[TypeRef.root_type_name(type_ref)], value):
      # Only allow Null values when non_null=True
      case (_, _, None):
        if not non_null:
          return InputValue.Null()
        else:
          raise TypeError(f"Argument {meta.name} cannot be None!")

      # If type is non_null, recurse with non_null=True
      case (TypeRef.NonNull(t), _, _):
        return fmt_value(t, value, non_null=True)

      case (TypeRef.Named("ID"), _, str()):
        return InputValue.String(value)

      case (TypeRef.Named("Int"), _, int()):
        return InputValue.Int(value)
      case (TypeRef.Named("BigInt"), _, int()):
        return InputValue.String(str(value))

      case (TypeRef.Named("Float"), _, int() | float()):
        return InputValue.Float(float(value))
      case (TypeRef.Named("BigDecimal"), _, int() | float()):
        return InputValue.String(str(float(value)))

      case (TypeRef.Named("String" | "Bytes"), _, str()):
        return InputValue.String(value)
      case (TypeRef.Named(), TypeMeta.EnumMeta(_), str()):
        return InputValue.Enum(value)

      case (TypeRef.Named("Boolean"), _, bool()):
        return InputValue.Boolean(value)

      case (TypeRef.List(t), _, list()):
        return InputValue.List([fmt_value(t, val, non_null) for val in value])

      case (TypeRef.Named(), TypeMeta.InputObjectMeta() as input_object, dict()):
        return InputValue.Object({key: fmt_value(typeref_of_input_field(input_object, key), val, non_null) for key, val in value.items()})

      case (value, typ, non_null):
        raise TypeError(f"mk_input_value({value}, {typ}, {non_null})")

  match meta:
    case TypeMeta.ArgumentMeta(type_=type_):
      return fmt_value(type_, value)
    case _:
      raise TypeError(f"input_value_of_argument: TypeMeta {meta.name} is not of type TypeMeta.ArgumentMeta")


def add_object_field(
  object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta,
  field: TypeMeta.FieldMeta
) -> None:
  object_.fields.append(field)


def arguments_of_field_args(
  schema: SchemaMeta,
  field: TypeMeta.FieldMeta,
  args: Optional[dict[str, Any]]
) -> list[Argument]:
  if args is None:
    args = {}

  def f(arg_meta: TypeMeta.ArgumentMeta) -> Optional[Argument]:
    if arg_meta.name in args:
      return Argument(
        arg_meta.name,
        input_value_of_argument(schema, arg_meta, args[arg_meta.name])
      )
    else:
      if (arg_meta.default_value) or (not TypeRef.is_non_null(arg_meta.type_)):
        return None
      else:
        raise TypeError(f"arguments_of_field_args: Argument {arg_meta.name} of field {field.name} is required but not provided!")

  # TODO: Add warnings if arguments are not used

  match field:
    case TypeMeta.FieldMeta() as field:
      args = [f(arg_meta) for arg_meta in field.arguments]
      return list(filter(lambda arg: arg is not None, args))
    case _:
      raise TypeError(f"arguments_of_field_args: TypeMeta {field.name} is not of type FieldMeta")


def selection_of_path(
  schema: SchemaMeta,
  fpath: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]
) -> list[Selection]:
  match fpath:
    case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
      return [Selection(
        fmeta,
        arguments=arguments_of_field_args(schema, fmeta, args),
        selection=selection_of_path(schema, rest)
      )]
    case []:
      return []


def pagination_args(page_size: int, num_entities: int) -> list[dict[str, int]]:
  num_pages = math.ceil(num_entities / page_size)
  
  return [
    {'first': num_entities % page_size, 'skip': i * page_size} if (i == num_pages - 1 and num_entities % page_size != 0)
    else {'first': page_size, 'skip': i * page_size}
    for i in range(0, num_pages)
  ]
