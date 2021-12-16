from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple

from subgrounds.schema import (
  TypeMeta,
  SchemaMeta,
  TypeRef,
  typeref_of_input_field
)
from subgrounds.utils import identity, union


# ================================================================
# Query definitions, data structures and types
# ================================================================
class InputValue:
  @dataclass(frozen=True)
  class T(ABC):
    @property
    @abstractmethod
    def graphql_string(self) -> str:
      pass

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

  @dataclass(frozen=True)
  class Float(T):
    value: float

    @property
    def graphql_string(self) -> str:
      return str(self.value)

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
  def combine(select: Selection, other: Selection) -> Selection:
    if select.fmeta != other.fmeta:
      raise Exception(f"Selection.combine: {select.fmeta} != {other.fmeta}")

    return Selection(
      fmeta=select.fmeta,
      alias=select.alias,
      arguments=select.arguments,
      selection=union(
        select.selection,
        other.selection,
        key=lambda select: select.fmeta.name,
        combine=Selection.combine
      )
    )


@dataclass(frozen=True)
class Query:
  name: Optional[str] = None
  selection: list[Selection] = field(default_factory=list)

  # Variables as query arguments, not the values of those variables
  # NOTE: Temporarily add the values with the definitions
  variables: list[Tuple[VariableDefinition, Any]] = field(default_factory=list)

  @property
  def graphql_string(self) -> str:
    selection_str = "\n".join(
      [select.graphql_string(level=1) for select in self.selection]
    )
    return f"""query {{\n{selection_str}\n}}"""

  @staticmethod
  def add_selections(query: Query, new_selections: list[Selection]) -> Query:
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
    return Query.add_selections(query, [new_selection])

  @staticmethod
  def combine(query: Query, other: Query) -> Query:
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
    variable_f: Callable[[Tuple[VariableDefinition, Any]], Tuple[VariableDefinition, Any]] = identity,
    selection_f: Callable[[Selection], Selection] = identity
  ) -> Query:
    return Query(
      name=query.name,
      selection=list(map(selection_f, query.selection)),
      variables=list(map(variable_f, query.variables))
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
      selection=list(map(f, frag.selection))
    )


@dataclass(frozen=True)
class Document:
  url: str
  query: Optional[Query]
  fragments: list[Fragment] = field(default_factory=list)

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
      fragments=list(map(fragment_f, doc.fragments))
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
      documents=list(map(f, req.documents))
    )


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
