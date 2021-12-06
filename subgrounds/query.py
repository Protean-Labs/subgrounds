from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

from subgrounds.schema import (
  TypeMeta,
  SchemaMeta,
  TypeRef,
  typeref_of_input_field
)

# ================================================================
# Query definitions, data structures and types
# ================================================================


class InputValue:
  @dataclass
  class T(ABC):
    @abstractmethod
    def graphql_string(self) -> str:
      pass

  @dataclass
  class Null(T):
    def graphql_string(self) -> str:
      return "null"

  @dataclass
  class Int(T):
    value: int

    def graphql_string(self) -> str:
      return str(self.value)

  @dataclass
  class Float(T):
    value: float

    def graphql_string(self) -> str:
      return str(self.value)

  @dataclass
  class String(T):
    value: str

    def graphql_string(self) -> str:
      return f"\"{self.value}\""

  @dataclass
  class Boolean(T):
    value: bool

    def graphql_string(self) -> str:
      return str(self.value).lower()

  @dataclass
  class Enum(T):
    value: str

    def graphql_string(self) -> str:
      return self.value

  @dataclass
  class List(T):
    value: list[InputValue.T]

    def graphql_string(self) -> str:
      return f"[{', '.join([val.graphql_string() for val in self.value])}]"

  @dataclass
  class Object(T):
    value: dict[str, InputValue.T]

    def graphql_string(self) -> str:
      return f"{{{', '.join([f'{key}: {value.graphql_string()}' for key, value in self.value.items()])}}}"


@dataclass
class Argument:
  name: str
  value: InputValue

  def graphql_string(self) -> str:
    return f"{self.name}: {self.value.graphql_string()}"


@dataclass
class Selection:
  # name: str
  fmeta: TypeMeta.FieldMeta
  alias: Optional[str] = None
  arguments: list[Argument] = field(default_factory=list)
  selection: list[Selection] = field(default_factory=list)

  def graphql_string(self, level: int = 0) -> str:
    indent = "  " * level

    match (self.arguments, self.selection):
      case (None | [], None | []):
        return f"{indent}{self.fmeta.name}"
      case (args, None | []):
        args_str = "(" + ", ".join([arg.graphql_string() for arg in args]) + ")"
        return f"{indent}{self.fmeta.name}{args_str}"
      case (None | [], inner_selection):
        inner_str = "\n".join(
          [f.graphql_string(level=level + 1) for f in inner_selection]
        )
        return f"{indent}{self.fmeta.name} {{\n{inner_str}\n{indent}}}"
      case (args, inner_selection):
        args_str = "(" + ", ".join(
          [arg.graphql_string() for arg in args]
        ) + ")"
        inner_str = "\n".join([f.graphql_string(level=level + 1) for f in inner_selection])
        return f"{indent}{self.fmeta.name}{args_str} {{\n{inner_str}\n{indent}}}"

  def add_selection(self, new_selection: Selection) -> None:
    if self.selection is None:
      self.selection = []

    try:
      select = next(filter(lambda select: select.fmeta.name == new_selection.fmeta.name, self.selection))
      for new_select in new_selection.selection:
        select.add_selection(new_select)

    except StopIteration:
      self.selection.append(new_selection)

  def add_selections(self, new_selections):
    for s in new_selections:
      self.add_selection(s)


@dataclass
class Query:
  selection: Optional[list[Selection]] = None

  def graphql_string(self) -> str:
    selection_str = "\n".join(
      [f.graphql_string(level=1) for f in self.selection]
    )
    return f"""query {{\n{selection_str}\n}}"""

  def add_selection(self, new_selection: Selection) -> None:
    if self.selection is None:
      self.selection = []

    try:
      select = next(filter(
        lambda select: select.fmeta.name == new_selection.fmeta.name,
        self.selection
      ))

      for s in new_selection.selection:
        select.add_selection(s)

    except StopIteration:
      self.selection.append(new_selection)

  def add_selections(self, new_selections):
    for s in new_selections:
      self.add_selection(s)


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
