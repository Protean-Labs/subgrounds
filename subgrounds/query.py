from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from subgrounds.schema import (
  ArgumentMeta,
  EnumMeta,
  FieldMeta,
  InputObjectMeta,
  InterfaceMeta,
  ObjectMeta,
  SchemaMeta,
  TypeMeta,
  TypeRef,
  # type_of_field,
  typeref_of_input_field
)

# ================================================================
# Query definitions, data structures and types
# ================================================================


@dataclass
class InputValue(ABC):
  @abstractmethod
  def graphql_string(self) -> str:
    pass


@dataclass
class Null(InputValue):
  def graphql_string(self) -> str:
    return "null"


@dataclass
class Int(InputValue):
  value: int

  def graphql_string(self) -> str:
    return str(self.value)


@dataclass
class Float(InputValue):
  value: float

  def graphql_string(self) -> str:
    return str(self.value)


@dataclass
class String(InputValue):
  value: str

  def graphql_string(self) -> str:
    return f"\"{self.value}\""


@dataclass
class Boolean(InputValue):
  value: bool

  def graphql_string(self) -> str:
    return str(self.value).lower()


@dataclass
class Enum(InputValue):
  value: str

  def graphql_string(self) -> str:
    return self.value


@dataclass
class List(InputValue):
  value: list[InputValue]

  def graphql_string(self) -> str:
    return f"[{', '.join([val.graphql_string() for val in self.value])}]"


@dataclass
class Object(InputValue):
  value: dict[str, InputValue]

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
  name: str
  alias: Optional[str] = None
  arguments: Optional[list[Argument]] = None
  selection: Optional[list[Selection]] = None

  def graphql_string(self, level: int = 0) -> str:
    indent = "  " * level

    match (self.arguments, self.selection):
      case (None | [], None | []):
        return f"{indent}{self.name}"
      case (args, None | []):
        args_str = "(" + ", ".join([arg.graphql_string() for arg in args]) + ")"
        return f"{indent}{self.name}{args_str}"
      case (None | [], inner_selection):
        inner_str = "\n".join(
          [f.graphql_string(level=level + 1) for f in inner_selection]
        )
        return f"{indent}{self.name} {{\n{inner_str}\n{indent}}}"
      case (args, inner_selection):
        args_str = "(" + ", ".join(
          [arg.graphql_string() for arg in args]
        ) + ")"
        inner_str = "\n".join([f.graphql_string(level=level + 1) for f in inner_selection])
        return f"{indent}{self.name}{args_str} {{\n{inner_str}\n{indent}}}"

  def add_selection(self, new_selection):
    if self.selection is None:
      self.selection = []

    try:
      select = next(filter(lambda select: select.name == new_selection.name, self.selection))
      for s in new_selection.selection:
        select.add_selection(s)
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

  def add_selection(self, new_selection):
    if self.selection is None:
      self.selection = []

    try:
      select = next(filter(
        lambda select: select.name == new_selection.name,
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
      return String(value)

    case TypeRef.Named("Int"):
      return Int(int(value))
    case TypeRef.Named("BigInt"):
      return String(value)

    case (TypeRef.Named("Float")):
      return Float(float(value))
    case (TypeRef.Named("BigDecimal")):
      return String(value)

    case (TypeRef.Named("Boolean")):
      return Boolean(bool(value))

    case (TypeRef.Named("String" | "Bytes")):
      return String(value)
    case (TypeRef.Named()):
      return Enum(value)

    case type_:
      raise TypeError(f"input_value_of_string: invalid type {type_}")


def input_value_of_value(type_: TypeRef.T, value: Any) -> InputValue:
  match type_:
    case (TypeRef.Named("ID"), _, str()):
      return String(str(value))

    case TypeRef.Named("Int"):
      return Int(int(value))
    case TypeRef.Named("BigInt"):
      return String(str(value))

    case (TypeRef.Named("Float")):
      return Float(float(value))
    case (TypeRef.Named("BigDecimal")):
      return String(str(value))

    case (TypeRef.Named("Boolean")):
      return Boolean(bool(value))

    case (TypeRef.Named("String" | "Bytes")):
      return String(str(value))
    case (TypeRef.Named()):
      return Enum(str(value))

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
          return Null()
        else:
          raise TypeError(f"Argument {meta.name} cannot be None!")

      # If type is non_null, recurse with non_null=True
      case (TypeRef.NonNull(t), _, _):
        return fmt_value(t, value, non_null=True)

      case (TypeRef.Named("ID"), _, str()):
        return String(value)

      case (TypeRef.Named("Int"), _, int()):
        return Int(value)
      case (TypeRef.Named("BigInt"), _, int()):
        return String(str(value))

      case (TypeRef.Named("Float"), _, int() | float()):
        return Float(float(value))
      case (TypeRef.Named("BigDecimal"), _, int() | float()):
        return String(str(float(value)))

      case (TypeRef.Named("String" | "Bytes"), _, str()):
        return String(value)
      case (TypeRef.Named(), EnumMeta(_), str()):
        return Enum(value)

      case (TypeRef.Named("Boolean"), _, bool()):
        return Boolean(value)

      case (TypeRef.List(t), _, list()):
        return List([fmt_value(t, val, non_null) for val in value])

      case (TypeRef.Named(), InputObjectMeta() as input_object, dict()):
        return Object({key: fmt_value(typeref_of_input_field(input_object, key), val, non_null) for key, val in value.items()})

      case (value, typ, non_null):
        raise TypeError(f"mk_input_value({value}, {typ}, {non_null})")

  match meta:
    case ArgumentMeta(type_=type_):
      return fmt_value(type_, value)
    case _:
      raise TypeError(f"input_value_of_argument: TypeMeta {meta.name} is not of type TypeMeta.ArgumentMeta")


def add_object_field(
  object_: ObjectMeta | InterfaceMeta,
  field: FieldMeta
) -> None:
  object_.fields.append(field)


def arguments_of_field_args(
  schema: SchemaMeta,
  field: FieldMeta,
  args: dict[str, Any]
) -> list[Argument]:
  def f(arg_meta: ArgumentMeta) -> Optional[Argument]:
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
    case FieldMeta() as field:
      args = [f(arg_meta) for arg_meta in field.arguments]
      return list(filter(lambda arg: arg is not None, args))
    case _:
      raise TypeError(f"arguments_of_field_args: TypeMeta {field.name} is not of type FieldMeta")


def selection_of_path(
  schema: SchemaMeta,
  fpath: list[Tuple[Optional[dict[str, Any]], FieldMeta]]
) -> Optional[Selection]:
  match fpath:
    case [(args, FieldMeta() as fmeta), *rest]:
      return Selection(
        fmeta.name,
        arguments=arguments_of_field_args(schema, fmeta, args),
        selection=selection_of_path(schema, rest)
      )
    case []:
      return None
