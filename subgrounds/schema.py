from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import Field, dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple, NewType
from subgrounds.query import *
from subgrounds.utils import flatten

# ================================================================
# Schema definitions, data structures and types
# ================================================================
class TypeError(Exception):
  pass

class TypeRef:
  class T(ABC):
    pass

  @dataclass
  class Named(T):
    name: str

  @dataclass
  class NonNull(T):
    inner: TypeRef.T

  @dataclass
  class List(T):
    inner: TypeRef.T

  @staticmethod
  def root_type_name(type_: TypeRef.T) -> str:
    match type_:
      case TypeRef.NonNull(inner) | TypeRef.List(inner):
        return TypeRef.root_type_name(inner)
      case TypeRef.Named(name):
        return name

  @staticmethod
  def non_null(name: str):
    return TypeRef.NonNull(TypeRef.Named(name))

  @staticmethod
  def non_null_list(name: str):
    return TypeRef.NonNull(TypeRef.List(TypeRef.NonNull(TypeRef.Named(name))))

  @staticmethod
  def is_non_null(type_: TypeRef.T):
    match type_:
      case TypeRef.NonNull():
        return True
      case _:
        return False

class TypeMeta:
  @dataclass
  class T(ABC):
    name: str
    description: str

  @dataclass
  class ArgumentMeta(T):
    type_: TypeRef.T
    default_value: Optional[str]

  @dataclass
  class FieldMeta(T):
    arguments: List[TypeMeta.ArgumentMeta]
    type_: TypeRef.T

  @dataclass
  class SyntheticFieldMeta(T):
    func: function
    dependencies: List[(
      # E.g.: In `Entity.newField = Entity.sfield1 + Entity.sfield2 / Entity.sfield3`, `Entity.sfield2 / Entity.sfield3` is an anonymous synthetic field
      TypeMeta.SyntheticFieldMeta
      # Field Path
      | List[Tuple[Optional[List[Argument]], TypeMeta.FieldMeta] | TypeMeta.SyntheticFieldMeta] 
      # Constants
      | int 
      | float 
      | str
    )]

  @dataclass
  class ScalarMeta(T):
    pass

  @dataclass
  class ObjectMeta(T):
    fields: List[TypeMeta.FieldMeta | TypeMeta.SyntheticFieldMeta]
    interfaces: List[str] = field(default_factory=list)

  @dataclass
  class EnumValueMeta(T):
    pass

  @dataclass
  class EnumMeta(T):
    values: List[TypeMeta.EnumValueMeta]

  @dataclass
  class InterfaceMeta(T):
    fields: List[TypeMeta.FieldMeta | TypeMeta.SyntheticFieldMeta]

  @dataclass
  class UnionMeta(T):
    types: List[str]

  @dataclass
  class InputObjectMeta(T):
    input_fields: List[TypeMeta.ArgumentMeta]

@dataclass
class SchemaMeta:
  query_type: str
  type_map: Dict[str, TypeMeta.T]
  mutation_type: Optional[str] = None
  subscription_type: Optional[str] = None

# ================================================================
# Schema parsing
# ================================================================
class ParsingError(Exception):
  pass

def mk_schema(json):
  def mk_type_ref(json: dict) -> TypeRef.T:
    match json:
      case {'kind': 'NON_NULL', 'ofType': inner}:
        return TypeRef.NonNull(mk_type_ref(inner))
      case {'kind': 'LIST', 'ofType': inner}:
        return TypeRef.List(mk_type_ref(inner))
      case {'kind': 'SCALAR' | 'OBJECT' | 'INTERFACE' | 'ENUM' | 'INPUT_OBJECT', 'name': name}:
        return TypeRef.Named(name)
      case _ as json:
        raise ParsingError(f"mk_type_ref: {json}")

  def mk_argument_meta(json: dict) -> TypeMeta.ArgumentMeta:
    match json:
      case {'name': name, 'description': desc, 'type': type_, 'defaultValue': default_value}:
        return TypeMeta.ArgumentMeta(name=name, type_=mk_type_ref(type_), description=desc, default_value=default_value)
      case _ as json:
        raise ParsingError(f"mk_argument_meta: {json}")

  def mk_field_meta(json: dict) -> TypeMeta.FieldMeta:
    match json:
      case {'name': name, 'description': desc, 'args': args, 'type': type_}:
        return TypeMeta.FieldMeta(name, arguments=[mk_argument_meta(arg) for arg in args], type_=mk_type_ref(type_), description=desc)
      case _ as json:
        raise ParsingError(f"mk_field_meta: {json}")

  def mk_enum_value(json: dict) -> TypeMeta.EnumValueMeta:
    match json:
      case {'name': name, 'description': desc}:
        return TypeMeta.EnumValueMeta(name, description=desc)
      case _ as json:
        raise ParsingError(f"mk_enum_value: {json}")

  def mk_type_meta(json: dict) -> TypeMeta:
    match json:
      case {'kind': 'SCALAR', 'name': name, 'description': desc}:
        return TypeMeta.ScalarMeta(name, description=desc)
      case {'kind': 'OBJECT', 'name': name, 'description': desc, 'fields': fields, 'interfaces': intfs}:
        return TypeMeta.ObjectMeta(name, fields=[mk_field_meta(f) for f in fields], interfaces=intfs, description=desc)
      case {'kind': 'ENUM', 'name': name, 'description': desc, 'enumValues': enum_values}:
        return TypeMeta.EnumMeta(name, values=[mk_enum_value(val) for val in enum_values], description=desc)
      case {'kind': 'INTERFACE', 'name': name, 'description': desc, 'fields': fields}:
        return TypeMeta.InterfaceMeta(name, fields=[mk_field_meta(f) for f in fields], description=desc)
      case {'kind': 'UNION', 'name': name, 'description': desc, 'possibleTypes': types}:
        return TypeMeta.UnionMeta(name, types=types, description=desc)
      case {'kind': 'INPUT_OBJECT', 'name': name, 'description': desc, 'inputFields': input_feilds}:
        return TypeMeta.InputObjectMeta(name,  input_fields=[mk_argument_meta(f) for f in input_feilds], description=desc)
      case _ as json:
        raise ParsingError(f"mk_type_meta: {json}")

  match json['__schema']:
    case {'queryType': query_type, 'types': types}:
      try:
        mutation_type = json['__schema']['mutationType']['name']
      except:
        mutation_type = None

      try:
        subscription_type = json['__schema']['subscriptionType']['name']
      except:
        subscription_type = None

      types_meta = [mk_type_meta(type_) for type_ in types]
      schema = SchemaMeta(
        query_type=query_type,
        mutation_type=mutation_type,
        subscription_type=subscription_type,
        type_map={type_.name: type_ for type_ in types_meta}
      )

      return schema

    case _ as json:
      raise ParsingError(f"mk_schema: {json}")

# ================================================================
# Utility functions
# ================================================================
def field_of_object(meta: TypeMeta.T, fname: str) -> TypeMeta.FieldMeta:
  match meta:
    case TypeMeta.ObjectMeta(fields=fields) | TypeMeta.InterfaceMeta(fields=fields):
      return next(filter(lambda field: field.name == fname, fields))
    case _:
      raise TypeError(f"field_of_object: TypeMeta {meta.name} is not of type TypeMeta.ObjectMeta or TypeMeta.InterfaceMeta")

def type_of_arg(schema: SchemaMeta, meta: TypeMeta.T) -> TypeMeta.T:
  match meta:
    case TypeMeta.ArgumentMeta(type_=type_):
      tname = TypeRef.root_type_name(type_)
      return schema.type_map[tname]
    case _:
      raise TypeError(f"type_of_arg: TypeMeta {meta.name} is not of type TypeMeta.ArgumentMeta")

def type_of_field(schema: SchemaMeta, meta: TypeMeta.T) -> TypeMeta.T:
  match meta:
    case TypeMeta.FieldMeta(type_=type_):
      tname = TypeRef.root_type_name(type_)
      return schema.type_map[tname]
    case TypeMeta.SyntheticFieldMeta() as type_:
      return type_
    case _:
      raise TypeError(f"type_of_field: TypeMeta {meta.name} is not a field type")

def typeref_of_input_field(meta: TypeMeta.T, fname: str) -> TypeRef.T:
  match meta:
    case TypeMeta.InputObjectMeta(input_fields=input_fields):
      arg = next(filter(lambda field: field.name == fname, input_fields))
      return arg.type_
    case _:
      raise TypeError(f"type_of_field: TypeMeta {meta.name} is not of type TypeMeta.FieldMeta")

def input_value_of_string(type_: TypeRef.T, value: str) -> InputValue:
  match type_:
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

def input_value_of_argument(schema: SchemaMeta, meta: TypeMeta.T, value: Any) -> InputValue:
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
        return fmt_value(value, t, non_null=True)

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
        return InputValue.List([fmt_value(val, t, non_null) for val in value])

      case (TypeRef.Named(), TypeMeta.InputObjectMeta() as input_object, dict()):
        return InputValue.Object({key: fmt_value(typeref_of_input_field(input_object, key), val, non_null) for key, val in value.items()})

      case (value, typ, non_null):
        raise TypeError(f"mk_input_value({value}, {typ}, {non_null})")
  
  match meta:
    case TypeMeta.ArgumentMeta(type_=type_):
      return fmt_value(type_, value)
    case _:
      raise TypeError(f"input_value_of_argument: TypeMeta {meta.name} is not of type TypeMeta.ArgumentMeta")

def add_object_field(object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta, sfield: TypeMeta.T) -> None:
  object_.fields.append(sfield)

def arguments_of_field_args(schema: SchemaMeta, field: TypeMeta.FieldMeta, args: Dict[str, Any]) -> List[Argument]:
  def f(arg_meta):
    if arg_meta.name in args:
      return Argument(arg_meta.name, input_value_of_argument(schema, arg_meta, args[arg_meta.name]))
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
      raise TypeError(f"arguments_of_field_args: TypeMeta {field.name} is not of type TypeMeta.FieldMeta")

def selections_of_synthetic_field(sfmeta: TypeMeta.SyntheticFieldMeta) -> List[Selection]:
  def selection_of_fmeta_path(path: List[TypeMeta.T]) -> List[Selection]:
    match path:
      case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
        return [Selection(fmeta.name, arguments=args, selection=selection_of_fmeta_path(rest))]
      case [TypeMeta.SyntheticFieldMeta() as sfield]:
        return selections_of_synthetic_field(sfield)
      case []:
        return None
      case _:
        raise Exception(f"selection_of_fmeta_path: Unexpected dependency path {path}")

  def selection_of_dep(dep):
    match dep:
      case int() | float() | str() | bool():
        return []
      case TypeMeta.SyntheticFieldMeta() as sfield:
        return selections_of_synthetic_field(sfield)
      case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
        return [Selection(fmeta.name, arguments=args, selection=selection_of_fmeta_path(rest))]
      case _:
        raise Exception(f"selection_of_dep: Unexpected dependency {dep}")

  match sfmeta:
    case TypeMeta.SyntheticFieldMeta():
      return flatten([selection_of_dep(dep) for dep in sfmeta.dependencies])
    case _:
      raise TypeError(f"selections_of_synthetic_field: TypeMeta {sfmeta.name} is not of type TypeMeta.SyntheticFieldMeta")

Path = List[
  Tuple[Optional[List[Argument]], TypeMeta.FieldMeta] 
  | TypeMeta.SyntheticFieldMeta
]
def selections_of_path(fpath: Path) -> List[Selection]:
  def selection_of_fmeta_path(path: List[TypeMeta.T]) -> List[Selection]:
    match path:
      case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
        return [Selection(fmeta.name, arguments=args, selection=selection_of_fmeta_path(rest))]
      case [TypeMeta.SyntheticFieldMeta() as sfield]:
        return selections_of_synthetic_field(sfield)
      case []:
        return None
      case _:
        raise Exception(f"selection_of_fmeta_path: Unexpected dependency path {path}")

  def selection_of_sfmeta_dep(dep):
    match dep:
      case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
        return [Selection(fmeta.name, arguments=args, selection=selection_of_fmeta_path(rest))]
      case TypeMeta.SyntheticFieldMeta() as sfmeta:
        return flatten(map(selection_of_sfmeta_dep, sfmeta.dependencies))
      case int() | float() | str() | bool():
        return []
      case _:
        raise Exception(f"selection_of_dep: Unexpected dependency {dep}")

  return selection_of_fmeta_path(fpath)

def apply_field_path(schema: SchemaMeta, fpath: Path, data: dict) -> dict:
  def field_path_data(fpath: Path, data: dict) -> Any:
    match (fpath, data):
      case ([(_, TypeMeta.FieldMeta() as fmeta)], dict()):
        return data[fmeta.name]
      case ([(_, TypeMeta.FieldMeta() as fmeta), *rest], dict()):
        return field_path_data(rest, data[fmeta.name])

      case ([TypeMeta.SyntheticFieldMeta() as sfmeta], dict()):
        apply_synthetic_field(sfmeta, data)
        return data[sfmeta.name]

      case ([(_, TypeMeta.FieldMeta() as fmeta), *rest], _):
        raise TypeError(f"field_path_data: data is not a dictionary! {data}")
      case (_, _):
        raise TypeError(f"field_path_data: Unexpected fpath {fpath}")

  def get_arg(arg, data):
    match arg:
      case TypeMeta.SyntheticFieldMeta() as sfmeta:
        args = list(map(lambda arg: get_arg(arg, data), sfmeta.dependencies))
        return sfmeta.func(*args)
      case list():
        apply_field_path(schema, arg, data)
        return field_path_data(arg, data)
      case int() | float() | str() | bool():
        return arg
      case TypeMeta.SyntheticFieldMeta() | list():
        return field_path_data(arg, data)

  def apply_synthetic_field(sfmeta: TypeMeta.SyntheticFieldMeta, data: dict) -> None:
    args = list(map(lambda arg: get_arg(arg, data), sfmeta.dependencies))
    data[sfmeta.name] = sfmeta.func(*args)

  def apply_field(fmeta: TypeMeta.FieldMeta, data: dict) -> None:
    match type_of_field(schema, fmeta):
      case TypeMeta.ScalarMeta("BigInt"):
        data[fmeta.name] = int(data[fmeta.name])
      case TypeMeta.ScalarMeta("BigDecimal"):
        data[fmeta.name] = float(data[fmeta.name])
      case TypeMeta.ScalarMeta("Boolean"):
        data[fmeta.name] = bool(data[fmeta.name])
    
  match (fpath, data):
    case ([(_, TypeMeta.FieldMeta() as fmeta)], dict()):
      apply_field(fmeta, data)
    case ([(_, TypeMeta.FieldMeta() as fmeta)], list()):
      for item in data:
        apply_field(fmeta, item)

    case ([(_, TypeMeta.FieldMeta() as fmeta), *rest], dict()):
      apply_field_path(schema, rest, data[fmeta.name])
    case ([(_, TypeMeta.FieldMeta() as fmeta), *rest], list()):
      for item in data:
        apply_field_path(schema, rest, item[fmeta.name])

    case ([TypeMeta.SyntheticFieldMeta() as sfmeta], dict()):
      apply_synthetic_field(sfmeta, data)
    case ([TypeMeta.SyntheticFieldMeta() as sfmeta], list()):
      for item in data:
        apply_synthetic_field(sfmeta, item)