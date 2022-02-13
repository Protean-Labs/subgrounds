from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from pipe import where


# ================================================================
# Schema definitions, data structures and types
# ================================================================
class TypeRef:
  class T(ABC):
    pass

    @property
    @abstractmethod
    def name(self) -> str:
      raise NotImplementedError

    @property
    @abstractmethod
    def is_list(self) -> str:
      raise NotImplementedError

    @property
    @abstractmethod
    def is_non_null(self) -> str:
      raise NotImplementedError

  @dataclass
  class Named(T):
    name_: str

    @property
    def name(self) -> str:
      return self.name_

    @property
    def is_list(self) -> str:
      return False

    @property
    def is_non_null(self) -> str:
      return False

  @dataclass
  class NonNull(T):
    inner: TypeRef.T

    @property
    def name(self) -> str:
      return self.inner.name

    @property
    def is_list(self) -> str:
      return self.inner.is_list

    @property
    def is_non_null(self) -> str:
      return True

  @dataclass
  class List(T):
    inner: TypeRef.T

    @property
    def name(self) -> str:
      return self.inner.name

    @property
    def is_list(self) -> str:
      return True

    @property
    def is_non_null(self) -> str:
      return self.inner.is_non_null

  @staticmethod
  def non_null(name: str) -> TypeRef.T:
    return TypeRef.NonNull(TypeRef.Named(name))

  @staticmethod
  def non_null_list(name: str) -> TypeRef.T:
    return TypeRef.NonNull(TypeRef.List(TypeRef.NonNull(TypeRef.Named(name))))

  @staticmethod
  def root_type_name(type_: TypeRef.T) -> str:
    return type_.name

  @staticmethod
  def is_non_null(type_: TypeRef.T) -> bool:
    return type_.is_non_null

  @staticmethod
  def is_list(type_: TypeRef.T) -> bool:
    return type_.is_list

  @staticmethod
  def graphql(type_: TypeRef.T) -> str:
    match type_:
      case TypeRef.Named(name):
        return name
      case TypeRef.NonNull(t):
        return f'{TypeRef.graphql(t)}!'
      case TypeRef.List(t):
        return f'[{TypeRef.graphql(t)}]'


class TypeMeta:
  @dataclass
  class T(ABC):
    name: str
    description: str

    @property
    def is_object(self) -> bool:
      return False

  @dataclass
  class ArgumentMeta(T):
    type_: TypeRef.T
    default_value: Optional[str]

  @dataclass
  class FieldMeta(T):
    arguments: list[TypeMeta.ArgumentMeta]
    type_: TypeRef.T

    def has_arg(self, argname: str) -> bool:
      try:
        next(self.arguments | where(lambda arg: arg.name == argname))
        return True
      except StopIteration:
        return False

  @dataclass
  class ScalarMeta(T):
    pass

  @dataclass
  class ObjectMeta(T):
    fields: list[TypeMeta.FieldMeta]
    interfaces: list[str] = field(default_factory=list)

    @property
    def is_object(self) -> bool:
      return True

  @dataclass
  class EnumValueMeta(T):
    pass

  @dataclass
  class EnumMeta(T):
    values: list[TypeMeta.EnumValueMeta]

  @dataclass
  class InterfaceMeta(T):
    fields: list[TypeMeta.FieldMeta]

    @property
    def is_object(self) -> bool:
      return False

  @dataclass
  class UnionMeta(T):
    types: list[str]

  @dataclass
  class InputObjectMeta(T):
    input_fields: list[TypeMeta.ArgumentMeta]


@dataclass
class SchemaMeta:
  query_type: str
  type_map: dict[str, TypeMeta.T]
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

  def mk_type_meta(json: dict) -> TypeMeta.T:
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
        return TypeMeta.InputObjectMeta(name, input_fields=[mk_argument_meta(f) for f in input_feilds], description=desc)

      case _ as json:
        raise ParsingError(f"mk_type_meta: {json}")

  match json['__schema']:
    case {'queryType': query_type, 'types': types}:
      try:
        mutation_type = json['__schema']['mutationType']['name']
      except (KeyError, TypeError):
        mutation_type = None

      try:
        subscription_type = json['__schema']['subscriptionType']['name']
      except (KeyError, TypeError):
        subscription_type = None

      types_meta = [mk_type_meta(type_) for type_ in types]
      schema = SchemaMeta(
        query_type=query_type,
        mutation_type=mutation_type,
        subscription_type=subscription_type,
        type_map={type_.name: type_ for type_ in types_meta}
      )

      # Manually add Float type
      schema.type_map['Float'] = TypeMeta.ScalarMeta('Float', '')

      return schema

    case _ as json:
      raise ParsingError(f"mk_schema: {json}")


# ================================================================
# Utility functions
# ================================================================
def field_of_object(meta: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta, fname: str) -> TypeMeta.FieldMeta:
  match meta:
    case TypeMeta.ObjectMeta(fields=fields) | TypeMeta.InterfaceMeta(fields=fields):
      return next(filter(lambda field: field.name == fname, fields))
    case _:
      raise TypeError(f"field_of_object: TypeMeta {meta.name} is not of type ObjectMeta or InterfaceMeta")


def type_of_arg(schema: SchemaMeta, meta: TypeMeta.T) -> TypeMeta.T:
  match meta:
    case TypeMeta.ArgumentMeta(type_=type_):
      tname = TypeRef.root_type_name(type_)
      return schema.type_map[tname]
    case _:
      raise TypeError(f"type_of_arg: TypeMeta {meta.name} is not of type ArgumentMeta")


def type_of_field(schema: SchemaMeta, meta: TypeMeta.T) -> TypeMeta.T:
  match meta:
    case TypeMeta.FieldMeta(type_=type_):
      tname = TypeRef.root_type_name(type_)
      return schema.type_map[tname]
    case _:
      raise TypeError(f"type_of_field: TypeMeta {meta.name} is not a field type")


def type_of_typeref(schema: SchemaMeta, typeref: TypeRef.T) -> TypeMeta.T:
  tname = TypeRef.root_type_name(typeref)
  return schema.type_map[tname]


def typeref_of_input_field(meta: TypeMeta.InputObjectMeta, fname: str) -> TypeRef.T:
  match meta:
    case TypeMeta.InputObjectMeta(input_fields=input_fields):
      arg = next(filter(lambda field: field.name == fname, input_fields))
      return arg.type_
    case _:
      raise TypeError(f"type_of_field: TypeMeta {meta.name} is not of type FieldMeta")
