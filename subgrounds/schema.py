""" Schema data structure module

This module contains various data structures in the form of dataclasses that
are used to represent GraphQL schemas in Subgrounds using an AST-like approach.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional
import warnings

from pipe import where, map

warnings.simplefilter('default')


# ================================================================
# Schema definitions, data structures and types
# ================================================================
class TypeRef:
  """ Class used as namespace for all types and functions related to
  GraphQL schema type references.
  """
  class T(ABC):
    """ Base class of all types of type references.
    """
    pass

    @property
    @abstractmethod
    def name(self) -> str:
      raise NotImplementedError

    @property
    @abstractmethod
    def is_list(self) -> bool:
      raise NotImplementedError

    @property
    @abstractmethod
    def is_non_null(self) -> bool:
      raise NotImplementedError

  @dataclass
  class Named(T):
    """ Class used to represent a simple type reference.

    Attributes:
      name_ (str): Name of the type being referenced.
    """
    name_: str

    @property
    def name(self) -> str:
      return self.name_

    @property
    def is_list(self) -> bool:
      return False

    @property
    def is_non_null(self) -> bool:
      return False

  @dataclass
  class NonNull(T):
    """ Class used to represent a non-nullable type reference.

    Attributes:
      inner (TypeRef.T): Non-nullable type being referenced.
    """
    inner: TypeRef.T

    @property
    def name(self) -> str:
      return self.inner.name

    @property
    def is_list(self) -> bool:
      return self.inner.is_list

    @property
    def is_non_null(self) -> bool:
      return True

  @dataclass
  class List(T):
    """ Class used to represent a list type reference.

    Attributes:
      inner (TypeRef.T): List type being referenced.
    """
    inner: TypeRef.T

    @property
    def name(self) -> str:
      return self.inner.name

    @property
    def is_list(self) -> bool:
      return True

    @property
    def is_non_null(self) -> bool:
      return self.inner.is_non_null

  @staticmethod
  def non_null(name: str) -> TypeRef.T:
    return TypeRef.NonNull(TypeRef.Named(name))

  @staticmethod
  def non_null_list(name: str) -> TypeRef.T:
    return TypeRef.NonNull(TypeRef.List(TypeRef.NonNull(TypeRef.Named(name))))

  @staticmethod
  def root_type_name(type_: TypeRef.T) -> str:
    # warnings.warn("`TypeRef.root_type_name` will be deprecated! Use `TypeRef.T.name` instead", DeprecationWarning)
    return type_.name

  @staticmethod
  def is_non_null(type_: TypeRef.T) -> bool:
    # warnings.warn("`TypeRef.is_non_null` will be deprecated! Use `TypeRef.T.is_non_null` instead", DeprecationWarning)
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

    assert False


class TypeMeta:
  """ Class used as namespace for all types and functions related to
  GraphQL schema types.
  """
  @dataclass
  class T(ABC):
    """ Base class of all GraphQL schema types."""
    name: str
    description: str

    @property
    def is_object(self) -> bool:
      return False

  @dataclass
  class ArgumentMeta(T):
    """ Class representing a field argument definition."""
    type_: TypeRef.T
    default_value: Optional[str]

  @dataclass
  class FieldMeta(T):
    """ Class representing an object field definition."""
    arguments: list[TypeMeta.ArgumentMeta]
    type_: TypeRef.T

    def has_arg(self, argname: str) -> bool:
      try:
        next(self.arguments | where(lambda arg: arg.name == argname))
        return True
      except StopIteration:
        return False

    def type_of_arg(self: TypeMeta.FieldMeta, argname: str) -> TypeRef.T:
      try:
        return next(
          self.arguments
          | where(lambda argmeta: argmeta.name == argname)
          | map(lambda arg: arg.type_)
        )
      except StopIteration:
        raise Exception(f'TypeMeta.FieldMeta.type_of_arg: no argument named {argname} for field {self.name}')

  @dataclass
  class ScalarMeta(T):
    """ Class representing an scalar definition."""

  @dataclass
  class ObjectMeta(T):
    """ Class representing an object definition."""
    fields: list[TypeMeta.FieldMeta]
    interfaces: list[str] = field(default_factory=list)

    @property
    def is_object(self) -> bool:
      return True

    def field(self: TypeMeta.ObjectMeta, fname: str) -> TypeMeta.FieldMeta:
      """ Returns the field definition of object :attr:`self` with name :attr:`fname`, if any.

      Args:
        self (TypeMeta.ObjectMeta): The object type
        fname (str): The name of the desired field definition

      Raises:
        KeyError: If no field named :attr:`fname` is defined for object :attr:`self`.

      Returns:
        TypeMeta.FieldMeta: The field definition
      """
      try:
        return next(
          self.fields
          | where(lambda fmeta: fmeta.name == fname)
        )
      except StopIteration:
        raise KeyError(f'TypeMeta.ObjectMeta.field: no field named {fname} for interface {self.name}')

    def type_of_field(self: TypeMeta.ObjectMeta, fname: str) -> TypeRef.T:
      """ Returns the type reference of the field of object :attr:`self` with name :attr:`fname`, if any.

      Args:
        self (TypeMeta.ObjectMeta): The object type
        fname (str): The name of the desired field type

      Raises:
        KeyError: If no field named :attr:`fname` is defined for object :attr:`self`.

      Returns:
        TypeRef.T: The field type reference
      """
      try:
        return next(
          self.fields
          | where(lambda fmeta: fmeta.name == fname)
          | map(lambda fmeta: fmeta.type_)
        )
      except StopIteration:
        raise KeyError(f'TypeMeta.ObjectMeta.type_of_field: no field named {fname} for object {self.name}')

  @dataclass
  class EnumValueMeta(T):
    """ Class representing an enum value definition."""
    pass

  @dataclass
  class EnumMeta(T):
    """ Class representing an enum definition."""
    values: list[TypeMeta.EnumValueMeta]

  @dataclass
  class InterfaceMeta(T):
    """ Class representing an interface definition."""
    fields: list[TypeMeta.FieldMeta]

    @property
    def is_object(self) -> bool:
      return True

    def field(self: TypeMeta.InterfaceMeta, fname: str) -> TypeMeta.FieldMeta:
      """ Returns the field definition of interface `self` with name `fname`, if any.

      Args:
        self (TypeMeta.InterfaceMeta): The interface type
        fname (str): The name of the desired field definition

      Raises:
        KeyError: If no field named :attr:`fname` is defined for interface :attr:`self`.

      Returns:
        TypeMeta.FieldMeta: The field definition
      """
      try:
        return next(
          self.fields
          | where(lambda fmeta: fmeta.name == fname)
        )
      except StopIteration:
        raise KeyError(f'TypeMeta.InterfaceMeta.field: no field named {fname} for interface {self.name}')

    def type_of_field(self: TypeMeta.InterfaceMeta, fname: str) -> TypeRef.T:
      """ Returns the type reference of the field of interface `self` with name `fname`, if any.

      Args:
        self (TypeMeta.InterfaceMeta): The interface type
        fname (str): The name of the desired field type

      Raises:
        KeyError: If no field named `fname` is defined for interface `self`.

      Returns:
        TypeRef.T: The field type reference
      """
      try:
        return next(
          self.fields
          | where(lambda fmeta: fmeta.name == fname)
          | map(lambda fmeta: fmeta.type_)
        )
      except StopIteration:
        raise KeyError(f'TypeMeta.InterfaceMeta.type_of_field: no field named {fname} for interface {self.name}')

  @dataclass
  class UnionMeta(T):
    """ Class representing an union definition."""
    types: list[str]

  @dataclass
  class InputObjectMeta(T):
    """ Class representing an input object definition."""
    input_fields: list[TypeMeta.ArgumentMeta]

    def type_of_input_field(self: TypeMeta.InputObjectMeta, fname: str) -> TypeRef.T:
      """ Returns the type reference of the input field named `fname` in the
      input object `self`, if any.

      Args:
        self (TypeMeta.InputObjectMeta): The input object
        fname (str): The name of the input field

      Raises:
        KeyError: If `fname` is not an input field of input object `self`

      Returns:
        TypeRef.T: The type reference for input field `fname`
      """
      try:
        return next(
          self.input_fields
          | where(lambda infield: infield.name == fname)
          | map(lambda infield: infield.type_)
        )
      except StopIteration:
        raise KeyError(f'TypeMeta.InputObjectMeta.type_of_input_field: no input field named {fname} for input object {self.name}')


@dataclass
class SchemaMeta:
  """ Class representing a GrpahQL schema.

  Contains all type definitions."""
  query_type: str
  type_map: dict[str, TypeMeta.T]
  mutation_type: Optional[str] = None
  subscription_type: Optional[str] = None

  def type_of_typeref(self: SchemaMeta, typeref: TypeRef.T) -> TypeMeta.T:
    """ Returns the type information of the type reference `typeref`

    Args:
      self (SchemaMeta): The schema.
      typeref (TypeRef.T): The type reference pointing to the type of interest.

    Raises:
      KeyError: If the type reference refers to a non-existant type

    Returns:
      TypeMeta.T: _description_
    """
    tname = TypeRef.root_type_name(typeref)
    try:
      return self.type_map[tname]
    except KeyError:
      raise KeyError(f'SchemaMeta.type_of_typeref: No type named {typeref.name} in schema!')

  def type_of(self: SchemaMeta, tmeta: TypeMeta.ArgumentMeta | TypeMeta.FieldMeta) -> TypeMeta.T:
    """ Returns the argument or field definition's underlying type.
    """
    return self.type_of_typeref(tmeta.type_)


# ================================================================
# Schema parsing
# ================================================================
class ParsingError(Exception):
  pass


def mk_schema(json: dict[str, Any]) -> SchemaMeta:
  """ Builds the schema data structure from a dictionary containing a
  JSON GraphQL schema definition (the result of the introspection query
  on a GraphQL API endpoint).

  Args:
    json (dict[str, Any]): JSON GraphQL schema definition

  Returns:
    SchemaMeta: A schema data structure
  """
  def mk_type_ref(json: dict[str, Any]) -> TypeRef.T:
    match json:
      case {'kind': 'NON_NULL', 'ofType': inner}:
        return TypeRef.NonNull(mk_type_ref(inner))

      case {'kind': 'LIST', 'ofType': inner}:
        return TypeRef.List(mk_type_ref(inner))

      case {'kind': 'SCALAR' | 'OBJECT' | 'INTERFACE' | 'ENUM' | 'INPUT_OBJECT', 'name': name}:
        return TypeRef.Named(name)

      case _ as json:
        raise ParsingError(f"mk_type_ref: {json}")

  def mk_argument_meta(json: dict[str, Any]) -> TypeMeta.ArgumentMeta:
    match json:
      case {'name': name, 'description': desc, 'type': type_, 'defaultValue': default_value}:
        return TypeMeta.ArgumentMeta(name=name, type_=mk_type_ref(type_), description=desc, default_value=default_value)
      case _ as json:
        raise ParsingError(f"mk_argument_meta: {json}")

  def mk_field_meta(json: dict[str, Any]) -> TypeMeta.FieldMeta:
    match json:
      case {'name': name, 'description': desc, 'args': args, 'type': type_}:
        return TypeMeta.FieldMeta(name, arguments=[mk_argument_meta(arg) for arg in args], type_=mk_type_ref(type_), description=desc)
      case _ as json:
        raise ParsingError(f"mk_field_meta: {json}")

  def mk_enum_value(json: dict[str, Any]) -> TypeMeta.EnumValueMeta:
    match json:
      case {'name': name, 'description': desc}:
        return TypeMeta.EnumValueMeta(name, description=desc)
      case _ as json:
        raise ParsingError(f"mk_enum_value: {json}")

  def mk_type_meta(json: dict[str, Any]) -> TypeMeta.T:
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
