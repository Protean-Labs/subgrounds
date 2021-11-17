from __future__ import annotations
from enum import Enum, auto
from dataclasses import Field, dataclass, field
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

import client
import query as q

def flatten(t):
  return [item for sublist in t for item in sublist]

# ================================================================
# Field arguments
# ================================================================
class Where:
  class Operator(Enum):
    EQ  = auto()
    NEQ = auto()
    LT  = auto()
    LTE = auto()
    GT  = auto()
    GTE = auto()

  @dataclass
  class Filter:
    name: str
    type_: Where.Operator
    value: Any

  def __init__(self, filters: List[Filter]) -> None:
    self.filters = filters

  def argument(self):
    def name_of_arg(filter):
      if filter.type_ == Where.Operator.EQ:
        return filter.name
      else:
        return f'{filter.name}_{filter.type_.name.lower()}'
    
    return q.Argument(
      "where", 
      q.Argument.InputValue.OBJECT, 
      {f'{name_of_arg(f)}': f.value for f in self.filters}
    )

def and_(*wheres):
  return Where(flatten([w.filters for w in wheres]))

class OrderDirection(Enum):
  ASC   = auto()
  DESC  = auto()

  def argument(self):
    if self.name == "ASC":
      value = "asc"
    else:
      value = "desc"

    return q.Argument(
      "orderDirection", 
      q.Argument.InputValue.ENUM,
      value
    )

class QueryField(q.Field):
  @staticmethod
  def mk_no_args(field: 'Field') -> QueryField:
    return QueryField(name=field.name)

  @staticmethod
  def mk_single_item(field: 'Field', id_: str) -> QueryField:
    return QueryField(
      name=field.name,
      arguments=[q.Argument("id", q.Argument.InputValue.STRING, id_)]
    )

  @staticmethod
  def mk_multiple_items(
    field: 'Field', 
    where: Optional[Where] = None, 
    order_by: Optional[Field] = None,
    order_direction: Optional[OrderDirection] = None,
    skip: Optional[int] = None,
    first: Optional[int] = None
  ) -> QueryField:
    
    args = []

    if where != None:
      args.append(where.argument())
    
    if order_by != None:
      args.append(q.Argument("orderBy", q.Argument.InputValue.ENUM, order_by.name))

    if order_direction != None:
      args.append(order_direction.argument())

    if skip != None:
      args.append(q.Argument("skip", q.Argument.InputValue.INT, skip))

    if first != None:
      args.append(q.Argument("first", q.Argument.InputValue.INT, first))

    return QueryField(
      name=field.name,
      arguments=args,
    )

  def select(self, *fields: List[QueryField]) -> QueryField:
    def handle_scalar_fields(field):
      if isinstance(field, QueryField):
        return field
      else:
        return QueryField.mk_no_args(field)

    self.selection = [handle_scalar_fields(f) for f in fields]
    return self


# ================================================================
# Schema definition and data structure
# ================================================================
class Schema:
  class TypeKind(Enum):
    NON_NULL  = auto()
    LIST      = auto()
    SCALAR    = auto()
    OBJECT    = auto()
    ENUM      = auto()

  @dataclass
  class Type:
    kind:   Schema.TypeKind
    name:   Union[str, None]
    inner:  Union[Schema.Type, None]

    def innermost(self):
      if self.inner != None:
        return self.inner.innermost()
      else:
        return self

    def is_list(self) -> bool:
      if self.kind == Schema.TypeKind.LIST:
        return True
      elif self.inner != None:
        return self.inner.is_list()
      else:
        return False

  class Field:
    name: str
    type_: Schema.Type

    def __init__(self, name, type_) -> None:
      self.name = name
      self.type_ = type_

    def __eq__(self, value: object) -> Where:
      return Where([Where.Filter(self.name, Where.Operator.EQ, value)])

    def __lt__(self, value: object) -> Where:
      return Where([Where.Filter(self.name, Where.Operator.LT, value)])

    def __gt__(self, value: object) -> Where:
      return Where([Where.Filter(self.name, Where.Operator.GT, value)])

    def __call__(self, **kwargs: dict[str, Any]) -> Any:
      if self.type_.is_list():
        return QueryField.mk_multiple_items(self, **kwargs)
      elif self.type_.innermost().kind == Schema.TypeKind.SCALAR:
        return QueryField.mk_no_args(self)
      else:
        return QueryField.mk_single_item(self, **kwargs)

    def select(self, *fields: List[Field]) -> QueryField:
      return QueryField.mk_no_args(self).select(*fields)


  class Object:
    def __init__(self, name_: str, fields: List[Schema.Field]) -> None:
      self.name_ = name_
      for field in fields:
        setattr(self, field.name, field)    

  class Enum:
    def __init__(self, name_: str, values: List[str]) -> None:
      self.name_ = name_
      for val in values:
        setattr(self, val, val)

  class Schema:
    def __init__(self, url, objects) -> None:
      self.url = url
      for obj in objects:
        setattr(self, obj.name_.capitalize(), obj)

    def query(self, field: List[Field]) -> q.Query:
      query = q.Query(name="Q", selection=[field])
      return query.query(self.url)

class UnknownTypeKind(Exception):
  pass

def parse_schema(url, json):
  def mk_type(json: dict) -> Schema.Type:
    if json["kind"] == "NON_NULL":
      return Schema.Type(
        kind=Schema.TypeKind.NON_NULL,
        name=None,
        inner=mk_type(json["ofType"])
      )
    elif json["kind"] == "LIST":
      return Schema.Type(
        kind=Schema.TypeKind.LIST,
        name=None,
        inner=mk_type(json["ofType"])
      )
    elif json["kind"] == "SCALAR":
      return Schema.Type(
        kind=Schema.TypeKind.SCALAR,
        name=json["name"],
        inner=None
      )
    elif json["kind"] == "OBJECT":
      return Schema.Type(
        kind=Schema.TypeKind.OBJECT,
        name=json["name"],
        inner=None
      )
    elif json["kind"] == "ENUM":
      return Schema.Type(
        kind=Schema.TypeKind.ENUM,
        name=json["name"],
        inner=None
      )
    else:
      raise Schema.UnknownTypeKind(json["kind"])
  
  def mk_field(json: dict) -> Schema.Field:
    return Schema.Field(
      name=json["name"],
      type_=mk_type(json["type"])
    )

  def mk_object(json: dict) -> Schema.Object:
    if json["kind"] == "OBJECT":
      return Schema.Object(
        name_=json["name"],
        fields=[mk_field(field) for field in json["fields"]]
      )
    elif json["kind"] == "ENUM":
      return Schema.Enum(
        name_=json["name"],
        values=[mk_field(value["name"]) for value in json["enumValues"]]
      )
    else:
      raise Exception("mk_object: Not implemented")

  types = json["__schema"]["types"]

  objects = []
  for typ in types:
    if typ["kind"] == "OBJECT":
      objects.append(mk_object(typ))

  return Schema.Schema(url, objects)

def get_schema(url):
  return parse_schema(url, client.introspection(url))