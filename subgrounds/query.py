from __future__ import annotations
from typing import List, Optional, Union, Any
from enum import Enum, auto
from dataclasses import dataclass
from abc import ABC, abstractmethod
import operator

from sgqlc.operation import Operation

from subgrounds.utils import flatten

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

  def __init__(self, filters: list[Filter]) -> None:
    self.filters = filters

  @staticmethod
  def and_(wheres):
    return Where(flatten([w.filters for w in wheres]))

  def argument(self):
    def name_of_arg(filter):
      if filter.type_ == Where.Operator.EQ:
        return filter.name
      else:
        return f'{filter.name}_{filter.type_.name.lower()}'
    
    return {name_of_arg(f): f.value for f in self.filters}

class Field(ABC):
  """Adds the selection represented by the current field to an 
  entity query.
  
  :param entity: The entity query to be extended
  :type entity: Entity
  """
  @abstractmethod
  def add_selection(self, entity: 'Entity') -> None:
    pass

  """Defines transformations to be run on the returned data of a query
  containing that field.

  :param data: The data to be processed
  :type data: list[dict]
  """
  @abstractmethod
  def process(self, data: dict) -> None:
    pass

  """Returns the name of the field as it appears in the data.

  :rtype: str
  :return: The name of the field in the data.
  """
  @abstractmethod
  def data_name(self) -> str:
    pass

class SyntheticField(Field):
  counter = 0

  def __init__(self, func: function, *args: List[Any]) -> None:
    self.func = func
    self.name = f"SyntheticField_{SyntheticField.counter}"
    SyntheticField.counter += 1 
    self.args = args

  def add_selection(self, entity):
    for arg in self.args:
      if isinstance(arg, Field):
        arg.add_selection(entity)

  def process(self, data):
    def value_of_arg(row, arg):
      if isinstance(arg, Field):
        return row[arg.data_name()]
      else:
        return arg

    for arg in self.args:
      if isinstance(arg, Field):
        arg.process(data)

    for row in data:
      args = [value_of_arg(row, arg) for arg in self.args]
      row[self.name] = self.func(*args)

  def data_name(self):
    return self.name

  def __add__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.add, self, other)

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.sub, self, other)

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.mul, self, other)

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.truediv, self, other)

  def __pow__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.pow, self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, self)

class ScalarField(Field):
  def __init__(self, prev, field) -> None:
    self.prev = prev
    self.field = field

  def name(self):
    return self.field.name

  def graphql_name(self):
    return self.field.graphql_name

  def path(self) -> str:
    path = []
    field = self
    while field is not None:
      path.append(field.field.name)
      field = field.prev

    path.reverse()
    return path

  def add_selection(self, entity):
    next = entity
    for fname in self.path():
      next = next.__getattr__(fname)
    next()

  def process(self, data):
    if 'BigInt' in str(self.field.type):
      for row in data:
        row[self.graphql_name()] = int(row[self.graphql_name()])
    elif 'BigDecimal' in str(self.field.type):
      for row in data:
        row[self.graphql_name()] = float(row[self.graphql_name()])

  def data_name(self) -> str:
    path = []
    field = self
    while field is not None:
      path.append(field.graphql_name())
      field = field.prev

    path.reverse()
    return '_'.join(path)

  def __getattribute__(self, __name: str) -> Any:
    try:
      return ScalarField(self, self.field.type.__getattr__(__name))
    except:
      return super().__getattribute__(__name)

  def __eq__(self, value: Any) -> Where:
    return Where([Where.Filter(self.graphql_name(), Where.Operator.EQ, value)])

  def __lt__(self, value: Any) -> Where:
    return Where([Where.Filter(self.graphql_name(), Where.Operator.LT, value)])

  def __lte__(self, value: Any) -> Where:
    return Where([Where.Filter(self.graphql_name(), Where.Operator.LTE, value)])

  def __gt__(self, value: Any) -> Where:
    return Where([Where.Filter(self.graphql_name(), Where.Operator.GT, value)])

  def __gte__(self, value: Any) -> Where:
    return Where([Where.Filter(self.graphql_name(), Where.Operator.GTE, value)])

  def __add__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.add, self, other)

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.sub, self, other)

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.mul, self, other)

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.truediv, self, other)

  def __pow__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.pow, self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, self)

def transform(func, *fields):
  return SyntheticField(func, *fields)

class Entity:
  def __init__(self, subgraph, type_) -> None:
    self.type_ = type_
    self.subgraph = subgraph
  
  def single_query(self):
    return str(self.type_)[0].lower() + str(self.type_)[1:]

  def multi_query(self):
    return str(self.type_)[0].lower() + str(self.type_)[1:] + "s"

  def __getattribute__(self, __name: str) -> Any:
    try:
      return ScalarField(None, self.type_.__getattr__(__name))
    except:
      return super().__getattribute__(__name)

  def __setattr__(self, __name: str, __value: Any) -> None:
    self.__dict__[__name] = __value
    if type(__value) == SyntheticField:
      __value.name = __name

  def __repr__(self) -> str:
    return self.type_.name

class OrderDirection(Enum):
  ASC   = auto()
  DESC  = auto()

class Query:
  def __init__(    
    self, 
    entity: Entity,
    selection: List[ScalarField] = [], 
    first: int = 10,
    order_by: Optional[ScalarField] = None, 
    order_direction: Optional[OrderDirection] = None,
    where: Optional[List[Where]] = None
  ) -> None:
    self.entity = entity
    self.selection = selection
    self.endpoint = entity.subgraph.endpoint
    self.name = entity.multi_query()

    self.query_args = {'first': first}
    if order_by is not None:
      self.query_args['order_by'] = order_by.graphql_name()

    if order_direction is not None:
      self.query_args['order_direction'] = order_direction.name.lower()

    if where is not None:
      self.query_args['where'] = Where.and_(where).argument()

  def build_query(self, op):
    entity = op.__getattr__(self.name)(**self.query_args)

    for field in self.selection:
      field.add_selection(entity)

    print(op)

  def execute(self):
    # Build query
    op = Operation(self.entity.subgraph.schema().Query)
    self.build_query(op)

    # Query data
    data = self.endpoint(op)

    for field in self.selection:
      field.process(data['data'][self.name])

    try:
      return data['data'][self.name]
    except:
      print(f"Error: {data['errors']}")
      return []