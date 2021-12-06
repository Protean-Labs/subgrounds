from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple
import os
import json
import operator

import subgrounds.client as client
import subgrounds.schema as schema
from subgrounds.query import Query
from subgrounds.schema import SchemaMeta, TypeMeta, mk_schema
from subgrounds.transform import Transform
from subgrounds.utils import flatten, identity


@dataclass
class Filter:
  test_mode: ClassVar[bool] = False

  field: TypeMeta.FieldMeta
  op: Filter.Operator
  value: Any

  class Operator(Enum):
    EQ  = auto()
    NEQ = auto()
    LT  = auto()
    LTE = auto()
    GT  = auto()
    GTE = auto()

  @property
  def name(self):
    match self.op:
      case Filter.Operator.EQ:
        return self.field.name
      case Filter.Operator.NEQ:
        return f"{self.field.name}_not"
      case Filter.Operator.LT:
        return f"{self.field.name}_lt"
      case Filter.Operator.GT:
        return f"{self.field.name}_gt"
      case Filter.Operator.LTE:
        return f"{self.field.name}_lte"
      case Filter.Operator.GTE:
        return f"{self.field.name}_gte"

  @staticmethod
  def to_dict(filters: list[Filter]) -> dict[str, Any]:
    return {f.name: f.value for f in filters}

@dataclass
class SyntheticField:
  counter: ClassVar[int] = 0

  subgraph: Subgraph
  f: Callable
  deps: list[FieldPath | SyntheticField]

  def __init__(self, subgraph: Subgraph, f: Callable, *deps: list[FieldPath | SyntheticField]) -> None:
    self.subgraph = subgraph

    def mk_deps(
      deps: list(FieldPath | SyntheticField),
      f: Callable,
      acc: list[Tuple[Optional[Callable], list[FieldPath]]] = []
    ) -> Tuple[Callable, list[FieldPath]]:
      match deps:
        case []:
          def new_f(*args):
            new_args = []
            counter = 0
            for (f_, deps) in acc:
              if f_ is None:
                new_args.append(args[counter])
                counter += 1
              else:
                new_args.append(f_(*args[counter:counter + len(deps)]))
                counter += len(deps)

            return f(*new_args)

          new_deps = flatten([args_ for (_, args_) in acc])
          return (new_f, new_deps)

        case [SyntheticField(_, f=inner_f, deps=inner_deps), *rest]:
          acc.append((inner_f, inner_deps))
          return mk_deps(rest, f, acc)

        case [FieldPath() as dep, *rest]:
          acc.append((None, [dep]))
          return mk_deps(rest, f, acc)

    (f, deps) = mk_deps(deps, f)
    self.f = f
    self.deps = deps

    # def f(dep):
    #   match dep:

    #     case SyntheticField() as sfield:
    #       return sfield.meta

    #     case int() | float() | str() as value:
    #       return value

    # self.meta = TypeMeta.SyntheticFieldMeta(
    #   name=f'SyntheticField_{SyntheticField.counter}',
    #   description='', 
    #   func=self.func,
    #   dependencies=list(map(f, self.deps))
    # )

    SyntheticField.counter += 1

  @property
  def schema(self):
    return self.subgraph.schema

  def __add__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.add, self, other)

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.sub, self, other)

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.mul, self, other)

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.truediv, self, other)

  def __pow__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.pow, self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.neg, self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.abs, self)


@dataclass
class FieldPath:
  subgraph: Subgraph
  root_type: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
  type_: TypeMeta
  path: List[Tuple[Optional[Dict[str, Any]], TypeMeta.FieldMeta]]

  # @dataclass 
  # class PathElement:
  #   type_: TypeMeta.FieldMeta | TypeMeta.SyntheticFieldMeta
  #   args: Optional[List[Argument]] = None

  @property
  def schema(self):
    return self.subgraph.schema

  # @property
  # def fieldmeta_path(self) -> Path:
  #   def f(path_ele):
  #     match path_ele:
  #       case FieldPath.PathElement(type_=TypeMeta.FieldMeta() as fmeta, args=args):
  #         return (args, fmeta)
  #       case FieldPath.PathElement(type_=TypeMeta.SyntheticFieldMeta() as sfmeta):
  #         return sfmeta
  #       case _:
  #         raise TypeError(f"fieldmeta_path: Unexptected type in FieldPath {self}: {path_ele}")

  #   return list(map(f, self.path))

  @property
  def longname(self):
    return '_'.join(map(lambda ele: ele.type_.name, self.path))

  @property
  def root(self):
    return self.path[0]

  @property
  def leaf(self):
    return self.path[-1]

  def __str__(self):
    return '.'.join(map(lambda ele: ele.type_.name, self.path))

  def split_args(self, kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    query_args = {}
    other_args = {}
    for key, item in kwargs.items():
      try:
        next(filter(lambda arg: arg.name == key, self.leaf.type_.arguments))
        query_args[key] = item
      except StopIteration:
        other_args[key] = item

    return query_args, other_args

  # When setting arguments
  def __call__(self, **kwargs: Any) -> Any:
    def fmt_arg(name, raw_arg):
      match (name, raw_arg):
        case ('where', [Filter(), *_] as filters):
          return Filter.to_dict(filters)
        case ('orderBy', FieldPath() as fpath):
          match fpath.leaf.type_:
            case TypeMeta.FieldMeta() as fmeta:
              return fmeta.name
            case TypeMeta.SyntheticFieldMeta() as sfmeta:
              raise Exception(f"Cannot use synthetic field {fpath} as orderBy argument")
            case _:
              raise Exception(f"Cannot use non field {fpath} as orderBy argument")
        case _:
          return raw_arg

    match self.leaf.type_:
      case TypeMeta.FieldMeta() as field:
        self.leaf.args = schema.arguments_of_field_args(self.schema, field, {key: fmt_arg(key, val) for key, val in kwargs.items()})
        return self
      case TypeMeta.SyntheticFieldMeta():
        raise TypeError(f"FieldPath {self} is a SyntheticField; no arguments allowed!")
      case _:
        raise TypeError(f"Unexpected type for FieldPath {self}")

  # When selecting a nested field
  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      match self.type_:
        case TypeMeta.EnumMeta() | TypeMeta.ScalarMeta() | TypeMeta.SyntheticFieldMeta():
          raise TypeError(f"FieldPath: field {__name} of path {self} is terminal! cannot select field {__name}")

        case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta():
          field = schema.field_of_object(self.type_, __name)
          match schema.type_of_field(self.schema, field):
            case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() | TypeMeta.EnumMeta() | TypeMeta.ScalarMeta() | TypeMeta.SyntheticFieldMeta() as type_:
              path = self.path.copy()
              path.append(FieldPath.PathElement(field, None))
              return FieldPath(self.subgraph, self.root_type, type_, path)
            case _:
              raise TypeError(f"FieldPath: field {__name} is not a valid field for object {self.type_.name} at path {self}")

        case _ as type_:
          raise TypeError(f"FieldPath: Unexpected type {type_.name} when selection {__name} on {self}")

  @staticmethod
  def extend(fpath: FieldPath, ext: FieldPath) -> FieldPath:
    match fpath.leaf.type_:
      case TypeMeta.FieldMeta() as fmeta:
        match schema.type_of_field(fpath.schema, fmeta):
          case TypeMeta.ObjectMeta(name=name) | TypeMeta.InterfaceMeta(name=name):
            if name == ext.root_type.name:
              return FieldPath(fpath.subgraph, fpath.root_type, ext.type_, fpath.path + ext.path)
            else:
              raise TypeError(f"extend: FieldPath {ext} does not start at the same type from where FieldPath {fpath} ends")
          case _:
            raise TypeError(f"extend: FieldPath {fpath} is not object field")
      case _:
        raise TypeError(f"extend: FieldPath {fpath} is not an object field")

  # Filter construction
  @staticmethod
  def mk_filter(fpath: FieldPath, op: Filter.Operator, value: Any) -> Filter:
    match fpath.leaf:
      case TypeMeta.FieldMeta() as fmeta:
        return Filter(fmeta, op, value)
      case _:
        raise TypeError(f"Cannot create filter on FieldPath {fpath}: not a native field!")

  def __eq__(self, value: Any) -> Filter:
    if Filter.test_mode:
      # Purely used for testing so that assertEqual works
      return self.subgraph == value.subgraph and self.type_ == value.type_ and self.path == value.path
    else:
      return FieldPath.mk_filter(self, Filter.Operator.EQ, value)

  def __lt__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.LT, value)

  def __gt__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.GT, value)

  def __lte__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.LTE, value)

  def __gte__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.GTE, value)

  # SyntheticField operations
  def __add__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.add, self, other)

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.sub, self, other)

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.mul, self, other)

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.truediv, self, other)

  def __pow__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.pow, self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.neg, self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.abs, self)

@dataclass
class Object:
  subgraph: Subgraph
  object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta

  @property
  def schema(self):
    return self.subgraph.schema

  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      field = schema.field_of_object(self.object_, __name)
      match schema.type_of_field(self.schema, field):
        case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() | TypeMeta.EnumMeta() | TypeMeta.ScalarMeta() as type_:
          return FieldPath(self.subgraph, self.object_, type_, [field])
        case TypeMeta.T as type_:
          raise TypeError(f"Object: Unexpected type {type_.name} when selection {__name} on {self}")

  def __setattr__(self, __name: str, __value: Any) -> None:
    match __value:
      case SyntheticField() as sfield:
        sfield.meta.name = __name
        schema.add_object_field(self.object_, sfield.meta)
      case FieldPath() as fpath:
        sfield = SyntheticField(self.schema, identity, fpath)
        sfield.meta.name = __name
        schema.add_object_field(self.object_, sfield.meta)
      case _:
        super().__setattr__(__name, __value)

@dataclass
class Subgraph:
  url: str
  schema: SchemaMeta
  transforms: List[Transform] = field(default_factory=list)

  @staticmethod
  def of_url(url: str) -> None:
    filename = url.split("/")[-1] + ".json"
    if os.path.isfile(filename):
      with open(filename) as f:
        schema = json.load(f)
    else:
      schema = client.get_schema(url)
      with open(filename, mode="w") as f:
        json.dump(schema, f)

    return Subgraph(url, mk_schema(schema))

  @staticmethod
  def mk_query(fpaths: List[FieldPath]) -> Query:
    selections = flatten(map(lambda fpath: selections_of_path(fpath.fieldmeta_path), fpaths))
    query = Query()
    query.add_selections(selections)
    return query

  def process_data(self, fpaths: List[FieldPath], data: dict) -> None:
    for fpath in fpaths:
      apply_field_path(self.schema, fpath.fieldmeta_path, data)

  def query(self, query: Query) -> dict:
    return client.query(self.url, query.graphql_string())

  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except:
      return Object(self, self.schema.type_map[__name])

# @dataclass
# class Subgraph2:
#   schema: SchemaMeta
#   transforms: list[Transform]

#   def add_field()