from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple
from functools import reduce
import os
import json
import operator

import subgrounds.client as client
import subgrounds.schema as schema
from subgrounds.query import Query, Selection, arguments_of_field_args, selection_of_path
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef, field_of_object, mk_schema, type_of_field, type_of_typeref
from subgrounds.transform import DEFAULT_TRANSFORMS, LocalSyntheticField, Transform, chain_transforms
from subgrounds.utils import identity


@dataclass
class Filter:
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


def typeref_of_binary_op(op: str, t1: TypeRef.T, t2: int | float | str | bool | FieldPath | SyntheticField):
  def f_typeref(t1, t2):
    match (op, TypeRef.root_type_name(t1), TypeRef.root_type_name(t2)):
      case ('add', 'String' | 'Bytes', 'String' | 'Bytes'):
        return TypeRef.Named('String')

      case ('add' | 'sub' | 'mul' | 'div', 'BigInt' | 'Int', 'BigInt' | 'Int'):
        return TypeRef.Named('Int')
      case ('add' | 'sub' | 'mul' | 'div', 'BigInt' | 'Int', 'BigDecimal' | 'Float'):
        return TypeRef.Named('Float')
      case ('add' | 'sub' | 'mul' | 'div', 'BigDecimal' | 'Float', 'BigInt' | 'Int' | 'BigDecimal' | 'Float'):
        return TypeRef.Named('Float')

      case _ as args:
        raise Exception(f'typeref_of_binary_op: f_typeref: unhandled arguments {args}')

  def f_const(t1, const):
    match (op, TypeRef.root_type_name(t1), const):
      case ('add', 'String' | 'Bytes', str()):
        return TypeRef.Named('String')

      case ('add' | 'sub' | 'mul' | 'div' | 'pow', 'BigInt' | 'Int', int()):
        return TypeRef.Named('Int')
      case ('add' | 'sub' | 'mul' | 'div' | 'pow', 'BigInt' | 'Int', float()):
        return TypeRef.Named('Float')
      case ('add' | 'sub' | 'mul' | 'div' | 'pow', 'BigDecimal' | 'Float', int() | float()):
        return TypeRef.Named('Float')

      case _ as args:
        raise Exception(f'typeref_of_binary_op: f_typeref: unhandled arguments {args}')

  match t2:
    case int() | float() | str() | bool() as constant:
      return f_const(t1, constant)
    case FieldPath() | SyntheticField() as field:
      return f_typeref(t1, field.type_)


def type_ref_of_unary_op(op: str, t: TypeRef.T):
  match (op, TypeRef.root_type_name(t)):
    case ('abs', 'BigInt' | 'Int'):
      return TypeRef.Named('Int')
    case ('abs', 'BigDecimal' | 'Float'):
      return TypeRef.Named('Float')

    case ('neg', 'BigInt' | 'Int'):
      return TypeRef.Named('Int')
    case ('neg', 'BigDecimal' | 'Float'):
      return TypeRef.Named('Float')

    case _ as args:
      raise Exception(f'typeref_of_binary_op: f_typeref: unhandled arguments {args}')


class FieldOperatorMixin:
  subgraph: Subgraph
  type_: TypeRef.T

  def __add__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.add, typeref_of_binary_op('add', self.type_, other), self, other)

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.sub, typeref_of_binary_op('sub', self.type_, other), self, other)

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.mul, typeref_of_binary_op('mul', self.type_, other), self, other)

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.truediv, typeref_of_binary_op('div', self.type_, other), self, other)

  def __pow__(self, other: Any) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.pow, typeref_of_binary_op('pow', self.type_, other), self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.neg, type_ref_of_unary_op('neg', self.type_), self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(self.subgraph, operator.abs, type_ref_of_unary_op('abs', self.type_), self)


@dataclass
class SyntheticField(FieldOperatorMixin):
  counter: ClassVar[int] = 0

  subgraph: Subgraph
  f: Callable
  type_: TypeRef.T
  deps: list[FieldPath | SyntheticField]

  def __init__(self, subgraph: Subgraph, f: Callable, type_: TypeRef.T, *deps: list[FieldPath | SyntheticField]) -> None:
    self.subgraph = subgraph

    def mk_deps(
      deps: list(FieldPath | SyntheticField),
      f: Callable,
      acc: list[Tuple[Optional[Callable], int]] = []
    ) -> Tuple[Callable, list[FieldPath]]:
      """If all dependencies are field paths, then this function does nothing. If the dependencies contain
      one or more other synthetic fields, as is the case when chaining binary operators, then the synthetic
      field tree is flattened to a single synthetic field containing all leaf dependencies.

      Args:
          deps (list): Initial dependencies for synthetic field
          f (Callable): Function to apply to the values of those dependencies
          acc (list[Tuple[Optional[Callable], list[FieldPath]]], optional): Accumulator. Defaults to [].

      Returns:
          Tuple[Callable, list[FieldPath]]: A tuple containing the potentially modified
          function and dependency list.
      """
      match deps:
        case []:
          def new_f(*args):
            new_args = []
            counter = 0
            for (f_, deps) in acc:
              match (f_, deps):
                case (None, FieldPath()):
                  new_args.append(args[counter])
                  counter += 1
                case (None, int() | float() | str() | bool() as constant):
                  new_args.append(constant)
                case (f_, list() as deps):
                  new_args.append(f_(*args[counter:counter + len(deps)]))
                  counter += len(deps)

            return f(*new_args)

          new_deps = []
          for (_, deps) in acc:
            match deps:
              case FieldPath() as dep:
                new_deps.append(dep)
              case int() | float() | str() | bool():
                pass
              case list() as deps:
                new_deps = new_deps + deps

          return (new_f, new_deps)

        case [SyntheticField(_, f=inner_f, deps=inner_deps), *rest]:
          acc.append((inner_f, inner_deps))
          return mk_deps(rest, f, acc)

        case [FieldPath() as dep, *rest]:
          acc.append((None, dep))
          return mk_deps(rest, f, acc)

        case [int() | float() | str() | bool() as constant, *rest]:
          acc.append((None, constant))
          return mk_deps(rest, f, acc)

        case _ as deps:
          raise TypeError(f'mk_deps: unexpected argument {deps}')

    (f, deps) = mk_deps(deps, f)
    self.f = f
    self.type_ = type_
    self.deps = deps

    SyntheticField.counter += 1

  @property
  def schema(self):
    return self.subgraph.schema


@dataclass
class FieldPath(FieldOperatorMixin):
  subgraph: Subgraph
  root_type: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
  type_: TypeRef.T
  path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]

  # Purely for testing
  test_mode: ClassVar[bool] = False

  @property
  def schema(self):
    return self.subgraph.schema

  @property
  def longname(self) -> str:
    return '_'.join(map(lambda ele: ele[1].name, self.path))

  @property
  def root(self) -> TypeMeta.FieldMeta:
    return self.path[0][1]

  @property
  def leaf(self) -> TypeMeta.FieldMeta:
    return self.path[-1][1]

  def __str__(self) -> str:
    return '.'.join(map(lambda ele: ele[1].name, self.path))

  def split_args(self, kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    query_args = {}
    other_args = {}
    for key, item in kwargs.items():
      try:
        next(filter(lambda arg: arg.name == key, self.leaf.arguments))
        query_args[key] = item
      except StopIteration:
        other_args[key] = item

    return query_args, other_args

  @staticmethod
  def selection(fpath: FieldPath) -> Selection:
    def f(path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]) -> list[Selection]:
      match path:
        case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
          return [Selection(
            fmeta,
            arguments=arguments_of_field_args(fpath.subgraph.schema, fmeta, args),
            selection=selection_of_path(fpath.subgraph.schema, rest)
          )]
        case []:
          return []

    return f(fpath.path)[0]

  # When setting arguments
  def __call__(self, **kwargs: Any) -> Any:
    def fmt_arg(name, raw_arg):
      match (name, raw_arg):
        case ('where', [Filter(), *_] as filters):
          return Filter.to_dict(filters)
        case ('orderBy', FieldPath() as fpath):
          match fpath.leaf:
            case TypeMeta.FieldMeta() as fmeta:
              return fmeta.name
            case _:
              raise Exception(f"Cannot use non field {fpath} as orderBy argument")
        case _:
          return raw_arg

    match self.leaf:
      case TypeMeta.FieldMeta():
        # args = arguments_of_field_args(self.schema, field, {key: fmt_arg(key, val) for key, val in kwargs.items()})
        args = {key: fmt_arg(key, val) for key, val in kwargs.items()}
        self.path[-1] = (args, self.path[-1][1])
        return self
      case _:
        raise TypeError(f"Unexpected type for FieldPath {self}")

  # When selecting a nested field
  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      match type_of_typeref(self.schema, self.type_):
        case TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
          raise TypeError(f"FieldPath: field {__name} of path {self} is terminal! cannot select field {__name}")

        case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as obj:
          field = field_of_object(obj, __name)
          match type_of_field(self.schema, field):
            case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() | TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
              path = self.path.copy()
              path.append((None, field))
              return FieldPath(self.subgraph, self.root_type, field.type_, path)
            case _:
              raise TypeError(f"FieldPath: field {__name} is not a valid field for object {TypeRef.root_type_name(self.type_)} at path {self}")

        case _:
          raise TypeError(f"FieldPath: Unexpected type {TypeRef.root_type_name(self.type_)} when selection {__name} on {self}")

  @staticmethod
  def extend(fpath: FieldPath, ext: FieldPath) -> FieldPath:
    match fpath.leaf:
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

  def __eq__(self, value: FieldPath | Any) -> Filter:
    if FieldPath.test_mode:
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
          return FieldPath(self.subgraph, self.object_, field.type_, [(None, field)])

        case TypeMeta.T as type_:
          raise TypeError(f"Object: Unexpected type {type_.name} when selection {__name} on {self}")

  def __setattr__(self, __name: str, __value: SyntheticField | FieldPath | Any) -> None:
    match __value:
      case SyntheticField() as sfield:
        self.subgraph.add_synthetic_field(self.object_, __name, sfield)
      case FieldPath() as fpath:
        sfield = SyntheticField(self.schema, identity, fpath.type_, fpath)
        self.subgraph.add_synthetic_field(self.object_, __name, sfield)
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

    return Subgraph(url, mk_schema(schema), DEFAULT_TRANSFORMS)

  @staticmethod
  def mk_query(fpaths: List[FieldPath]) -> Query:
    return reduce(Query.add_selection, map(FieldPath.selection, fpaths), Query())

  def add_synthetic_field(
    self,
    object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta,
    name: str,
    sfield: SyntheticField
  ) -> None:
    fmeta = TypeMeta.FieldMeta(name, '', [], sfield.type_)
    object_.fields.append(fmeta)

    transform = LocalSyntheticField(
      self,
      fmeta,
      sfield.f,
      [dep.selection for dep in sfield.deps]
    )

    self.transforms = [transform, *self.transforms]

  def query(self, query: Query) -> dict:
    return chain_transforms(self.transforms, query, self.url)

  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      return Object(self, self.schema.type_map[__name])
