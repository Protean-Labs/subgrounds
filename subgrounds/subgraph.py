from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple
from functools import partial, reduce
import os
import json
import operator
from hashlib import blake2b
from pipe import map, where

import warnings
warnings.simplefilter('default')

import logging
logger = logging.getLogger('subgrounds')

import subgrounds.client as client
import subgrounds.schema as schema
from subgrounds.query import Selection, arguments_of_field_args, selection_of_path
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef, field_of_object, mk_schema, type_of_field, type_of_typeref
from subgrounds.transform import DEFAULT_GLOBAL_TRANSFORMS, LocalSyntheticField, DocumentTransform
from subgrounds.utils import extract_data, identity


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

      case ('add' | 'sub' | 'mul' | 'div' | 'pow' | 'mod', 'BigInt' | 'Int', 'BigInt' | 'Int'):
        return TypeRef.Named('Int')
      case ('add' | 'sub' | 'mul' | 'div' | 'pow', 'BigInt' | 'Int', 'BigDecimal' | 'Float'):
        return TypeRef.Named('Float')
      case ('add' | 'sub' | 'mul' | 'div' | 'pow', 'BigDecimal' | 'Float', 'BigInt' | 'Int' | 'BigDecimal' | 'Float'):
        return TypeRef.Named('Float')

      case _ as args:
        raise Exception(f'typeref_of_binary_op: f_typeref: unhandled arguments {args}')

  def f_const(t1, const):
    match (op, TypeRef.root_type_name(t1), const):
      case ('add', 'String' | 'Bytes', str()):
        return TypeRef.Named('String')

      case ('add' | 'sub' | 'mul' | 'div' | 'pow' | 'mod', 'BigInt' | 'Int', int()):
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
    return SyntheticField(operator.add, typeref_of_binary_op('add', self.type_, other), [self, other])

  def __radd__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.add(y, x), typeref_of_binary_op('add', self.type_, other), [self, other])

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.sub, typeref_of_binary_op('sub', self.type_, other), [self, other])

  def __rsub__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.sub(y, x), typeref_of_binary_op('sub', self.type_, other), [self, other])

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.mul, typeref_of_binary_op('mul', self.type_, other), [self, other])

  def __rmul__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.mul(y, x), typeref_of_binary_op('mul', self.type_, other), [self, other])

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.truediv, typeref_of_binary_op('div', self.type_, other), [self, other])

  def __rtruediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.truediv(y, x), typeref_of_binary_op('div', self.type_, other), [self, other])

  def __floordiv__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.floordiv, typeref_of_binary_op('div', self.type_, other), [self, other])

  def __rfloordiv__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.floordiv(y, x), typeref_of_binary_op('div', self.type_, other), [self, other])

  def __pow__(self, rhs: Any) -> SyntheticField:
    return SyntheticField(operator.pow, typeref_of_binary_op('pow', self.type_, rhs), [self, rhs])

  def __rpow__(self, lhs: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.pow(y, x), typeref_of_binary_op('pow', self.type_, lhs), [self, lhs])

  def __mod__(self, rhs: Any) -> SyntheticField:
    return SyntheticField(operator.mod, typeref_of_binary_op('mod', self.type_, rhs), [self, rhs])

  def __rmod__(self, lhs: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.mod(y, x), typeref_of_binary_op('mod', self.type_, lhs), [self, lhs])

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, type_ref_of_unary_op('neg', self.type_), self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, type_ref_of_unary_op('abs', self.type_), self)


@dataclass
class SyntheticField(FieldOperatorMixin):
  STRING: ClassVar[TypeRef.Named] = TypeRef.Named('String')
  INT:    ClassVar[TypeRef.Named] = TypeRef.Named('Int')
  FLOAT:  ClassVar[TypeRef.Named] = TypeRef.Named('Float')
  BOOL:   ClassVar[TypeRef.Named] = TypeRef.Named('Boolean')

  counter: ClassVar[int] = 0


  # subgraph: Subgraph
  f: Callable
  type_: TypeRef.T
  default: Any
  deps: list[FieldPath]

  # def __init__(self, subgraph: Subgraph, f: Callable, type_: TypeRef.T, *deps: list[FieldPath | SyntheticField]) -> None:
  def __init__(
    self,
    f: Callable,
    type_: TypeRef.T,
    deps: list[FieldPath | SyntheticField] | FieldPath | SyntheticField,
    default: Any = None
  ) -> None:
    deps = deps if type(deps) == list else [deps]

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

        case [SyntheticField(f=inner_f, deps=inner_deps), *rest]:
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
    self.default = default if default is not None else SyntheticField.default_of_type(type_)
    self.deps = deps

    SyntheticField.counter += 1

  @staticmethod
  def default_of_type(type_: TypeRef.T):
    match type_.name:
      case 'String':
        return ''
      case 'Int':
        return 0
      case 'Float':
        return 0.0
      case 'Boolean':
        return False
      case _:
        return 0

  @staticmethod
  def constant(value: str | int | float | bool) -> SyntheticField:
    match value:
      case str():
        return SyntheticField(lambda: value, SyntheticField.STRING, [])
      case int():
        return SyntheticField(lambda: value, SyntheticField.INT, [])
      case float():
        return SyntheticField(lambda: value, SyntheticField.FLOAT, [])
      case bool():
        return SyntheticField(lambda: value, SyntheticField.BOOL, [])


@dataclass
class FieldPath(FieldOperatorMixin):
  subgraph: Subgraph
  root_type: TypeRef.T
  type_: TypeRef.T
  path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]

  # Purely for testing
  test_mode: ClassVar[bool] = False

  @property
  def schema(self):
    return self.subgraph.schema

  @property
  def root(self) -> TypeMeta.FieldMeta:
    return self.path[0][1]

  @property
  def leaf(self) -> TypeMeta.FieldMeta:
    return self.path[-1][1]

  @property
  def data_path(self) -> list[str]:
    return list(self.path | map(lambda ele: FieldPath.hash(ele[1].name + str(ele[0])) if ele[0] != {} and ele[0] is not None else ele[1].name))

  @property
  def dataname(self) -> str:
    return '_'.join(self.data_path)

  @property
  def name_path(self) -> list[str]:
    return list(self.path | map(lambda ele: ele[1].name))

  @property
  def longname(self) -> str:
    return '_'.join(self.name_path)

  @property
  def deepest_list_path(self) -> list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]:
    def has_list(path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]) -> bool:
      match path:
        case []:
          return False
        case [(_, fmeta), *_] if fmeta.type_.is_list:
          return True
        case [_, *rest]:
          return has_list(rest)
    
    def f(path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]) -> list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]:
      match path:
        case []:
          return []
        case [(_, fmeta) as field, *rest] if fmeta.type_.is_list:
          if not has_list(rest):
            return [field]
          else:
            return [field, *f(rest)]
        case [field, *rest]:
          return [field, *f(rest)]

    return f(self.path)

  @staticmethod
  def hash(msg: str) -> str:
    h = blake2b(digest_size=8)
    h.update(msg.encode('UTF-8'))
    return 'x' + h.hexdigest()

  def extract_data(self, data: dict | list[dict]) -> list[Any] | Any:
    return extract_data(self.data_path, data)

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
        case [(None, TypeMeta.FieldMeta() as fmeta), *rest]:
          return [Selection(fmeta, selection=f(rest))]

        case [(args, TypeMeta.FieldMeta() as fmeta), *rest] if args == {}:
          return [Selection(fmeta, selection=f(rest))]

        case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
          return [Selection(
            fmeta,
            # TODO: Revisit this
            alias=FieldPath.hash(fmeta.name + str(args)),
            arguments=arguments_of_field_args(fpath.subgraph.schema, fmeta, args),
            selection=f(rest)
          )]

        case []:
          return []

    # print(f'FieldPath.selection: path = {fpath.path}')
    return f(fpath.path)[0]

  @staticmethod
  def set_arguments(fpath: FieldPath, args: dict[str, Any], selection: list[FieldPath] = []) -> FieldPath:
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

    match fpath.leaf:
      case TypeMeta.FieldMeta():
        args = {key: fmt_arg(key, val) for key, val in args.items()}
        fpath.path[-1] = (args, fpath.path[-1][1])
        if selection:
          return list(selection | map(partial(FieldPath.extend, fpath)))
        else:
          return fpath
      case _:
        raise TypeError(f"Unexpected type for FieldPath {fpath}")

  # When setting arguments
  def __call__(self, **kwargs: Any) -> Any:
    """ Sets field arguments and expand subfields. The updated FieldPath is returned. 

    Example:
    ```python
    aaveV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")

    query = aaveV2.Query.borrows(
      first=10,
      order_by=aaveV2.Borrow.timestamp,
      order_direction="desc",
      selection=[
        aaveV2.Borrow.id,
        aaveV2.Borrow.timestamp,
        aaveV2.Borrow.amount
      ]
    )
    ```

    Returns:
      FieldPath | list[FieldPath]: The updated field path if selection is not set, or a list of FieldPaths when selection is set
    """
    selection = kwargs.pop('selection', [])
    return FieldPath.set_arguments(self, kwargs, selection)

  @staticmethod
  def select(fpath: FieldPath, name: str) -> FieldPath:
    """ Returns a new FieldPath corresponding to the FieldPath `fpath` extended with an additional 
    selection on the field named `name`.

    Args:
      fpath (FieldPath): The FieldPath on which to perform the selection/extension
      name (str): The name of the field to expand on the leaf of `fpath`

    Raises:
      TypeError: [description]
      TypeError: [description]
      TypeError: [description]

    Returns:
      FieldPath: A new FieldPath containing `fpath` extended with the field named `name`
    """
    match type_of_typeref(fpath.schema, fpath.type_):
      # If the FieldPath fpath
      case TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
        raise TypeError(f"FieldPath: path {fpath} ends with a scalar field! cannot select field {name}")

      case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as obj:
        field = field_of_object(obj, name)

        match type_of_field(fpath.schema, field):
          case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() | TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
            # Copy current path and append newly selected field
            path = fpath.path.copy()
            path.append((None, field))

            # Return new FieldPath
            return FieldPath(
              subgraph=fpath.subgraph,
              root_type=fpath.root_type,
              type_=field.type_,
              path=path
            )
          case _:
            raise TypeError(f"FieldPath: field {name} is not a valid field for object {fpath.type_.name} at path {fpath}")

      case _:
        raise TypeError(f"FieldPath: Unexpected type {fpath.type_.name} when selection {name} on {fpath}")

  @staticmethod
  def extend(fpath: FieldPath, ext: FieldPath) -> FieldPath:
    """ Extends the FieldPath `fpath` with the FieldPath `ext`. `ext` must start where the `fpath` ends.

    Args:
      fpath (FieldPath): The FieldPath to extend
      ext (FieldPath): The FieldPath representing the extension

    Raises:
      TypeError: [description]
      TypeError: [description]
      TypeError: [description]

    Returns:
      FieldPath: A new FieldPath containing the initial FieldPath `fpath` extended with `ext`
    """
    match fpath.leaf:
      case TypeMeta.FieldMeta() as fmeta:
        match schema.type_of_field(fpath.schema, fmeta):
          case TypeMeta.ObjectMeta(name=name) | TypeMeta.InterfaceMeta(name=name):
            if name == ext.root_type.name:
              return FieldPath(
                subgraph=fpath.subgraph,
                root_type=fpath.root_type,
                type_=ext.type_,
                path=fpath.path + ext.path
              )
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

  # ================================================================
  # Overloaded magic functions
  # ================================================================
  # Field selection
  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      return FieldPath.select(self, __name)

  # Filtering
  def __eq__(self, value: FieldPath | Any) -> Filter:
    if FieldPath.test_mode:
      # Purely used for testing so that assertEqual works
      return self.subgraph == value.subgraph and self.type_ == value.type_ and self.path == value.path
    else:
      return FieldPath.mk_filter(self, Filter.Operator.EQ, value)

  def __ne__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.NEQ, value)

  def __lt__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.LT, value)

  def __gt__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.GT, value)

  def __le__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.LTE, value)

  def __ge__(self, value: Any) -> Filter:
    return FieldPath.mk_filter(self, Filter.Operator.GTE, value)

  # Utility
  def __str__(self) -> str:
    return '.'.join(self.path | map(lambda ele: ele[1].name))

  def __repr__(self) -> str:
    return f'FieldPath({self.subgraph.url}, {self.root_type.name}, {self.name_path})'


@dataclass
class Object:
  subgraph: Subgraph
  object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta

  @property
  def schema(self):
    return self.subgraph.schema

  @staticmethod
  def select(obj: Object, name: str) -> FieldPath:
    field = schema.field_of_object(obj.object_, name)

    match schema.type_of_field(obj.schema, field):
      case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() | TypeMeta.EnumMeta() | TypeMeta.ScalarMeta() as type_:
        return FieldPath(obj.subgraph, TypeRef.Named(obj.object_.name), field.type_, [(None, field)])

      case TypeMeta.T as type_:
        raise TypeError(f"Object: Unexpected type {type_.name} when selection {name} on {obj}")

  @staticmethod
  def add_field(obj: Object, name: str, fpath: FieldPath) -> None:
    sfield = SyntheticField(identity, fpath.type_, fpath)
    obj.subgraph.add_synthetic_field(obj.object_, name, sfield)

  @staticmethod
  def add_sfield(obj: Object, name: str, sfield: SyntheticField) -> None:
    # TODO: Add check to make sure obj has the deps of sfield
    # obj_fields = [field.name for field in obj.object_.fields]
    # sfield_deps = [fpath.leaf.name for fpath in sfield.deps]
    # for dep in sfield_deps:
    #   if dep not in obj_fields:
    #     raise Exception(f'SyntheticField {obj.object_.name}.{name}: {obj.object_.name} does not have the field {dep}')

    def f(obj_: TypeMeta.ObjectMeta, path: list[str]) -> None:
      match path:
        case [field_name, *rest]:
          try:
            field: TypeMeta.FieldMeta = next(obj_.fields | where(lambda field: field.name == field_name))

            f(obj.schema.type_map[field.type_.name], rest)
          except StopIteration:
            raise Exception(f'SyntheticField {obj.object_.name}.{name}: {obj_.name} does not have the field {field_name}')
        case []:
          return
          
    for fpath in sfield.deps:
      f(obj.object_, fpath.name_path)

    obj.subgraph.add_synthetic_field(obj.object_, name, sfield)

  # ================================================================
  # Overloaded magic functions
  # ================================================================
  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      return Object.select(self, __name)

  def __setattr__(self, __name: str, __value: SyntheticField | FieldPath | Any) -> None:
    match __value:
      case SyntheticField() as sfield:
        Object.add_sfield(self, __name, sfield)
      case FieldPath() as fpath:
        Object.add_field(self, __name, fpath)
      case _:
        super().__setattr__(__name, __value)


@dataclass
class Subgraph:
  url: str
  schema: SchemaMeta
  transforms: list[DocumentTransform] = field(default_factory=list)

  # TODO: Remove
  @staticmethod
  def of_url(url: str) -> None:
    warnings.warn("`of_url` will be deprecated! Use `Subgrounds`'s `load_subgraph` instead", DeprecationWarning)
    filename = url.split("/")[-1] + ".json"
    if os.path.isfile(filename):
      with open(filename) as f:
        schema = json.load(f)
    else:
      schema = client.get_schema(url)
      with open(filename, mode="w") as f:
        json.dump(schema, f)

    return Subgraph(url, mk_schema(schema), DEFAULT_GLOBAL_TRANSFORMS)

  def add_synthetic_field(
    self,
    object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta,
    name: str,
    sfield: SyntheticField
  ) -> None:
    fmeta = TypeMeta.FieldMeta(name, '', [], sfield.type_)
    object_.fields.append(fmeta)

    # dep_selections = Selection.consolidate(list(sfield.deps | map(FieldPath.selection)))
    # match dep_selections:
    #   case []:
    #     dep_selections = []
    #   case [select]:
    #     dep_selections = []
    #   case [select, *rest]:
    #     dep_selections = reduce(Selection.consolidate, rest, select)

    # print(sfield.deps)
    # print(Selection.consolidate(list(sfield.deps | map(FieldPath.selection))))
    sfield_fpath = FieldPath(self, TypeRef.Named(object_.name), sfield.type_, [(None, fmeta)])
    logger.debug(f'Subgraph: Adding SyntheticField at FieldPath {sfield_fpath.root_type.name}.{sfield_fpath.name_path}')

    transform = LocalSyntheticField(
      self,
      fmeta,
      object_,
      FieldPath.selection(sfield_fpath),
      sfield.f,
      sfield.default,
      list(sfield.deps | map(FieldPath.selection))
    )

    self.transforms = [transform, *self.transforms]

  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      return Object(self, self.schema.type_map[__name])
