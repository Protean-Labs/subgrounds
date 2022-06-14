from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Optional, Tuple, TYPE_CHECKING
from functools import partial, reduce
import operator
from hashlib import blake2b
from pipe import map, traverse
import logging
import warnings
from datetime import datetime

from subgrounds.query import Query, Selection, arguments_of_field_args
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.utils import extract_data
from subgrounds.subgraph.filter import Filter
if TYPE_CHECKING:
  from subgrounds.subgraph.subgraph import Subgraph

logger = logging.getLogger('subgrounds')
warnings.simplefilter('default')

FPATH_DEPTH_LIMIT: int = 4


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
      return f_typeref(t1, field._type)


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
  _subgraph: Subgraph
  _type: TypeRef.T

  def __add__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.add, typeref_of_binary_op('add', self._type, other), [self, other])

  def __radd__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.add(y, x), typeref_of_binary_op('add', self._type, other), [self, other])

  def __sub__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.sub, typeref_of_binary_op('sub', self._type, other), [self, other])

  def __rsub__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.sub(y, x), typeref_of_binary_op('sub', self._type, other), [self, other])

  def __mul__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.mul, typeref_of_binary_op('mul', self._type, other), [self, other])

  def __rmul__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.mul(y, x), typeref_of_binary_op('mul', self._type, other), [self, other])

  def __truediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.truediv, typeref_of_binary_op('div', self._type, other), [self, other])

  def __rtruediv__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.truediv(y, x), typeref_of_binary_op('div', self._type, other), [self, other])

  def __floordiv__(self, other: Any) -> SyntheticField:
    return SyntheticField(operator.floordiv, typeref_of_binary_op('div', self._type, other), [self, other])

  def __rfloordiv__(self, other: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.floordiv(y, x), typeref_of_binary_op('div', self._type, other), [self, other])

  def __pow__(self, rhs: Any) -> SyntheticField:
    return SyntheticField(operator.pow, typeref_of_binary_op('pow', self._type, rhs), [self, rhs])

  def __rpow__(self, lhs: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.pow(y, x), typeref_of_binary_op('pow', self._type, lhs), [self, lhs])

  def __mod__(self, rhs: Any) -> SyntheticField:
    return SyntheticField(operator.mod, typeref_of_binary_op('mod', self._type, rhs), [self, rhs])

  def __rmod__(self, lhs: Any) -> SyntheticField:
    return SyntheticField(lambda x, y: operator.mod(y, x), typeref_of_binary_op('mod', self._type, lhs), [self, lhs])

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, type_ref_of_unary_op('neg', self._type), self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, type_ref_of_unary_op('abs', self._type), self)


def fieldpaths_of_object(
  subgraph: Subgraph,
  object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
):
  """ Returns generator of FieldPath objects that selects all non-list fields of
  GraphQL Object of Interface :attr:`object_`.

  Args:
    schema (SchemaMeta): _description_
    object_ (TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta): _description_

  Yields:
    _type_: _description_
  """
  for fmeta in object_.fields:
    if not fmeta.type_.is_list and len(fmeta.arguments) == 0:
      match subgraph._schema.type_of_typeref(fmeta.type_):
        case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta():
          yield subgraph.__getattribute__(object_.name).__getattribute__(fmeta.name).id
        case _:
          yield subgraph.__getattribute__(object_.name).__getattribute__(fmeta.name)


@dataclass
class FieldPath(FieldOperatorMixin):
  _subgraph: Subgraph
  _root_type: TypeRef.T
  _type: TypeRef.T
  _path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]

  # Purely for testing
  __test_mode: ClassVar[bool] = False

  def __init__(
    self,
    subgraph: Subgraph,
    root_type: TypeRef.T,
    type_: TypeRef.T,
    path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]],
  ) -> None:
    self._subgraph = subgraph
    self._root_type = root_type
    self._type = type_
    self._path = path

    # Add fields as attributes if leaf is object
    match self._subgraph._schema.type_of(self._leaf):
      case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as type_ if len(self._path) < FPATH_DEPTH_LIMIT:
        # We generate fieldpaths up to depth 8
        for fmeta in type_.fields:
          path = self._path.copy()
          path.append((None, fmeta))

          super().__setattr__(fmeta.name, FieldPath(
            subgraph=self._subgraph,
            root_type=self._root_type,
            type_=fmeta.type_,
            path=path
          ))

      case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as type_ if len(self._path) == FPATH_DEPTH_LIMIT:
        for fmeta in type_.fields:
          # NOTE: We set the attribute to None on purpose since we want code
          # completion to work while avoiding infinite loops caused by cycles
          # in the GraphQL schema. The attribute itself will be initialized
          # on __getattribute_
          super().__setattr__(fmeta.name, None)

      case _:
        pass

  @property
  def _schema(self) -> SchemaMeta:
    return self._subgraph._schema

  @property
  def _root(self) -> TypeMeta.FieldMeta:
    """ Returns the type information of the root field of the current
    :class:`FieldPath`

    Returns:
      TypeMeta.FieldMeta: Type information of the root field of the current
      :class:`FieldPath`
    """
    return self._path[0][1]

  @property
  def _leaf(self) -> TypeMeta.FieldMeta:
    """ Returns the type information of the leaf field of the current
    :class:`FieldPath`

    Returns:
      TypeMeta.FieldMeta: Type information of the leaf field of the current
      :class:`FieldPath`
    """
    return self._path[-1][1]

  @staticmethod
  def _hash(msg: str) -> str:
    h = blake2b(digest_size=8)
    h.update(msg.encode('UTF-8'))
    return 'x' + h.hexdigest()

  @staticmethod
  def _merge(fpaths: list[FieldPath]) -> list[Selection]:
    """ Returns a Selection tree containing all selection paths in `fpaths`.
    This function assumes that all fieldpaths in `fpaths` belong to the same subgraph

    Args:
      fpaths (list[FieldPath]): _description_

    Returns:
      list[Selection]: _description_
    """
    query = reduce(Query.add, fpaths | map(FieldPath._selection), Query())
    return query.selection

  def _name_path(self, use_aliases: bool = False) -> list[str]:
    """ Returns a list of strings correspoding to the names of all fields
    selected in the current :class:`FieldPath`. If :attr:`use_aliases` is True,
    then if a field has an automatically generated alias, the alias will be
    returned.

    Args:
      use_aliases (bool, optional): Flag indicating wether of not to use the
      fields' automatically generated alias (if present). Defaults to False.

    Returns:
      list[str]: List of field names selected in the current :class:`FieldPath`
    """

    def gen_alias(
      ele: Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]
    ) -> str:
      if ele[0] != {} and ele[0] is not None:
        return FieldPath._hash(ele[1].name + str(ele[0]))
      else:
        return ele[1].name

    return list(
      self._path
      | map(lambda ele: gen_alias(ele) if use_aliases else ele[1].name)
    )

  def _name(self, use_aliases: bool = False) -> str:
    """ Generates the name of the current :class:`FieldPath` using the names of
    the fields it selects. If :attr:`use_aliases` is True, then if a field has
    an automatically generated alias, the alias will be used.

    Args:
      use_aliases (bool, optional): Flag indicating wether of not to use the
      fields' automatically generated alias (if present). Defaults to False.

    Returns:
      str: The generated name of the current :class:`FieldPath`.
    """
    return '_'.join(self._name_path(use_aliases=use_aliases))

  def _auto_select(self) -> FieldPath | list[FieldPath]:
    match self._subgraph._schema.type_of_typeref(self._leaf.type_):
      case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as obj:
        return list(
          fieldpaths_of_object(self._subgraph, obj)
          | map(partial(FieldPath._extend, self))
        )

      case _:
        return self

  def _extract_data(self, data: dict | list[dict]) -> list[Any] | Any:
    """ Extract the data corresponding to the current :class:`FieldPath` from
    the dictionary :attr:`data`.

    Args:
      data (dict | list[dict]): Data dictionary that contains the data
      corresponding to the current :class:`FieldPath`.

    Returns:
      list[Any] | Any: Data corresponding to the current :class:`FieldPath`.
    """
    return extract_data(self._name_path(use_aliases=True), data)

  def _selection(self) -> Selection | list[Selection]:
    """ Returns a selection or list of selections corresponding to the current
    :class:`FieldPath`.

    Returns:
      Selection | list[Selection]: _description_
    """
    def f(path: list[Tuple[Optional[dict[str, Any]], TypeMeta.FieldMeta]]) -> list[Selection]:
      match path:
        case [(args, TypeMeta.FieldMeta() as fmeta), *rest] if args == {} or args is None:
          return [Selection(fmeta, selection=f(rest))]

        case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
          return [Selection(
            fmeta,
            # TODO: Revisit this
            alias=FieldPath._hash(fmeta.name + str(args)),
            arguments=arguments_of_field_args(self._subgraph._schema, fmeta, args),
            selection=f(rest)
          )]

        case []:
          return []

      assert False  # Suppress mypy missing return statement warning

    return f(self._path)[0]

  def _set_arguments(
    self,
    args: dict[str, Any],
    selection: list[FieldPath] = []
  ) -> FieldPath | list[FieldPath]:
    """ Set the arguments to the leaf of the current :class:`FieldPath`. The
    method returns the :attr:`self`.

    Args:
      args (dict[str, Any]): _description_
      selection (list[FieldPath], optional): _description_. Defaults to [].

    Returns:
      FieldPath: _description_
    """
    def fmt_arg(name, raw_arg):
      match (name, raw_arg):
        case ('where', [Filter(), *_] as filters):
          return Filter.to_dict(filters)
        case ('orderBy', FieldPath() as fpath):
          match fpath._leaf:
            case TypeMeta.FieldMeta() as fmeta:
              return fmeta.name
            case _:
              raise Exception(f"Cannot use non field {fpath} as orderBy argument")
        case _:
          return raw_arg

    match self._leaf:
      case TypeMeta.FieldMeta():
        args = {key: fmt_arg(key, val) for key, val in args.items()}
        self._path[-1] = (args, self._path[-1][1])
        if len(selection) > 0:
          return list(selection | map(partial(FieldPath._extend, self)))
        else:
          return self
      case _:
        raise TypeError(f"Unexpected type for FieldPath {self}")

  def _select(self, name: str) -> FieldPath:
    """ Returns a new FieldPath corresponding to the FieldPath `self` extended with an additional
    selection on the field named `name`.
    Args:
      name (str): The name of the field to expand on the leaf of `fpath`
    Raises:
      TypeError: [description]
      TypeError: [description]
      TypeError: [description]
    Returns:
      FieldPath: A new FieldPath containing `fpath` extended with the field named `name`
    """
    match self._schema.type_of_typeref(self._type):
      # If the FieldPath fpath
      case TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
        raise TypeError(f"FieldPath: path {self} ends with a scalar field! cannot select field {name}")

      case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as obj:
        field = obj.field(name)

        match self._schema.type_of_typeref(field.type_):
          case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() | TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
            # Copy current path and append newly selected field
            path = self._path.copy()
            path.append((None, field))

            # Return new FieldPath
            return FieldPath(
              subgraph=self._subgraph,
              root_type=self._root_type,
              type_=field.type_,
              path=path
            )
          case _:
            raise TypeError(f"FieldPath: field {name} is not a valid field for object {self._type.name} at path {self}")

      case _:
        raise TypeError(f"FieldPath: Unexpected type {self._type.name} when selection {name} on {self}")

  def _extend(self, ext: FieldPath) -> FieldPath:
    """ Extends the current :class:`FieldPath` with the :class:`FieldPath`
    :attr:`ext`. :attr:`ext` must start where the current :class:`FieldPath` ends.

    Args:
      ext (FieldPath): The :class:`FieldPath` representing the extension

    Raises:
      TypeError: [description]
      TypeError: [description]
      TypeError: [description]

    Returns:
      FieldPath: A new :class:`FieldPath` containing the initial current
      :class:`FieldPath` extended with :attr:`ext`
    """
    match self._leaf:
      case TypeMeta.FieldMeta() as fmeta:
        match self._schema.type_of_typeref(fmeta.type_):
          case TypeMeta.ObjectMeta(name=name) | TypeMeta.InterfaceMeta(name=name):
            if name == ext._root_type.name:
              return FieldPath(
                subgraph=self._subgraph,
                root_type=self._root_type,
                type_=ext._type,
                path=self._path + ext._path
              )
            else:
              raise TypeError(f"extend: FieldPath {ext} does not start at the same type from where FieldPath {self} ends")
          case _:
            raise TypeError(f"extend: FieldPath {self} is not object field")
      case _:
        raise TypeError(f"extend: FieldPath {self} is not an object field")

  # ================================================================
  # Overloaded magic functions
  # ================================================================
  # When setting arguments
  def __call__(self, **kwargs: Any) -> Any:
    """ Sets field arguments and expand subfields. The updated FieldPath is returned.
    Example:
    >>> aaveV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")
    >>> query = aaveV2.Query.borrows(
    ...   first=10,
    ...   order_by=aaveV2.Borrow.timestamp,
    ...   order_direction="desc",
    ...   selection=[
    ...     aaveV2.Borrow.id,
    ...     aaveV2.Borrow.timestamp,
    ...     aaveV2.Borrow.amount
    ...   ]
    ... )
    Returns:
      FieldPath | list[FieldPath]: The updated field path if :attr:`selection`
        is not specified, or a list of fieldpaths when :attr:`selection` is
        specified.
    """
    selection = kwargs.pop('selection', [])
    return self._set_arguments(kwargs, selection)

  # Field selection
  def __getattribute__(self, __name: str) -> Any:
    # Small hack to get code completion to work while allowing updates to FieldPath
    # (i.e.: setting arguments)
    try:
      match super().__getattribute__(__name):
        case FieldPath() | SyntheticField() | None:
          return self._select(__name)
        case value:
          return value
    except AttributeError:
      return self._select(__name)

  # Filtering
  def __eq__(self, value: FieldPath | Any) -> Filter | bool:
    if FieldPath.__test_mode:
      # Purely used for testing so that assertEqual works
      return self._subgraph == value._subgraph and self._type == value._type and self._path == value._path
    else:
      return Filter.mk_filter(self, Filter.Operator.EQ, value)

  def __ne__(self, value: Any) -> Filter:
    return Filter.mk_filter(self, Filter.Operator.NEQ, value)

  def __lt__(self, value: Any) -> Filter:
    return Filter.mk_filter(self, Filter.Operator.LT, value)

  def __gt__(self, value: Any) -> Filter:
    return Filter.mk_filter(self, Filter.Operator.GT, value)

  def __le__(self, value: Any) -> Filter:
    return Filter.mk_filter(self, Filter.Operator.LTE, value)

  def __ge__(self, value: Any) -> Filter:
    return Filter.mk_filter(self, Filter.Operator.GTE, value)

  # Utility
  def __str__(self) -> str:
    return '.'.join(self._path | map(lambda ele: ele[1].name))

  def __repr__(self) -> str:
    return f'FieldPath({self._subgraph._url}, {self._root_type.name}, {self._name_path()})'


@dataclass
class SyntheticField(FieldOperatorMixin):
  STRING: ClassVar[TypeRef.Named] = TypeRef.Named('String')
  INT: ClassVar[TypeRef.Named] = TypeRef.Named('Int')
  FLOAT: ClassVar[TypeRef.Named] = TypeRef.Named('Float')
  BOOL: ClassVar[TypeRef.Named] = TypeRef.Named('Boolean')

  _counter: ClassVar[int] = 0

  _f: Callable
  _type: TypeRef.T
  _default: Any
  _deps: list[FieldPath]

  def __init__(
    self,
    f: Callable,
    type_: TypeRef.T,
    deps: list[FieldPath | SyntheticField] | FieldPath | SyntheticField,
    default: Any = None
  ) -> None:
    deps = list([deps] | traverse)

    def mk_deps(
      deps: list[FieldPath | SyntheticField],
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
            _counter = 0
            for (f_, deps) in acc:
              match (f_, deps):
                case (None, FieldPath()):
                  new_args.append(args[_counter])
                  _counter += 1
                case (None, int() | float() | str() | bool() as constant):
                  new_args.append(constant)
                case (f_, list() as deps):
                  new_args.append(f_(*args[_counter:_counter + len(deps)]))
                  _counter += len(deps)

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

        case [SyntheticField(_f=inner_f, _deps=inner_deps), *rest]:
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
    self._f = f
    self._type = type_
    self._default = default if default is not None else SyntheticField.default_of_type(type_)
    self._deps = deps

    SyntheticField._counter += 1

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
    """ Returns a constant SyntehticField with value ``value``. Useful for injecting
    additional static data to a schema.

    Args:
      value (str | int | float | bool): The constant field's value

    Returns:
      SyntheticField: The constant SyntheticField
    """
    match value:
      case str():
        return SyntheticField(lambda: value, SyntheticField.STRING, [])
      case int():
        return SyntheticField(lambda: value, SyntheticField.INT, [])
      case float():
        return SyntheticField(lambda: value, SyntheticField.FLOAT, [])
      case bool():
        return SyntheticField(lambda: value, SyntheticField.BOOL, [])

  @staticmethod
  def datetime_of_timestamp(timestamp: FieldPath | SyntheticField) -> SyntheticField:
    """ Returns a SyntheticField that will transform the ``FieldPath`` ``timestamp``
    into a human-readable ISO8601 string.

    Args:
      timestamp (FieldPath | SyntheticField): A ``FieldPath`` pointing to a
      Unix timestamp field.

    Returns:
      SyntheticField: An ISO8601 datetime string SyntheticField.
    """
    return SyntheticField(
      lambda timestamp: str(datetime.fromtimestamp(timestamp)),
      SyntheticField.STRING,
      timestamp
    )
