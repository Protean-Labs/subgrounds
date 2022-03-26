""" Query data structure module

This module contains various data structures in the form of dataclasses that
are used to represent GraphQL queries in Subgrounds. To the extent possible,
these dataclasses are immutable (i.e.: :attr:`frozen=True`) to enforce a
functional programming style and reduce side-effects.

A typical Subgrounds request will have the following dataclass hierarchy:
.. code-block:: none

  DataRequest
    Document
      Query
        VariableDefinition
          InputValue
        Selection
          Argument
            InputValue
          Selection
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import partial, reduce
from typing import Any, Callable, Optional, Protocol
from pipe import map, traverse, where, take
import warnings

from subgrounds.schema import (
  TypeMeta,
  SchemaMeta,
  TypeRef
)
from subgrounds.utils import (
  extract_data,
  filter_none,
  identity,
  rel_complement,
  union
)

import logging
logger = logging.getLogger('subgrounds')
warnings.simplefilter('default')


# ================================================================
# Query definitions, data structures and types
# ================================================================
class InputValue:
  # @dataclass(frozen=True)
  class T(ABC):
    @property
    @abstractmethod
    def graphql(self) -> str:
      """ Returns a GraphQL string representation of the input value

      Returns:
        str: The GraphQL string representation of the input value
      """
      pass

    @property
    def is_variable(self) -> bool:
      """ Returns True i.f.f. the input value is of type Variable

      Returns:
        bool: True i.f.f. the input value is of type Variable, otherwise False
      """
      return False

    @property
    def is_number(self) -> bool:
      """ Returns True i.f.f. the input value is of type Float or Int

      Returns:
        bool: True i.f.f. the input value is of type Float or Int, otherwise False
      """
      return False

  @dataclass(frozen=True)
  class Null(T):
    @property
    def graphql(self) -> str:
      return "null"

  @dataclass(frozen=True)
  class Int(T):
    value: int

    @property
    def graphql(self) -> str:
      return str(self.value)

    @property
    def is_number(self) -> bool:
      return True

  @dataclass(frozen=True)
  class Float(T):
    value: float

    @property
    def graphql(self) -> str:
      return str(self.value)

    @property
    def is_number(self) -> bool:
      return True

  @dataclass(frozen=True)
  class String(T):
    value: str

    @property
    def graphql(self) -> str:
      return f"\"{self.value}\""

  @dataclass(frozen=True)
  class Boolean(T):
    value: bool

    @property
    def graphql(self) -> str:
      return str(self.value).lower()

  @dataclass(frozen=True)
  class Enum(T):
    value: str

    @property
    def graphql(self) -> str:
      return self.value

  @dataclass(frozen=True)
  class Variable(T):
    name: str

    @property
    def graphql(self) -> str:
      return f'${self.name}'

    @property
    def is_variable(self) -> bool:
      return True

  @dataclass(frozen=True)
  class List(T):
    value: list[InputValue.T]

    @property
    def graphql(self) -> str:
      return f"[{', '.join([val.graphql for val in self.value])}]"

  @dataclass(frozen=True)
  class Object(T):
    value: dict[str, InputValue.T]

    @property
    def graphql(self) -> str:
      return f"{{{', '.join([f'{key}: {value.graphql}' for key, value in self.value.items()])}}}"


@dataclass(frozen=True)
class VariableDefinition:
  """ Representation of a GraphQL variable definition

  Attributes:
    name (str): Name of the argument
    type_ (TypeRef.T): GraphQL type of the argument
    default (InputValue.T, optional): Default value of the variable.
      Defaults to None.
  """
  name: str
  type_: TypeRef.T
  default: Optional[InputValue.T] = None

  @property
  def graphql(self) -> str:
    """ Returns the GraphQL string representation of the variable definition

    Example:

    >>> vardef = VariableDefinition(
    ...   name='foo',
    ...   type_=TypeRef.NonNull(TypeRef.Named('Int')),
    ...   default=InputValue.Int(100)
    ... )
    >>> print(vardef.graphql)
    $foo: Int! = 100

    Returns:
        str: The GraphQL string representation of the variable definition
    """
    if self.default is None:
      return f'${self.name}: {TypeRef.graphql(self.type_)}'
    else:
      return f'${self.name}: {TypeRef.graphql(self.type_)} = {self.default.graphql}'


@dataclass(frozen=True)
class Argument:
  name: str
  value: InputValue.T

  @property
  def graphql(self) -> str:
    return f"{self.name}: {self.value.graphql}"


@dataclass(frozen=True)
class Selection:
  """ Represents a GraphQL field selection.

  Attributes:
    fmeta (TypeMeta.FieldMeta): The type definition of the field being selected.
    alias (str, optional): The alias of the field selection. Defaults to None.
    arguments (list[Argument]): The arguments, if any, of the field selection.
      Defaults to [].
    selection (list[Selection]): The inner field selections, if any.
      Defaults to [].
  """
  fmeta: TypeMeta.FieldMeta
  alias: Optional[str] = None
  arguments: list[Argument] = field(default_factory=list)
  selection: list[Selection] = field(default_factory=list)

  @property
  def key(self):
    if self.alias is not None:
      return self.alias
    else:
      return self.fmeta.name

  @property
  def args_graphql(self) -> str:
    if self.arguments:
      return f'({", ".join([arg.graphql for arg in self.arguments])})'
    else:
      return ""

  def graphql(self, level: int = 0) -> str:
    indent = "  " * level

    if self.alias:
      alias_str = f'{self.alias}: '
    else:
      alias_str = ''

    match (self.selection):
      case None | []:
        return f"{indent}{alias_str}{self.fmeta.name}{self.args_graphql}"
      case inner_selection:
        inner_str = "\n".join(
          [f.graphql(level=level + 1) for f in inner_selection]
        )
        return f"{indent}{alias_str}{self.fmeta.name}{self.args_graphql} {{\n{inner_str}\n{indent}}}"

  @property
  def data_path(self) -> list[str]:
    match self:
      case Selection(TypeMeta.FieldMeta(name), None, _, []) | Selection(TypeMeta.FieldMeta(_), str() as name, _, []):
        return [name]
      case Selection(TypeMeta.FieldMeta(name), None, _, [inner_select, *_]) | Selection(TypeMeta.FieldMeta(_), str() as name, _, [inner_select, *_]):
        return [name] + inner_select.data_path

    assert False  # Suppress mypy missing return statement warning

  @property
  def data_paths(self) -> list[list[str]]:
    def f(select: Selection, keys: list[str] = []):
      match select:
        case Selection(TypeMeta.FieldMeta(name), None, _, []) | Selection(TypeMeta.FieldMeta(_), str() as name, _, []):
          yield [*keys, name]
        case Selection(TypeMeta.FieldMeta(name), None, _, inner) | Selection(TypeMeta.FieldMeta(_), str() as name, _, inner):
          for select in inner:
            yield from f(select, keys=[*keys, name])

    return list(f(self))

  def contains_list(self: Selection, recurse: bool = True) -> bool:
    """ Returns True i.f.f. the selection :attr:`self` selects a field of type
    list. If :attr:`recurse` is set to True, then the method also checks for
    list fields in nested selections.

    Args:
      self (Selection): The selection to traverse
      recurse (bool, optional): Flag indicating if the method should be executed
        on the nested selections. Defaults to True.

    Returns:
      bool: True if selection (or possibly nested selections) selects a list
      field. False otherwise.
    """
    if self.fmeta.type_.is_list:
      return True
    else:
      if recurse:
        return any(self.selection | map(Selection.contains_list))
      else:
        return False

  def split(self: Selection) -> list[Selection]:
    """ Returns a list of selections where each of the selections corresponds
    to a single selection path from the root to a leaf for each leaf selected
    in :attr:`self`.

    Example (simplified, does not show all attributes):

    >>> select = Selection('foo', inner=[
    ...   Selection('bar', inner=[
    ...     Selection('field0', inner=[]),
    ...     Selection('field1', inner=[]),
    ...   ]),
    ...   Selection('x', inner=[])
    ... ])
    >>> split(select)
    [
      Selection('foo', inner=[Selection('bar', inner=[Selection('field0', inner=[])])]),
      Selection('foo', inner=[Selection('bar', inner=[Selection('field1', inner=[])])]),
      Selection('foo', inner=[Selection('x', inner=[])]),
    ]

    Args:
      self (Selection): The selection to split

    Returns:
      list[Selection]: The split selections
    """
    match self:
      case Selection(_, _, _, [] | None):
        return [self]
      case Selection(fmeta, alias, args, inner_select):
        return list(
          inner_select
          | map(Selection.split)
          | traverse
          | map(lambda inner_select: Selection(fmeta, alias, args, inner_select))
        )

  def extract_data(self: Selection, data: dict | list[dict]) -> list[Any] | Any:
    return extract_data(self.data_path, data)

  def add(
    self: Selection,
    new_selections: Selection | list[Selection]
  ) -> Selection:
    """ Returns a new selection consisting of a copy of :attr:`self` expanded
    with the selection(s) :attr:`new_selections`. It is assumed that
    :attr:`new_selections` are inner selections of the root selection
    :attr:`self`.

    Args:
      self (Selection): The Selection object to be expanded
      new_selections (Selection | list[Selection]): A single or multiple
        Selection object(s) to be added to :attr:`self`

    Returns:
      Selection: The resulting new selection, i.e.: :attr:`self`
        expanded with :attr:`new_selections`
    """
    match new_selections:
      case Selection() as new_selection:
        return self.add([new_selection])

      case list() as new_selections:
        return Selection(
          fmeta=self.fmeta,
          alias=self.alias,
          selection=union(
            self.selection,
            new_selections,
            key=lambda select: select.fmeta.name,
            combine=Selection.combine
          )
        )

  def remove(
    self: Selection,
    to_remove: Selection | list[Selection]
  ) -> Selection:
    """ Returns a new Selection object consisting of a copy of :attr:`self`
    without the selections in :attr:`selections_to_remove`.

    Args:
      self (Selection): The selection to trim down
      to_remove (Selection | list[Selection]): The selection(s) to remove from
        :attr:`self`

    Returns:
      Selection: The new trimmed down selection, i.e.: :attr:`self` without
        :attr:`selections_to_remove`
    """
    def combine(select: Selection, selection_to_remove: Selection) -> Optional[Selection]:
      if selection_to_remove.selection == []:
        return None
      else:
        return select.remove(selection_to_remove.selection)

    # TODO: Resolve mypy complaints
    match to_remove:
      case Selection() as to_remove:
        return self.remove([to_remove])

      case list() as to_remove:
        return Selection(
          fmeta=self.fmeta,
          alias=self.alias,
          arguments=self.arguments,
          selection=filter_none(union(
            self.selection,
            to_remove,
            key=lambda s: s.fmeta.name,
            combine=combine
          ))
        )

  def variable_args(self: Selection, recurse: bool = True) -> list[Argument]:
    var_args = list(self.arguments | where(lambda arg: type(arg.value) == InputValue.Variable))
    return list(self.selection | map(Selection.variable_args) | traverse) + var_args

  def infer_variable_definitions(self: Selection) -> list[VariableDefinition]:
    def vardef_of_arg(arg):
      match arg:
        case Argument(name=argname, value=InputValue.Variable(name=varname)):
          return VariableDefinition(varname, self.fmeta.type_of(argname))

        case Argument(name=argname, value=InputValue.Object(fields)):
          return list(
            fields.values()
            | where(lambda iv: type(iv) == InputValue.Variable)
            | map(lambda iv: VariableDefinition(iv.name, self.fmeta.type_of(argname)))
          )

        case _:
          return []

    var_defs = list(self.arguments | map(vardef_of_arg) | traverse)

    return list(self.selection | map(Selection.infer_variable_definitions) | traverse) + var_defs

  def combine(self: Selection, other: Selection) -> Selection:
    if self.key != other.key:
      raise Exception(f"Selection.combine: {self.key} != {other.key}")

    return Selection(
      fmeta=self.fmeta,
      alias=self.alias,
      arguments=self.arguments,
      selection=filter_none(union(
        self.selection,
        other.selection,
        key=lambda select: select.fmeta.name,
        combine=Selection.combine
      ))
    )

  @staticmethod
  def merge(selections: list[Selection]) -> list[Selection]:
    """ Returns a list of Selection objects resulting from merging
    :attr:`selections` to the extent possible.

    Args:
      selections (list[Selection]): The selections to be merged

    Returns:
      list[Selection]: _description_
    """
    def f(selections: list[Selection], other: Selection) -> list[Selection]:
      try:
        next(selections | where(lambda select: select.key == other.key))
        return list(selections | map(lambda select: Selection.combine(select, other) if select.key == other.key else select))
      except StopIteration:
        return selections + [other]

    return reduce(f, selections, [])

  def contains(self: Selection, other: Selection) -> bool:
    """ Returns True i.f.f. the Selection :attr:`other` is a subtree of the
    Selection :attr:`self` and False otherwise

    Args:
      self (Selection): The selection
      other (Selection): The subselection

    Returns:
      bool: True i.f.f. :attr:`other` is in :attr:`self`
    """
    if (self.fmeta == other.fmeta and rel_complement(other.selection, self.selection, key=lambda s: s.fmeta.name) == []):
      return all(
        other.selection
        | map(lambda s: Selection.contains(next(filter(lambda s_: s.fmeta.name == s_.fmeta.name, self.selection)), s))
      )
    else:
      return False

  def contains_argument(
    self: Selection,
    argname: str,
    recurse: bool = True
  ) -> bool:
    """ Returns True i.f.f. there is an Argument object in :attr:`self` named
    :attr:`argname`. If :attr:`recurse` is True, then the method also checks the
    nested selections for an argument named :attr:`argname`.

    Args:
      self (Selection): The selection
      argname (str): The name of the argument
      recurse (bool, optional): Flag indicating whether or not the method should
        be run recursively on nested selections. Defaults to True.

    Returns:
      bool: True i.f.f. there is an argument named :attr:`argname` in :attr:`self`
    """
    try:
      next(filter(lambda arg: arg.name == argname, self.arguments))
      return True
    except StopIteration:
      if recurse:
        return any(
          self.selection
          | map(partial(Selection.contains_argument, argname=argname))
        )
      else:
        return False

  def get_argument(
    self: Selection,
    argname: str,
    recurse: bool = True
  ) -> Argument:
    """ Returns an Argument object corresponding to the argument in the Selection
    object :attr:`select` with name :attr:`argname`. If :attr:`select` does not
    contain such an argument and :attr:`recurse` is True, then the function is
    called recursively on :attr:`select`'s inner selections. If no such argument
    is found in :attr:`select` or its inner selections, then the function raises
    an exception.

    Args:
      select (Selection): The selection to scan
      argname (str): The name of the argument to find
      recurse (bool, optional): Flag indicating whether or not the method should
        be run recursively on nested selections. Defaults to True.

    Raises:
      KeyError: If no argument named :attr:`argname` exists in the selection
        :attr:`self`.

    Returns:
      Argument: The argument in :attr:`select` with name :attr:`argname` (if any).
    """
    try:
      return next(self.arguments | where(lambda arg: arg.name == argname))
    except StopIteration:
      if recurse:
        try:
          return next(
            self.selection
            | map(partial(Selection.get_argument, argname=argname))
            | where(lambda x: x is not None)
          )
        except StopIteration:
          raise KeyError(f'Selection.get_argument: no argument named {argname} found in selection')
      else:
        raise KeyError(f'Selection.get_argument: no argument named {argname} found in selection')

  def get_argument_by_variable(
    self: Selection,
    varname: str,
    recurse: bool = True
  ) -> Argument:
    """ Returns an Argument object corresponding to the argument in the Selection
    object :attr:`select` whose value is a variable named :attr:`varname`. If
    :attr:`select` does not contain such an argument and :attr:`recurse` is True,
    then the function is called recursively on :attr:`select`'s inner selections.
    If no such argument is found in :attr:`select` or its inner selections, then
    the function raises an exception

    Args:
      select (Selection): The selection to scan
      varname (str): The name of the variable to find
      recurse (bool, optional): Flag indicating whether or not the function
        should be run recursively. Defaults to True.

    Raises:
      KeyError: If no argument with variable value named :attr:`varname` exists
        in the selection :attr:`self`.

    Returns:
      Argument: The argument in :attr:`select` with variable value named
        :attr:`varname` if it exists
    """
    try:
      return next(self.arguments | where(lambda arg: arg.value.name == varname))
    except (StopIteration, AttributeError):
      if recurse:
        try:
          return next(
            self.selection
            | map(partial(Selection.get_argument, varname=varname))
            | where(lambda x: x is not None)
          )
        except StopIteration:
          raise KeyError(f'Selection.get_argument_by_variable: no argument with variable value named {varname} found in selection')
      else:
        raise KeyError(f'Selection.get_argument_by_variable: no argument with variable value named {varname} found in selection')

  def substitute_arg(
    self: Selection,
    argname: str,
    replacement: Argument | list[Argument],
    recurse: bool = True
  ) -> Selection:
    """ Returns a new Selection object containing the same data as :attr:`self`
    with the argument named :attr:`argname` replaced with :attr:`replacement`.
    If :attr:`recurse` is True, then the method is called recursively on
    :attr:`self`'s inner selections and the substitution is also applied to the
    latter.

    Args:
      self (Selection): _description_
      argname (str): The name of the argument to substitute.
      replacement (Argument | list[Argument]): The argument(s) replacement
      recurse (bool, optional): Flag indicating whether or not the method
        should be run recursively. Defaults to True.

    Returns:
      Selection: _description_
    """
    return Selection(
      fmeta=self.fmeta,
      alias=self.alias,
      arguments=list(
        self.arguments
        | map(lambda arg: replacement if arg.name == argname else arg)
        | traverse
      ),
      selection=list(
        self.selection
        | map(partial(Selection.substitute_arg, argname=argname, replacement=replacement))
      ) if recurse else self.selection
    )

  def select(self: Selection, other: Selection) -> Selection:
    if other.selection == []:
      return self
    else:
      return Selection(
        fmeta=self.fmeta,
        alias=self.alias,
        arguments=self.arguments,
        selection=list(
          other.selection
          | map(lambda s: next(
            self.selection
            | where(lambda s_: s_.fmeta.name == s.fmeta.name)
            | map(lambda s_: Selection.select(s_, s))
            | take(1)
          ))
        )
      )

  # TODO: Function to recover an approximate selection from a JSON data object
  # @staticmethod
  # def of_json(data: dict) -> Selection:
  #   pass


@dataclass(frozen=True)
class Query:
  name: Optional[str] = None
  selection: list[Selection] = field(default_factory=list)

  # Variables as query arguments, not the values of those variables
  variables: list[VariableDefinition] = field(default_factory=list)

  @property
  def graphql(self) -> str:
    """ Returns a string containing a GraphQL query matching the current query

    Returns:
      str: The string containing the GraphQL query
    """
    selection_str = "\n".join(
      [select.graphql(level=1) for select in self.selection]
    )

    if len(self.variables) > 0:
      args_str = f'({", ".join([vardef.graphql for vardef in self.variables])})'
    else:
      args_str = ''

    return f'query{args_str} {{\n{selection_str}\n}}'

  def infer_variable_definitions(self: Query) -> list[VariableDefinition]:
    return list(self.selection | map(Selection.infer_variable_definitions) | traverse)

  def add(
    self: Query,
    other: Query | Selection | list[Selection]
  ) -> Query:
    """ Returns a new Query containing all selections in :attr:'self' along with
    the new selections in :attr:`other`

    Args:
      self (Query): The query to which new selection(s) or query are to be added
      other (Query | Selection | list[Selection]): The new selection(s)
      or query to be added to the query

    Returns:
      Query: A new `Query` objects containing all selections
    """
    match other:
      case Selection() as new_selection:
        return self.add([new_selection])

      case Query() as query:
        return self.add(query.selection)

      case list() as new_selections:
        return Query(
          name=self.name,
          selection=union(
            self.selection,
            new_selections,
            key=lambda select: select.key,
            combine=Selection.combine
          )
        )

  def remove(
    self: Query,
    other: Query | Selection | list[Selection]
  ) -> Query:
    """ Returns a new :class:`Query` object containing all selections in
    :attr:`self` minus the subquery or selection(s) specified in :attr:`other`.

    Note: :attr:`other` does not need to be a "full" selection (i.e.: a
    selection all the way to leaves of the GraphQL schema).

    Example:

    >>> og_selection = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
    ...   Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
    ...     Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
    ...     Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
    ...     Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
    ...   ])
    ... ])
    >>> selection_to_remove = Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [])
    >>> og_selection.remove(selection_to_remove)
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [])

    Args:
      query (Query): The query to which a selection has to be removed
      other (Query | Selection | list[Selection]): The subquery or
        selection(s) to remove from :attr:`self`

    Returns:
      Query: A new `Query` object containing the original query selections
        minus :attr:`other`
    """
    def combine(select: Selection, selection_to_remove: Selection) -> Optional[Selection]:
      if selection_to_remove.selection == []:
        return None
      else:
        return select.remove(selection_to_remove.selection)

    match other:
      case Selection() as to_remove:
        return self.remove([to_remove])

      case Query() as to_remove:
        return self.remove(to_remove.selection)

      case list() as to_remove:
        return Query(
          name=self.name,
          selection=filter_none(union(
            self.selection,
            to_remove,
            key=lambda s: s.fmeta.name,
            combine=combine
          )),
          variables=self.variables
        )

  @staticmethod
  def transform(
    query: Query,
    variable_f: Callable[[VariableDefinition], VariableDefinition] = identity,
    selection_f: Callable[[Selection], Selection] = identity
  ) -> Query:
    return reduce(Query.add, query.selection | map(selection_f) | traverse, Query(
      name=query.name,
      variables=list(query.variables | map(variable_f) | traverse)
    ))

  def contains_selection(self: Query, selection: Selection) -> bool:
    """ Returns True i.f.f. the selection tree :attr:`selection` is present in
    :attr:`query`.

    Args:
      query (Query): A query object
      selection (Selection): The selection to be found (or not) in :attr:`query`

    Returns:
      bool: True if the :attr:`selection` is present in :attr:`query`, False
        otherwise.
    """
    return any(
      self.selection
      | map(lambda select: Selection.contains(select, selection))
    )

  @staticmethod
  def contains_argument(query: Query, arg_name: str) -> bool:
    return any(query.selection | map(partial(Selection.contains_argument, arg_name=arg_name)))

  @staticmethod
  def get_argument(query: Query, target: str) -> Optional[Argument]:
    try:
      return next(
        query.selection
        | map(partial(Selection.get_argument, target=target))
        | where(lambda x: x is not None)
      )
    except StopIteration:
      return None

  @staticmethod
  def substitute_arg(query: Query, arg_name: str, replacement: Argument | list[Argument]) -> Query:
    return Query(
      name=query.name,
      selection=list(
        query.selection
        | map(partial(Selection.substitute_arg, arg_name=arg_name, replacement=replacement))
      ),
      variables=query.variables
    )

  @staticmethod
  def contains(query: Query, other: Query) -> bool:
    """ Returns True i.f.f. all selections in `other` are contained in `query`. In other words,
    returns true i.f.f. `other` is a subset of `query`.

    Note: `other` does not need to include "full" selections (i.e.: selections all the way to
    leaves of the GraphQL schema).

    Args:
      query (Query): The query that is to be checked
      other (Query): The query that has to be in `query`

    Returns:
      bool: True i.f.f. all selections in `other` are contained in `query`, otherwise False
    """
    return all(other.selection | map(partial(Query.contains_selection, query)))

  @staticmethod
  def select(query: Query, other: Query) -> Query:
    """ Returns a new Query

    Args:
        query (Query): [description]
        other (Query): [description]

    Returns:
        Query: [description]
    """
    return Query(
      name=query.name,
      selection=list(
        other.selection
        | map(lambda s: next(
          query.selection
          | where(lambda s_: s_.fmeta.name == s.fmeta.name)
          | map(lambda s_: Selection.select(s_, s))
          | take(1)
        ))
      ),
      variables=query.variables
    )


@dataclass(frozen=True)
class Fragment:
  name: str
  type_: TypeRef.T
  selection: list[Selection] = field(default_factory=list)

  # Variables as fragment arguments, not the values of those variables
  variables: list[VariableDefinition] = field(default_factory=list)

  @property
  def graphql(self):
    selection_str = "\n".join(
      [select.graphql(level=1) for select in self.selection]
    )
    return f"""fragment {self.name} on {TypeRef.root_type_name(self.type_)} {{\n{selection_str}\n}}"""

  @staticmethod
  def combine(frag: Fragment, other: Fragment) -> Fragment:
    pass

  @staticmethod
  def transform(frag: Fragment, f: Callable[[Selection], Selection]) -> Fragment:
    return Fragment(
      name=frag.name,
      type_=frag.type_,
      selection=list(frag.selection | map(f))
    )


@dataclass(frozen=True)
class Document:
  url: str
  query: Query
  fragments: list[Fragment] = field(default_factory=list)

  # A list of variable assignments. For non-repeating queries
  # the list would be of length 1 (i.e.: only one set of query variable
  # assignments)
  variables: dict[str, Any] = field(default_factory=dict)

  @property
  def graphql(self):
    return '\n'.join([self.query.graphql, *list(self.fragments | map(lambda frag: frag.graphql))])

  @staticmethod
  def mk_single_query(url: str, query: Query) -> Document:
    return Document(url, query)

  @staticmethod
  def combine(doc: Document, other: Document) -> Document:
    return Document(
      url=doc.url,
      query=doc.query.combine(other.query),
      fragments=union(
        doc.fragments,
        other.fragments,
        key=lambda frag: frag.name,
        combine=Fragment.combine
      )
    )

  @staticmethod
  def transform(
    doc: Document,
    query_f: Callable[[Query], Query] = identity,
    fragment_f: Callable[[Fragment], Fragment] = identity
  ) -> Document:
    return Document(
      url=doc.url,
      query=query_f(doc.query),
      fragments=list(doc.fragments | map(fragment_f)),
      variables=doc.variables
    )


@dataclass(frozen=True)
class DataRequest:
  documents: list[Document] = field(default_factory=list)

  @property
  def graphql(self):
    return '\n'.join(list(self.documents | map(lambda doc: doc.graphql)))

  @staticmethod
  def combine(req: DataRequest, other: DataRequest) -> None:
    return DataRequest(
      documents=union(
        req.documents,
        other.documents,
        key=lambda doc: doc.url,
        combine=Document.combine
      )
    )

  @staticmethod
  def transform(req: DataRequest, f: Callable[[Document], Document]) -> DataRequest:
    return DataRequest(
      documents=list(req.documents | map(f))
    )

  @staticmethod
  def single_query(url: str, query: Query) -> DataRequest:
    return DataRequest([
      Document(url, query)
    ])

  @staticmethod
  def single_document(doc: Document) -> DataRequest:
    return DataRequest([doc])

  @staticmethod
  def add_documents(self: DataRequest, docs: Document | list[Document]) -> DataRequest:
    return DataRequest(list([self.documents, docs] | traverse))


# ================================================================
# Utility functions
# ================================================================
def input_value_of_argument(
  schema: SchemaMeta,
  argmeta: TypeMeta.ArgumentMeta,
  value: Any
) -> InputValue:
  def fmt_value(type_ref: TypeRef.T, value: Any, non_null=False):
    match (type_ref, schema.type_map[TypeRef.root_type_name(type_ref)], value):
      # Only allow Null values when non_null=True
      case (_, _, None):
        if not non_null:
          return InputValue.Null()
        else:
          raise TypeError(f"Argument {argmeta.name} cannot be None!")

      # If type is non_null, recurse with non_null=True
      case (TypeRef.NonNull(t), _, _):
        return fmt_value(t, value, non_null=True)

      case (TypeRef.Named("ID"), _, str()):
        return InputValue.String(value)

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
        return InputValue.List([fmt_value(t, val, non_null) for val in value])

      case (TypeRef.Named(), TypeMeta.InputObjectMeta() as input_object, dict()):
        return InputValue.Object({key: fmt_value(input_object.type_of_input_field(key), val, non_null) for key, val in value.items()})

      case (value, typ, non_null):
        raise TypeError(f"mk_input_value({value}, {typ}, {non_null})")

  return fmt_value(argmeta.type_, value)


def arguments_of_field_args(
  schema: SchemaMeta,
  field: TypeMeta.FieldMeta,
  args: Optional[dict[str, Any]]
) -> list[Argument]:
  if args is None:
    args = {}

  def f(arg_meta: TypeMeta.ArgumentMeta) -> Optional[Argument]:
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
    case TypeMeta.FieldMeta() as field:
      args = [f(arg_meta) for arg_meta in field.arguments]
      return list(filter(lambda arg: arg is not None, args))
    case _:
      raise TypeError(f"arguments_of_field_args: TypeMeta {field.name} is not of type FieldMeta")