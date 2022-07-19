from __future__ import annotations
from ast import Tuple
from dataclasses import dataclass, field
from functools import partial
from itertools import count
from pprint import pprint
from pipe import traverse, map
from typing import Any, Callable, Iterator, Literal, Optional

from subgrounds.pagination.preprocess import PaginationNode
from subgrounds.pagination.utils import PAGE_SIZE
from subgrounds.utils import extract_data


class StopPagination(Exception):
  def __init__(self, *args: object) -> None:
    super().__init__(*args)


@dataclass
class LegacyStrategy:
  cursor: list[Cursor]
  active_idx: int = 0

  @dataclass
  class Cursor:
    page_node: PaginationNode

    inner: list[LegacyStrategy.Cursor]
    inner_idx: int = 0

    filter_value: Any = None
    queried_entities: int = 0
    stop: bool = False
    page_count: int = 0
    keys: set[str] = field(default_factory=set)

    def __init__(self, page_node: PaginationNode) -> None:
      self.page_node = page_node
      self.inner = list(page_node.inner | map(LegacyStrategy.Cursor))
      self.reset()

    @property
    def is_leaf(self):
      return len(self.inner) == 0

    def update(self, data: dict) -> None:
      """ Moves ``self`` cursor forward according to previous response data ``data``
      Args:
        data (dict): Previous response data
      Raises:
        StopIteration: _description_
      """
      # Current node step
      index_field_data = list(extract_data([*self.page_node.key_path, self.page_node.filter_field], data) | traverse)
      num_entities = len(index_field_data)
      filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

      id_data = list(extract_data([*self.page_node.key_path, 'id'], data) | traverse)
      for key in id_data:
        if key not in self.keys:
          self.keys.add(key)

      self.page_count = self.page_count + 1
      self.queried_entities = len(self.keys)

      if filter_value:
        self.filter_value = filter_value

      if (
        (self.is_leaf and num_entities < PAGE_SIZE)
        or (not self.is_leaf and num_entities == 0)
        or (self.queried_entities == self.page_node.first_value)
      ):
        raise StopPagination

    def step(self, data: dict) -> None:
      """ Updates either ``self`` cursor or inner state machine depending on
      whether the inner state machine has reached its limit
      Args:
        data (dict): _description_
      """
      if self.is_leaf:
        self.update(data)
      else:
        try:
          self.inner[self.inner_idx].step(data)
        except StopPagination:
          if self.inner_idx < len(self.inner) - 1:
            self.inner_idx = self.inner_idx + 1
          else:
            self.update(data)
            self.inner_idx = 0

          self.inner[self.inner_idx].reset()

    def first_arg_value(self) -> int:
      if self.is_leaf:
        return (
          self.page_node.first_value - self.queried_entities
          if self.page_node.first_value - self.queried_entities < PAGE_SIZE
          else PAGE_SIZE
        )
      else:
        return 1

    def args(self) -> dict:
      """ Returns the pagination arguments for the current state of the state machine
      Returns:
          dict: _description_
      """
      args = {}
      args[f'first{self.page_node.node_idx}'] = self.first_arg_value()

      args[f'skip{self.page_node.node_idx}'] = self.page_node.skip_value if self.page_count == 0 else 0

      if self.filter_value is not None:
        args[f'lastOrderingValue{self.page_node.node_idx}'] = self.filter_value

      if self.is_leaf:
        return args
      else:
        inner_args = self.inner[self.inner_idx].args()
        return args | inner_args

    def reset(self):
      """ Reset state machine
      """
      self.inner_idx = 0
      self.filter_value = self.page_node.filter_value
      self.queried_entities = 0
      self.stop = False
      self.page_count = 0
      self.keys = set()

  def __init__(self, pagination_nodes: list[PaginationNode]) -> None:
    self.cursor = self.cursor = list(pagination_nodes | map(LegacyStrategy.Cursor))

  def step(self, page_data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    try:
      if page_data is not None:
        self.cursor[self.active_idx].step(page_data)

      return self.cursor[self.active_idx].args()
    except StopPagination:
      if self.active_idx < len(self.cursor) - 1:
        self.active_idx = self.active_idx + 1
        return self.cursor[self.active_idx].args()
      else:
        raise StopPagination


@dataclass
class GreedyStrategy:
  @dataclass
  class Cursor:
    # TODO: Add GreedyStrategy doc
    """ Class used to generate the pagination variables for a given tree of
    ``PaginationNode`` objects.

    Attributes:
      page_node: The ``PaginationNode`` object which this cursor is iterating
        through.
      inner: The cursors for nested ``PaginationNodes``, if any.
      inner_idx: The index of the inner ``PaginationNode`` through which this cursor
        iterating.
      filter_value: The previous page's index value used to query the next data page.
        Depends on ``page_node.filter_field``, e.g.: if ``page_node.filter_field``
        is ``timestamp_gt``, then ``filter_value`` will be the highest timestamp
        the entities returned in the previous data page.
      queried_entities: Counter keeping track of the total number of queried entities.
      stop: Flag indicating whether or not to stop the cursor.
      page_count: Counter keeping track of the total number data pages queried.
      keys: Set keeping track of the keys of all queried entities to avoid duplicates.
    """
    page_node: PaginationNode

    inner: list[GreedyStrategy.Cursor]
    inner_idx: int = 0

    filter_value: Any = None
    queried_entities: int = 0
    page_count: int = 0

    @staticmethod
    def from_pagination_node(page_node: PaginationNode) -> GreedyStrategy.Cursor:
      return GreedyStrategy.Cursor(
        page_node=page_node,
        filter_value=page_node.filter_value,
        inner=list(
          page_node.inner
          | map(GreedyStrategy.Cursor.from_pagination_node)
        )
      )

    @property
    def is_leaf(self) -> bool:
      return len(self.inner) == 0

    @property
    def active_cursor(self) -> Optional[GreedyStrategy.Cursor]:
      if len(self.inner) > 0:
        return self.inner[self.inner_idx]
      else:
        return None

    def iter(self, only_active: bool = False) -> Iterator[GreedyStrategy.Cursor]:
      yield self
      if only_active and len(self.inner) > 0:
        yield from self.inner[self.inner_idx].iter(only_active=True)
      else:
        for cursor in self.inner:
          yield from cursor.iter()

    def mapi(
      self,
      map_f: Callable[[int, GreedyStrategy.Cursor], GreedyStrategy.Cursor],
      priority: Literal['self'] | Literal['children'] = 'self',
      counter: Optional(count[int]) = None
    ) -> GreedyStrategy.Cursor:
      if counter is None:
        counter = count()

      match priority:
        case 'self':
          new_cursor = map_f(next(counter), self)

          return GreedyStrategy.Cursor(
            page_node=new_cursor.page_node,
            inner=list(new_cursor.inner | map(partial(GreedyStrategy.Cursor.mapi, map_f=map_f, counter=counter))),
            inner_idx=new_cursor.inner_idx,
            filter_value=new_cursor.filter_value,
            queried_entities=new_cursor.queried_entities,
            page_count=new_cursor.page_count,
          )

        case 'children':
          i = next(counter)

          new_cursor = GreedyStrategy.Cursor(
            page_node=self.page_node,
            inner=list(self.inner | map(partial(GreedyStrategy.Cursor.mapi, map_f=map_f, counter=counter))),
            inner_idx=self.inner_idx,
            filter_value=self.filter_value,
            queried_entities=self.queried_entities,
            page_count=self.page_count,
          )

          return map_f(i, new_cursor)

        case _:
          raise Exception(f"Cursor.mapi: invalid priority {priority}")

  cursor: list[Cursor]

  def __init__(self, pagination_nodes: list[PaginationNode]) -> None:
    self.cursor = list(pagination_nodes | map(GreedyStrategy.Cursor.from_pagination_node))

  def iter_cursors(self) -> Iterator[Cursor]:
    for cur in self.cursor:
      yield from cur.iter()

  @staticmethod
  def update_cursor(cursor: Cursor, data: dict[str, Any]) -> Cursor:
    index_field_data = list(extract_data([*cursor.page_node.key_path, cursor.page_node.filter_field], data) | traverse)
    num_entities = len(index_field_data)
    filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

    new_cursor = GreedyStrategy.Cursor(
      page_node=cursor.page_node,
      inner=cursor.inner,
      inner_idx=0,
      filter_value=filter_value,
      queried_entities=cursor.queried_entities + num_entities,
      page_count=cursor.page_count + 1,
    )

    if (
      new_cursor.queried_entities >= new_cursor.page_node.first_value
      or num_entities < PAGE_SIZE
    ):
      raise StopPagination

    return new_cursor

  def step(self, page_data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if page_data is not None:
      self.cursor = list(
        self.cursor
        | map(partial(GreedyStrategy.Cursor.mapi, map_f=lambda i, cursor: (
          GreedyStrategy.update_cursor(cursor, page_data)
          if i == 0 
          else cursor
        )))
      )

    args = {}
    for cur in self.iter_cursors():
      args[f'first{cur.page_node.node_idx}'] = (
        cur.page_node.first_value - cur.queried_entities
        if cur.page_node.first_value - cur.queried_entities < PAGE_SIZE
        else PAGE_SIZE
      )

      args[f'skip{cur.page_node.node_idx}'] = cur.page_node.skip_value if cur.page_count == 0 else 0

      if cur.filter_value is not None:
        args[f'lastOrderingValue{cur.page_node.node_idx}'] = cur.filter_value

    return args