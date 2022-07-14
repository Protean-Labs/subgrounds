from __future__ import annotations
from ast import Tuple
from dataclasses import dataclass, field
from functools import partial
from itertools import count
from pipe import traverse, map
from typing import Any, Callable, Iterator, Literal, Optional

from subgrounds.pagination.preprocess import PaginationNode
from subgrounds.pagination.utils import PAGE_SIZE
from subgrounds.utils import extract_data


# TODO: See how to decouple cursor state from strategy


# @dataclass
# class LegacyStrategy:
#   # TODO: Add LegacyStrategy doc
#   """ Class used to generate the pagination variables for a given tree of
#   ``PaginationNode`` objects.

#   Attributes:
#     page_node: The ``PaginationNode`` object which this cursor is iterating
#       through.
#     inner: The cursors for nested ``PaginationNodes``, if any.
#     inner_idx: The index of the inner ``PaginationNode`` through which this cursor
#       iterating.
#     filter_value: The previous page's index value used to query the next data page.
#       Depends on ``page_node.filter_field``, e.g.: if ``page_node.filter_field``
#       is ``timestamp_gt``, then ``filter_value`` will be the highest timestamp
#       the entities returned in the previous data page.
#     queried_entities: Counter keeping track of the total number of queried entities.
#     stop: Flag indicating whether or not to stop the cursor.
#     page_count: Counter keeping track of the total number data pages queried.
#     keys: Set keeping track of the keys of all queried entities to avoid duplicates.
#   """
#   page_node: PaginationNode

#   inner: list[LegacyStrategy]
#   inner_idx: int = 0

#   filter_value: Any = None
#   queried_entities: int = 0
#   stop: bool = False
#   page_count: int = 0
#   keys: set[str] = field(default_factory=set)

#   def __init__(self, page_node: PaginationNode) -> None:
#     self.page_node = page_node
#     self.inner = list(page_node.inner | map(LegacyStrategy))
#     self.reset()

#   @property
#   def is_leaf(self):
#     return len(self.inner) == 0

#   def update(self, data: dict) -> None:
#     """ Moves ``self`` cursor forward according to previous response data ``data``
#     Args:
#       data (dict): Previous response data
#     Raises:
#       StopIteration: _description_
#     """
#     # Current node step
#     index_field_data = list(extract_data([*self.page_node.key_path, self.page_node.filter_field], data) | traverse)
#     num_entities = len(index_field_data)
#     filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

#     id_data = list(extract_data([*self.page_node.key_path, 'id'], data) | traverse)
#     for key in id_data:
#       if key not in self.keys:
#         self.keys.add(key)

#     self.page_count = self.page_count + 1
#     self.queried_entities = len(self.keys)

#     if filter_value:
#       self.filter_value = filter_value

#     if (
#       (self.is_leaf and num_entities < PAGE_SIZE)
#       or (not self.is_leaf and num_entities == 0)
#       or (self.queried_entities == self.page_node.first_value)
#     ):
#       raise StopIteration(self)

#   def step(self, data: dict) -> None:
#     """ Updates either ``self`` cursor or inner state machine depending on
#     whether the inner state machine has reached its limit
#     Args:
#       data (dict): _description_
#     """
#     if self.is_leaf:
#       self.update(data)
#     else:
#       try:
#         self.inner[self.inner_idx].step(data)
#       except StopIteration:
#         if self.inner_idx < len(self.inner) - 1:
#           self.inner_idx = self.inner_idx + 1
#         else:
#           self.update(data)
#           self.inner_idx = 0

#         self.inner[self.inner_idx].reset()

#   def args(self) -> dict:
#     """ Returns the pagination arguments for the current state of the state machine
#     Returns:
#         dict: _description_
#     """
#     if self.is_leaf:
#       if self.filter_value is None:
#         return {
#           # `first`
#           f'first{self.page_node.node_idx}': self.page_node.first_value - self.queried_entities
#           if self.page_node.first_value - self.queried_entities < PAGE_SIZE
#           else PAGE_SIZE,

#           # `skip`
#           f'skip{self.page_node.node_idx}': self.page_node.skip_value if self.page_count == 0 else 0,
#         }
#       else:
#         return {
#           # `first`
#           f'first{self.page_node.node_idx}': self.page_node.first_value - self.queried_entities
#           if self.page_node.first_value - self.queried_entities < PAGE_SIZE
#           else PAGE_SIZE,

#           # `skip`
#           f'skip{self.page_node.node_idx}': self.page_node.skip_value if self.page_count == 0 else 0,

#           # `filter`
#           f'lastOrderingValue{self.page_node.node_idx}': self.filter_value
#         }
#     else:
#       if self.filter_value is None:
#         args = {
#           # `first`
#           f'first{self.page_node.node_idx}': 1,

#           # `skip`
#           f'skip{self.page_node.node_idx}': self.page_node.skip_value if self.page_count == 0 else 0,
#         }
#       else:
#         args = {
#           # `first`
#           f'first{self.page_node.node_idx}': 1,

#           # `skip`
#           f'skip{self.page_node.node_idx}': self.page_node.skip_value if self.page_count == 0 else 0,

#           # `filter`
#           f'lastOrderingValue{self.page_node.node_idx}': self.filter_value
#         }

#       inner_args = self.inner[self.inner_idx].args()
#       return args | inner_args

#   def reset(self):
#     """ Reset state machine
#     """
#     self.inner_idx = 0
#     self.filter_value = self.page_node.filter_value
#     self.queried_entities = 0
#     self.stop = False
#     self.page_count = 0
#     self.keys = set()


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

  inner: list[Cursor]
  inner_idx: int = 0

  filter_value: Any = None
  queried_entities: int = 0
  page_count: int = 0
  keys: set[str] = field(default_factory=set)

  @staticmethod
  def from_pagination_nodes(page_node: PaginationNode) -> Cursor:
    return Cursor(
      page_node=page_node,
      inner=list(
        page_node.inner
        | map(Cursor.from_pagination_nodes)
      )
    )

  @property
  def is_leaf(self) -> bool:
    return len(self.inner) == 0

  def iter(self) -> Iterator[Cursor]:
    yield self
    for cursor in self.inner:
      yield from cursor.iter()

  def mapi(
    self,
    map_f: Callable[[int, Cursor], Cursor],
    priority: Literal['self'] | Literal['children'] = 'self',
    counter: Optional(count[int]) = None
  ) -> Cursor:
    if counter is None:
      counter = count()

    match priority:
      case 'self':
        new_cursor = map_f(next(counter), self)

        # return Cursor(**(new_cursor.__dict__ | {
        #   'inner': list(new_cursor.inner | map(partial(Cursor.mapi, map_f=map_f, counter=counter)))
        # }))

        return Cursor(
          page_node=new_cursor.page_node,
          inner=list(new_cursor.inner | map(partial(Cursor.mapi, map_f=map_f, counter=counter))),
          inner_idx=new_cursor.inner_idx,
          filter_value=new_cursor.filter_value,
          queried_entities=new_cursor.queried_entities,
          page_count=new_cursor.page_count,
          keys=new_cursor.keys
        )

      case 'children':
        i = next(counter)

        # new_cursor = Cursor(**(new_cursor.__dict__ | {
        #   'inner': list(self.inner | map(partial(Cursor.mapi, map_f=map_f, counter=counter)))
        # }))
        new_cursor = Cursor(
          page_node=self.page_node,
          inner=list(self.inner | map(partial(Cursor.mapi, map_f=map_f, counter=counter))),
          inner_idx=self.inner_idx,
          filter_value=self.filter_value,
          queried_entities=self.queried_entities,
          page_count=self.page_count,
          keys=self.keys
        )

        return map_f(i, new_cursor)

      case _:
        raise Exception(f"Cursor.mapi: invalid priority {priority}")


def legacy_strategy(
  cursor: Cursor,
  data: Optional[dict[str, Any]] = None
) -> Tuple[Cursor, dict[str, Any]]:
  def update_cursor(cursor: Cursor, data: dict[str, Any]) -> Cursor:
    """ Moves  cursor forward according to previous response data ``data``
    Args:
      data (dict): Previous response data
    Raises:
      StopIteration: _description_
    """
    index_field_data = list(extract_data([*cursor.page_node.key_path, cursor.page_node.filter_field], data) | traverse)
    num_entities = len(index_field_data)
    filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

    new_cursor = Cursor(
      page_node=cursor.page_node,
      inner=cursor.inner,
      inner_idx=0,
      filter_value=filter_value,
      queried_entities=cursor.queried_entities + num_entities,
      page_count=cursor.page_count + 1,
    )

    if (
      (new_cursor.is_leaf and num_entities < PAGE_SIZE)
      or (not new_cursor.is_leaf and num_entities == 0)
      or (new_cursor.queried_entities == new_cursor.page_node.first_value)
    ):
      # raise StopIteration(new_cursor)
      raise StopIteration

    return new_cursor

  if data is not None:
    cursor = cursor.mapi(lambda i, cursor: update_cursor(cursor, data))

  args = {}
  for cur in cursor.iter():
    if cur.is_leaf:
      args[f'first{cur.page_node.node_idx}'] = (
        cur.page_node.first_value - cur.queried_entities
        if cur.page_node.first_value - cur.queried_entities < PAGE_SIZE
        else PAGE_SIZE
      )
    else:
      args[f'first{cur.page_node.node_idx}'] = 1

    args[f'skip{cur.page_node.node_idx}'] = cur.page_node.skip_value if cur.page_count == 0 else 0

    if cur.filter_value is not None:
      args[f'lastOrderingValue{cur.page_node.node_idx}'] = cur.filter_value

  return (cursor, args)


def greedy_strategy(
  cursor: Cursor,
  data: Optional[dict[str, Any]] = None
) -> Tuple[Cursor, dict[str, Any]]:
  def update_cursor(cursor: Cursor, data: dict[str, Any]) -> Cursor:
    index_field_data = list(extract_data([*cursor.page_node.key_path, cursor.page_node.filter_field], data) | traverse)
    num_entities = len(index_field_data)
    filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

    new_cursor = Cursor(
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
      # raise StopIteration(new_cursor)
      raise StopIteration

    return new_cursor

  
  new_cursor = (
    cursor.mapi(lambda i, cursor_: update_cursor(cursor_, data) if i == 0 else cursor_)
    if data is not None
    else cursor
  )

  args = {}
  for cur in new_cursor.iter():
    args[f'first{cur.page_node.node_idx}'] = (
      cur.page_node.first_value - cur.queried_entities
      if cur.page_node.first_value - cur.queried_entities < PAGE_SIZE
      else PAGE_SIZE
    )

    args[f'skip{cur.page_node.node_idx}'] = cur.page_node.skip_value if cur.page_count == 0 else 0

    if cur.filter_value is not None:
      args[f'lastOrderingValue{cur.page_node.node_idx}'] = cur.filter_value

  return (new_cursor, args)
