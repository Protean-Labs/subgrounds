from itertools import filterfalse
from typing import Any, Callable, Optional, Tuple, TypeVar


def flatten(t):
  return [item for sublist in t for item in sublist]


def identity(x):
  return x


T = TypeVar('T')
U = TypeVar('U')


def fst(tup: Tuple[T, U]) -> T:
  return tup[0]


def snd(tup: Tuple[T, U]) -> U:
  return tup[1]


def intersection(
  l1: list[T],
  l2: list[T],
  key: Callable[[T], Any] = identity,
  combine: Callable[[T, T], T] = lambda x, _: x
) -> list[T]:
  l1_keys, l2_keys = list(map(key, l1)), list(map(key, l2))
  l2_in_l1 = list(filter(lambda x: key(x) in l1_keys, l2))
  l1_in_l2 = list(filter(lambda x: key(x) in l2_keys, l1))
  l2_in_l1.sort(key=key)
  l1_in_l2.sort(key=key)

  return list(map(lambda tup: combine(tup[0], tup[1]), zip(l1_in_l2, l2_in_l1)))


def rel_complement(
  l1: list[T],
  l2: list[T],
  key: Callable[[T], Any] = identity
) -> list[T]:
  l2_keys = list(map(key, l2))
  return list(filterfalse(lambda x: key(x) in l2_keys, l1))


def sym_diff(
  l1: list[T],
  l2: list[T],
  key: Callable[[T], Any] = identity
) -> list[T]:
  return rel_complement(l1, l2, key) + rel_complement(l2, l1, key)


def union(
  l1: list[T],
  l2: list[T],
  key: Callable[[T], Any] = identity,
  combine: Callable[[T, T], T] = lambda x, _: x
) -> list[T]:
  return rel_complement(l1, l2, key) + intersection(l1, l2, key, combine) + rel_complement(l2, l1, key)


def filter_none(l: list[Optional[T]]) -> list[T]:
  return list(filter(None, l))
