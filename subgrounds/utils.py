from itertools import filterfalse
from typing import Any, Callable, Optional, Tuple, TypeVar

from pipe import map, where

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
  l1_keys, l2_keys = list(l1 | map(key)), list(l2 | map(key))
  l2_in_l1 = list(filter(lambda x: key(x) in l1_keys, l2))
  l1_in_l2 = list(filter(lambda x: key(x) in l2_keys, l1))
  l2_in_l1.sort(key=key)
  l1_in_l2.sort(key=key)

  return list(zip(l1_in_l2, l2_in_l1) | map(lambda tup: combine(tup[0], tup[1])))


def rel_complement(
  l1: list[T],
  l2: list[T],
  key: Callable[[T], Any] = identity
) -> list[T]:
  l2_keys = list(l2 | map(key))
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


def extract_data(keys: list[str], data: dict[str, Any] | list[dict[str, Any]]) -> list[Any] | Any:
  def f(keys: list[str], data: dict | list | Any):
    match keys:
      case []:
        return data
      case [name, *rest]:
        match data:
          case dict():
            return f(rest, data[name])
          case list():
            return list(data | map(lambda row: f(rest, row[name])))
          case None:
            return None
          case _:
            raise Exception(f"extract_data: unexpected state! path = {keys}, data = {data}")

  match data:
    case dict():
      return f(keys, data)
    case list():
      for doc_data in data:
        try:
          return f(keys, doc_data)
        except KeyError:
          continue
      raise Exception('extract_data: not found')
    case _:
      raise Exception('extract_data: data is not dict or list')
