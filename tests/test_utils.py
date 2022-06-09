import operator
import unittest
from dataclasses import dataclass

import pytest

from subgrounds.utils import extract_data, flatten_dict, intersection, rel_complement, union


def test_rel_complement_1():
  l1 = [1, 2, 3]
  l2 = [3, 4, 5]
  assert rel_complement(l1, l2) == [1, 2]


def test_rel_complement_2():
  l1 = [3, 4]
  l2 = [3, 4, 5]
  assert rel_complement(l1, l2) == []


def test_rel_complement_3():
  @dataclass
  class X:
    id: int
    txt: str

  l1 = [X(1, 'hello'), X(2, 'bob'), X(6, 'world!')]
  l2 = [X(5, 'abcd'), X(6, 'message')]

  assert rel_complement(l1, l2, key=lambda x: x.id) == [X(1, 'hello'), X(2, 'bob')]


def test_intersection_1():
  l1 = [1, 2, 3]
  l2 = [3, 4, 5]
  assert intersection(l1, l2) == [3]


def test_intersection_2():
  l1 = [1, 2, 3]
  l2 = [3, 4, 5]
  assert intersection(l1, l2, combine=operator.add) == [6]


def test_intersection_3():
  l1 = [1, 2]
  l2 = [3, 4, 5]
  assert intersection(l1, l2) == []


def test_intersection_4():
  @dataclass
  class X:
    id: int
    txt: str

  l1 = [X(1, 'hello'), X(2, 'bob'), X(6, 'world!')]
  l2 = [X(5, 'abcd'), X(6, 'message')]

  assert intersection(l1, l2, key=lambda x: x.id, combine=lambda x, y: X(x.id, f'{x.txt} {y.txt}')) == [X(6, 'world! message')]


def test_union_1():
  l1 = [1, 2, 3]
  l2 = [3, 4, 5]
  assert union(l1, l2) == [1, 2, 3, 4, 5]


def test_union_2():
  l1 = [1, 2, 3]
  l2 = [3, 4, 5]
  assert union(l1, l2, combine=operator.add) == [1, 2, 6, 4, 5]


def test_union_3():
  @dataclass
  class X:
    id: int
    txt: str

  l1 = [X(1, 'hello'), X(2, 'bob'), X(6, 'world!')]
  l2 = [X(5, 'abcd'), X(6, 'message')]

  expected = [
    X(1, 'hello'),
    X(2, 'bob'),
    X(6, 'world! message'),
    X(5, 'abcd')
  ]

  assert union(l1, l2, key=lambda x: x.id, combine=lambda x, y: X(x.id, f'{x.txt} {y.txt}')) == expected


def test_extract_data_1():
  expected = [1, 2, 3]

  data = {
    'a': [
      {'b': {'c': 1}},
      {'b': {'c': 2}},
      {'b': {'c': 3}},
    ]
  }

  path = ['a', 'b', 'c']

  assert extract_data(path, data) == expected


def test_extract_data_2():
  expected = []

  data = {
    'a': []
  }

  path = ['a', 'b', 'c']

  assert extract_data(path, data) == expected


def test_extract_data_3():
  expected = []

  data = {
    'a': []
  }

  path = ['a']

  assert extract_data(path, data) == expected


def tests_flatten_dict_1():
  expected = {
    'x': 1,
    'a_b_c': 10,
    'd_e': 'hello',
    'd_f': 'world',
    'foo': True
  }

  data = {
    'x': 1,
    'a': {
      'b': {
        'c': 10
      }
    },
    'd': {
      'e': 'hello',
      'f': 'world'
    },
    'foo': True
  }

  assert flatten_dict(data) == expected
