import unittest
from dataclasses import dataclass

from subgrounds.utils import intersection, rel_complement, union


class TestSet(unittest.TestCase):
  def test_rel_complement_1(self):
    l1 = [1, 2, 3]
    l2 = [3, 4, 5]
    self.assertEqual(rel_complement(l1, l2), [1, 2])

  def test_rel_complement_2(self):
    l1 = [3, 4]
    l2 = [3, 4, 5]
    self.assertEqual(rel_complement(l1, l2), [])

  def test_rel_complement_3(self):
    @dataclass
    class X:
      id: int
      txt: str

    l1 = [X(1, 'hello'), X(2, 'bob'), X(6, 'world!')]
    l2 = [X(5, 'abcd'), X(6, 'message')]

    self.assertEqual(rel_complement(l1, l2, key=lambda x: x.id), [X(1, 'hello'), X(2, 'bob')])

  def test_intersection_1(self):
    l1 = [1, 2, 3]
    l2 = [3, 4, 5]
    self.assertEqual(intersection(l1, l2), [3])

  def test_intersection_2(self):
    l1 = [1, 2, 3]
    l2 = [3, 4, 5]
    self.assertEqual(intersection(l1, l2, combine=lambda tup: tup[0] + tup[1]), [6])

  def test_intersection_3(self):
    l1 = [1, 2]
    l2 = [3, 4, 5]
    self.assertEqual(intersection(l1, l2), [])

  def test_intersection_4(self):
    @dataclass
    class X:
      id: int
      txt: str

    l1 = [X(1, 'hello'), X(2, 'bob'), X(6, 'world!')]
    l2 = [X(5, 'abcd'), X(6, 'message')]

    self.assertEqual(intersection(l1, l2, key=lambda x: x.id, combine=lambda tup: X(tup[0].id, f'{tup[0].txt} {tup[1].txt}')), [X(6, 'world! message')])

  def test_union_1(self):
    l1 = [1, 2, 3]
    l2 = [3, 4, 5]
    self.assertEqual(union(l1, l2), [1, 2, 3, 4, 5])

  def test_union_2(self):
    l1 = [1, 2, 3]
    l2 = [3, 4, 5]
    self.assertEqual(union(l1, l2, combine=lambda tup: tup[0] + tup[1]), [1, 2, 6, 4, 5])

  def test_union_3(self):
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

    self.assertEqual(union(l1, l2, key=lambda x: x.id, combine=lambda tup: X(tup[0].id, f'{tup[0].txt} {tup[1].txt}')), expected)
