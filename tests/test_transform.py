import unittest

from subgrounds.query import Query, Selection
from subgrounds.transform import transform_selection

class TestQueryTransform(unittest.TestCase):
  def test_transform_selection1(self):
    expected = Query([
      Selection('swaps', None, None, [
        Selection('amount0In', None, None, None),
        Selection('amount0Out', None, None, None),
        Selection('amount1In', None, None, None),
        Selection('amount1Out', None, None, None),
      ])
    ])

    query = Query([
      Selection('swaps', None, None, [
        Selection('price1', None, None, None),
      ])
    ])

    replacement = [
      Selection('amount0In', None, None, None),
      Selection('amount0Out', None, None, None),
      Selection('amount1In', None, None, None),
      Selection('amount1Out', None, None, None),
    ]

    new_query = transform_selection(query, None, 'price1', replacement)

    self.assertEqual(new_query, expected)