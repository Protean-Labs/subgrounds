import unittest
from subgrounds.schema import TypeMeta, TypeRef

from subgrounds.subgraph import FieldPath, Subgraph

from tests.utils import schema


class TestQueryString(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def test_query_building_1(self):
    expected = FieldPath(
      self.subgraph,
      TypeRef.Named('Query'),
      TypeRef.Named('String'),
      [
        (
          {
            'first': 100,
            'where': {
              'reserveUSD_lt': 10
            },
            'orderBy': 'reserveUSD',
            'orderDirection': 'desc'
          },
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
        ),
        (
          None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )

    pairs = self.subgraph.Query.pairs(
      first=100,
      where={'reserveUSD_lt': 10},
      orderBy='reserveUSD',
      orderDirection='desc'
    )
    query = pairs.id

    FieldPath.test_mode = True
    self.assertEqual(query, expected)

  def test_query_building_2(self):
    expected = [
      FieldPath(
        self.subgraph,
        TypeRef.Named('Query'),
        TypeRef.Named('String'),
        [
          (
            {
              'first': 100,
              'where': {
                'reserveUSD_lt': 10
              },
              'orderBy': 'reserveUSD',
              'orderDirection': 'desc'
            },
            TypeMeta.FieldMeta('pairs', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
              TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
              TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
            ], TypeRef.non_null_list('Pair')),
          ),
          (
            None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
          )
        ]
      ),
      FieldPath(
        self.subgraph,
        TypeRef.Named('Query'),
        TypeRef.Named('BigDecimal'),
        [
          (
            {
              'first': 100,
              'where': {
                'reserveUSD_lt': 10
              },
              'orderBy': 'reserveUSD',
              'orderDirection': 'desc'
            },
            TypeMeta.FieldMeta('pairs', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
              TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
              TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
            ], TypeRef.non_null_list('Pair')),
          ),
          (
            None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal'))
          )
        ]
      )
    ]

    query = self.subgraph.Query.pairs(
      first=100,
      where={'reserveUSD_lt': 10},
      orderBy='reserveUSD',
      orderDirection='desc',
      selection=[
        self.subgraph.Pair.id,
        self.subgraph.Pair.reserveUSD
      ]
    )

    FieldPath.test_mode = True
    self.assertEqual(query, expected)

  def test_query_building_3(self):
    expected = [
      FieldPath(
        self.subgraph,
        TypeRef.Named('Query'),
        TypeRef.Named('String'),
        [
          (
            {
              'first': 100,
              'where': {
                'reserveUSD_lt': 10
              },
              'orderBy': 'reserveUSD',
              'orderDirection': 'desc'
            },
            TypeMeta.FieldMeta('pairs', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
              TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
              TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
            ], TypeRef.non_null_list('Pair')),
          ),
          (
            None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
          )
        ]
      ),
      FieldPath(
        self.subgraph,
        TypeRef.Named('Query'),
        TypeRef.Named('BigDecimal'),
        [
          (
            {
              'first': 100,
              'where': {
                'reserveUSD_lt': 10
              },
              'orderBy': 'reserveUSD',
              'orderDirection': 'desc'
            },
            TypeMeta.FieldMeta('pairs', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
              TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
              TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
            ], TypeRef.non_null_list('Pair')),
          ),
          (
            None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal'))
          )
        ]
      )
    ]

    pairs = self.subgraph.Query.pairs(
      first=100,
      where={'reserveUSD_lt': 10},
      orderBy='reserveUSD',
      orderDirection='desc',
    )

    query = [
      pairs.id,
      pairs.reserveUSD
    ]

    FieldPath.test_mode = True
    self.assertEqual(query, expected)