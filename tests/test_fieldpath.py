import unittest
from subgrounds.query import Argument, DataRequest, Document, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import TypeMeta, TypeRef

from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.subgrounds import mk_request

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


class TestQueryString(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph1 = Subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', self.schema)
    self.subgraph2 = Subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v1', self.schema)

  def test_mk_request_1(self):
    expected = DataRequest.single_query('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', 
      Query(selection=[
        Selection(
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
          arguments=[
            Argument("first", InputValue.Int(10))
          ],
          selection=[
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
            Selection(TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')))
          ]
        )
      ])
    )
    
    pairs = self.subgraph1.Query.pairs(first=10)

    req = mk_request([
      pairs.id,
      pairs.reserveUSD
    ])

    self.assertEqual(req, expected)

  def test_mk_request_2(self):
    expected = DataRequest(documents=[
      Document(
        'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v1', 
        Query(selection=[
          Selection(
            TypeMeta.FieldMeta('pairs', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
              TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
              TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
            ], TypeRef.non_null_list('Pair')),
            arguments=[
              Argument("first", InputValue.Int(10))
            ],
            selection=[
              Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
              Selection(TypeMeta.FieldMeta('token0Id', '', [], TypeRef.Named('String')))
            ]
          )
        ])
      ),
      Document(
        'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', 
        Query(selection=[
          Selection(
            TypeMeta.FieldMeta('pairs', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
              TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
              TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
            ], TypeRef.non_null_list('Pair')),
            arguments=[
              Argument("first", InputValue.Int(10))
            ],
            selection=[
              Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
              Selection(TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')))
            ]
          )
        ])
      ),
    ])
    
    pairs = self.subgraph1.Query.pairs(first=10)

    Pair = self.subgraph2.Pair
    Pair.token0Id = Pair.token0.id

    self.subgraph2.Query.pairs(first=10).id,
    self.subgraph2.Query.pairs.token0Id

    req = mk_request([
      pairs.id,
      pairs.reserveUSD,
      self.subgraph2.Query.pairs(first=10).id,
      self.subgraph2.Query.pairs.token0Id
    ])

    self.assertEqual(req, expected)