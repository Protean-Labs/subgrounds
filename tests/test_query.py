import unittest

from subgrounds.query import Argument, Query, Selection, InputValue
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import Subgraph, SyntheticField

from tests.utils import schema


class TestQueryString(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_graphql_string_1(self):
    expected = """query {
  pairs(first: 100, where: {reserveUSD_lt: "10.0"}, orderBy: reserveUSD, orderDirection: desc) {
    id
    token0 {
      name
      symbol
    }
    token1 {
      name
      symbol
    }
  }
}"""

    query = Query(None, [
      Selection(
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Int(100)),
          Argument('where', InputValue.Object({'reserveUSD_lt': InputValue.String('10.0')})),
          Argument('orderBy', InputValue.Enum('reserveUSD')),
          Argument('orderDirection', InputValue.Enum('desc'))
        ],
        selection=[
          Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), selection=[
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String'))),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')))
          ]),
          Selection(TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')), selection=[
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String'))),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')))
          ])
        ]
      )
    ])

    self.assertEqual(query.graphql_string, expected)

  def test_graphql_string_2(self):
    expected = """query {
  pairs(first: 100, where: {reserveUSD_lt: "10.0"}, orderBy: reserveUSD, orderDirection: desc) {
    id
    token0 {
      name
      symbol
    }
    token1 {
      name
      symbol
    }
  }
}"""

    pairs = self.subgraph.Query.pairs(
      first=100,
      where=[
        self.subgraph.Pair.reserveUSD < 10
      ],
      orderBy=self.subgraph.Pair.reserveUSD,
      orderDirection='desc'
    )

    query = self.subgraph.mk_query([
      pairs.id,
      pairs.token0.name,
      pairs.token0.symbol,
      pairs.token1.name,
      pairs.token1.symbol,
    ])

    self.assertEqual(query.graphql_string, expected)
