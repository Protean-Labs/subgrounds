import unittest

from subgrounds.query import Argument, Enum, Int, Object, Query, Selection, String
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

    query = Query([
      Selection(
        "pairs",
        arguments=[
          Argument('first', Int(100)),
          Argument('where', Object({'reserveUSD_lt': String('10.0')})),
          Argument('orderBy', Enum('reserveUSD')),
          Argument('orderDirection', Enum('desc'))
        ],
        selection=[
          Selection("id"),
          Selection("token0", selection=[
            Selection("name"),
            Selection("symbol")
          ]),
          Selection("token1", selection=[
            Selection("name"),
            Selection("symbol")
          ])
        ]
      )
    ])

    self.assertEqual(query.graphql_string(), expected)

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

    self.assertEqual(query.graphql_string(), expected)


class TestQueryDataProcessing(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_query_data_process_1(self):
    expected = {'pairs': [
      {'id': "id1", 'token0': {'name': "token0name1", 'symbol': "token0symbol1"}, 'token1': {'name': "token1name1", 'symbol': "token1symbol1"}, 'reserveUSD': 1000.0},
      {'id': "id2", 'token0': {'name': "token0name2", 'symbol': "token0symbol2"}, 'token1': {'name': "token1name2", 'symbol': "token1symbol2"}, 'reserveUSD': 1000.0},
      {'id': "id3", 'token0': {'name': "token0name3", 'symbol': "token0symbol3"}, 'token1': {'name': "token1name3", 'symbol': "token1symbol3"}, 'reserveUSD': 1000.0},
      {'id': "id4", 'token0': {'name': "token0name4", 'symbol': "token0symbol4"}, 'token1': {'name': "token1name4", 'symbol': "token1symbol4"}, 'reserveUSD': 1000.0},
    ]}

    pairs = self.subgraph.Query.pairs(
      first=4,
      where=[
        self.subgraph.Pair.reserveUSD < 10
      ],
      orderBy=self.subgraph.Pair.reserveUSD,
      orderDirection='desc'
    )

    selections = [
      pairs.id,
      pairs.token0.name,
      pairs.token0.symbol,
      pairs.token1.name,
      pairs.token1.symbol,
      pairs.reserveUSD
    ]

    data = {'pairs': [
      {'id': "id1", 'token0': {'name': "token0name1", 'symbol': "token0symbol1"}, 'token1': {'name': "token1name1", 'symbol': "token1symbol1"}, 'reserveUSD': "1000.0"},
      {'id': "id2", 'token0': {'name': "token0name2", 'symbol': "token0symbol2"}, 'token1': {'name': "token1name2", 'symbol': "token1symbol2"}, 'reserveUSD': "1000.0"},
      {'id': "id3", 'token0': {'name': "token0name3", 'symbol': "token0symbol3"}, 'token1': {'name': "token1name3", 'symbol': "token1symbol3"}, 'reserveUSD': "1000.0"},
      {'id': "id4", 'token0': {'name': "token0name4", 'symbol': "token0symbol4"}, 'token1': {'name': "token1name4", 'symbol': "token1symbol4"}, 'reserveUSD': "1000.0"},
    ]}

    self.subgraph.process_data(selections, data)

    self.assertEqual(data, expected)

  def test_query_data_process_2(self):
    expected = {'pairs': [
      {'id': "id1", 'token0': {'symbol': "token0symbol1"}, 'token1': {'symbol': "token1symbol1"}, 'token0symbol': "token0symbol1", 'token1symbol': "token1symbol1"},
      {'id': "id2", 'token0': {'symbol': "token0symbol2"}, 'token1': {'symbol': "token1symbol2"}, 'token0symbol': "token0symbol2", 'token1symbol': "token1symbol2"},
      {'id': "id3", 'token0': {'symbol': "token0symbol3"}, 'token1': {'symbol': "token1symbol3"}, 'token0symbol': "token0symbol3", 'token1symbol': "token1symbol3"},
      {'id': "id4", 'token0': {'symbol': "token0symbol4"}, 'token1': {'symbol': "token1symbol4"}, 'token0symbol': "token0symbol4", 'token1symbol': "token1symbol4"},
    ]}

    pairs = self.subgraph.Query.pairs(
      first=4,
      where=[
        self.subgraph.Pair.reserveUSD < 10
      ],
      orderBy=self.subgraph.Pair.reserveUSD,
      orderDirection='desc'
    )

    self.subgraph.Pair.token0symbol = self.subgraph.Pair.token0.symbol
    self.subgraph.Pair.token1symbol = self.subgraph.Pair.token1.symbol

    selections = [
      pairs.id,
      pairs.token0symbol,
      pairs.token1symbol,
    ]

    data = {'pairs': [
      {'id': "id1", 'token0': {'symbol': "token0symbol1"}, 'token1': {'symbol': "token1symbol1"}},
      {'id': "id2", 'token0': {'symbol': "token0symbol2"}, 'token1': {'symbol': "token1symbol2"}},
      {'id': "id3", 'token0': {'symbol': "token0symbol3"}, 'token1': {'symbol': "token1symbol3"}},
      {'id': "id4", 'token0': {'symbol': "token0symbol4"}, 'token1': {'symbol': "token1symbol4"}},
    ]}

    self.subgraph.process_data(selections, data)

    self.assertEqual(data, expected)

  def test_query_data_process_3(self):
    expected = {'swaps': [
      {'timestamp': 1001, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0, 'price0': 0.5, 'price1': 2.0},
      {'timestamp': 1002, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0, 'price0': 0.5, 'price1': 2.0},
      {'timestamp': 1003, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0, 'price0': 0.5, 'price1': 2.0},
      {'timestamp': 1004, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0, 'price0': 0.5, 'price1': 2.0},
    ]}

    swaps = self.subgraph.Query.swaps(
      first=4,
      orderBy=self.subgraph.Swap.timestamp,
      orderDirection='desc'
    )

    Swap = self.subgraph.Swap
    Swap = self.subgraph.Swap
    Swap.price0 = abs(Swap.amount1In - Swap.amount1Out) / abs(Swap.amount0In - Swap.amount0Out)
    Swap.price1 = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

    selections = [
      swaps.timestamp,
      swaps.price0,
      swaps.price1
    ]

    data = {'swaps': [
      {'timestamp': 1001, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0},
      {'timestamp': 1002, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0},
      {'timestamp': 1003, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0},
      {'timestamp': 1004, 'amount0In': 10.0, 'amount0Out': 0.0, 'amount1In': 0.0, 'amount1Out': 5.0},
    ]}

    self.subgraph.process_data(selections, data)

    self.assertEqual(data, expected)
