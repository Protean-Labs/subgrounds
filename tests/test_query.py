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


class TestSelectionModification(unittest.TestCase):
  def test_add_selection_1(self):
    expected = Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ])

    og_selection = Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ])

    new_selection = Selection.add_selection(
      og_selection,
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], [])
    )

    self.assertEqual(new_selection, expected)

  def test_add_selection_2(self):
    expected = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])

    og_selection = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])

    new_selection = Selection.add_selection(
      og_selection,
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    )

    self.assertEqual(new_selection, expected)

  def test_add_selections_1(self):
    expected = Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    ])

    og_selection = Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ])

    new_selection = Selection.add_selections(
      og_selection,
      [
        Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
      ]
    )

    self.assertEqual(new_selection, expected)

  def test_remove_selection_1(self):
    expected = Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ])

    og_selection = Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    ])

    new_selection = Selection.remove_selection(
      og_selection,
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    )

    self.assertEqual(new_selection, expected)

  def test_remove_selection_2(self):
    expected = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])

    og_selection = Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])

    new_selection = Selection.remove_selection(
      og_selection,
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    )

    self.assertEqual(new_selection, expected)


class TestQueryModification(unittest.TestCase):
  def test_add_selection_1(self):
    expected = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )

    og_query = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )

    new_query = Query.add_selection(
      og_query,
      Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
        Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], [])
      ])
    )

    self.assertEqual(new_query, expected)

  def test_add_selection_2(self):
    expected = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ],
      []
    )

    og_query = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ],
      []
    )

    new_query = Query.add_selection(
      og_query,
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ])
    )

    self.assertEqual(new_query, expected)

  def test_add_selections_1(self):
    expected = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
        ])
      ],
      []
    )

    og_query = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )

    new_query = Query.add_selections(
      og_query,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
        ])
      ]
    )

    self.assertEqual(new_query, expected)

  def test_remove_selection_1(self):
    expected = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )

    og_query = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
        ])
      ],
      []
    )

    new_query = Query.remove_selection(
      og_query,
      Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
        Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
      ])
    )

    self.assertEqual(new_query, expected)

  def test_remove_selection_2(self):
    expected = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ],
      []
    )

    og_query = Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ],
      []
    )

    new_query = Query.remove_selection(
      og_query,
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ])
    )

    self.assertEqual(new_query, expected)