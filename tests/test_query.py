import pytest

from subgrounds.query import (Argument, InputValue, Query, Selection,
                              VariableDefinition)
from subgrounds.schema import TypeMeta, TypeRef

# ================================================================
# Query printing tests
# ================================================================

@pytest.mark.parametrize("test_input, expected", [
  (
    Query(None, [
      Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
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
    ]),
    """query {
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
  ),
  (
    Query(
      None,
      [
        Selection(
          fmeta=TypeMeta.FieldMeta('token', '', [], TypeRef.non_null_list('Token')),
          alias=None,
          arguments=[
            Argument('id', InputValue.Variable('tokenId'))
          ],
          selection=[
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ]
        )
      ],
      [
        VariableDefinition('tokenId', TypeRef.non_null('String'))
      ]
    ),
    """query($tokenId: String!) {
  token(id: $tokenId) {
    id
    name
    symbol
  }
}"""
  )
])
def test_graphql_compilation(test_input: Query, expected: str):
  assert test_input.graphql == expected


# ================================================================
# Selection class tests
# ================================================================


@pytest.mark.parametrize("selection1, selection2, expected", [
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ])
  ),
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
      Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
      Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])
  ),
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    [
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    ],
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    ])
  )
])
def test_selection_add(selection1, selection2, expected):
  assert Selection.add(selection1, selection2) == expected


@pytest.mark.parametrize("selection1, selection2, expected", [
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ])
  ),
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
      Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
      Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])
  ),
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], []),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [])
  )
])
def test_selection_remove(selection1, selection2, expected):
  assert Selection.remove(selection1, selection2) == expected


@pytest.mark.parametrize("selection1, selection2, expected", [
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    True
  ),
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    False
  ),
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [
      Argument('first', InputValue.Int(100)),
    ], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    True
  ),
  (
    Selection(fmeta=TypeMeta.FieldMeta(name='log', description='', arguments=[], type_=TypeRef.Named(name_='String')), alias=None, arguments=[], selection=[]),
    Selection(fmeta=TypeMeta.FieldMeta(name='log', description='', arguments=[], type_=TypeRef.Named(name_='String')), alias=None, arguments=[], selection=[]),
    True
  )
])
def test_selection_contains(selection1, selection2, expected):
  assert Selection.contains(selection1, selection2) == expected


@pytest.mark.parametrize("selection1, selection2, expected", [
  (
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ]),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
    ])
  ),
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [])
    ]),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ])
  )
])
def test_selection_select(selection1, selection2, expected):
  assert Selection.select(selection1, selection2) == expected


@pytest.mark.parametrize("selections, expected", [
  (
    [
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ]),
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ]),
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ])
    ],
    [
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
          Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ])
    ]
  ),
  (
    [
      Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
      ]),
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ]),
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ]),
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ])
    ],
    [
      Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
      ]),
      Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
          Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
          Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
        ])
      ])
    ]
  )
])
def test_selection_merge(selections, expected):
  assert Selection.merge(selections) == expected


@pytest.mark.parametrize("selection, expected", [
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    True
  ),
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.non_null_list('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    True
  ),
  (
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.Named('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    False
  )
])
def test_selection_contains_list(selection, expected):
  assert Selection.contains_list(selection) == expected


# ================================================================
# Query class tests
# ================================================================

@pytest.mark.parametrize("query, other, expected", [
  (
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    ),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], [])
    ]),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )
  ),
  (
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ],
      []
    ),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    Query(
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
  ),
  (
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    ),
    [
      Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
        Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
      ])
    ],
    Query(
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
  ),
])
def test_query_add(query, other, expected):
  assert Query.add(query, other) == expected


@pytest.mark.parametrize("query, other, expected", [
  (
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
        ])
      ],
      []
    ),
    Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('Int')), None, [], []),
    ]),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )
  ),
  (
    Query(
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
    ),
    Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
      Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
        Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
        Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
      ])
    ]),
    Query(
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
  )
])
def test_query_remove(query, other, expected):
  assert Query.remove(query, other) == expected


@pytest.mark.parametrize("query, other, expected", [
  (
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    ),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    ),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, [], [
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('Float')), None, [], []),
        ])
      ],
      []
    )
  ),
  (
    Query(
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
    ),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [])
        ])
      ],
      []
    ),
    Query(
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
  )
])
def test_query_select(query, other, expected):
  assert Query.select(query, other) == expected


@pytest.mark.parametrize("query, other, expected", [
  (
    Query(
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
    ),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [])
        ])
      ],
      []
    ),
    True
  ),
  (
    Query(
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
    ),
    Query(
      None,
      [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ],
      []
    ),
    True
  )
])
def test_query_contains(query, other, expected):
  assert Query.contains(query, other) == expected
