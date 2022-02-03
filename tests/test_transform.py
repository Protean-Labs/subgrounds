import unittest

from subgrounds.query import Argument, DataRequest, Document, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import Subgraph
from subgrounds.subgrounds import Subgrounds
from subgrounds.transform import LocalSyntheticField, PaginationTransform, SplitTransform, TypeTransform
from tests.utils import schema


class TestQueryTransform(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', self.schema)

  def test_roundtrip1(self):
    expected = [{
      'swaps': [{
        'amount0In': 0.25,
        'amount0Out': 0.0,
        'amount1In': 0.0,
        'amount1Out': 89820.904371079570860909
      }]
    }]

    transform = TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))

    req = DataRequest(documents=[
      Document(
        'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2',
        Query(None, [
          Selection(
            TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
            None,
            [
              Argument('first', InputValue.Int(1)),
              Argument('orderBy', InputValue.Enum('timestamp')),
              Argument('orderDirection', InputValue.Enum('desc')),
              Argument('where', InputValue.Object({
                'timestamp_lt': InputValue.Int(1638554700)
              }))
            ],
            [
              Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
              Selection(TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
              Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
              Selection(TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
            ]
          )
        ])
      )
    ])

    self.subgraph.transforms = [transform]
    app = Subgrounds(
      global_transforms=[],
      subgraphs={'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2': self.subgraph}
    )
    data = app.execute(req)

    self.assertEqual(data, expected)

  def test_roundtrip2(self):
    expected = [{
      'swaps': [{
        'price0': 359283.61748431827,
        'amount0In': 0.25,
        'amount0Out': 0.0,
        'amount1In': 0.0,
        'amount1Out': 89820.904371079570860909
      }]
    }]

    subgraph_transforms = [
      LocalSyntheticField(
        subgraph=self.subgraph,
        fmeta=TypeMeta.FieldMeta('price0', '', [], TypeRef.non_null('Float')),
        type_=TypeMeta.ObjectMeta('Swap', '', fields=[
          TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
          TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
          TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
          TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
          TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
          TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
        ]),
        fpath_selection=Selection(TypeMeta.FieldMeta('price0', '', [], TypeRef.Named('Float')), None, None, None),
        f=lambda in0, out0, in1, out1: abs(in1 - out1) / abs(in0 - out0),
        default=0.0,
        args=[
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        ]
      ),
      TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))
    ]

    query = Query(None, [
      Selection(
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
        None,
        [
          Argument('first', InputValue.Int(1)),
          Argument('orderBy', InputValue.Enum('timestamp')),
          Argument('orderDirection', InputValue.Enum('desc')),
          Argument('where', InputValue.Object({
            'timestamp_lt': InputValue.Int(1638554700)
          }))
        ],
        [
          Selection(TypeMeta.FieldMeta('price0', '', [], TypeRef.Named('Float')), None, None, None)
        ]
      )
    ])

    req = DataRequest(documents=[
      Document(url='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', query=query)
    ])

    self.subgraph.transforms = subgraph_transforms
    app = Subgrounds(
      global_transforms=[],
      subgraphs={'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2': self.subgraph}
    )
    data = app.execute(req)

    self.assertEqual(data, expected)

  def test_roundtrip_3(self):
    expected = [{
      'x5847f08709be4c59': [
        {
          'amount': 52627231563,
          'reserve': {
            'decimals': 6
          },
          'adjusted_amount': 52627.231563
        },
        {
          'amount': 9000000000,
          'reserve': {
            'decimals': 6
          },
          'adjusted_amount': 9000.0
        },
      ]
    }]

    sg = Subgrounds()
    aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')
    aaveV2.Borrow.adjusted_amount = aaveV2.Borrow.amount / 10 ** aaveV2.Borrow.reserve.decimals

    borrows = aaveV2.Query.borrows(
      orderBy=aaveV2.Borrow.timestamp,
      orderDirection='desc',
      first=2,
      where=[
        aaveV2.Borrow.timestamp < 1642020500
      ]
    )

    req = sg.mk_request([
      borrows.adjusted_amount
    ])

    data = sg.execute(req)

    self.assertEqual(data, expected)

  def test_roundtrip_4(self):
    expected = [{
      'x5847f08709be4c59': [
        {
          'amount': 52627231563,
          'reserve': {
            'decimals': 6,
            'symbol': 'USDC'
          },
          'adjusted_amount': 52627.231563
        },
        {
          'amount': 9000000000,
          'reserve': {
            'decimals': 6,
            'symbol': 'USDC'
          },
          'adjusted_amount': 9000.0
        },
      ]
    }]

    sg = Subgrounds()
    aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')
    aaveV2.Borrow.adjusted_amount = aaveV2.Borrow.amount / 10 ** aaveV2.Borrow.reserve.decimals

    borrows = aaveV2.Query.borrows(
      orderBy=aaveV2.Borrow.timestamp,
      orderDirection='desc',
      first=2,
      where=[
        aaveV2.Borrow.timestamp < 1642020500
      ]
    )

    req = sg.mk_request([
      borrows.reserve.symbol,
      borrows.adjusted_amount
    ])

    data = sg.execute(req)

    self.assertEqual(data, expected)

  def test_split_transform_1(self):
    expected = DataRequest([
      Document('abc', Query(None, [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ])),
      Document('abc', Query(None, [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ]))
    ])

    req = DataRequest([
      Document('abc', Query(None, [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ]))
    ])

    transform = SplitTransform(
      Query(None, [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ])
    )

    self.assertEqual(transform.transform_request(req), expected)

  def test_split_transform_2(self):
    expected = [
      {
        'pair': {
          'token0': {
            'id': '0x123',
            'name': 'ABC Token',
            'symbol': 'ABC'
          }
        }
      }
    ]

    data = [
      {
        'pair': {
          'token0': {
            'id': '0x123'
          }
        }
      },
      {
        'pair': {
          'token0': {
            'id': '0x123',
            'name': 'ABC Token',
            'symbol': 'ABC'
          }
        }
      }
    ]

    req = DataRequest([
      Document('abc', Query(None, [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ]))
    ])

    transform = SplitTransform(
      Query(None, [
        Selection(TypeMeta.FieldMeta('pair', '', [], TypeRef.non_null_list('Pair')), None, [], [
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None, [], [
            Selection(TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')), None, [], []),
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None, [], []),
          ])
        ])
      ])
    )

    self.assertEqual(transform.transform_response(req, data), expected)

  def test_pagination_transform_1(self):
    fmeta = TypeMeta.FieldMeta('swaps', '', [
      TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
      TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
    ], TypeRef.non_null_list('Swap'))

    expected = DataRequest([
      Document(
        'abc',
        Query(
          None,
          [
            Selection(fmeta, None, [Argument('first', InputValue.Variable('first')), Argument('skip', InputValue.Variable('skip'))], [
              Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            ])
          ],
          [
            VariableDefinition('first', TypeRef.non_null('Int')),
            VariableDefinition('skip', TypeRef.non_null('Int'))
          ]
        ),
        [],
        [
          {'first': 10, 'skip': 0},
          {'first': 10, 'skip': 10},
          {'first': 4, 'skip': 20}
        ]
      )
    ])

    req = DataRequest([
      Document(
        'abc',
        Query(
          None,
          [
            Selection(fmeta, None, [Argument('first', InputValue.Int(24))], [
              Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            ])
          ]
        )
      )
    ])

    transform = PaginationTransform(page_size=10)

    self.assertEqual(transform.transform_request(req), expected)

  def test_pagination_transform_2(self):
    expected = [{
      "swaps": [
        {"id": "0xe92c06b11abc6dc98e3baa012acd332094ec0103bfd7643faea331f7e7be1542-0"},
        {"id": "0xdfdcb69bdf08df1c53f1ca40091d6d13c58877b354a6d70e58273d3a8aa3c9f5-0"},
        {"id": "0xb28fb5435a39f843595f1614e9b9d965232ecaafa9ad7184ad83de211a37ee5c-0"},
        {"id": "0x96b6bf236e0fafb4456d547c6bbda8ca69cfa198732b3d26dfac12567eb37f55-0"},
        {"id": "0x89bbe3992ae54415e149634222fc96d95545f7f648c271c5d0c570645013f300-0"},
        {"id": "0x7dd07c5b1d8d3850800b08849e9a8d8d8eb8e03bb00e95ccb680d0c71060b68f-1"},
        {"id": "0x7dd07c5b1d8d3850800b08849e9a8d8d8eb8e03bb00e95ccb680d0c71060b68f-0"},
        {"id": "0x7154df8c4738a41ad27ad99e656c6a8e91f6be58ef5d98658654077102921156-0"},
        {"id": "0x6290405ba20c942672bc5b16fde6df2d3df82db9bf90db92959bec67abea00c8-0"},
        {"id": "0x3ec92b6c3a1015890c7eb849ecc80253bde41061fc9e7ec35e9b392622af54bb-0"},
        {"id": "0xf81da2f829ad610d4fb5ac0d8ef8ebaa57d58e96b513d53d8a81fabc007817cd-1"},
        {"id": "0xf81da2f829ad610d4fb5ac0d8ef8ebaa57d58e96b513d53d8a81fabc007817cd-0"},
        {"id": "0xdf0d79adfe4c1d1189c2f92749a3d11f0d97a66e97349bd7e58a8811067658ce-0"},
        {"id": "0x664d2a9460f83484b6a1c2b1679b770c8e2df3a0a10b78877113139a010b6edf-0"},
        {"id": "0x07ceae4bd995d185c92df36641ef0f168413605296b0772347f8dce6587ce267-0"},
        {"id": "0xff6d8cfa356d5916854b115095b3911700000e15b65d1a46134ebf0a5a42c031-0"},
        {"id": "0xcd33d2976f8ad380e76b166b32e4f40b1591c1880c06f5a3322071cac7d994f6-0"},
        {"id": "0xc4d888e20298a0129c6a15661485ae1ff5aeec97e20a5c98ca646d8f56c077b0-0"},
        {"id": "0xc13d3827ed2e5aa00baf23bee9cc5886ffbc96b484db840304b95b286c809a9c-1"},
        {"id": "0xc13d3827ed2e5aa00baf23bee9cc5886ffbc96b484db840304b95b286c809a9c-0"}
      ]
    }]

    fmeta = TypeMeta.FieldMeta('swaps', '', [
      TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
      TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
    ], TypeRef.non_null_list('Swap'))

    req = DataRequest([
      Document(
        'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2',
        Query(
          None,
          [
            Selection(fmeta, None, [
              Argument('first', InputValue.Int(20)),
              Argument('orderBy', InputValue.Enum('timestamp')),
              Argument('orderDirection', InputValue.Enum('desc')),
              Argument('where', InputValue.Object({'timestamp_lt': InputValue.Int(1640809000)}))
            ], [
              Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None, [], []),
            ])
          ]
        )
      )
    ])

    app = Subgrounds(
      global_transforms=[PaginationTransform(page_size=10)],
      subgraphs={'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2': self.subgraph}
    )
    data = app.execute(req)
    # data = chain_transforms([], req)
    self.assertEqual(data, expected)