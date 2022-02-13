import unittest

from subgrounds.query import Argument, DataRequest, Document, InputValue, Query, Selection
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import Subgraph
from subgrounds.subgrounds import Subgrounds
from subgrounds.transform import LocalSyntheticField, SplitTransform, TypeTransform
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
        url='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2',
        query=Query(None, [
          Selection(
            fmeta=TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
            alias=None,
            arguments=[
              Argument('first', InputValue.Int(1)),
              Argument('orderBy', InputValue.Enum('timestamp')),
              Argument('orderDirection', InputValue.Enum('desc')),
              Argument('where', InputValue.Object({
                'timestamp_lt': InputValue.Int(1638554700)
              }))
            ],
            selection=[
              Selection(fmeta=TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal'))),
              Selection(fmeta=TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal'))),
              Selection(fmeta=TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal'))),
              Selection(fmeta=TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal'))),
            ]
          )
        ])
      )
    ])

    self.subgraph.transforms = [transform]
    sg = Subgrounds(
      global_transforms=[],
      subgraphs={'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2': self.subgraph}
    )
    data = sg.execute(req)

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
        fpath_selection=Selection(TypeMeta.FieldMeta('price0', '', [], TypeRef.Named('Float'))  ),
        f=lambda in0, out0, in1, out1: abs(in1 - out1) / abs(in0 - out0),
        default=0.0,
        args=[
          Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal'))),
          Selection(TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal'))),
          Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal'))),
          Selection(TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal'))),
        ]
      ),
      TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))
    ]

    query = Query(name=None, selection=[
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
          Selection(TypeMeta.FieldMeta('price0', '', [], TypeRef.Named('Float')))
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
          'id': '0xf7b4c5bfaa6194720d984785827a2b325bd4851cc1735550664ce8d50bf40cf4:2',
          'amount': 52627231563,
          'reserve': {
            'decimals': 6
          },
          'adjusted_amount': 52627.231563
        },
        {
          'id': '0x4f05a63d43d0654d4649eafd319040b688ac4805aca0341f587e77db112875e2:2',
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
          'id': '0xf7b4c5bfaa6194720d984785827a2b325bd4851cc1735550664ce8d50bf40cf4:2',
          'amount': 52627231563,
          'reserve': {
            'decimals': 6,
            'symbol': 'USDC'
          },
          'adjusted_amount': 52627.231563
        },
        {
          'id': '0x4f05a63d43d0654d4649eafd319040b688ac4805aca0341f587e77db112875e2:2',
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