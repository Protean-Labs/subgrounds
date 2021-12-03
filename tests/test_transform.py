import unittest

from subgrounds.query import Argument, InputValue, Query, Selection
from subgrounds.schema import FieldMeta, TypeRef
from subgrounds.transform import LocalSyntheticField, TypeTransform, chain_transforms, transform_data, transform_data_type, transform_selection


class TestTransform(unittest.TestCase):
  def test_transform_selection1(self):
    expected = Query([
      Selection(FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, None, [
        Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      ])
    ])

    fmeta = FieldMeta('price1', '', [], TypeRef.non_null('Float'))

    query = Query([
      Selection(FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, None, [
        Selection(FieldMeta('price1', '', [], TypeRef.Named('Float')), None, None, None),
      ])
    ])

    replacement = [
      Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
    ]

    new_query = transform_selection(fmeta, replacement, query)

    self.assertEqual(new_query, expected)

  def test_transform_data1(self):
    expected = {
      'price1': 0.5,
      'amount0In': 0.0,
      'amount0Out': 10.0,
      'amount1In': 20.0,
      'amount1Out': 0.0
    }

    data = {
      'amount0In': 0.0,
      'amount0Out': 10.0,
      'amount1In': 20.0,
      'amount1Out': 0.0
    }

    fmeta = FieldMeta('price1', '', [], TypeRef.non_null('Float'))

    def f(in0, out0, in1, out1):
      return abs(in0 - out0) / abs(in1 - out1)

    arg_select = [
      Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
    ]

    query = Query([
      Selection(FieldMeta('price1', '', [], TypeRef.Named('Float')), None, None, None)
    ])

    transformed_data = transform_data(fmeta, f, arg_select, query, data)

    self.assertEqual(transformed_data, expected)

  def test_transform_data2(self):
    expected = {
      'swap': {
        'price1': 0.5,
        'amount0In': 0.0,
        'amount0Out': 10.0,
        'amount1In': 20.0,
        'amount1Out': 0.0
      }
    }

    data = {
      'swap': {
        'amount0In': 0.0,
        'amount0Out': 10.0,
        'amount1In': 20.0,
        'amount1Out': 0.0
      }
    }

    fmeta = FieldMeta('price1', '', [], TypeRef.non_null('Float'))

    def f(in0, out0, in1, out1):
      return abs(in0 - out0) / abs(in1 - out1)

    arg_select = [
      Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
    ]

    query = Query([
      Selection(FieldMeta('swap', '', [], TypeRef.Named('Swap')), None, None, [
        Selection(FieldMeta('price1', '', [], TypeRef.Named('Float')), None, None, None),
      ])
    ])

    transformed_data = transform_data(fmeta, f, arg_select, query, data)

    self.assertEqual(transformed_data, expected)

  def test_transform_data3(self):
    expected = {
      'swaps': [{
        'price1': 0.5,
        'amount0In': 0.0,
        'amount0Out': 10.0,
        'amount1In': 20.0,
        'amount1Out': 0.0
      }]
    }

    data = {
      'swaps': [{
        'amount0In': 0.0,
        'amount0Out': 10.0,
        'amount1In': 20.0,
        'amount1Out': 0.0
      }]
    }

    def f(in0, out0, in1, out1):
      return abs(in0 - out0) / abs(in1 - out1)

    arg_select = [
      Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
    ]

    fmeta = FieldMeta('price1', '', [], TypeRef.non_null('Float'))

    query = Query([
      Selection(FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, None, [
        Selection(FieldMeta('price1', '', [], TypeRef.Named('Float')), None, None, None),
      ])
    ])

    transformed_data = transform_data(fmeta, f, arg_select, query, data)

    self.assertEqual(transformed_data, expected)

  def test_transform_data4(self):
    expected = {
      'pair': {
        'token0Symbol': 'USDC',
        'token0': {
          'symbol': 'USDC'
        }
      }
    }

    data = {
      'pair': {
        'token0': {
          'symbol': 'USDC'
        }
      }
    }

    fmeta = FieldMeta('token0Symbol', '', [], TypeRef.non_null('String'))

    def f(x):
      return x

    arg_select = [
      Selection(FieldMeta('token0', '', [], TypeRef.Named('Token')), None, None, [
        Selection(FieldMeta('symbol', '', [], TypeRef.non_null('String')))
      ])
    ]

    query = Query([
      Selection(FieldMeta('pair', '', [], TypeRef.Named('Pair')), None, None, [
        Selection(FieldMeta('token0Symbol', '', [], TypeRef.Named('String')), None, None, None),
      ])
    ])

    transformed_data = transform_data(fmeta, f, arg_select, query, data)

    self.assertEqual(transformed_data, expected)

  def test_transform_data_type1(self):
    expected = {
      'swaps': [{
        'amount0In': 0.0,
        'amount0Out': 10.0,
        'amount1In': 20.0,
        'amount1Out': 0.0
      }]
    }

    data = {
      'swaps': [{
        'amount0In': '0.0',
        'amount0Out': '10.0',
        'amount1In': '20.0',
        'amount1Out': '0.0'
      }]
    }

    def f(bigdecimal):
      return float(bigdecimal)

    query = Query([
      Selection(FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), None, None, [
        Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
      ])
    ])

    type_ = TypeRef.Named('BigDecimal')

    transformed_data = transform_data_type(type_, f, query, data)

    self.assertEqual(transformed_data, expected)


class TestQueryTransform(unittest.TestCase):
  def test_roundtrip1(self):
    expected = {
      'swaps': [{
        'amount0In': 0.25,
        'amount0Out': 0.0,
        'amount1In': 0.0,
        'amount1Out': 89820.904371079570860909
      }]
    }

    transform = TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))

    query = Query([
      Selection(
        FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
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
          Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        ]
      )
    ])

    data = chain_transforms([transform], query, 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')

    self.assertEqual(data, expected)

  def test_roundtrip2(self):
    expected = {
      'swaps': [{
        'price0': 359283.61748431827,
        'amount0In': 0.25,
        'amount0Out': 0.0,
        'amount1In': 0.0,
        'amount1Out': 89820.904371079570860909
      }]
    }
    
    transforms = [
      LocalSyntheticField(
        None,
        FieldMeta('price0', '', [], TypeRef.non_null('Float')),
        lambda in0, out0, in1, out1: abs(in1 - out1) / abs(in0 - out0),
        [
          Selection(FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')), None, None, None),
          Selection(FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')), None, None, None),
        ]
      ),
      TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))
    ]

    query = Query([
      Selection(
        FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
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
          Selection(FieldMeta('price0', '', [], TypeRef.Named('Float')), None, None, None)
        ]
      )
    ])

    data = chain_transforms(transforms, query, 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')

    self.assertEqual(data, expected)