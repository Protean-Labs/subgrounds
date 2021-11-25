import unittest
import operator

from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.subgraph2 import FieldPath, Filter, Object, Subgraph, SyntheticField
from subgrounds.query import Argument, InputValue, Query, Selection
from subgrounds.utils import identity

from tests.utils import schema, subgraph

class TestAddType(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_add_synthetic_field_1(self):
    sfield = SyntheticField(self.subgraph, identity)

    expected = SchemaMeta(query_type='Query', type_map={
      'Int': TypeMeta.ScalarMeta('Int', ''),
      'BigInt': TypeMeta.ScalarMeta('BigInt', ''),
      'BigDecimal': TypeMeta.ScalarMeta('BigDecimal', ''),
      'String': TypeMeta.ScalarMeta('String', ''),
      'OrderDirection': TypeMeta.EnumMeta('OrderDirection', '', [
        TypeMeta.EnumValueMeta('asc', ''),
        TypeMeta.EnumValueMeta('desc', '')
      ]),
      'Query': TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      'Swap': TypeMeta.ObjectMeta('Swap', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
        TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
      ]),
      'Token': TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      'Pair': TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.SyntheticFieldMeta('reserveCAD', '', identity, [])
      ]),
      'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
        TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      ]),
      'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
        TypeMeta.EnumValueMeta('id', ''),
        TypeMeta.EnumValueMeta('reserveUSD', '')
      ])
    })

    Pair = self.subgraph.Pair
    Pair.reserveCAD = sfield

    self.assertEqual(self.subgraph.schema, expected)

  def test_add_synthetic_field_2(self):
    expected = SchemaMeta(query_type='Query', type_map={
      'Int': TypeMeta.ScalarMeta('Int', ''),
      'BigInt': TypeMeta.ScalarMeta('BigInt', ''),
      'BigDecimal': TypeMeta.ScalarMeta('BigDecimal', ''),
      'String': TypeMeta.ScalarMeta('String', ''),
      'OrderDirection': TypeMeta.EnumMeta('OrderDirection', '', [
        TypeMeta.EnumValueMeta('asc', ''),
        TypeMeta.EnumValueMeta('desc', '')
      ]),
      'Query': TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      'Swap': TypeMeta.ObjectMeta('Swap', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
        TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
      ]),
      'Token': TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      'Pair': TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.SyntheticFieldMeta('reserveCAD', '', operator.mul, [
          [(None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')))],
          1.3
        ])
      ]),
      'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
        TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      ]),
      'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
        TypeMeta.EnumValueMeta('id', ''),
        TypeMeta.EnumValueMeta('reserveUSD', '')
      ])
    })

    Pair = self.subgraph.Pair
    Pair.reserveCAD = Pair.reserveUSD * 1.3

    self.assertEqual(self.subgraph.schema, expected)

  def test_add_synthetic_field_3(self):
    expected = SchemaMeta(query_type='Query', type_map={
      'Int': TypeMeta.ScalarMeta('Int', ''),
      'BigInt': TypeMeta.ScalarMeta('BigInt', ''),
      'BigDecimal': TypeMeta.ScalarMeta('BigDecimal', ''),
      'String': TypeMeta.ScalarMeta('String', ''),
      'OrderDirection': TypeMeta.EnumMeta('OrderDirection', '', [
        TypeMeta.EnumValueMeta('asc', ''),
        TypeMeta.EnumValueMeta('desc', '')
      ]),
      'Query': TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      'Swap': TypeMeta.ObjectMeta('Swap', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
        TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
      ]),
      'Token': TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      'Pair': TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.SyntheticFieldMeta('reserveCAD', '', operator.mul, [
          [(None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')))],
          1.3
        ]),
        TypeMeta.SyntheticFieldMeta('reserveEUR', '', operator.mul, [[
          TypeMeta.SyntheticFieldMeta('reserveCAD', '', operator.mul, [
            [(None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')))],
            1.3
          ])],
          1.5
        ])
      ]),
      'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
        TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      ]),
      'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
        TypeMeta.EnumValueMeta('id', ''),
        TypeMeta.EnumValueMeta('reserveUSD', '')
      ])
    })

    Pair = self.subgraph.Pair
    Pair.reserveCAD = Pair.reserveUSD * 1.3
    Pair.reserveEUR = Pair.reserveCAD * 1.5

    self.assertEqual(self.subgraph.schema, expected)

  def test_add_synthetic_field_4(self):
    expected = SchemaMeta(query_type='Query', type_map={
      'Int': TypeMeta.ScalarMeta('Int', ''),
      'BigInt': TypeMeta.ScalarMeta('BigInt', ''),
      'BigDecimal': TypeMeta.ScalarMeta('BigDecimal', ''),
      'String': TypeMeta.ScalarMeta('String', ''),
      'OrderDirection': TypeMeta.EnumMeta('OrderDirection', '', [
        TypeMeta.EnumValueMeta('asc', ''),
        TypeMeta.EnumValueMeta('desc', '')
      ]),
      'Query': TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      'Swap': TypeMeta.ObjectMeta('Swap', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
        TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
      ]),
      'Token': TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      'Pair': TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.SyntheticFieldMeta('tokenString', '', operator.add, [
          TypeMeta.SyntheticFieldMeta('SyntheticField_4', '', operator.add, [
            TypeMeta.SyntheticFieldMeta('SyntheticField_3', '', operator.add, [
              TypeMeta.SyntheticFieldMeta('SyntheticField_2', '', operator.add, [
                TypeMeta.SyntheticFieldMeta('SyntheticField_1', '', operator.add, [
                  TypeMeta.SyntheticFieldMeta('SyntheticField_0', '', operator.add, [
                    [
                      (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))), 
                      (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')))
                    ], 
                    '_'
                  ]), 
                  [
                    (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))), 
                    (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')))
                  ]
                ]), 
                '_'
              ]), 
              [
                (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))), 
                (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')))
              ]
            ]), 
            '_'
          ]), 
          [
            (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))), 
            (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')))
          ]
        ])
      ]),
      'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
        TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
        TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
        TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      ]),
      'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
        TypeMeta.EnumValueMeta('id', ''),
        TypeMeta.EnumValueMeta('reserveUSD', '')
      ])
    })

    Pair = self.subgraph.Pair
    Pair.tokenString = Pair.token0.symbol + "_" + Pair.token0.id + "_" + Pair.token1.symbol + "_" + Pair.token1.id

    self.assertEqual(self.subgraph.schema, expected)

class TestFieldPath(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    Filter.test_mode = False

  def test_object(self):
    object_ = TypeMeta.ObjectMeta('Pair', '', fields=[
      TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
      TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
      TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
      TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
      TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
      TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
    ])

    expected = Object(self.subgraph, object_)
    
    Pair = self.subgraph.Pair

    Filter.test_mode = True
    self.assertEqual(Pair, expected)

  def test_field_path_1(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),
      TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      [
        FieldPath.PathElement(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None),
      ]
    )

    fpath = self.subgraph.Pair.token0

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_2(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),      
      TypeMeta.ScalarMeta('String', ''), 
      [
        FieldPath.PathElement(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None),
      ]
    )

    fpath = self.subgraph.Pair.id

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_3(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),      
      TypeMeta.ScalarMeta('String', ''), 
      [
        FieldPath.PathElement(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None),
        FieldPath.PathElement(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')), None),
      ]
    )

    fpath = self.subgraph.Pair.token0.id

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_synthetic_field_1(self):
    sfield = SyntheticField(self.subgraph, identity)

    expected = FieldPath(self.subgraph,
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),      
      TypeMeta.SyntheticFieldMeta('reserveCAD', '', identity, []),
      [
        FieldPath.PathElement(TypeMeta.SyntheticFieldMeta('reserveCAD', '', identity, []), None),
      ]
    )

    self.subgraph.Pair.reserveCAD = sfield
    fpath = self.subgraph.Pair.reserveCAD

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_synthetic_field_2(self):
    sfield = SyntheticField(self.subgraph, identity)

    expected = FieldPath(self.subgraph,
      TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      TypeMeta.SyntheticFieldMeta("frenchName", "", identity, []), 
      [
        FieldPath.PathElement(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None),
        FieldPath.PathElement(TypeMeta.SyntheticFieldMeta('frenchName', '', identity, []), None),
      ]
    )

    self.subgraph.Token.frenchName = sfield
    fpath = self.subgraph.Pair.token0.frenchName

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_synthetic_field_3(self):
    expected = FieldPath(self.subgraph,
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.SyntheticFieldMeta('token0Id', '', identity, [
          [
            (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))), 
            (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')))
          ]
        ]),
      ]),
      TypeMeta.SyntheticFieldMeta('token0Id', '', identity, [
        [
          (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))), 
          (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')))
        ]
      ]),
      [
        FieldPath.PathElement(TypeMeta.SyntheticFieldMeta('token0Id', '', identity, [
          [
            (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))), 
            (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')))
          ]
        ]))
      ]
    )

    self.subgraph.Pair.token0Id = self.subgraph.Pair.token0.id
    fpath = self.subgraph.Pair.token0Id

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_filter_1(self):
    expected = Filter(
      TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
      Filter.Operator.GT,
      100
    )

    filter_ = self.subgraph.Pair.reserveUSD > 100

    Filter.test_mode = True
    self.assertEqual(filter_, expected)

  def test_field_path_args_1(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]), 
      [
        FieldPath.PathElement(
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
          [
            Argument('first', InputValue.Int(100)),
            Argument('where', InputValue.Object({'reserveUSD_lt': InputValue.String('10.0')})),
            Argument('orderBy', InputValue.Enum('reserveUSD')),
            Argument('orderDirection', InputValue.Enum('desc'))
          ]
        ),
      ]
    )

    fpath = self.subgraph.Query.pairs(
      first=100,
      where={'reserveUSD_lt': 10},
      orderBy='reserveUSD',
      orderDirection='desc'
    )

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_args_2(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]), 
      [
        FieldPath.PathElement(
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
          [
            Argument('first', InputValue.Int(100)),
            Argument('where', InputValue.Object({'reserveUSD_gt': InputValue.String('100.0'), 'token0': InputValue.String("abcd")})),
          ]
        ),
      ]
    )

    fpath = self.subgraph.Query.pairs(
      first=100,
      where=[
        self.subgraph.Pair.reserveUSD > 100,
        self.subgraph.Pair.token0 == "abcd"
      ]
    )

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_args_3(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]), 
      [
        FieldPath.PathElement(
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
          [
            Argument('first', InputValue.Int(100)),
            Argument('orderBy', InputValue.Enum('reserveUSD'))
          ]
        ),
      ]
    )

    fpath = self.subgraph.Query.pairs(
      first=100,
      orderBy=self.subgraph.Pair.reserveUSD
    )

    Filter.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_extend_1(self):
    expected = FieldPath(self.subgraph, 
      TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      TypeMeta.ScalarMeta('String', ''), 
      [
        FieldPath.PathElement(
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
          None
        ),
        FieldPath.PathElement(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), None),
        FieldPath.PathElement(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')), None),
      ]
    )

    fpath1 = self.subgraph.Query.pairs
    fpath2 = self.subgraph.Pair.token0.symbol

    fpath = FieldPath.extend(fpath1, fpath2)
    
    Filter.test_mode = True
    self.assertEqual(fpath, expected)

class TestQueryBuilding(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_mk_query_1(self):
    expected = Query(selection=[
      Selection("pairs", arguments=[Argument("first", InputValue.Int(10))], selection=[
        Selection("id"),
        Selection("token0", selection=[
          Selection("symbol")
        ]),
      ])
    ])

    query = Subgraph.mk_query([
      self.subgraph.Query.pairs(first=10).id,
      self.subgraph.Query.pairs.token0.symbol
    ])

    Filter.test_mode = True
    self.maxDiff = None
    self.assertEqual(query, expected)

  def test_mk_query_2(self):
    expected = Query(selection=[
      Selection("pairs", arguments=[Argument("first", InputValue.Int(10))], selection=[
        Selection("id"),
        Selection("token0", selection=[
          Selection("id")
        ]),
      ])
    ])

    Pair = self.subgraph.Pair
    Pair.token0Id = Pair.token0.id

    query = Subgraph.mk_query([
      self.subgraph.Query.pairs(first=10).id,
      self.subgraph.Query.pairs.token0Id
    ])

    Filter.test_mode = True
    self.maxDiff = None
    self.assertEqual(query, expected)

  def test_mk_query_3(self):
    expected = Query(selection=[
      Selection("swaps", selection=[
        Selection("timestamp"),
        Selection("amount0In"),
        Selection("amount0Out"),
        Selection("amount1In"),
        Selection("amount1Out"),
      ])
    ])

    Swap = self.subgraph.Swap
    Swap.price = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

    query = Subgraph.mk_query([
      self.subgraph.Query.swaps.timestamp,
      self.subgraph.Query.swaps.price
    ])

    Filter.test_mode = True
    self.maxDiff = None
    self.assertEqual(query, expected)