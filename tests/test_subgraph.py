import unittest

from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, Filter, Object, Subgraph, SyntheticField
from subgrounds.query import Argument, DataRequest, InputValue, Query, Selection
from subgrounds.utils import identity

from tests.utils import schema


class TestAddType(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_add_synthetic_field_1(self):
    sfield = SyntheticField(self.subgraph, identity, TypeRef.Named('Float'))

    expected = SchemaMeta(query_type='Query', type_map={
      'Int': TypeMeta.ScalarMeta('Int', ''),
      'Float': TypeMeta.ScalarMeta('Float', ''),
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
        TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float'))
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
      'Float': TypeMeta.ScalarMeta('Float', ''),
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
        TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float'))
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
      'Float': TypeMeta.ScalarMeta('Float', ''),
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
        TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float')),
        TypeMeta.FieldMeta('reserveEUR', '', [], TypeRef.Named('Float')),
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
      'Float': TypeMeta.ScalarMeta('Float', ''),
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
        TypeMeta.FieldMeta('tokenString', '', [], TypeRef.Named('String')),
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
    FieldPath.test_mode = False

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

    FieldPath.test_mode = True
    self.assertEqual(Pair, expected)

  def test_field_path_1(self):
    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),
      TypeRef.Named('Token'),
      [
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
      ]
    )

    fpath = self.subgraph.Pair.token0

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_2(self):
    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),
      TypeRef.Named('String'),
      [
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]
    )

    fpath = self.subgraph.Pair.id

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_3(self):
    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),
      TypeRef.Named('String'),
      [
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]
    )

    fpath = self.subgraph.Pair.token0.id

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_synthetic_field_path_1(self):
    sfield = SyntheticField(self.subgraph, identity, TypeRef.Named('Float'))

    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Pair', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
        TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
      ]),
      TypeRef.Named('Float'),
      [
        (None, TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float'))),
      ]
    )

    self.subgraph.Pair.reserveCAD = sfield
    fpath = self.subgraph.Pair.reserveCAD

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_synthetic_field_path_2(self):
    sfield = SyntheticField(self.subgraph, identity, TypeRef.Named('String'))

    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Token', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
      ]),
      TypeRef.Named('String'),
      [
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta("frenchName", "", [], TypeRef.Named('String'))),
      ]
    )

    self.subgraph.Token.frenchName = sfield
    fpath = self.subgraph.Pair.token0.frenchName

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_synthetic_field_path_3(self):
    expected = FieldPath(
      self.subgraph,
      TypeRef.Named('Pair'),
      TypeRef.Named('String'),
      [
        (None, TypeMeta.FieldMeta('token0Id', '', [], TypeRef.Named('String')))
      ]
    )

    self.subgraph.Pair.token0Id = self.subgraph.Pair.token0.id
    fpath = self.subgraph.Pair.token0Id

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_filter_1(self):
    expected = Filter(
      TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
      Filter.Operator.GT,
      100
    )

    filter_ = self.subgraph.Pair.reserveUSD > 100

    FieldPath.test_mode = True
    self.assertEqual(filter_, expected)

  def test_field_path_args_1(self):
    expected = FieldPath(
      self.subgraph,
      TypeRef.Named("Query"),
      TypeRef.non_null_list('Pair'),
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
      ]
    )

    fpath = self.subgraph.Query.pairs(
      first=100,
      where={'reserveUSD_lt': 10},
      orderBy='reserveUSD',
      orderDirection='desc'
    )

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_args_2(self):
    expected = FieldPath(
      self.subgraph,
      TypeRef.Named("Query"),
      TypeRef.non_null_list('Pair'),
      [
        (
          {
            'first': 100,
            'where': {
              'reserveUSD_gt': 100.0,
              'token0': 'abcd'
            }
          },
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair'))
        )
      ]
    )

    fpath = self.subgraph.Query.pairs(
      first=100,
      where=[
        self.subgraph.Pair.reserveUSD > 100,
        self.subgraph.Pair.token0 == "abcd"
      ]
    )

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_args_3(self):
    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      TypeRef.non_null_list('Pair'),
      [
        (
          {
            'first': 100,
            'orderBy': 'reserveUSD'
          },
          # [
          #   Argument('first', InputValue.Int(100)),
          #   Argument('orderBy', InputValue.Enum('reserveUSD'))
          # ],
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair')),
        ),
      ]
    )

    fpath = self.subgraph.Query.pairs(
      first=100,
      orderBy=self.subgraph.Pair.reserveUSD
    )

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)

  def test_field_path_extend_1(self):
    expected = FieldPath(
      self.subgraph,
      TypeMeta.ObjectMeta('Query', '', fields=[
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
      ]),
      TypeRef.Named('String'),
      [
        (
          None,
          TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Pair'))
        ),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]
    )

    fpath1 = self.subgraph.Query.pairs
    fpath2 = self.subgraph.Pair.token0.symbol

    fpath = FieldPath.extend(fpath1, fpath2)

    FieldPath.test_mode = True
    self.assertEqual(fpath, expected)


class TestQueryBuilding(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_mk_request_1(self):
    expected = DataRequest.single_query("", Query(selection=[
      Selection(
        TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
        arguments=[Argument("first", InputValue.Int(10))],
        selection=[
          Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
          Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), selection=[
            Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')))
          ])
        ]
      )
    ]))

    query = self.subgraph.mk_request([
      self.subgraph.Query.pairs(first=10).id,
      self.subgraph.Query.pairs.token0.symbol
    ])

    FieldPath.test_mode = True
    self.maxDiff = None
    self.assertEqual(query, expected)

  def test_mk_request_2(self):
    expected = DataRequest.single_query("", Query(selection=[
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
    ]))

    Pair = self.subgraph.Pair
    Pair.token0Id = Pair.token0.id

    query = self.subgraph.mk_request([
      self.subgraph.Query.pairs(first=10).id,
      self.subgraph.Query.pairs.token0Id
    ])

    FieldPath.test_mode = True
    self.maxDiff = None
    self.assertEqual(query, expected)

  def test_mk_request_3(self):
    expected = DataRequest.single_query("", Query(selection=[
      Selection(TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')), selection=[
        Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
        Selection(TypeMeta.FieldMeta('price', '', [], TypeRef.Named('Float')))
      ])
    ]))

    Swap = self.subgraph.Swap
    Swap.price = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

    query = self.subgraph.mk_request([
      self.subgraph.Query.swaps.timestamp,
      self.subgraph.Query.swaps.price
    ])

    FieldPath.test_mode = True
    self.maxDiff = None
    self.assertEqual(query, expected)


class TestSyntheticField(unittest.TestCase):
  def setUp(self):
    self.schema = schema()
    self.subgraph = Subgraph("", self.schema)

  def tearDown(self) -> None:
    SyntheticField.counter = 0

  def test_synthetic_field_1(self):
    Swap = self.subgraph.Swap

    expected_deps = [
      Swap.amount0In
    ]

    sfield = SyntheticField(
      self.subgraph,
      lambda x: x * 10,
      TypeRef.Named('Int'),
      Swap.amount0In
    )

    self.assertEqual(sfield.f(1), 10)
    self.assertEqual(sfield.deps, expected_deps)

  def test_synthetic_field_2(self):
    Swap = self.subgraph.Swap

    expected_deps = [
      Swap.amount0In,
      Swap.amount0Out
    ]

    sfield: SyntheticField = Swap.amount0In - Swap.amount0Out

    self.assertEqual(sfield.f(10, 0), 10)
    self.assertEqual(sfield.deps, expected_deps)

  def test_synthetic_field_3(self):
    Swap = self.subgraph.Swap

    expected_deps = [
      Swap.amount0In,
      Swap.amount0Out,
      Swap.amount1In,
      # Swap.amount1Out
    ]

    sfield: SyntheticField = Swap.amount0In - Swap.amount0Out + Swap.amount1In

    self.assertEqual(sfield.f(10, 0, 4), 14)
    self.assertEqual(sfield.deps, expected_deps)

  def test_synthetic_field_5(self):
    Swap = self.subgraph.Swap

    expected_deps = [
      Swap.amount0In,
      Swap.amount0Out,
      Swap.amount1In,
      Swap.amount1Out
    ]

    sfield: SyntheticField = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

    self.assertEqual(sfield.f(10, 0, 0, 20), 0.5)
    self.assertEqual(sfield.deps, expected_deps)
