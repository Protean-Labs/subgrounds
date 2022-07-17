from ast import Sub
import unittest
import pytest

from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, Filter, Object, Subgraph, SyntheticField
from subgrounds.query import Argument, DataRequest, InputValue, Query, Selection
from subgrounds.subgrounds import Subgrounds
from subgrounds.utils import identity


# class TestAddType(unittest.TestCase):
#   def setUp(self):
#     self.schema = schema()
#     self.subgraph = Subgraph("", self.schema)

#   def tearDown(self) -> None:
#     SyntheticField.counter = 0

def test_add_synthetic_field_1(subgraph: Subgraph):
  sfield = SyntheticField(identity, SyntheticField.FLOAT, subgraph.Pair.reserveUSD)

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
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      TypeMeta.FieldMeta('swaps', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Swap')),
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
      TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')),
      TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float')),
    ]),
    'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_gt', '', TypeRef.Named('BigInt'), None),
    ]),
    'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
      TypeMeta.EnumValueMeta('id', ''),
      TypeMeta.EnumValueMeta('reserveUSD', ''),
      TypeMeta.EnumValueMeta('createdAtTimestamp', ''),
    ]),
    'Swap_filter': TypeMeta.InputObjectMeta('Swap_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('timestamp', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_gt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('amount0In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount0Out', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1Out', '', TypeRef.Named('BigDecimal'), None),
    ]),
  })

  Pair = subgraph.Pair
  Pair.reserveCAD = sfield

  assert subgraph._schema == expected

def test_add_synthetic_field_2(subgraph: Subgraph):
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
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      TypeMeta.FieldMeta('swaps', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Swap')),
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
      TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')),
      TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float')),
    ]),
    'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_gt', '', TypeRef.Named('BigInt'), None),
    ]),
    'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
      TypeMeta.EnumValueMeta('id', ''),
      TypeMeta.EnumValueMeta('reserveUSD', ''),
      TypeMeta.EnumValueMeta('createdAtTimestamp', ''),
    ]),
    'Swap_filter': TypeMeta.InputObjectMeta('Swap_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('timestamp', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_gt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('amount0In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount0Out', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1Out', '', TypeRef.Named('BigDecimal'), None),
    ]),
  })

  Pair = subgraph.Pair
  Pair.reserveCAD = Pair.reserveUSD * 1.3

  assert subgraph._schema == expected

def test_add_synthetic_field_3(subgraph: Subgraph):
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
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      TypeMeta.FieldMeta('swaps', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Swap')),
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
      TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')),
      TypeMeta.FieldMeta('reserveCAD', '', [], TypeRef.Named('Float')),
      TypeMeta.FieldMeta('reserveEUR', '', [], TypeRef.Named('Float')),
    ]),
    'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_gt', '', TypeRef.Named('BigInt'), None),
    ]),
    'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
      TypeMeta.EnumValueMeta('id', ''),
      TypeMeta.EnumValueMeta('reserveUSD', ''),
      TypeMeta.EnumValueMeta('createdAtTimestamp', ''),
    ]),
    'Swap_filter': TypeMeta.InputObjectMeta('Swap_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('timestamp', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_gt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('amount0In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount0Out', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1Out', '', TypeRef.Named('BigDecimal'), None),
    ]),
  })

  Pair = subgraph.Pair
  Pair.reserveCAD = Pair.reserveUSD * 1.3
  Pair.reserveEUR = Pair.reserveCAD * 1.5

  assert subgraph._schema == expected

def test_add_synthetic_field_4(subgraph: Subgraph):
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
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      TypeMeta.FieldMeta('swaps', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Swap')),
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
      TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')),
      TypeMeta.FieldMeta('tokenString', '', [], TypeRef.Named('String')),
    ]),
    'Pair_filter': TypeMeta.InputObjectMeta('Pair_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token0', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('token1', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('createdAtTimestamp_gt', '', TypeRef.Named('BigInt'), None),
    ]),
    'Pair_orderBy': TypeMeta.EnumMeta('Pair_orderBy', '', [
      TypeMeta.EnumValueMeta('id', ''),
      TypeMeta.EnumValueMeta('reserveUSD', ''),
      TypeMeta.EnumValueMeta('createdAtTimestamp', ''),
    ]),
    'Swap_filter': TypeMeta.InputObjectMeta('Swap_filter', '', [
      TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_gt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('id_lt', '', TypeRef.Named('String'), None),
      TypeMeta.ArgumentMeta('timestamp', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_gt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('timestamp_lt', '', TypeRef.Named('BigInt'), None),
      TypeMeta.ArgumentMeta('amount0In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount0Out', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1In', '', TypeRef.Named('BigDecimal'), None),
      TypeMeta.ArgumentMeta('amount1Out', '', TypeRef.Named('BigDecimal'), None),
    ]),
  })

  Pair = subgraph.Pair
  Pair.tokenString = Pair.token0.symbol + "_" + Pair.token0.id + "_" + Pair.token1.symbol + "_" + Pair.token1.id

  assert subgraph._schema == expected


def test_object(subgraph: Subgraph):
  object_ = TypeMeta.ObjectMeta('Pair', '', fields=[
    TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
    TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
    TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')),
  ])

  expected = Object(subgraph, object_)

  Pair = subgraph.Pair

  FieldPath.__test_mode = True
  assert Pair == expected


def test_field_path_1(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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

  fpath = subgraph.Pair.token0

  FieldPath.__test_mode = True
  assert fpath == expected


def test_field_path_2(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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

  fpath = subgraph.Pair.id

  FieldPath.__test_mode = True
  assert fpath == expected


def test_field_path_3(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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

  fpath = subgraph.Pair.token0.id

  FieldPath.__test_mode = True
  assert fpath == expected


def test_synthetic_field_path_1(subgraph: Subgraph):
  sfield = SyntheticField(identity, SyntheticField.FLOAT, subgraph.Pair.reserveUSD)

  expected = FieldPath(
    subgraph,
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

  subgraph.Pair.reserveCAD = sfield
  fpath = subgraph.Pair.reserveCAD

  FieldPath.__test_mode = True
  assert fpath == expected


def test_synthetic_field_path_2(subgraph: Subgraph):
  sfield = SyntheticField(identity, SyntheticField.STRING, "Le Token")

  expected = FieldPath(
    subgraph,
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

  subgraph.Token.frenchName = sfield
  fpath = subgraph.Pair.token0.frenchName

  FieldPath.__test_mode = True
  assert fpath == expected


def test_synthetic_field_path_3(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
    TypeRef.Named('Pair'),
    TypeRef.Named('String'),
    [
      (None, TypeMeta.FieldMeta('token0Id', '', [], TypeRef.Named('String')))
    ]
  )

  subgraph.Pair.token0Id = subgraph.Pair.token0.id
  fpath = subgraph.Pair.token0Id

  FieldPath.__test_mode = True
  assert fpath == expected


def test_filter_1(subgraph: Subgraph):
  expected = Filter(
    TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
    Filter.Operator.GT,
    100
  )

  filter_ = subgraph.Pair.reserveUSD > 100

  FieldPath.__test_mode = True
  assert filter_ == expected


def test_field_path_args_1(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
      ),
    ]
  )

  fpath = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc'
  )

  FieldPath.__test_mode = True
  assert fpath == expected


def test_field_path_args_2(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
      )
    ]
  )

  fpath = subgraph.Query.pairs(
    first=100,
    where=[
      subgraph.Pair.reserveUSD > 100,
      subgraph.Pair.token0 == "abcd"
    ]
  )

  FieldPath.__test_mode = True
  assert fpath == expected


def test_field_path_args_3(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
      ),
    ]
  )

  fpath = subgraph.Query.pairs(
    first=100,
    orderBy=subgraph.Pair.reserveUSD
  )

  FieldPath.__test_mode = True
  assert fpath == expected


def test_field_path_extend_1(subgraph: Subgraph):
  expected = FieldPath(
    subgraph,
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
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
          TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
          TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
        ], TypeRef.non_null_list('Pair')),
      ),
      (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
      (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
    ]
  )

  fpath1 = subgraph.Query.pairs
  fpath2 = subgraph.Pair.token0.symbol

  fpath = FieldPath._extend(fpath1, fpath2)

  FieldPath.__test_mode = True
  assert fpath == expected


def test_mk_request_1(subgraph: Subgraph):
  expected = DataRequest.single_query(subgraph._url, Query(selection=[
    Selection(
      TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      alias='x7ecb1bc5fd9e0dcf',
      arguments=[Argument("first", InputValue.Int(10))],
      selection=[
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
        Selection(TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')), selection=[
          Selection(TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')))
        ])
      ]
    )
  ]))

  sg = Subgrounds()

  pairs = subgraph.Query.pairs(first=10)

  req = sg.mk_request([
    pairs.id,
    pairs.token0.symbol
  ])

  FieldPath.__test_mode = True
  assert req == expected


def test_mk_request_2(sg: Subgrounds, subgraph: Subgraph):
  expected = DataRequest.single_query(subgraph._url, Query(selection=[
    Selection(
      TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      alias='x7ecb1bc5fd9e0dcf',
      arguments=[
        Argument("first", InputValue.Int(10))
      ],
      selection=[
        Selection(TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
        Selection(TypeMeta.FieldMeta('token0Id', '', [], TypeRef.Named('String')))
      ]
    )
  ]))

  Pair = subgraph.Pair
  Pair.token0Id = Pair.token0.id

  pairs = subgraph.Query.pairs(first=10)

  req = sg.mk_request([
    pairs.id,
    pairs.token0Id
  ])

  FieldPath.__test_mode = True
  assert req == expected


def test_mk_request_3(sg: Subgrounds, subgraph: Subgraph):
  expected = DataRequest.single_query(subgraph._url, Query(selection=[
    Selection(TypeMeta.FieldMeta('swaps', '', [
      TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
      TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
      TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
      TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
      TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
    ], TypeRef.non_null_list('Swap')), selection=[
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      Selection(TypeMeta.FieldMeta('price', '', [], TypeRef.Named('Float')))
    ])
  ]))

  Swap = subgraph.Swap
  Swap.price = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

  req = sg.mk_request([
    subgraph.Query.swaps.timestamp,
    subgraph.Query.swaps.price
  ])

  FieldPath.__test_mode = True
  assert req == expected


def test_mk_request_4(sg: Subgrounds, subgraph: Subgraph):
  expected = DataRequest.single_query(subgraph._url, Query(selection=[
    Selection(TypeMeta.FieldMeta('swaps', '', [
      TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
      TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
      TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
      TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
      TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
    ], TypeRef.non_null_list('Swap')), selection=[
      Selection(TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      Selection(TypeMeta.FieldMeta('my_value', '', [], TypeRef.Named('Float')))
    ])
  ]))

  Swap = subgraph.Swap
  Swap.my_value = Swap.amount0In / 10 ** Swap.amount0Out

  req = sg.mk_request([
    subgraph.Query.swaps.timestamp,
    subgraph.Query.swaps.my_value
  ])

  FieldPath.__test_mode = True
  assert req == expected


def test_synthetic_field_1(subgraph: Subgraph):
  Swap = subgraph.Swap

  expected_deps = [
    Swap.amount0In
  ]

  sfield = SyntheticField(
    lambda x: x * 10,
    SyntheticField.INT,
    Swap.amount0In
  )

  sfield._f(1) == 10
  sfield._deps == expected_deps


def test_synthetic_field_2(subgraph: Subgraph):
  Swap = subgraph.Swap

  expected_deps = [
    Swap.amount0In,
    Swap.amount0Out
  ]

  sfield: SyntheticField = Swap.amount0In - Swap.amount0Out

  sfield._f(10, 0) == 10
  sfield._deps == expected_deps


def test_synthetic_field_3(subgraph: Subgraph):
  Swap = subgraph.Swap

  expected_deps = [
    Swap.amount0In,
    Swap.amount0Out,
    Swap.amount1In,
  ]

  sfield: SyntheticField = Swap.amount0In - Swap.amount0Out + Swap.amount1In

  assert sfield._f(10, 0, 4) == 14
  assert sfield._deps == expected_deps


def test_synthetic_field_5(subgraph: Subgraph):
  Swap = subgraph.Swap

  expected_deps = [
    Swap.amount0In,
    Swap.amount0Out,
    Swap.amount1In,
    Swap.amount1Out
  ]

  sfield: SyntheticField = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

  assert sfield._f(10, 0, 0, 20) == 0.5
  assert sfield._deps == expected_deps


def test_synthetic_field_6(subgraph: Subgraph):
  Swap = subgraph.Swap

  expected_deps = [
    Swap.amount0In,
    Swap.amount0Out,
  ]

  sfield: SyntheticField = Swap.amount0In / 10 ** Swap.amount0Out

  assert sfield._f(100, 2) == 1
  assert sfield._deps == expected_deps
