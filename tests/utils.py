from enum import EnumMeta
from subgrounds.schema import (
  ArgumentMeta,
  EnumValueMeta,
  FieldMeta,
  InputObjectMeta,
  ObjectMeta,
  ScalarMeta,
  SchemaMeta,
  TypeRef,
  # input_value_of_argument
)
from subgrounds.subgraph import Subgraph


def schema():
  return SchemaMeta(query_type='Query', type_map={
    'Int': ScalarMeta('Int', ''),
    'BigInt': ScalarMeta('BigInt', ''),
    'BigDecimal': ScalarMeta('BigDecimal', ''),
    'String': ScalarMeta('String', ''),
    'OrderDirection': EnumMeta('OrderDirection', '', [
      EnumValueMeta('asc', ''),
      EnumValueMeta('desc', '')
    ]),
    'Query': ObjectMeta('Query', '', fields=[
      FieldMeta('pairs', '', [
        ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
    ]),
    'Swap': ObjectMeta('Swap', '', fields=[
      FieldMeta('id', '', [], TypeRef.Named('String')),
      FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
      FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
    ]),
    'Token': ObjectMeta('Token', '', fields=[
      FieldMeta('id', '', [], TypeRef.Named('String')),
      FieldMeta('name', '', [], TypeRef.Named('String')),
      FieldMeta('symbol', '', [], TypeRef.Named('String')),
      FieldMeta('decimals', '', [], TypeRef.Named('Int')),
    ]),
    'Pair': ObjectMeta('Pair', '', fields=[
      FieldMeta('id', '', [], TypeRef.Named('String')),
      FieldMeta('token0', '', [], TypeRef.Named('Token')),
      FieldMeta('token1', '', [], TypeRef.Named('Token')),
      FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
    ]),
    'Pair_filter': InputObjectMeta('Pair_filter', '', [
      ArgumentMeta('token0', '', TypeRef.Named('String'), None),
      ArgumentMeta('token1', '', TypeRef.Named('String'), None),
      ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
    ]),
    'Pair_orderBy': EnumMeta('Pair_orderBy', '', [
      EnumValueMeta('id', ''),
      EnumValueMeta('reserveUSD', '')
    ])
  })


def subgraph():
  return Subgraph("", SchemaMeta(query_type='Query', type_map={
    'Int': ScalarMeta('Int', ''),
    'BigInt': ScalarMeta('BigInt', ''),
    'BigDecimal': ScalarMeta('BigDecimal', ''),
    'String': ScalarMeta('String', ''),
    'OrderDirection': EnumMeta('OrderDirection', '', [
      EnumValueMeta('asc', ''),
      EnumValueMeta('desc', '')
    ]),
    'Query': ObjectMeta('Query', '', fields=[
      FieldMeta('pairs', '', [
        ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
        ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
        ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
      ], TypeRef.non_null_list('Pair')),
      FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
    ]),
    'Swap': ObjectMeta('Swap', '', fields=[
      FieldMeta('id', '', [], TypeRef.Named('String')),
      FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
      FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
    ]),
    'Token': ObjectMeta('Token', '', fields=[
      FieldMeta('id', '', [], TypeRef.Named('String')),
      FieldMeta('name', '', [], TypeRef.Named('String')),
      FieldMeta('symbol', '', [], TypeRef.Named('String')),
      FieldMeta('decimals', '', [], TypeRef.Named('Int')),
    ]),
    'Pair': ObjectMeta('Pair', '', fields=[
      FieldMeta('id', '', [], TypeRef.Named('String')),
      FieldMeta('token0', '', [], TypeRef.Named('Token')),
      FieldMeta('token1', '', [], TypeRef.Named('Token')),
      FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
      FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
    ]),
    'Pair_filter': InputObjectMeta('Pair_filter', '', [
      ArgumentMeta('token0', '', TypeRef.Named('String'), None),
      ArgumentMeta('token1', '', TypeRef.Named('String'), None),
      ArgumentMeta('reserveUSD_lt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('reserveUSD_gt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken0_lt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken0_gt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken1_lt', '', TypeRef.Named('BigDecimal'), None),
      ArgumentMeta('priceToken1_gt', '', TypeRef.Named('BigDecimal'), None),
    ]),
    'Pair_orderBy': EnumMeta('Pair_orderBy', '', [
      EnumValueMeta('id', ''),
      EnumValueMeta('reserveUSD', '')
    ])
  }))


def identity(x):
  return x
