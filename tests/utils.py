from subgrounds.schema import (
  TypeMeta,
  SchemaMeta,
  TypeRef,
  # input_value_of_argument
)
from subgrounds.subgraph import Subgraph


def schema():
  return SchemaMeta(query_type='Query', type_map={
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


def subgraph():
  return Subgraph("", SchemaMeta(query_type='Query', type_map={
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
  }))


def identity(x):
  return x
