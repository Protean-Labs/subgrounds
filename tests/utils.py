from subgrounds.schema import (
  TypeMeta,
  SchemaMeta,
  TypeRef,
  # input_value_of_argument
)
from subgrounds.subgraph import Subgraph

import pytest


@pytest.fixture
def swap_typemeta():
  return TypeMeta.ObjectMeta('Swap', '', fields=[
    TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
    TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
  ])


@pytest.fixture
def token_typemeta():
  return TypeMeta.ObjectMeta('Token', '', fields=[
    TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
  ])


@pytest.fixture
def pair_typemeta():
  return TypeMeta.ObjectMeta('Pair', '', fields=[
    TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token')),
    TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token')),
    TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('priceToken0', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('priceToken1', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')),
  ])


@pytest.fixture
def schema(swap_typemeta, token_typemeta, pair_typemeta):
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
    'Swap': swap_typemeta,
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
    'Token': token_typemeta,
    'Pair': pair_typemeta,
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
  })


@pytest.fixture
def subgraph(schema):
  return Subgraph("", schema)


def identity(x):
  return x
