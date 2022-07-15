import json
import pytest

from subgrounds.schema import (SchemaMeta, TypeMeta,  # input_value_of_argument
                               TypeRef)
from subgrounds.subgraph import Subgraph, FieldPath
from subgrounds import Subgrounds

@pytest.fixture
def pairs_fieldmeta():
  return TypeMeta.FieldMeta('pairs', '', [
    TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
    TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
    TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
    TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
    TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
  ], TypeRef.non_null_list('Pair'))


@pytest.fixture
def swaps_fieldmeta():
  return TypeMeta.FieldMeta('swaps', '', [
    TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
    TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
    TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
    TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
    TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
  ], TypeRef.non_null_list('Swap'))


@pytest.fixture
def swap_objectmeta():
  return TypeMeta.ObjectMeta('Swap', '', fields=[
    TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
    TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
    TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
  ])


@pytest.fixture
def token_objectmeta():
  return TypeMeta.ObjectMeta('Token', '', fields=[
    TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String')),
    TypeMeta.FieldMeta('decimals', '', [], TypeRef.Named('Int')),
  ])


@pytest.fixture
def pair_objectmeta():
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
def schema(
  pairs_fieldmeta,
  swaps_fieldmeta,
  swap_objectmeta,
  token_objectmeta,
  pair_objectmeta
):
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
      pairs_fieldmeta,
      swaps_fieldmeta,
    ]),
    'Swap': swap_objectmeta,
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
    'Token': token_objectmeta,
    'Pair': pair_objectmeta,
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
  return Subgraph("www.abc.xyz/graphql", schema)


@pytest.fixture
def subgraph_diff_url(schema):
  return Subgraph("www.foo.xyz/graphql", schema)


@pytest.fixture
def sg(subgraph: Subgraph, subgraph_diff_url: Subgraph):
  return Subgrounds(
    subgraphs={
      subgraph._url: subgraph,
      subgraph_diff_url._url: subgraph_diff_url
    }
  )


@pytest.fixture
def klima_bridged_carbon_subgraph(mocker, sg: Subgrounds):
  with open("tests/schemas/cujowolf_polygon-bridged-carbon.json", "r") as f:
    schema = json.load(f)
  
  mocker.patch("subgrounds.client.get_schema", return_value=schema)
  return sg.load_subgraph('https://api.thegraph.com/subgraphs/name/cujowolf/polygon-bridged-carbon')


@pytest.fixture
def univ2_subgraph(mocker, sg: Subgrounds):
  with open("tests/schemas/uniswap_uniswap-v2.json", "r") as f:
    schema = json.load(f)
  
  mocker.patch("subgrounds.client.get_schema", return_value=schema)
  return sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')


@pytest.fixture
def univ3_subgraph(mocker, sg: Subgrounds):
  with open("tests/schemas/uniswap_uniswap-v3.json", "r") as f:
    schema = json.load(f)
  
  mocker.patch("subgrounds.client.get_schema", return_value=schema)
  return sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')


@pytest.fixture
def curve_subgraph(mocker, sg: Subgrounds):
  with open("tests/schemas/gvladika_curve.json", "r") as f:
    schema = json.load(f)
  
  mocker.patch("subgrounds.client.get_schema", return_value=schema)
  return sg.load_subgraph('https://api.thegraph.com/subgraphs/name/gvladika/curve')


def identity(x):
  return x


def fieldpath_test_mode(func):
  def wrapper(*args, **kwargs):
    FieldPath.__test_mode = True
    func(*args, **kwargs)

  return wrapper