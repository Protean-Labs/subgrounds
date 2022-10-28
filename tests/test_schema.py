import pytest

from subgrounds.query import (Argument, InputValue, arguments_of_field_args,
                              input_value_of_argument)
from subgrounds.schema import TypeMeta, TypeRef
# from tests.conftest import *


@pytest.mark.parametrize("argmeta, argvalue, expected", [
  (
    TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Pair_filter', kind="INPUT_OBJECT"), defaultValue=None),
    {'reserveUSD_lt': 10},
    InputValue.Object({'reserveUSD_lt': InputValue.String('10.0')})
  ),
  (
    TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Pair_orderBy', kind="ENUM"), defaultValue=None),
    'reserveUSD',
    InputValue.Enum('reserveUSD'),
  ),
  (
    TypeMeta.ArgumentMeta(name='id', description='', type=TypeRef.Named(name='String', kind="SCALAR"), defaultValue=None),
    'abc',
    InputValue.String('abc')
  )
])
def test_input_value_of_argument(schema, argmeta, argvalue, expected):
  assert input_value_of_argument(schema, argmeta, argvalue) == expected


def test_arguments_of_field(schema, pairs_fieldmeta):
  field = pairs_fieldmeta

  expected = [
    Argument('first', InputValue.Int(100)),
    Argument('where', InputValue.Object({'reserveUSD_lt': InputValue.String('10.0')})),
    Argument('orderBy', InputValue.Enum('reserveUSD')),
    Argument('orderDirection', InputValue.Enum('desc'))
  ]

  raw_args = {
    'first': 100,
    'where': {'reserveUSD_lt': 10},
    'orderBy': 'reserveUSD',
    'orderDirection': 'desc'
  }

  args = arguments_of_field_args(schema, field, raw_args)

  assert args == expected
