import unittest

from subgrounds.query import Argument, InputValue, arguments_of_field_args, input_value_of_argument
from subgrounds.schema import TypeMeta, TypeRef

from tests.utils import schema


class TestArguments(unittest.TestCase):
  def setUp(self):
    self.schema = schema()

  def test_input_value_of_argument_1(self):
    arg = TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
    expected = InputValue.Object({'reserveUSD_lt': InputValue.String('10.0')})
    self.assertEqual(input_value_of_argument(self.schema, arg, {'reserveUSD_lt': 10}), expected)

  def test_input_value_of_argument_2(self):
    arg = TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None)
    expected = InputValue.Enum('reserveUSD')
    self.assertEqual(input_value_of_argument(self.schema, arg, 'reserveUSD'), expected)

  def test_input_value_of_argument_3(self):
    arg = TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None)
    expected = InputValue.String('abc')
    self.assertEqual(input_value_of_argument(self.schema, arg, 'abc'), expected)

  def test_arguments_of_field(self):
    field = TypeMeta.FieldMeta('pairs', '', [
      TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), "100"),
      TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None),
      TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Pair_orderBy'), None),
      TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
    ], TypeRef.non_null_list('Pair'))

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

    args = arguments_of_field_args(self.schema, field, raw_args)

    self.assertEqual(args, expected)
