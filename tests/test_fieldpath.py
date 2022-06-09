import pytest

from subgrounds.query import (Argument, DataRequest, Document, InputValue,
                              Query, Selection)
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.subgrounds import Subgrounds

from tests.utils import *


def test_fieldpath_building_1(subgraph):
  expected = FieldPath(
    subgraph,
    TypeRef.Named('Query'),
    TypeRef.Named('String'),
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
      (
        None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
      )
    ]
  )

  pairs = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc'
  )
  query = pairs.id

  FieldPath.__test_mode = True
  query == expected


def test_fieldpath_building_2(subgraph):
  expected = [
    FieldPath(
      subgraph,
      TypeRef.Named('Query'),
      TypeRef.Named('String'),
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
        (
          None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    ),
    FieldPath(
      subgraph,
      TypeRef.Named('Query'),
      TypeRef.Named('BigDecimal'),
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
        (
          None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal'))
        )
      ]
    )
  ]

  query = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc',
    selection=[
      subgraph.Pair.id,
      subgraph.Pair.reserveUSD
    ]
  )

  FieldPath.__test_mode = True
  assert query == expected


def test_fieldpath_building_3(subgraph):
  expected = [
    FieldPath(
      subgraph,
      TypeRef.Named('Query'),
      TypeRef.Named('String'),
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
        (
          None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    ),
    FieldPath(
      subgraph,
      TypeRef.Named('Query'),
      TypeRef.Named('BigDecimal'),
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
        (
          None, TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal'))
        )
      ]
    )
  ]

  pairs = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc',
  )

  query = [
    pairs.id,
    pairs.reserveUSD
  ]

  FieldPath.__test_mode = True
  assert query == expected


def test_mk_request_1(subgraph):
    expected = DataRequest.single_query(
      'www.abc.xyz/graphql',
      Query(selection=[
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
            Selection(TypeMeta.FieldMeta('reserveUSD', '', [], TypeRef.Named('BigDecimal')))
          ]
        )
      ])
    )

    app = Subgrounds()

    pairs = subgraph.Query.pairs(first=10)

    req = app.mk_request([
      pairs.id,
      pairs.reserveUSD
    ])

    assert req == expected
