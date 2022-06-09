# import unittest
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from subgrounds.query import (Argument, DataRequest, Document, InputValue,
                              Query, Selection)
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.subgrounds import Subgrounds
from tests.utils import *


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


def test_mk_request_2(subgraph, subgraph_diff_url):
    expected = DataRequest(documents=[
      Document(
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
      ),
      Document(
        'www.foo.xyz/graphql',
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
              Selection(TypeMeta.FieldMeta('token0Id', '', [], TypeRef.Named('String')))
            ]
          )
        ])
      ),
    ])

    pairs = subgraph.Query.pairs(first=10)

    Pair = subgraph_diff_url.Pair
    Pair.token0Id = Pair.token0.id

    app = Subgrounds()

    req = app.mk_request([
      pairs.id,
      pairs.reserveUSD,
      *subgraph_diff_url.Query.pairs(first=10, selection=[
        subgraph_diff_url.Pair.id,
        subgraph_diff_url.Pair.token0Id
      ]),
    ])

    assert req == expected
