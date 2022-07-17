import unittest
from typing import Any

import pytest

from subgrounds.query import DataRequest, Document, Query, Selection
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import Subgraph
from subgrounds.subgrounds import Subgrounds
from subgrounds.transform import (DocumentTransform, LocalSyntheticField,
                                  TypeTransform)


@pytest.fixture
def datarequest(subgraph: Subgraph):
  return DataRequest(documents=[
    Document(
      url=subgraph._url,
      query=Query(None, [
        Selection(
          fmeta=TypeMeta.FieldMeta('swaps', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Swap')),
          alias=None,
          arguments=[],
          selection=[
            Selection(fmeta=TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal'))),
          ]
        )
      ])
    )
  ])


@pytest.fixture
def response():
  return {
    'swaps': [{
      'amount0In': '0.25',
      'amount0Out': '0.0',
      'amount1In': '0.0',
      'amount1Out': '89820.904371079570860909',
      'id': '0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0',
      'timestamp': '1638554699'
    }]
  }


@pytest.mark.parametrize("transforms, expected", [
  (
    [TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))],
    [{
      'swaps': [{
        'amount0In': 0.25,
        'amount0Out': 0.0,
        'amount1In': 0.0,
        'amount1Out': 89820.904371079570860909,
        'id': '0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0',
        'timestamp': '1638554699'
      }]
    }]
  ),
])
def test_typetransform_roundtrip(
  mocker,
  datarequest: DataRequest,
  response: list[dict[str, Any]],
  subgraph: Subgraph,
  transforms: list[DocumentTransform],
  expected: list[dict[str, Any]]
) -> None:
  mocker.patch("subgrounds.client.query", return_value=response)

  subgraph._transforms = transforms
  sg = Subgrounds(
    global_transforms=[],
    subgraphs={subgraph._url: subgraph}
  )

  data = sg.execute(datarequest)

  assert data == expected


def test_localsyntheticfield_literal_roundtrip1(
  mocker,
  response: list[dict[str, Any]],
  subgraph: Subgraph,
):
  mocker.patch("subgrounds.client.query", return_value=response)

  expected = [{
    'swaps': [{
      'amount0In': 0.25,
      'amount0Out': 0.0,
      'amount1In': 0.0,
      'amount1Out': 89820.904371079570860909,
      'price0': 359283.61748431827,
      'id': '0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0',
      'timestamp': '1638554699'
    }]
  }]

  datarequest = DataRequest(documents=[
    Document(
      url=subgraph._url,
      query=Query(None, [
        Selection(
          fmeta=TypeMeta.FieldMeta('swaps', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None),
            TypeMeta.ArgumentMeta('orderBy', '', TypeRef.Named('Swap_orderBy'), None),
            TypeMeta.ArgumentMeta('orderDirection', '', TypeRef.Named('OrderDirection'), None),
          ], TypeRef.non_null_list('Swap')),
          alias=None,
          arguments=[],
          selection=[
            Selection(fmeta=TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal'))),
            Selection(fmeta=TypeMeta.FieldMeta('price0', '', [], TypeRef.Named('BigDecimal'))),
          ]
        )
      ])
    )
  ])

  subgraph_transforms = [
    LocalSyntheticField(
      subgraph=subgraph,
      fmeta=TypeMeta.FieldMeta('price0', '', [], TypeRef.non_null('Float')),
      type_=TypeMeta.ObjectMeta('Swap', '', fields=[
        TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
        TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt')),
        TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal')),
        TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal')),
      ]),
      f=lambda in0, out0, in1, out1: abs(in1 - out1) / abs(in0 - out0),
      default=0.0,
      args=[
        Selection(TypeMeta.FieldMeta('amount0In', '', [], TypeRef.Named('BigDecimal'))),
        Selection(TypeMeta.FieldMeta('amount0Out', '', [], TypeRef.Named('BigDecimal'))),
        Selection(TypeMeta.FieldMeta('amount1In', '', [], TypeRef.Named('BigDecimal'))),
        Selection(TypeMeta.FieldMeta('amount1Out', '', [], TypeRef.Named('BigDecimal'))),
      ]
    ),
    TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal))
  ]

  subgraph._transforms = subgraph_transforms
  sg = Subgrounds(
    global_transforms=[],
    subgraphs={subgraph._url: subgraph}
  )
  data = sg.execute(datarequest)

  assert data == expected


def test_localsyntheticfield_toplevel_roundtrip(
  mocker,
  response: list[dict[str, Any]],
  subgraph: Subgraph,
):
  mocker.patch("subgrounds.client.query", return_value=response)
  
  expected = [{
    'swaps': [{
      'amount0In': 0.25,
      'amount0Out': 0.0,
      'amount1In': 0.0,
      'amount1Out': 89820.904371079570860909,
      'price0': 359283.61748431827,
      'id': '0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0',
      'timestamp': '1638554699'
    }]
  }]

  sg = Subgrounds(
    global_transforms=[],
    subgraphs={subgraph._url: subgraph}
  )

  subgraph.Swap.price0 = abs(subgraph.Swap.amount1In - subgraph.Swap.amount1Out) / abs(subgraph.Swap.amount0In - subgraph.Swap.amount0Out)

  req = sg.mk_request([
    subgraph.Query.swaps.price0
  ])

  data = sg.execute(req)

  assert data == expected