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
          fmeta=TypeMeta.FieldMeta(name='swaps', description='', args=[
            TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Swap_filter', kind="INPUT_OBJECT"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Swap_orderBy', kind="ENUM"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="ENUM"), defaultValue=None),
          ], type=TypeRef.non_null_list("Swap", kind="OBJECT")),
          alias=None,
          arguments=[],
          selection=[
            Selection(fmeta=TypeMeta.FieldMeta(name='amount0In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='amount0Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='amount1In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='amount1Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
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
    [TypeTransform(TypeRef.Named(name='BigDecimal', kind="SCALAR"), lambda bigdecimal: float(bigdecimal))],
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
          fmeta=TypeMeta.FieldMeta(name='swaps', description='', args=[
            TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Swap_filter', kind="INPUT_OBJECT"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Swap_orderBy', kind="ENUM"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="ENUM"), defaultValue=None),
          ], type=TypeRef.non_null_list("Swap", kind="OBJECT")),
          alias=None,
          arguments=[],
          selection=[
            Selection(fmeta=TypeMeta.FieldMeta(name='amount0In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='amount0Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='amount1In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='amount1Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
            Selection(fmeta=TypeMeta.FieldMeta(name='price0', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
          ]
        )
      ])
    )
  ])

  subgraph_transforms = [
    LocalSyntheticField(
      subgraph=subgraph,
      fmeta=TypeMeta.FieldMeta(name='price0', description='', args=[], type=TypeRef.non_null('Float')),
      type_=TypeMeta.ObjectMeta(name='Swap', description='', fields=[
        TypeMeta.FieldMeta(name='id', description='', args=[], type=TypeRef.Named(name='String', kind="SCALAR")),
        TypeMeta.FieldMeta(name='timestamp', description='', args=[], type=TypeRef.Named(name='BigInt', kind="SCALAR")),
        TypeMeta.FieldMeta(name='amount0In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR")),
        TypeMeta.FieldMeta(name='amount0Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR")),
        TypeMeta.FieldMeta(name='amount1In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR")),
        TypeMeta.FieldMeta(name='amount1Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR")),
      ]),
      f=lambda in0, out0, in1, out1: abs(in1 - out1) / abs(in0 - out0),
      default=0.0,
      args=[
        Selection(TypeMeta.FieldMeta(name='amount0In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
        Selection(TypeMeta.FieldMeta(name='amount0Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
        Selection(TypeMeta.FieldMeta(name='amount1In', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
        Selection(TypeMeta.FieldMeta(name='amount1Out', description='', args=[], type=TypeRef.Named(name='BigDecimal', kind="SCALAR"))),
      ]
    ),
    TypeTransform(TypeRef.Named(name='BigDecimal', kind="SCALAR"), lambda bigdecimal: float(bigdecimal))
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