from abc import ABC
from typing import Any
from pipe import traverse, map

import plotly.graph_objects as go
from plotly.basedatatypes import BaseTraceType
from subgrounds.query import DataRequest

from subgrounds.schema import TypeRef
from subgrounds.subgraph import Filter, FieldPath
from subgrounds.subgrounds import Subgrounds


class TraceWrapper(ABC):
  x: FieldPath
  y: FieldPath

  args: dict[str, Any]
  graph_object: BaseTraceType

  def __init__(
    self,
    x: FieldPath,
    y: FieldPath,
    **kwargs
  ) -> None:
    self.x = x
    self.y = y

    self.args = kwargs


class Scatter(TraceWrapper):
  graph_object = go.Scatter


class Bar(TraceWrapper):
  graph_object = go.Bar


class Figure:
  subgrounds: Subgrounds
  traces: TraceWrapper | list[TraceWrapper]
  req: DataRequest
  data: list[dict[str, Any]]
  figure: go.Figure

  def __init__(
    self,
    subgrounds: Subgrounds,
    traces: TraceWrapper | list[TraceWrapper]
  ) -> None:
    self.subgrounds = subgrounds
    self.traces = list([traces] | traverse)
    self.req = self.subgrounds.mk_request(list(self.traces | map(lambda trace: [trace.x, trace.y]) | traverse))
    self.data = self.subgrounds.execute(self.req)

    self.refresh()

  def refresh(self) -> None:
    # TODO: Modify this to support x/y in different documents
    self.data = self.subgrounds.execute(self.req)[0]

    self.figure = go.Figure()
    for trace in self.traces:
      self.figure.add_trace(trace.graph_object(
        name=trace.y.longname,
        x=trace.x.extract_data(self.data),
        y=trace.y.extract_data(self.data),
        **trace.args
      ))
