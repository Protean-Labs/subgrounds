from abc import ABC, abstractmethod
from trace import Trace
from typing import Any
from pipe import traverse, map

import plotly.graph_objects as go
from plotly.basedatatypes import BaseTraceType
from subgrounds.query import DataRequest

from subgrounds.schema import TypeRef
from subgrounds.subgraph import Filter, FieldPath
from subgrounds.subgrounds import Subgrounds


class TraceWrapper(ABC):
  args: dict[str, Any]
  graph_object: BaseTraceType

  @abstractmethod
  def mk_trace(self, data: dict[str, Any]) -> BaseTraceType:
    raise NotImplementedError

  @property
  @abstractmethod
  def field_paths(self) -> list[FieldPath]:
    raise NotImplementedError


class Scatter(TraceWrapper):
  graph_object = go.Scatter

  x: FieldPath
  y: FieldPath

  def __init__(self, x: FieldPath, y: FieldPath, **kwargs) -> None:
    self.x = x
    self.y = y

    self.args = kwargs

  def mk_trace(self, data: dict[str, Any]) -> BaseTraceType:
    return self.graph_object(
      x=self.x.extract_data(data),
      y=self.y.extract_data(data),
      **self.args
    )

  @property
  def field_paths(self) -> list[FieldPath]:
    return [self.x, self.y]


class Bar(TraceWrapper):
  graph_object = go.Bar

  x: FieldPath
  y: FieldPath

  def __init__(self, x: FieldPath, y: FieldPath, **kwargs) -> None:
    self.x = x
    self.y = y

    self.args = kwargs

  def mk_trace(self, data: dict[str, Any]) -> BaseTraceType:
    return self.graph_object(
      x=self.x.extract_data(data),
      y=self.y.extract_data(data),
      **self.args
    )

  @property
  def field_paths(self) -> list[FieldPath]:
    return [self.x, self.y]


class Indicator(TraceWrapper):
  graph_object = go.Indicator

  value: FieldPath

  def __init__(self, value: FieldPath, **kwargs) -> None:
    self.value = value

    self.args = kwargs

  def mk_trace(self, data: dict[str, Any]) -> BaseTraceType:
    match self.value:
      case FieldPath():
        val = self.value.extract_data(data)

        return self.graph_object(
          value=val[0] if type(val) == list else val,
          **self.args
        )
      case value:
        return self.graph_object(value=value, **self.args)

  @property
  def field_paths(self) -> list[FieldPath]:
    return [self.value] if type(self.value) == FieldPath else []


class Pie(TraceWrapper):
  graph_object = go.Pie

  labels: FieldPath
  values: FieldPath

  def __init__(self, labels: FieldPath, values: FieldPath, **kwargs) -> None:
    self.labels = labels
    self.values = values

    self.args = kwargs

  def mk_trace(self, data: dict[str, Any]) -> BaseTraceType:
    return self.graph_object(
      labels=self.labels.extract_data(data),
      values=self.values.extract_data(data),
      **self.args
    )

  @property
  def field_paths(self) -> list[FieldPath]:
    return [self.labels, self.values]


class Figure:
  subgrounds: Subgrounds
  traces: list[TraceWrapper]
  req: DataRequest
  data: list[dict[str, Any]]
  figure: go.Figure

  args: dict[str, Any]

  def __init__(
    self,
    subgrounds: Subgrounds,
    traces: TraceWrapper | list[TraceWrapper],
    **kwargs
  ) -> None:
    self.subgrounds = subgrounds
    self.traces = list([traces] | traverse)
    self.req = self.subgrounds.mk_request(list(self.traces | map(lambda trace: trace.field_paths) | traverse))
    self.data = self.subgrounds.execute(self.req)

    self.args = kwargs
    self.refresh()

  def refresh(self) -> None:
    # TODO: Modify this to support x/y in different documents
    self.data = self.subgrounds.execute(self.req)[0]

    self.figure = go.Figure(**self.args)
    for trace in self.traces:
      self.figure.add_trace(trace.mk_trace(self.data))
