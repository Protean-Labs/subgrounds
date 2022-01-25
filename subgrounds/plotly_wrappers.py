from abc import ABC, abstractmethod
from re import L
from typing import Any
from pipe import traverse, map

import plotly.graph_objects as go
from plotly.basedatatypes import BaseTraceType

from subgrounds.subgraph import FieldPath
from subgrounds.subgrounds import Subgrounds
from subgrounds.query import DataRequest


class TraceWrapper(ABC):
  graph_object: BaseTraceType

  fpaths: dict[str, FieldPath]
  args: dict[str, Any]

  def __init__(self, **kwargs) -> None:
    self.fpaths = {}
    self.args = {}

    for key, arg in kwargs.items():
      match arg:
        case FieldPath():
          self.fpaths[key] = arg
        case _:
          self.args[key] = arg

  def mk_trace(self, data: list[dict[str, Any]] | dict[str, Any]) -> BaseTraceType:
    fpath_data = {}
    for key, fpath in self.fpaths.items():
      item = fpath.extract_data(data)
      if type(item) == list and len(item) == 1:
        fpath_data[key] = item[0]
      else:
        fpath_data[key] = item
    # fpath_data = {key: fpath.extract_data(data) for key, fpath in self.fpaths.items()}

    return self.graph_object(
      **(fpath_data | self.args)
    )

  @property
  def field_paths(self) -> list[FieldPath]:
    return [fpath for _, fpath in self.fpaths.items()]


class Scatter(TraceWrapper):
  graph_object = go.Scatter


class Bar(TraceWrapper):
  graph_object = go.Bar


class Indicator(TraceWrapper):
  graph_object = go.Indicator


class Pie(TraceWrapper):
  graph_object = go.Pie


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
    self.data = self.subgrounds.execute(self.req)

    self.figure = go.Figure(**self.args)
    for trace in self.traces:
      self.figure.add_trace(trace.mk_trace(self.data))
