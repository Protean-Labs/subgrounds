from typing import Optional, Union

from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go

from subgrounds.schema import TypeRef
from subgrounds.subgraph import Filter, FieldPath
from subgrounds.subgrounds import Subgrounds


def columns(row):
  for key, value in row.items():
    if type(value) == dict:
      for inner_key in value:
        yield f"{key}_{inner_key}"
    else:
      yield key


def values(row):
  for key, value in row.items():
    if type(value) == dict:
      for inner_key, inner_value in value.items():
        yield (f"{key}_{inner_key}", inner_value)
    elif type(value) == list:
      values(value)
    else:
      yield (key, value)


class EntityTable(dash_table.DataTable):
  def __init__(
    self,
    entrypoint: FieldPath,
    component_id: str,
    first: int = 10,
    selection: list[FieldPath] = [],
    orderBy: Optional[FieldPath] = None,
    orderDirection: Optional[str] = None,
    where: Optional[list[Filter]] = None,
    **kwargs
  ) -> None:
    entrypoint = entrypoint(
      first=first,
      orderBy=orderBy,
      orderDirection=orderDirection,
      where=where
    )
    
    selection = [FieldPath.extend(entrypoint, s) for s in selection]

    query = entrypoint.subgraph.mk_request(selection)
    data = entrypoint.subgraph.query(query)[0][entrypoint.root.name]

    # Generate table
    cols = list(columns(data[0]))
    data = [dict(values(row)) for row in data]

    super().__init__(
      id=component_id,
      columns=[{"name": i, "id": i} for i in cols],
      data=data,
      **kwargs
    )


class Component(dcc.Graph):
  entrypoint: FieldPath

  def __init__(
    self,
    entrypoint: FieldPath,
    component_id: str,
    **kwargs
  ) -> None:
    query_args, other_args = entrypoint.split_args(kwargs)
    self.entrypoint = entrypoint(**query_args)

    super().__init__(id=f'{entrypoint.leaf.name}-{component_id}', figure=self.make_figure(**other_args))

  def make_figure(self, **kwargs):
    raise NotImplementedError(f"{self.name}.figure")

  def update(self):
    self.figure = self.make_figure()


class BarChart(Component):
  def __init__(
    self,
    entrypoint: FieldPath,
    component_id: str,
    x: FieldPath,
    y: Union[FieldPath, list[FieldPath]],
    **kwargs
  ) -> None:
    self.x = x
    if type(y) is not list:
      self.selection = [y]
      self.y = [y]
    else:
      self.selection = y
      self.y = y
    self.selection.append(x)

    super().__init__(entrypoint, component_id, **kwargs)

  def make_figure(self, **kwargs):
    selection = [FieldPath.extend(self.entrypoint, s) for s in self.selection]

    req = self.entrypoint.subgraph.mk_request(selection)
    data = self.entrypoint.subgraph.query(req)[0]

    data = [dict(values(row)) for row in data[self.entrypoint.root.name]]

    xdata = [row[self.x.longname] for row in data]
    return go.Figure(data=[go.Bar(name=y.longname, y=[row[y.longname] for row in data], x=xdata, **kwargs) for y in self.y])


class LinePlot(Component):
  def __init__(
    self,
    entrypoint: FieldPath,
    component_id: str,
    x: FieldPath,
    y: Union[FieldPath, list[FieldPath]],
    **kwargs
  ) -> None:
    self.x = x
    if type(y) is not list:
      self.selection = [y]
      self.y = [y]
    else:
      self.selection = y
      self.y = y
    self.selection.append(x)

    super().__init__(entrypoint, component_id, **kwargs)

  def make_figure(self, **kwargs):
    selection = [FieldPath.extend(self.entrypoint, s) for s in self.selection]

    req = self.entrypoint.subgraph.mk_request(selection)
    data = self.entrypoint.subgraph.query(req)[0]

    data = [dict(values(row)) for row in data[self.entrypoint.root.name]]

    xdata = [row[self.x.longname] for row in data]
    return go.Figure(data=[go.Scatter(name=y.longname, y=[row[y.longname] for row in data], x=xdata, **kwargs) for y in self.y])


class Indicator(Component):
  def __init__(
    self,
    entrypoint: FieldPath,
    component_id: str,
    x: FieldPath,
    **kwargs
  ) -> None:
    self.x = x
    super().__init__(entrypoint, component_id, **kwargs)

  def make_figure(self, **kwargs):
    if TypeRef.is_list(self.entrypoint.leaf.type_) or TypeRef.is_list(self.x.leaf.type_):
      selection = [FieldPath.extend(self.entrypoint, self.x)]

      req = self.entrypoint.subgraph.mk_request(selection)
      data = self.entrypoint.subgraph.query(req)[0]

      data = [dict(values(row)) for row in data[self.entrypoint.root.name]]
      value = [row[self.x.longname] for row in data][0]
    else:
      selection = [FieldPath.extend(self.entrypoint, self.x)]

      req = self.entrypoint.subgraph.mk_request(selection)
      data = self.entrypoint.subgraph.query(req)[0]

      value = data[self.entrypoint.root.type_][self.x.longname]

    return go.Figure(go.Indicator(
      mode='number',
      value=value,
      number={'valueformat': '.02f'},
      **kwargs
    ))


class IndicatorWithChange(Component):
  last = None

  def __init__(
    self,
    entrypoint: FieldPath,
    component_id: str,
    x: FieldPath,
    **kwargs
  ) -> None:
    self.x = x
    super().__init__(entrypoint, component_id, **kwargs)

  def make_figure(self, **kwargs):
    if TypeRef.is_list(self.entrypoint.leaf.type_) or TypeRef.is_list(self.x.leaf.type_):
      selection = [FieldPath.extend(self.entrypoint, self.x)]

      req = self.entrypoint.subgraph.mk_request(selection)
      data = self.entrypoint.subgraph.query(req)[0]

      data = [dict(values(row)) for row in data[self.entrypoint.root.name]]
      value = [row[self.x.longname] for row in data][0]
    else:
      selection = [FieldPath.extend(self.entrypoint, self.x)]

      req = self.entrypoint.subgraph.mk_request(selection)
      data = self.entrypoint.subgraph.query(req)[0]

      value = data[self.entrypoint.root.name][self.x.longname]

    if self.last is None:
      self.last = value

    fig = go.Figure(go.Indicator(
      mode='number+delta',
      value=value,
      delta={'position': "right", "reference": self.last, "valueformat": ".02f"},
      number={'valueformat': '.02f'},
      **kwargs
    ))

    self.last = value

    return fig


# Live update wrapper
class AutoUpdate(html.Div):
  def __init__(
    self,
    app,
    sec_interval: int,
    id: str,
    component: Component
  ) -> None:
    @app.callback(
      Output(component.id, 'figure'),
      Input(id, 'n_intervals'))
    def update(n):
      return component.make_figure()

    super().__init__([
      dcc.Interval(
        id=id,
        interval=sec_interval * 1000,  # in milliseconds
        n_intervals=0
      ),
      component
    ])



# class LinePlot:
#   def __init__(
#     self,
#     app: Subgrounds,
#     field: FieldPath,
#     x: FieldPath,
#     y: FieldPath | list[FieldPath]
#   ) -> None:
    