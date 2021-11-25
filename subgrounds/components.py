from typing import List, Optional, Union

from dash import dcc
from dash import dash_table
import plotly.graph_objects as go

from subgrounds.subgraph import Filter, Subgraph, FieldPath

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
    first: int = 10,
    selection: List[FieldPath] = [], 
    orderBy: Optional[FieldPath] = None, 
    orderDirection: Optional[str] = None,
    where: Optional[List[Filter]] = None,
    **kwargs
  ) -> None:
    entrypoint = entrypoint(
      first=first,
      orderBy=orderBy,
      orderDirection=orderDirection,
      where=where
    )
    selection = [FieldPath.extend(entrypoint, s) for s in selection]

    query = Subgraph.mk_query(selection)
    data = entrypoint.subgraph.query(query)
    
    # Generate table
    cols = list(columns(data[0]))
    data = [dict(values(row)) for row in data]

    super().__init__(
      id=f'{query.name}-table',
      columns=[{"name": i, "id": i} for i in cols],
      data=data,
      **kwargs
    )

class BarChart(dcc.Graph):
  def __init__(
    self,
    entrypoint: FieldPath,
    x: FieldPath,
    y: Union[FieldPath, List[FieldPath]],
    first: int = 10,
    orderBy: Optional[FieldPath] = None, 
    orderDirection: Optional[str] = None,
    where: Optional[List[Filter]] = None,
    **kwargs
  ) -> None:
    if type(y) is not list:
      selection = [y]
      y = [y]
    else:
      selection = y
    selection.append(x)
    
    entrypoint = entrypoint(
      first=first,
      orderBy=orderBy,
      orderDirection=orderDirection,
      where=where
    )
    selection = [FieldPath.extend(entrypoint, s) for s in selection]

    query = Subgraph.mk_query(selection)
    data = entrypoint.subgraph.query(query)
    entrypoint.subgraph.process_data(selection, data)

    data = [dict(values(row)) for row in data[entrypoint.root.type_.name]]

    # fig = px.bar(data, x=x.data_name(), y=y.data_name())
    
    xdata = [row[x.longname] for row in data]
    fig = go.Figure(data=[go.Bar(name=y.longname, y=[row[y.longname] for row in data], x=xdata) for y in y])

    super().__init__(id=f'{entrypoint.leaf.type_.name}-bar-chart', figure=fig, **kwargs)

class LinePlot(dcc.Graph):
  def __init__(
    self,
    entrypoint: FieldPath,
    x: FieldPath,
    y: Union[FieldPath, List[FieldPath]],
    first: int = 10,
    orderBy: Optional[FieldPath] = None, 
    orderDirection: Optional[str] = None,
    where: Optional[List[Filter]] = None,
    **kwargs
  ) -> None:
    if type(y) is not list:
      selection = [y]
      y = [y]
    else:
      selection = y
    selection.append(x)
    
    entrypoint = entrypoint(
      first=first,
      orderBy=orderBy,
      orderDirection=orderDirection,
      where=where
    )
    selection = [FieldPath.extend(entrypoint, s) for s in selection]

    query = Subgraph.mk_query(selection)
    data = entrypoint.subgraph.query(query)
    entrypoint.subgraph.process_data(selection, data)

    data = [dict(values(row)) for row in data[entrypoint.root.type_.name]]
    
    xdata = [row[x.longname] for row in data]
    fig = go.Figure(data=[go.Scatter(name=y.longname, y=[row[y.longname] for row in data], x=xdata) for y in y])

    super().__init__(id=f'{entrypoint.leaf.type_.name}-bar-chart', figure=fig, **kwargs)