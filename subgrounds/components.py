from typing import List, Optional, Union

from dash import dcc
from dash import dash_table
import plotly.graph_objects as go

from subgrounds.query import Query, OrderDirection, Entity, ScalarField, Where

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
    entity: Entity,
    first: int = 10,
    selection: List[ScalarField] = [], 
    order_by: Optional[ScalarField] = None, 
    order_direction: Optional[OrderDirection] = None,
    where: Optional[List[Where]] = None,
    **kwargs
  ) -> None:
    query = Query(entity, first=first, selection=selection, order_by=order_by, order_direction=order_direction, where=where)
    data = query.execute()
    
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
    entity: Entity,
    x: ScalarField,
    y: Union[ScalarField, List[ScalarField]],
    first: int = 10,
    order_by: Optional[ScalarField] = None, 
    order_direction: Optional[OrderDirection] = None,
    where: Optional[List[Where]] = None,
    **kwargs
  ) -> None:
    if type(y) is not list:
      selection = [y]
      y = [y]
    else:
      selection = y
    selection.append(x)
    
    query = Query(entity, selection=selection, first=first, order_by=order_by, order_direction=order_direction, where=where)
    data = query.execute()

    # cols = list(columns(data[0]))
    data = [dict(values(row)) for row in data]

    # fig = px.bar(data, x=x.data_name(), y=y.data_name())
    
    xdata = [row[x.data_name()] for row in data]
    fig = go.Figure(data=[go.Bar(name=y.data_name(), y=[row[y.data_name()] for row in data], x=xdata) for y in y])

    super().__init__(id=f'{query.name}-bar-chart', figure=fig, **kwargs)

class LinePlot(dcc.Graph):
  def __init__(
    self,
    entity: Entity,
    x: ScalarField,
    y: Union[ScalarField, List[ScalarField]],
    first: int = 10,
    order_by: Optional[ScalarField] = None, 
    order_direction: Optional[OrderDirection] = None,
    where: Optional[List[Where]] = None,
    **kwargs
  ) -> None:
    if type(y) is not list:
      selection = [y]
      y = [y]
    else:
      selection = y
    selection.append(x)
    
    query = Query(entity, selection=selection, first=first, order_by=order_by, order_direction=order_direction, where=where)
    data = query.execute()

    data = [dict(values(row)) for row in data]
    
    xdata = [row[x.data_name()] for row in data]
    fig = go.Figure(data=[go.Scatter(name=y.data_name(), y=[row[y.data_name()] for row in data], x=xdata) for y in y])

    super().__init__(id=f'{query.name}-bar-chart', figure=fig, **kwargs)