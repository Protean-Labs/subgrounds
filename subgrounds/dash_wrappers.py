from __future__ import annotations

from typing import ClassVar
from pipe import traverse, where, map

from dash import dcc
from dash import html
from dash.dependencies import Input, Output

from subgrounds.plotly_wrappers import Figure


class Graph(dcc.Graph):
  counter: ClassVar[int] = 0
  wrapped_figure: Figure

  def __init__(self, fig: Figure, **kwargs) -> None:
    super().__init__(id=f'graph-{Graph.counter}', figure=fig.figure, **kwargs)
    Graph.counter += 1
    self.wrapped_figure = fig


class AutoUpdate(html.Div):
  counter: ClassVar[int] = 0

  def __init__(self, app, sec_interval: int = 1, children=[], **kwargs):
    id = f'interval-{AutoUpdate.counter}'
    
    super().__init__(
      children=[dcc.Interval(id=id, interval=sec_interval * 1000, n_intervals=0), *children],
      **kwargs
    )

    AutoUpdate.counter += 1

    subgrounds_children = list(children | where(lambda child: isinstance(child, Graph)))
    deps = list(subgrounds_children | map(lambda child: Output(child.id, 'figure')))
    figures = list(subgrounds_children | map(lambda child: child.wrapped_figure))

    def update(n):
      for fig in figures:
        fig.refresh()

      if len(figures) == 1:
        return figures[0].figure
      else:
        return tuple(figures | map(lambda fig: fig.figure))

    # Register callback
    app.callback(*deps, Input(id, 'n_intervals'))(update)
