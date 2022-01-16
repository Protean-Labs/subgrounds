from dash import dcc

from subgrounds.plotly_wrappers import Figure

class Graph(dcc.Graph):
  def __init__(self, fig: Figure) -> None:
    super().__init__(figure=fig.figure)