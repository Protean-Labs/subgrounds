import dash
from dash import html

from datetime import datetime

from subgrounds.components import BarChart, LinePlot
from subgrounds.subgraph import Subgraph, SyntheticField

aaveV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")

# Not necessary, but nice for brevity
Query = aaveV2.Query
Borrow = aaveV2.Borrow

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.H4('Entities'),
    html.Div(id='step-display'),
    html.Div([
      BarChart(
        Query.borrows,
        orderBy=Borrow.timestamp,
        orderDirection="desc",
        first=100,
        x=Borrow.reserve.symbol,
        y=Borrow.amount
      )
    ])
  ])
)

if __name__ == '__main__':
  app.run_server(debug=True)
 