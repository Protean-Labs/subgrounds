import dash
from dash import html

from subgrounds.components import BarChart
from subgrounds.subgraph import Subgraph

aaveV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")

# Not necessary, but nice for brevity
Query = aaveV2.Query
Borrow = aaveV2.Borrow
Repay = aaveV2.Repay

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.H4('Entities'),
    html.Div(id='step-display'),
    html.Div([
      BarChart(
        Query.repays,
        orderBy=Repay.timestamp,
        orderDirection="desc",
        first=100,
        x=Repay.reserve.symbol,
        y=Repay.amount,

        component_id='bar-chart'
      )
    ])
  ])
)

if __name__ == '__main__':
  app.run_server(debug=True)