import dash
from dash import html
from dash import dcc
# from dash.dependencies import Input

from datetime import datetime

from subgrounds.components import Indicator, LinePlot
from subgrounds.subgraph import Subgraph, SyntheticField

uniswapV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

# This is unecessary, but nice for brevity
Query = uniswapV2.Query
Pair = uniswapV2.Pair

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.Div([
      Indicator(
        Query.pair,
        component_id='price-indicator',
        id='0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc',
        x=Pair.token0Price
      )
    ])
  ])
)

if __name__ == '__main__':
    app.run_server(debug=True)
 