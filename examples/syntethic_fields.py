import dash
from dash import html

from datetime import datetime

from subgrounds.components import LinePlot
from subgrounds.query import SyntheticField, OrderDirection
from subgrounds.subgraph import Subgraph

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

uniswapV2 = Subgraph("uniswapV2", "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

# Define the query object
Swap = uniswapV2.Swap

Swap.price = abs(Swap.amount0_in - Swap.amount0_out) / abs(Swap.amount1_in - Swap.amount1_out)

Swap.datetime = SyntheticField(
  lambda timestamp: str(datetime.fromtimestamp(timestamp)),
  Swap.timestamp
)

# Dashboard
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
  html.Div([
    html.H4('Entities'),
    html.Div(id='step-display'),
    html.Div([
      LinePlot(
        Swap, 
        order_by=Swap.timestamp,
        order_direction=OrderDirection.DESC,
        first=100,
        where=[
          Swap.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
        ],
        x=Swap.datetime,
        y=Swap.price
      )
    ])
  ])
)

if __name__ == '__main__':
    app.run_server(debug=True)
