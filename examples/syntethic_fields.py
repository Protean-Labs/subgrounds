import dash
from dash import html

from datetime import datetime

from subgrounds.components import LinePlot
from subgrounds.subgraph import Subgraph, SyntheticField

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

uniswapV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

# Define the query object
Query = uniswapV2.Query

Swap = uniswapV2.Swap

Swap.price = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

Swap.datetime = SyntheticField(
  uniswapV2,
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
        Query.swaps, 
        orderBy=Swap.timestamp,
        orderDirection="desc",
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
 