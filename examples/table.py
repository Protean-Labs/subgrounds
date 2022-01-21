import dash
from dash import html

from subgrounds.components import EntityTable
from subgrounds.subgraph import Subgraph

uniswapV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

# Not necessary, but nice for brevity
Query = uniswapV2.Query
Burn = uniswapV2.Burn
Mint = uniswapV2.Mint

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.H4('Entities'),
    html.Div([
      html.Div([
        html.H4('Last 10 Mints of ETH-USDC LP tokens'),
        EntityTable(
          Query.mints,
          component_id='mints',
          orderBy=Mint.timestamp,
          orderDirection="desc",
          first=10,
          where=[
            Mint.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
          ],
          selection=[
            Mint.timestamp,
            Mint.transaction,
            Mint.to,
            Mint.amountUSD
          ]
        ),
      ], style={'width': '48%', 'display': 'inline-block', "margin-right": "4%"}),
      html.Div([
        html.H4('Last 10 Burns of ETH-USDC LP tokens'),
        EntityTable(
          Query.burns,
          component_id='burns',
          orderBy=Burn.timestamp,
          orderDirection="desc",
          first=10,
          where=[
            Burn.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
          ],
          selection=[
            Burn.timestamp,
            Burn.transaction,
            Burn.to,
            Burn.amountUSD
          ]
        )
      ], style={'width': '48%', 'display': 'inline-block'}),
    ])
  ])
)

if __name__ == '__main__':
  app.run_server(debug=True)