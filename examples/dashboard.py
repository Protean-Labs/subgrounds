import dash
from dash import html
from dash import dcc
# from dash.dependencies import Input

from datetime import datetime

from subgrounds.components import AutoUpdate, BarChart, Indicator, IndicatorWithChange
from subgrounds.subgraph import Subgraph, SyntheticField

uniswapV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")
aaveV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")
klima_markets = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/0xplaygrounds/playgrounds-klima-markets")

# This is unecessary, but nice for brevity
univ2Pair = uniswapV2.Pair
Borrow = aaveV2.Borrow
Repay = aaveV2.Repay

Trade = klima_markets.Trade

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div([
  html.H3('Price stuff'),
  html.Div([
    html.Div([
      html.H4('ETH price (USDC) (UniswapV2-mainnet)'),
      AutoUpdate(
        app,
        sec_interval=20,
        id='price-indicator-update',
        component=IndicatorWithChange(
          uniswapV2.Query.pair,
          component_id='price-indicator',
          id='0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc',
          x=univ2Pair.token0Price
        )
      )
    ], style={'width': '25%', 'display': 'inline-block'}),
    html.Div([
      html.H4('WBTC price (USDC) (UniswapV2-mainnet)'),
      AutoUpdate(
        app,
        sec_interval=20,
        id='price-indicator-update2',
        component=IndicatorWithChange(
          uniswapV2.Query.pair,
          component_id='price-indicator2',
          id='0x004375dff511095cc5a197a54140a24efef3a416',
          x=univ2Pair.token1Price
        )
      )
    ], style={'width': '25%', 'display': 'inline-block'}),
    html.Div([
      html.H4('KLIMA price (USDC) (Sushiswap-matic)'),
      AutoUpdate(
        app,
        sec_interval=20,
        id='price-indicator-update3',
        component=IndicatorWithChange(
          klima_markets.Query.trades,
          component_id='price-indicator3',
          orderBy=Trade.timestamp,
          orderDirection="desc",
          first=1,
          where=[
            Trade.pair == "0x5786b267d35f9d011c4750e0b0ba584e1fdbead1"
          ],
          x=Trade.close
        )
      )
    ], style={'width': '25%', 'display': 'inline-block'}),
    html.Div([
      html.H4('BCT price (USDC) (Sushiswap-matic)'),
      AutoUpdate(
        app,
        sec_interval=20,
        id='price-indicator-update4',
        component=IndicatorWithChange(
          klima_markets.Query.trades,
          component_id='price-indicator4',
          orderBy=Trade.timestamp,
          orderDirection="desc",
          first=1,
          where=[
            Trade.pair == "0x1e67124681b402064cd0abe8ed1b5c79d2e02f64"
          ],
          x=Trade.close
        )
      )
    ], style={'width': '25%', 'display': 'inline-block'})
  ]),

  html.H3('Aave V2 stuff'),
  html.Div([
    html.Div([
      html.H4('Aave V2 last 100 borrows'),
      html.Div([
        BarChart(
          aaveV2.Query.borrows,
          component_id='bar-chart2',
          orderBy=Borrow.timestamp,
          orderDirection="desc",
          first=100,
          x=Borrow.reserve.symbol,
          y=Borrow.amount
        )
      ])
    ], style={'width': '48%', 'display': 'inline-block'}),
    html.Div([
      html.H4('Aave V2 last 100 repayments'),
      html.Div([
        BarChart(
          aaveV2.Query.repays,
          component_id='bar-chart2',
          orderBy=Repay.timestamp,
          orderDirection="desc",
          first=100,
          x=Repay.reserve.symbol,
          y=Repay.amount
        )
      ])
    ], style={'width': '48%', 'display': 'inline-block'})
  ])
])

if __name__ == '__main__':
    app.run_server(debug=True)
 