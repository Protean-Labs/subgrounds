# Subgrounds
A framework for integrating The Graph data with dash components

## Installation
Clone this repository. Then run the following in the repository root:
`pipenv install`

## Usage
Generate the subgraph object
```python
from subgrounds.subgraph import Subgraph

aaveV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")
```

Configure the subgrounds component. In this example, we are creating a bar chart of the amount borrowed in the last 100 borrows on Aave V2, grouped by the reserve token.

```python
# Not necessary, but nice for brevity
Query = aaveV2.Query
Borrow = aaveV2.Borrow

# Dashboard
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

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
```

Generates the following dash component:
![Alt text](/img/bar-chart-example.png?raw=true)

See `examples/bar_chart.py` for full code.

### Synthetic fields
With subgrounds, it is possible to define synthetic fields on subgraph Entities. A synthetic fields is a field which does not belong to the subgraph, but that can be computed using other fields that do belong to the subgraph.

For example, consider problem of getting the trade price of a swap on Uniswap V2. Uniswap V2 Swap entities contain information about the amounts of token0 and token1 that enter and leave the pool, which can be used to calculate the price.

Generate the subgraph and define the synthetic fields
```python
uniswapV2 = Subgraph.of_url("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

# Define the query object
Query = uniswapV2.Query
Swap = uniswapV2.Swap

# Synthetic field price1 is the trade price of token1 in terms of token0
Swap.price1 = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

# Synthetic field datetime is simply the timestamp of the swap formatted to ISO8601
Swap.datetime = SyntheticField(
  uniswapV2,
  lambda timestamp: str(datetime.fromtimestamp(timestamp)),
  Swap.timestamp
)
```

Once synthetic fields are defined, they can be used in subgrounds graphical components just like regular fields. In this example, we are generating a line plot that plots the price of token1 in terms of token0 (the synthetic field `price1`) and the time of the swap for the last 100 swaps of the pair `0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc`.
This pair address corresponds to the USDC-WETH pair, where USDC is token0 and WETH is token1. Therefore, price1 should be the price of WETH in USDC).

```python
app = dash.Dash(__name__)

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
        y=Swap.price1
      )
    ])
  ])
)
``` 

This code generates the following dash component:
![Alt text](/img/synthetic-field-example.png?raw=true)

Notice that the `where` argument of the component uses native python predicates to set the query arguments. behind the scenes, 
```python
where=[
  Swap.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
]
```
is translated to 
```graphql
query {
  swaps(where: {pair: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"}) {
    ...
  }
}
```

See `examples/synthetic_fields.py` for full code.