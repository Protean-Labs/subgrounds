# Subgrounds
A framework for integrating The Graph data with dash components

## WARNING
Subgrounds is still in the very early stages of development. APIs can and will change (now is the time to make suggestions!). Some features are still incomplete. Documentation is sparse (but getting better). Expect the Subgrounds API to change frequently.

## Installation
Subgrounds is available on PyPi. To install it, run the following:<br>
`pip install subgrounds`.

## Usage
Initialize Subgrounds and load a subgraph
```python
from subgrounds.subgrounds import Subgrounds

sg = Subgrounds()
aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')
```

<!-- Configure the subgrounds component. In this example, we are creating a bar chart of the amount borrowed in the last 100 borrows on Aave V2, grouped by the reserve token. -->

Create a subgrounds request by combining field paths. Field paths are selection paths through the graphql entities starting from the root entity `Query`. In the code below, `borrows` is a `FieldPath` object representing the selection path `Query.borrows` with the arguments `orderBy`, `orderDirection` and `first` specified. The two field paths used in the request (i.e.: `borrows.reserve.symbol` and `borrows.amount`) extend the `borrows` field path with further selections.

```python
borrows = aaveV2.Query.borrows(
  orderBy=aaveV2.Borrow.timestamp,
  orderDirection='desc',
  first=100
)

req = sg.mk_request([
  borrows.reserve.symbol,
  borrows.amount
])
```

The query in the request `req` crafted in the code snippet above translates to the following GraphQL query:
```graphql
query {
  borrows(
    orderBy: timestamp,
    orderDirection: desc,
    first: 100
  ) {
    reserve {
      symbol
    }
    amount
  }
}
```

Once a request has been created, it can be executed and the data can be formatted into a Pandas DataFrame:
```python
from subgrounds.subgrounds import to_dataframe

data = sg.execute(req)

df = to_dataframe(data)
```

Alternatively, field paths can be used directly as arguments to subgrounds graphical components.

```python
from subgrounds.plotly_wrappers import Bar, Figure
from subgrounds.dash_wrappers import Graph

borrows = aaveV2.Query.borrows(
  orderBy=aaveV2.Borrow.timestamp,
  orderDirection='desc',
  first=100
)

repays = aaveV2.Query.repays(
  orderBy=aaveV2.Repay.timestamp,
  orderDirection='desc',
  first=100
)

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.H4('Entities'),
    html.Div([
      Graph(Figure(
        subgrounds=sg,
        traces=[
          Bar(x=borrows.reserve.symbol, y=borrows.amount),
          Bar(x=repays.reserve.symbol, y=repays.amount)
        ]
      ))
    ])
  ])
)
```

Generates the following dash component (at time of writing):
![Alt text](https://raw.githubusercontent.com/Protean-Labs/subgrounds/main/img/bar-chart-example.png)

See `examples/bar_chart.py` for full code.

### Synthetic fields
With Subgrounds, it is possible to define synthetic fields on subgraph Entities. A synthetic fields is a field which does not belong to the subgraph, but that can be computed using other fields that do belong to the subgraph. Creating multiple synthetic fields on a subgraph is analogous to creating a view for a SQL database.

For example, consider problem of getting the trade price of a swap on Uniswap V2. Uniswap V2 Swap entities contain information about the amounts of token0 and token1 that enter and leave the pool, which can be used to calculate the price.

Generate the subgraph and define the synthetic fields
```python
from datetime import datetime

from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import SyntheticField
from subgrounds.schema import TypeRef


sg = Subgrounds()
uniswapV2 = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

# Not necessary, but nice for brevity
Query = uniswapV2.Query
Swap = uniswapV2.Swap

# Synthetic field price1 is the trade price of token1 in terms of token0
Swap.price1 = abs(Swap.amount0In - Swap.amount0Out) / abs(Swap.amount1In - Swap.amount1Out)

# Synthetic field datetime is simply the timestamp of the swap formatted to ISO8601
Swap.datetime = SyntheticField(
  lambda timestamp: str(datetime.fromtimestamp(timestamp)), # The arbitrary transformation function that computes the synthetic field's value
  TypeRef.Named('String'),                                  # The GraphQL type of the synthetic field
  Swap.timestamp,                                           # The input(s) to the transformation function
)
```

Once synthetic fields are defined, they can be used in subgrounds graphical components and subgrounds requests just like regular fields. In the following example, we are generating a line plot that plots the price of token1 in terms of token0 (the synthetic field `price1`) and the time of the swap for the last 100 swaps of the pair `0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc`.
This pair address corresponds to the USDC-WETH pair, where USDC is token0 and WETH is token1. Therefore, price1 should be the price of WETH in USDC.

```python
swaps = Query.swaps(
  orderBy=Swap.timestamp,
  orderDirection='desc',
  first=500,
  where=[
    Swap.pair == '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'
  ]
)

from subgrounds.plotly_wrappers import Scatter, Figure
from subgrounds.dash_wrappers import Graph

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.Div([
      Graph(Figure(
        subgrounds=sg,
        traces=[
          Scatter(x=swaps.datetime, y=swaps.price1)
        ]
      ))
    ])
  ])
)
``` 

This code generates the following dash component:
![Alt text](https://raw.githubusercontent.com/Protean-Labs/subgrounds/main/img/synthetic-field-example.png)

Notice that the `where` argument of the component uses native python predicates to set the query arguments. Behind the scenes, 
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