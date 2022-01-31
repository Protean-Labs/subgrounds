# Subgrounds
A framework for querying, manipulating and visualizing data indexed on The Graph network.

## WARNING
Subgrounds is still in the very early stages of development. APIs can and will change (now is the time to make suggestions!). Some features are still incomplete. Documentation is sparse (but getting better). Expect the Subgrounds API to change frequently.

## Installation
**IMPORTANT**: Subgrounds requires Python version 3.10 or higher

Subgrounds is available on PyPi. To install it, run the following:<br>
`pip install subgrounds`.

# Simple example
```python
>>> from subgrounds.subgrounds import Subgrounds

>>> sg = Subgrounds()
>>> aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')

>>> aaveV2.Borrow.adjusted_amount = aaveV2.Borrow.amount / 10 ** aaveV2.Borrow.reserve.decimals

>>> last10_borrows = aaveV2.Query.borrows(
...   orderBy=aaveV2.Borrow.timestamp,
...   orderDirection='desc',
...   first=10
... )

>>> sg.query_df([
...   last10_borrows.reserve.symbol, 
...   last10_borrows.timestamp,
...   last10_borrows.adjusted_amount
... ])
  borrows_reserve_symbol  borrows_timestamp  borrows_adjusted_amount
0                   USDT         1643300294            500000.000000
1                    DAI         1643299575              6000.000000
2                   USDT         1643298921            900000.000000
3                   USDT         1643297685            500000.000000
4                   USDC         1643296256             50000.000000
5                    PAX         1643295342              4150.000000
6                   USDT         1643294783              9000.000000
7                    DAI         1643293451             45585.919063
8                    UNI         1643289600             50000.000000
9                   USDT         1643289117             14000.000000
```

# Getting started
## Loading a subgraph
Initialize Subgrounds and load a subgraph.

**Note**: The Aave V2 subgraph will be used throughout the introduction. You can explore the subgraph [here](https://thegraph.com/hosted-service/subgraph/aave/protocol-v2).
```python
>>> from subgrounds.subgrounds import Subgrounds

>>> sg = Subgrounds()
>>> aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')
```

## FieldPaths
Create a subgrounds request by combining `FieldPath` objects. `FieldPath` objects represent selection paths through the graphql entities starting from the root entity `Query`. `FieldPath` objects do NOT contain any subgraph data. Instead they are used to build queries. The example below shows how to create a single `FieldPath` and print out its GraphQL equivalent.
```python
>>> fpath = aaveV2.Query.borrows.reserve.symbol

>>> req = sg.mk_request([fpath])
>>> print(req.graphql)
query {
  borrows {
    reserve {
      symbol
    }
  }
}
```

Partial `FieldPath` objects can also be created to avoid rewriting the field path from the start and multiple `FieldPath` objects can be combined to make a more complicated request:
```python
>>> borrows = aaveV2.Query.borrows
>>> borrows_reserve_symbol = borrows.reserve.symbol
>>> borrows_amounts = borrows.amount

>>> req = sg.mk_request([
...   borrows_reserve_symbol,
...   borrows_amounts
... ])
>>> print(req.graphql)
query {
  borrows {
    reserve {
      symbol
    }
    amount
  }
}
```

`FieldPath` from different subgraphs can also be combined in a single request (internally, Subgrounds sends one request to each subgraph):
```python
>>> uniV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

>>> aaveV2_borrows = aaveV2.Query.borrows
>>> uniV3_swaps = uniV3.Query.swaps

>>> req = sg.mk_request([
...   aaveV2_borrows.reserve.symbol,
...   aaveV2_borrows.amount,
...   uniV3_swaps.timestamp,
...   uniV3_swaps.amountUSD
... ])

>>> print(req.graphql)
query {
  borrows {
    reserve {
      symbol
    }
    amount
  }
}
query {
  swaps {
    timestamp
    amountUSD
  }
}
```

`FieldPath` also accept arguments which correspond to GraphQL arguments:
```python
>>> borrows = aaveV2.Query.borrows(
...  orderBy=aaveV2.Borrow.timestamp,
...  orderDirection='desc',
...  first=100
...)

>>> req = sg.mk_request([
...   borrows.reserve.symbol,
...   borrows.amount
... ])
>>> print(req.graphql)
query {
  xf608864358427cfb: borrows(first: 100, orderBy: timestamp, orderDirection: desc) {
    reserve {
      symbol
    }
    amount
  }
}
```
Note: For an explanation of why the `xf608864358427cfb` alias is present in the query string, see `GraphQL Aliases` in the `Notes` section at the end of the README. 

### FieldPaths as arguments
Notice in the previous example that the `FieldPath` `aaveV2.Borrow.timestamp` was used as the value for the `orderBy` argument when creating the `borrows` variable. With Subgrounds it is possible to use such `FieldPath` objects as GraphQL arguments in specific cases (as is the case with `orderBy`). They can also be used with the `where` argument to create a filter:
```python
>>> usdc_borrows = aaveV2.Query.reserves(
...   first=1, 
...   where=[
...     aaveV2.Reserve.symbol == 'USDC'
...   ]
... ).borrowHistory(
...   orderBy=aaveV2.Borrow.timestamp, 
...   orderDirection='desc', 
...   first=10
... )

>>> req = sg.mk_request([
...   usdc_borrows.timestamp, 
...   usdc_borrows.amount
... ])

>>> print(req.graphql)
query {
  x47607b0dcc7608b8: reserves(first: 1, where: {symbol: "USDC"}) {
    xc1acc74cbca2083c: borrowHistory(first: 10, orderBy: timestamp, orderDirection: desc) {
      timestamp
      amount
    }
  }
}
```
Alternatively, raw values can also be used as arguments. The following is equivalent to the above:
```python
>>> usdc_borrows = aaveV2.Query.reserves(
...   first=1, 
...   where={
...     'symbol': 'USDC'
...   }
... ).borrowHistory(
...   orderBy='timestamp', 
...   orderDirection='desc', 
...   first=10
... )
```

## Getting data
Following the code above, we can use fieldpaths to get data from The Graph.

**NOTE**: The data shown depends on when the query was executed.

Fetch one or multiple fieldpath and return the data immediately using the `query` method:
```python
>>> last_borrow = aaveV2.Query.borrows(
...   orderBy=aaveV2.Borrow.timestamp,
...   orderDirection='desc',
...   first=1
... )

>>> sg.query(last_borrow.reserve.symbol)
'PAX'

>>> sg.query([
...   last_borrow.reserve.symbol,
...   last_borrow.amount
... ])
('PAX', 4150000000000000000000)

>>> last10_borrow = aaveV2.Query.borrows(
...   orderBy=aaveV2.Borrow.timestamp,
...   orderDirection='desc',
...   first=10
... )

>>> sg.query([
...   last10_borrow.reserve.symbol,
...   last10_borrow.amount
... ])
(['USDC', 'PAX', 'USDT', 'DAI', 'UNI', 'USDT', 'USDT', 'DAI', 'USDC', 'DAI'],
 [50000000000,
  4150000000000000000000,
  9000000000,
  45585919063411815129359,
  50000000000000000000000,
  14000000000,
  15000000000,
  15000000000000000000000,
  2500000000,
  38942581537902421206923])
```

Fetch multiple fieldpaths and return the data as a DataFrame using the `query_df` method:
```python
>>> last10_borrow = aaveV2.Query.borrows(
...   orderBy=aaveV2.Borrow.timestamp,
...   orderDirection='desc',
...   first=10
... )

>>> sg.query_df([
...   last_borrow.reserve.symbol,
...   last_borrow.timestamp,
...   last_borrow.amount
... ])
  borrows_reserve_symbol  borrows_timestamp           borrows_amount
0                   USDT         1643300294             500000000000
1                    DAI         1643299575   6000000000000000000000
2                   USDT         1643298921             900000000000
3                   USDT         1643297685             500000000000
4                   USDC         1643296256              50000000000
5                    PAX         1643295342   4150000000000000000000
6                   USDT         1643294783               9000000000
7                    DAI         1643293451  45585919063411815129359
8                    UNI         1643289600  50000000000000000000000
9                   USDT         1643289117              14000000000
```

Fetch multiple fieldpaths and return the raw data as Python dictionaries using the `query_json` method.

**WARNING**: Query aliases will be present in the dictionaries (see `GraphQL Aliases` in the `Notes` section at the end of the README). This method is not the preferred approach to fetching data with Subgrounds. 
```python
>>> last10_borrow = aaveV2.Query.borrows(
...   orderBy=aaveV2.Borrow.timestamp,
...   orderDirection='desc',
...   first=10
... )

>>> sg.query_json([
...   last10_borrow.reserve.symbol,
...   last10_borrow.amount
... ])
[{'x15bcf1ad85e78eca': [{'amount': 50000000000, 'reserve': {'symbol': 'USDC'}},
                        {'amount': 4150000000000000000000,
                         'reserve': {'symbol': 'PAX'}},
                        {'amount': 9000000000, 'reserve': {'symbol': 'USDT'}},
                        {'amount': 45585919063411815129359,
                         'reserve': {'symbol': 'DAI'}},
                        {'amount': 50000000000000000000000,
                         'reserve': {'symbol': 'UNI'}},
                        {'amount': 14000000000, 'reserve': {'symbol': 'USDT'}},
                        {'amount': 15000000000, 'reserve': {'symbol': 'USDT'}},
                        {'amount': 15000000000000000000000,
                         'reserve': {'symbol': 'DAI'}},
                        {'amount': 2500000000, 'reserve': {'symbol': 'USDC'}},
                        {'amount': 38942581537902421206923,
                         'reserve': {'symbol': 'DAI'}}]}]
```

**Note**: `FieldPath` objects contain a method called `extract_data`, which takes raw response data in Python dictionaries and returns the data corresponding to the field path. Reusing the previous code:
```python
>>> raw_data = sg.query_json([
...   last10_borrow.reserve.symbol,
...   last10_borrow.amount
... ])
>>> raw_data
[{'x15bcf1ad85e78eca': [{'amount': 50000000000, 'reserve': {'symbol': 'USDC'}},
                        {'amount': 4150000000000000000000,
                         'reserve': {'symbol': 'PAX'}},
                        {'amount': 9000000000, 'reserve': {'symbol': 'USDT'}},
                        {'amount': 45585919063411815129359,
                         'reserve': {'symbol': 'DAI'}},
                        {'amount': 50000000000000000000000,
                         'reserve': {'symbol': 'UNI'}},
                        {'amount': 14000000000, 'reserve': {'symbol': 'USDT'}},
                        {'amount': 15000000000, 'reserve': {'symbol': 'USDT'}},
                        {'amount': 15000000000000000000000,
                         'reserve': {'symbol': 'DAI'}},
                        {'amount': 2500000000, 'reserve': {'symbol': 'USDC'}},
                        {'amount': 38942581537902421206923,
                         'reserve': {'symbol': 'DAI'}}]}]

>>> last10_borrow.reserve.symbol.extract_data(data)
['USDC', 'PAX', 'USDT', 'DAI', 'UNI', 'USDT', 'USDT', 'DAI', 'USDC', 'DAI']
```

### Synthetic fields
With Subgrounds, it is possible to define synthetic fields on subgraph entities. Synthetic fields are analogous to SQL views: they allow querying through a layer of transformations. Once synthetic fields are defined, they can be accessed just like regular entity fields.

For example, consider our previous example fetching data about Aave V2's `Borrow` entities. Notice that the `amount` does not take into account the number of decimals of each token. This can be fixed by defining a synthetic field `adjusted_amount` on the `Borrow` entity that will format `Borrow.amount` by the correct number of decimals:
```python
>>> aaveV2.Borrow.adjusted_amount = aaveV2.Borrow.amount / 10 ** aaveV2.Borrow.reserve.decimals

>>> sg.query_df([
...   last10_borrow.reserve.symbol, 
...   last10_borrow.timestamp, 
...   last10_borrow.adjusted_amount
... ])
  borrows_reserve_symbol  borrows_timestamp  borrows_adjusted_amount
0                   USDT         1643300294            500000.000000
1                    DAI         1643299575              6000.000000
2                   USDT         1643298921            900000.000000
3                   USDT         1643297685            500000.000000
4                   USDC         1643296256             50000.000000
5                    PAX         1643295342              4150.000000
6                   USDT         1643294783              9000.000000
7                    DAI         1643293451             45585.919063
8                    UNI         1643289600             50000.000000
9                   USDT         1643289117             14000.000000
```
Notice how the `aaveV2.Borrow.adjusted_amount` is defined using Python arithmetic operators on other `FieldPath` objects!

The `Borrow.timestamp` field can also be formatted to something more human-readable using synthetic fields. This time we will use the `SyntheticField` constructor which is more verbose than using Python operators, but also much more flexible.
```python
>>> from subgrounds.subgraph import SyntheticField
>>> from datetime import datetime

>>> aaveV2.Borrow.datetime = SyntheticField(
...   f=lambda timestamp: str(datetime.fromtimestamp(timestamp)),
...   type_=SyntheticField.STRING,
...   deps=aaveV2.Borrow.timestamp,
... )

>>> sg.query_df([
...   last10_borrow.reserve.symbol, 
...   last10_borrow.datetime, 
...   last10_borrow.adjusted_amount
... ])
  borrows_reserve_symbol     borrows_datetime  borrows_adjusted_amount
0                   USDT  2022-01-27 11:18:14            500000.000000
1                    DAI  2022-01-27 11:06:15              6000.000000
2                   USDT  2022-01-27 10:55:21            900000.000000
3                   USDT  2022-01-27 10:34:45            500000.000000
4                   USDC  2022-01-27 10:10:56             50000.000000
5                    PAX  2022-01-27 09:55:42              4150.000000
6                   USDT  2022-01-27 09:46:23              9000.000000
7                    DAI  2022-01-27 09:24:11             45585.919063
8                    UNI  2022-01-27 08:20:00             50000.000000
9                   USDT  2022-01-27 08:11:57             14000.000000
```

Looking at the `SyntheticField` constructor arguments, `f` is the function to apply to the dependencies `deps`, `type_` is the GraphQL type of the resulting synthetic field `datetime` and `deps` are the `FieldPath` objects on which the synthetic field depends.

You can also create `SyntheticField` objects that take more than one argument (notice the two `FieldPath` objects for `deps` as well as the two arguments to the funciton `f`), as well as default values which will be used in case of error (e.g.: division by zero) or if the data is missing (e.g.: GraphQL optional fields):
```python
>>> aaveV2.Borrow.token_name = SyntheticField(
...   f=lambda symbol, name: f'{symbol}: {name}'),
...   type_=SyntheticField.STRING,
...   deps=[
...     aaveV2.Borrow.reserve.symbol,
...     aaveV2.Borrow.reserve.name
...   ],
...   default='UNKNOWN: Unknown'
... )

>>> sg.query_df([
...   last10_borrow.token_name, 
...   last10_borrow.datetime, 
...   last10_borrow.adjusted_amount
... ])
    borrows_token_name     borrows_datetime  borrows_adjusted_amount
0       USDC: USD Coin  2022-01-27 11:39:44            220000.000000
1     USDT: Tether USD  2022-01-27 11:18:14            500000.000000
2  DAI: Dai Stablecoin  2022-01-27 11:06:15              6000.000000
3     USDT: Tether USD  2022-01-27 10:55:21            900000.000000
4     USDT: Tether USD  2022-01-27 10:34:45            500000.000000
5       USDC: USD Coin  2022-01-27 10:10:56             50000.000000
6  PAX: Paxos Standard  2022-01-27 09:55:42              4150.000000
7     USDT: Tether USD  2022-01-27 09:46:23              9000.000000
8  DAI: Dai Stablecoin  2022-01-27 09:24:11             45585.919063
9         UNI: Uniswap  2022-01-27 08:20:00             50000.000000
```

To make the most of Subgrounds, it is advised to use synthethic fields as much as possible to transform the data.

**IMPORTANT**: It is not currently possible to use synthetic fields as query *arguments* (e.g.: as an argument to `orderBy`).

# Dash and Plotly wrappers
Subgrounds provides wrappers for Plotly objects and Dash components to facilitate visualization of data from The Graph.

Plotly wrappers can be found in the `subgrounds.plotly_wrappers` submodule. The wrappers include a `Figure` wrapper as well as wrappers for most Plotly traces (see https://plotly.com/python/reference/). All Plotly trace wrappers accept the same arguments as their underlying Plotly trace with the notable difference being that Subgrounds `FieldPath` objects can be used as arguments wherever one would usually provide data to the traces.

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
      # Subgrounds Graph Dash component
      Graph(
        # A Subgrounds Plotly figure 
        Figure(
          subgrounds=sg,
          traces=[
            # Subgrounds Plotly traces
            Bar(x=borrows.reserve.symbol, y=borrows.amount),
            Bar(x=repays.reserve.symbol, y=repays.amount)
          ]
        )
      )
    ])
  ])
)
```

Generates the following Dash dashboard (at time of writing):
![Alt text](https://raw.githubusercontent.com/Protean-Labs/subgrounds/main/img/bar-chart-example.png)

# Examples
See the `examples/` directory for an evergrowing list of examples.

# Acknowledgments
This software project would not be possible without the support of The Graph Foundation. You can learn more about The Graph and its mission [here](https://thegraph.com/en/).

This project also builds on the excellent work by the good folks over at Plotly. Lean more about Dash and Plotly [here](https://plotly.com/).

# Notes
## GraphQL Aliases
The use of the alias `xf608864358427cfb` in the query string is to prevent conflict when merging fieldpaths that select the same fields with different arguments. For example, in the following code, the `borrows` query field is selected twice with different arguments:
```python
>>> latest_borrows = aaveV2.Query.borrows(
...  orderBy=aaveV2.Borrow.timestamp,
...  orderDirection='desc',
...  first=100
...)

>>> largest_borrows = aaveV2.Query.borrows(
...  orderBy=aaveV2.Borrow.amount,
...  orderDirection='desc',
...  first=100
...)

>>> req = sg.mk_request([
...   latest_borrows.reserve.symbol,
...   latest_borrows.amount,
...   largest_borrows.reserve.symbol,
...   largest_borrows.amount,
... ])
>>> print(req.graphql)
query {
  x8b3edf3dc6501837: borrows(first: 100, orderBy: amount, orderDirection: desc) {
    reserve {
      symbol
    }
    amount
  }
  xf608864358427cfb: borrows(first: 100, orderBy: timestamp, orderDirection: desc) {
    reserve {
      symbol
    }
    amount
  }
}
```
