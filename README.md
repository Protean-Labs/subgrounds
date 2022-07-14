# Subgrounds
A Pythonic data access layer for applications querying data from The Graph Network.

<!-- ## WARNING
Subgrounds is still in the very early stages of development. APIs can and will change (now is the time to make suggestions!). Some features are still incomplete. Documentation is sparse (but getting better). Expect the Subgrounds API to change frequently. -->

## Design goals
- **Simple**: Small, easy to use toplevel API
- **Automated**: Schema introspection and class generation, type checking and pagination is all handled automatically
- **Schema-driven**: Queries are built with `FieldPaths` while transformations are defined with `SyntheticFields`, as opposed to raw GraphQL and transforming raw data

## Docs
- [**Subgrounds Docs**](https://protean-labs.github.io/subgrounds/): User guide and API documentation (WIP)
- [**MetricsDAO Subgrounds Workshop**](https://docs.metricsdao.xyz/get-involved/workshops/2022-03-30+-subgrounds-workshop-series): Subgrounds workshop series hosted by MetricsDAO 
- [**Examples**](https://github.com/Protean-Labs/subgrounds/tree/main/examples): A list of examples showcasing Subgrounds integration with Dash and Plotly
- [**Community projects**](https://protean-labs.github.io/subgrounds/examples/): An ever growing list of projects created by our community members

## Installation
**IMPORTANT**: Subgrounds requires Python version 3.10 or higher

Subgrounds is available on PyPi. To install it, run the following:<br>
`pip install subgrounds`.

Subgrounds also comes bundled with some handy `dash` wrappers. To use those wrappers, you can install the extra `dash` dependencies.<br>
`pip install subgrounds[dash]`.

## Simple example
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

## About us
Protean Labs was started by web3 data enthusiasts to build open source tooling for The Graph Network as part of its grants program. Although Protean Labs remains an independent entity, the team has since joined [Playgrounds](https://www.playgrounds.network/) with the goal of revolutionizing the web3 data stack.


## Community
- [**Discord**](https://discord.gg/uxCv2M9J): Playgrounds' official discord server
- [**Twitter (Playgrounds)**](https://twitter.com/Playgrounds0x): Playgrounds' official Twitter account
- [**Twitter (Protean Labs)**](https://twitter.com/protean_labs): Protean Labs' official Twitter account





<!-- TODO: Move this to github pages docs -->
<!-- # Dash and Plotly wrappers
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
``` -->

<!-- Generates the following Dash dashboard (at time of writing):
![Alt text](https://raw.githubusercontent.com/Protean-Labs/subgrounds/main/img/bar-chart-example.png) -->

<!-- # Examples and resources
See the `examples/` directory for an evergrowing list of examples. -->


## Acknowledgments
This software project would not be possible without the support of The Graph Foundation. You can learn more about The Graph and its mission [here](https://thegraph.com/en/).

This project also builds on the excellent work by the good folks over at Plotly. Lean more about Dash and Plotly [here](https://plotly.com/).

<!-- TODO: Move this to github pages docs -->
<!-- # Notes
## Non-subgraph GraphQL APIs
Although Subgrounds has been developed with subgraph APIs in mind, most features will also work with any GraphQL API. However, Subgrounds has not been tested with non-subgraph GraphQL APIs and some features will definitely break if the non-subgraph APIs do not follow the same conventions as subgraph APIs (e.g.: automatic pagination relies on each entity having a unique `id` field).

It is nonetheless possible to use Subgrounds with non-subgraph GraphQL APIs by using `load_api` instead of `load_subgraph`. This will bypass Subgrounds automatic pagination feature and pagination will therefore have to be done manually. Below is an example of using Subgrounds with the snapshot GraphQL API.
```python
>>> from datetime import datetime

>>> from subgrounds.subgrounds import Subgrounds
>>> from subgrounds.subgraph import SyntheticField

>>> sg = Subgrounds()
>>> snapshot = sg.load_api('https://hub.snapshot.org/graphql')

>>> snapshot.Proposal.datetime = SyntheticField(
...   lambda timestamp: str(datetime.fromtimestamp(timestamp)),
...   SyntheticField.STRING,
...   snapshot.Proposal.end,
... )

>>> proposals = snapshot.Query.proposals(
...   orderBy='created',
...   orderDirection='desc',
...   first=100,
...   where=[
...     snapshot.Proposal.space == 'olympusdao.eth',
...     snapshot.Proposal.state == 'closed'
...   ]
... )

>>> sg.query_df([
...   proposals.datetime,
...   proposals.title,
...   proposals.votes,
... ])
     proposals_datetime                                    proposals_title  proposals_votes
0   2022-03-25 15:33:00  OIP-87: BeraChain Investment + Strategic Partn...              184
1   2022-03-25 12:00:00                 OIP-86: Uniswap Migration Proposal              146
2   2022-03-25 13:12:00                    TAP 8 - Yearn Finance Whitelist              137
3   2022-03-22 15:10:10                      OIP-85: Emissions Adjustments              167
4   2022-03-13 20:17:26                  TAP 7 - Anchor Protocol Whitelist              141
..                  ...                                                ...              ...
95  2021-11-20 23:00:00                 OlympusDAO Add ETH to the Treasury               52
96  2022-01-31 12:00:00                            Add BTC to the Treasury              232
97  2021-11-29 23:00:00   OlympusDAO OlympusDAO Launch OHM on POLYGON c...              234
98  2021-11-29 23:00:00                 OlympusDAO Launch OHM on BSC chain               92
99  2021-11-20 23:00:00  Suggestions to add more video operation guidan...               53

[100 rows x 3 columns]
```

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
``` -->
