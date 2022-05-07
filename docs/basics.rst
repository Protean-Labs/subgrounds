Subgrounds basics
================================
This page was written to provide an overview of the main concepts in Subgrounds 
and to serve as a quick reference for those concepts.

Subgrounds
--------------------------------
The ``Subgrounds`` class provides the toplevel Subgrounds API and most Subgrounds 
users will be using this class exclusively. This class is used to load (i.e.: 
introspect) GraphQL APIs (subgraph APIs or vanilla GraphQL APIs) as well as execute 
querying operations. Moreover, this class is meant to be used as a singleton, 
i.e.: initialized once and reused throughout a project.

The code cell below demonstrates how to initialize your ``Subgrounds`` object and 
load a GraphQL API:

.. code-block:: python

  >>> from subgrounds.subgrounds import Subgrounds
  
  # Initialize Subgrounds
  >>> sg = Subgrounds()
  
  # Load a subgraph using its API URL
  >>> aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')
  
  # Load a vanilla GraphQL API using its API URL
  >>> snapshot = sg.load_api('https://hub.snapshot.org/graphql')

FieldPaths
--------------------------------
``FieldPaths`` are the main building blocks used to construct Subgrounds queries. 
A ``FieldPath`` represents a selection path through a GraphQL schema starting from 
the root ``Query`` entity (see `The Query and Mutation types <https://graphql.org/learn/schema/#the-query-and-mutation-types>`_) 
all the way down to a scalar leaf.

``FieldPaths`` are created by simply selecting attributes starting from the 
subgraph object returned by the ``Subgrounds.load_subgraph`` or 
``Subgrounds.load_api`` methods:

.. code-block:: python

  >>> uniswapV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')
  
  # Create a FieldPath
  >>> pools_tvl = uniswapV3.Query.pools.totalValueLockedUSD

In the example above, the ``pools_tvl`` variable is a ``FieldPath`` object 
representing the following GraphQL query:

.. code-block::

  query {
    pools {
      totalValueLockedUSD
    }
  }


Note that ``FieldPaths`` don't have to be selected from root to leaf each time 
and partial ``FieldPaths`` can be reused:

.. code-block:: python

  >>> uniswapV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

  # Create a FieldPath
  >>> pools_tvl = uniswapV3.Query.pools.totalValueLockedUSD

  # Create a partial FieldPath
  >>> pools = uniswapV3.Query.pools

  # This FieldPath is equivalent to the `pools_tvl` FieldPath
  >>> pools_tvl2 = pools.totalValueLockedUSD

FieldPath arguments
********************************
Setting arguments to fields in a `FieldPath` by "calling" the field in question:

.. code-block:: python

  >>> uniswapV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

  # Create a partial FieldPath of `pools` created after block 14720000 
  # ordered by their TVL in descending order
  >>> new_pools = uniswapV3.Query.pools(
  ...   orderBy=uniswapV3.Pool.totalValueLockedUSD,
  ...   orderDirection='desc',
  ...   where=[
  ...     uniswapV3.Pool.createdAtBlockNumber > 14720000
  ...   ]
  ... )

  # Select the `totalValueLockedUSD` of the
  >>> pools_tvl = new_pools.totalValueLockedUSD

In the example above, the ``pools_tvl`` ``FieldPath`` represents the following 
GraphQL query:

.. code-block::
  
  query {
    pools(
      orderBy: totalValueLockedUSD,
      orderDirection: desc,
      where: {
        createdAtBlockNumber_gt: 14720000
      }
    ) {
      totalValueLockedUSD
    }
  }

Notice that the values for the ``orderBy`` and ``where`` arguments are 
``FieldPath`` themselves. This allows users to construct complex queries in pure 
Python by using the ``Subgraph`` object returned when loading an API. Note however 
that the ``FieldPaths`` used as argument values are `relative` ``FieldPath``, 
i.e.: they do not start from the root ``Query`` entity, but rather start from an 
entity type (in this case the ``Pool`` entity). It is important to make sure that 
the relative ``FieldPath`` used as values for the ``orderBy`` and ``where`` 
arguments match the entity type of the field on which the arguments are applied 
(in our example, the ``pools`` field is of type ``Pool``). If this is not 
respected, a type error exception will be thrown. 

Argument values can also be supplied in their "raw" form, without the use of 
relative ``FieldPaths``:

.. code-block:: python
  
  >>> uniswapV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

  # This partial FieldPath is equivalant to the `new_pools` FieldPath
  # in the previous example
  >>> new_pools2 = uniswapV3.Query.pools(
  ...   orderBy='totalValueLockedUSD',
  ...   orderDirection='desc',
  ...   where={
  ...     'createdAtBlockNumber_gt': 14720000
  ...   }
  ... )

Querying data
--------------------------------
The ``Subgrounds`` class provides three methods for querying data:

#. ``query``
#. ``query_df``
#. ``query_json`` 

All three methods take the same arguments (ignoring optional arguments), namely 
a list of ``FieldPaths`` and differ only in the way the returned data is 
formatted. This section will go over each method individually.

``Subgrounds.query``
********************************
The ``query`` method returns the data in its simplest form which, depending on 
the ``FieldPaths`` given as argument, will be either: 1) a single value; 2) a 
list of values; or 3) a tuple containing single values or lists of values.

.. code-block:: python

  >>> uniswapV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

  # 1) Single value: Query the quantity of WETH locked on Uniswap V3 
  >>> sg.query([
  ...   uniswapV3.Query.token(id='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2').totalValueLocked
  ... ])
  505722.3421353012

  # Partial FieldPath of top 10 most traded tokens 
  >>> most_traded_tokens = uniswapV3.Query.tokens(
  ...   orderBy=uniswapV3.Token.volumeUSD,
  ...   orderDirection='desc',
  ...   first=10
  ... )

  # 2) List of values: Query symbol of top 10 most traded tokens
  >>> sg.query([
  ...   most_traded_tokens.symbol
  ... ])
  ['WETH', 'USDC', 'USDT', 'DAI', 'WBTC', 'FEI', 'UST', 'APE', 'LOOKS', 'HEX']

  # 3) Tuple of values: Query symbol and TVL of top 10 most traded tokens
  >>> sg.query([
  ...   most_traded_tokens.symbol,
  ...   most_traded_tokens.totalValueLocked
  ... ])
  (['WETH', 'USDC', 'USDT', 'DAI', 'WBTC', 'FEI', 'UST', 'APE', 'LOOKS', 'HEX'],
   [506128.5352471704,
    774159914.44222,
    353261712.912211,
    384420834.9304443,
    11069.25245941,
    42232365.752994545,
    8030872.929855888,
    2604424.599749661,
    11610722.749456603,
    131046877.18519919])


``Subgrounds.query_df``
********************************
This section is under construction!

``Subgrounds.query_json``
********************************
This section is under construction!

Combining FieldPaths
********************************
When passing a list of ``FieldPaths`` as argument to one of the aforementioned 
functions, the ``FieldPaths`` are merged together in a single query 
**if the** ``FieldPaths`` **originate from the same subgraph**:

.. code-block:: python

  >>> uniswapV3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

  # Partial FieldPath selecting the top 4 most traded tokens on Uniswap V3
  >>> most_traded_tokens = uniswapV3.Query.tokens(
  ...   orderBy=uniswapV3.Token.volumeUSD,
  ...   orderDirection='desc',
  ...   first=4
  ... )

  # Partial FieldPath selecting the top 2 pools by TVL of the top 4 most traded tokens
  # (notice the FieldPath starts from most_traded_tokens) 
  >>> most_liquid_pairs = most_traded_tokens.whitelistPools(
  ...   orderBy=uniswapV3.Pool.totalValueLockedUSD,
  ...   orderDirection='desc',
  ...   first=2
  ... )

  # Querying the symbol of the top 4 most traded tokens, their 2 most liquid 
  # pools' token symbols and their 2 most liquid pool's TVL in USD
  >>> sg.query_df([
  ...   most_traded_tokens.symbol,
  ...   most_liquid_pairs.token0.symbol,
  ...   most_liquid_pairs.token1.symbol,
  ...   most_liquid_pairs.totalValueLockedUSD
  ... ])
    tokens_symbol tokens_whitelistPools_token0_symbol tokens_whitelistPools_token1_symbol  tokens_whitelistPools_totalValueLockedUSD
  0          WETH                                USDC                                WETH                               4.068723e+08
  1          WETH                                WBTC                                WETH                               3.311227e+08
  2          USDC                                 DAI                                USDC                               3.284779e+08
  3          USDC                                USDC                                WETH                               4.068723e+08
  4          USDT                                WETH                                USDT                               2.055448e+08
  5          USDT                                USDC                                USDT                               1.980053e+08
  6           DAI                                 DAI                                USDC                               3.284779e+08
  7           DAI                                 DAI                                WETH                               9.759597e+07

Under the hood, when executing the previous code, Subgrounds will combine the 
queried ``FieldPaths`` into the following GraphQL query:

.. code-block::

  query {
    tokens(first: 4, orderBy: volumeUSD, orderDirection: desc) {
      symbol
      whitelistPools(first: 2, orderBy: totalValueLockedUSD, orderDirection: desc) {
        token0 {
          symbol
        }
        token1 {
          symbol
        }
        totalValueLockedUSD
      }
    }
  }

In the cases where the ``FieldPaths`` originate from different subgraphs, then 
multiple queries will be executed concurrently:

SyntheticFields
--------------------------------
This section is under construction!