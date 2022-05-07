Getting started
================================

Installation
--------------------------------
Subgrounds can be installed via ``pip`` with the following command:
``pip install subgrounds``

.. Important:: Subgrounds requires ``python>=3.10``

.. Note:: If you run into problems during installation, see `Set up an isolated 
  environment`

Simple example
--------------------------------

.. code-block:: python
  
  >>> from subgrounds.subgrounds import Subgrounds

  # Initialize Subgrounds
  >>> sg = Subgrounds()

  # Load a subgraph
  >>> aaveV2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')

  # Create a SyntheticField on the Borrow entity type using Python arithmetic operators
  >>> aaveV2.Borrow.adjusted_amount = aaveV2.Borrow.amount / 10 ** aaveV2.Borrow.reserve.decimals

  # Create a partial FieldPath representing a selection on the borrows field with arguments
  >>> last10_borrows = aaveV2.Query.borrows(
  ...   orderBy=aaveV2.Borrow.timestamp,
  ...   orderDirection='desc',
  ...   first=10
  ... )

  # Query FieldPaths and format the result into a Pandas DataFrame
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