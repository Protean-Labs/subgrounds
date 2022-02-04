from datetime import datetime
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from subgrounds.subgraph import SyntheticField

from subgrounds.subgrounds import Subgrounds

class TestDataFrame(unittest.TestCase):
  def setUp(self):
    self.sg = Subgrounds()
    self.univ3 = self.sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', save_schema=True)
    self.univ3.Swap.tx_type = SyntheticField.constant('SWAP')
    self.univ3.Mint.tx_type = SyntheticField.constant('MINT')
    self.univ3.Burn.tx_type = SyntheticField.constant('BURN')

  def test_df_of_json1(self):
    expected = pd.DataFrame(data={
      'swaps_timestamp': [
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992
      ],
      'swaps_pool_id': [
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
        '0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424',
        '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x11b815efb8f581194ae79006d24e0d814b7697f6',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x60594a405d53811d3bc4766596efd80fd545a270'
      ],
      'swaps_token0_symbol': [
        'USDC',
        'DAI',
        'gOHM',
        'USDC',
        'STC',
        'MIM',
        'USDC',
        'WETH',
        'USDC',
        'DAI'
      ],
      'swaps_token1_symbol': [
        'WETH',
        'WETH',
        'WETH',
        'WETH',
        'WETH',
        'USDC',
        'WETH',
        'USDT',
        'WETH',
        'WETH'
      ]
    })

    swaps = self.univ3.Query.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='desc',
      first=10,
      where=[
        self.univ3.Swap.timestamp <= 1643207000   # Constant timestamp so test is reproducible
      ]
    )

    json = [{'xdb3bb5af86aa7f94': [{'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'DAI'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'gOHM'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'STC'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'MIM'},
        'token1': {'symbol': 'USDC'}},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0x11b815efb8f581194ae79006d24e0d814b7697f6'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'USDT'}},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'}},
      {'pool': {'id': '0x60594a405d53811d3bc4766596efd80fd545a270'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'DAI'},
        'token1': {'symbol': 'WETH'}}]}]

    df = Subgrounds.df_of_json(json, fpaths=[
      swaps.timestamp,
      swaps.pool.id,
      swaps.token0.symbol,
      swaps.token1.symbol
    ])

    assert_frame_equal(df, expected)

  def test_df_of_json2(self):
    expected = pd.DataFrame(data={
      'swaps_timestamp': [
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992,
        1643206992
      ],
      'swaps_pool_id': [
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
        '0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424',
        '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x11b815efb8f581194ae79006d24e0d814b7697f6',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x60594a405d53811d3bc4766596efd80fd545a270'
      ],
      'swaps_token0_symbol': [
        'USDC',
        'DAI',
        'gOHM',
        'USDC',
        'STC',
        'MIM',
        'USDC',
        'WETH',
        'USDC',
        'DAI'
      ],
      'swaps_token1_symbol': [
        'WETH',
        'WETH',
        'WETH',
        'WETH',
        'WETH',
        'USDC',
        'WETH',
        'USDT',
        'WETH',
        'WETH'
      ],
      'swaps_price': [
        2658.5258552452533,
        2671.2712442099437,
        0.514571064347756,
        2653.578717169152,
        509832.97241621936,
        0.9994968933919802,
        2654.7704568332915,
        0.00037697663801254737,
        2661.9363854642856,
        2657.060762848602
      ]
    })

    self.univ3.Swap.price = abs(self.univ3.Swap.amount0) / abs(self.univ3.Swap.amount1)

    swaps = self.univ3.Query.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='desc',
      first=10,
      where=[
        self.univ3.Swap.timestamp <= 1643207000   # Constant timestamp so test is reproducible
      ]
    )

    json = [{'xdb3bb5af86aa7f94': [{'amount0': -233931.515098,
        'amount1': 87.99294339622642,
        'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'price': 2658.5258552452533},
      {'amount0': 2298.7018250845344,
        'amount1': -0.8605272976553902,
        'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'DAI'},
        'token1': {'symbol': 'WETH'},
        'price': 2671.2712442099437},
      {'amount0': -0.4428024474548323,
        'amount1': 0.8605272976553902,
        'pool': {'id': '0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'gOHM'},
        'token1': {'symbol': 'WETH'},
        'price': 0.514571064347756},
      {'amount0': -1847.299583,
        'amount1': 0.6961540545406192,
        'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'price': 2653.578717169152},
      {'amount0': 791669.33237876,
        'amount1': -1.5528013588977019,
        'pool': {'id': '0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'STC'},
        'token1': {'symbol': 'WETH'},
        'price': 509832.97241621936},
      {'amount0': -99949.68933919803,
        'amount1': 100000.0,
        'pool': {'id': '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'MIM'},
        'token1': {'symbol': 'USDC'},
        'price': 0.9994968933919802},
      {'amount0': -106116.660281,
        'amount1': 39.972066137717945,
        'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'price': 2654.7704568332915},
      {'amount0': 29.51901886792453,
        'amount1': -78304.637188,
        'pool': {'id': '0x11b815efb8f581194ae79006d24e0d814b7697f6'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'USDT'},
        'price': 0.00037697663801254737},
      {'amount0': -74534.218793,
        'amount1': 28.0,
        'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'price': 2661.9363854642856},
      {'amount0': -15043.573567,
        'amount1': 5.661734867843959,
        'pool': {'id': '0x60594a405d53811d3bc4766596efd80fd545a270'},
        'timestamp': 1643206992,
        'token0': {'symbol': 'DAI'},
        'token1': {'symbol': 'WETH'},
        'price': 2657.060762848602}]}]

    df = Subgrounds.df_of_json(json, fpaths=[
      swaps.timestamp,
      swaps.pool.id,
      swaps.token0.symbol,
      swaps.token1.symbol,
      swaps.price
    ])

    assert_frame_equal(df, expected)

  def test_df_of_json3(self):
    expected = pd.DataFrame(data={
      'pools_id': [
        '0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801',
        '0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801',
        '0x6c6bc977e13df9b0de53b251522280bb72383700',
        '0x6c6bc977e13df9b0de53b251522280bb72383700'
      ],
      'pools_token0_symbol': [
        'UNI',
        'UNI',
        'DAI',
        'DAI'
      ],
      'pools_token1_symbol': [
        'WETH',
        'WETH',
        'USDC',
        'USDC'
      ],
      'pools_swaps_timestamp': [
        1643206408,
        1643205177,
        1643206881,
        1643206138
      ],
      'pools_swaps_amountUSD': [
        2261.033938028949777104204086349881,
        35534.09369471071499539246013240419,
        5497.8352603453796463465,
        243989.9166851892808097625
      ],
    })

    first_two_pools = self.univ3.Query.pools(
      orderBy=self.univ3.Pool.createdAtTimestamp,
      orderDirection='asc',
      first=2
    )

    latest_swaps = first_two_pools.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='desc',
      first=2,
      where=[
        self.univ3.Swap.timestamp <= 1643207000   # Constant timestamp so test is reproducible
      ]
    )

    json = [{'x210c83be02d64e23': [{'id': '0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801',
      'token0': {'symbol': 'UNI'},
      'token1': {'symbol': 'WETH'},
      'x17ad4517c7e7e8f3': [{'amountUSD': 2261.0339380289497,
        'timestamp': 1643206408},
        {'amountUSD': 35534.09369471072, 'timestamp': 1643205177}]},
      {'id': '0x6c6bc977e13df9b0de53b251522280bb72383700',
      'token0': {'symbol': 'DAI'},
      'token1': {'symbol': 'USDC'},
      'x17ad4517c7e7e8f3': [{'amountUSD': 5497.8352603453795,
        'timestamp': 1643206881},
        {'amountUSD': 243989.9166851893, 'timestamp': 1643206138}]}]}]

    df = Subgrounds.df_of_json(json, fpaths=[
      first_two_pools.id,
      first_two_pools.token0.symbol,
      first_two_pools.token1.symbol,
      latest_swaps.timestamp,
      latest_swaps.amountUSD
    ])

    assert_frame_equal(df, expected)

  def test_df_of_json4(self):
    expected = pd.DataFrame(data=[{
      'token_id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      'token_name': 'Alchemix',
      'token_symbol': 'ALCX'
    }])

    token = self.univ3.Query.token(id='0xdbdb4d16eda451d0503b854cf79d55697f90c8df')

    json = [{'x1cd7b9981b117967': {'id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      'name': 'Alchemix',
      'symbol': 'ALCX'}}]

    df = Subgrounds.df_of_json(json, fpaths=[
      token.id,
      token.name,
      token.symbol
    ])

    assert_frame_equal(df, expected)

  def test_df_of_json5(self):
    expected = pd.DataFrame(data={
      'token_id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      'token_name': 'Alchemix',
      'token_symbol': 'ALCX',
      'token_whitelistPools_id': [
        '0x689b322bf5056487eec7f9b2577cd43a37eb6302',
        '0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8'
      ]
    })

    token = self.univ3.Query.token(id='0xdbdb4d16eda451d0503b854cf79d55697f90c8df')

    json = [{'x1cd7b9981b117967': {'id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      'name': 'Alchemix',
      'symbol': 'ALCX',
      'whitelistPools': [
        {'id': '0x689b322bf5056487eec7f9b2577cd43a37eb6302'},
        {'id': '0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8'}]}}]

    df = Subgrounds.df_of_json(json, fpaths=[
      token.id,
      token.name,
      token.symbol,
      token.whitelistPools.id
    ])

    assert_frame_equal(df, expected)

  def test_df_of_json6(self):
    expected = [
      pd.DataFrame(data={
        'mints_timestamp': [1643940402, 1643940022, 1643940004, 1643939765, 1643939339],
        'mints_tx_type': ['MINT', 'MINT', 'MINT', 'MINT', 'MINT'],
        'mints_pool_id': [
          '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011',
          '0x369bca127b8858108536b71528ab3befa1deb6fc',
          '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
          '0x3416cf6c708da44db2624d63ea0aaef7113527c6',
          '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'
        ],
        'mints_token0_symbol': ['WETH', 'DOC', 'DAI', 'USDC', 'WETH'],
        'mints_token1_symbol': ['LOOKS', 'TOS', 'WETH', 'USDT', 'LOOKS'],
      }),
      pd.DataFrame(data={
        'swaps_timestamp': [1643940402, 1643940402, 1643940402, 1643940402, 1643940402],
        'swaps_tx_type': ['SWAP', 'SWAP', 'SWAP', 'SWAP', 'SWAP'],
        'swaps_pool_id': [
          '0x512011c2573e0ecbd66be051b9a1c0fd097f2092',
          '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'
        ],
        'swaps_token0_symbol': ['WETH', 'WETH', 'USDC', 'USDC', 'STRONG'],
        'swaps_token1_symbol': ['CMB', 'LOOMI', 'WETH', 'WETH', 'WETH'],
      }),
    ]

    swaps = self.univ3.Query.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='desc',
      first=5,
      where=[
        self.univ3.Swap.timestamp <= 1643940402   # Constant timestamp so test is reproducible
      ]
    )

    mints = self.univ3.Query.mints(
      orderBy=self.univ3.Mint.timestamp,
      orderDirection='desc',
      first=5,
      where=[
        self.univ3.Swap.timestamp <= 1643940402   # Constant timestamp so test is reproducible
      ]
    )

    json = [{'x6f9d8ef399e0496f': [{'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'LOOKS'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0x369bca127b8858108536b71528ab3befa1deb6fc'},
        'timestamp': 1643940022,
        'token0': {'symbol': 'DOC'},
        'token1': {'symbol': 'TOS'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'},
        'timestamp': 1643940004,
        'token0': {'symbol': 'DAI'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0x3416cf6c708da44db2624d63ea0aaef7113527c6'},
        'timestamp': 1643939765,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'USDT'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'},
        'timestamp': 1643939339,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'LOOKS'},
        'tx_type': 'MINT'}],
      'x71d68b7e4ef012bd': [{'pool': {'id': '0x512011c2573e0ecbd66be051b9a1c0fd097f2092'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'CMB'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'LOOMI'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'STRONG'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'SWAP'}]}]

    dfs = Subgrounds.df_of_json(json, fpaths=[
      swaps.timestamp,
      swaps.tx_type,
      swaps.pool.id,
      swaps.token0.symbol,
      swaps.token1.symbol,

      mints.timestamp,
      mints.tx_type,
      mints.pool.id,
      mints.token0.symbol,
      mints.token1.symbol,
    ])

    for i in range(len(expected)):
      assert_frame_equal(dfs[i], expected[i])

  def test_df_of_json7(self):
    expected = pd.DataFrame(data={
      'timestamp': [1643940402, 1643940022, 1643940004, 1643939765, 1643939339, 1643940402, 1643940402, 1643940402, 1643940402, 1643940402],
      'tx_type': ['MINT', 'MINT', 'MINT', 'MINT', 'MINT', 'SWAP', 'SWAP', 'SWAP', 'SWAP', 'SWAP'],
      'pool_id': [
        '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011',
        '0x369bca127b8858108536b71528ab3befa1deb6fc',
        '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
        '0x3416cf6c708da44db2624d63ea0aaef7113527c6',
        '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011',
        '0x512011c2573e0ecbd66be051b9a1c0fd097f2092',
        '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'
      ],
      'token0_symbol': ['WETH', 'DOC', 'DAI', 'USDC', 'WETH', 'WETH', 'WETH', 'USDC', 'USDC', 'STRONG'],
      'token1_symbol': ['LOOKS', 'TOS', 'WETH', 'USDT', 'LOOKS', 'CMB', 'LOOMI', 'WETH', 'WETH', 'WETH'],
    })

    swaps = self.univ3.Query.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='desc',
      first=5,
      where=[
        self.univ3.Swap.timestamp <= 1643940402   # Constant timestamp so test is reproducible
      ]
    )

    mints = self.univ3.Query.mints(
      orderBy=self.univ3.Mint.timestamp,
      orderDirection='desc',
      first=5,
      where=[
        self.univ3.Swap.timestamp <= 1643940402   # Constant timestamp so test is reproducible
      ]
    )

    json = [{'x6f9d8ef399e0496f': [{'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'LOOKS'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0x369bca127b8858108536b71528ab3befa1deb6fc'},
        'timestamp': 1643940022,
        'token0': {'symbol': 'DOC'},
        'token1': {'symbol': 'TOS'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'},
        'timestamp': 1643940004,
        'token0': {'symbol': 'DAI'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0x3416cf6c708da44db2624d63ea0aaef7113527c6'},
        'timestamp': 1643939765,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'USDT'},
        'tx_type': 'MINT'},
      {'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'},
        'timestamp': 1643939339,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'LOOKS'},
        'tx_type': 'MINT'}],
      'x71d68b7e4ef012bd': [{'pool': {'id': '0x512011c2573e0ecbd66be051b9a1c0fd097f2092'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'CMB'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'WETH'},
        'token1': {'symbol': 'LOOMI'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'SWAP'},
      {'pool': {'id': '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'},
        'timestamp': 1643940402,
        'token0': {'symbol': 'STRONG'},
        'token1': {'symbol': 'WETH'},
        'tx_type': 'SWAP'}]}]

    dfs = Subgrounds.df_of_json(json, fpaths=[
      swaps.timestamp,
      swaps.tx_type,
      swaps.pool.id,
      swaps.token0.symbol,
      swaps.token1.symbol,

      mints.timestamp,
      mints.tx_type,
      mints.pool.id,
      mints.token0.symbol,
      mints.token1.symbol,
    ], columns=['timestamp', 'tx_type', 'pool_id', 'token0_symbol', 'token1_symbol'], merge=True)

    assert_frame_equal(dfs, expected)

  def test_df_of_json8(self):
    expected = pd.DataFrame(data={
      'pool_token0_symbol': 'USDC',
      'pool_token1_symbol': 'WETH',
      'pool_swaps_timestamp': [1620179783, 1620203006, 1620208251, 1620215997]
    })

    json = [
      {'xf484914bcadb103b': {
        'token0': {'symbol': 'USDC'}, 
        'token1': {'symbol': 'WETH'}, 
        'xd651e253b2c90f7f': [
          {'timestamp': 1620179783}, 
          {'timestamp': 1620203006}, 
          {'timestamp': 1620208251}, 
          {'timestamp': 1620215997}
        ]
      }}
    ]

    pool = self.univ3.Query.pool(id='0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8')

    swaps = pool.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='asc',
      first=4
    )

    df = Subgrounds.df_of_json(json, fpaths=[
      pool.token0.symbol,
      pool.token1.symbol,
      swaps.timestamp
    ])

    assert_frame_equal(df, expected)
