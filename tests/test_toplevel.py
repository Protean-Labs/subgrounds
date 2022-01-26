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

  def test_query_df_1(self):
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

    df = self.sg.query_df([
      swaps.timestamp,
      swaps.pool.id,
      swaps.token0.symbol,
      swaps.token1.symbol
    ])

    assert_frame_equal(df, expected)

  def test_query_df_2(self):
    expected = pd.DataFrame(data={
      'swaps_datetime': [
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12',
        '2022-01-26 09:23:12'
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

    self.univ3.Swap.datetime = SyntheticField(
      lambda timestamp: str(datetime.fromtimestamp(timestamp)),
      SyntheticField.STRING,
      self.univ3.Swap.timestamp,
    )

    swaps = self.univ3.Query.swaps(
      orderBy=self.univ3.Swap.timestamp,
      orderDirection='desc',
      first=10,
      where=[
        self.univ3.Swap.timestamp <= 1643207000   # Constant timestamp so test is reproducible
      ]
    )

    df = self.sg.query_df([
      swaps.datetime,
      swaps.pool.id,
      swaps.token0.symbol,
      swaps.token1.symbol
    ])

    assert_frame_equal(df, expected)

  def test_query_df_3(self):
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

    df = self.sg.query_df([
      first_two_pools.id,
      first_two_pools.token0.symbol,
      first_two_pools.token1.symbol,
      latest_swaps.timestamp,
      latest_swaps.amountUSD
    ])

    assert_frame_equal(df, expected)