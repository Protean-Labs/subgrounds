from functools import reduce
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from pipe import map

from subgrounds.dataframe_utils import DataFrameColumns, columns_of_selections, df_of_json
from subgrounds.query import Query
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, SyntheticField
from subgrounds.subgrounds import Subgrounds


class TestColumnsOfSelection(unittest.TestCase):
  sg = Subgrounds()
  subgraph = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/cujowolf/polygon-bridged-carbon')
  
  def test_columns_of_selections_1(self):
    expected = [
      DataFrameColumns(
        key='x6237846447a16188_retirements', 
        fpaths=['x6237846447a16188_tokenAddress', 'x6237846447a16188_retirements_value']
      )
    ]
    
    carbon_offsets = self.subgraph.Query.carbonOffsets(
      orderBy=self.subgraph.CarbonOffset.klimaRanking,
      orderDirection='desc',
      first=100
    )

    fpaths = [
      carbon_offsets.retirements.value,
      carbon_offsets.tokenAddress,
    ]

    query = reduce(Query.add_selection, fpaths | map(FieldPath.selection), Query())

    self.assertEqual(columns_of_selections(query.selection), expected)

  def test_columns_of_selections_2(self):
    expected = [
      DataFrameColumns(
        key='x6237846447a16188_bridges',
        fpaths=['x6237846447a16188_tokenAddress', 'x6237846447a16188_bridges_value']
      ),
      DataFrameColumns(
        key='x6237846447a16188_retirements',
        fpaths=['x6237846447a16188_tokenAddress', 'x6237846447a16188_retirements_value']
      ),
    ]
    
    carbon_offsets = self.subgraph.Query.carbonOffsets(
      orderBy=self.subgraph.CarbonOffset.klimaRanking,
      orderDirection='desc',
      first=100
    )

    fpaths = [
      carbon_offsets.bridges.value,
      carbon_offsets.retirements.value,
      carbon_offsets.tokenAddress,
    ]

    query = reduce(Query.add_selection, fpaths | map(FieldPath.selection), Query())

    self.assertEqual(columns_of_selections(query.selection), expected)


class TestDFOfJSON(unittest.TestCase):
  sg = Subgrounds()
  carbon_offsets = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/cujowolf/polygon-bridged-carbon')
  uniswapV2 = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")

  univ3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', save_schema=True)
  univ3.Swap.tx_type = SyntheticField.constant('SWAP')
  univ3.Mint.tx_type = SyntheticField.constant('MINT')
  univ3.Burn.tx_type = SyntheticField.constant('BURN')

  def test_df_of_json_1(self):
    expected = pd.DataFrame(data={
      'carbonOffsets_retirements_value': [
        1, 2, 3, 4, 5
      ],
      'carbonOffsets_tokenAddress': [
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'
      ]
    })

    json = [{
      'carbonOffsets': [
        {'tokenAddress': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'retirements': [
          {'value': 1},
          {'value': 2},
          {'value': 3},
          {'value': 4},
        ]},
        {'tokenAddress': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8', 'retirements': [
          {'value': 5},
        ]},
        {'tokenAddress': '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27', 'retirements': []}
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('CarbonOffset'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('carbonOffsets', '', [], TypeRef.non_null_list('CarbonOffset'))),
        (None, TypeMeta.FieldMeta('retirements', '', [], TypeRef.non_null_list('Retirement'))),
        (None, TypeMeta.FieldMeta('value', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('CarbonOffset'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('carbonOffsets', '', [], TypeRef.non_null_list('CarbonOffset'))),
        (None, TypeMeta.FieldMeta('tokenAddress', '', [], TypeRef.Named('String'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_multidf_1(self):
    expected = [
      pd.DataFrame(data={
        'carbonOffsets_retirements_value': [
          1, 2, 3, 4, 5
        ],
        'carbonOffsets_tokenAddress': [
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'
        ]
      }),
      pd.DataFrame(data={
        'carbonOffsets_bridges_value': [
          10, 20, 50, 60
        ],
        'carbonOffsets_tokenAddress': [
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
          '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'
        ]
      }),
    ]

    json = [{
      'carbonOffsets': [
        {'tokenAddress': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'retirements': [
          {'value': 1},
          {'value': 2},
          {'value': 3},
          {'value': 4},
        ], 'bridges': [
          {'value': 10},
          {'value': 20},
        ]},
        {'tokenAddress': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8', 'retirements': [
          {'value': 5},
        ], 'bridges': [
          {'value': 50},
          {'value': 60},
        ]},
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('CarbonOffset'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('carbonOffsets', '', [], TypeRef.non_null_list('CarbonOffset'))),
        (None, TypeMeta.FieldMeta('retirements', '', [], TypeRef.non_null_list('Retirement'))),
        (None, TypeMeta.FieldMeta('value', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('CarbonOffset'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('carbonOffsets', '', [], TypeRef.non_null_list('CarbonOffset'))),
        (None, TypeMeta.FieldMeta('bridges', '', [], TypeRef.non_null_list('Bridge'))),
        (None, TypeMeta.FieldMeta('value', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('CarbonOffset'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('carbonOffsets', '', [], TypeRef.non_null_list('CarbonOffset'))),
        (None, TypeMeta.FieldMeta('tokenAddress', '', [], TypeRef.Named('String'))),
      ]),
    ]

    for df in zip(df_of_json(json, fpaths), expected):
      assert_frame_equal(df[0], df[1])

  def test_df_of_json_multidf_2(self):
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

    json = [{
      'mints': [
        {'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'}, 'timestamp': 1643940402, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOKS'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0x369bca127b8858108536b71528ab3befa1deb6fc'}, 'timestamp': 1643940022, 'token0': {'symbol': 'DOC'}, 'token1': {'symbol': 'TOS'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'}, 'timestamp': 1643940004, 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0x3416cf6c708da44db2624d63ea0aaef7113527c6'}, 'timestamp': 1643939765, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'USDT'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'}, 'timestamp': 1643939339, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOKS'}, 'tx_type': 'MINT'}
      ],
      'swaps': [
        {'pool': {'id': '0x512011c2573e0ecbd66be051b9a1c0fd097f2092'}, 'timestamp': 1643940402, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'CMB'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c'}, 'timestamp': 1643940402, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOMI'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643940402, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643940402, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'}, 'timestamp': 1643940402, 'token0': {'symbol': 'STRONG'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'SWAP'}
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('tx_type', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.Named('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),

      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('tx_type', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.Named('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
    ]

    for df in zip(df_of_json(json, fpaths), expected):
      assert_frame_equal(df[0], df[1])

  def test_df_of_json_merge(self):
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

    json = [{
      'mints': [
        {'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'}, 'timestamp': 1643940402, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOKS'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0x369bca127b8858108536b71528ab3befa1deb6fc'}, 'timestamp': 1643940022, 'token0': {'symbol': 'DOC'}, 'token1': {'symbol': 'TOS'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'}, 'timestamp': 1643940004, 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0x3416cf6c708da44db2624d63ea0aaef7113527c6'}, 'timestamp': 1643939765, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'USDT'}, 'tx_type': 'MINT'},
        {'pool': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'}, 'timestamp': 1643939339, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOKS'}, 'tx_type': 'MINT'}
      ],
      'swaps': [
        {'pool': {'id': '0x512011c2573e0ecbd66be051b9a1c0fd097f2092'}, 'timestamp': 1643940402, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'CMB'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c'}, 'timestamp': 1643940402, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOMI'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643940402, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643940402, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'SWAP'},
        {'pool': {'id': '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'}, 'timestamp': 1643940402, 'token0': {'symbol': 'STRONG'}, 'token1': {'symbol': 'WETH'}, 'tx_type': 'SWAP'}
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('tx_type', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.Named('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Mint'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('mints', '', [], TypeRef.non_null_list('Mint'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),

      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('tx_type', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.Named('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths, columns=['timestamp', 'tx_type', 'pool_id', 'token0_symbol', 'token1_symbol'], merge=True), expected)

  def test_df_of_json_3(self):
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

    json = [{
      'swaps': [
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'}, 'timestamp': 1643206992, 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70'}, 'timestamp': 1643206992, 'token0': {'symbol': 'gOHM'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424'}, 'timestamp': 1643206992, 'token0': {'symbol': 'STC'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27'}, 'timestamp': 1643206992, 'token0': {'symbol': 'MIM'}, 'token1': {'symbol': 'USDC'}},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0x11b815efb8f581194ae79006d24e0d814b7697f6'}, 'timestamp': 1643206992, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'USDT'}},
        {'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}},
        {'pool': {'id': '0x60594a405d53811d3bc4766596efd80fd545a270'}, 'timestamp': 1643206992, 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}}
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.Named('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_flat_1(self):
    expected = pd.DataFrame(data={
      'foo_a': ['hello'],
      'foo_b': ['world'],
      'foo_c_x': [10],
      'bar_d': [20],
      'bar_e': [False]
    })

    json = [{
      'foo': {
        'a': 'hello',
        'b': 'world',
        'c': {
          'x': 10
        }
      },
      'bar': {
        'd': 20,
        'e': False
      }
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.Named('Foo'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('foo', '', [], TypeRef.Named('Foo'))),
        (None, TypeMeta.FieldMeta('a', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Foo'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('foo', '', [], TypeRef.Named('Foo'))),
        (None, TypeMeta.FieldMeta('b', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Foo'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('foo', '', [], TypeRef.Named('Foo'))),
        (None, TypeMeta.FieldMeta('c', '', [], TypeRef.Named('C'))),
        (None, TypeMeta.FieldMeta('x', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Bar'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('bar', '', [], TypeRef.Named('Bar'))),
        (None, TypeMeta.FieldMeta('d', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Bar'), TypeRef.Named('Boolean'), [
        (None, TypeMeta.FieldMeta('bar', '', [], TypeRef.Named('Bar'))),
        (None, TypeMeta.FieldMeta('e', '', [], TypeRef.Named('Boolean'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_flat_2(self):
    expected = pd.DataFrame(data=[{
      'token_id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      'token_name': 'Alchemix',
      'token_symbol': 'ALCX'
    }])

    json = [{
      'token': {
        'id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
        'name': 'Alchemix',
        'symbol': 'ALCX'
      }
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_sfield(self):
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

    json = [{
      'swaps': [
        {'amount0': -233931.515098, 'amount1': 87.99294339622642, 'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'price': 2658.5258552452533},
        {'amount0': 2298.7018250845344, 'amount1': -0.8605272976553902, 'pool': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'}, 'timestamp': 1643206992, 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}, 'price': 2671.2712442099437},
        {'amount0': -0.4428024474548323, 'amount1': 0.8605272976553902, 'pool': {'id': '0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70'}, 'timestamp': 1643206992, 'token0': {'symbol': 'gOHM'}, 'token1': {'symbol': 'WETH'}, 'price': 0.514571064347756},
        {'amount0': -1847.299583, 'amount1': 0.6961540545406192, 'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'price': 2653.578717169152},
        {'amount0': 791669.33237876, 'amount1': -1.5528013588977019, 'pool': {'id': '0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424'}, 'timestamp': 1643206992, 'token0': {'symbol': 'STC'}, 'token1': {'symbol': 'WETH'}, 'price': 509832.97241621936},
        {'amount0': -99949.68933919803, 'amount1': 100000.0, 'pool': {'id': '0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27'}, 'timestamp': 1643206992, 'token0': {'symbol': 'MIM'}, 'token1': {'symbol': 'USDC'}, 'price': 0.9994968933919802},
        {'amount0': -106116.660281, 'amount1': 39.972066137717945, 'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'price': 2654.7704568332915},
        {'amount0': 29.51901886792453, 'amount1': -78304.637188, 'pool': {'id': '0x11b815efb8f581194ae79006d24e0d814b7697f6'}, 'timestamp': 1643206992, 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'USDT'}, 'price': 0.00037697663801254737},
        {'amount0': -74534.218793, 'amount1': 28.0, 'pool': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'}, 'timestamp': 1643206992, 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}, 'price': 2661.9363854642856},
        {'amount0': -15043.573567, 'amount1': 5.661734867843959, 'pool': {'id': '0x60594a405d53811d3bc4766596efd80fd545a270'}, 'timestamp': 1643206992, 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}, 'price': 2657.060762848602}
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.Named('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('String'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Swap'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('price', '', [], TypeRef.Named('BigDecimal'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_6(self):
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

    json = [{
      'pools': [
        {'id': '0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801', 'token0': {'symbol': 'UNI'}, 'token1': {'symbol': 'WETH'}, 'swaps': [
          {'amountUSD': 2261.0339380289497, 'timestamp': 1643206408},
          {'amountUSD': 35534.09369471072, 'timestamp': 1643205177}
        ]},
        {'id': '0x6c6bc977e13df9b0de53b251522280bb72383700', 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'USDC'}, 'swaps': [
          {'amountUSD': 5497.8352603453795, 'timestamp': 1643206881},
          {'amountUSD': 243989.9166851893, 'timestamp': 1643206138}
        ]}
      ]
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pools', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pools', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pools', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pools', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pools', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('amountUSD', '', [], TypeRef.Named('BigDecimal'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_semiflat_1(self):
    expected = pd.DataFrame(data={
      'token_id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      'token_name': 'Alchemix',
      'token_symbol': 'ALCX',
      'token_whitelistPools_id': [
        '0x689b322bf5056487eec7f9b2577cd43a37eb6302',
        '0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8'
      ]
    })

    # token = self.univ3.Query.token(id='0xdbdb4d16eda451d0503b854cf79d55697f90c8df')

    json = [{
      'token': {
        'id': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
        'name': 'Alchemix',
        'symbol': 'ALCX',
        'whitelistPools': [
          {'id': '0x689b322bf5056487eec7f9b2577cd43a37eb6302'},
          {'id': '0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8'}
        ]
      }
    }]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('name', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Token'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('token', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('whitelistPools', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_semiflat_2(self):
    expected = pd.DataFrame(data={
      'pool_token0_symbol': 'USDC',
      'pool_token1_symbol': 'WETH',
      'pool_swaps_timestamp': [1620179783, 1620203006, 1620208251, 1620215997]
    })

    json = [
      {'pool': {
        'token0': {'symbol': 'USDC'},
        'token1': {'symbol': 'WETH'},
        'swaps': [
          {'timestamp': 1620179783},
          {'timestamp': 1620203006},
          {'timestamp': 1620208251},
          {'timestamp': 1620215997}
        ]
      }}
    ]

    fpaths = [
      FieldPath(self.carbon_offsets, TypeRef.Named('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('token0', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.Named('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('token1', '', [], TypeRef.Named('Token'))),
        (None, TypeMeta.FieldMeta('symbol', '', [], TypeRef.Named('String'))),
      ]),
      FieldPath(self.carbon_offsets, TypeRef.non_null_list('Pool'), TypeRef.Named('BigInt'), [
        (None, TypeMeta.FieldMeta('pool', '', [], TypeRef.non_null_list('Pool'))),
        (None, TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap'))),
        (None, TypeMeta.FieldMeta('timestamp', '', [], TypeRef.Named('BigInt'))),
      ]),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)

  def test_df_of_json_aliases(self):
    expected = [
      pd.DataFrame(data={
        'mints_timestamp': [1643940402, 1643940022, 1643940004, 1643939765, 1643939339],
        'mints_pair_id': [
          '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011',
          '0x369bca127b8858108536b71528ab3befa1deb6fc',
          '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
          '0x3416cf6c708da44db2624d63ea0aaef7113527c6',
          '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011'
        ],
        'mints_pair_token0_symbol': ['WETH', 'DOC', 'DAI', 'USDC', 'WETH'],
        'mints_pair_token1_symbol': ['LOOKS', 'TOS', 'WETH', 'USDT', 'LOOKS'],
      }),
      pd.DataFrame(data={
        'burns_timestamp': [1643940402, 1643940402, 1643940402, 1643940402, 1643940402],
        'burns_pair_id': [
          '0x512011c2573e0ecbd66be051b9a1c0fd097f2092',
          '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
          '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8'
        ],
        'burns_pair_token0_symbol': ['WETH', 'WETH', 'USDC', 'USDC', 'STRONG'],
        'burns_pair_token1_symbol': ['CMB', 'LOOMI', 'WETH', 'WETH', 'WETH'],
      }),
    ]

    json = [{
      'x7d5c9285fb80ec53': [
        {'pair': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011', 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOKS'}}, 'timestamp': 1643940402},
        {'pair': {'id': '0x369bca127b8858108536b71528ab3befa1deb6fc', 'token0': {'symbol': 'DOC'}, 'token1': {'symbol': 'TOS'}}, 'timestamp': 1643940022},
        {'pair': {'id': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8', 'token0': {'symbol': 'DAI'}, 'token1': {'symbol': 'WETH'}}, 'timestamp': 1643940004},
        {'pair': {'id': '0x3416cf6c708da44db2624d63ea0aaef7113527c6', 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'USDT'}}, 'timestamp': 1643939765},
        {'pair': {'id': '0x4b5ab61593a2401b1075b90c04cbcdd3f87ce011', 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOKS'}}, 'timestamp': 1643939339}
      ],
      'x08e600d65ccd5771': [
        {'pair': {'id': '0x512011c2573e0ecbd66be051b9a1c0fd097f2092', 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'CMB'}}, 'timestamp': 1643940402},
        {'pair': {'id': '0x223203a27dfc1b6f9687e57b9ec7ed68298bb59c', 'token0': {'symbol': 'WETH'}, 'token1': {'symbol': 'LOOMI'}}, 'timestamp': 1643940402},
        {'pair': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}}, 'timestamp': 1643940402},
        {'pair': {'id': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'token0': {'symbol': 'USDC'}, 'token1': {'symbol': 'WETH'}}, 'timestamp': 1643940402},
        {'pair': {'id': '0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8', 'token0': {'symbol': 'STRONG'}, 'token1': {'symbol': 'WETH'}}, 'timestamp': 1643940402}
      ]
    }]

    mints = self.uniswapV2.Query.mints(
      orderBy=self.uniswapV2.Mint.timestamp,
      orderDirection='desc',
      first=10,
      where=[
        self.uniswapV2.Mint.pair == '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'
      ]
    )

    burns = self.uniswapV2.Query.burns(
      orderBy=self.uniswapV2.Burn.timestamp,
      orderDirection='desc',
      first=10,
      where=[
        self.uniswapV2.Burn.pair == '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'
      ]
    )

    fpaths = [
      mints.timestamp,
      mints.pair.id,
      mints.pair.token0.symbol,
      mints.pair.token1.symbol,

      burns.timestamp,
      burns.pair.id,
      burns.pair.token0.symbol,
      burns.pair.token1.symbol,
    ]

    for df in zip(df_of_json(json, fpaths), expected):
      assert_frame_equal(df[0], df[1])