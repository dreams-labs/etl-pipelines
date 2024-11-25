"""
tests used to audit the files in the etl-pipelines repository
"""
# pylint: disable=W1203 # fstrings in logs
# pylint: disable=C0301 # line over 100 chars
# pylint: disable=C0302 # file over 1000 lines
# pylint: disable=E0401 # can't find import (due to local import)
# pylint: disable=C0413 # import not at top of doc (due to local import)
# pylint: disable=W0612 # unused variables (due to test reusing functions with 2 outputs)
# pylint: disable=W0621 # redefining from outer scope triggering on pytest fixtures


import sys
import os
import pandas as pd
import numpy as np
import pytest
from dotenv import load_dotenv
from dreams_core import core as dc

# Project Modules
# pyright: reportMissingImports=false
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../cloud_functions/core_coin_wallet_profits')))
import core_coin_wallet_profits as cwp

load_dotenv()
logger = dc.setup_logger()




# ===================================================== #
#                                                       #
#                 U N I T   T E S T S                   #
#                                                       #
# ===================================================== #

# ---------------------------------------- #
# merge_prices_and_transfers() unit tests
# ---------------------------------------- #

@pytest.mark.unit
def test_merge_prices_and_transfers_basic():
    """
    Tests the basic functionality of merge_prices_and_transfers with complete matching data.

    Validates that:
    1. The merge preserves all transfer records
    2. Price data is correctly aligned with transfer dates
    3. No data is lost or corrupted in the merge process
    4. DataFrame structure and data types are maintained
    """
    # Create test transfer data
    transfers_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin1', 'coin1'],
        'wallet_address': ['wallet1', 'wallet1', 'wallet1', 'wallet1'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']),
        'net_transfers': [100.0, 50.0, -30.0, 20.0],
        'balance': [100.0, 150.0, 120.0, 140.0]
    })

    # Create test price data with matching dates
    prices_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin1', 'coin1'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']),
        'price': [1.0, 1.1, 1.2, 1.15]
    })

    # Execute merge operation
    result_df = cwp.merge_prices_and_transfers(transfers_df, prices_df)

    # Calculate expected results
    # The merged DataFrame should:
    # 1. Have all rows from transfers_df (4 rows)
    # 2. Include price data for each date
    # 3. Maintain the original column structure plus price
    expected_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin1', 'coin1'],
        'wallet_address': ['wallet1', 'wallet1', 'wallet1', 'wallet1'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']),
        'net_transfers': [100.0, 50.0, -30.0, 20.0],
        'balance': [100.0, 150.0, 120.0, 140.0],
        'price': [1.0, 1.1, 1.2, 1.15]
    })

    # Verify results
    assert len(result_df) == len(transfers_df), "Should preserve all transfer records"

    assert set(result_df.columns) == {'coin_id', 'wallet_address', 'date', 'net_transfers',
                                     'balance', 'price'}, "Should have all expected columns"

    assert result_df['coin_id'].dtype == 'category', "coin_id should be categorical"

    # Compare DataFrames
    # Note: We exclude coin_id from the comparison because the result will be categorical
    # while our expected_df has string type
    numeric_cols = ['net_transfers', 'balance', 'price']
    for col in numeric_cols:
        assert np.allclose(result_df[col], expected_df[col], equal_nan=True), \
            f"Mismatch in {col} values"

    # Compare dates
    assert (result_df['date'] == expected_df['date']).all(), "Date alignment mismatch"

    # Compare wallet addresses
    assert (result_df['wallet_address'] == expected_df['wallet_address']).all(), \
        "Wallet address mismatch"


@pytest.mark.unit
def test_merge_prices_and_transfers_multiple_wallets_coins():
    """
    Tests merge_prices_and_transfers functionality with multiple wallets trading multiple coins.

    Validates that:
    1. All wallet-coin combinations are correctly merged with their prices
    2. Price data is consistent across wallets for the same coin and date
    3. Transfers and balances remain specific to each wallet-coin pair
    4. Date alignment is maintained across all combinations
    """
    # Create test transfer data with 2 wallets and 2 coins
    transfers_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin2', 'coin2',  # Wallet 1 transactions
                    'coin1', 'coin1', 'coin2', 'coin2'],   # Wallet 2 transactions
        'wallet_address': ['wallet1', 'wallet1', 'wallet1', 'wallet1',
                            'wallet2', 'wallet2', 'wallet2', 'wallet2'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02'] * 4),  # Same dates for all
        'net_transfers': [100.0, 50.0,    # Wallet 1, Coin 1
                            200.0, -50.0,    # Wallet 1, Coin 2
                            75.0, 25.0,      # Wallet 2, Coin 1
                            150.0, -30.0],   # Wallet 2, Coin 2
        'balance': [100.0, 150.0,    # Wallet 1, Coin 1
                    200.0, 150.0,     # Wallet 1, Coin 2
                    75.0, 100.0,      # Wallet 2, Coin 1
                    150.0, 120.0]     # Wallet 2, Coin 2
    })

    # Create test price data for both coins
    prices_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin2', 'coin2'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02'] * 2),
        'price': [1.0, 1.1,     # Coin 1 prices
                    2.0, 1.9]      # Coin 2 prices
    })

    # Execute merge operation
    result_df = cwp.merge_prices_and_transfers(transfers_df, prices_df)

    # Calculate expected results
    # The merged DataFrame should:
    # 1. Have all 8 rows (2 dates × 2 wallets × 2 coins)
    # 2. Maintain wallet-specific transfer and balance data
    # 3. Share same price for same coin-date combinations
    expected_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin2', 'coin2',
                    'coin1', 'coin1', 'coin2', 'coin2'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02'] * 4),
        'wallet_address': ['wallet1', 'wallet1', 'wallet1', 'wallet1',
                            'wallet2', 'wallet2', 'wallet2', 'wallet2'],
        'net_transfers': [100.0, 50.0, 200.0, -50.0,
                            75.0, 25.0, 150.0, -30.0],
        'balance': [100.0, 150.0, 200.0, 150.0,
                    75.0, 100.0, 150.0, 120.0],
        'price': [1.0, 1.1, 2.0, 1.9,    # Same prices repeated for each wallet
                    1.0, 1.1, 2.0, 1.9]
    }).sort_values(by=['coin_id','wallet_address','date'])

    # Verify results
    # Check record count
    assert len(result_df) == len(transfers_df), "Should preserve all transfer records"

    # Verify column structure
    assert set(result_df.columns) == {'coin_id', 'wallet_address', 'date', 'net_transfers',
                                        'balance', 'price'}, "Should have all expected columns"

    # Verify coin_id is categorical
    assert result_df['coin_id'].dtype == 'category', "coin_id should be categorical"

    # Compare numeric columns
    numeric_cols = ['net_transfers', 'balance', 'price']
    for col in numeric_cols:
        assert np.allclose(result_df[col], expected_df[col], equal_nan=True), \
            f"Mismatch in {col} values"

    # Verify price consistency for same coin-date combinations
    for coin in ['coin1', 'coin2']:
        for date in ['2024-01-01', '2024-01-02']:
            mask = (result_df['coin_id'] == coin) & (result_df['date'] == pd.to_datetime(date))
            unique_prices = result_df.loc[mask, 'price'].unique()
            assert len(unique_prices) == 1, \
                f"Multiple prices found for {coin} on {date}"

    # Verify wallet-specific data maintained
    for wallet in ['wallet1', 'wallet2']:
        wallet_mask = result_df['wallet_address'] == wallet
        assert not result_df[wallet_mask]['balance'].equals(
            result_df[~wallet_mask]['balance']), \
            "Balance values should differ between wallets"



# ---------------------------------------- #
# add_first_price_info() unit tests
# ---------------------------------------- #

@pytest.mark.unit
def test_add_first_price_info_standard():
    """
    Tests add_first_price_info with standard scenario where price data starts after transfers begin.

    Tests that:
    1. First price date is correctly identified from prices_df for each coin
    2. First price value is correctly captured from prices_df
    3. Original transfer data is preserved
    4. New columns are properly added with correct values
    5. Handles dates with no transfers correctly
    """
    # Create test profits_df (merged transfers and prices)
    profits_df = pd.DataFrame({
        'coin_id': ['coin1'] * 6 + ['coin2'] * 6,
        'wallet_address': ['wallet1'] * 6 + ['wallet1'] * 6,
        'date': pd.to_datetime([
            # Coin1 dates
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06',
            # Coin2 dates
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06'
        ]),
        'net_transfers': [10.0] * 12,  # Dummy values
        'balance': [100.0] * 12,       # Dummy values
        'price': [
            # Coin1 prices: null before Jan 3
            np.nan, np.nan, 1.0, 1.1, 1.2, 1.1,
            # Coin2 prices: null before Jan 4
            np.nan, np.nan, np.nan, 2.0, 2.1, 2.2
        ]
    })
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Create test prices_df
    prices_df = pd.DataFrame({
        'coin_id': ['coin1'] * 4 + ['coin2'] * 3,
        'date': pd.to_datetime([
            # Coin1 prices start Jan 3
            '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06',
            # Coin2 prices start Jan 4
            '2024-01-04', '2024-01-05', '2024-01-06'
        ]),
        'price': [
            # Coin1 prices
            1.0, 1.1, 1.2, 1.1,
            # Coin2 prices
            2.0, 2.1, 2.2
        ]
    })

    # Execute function
    result_df = cwp.add_first_price_info(profits_df, prices_df)

    # Verify results
    # Check that all original records are preserved
    assert len(result_df) == len(profits_df), "Should preserve all records"

    # Verify new columns exist
    assert 'first_price_date' in result_df.columns, "Should have first_price_date column"
    assert 'first_price' in result_df.columns, "Should have first_price column"

    # Verify all original columns exist in result
    assert all(col in result_df.columns for col in profits_df.columns), \
        "All original columns should be present"

    # Compare original data
    result_orig_cols = result_df[profits_df.columns]

    # Compare numeric columns
    numeric_cols = ['net_transfers', 'balance', 'price']
    for col in numeric_cols:
        assert np.allclose(result_orig_cols[col], profits_df[col],
                            equal_nan=True), f"Original {col} data was modified"

    # Compare date column
    assert (result_orig_cols['date'] == profits_df['date']).all(), "Date data was modified"

    # Compare wallet_address column
    assert (result_orig_cols['wallet_address'] ==
            profits_df['wallet_address']).all(), "Wallet address data was modified"

    # Check first price info for Coin1 (from prices_df)
    coin1_mask = result_df['coin_id'] == 'coin1'
    assert (result_df[coin1_mask]['first_price_date'] ==
            pd.to_datetime('2024-01-03')).all(), "Incorrect first_price_date for coin1"
    assert np.allclose(result_df[coin1_mask]['first_price'],
                        1.0, equal_nan=True), "Incorrect first_price for coin1"

    # Check first price info for Coin2 (from prices_df)
    coin2_mask = result_df['coin_id'] == 'coin2'
    assert (result_df[coin2_mask]['first_price_date'] ==
            pd.to_datetime('2024-01-04')).all(), "Incorrect first_price_date for coin2"
    assert np.allclose(result_df[coin2_mask]['first_price'],
                        2.0, equal_nan=True), "Incorrect first_price for coin2"

    # Verify consistent first price info within each coin
    for coin in ['coin1', 'coin2']:
        coin_mask = result_df['coin_id'] == coin
        assert len(result_df[coin_mask]['first_price_date'].unique()) == 1, \
            f"Inconsistent first_price_date for {coin}"
        assert len(result_df[coin_mask]['first_price'].unique()) == 1, \
            f"Inconsistent first_price for {coin}"

@pytest.mark.unit
def test_add_first_price_info_staggered_starts():
    """
    Tests add_first_price_info with multiple coins having different price data start dates.
    Tests both recent and historical first prices and ensures each coin's first price info
    is independently tracked, even when transfers occur before prices exist.

    Validates that:
    1. First prices are correctly identified from prices_df regardless of when they appear
    2. First price dates are properly captured even with large gaps
    3. Each coin's first price info is independent of other coins
    4. All coins maintain consistent first price info across their records
    """
    # Create test profits_df (with transfers and merged prices)
    profits_df = pd.DataFrame({
        'coin_id': (['coin1'] * 7 + ['coin2'] * 7 + ['coin3'] * 7),
        'wallet_address': ['wallet1'] * 21,  # Not relevant for this test
        'date': pd.to_datetime([
            # Coin1 - early transfers
            '2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
            '2024-01-01', '2024-01-02',
            # Coin2 - mid-range transfers
            '2023-06-01', '2023-06-02', '2023-06-03', '2023-06-04', '2023-06-05',
            '2024-01-01', '2024-01-02',
            # Coin3 - recent transfers
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
            '2024-01-06', '2024-01-07'
        ]),
        'net_transfers': [1.0] * 21,  # Not relevant for this test
        'balance': [100.0] * 21,      # Not relevant for this test
        'price': [
            # Coin1 prices: Start at 0.1 in Jan 2023
            np.nan, 0.1, 0.2, 0.3, 0.4, 1.1, 1.2,
            # Coin2 prices: Start at 0.5 in June 2023
            np.nan, np.nan, 0.5, 0.6, 0.7, 2.1, 2.2,
            # Coin3 prices: Start at 1.0 in Jan 2024
            np.nan, np.nan, 1.0, 1.1, 1.2, 1.3, 1.4
        ]
    })
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Create test prices_df with staggered start dates
    prices_df = pd.DataFrame({
        'coin_id': (['coin1'] * 6 + ['coin2'] * 5 + ['coin3'] * 5),
        'date': pd.to_datetime([
            # Coin1 - early start (2023-01)
            '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
            '2024-01-01', '2024-01-02',
            # Coin2 - mid-range start (2023-06)
            '2023-06-03', '2023-06-04', '2023-06-05',
            '2024-01-01', '2024-01-02',
            # Coin3 - recent start (2024-01)
            '2024-01-03', '2024-01-04', '2024-01-05',
            '2024-01-06', '2024-01-07'
        ]),
        'price': [
            # Coin1 prices
            0.1, 0.2, 0.3, 0.4, 1.1, 1.2,
            # Coin2 prices
            0.5, 0.6, 0.7, 2.1, 2.2,
            # Coin3 prices
            1.0, 1.1, 1.2, 1.3, 1.4
        ]
    })

    # Execute function
    result_df = cwp.add_first_price_info(profits_df, prices_df)

    # Verify results
    # Check basic structure
    assert len(result_df) == len(profits_df), "Should preserve all records"
    assert 'first_price_date' in result_df.columns, "Should have first_price_date column"
    assert 'first_price' in result_df.columns, "Should have first_price column"

    # Verify all original columns exist and data is preserved
    assert all(col in result_df.columns for col in profits_df.columns), \
        "All original columns should be present"

    result_orig_cols = result_df[profits_df.columns]
    numeric_cols = ['net_transfers', 'balance', 'price']
    for col in numeric_cols:
        assert np.allclose(result_orig_cols[col], profits_df[col],
                          equal_nan=True), f"Original {col} data was modified"

    # Define expected first price info for each coin (from prices_df)
    expected_first_prices = {
        'coin1': {'date': '2023-01-02', 'price': 0.1},
        'coin2': {'date': '2023-06-03', 'price': 0.5},
        'coin3': {'date': '2024-01-03', 'price': 1.0}
    }

    # Verify first price info for each coin
    for coin, expected in expected_first_prices.items():
        coin_mask = result_df['coin_id'] == coin

        # Check first price date
        assert (result_df[coin_mask]['first_price_date'] ==
                pd.to_datetime(expected['date'])).all(), \
            f"Incorrect first_price_date for {coin}"

        # Check first price value
        assert np.allclose(result_df[coin_mask]['first_price'],
                          expected['price'], equal_nan=True), \
            f"Incorrect first_price for {coin}"

        # Verify consistency within each coin
        assert len(result_df[coin_mask]['first_price_date'].unique()) == 1, \
            f"Inconsistent first_price_date for {coin}"
        assert len(result_df[coin_mask]['first_price'].unique()) == 1, \
            f"Inconsistent first_price for {coin}"


@pytest.mark.unit
def test_add_first_price_info_early_transfer_gap():
    """
    Tests add_first_price_info with early transfer followed by gap before first price.

    Tests scenario where:
    1. Wallet has very early transfer (e.g., Jan 1)
    2. No activity during gap period
    3. First price appears later (e.g., Jan 15)
    4. Activity resumes after price exists

    Validates that first_price_info is correctly identified even when there's no
    transfer activity near the first price date.
    """
    # Create test profits_df with gap in transfer activity
    profits_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5,
        'wallet_address': ['wallet1'] * 5,
        'date': pd.to_datetime([
            # Early transfer
            '2024-01-01',
            # No transfers between Jan 1 and Jan 15
            # Activity resumes after price exists
            '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'
        ]),
        'net_transfers': [
            100.0,    # Initial transfer
            50.0,     # Resume activity
            -30.0,
            20.0,
            -10.0
        ],
        'balance': [
            100.0,    # Initial balance
            150.0,
            120.0,
            140.0,
            130.0
        ],
        'price': [
            np.nan,    # No price yet
            1.2,       # Prices exist for all later transfers
            1.3,
            1.1,
            1.4
        ]
    })
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Create test prices_df starting Jan 15
    prices_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5,
        'date': pd.to_datetime([
            '2024-01-15',  # First price
            '2024-01-16',
            '2024-01-17',
            '2024-01-18',
            '2024-01-19'
        ]),
        'price': [
            1.0,     # First price
            1.2,
            1.3,
            1.1,
            1.4
        ]
    })

    # Execute function
    result_df = cwp.add_first_price_info(profits_df, prices_df)

    # Verify results
    # Check record count preserved
    assert len(result_df) == len(profits_df), "Should preserve all records"

    # Verify new columns exist
    assert 'first_price_date' in result_df.columns, "Should have first_price_date column"
    assert 'first_price' in result_df.columns, "Should have first_price column"

    # Verify all original data preserved
    result_orig_cols = result_df[profits_df.columns]
    numeric_cols = ['net_transfers', 'balance', 'price']
    for col in numeric_cols:
        assert np.allclose(result_orig_cols[col], profits_df[col],
                          equal_nan=True), f"Original {col} data was modified"

    # Verify first price info (should come from prices_df)
    assert (result_df['first_price_date'] ==
            pd.to_datetime('2024-01-15')).all(), "Incorrect first_price_date"
    assert np.allclose(result_df['first_price'],
                      1.0, equal_nan=True), "Incorrect first_price"

    # Verify consistency of first price info
    assert len(result_df['first_price_date'].unique()) == 1, "Inconsistent first_price_date"
    assert len(result_df['first_price'].unique()) == 1, "Inconsistent first_price"



# ---------------------------------------- #
# create_imputed_records() unit tests
# ---------------------------------------- #

@pytest.mark.unit
def test_create_imputed_records_multiple_wallets():
    """
    Tests create_imputed_records with multiple wallets having activity before price data exists.
    Only wallets with pre-price activity AND no activity on first_price_date should get
    imputed records.

    Validates that:
    1. Imputed record is only created for wallet1 (has pre-price activity, no first_price_date activity)
    2. No imputed record for wallet2 (has activity on first_price_date)
    3. Imputed record uses the latest balance before first price date
    4. Record is created with correct coin_id, wallet_address, and value mappings
    """
    # Create test data with multiple wallets having pre-price activity
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 8,  # Same coin for simplicity
        'wallet_address': ['wallet1', 'wallet1', 'wallet1', 'wallet1',
                            'wallet2', 'wallet2', 'wallet2', 'wallet2'],
        'date': pd.to_datetime([
            # Wallet1 dates (2 pre-price, 2 post-price)
            '2024-01-01', '2024-01-02', '2024-01-04', '2024-01-05',
            # Wallet2 dates (2 pre-price, 1 on price date, 1 post-price)
            '2024-01-01', '2024-01-03', '2024-01-04', '2024-01-05'
        ]),
        'net_transfers': [
            # Wallet1 transfers
            100.0, 50.0, -30.0, 20.0,
            # Wallet2 transfers
            200.0, -50.0, 75.0, 25.0
        ],
        'balance': [
            # Wallet1 balances
            100.0, 150.0, 120.0, 140.0,
            # Wallet2 balances
            200.0, 150.0, 225.0, 250.0
        ],
        'price': [np.nan, np.nan, 1.1, 1.2, np.nan, np.nan, 1.1, 1.2],
        'first_price_date': pd.to_datetime('2024-01-03'),  # First price date for the coin
        'first_price': 1.0  # First available price
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.create_imputed_records(input_df)

    # Verify results
    # Should have exactly 1 imputed record (only for wallet1)
    assert len(result_df) == 1, "Should create imputed record only for wallet1"

    # Expected imputed record for Wallet1 only
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'],
        'wallet_address': ['wallet1'],
        'date': pd.to_datetime(['2024-01-03']),  # first_price_date
        'net_transfers': [150.0],  # Latest pre-price balance
        'balance': [150.0],        # Same as net_transfers for imputed record
        'price': [1.0],
        'first_price_date': pd.to_datetime(['2024-01-03']),
        'first_price': [1.0]
    })

    # Verify structure
    assert set(result_df.columns) == set(input_df.columns), "Column mismatch"

    # Verify coin_id is categorical
    assert result_df['coin_id'].dtype == 'category', "coin_id should be categorical"

    # Compare numeric columns
    numeric_cols = ['net_transfers', 'balance', 'first_price']
    for col in numeric_cols:
        assert np.allclose(result_df[col].astype(float),
                            expected_df[col], equal_nan=True), f"Mismatch in {col} values"

    # Compare date columns
    date_cols = ['date', 'first_price_date']
    for col in date_cols:
        assert result_df[col].values == expected_df[col].values, f"Mismatch in {col} values"

    # Compare wallet address
    assert result_df['wallet_address'].iloc[0] == 'wallet1', \
        "Imputed record should be for wallet1"

    # Verify latest pre-price balance was used for wallet1
    wallet1_mask = (input_df['wallet_address'] == 'wallet1') & \
                    (input_df['date'] < input_df['first_price_date'])
    expected_balance = input_df[wallet1_mask]['balance'].iloc[-1]

    assert np.isclose(result_df['balance'].iloc[0],
                        expected_balance), "Wrong balance for wallet1"

    # Verify no imputed record for wallet2
    assert not (result_df['wallet_address'] == 'wallet2').any(), \
        "Should not create imputed record for wallet2"

@pytest.mark.unit
def test_create_imputed_records_none_needed():
    """
    Tests create_imputed_records when no wallets need imputation because either:
    1. All activity occurs after prices exist
    2. Activity exists on the first price date

    Validates that:
    1. Empty DataFrame is returned when no imputation is needed
    2. Empty DataFrame has the same columns as input
    """
    # Create test data with multiple scenarios where imputation is not needed
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 4 + ['coin2'] * 4,
        'wallet_address': ['wallet1'] * 4 + ['wallet2'] * 4,
        'date': pd.to_datetime([
            # Wallet1 - has activity on first price date
            '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06',
            # Wallet2 - all activity after first price date
            '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07'
        ]),
        'net_transfers': [
            # Wallet1 transfers
            100.0, 50.0, -30.0, 20.0,
            # Wallet2 transfers
            200.0, -50.0, 75.0, 25.0
        ],
        'balance': [
            # Wallet1 balances
            100.0, 150.0, 120.0, 140.0,
            # Wallet2 balances
            200.0, 150.0, 225.0, 250.0
        ],
        'first_price_date': pd.to_datetime('2024-01-03'),  # First price date for both coins
        'first_price': 1.0,  # First available price
        'price': 1.0  # Current price (not relevant for this test)
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.create_imputed_records(input_df)

    # Verify results
    assert len(result_df) == 0, "Should return empty DataFrame when no imputation needed"

    # Verify the empty DataFrame has the correct structure
    assert set(result_df.columns) == set(input_df.columns), \
        "Empty DataFrame should have same columns as input"


# ---------------------------------------- #
# append_imputed_records() unit tests
# ---------------------------------------- #

@pytest.mark.unit
def test_append_imputed_records_basic():
    """
    Tests append_imputed_records basic functionality by merging imputed records with existing data
    and ensuring earliest records for each wallet show full balance as transfer-in.

    Validates that:
    1. Imputed records are properly appended
    2. Earliest record for each wallet-coin pair has net_transfers equal to balance
    3. All records are properly sorted by coin_id, wallet_address, and date
    4. Records with NA prices are correctly handled
    5. No data is lost or modified for non-earliest records
    """
    # Create test profits_df with both NA price and price data
    profits_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5,
        'wallet_address': ['wallet1'] * 5,
        'date': pd.to_datetime(['2024-01-02', '2024-01-04', '2024-01-05',
                                '2024-01-06', '2024-01-07']),
        'net_transfers': [25.0, 50.0, -30.0, 20.0, 10.0],
        'balance': [25.0, 150.0, 120.0, 140.0, 150.0],
        'price': [np.nan, 1.1, 1.2, 1.15, 1.25],
        'first_price_date': pd.to_datetime('2024-01-03'),
        'first_price': 1.0
    })
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Create test imputed_records
    imputed_records = pd.DataFrame({
        'coin_id': ['coin1'],
        'wallet_address': ['wallet1'],
        'date': pd.to_datetime(['2024-01-03']),  # first_price_date
        'net_transfers': [25.0],  # Previous balance carried forward
        'balance': [25.0],
        'price': [1.0],
        'first_price_date': pd.to_datetime('2024-01-03'),
        'first_price': 1.0
    })
    imputed_records['coin_id'] = imputed_records['coin_id'].astype('category')

    # Execute function
    result_df = cwp.append_imputed_records(profits_df, imputed_records)

    # Verify results
    # Should have all original records plus imputed record
    assert len(result_df) == profits_df['price'].count() + len(imputed_records), \
        "Should contain all original records with price data and imputed records"

    # Verify ordering
    assert result_df['date'].is_monotonic_increasing, \
        "Results should be sorted by date"

    # Verify first record with price for the wallet
    first_price_record_mask = (result_df['coin_id'] == 'coin1') & \
                                (result_df['wallet_address'] == 'wallet1') & \
                                (result_df['date'] == pd.to_datetime('2024-01-03'))

    first_price_record = result_df[first_price_record_mask].iloc[0]
    assert np.isclose(first_price_record['net_transfers'],
                        first_price_record['balance']), \
        "First record with price should have net_transfers equal to balance"

    # Create expected DataFrame to verify complete result
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5,
        'wallet_address': ['wallet1'] * 5,
        'date': pd.to_datetime(['2024-01-03', '2024-01-04',
                                '2024-01-05', '2024-01-06', '2024-01-07']),
        'net_transfers': [25.0, 50.0, -30.0, 20.0, 10.0],
        'balance': [25.0, 150.0, 120.0, 140.0, 150.0],
        'price': [1.0, 1.1, 1.2, 1.15, 1.25],
        'first_price_date': pd.to_datetime(['2024-01-03'] * 5),
        'first_price': [1.0] * 5
    })

    # Compare numeric columns
    numeric_cols = ['net_transfers', 'balance', 'first_price']
    for col in numeric_cols:
        assert np.allclose(result_df[col], expected_df[col],
                            equal_nan=True), f"Mismatch in {col} values"

    # Compare price column separately to handle NA values
    price_mask = ~expected_df['price'].isna()
    assert np.allclose(result_df.loc[price_mask, 'price'],
                        expected_df.loc[price_mask, 'price'],
                        equal_nan=True), "Mismatch in price values"
    assert result_df.loc[~price_mask, 'price'].isna().all(), \
        "NA prices not preserved correctly"

    # Compare date columns
    date_cols = ['date', 'first_price_date']
    for col in date_cols:
        assert (result_df[col] == expected_df[col]).all(), f"Mismatch in {col} values"

    # Verify non-first records remain unchanged
    for idx in range(2, len(result_df)):  # Start after imputed record
        assert np.isclose(result_df.iloc[idx]['net_transfers'],
                            expected_df.iloc[idx]['net_transfers']), \
            f"Non-first record at index {idx} was modified"


@pytest.mark.unit
def test_append_imputed_records_earliest_updates():
    """
    Tests append_imputed_records with multiple wallets and coins, ensuring earliest records
    for each wallet-coin pair are properly adjusted for net_transfers.

    Tests three scenarios:
    1. Wallet with imputed record (imputed record should become earliest with balance as transfer)
    2. Wallet with only post-price records (earliest should have net_transfers = balance)
    3. Wallet with NA price record before first price (imputed based on that balance)

    Validates that net_transfers adjustments occur correctly for each scenario.
    """
    # Create test profits_df with multiple wallets and coins
    profits_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin1',           # Wallet1 Coin1 (will get imputed)
                    'coin1', 'coin1',                      # Wallet2 Coin1 (post-price only)
                    'coin2', 'coin2', 'coin2'],           # Wallet3 Coin2 (has pre-price)
        'wallet_address': ['wallet1'] * 3 + ['wallet2'] * 2 + ['wallet3'] * 3,
        'date': pd.to_datetime([
            # Wallet1 Coin1 records
            '2024-01-04', '2024-01-05', '2024-01-06',
            # Wallet2 Coin1 records (starts post-price)
            '2024-01-04', '2024-01-05',
            # Wallet3 Coin2 records (has pre-price)
            '2024-01-02', '2024-01-04', '2024-01-05'
        ]),
        'net_transfers': [
            50.0, -30.0, 20.0,    # Wallet1
            75.0, -25.0,          # Wallet2
            30.0, 20.0, -10.0     # Wallet3
        ],
        'balance': [
            150.0, 120.0, 140.0,  # Wallet1
            75.0, 50.0,           # Wallet2
            30.0, 50.0, 40.0      # Wallet3
        ],
        'price': [
            1.1, 1.2, 1.15,       # Wallet1
            1.1, 1.2,             # Wallet2
            np.nan, 2.1, 2.2      # Wallet3
        ],
        'first_price_date': pd.to_datetime([
            '2024-01-03', '2024-01-03', '2024-01-03',    # Coin1 first price date
            '2024-01-03', '2024-01-03',                  # Coin1 first price date
            '2024-01-03', '2024-01-03', '2024-01-03'     # Coin2 first price date
        ]),
        'first_price': [1.0] * 5 + [2.0] * 3  # Different first prices for each coin
    })
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Create imputed records
    imputed_records = pd.DataFrame({
        'coin_id': ['coin1', 'coin2'],
        'wallet_address': ['wallet1', 'wallet3'],
        'date': pd.to_datetime(['2024-01-03', '2024-01-03']),
        'net_transfers': [100.0, 30.0],      # Previous balances
        'balance': [100.0, 30.0],
        'price': [1.0, 2.0],
        'first_price_date': pd.to_datetime(['2024-01-03', '2024-01-03']),
        'first_price': [1.0, 2.0]
    })
    imputed_records['coin_id'] = imputed_records['coin_id'].astype('category')

    # Execute function
    result_df = cwp.append_imputed_records(profits_df, imputed_records)

    # Verify results
    # Check total record count
    expected_total_records = profits_df['price'].count() + len(imputed_records)
    assert len(result_df) == expected_total_records, "Should contain all records with prices"

    # Verify earliest records for each wallet-coin pair
    wallet_coin_pairs = [
        ('wallet1', 'coin1', pd.to_datetime('2024-01-03'), 100.0),  # Imputed record
        ('wallet2', 'coin1', pd.to_datetime('2024-01-04'), 75.0),   # Post-price only
        ('wallet3', 'coin2', pd.to_datetime('2024-01-03'), 30.0)    # Imputed from pre-price
    ]

    for wallet, coin, exp_date, exp_balance in wallet_coin_pairs:
        # Get earliest record for this wallet-coin pair
        mask = ((result_df['wallet_address'] == wallet) &
                (result_df['coin_id'] == coin))
        earliest_record = result_df[mask].sort_values('date').iloc[0]

        # Verify date and balance match
        assert earliest_record['date'] == exp_date, \
            f"Wrong earliest date for {wallet}-{coin}"

        # Verify net_transfers equals balance for earliest record
        assert np.isclose(earliest_record['net_transfers'],
                            earliest_record['balance']), \
            f"net_transfers should equal balance for earliest {wallet}-{coin} record"

        assert np.isclose(earliest_record['balance'],
                            exp_balance), \
            f"Wrong balance for earliest {wallet}-{coin} record"

    # Verify non-earliest records were not modified
    # Group by wallet-coin and check all but first record
    for (wallet, coin), group in result_df.groupby(['wallet_address', 'coin_id'], observed=True):
        sorted_group = group.sort_values('date')
        if len(sorted_group) > 1:
            orig_transfers = profits_df[
                (profits_df['wallet_address'] == wallet) &
                (profits_df['coin_id'] == coin)
            ]['net_transfers'].values

            # Compare non-first records
            later_transfers = sorted_group.iloc[1:]['net_transfers'].values
            matching_orig = orig_transfers[
                orig_transfers != sorted_group.iloc[0]['net_transfers']
            ]
            assert np.allclose(later_transfers, matching_orig, equal_nan=True), \
                f"Non-earliest records modified for {wallet}-{coin}"


@pytest.mark.unit
def test_append_imputed_records_no_imputed():
    """
    Tests append_imputed_records when there are no imputed records to append.

    Validates that:
    1. Function still adjusts earliest records to have net_transfers match balance
    2. Input data is otherwise unmodified
    3. Handles multiple wallets and coins correctly
    4. Works with both NA and non-NA price records
    """
    # Create test profits_df with multiple wallets and coins
    profits_df = pd.DataFrame({
        'coin_id': ['coin1', 'coin1', 'coin1',           # Wallet1 Coin1
                    'coin2', 'coin2', 'coin2'],           # Wallet1 Coin2
        'wallet_address': ['wallet1'] * 6,
        'date': pd.to_datetime([
            # Wallet1 Coin1 records
            '2024-01-03', '2024-01-04', '2024-01-05',
            # Wallet1 Coin2 records
            '2024-01-04', '2024-01-05', '2024-01-06'
        ]),
        'net_transfers': [
            50.0, -30.0, 20.0,    # Original transfers don't match balance for earliest
            75.0, -25.0, 30.0
        ],
        'balance': [
            100.0, 70.0, 90.0,    # Wallet1 Coin1
            150.0, 125.0, 155.0   # Wallet1 Coin2
        ],
        'price': [
            1.0, 1.1, 1.2,        # Wallet1 Coin1
            2.0, 2.1, 2.2         # Wallet1 Coin2
        ],
        'first_price_date': pd.to_datetime([
            '2024-01-03', '2024-01-03', '2024-01-03',    # Coin1 first price date
            '2024-01-04', '2024-01-04', '2024-01-04'     # Coin2 first price date
        ]),
        'first_price': [1.0] * 3 + [2.0] * 3  # Different first prices for each coin
    })
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Create empty imputed_records DataFrame with same structure
    imputed_records = pd.DataFrame(columns=profits_df.columns)
    imputed_records['coin_id'] = imputed_records['coin_id'].astype('category')

    # Execute function
    result_df = cwp.append_imputed_records(profits_df, imputed_records)

    # Verify results
    # Check total record count
    assert len(result_df) == len(profits_df), "Should maintain same number of records"

    # Verify earliest records for each coin have net_transfers matching balance
    wallet_coin_pairs = [
        ('wallet1', 'coin1', pd.to_datetime('2024-01-03'), 100.0),  # First record
        ('wallet1', 'coin2', pd.to_datetime('2024-01-04'), 150.0)   # First record
    ]

    for wallet, coin, exp_date, exp_balance in wallet_coin_pairs:
        # Get earliest record for this wallet-coin pair
        mask = ((result_df['wallet_address'] == wallet) &
                (result_df['coin_id'] == coin))
        earliest_record = result_df[mask].sort_values('date').iloc[0]

        # Verify date and balance match
        assert earliest_record['date'] == exp_date, \
            f"Wrong earliest date for {wallet}-{coin}"

        # Verify net_transfers equals balance for earliest record
        assert np.isclose(earliest_record['net_transfers'],
                            earliest_record['balance']), \
            f"net_transfers should equal balance for earliest {wallet}-{coin} record"

        assert np.isclose(earliest_record['balance'],
                            exp_balance), \
            f"Wrong balance for earliest {wallet}-{coin} record"

    # Verify non-earliest records were not modified
    # Group by wallet-coin and check all but first record
    for (wallet, coin), group in result_df.groupby(['wallet_address', 'coin_id']
                                                ,observed=True):
        sorted_group = group.sort_values('date')
        if len(sorted_group) > 1:
            orig_transfers = profits_df[
                (profits_df['wallet_address'] == wallet) &
                (profits_df['coin_id'] == coin)
            ].sort_values('date')['net_transfers'].values[1:]  # Skip first record

            # Compare non-first records
            later_transfers = sorted_group.iloc[1:]['net_transfers'].values
            assert np.allclose(later_transfers, orig_transfers,
                                equal_nan=True), \
                f"Non-earliest records modified for {wallet}-{coin}"

    # Verify all other columns remain unchanged except net_transfers for earliest records
    unchanged_cols = ['coin_id', 'wallet_address', 'date', 'balance', 'price',
                        'first_price_date', 'first_price']

    for col in unchanged_cols:
        if col == 'coin_id':
            continue  # Skip categorical comparison
        assert np.array_equal(result_df[col].values, profits_df[col].values,
                            ), f"Column {col} was modified"




# ---------------------------------------- #
# filter_pre_inflow_records() unit tests
# ---------------------------------------- #

@pytest.mark.unit
def test_filter_pre_inflow_records_basic():
    """
    Tests filter_pre_inflow_records with clear pre-inflow and post-inflow periods.

    Validates that:
    1. Records before first positive inflow are removed
    2. Records on and after first positive inflow are kept
    3. Zero and negative transfers before first inflow are removed
    4. Multiple wallet-coin combinations are handled correctly
    5. Behavior is consistent across wallets with different inflow patterns
    """
    # Create test data with clear pre and post inflow periods
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 8 + ['coin2'] * 8,
        'wallet_address': ['wallet1'] * 8 + ['wallet2'] * 8,
        'date': pd.to_datetime([
            # Wallet1 Coin1: first inflow on day 4
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04',
            '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08',
            # Wallet2 Coin2: first inflow on day 3
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04',
            '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08'
        ]),
        'net_transfers': [
            # Wallet1 Coin1: outflows, zero, then inflow
            -10.0, 0.0, -5.0, 100.0, -20.0, 30.0, -15.0, 10.0,
            # Wallet2 Coin2: zero, outflow, then inflow
            0.0, -15.0, 50.0, -10.0, 20.0, -5.0, 15.0, -10.0
        ],
        'balance': [
            # Wallet1 Coin1 balances
            -10.0, -10.0, -15.0, 85.0, 65.0, 95.0, 80.0, 90.0,
            # Wallet2 Coin2 balances
            0.0, -15.0, 35.0, 25.0, 45.0, 40.0, 55.0, 45.0
        ],
        'first_price_date': pd.to_datetime(['2024-01-01'] * 8 + ['2024-01-02'] * 8),
        'first_price': [1.0] * 8 + [2.0] * 8
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.filter_pre_inflow_records(input_df)

    # Calculate expected results
    # Should only include records from first positive inflow onwards
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5 + ['coin2'] * 6,
        'wallet_address': ['wallet1'] * 5 + ['wallet2'] * 6,
        'date': pd.to_datetime([
            # Wallet1 Coin1: from day 4 onwards
            '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08',
            # Wallet2 Coin2: from day 3 onwards
            '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07',
            '2024-01-08'
        ]),
        'net_transfers': [
            # Wallet1 Coin1: from first inflow
            100.0, -20.0, 30.0, -15.0, 10.0,
            # Wallet2 Coin2: from first inflow
            50.0, -10.0, 20.0, -5.0, 15.0, -10.0
        ],
        'balance': [
            # Wallet1 Coin1 balances
            85.0, 65.0, 95.0, 80.0, 90.0,
            # Wallet2 Coin2 balances
            35.0, 25.0, 45.0, 40.0, 55.0, 45.0
        ]
    })
    expected_df['coin_id'] = expected_df['coin_id'].astype('category')

    # Verify results
    # Check record count
    assert len(result_df) == len(expected_df), \
        "Should only keep records from first inflow onwards"

    # Verify first record for each wallet has positive inflow
    for wallet in ['wallet1', 'wallet2']:
        for coin in ['coin1', 'coin2']:
            mask = (result_df['wallet_address'] == wallet) & \
                    (result_df['coin_id'] == coin)
            if mask.any():  # If this wallet-coin combination exists
                first_record = result_df[mask].sort_values('date').iloc[0]
                assert first_record['net_transfers'] > 0, \
                    f"First record for {wallet}-{coin} should be positive inflow"

    # Compare numeric columns
    numeric_cols = ['net_transfers', 'balance']
    for col in numeric_cols:
        assert np.allclose(
            result_df[col].sort_index(),
            expected_df[col].sort_index(),
            equal_nan=True
        ), f"Mismatch in {col} values"

    # Compare non-numeric columns
    date_cols = ['date']
    for col in date_cols:
        assert (result_df[col].sort_index() ==
                expected_df[col].sort_index()).all(), f"{col} mismatch"

    assert (result_df['wallet_address'].sort_index() ==
            expected_df['wallet_address'].sort_index()).all(), "Wallet address mismatch"

    # Verify no pre-inflow records remain
    for (wallet, coin), group in result_df.groupby(['wallet_address', 'coin_id'],
                                                observed=True):
        cumulative_inflows = group['net_transfers'].clip(lower=0).cumsum()
        assert (cumulative_inflows > 0).all(), \
            f"Found pre-inflow records for {wallet}-{coin}"


@pytest.mark.unit
def test_filter_pre_inflow_records_zero_transitions():
    """
    Tests filter_pre_inflow_records with wallets having zero balance periods between inflows.

    Tests scenarios where:
    1. Wallet drains to zero after initial inflow, then receives new inflow
    2. Wallet has multiple zero-balance gaps between activity periods
    3. Wallet has offsetting transfers that net to zero

    Validates that:
    1. Records after first inflow are kept, even through zero balance periods
    2. Zero balance transitions don't reset the "post-inflow" state
    3. All records after first inflow are preserved regardless of balance
    4. Temporary columns are properly dropped in output
    """
    # Create test data with zero balance transitions
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 8 + ['coin2'] * 7,
        'wallet_address': ['wallet1'] * 8 + ['wallet2'] * 7,
        'date': pd.to_datetime([
            # Wallet1 Coin1: inflow -> zero -> inflow
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04',
            '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08',
            # Wallet2 Coin2: offsetting transfers then inflow
            '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
            '2024-01-06', '2024-01-07', '2024-01-08'
        ]),
        'net_transfers': [
            # Wallet1: inflow, drain to zero, new inflow
            50.0, -30.0, -20.0, 0.0, 75.0, -25.0, -50.0, 100.0,
            # Wallet2: offsetting transfers, then real inflow
            100.0, -100.0, 0.0, 200.0, -150.0, -50.0, 75.0
        ],
        'balance': [
            # Wallet1 balances
            50.0, 20.0, 0.0, 0.0, 75.0, 50.0, 0.0, 100.0,
            # Wallet2 balances
            100.0, 0.0, 0.0, 200.0, 50.0, 0.0, 75.0
        ],
        'first_price_date': pd.to_datetime(['2024-01-01'] * 8 + ['2024-01-01'] * 7),
        'first_price': [1.0] * 8 + [2.0] * 7
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.filter_pre_inflow_records(input_df)

    # Calculate expected results - all records from first positive inflow onwards
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'] * 8 + ['coin2'] * 7,
        'wallet_address': ['wallet1'] * 8 + ['wallet2'] * 7,
        'date': pd.to_datetime([
            # Wallet1: keep all records (first record is positive)
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04',
            '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08',
            # Wallet2: keep all records (first record is positive)
            '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
            '2024-01-06', '2024-01-07', '2024-01-08'
        ]),
        'net_transfers': [
            # Wallet1: all transfers after first inflow
            50.0, -30.0, -20.0, 0.0, 75.0, -25.0, -50.0, 100.0,
            # Wallet2: all transfers after first inflow
            100.0, -100.0, 0.0, 200.0, -150.0, -50.0, 75.0
        ],
        'balance': [
            # Wallet1 balances
            50.0, 20.0, 0.0, 0.0, 75.0, 50.0, 0.0, 100.0,
            # Wallet2 balances
            100.0, 0.0, 0.0, 200.0, 50.0, 0.0, 75.0
        ]
    })
    expected_df['coin_id'] = expected_df['coin_id'].astype('category')

    # Verify results
    # Check record count
    assert len(result_df) == len(expected_df), \
        "Should keep all records after first inflow"

    # Verify temporary columns are dropped
    assert 'first_price_date' not in result_df.columns, \
        "first_price_date should be dropped"
    assert 'first_price' not in result_df.columns, \
        "first_price should be dropped"

    # Verify first record for each wallet has positive inflow
    for wallet in ['wallet1', 'wallet2']:
        for coin in ['coin1', 'coin2']:
            mask = (result_df['wallet_address'] == wallet) & \
                    (result_df['coin_id'] == coin)
            if mask.any():
                first_record = result_df[mask].sort_values('date').iloc[0]
                assert first_record['net_transfers'] > 0, \
                    f"First record for {wallet}-{coin} should be positive inflow"

    # Compare numeric columns
    numeric_cols = ['net_transfers', 'balance']
    for col in numeric_cols:
        assert np.allclose(
            result_df[col].sort_index(),
            expected_df[col].sort_index(),
            equal_nan=True
        ), f"Mismatch in {col} values"

    # Compare non-numeric columns
    assert (result_df['date'].sort_index() ==
            expected_df['date'].sort_index()).all(), "Date mismatch"
    assert (result_df['wallet_address'].sort_index() ==
            expected_df['wallet_address'].sort_index()).all(), "Wallet address mismatch"

    # Verify that zero balance periods are preserved after first inflow
    for (wallet, coin), group in result_df.groupby(['wallet_address', 'coin_id']
                                                ,observed=True):
        # Get indices where balance is zero
        zero_balance_indices = group[group['balance'] == 0.0].index
        if len(zero_balance_indices) > 0:
            # Verify records exist after zero balance periods
            for idx in zero_balance_indices:
                later_records = group[group.index > idx]
                assert not later_records.empty, \
                    f"Missing records after zero balance for {wallet}-{coin}"







# ======================================================== #
#                                                          #
#            I N T E G R A T I O N   T E S T S             #
#                                                          #
# ======================================================== #

@pytest.fixture(scope="session")
def sample_transfers_df():
    """Provides test transfer data"""
    transfers_df = pd.DataFrame({
        'coin_id': [
            # Wallet1 Coin1
            'coin1', 'coin1', 'coin1', 'coin1', 'coin1', 'coin1', 'coin1',
            # Wallet1 Coin2 - post-price only
            'coin2', 'coin2', 'coin2',
            # Wallet2 Coin1 - moved first transaction earlier
            'coin1', 'coin1', 'coin1',
            # Wallet2 Coin2 - has exit and re-entry
            'coin2', 'coin2', 'coin2', 'coin2', 'coin2',
            # Wallet3 Coin1 - post-price with same-day offsetting
            'coin1', 'coin1',
            # Wallet3 Coin3 - moved transaction earlier
            'coin3', 'coin3', 'coin3', 'coin3'
        ],
        'wallet_address': [
            # Wallet1 Coin1
            'wallet1', 'wallet1', 'wallet1', 'wallet1', 'wallet1', 'wallet1', 'wallet1',
            # Wallet1 Coin2
            'wallet1', 'wallet1', 'wallet1',
            # Wallet2 Coin1
            'wallet2', 'wallet2', 'wallet2',
            # Wallet2 Coin2
            'wallet2', 'wallet2', 'wallet2', 'wallet2', 'wallet2',
            # Wallet3 Coin1
            'wallet3', 'wallet3',
            # Wallet3 Coin3
            'wallet3', 'wallet3', 'wallet3', 'wallet3'
        ],
        'date': pd.to_datetime([
            # Wallet1 Coin1 - pre and post price activity
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
            '2024-01-06', '2024-01-07',
            # Wallet1 Coin2 - all post-price
            '2024-01-05', '2024-01-06', '2024-01-07',
            # Wallet2 Coin1 - now starts on 01-01
            '2024-01-01', '2024-01-04', '2024-01-05',
            # Wallet2 Coin2 - exit and re-entry
            '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08',
            # Wallet3 Coin1 - post-price offsetting
            '2024-01-04', '2024-01-05',
            # Wallet3 Coin3 - moved to 01-03
            '2024-01-03', '2024-01-06', '2024-01-07', '2024-01-08'
        ]),
        'net_transfers': [
            # Wallet1 Coin1
            100.0, 50.0, -30.0, 20.0, -40.0, 0.0, 10.0,
            # Wallet1 Coin2
            200.0, -50.0, 25.0,
            # Wallet2 Coin1
            150.0, -50.0, 25.0,
            # Wallet2 Coin2
            100.0, 50.0, -150.0, 75.0, 25.0,
            # Wallet3 Coin1
            0.0, 50.0,
            # Wallet3 Coin3
            50.0, 30.0, -10.0, 25.0
        ],
        'balance': [
            # Wallet1 Coin1
            100.0, 150.0, 120.0, 140.0, 100.0, 100.0, 110.0,
            # Wallet1 Coin2
            200.0, 150.0, 175.0,
            # Wallet2 Coin1
            150.0, 100.0, 125.0,
            # Wallet2 Coin2
            100.0, 150.0, 0.0, 75.0, 100.0,
            # Wallet3 Coin1
            0.0, 50.0,
            # Wallet3 Coin3
            50.0, 80.0, 70.0, 95.0
        ]
    })
    return transfers_df

@pytest.fixture(scope="session")
def sample_prices_df():
    """Provides test price data"""
    prices_df = pd.DataFrame({
        'coin_id': [
            # Coin1 - earliest prices
            'coin1', 'coin1', 'coin1', 'coin1', 'coin1',
            # Coin2 - starts a bit later
            'coin2', 'coin2', 'coin2', 'coin2',
            # Coin3 - starts latest
            'coin3', 'coin3', 'coin3'
        ],
        'date': pd.to_datetime([
            # Coin1 prices
            '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07',
            # Coin2 prices
            '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07',
            # Coin3 prices
            '2024-01-05', '2024-01-06', '2024-01-07'
        ]),
        'price': [
            # Coin1 - steady progression
            1.0, 1.1, 1.2, 1.15, 1.25,
            # Coin2 - volatile
            2.0, 1.8, 2.2, 1.9,
            # Coin3 - extreme moves
            3.0, 4.5, 2.5
        ]
    })
    return prices_df

@pytest.fixture(scope="session")
def pipeline_result_df(sample_transfers_df, sample_prices_df):
    """
    Executes the complete data preparation pipeline and returns the final DataFrame.
    This fixture can be used by other test files to access the pipeline result.
    """
    # Execute pipeline
    # 1. Merge prices and transfers
    merged_df = cwp.merge_prices_and_transfers(sample_transfers_df, sample_prices_df)

    # 2. Add first price info
    with_prices_df = cwp.add_first_price_info(merged_df, sample_prices_df)

    # 3. Create imputed records
    imputed_records = cwp.create_imputed_records(with_prices_df)

    # 4. Append imputed records
    with_imputed_df = cwp.append_imputed_records(with_prices_df, imputed_records)

    # 5. Filter pre-inflow records
    result_df = cwp.filter_pre_inflow_records(with_imputed_df)

    return result_df


# Tests for the structure of profits_df
# -------------------------------------
@pytest.mark.integration
def test_profits_df_structure(pipeline_result_df):
    """Tests the structure and data types of the final DataFrame"""
    # Verify columns
    assert set(pipeline_result_df.columns) == {
        'coin_id', 'wallet_address', 'date', 'net_transfers', 'balance', 'price'
    }, "Incorrect columns in result"

    # Verify data types
    assert pipeline_result_df['coin_id'].dtype == 'category', \
        "coin_id should be categorical"
    assert pd.api.types.is_datetime64_any_dtype(pipeline_result_df['date']), \
        "date should be datetime type"

    # Verify unique combinations
    unique_records = len(pipeline_result_df.drop_duplicates(
        subset=['coin_id', 'date', 'wallet_address']))
    assert len(pipeline_result_df) == unique_records, \
        "duplicate coin-date-wallet pairs found"

    # Verify no missing prices
    assert not pipeline_result_df['price'].isna().any(), \
        "Should not have any records without prices"


# Tests for wallet-specific behaviors in the profits DataFrame
# ------------------------------------------------------------
@pytest.mark.integration
def test_wallet_inflow_conditions(pipeline_result_df):
    """Tests that each wallet's first record has positive inflow"""
    for (wallet, coin), group in pipeline_result_df.groupby(
        ['wallet_address', 'coin_id'], observed=True):
        first_record = group.sort_values('date').iloc[0]
        assert first_record['net_transfers'] > 0, \
            f"First record for {wallet}-{coin} should be positive inflow"

@pytest.mark.integration
def test_wallet_combinations(pipeline_result_df):
    """Tests for expected wallet-coin combinations"""
    expected_combinations = {
        ('wallet1', 'coin1'), ('wallet1', 'coin2'),
        ('wallet2', 'coin1'), ('wallet2', 'coin2'),
        ('wallet3', 'coin1'), ('wallet3', 'coin3')
    }
    actual_combinations = set(zip(
        pipeline_result_df['wallet_address'],
        pipeline_result_df['coin_id']))
    assert expected_combinations.issubset(actual_combinations), \
        "Missing expected wallet-coin combinations"


# Tests for balance calculations and consistency in profits_df
# ------------------------------------------------------------
@pytest.mark.integration
def test_balance_consistency(pipeline_result_df):
    """Tests that balance changes match net transfers"""
    for (wallet, coin), group in pipeline_result_df.groupby(
        ['wallet_address', 'coin_id'], observed=True):
        sorted_group = group.sort_values('date')
        balance_changes = sorted_group['balance'].diff()
        assert np.allclose(
            balance_changes.fillna(sorted_group['balance'].iloc[0]),
            sorted_group['net_transfers'],
            equal_nan=True
        ), f"Balance changes don't match transfers for {wallet}-{coin}"

@pytest.mark.integration
def test_specific_balance_conditions(pipeline_result_df):
    """Tests specific known balance conditions"""
    # Test Wallet2 Coin2 exit
    zero_balance_mask = (
        (pipeline_result_df['wallet_address'] == 'wallet2') &
        (pipeline_result_df['coin_id'] == 'coin2') &
        (pipeline_result_df['date'] == pd.to_datetime('2024-01-06'))
    )
    if zero_balance_mask.any():
        assert np.allclose(
            pipeline_result_df[zero_balance_mask]['balance'],
            0.0,
            atol=1e-10
        ), "Wallet2-Coin2 exit balance should be zero"
