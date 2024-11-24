"""
tests used to audit the files in the etl-pipelines repository
"""
# pylint: disable=W1203 # fstrings in logs
# pylint: disable=C0301 # line over 100 chars
# pylint: disable=E0401 # can't find import (due to local import)
# pylint: disable=C0413 # import not at top of doc (due to local import)
# pylint: disable=W0612 # unused variables (due to test reusing functions with 2 outputs)
# pylint: disable=W0621 # redefining from outer scope triggering on pytest fixtures


import sys
import os
import logging
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
# calculate_wallet_profitability() unit tests
# ---------------------------------------- #

@pytest.mark.unit
def test_calculate_wallet_profitability_basic(caplog):
    """
    Tests calculate_wallet_profitability with simple price increases and stable balance.

    Tests a straightforward scenario where:
    1. Single wallet holds constant balance
    2. Price steadily increases
    3. No additional transfers during the period

    This allows for easy manual verification of profit calculations:
    - profits_change = (current_price - previous_price) * previous_balance
    - profits_cumulative = running sum of profits_change
    - usd values are straightforward price * quantity calculations
    - total_return = profits_cumulative / cumulative_inflows

    Parameters:
    -----------
    caplog : pytest fixture
        Captures logging output during the test
    """
    # Set logging level to WARNING to suppress INFO messages
    caplog.set_level(logging.WARNING)

    # Create test data with stable balance and rising prices
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 4,
        'wallet_address': ['wallet1'] * 4,
        'date': pd.to_datetime([
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'
        ]),
        'net_transfers': [
            100.0, 0.0, 0.0, 0.0  # Single inflow, then no activity
        ],
        'balance': [
            100.0, 100.0, 100.0, 100.0  # Stable balance
        ],
        'price': [
            1.0, 1.1, 1.2, 1.3  # Steady price increase
        ]
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.calculate_wallet_profitability(input_df)

    # Calculate expected results
    # Day 1: No profit change (first day)
    # Day 2: (1.1 - 1.0) * 100 = 10.0 profit
    # Day 3: (1.2 - 1.1) * 100 = 10.0 profit
    # Day 4: (1.3 - 1.2) * 100 = 10.0 profit
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'] * 4,
        'wallet_address': ['wallet1'] * 4,
        'date': pd.to_datetime([
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'
        ]),
        'profits_change': [
            0.0, 10.0, 10.0, 10.0
        ],
        'profits_cumulative': [
            0.0, 10.0, 20.0, 30.0
        ],
        'usd_balance': [
            100.0, 110.0, 120.0, 130.0  # balance * price
        ],
        'usd_net_transfers': [
            100.0, 0.0, 0.0, 0.0  # net_transfers * price
        ],
        'usd_inflows': [
            100.0, 0.0, 0.0, 0.0  # Only positive transfers
        ],
        'usd_inflows_cumulative': [
            100.0, 100.0, 100.0, 100.0
        ],
        'total_return': [
            0.0, 0.1, 0.2, 0.3  # profits_cumulative / inflows_cumulative
        ]
    })
    expected_df['coin_id'] = expected_df['coin_id'].astype('category')

    # Verify results
    # Check that all expected columns exist
    expected_cols = {'coin_id', 'wallet_address', 'date', 'profits_change',
                    'profits_cumulative', 'usd_balance', 'usd_net_transfers',
                    'usd_inflows', 'usd_inflows_cumulative', 'total_return'}
    assert set(result_df.columns) == expected_cols, "Missing or extra columns in result"

    # Compare numeric columns
    numeric_cols = ['profits_change', 'profits_cumulative', 'usd_balance',
                    'usd_net_transfers', 'usd_inflows', 'usd_inflows_cumulative',
                    'total_return']
    for col in numeric_cols:
        assert np.allclose(result_df[col], expected_df[col],
                            equal_nan=True), f"Mismatch in {col} values"

    # Verify dates
    assert (result_df['date'] == expected_df['date']).all(), "Date mismatch"

    # Verify non-negative inflows
    assert (result_df['usd_inflows'] >= 0).all(), "Found negative USD inflows"

    # Verify running sum relationships
    assert np.allclose(
        result_df['profits_cumulative'],
        result_df['profits_change'].cumsum(),
        equal_nan=True
    ), "profits_cumulative should be cumsum of profits_change"

    assert np.allclose(
        result_df['usd_inflows_cumulative'],
        result_df['usd_inflows'].cumsum(),
        equal_nan=True
    ), "usd_inflows_cumulative should be cumsum of usd_inflows"

    # Verify total_return calculation
    expected_return = (result_df['profits_cumulative'] /
                        result_df['usd_inflows_cumulative'].where(
                            result_df['usd_inflows_cumulative'] != 0, np.nan))
    assert np.allclose(result_df['total_return'],
                        expected_return,
                        equal_nan=True), "total_return calculation incorrect"


@pytest.mark.unit
def test_calculate_wallet_profitability_complex(caplog):
    """
    Tests calculate_wallet_profitability with variable balances and volatile prices.

    Tests complex scenarios where:
    1. Multiple transfers in/out during period
    2. Volatile price movements up and down
    3. Multiple days of activity to test cumulative calculations

    Profits are calculated as: (current_price - previous_price) * previous_balance
    Each day's values were calculated manually to ensure correctness.
    """
    # Set logging level to WARNING
    caplog.set_level(logging.WARNING)

    # Create test data with varying balances and volatile prices
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5,
        'wallet_address': ['wallet1'] * 5,
        'date': pd.to_datetime([
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'
        ]),
        'net_transfers': [
            100.0,   # Initial purchase
            50.0,    # Buy more
            -30.0,   # Sell some
            20.0,    # Buy more
            -60.0    # Large sale
        ],
        'balance': [
            100.0,   # After initial purchase
            150.0,   # After second purchase
            120.0,   # After first sale
            140.0,   # After third purchase
            80.0     # After final sale
        ],
        'price': [
            1.0,     # Starting price
            0.8,     # Price drop
            1.2,     # Price spike
            0.9,     # Price drop
            1.1      # Price recovery
        ]
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.calculate_wallet_profitability(input_df)

    # Calculate expected results with detailed profit calculations:
    # Day 1: No profit (first day)
    # Day 2: (0.8 - 1.0) * 100 = -20.0 loss
    # Day 3: (1.2 - 0.8) * 150 = 60.0 profit
    # Day 4: (0.9 - 1.2) * 120 = -36.0 loss
    # Day 5: (1.1 - 0.9) * 140 = 28.0 profit
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'] * 5,
        'wallet_address': ['wallet1'] * 5,
        'date': pd.to_datetime([
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'
        ]),
        'profits_change': [
            0.0,    # Day 1
            -20.0,  # Day 2
            60.0,   # Day 3
            -36.0,  # Day 4
            28.0    # Day 5
        ],
        'profits_cumulative': [
            0.0,    # Day 1
            -20.0,  # Day 1 + Day 2
            40.0,   # Day 1 + Day 2 + Day 3
            4.0,    # Day 1 + Day 2 + Day 3 + Day 4
            32.0    # Day 1 + Day 2 + Day 3 + Day 4 + Day 5
        ],
        'usd_balance': [
            100.0,  # 100 * 1.0
            120.0,  # 150 * 0.8
            144.0,  # 120 * 1.2
            126.0,  # 140 * 0.9
            88.0    # 80 * 1.1
        ],
        'usd_net_transfers': [
            100.0,  # 100 * 1.0
            40.0,   # 50 * 0.8
            -36.0,  # -30 * 1.2
            18.0,   # 20 * 0.9
            -66.0   # -60 * 1.1
        ],
        'usd_inflows': [
            100.0,  # First purchase
            40.0,   # Second purchase
            0.0,    # Sale
            18.0,   # Third purchase
            0.0     # Sale
        ],
        'usd_inflows_cumulative': [
            100.0,  # Day 1
            140.0,  # Day 1 + Day 2
            140.0,  # No change
            158.0,  # Added Day 4 purchase
            158.0   # No change
        ],
        'total_return': [
            0.0,    # 0 / 100
            -0.143, # -20 / 140
            0.286,  # 40 / 140
            0.025,  # 4 / 158
            0.203   # 32 / 158
        ]
    })
    expected_df['coin_id'] = expected_df['coin_id'].astype('category')

    # Verify results
    # Check that all expected columns exist
    expected_cols = {'coin_id', 'wallet_address', 'date', 'profits_change',
                    'profits_cumulative', 'usd_balance', 'usd_net_transfers',
                    'usd_inflows', 'usd_inflows_cumulative', 'total_return'}
    assert set(result_df.columns) == expected_cols, "Missing or extra columns in result"

    # Compare numeric columns
    numeric_cols = ['profits_change', 'profits_cumulative', 'usd_balance',
                    'usd_net_transfers', 'usd_inflows', 'usd_inflows_cumulative']
    for col in numeric_cols:
        assert np.allclose(result_df[col], expected_df[col],
                            equal_nan=True), f"Mismatch in {col} values"

    # Compare total_return with tolerance due to potential floating point differences
    assert np.allclose(result_df['total_return'], expected_df['total_return'],
                        rtol=1e-1, equal_nan=True), "Mismatch in total_return values"

    # Verify running sum relationships
    assert np.allclose(
        result_df['profits_cumulative'],
        result_df['profits_change'].cumsum(),
        equal_nan=True
    ), "profits_cumulative should be cumsum of profits_change"

    assert np.allclose(
        result_df['usd_inflows_cumulative'],
        result_df['usd_inflows'].cumsum(),
        equal_nan=True
    ), "usd_inflows_cumulative should be cumsum of usd_inflows"



@pytest.mark.unit
def test_calculate_wallet_profitability_edge_cases(caplog):
    """
    Tests calculate_wallet_profitability with various edge cases.

    Tests scenarios including:
    1. Very small balances and transfers (near zero)
    2. Very large balances and transfers
    3. Zero balances
    4. Extreme price movements
    5. Transfers with unchanging price

    Ensures calculations remain accurate and stable across edge cases.
    """
    # Set logging level to WARNING
    caplog.set_level(logging.WARNING)

    # Create test data with edge cases
    input_df = pd.DataFrame({
        'coin_id': ['coin1'] * 8,
        'wallet_address': ['wallet1'] * 8,
        'date': pd.to_datetime([
            '2024-01-01',  # Day 1: Initial tiny position
            '2024-01-02',  # Day 2: Large position
            '2024-01-03',  # Day 3: Zero balance
            '2024-01-04',  # Day 4: Extreme price jump
            '2024-01-05',  # Day 5: Tiny price movement
            '2024-01-06',  # Day 6: Zero price movement
            '2024-01-07',  # Day 7: Large price drop
            '2024-01-08'   # Day 8: Recovery with large balance
        ]),
        'net_transfers': [
            0.000001,           # Tiny initial transfer
            1000000.0,          # Large transfer in
            -1000000.000001,    # Complete withdrawal
            100.0,              # New position
            0.000001,           # Tiny transfer
            0.0,                # No transfer
            1000000.0,          # Large transfer
            0.0                 # No transfer
        ],
        'balance': [
            0.000001,           # Tiny balance
            1000000.000001,     # Large balance
            0.0,                # Zero balance
            100.0,              # Normal balance
            100.000001,         # Tiny increment
            100.000001,         # Unchanged
            1100000.000001,     # Large balance
            1100000.000001      # Unchanged
        ],
        'price': [
            1.0,                # Starting price
            1.0,                # No change
            1.0,                # No change
            1000.0,             # Extreme price jump
            1000.000001,        # Tiny price increment
            1000.000001,        # No change
            0.01,               # Large price drop
            1.0                 # Recovery
        ]
    })
    input_df['coin_id'] = input_df['coin_id'].astype('category')

    # Execute function
    result_df = cwp.calculate_wallet_profitability(input_df)

    # Calculate expected results
    # Day 1: No profit (first day)
    # Day 2: (1.0 - 1.0) * 0.000001 = 0.0
    # Day 3: (1.0 - 1.0) * 1000000.000001 = 0.0
    # Day 4: (1000.0 - 1.0) * 0.0 = 0.0
    # Day 5: (1000.000001 - 1000.0) * 100.0 = 0.0001
    # Day 6: (1000.000001 - 1000.000001) * 100.000001 = 0.0
    # Day 7: (0.01 - 1000.000001) * 100.000001 = -99999.0001
    # Day 8: (1.0 - 0.01) * 1100000.000001 = 1089000.0
    expected_df = pd.DataFrame({
        'coin_id': ['coin1'] * 8,
        'wallet_address': ['wallet1'] * 8,
        'date': input_df['date'],
        'profits_change': [
            0.0,            # Day 1
            0.0,            # Day 2
            0.0,            # Day 3
            0.0,            # Day 4
            0.0001,         # Day 5
            0.0,            # Day 6
            -99999.0001,    # Day 7
            1089000.0       # Day 8
        ],
        'profits_cumulative': [
            0.0,            # Day 1
            0.0,            # Sum through Day 2
            0.0,            # Sum through Day 3
            0.0,            # Sum through Day 4
            0.0001,         # Sum through Day 5
            0.0001,         # Sum through Day 6
            -99998.9999,    # Sum through Day 7
            989001.0001     # Sum through Day 8
        ],
        'usd_balance': [
            0.000001,           # 0.000001 * 1.0
            1000000.000001,     # 1000000.000001 * 1.0
            0.0,                # 0.0 * 1.0
            100000.0,           # 100.0 * 1000.0
            100000.001,         # 100.000001 * 1000.000001
            100000.001,         # 100.000001 * 1000.000001
            11000.000001,       # 1100000.000001 * 0.01
            1100000.000001      # 1100000.000001 * 1.0
        ],
        'usd_net_transfers': [
            0.000001,           # 0.000001 * 1.0
            1000000.0,          # 1000000.0 * 1.0
            -1000000.000001,    # -1000000.000001 * 1.0
            100000.0,           # 100.0 * 1000.0
            0.001,              # 0.000001 * 1000.000001
            0.0,                # 0.0 * 1000.000001
            10000.0,            # 1000000.0 * 0.01
            0.0                 # 0.0 * 1.0
        ],
        'usd_inflows': [
            0.000001,      # First tiny transfer
            1000000.0,     # Large transfer
            0.0,           # Outflow
            100000.0,      # New position
            0.001,         # Tiny transfer
            0.0,           # No transfer
            10000.0,       # Large transfer at low price
            0.0            # No transfer
        ],
        'usd_inflows_cumulative': [
            0.000001,
            1000000.000001,
            1000000.000001,
            1100000.000001,
            1100000.001001,
            1100000.001001,
            1110000.001001,
            1110000.001001
        ]
    })

    # Calculate total_return separately to handle division by zero
    expected_df['total_return'] = expected_df['profits_cumulative'] / \
                                    expected_df['usd_inflows_cumulative']

    expected_df['coin_id'] = expected_df['coin_id'].astype('category')

    # Verify results
    # Check that all expected columns exist
    expected_cols = {'coin_id', 'wallet_address', 'date', 'profits_change',
                    'profits_cumulative', 'usd_balance', 'usd_net_transfers',
                    'usd_inflows', 'usd_inflows_cumulative', 'total_return'}
    assert set(result_df.columns) == expected_cols, "Missing or extra columns in result"

    # Compare numeric columns with appropriate tolerances for different scales
    for col in ['profits_change', 'profits_cumulative']:
        assert np.allclose(result_df[col], expected_df[col],
                            rtol=1e-5, atol=1e-5, equal_nan=True), \
            f"Mismatch in {col} values"

    # Use higher relative tolerance for large USD values
    for col in ['usd_balance', 'usd_net_transfers', 'usd_inflows',
                'usd_inflows_cumulative']:
        assert np.allclose(result_df[col], expected_df[col],
                            rtol=1e-4, atol=1e-6, equal_nan=True), \
            f"Mismatch in {col} values"

    # Verify total_return with appropriate tolerance
    assert np.allclose(
        result_df['total_return'],
        expected_df['total_return'],
        rtol=1e-4, atol=1e-6, equal_nan=True
    ), "Mismatch in total_return values"

    # Verify no infinity or NaN values (except where expected in total_return)
    numeric_cols = ['profits_change', 'profits_cumulative', 'usd_balance',
                    'usd_net_transfers', 'usd_inflows', 'usd_inflows_cumulative']
    for col in numeric_cols:
        assert not np.any(np.isinf(result_df[col])), f"Found infinity in {col}"
        assert not np.any(np.isnan(result_df[col])), f"Found NaN in {col}"