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
import pandas as pd
import numpy as np
import pytest
from dotenv import load_dotenv
from dreams_core import core as dc

# Project Modules
# pyright: reportMissingImports=false
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../cloud_functions/core_coin_market_data')))
import core_coin_market_data as cmd

load_dotenv()
logger = dc.setup_logger()


# ===================================================== #
#                                                       #
#                 U N I T   T E S T S                   #
#                                                       #
# ===================================================== #

# ---------------------------------------- #
# fill_market_data_gaps() unit tests
# ---------------------------------------- #

@pytest.fixture
def sample_market_data_small_gaps():
    """
    Fixture to create a sample DataFrame with small gaps in market data.
    This fixture reflects the state of the data after upstream processing,
    where no null values exist, but entire rows for some dates are missing.

    Returns:
    - pd.DataFrame: Sample market data with small gaps (missing rows)
    """
    data = {
        'coin_id': ['btc', 'btc', 'btc', 'eth', 'eth', 'eth'],
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-04',
                                '2023-01-01', '2023-01-02', '2023-01-05']),
        'price': [100, 102, 105, 50, 51, 54],
        'volume': [1000, 1100, 1200, 500, 510, 540],
        'market_cap': [10000, 10200, 10500, 5000, 5100, 5400],
        'fdv': [12000, 12200, 12500, 6000, 6100, 6400],
        'circulating_supply': [100, 100, 100, 100, 100, 100],
        'total_supply': [120, 120, 120, 120, 120, 120],
        'data_source': ['source1', 'source1', 'source1', 'source2', 'source2', 'source2'],
        'updated_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-04',
                                      '2023-01-01', '2023-01-02', '2023-01-05'])
    }
    return pd.DataFrame(data)

@pytest.mark.unit
def test_fill_market_data_gaps_small_gaps(sample_market_data_small_gaps):
    """
    Test the fill_market_data_gaps function with a DataFrame containing small gaps (missing rows).

    This test ensures that:
    1. Small gaps (1-2 days) are filled correctly for all metrics
    2. days_imputed is calculated accurately
    3. The resulting DataFrame has the expected shape and content

    Args:
    - sample_market_data_small_gaps: Fixture providing sample data with small gaps (missing rows)
    """
    # Call the function
    result_df = cmd.fill_market_data_gaps(sample_market_data_small_gaps)

    # Check that all dates are present
    assert len(result_df) == 10  # 5 days for BTC and 5 days for ETH

    # Check that gaps are filled
    assert result_df['price'].isna().sum() == 0
    assert result_df['market_cap'].isna().sum() == 0
    assert result_df['fdv'].isna().sum() == 0
    assert result_df['circulating_supply'].isna().sum() == 0
    assert result_df['total_supply'].isna().sum() == 0

    # Check days_imputed
    btc_imputed = result_df[result_df['coin_id'] == 'btc']['days_imputed']
    eth_imputed = result_df[result_df['coin_id'] == 'eth']['days_imputed']
    assert btc_imputed.iloc[2] == 1  # 2023-01-03 for BTC
    assert eth_imputed.iloc[2] == 1  # 2023-01-03 for ETH
    assert eth_imputed.iloc[3] == 2  # 2023-01-04 for ETH

    # Check that values are correctly forward-filled
    assert result_df[result_df['coin_id'] == 'btc']['price'].iloc[2] == 102  # 2023-01-03 for BTC
    assert result_df[result_df['coin_id'] == 'eth']['price'].iloc[2] == 51  # 2023-01-03 for ETH
    assert result_df[result_df['coin_id'] == 'eth']['price'].iloc[3] == 51  # 2023-01-04 for ETH

    # Check that volume is 0 for imputed days
    assert result_df[result_df['coin_id'] == 'btc']['volume'].iloc[2] == 0  # 2023-01-03 for BTC
    assert result_df[result_df['coin_id'] == 'eth']['volume'].iloc[2] == 0  # 2023-01-03 for ETH
    assert result_df[result_df['coin_id'] == 'eth']['volume'].iloc[3] == 0  # 2023-01-04 for ETH

    # Check that updated_at is NaT for imputed days
    assert pd.isna(result_df[result_df['coin_id'] == 'btc']['updated_at'].iloc[2])  # 2023-01-03 for BTC
    assert pd.isna(result_df[result_df['coin_id'] == 'eth']['updated_at'].iloc[2])  # 2023-01-03 for ETH
    assert pd.isna(result_df[result_df['coin_id'] == 'eth']['updated_at'].iloc[3])  # 2023-01-04 for ETH

    # Additional assertions can be added to check specific values or other conditions


@pytest.fixture
def sample_market_data_no_gaps():
    """
    Fixture to create a sample DataFrame with no gaps in market data.

    Returns:
    - pd.DataFrame: Sample market data with no gaps
    """
    data = {
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03',
                                '2023-01-01', '2023-01-02', '2023-01-03']),
        'coin_id': ['btc', 'btc', 'btc', 'eth', 'eth', 'eth'],
        'price': [100, 102, 105, 50, 51, 52],
        'volume': [1000, 1100, 1200, 500, 510, 520],
        'market_cap': [10000, 10200, 10500, 5000, 5100, 5200],
        'fdv': [12000, 12200, 12500, 6000, 6100, 6200],
        'circulating_supply': [100, 100, 100, 100, 100, 100],
        'total_supply': [120, 120, 120, 120, 120, 120],
        'data_source': ['source1', 'source1', 'source1', 'source2', 'source2', 'source2'],
        'updated_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03',
                                      '2023-01-01', '2023-01-02', '2023-01-03'])
    }
    return pd.DataFrame(data)
@pytest.mark.unit
def test_fill_market_data_gaps_no_gaps(sample_market_data_no_gaps):
    """
    Test the fill_market_data_gaps function with a DataFrame containing no gaps.

    This test ensures that:
    1. The function doesn't modify the data when there are no gaps
    2. No days are imputed (all values in days_imputed column are NaN)
    3. The resulting DataFrame is identical to the input, except for the added days_imputed column

    Args:
    - sample_market_data_no_gaps: Fixture providing sample data with no gaps
    """
    # Call the function
    result_df = cmd.fill_market_data_gaps(sample_market_data_no_gaps)

    # Check that the resulting DataFrame has the same columns as the input, plus 'days_imputed'
    expected_columns = set(sample_market_data_no_gaps.columns).union({'days_imputed'})
    assert set(result_df.columns) == expected_columns

    # Check that the values in the original columns are unchanged
    original_columns = sample_market_data_no_gaps.columns
    assert np.array_equal(result_df[original_columns].values, sample_market_data_no_gaps.values)

    # Check that no days were imputed (all values in days_imputed should be NaN)
    assert result_df['days_imputed'].isna().all()

    # Check that the number of rows hasn't changed
    assert len(result_df) == len(sample_market_data_no_gaps)

    # Check that all dates are present and unchanged
    assert set(result_df['date']) == set(sample_market_data_no_gaps['date'])

    # Additional check: Ensure that the order of the original columns is preserved
    assert list(result_df.columns)[:-1] == list(sample_market_data_no_gaps.columns)
    assert result_df.columns[-1] == 'days_imputed'


@pytest.fixture
def sample_market_data_large_gap():
    """
    Fixture to create a sample DataFrame with a large gap in market data for one coin.

    Returns:
    - pd.DataFrame: Sample market data with a large gap (30 days) for one coin
    """
    date_range = pd.date_range(start='2023-01-01', end='2023-02-15')
    n_days = len(date_range)

    btc_data = pd.DataFrame({
        'date': date_range,
        'coin_id': 'btc',
        'price': 100,
        'volume': 1000,
        'market_cap': 10000,
        'fdv': 12000,
        'circulating_supply': 100,
        'total_supply': 120,
        'data_source': 'source1',
        'updated_at': date_range
    })

    eth_data = pd.DataFrame({
        'date': date_range,
        'coin_id': 'eth',
        'price': [50] * 15 + [np.nan] * 30 + [75] * (n_days - 45),
        'volume': [500] * 15 + [np.nan] * 30 + [750] * (n_days - 45),
        'market_cap': [5000] * 15 + [np.nan] * 30 + [7500] * (n_days - 45),
        'fdv': [6000] * 15 + [np.nan] * 30 + [8000] * (n_days - 45),
        'circulating_supply': [100] * 15 + [np.nan] * 30 + [100] * (n_days - 45),
        'total_supply': [120] * 15 + [np.nan] * 30 + [120] * (n_days - 45),
        'data_source': 'source2',
        'updated_at': date_range
    })

    df = pd.concat([btc_data, eth_data], ignore_index=True)
    return df[df['price'].notna()].reset_index(drop=True)  # Remove rows with NaN to simulate missing data

@pytest.mark.unit
def test_fill_market_data_gaps_large_gap(sample_market_data_large_gap):
    """
    Test the fill_market_data_gaps function with a DataFrame containing a large gap for one coin.

    This test ensures that:
    1. Large gaps (30 days) are filled correctly for all metrics
    2. days_imputed is calculated accurately for the large gap
    3. The resulting DataFrame has the expected shape and content

    Args:
    - sample_market_data_large_gap: Fixture providing sample data with a large gap for one coin
    """
    # Call the function
    result_df = cmd.fill_market_data_gaps(sample_market_data_large_gap)

    # Check that all dates are present for both coins
    assert len(result_df) == 92  # 46 days * 2 coins

    # Check that gaps are filled
    assert result_df['price'].isna().sum() == 0
    assert result_df['volume'].isna().sum() == 0
    assert result_df['market_cap'].isna().sum() == 0
    assert result_df['fdv'].isna().sum() == 0
    assert result_df['circulating_supply'].isna().sum() == 0
    assert result_df['total_supply'].isna().sum() == 0

    # Check days_imputed for the large gap in ETH data
    eth_imputed = result_df[result_df['coin_id'] == 'eth']['days_imputed']
    assert (eth_imputed.iloc[15:45] == range(1, 31)).all()  # 30 days imputed
    assert pd.isna(eth_imputed.iloc[45])  # Last day is not imputed

    # Check that values are correctly forward-filled for ETH
    assert (result_df[result_df['coin_id'] == 'eth']['price'].iloc[15:45] == 50).all()
    assert (result_df[result_df['coin_id'] == 'eth']['volume'].iloc[15:45] == 0).all()  # Volume should be 0 for imputed days
    assert (result_df[result_df['coin_id'] == 'eth']['market_cap'].iloc[15:45] == 5000).all()
    assert (result_df[result_df['coin_id'] == 'eth']['fdv'].iloc[15:45] == 6000).all()
    assert (result_df[result_df['coin_id'] == 'eth']['circulating_supply'].iloc[15:45] == 100).all()
    assert (result_df[result_df['coin_id'] == 'eth']['total_supply'].iloc[15:45] == 120).all()

    # Check that BTC data is unchanged
    btc_data = result_df[result_df['coin_id'] == 'btc']
    assert np.array_equal(btc_data[sample_market_data_large_gap.columns].values,
                            sample_market_data_large_gap[sample_market_data_large_gap['coin_id'] == 'btc'].values)

    # Check that updated_at is NaT for imputed days
    assert result_df[result_df['coin_id'] == 'eth']['updated_at'].iloc[15:45].isna().all()


@pytest.fixture
def sample_market_data_non_consecutive():
    """
    Fixture to create a sample DataFrame with non-consecutive dates (weekends missing).
    Returns:
    - pd.DataFrame: Sample market data with weekends omitted
    """
    # Create a date range for weekdays only
    date_range = pd.bdate_range(start='2023-01-01', end='2023-01-31')

    # Create BTC dataset
    btc_data = pd.DataFrame({
        'date': date_range,
        'coin_id': 'btc',
        'price': [100 + i for i in range(len(date_range))],
        'volume': 1000,
        'market_cap': 10000,
        'fdv': 12000,
        'circulating_supply': 100,
        'total_supply': 120,
        'data_source': 'source1',
        'updated_at': date_range
    })

    # Create ETH dataset
    eth_data = pd.DataFrame({
        'date': date_range,
        'coin_id': 'eth',
        'price': [50 + i for i in range(len(date_range))],
        'volume': 500,
        'market_cap': 5000,
        'fdv': 6000,
        'circulating_supply': 100,
        'total_supply': 120,
        'data_source': 'source2',
        'updated_at': date_range
    })

    # Concatenate the datasets
    df = pd.concat([btc_data, eth_data], ignore_index=True)

    # Sort the DataFrame and reset the index
    return df.sort_values(['coin_id', 'date']).reset_index(drop=True)

@pytest.mark.unit
def test_fill_market_data_gaps_non_consecutive(sample_market_data_non_consecutive):
    """
    Test the fill_market_data_gaps function with a DataFrame containing non-consecutive dates (weekends missing).

    This test ensures that:
    1. All dates (including weekends) are filled in the result
    2. days_imputed is calculated accurately for weekend days
    3. The resulting DataFrame has the expected shape and content

    Args:
    - sample_market_data_non_consecutive: Fixture providing sample data with non-consecutive dates
    """
    # Call the function
    result_df = cmd.fill_market_data_gaps(sample_market_data_non_consecutive)

    # Check that all dates (including weekends) are present in the result
    expected_date_range = pd.date_range(start=sample_market_data_non_consecutive['date'].min(),
                                        end=sample_market_data_non_consecutive['date'].max())
    assert set(result_df['date']) == set(expected_date_range)

    # Check that the number of rows has increased to include weekend days
    assert len(result_df) > len(sample_market_data_non_consecutive)

    # Check days_imputed for weekend days
    for coin in ['btc', 'eth']:
        coin_data = result_df[result_df['coin_id'] == coin]
        weekend_days = coin_data[coin_data['date'].dt.dayofweek.isin([5, 6])]
        assert (weekend_days['days_imputed'] > 0).all()
        assert (weekend_days['days_imputed'] <= 2).all()  # Should be 1 or 2 for weekends

    # Check that weekday data remains unchanged
    weekday_data = result_df[~result_df['date'].dt.dayofweek.isin([5, 6])]
    original_weekday_data = sample_market_data_non_consecutive
    for column in ['price', 'volume', 'market_cap', 'fdv', 'circulating_supply', 'total_supply']:
        assert np.array_equal(weekday_data[column].values, original_weekday_data[column].values)

    # Check that volume is 0 for imputed weekend days
    weekend_volume = result_df[(result_df['date'].dt.dayofweek.isin([5, 6])) & (result_df['days_imputed'] > 0)]['volume']
    assert (weekend_volume == 0).all()

    # Check that other metrics are forward-filled for weekend days
    for coin in ['btc', 'eth']:
        coin_data = result_df[result_df['coin_id'] == coin].sort_values('date')
        for column in ['price', 'market_cap', 'fdv', 'circulating_supply', 'total_supply']:
            assert (coin_data[column].diff().dropna() >= 0).all(), f"{column} for {coin} is not non-decreasing"

    # Check that updated_at is NaT for imputed days
    imputed_days = result_df[result_df['days_imputed'] > 0]
    assert imputed_days['updated_at'].isna().all()



@pytest.fixture
def sample_data_with_dips():
    """
    Fixture to create a sample DataFrame with clear single-day dips for multiple coins.
    The dips are now set to represent an 80% drop and 90% recovery.

    Returns:
    - pd.DataFrame: Sample market data with single-day dips
    """
    data = {
        'coin_id': ['btc', 'btc', 'btc', 'btc', 'btc', 'eth', 'eth', 'eth', 'eth', 'eth'],
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
                                '2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']),
        'price': [100, 15, 95, 110, 115,  # BTC with a dip on day 2 (80% drop, 95% recovery)
                  200, 210, 40, 195, 230],  # ETH with a dip on day 3 (81% drop, 93% recovery)
        'market_cap': [10000, 2000, 9500, 11000, 11500,
                       20000, 21000, 4000, 19500, 23000],
        'fdv': [12000, 2400, 11400, 13200, 13800,
                24000, 25200, 4800, 23400, 27600],
        'volume': [1000, 1500, 1200, 1100, 1300,
                   2000, 2100, 2500, 2200, 2300],
        'circulating_supply': [100, 100, 100, 100, 100,
                               100, 100, 100, 100, 100],
        'total_supply': [120, 120, 120, 120, 120,
                         120, 120, 120, 120, 120]
    }
    return pd.DataFrame(data)

@pytest.mark.unit
def test_remove_single_day_dips_normal_scenario(sample_data_with_dips):
    """
    Test the remove_single_day_dips function with a dataset containing clear single-day dips.

    This test ensures that:
    1. Single-day dips are correctly identified and removed
    2. The resulting DataFrame has the correct number of rows
    3. The dip rows are no longer present in the result
    4. Non-dip rows remain unchanged

    Args:
    - sample_data_with_dips: Fixture providing sample data with single-day dips
    """
    # Call the function
    result_df = cmd.remove_single_day_dips(sample_data_with_dips, dip_threshold=0.2, recovery_threshold=0.9)

    # 1. Check that the correct number of rows were removed (2 dips)
    assert len(result_df) == len(sample_data_with_dips) - 2

    # 2. Check that the dip rows are removed
    # For BTC: Day 2 (2023-01-02) should be removed
    assert not any((result_df['coin_id'] == 'btc') & (result_df['date'] == '2023-01-02'))

    # For ETH: Day 3 (2023-01-03) should be removed
    assert not any((result_df['coin_id'] == 'eth') & (result_df['date'] == '2023-01-03'))

    # 3. Check that non-dip rows remain unchanged
    btc_non_dip = result_df[result_df['coin_id'] == 'btc']
    eth_non_dip = result_df[result_df['coin_id'] == 'eth']

    assert np.array_equal(btc_non_dip['price'].values, [100, 95, 110, 115])
    assert np.array_equal(eth_non_dip['price'].values, [200, 210, 195, 230])

    # 4. Check that other columns for non-dip rows are unchanged
    for col in ['market_cap', 'fdv', 'volume', 'circulating_supply', 'total_supply']:
        assert np.array_equal(
            btc_non_dip[col].values,
            sample_data_with_dips[(sample_data_with_dips['coin_id'] == 'btc') &
                                    (sample_data_with_dips['date'] != '2023-01-02')][col].values
        )
        assert np.array_equal(
            eth_non_dip[col].values,
            sample_data_with_dips[(sample_data_with_dips['coin_id'] == 'eth') &
                                    (sample_data_with_dips['date'] != '2023-01-03')][col].values
        )

    # 5. Check that the function logs the correct number of removed dips
    # Note: This assumes that the function uses a logger. If it doesn't, you can remove this assertion.
    # You might need to use a mocking library to capture the log output for this assertion.
    # assert "Removed 2 single-day dips from the market data." in caplog.text

    # 6. Check that 'prev_price' and 'next_price' columns are not in the result
    assert 'prev_price' not in result_df.columns
    assert 'next_price' not in result_df.columns