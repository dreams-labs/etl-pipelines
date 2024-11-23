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
import pytest
from dotenv import load_dotenv
from dreams_core import core as dc

# Project Modules
# pyright: reportMissingImports=false
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../cloud_functions/core_coin_wallet_profits')))
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

@pytest.fixture
def sample_transfers_df():
    """
    Create a sample transfers DataFrame for testing.
    """
    data = {
        'coin_id': ['BTC', 'BTC', 'ETH', 'ETH', 'BTC', 'ETH', 'MYRO', 'MYRO', 'MYRO',
                    'BTC', 'ETH', 'BTC', 'ETH', 'MYRO'],
        'wallet_address': ['wallet1', 'wallet1', 'wallet1', 'wallet2', 'wallet2',
                           'wallet2', 'wallet3', 'wallet3', 'wallet3', 'wallet1',
                           'wallet1', 'wallet2', 'wallet2', 'wallet3'],
        'date': [
            '2023-01-01', '2023-02-01', '2023-01-01', '2023-01-01', '2023-01-01', '2023-02-01',
            '2023-01-01', '2023-02-01', '2023-03-01',
            '2023-04-01', '2023-04-01', '2023-04-01', '2023-04-01', '2023-04-01'
        ],
        'net_transfers': [10.0, 5, 100, 50, 20, 25, 1000, 500, -750,
                          0, 0, 0, -10, 0],
        'balance': [10.0, 15, 100, 50, 20, 75, 1000, 1500, 750,
                    15, 100, 20, 65, 750]
    }
    df = pd.DataFrame(data)

    # Convert coin_id to categorical and date to datetime
    df['coin_id'] = df['coin_id'].astype('category')
    df['date'] = pd.to_datetime(df['date'])

    return df

@pytest.fixture
def sample_prices_df():
    """
    Create a sample prices DataFrame for testing.
    """
    data = {
        'date': [
            '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01',
            '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01',
            '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'
        ],
        'coin_id': ['BTC', 'BTC', 'BTC', 'BTC', 'ETH', 'ETH', 'ETH', 'ETH', 'MYRO', 'MYRO', 'MYRO', 'MYRO'],
        'price': [20000.0, 21000, 22000, 23000, 1500, 1600, 1700, 1800, 10, 15, 12, 8]
    }
    df = pd.DataFrame(data)

    # Convert coin_id to categorical and date to datetime
    df['coin_id'] = df['coin_id'].astype('category')
    df['date'] = pd.to_datetime(df['date'])

    return df

@pytest.mark.unit
def test_calculate_wallet_profitability_profitability(sample_transfers_df, sample_prices_df):
    """
    Test profitability calculations for multiple wallets and coins.
    """
    profits_df = cwp.prepare_profits_data(sample_transfers_df, sample_prices_df)
    result = cwp.calculate_wallet_profitability(profits_df)

    # Check profitability for wallet1, BTC
    wallet1_btc = result[
        (result['wallet_address'] == 'wallet1') & (result['coin_id'] == 'BTC')
    ]
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-02-01', 'profits_change'].values[0] \
        == pytest.approx(10000)  # (21000 - 20000) * 10
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-02-01', 'profits_cumulative'].values[0] \
        == pytest.approx(10000)
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-04-01', 'profits_change'].values[0] \
        == pytest.approx(30000)  # (23000 - 21000) * 15
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-04-01', 'profits_cumulative'].values[0] \
        == pytest.approx(40000)  # 10000 + 15000 + 15000

    # Check profitability for wallet2, ETH
    wallet2_eth = result[
        (result['wallet_address'] == 'wallet2') & (result['coin_id'] == 'ETH')
    ]
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-02-01', 'profits_change'].values[0] \
        == pytest.approx(5000)  # (1600 - 1500) * 50
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-02-01', 'profits_cumulative'].values[0] \
        == pytest.approx(5000)
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-04-01', 'profits_change'].values[0] \
        == pytest.approx(15000)  # (1800 - 1600) * 75
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-04-01', 'profits_cumulative'].values[0] \
        == pytest.approx(20000)  # 5000 + 15000

    # Check profitability for wallet3, MYRO
    wallet3_myro = result[
        (result['wallet_address'] == 'wallet3') & (result['coin_id'] == 'MYRO')
    ]
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-02-01', 'profits_change'].values[0] \
        == pytest.approx(5000)  # (15 - 10) * 1000
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-03-01', 'profits_change'].values[0] \
        == pytest.approx(-4500)  # (12 - 15) * 1500
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-03-01', 'profits_cumulative'].values[0] \
        == pytest.approx(500)
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-04-01', 'profits_change'].values[0] \
        == pytest.approx(-3000)  # (8 - 12) * 750
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-04-01', 'profits_cumulative'].values[0] \
        == pytest.approx(-2500)  # 500 - 3000


# pylint: disable=R0914 # too many local variables
@pytest.mark.unit
def test_calculate_wallet_profitability_usd_calculations(sample_transfers_df, sample_prices_df):
    """
    Test USD-related calculations (inflows, balances, total return).
    """
    profits_df = cwp.prepare_profits_data(sample_transfers_df, sample_prices_df)
    result = cwp.calculate_wallet_profitability(profits_df)

    # Check USD calculations for wallet1, BTC
    wallet1_btc = result[(result['wallet_address'] == 'wallet1') & (result['coin_id'] == 'BTC')]
    initial_investment_wallet1 = 10 * 20000  # 10 BTC * $20,000
    second_investment_wallet1 = 5 * 21000    # 5 BTC * $21,000
    total_investment_wallet1 = initial_investment_wallet1 + second_investment_wallet1
    expected_balance_wallet1 = 15 * 23000    # 15 BTC * $23,000
    expected_total_return_wallet1 = 40000 / total_investment_wallet1

    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-01-01', 'usd_inflows'].values[0] \
        == pytest.approx(initial_investment_wallet1)
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-02-01', 'usd_inflows'].values[0] \
        == pytest.approx(second_investment_wallet1)
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-02-01', 'usd_inflows_cumulative'].values[0] \
        == pytest.approx(total_investment_wallet1)
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-04-01', 'usd_balance'].values[0] \
        == pytest.approx(expected_balance_wallet1)
    assert wallet1_btc.loc[wallet1_btc['date'] == '2023-04-01', 'total_return'].values[0] \
        == pytest.approx(expected_total_return_wallet1)

    # Check USD calculations for wallet2, ETH
    wallet2_eth = result[(result['wallet_address'] == 'wallet2') & (result['coin_id'] == 'ETH')]
    initial_investment_wallet2 = 50 * 1500  # 50 ETH * $1,500
    second_investment_wallet2 = 25 * 1600  # 25 ETH * $1,600
    total_investment_wallet2 = initial_investment_wallet2 + second_investment_wallet2
    expected_balance_wallet2 = 65 * 1800  # 65 ETH * $1,800
    expected_total_return_wallet2 = 20000 / total_investment_wallet2

    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-01-01', 'usd_inflows'].values[0] \
        == pytest.approx(initial_investment_wallet2)
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-02-01', 'usd_inflows'].values[0] \
        == pytest.approx(second_investment_wallet2)
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-04-01', 'usd_inflows_cumulative'].values[0] \
        == pytest.approx(total_investment_wallet2)
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-04-01', 'usd_balance'].values[0] \
        == pytest.approx(expected_balance_wallet2)
    assert wallet2_eth.loc[wallet2_eth['date'] == '2023-04-01', 'total_return'].values[0] \
        == pytest.approx(expected_total_return_wallet2)

    # Check USD calculations for wallet3, MYRO
    wallet3_myro = result[(result['wallet_address'] == 'wallet3') & (result['coin_id'] == 'MYRO')]
    initial_investment_wallet3 = 1000 * 10  # 1000 MYRO * $10
    second_investment_wallet3 = 500 * 15   # 500 MYRO * $15
    total_investment_wallet3 = initial_investment_wallet3 + second_investment_wallet3
    expected_balance_wallet3 = 750 * 8  # 750 MYRO * $8
    current_value_wallet3 = 750 * 8  # Current holdings: 750 MYRO * $8
    sold_value_wallet3 = 750 * 12     # Sold tokens: 750 MYRO * $12
    profit_wallet3 = current_value_wallet3 + sold_value_wallet3 - total_investment_wallet3
    expected_total_return_wallet3 = profit_wallet3 / total_investment_wallet3

    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-01-01', 'usd_inflows'].values[0] \
        == pytest.approx(initial_investment_wallet3)
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-02-01', 'usd_inflows'].values[0] \
        == pytest.approx(second_investment_wallet3)
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-03-01', 'usd_inflows_cumulative'].values[0] \
        == pytest.approx(total_investment_wallet3)
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-04-01', 'usd_balance'].values[0] \
        == pytest.approx(expected_balance_wallet3)
    assert wallet3_myro.loc[wallet3_myro['date'] == '2023-04-01', 'total_return'].values[0] \
        == pytest.approx(expected_total_return_wallet3)


@pytest.fixture
def price_data_transfers_df():
    """
    Create a sample transfers DataFrame for testing interactions with price data.
    """
    data = {
        'coin_id': ['BTC', 'BTC', 'MYRO', 'MYRO'],
        'wallet_address': ['wallet1', 'wallet1', 'wallet2', 'wallet2'],
        'date': [
            '2023-03-01', '2023-04-01',  # BTC wallet1 buys during training period
            '2023-02-20', '2023-03-10'   # MYRO wallet2 buys during training period (before price data)
        ],
        'net_transfers': [10.0, -10.0, 1000.0, -1000.0],  # Buys and sells
        'balance': [10.0, 0.0, 1000.0, 0.0]  # Balance adjustments after buy and sell
    }
    df = pd.DataFrame(data)
    df['coin_id'] = df['coin_id'].astype('category')
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture
def price_data_prices_df():
    """
    Create a sample prices DataFrame for testing interactions with price data.
    """
    # Define the coins and the date range
    coins = ['BTC', 'MYRO']
    date_range = pd.date_range(start='2023-03-15', end='2023-04-01', freq='D')

    # Create all combinations of coin_id and date
    data = pd.DataFrame([(coin, date) for coin in coins for date in date_range]
                        , columns=['coin_id', 'date'])

    # Generate linear prices for each coin
    data['price'] = data.apply(
        lambda row:  ((
                1000 * (row['date'] - pd.Timestamp('2023-03-15')).days + 10000
                ) if row['coin_id'] == 'BTC'
            else (
                10 * (row['date'] - pd.Timestamp('2023-03-15')).days + 100)
        )
        ,axis=1)

    # Convert 'coin_id' to categorical
    data['coin_id'] = data['coin_id'].astype('category')
    return data


@pytest.mark.unit
def test_price_data_interactions(price_data_transfers_df, price_data_prices_df):
    """
    Test interactions between wallet transfers and available price data.
    """
    profits_df = cwp.prepare_profits_data(price_data_transfers_df, price_data_prices_df)
    result = cwp.calculate_wallet_profitability(profits_df)

    # Test scenario: Buy during training period before price data, sell after price data
    wallet1_btc = result[(result['wallet_address'] == 'wallet1') & (result['coin_id'] == 'BTC')]
    wallet1_btc_profits = (27000-10000) * 10

    # First row should reflect earliest price data
    assert wallet1_btc.iloc[0]['date'] == pd.Timestamp('2023-03-15')

    # No profit on initial transfer in
    assert wallet1_btc.iloc[0]['profits_cumulative'] == 0

    # Profitability calculation should be valid
    assert wallet1_btc.iloc[1]['profits_cumulative'] == wallet1_btc_profits

    # Test scenario: Buy and sell before price data is available
    wallet2_myro = result[(result['wallet_address'] == 'wallet2') & (result['coin_id'] == 'MYRO')]

    # No rows should exist, as no price data was available for the transaction
    assert wallet2_myro.empty
