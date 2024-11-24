"""
Cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_profits
This function is heavily optimized to reduce memory usage by taking advantage of categorical
columns, int32, and dropping columns as soon as they are no longer needed.
"""
import time
import gc
import pandas as pd
import numpy as np
import functions_framework
import pandas_gbq
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()

@functions_framework.http
def update_core_coin_wallet_profits(batch_number=None):  # pylint: disable=W0613
    """
    runs all functions in sequence to refresh core.coin_wallet_profits
    """
    start_time = time.time()

    # Retrieve transfers and prices data
    transfers_df, wallet_address_mapping = retrieve_transfers_data(batch_number)
    prices_df = retrieve_prices_df(batch_number)
    logger.info('<1> Retrieved transfers and prices data  (%.2f seconds).',
                time.time() - start_time)
    step_time = time.time()

    # Calculate basic profitability data from transfers and prices, then clear memory
    profits_df = merge_prices_and_transfers(transfers_df, prices_df)
    profits_df = add_first_price_info(profits_df,prices_df)
    del transfers_df,prices_df
    gc.collect()
    logger.info('<2> Merged transfers and prices data after (%.2f seconds).',
                time.time() - step_time)
    step_time = time.time()

    # Append new records to accomodate transfer history without price data
    imputed_records = create_imputed_records(profits_df)
    profits_df = append_imputed_records(profits_df, imputed_records)
    profits_df = filter_pre_inflow_records(profits_df)
    logger.info('<3> Imputed and filtered records to align wallet and price timing (%.2f seconds).',
                time.time() - step_time)
    step_time = time.time()

    # Calculate USD profitability metrics
    profits_df = calculate_wallet_profitability(profits_df)
    logger.info('<4> Calculated wallet proftability (%.2f seconds).',
                time.time() - step_time)
    step_time = time.time()


    # Upload the df
    profits_df['wallet_address'] = wallet_address_mapping[profits_df['wallet_address']]
    upload_profits_data(profits_df, batch_number)
    logger.info('<5> Uploaded profits data.  (%.2f seconds).',
                time.time() - step_time)

    return '{{"profits_df upload successful."}}'



def retrieve_transfers_data(batch_number=None):
    """
    Retrieves market data from the core.coin_wallet_transfers table and converts columns to
    memory-efficient formats. If a batch number is provided, the transfers data is limited
    to only the coin_ids associated with the batch_number in temp.temp_coin_batches table.

    Parameters:
    - batch_number (int): the batch number in the temp.temp_coin_batches table specifying
        which coins to process

    Returns:
    - transfers_df (DataFrame): df containing transfers data
    - wallet_address_mapping (pandas Index): mapping that will allow us to convert wallet_address
        back to the original strings, rather than the integer values they get mapped to for
        memory optimization
    """
    start_time = time.time()
    logger.info('Retrieving transfers data...')

    # Generate SQL to limit coins to only specified batch if applicable
    batch_sql = ""
    if batch_number is not None:
        logger.info('Generating transfers query for coin_id batch %s...', batch_number)

        batch_sql = f"""
        join temp.temp_coin_batches b on b.coin_id = cwt.coin_id
        and b.batch_number = {batch_number}
        """

    # SQL query to retrieve transfers data
    query_sql = f"""
        select cwt.coin_id
        ,cwt.wallet_address
        ,cwt.date
        ,cast(cwt.net_transfers as float64) as net_transfers
        ,cast(cwt.balance as float64) as balance
        from `core.coin_wallet_transfers` cwt

        -- inner join to filter onto only coins with price data
        join (
            select coin_id
            from `core.coin_market_data`
            group by 1
        ) cmd on cmd.coin_id = cwt.coin_id

        {batch_sql}
        """


    # Run the SQL query using dgc's run_sql method
    transfers_df = dgc().run_sql(query_sql)

    logger.info('Converting columns to memory-efficient data types...')

    # Convert coin_id to categorical (original strings are preserved)
    transfers_df['coin_id'] = transfers_df['coin_id'].astype('category')

    # Dates as dates
    transfers_df['date'] = pd.to_datetime(transfers_df['date'])

    # Convert wallet_address to categorical, store the mapping, and convert the column to int32
    wallet_address_categorical = transfers_df['wallet_address'].astype('category')
    wallet_address_mapping = wallet_address_categorical.cat.categories
    transfers_df['wallet_address'] = wallet_address_categorical.cat.codes.astype('uint32')

    logger.info('Retrieved market_data_df with %s rows after %.2f seconds',
                len(transfers_df),
                time.time()-start_time)

    return transfers_df, wallet_address_mapping



def retrieve_prices_df(batch_number=None):
    """
    Retrieves market data from the core.coin_market_data table and converts coin_id to categorical

    Parameters:
    - batch_number (int): the batch number in the temp.temp_coin_batches table specifying
        which coins to process

    Returns:
    - prices_df: DataFrame containing market data with 'coin_id' as a categorical column.
    - coin_id_mapping: Categories for coin_id.
    """
    start_time = time.time()
    logger.info('Retrieving market data...')

    # Generate SQL to limit coins to only specified batch if applicable
    batch_sql = ""
    if batch_number is not None:
        batch_sql = f"""
        join temp.temp_coin_batches b on b.coin_id = cmd.coin_id
        and b.batch_number = {batch_number}
        """

    # SQL query to retrieve market data
    query_sql = f"""
        select cmd.coin_id
        ,date
        ,cast(cmd.price as float64) as price
        from core.coin_market_data cmd
        {batch_sql}
        order by 1,2
    """

    # Run the SQL query using dgc's run_sql method
    prices_df = dgc().run_sql(query_sql)

    # Convert coin_id column to categorical to reduce memory usage
    prices_df['coin_id'] = prices_df['coin_id'].astype('category')

    # Downcast numeric columns to reduce memory usage
    prices_df['price'] = pd.to_numeric(prices_df['price'], downcast='float')

    # Dates as dates
    prices_df['date'] = pd.to_datetime(prices_df['date']).dt.date

    logger.info('Retrieved prices_df with %s unique coins and %s rows after %s seconds',
                len(set(prices_df['coin_id'])),
                len(prices_df),
                round(time.time()-start_time, 1))

    return prices_df



def merge_prices_and_transfers(transfers_df, prices_df):
    """
    Merges wallet transfer data with coin price data, performing basic data preparation
    and validation. This function handles the fundamental data joining operation without
    any special handling of missing historical price data.

    Parameters
    ----------
    transfers_df : pd.DataFrame
        DataFrame containing wallet transaction data with columns:
        - coin_id: Identifier for the coin/token
        - wallet_address: Unique identifier for the wallet
        - date: Transaction date
        - net_transfers: Net tokens transferred in/out of wallet on this date
        - balance: Token balance in wallet at end of day

    prices_df : pd.DataFrame
        DataFrame containing price data with columns:
        - coin_id: Identifier for the coin/token
        - date: Date of the price record
        - price: Price of the coin/token on that date

    Returns
    -------
    pd.DataFrame
        Merged DataFrame containing all transfer data with corresponding prices where available.
        Price will be NaN for any transfer dates without corresponding price data.
        Maintains original transfer data integrity - no records are dropped or modified.

    Raises
    ------
    ValueError
        If either input DataFrame is empty
    """
    start_time = time.time()
    logger.info('Merging prices and transfers into profits_df...')

    # Input validation
    if transfers_df.empty or prices_df.empty:
        raise ValueError("Input DataFrames cannot be empty")

    # Create copies to avoid modifying original DataFrames
    transfers_df = transfers_df.copy()
    prices_df = prices_df.copy()

    # Convert dates to datetime for consistent joining
    transfers_df['date'] = pd.to_datetime(transfers_df['date'])
    prices_df['date'] = pd.to_datetime(prices_df['date'])

    # Set coin_id and date as indices for the merge
    # This ensures we join on both fields and maintain proper alignment
    transfers_df = transfers_df.set_index(['coin_id', 'date'])
    prices_df = prices_df.set_index(['coin_id', 'date'])

    # Merge transfers with prices
    # - left merge preserves all transfer records
    # - missing prices will result in NaN values
    profits_df = (transfers_df
                .merge(prices_df,
                      left_index=True,
                      right_index=True,
                      how='left')
                .reset_index())

    # Ensure coin_id remains categorical for efficiency
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Sort by coin, wallet, and date for consistency
    profits_df = (profits_df
                .sort_values(['coin_id', 'wallet_address', 'date'])
                .reset_index(drop=True))

    logger.info("Merge of transfers and prices complete: %.2f seconds",
                 time.time() - start_time)


    return profits_df



def add_first_price_info(profits_df, prices_df):
    """
    Adds first_price_date and first_price columns for each coin in the dataset.
    These columns indicate when price data becomes available for each coin.

    Parameters:
    - profits_df (pd.DataFrame): Merged transfers and prices with coin_id, date, and price
    - prices_df (pd.DataFrame): Original price data with coin_id, date, and price columns

    Returns
    - profits_df (pd.DataFrame): Input df with first_price_date and first_price columns added
    """
    if profits_df.empty:
        raise ValueError("Input DataFrame cannot be empty")
    if prices_df.empty:
        raise ValueError("Prices DataFrame cannot be empty")

    df = profits_df.copy()

    # Get first price date and value for each coin directly from prices_df
    first_prices = (prices_df
                   .groupby('coin_id', observed=True)
                   .agg({'date': 'min', 'price': 'first'})
                   .reset_index()
                   .rename(columns={'date': 'first_price_date', 'price': 'first_price'}))

    # Append price columns and set coin_id to categorical
    profits_df = df.merge(first_prices, on='coin_id', how='left')
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    return profits_df



def create_imputed_records(profits_df):
    """
    Creates imputed records for wallets that have activity before price data exists.
    For each such wallet, creates a record on first_price_date that carries forward
    their balance as a transfer-in.

    Imputation is assessed independently for each wallet-coin combination.

    Parameters:
    - profits_df (pd.DataFrame): df with transfer and price data, including first_price_date

    Returns:
    - imputed_records_df (pd.DataFrame): New records to be added, with same schema as input df
    """
    start_time = time.time()
    logger.info('Generating imputed rows for profits_df...')

    if profits_df.empty:
        return pd.DataFrame(columns=profits_df.columns)

    # Group by coin and wallet to assess each combination independently
    groups = profits_df.groupby(['coin_id', 'wallet_address'], observed=True)

    imputation_needed = []
    for _, group_df in groups:
        # Check conditions for this specific wallet-coin combination
        has_transfers_before_prices = not group_df[
            group_df['date'] < group_df['first_price_date']].empty
        no_activity_on_first_price_date = group_df[
            group_df['date'] == group_df['first_price_date']].empty

        if has_transfers_before_prices and no_activity_on_first_price_date:
            # Get the latest pre-price balance for this wallet-coin
            pre_price_balance = (
                group_df[group_df['date'] < group_df['first_price_date']]
                .sort_values('date')
                .iloc[-1]
            )
            imputation_needed.append(pre_price_balance)

    if not imputation_needed:
        return pd.DataFrame(columns=profits_df.columns)

    # Create imputed records for all wallets needing them
    imputed_records = pd.DataFrame(imputation_needed)

    # Set up the imputed records with carried forward balance as transfer
    imputed_records['coin_id'] = imputed_records['coin_id'].astype('category')
    imputed_records['date'] = imputed_records['first_price_date']
    imputed_records['net_transfers'] = imputed_records['balance']
    imputed_records['price'] = imputed_records['first_price']

    logger.info("Row imputation complete: %.2f seconds",
                 time.time() - start_time)

    # Return only the columns from the input DataFrame
    return imputed_records[profits_df.columns]



def append_imputed_records(profits_df, imputed_records):
    """
    Updates earliest post-price records to show full balance as transfer-in,
    including imputed records that may become the earliest record.

    Parameters:
    - profits_df (pd.DataFrame): DataFrame with transfer and price data
    - imputed_records (pd.DataFrame): New records to be added, with same schema as profits_df

    Returns:
    - combined_df (pd.DataFrame): DataFrame with correct transfer records from price data start
    """
    start_time = time.time()
    logger.info('Appending imputed rows to profits_df...')

    if profits_df.empty:
        return profits_df

    # Start with records that have price data
    result_df = profits_df[profits_df['price'].notna()].copy()

    # Add imputed records first as they may be earliest for some wallet-coin pairs
    if not imputed_records.empty:
        result_df = pd.concat([result_df, imputed_records], ignore_index=True)
        result_df = result_df.sort_values(['coin_id', 'wallet_address', 'date'])

    # Find indices of earliest records for each wallet-coin pair
    earliest_idx = result_df.groupby(['coin_id', 'wallet_address'], observed=True)['date'].idxmin()

    # Update net_transfers to match balance for these records
    result_df.loc[earliest_idx, 'net_transfers'] = result_df.loc[earliest_idx, 'balance']

    # Reset coin_id to categorical
    result_df['coin_id'] = result_df['coin_id'].astype('category')

    logger.info("Finished appending imputed records: %.2f seconds",
                 time.time() - start_time)


    return result_df.reset_index(drop=True)



def filter_pre_inflow_records(profits_df):
    """
    Removes records before each wallet's first token inflow and drops temporary columns.
    Wallets will have a transfer of 0 on a date prior to their first recorded inflows in
    scenarios when a single date has an inflow and outflow that fully offset.

    Parameters:
    - profits_df (pd.DataFrame): DataFrame with transfer and price data

    Returns:
    - filtered_df (pd.DataFrame): DataFrame with only post-inflow records
    """
    start_time = time.time()
    logger.info('Filtering missing data and formatting profits_df...')

    if profits_df.empty:
        return profits_df

    result_df = profits_df.copy()

    # Calculate cumulative inflows for each wallet-coin pair
    result_df['token_inflows'] = result_df['net_transfers'].clip(lower=0)
    result_df['token_inflows_cumulative'] = (
        result_df.groupby(['coin_id', 'wallet_address'], observed=True)['token_inflows']
        .cumsum()
    )

    # Keep only records after first positive inflow
    result_df = result_df[result_df['token_inflows_cumulative'] > 0]

    # Drop temporary columns
    result_df = result_df.drop(columns=[
        'first_price_date',
        'first_price',
        'token_inflows',
        'token_inflows_cumulative'
    ])

    logger.info("Finished filtering and formatting records: %.2f seconds",
                 time.time() - start_time)

    return result_df.reset_index(drop=True)



def calculate_wallet_profitability(profits_df):
    """
    Calculates the profitability metrics for each wallet-coin pair by analyzing changes in price
    and balance over time. The function computes both daily profitability changes and cumulative
    profitability, along with additional metrics such as USD inflows and returns.

    Process Summary:
    1. Ensure there are no missing prices in the `profits_df`.
    2. Calculate the daily profitability based on price changes and previous balances.
    3. Compute the cumulative profitability for each wallet-coin pair using `cumsum()`.
    4. Calculate USD balances, net transfers, inflows, and the overall rate of return.

    Parameters:
    - profits_df (pd.DataFrame):
        A DataFrame containing merged wallet transaction and price data with columns:
        - coin_id: The ID of the coin/token.
        - wallet_address: The unique identifier of the wallet.
        - date: The date of the transaction.
        - net_transfers: The net tokens transferred in or out of the wallet on that date.
        - balance: The token balance in the wallet at the end of the day.
        - price: The price of the coin/token on that date.

    Returns:
    - pd.DataFrame:
        A DataFrame with the following additional columns:
        - profits_change: The daily change in profitability, calculated as the difference between
          the current price and the previous price, multiplied by the previous balance.
        - profits_cumulative: The cumulative profitability for each wallet-coin pair over time.
        - usd_balance: The USD value of the wallet's balance, based on the current price.
        - usd_net_transfers: The USD value of the net transfers on a given day.
        - usd_inflows: The USD value of net transfers into the wallet (positive transfers only).
        - usd_inflows_cumulative: The cumulative USD inflows for each wallet-coin pair.
        - total_return: The total return, calculated as total profits divided by total USD inflows.

    Raises:
    - ValueError: If any missing prices are found in the `profits_df`.
    """
    start_time = time.time()
    logger.info('Calculating coin-wallet level profitability...')

    # Raise an error if there are any missing prices
    if profits_df['price'].isnull().any():
        raise ValueError("Missing prices found for transfer dates which should not be possible.")

    # create offset price and balance rows to easily calculate changes between periods
    profits_df['previous_price'] = (
        profits_df.groupby(['coin_id', 'wallet_address'], observed=True)['price']
        .shift(1))
    profits_df['previous_price'] = profits_df['previous_price'].fillna(profits_df['price'])
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    profits_df['previous_balance'] = (
        profits_df.groupby(['coin_id', 'wallet_address'], observed=True)['balance']
        .shift(1).fillna(0))
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    logger.info("Offset prices and balances for profitability logic: %.2f seconds",
                 time.time() - start_time)
    step_time = time.time()

    # Calculate the profits change in each period and sum them to get cumulative profitability
    profits_df['profits_change'] = (
        (profits_df['price'] - profits_df['previous_price'])
        * profits_df['previous_balance']
    )
    profits_df = dc.safe_downcast(profits_df,'profits_change','float32')

    # Drop helper columns to preserve memory
    profits_df.drop(columns=['previous_price', 'previous_balance'], inplace=True)

    # Calculate cumulative profits
    profits_df['profits_cumulative'] = (
        profits_df.groupby(['coin_id', 'wallet_address'], observed=True)['profits_change']
        .cumsum())
    profits_df = dc.safe_downcast(profits_df,'profits_cumulative','float32')

    # Reconvert to category
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    logger.info("Calculate profitability: %.2f seconds", time.time() - step_time)
    step_time = time.time()

    # Calculate USD inflows, balances, and rate of return
    profits_df['usd_balance'] = profits_df['balance'] * profits_df['price']
    profits_df = dc.safe_downcast(profits_df,'usd_balance','float32')

    profits_df['usd_net_transfers'] = profits_df['net_transfers'] * profits_df['price']
    profits_df = dc.safe_downcast(profits_df,'usd_net_transfers','float32')

    # Drop price and token-denominated columns to preserve memory
    profits_df.drop(columns=['price', 'balance', 'net_transfers'], inplace=True)

    # Calculate usd inflows metrics
    profits_df['usd_inflows'] = (profits_df['usd_net_transfers']
                                 .where(profits_df['usd_net_transfers'] > 0, 0))
    profits_df = dc.safe_downcast(profits_df,'usd_inflows','float32')

    profits_df['usd_inflows_cumulative'] = (
        profits_df.groupby(['coin_id', 'wallet_address'], observed=True)['usd_inflows']
        .cumsum())
    profits_df = dc.safe_downcast(profits_df,'usd_inflows_cumulative','float32')

    profits_df['total_return'] = (profits_df['profits_cumulative']
                                  / profits_df['usd_inflows_cumulative']
                                  .where(profits_df['usd_inflows_cumulative'] != 0, np.nan))
    profits_df = dc.safe_downcast(profits_df,'total_return','float32')

    # Final recategorization
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    logger.info("Calculate rate of return %.2f seconds", time.time() - step_time)
    step_time = time.time()

    return profits_df



def upload_profits_data(profits_df,batch_number=None):
    """
    Uploads profits dataframe to either the core.coin_wallet_profits table if there is
    no batch_number provided, or to a temp table based on the batch_number if it is.

    Parameters:
    - profits_df (DataFrame): The DataFrame containing the profits data to upload.
    - batch_number (int): the batch number in the temp.temp_coin_batches table specifying
        which coins to process
    """
    if batch_number is None:
        destination_table = 'core.coin_wallet_profits'
    else:
        destination_table = f'temp.coin_wallet_profits_batch_{batch_number}'

    logger.info('Uploading profits_df with shape (%s) to %s...',
                profits_df.shape, destination_table)

    start_time = time.time()

    # Apply explicit typecasts
    profits_df['date'] = pd.to_datetime(profits_df['date'])
    profits_df['coin_id'] = profits_df['coin_id'].astype(str)
    profits_df['wallet_address'] = profits_df['wallet_address'].astype(str)
    profits_df['profits_change'] = profits_df['profits_change'].astype(float)
    profits_df['profits_cumulative'] = profits_df['profits_cumulative'].astype(float)
    profits_df['usd_balance'] = profits_df['usd_balance'].astype(float)
    profits_df['usd_net_transfers'] = profits_df['usd_net_transfers'].astype(float)
    profits_df['usd_inflows'] = profits_df['usd_inflows'].astype(float)
    profits_df['usd_inflows_cumulative'] = profits_df['usd_inflows_cumulative'].astype(float)
    profits_df['total_return'] = profits_df['total_return'].astype(float)

    # Define the schema
    schema = [
        {'name': 'coin_id', 'type': 'STRING'},
        {'name': 'wallet_address', 'type': 'STRING'},
        {'name': 'date', 'type': 'DATETIME'},
        {'name': 'profits_change', 'type': 'FLOAT'},
        {'name': 'profits_cumulative', 'type': 'FLOAT'},
        {'name': 'usd_balance', 'type': 'FLOAT'},
        {'name': 'usd_net_transfers', 'type': 'FLOAT'},
        {'name': 'usd_inflows', 'type': 'FLOAT'},
        {'name': 'usd_inflows_cumulative', 'type': 'FLOAT'},
        {'name': 'total_return', 'type': 'FLOAT'}
    ]

    # Upload df to BigQuery
    project_id = 'western-verve-411004'
    pandas_gbq.to_gbq(
        profits_df,
        destination_table=destination_table,
        project_id=project_id,
        if_exists='replace',
        table_schema=schema
    )
    logger.info("Upload to %s complete after %.2f seconds",
                destination_table,
                time.time() - start_time)

    # Log batch updates to temp table if applicable
    if batch_number is not None:
        log_batch_sql = f"""
            update temp.temp_coin_batches
            set batch_table = '{destination_table}'
            where batch_number = {batch_number}
            """

        _ = dgc().run_sql(log_batch_sql)
        logger.info("Updated temp.temp_coin_batches for batch %s.", batch_number)
