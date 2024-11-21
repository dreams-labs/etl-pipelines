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
    # Retrieve transfers data and the mapping needed to convert wallet ids back to addresses
    transfers_df, wallet_address_mapping = retrieve_transfers_data(batch_number)

    # Retrieve prices data
    prices_df = retrieve_prices_df()

    # Calculate basic profitability data by comparing price changes
    profits_df = prepare_profits_data(transfers_df, prices_df)

    # Remove DataFrames from memory after they are no longer needed
    del transfers_df, prices_df
    gc.collect()

    # Calculate USD profitability metrics
    profits_df = calculate_wallet_profitability(profits_df)

    # Upload the df
    profits_df['wallet_address'] = wallet_address_mapping[profits_df['wallet_address']]
    upload_profits_data(profits_df)

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



def prepare_profits_data(transfers_df, prices_df):
    """
    Prepares a DataFrame (profits_df) by merging wallet transfer data with coin price data,
    ensuring valid pricing data is available for each transaction, and handling cases where
    wallets had balances prior to the first available pricing data.

    The function performs the following steps:
    1. Merges the `transfers_df` and `prices_df` on 'coin_id' and 'date'.
    2. Identifies wallets with transfer records before the first available price for each coin.
    3. Creates new records for these wallets, treating the balance as a net transfer on the
       first price date.
    4. Removes original records with missing price data.
    5. Appends the newly created records and sorts the resulting DataFrame.

    Parameters:
    - transfers_df (pd.DataFrame):
        A DataFrame containing wallet transaction data with columns:
        - coin_id: The ID of the coin/token.
        - wallet_address: The unique identifier of the wallet.
        - date: The date of the transaction.
        - net_transfers: The net tokens transferred in or out of the wallet on that date.
        - balance: The token balance in the wallet at the end of the day.

    - prices_df (pd.DataFrame):
        A DataFrame containing price data with columns:
        - coin_id: The ID of the coin/token.
        - date: The date of the price record.
        - price: The price of the coin/token on that date.

    Returns:
    - pd.DataFrame:
        A merged DataFrame containing profitability data, with new records added for wallets
        that had balances prior to the first available price date for each coin.
    """
    start_time = time.time()
    logger.info('Imputing rows based on prices-transfers data availability...')


    # Raise an error if either df is empty
    if transfers_df.empty or prices_df.empty:
        raise ValueError("Input DataFrames cannot be empty.")

    # 1. Format dataframes and merge on 'coin_id' and 'date'
    # ----------------------------------------------------------
    # set dates to datetime and coin_ids to categorical
    transfers_df = transfers_df.copy()
    prices_df = prices_df.copy()

    transfers_df['date'] = pd.to_datetime(transfers_df['date'])
    prices_df['date'] = pd.to_datetime(prices_df['date'])

    # Set 'coin_id' and 'date' as indices in both DataFrames before merging
    transfers_df = transfers_df.set_index(['coin_id', 'date'])
    prices_df = prices_df.set_index(['coin_id', 'date'])

    # Perform the merge operation using the indexes
    profits_df = transfers_df.merge(
        prices_df, left_index=True, right_index=True, how='left'
        ).reset_index()

    # Reset index on prices_df if it's needed elsewhere later with 'date' as a column
    prices_df = prices_df.reset_index()

    # Ensure 'coin_id' is still categorical after the merge
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    logger.info("<Step 1> merge transfers and prices: %.2f seconds",
                 time.time() - start_time)
    step_time = time.time()


    # 2. Attach data showing the first price record of all coins
    # ----------------------------------------------------------
    # Identify the earliest pricing data for each coin and merge to get the first price date
    first_prices_df = prices_df.groupby('coin_id',observed=True).agg({
        'date': 'min',
        'price': 'first'  # Assuming we want the first available price on the first_price_date
    }).reset_index()
    first_prices_df.columns = ['coin_id', 'first_price_date', 'first_price']
    first_prices_df['coin_id'] = first_prices_df['coin_id'].astype('category')

    # Merge the first price data into profits_df
    profits_df = profits_df.merge(first_prices_df, on='coin_id', how='inner')
    logger.info("<Step 2> identify first prices of coins: %.2f seconds",
                 time.time() - step_time)
    step_time = time.time()


    # 3. Create new records for the first_price_date for each wallet-coin pair
    # ------------------------------------------------------------------------
    # Identify wallets with transfer data before first_price_date
    pre_price_transfers = profits_df[profits_df['date'] < profits_df['first_price_date']]

    # Group by coin_id and wallet_address to ensure only one record per pair
    pre_price_transfer_groups = (
        pre_price_transfers.groupby(['coin_id', 'wallet_address'],observed=True)
        .agg({
            'first_price_date': 'first',
            'balance': 'last',  # Get the balance as of the latest transfer before first_price_date
            'first_price': 'first'  # Retrieve the first price for this coin
        }).reset_index()
    )
    pre_price_transfer_groups['coin_id'] = pre_price_transfer_groups['coin_id'].astype('category')

    # Create the new records to reflect transfer in of balance as of first_price_date
    new_records = pre_price_transfer_groups.copy()
    new_records['date'] = new_records['first_price_date']
    new_records['net_transfers'] = new_records['balance']  # Treat the balance as a net transfer
    new_records['price'] = new_records['first_price']  # Use the first price on the first_price_date

    # # Select necessary columns for new records
    new_records = new_records[['coin_id', 'wallet_address', 'date', 'net_transfers',
                            'balance', 'price', 'first_price_date', 'first_price']]
    logger.info("<Step 3> created new records as of the first_price_date: %.2f seconds"
                 , time.time() - step_time)
    step_time = time.time()


    # 4. Set a transfer in of the balance as of the first price date as the first record
    # ----------------------------------------------------------------------------------
    # Remove original records with no price data (NaN in 'price' column)
    profits_df = profits_df[profits_df['price'].notna()]

    # Remove the records in profits_df that will become the initial transfer in
    if not new_records.empty:
        # Merge profits_df with new_records, using an indicator
        merged = pd.merge(profits_df, new_records[['coin_id', 'wallet_address', 'date']],
                        on=['coin_id', 'wallet_address', 'date'],
                        how='left',
                        indicator=True)

        # Keep only the rows that are in profits_df but not in new_records
        profits_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        # Reset the index if needed
        profits_df = profits_df.reset_index(drop=True)

    # Append the initial transfer in records to the original dataframe
    if not new_records.empty:
        profits_df = pd.concat([profits_df, new_records], ignore_index=True)
        profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # Sort by coin_id, wallet_address, and date to maintain order
    profits_df = (profits_df.sort_values(by=['coin_id', 'wallet_address', 'date'])
                  .reset_index(drop=True))
    logger.info("<Step 4> merge new records into profits_df: %.2f seconds",
                 time.time() - step_time)
    step_time = time.time()


    # 5. Remove all records before a coin-wallet pair has any priced tokens
    # ---------------------------------------------------------------------
    # these are artifacts resulting from activity prior to price data availability. if wallets
    # purchased and sold all coins in these pre-data eras, their first record will be of a
    # zero-balance state.

    # calculate cumulative token inflows
    profits_df['token_inflows'] = profits_df['net_transfers'].clip(lower=0)
    profits_df['token_inflows_cumulative'] = (
        profits_df.groupby(['coin_id', 'wallet_address'],observed=True)['token_inflows']
        .cumsum()
    )
    profits_df['coin_id'] = profits_df['coin_id'].astype('category')

    # remove records prior to positive token_inflows
    profits_df = profits_df[profits_df['token_inflows_cumulative']>0]

    logger.info("<Step 5> removed records before each wallet's first token inflows: %.2f seconds",
                 time.time() - step_time)
    step_time = time.time()


    # 6. Tidy up and return profits_df
    # ---------------------------------
    # remove helper columns
    profits_df.drop(columns=['first_price_date', 'first_price', 'token_inflows',
                             'token_inflows_cumulative'], inplace=True)

    # Reset the index
    profits_df = profits_df.reset_index(drop=True)

    return profits_df



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
