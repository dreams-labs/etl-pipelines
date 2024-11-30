"""
cloud function that runs a query to refresh the data in bigquery table core.coin_market_data
"""
import time
import pandas as pd
import numpy as np
import functions_framework
import pandas_gbq
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def update_coin_market_data(request): # pylint: disable=unused-argument  # noqa: F841
    """
    runs all functions in sequence to refresh core.coin_market_data
    """
    logger.info("Beginning refresh of core.coin_market_data...")

    # retrieve the unadjusted data from the coingecko and geckoterminal etl_pipelines tables
    market_data_df = retrieve_raw_market_data()

    # Remove single-day dips of 80% or more that fully recover the next day
    market_data_cleaned = remove_single_day_dips(market_data_df)

    # fill empty records, leaving a 'days_imputed' column to make data lineage clear
    market_data_filled_df = fill_market_data_gaps(market_data_cleaned)

    # upload the filled data to bigquery
    upload_market_data_filled(market_data_filled_df)

    logger.info("Uploaded %s rows to core.coin_market_data.",
                len(market_data_filled_df))

    return f'{{"status":"200", "new_records": "{len(market_data_filled_df)}"}}'


def retrieve_raw_market_data():
    """
    adds new records in etl_pipelines.coin_market_data_coingecko to core.coin_market_data after
    normalizing and filling relevant fields
    """
    start_time = time.time()

    query_sql = """
        with coingecko_market_data as (
            select md.date
            ,co.coin_id
            ,md.price
            ,md.volume

            -- imputed market cap data is unreliable so just trust coingecko to validate
            ,md.market_cap

            ,'coingecko' as data_source
            ,md.updated_at
            from core.coins co
            join etl_pipelines.coin_market_data_coingecko md on md.coingecko_id = co.coingecko_id
        ),

        geckoterminal_market_data as (
            select md.date
            ,co.coin_id
            ,cast(md.close as bignumeric) as price
            ,cast(md.volume as int64) as volume

            -- total supply retrieved from coingecko metadata tables
            ,co.total_supply as total_supply

            ,'geckoterminal' as data_source
            ,md.updated_at
            from core.coins co
            join etl_pipelines.coin_market_data_geckoterminal md on md.geckoterminal_id = co.geckoterminal_id

            -- filter out any coin_id-date pairs that already have records from coingecko
            left join coingecko_market_data on coingecko_market_data.coin_id = co.coin_id
                and coingecko_market_data.date = md.date
            where coingecko_market_data.coin_id is null
        )

        select * from coingecko_market_data
        union all
        select * from geckoterminal_market_data
        """

    market_data_df = dgc().run_sql(query_sql)

    # Dates as dates
    market_data_df['date'] = pd.to_datetime(market_data_df['date'])

    # Convert object columns to categorical to reduce memory usage
    market_data_df['coin_id'] = market_data_df['coin_id'].astype('category')
    market_data_df['data_source'] = market_data_df['data_source'].astype('category')

    # Downcast numeric columns to reduce memory usage
    market_data_df['price'] = pd.to_numeric(market_data_df['price'], downcast='float')
    market_data_df['volume'] = pd.to_numeric(market_data_df['volume'], downcast='integer')
    market_data_df['market_cap'] = pd.to_numeric(market_data_df['market_cap'], downcast='integer')

    logger.info('Retrieved unadjusted market data after %s seconds.',
                round(time.time() - start_time))

    return market_data_df



def remove_single_day_dips(df, price_col='price', dip_threshold=0.8, recovery_threshold=0.9):
    """
    Removes rows from the DataFrame that represent single-day dips in price data.

    Parameters:
    - df: DataFrame containing the price data
    - price_col: Name of the column containing price data (default: 'price')
    - dip_threshold: Threshold for identifying a significant dip (default: 0.8, i.e., 80% drop)
    - recovery_threshold: Threshold for identifying recovery (default: 0.9, i.e., 90% recovery)

    Returns:
    - DataFrame with single-day dip rows removed
    """
    # Sort the DataFrame by coin_id and date
    df = df.sort_values(['coin_id', 'date'])

    # Calculate previous and next day prices
    df['prev_price'] = df.groupby('coin_id', observed=True)[price_col].shift(1)
    df['next_price'] = df.groupby('coin_id', observed=True)[price_col].shift(-1)

    # Identify single-day dips
    dip_mask = (
        (df[price_col] / df['prev_price'] < dip_threshold) &
        (df['next_price'] / df['prev_price'] > recovery_threshold)
    )

    # Count the number of dips removed
    num_dips_removed = dip_mask.sum()

    # Remove the rows identified as single-day dips
    df_cleaned = df[~dip_mask].copy()

    # Log the number of dips removed
    logger.info("Removed %s single-day dips from the market data.", num_dips_removed)

    # Drop the temporary columns used for calculations
    df_cleaned = df_cleaned.drop(['prev_price', 'next_price'], axis=1)

    return df_cleaned



def fill_market_data_gaps(market_data_df):
    """
    Forward-fills small gaps in market data for each coin_id. The function first confirms that
    there are no null values in any metric columns of the df, then uses price to identify dates that
    need to be filled for all of the metric columns.

    Parameters:
    - market_data_df: DataFrame containing market data keyed on coin_id-date

    Returns:
    - market_data_filled_df: DataFrame with small gaps forward-filled, excluding coins with gaps
        too large to fill.
    """

    # Confirm there are no null values in the primary market data columns
    if market_data_df[['date','price','volume']].isna().sum().sum() > 0:
        raise ValueError("Market data df contains null values")

    # Define the max date that all coins will be filled through
    max_date = market_data_df['date'].max()
    logger.info("Filling market data records for all coins through %s...",
                max_date.strftime('%Y-%m-%d'))

    # Get unique coin_ids
    unique_coins = market_data_df['coin_id'].unique()

    # List to store results
    filled_results = []

    # Iterate over each coin_id
    for coin_id in unique_coins:

        # Step 1: Reindex to create rows for all missing dates
        # ----------------------------------------------------
        coin_df = (market_data_df[market_data_df['coin_id'] == coin_id]
                   .sort_values('date', ascending=True).copy())

        # Create the full date range
        full_date_range = pd.date_range(start=coin_df['date'].min(), end=max_date, freq='D')

        # Reindex to get all dates
        coin_df = (coin_df.set_index('date').reindex(full_date_range)
                   .rename_axis('date').reset_index())

        # Step 2: Calculate days_imputed
        # ----------------------------------------------------
        # Create imputation_groups, which are streaks of days that all have empty prices
        coin_df['imputation_group'] = (             # IF...
            coin_df['price'].notnull()              # the row has a price...
            | (                                     # OR is a price gap of 1 row, defined as
                coin_df['price'].isnull()               # ( the row doesn't have a price
                & coin_df['price'].shift().notnull()    # AND the previous date had a price )
            )
        ).cumsum()                                  # THEN increment to the next imputation_group

        # Calculate days_imputed by counting how many records in a row are in each imputation_group
        coin_df['days_imputed'] = coin_df.groupby('imputation_group').cumcount() + 1

        # Set records that have price data to have null days_imputed
        coin_df.loc[coin_df['price'].notnull(), 'days_imputed'] = np.nan


        # Step 3: Fill the imputed rows, assuming price stays the same and there is 0 volume
        # ----------------------------------------------------------------------------------
        coin_df['coin_id'] = coin_id
        coin_df['price'] = coin_df['price'].ffill()
        coin_df['volume'] = coin_df['volume'].fillna(0)
        coin_df['market_cap'] = coin_df['market_cap'].ffill()
        coin_df['data_source'] = coin_df['data_source'].ffill()
        # coin_df['updated_at'] is left with nulls for imputed records
        # coin_df['days_imputed'] is left as a data lineage tool

        # Drop the 'imputation_group' helper column
        coin_df = coin_df.drop(columns=['imputation_group'])

        # Append to the result list
        filled_results.append(coin_df)

    # Concatenate all results
    if filled_results:
        market_data_filled_df = pd.concat(filled_results).reset_index(drop=True)
    else:
        market_data_filled_df = pd.DataFrame()  # Handle case where no coins were filled

    # coin_id as categorical
    market_data_filled_df['coin_id'] = market_data_filled_df['coin_id'].astype('category')
    market_data_filled_df['data_source'] = market_data_filled_df['data_source'].astype('category')

    # remove timezone for consistent joins
    market_data_filled_df['date'] = market_data_filled_df['date'].dt.tz_localize(None)

    return market_data_filled_df



def upload_market_data_filled(market_data_filled_df):
    """
    Uploads filled dataframe to the core.coin_market_data table

    Parameters:
        market_data_filled_df (DataFrame): The DataFrame containing the market data to upload.
    """

    # address negative values in 2019 data for for coin_ids
        # 12c7e173-35f7-4636-a8bc-a9cd0b55d8e9
        # 347bed7d-5fbf-4f93-8f2b-427846ef2c36
    market_data_filled_df['volume'] = market_data_filled_df['volume'].abs()

    # Apply explicit typecasts
    # pylint: disable=C0301
    market_data_filled_df['date'] = pd.to_datetime(market_data_filled_df['date'])
    market_data_filled_df['coin_id'] = market_data_filled_df['coin_id'].astype(str)
    market_data_filled_df['price'] = market_data_filled_df['price'].astype(float)
    market_data_filled_df['volume'] = market_data_filled_df['volume'].astype('int64')
    market_data_filled_df['market_cap'] = market_data_filled_df['market_cap'].astype('int64')
    market_data_filled_df['data_source'] = market_data_filled_df['data_source'].astype(str)
    market_data_filled_df['updated_at'] = pd.to_datetime(market_data_filled_df['updated_at'])
    market_data_filled_df['days_imputed'] = market_data_filled_df['days_imputed'].astype(float)

    # Define the schema
    schema = [
        {'name': 'date', 'type': 'DATETIME'},
        {'name': 'coin_id', 'type': 'STRING'},
        {'name': 'price', 'type': 'FLOAT'},
        {'name': 'volume', 'type': 'INTEGER'},
        {'name': 'market_cap', 'type': 'INTEGER'},
        {'name': 'data_source', 'type': 'STRING'},
        {'name': 'updated_at', 'type': 'DATETIME'},
        {'name': 'days_imputed', 'type': 'FLOAT'}
    ]

    # upload df to bigquery
    project_id = 'western-verve-411004'
    destination_table = 'core.coin_market_data'
    pandas_gbq.to_gbq(
        market_data_filled_df,
        destination_table=destination_table,
        project_id=project_id,
        if_exists='replace',
        table_schema=schema
    )
