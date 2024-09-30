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

    # fill empty records, leaving a 'days_imputed' column to make data lineage clear
    market_data_filled_df = fill_market_data_gaps(market_data_df)

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

            -- use fdv if market cap data isn't available
            ,case
                when md.market_cap > 0 then md.market_cap
                else cast(md.price*co.total_supply as int64)
                end as market_cap

            -- calculate fdv using total supply
            ,cast(md.price*co.total_supply as int64) as fdv

            -- calculate circulating supply using market cap
            ,case
                when md.market_cap > 0 then cast(md.market_cap/md.price as int64)
                else cast(co.total_supply as int64)
                end as circulating_supply

            -- total supply retrieved from coingecko metadata tables
            ,cast(co.total_supply as int64) as total_supply

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

            -- geckoterminal doesn't provide market cap so use fdv as as market cap
            ,cast(md.close*co.total_supply as int64) as market_cap

            -- calculate fdv using total supply
            ,cast(md.close*co.total_supply as int64) as fdv

            -- geckoterminal doesn't provide circulating supply so use total supply
            ,cast(co.total_supply as int64) as circulating_supply

            -- total supply retrieved from coingecko metadata tables
            ,cast(co.total_supply as int64) as total_supply

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

    # Convert coin_id column to categorical to reduce memory usage
    market_data_df['coin_id'] = market_data_df['coin_id'].astype('category')

    # Downcast numeric columns to reduce memory usage
    market_data_df['price'] = pd.to_numeric(market_data_df['price'], downcast='float')
    market_data_df['volume'] = pd.to_numeric(market_data_df['volume'], downcast='integer')
    market_data_df['market_cap'] = pd.to_numeric(market_data_df['market_cap'], downcast='integer')

    logger.info('Retrieved unadjusted market data after %s seconds.',
                round(time.time() - start_time))

    return market_data_df



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

    # Confirm there are no null values in the input df
    if market_data_df.isna().sum().sum() > 0:
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
        coin_df['fdv'] = coin_df['fdv'].ffill()
        coin_df['circulating_supply'] = coin_df['circulating_supply'].ffill()
        coin_df['total_supply'] = coin_df['total_supply'].ffill()
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


    return market_data_filled_df



def upload_market_data_filled(market_data_filled_df):
    """
    Uploads filled dataframe to the core.coin_market_data table

    Parameters:
        market_data_filled_df (DataFrame): The DataFrame containing the market data to upload.
    """
    destination_table = 'core.coin_market_data'
    project_id = 'western-verve-411004'
    pandas_gbq.to_gbq(
        market_data_filled_df,
        destination_table=destination_table,
        project_id=project_id,
        if_exists='replace'
    )
