'''
cloud function that runs a query to refresh the data in bigquery table core.coin_market_data
'''
import time
import logging
import pandas as pd
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()



@functions_framework.http
def update_coin_market_data(request): # pylint: disable=unused-argument  # noqa: F841
    '''
    runs all functions in sequence to refresh core.coin_market_data
    '''
    # configure logger
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%d/%b/%Y %H:%M:%S'
        )
    logger = logging.getLogger(__name__)

    # retrieve initial record count
    initial_records = get_coin_market_data_record_count()
    logger.info('initial core.coin_market_data records: %s', initial_records)

    # Insert new coingecko market data records to core.coin_market_data
    retrieve_raw_market_data()

    # Calculate the number of records added
    updated_records = get_coin_market_data_record_count()
    new_records = updated_records - initial_records

    logger.info('updated core.coin_market_data records: %s', updated_records)
    logger.info('refreshed core.coin_market_data with %s new coingecko records.', new_records)

    return f'{{"status":"200", "new_records": "{new_records}"}}'


def get_coin_market_data_record_count():
    '''
    retrieves the count of total records in core.coin_market_data for logging purposes
    '''

    query_sql = '''
        select count(*) as records
        from core.coin_market_data
        '''

    df = dgc().run_sql(query_sql)
    records = df.iloc[0, 0]

    return records



def retrieve_raw_market_data():
    '''
    adds new records in etl_pipelines.coin_market_data_coingecko to core.coin_market_data after
    normalizing and filling relevant fields
    '''
    start_time = time.time()

    query_sql = '''
        with coingecko_market_data as (
            select md.date
            ,co.coin_id
            ,co.chain_id
            ,co.address
            ,md.price

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

            ,md.volume
            ,'coingecko' as data_source
            ,md.updated_at
            from core.coins co
            join etl_pipelines.coin_market_data_coingecko md on md.coingecko_id = co.coingecko_id
        ),

        geckoterminal_market_data as (
            select md.date
            ,co.coin_id
            ,co.chain_id
            ,co.address
            ,cast(md.close as bignumeric) as price

            -- geckoterminal doesn't provide market cap
            ,null as market_cap

            -- calculate fdv using total supply
            ,cast(md.close*co.total_supply as int64) as fdv

            -- geckoterminal doesn't provide market cap to calculate circulating supply with
            ,null as circulating_supply

            -- total supply retrieved from coingecko metadata tables
            ,cast(co.total_supply as int64) as total_supply

            ,cast(md.volume as int64)
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
        '''

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
