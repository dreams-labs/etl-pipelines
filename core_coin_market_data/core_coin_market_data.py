'''
cloud function that runs a query to refresh the data in bigquery table core.coin_market_data
'''
import datetime
import time
import logging
import os
from pytz import utc
import pandas as pd
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc



def insert_coingecko_market_data():
    '''
    adds new records in etl_pipelines.coin_market_data_coingecko to core.coin_market_data after 
    normalizing and filling relevant fields
    '''

    query_sql = '''
        insert into core.coin_market_data (

        select md.date
        ,co.coin_id
        ,co.chain_id
        ,co.address
        ,md.price

        -- use fdv if market cap data isn't available
        ,case 
            when md.market_cap > 0 then md.market_cap
            else cast(md.price*cgf.total_supply as int64)
            end as market_cap

        -- calculate fdv using total supply
        ,cast(md.price*cgf.total_supply as int64) as fdv

        -- calculate circulating supply using market cap
        ,case
            when md.market_cap > 0 then cast(md.market_cap/md.price as int64)
            else cast(cgf.total_supply as int64)
            end as circulating_supply

        -- total supply retrieved from coingecko metadata tables
        ,cast(cgf.total_supply as int64) as total_supply

        ,md.volume
        ,'coingecko' as data_source
        ,md.updated_at
        from core.coins co
        join core.coin_facts_coingecko cgf on cgf.coin_id = co.coin_id
        join etl_pipelines.coin_market_data_coingecko md on md.coin_id = co.coin_id

        -- don't insert rows that already have data
        left join core.coin_market_data cmd on cmd.coin_id = md.coin_id and cmd.date = md.date
        where cmd.date is null

        )
        '''

    dgc().run_sql(query_sql)


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


@functions_framework.http
def update_coin_market_data(request):
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

    # insert new coingecko market data records to core.coin_market_data
    insert_coingecko_market_data()

    # calculate the number of records added
    new_records = get_coin_market_data_record_count() - initial_records

    logger.info(f'refreshed core.coin_market_data with {new_records} new coingecko records.')

    return f'{{"status":"200", "new_records": "{new_records}"}}'
