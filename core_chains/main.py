'''
cloud function that runs a query to refresh core.chains and reference.chain_nicknames
'''
import datetime
import time
import logging
import os
from pytz import utc
import pandas as pd
import numpy as np
import functions_framework
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set up logger at the module level
logger = dc.setup_logger()



@functions_framework.http
def update_chains_tables(request):
    '''
    updates core.chains by ingesting the sheet containing chains, chain_ids, and chain aliases, 
        then rebuilding the core.chains and reference.chain_nicknames tables
    '''
    # load new chains aliases into bigquery
    ingest_chains_sheet()

    # refresh the core and reference tables
    refresh_chains_tables()

    logger.info(f"refreshed core.chains and reference.chain_nicknames.")

    return f'{{"finished refreshing chains tables."}}'



def ingest_chains_sheet():
    '''
    refreshes the etl_pipelines.chains_sheet bigquery table by ingesting the underlying 
        sheets data, formatting it, and uploading it to bigquery
    '''

    # Step 1: Read core.chains data out of the Dreams Data Schema workbook
    # -------------------------------------------------------------------- 
    # link: https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=388901135#gid=388901135
    df = dgc().read_google_sheet('11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs','core.chains!A:L')


    # Step 2: Normalize formatting of ingested data
    # ---------------------------------------------
    # Convert 'chain_id' to int64
    df['chain_id'] = df['chain_id'].astype('int64')

    # Convert 'is_case_sensitive' to boolean
    df['is_case_sensitive'].replace('FALSE',False).replace('TRUE',True).replace('',np.nan)

    # Replace empty strings and None strings with NaN (which represents NULL in Pandas)
    df = df.replace('', np.nan).replace('None', np.nan, regex=False)
    df = df.fillna(value=np.nan)


    # Step 3: Upload the normalized data as a bigquery table
    # ------------------------------------------------------
    dgc().upload_df_to_bigquery(
        df,
        'etl_pipelines',
        'chains_sheet',
        if_exists='replace'
    )



def refresh_chains_tables():
    '''
    1. updates core.chains by copying the etl_pipelines_chains_sheet data
    2. updates the lookup table reference.chain_nicknames by aggregating and deduping chain aliases
    '''

    query_sql = '''
        create or replace table core.chains as (
            select *
            from etl_pipelines.chains_sheet
            order by chain_id
        );

        create or replace table reference.chain_nicknames as (

            with all_aliases as (
                select chain_id, lower(chain) as chain_reference from core.chains
                union all 
                select chain_id, lower(nickname_1) from core.chains
                union all 
                select chain_id, lower(nickname_2) from core.chains
                union all 
                select chain_id, lower(chain_text_geckoterminal) from core.chains
                union all 
                select chain_id, lower(chain_text_coingecko) from core.chains
                union all 
                select chain_id, lower(chain_text_dune) from core.chains
                union all 
                select chain_id, lower(chain_text_defillama) from core.chains
                union all 
                select chain_id, lower(chain_text_dexscreener) from core.chains
                union all 
                select chain_id, lower(chain_text_dextools) from core.chains
            ),

            aliases_dedupe as (
                select chain_id
                ,chain_reference
                ,row_number() over (partition by chain_reference order by chain_id asc) as rn
                from all_aliases
                where chain_reference is not null
                group by 1,2
            )

            select chain_id
            ,chain_reference
            from aliases_dedupe
            where rn=1

        );

        select *
        from reference.chain_nicknames
        '''

    dgc().run_sql(query_sql)
