"""
adds new coins from core.coins to the etl_pipelines.coin_wallet_net_transfers table
"""
import datetime
import os
import logging
import json
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase


# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def get_transfers_for_new_coins(request):
    """

    """
    # identify records in core.coins that do not have any etl_pipelines.coin_wallet_net_transfers

    return f"coingecko json parsing complete. processed {len(coins_to_process)} coins."


def dune_get_token_transfers(
        chain_text_dune,
        contract_address,
        decimals
    ):
    '''
    retrieves the daily net transfers from dune for each wallet that has transferred the given 
    token, consolidated by day and wallet address to reduce ETL load and table size. 

    param: chain_text_dune <string> the dune text of the blockchain 
    param: contract_address <string> the contract address of the token
    param: decimals <int> the number of decimals of the token
    return: transfers_df <dataframe> a dataframe with all historical transfers by wallet address
    '''
    # determine which query based on blockchain
    if chain_text_dune == 'solana':
        query_id = 3658238  # url: https://dune.com/queries/{query_id}
    else:
        query_id = 3628115

    # define query params
    dune = DuneClient.from_env()
    transfers_query = QueryBase(
        query_id=query_id,
        params=[
            QueryParameter.text_type(name='blockchain_name', value=chain_text_dune),
            QueryParameter.text_type(name='contract_address', value=contract_address),
            QueryParameter.number_type(name='decimals', value=decimals),
        ]
    )
    # run dune query and load to a dataframe
    logger = logging.getLogger('dune_client')
    logger.setLevel(logging.ERROR)
    transfers_df = dune.run_query_dataframe(transfers_query, ping_frequency=5)

    return transfers_df
