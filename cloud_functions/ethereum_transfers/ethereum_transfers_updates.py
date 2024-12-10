"""
Retrieves new Ethereum blockchain transfer data from the BigQuery public tables, loads
it to the ethereum_tranfers schema, and updates etl_pipelines.ethereum_net_transfers to
add the newest tables.
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
def update_ethereum_transfers(request): # pylint: disable=unused-argument  # noqa: F841
    """
    runs all functions in sequence to refresh core.coin_market_data
    """
