# """
# Sequence to extract Ethereum transfers data from public BigQuery database, transform it to
# the same format as coin_wallet_net_transfers, and prepare it for merging into core tables.
# """
# from io import StringIO
# import pandas as pd
# from google.cloud import bigquery
# from google.cloud import storage
# from dreams_core import core as dc

# # set up logger at the module level
# logger = dc.setup_logger()
