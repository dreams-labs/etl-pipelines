'''
provides updated whale chart data by following this sequence:
1. updates the dune table net_transfers_state with the current state of the bigquery table \
    etl_pipelines.coin_wallet_net_transfers

'''

import time
import datetime
import json
import logging
import os
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from PIL import Image, ImageOps
from google.cloud import bigquery
from google.cloud import storage
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import functions_framework
import pandas_gbq
from dreams_core import core as dc

