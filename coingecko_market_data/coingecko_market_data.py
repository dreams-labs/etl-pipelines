import datetime
import logging
import pandas as pd
import json
import requests
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc

logging.basicConfig(
    level=logging.ERROR,
    format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S'
    )
logger = logging.getLogger(__name__)


# The miliseconds are removed
def strip_and_format_unixtime(unix_time):
    # Convert the number to a string
    number_str = str(unix_time)

    # Strip the last two digits by slicing the string
    stripped_number_str = number_str[:-3]

    # Convert the stripped string back to an integer
    stripped_number = int(stripped_number_str)

    # Convert Unix timestamp to datetime object
    datetime_obj = datetime.datetime.fromtimestamp(stripped_number)

    # Format the datetime object to a human-readable date and time string
    formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

    # Print the formatted date and time
    return formatted_datetime


# Function to divide the first item in the list by 2
def replace_unix_timestamp(lst):
    lst[0] = strip_and_format_unixtime(lst[0])
    return lst

def format_and_add_columns(df, id, contract):
    # Loop through each row in the DataFrame and apply the function
    for index, row in df.iterrows():
        # Formatting unix timestamp of prices column
        df.at[index, 'prices'] = replace_unix_timestamp(row['prices'])

        # Removing the unix timestamps from the columns
        df.at[index, 'market_caps'] = row['market_caps'][1]
        df.at[index, 'total_volumes'] = row['total_volumes'][1]

    df = df.assign(date=[i[0] for i in df["prices"]],
                   prices=[i[1] for i in df["prices"]],
                   chain=id,
                   address=contract)

    # Rearranging columns
    df = df[['chain', 'address', "date",'prices', "market_caps", "total_volumes"]]

    # Renaming columns
    df.columns = ['chain', 'address', "date", 'price', "market_cap", "volume"]

    return df


def retrieve_coin_data(id, contract):
    BASE_URL = "https://api.coingecko.com/api/v3/coins"

    r = requests.get(f"{BASE_URL}/{id}/contract/{contract}/market_chart/?vs_currency=usd&days=365")

    data = r.json()
    df = pd.DataFrame(data)
    df = format_and_add_columns(df, id, contract)

    return df