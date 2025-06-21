import os
import glob
import logging
import pandas as pd
import pandas_gbq

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base configuration
DATASETS_FOLDER = "/Users/jeremymeadow/DreamsData/Local/datasets/"
DATE_STR = '250617'
PROJECT_ID = 'western-verve-411004'


def process_crypto_global_market():
    """Process and upload crypto global market data."""
    folder = f"{DATASETS_FOLDER}macro_trends/crypto_global_market/{DATE_STR}"

    # Check folder contains exactly one file
    files = os.listdir(folder)
    if len(files) != 1:
        raise ValueError(f"Expected 1 file, found {len(files)}: {files}")

    # Load the file and validate required columns
    df = pd.read_csv(os.path.join(folder, files[0]))
    required_cols = ['market_cap', 'total_volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    logger.info(f"✓ Single file validated: {files[0]} with shape {df.shape}")

    # Convert timestamp and clean columns
    df['date'] = pd.to_datetime(df['snapped_at'], unit='ms')
    df = df.drop(columns='snapped_at')
    df = df[['date', 'market_cap', 'total_volume']]

    # Validate date range coverage
    min_date = df['date'].min()
    max_date = df['date'].max()
    if min_date >= pd.Timestamp('2014-01-01'):
        raise ValueError(f"Data doesn't include pre-2014 records. Earliest date: {min_date}")
    if max_date <= pd.Timestamp('2025-06-01'):
        raise ValueError(f"Data doesn't include recent records. Latest date: {max_date}")

    logger.info("Prepared and validated crypto_global_market df")

    # Upload data
    table_id = f"{PROJECT_ID}.macro_trends.crypto_global_market"
    schema = [
        {'name': 'date', 'type': 'datetime'},
        {'name': 'market_cap', 'type': 'float'},
        {'name': 'total_volume', 'type': 'float'}
    ]
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=PROJECT_ID,
        if_exists='replace',
        table_schema=schema,
        progress_bar=False
    )
    logger.info(f"✓ Uploaded {len(df)} records to crypto_global_market table")


def load_and_process_bitcoin_data(directory_path):
    """
    Load and process Bitcoin indicator data from CSV files.

    Params:
    - directory_path (str): Path to directory containing CSV files.

    Returns:
    - combined_df (DataFrame): Processed and combined Bitcoin data.
    """
    dfs = []

    # Iterate through CSV files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            df['DateTime'] = pd.to_datetime(df['DateTime'])
            df.set_index('DateTime', inplace=True)
            dfs.append(df)

    # Join all dataframes
    combined_df = pd.concat(dfs, axis=1, join='outer')

    # Remove duplicate columns
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

    # Define column renaming dictionary
    rename_dict = {
        'BTC price': 'btc_price',
        'CDD Terminal Ajusted 90dma': 'cdd_terminal_adjusted_90dma',
        'Fear and Greed': 'fear_and_greed',
        'MVRV Z-Score': 'mvrv_z_score',
        'VDD Multiple': 'vdd_multiple'
    }

    combined_df = combined_df.rename(columns=rename_dict)
    combined_df.index.name = 'date'

    return combined_df.reset_index()


def process_bitcoin_indicators():
    """Process and upload Bitcoin indicators data."""
    directory_path = f"{DATASETS_FOLDER}macro_trends/bitcoin_indicators/{DATE_STR}"
    df = load_and_process_bitcoin_data(directory_path)

    logger.info(f"Processed Bitcoin indicators data with shape {df.shape}")

    # Upload data
    table_id = f"{PROJECT_ID}.macro_trends.bitcoin_indicators"
    schema = [
        {'name': 'date', 'type': 'datetime'},
        {'name': 'btc_price', 'type': 'float'},
        {'name': 'vdd_multiple', 'type': 'float'},
        {'name': 'mvrv_z_score', 'type': 'float'}
    ]
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=PROJECT_ID,
        if_exists='replace',
        table_schema=schema,
        progress_bar=False
    )
    logger.info(f"✓ Uploaded {len(df)} records to bitcoin_indicators table")


def process_google_trends_file(file_path):
    """
    Process individual Google Trends CSV file.

    Params:
    - file_path (str): Path to CSV file.

    Returns:
    - df (DataFrame): Processed Google Trends data.
    """
    df = pd.read_csv(file_path, skiprows=2)
    df.columns = df.columns.str.replace(': \(United States\)', '_us', regex=True)
    df.columns = df.columns.str.replace(': \(Worldwide\)', '_worldwide', regex=True)
    df.columns = df.columns.str.lower()
    df.rename(columns={df.columns[0]: 'date'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])

    return df


def process_google_trends():
    """Process and upload Google Trends data with incremental updates."""
    file_path_pattern = f"{DATASETS_FOLDER}macro_trends/google_trends/{DATE_STR}/*.csv"
    files = glob.glob(file_path_pattern)

    if not files:
        logger.warning(f"No CSV files found at {file_path_pattern}")
        return

    # Process all files and merge
    dfs = [process_google_trends_file(f) for f in files]
    merged_df = pd.concat(dfs, axis=1)
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    logger.info(f"Processed {len(files)} Google Trends files with shape {merged_df.shape}")

    # Get current data from BigQuery
    query = "SELECT * FROM macro_trends.google_trends"
    try:
        current_df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)

        # Identify new records
        new_records = merged_df[~merged_df['date'].isin(current_df['date'])]

        if len(new_records) > 0:
            logger.info(f"Found {len(new_records)} new records to append")

            table_id = f"{PROJECT_ID}.macro_trends.google_trends"
            pandas_gbq.to_gbq(
                new_records,
                table_id,
                project_id=PROJECT_ID,
                if_exists='append',
                progress_bar=False
            )
            logger.info("✓ Google Trends table updated successfully")
        else:
            logger.info("No new records found - Google Trends table is current")

    except Exception as e:
        logger.warning(f"Could not query existing data, uploading all records: {e}")
        table_id = f"{PROJECT_ID}.macro_trends.google_trends"
        pandas_gbq.to_gbq(
            merged_df,
            table_id,
            project_id=PROJECT_ID,
            if_exists='replace',
            progress_bar=False
        )
        logger.info(f"✓ Uploaded {len(merged_df)} records to google_trends table")


# def main():
#     """Execute the complete macro trends data pipeline."""
#     logger.info("Starting macro trends data pipeline")

#     try:
#         process_crypto_global_market()
#         process_bitcoin_indicators()
#         process_google_trends()
#         logger.info("✓ Pipeline completed successfully")

#     except Exception as e:
#         logger.error(f"Pipeline failed: {e}")
#         raise


# if __name__ == "__main__":
#     main()