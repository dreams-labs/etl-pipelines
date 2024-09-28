"""
Sequence to extract Ethereum transfers data from public BigQuery database, transform it to
the same format as coin_wallet_net_transfers, and prepare it for merging into core tables.
"""
from io import StringIO
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()



def combine_csv_files(bucket_name, prefix):
    """
    Merged all csvs in a cloud storage bucket using the columns from the
    ethereum transfers table.
    """
    # Initialize the GCS client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # List all blobs with the given prefix
    blobs = bucket.list_blobs(prefix=prefix)

    # Column names
    colnames = [
        "token_address",
        "from_address",
        "to_address",
        "value",
        "transaction_hash",
        "log_index",
        "block_timestamp",
        "block_number",
        "block_hash",
    ]

    # List to store all dataframes
    dfs = []

    # Iterate through all blobs
    for blob in blobs:
        if blob.name.endswith('.csv'):
            # Download the content of the blob
            content = blob.download_as_text()

            # Create a DataFrame from the content
            df = pd.read_csv(StringIO(content), names=colnames, header=None)

            # Only append if the DataFrame is not empty
            if not df.empty:
                dfs.append(df)

            print(f"Processed {blob.name}")

    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    return combined_df


def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Uploads the ethereum transfer formatted csv to BigQuery
    """
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the job config
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",  # This will overwrite the table if it exists
    )

    # Load the dataframe into BigQuery
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {project_id}:{dataset_id}.{table_id}")
