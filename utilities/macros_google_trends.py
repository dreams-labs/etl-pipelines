import pandas as pd
import glob

# Path to files
file_path = 'datasets/google_trends/exports/*.csv'  # Adjust the path to where the files are located

# Load all CSV files
files = glob.glob(file_path)

# Function to process individual CSV files
def process_file(file):
    df = pd.read_csv(file, skiprows=2)  # Skip first two rows
    df.columns = df.columns.str.replace(': \(United States\)', '_us', regex=True)  # Replace ": (United States)" with "_us"
    df.columns = df.columns.str.replace(': \(Worldwide\)', '_worldwide', regex=True)  # Replace ": (United States)" with "_us"
    df.columns = df.columns.str.lower()
    df.rename(columns={df.columns[0]: 'Date'}, inplace=True)  # Rename first column to 'Date'
    df['Date'] = pd.to_datetime(df['Date'])  # Ensure Date column is in datetime format
    return df

# Process all files and merge them on the 'Date' column
dfs = [process_file(f) for f in files]
merged_df = pd.concat(dfs, axis=1).loc[:, ~pd.concat(dfs, axis=1).columns.duplicated()]  # Avoid duplicate 'Date' columns

# Show the result
# print(merged_df.head())

# Optionally save to CSV
merged_df.to_csv('datasets/google_trends/google_trends.csv', index=False)
merged_df
