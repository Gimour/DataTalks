import io
import pandas as pd
import requests
import gzip
import urllib.request
from io import BytesIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    file_urls = [
        'green_tripdata_2020-10.csv.gz',
        'green_tripdata_2020-11.csv.gz',
        'green_tripdata_2020-12.csv.gz'
        # Add other file URLs as needed
    ]
    
    taxi_dtypes = {
        'VendorID' : pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'RatecodeID': pd.Int64Dtype(),
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'ehail_fee': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'congestion_surcharge': float
    }

    parse_dates = ['lpep_pickup_datetime','lpep_dropoff_datetime']

     # Initialize an empty list to store DataFrames
    dfs = []

    for file_url in file_urls:
        url = base_url + file_url
        urllib.request.urlretrieve(url, file_url)
        with gzip.open(file_url, 'rt') as gz_file:
            df = pd.read_csv(gz_file, sep=",", dtype=taxi_dtypes, parse_dates=parse_dates)
            dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    return combined_df

    # return pd.read_csv(url,sep=",", compression="gzip", dtype = taxi_dtypes, parse_dates = parse_dates)

# Call the function to load the data
df_combined = load_data_from_api()

# Display the first few rows of the combined DataFrame
print(df_combined.head())


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'