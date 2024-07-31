import pandas as pd
import s3fs
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

# S3 configuration
s3 = s3fs.S3FileSystem(anon=False)
bucket_name = 'taxi608v'

def load_parquet_file(file_key):
    with s3.open(f'{bucket_name}/{file_key}') as file:
        df = pd.read_parquet(file)
    logging.info(f"Loaded data from {file_key}")
    return df

def clean_data(df):
    # Convert pickup and dropoff datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')
    
    # Drop rows with any missing values
    df.dropna(inplace=True)

    # Ensure data types match the data dictionary
    df['passenger_count'] = df['passenger_count'].astype(int)
    df['trip_distance'] = df['trip_distance'].astype(float)
    df['fare_amount'] = df['fare_amount'].astype(float)
    df['tip_amount'] = df['tip_amount'].astype(float)
    df['total_amount'] = df['total_amount'].astype(float)

    logging.info("Data cleaning completed.")
    
    return df

def main():
    # List of all parquet file keys
    file_keys = [
        'yellow_tripdata_2023-01.parquet/part.0.parquet',
        'yellow_tripdata_2023-02.parquet',
        'yellow_tripdata_2023-03.parquet',
        'yellow_tripdata_2023-04.parquet',
        'yellow_tripdata_2023-05.parquet',
        'yellow_tripdata_2023-06.parquet',
        'yellow_tripdata_2023-07.parquet',
        'yellow_tripdata_2023-08.parquet',
        'yellow_tripdata_2023-09.parquet',
        'yellow_tripdata_2023-10.parquet',
        'yellow_tripdata_2023-11.parquet',
        'yellow_tripdata_2023-12.parquet',
        'yellow_tripdata_2024-01.parquet',
        'yellow_tripdata_2024-02.parquet',
        'yellow_tripdata_2024-03.parquet',
        'yellow_tripdata_2024-04.parquet'
    ]

    for file_key in file_keys:
        logging.info(f"Loading data from {file_key}...")
        df = load_parquet_file(file_key)
        
        logging.info(f"Cleaning data from {file_key}...")
        cleaned_df = clean_data(df)
        
        # Save the cleaned data back to S3 with a new name
        cleaned_file_key = f'cleaned_{file_key.replace("/", "_")}'
        with s3.open(f'{bucket_name}/{cleaned_file_key}', 'wb') as file:
            cleaned_df.to_parquet(file)

        logging.info(f"Cleaned data saved to s3://{bucket_name}/{cleaned_file_key}")

    logging.info("Data cleaning process completed.")

if __name__ == '__main__':
    main()
