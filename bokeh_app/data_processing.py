import pandas as pd
from io import BytesIO
import logging

month_names = {
    'all': 'All',
    '2024-01': 'January 2024',
    '2024-02': 'February 2024',
    '2024-03': 'March 2024',
    '2024-04': 'April 2024',
    '2023-01': 'January 2023',
    '2023-02': 'February 2023',
    '2023-03': 'March 2023',
    '2023-04': 'April 2023',
    '2023-05': 'May 2023',
    '2023-06': 'June 2023',
    '2023-07': 'July 2023',
    '2023-08': 'August 2023',
    '2023-09': 'September 2023',
    '2023-10': 'October 2023',
    '2023-11': 'November 2023',
    '2023-12': 'December 2023',
    # Add other months as needed
}

def process_data(data, file_key):
    df = pd.read_parquet(BytesIO(data))

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60.0
    df['day'] = df['tpep_pickup_datetime'].dt.day

    year_month = file_key.replace('cleaned_yellow_tripdata_', '').replace('.parquet', '')
    agg_df = df.groupby('day').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'passenger_count': 'mean',
        'trip_duration': 'mean',
        'tip_amount': 'mean'
    }).reset_index()
    agg_df['year_month'] = year_month
    agg_df['passenger_count'] = agg_df['passenger_count'].round()

    logging.info(f"Aggregated data for {year_month}: {agg_df.head()}")
    return agg_df, year_month
