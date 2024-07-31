import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import logging

# S3 configuration
s3 = boto3.client('s3')
bucket_name = 'taxi608v'
file_keys = [
    'cleaned_yellow_tripdata_2024-01.parquet',
    'cleaned_yellow_tripdata_2024-02.parquet',
    'cleaned_yellow_tripdata_2024-03.parquet',
    'cleaned_yellow_tripdata_2024-04.parquet',
    'cleaned_yellow_tripdata_2023-02.parquet',
    'cleaned_yellow_tripdata_2023-03.parquet',
    'cleaned_yellow_tripdata_2023-04.parquet',
    'cleaned_yellow_tripdata_2023-05.parquet',
    'cleaned_yellow_tripdata_2023-06.parquet',
    'cleaned_yellow_tripdata_2023-07.parquet',
    'cleaned_yellow_tripdata_2023-08.parquet',
    'cleaned_yellow_tripdata_2023-09.parquet',
    'cleaned_yellow_tripdata_2023-10.parquet',
    'cleaned_yellow_tripdata_2023-11.parquet',
    'cleaned_yellow_tripdata_2023-12.parquet',
    # Add other months as needed
]

def load_data_from_s3(file_key):
    try:
        logging.info(f"Loading data from S3 for {file_key}...")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        return obj['Body'].read()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error(f"The specified key does not exist: {file_key}")
            return None
        else:
            raise e