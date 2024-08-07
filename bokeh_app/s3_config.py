import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# S3 configuration
s3 = boto3.client('s3')
bucket_name = 'taxi608v'
file_keys = [
    'cleaned_yellow_tripdata_2024-01.parquet',
    'cleaned_yellow_tripdata_2024-02.parquet',
    'cleaned_yellow_tripdata_2024-03.parquet',
    'cleaned_yellow_tripdata_2024-04.parquet',
    # 'cleaned_yellow_tripdata_2023-02.parquet',
    # 'cleaned_yellow_tripdata_2023-03.parquet',
    # 'cleaned_yellow_tripdata_2023-04.parquet',
    # 'cleaned_yellow_tripdata_2023-05.parquet',
     'cleaned_yellow_tripdata_2023-06.parquet',
     'cleaned_yellow_tripdata_2023-07.parquet',
     'cleaned_yellow_tripdata_2023-08.parquet',
     'cleaned_yellow_tripdata_2023-09.parquet',
     'cleaned_yellow_tripdata_2023-10.parquet',
     'cleaned_yellow_tripdata_2023-11.parquet',
     'cleaned_yellow_tripdata_2023-12.parquet',
    # # Add other months as needed
]
map_key = 'taxi_zones_coordinates.csv'
def load_data_from_s3(file_key):
    try:
        logging.info(f"Attempting to load data from S3 for {file_key}...")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        logging.info(f"Successfully loaded data for {file_key}")
        return obj['Body'].read()
    except NoCredentialsError:
        logging.error("No credentials provided for accessing S3.")
    except PartialCredentialsError:
        logging.error("Incomplete credentials provided for accessing S3.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logging.error(f"The specified key does not exist: {file_key}")
        elif error_code == 'AccessDenied':
            logging.error(f"Access denied for key: {file_key}. Please check your permissions.")
        else:
            logging.error(f"ClientError: {e.response['Error']['Message']}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

for key in file_keys:
    data = load_data_from_s3(key)
    if data:
        logging.info(f"Data for {key} loaded successfully. Size: {len(data)} bytes")
    else:
        logging.info(f"Failed to load data for {key}")


def load_map_from_s3(map_key):
    try:
        logging.info(f"Loading map data from S3 for {map_key}...")
        obj = s3.get_object(Bucket=bucket_name, Key=map_key)
        logging.info(f"Successfully loaded map data from S3 for {map_key}.")
        return obj['Body'].read()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error(f"The specified key does not exist: {map_key}")
            return None
        else:
            logging.error(f"Failed to load map data from S3 for {map_key}: {e}")
            raise e