import logging
import boto3
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from dask_ml.linear_model import LinearRegression
from dask.array import from_array

def main():
    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

    # Initialize the Dask cluster and client
    cluster = LocalCluster()
    client = Client(cluster)
    logging.info('Dask cluster and client initialized.')

    # S3 configuration
    s3 = boto3.client('s3')
    bucket_name = 'taxi608v'
    file_keys = [
        'cleaned_yellow_tripdata_2024-01.parquet',
        'cleaned_yellow_tripdata_2024-02.parquet'
       
        # Add other months as needed
    ]

    # Generate S3 URLs for the parquet files
    s3_urls = [f's3://{bucket_name}/{file_key}' for file_key in file_keys]

    # Load the parquet files from S3 into a Dask DataFrame
    data = dd.read_parquet(s3_urls)
    logging.info('Parquet files loaded into Dask DataFrame.')

    # Select the features and target variable
    X = data[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 
              'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID']]
    y = data['fare_amount']

    # Compute the Dask DataFrame to a pandas DataFrame
    X_pd = X.compute()
    y_pd = y.compute()

    # Convert pandas DataFrames to numpy arrays
    X_array = from_array(X_pd.values, chunks='auto')
    y_array = from_array(y_pd.values, chunks='auto')

    # Initialize the linear regression model
    model = LinearRegression()

    # Fit the model
    model.fit(X_array, y_array)

    # Predict (example)
    predictions = model.predict(X_array)

    # Compute and gather results
    predictions_computed = predictions.compute()

    # Print results in a paragraph
    print(f"After processing the data from the S3 bucket '{bucket_name}', the cleaned taxi trip data was used to train a linear regression model. "
          f"The model was trained using features such as pickup and dropoff times, passenger count, trip distance, and various fare components. "
          f"Once trained, the model made predictions on the fare amount for the given dataset. "
          f"Here is a sample of the predicted fare amounts: {predictions_computed[:5]}.")

if __name__ == '__main__':
    main()
