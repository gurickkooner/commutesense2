import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Select
from bokeh.plotting import figure
import boto3
import logging
from io import BytesIO
import sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

# S3 configuration
s3 = boto3.client('s3')
bucket_name = 'taxi608v'
file_keys = [
    'cleaned_yellow_tripdata_2024-01.parquet',
    'cleaned_yellow_tripdata_2024-02.parquet',
    'cleaned_yellow_tripdata_2024-03.parquet',
    'cleaned_yellow_tripdata_2024-04.parquet',
    # Add other months as needed
]

logging.info("Initializing S3 client and reading Parquet files.")
sys.stdout.flush()

# Initialize data dictionary
data_dict = {}
month_names = {
    '2024-01': 'January 2024',
    '2024-02': 'February 2024',
    '2024-03': 'March 2024',
    '2024-04': 'April 2024',
    # Add other months as needed
}

try:
    # Load data from S3 and preprocess
    for file_key in file_keys:
        try:
            logging.info(f"Loading data from S3 for {file_key}...")
            print(f"Loading data from S3 for {file_key}...")
            sys.stdout.flush()

            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            data = obj['Body'].read()  # Read the data from S3
            df = pd.read_parquet(BytesIO(data))  # Load the data into a DataFrame

            logging.info(f"Data loaded for {file_key}")
            print(f"Data loaded for {file_key}")
            sys.stdout.flush()

            # Convert pickup datetime to pandas datetime type
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

            # Extract the year and month from the file key
            year_month = file_key.replace('cleaned_yellow_tripdata_', '').replace('.parquet', '')
            logging.debug(f"Year and month extracted: {year_month}")
            print(f"Year and month extracted: {year_month}")
            sys.stdout.flush()

            # Aggregate data to compute average trip distance per day in the month
            df['day'] = df['tpep_pickup_datetime'].dt.day
            agg_df = df.groupby('day').agg({'trip_distance': 'mean'}).reset_index()
            agg_df['year_month'] = year_month

            logging.info(f"Aggregated data for {year_month}: {agg_df.head()}")
            print(f"Aggregated data for {year_month}: {agg_df.head()}")
            sys.stdout.flush()

            # Store the aggregated data in the dictionary
            data_dict[year_month] = agg_df
            logging.debug(f"Aggregated data for {year_month} stored in data_dict")
            print(f"Aggregated data for {year_month} stored in data_dict")
            sys.stdout.flush()

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logging.error(f"The specified key does not exist: {file_key}")
                print(f"The specified key does not exist: {file_key}")
                sys.stdout.flush()
            else:
                raise e

    if not data_dict:
        logging.error("No data available to plot.")
        print("No data available to plot.")
        sys.stdout.flush()
        sys.exit(1)

    # Create initial ColumnDataSource
    initial_month = list(data_dict.keys())[0]
    source = ColumnDataSource(data_dict[initial_month])

    # Create the plot
    p = figure(x_axis_type="linear", title="Average Trip Distance per Day", height=350, width=800)
    p.line(x='day', y='trip_distance', source=source, legend_label="Average Trip Distance", line_width=2)

    # Set axis labels with units
    p.xaxis.axis_label = 'Day of the Month'
    p.yaxis.axis_label = 'Average Trip Distance (miles)'

    # Create a dropdown filter with proper month names
    options = [(key, month_names[key]) for key in data_dict.keys()]
    select = Select(title="Month", value=options[0][1], options=[name for key, name in options])

    def update_plot(attr, old, new):
        """
        Update the plot based on the selected month and year.
        """
        selected_month = next(key for key, name in options if name == select.value)
        logging.debug(f"Selected month for update: {selected_month}")
        print(f"Selected month for update: {selected_month}")
        sys.stdout.flush()
        source.data = dict(ColumnDataSource(data_dict[selected_month]).data)

    select.on_change('value', update_plot)

    # Create layout
    layout = column(select, p)

    # Add layout to document
    curdoc().add_root(layout)
    logging.info("Bokeh document created.")
    print("Bokeh document created.")
    sys.stdout.flush()

except (NoCredentialsError, PartialCredentialsError) as e:
    logging.error("AWS credentials not found or incomplete.")
    print("AWS credentials not found or incomplete.")
    sys.stdout.flush()
except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")
    sys.stdout.flush()
