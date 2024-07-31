from bokeh.models import ColumnDataSource, Range1d, LinearAxis
from bokeh.plotting import figure
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
from io import BytesIO
import pandas as pd
import logging

def create_combined_plot(source):
    p = figure(x_axis_type="linear", title="Average Trip Distance and Fare Amount per Day", height=350, width=800)
    p.line(x='day', y='trip_distance', source=source, legend_label="Average Trip Distance", line_width=2, color="blue")
    p.yaxis.axis_label = 'Average Trip Distance (miles)'

    p.extra_y_ranges = {"fare_amount": Range1d(start=0, end=source.data['fare_amount'].max() * 1.1)}
    p.add_layout(LinearAxis(y_range_name="fare_amount", axis_label='Average Fare Amount (USD)'), 'right')
    p.line(x='day', y='fare_amount', source=source, legend_label="Average Fare Amount", line_width=2, color="green", y_range_name="fare_amount")

    return p

def create_trip_duration_plot(source):
    p = figure(x_axis_type="linear", title="Average Trip Duration per Day", height=350, width=800)
    p.line(x='day', y='trip_duration', source=source, legend_label="Average Trip Duration", line_width=2, color="red")
    p.xaxis.axis_label = 'Day of the Month'
    p.yaxis.axis_label = 'Average Trip Duration (minutes)'

    return p

def create_scatter_plot(source):
    p = figure(x_axis_type="linear", title="Fare Amount vs. Trip Distance", height=350, width=800)
    p.scatter(x='trip_distance', y='fare_amount', source=source, legend_label="Fare vs. Distance", size=5, color="navy", alpha=0.5)
    p.xaxis.axis_label = 'Trip Distance (miles)'
    p.yaxis.axis_label = 'Fare Amount (USD)'
    return p

def create_fare_vs_passenger_plot(source):
    p = figure(x_axis_type="linear", title="Average Fare Amount vs. Passenger Count", height=350, width=800)
    p.scatter(x='passenger_count', y='fare_amount', source=source, legend_label="Fare vs. Passenger Count", size=5, color="purple", alpha=0.5)
    p.xaxis.axis_label = 'Passenger Count'
    p.yaxis.axis_label = 'Average Fare Amount (USD)'
    p.xaxis.ticker = list(range(int(source.data['passenger_count'].min()), int(source.data['passenger_count'].max()) + 1))
    return p

def create_tips_bar_chart(data_dict):
    tips_data = {key: df['tip_amount'].mean() for key, df in data_dict.items() if key != 'all'}
    tips_df = pd.DataFrame(list(tips_data.items()), columns=['month', 'average_tip'])
    source_tips = ColumnDataSource(tips_df)

    p = figure(x_range=tips_df['month'], title="Average Tips per Month", height=350, width=800)
    p.vbar(x='month', top='average_tip', width=0.9, source=source_tips, legend_label="Average Tip", color="orange")
    p.xaxis.axis_label = 'Month'
    p.yaxis.axis_label = 'Average Tip Amount (USD)'

    return p

def create_payment_type_bar_chart(s3, bucket_name, file_keys):
    payment_labels = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
    payment_data = {label: 0 for label in payment_labels.values()}
    for file_key in file_keys:
        logging.info(f"Loading data from S3 for {file_key} to get payment types...")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        payment_counts = df['payment_type'].value_counts()
        for payment_type, count in payment_counts.items():
            label = payment_labels.get(payment_type, 'Unknown')
            payment_data[label] += count

    payment_df = pd.DataFrame(list(payment_data.items()), columns=['payment_type', 'count'])
    source_payment = ColumnDataSource(payment_df)

    p = figure(x_range=payment_df['payment_type'], title="Most Common Payment Types", height=350, width=800)
    p.vbar(x='payment_type', top='count', width=0.9, source=source_payment, legend_label="Payment Type", color="teal")
    p.xaxis.axis_label = 'Payment Type'
    p.yaxis.axis_label = 'Count'

    return p

def create_plotly_map(s3, bucket_name, geojson_key):
    try:
        logging.info(f"Downloading {geojson_key} from S3 bucket {bucket_name}")
        geojson_obj = s3.get_object(Bucket=bucket_name, Key=geojson_key)
        geojson_content = geojson_obj['Body'].read()
        logging.info("GeoJSON file downloaded successfully")
    except Exception as e:
        logging.error(f"Error downloading GeoJSON file: {e}")
        raise

    try:
        geo_df = gpd.read_file(BytesIO(geojson_content))
        logging.info("GeoJSON file loaded into GeoDataFrame successfully")
    except Exception as e:
        logging.error(f"Error loading GeoJSON file into GeoDataFrame: {e}")
        raise

    fig = px.choropleth_mapbox(
        geo_df,
        geojson=geo_df.geometry,
        locations=geo_df.index,
        color='borough',  # Replace 'borough' with the column you want to use for coloring
        mapbox_style="carto-positron",
        zoom=10,
        center={"lat": 40.7128, "lon": -74.0060},
        opacity=0.5
    )
    return pio.to_html(fig, full_html=False)