from bokeh.models import ColumnDataSource, Range1d, LinearAxis, LinearColorMapper, ColorBar, BasicTicker,PrintfTickFormatter
from bokeh.plotting import figure
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis256,Inferno256
import dask.dataframe as dd
import numpy as np
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
from io import BytesIO
import pandas as pd
import logging
import folium
from bokeh.models import Div 

def create_combined_plot(source):
    p = figure(x_axis_type="linear", title="Average Trip Distance, Fare Amount per Day", height=350, width=800)
    p.line(x='day', y='trip_distance', source=source, legend_label="Average Trip Distance", line_width=2, color="blue")
    p.yaxis.axis_label = 'Average Trip Distance (miles)'

    p.extra_y_ranges = {"fare_amount": Range1d(start=0, end=source.data['fare_amount'].max() * 1.1)}
    p.add_layout(LinearAxis(y_range_name="fare_amount", axis_label='Average Fare Amount (USD)'), 'right')
    p.line(x='day', y='fare_amount', source=source, legend_label="Average Fare Amount", line_width=2, color="green", y_range_name="fare_amount")

    return p


def create_congestion_surcharge__plot(source):
    p = figure(x_axis_type="linear", title="Average congestion surcharge per Day", height=350, width=800)
    p.line(x='day', y='congestion_surcharge', source=source, legend_label="Average congestion surcharge", line_width=2, color="green")
    p.xaxis.axis_label = 'Day of the Month'
    p.yaxis.axis_label = 'Average congestion surcharge'

    return p

def create_trip_duration_plot(source):
    p = figure(x_axis_type="linear", title="Average Trip Duration per Day", height=350, width=800)
    p.line(x='day', y='trip_duration', source=source, legend_label="Average Trip Duration", line_width=2, color="red")
    p.xaxis.axis_label = 'Day of the Month'
    p.yaxis.axis_label = 'Average Trip Duration (minutes)'

    return p

def create_scatter_plot(source):
    # Create a color mapper for the passenger count
    color_mapper = LinearColorMapper(palette=Viridis256, low=source.data['passenger_count'].min(), high=source.data['passenger_count'].max())
    
    # Create the scatter plot
    p = figure(x_axis_type="linear", title="Fare Amount vs. Trip Distance vs.passenger count", height=350, width=800)
    p.scatter(x='trip_distance', y='fare_amount', size=10, source=source, legend_label="Fare vs. Distance",
              color={'field': 'passenger_count', 'transform': color_mapper}, alpha=0.6)
    p.xaxis.axis_label = 'Trip Distance (miles)'
    p.yaxis.axis_label = 'Fare Amount (USD)'

    # Add the color bar to the plot
    color_bar = ColorBar(color_mapper=color_mapper, location=(0, 0), ticker=BasicTicker(desired_num_ticks=10),
                         formatter=PrintfTickFormatter(format="%d"), title='Passenger Count')
    p.add_layout(color_bar, 'right')
    
    return p


def create_tips_bar_chart(data_dict):
    tips_data = {key: df['tip_amount'].mean() for key, df in data_dict.items() if key != 'all'}
    tips_df = pd.DataFrame(list(tips_data.items()), columns=['month', 'average_tip'])
    source_tips = ColumnDataSource(tips_df)

    p = figure(x_range=tips_df['month'], title="Average Tips per Month", height=350, width=800)
    p.vbar(x='month', top='average_tip', width=0.9, source=source_tips, legend_label="Average Tip", color="blue")
    p.xaxis.axis_label = 'Month'
    p.yaxis.axis_label = 'Average Tip Amount (USD)'

    return p

def create_payment_type_bar_chart(payment):
    payment_labels = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
    payment_data = {label: 0 for label in payment_labels.values()}
    for payment_type, count in payment.items():
            label = payment_labels.get(payment_type, 'Unknown')
            payment_data[label] += count

    payment_df = pd.DataFrame(list(payment_data.items()), columns=['payment_type', 'count'])
    source_payment = ColumnDataSource(payment_df)

    p = figure(x_range=payment_df['payment_type'], title="Most Common Payment Types", height=350, width=800)
    p.vbar(x='payment_type', top='count', width=0.9, source=source_payment, legend_label="Payment Type", color="teal")
    p.xaxis.axis_label = 'Payment Type'
    p.yaxis.axis_label = 'Count'

    return p


# New functions for histograms
def create_total_amount_histogram(source):
    color_mapper = LinearColorMapper(palette=Inferno256, low=source.data['passenger_count'].min(), high=source.data['passenger_count'].max())
   # Create the scatter plot
    p = figure(x_axis_type="linear", title="Total Amount vs. Trip Distance vs.passenger count", height=350, width=800)
    p.scatter(x='trip_distance', y='total_amount', size=10, source=source, legend_label="total vs. Distance",
              color={'field': 'passenger_count', 'transform': color_mapper}, alpha=0.6)
    p.xaxis.axis_label = 'Trip Distance (miles)'
    p.yaxis.axis_label = 'total Amount (USD)'

    # Add the color bar to the plot
    color_bar = ColorBar(color_mapper=color_mapper, location=(0, 0), ticker=BasicTicker(desired_num_ticks=10),
                         formatter=PrintfTickFormatter(format="%d"), title='Passenger Count')
    p.add_layout(color_bar, 'right')
    
    return p

def create_passenger_count_histogram(source):
    passenger_count_values = source['passenger_count'].to_numpy()
    passenger_hist, passenger_bins = np.histogram(passenger_count_values, bins=[1,2,3,4,5,6,7,8,9,10,1000])

    p = figure(height=600, width=600, title='Histogram of Passenger Count',
               x_axis_label='Bins', y_axis_label='Frequency', x_range=[0, 10])
    p.quad(bottom=0, top=passenger_hist, 
           left=passenger_bins[:-1], right=passenger_bins[1:], 
           fill_color='green', line_color='black')
    return p




def create_zones_map_pickup(zones_df, pickup, title):
    logging.info("Creating taxi zones map.")
    
    # Create the map centered around the average location
    map_center = [zones_df['latitude'].max(), zones_df['longitude'].max()]
    mymap = folium.Map(location=map_center, zoom_start=10)

    # Highlight only the top 10 PULocationID locations
    for location_id in pickup:
        filtered_location = zones_df[zones_df['LocationID'] == location_id]
        if not filtered_location.empty:
            top_location = filtered_location.iloc[0]
            folium.Marker(
                location=[top_location['latitude'], top_location['longitude']],
                popup=f"Top Zone: {top_location['zone']}<br>LocationID: {top_location['LocationID']}",
                icon=folium.Icon(color='red')
            ).add_to(mymap)
    
    
    return mymap._repr_html_()
  

def create_zones_map_drop(zones_df, dropoff, title):
    logging.info("Creating taxi zones map.")
    
    # Create the map centered around the average location with specified width and height
    map_center = [zones_df['latitude'].mean(), zones_df['longitude'].mean()]
    mymap = folium.Map(location=map_center, zoom_start=10)

    # Highlight only the top 10 PULocationID locations
    for location_id in dropoff:
        filtered_location = zones_df[zones_df['LocationID'] == location_id]
        if not filtered_location.empty:
            top_location = filtered_location.iloc[0]
            folium.Marker(
                location=[top_location['latitude'], top_location['longitude']],
                popup=f"Top Zone: {top_location['zone']}<br>LocationID: {top_location['LocationID']}",
                icon=folium.Icon(color='green')
            ).add_to(mymap)
    
    # Convert the map to HTML representation
    map_html = mymap._repr_html_()
   
    return map_html
  
def create_airport_fee_chart(source):
    p = figure(x_axis_type="linear", title="Airport Fee Distribution per Day", height=350, width=800)
    p.vbar(x='day', top='Airport_fee', source=source, legend_label="Airport Fee", width=0.9, color="purple")
    p.xaxis.axis_label = 'Day of the Month'
    p.yaxis.axis_label = 'Airport Fee (USD)'
    return p   
