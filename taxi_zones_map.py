import logging
import boto3
import geopandas as gpd
from io import BytesIO
import plotly.express as px
from bokeh.io import show, output_file
from bokeh.layouts import column
from bokeh.models import Div
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

# S3 configuration
s3 = boto3.client('s3')
bucket_name = 'taxi608v'  # Replace with your bucket name
geojson_key = 'taxi_zones.geojson'  # Replace with your GeoJSON key

# Download GeoJSON file from S3
try:
    logging.info(f"Downloading {geojson_key} from S3 bucket {bucket_name}")
    geojson_obj = s3.get_object(Bucket=bucket_name, Key=geojson_key)
    geojson_content = geojson_obj['Body'].read()
    logging.info("GeoJSON file downloaded successfully")
except Exception as e:
    logging.error(f"Error downloading GeoJSON file: {e}")
    raise

# Load GeoJSON file into a GeoDataFrame
try:
    geo_df = gpd.read_file(BytesIO(geojson_content))
    logging.info("GeoJSON file loaded into GeoDataFrame successfully")
except Exception as e:
    logging.error(f"Error loading GeoJSON file into GeoDataFrame: {e}")
    raise

# Draw the map using Plotly
fig = px.choropleth_mapbox(
    geo_df,
    geojson=geo_df.geometry,
    locations=geo_df.index,
    color='borough',  # Replace 'borough' with the column you want to use for coloring
    mapbox_style="carto-positron",
    zoom=10,
    center={"lat": 40.7128, "lon": -74.0060},  # Adjust center coordinates as needed
    opacity=0.5
)

# Save the Plotly figure as an HTML div
plotly_html = file_html(fig, CDN, "Plotly Map")

# Display Plotly figure in Bokeh layout
div = Div(text=plotly_html, render_as_text=False)
layout = column(div)

from bokeh.io import curdoc
curdoc().add_root(layout)
