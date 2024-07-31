# bokeh_app/main.py

from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, Div
import pandas as pd
import sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging

# Import modules
from s3_config import s3, bucket_name, file_keys, load_data_from_s3
from data_processing import process_data, month_names
from plot_creation import (
    create_combined_plot,
    create_trip_duration_plot,
    create_scatter_plot,
    create_fare_vs_passenger_plot,
    create_tips_bar_chart,
    create_payment_type_bar_chart,
    create_plotly_map
)
from callbacks import update_plots

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

logging.info("Initializing S3 client and reading Parquet files.")
sys.stdout.flush()

data_dict = {}
all_data_agg = pd.DataFrame()

try:
    for file_key in file_keys:
        data = load_data_from_s3(file_key)
        if data:
            agg_df, year_month = process_data(data, file_key)
            data_dict[year_month] = agg_df
            all_data_agg = pd.concat([all_data_agg, agg_df]) if not all_data_agg.empty else agg_df

    all_agg_df = all_data_agg.groupby('day').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'passenger_count': 'mean',
        'trip_duration': 'mean',
        'tip_amount': 'mean'
    }).reset_index()
    all_agg_df['year_month'] = 'all'
    all_agg_df['passenger_count'] = all_agg_df['passenger_count'].round()
    data_dict['all'] = all_agg_df

    if not data_dict:
        logging.error("No data available to plot.")
        sys.exit(1)

    initial_month = list(data_dict.keys())[1]
    source = ColumnDataSource(data_dict[initial_month])
    initial_scatter_source = ColumnDataSource(data_dict[initial_month])

    p = create_combined_plot(source)
    p_duration = create_trip_duration_plot(source)
    scatter_plot = create_scatter_plot(initial_scatter_source)
    fare_vs_passenger_plot = create_fare_vs_passenger_plot(initial_scatter_source)
    tips_bar_chart = create_tips_bar_chart(data_dict)
    payment_type_bar_chart = create_payment_type_bar_chart(s3, bucket_name, file_keys)
    plotly_html = create_plotly_map(s3, bucket_name, 'taxi_zones.geojson')
    map_div = Div(text=plotly_html)

    options = [('all', 'All')] + [(key, month_names[key]) for key in data_dict.keys() if key != 'all']
    select = Select(title="Month", value=options[0][1], options=[name for key, name in options])
    select.on_change('value', lambda attr, old, new: update_plots(attr, old, new, source, initial_scatter_source, data_dict, fare_vs_passenger_plot, options, select))

    layout = column(
        row(select),
        row(p, p_duration),
        row(scatter_plot, fare_vs_passenger_plot),
        row(tips_bar_chart, payment_type_bar_chart),
        row(map_div)
    )

    curdoc().add_root(layout)
    logging.info("Bokeh document created.")
    sys.stdout.flush()

except (NoCredentialsError, PartialCredentialsError) as e:
    logging.error("AWS credentials not found or incomplete.")
    sys.stdout.flush()
except Exception as e:
    logging.error(f"An error occurred: {e}")
    sys.stdout.flush()