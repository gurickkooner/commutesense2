# from bokeh.io import curdoc
# from bokeh.layouts import column, row, gridplot
# from bokeh.models import ColumnDataSource, Select, Div
# import pandas as pd
# import sys
# from botocore.exceptions import NoCredentialsError, PartialCredentialsError
# import logging
# import os
# from bokeh.themes import Theme

# # Import modules
# from s3_config import s3, bucket_name, file_keys, load_data_from_s3,map_key,load_map_from_s3
# from data_processing import process_data, month_names,process_map_data
# from plot_creation import (
#     create_combined_plot,
#     create_trip_duration_plot,
#     create_scatter_plot,
#     create_tips_bar_chart,
#     create_payment_type_bar_chart,
#     create_congestion_surcharge__plot,
#     create_zones_map_pickup,
#     create_zones_map_drop,
#     create_airport_fee_chart,
#     create_total_amount_histogram
# )
# from callbacks import update_plots

# # Configure logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

# logging.info("Initializing S3 client and reading Parquet files.")
# sys.stdout.flush()

# data_dict = {}
# all_data_agg = pd.DataFrame()
# payment = {}
# pickup=[]
# dropoff=[]
# try:
#     for file_key in file_keys:
#         data = load_data_from_s3(file_key)
#         if data:
#             agg_df, year_month, payment,pickup,dropoff= process_data(data, file_key)
#             data_dict[year_month] = agg_df
#             all_data_agg = pd.concat([all_data_agg, agg_df]) if not all_data_agg.empty else agg_df

#     all_agg_df = all_data_agg.groupby('day').agg({
#         'trip_distance': 'mean',
#         'fare_amount': 'mean',
#         'passenger_count': 'mean',
#         'trip_duration': 'mean',
#         'tip_amount': 'mean',
#         'congestion_surcharge': 'mean',
#         'total_amount':'mean' ,
#     }).reset_index()
#     all_agg_df['year_month'] = 'all'
#     all_agg_df['passenger_count'] = all_agg_df['passenger_count'].round()
#     data_dict['all'] = all_agg_df

#     if not data_dict:
#         logging.error("No data available to plot.")
#         sys.exit(1)

#     initial_month = list(data_dict.keys())[1]
#     source = ColumnDataSource(data_dict[initial_month])
#     initial_scatter_source = ColumnDataSource(data_dict[initial_month])

#     # Load the zones data
#     logging.info("Loading zones data from S3.")
#     zones_data = load_map_from_s3(map_key)
#     if not zones_data:
#         logging.error("Zones data could not be loaded.")
#         sys.exit(1)

#     # Process and log zones data
#     logging.info("Processing zones data.")
#     zones_df = process_map_data(zones_data)
#     logging.info(f"Zones data processed: {zones_df.head()}")
    
#      # Create a simplified zones map for debugging
    
    

#     # Adjust plot sizes
#     plot_width = 750
#     plot_height = 400

#     p = create_combined_plot(source)
#     p.width = plot_width
#     p.height = plot_height

#     p_duration = create_trip_duration_plot(source)
#     p_duration.width = plot_width
#     p_duration.height = plot_height

#     scatter_plot = create_scatter_plot(initial_scatter_source)
#     scatter_plot.width = plot_width
#     scatter_plot.height = plot_height

#     tips_bar_chart = create_tips_bar_chart(data_dict)
#     tips_bar_chart.width = plot_width
#     tips_bar_chart.height = plot_height

#     payment_type_bar_chart = create_payment_type_bar_chart(payment)
#     payment_type_bar_chart.width = plot_width
#     payment_type_bar_chart.height = plot_height

#     congestion_hist = create_congestion_surcharge__plot(source)
#     congestion_hist.width = plot_width
#     congestion_hist.height = plot_height

#     totalamount_hist = create_total_amount_histogram(source)
#     totalamount_hist.width = plot_width
#     totalamount_hist.height = plot_height

#     zones_map_plot_pick = create_zones_map_pickup(zones_df,pickup,"NYC Taxi Zones Map - Top Pickup Locations")
#     folium_map_pick = Div(text=zones_map_plot_pick,render_as_text=False ,css_classes=['map-container'])
#     zones_map_plot_drop = create_zones_map_drop(zones_df,dropoff,"NYC Taxi Zones Map - Top Dropoff Locations")
#     folium_map_drop = Div(text=zones_map_plot_drop,render_as_text=False)
#     #Create the maps with titles
#     #folium_map_pick = Div(text=create_zones_map_pickup(zones_df, pickup, "NYC Taxi Zones Map - Top Pickup Locations"), render_as_text=False, width=600, height=800, style={'border': '2px solid black'})
#     #folium_map_drop = Div(text=create_zones_map_pickup(zones_df, pickup, "NYC Taxi Zones Map - Top Dropoff Locations"), render_as_text=False, width=600, height=800, style={'border': '2px solid black'})
# # Create title Divs
#     title_pick = Div(text="<h2 style='text-align: center;'>NYC Taxi Zones Map - Top Pickup Locations</h2>", width=500, height=50)
#     title_drop = Div(text="<h2 style='text-align: center;'>NYC Taxi Zones Map - Top Dropoff Locations</h2>", width=500, height=50)
#     folium_map_pick.width = plot_width
#     folium_map_pick.height = plot_height
#     folium_map_drop.width = plot_width
#     folium_map_drop.height = plot_height

#     airport_fee_chart = create_airport_fee_chart(source)
#     airport_fee_chart.width = plot_width
#     airport_fee_chart.height = plot_height

#     options = [('all', 'All')] + [(key, month_names[key]) for key in data_dict.keys() if key != 'all']
#     select = Select(title="Month", value=options[0][1], options=[name for key, name in options])
#     select.on_change('value', lambda attr, old, new: update_plots(attr, old, new, source, initial_scatter_source, data_dict, options, select))

#     title = Div(text="<h1 style='text-align: center; color: black;'>NYC Taxi Data Dashboard</h1>")

# #     layout = column(
# #     title,
# #     row(select),
# #     gridplot([
# #         [p, scatter_plot,p_duration,], 
# #         [congestion_hist,tips_bar_chart, payment_type_bar_chart]
# #         [zones_map_plot]
# #     ], toolbar_location='right')
# # )

#     # layout = column(
#     #     title,
#     #     row(select),
#     #     gridplot([
#     #         [p, scatter_plot, p_duration], 
#     #         [congestion_hist, tips_bar_chart, payment_type_bar_chart],
#     #     ], toolbar_location='right'),
#     #    row(title_pick,title_drop ,None),  # Place titles and maps side by side 
#     #    row(folium_map_pick, folium_map_drop,None)  # Place titles and maps side by side 
#     # )
# # Adjust the main layout to include the titles above the maps
# # Define the layout
#     layout = column(
#     title,
#     row(select),
#     gridplot([
#         [p, scatter_plot], 
#         [payment_type_bar_chart, p_duration],
#         [congestion_hist, tips_bar_chart],
#         [airport_fee_chart,totalamount_hist],
#     ], toolbar_location='right'),
#     row(
#         column(title_drop, folium_map_drop),
#         column(title_pick, folium_map_pick) 
#          # Place titles and maps side by side
#     )
# )
#     curdoc().theme = 'light_minimal'
#     curdoc().add_root(layout)

    
   

# # Existing layout code
#     curdoc().title = "NYC Taxi Data Dashboard"

#     # Define the path to the template and the CSS file
#     current_dir = os.path.dirname(__file__)
#     template_path = os.path.join(current_dir, 'template.html')
#     css_path = os.path.join(current_dir, 'styles.css')

#     # Read the CSS content
#     with open(css_path) as f:
#         css_content = f.read()

#     # Apply the template and CSS content
#     with open(template_path) as f:
#         curdoc().template = f.read()
#     curdoc().template_variables["app_css"] = css_content

#     logging.info("Bokeh document created.")
#     sys.stdout.flush()

# except (NoCredentialsError, PartialCredentialsError) as e:
#     logging.error("AWS credentials not found or incomplete.")
#     sys.stdout.flush()
# except Exception as e:
#     logging.error(f"An error occurred: {e}")
#     sys.stdout.flush()


from bokeh.io import curdoc
from bokeh.layouts import column, row, gridplot
from bokeh.models import ColumnDataSource, Select, Div, Button, Paragraph
import pandas as pd
import sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging
import os
from bokeh.themes import Theme
import joblib

# Import modules
from s3_config import s3, bucket_name, file_keys, load_data_from_s3, map_key, load_map_from_s3
from data_processing import process_data, month_names, process_map_data
from plot_creation import (
    create_combined_plot,
    create_trip_duration_plot,
    create_scatter_plot,
    create_tips_bar_chart,
    create_payment_type_bar_chart,
    create_congestion_surcharge__plot,
    create_zones_map_pickup,
    create_zones_map_drop,
    create_airport_fee_chart,
    create_total_amount_histogram
)
from callbacks import update_plots

# Load the model and scaler
model_path = '/app/bokeh_app/linear_model.pkl'
scaler_path = '/app/bokeh_app/scaler.pkl'

model = joblib.load(model_path)
scaler = joblib.load(scaler_path)

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

logging.info("Initializing S3 client and reading Parquet files.")
sys.stdout.flush()

data_dict = {}
all_data_agg = pd.DataFrame()
payment = {}
pickup = []
dropoff = []
try:
    for file_key in file_keys:
        data = load_data_from_s3(file_key)
        if data:
            agg_df, year_month, payment, pickup, dropoff = process_data(data, file_key)
            data_dict[year_month] = agg_df
            all_data_agg = pd.concat([all_data_agg, agg_df]) if not all_data_agg.empty else agg_df

    all_agg_df = all_data_agg.groupby('day').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'passenger_count': 'mean',
        'trip_duration': 'mean',
        'tip_amount': 'mean',
        'congestion_surcharge': 'mean',
        'total_amount': 'mean',
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

    # Load the zones data
    logging.info("Loading zones data from S3.")
    zones_data = load_map_from_s3(map_key)
    if not zones_data:
        logging.error("Zones data could not be loaded.")
        sys.exit(1)

    # Process and log zones data
    logging.info("Processing zones data.")
    zones_df = process_map_data(zones_data)
    logging.info(f"Zones data processed: {zones_df.head()}")

    # Adjust plot sizes
    plot_width = 750
    plot_height = 400

    p = create_combined_plot(source)
    p.width = plot_width
    p.height = plot_height

    p_duration = create_trip_duration_plot(source)
    p_duration.width = plot_width
    p_duration.height = plot_height

    scatter_plot = create_scatter_plot(initial_scatter_source)
    scatter_plot.width = plot_width
    scatter_plot.height = plot_height

    tips_bar_chart = create_tips_bar_chart(data_dict)
    tips_bar_chart.width = plot_width
    tips_bar_chart.height = plot_height

    payment_type_bar_chart = create_payment_type_bar_chart(payment)
    payment_type_bar_chart.width = plot_width
    payment_type_bar_chart.height = plot_height

    congestion_hist = create_congestion_surcharge__plot(source)
    congestion_hist.width = plot_width
    congestion_hist.height = plot_height

    totalamount_hist = create_total_amount_histogram(source)
    totalamount_hist.width = plot_width
    totalamount_hist.height = plot_height

    zones_map_plot_pick = create_zones_map_pickup(zones_df, pickup, "NYC Taxi Zones Map - Top Pickup Locations")
    folium_map_pick = Div(text=zones_map_plot_pick, render_as_text=False, css_classes=['map-container'])
    zones_map_plot_drop = create_zones_map_drop(zones_df, dropoff, "NYC Taxi Zones Map - Top Dropoff Locations")
    folium_map_drop = Div(text=zones_map_plot_drop, render_as_text=False)

    title_pick = Div(text="<h2 style='text-align: center;'>NYC Taxi Zones Map - Top Pickup Locations</h2>", width=500, height=50)
    title_drop = Div(text="<h2 style='text-align: center;'>NYC Taxi Zones Map - Top Dropoff Locations</h2>", width=500, height=50)
    folium_map_pick.width = plot_width
    folium_map_pick.height = plot_height
    folium_map_drop.width = plot_width
    folium_map_drop.height = plot_height

    airport_fee_chart = create_airport_fee_chart(source)
    airport_fee_chart.width = plot_width
    airport_fee_chart.height = plot_height

    options = [('all', 'All')] + [(key, month_names[key]) for key in data_dict.keys() if key != 'all']
    select = Select(title="Month", value=options[0][1], options=[name for key, name in options])
    select.on_change('value', lambda attr, old, new: update_plots(attr, old, new, source, initial_scatter_source, data_dict, options, select))

    title = Div(text="<h1 style='text-align: center; color: black;'>NYC Taxi Data Dashboard</h1>")

    # Create dropdowns for source and destination
    source_select = Select(title="Source Zone", options=[str(zone) for zone in sorted(zones_df['LocationID'].unique())])
    destination_select = Select(title="Destination Zone", options=[str(zone) for zone in sorted(zones_df['LocationID'].unique())])

    # Create a paragraph to display the predicted fare
    fare_prediction = Paragraph(text="Predicted Fare: ")

    # Button to trigger fare prediction
    predict_button = Button(label="Predict Fare", button_type="success")
    
    # Callback function to predict fare
    def predict_fare():
        source_zone = int(source_select.value)
        destination_zone = int(destination_select.value)

        # Prepare the input data for prediction
        input_data = pd.DataFrame({
            'passenger_count': [1],
            'trip_distance': [3],  # Example distance
            'RatecodeID': [1],  # Default value
            'PULocationID': [source_zone],
            'DOLocationID': [destination_zone],
            'payment_type': [1],  # Default value
            'improvement_surcharge': [0.3],  # Default value
            'congestion_surcharge': [2.5],  # Example value
            'store_and_fwd_flag_Y': [0]  # Default value
        })

        input_data_scaled = scaler.transform(input_data)

        # Predict the fare
        predicted_fare = model.predict(input_data_scaled)[0]

        # Update the paragraph text
        fare_prediction.text = f"Predicted Fare: ${predicted_fare:.2f}"

    predict_button.on_click(predict_fare)

    # Define the layout
    layout = column(
        title,
        row(select),
        gridplot([
            [p, scatter_plot], 
            [payment_type_bar_chart, p_duration],
            [congestion_hist, tips_bar_chart],
            [airport_fee_chart, totalamount_hist],
        ], toolbar_location='right'),
        row(
            column(title_drop, folium_map_drop),
            column(title_pick, folium_map_pick)
        ),
        row(source_select, destination_select, predict_button),
        fare_prediction
    )

    curdoc().theme = 'light_minimal'
    curdoc().add_root(layout)

    curdoc().title = "NYC Taxi Data Dashboard"

    current_dir = os.path.dirname(__file__)
    template_path = os.path.join(current_dir, 'template.html')
    css_path = os.path.join(current_dir, 'styles.css')

    with open(css_path) as f:
        css_content = f.read()

    with open(template_path) as f:
        curdoc().template = f.read()
    curdoc().template_variables["app_css"] = css_content

    logging.info("Bokeh document created.")
    sys.stdout.flush()

except (NoCredentialsError, PartialCredentialsError) as e:
    logging.error("AWS credentials not found or incomplete.")
    sys.stdout.flush()
except Exception as e:
    logging.error(f"An error occurred: {e}")
    sys.stdout.flush()
