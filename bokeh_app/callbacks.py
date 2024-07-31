import logging
from bokeh.models import ColumnDataSource

def update_plots(attr, old, new, source, initial_scatter_source, data_dict, fare_vs_passenger_plot, options, select):
    selected_month = next(key for key, name in options if name == select.value)
    logging.debug(f"Selected month for update: {selected_month}")
    source.data = dict(ColumnDataSource(data_dict[selected_month]).data)
    initial_scatter_source.data = dict(ColumnDataSource(data_dict[selected_month]).data)
    fare_vs_passenger_plot.xaxis.ticker = list(range(int(source.data['passenger_count'].min()), int(source.data['passenger_count'].max()) + 1))