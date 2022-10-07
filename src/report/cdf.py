
from typing import Dict

from bokeh.layouts import column, row

from bokeh.plotting import curdoc, figure


def plot_cdf(runnint_id: str, metadata: Dict, width = 370, height = 350, **args):

    corr = figure(width=width, height=height, tools="pan,wheel_zoom,box_select,reset", active_drag="box_select")

    pass

# ['2022-09-22 20:57:24', '2022-09-24 02:57:26', '2022-09-24 02:58:22', '2022-09-27 21:07:34', '2022-09-27 21:08:03', '2022-10-05 19:24:00', '2022-10-05 19:24:42', '2022-10-05 19:25:00', '2022-10-05 19:25:34', '2022-10-05 20:02:01']
# {'agetbl': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 25), 'total_query': '15', 'query_index': '1', 'query_name': 'agetbl'}, 'tmp_labevents_part': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 25), 'total_query': '15', 'query_index': '2', 'query_name': 'tmp_labevents_part'}, 'gcs': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 26), 'total_query': '15', 'query_index': '3', 'query_name': 'gcs'}, 'heart_rate': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 26), 'total_query': '15', 'query_index': '4', 'query_name': 'heart_rate'}, 'min_surviving_bp': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 26), 'total_query': '15', 'query_index': '5', 'query_name': 'min_surviving_bp'}, 'rr': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 26), 'total_query': '15', 'query_index': '6', 'query_name': 'rr'}, 'sbp': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 30), 'total_query': '15', 'query_index': '7', 'query_name': 'sbp'}, 'temp': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 33), 'total_query': '15', 'query_index': '8', 'query_name': 'temp'}, 'bun': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 55), 'total_query': '15', 'query_index': '9', 'query_name': 'bun'}, 'glucose': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 55), 'total_query': '15', 'query_index': '10', 'query_name': 'glucose'}, 'hco': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 55), 'total_query': '15', 'query_index': '11', 'query_name': 'hco'}, 'pivoted_bg_2': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 55), 'total_query': '15', 'query_index': '12', 'query_name': 'pivoted_bg_2'}, 'potassium': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 55), 'total_query': '15', 'query_index': '13', 'query_name': 'potassium'}, 'sodium': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 59), 'total_query': '15', 'query_index': '14', 'query_name': 'sodium'}, 'wbc': {'query_start_time': datetime.datetime(1900, 1, 1, 20, 57, 59), 'total_query': '15', 'query_index': '15', 'query_name': 'wbc'}}
