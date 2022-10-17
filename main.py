import utils

# utils.display_html('hwllo world')
from src.adapter.parser import DbtLogAdapter
from src.report.index import Report
from src.adapter.dbt_log_parser import DbtJsonLogAdapter
# # if __name__ == '__main__':
# #     log_path = '/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic/logs'
# #     dla = DbtLogAdapter(log_path= log_path)
# #     print(dla.get_period())
# #     report = Report(periods= dla.get_period(), metadata= dla.get_metadata())

from random import random

from bokeh.layouts import column
from bokeh.models import Button
from bokeh.palettes import RdYlBu3
from bokeh.plotting import figure, curdoc

# log_path = '/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic/logs'
log_path = '/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic-dbt/logs'

dja = DbtJsonLogAdapter(log_path= log_path)



# dla = DbtLogAdapter(log_path=log_path)
# # print(dla.get_period())

report = Report(dja.get_df())

# # report = Report(periods= dla.get_period(), metadata= dla.get_metadata())

# # print(dla.get_df())

# # dla.get_df
# # myapp.py


curdoc().add_root(report.layouts())


# from bokeh.layouts import column
# from bokeh.models import ColumnDataSource, Slider
# from bokeh.plotting import figure
# from bokeh.sampledata.sea_surface_temperature import sea_surface_temperature
# from bokeh.server.server import Server
# from bokeh.themes import Theme


# def bkapp(doc):
#     df = sea_surface_temperature.copy()
#     source = ColumnDataSource(data=df)

#     plot = figure(x_axis_type='datetime', y_range=(0, 25), y_axis_label='Temperature (Celsius)',
#                   title="Sea Surface Temperature at 43.18, -70.43")
#     plot.line('time', 'temperature', source=source)

#     def callback(attr, old, new):
#         print('jjjjjjj')
#         if new == 0:
#             data = df
#         else:
#             data = df.rolling(f"{new}D").mean()
#         source.data = ColumnDataSource.from_df(data)

#     slider = Slider(start=0, end=30, value=0, step=1, title="Smoothing by N Days")
#     slider.on_change('value', callback)

#     doc.add_root(column(slider, plot))

#     doc.theme = Theme(filename="theme.yaml")

# # Setting num_procs here means we can't touch the IOLoop before now, we must
# # let Server handle that. If you need to explicitly handle IOLoops then you
# # will need to use the lower level BaseServer class.
# server = Server({'/': bkapp}, num_procs=4)
# server.start()


# if __name__ == '__main__':
#     print('Opening Bokeh application on http://localhost:5006/')

#     server.io_loop.add_callback(server.show, "/")
#     server.io_loop.start()


# import datetime

# import bokeh.plotting
# from bokeh.io import curdoc
# from bokeh.models import ColumnDataSource, Range1d, RangeTool
# from bokeh.models.tickers import FixedTicker, DatetimeTicker, DaysTicker
# from bokeh.models import DatetimeTickFormatter

# time_strs = ['2019-07-11 10:00:00', '2019-07-11 10:15:00', '2019-07-11 10:30:00', '2019-07-11 10:45:00']
# time_objs = [datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S') for time_str in time_strs]
# data = dict(x=time_objs, y=[5,4,6,5])

# timeline = bokeh.plotting.figure(x_axis_type='datetime')
# timeline.x(x='x', y='y', source=ColumnDataSource(data))

     
# timeline.xaxis.formatter=DatetimeTickFormatter(days="%m/%d %H:%M",
#             months="%m/%d %H:%M",
#             hours="%m/%d %H:%M",
#             minutes="%m/%d %H:%M")

# def print_values(attr, old, new):
#     print(attr)
#     print(old, type(old))
#     print(new, type(new))
#     print(datetime.datetime.fromtimestamp(new / 1e3))
#     print(datetime.datetime.utcfromtimestamp(new / 1e3))

# range = Range1d(start=time_objs[0], end=time_objs[-1])
# range.on_change('end', print_values)
# range_tool = RangeTool(x_range=range)
# timeline.add_tools(range_tool)



# curdoc().add_root(timeline)