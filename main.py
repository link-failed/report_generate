import utils

# utils.display_html('hwllo world')
from src.adapter.parser import DbtLogAdapter
from src.report.index import Report

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

log_path = '/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic-dbt/logs/'
dla = DbtLogAdapter(log_path= log_path)
# print(dla.get_period())

report = Report(dla.get_df())

# report = Report(periods= dla.get_period(), metadata= dla.get_metadata())

# print(dla.get_df())

# dla.get_df
# myapp.py


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