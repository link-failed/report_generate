import utils

# utils.display_html('hwllo world')
# from src.adapter.parser import DbtLogAdapter
from src.report.index import Report
from src.adapter.parser import DbtJsonLogAdapter
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
# log_path = '/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic-dbt/logs'
log_path = "/home/ceci/Desktop/report_generate/create_report/temp_files/json_logs/dir1"
dja = DbtJsonLogAdapter(log_path= log_path)
report = Report(dja.get_df())
print(Report(dja.get_metadata()))
curdoc().add_root(report.layouts())
