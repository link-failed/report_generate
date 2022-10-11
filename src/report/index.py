

from datetime import datetime
from typing import Dict

from bokeh.models import ColumnDataSource, PreText, Select

from bokeh.models import Div, RangeSlider, Spinner
from bokeh.layouts import layout, column
from bokeh.plotting import curdoc, figure, show

from bokeh.models import DatetimeTickFormatter, NumeralTickFormatter
from bokeh.models import Range1d, FactorRange
from bokeh.models import HoverTool
import numpy as np
import pandas as pd
from src.base.dataframe import LogDataframe


class Report:
    def __init__(self, metadata: LogDataframe, **args) -> None:
        self.metadata = metadata

        unique_ids = list(self.metadata.histories.keys())
        self._plot_select(unique_ids[0], list(self.metadata.histories.values()))

        self.init_layout(width= 1500, height= 800, args= None)

        cdf_data = self._get_cdf_data(unique_ids[0])
        self.cdf_source = ColumnDataSource(data= cdf_data)
        self._plot_cdf(self.cdf_source)
        gantt_data = self._get_gantt_data(unique_ids[0])

        self.gantt_source = ColumnDataSource(data = gantt_data)
        self._plot_gantt(source= self.gantt_source)


    def init_layout(self, width, height, **args):
        def init_cdf_figure(width, height, tools ="pan,wheel_zoom,box_select,reset, hover", tooltips =  "@name: @duration"):
            cdf = figure(width=int(width), height=int(height), 
                title = 'cdf for each query',
                tools= tools, 
                tooltips = tooltips,
                active_drag="box_select")
            cdf.legend.location = 'top_left'
            cdf.legend.click_policy = 'hide'
            return cdf
        def init_gantt_figure(y_range, x_range, width, height, **args):
            gantt = figure(y_range=y_range, x_range=x_range, width= int(width), height=int(height),
                x_axis_type="datetime",
                sizing_mode="stretch_width",
                toolbar_location=None, title="Gantt Chart") 
            # gantt.legend.location = 'top_left'
            gantt.ygrid.grid_line_color = None
            gantt.xaxis.axis_label = "Time (seconds)"
            gantt.outline_line_color = None
            gantt.xaxis[0].formatter = DatetimeTickFormatter(hourmin = '%H:%M')
            
            return gantt

        self.cdf = init_cdf_figure(width= width/ 2, height= height /2)
        self.gantt = init_gantt_figure(y_range = (1,6), x_range= None, width= width / 2, height= height /2 )

    def layouts(self):
        col = column(self.history_select, self.cdf, self.gantt)
        return col
        

    def _get_cdf_data(self, running_id):
        md:pd.DataFrame = self.metadata.get_contents(running_id)
        df = md.sort_values(by= ['duration'])
        N = len(df)
        y = np.arange(N) / float(N)
        df['y'] = y
        return df

    def _get_gantt_data(self, running_id):
        # self.gantt.x_range = Range1d('03:00', '05:00')
        md: pd.DataFrame = self.metadata.get_contents(running_id= running_id).copy()
        md.thread_name = md.thread_name.astype(str)
        md.qindex = md.qindex.astype(int)
        
        md['end_time'] = md['end_time'].astype('datetime64[ns]')
        group = md.groupby('thread_name')
        print(md.dtypes)
        
        x0 = md[md['qindex'] == 1]['start_time'][0]

        # x0 = datetime.strptime(str(x0), "%Y-%m-%d %H:%M:%S")

        
        
        
        
        # print()
        # print('------' * 10)
        # print(x0.time())

        # print('======' * 10)
        y0 = md[md['qindex'] == md['qindex'].max()]['end_time']
        y0 = y0[y0.index[0]]
        
        # print(y0)
        # print(len(y0))
        # print()
        
        # y0 = datetime.strptime(str(y0[y0.index[0]]), "%Y-%m-%d %H:%M:%S")

        # print(y0.time())
        # print('------' * 10)
        print(x0)
        print(y0)
        print('---' * 10)
        self.gantt.x_range = Range1d(x0, y0)
       

        return md



    def selected_history_change(self, attrname, old, new):
        
        running_id = self.metadata.get_running_id(new)
        # print(running_id)
        data = self._get_cdf_data(running_id)
        # print(data.head(5))
        self.cdf_source.data = data

        d2 = self._get_gantt_data(running_id= running_id)
        self.gantt_source.data = d2
        

        # self.metadata_source.data = self.get_cdf_data(running_id)

    def _plot_select(self, default, periods):
        self.history_select = Select(title="""Select the history using this select :""", value=default, options=periods)
        self.history_select.on_change('value', self.selected_history_change)
        
    def _plot_cdf(self, source):
        self.cdf.line('duration', 'y', source = source)
        self.cdf.circle('duration', 'y', source = source, legend_label = "cdf")

    def _plot_gantt(self, source):

        print(self.gantt.x_range.start)
        print(self.gantt.x_range.end)
        print('--=-=-' * 10)
        self.gantt.hbar(y="thread_name", left='start_time', right='end_time', height=0.4, source=source)
        # self.gantt.line('dates', 'y', color="navy", line_width=1,  source=source)
        


# def nix(val, lst):
#     return [x for x in lst if x != val]

# class Report:
#     def __init__(self, periods: Dict, metadata: Dict, width = 1500, height = 900) -> None:
#         self.width = width
#         self.height = height
#         self.metadata = metadata
#         self.periods = periods
#         assert len(periods) > 0

 
#         periods_list = list(periods.keys())
#         # print(periods_list)
#         default_period = periods_list[0]
#         default_running_id = periods.get(default_period)

#         self.select = self._plot_select(default = default_period, periods= periods_list)

#         self.metadata_source = ColumnDataSource(data= self.get_cdf_data(default_running_id))
#         self.gantt_source = ColumnDataSource(data= self.get_gantt_data(default_running_id))

#         # print(self.metadata.get(default_running_id))
#         # print()
#         # print()
#         # print()


#         self.cdf = self._plot_cdf(running_id= default_running_id, source= self.metadata_source)

#         # self.cdf = self._plot_cdf()
#         # self.gantt = self._plot_gantt()
        
#         div = Div(
#             text="""
#                 <p>Select the history using this select :</p>
#                 """,
#             width=200,
#             height=30,
#         )
  

#     def get_column(self):
#         pass

#     def get_gantt_data(self, running_id):
#         md:Dict = self.metadata.get(running_id)

#         query_names = list(md.keys())
#         # df = pd.DataFrame.from_dict(md.values())
#         # print(df.head())
#         # print(md.values())

#         return {}
    
#     def get_data(self, running_id):
#         md = self.metadata.get(running_id)
#         x = []
#         y = []
#         for k, v in md.items():
#             x.append(k)
#             y.append(v.get('duration') or 0)
#         data = dict(query_name=x, duration = y)
#         return data
        
#     def selected_period_change(self, attrname, old, new):
#         t1 = self.select.value
        

#         # print(f'{attrname} --- {old} --- {new} -- {t1}')
#         running_id = self.periods.get(new)

#         self.metadata_source.data = self.get_cdf_data(running_id)

#     def get_cdf_data(self, running_id):
#         project_start_time = None
#         md:Dict = self.metadata.get(running_id)
#         for k, v in md.items():
#             if v['query_index'] == '1':
#                 project_start_time = v['query_start_time'] or 0
#             # if v['query_index'] == str(len(md)):
#             #     project_end_time = v['query_end_time'] or 0
        
#         x = []
#         if project_start_time is not None:
#             for k, v in md.items():
#                 query_start_time = v['query_start_time']
#                 duration = v['duration'] if 'duration' in v else 0
#                 makespan = (query_start_time - project_start_time).total_seconds()
#                 x.append(makespan + duration)
#         N = len(x)
#         x = np.sort(x)
#         y = np.arange(N) / float(N)
#         return dict(x = x, y = y)
        
        

#     def _plot_select(self, default, periods):
#         period_select = Select(title="""Select the history using this select :""", value=default, options=periods)
#         # period_select.js_link('value')
#         period_select.on_change('value', self.selected_period_change)
#         return period_select

#     def _plot_cdf(self, running_id: str, source: ColumnDataSource, width = 870, height = 350, **args):        
#         cdf = figure(width=width, height=height, tools="pan,wheel_zoom,box_select,reset", active_drag="box_select", **args)     
#         cdf.line('x', 'y', source = source)
#         cdf.circle('x', 'y', source = source)
#         return cdf
    
#     def _plot_gantt(self, running_id: str, metadata: Dict, width = 370, height = 350, **args):
#         gantt = figure(width=width, height=height, tools="pan,wheel_zoom,box_select,reset", active_drag="box_select", **args)

#         return gantt
#         pass