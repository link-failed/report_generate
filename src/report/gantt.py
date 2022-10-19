
from datetime import datetime
from typing import Dict, List
import pandas as pd
import numpy as np
from bokeh.layouts import column, row
from bokeh.palettes import Spectral5
from bokeh.transform import factor_cmap

from bokeh.palettes import Category20
from bokeh.plotting import figure

from . import BaseComponent
from src.base.dataframe import LogDataframe
from bokeh.models import ColumnDataSource, RangeTool, Range1d, CategoricalColorMapper, DatetimeTickFormatter
from bokeh.palettes import Turbo256

class GanttComponent(BaseComponent):
    def __init__(self, running_id: str, content: pd.DataFrame, height = 480, width = 800) -> None:
        super().__init__()
        self.running_id = running_id
        self.__metadata = content
        self.data_source = {}
        factors = content['thread_name'].unique()
        self.f = self.init(y_range = factors, x_range= None, width= width, height= height)
        color = factor_cmap('name', Turbo256, content['name'].unique())
        data = self._get_gantt_data(content)
        self.source= ColumnDataSource(data = data)
        self._plot_gantt(source= self.source, color = color)
       

    def _plot_gantt(self, source, color):
        self.f.hbar(y="thread_name", left='start_time', right='end_time', height=0.4, source=source, fill_color = color, line_color="white")

    @property
    def metadata(self):
        return self.__metadata
    @metadata.setter
    def metadata(self, content: pd.DataFrame):
        self.__metadata = content.copy()
        self.source.data = self._get_gantt_data(content.copy())
        # print('sssdasdsasdasdad' * 100)

    def _get_gantt_data(self, md: pd.DataFrame):
 
        md = md.copy()
        # assert md.empty is True
        md.thread_name = md.thread_name.astype(str)
        md.qindex = md.qindex.astype(int)       
        md['end_time'] = md['end_time'].astype('datetime64[ns]')
        # group = md.groupby('thread_name')
        start = md[md['qindex'] == md['qindex'].min()]['start_time']
        x0 = start[start.index[0]]#start[start.first_valid_index()]
        y0 = md[md['qindex'] == md['qindex'].max()]['end_time']
        y0 = y0[y0.index[0]]
        
        return md
        

    def init(self, y_range, x_range, width, height):
        gantt = figure(y_range=y_range, x_range=x_range, width= int(width), height=int(height),
                tools ="pan,wheel_zoom,box_select,reset, hover", tooltips =  "@name: @duration",
                x_axis_type="datetime",
                sizing_mode="stretch_width",
                toolbar_location=None, title="Gantt Chart") 
            # gantt.legend.location = 'top_left'
        gantt.ygrid.grid_line_color = None
        gantt.xaxis.axis_label = "Time (seconds)"
        gantt.outline_line_color = None
        gantt.xaxis.formatter=DatetimeTickFormatter(days="%m/%d %H:%M",
            months="%m/%d %H:%M",
            hours="%m/%d %H:%M",
            minutes="%m/%d %H:%M")
        return gantt
    
    def get_layouts(self):
        return column(self.f)
