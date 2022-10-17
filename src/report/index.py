

from datetime import datetime
from typing import Dict

from bokeh.models import ColumnDataSource, PreText, Select, RangeSlider

from bokeh.models import Div, RangeSlider, Spinner
from bokeh.layouts import layout, column
from bokeh.plotting import curdoc, figure, show

from bokeh.models import DatetimeTickFormatter, NumeralTickFormatter
from bokeh.models import Range1d, FactorRange
from bokeh.models import HoverTool
import numpy as np
import pandas as pd
from src.base.dataframe import LogDataframe
from .range_select import HistoryRunning
from . import BaseComponent
# from .cdf import CDFComponent
from .gantt import GanttComponent

class Report(BaseComponent):
    def __init__(self, metadata: LogDataframe, span: int = 2, **args) -> None:
        self.metadata = metadata

        self.span = span if len(self.metadata.histories) > span else len(self.metadata.histories)

        run_span = []
        for running_id, running_date in self.metadata.histories.items():
            run_span.append(datetime.strptime(running_date, "%Y-%m-%d %H:%M:%S"))
        run_span.sort()

        self.rng = Range1d(start = run_span[0 - self.span], end = run_span[-1])     

        self.range_slider = HistoryRunning(metadata= metadata, selected_range= self.rng).get_layouts()

        

        unique_id = self.metadata.get_running_id(run_span[-1])

        self._plot_select(unique_id, self.metadata.histories)

        # print(f'unique_id is {unique_id}')
        # print(f'{self.metadata.get_contents(unique_id).head()}')
        md = self.metadata.get_contents(unique_id)

        self.gantt = GanttComponent(unique_id, md)
        
        
    def get_layouts(self):
        return super().get_layouts()

    def layouts(self):
        try:
            col = column(self.range_slider,  self.history_select, self.gantt.get_layouts())
        except Exception as e:
            print(e)
            col = column([])
        return col
        

    def _get_gantt_data(self, running_id, md: pd.DataFrame):
        # md: pd.DataFrame = self.metadata.get_contents(running_id= running_id).copy()
        md.thread_name = md.thread_name.astype(str)
        md.qindex = md.qindex.astype(int)
        md['end_time'] = md['end_time'].astype('datetime64[ns]')
        group = md.groupby('thread_name')
        x0 = md[md['qindex'] == 1]['start_time'][0]
        y0 = md[md['qindex'] == md['qindex'].max()]['end_time']
        y0 = y0[y0.index[0]]
        # self.gantt.x_range = Range1d(x0, y0)
        return md



    def selected_history_change(self, attrname, old, new):
        running_id = new
        md: pd.DataFrame = self.metadata.get_contents(running_id= running_id).copy()
        # data = self._get_cdf_data(running_id, md)
        # self.cdf_source.data = data

        # d2 = self._get_gantt_data(running_id= running_id, md=md)
        # self.gantt_source.data = d2
        self.gantt.running_id = running_id
        self.gantt.metadata = md

        # self.metadata_source.data = self.get_cdf_data(running_id)

    def _plot_select(self, default, histories: Dict):
        select_list = [(k,v) for k,v in histories.items()]
        self.history_select = Select(title="""Select the history using this select :""", value=default, options=select_list)
        self.history_select.on_change('value', self.selected_history_change)
        