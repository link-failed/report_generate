

from datetime import datetime
from typing import Dict

from bokeh.models import ColumnDataSource, PreText, Select, RangeSlider

from bokeh.models import Div, RangeSlider, Spinner, Range1d
from bokeh.layouts import layout, column



import pandas as pd
from src.base.dataframe import LogDataframe
from .range_select import HistoryRunning
from . import BaseComponent
# from .cdf import CDFComponent
from .gantt import GanttComponent
from .scatter import ScatterComponent
from .cdf import CDFComponent

class Report(BaseComponent):
    def __init__(self, metadata: LogDataframe, span: int = 2, **args) -> None:
        self.metadata = metadata

        self.span = span if len(self.metadata.histories) > span else len(self.metadata.histories)

        run_span = []
        for running_id, running_date in self.metadata.histories.items():
            run_span.append(datetime.strptime(running_date, "%Y-%m-%d %H:%M:%S"))
        run_span.sort()
        unique_id = self.metadata.get_running_id(run_span[-1])

        self.rng = Range1d(start = run_span[0 - self.span], end = run_span[-1])     
        self.range_slider = HistoryRunning(metadata= metadata, selected_range= self.rng).get_layouts()       

        self.allcdfc = CDFComponent(metadata= metadata, selected_range= self.rng)

        self._plot_select(unique_id, self.metadata.histories)

        md = self.metadata.get_contents(unique_id)

        self.gantt = GanttComponent(unique_id, md)
        
        self.scatter = ScatterComponent(metadata= metadata, selected_range= self.rng)
        
    def get_layouts(self):
        return super().get_layouts()

    def layouts(self):
        try:
            header = column(self.range_slider)
            # body = column( self.allcdfc.get_layouts(), self.scatter.get_layouts())
            foot = column( self.history_select, self.gantt.get_layouts())
            col = layout(
                [
                    [header],
                    [ self.allcdfc.get_layouts(), self.scatter.get_layouts()],
                    [foot]
                ]
            )
            # col = column(self.range_slider, self.allcdfc.get_layouts(), self.history_select, self.gantt.get_layouts(), self.scatter.get_layouts())
        except Exception as e:
            print(e)
            col = layout(column([]))
        return col
        

    def selected_history_change(self, attrname, old, new):
        running_id = new
        md: pd.DataFrame = self.metadata.get_contents(running_id= running_id).copy()
        self.gantt.running_id = running_id
        self.gantt.metadata = md


    def _plot_select(self, default, histories: Dict):
        select_list = [(k,v) for k,v in histories.items()]
        self.history_select = Select(title="""Select the history using this select :""", value=default, options=select_list)
        self.history_select.on_change('value', self.selected_history_change)
        