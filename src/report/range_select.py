
from math import pi
from copy import copy

from datetime import datetime, timedelta
from . import BaseComponent
import pandas as pd
import numpy as np
from bokeh.models import ColumnDataSource, RangeTool, Range1d, HoverTool, BoxSelectTool, CustomJS, LinearScale
from bokeh.plotting import figure
from src.base.dataframe import LogDataframe
from bokeh.layouts import column
from bokeh.models import DatetimeTickFormatter
from bokeh.models.tickers import FixedTicker, DatetimeTicker, DaysTicker
from typing import Any
from bokeh.events import MouseLeave, RangesUpdate, PressUp, Press, Pan

from .cdf import CDFComponent
from bokeh.models import BoxAnnotation



class HistoryRunning(BaseComponent):
    def __init__(self, metadata: LogDataframe, selected_range: Range1d = None, height = 230, width = 800) -> None:
        super().__init__()
        self.metadata = metadata
        df = self._parse_metadata(metadata= metadata)
        df['running_date'] = df['running_date'].astype('datetime64[ns]')
        source = ColumnDataSource(data=df)
        
        ht = HoverTool(
            tooltips=[
                ( 'rdate',   '@running_date{%m/%d %Y %H:%M:%S}'),
                ( 'duration',  '@total_duration{%0.2f}' ), # use @{ } for field names with spaces
            
                
            ],

            formatters={
                '@running_date'        : 'datetime', # use 'datetime' formatter for '@date' field
                '@{total_duration}'   : 'printf',   # use 'printf' formatter for '@{adj close}' field
                '$x': 'datetime'
                                            # use default 'numeral' formatter for other fields
            },
            mode='vline'
        )

        self.f = figure(title = "Drag the middle and edges of the selection box to change the range above",
                    height = height,
                    width = width,
                    x_axis_type="datetime",
                    sizing_mode="stretch_width",
                    max_width=1200,
                    tools ="wheel_zoom,box_select,reset, pan",
                    
                     toolbar_location=None, background_fill_color="#efefef"
                )

        cdfc = CDFComponent(metadata= metadata, selected_range= selected_range)
        self.allcdfc = cdfc.get_layouts()
        # self.allcdfc = figure()
 
        # source.
        self.f.circle('running_date', 'total_duration', source=source)
        self.f.line('running_date', 'total_duration', source=source)
        
        self.f.xaxis.formatter=DatetimeTickFormatter(days="%m/%d %H:%M",
            months="%m/%d %H:%M",
            hours="%m/%d %H:%M",
            minutes="%m/%d %H:%M")
        
        self.f.xaxis.major_label_orientation = pi/4
        self.f.ygrid.grid_line_color = None

        range_tool = RangeTool(x_range = selected_range)
        range_tool.overlay.fill_color = "navy"
        range_tool.overlay.fill_alpha = 0.2
        self.f.add_tools(range_tool)
        self.f.add_tools(ht)
        self.f.toolbar.active_multi = range_tool
    
    def _parse_metadata(self, metadata: LogDataframe) -> pd.DataFrame:
        source = pd.DataFrame({
                'running_id': pd.Series(dtype= 'str'),
                'running_date': pd.Series(dtype= 'datetime64[ns]'),
                'success': pd.Series(dtype= 'int'),
                'fail': pd.Series(dtype= 'int'),
                'skip': pd.Series(dtype= 'int'),
                'total_duration': pd.Series(dtype= 'float')                
            })
        for running_id, running_date in metadata.histories.items():
            df: pd.DataFrame = metadata.get_contents(running_id= running_id)
            total = df['duration'].sum()
            data = dict(
                running_id = running_id,
                running_date = running_date,
                total_duration = total
            )
            source = source.append(data, ignore_index = True)
        return source

    def get_layouts(self):
        return column(self.f, self.allcdfc)

