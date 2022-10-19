
from math import pi
from datetime import datetime
from typing import Dict, List
import pandas as pd
import numpy as np
from bokeh.layouts import column, row
from bokeh.palettes import Spectral6, Category10
from bokeh.transform import factor_cmap


from bokeh.plotting import figure

from . import BaseComponent
from src.base.dataframe import LogDataframe
from bokeh.models import ColumnDataSource, RangeTool, Range1d, CategoricalColorMapper, DatetimeTickFormatter, FactorRange, GlyphRenderer


class ScatterComponent(BaseComponent):
    def __init__(self, metadata: LogDataframe, selected_range: Range1d = None, height = 480, width = 800) -> None:
        super().__init__()
        self.metadata = metadata
        self.data_source = {}

        tools ="pan,wheel_zoom,box_select,reset, hover"
        tooltips = "@name: @duration"
        self.f = figure(
                x_range = [],
                width=int(width), height=int(height), 
                title = 'scatter for all running histroies',
                tools= tools, 
                tooltips = "@name: @duration",
                toolbar_location=None,
                background_fill_color="#fafafa"
            )       

        self.f.xaxis.major_label_orientation = pi/2

        self.color_mapper =  self._get_categorical_palette(list(metadata.histories.keys()) )
        
        # CategoricalColorMapper(palette = Turbo256, factors = list(metadata.histories.keys()) , nan_color = 'gray')

        self._start = datetime.utcfromtimestamp(selected_range.start / 1000.0) if isinstance(selected_range.start, float) else selected_range.start
        self._end = datetime.utcfromtimestamp(selected_range.end / 1000.0) if isinstance(selected_range.end, float) else selected_range.end 

        selected_range.on_change('start', self.update_figure)
        selected_range.on_change('end', self.update_figure)
        
        self._update_data_source()


    def _update_data_source(self):
        start = self._start
        end = self._end
        rid_list = []  ### lines should be plotted
        rm_rids= []
        for running_id, running_date in self.metadata.histories.items():
            r_date = datetime.strptime(running_date, "%Y-%m-%d %H:%M:%S")
            if r_date >= start and r_date <= end:
                if running_id not in self.data_source:                
                    self.data_source[running_id] = ColumnDataSource(data = self.metadata.get_contents(running_id = running_id))
                    rid_list.append(running_id)
            else:
                rm_rids.append(running_id)
     
        
        self.remove_glyphs(self.f, rm_rids)
        for rid in rm_rids:
            if rid in self.data_source:
                del self.data_source[rid]

        
        
        for rid in rid_list:
            color = self.color_mapper.palette[self.color_mapper.factors.index(rid)]
            
            self._plot_circle(rid, self.f, self.data_source[rid], label= self.metadata.get_running_time(rid),color= color)

    def remove_glyphs(self, figure, glyph_name_list):
        renderers = figure.select(dict(type=GlyphRenderer))
        for r in renderers:
            if r.name in glyph_name_list:
                col = r.glyph.y
                r.data_source.data[col] = [np.nan] * len(r.data_source.data[col])
    def restore_glyphs(self, figure, src_dict, glyph_name_list):
        renderers = figure.select(dict(type=GlyphRenderer))
        for r in renderers:
            if r.name in glyph_name_list:
                col = r.glyph.y
                r.data_source.data[col] = src_dict[col]

    def _plot_circle(self, rid, f: figure, source:ColumnDataSource, label, color, x = 'name'):
        factors = list(self.metadata.get_contents(rid)['name'])
        f.x_range = FactorRange(factors=factors)       
        f.circle(x, 'duration', source = source, fill_color= color, line_color = color, legend_label = label, name = rid)
        f.legend.location = 'top_right'
        # f.legend.orientation = "horizontal"
        f.legend.click_policy = 'hide'



    def update_figure(self, attrname, old, new):
        if attrname == 'start':
            self._start = datetime.utcfromtimestamp(new / 1e3)
        if attrname == 'end':
            self._end = datetime.utcfromtimestamp(new / 1e3)
        # print('xxx' * 10)
        self._update_data_source()    

    def get_layouts(self):
        return column(self.f)
