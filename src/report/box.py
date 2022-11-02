
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
from bokeh.models import ColumnDataSource, RangeTool, Range1d, CategoricalColorMapper
from bokeh.palettes import Spectral6

class BOXComponent(BaseComponent):
    def __init__(self, metadata: LogDataframe, selected_range: Range1d = None, height = 480, width = 600) -> None:
        super().__init__()
        self.metadata = metadata
        self.data_source = {}

        tools ="pan,wheel_zoom,box_select,reset, hover"
        tooltips = "@name: @duration"
        self.cdf_all_query = figure(
            width=int(width), height=int(height),
            title='cdf for all running histroies',
            tools=tools,
            tooltips="@name: @wait",
            active_drag="box_select",
            background_fill_color="#fafafa"
        )
        self.cdf_each_query = figure(
            width=int(width), height=int(height),
            title='cdf for each query',
            tools=tools,
            tooltips=tooltips,
            active_drag="box_select",
            background_fill_color="#fafafa"
        )

        self.select_range = selected_range

        self.color_map = factor_cmap('rid', palette=Spectral6, factors=list(self.metadata.histories.keys()))

        factors = list(self.metadata.histories.keys())
        ccm = CategoricalColorMapper(palette=Spectral6, factors=factors, start=0, end=None, nan_color="gray")

        self.select_range.on_change('start', self.update_figure)
        self.select_range.on_change('end', self.update_figure)

        rid_list = self._get_target_running_id(selected_range.start, selected_range.end)
        self._update_figure2(rid_list=rid_list)

    def _get_target_running_id(self, start, end):
        rid_list = []  ### lines should be plotted
        for running_id, running_date in self.metadata.histories.items():
            r_date = datetime.strptime(running_date, "%Y-%m-%d %H:%M:%S")
            if r_date >= start and r_date <= end:
                rid_list.append(running_id)
                if running_id not in self.data_source:
                    self.data_source[running_id] = self._get_cdf_data(running_id)
                    # print(f'data source is {self._get_cdf_data(running_id)}')
                elif running_id in self.data_source and len(
                        self.data_source[running_id].get('each').data.get('duration')) == 0:
                    self.data_source[running_id] = self._get_cdf_data(running_id)

        return rid_list

    def _update_figure2(self, rid_list: List):

        render_name_list = []
        for renders in self.cdf_each_query.renderers:
            if renders.name not in rid_list and renders.name in self.data_source:
                render_name_list.append(renders.name)
                self._get_cdf_data(renders.name, empty=True)

        color = Category20[20]
        idx = 0
        for rid in rid_list:
            if rid not in render_name_list:
                data = self.data_source[rid]
                cdf_each = data.get('each')
                label = self.metadata.get_running_time(rid)
                self._plot_line2(self.cdf_each_query, cdf_each, rid, label, color[idx])

                cdf_all = data.get('all')
                self._plot_line2(self.cdf_all_query, cdf_all, rid, label, color[idx], x='wait')

            idx += 1

    def update_figure(self, attrname, old, new):
        # print(new)
        # if attrname == 'start':
        #     start = datetime.utcfromtimestamp(new / 1000.0) if not isinstance(new, datetime) else new
        # if attrname == 'end':
        #     end = datetime.utcfromtimestamp(new / 1000.0) if not isinstance(new, datetime) else new

        start = datetime.utcfromtimestamp(self.select_range.start / 1000.0) if isinstance(self.select_range.start,
                                                                                          float) else self.select_range.start
        end = datetime.utcfromtimestamp(self.select_range.end / 1000.0) if isinstance(self.select_range.end,
                                                                                      float) else self.select_range.end
        rid_list = self._get_target_running_id(start, end)
        self._update_figure2(rid_list)

    def _get_cdf_data(self, rid, empty=False):
        md: pd.DataFrame = self.metadata.get_contents(rid)

        data = dict(
            each=ColumnDataSource(data=dict(duration=[], y=[])),
            all=ColumnDataSource(data=dict(y=[], wait=[]))
        )

        if empty:
            if rid in self.data_source:
                self.data_source[rid].get('each').update(data=dict(duration=[], y=[]))
                self.data_source[rid].get('all').update(data=dict(wait=[], y=[]))

        ### compute cdf for each query
        each = md.copy()
        each = each.sort_values(by=['duration'])
        N = len(each)
        y = np.arange(N) / float(N)
        each['y'] = y
        data['each'].data = each

        ### compute cdf for all queries
        all_df = md.copy()
        base_start_time = all_df[all_df['qindex'] == 1]['start_time']
        project_start_time = base_start_time[base_start_time.first_valid_index()]
        if len(base_start_time) > 0:
            all_df['start_time'] = pd.to_datetime(all_df['start_time'])
            sssl = [project_start_time for i in range(len(all_df))]
            base_series = pd.Series(sssl, copy=False)
            sstart2 = pd.to_datetime(base_series)
            all_df['wait'] = (all_df['start_time'] - sstart2).dt.seconds
            all_df['wait'] = all_df['wait'] + all_df['duration']
            all_df = all_df.sort_values(by=['wait'])
            N = len(all_df)
            y = np.arange(N) / float(N)
            all_df['y'] = y
            data['all'].data = all_df
        return data

    def _plot_line2(self, f: figure, source, rid, label, color, x='duration'):
        # label = self.metadata.get_running_time(rid)

        f.line(x, 'y', source=source, legend_label=label, line_width=2, line_color=color, name=rid)
        f.circle(x, 'y', source=source, fill_color=color, line_color=color, legend_label=label, name=rid)

    def get_layouts(self):
        return row(self.cdf_all_query, self.cdf_each_query)





