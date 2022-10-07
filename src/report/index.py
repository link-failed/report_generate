

from typing import Dict
from attr import attr
from bokeh.models import ColumnDataSource, PreText, Select

from bokeh.models import Div, RangeSlider, Spinner
from bokeh.layouts import layout, column
from bokeh.plotting import curdoc, figure, show

from bokeh.models import Range1d, FactorRange
import numpy as np
# ticker1 = Select(value="AAPL", options=nix("GOOG", DEFAULT_TICKERS))
# def ticker1_change(attrname, old, new):
#     ticker2.options = nix(new, DEFAULT_TICKERS)
#     update()
# ticker1.on_change("value", ticker1_change)

def nix(val, lst):
    return [x for x in lst if x != val]

class Report:
    def __init__(self, periods: Dict, metadata: Dict, width = 1500, height = 900) -> None:
        self.width = width
        self.height = height
        self.metadata = metadata
        self.periods = periods
        assert len(periods) > 0

        periods_list = list(periods.keys())
        # print(periods_list)
        default_period = periods_list[0]
        default_running_id = periods.get(default_period)

        self.select = self._plot_select(default = default_period, periods= periods_list)

        self.metadata_source = ColumnDataSource(data= self.get_cdf_data(default_running_id))

        # print(self.metadata.get(default_running_id))
        # print()
        # print()
        # print()

        self.cdf = self._plot_cdf(running_id= default_running_id, source= self.metadata_source)

        # self.cdf = self._plot_cdf()
        # self.gantt = self._plot_gantt()
        
        div = Div(
            text="""
                <p>Select the history using this select :</p>
                """,
            width=200,
            height=30,
        )
        # l = layout([
        #     [div],
        #     [self.select],
        #     [self.cdf]
        # ])
        # curdoc().add_root(column(div, self.select, self.cdf))
        # ,
            # [self.cdf, self.gantt]
        # show(l)

    def get_column(self):
        pass
    
    def get_data(self, running_id):
        md = self.metadata.get(running_id)
        x = []
        y = []
        for k, v in md.items():
            x.append(k)
            y.append(v.get('duration') or 0)
        data = dict(query_name=x, duration = y)
        return data
        
    def selected_period_change(self, attrname, old, new):
        t1 = self.select.value
        

        # print(f'{attrname} --- {old} --- {new} -- {t1}')
        running_id = self.periods.get(new)

        self.metadata_source.data = self.get_cdf_data(running_id)

    def get_cdf_data(self, running_id):
        project_start_time = None
        md:Dict = self.metadata.get(running_id)
        for k, v in md.items():
            if v['query_index'] == '1':
                project_start_time = v['query_start_time'] or 0
            # if v['query_index'] == str(len(md)):
            #     project_end_time = v['query_end_time'] or 0
        
        x = []
        if project_start_time is not None:
            for k, v in md.items():
                query_start_time = v['query_start_time']
                duration = v['duration'] if 'duration' in v else 0
                makespan = (query_start_time - project_start_time).total_seconds()
                x.append(makespan + duration)
        N = len(x)
        x = np.sort(x)
        y = np.arange(N) / float(N)
        return dict(x = x, y = y)
        
        

    def _plot_select(self, default, periods):
        period_select = Select(title="""Select the history using this select :""", value=default, options=periods)
        # period_select.js_link('value')
        period_select.on_change('value', self.selected_period_change)
        return period_select

    def _plot_cdf(self, running_id: str, source: ColumnDataSource, width = 870, height = 350, **args):        
        cdf = figure(width=width, height=height, tools="pan,wheel_zoom,box_select,reset", active_drag="box_select", **args)     
        cdf.line('x', 'y', source = source)
        cdf.circle('x', 'y', source = source)
        return cdf
    
    def _plot_gantt(self, running_id: str, metadata: Dict, width = 370, height = 350, **args):
        gantt = figure(width=width, height=height, tools="pan,wheel_zoom,box_select,reset", active_drag="box_select", **args)

        return gantt
        pass