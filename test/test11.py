# """ Demonstration Bokeh app of how to register event callbacks in both
# Javascript and Python using an adaptation of the color_scatter example
# from the bokeh gallery. This example extends the js_events.py example
# with corresponding Python event callbacks.
# """

# import numpy as np

# from bokeh import events
# from bokeh.io import curdoc
# from bokeh.layouts import column, row
# from bokeh.models import Button, CustomJS, Div
# from bokeh.plotting import figure


# def display_event(div, attributes=[]):
#     """
#     Function to build a suitable CustomJS to display the current event
#     in the div model.
#     """
#     style = 'float: left; clear: left; font-size: 13px'
#     return CustomJS(args=dict(div=div), code="""
#         const {to_string} = Bokeh.require("core/util/pretty")
#         const attrs = %s;
#         const args = [];
#         for (let i = 0; i<attrs.length; i++ ) {
#             const val = to_string(cb_obj[attrs[i]], {precision: 2})
#             args.push(attrs[i] + '=' + val)
#         }
#         const line = "<span style=%r><b>" + cb_obj.event_name + "</b>(" + args.join(", ") + ")</span>\\n";
#         const text = div.text.concat(line);
#         const lines = text.split("\\n")
#         if (lines.length > 35)
#             lines.shift();
#         div.text = lines.join("\\n");
#     """ % (attributes, style))

# def print_event(attributes=[]):
#     """
#     Function that returns a Python callback to pretty print the events.
#     """
#     def python_callback(event):
#         cls_name = event.__class__.__name__
#         attrs = ', '.join(['{attr}={val}'.format(attr=attr, val=event.__dict__[attr])
#                        for attr in attributes])
#         print(f"{cls_name}({attrs})")

#     return python_callback

# # Follows the color_scatter gallery example

# N = 4000
# x = np.random.random(size=N) * 100
# y = np.random.random(size=N) * 100
# radii = np.random.random(size=N) * 1.5
# colors = [
#     "#%02x%02x%02x" % (int(r), int(g), 150) for r, g in zip(50+2*x, 30+2*y)
# ]

# p = figure(tools="pan,wheel_zoom,zoom_in,zoom_out,reset,tap,lasso_select,box_select,box_zoom,undo,redo")

# p.scatter(x, y, radius=radii,
#           fill_color=colors, fill_alpha=0.6,
#           line_color=None)

# # Add a div to display events and a button to trigger button click events

# div = Div(width=1000)
# button = Button(label="Button", button_type="success", width=300)
# layout = column(button, row(p, div))


# point_attributes = ['x','y','sx','sy']
# pan_attributes = point_attributes + ['delta_x', 'delta_y']
# pinch_attributes = point_attributes + ['scale']
# wheel_attributes = point_attributes+['delta']

# ## Register Javascript event callbacks

# # Button event
# button.js_on_event(events.ButtonClick, display_event(div))

# # LOD events
# p.js_on_event(events.LODStart, display_event(div))
# p.js_on_event(events.LODEnd,   display_event(div))

# # Point events

# p.js_on_event(events.Tap,       display_event(div, attributes=point_attributes))
# p.js_on_event(events.DoubleTap, display_event(div, attributes=point_attributes))
# p.js_on_event(events.Press,     display_event(div, attributes=point_attributes))

# # Mouse wheel event
# p.js_on_event(events.MouseWheel, display_event(div,attributes=wheel_attributes))

# # Mouse move, enter and leave
# # p.js_on_event(events.MouseMove,  display_event(div, attributes=point_attributes))
# p.js_on_event(events.MouseEnter, display_event(div, attributes=point_attributes))
# p.js_on_event(events.MouseLeave, display_event(div, attributes=point_attributes))

# # Pan events
# p.js_on_event(events.Pan,      display_event(div, attributes=pan_attributes))
# p.js_on_event(events.PanStart, display_event(div, attributes=point_attributes))
# p.js_on_event(events.PanEnd,   display_event(div, attributes=point_attributes))

# # Pinch events
# p.js_on_event(events.Pinch,      display_event(div, attributes=pinch_attributes))
# p.js_on_event(events.PinchStart, display_event(div, attributes=point_attributes))
# p.js_on_event(events.PinchEnd,   display_event(div, attributes=point_attributes))

# # Selection events
# p.js_on_event(events.SelectionGeometry, display_event(div, attributes=['geometry', 'final']))

# # Ranges Update events
# p.js_on_event(events.RangesUpdate, display_event(div, attributes=['x0','x1','y0','y1']))

# # Reset events
# p.js_on_event(events.Reset, display_event(div))

# ## Register Python event callbacks

# # Button event
# button.on_event(events.ButtonClick, print_event())

# # LOD events
# p.on_event(events.LODStart, print_event())
# p.on_event(events.LODEnd,   print_event())

# # Point events

# p.on_event(events.Tap,       print_event(attributes=point_attributes))
# p.on_event(events.DoubleTap, print_event(attributes=point_attributes))
# p.on_event(events.Press,     print_event(attributes=point_attributes))

# # Mouse wheel event
# p.on_event(events.MouseWheel, print_event(attributes=wheel_attributes))

# # Mouse move, enter and leave
# # p.on_event(events.MouseMove,  print_event(attributes=point_attributes))
# p.on_event(events.MouseEnter, print_event(attributes=point_attributes))
# p.on_event(events.MouseLeave, print_event(attributes=point_attributes))

# # Pan events
# p.on_event(events.Pan,      print_event(attributes=pan_attributes))
# p.on_event(events.PanStart, print_event(attributes=point_attributes))
# p.on_event(events.PanEnd,   print_event(attributes=point_attributes))

# # Pinch events
# p.on_event(events.Pinch,      print_event(attributes=pinch_attributes))
# p.on_event(events.PinchStart, print_event(attributes=point_attributes))
# p.on_event(events.PinchEnd,   print_event(attributes=point_attributes))

# # Ranges Update events
# p.on_event(events.RangesUpdate, print_event(attributes=['x0','x1','y0','y1']))

# # Selection events
# p.on_event(events.SelectionGeometry, print_event(attributes=['geometry', 'final']))

# # Reset events
# p.on_event(events.Reset, print_event())

# curdoc().add_root(layout)

from bokeh.io import show
from bokeh.models import ColumnDataSource, RangeTool,DataRange1d, Range1d, CustomJS
from bokeh.plotting import figure
from bokeh.layouts import gridplot

from datetime import datetime
from typing import Dict, List
import pandas as pd
import numpy as np
from bokeh.layouts import column, row
from bokeh.palettes import Spectral5
from bokeh.transform import factor_cmap

from bokeh.palettes import Category20
from bokeh.plotting import figure



from bokeh.models import ColumnDataSource, RangeTool, Range1d, CategoricalColorMapper, DatetimeTickFormatter
from bokeh.palettes import Turbo256

x =  [1,2,3,4,5,6,7,8,9,10]
y1 = [4,5,6,4,8,9,5,6,4,5]
y2 = [3,5,6,8,7,5,9,2,4,5]

source = ColumnDataSource(data=dict(x=x, y1=y1, y2=y2))

xdr = Range1d(start=x[2],end=x[5])

p1 = figure(plot_height=300, plot_width=800, tools="xpan", toolbar_location=None,
            x_axis_location="above", background_fill_color="#efefef", x_range=xdr)

p1.line('x', 'y1', source=source, legend_label='y1', color="#2874A6")

p2 = figure(plot_height=100, plot_width=800, tools="", toolbar_location=None,
               x_axis_location="above",background_fill_color="#efefef", x_range=xdr)

p2.line('x', 'y2', source=source, legend_label='y2', color="#2874A6")


select = figure(title="Drag the middle and edges of the selection box to change the range above",
                plot_height=130, plot_width=800, y_range=p1.y_range, y_axis_type=None,
                tools="", toolbar_location=None, background_fill_color="#efefef")

range_tool = RangeTool(x_range=xdr)

select.line('x', 'y1', source=source, color="#2874A6")
select.ygrid.grid_line_color = None
select.add_tools(range_tool)
select.toolbar.active_multi = range_tool

# print(select.x_range.start)

factors = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']

# index_cmap = factor_cmap('factors', palette=Spectral5, factors=factors, end=1)


cmap = CategoricalColorMapper(palette = Spectral5, factors = factors , nan_color = 'blue')

# cmap.palette('Apples')
print(cmap.factors)
print(cmap.palette)

print(type(cmap.factors))

print(cmap.palette[cmap.factors.index('Apples')])
# color = self.color_mapper.palette[self.color_mapper.factors.index(rid)]
# print(cmap.factors.transform('Apples'))

# print(cmap['Apples'])
# {"field": 'label', "transform": cmap}

def _get_categorical_palette(factors: List[str]) -> Dict[str, str]:
    n = max(3, len(factors))
    # if n < len(palette):
    #     _palette = Spectral5
    # elif n < 21:
    #     from bokeh.palettes import Category20
    #     _palette = Category20[n]
    # else:
    from bokeh.palettes import viridis
    _palette = viridis(n)

    return CategoricalColorMapper(factors=factors, palette=_palette)

# print(_get_categorical_palette(factors= factors)['Apples'])
# palette=palette,
#                                                     factors=factors,
#                                                     start=start,
#                                                     end=end,
#                                                     nan_color=nan_color)
# xdr.


show(gridplot([p1,p2,select], ncols=1))