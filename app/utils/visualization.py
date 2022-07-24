import plotly.graph_objects as go
from plotly.subplots import make_subplots
from .log import get_logger

logger = get_logger(__name__)


def plot_data(plot_settings):
    """
    Create a plotly figure. The input should be a list of list.

    Args:
        plot_settings (list): [[plot_type1, dataset, x_column, y_column, data_label, secondary_axis_bool],
                               [plot_type1, dataset, x_column, y_column, data_label, secondary_axis_bool], ...]

    Returns:
        fig (plotly.graph_objs._figure.Figure): plot
    """
    for i in plot_settings:
        assert len(i) == 6

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    for i, setting in enumerate(plot_settings):
        plot_type = setting[0]
        assert plot_type in ['scatter', 'line', 'bar']
        trace = create_trace(plot_type, setting[1], setting[2], setting[3], setting[4], 'y' + str(i + 1))
        fig.add_trace(trace, secondary_y=setting[5])

    # update fig to make it
    update_dict = {}

    for i in range(len(plot_settings) - 1):
        update_dict['yaxis' + str(i + 2)] = {"overlaying": 'y', 'visible': False}
    fig['layout'].update(update_dict)
    fig['layout'].update({'yaxis': {'visible': False}})

    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1))

    return fig


def create_trace(plot_type, data, x, y, name, yaxis):
    """
    Create a plotly trace.

    Args:
        plot_type (str): Type of plot
        data (pd.DataFrame): Pandas dataframe object
        x (str): Column name that goes to the x axis
        y (str): Column name that goes to the y axis
        name (str): Name of the
        yaxis (str): Name of the 'y' axis

    Returns:
        trace (Union[plotly.graph_objs._bar.Bar, plotly.graph_objs._scatter.Scatter]):
    """
    if plot_type == 'bar':
        trace = go.Bar(x=data[x],
                       y=data[y],
                       name=name,
                       yaxis=yaxis)
    if plot_type == 'line':
        trace = go.Scatter(x=data[x],
                           y=data[y],
                           name=name,
                           yaxis=yaxis)
    if plot_type == 'scatter':
        trace = go.Scatter(x=data[x],
                           y=data[y],
                           name=name,
                           yaxis=yaxis,
                           mode='markers')

    return trace


