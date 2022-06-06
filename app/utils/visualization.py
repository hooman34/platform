import plotly.graph_objects as go
from plotly.subplots import make_subplots
from .log import get_logger

logger = get_logger(__name__)

def _plot_data(plot_type, plot_settings):
    """
    Create a plotly figure. The input should be a list of list.
    The inner list should be [dataset, x_column, y_column]

    Args:
        plot_type1(str): type of graph. line or bar
        plot_settings(list): [[dataset, x_column, y_column], [dataset, x_column, y_column], ...]

    Returns:
        fig(plotly.graph_objs._figure.Figure): plot
    """
    fig = go.Figure()

    if plot_type=='line':
        for i, setting in enumerate(plot_settings):
            trace = _create_trace(plot_type, setting[0], setting[1], setting[2], setting[3], 'y'+str(i+1))
            fig.add_trace(trace)
    elif plot_type=='bar':
        logger.info("bar graph not implemented")
        return None
    else:
        logger.info("specified graph type is not implemented")
        return None
    
    # update fig to make it 
    update_dict = {}

    for i in range(len(plot_settings)-1):
        update_dict['yaxis'+str(i+2)] = {"overlaying":'y', 'visible': False}
    fig['layout'].update(update_dict)
    fig['layout'].update({'yaxis':{'visible':False}})
    
    fig.update_layout(legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1))

    return fig


def _create_trace(plot_type, data, x, y, name, yaxis):
    """
    create trace, which will be a part of the plot
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
    return trace


