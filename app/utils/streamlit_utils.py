import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_plots(plot_settings):
    """
    Create plot based on user input.

    Args:
        plot_type:
        plot_settings(list): [[dataset, x_column, y_column], [dataset, x_column, y_column], ...]

    Returns:
        fig (plotly.graph_objs._figure.Figure): plot
    """
    fig = _plot_data('line', plot_settings)

    return fig