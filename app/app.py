# app.py - Main entry point for the Dash app.

from dash import Dash
from layout import *
from callbacks import *


def create_app() -> Dash:
    """Create and initialize the Dash app."""
    app = Dash(
        __name__, 
        external_stylesheets=[dbc.themes.FLATLY, dbc.icons.BOOTSTRAP]
    )
    app.title = "Exploring Academic World"
    app.layout = create_layout()
    return app


if __name__ == "__main__":
    app = create_app()

    app.run(
        debug=True,
        use_reloader=True,
        dev_tools_hot_reload=True,
        host="0.0.0.0",
        port=8050
    )
