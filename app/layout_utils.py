# layout_widget.py - Reusable layout widgets for Dash app.

from typing import Any, List, Optional, Dict
from dash import html, dcc
import plotly.express as px
from mysql_utils import *
from mongodb_utils import *
from neo4j_utils import *


class GraphWidget(html.Div):
    def __init__(self, title: str, graph_id: str, graph_type: str = "bar", 
                 control_type: Optional[str] = None, control_id: Optional[str] = None, 
                 control_options: Optional[Dict[str, Any]] = None, 
                 second_control_id: Optional[str] = None, 
                 second_control_options: Optional[Dict[str, Any]] = None, 
                 third_control_id: Optional[str] = None, 
                 third_control_options: Optional[Dict[str, Any]] = None, 
                 interval_id: Optional[str] = None, details_id: Optional[str] = None, 
                 **kwargs):

        children: List[Any] = [html.H3(title, style={'textAlign': 'center'})]

        # Control options: slider, slider+dropdown, radio, dropdown, double-dropdown, triple-dropdown
        if control_type == "slider" and control_options:
            children.append(
                dcc.Slider(
                    id=control_id,
                    min=control_options.get("min", 2012),
                    max=control_options.get("max", 2020),
                    marks={y: str(y) for y in range(control_options.get("min", 2012), control_options.get("max", 2020) + 1)},
                    value=control_options.get("value", None),
                    step=control_options.get("step", 1),
                    tooltip={"placement": "bottom", "always_visible": True}
                )
            )
        elif control_type == "slider+dropdown" and control_options and second_control_options:
            children.append(
                html.Div([
                    html.Div(
                        dcc.Dropdown(
                            id=second_control_id,
                            options=[{"label": opt, "value": opt} for opt in second_control_options.get("options", [])],
                            value=second_control_options.get("value", None),
                            clearable=second_control_options.get("clearable", False),
                            placeholder=second_control_options.get("placeholder", "Select an option"),
                        ),
                        style={'width': '50%'}
                    ),
                    html.Div(
                        dcc.Slider(
                            id=control_id,
                            min=control_options.get("min", 2012),
                            max=control_options.get("max", 2020),
                            marks={y: str(y) for y in range(control_options.get("min", 2012), control_options.get("max", 2020) + 1)},
                            value=control_options.get("value", None),
                            step=control_options.get("step", 1),
                            tooltip={"placement": "bottom", "always_visible": True},
                        ),
                        style={'width': '50%'}
                    ),
                ], style={'display': 'flex', 'gap': '4%'})

            )
        elif control_type == "radio" and control_options:
            children.append(
                dcc.RadioItems(
                    id=control_id,
                    options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                    value=control_options.get("value", None),
                    inline=control_options.get("inline", True),
                    style={'margin': '10px 0'}
                )
            )
        elif control_type == "dropdown" and control_options:
            children.append(
                dcc.Dropdown(
                    id=control_id,
                    options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                    value=control_options.get("value", None),
                    clearable=control_options.get("clearable", False),
                    placeholder=control_options.get("placeholder", "Select an option"),
                    style={'width': '100%'}
                )
            )
        elif control_type == "double-dropdown" and control_options and second_control_options:
            children.append(
                html.Div([
                    dcc.Dropdown(
                        id=control_id,
                        options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                        value=control_options.get("value", None),
                        clearable=control_options.get("clearable", False),
                        placeholder=control_options.get("placeholder", "Select an option"),
                        style={'width': '100%'}
                    ),
                    dcc.Dropdown(
                        id=second_control_id,
                        options=[{"label": opt, "value": opt} for opt in second_control_options.get("options", [])],
                        value=second_control_options.get("value", None),
                        clearable=second_control_options.get("clearable", False),
                        placeholder=second_control_options.get("placeholder", "Select another option"),
                        style={'width': '100%'}
                    ),
                ], style={'display': 'flex', 'gap': '4%'}  # Space between dropdowns
                )
            )
        elif control_type == "triple-dropdown" and control_options and second_control_options and third_control_options:
            children.append(
                html.Div([
                    dcc.Dropdown(
                        id=control_id,
                        options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                        value=control_options.get("value", None),
                        clearable=control_options.get("clearable", False),
                        placeholder=control_options.get("placeholder", "Select an option"),
                        style={'width': '100%'}
                    ),
                    dcc.Dropdown(
                        id=second_control_id,
                        options=[{"label": opt, "value": opt} for opt in second_control_options.get("options", [])],
                        value=second_control_options.get("value", None),
                        clearable=second_control_options.get("clearable", False),
                        placeholder=second_control_options.get("placeholder", "Select another option"),
                        style={'width': '100%'}
                    ),
                    dcc.Dropdown(
                        id=third_control_id,
                        options=[{"label": opt, "value": opt} for opt in third_control_options.get("options", [])],
                        value=third_control_options.get("value", None),
                        clearable=third_control_options.get("clearable", False),
                        placeholder=third_control_options.get("placeholder", "Select another option"),
                        style={'width': '100%'}
                    ),
                ], style={'display': 'flex', 'gap': '4%'}  # Space between dropdowns
                )
            )

        # Graph options: bar, pie, sunburst
        figure = {
            "bar": px.bar(title="Select from Options"),
            "pie": px.pie(title="Select from Options"),
            "sunburst": px.sunburst(title="Select from Options")
        }.get(graph_type, px.bar(title="Select from Options"))

        # Container holding graph and optional details side-by-side
        graph_and_details = []

        # Graph component (left side)
        graph_and_details.append(
            html.Div(
                dcc.Graph(id=graph_id, figure=figure),
                style={"flex": "1", "paddingRight": "15px"}
            )
        )

        # Details component (right side, if needed)
        if details_id:
            graph_and_details.append(
                html.Div(
                    id=details_id,
                    style={
                        "flex": "1",
                        "borderLeft": "1px solid #ddd",  # divider line
                        "paddingLeft": "15px",
                        "overflowX": "auto",
                        "maxHeight": "500px"
                    }
                )
            )

        children.append(
            html.Div(
                graph_and_details,
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "flex-start",
                    "marginTop": "20px",
                    "width": "100%"
                }
            )
        )

        # Interval:
        children.append(dcc.Interval(id=interval_id if interval_id else f"{graph_id}-interval", interval=10 * 1000, n_intervals=0, disabled=True))

        # Create the widget from super class
        super().__init__(
            children=children,
            className='six columns',
            style={
                'backgroundColor': 'white',
                'borderRadius': '10px',
                'padding': '15px',
                'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                'margin': '10px',
                'height': '650px',
                **kwargs
            }
        )


class ControlWidget(html.Div):
    def __init__(self,
                 title: str,
                 store_id: str,
                 dropdown_id: str,
                 view_dropdown_id: str,
                 add_button_id: str,
                 delete_button_id: str,
                 restore_button_id: str,
                 graph_id: str,
                 default_keywords: List[str],
                 **kwargs):

        children: List[Any] = [html.H3(title, style={"textAlign": "center", "marginBottom": "20px"})]

        # Row 1: Add keyword + View favorites dropdowns
        children.append(
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        id=dropdown_id,
                        options=[{"label": kw, "value": kw} for kw in get_all_keywords()],
                        placeholder="Select a keyword...",
                        style={"width": "100%"}
                    )
                ], style={"flex": "1", "marginRight": "10px"}),

                html.Div([
                    dcc.Dropdown(
                        id=view_dropdown_id,
                        options=[{"label": kw, "value": kw} for kw in default_keywords],
                        placeholder="Favorite keywords",
                        style={"width": "100%"}
                    )
                ], style={"flex": "1"})
            ], style={"display": "flex", "marginBottom": "10px"})
        )

        # Shared style base
        button_base_style = {
            "width": "auto",
            "padding": "10px",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px",
            "fontWeight": "bold",
            "textAlign": "center",
            "display": "flex",
            "justifyContent": "center",
            "alignItems": "center",
            "flex": "1"
        }

        # Row 2: Add/Delete and Restore buttons
        children.append(
            html.Div([
                # Left: ADD and DELETE buttons side-by-side
                html.Div([
                    html.Button("ADD", id=add_button_id, style={
                        **button_base_style,
                        "backgroundColor": "#2196F3",
                        "marginRight": "10px"
                    }),
                    html.Button("DELETE", id=delete_button_id, style={
                        **button_base_style,
                        "backgroundColor": "#f44336"
                    }),
                ], style={"display": "flex", "gap": "10px", "flex": "1", "marginRight": "10px"}),

                # Right: RESTORE button
                html.Div([
                    html.Button("RESTORE", id=restore_button_id, style={
                        **button_base_style,
                        "backgroundColor": "#4CAF50"
                    }),
                ], style={"display": "flex", "gap": "10px", "flex": "1", "marginRight": "0px"})
            ], style={"display": "flex", "marginBottom": "20px"})
        )

        # Store for active keywords
        children.append(dcc.Store(id=store_id, data=default_keywords))

        # Pie chart to visualize selected keywords
        children.append(
            dcc.Graph(id=graph_id, style={"marginTop": "10px"})
        )

        # Finalize wrapper
        super().__init__(
            children=children,
            className="six columns",
            style={
                "backgroundColor": "white",
                "borderRadius": "10px",
                "padding": "15px",
                "boxShadow": "2px 2px 10px rgba(0,0,0,0.2)",
                "margin": "10px",
                "height": "650px",
                **kwargs
            }
        )


class TableWidget(html.Div):
    def __init__(self, title: str, table_id: str, 
                 control_type: Optional[str] = None, control_id: Optional[str] = None, 
                 control_options: Optional[Dict[str, Any]] = None, 
                 second_control_id: Optional[str] = None, 
                 second_control_options: Optional[Dict[str, Any]] = None, 
                 layout: str = "one-col", right_panel_widgets: Optional[Any] = None, 
                 interval_id: Optional[str] = None, **kwargs):

        children: List[Any] = [html.H3(title, style={'textAlign': 'center'})]

        # Control options: slider, radio, dropdown, double-dropdown
        if control_type == "slider" and control_options:
            children.append(
                dcc.Slider(
                    id=control_id,
                    min=control_options.get("min", 2012),
                    max=control_options.get("max", 2020),
                    marks={y: str(y) for y in range(control_options.get("min", 2012), control_options.get("max", 2020) + 1)},
                    value=control_options.get("value", None),
                    step=control_options.get("step", 1),
                    tooltip={"placement": "bottom", "always_visible": True}
                )
            )
        elif control_type == "radio" and control_options:
            children.append(
                dcc.RadioItems(
                    id=control_id,
                    options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                    value=control_options.get("value", None),
                    inline=control_options.get("inline", True),
                    style={'margin': '10px 0'}
                )
            )
        elif control_type == "dropdown" and control_options:
            children.append(
                dcc.Dropdown(
                    id=control_id,
                    options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                    value=control_options.get("value", None),
                    clearable=control_options.get("clearable", False),
                    placeholder=control_options.get("placeholder", "Select an option"),
                    style={'width': '100%'}
                )
            )
        elif control_type == "double-dropdown" and control_options and second_control_options:
            children.append(
                html.Div([
                    dcc.Dropdown(
                        id=control_id,
                        options=[{"label": opt, "value": opt} for opt in control_options.get("options", [])],
                        value=control_options.get("value", None),
                        clearable=control_options.get("clearable", False),
                        placeholder=control_options.get("placeholder", "Select an option"),
                        style={'width': '100%'}
                    ),
                    dcc.Dropdown(
                        id=second_control_id,
                        options=[{"label": opt, "value": opt} for opt in second_control_options.get("options", [])],
                        value=second_control_options.get("value", None),
                        clearable=second_control_options.get("clearable", False),
                        placeholder=second_control_options.get("placeholder", "Select another option"),
                        style={'width': '100%'}
                    ),
                ], style={'display': 'flex', 'gap': '4%'}  # Space between dropdowns
                )
            )

        table_section = html.Div(id=table_id, 
                                 children=html.P("Select a Keyword to Display Data", style={'textAlign': 'center', 'color': '#555'}))

        # Layout options: one-col, two-col
        if layout == "one-col":
            children.append(table_section)
        else:  # layout == "two-col"
            children.append(
                html.Div([
                    html.Div(table_section, style={'width': '80%', 'paddingRight': '20px'}),

                    html.Div(right_panel_widgets, style={'width': '20%', 'display': 'flex', 'flexDirection': 'column', 'alignItems': 'center'})
                ], style={'display': 'flex', 'alignItems': 'flex-start', 'justifyContent': 'space-between'})
            )

        # Interval:
        children.append(dcc.Interval(id=interval_id if interval_id else f"{table_id}-interval",
                                     interval=10 * 1000, n_intervals=0, disabled=True))

        # Create the widget from super class
        super().__init__(
            children=children,
            className='six columns',
            style={
                'backgroundColor': 'white',
                'borderRadius': '10px',
                'padding': '15px',
                'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                'margin': '10px',
                'height': '650px',
                **kwargs
            }
        )


class CountDisplayWidget(html.Div):
    def __init__(self, title: str, count_id: str, interval_id: Optional[str] = None, **kwargs):
        super().__init__(
            children=[
                html.H4(title, style={'textAlign': 'center', 'marginBottom': '10px', 'fontSize': '20px'}),
                html.Div(id=count_id, children="Loading...",
                         style={'fontSize': '20px', 'fontWeight': 'bold', 'textAlign': 'center'}),
                dcc.Interval(id=interval_id if interval_id else f"{count_id}-interval",
                             interval=10 * 1000, n_intervals=0, disabled=True)
            ],
            style={
                'backgroundColor': '#f9f9f9',
                'padding': '15px',
                'borderRadius': '10px',
                'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                'marginBottom': '20px',
                **kwargs
            }
        )


class DeleteWidget(html.Div):
    def __init__(self, title: str, input_id: str, button_id: str, status_id: str, input_type: str,
                 interval_id: Optional[str] = None, min_value: int = 1, max_value: Optional[int] = None, 
                 placeholder: str = "Enter ID", **kwargs):
        super().__init__(
            children=[
                html.H4(title, style={'textAlign': 'center', 'marginBottom': '10px', 'fontSize': '20px'}),

                dcc.Input(
                    id=input_id, 
                    type="number" if input_type == "number" else "text",  
                    placeholder=placeholder,
                    min=min_value if input_type == "number" else None,
                    max=max_value if input_type == "number" else None,
                    style={'width': '100%', 'padding': '8px', 'borderRadius': '5px', 
                           'marginBottom': '10px', 'fontSize': '14px'}
                ),

                html.Button("DELETE", id=button_id, n_clicks=0, 
                            style={'width': '100%', 'padding': '10px', 'backgroundColor': '#e74c3c', 
                                   'color': 'white', 'border': 'none', 'borderRadius': '5px',
                                   'cursor': 'pointer', 'fontSize': '18px', 'fontWeight': 'bold',
                                   'display': 'flex', 'justifyContent': 'center', 
                                   'alignItems': 'center', 'textAlign': 'center'}),

                html.Div(id=status_id, 
                         style={'textAlign': 'center', 'marginTop': '10px', 'color': '#e74c3c', 
                                'minHeight': '25px'}),  # Keeps space reserved for delete status

                dcc.Interval(id=interval_id if interval_id else f"{status_id}-interval",
                             interval=2000, max_intervals=1, disabled=True)
            ],
            style={
                'backgroundColor': '#fff3f3',
                'padding': '15px',
                'borderRadius': '10px',
                'boxShadow': '2px 2px 10px rgba(255,0,0,0.2)',
                'marginBottom': '20px',
                **kwargs
            }
        )


class RestoreWidget(html.Div):
    def __init__(self, title: str, button_id: str, status_id: str, interval_id: Optional[str] = None, **kwargs):
        super().__init__(
            children=[
                html.Div([
                    html.Button("RESTORE", id=button_id, n_clicks=0, 
                                style={'width': '100%', 'padding': '10px', 
                                       'backgroundColor': '#2ecc71', 
                                       'color': 'white', 'border': 'none', 'borderRadius': '5px',
                                       'cursor': 'pointer', 'fontSize': '18px', 'fontWeight': 'bold',
                                       'display': 'flex', 'justifyContent': 'center', 
                                       'alignItems': 'center', 'textAlign': 'center'})
                ], style={'display': 'flex', 'justifyContent': 'center'}),

                html.Div(id=status_id, 
                         style={'textAlign': 'center', 'marginTop': '10px', 'color': '#2ecc71', 
                                'minHeight': '25px'}),  # Keeps space reserved for restore status

                dcc.Interval(id=interval_id if interval_id else f"{status_id}-interval",
                             interval=2000, max_intervals=1, disabled=True)
            ],
            style={
                'backgroundColor': '#e8f5e9',
                'padding': '15px',
                'borderRadius': '10px',
                'boxShadow': '2px 2px 10px rgba(46, 204, 113, 0.2)',
                'marginBottom': '20px',
                **kwargs
            }
        )


class RefreshWidget(html.Div):
    def __init__(self, button_id: str, **kwargs):
        super().__init__(
            children=[
                html.Button("Refresh", id=button_id, n_clicks=0, 
                            style={'width': '100%', 'padding': '10px', 'backgroundColor': '#3498db', 
                                   'color': 'white', 'border': 'none', 'borderRadius': '5px',
                                   'cursor': 'pointer', 'fontSize': '18px', 'fontWeight': 'bold',
                                   'display': 'flex', 'justifyContent': 'center', 
                                   'alignItems': 'center', 'textAlign': 'center'})
            ],
            style={'display': 'flex', 'justifyContent': 'center', 'marginTop': '10px', **kwargs}
        )
