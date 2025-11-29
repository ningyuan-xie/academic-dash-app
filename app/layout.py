# layout.py - Layout components for the Dash app.

from dash import html
import dash_bootstrap_components as dbc
from layout_utils import *


def create_layout() -> html.Div:
    """Creates a Dash app layout in a 3-row * 2-column format with a modern bootstrap theme."""
    
    # Helper function to wrap widgets in a modern card style
    def wrap_widget_card(widget_component):
        return dbc.Card(
            dbc.CardBody(widget_component, className="p-3"),
            className="shadow-sm border-0 h-100", 
            style={"borderRadius": "12px"}
        )

    return html.Div(
        style={'backgroundColor': '#f4f6f9', 'minHeight': '100vh', 'paddingBottom': '40px'},  # Modern light grey background
        children=[
            
            # Header Section
            dbc.Container(fluid=True, className="p-4", children=[
                html.Div(
                    children=[
                        html.H1("GradXplorer – Navigate academia, discover top schools, and find leading researchers with ease!", 
                                className="display-6 fw-bold text-white mb-2"),
                        html.Div(
                            [
                                html.Span("By Ningyuan Xie", className="me-2 text-white-50", style={'fontSize': '18px'}),
                                html.A(
                                    html.Img(src="https://cdn.jsdelivr.net/npm/simple-icons@v5/icons/github.svg", 
                                             style={'height': '24px', 'filter': 'invert(1)'}),
                                    href="https://github.com/ningyuan-xie/academic-dash-app",
                                    target="_blank",
                                    className="text-decoration-none"
                                )
                            ],
                            className="d-flex align-items-center justify-content-center"
                        )
                    ],
                    style={
                        'textAlign': 'center',
                        'padding': '30px',
                        'borderRadius': '15px',
                        'backgroundColor': '#0d6efd',  # Bootstrap Primary Blue
                        'boxShadow': '0 4px 20px rgba(0,0,0,0.1)',
                        'marginBottom': '30px'
                    }
                ),

                # Row 1
                dbc.Row(className='g-4 mb-4', children=[
                    
                    dbc.Col(width=12, lg=6, children=[
                        wrap_widget_card(
                            # 1. Widget One: MongoDB Bar Chart
                            GraphWidget("Top 10 Trending Keywords in Publications", 
                                        graph_id="widget-one", 
                                        graph_type="bar",
                                        control_type="slider+dropdown",
                                        control_id="widget-one-slider",
                                        control_options={"min": 2012, "max": 2020},
                                        second_control_id="widget-one-dropdown-db",
                                        second_control_options={"options": ["MongoDB", "MySQL"], "placeholder": "Select a Database"},
                                        interval_id="interval-one")
                        )
                    ]),

                    dbc.Col(width=12, lg=6, children=[
                        wrap_widget_card(
                            # 2. Widget Two: MySQL Controller
                            ControlWidget(
                                title="User-selected Favorite Keywords",
                                store_id="widget-two-keyword-options-store",
                                dropdown_id="widget-two-keyword-add-dropdown",         # Shared input
                                view_dropdown_id="widget-two-keyword-view-dropdown",   # View-only dropdown
                                add_button_id="widget-two-keyword-add-btn",
                                delete_button_id="widget-two-keyword-delete-btn",
                                restore_button_id="widget-two-keyword-restore-btn",
                                graph_id="widget-two-keyword-pie",
                                default_keywords=[
                                    "artificial intelligence",
                                    "deep learning",
                                    "reinforcement learning",
                                    "natural language processing",
                                    "data mining"
                                ]
                            )
                        )
                    ]),
                ]),

                # Row 2
                dbc.Row(className='g-4 mb-4', children=[
                    
                    dbc.Col(width=12, lg=6, children=[
                        wrap_widget_card(
                            # 3.1 Widget Three: MySQL Table
                            TableWidget("Faculty Members Relevant to Selected Keywords",
                                        table_id="widget-three",
                                        control_type="dropdown",
                                        control_id="widget-three-dropdown",
                                        control_options={"options": [],"placeholder": "Select a Keyword"},
                                        layout="two-col",
                                        interval_id="interval-three",
                                         
                                        right_panel_widgets=[
                                        
                                        # 3.2 Faculty Count Box (Top)
                                        CountDisplayWidget(title="Faculty Count",
                                                           count_id="widget-three-faculty-count-display",
                                                           interval_id="widget-three-faculty-count-interval"),

                                         # 3.3 Delete Faculty Section
                                        DeleteWidget(title="Delete Faculty",
                                                     input_id="widget-three-faculty-id-input",
                                                     button_id="widget-three-delete-button",
                                                     status_id="widget-three-delete-status",
                                                     interval_id="widget-three-clear-message-interval",
                                                     max_value = get_faculty_count(),
                                                     input_type="number",
                                                     placeholder="Enter ID"),
                                        
                                         # 3.4 Restore Button Section
                                        RestoreWidget(title="Restore Faculty",
                                                      button_id="widget-three-restore-button",
                                                      status_id="widget-three-restore-status",
                                                      interval_id="widget-three-restore-message-interval"),
                                        ])
                        )
                    ]),

                    dbc.Col(width=12, lg=6, children=[
                        wrap_widget_card(
                            # 4. Widget Four: MongoDB Bar Chart
                            GraphWidget("Faculty with Highest KRC for Selected Keywords",
                                        graph_id="widget-four",
                                        graph_type="bar",
                                        control_type="triple-dropdown",
                                        control_id="widget-four-dropdown-db",
                                        control_options={"options": ["MongoDB", "MySQL"], "placeholder": "Select a Database"},
                                        second_control_id="widget-four-dropdown-keyword",
                                        second_control_options={"options": [], "placeholder": "Select a Keyword"},
                                        third_control_id="widget-four-dropdown-affiliation",
                                        third_control_options={"options": [], "placeholder": "Select an Affiliation"},
                                        interval_id="interval-four")
                        )
                    ]),
                
                ]),

                # Row 3
                dbc.Row(className='g-4', children=[
                    
                    dbc.Col(width=12, lg=6, children=[
                        wrap_widget_card(
                            # 5. Widget Five: Neo4j Table
                            TableWidget("Top 10 Keywords in Faculty Interests",
                                         table_id="widget-five",
                                         control_type="dropdown",
                                         control_id="widget-five-dropdown",
                                         control_options={"options": get_all_institutes(), "placeholder": "Select a University"},
                                         layout="two-col",
                                         interval_id="interval-five",
                                         
                                         right_panel_widgets=[
                                        
                                        # 5.2 Keyword Count Box (Top)
                                        CountDisplayWidget(title="Keyword Count",
                                                           count_id="widget-five-keyword-count-display",
                                                           interval_id="widget-five-keyword-count-interval"),

                                         # 5.3 Delete Keyword Section
                                        DeleteWidget(title="Delete Keyword",
                                                     input_id="widget-five-keyword-id-input",
                                                     button_id="widget-five-delete-button",
                                                     status_id="widget-five-delete-status",
                                                     interval_id="widget-five-clear-message-interval",
                                                     input_type="text",
                                                     placeholder="Enter ID"),
                                        
                                         # 5.4 Restore Keyword Section
                                        RestoreWidget(title="Restore Keyword",
                                                      button_id="widget-five-restore-button",
                                                      status_id="widget-five-restore-status",
                                                      interval_id="widget-five-restore-message-interval"),
                                        ])
                        )
                    ]),

                    dbc.Col(width=12, lg=6, children=[
                        wrap_widget_card(
                            # 6. Widget Six: Neo4j Sunburst Chart
                            GraphWidget("Top 10 Universities Collaboration",
                                        graph_id="widget-six",
                                        graph_type="sunburst",
                                        control_type="dropdown",
                                        control_id="widget-six-dropdown",
                                        control_options={"options": get_all_institutes(), "placeholder": "Select a University"},
                                        interval_id="interval-six",
                                        details_id="widget-six-details")
                        )
                    ]),

                ])
            ])
        ])