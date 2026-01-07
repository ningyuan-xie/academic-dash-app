# callbacks.py - Register Dash callback functions.

from typing import Any, List, Tuple, Union
import pandas as pd
from dash import dash, Output, Input, html, State, callback
import plotly.express as px
from mysql_utils import *
from mongodb_utils import *
from neo4j_utils import *
from callbacks_utils import *


# 1. Widget One: MongoDB Bar Chart (with MySQL option)
@callback(
    Output("widget-one", "figure"),
    [Input("widget-one-slider", "value"), 
     Input("widget-one-dropdown-db", "value"), 
     Input("interval-one", "n_intervals")],
    prevent_initial_call=True
)
def widget_one(year: int, selected_db: str, n: int) -> Any:
    """Fetch collection data from MongoDB or MySQL and update UI."""
    # Prevent execution if either input is missing
    if not year or not selected_db:
        return px.bar(title="Select a Year and Database to Display Data")

    if selected_db == "MongoDB":
        keywords_data = find_most_popular_keywords_mongo(year)
    elif selected_db == "MySQL":
        keywords_data = find_most_popular_keywords_sql(year)

    # Create Bar Chart
    title = f"Top 10 Keywords in Publications Since {year} ({selected_db})"
    return create_bar_chart(keywords_data, title, "Keyword", "Publication Count", False, selected_db)


# 2.1 Widget Two: MySQL Controller - Viewer Dropdown
@callback(
    Output("widget-two-keyword-view-dropdown", "options"),
    Output("widget-three-dropdown", "options"),
    Output("widget-four-dropdown-keyword", "options"),
    Input("widget-two-keyword-options-store", "data")
)
def update_all_keyword_dropdowns(favorite_keywords: List[str]) -> Tuple[List[dict], List[dict], List[dict]]:
    """Update all keyword dropdowns whenever the favorites list changes."""
    options = [{"label": kw, "value": kw} for kw in favorite_keywords]
    return options, options, options


# 2.2 Widget Two: MySQL Controller - Add Keyword
@callback(
    Output("widget-two-keyword-options-store", "data"),
    Input("widget-two-keyword-add-btn", "n_clicks"),
    State("widget-two-keyword-add-dropdown", "value"),  # selected_keyword
    State("widget-two-keyword-options-store", "data"),  # current_keywords
    prevent_initial_call=True
)
def add_favorite_keyword(n_clicks: int, selected_keyword: str, current_keywords: List[str]) -> List[str]:
    """Add a new keyword to the list of keywords."""
    if not selected_keyword or selected_keyword in current_keywords:
        return current_keywords  # Do nothing if blank or already exists

    updated_keywords = current_keywords + [selected_keyword]
    return updated_keywords


# 2.3 Widget Two: MySQL Controller - Delete Keyword
@callback(
    Output("widget-two-keyword-options-store", "data", allow_duplicate=True),
    Input("widget-two-keyword-delete-btn", "n_clicks"),
    State("widget-two-keyword-add-dropdown", "value"),
    State("widget-two-keyword-options-store", "data"),
    prevent_initial_call=True
)
def delete_favorite_keyword(n_clicks: int, selected_keyword: str, current_keywords: List[str]) -> List[str]:
    """Delete a keyword from the list of keywords."""
    if not selected_keyword or selected_keyword not in current_keywords:
        return current_keywords  # Do nothing if blank or doesn't exist

    updated_keywords = [kw for kw in current_keywords if kw != selected_keyword]
    return updated_keywords


# 2.4 Widget Two: MySQL Controller - Restore Default Keywords
@callback(
    Output("widget-two-keyword-options-store", "data", allow_duplicate=True),
    Input("widget-two-keyword-restore-btn", "n_clicks"),
    prevent_initial_call=True
)
def restore_default_keywords(n_clicks: int) -> List[str]:
    """Restore the default list of keywords."""
    return [
        "artificial intelligence",
        "deep learning",
        "reinforcement learning",
        "natural language processing",
        "data mining"
    ]


# 2.5 Widget Two: MySQL Controller - Pie Chart
@callback(
    Output("widget-two-keyword-pie", "figure"),
    Input("widget-two-keyword-options-store", "data")
)
def update_keyword_pie_chart(keywords: List[str]) -> Any:
    """Update pie chart to reflect current favorite keywords."""
    if not keywords:
        # Return empty pie chart with message
        return create_pie_chart(
            pd.DataFrame(columns=["keyword", "count"]),
            names_col="keyword",
            values_col="count",
            title="No Keywords Selected"
        )

    # Build DataFrame with count = 1 for each keyword
    df = pd.DataFrame({
        "keyword": keywords,
        "count": [1] * len(keywords)
    })

    return create_pie_chart(df, names_col="keyword", values_col="count", 
    title="Current Favorite Keywords")


# 3.1 Widget Three: MySQL Table
@callback(
    [Output("widget-three", "children"),
     Output("widget-three-faculty-count-display", "children")],
    [Input("widget-three-dropdown", "value"), 
     Input("interval-three", "n_intervals")],
    prevent_initial_call=True
)
def widget_three(selected_keyword: str, n: int) -> Tuple[Any, Any]:
    """Fetch faculty members relevant to the selected keyword and update UI."""
    if not selected_keyword:
        return dash.no_update, dash.no_update

    # Get updated table
    faculty_count = get_faculty_count()
    faculty_data = find_faculty_relevant_to_keyword(selected_keyword)
    df = pd.DataFrame(faculty_data, columns=["ID", "Faculty", "University"])
    updated_table = create_data_table(df)

    return updated_table, faculty_count


# 3.2 Widget Three: MySQL Table - Delete Faculty
@callback(
    [Output("widget-three-delete-status", "children"),  # Show/Clear message
     Output("widget-three-faculty-count-display", "children", allow_duplicate=True),
     Output("widget-three-clear-message-interval", "disabled"),  # when clicked, disabled; after 2s, enabled
     Output("widget-three-clear-message-interval", "n_intervals"),  # when clicked, reset to 0
     Output("widget-three", "children", allow_duplicate=True)],  # 3.1 above
    [Input("widget-three-delete-button", "n_clicks"),   # Delete button click
     Input("widget-three-clear-message-interval", "n_intervals")],  # n_interval ticks from 0 to 1
    State("widget-three-faculty-id-input", "value"),  # Faculty ID input
    State("widget-three-dropdown", "value"),  # Selected keyword
    prevent_initial_call=True
)
def delete_faculty_callback(n_clicks: int, n_intervals: int, 
                            faculty_id: int, selected_keyword: str) -> Tuple[str, Any, bool, int, Any]:
    """Delete faculty member and update UI."""
    # Case 1: The timer finishes, 2s have passed, n_intervals reached 1
    if n_intervals == 1:
        # Clear message, no update for count, disable interval (stop triggering itself again), reset n_intervals
        return "", dash.no_update, True, 0, dash.no_update

    # Case 2: Just clicked the delete button
    # Check if faculty_id is None
    if faculty_id is None:
        return "Enter an ID.", dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Attempt to delete faculty
    success = delete_faculty(faculty_id)
    message = f"ID {faculty_id} deleted." if success else f"Delete failed."

    # Get updated table
    faculty_count = get_faculty_count()
    faculty_data = find_faculty_relevant_to_keyword(selected_keyword)
    df = pd.DataFrame(faculty_data, columns=["ID", "Faculty", "University"])
    updated_table = create_data_table(df)

    # Show message, update count, enable dcc.Interval (triggers itself again), reset n_intervals to 0 which will tick to 1
    return message, faculty_count, False, 0, updated_table


# 3.3 Widget Three: MySQL Table - Restore Faculty
@callback(
    [Output("widget-three-restore-status", "children"),  # Message display
     Output("widget-three-faculty-count-display", "children", allow_duplicate=True),
     Output("widget-three-restore-message-interval", "disabled"),  # when clicked, disabled; after 2s, enabled
     Output("widget-three-restore-message-interval", "n_intervals"),  # when clicked, reset to 0
     Output("widget-three", "children", allow_duplicate=True)], 
    [Input("widget-three-restore-button", "n_clicks"),  # Trigger on button click
     Input("widget-three-restore-message-interval", "n_intervals")],  # n_interval ticks from 0 to 1
     State("widget-three-dropdown", "value"),  # Selected keyword
    prevent_initial_call=True
)
def restore_faculty_callback(n_clicks: int, n_intervals: int,
                             selected_keyword: str) -> Tuple[str, Any, bool, int, Any]:
    """Restore faculty member and update UI."""
    # Case 1: The timer finishes, 2s have passed, n_intervals reached 1
    if n_intervals == 1:
        # Clear message, no update for count, disable interval (stop triggering itself again), reset n_intervals
        return "", dash.no_update, True, 0, dash.no_update

    # Case 2: Just clicked the restore button
    # Attempt to restore faculty
    success = restore_faculty()
    message = "Faculty restored." if success else "Restore failed."

    # Get updated table
    faculty_count = get_faculty_count()
    faculty_data = find_faculty_relevant_to_keyword(selected_keyword)
    df = pd.DataFrame(faculty_data, columns=["ID", "Faculty", "University"])
    updated_table = create_data_table(df)

    # Show message, update count, enable dcc.Interval (triggers itself again), reset n_intervals to 0 which will tick to 1
    return message, faculty_count, False, 0, updated_table


# 4.1 Widget Four: MongoDB Bar Chart (with MySQL option) - Database Dropdown
@callback(
    Output("widget-four-dropdown-affiliation", "options"),
    Output("widget-four-dropdown-affiliation", "value"),  # added value here for reset
    Input("widget-four-dropdown-db", "value")
)
def update_affiliation_options(selected_db: str) -> Tuple[List[dict], None]:
    """Update affiliation options based on selected database."""
    if selected_db == "MongoDB":
        options = [{"label": aff, "value": aff} for aff in get_all_affiliations()]
    elif selected_db == "MySQL":
        options = [{"label": uni, "value": uni} for uni in get_all_universities()]
    else:
        options = []
    return options, None  # Reset currently selected value to None to avoid inconsistent data


# 4.2 Widget Four: MongoDB Bar Chart (with MySQL option)
@callback(
    Output("widget-four", "figure"),
    [Input("widget-four-dropdown-db", "value"),
     Input("widget-four-dropdown-keyword", "value"), 
     Input("widget-four-dropdown-affiliation", "value"), 
     Input("interval-four", "n_intervals")],
    prevent_initial_call=True
)
def widget_four(selected_db: str, selected_keyword: str, selected_affiliation: str, n: int) -> Any:
    """Fetch collection data from MongoDB or SQL and update UI."""
    # Prevent execution if either input is missing
    if not selected_keyword or not selected_affiliation or not selected_db:
        return px.bar(title="Select a Database, a Keyword, and an Affiliation to Display Data")  # Default empty figure

    if selected_db == "MongoDB":
        faculty_data = find_top_faculties_with_highest_KRC_keyword(selected_keyword, selected_affiliation)
    elif selected_db == "MySQL":
        faculty_data = find_top_faculties_with_highest_KRC_keyword_sql(selected_keyword, selected_affiliation)

    # Prevent execution if no data found
    if not faculty_data:
        return px.bar(title="No Data Found for Selected Keyword and Affiliation")

    # Create Horizontal Bar Chart
    title = f"Faculty members with Highest KRC for {selected_keyword.title()} at: <br>{selected_affiliation} ({selected_db})"
    return create_bar_chart(faculty_data, title, "KRC", "Faculty", True, selected_db)


# 5.1 Widget Five: Neo4j Table
@callback(
    [Output("widget-five", "children"),
     Output("widget-five-keyword-count-display", "children")], # 5.2 below
    [Input("widget-five-dropdown", "value"), 
     Input("interval-five", "n_intervals")],
    prevent_initial_call=True
)
def widget_five(selected_university: str, n: int) -> Tuple[Any, Any]:
    """Fetch label count for selected Neo4j label and update UI."""
    if not selected_university:
        return dash.no_update, dash.no_update  # No keyword selected, return no update for table, update for count

    # Get updated table
    keyword_count = get_keyword_count()
    keyword_data = faculty_interested_in_keywords(selected_university)
    df_keyword_data = pd.DataFrame(keyword_data, columns=["ID", "Keyword", "Faculty Count"])
    updated_table = create_data_table(df_keyword_data)

    return updated_table, keyword_count


# 5.2 Widget Five: Neo4j Table - Delete Keywords
@callback(
    [Output("widget-five-delete-status", "children"),  # Show/Clear message
     Output("widget-five-keyword-count-display", "children", allow_duplicate=True),  # 5.2 above
     Output("widget-five-clear-message-interval", "disabled"),  # when clicked, disabled; after 2s, enabled
     Output("widget-five-clear-message-interval", "n_intervals"), # when clicked, reset to 0
     Output("widget-five", "children", allow_duplicate=True)],  # 5.1 above
    [Input("widget-five-delete-button", "n_clicks"),   # Delete button click
     Input("widget-five-clear-message-interval", "n_intervals")],  # n_interval ticks from 0 to 1
    State("widget-five-keyword-id-input", "value"),  # Faculty ID input
    State("widget-five-dropdown", "value"),  # Selected university
    prevent_initial_call=True
)
def delete_keyword_callback(n_clicks: int, n_intervals: int, 
                            keyword_id: str, selected_university: str) -> Tuple[str, Any, bool, int, Any]:
    """Delete keyword and update UI."""
    # Case 1: The timer finishes, 2s have passed, n_intervals reached 1
    if n_intervals == 1:
        # Clear message, no update for count, disable interval (stop triggering itself again), reset n_intervals
        return "", dash.no_update, True, 0, dash.no_update

    # Case 2: Just clicked the delete button
    # Check if keyword_id is None
    if keyword_id is None:
        return "Enter an ID.", dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Attempt to delete keywords
    success = delete_keyword(keyword_id)
    message = f"ID {keyword_id} deleted." if success else f"Delete failed."

    # Get updated table
    keyword_count = get_keyword_count()
    keyword_data = faculty_interested_in_keywords(selected_university)
    df_keyword_data = pd.DataFrame(keyword_data, columns=["ID", "Keyword", "Faculty Count"])
    updated_table = create_data_table(df_keyword_data)

    # Show message, update count, enable dcc.Interval (triggers itself again), reset n_intervals to 0 which will tick to 1
    return message, keyword_count, False, 0, updated_table


# 5.3 Widget Five: Neo4j Table - Restore Keywords
@callback(
    [Output("widget-five-restore-status", "children"),  # Message display
     Output("widget-five-keyword-count-display", "children", allow_duplicate=True),  # 3.2 above
     Output("widget-five-restore-message-interval", "disabled"),  # when clicked, disabled; after 2s, enabled
     Output("widget-five-restore-message-interval", "n_intervals"),  # when clicked, reset to 0
     Output("widget-five", "children", allow_duplicate=True)],  # 5.1 above
    [Input("widget-five-restore-button", "n_clicks"),  # Trigger on button click
     Input("widget-five-restore-message-interval", "n_intervals")],  # n_interval ticks from 0 to 1
    State("widget-five-dropdown", "value"),  # Selected university
    prevent_initial_call=True
)
def restore_keyword_callback(n_clicks: int, n_intervals: int,
                             selected_university: str) -> Tuple[str, Any, bool, int, Any]:
    """Restore keyword and update UI."""
    # Case 1: The timer finishes, 2s have passed, n_intervals reached 1
    if n_intervals == 1:
        # Clear message, no update for count, disable interval (stop triggering itself again), reset n_intervals
        return "", dash.no_update, True, 0, dash.no_update

    # Case 2: Just clicked the restore button
    # Attempt to restore keyword
    success = restore_keyword()
    message = "Keyword restored." if success else "Restore failed."

    # Get updated table
    keyword_count = get_keyword_count()
    keyword_data = faculty_interested_in_keywords(selected_university)
    df_keyword_data = pd.DataFrame(keyword_data, columns=["ID", "Keyword", "Faculty Count"])
    updated_table = create_data_table(df_keyword_data)

    # Show message, update count, enable dcc.Interval (triggers itself again), reset n_intervals to 0 which will tick to 1
    return message, get_keyword_count(), False, 0, updated_table


# 6.1 Widget Six: Neo4j Sunburst Chart
@callback(
    Output("widget-six", "figure"),
    [Input("widget-six-dropdown", "value"), Input("interval-six", "n_intervals")],
    prevent_initial_call=True
)
def widget_six(selected_university: str, n: int) -> Any:
    """Fetch university data collaborated with selected university and update UI."""
    # Return empty figure if no university is selected
    if not selected_university:
        return px.sunburst(title="Select a University to Display Data")  

    university_data = university_collaborate_with(selected_university)

    # Convert result into a DataFrame
    df_university_data = pd.DataFrame(university_data, 
                                      columns=["university", "faculty_collaborate_count"])

    # Create Sunburst Chart
    title=f"Top 10 Universities collaborated with: <br>{selected_university}"
    return create_sunburst_chart(df_university_data, "university", "faculty_collaborate_count", title)


# 6.2 Widget Six: Neo4j Sunburst Chart - University Information (from MySQL)
@callback(
    Output("widget-six-details", "children"),
    Input("widget-six", "clickData")
)
def show_collaboration_details(clickData: dict) -> Any:
    if not clickData:
        return "Click a university segment in the sunburst chart to view details."

    # ["points"][0]["label"] returns the university name from clicking the sunburst chart
    clicked_university = clickData["points"][0]["label"]

    # Fetch from MySQL Database
    result = get_university_information(clicked_university)

    if not result:
        return html.Div([
            html.H4(f"No information found for: {clicked_university}")
        ])

    # Build HTML header and table
    return html.Div([
        create_section_header("University Details:", clicked_university),
        create_info_table(["Name", "Total Faculty", "Logo"], result)
    ])
