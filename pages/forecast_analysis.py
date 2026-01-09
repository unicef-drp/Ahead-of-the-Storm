from dash import html, dcc, Input, Output, State, callback
import dash
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import numpy as np
import os
import warnings
import plotly.graph_objects as go

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config
from components.ui.header import make_header
from components.ui.footer import footer
from components.data.snowflake_utils import get_snowflake_connection, get_available_wind_thresholds, get_active_countries, get_snowflake_data
from gigaspatial.core.io.readers import read_dataset
from components.data.data_store_utils import get_data_store

# Constants
ZOOM_LEVEL = 14
VIEWS_DIR = config.VIEWS_DIR or "aos_views"
ROOT_DATA_DIR = config.ROOT_DATA_DIR or "geodb"

# Initialize data store
giga_store = get_data_store()

# Load active countries from Snowflake
countries_df = get_active_countries()

# Build country options list for dropdowns
COUNTRY_OPTIONS = []
if not countries_df.empty:
    COUNTRY_OPTIONS = [
        {"value": row['COUNTRY_CODE'], "label": row['COUNTRY_NAME']}
        for _, row in countries_df.iterrows()
    ]
    # Set default country to JAM if available, otherwise first in list
    DEFAULT_COUNTRY = "JAM" if "JAM" in [opt["value"] for opt in COUNTRY_OPTIONS] else (COUNTRY_OPTIONS[0]["value"] if COUNTRY_OPTIONS else None)
else:
    DEFAULT_COUNTRY = None
    print("⚠ No country options available - country dropdown will be empty")

dash.register_page(
    __name__, path="/analysis", name="Forecast Analysis"
)

# Load initial metadata and pre-process for efficiency
metadata_df = get_snowflake_data()
if not metadata_df.empty:
    metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date.astype(str)
    metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')
    unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
else:
    unique_dates = []

def make_custom_header():
    """Use the standard header which now includes Last Updated timestamp"""
    return make_header(active_tab="tab-analysis")

def create_wind_threshold_tabs_content(metric_prefix, metric_label):
    """Create SegmentedControl for wind thresholds with plots for a given metric - avoids duplicate IDs"""
    return [
        # Wind threshold selector - SegmentedControl instead of nested tabs
        dmc.SegmentedControl(
            id=f"analysis-{metric_prefix}-threshold-selector",
            value="34",
            data=THRESHOLD_OPTIONS,
            mb="md",
            fullWidth=True
        ),
        # Single set of graphs (no duplicates)
        dmc.Grid(
            [
                dmc.GridCol(
                    [
                        dmc.Paper(
                            [
                                dmc.Text(f"{metric_label.upper()} AFFECTED", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}, mb="xs"),
                                dcc.Graph(id=f"analysis-{metric_prefix}-plot", style={"height": "350px"})
                            ],
                            p="md",
                            shadow="xs"
                        )
                    ],
                    span=6
                ),
                dmc.GridCol(
                    [
                        dmc.Paper(
                            [
                                dmc.Text("EXCEEDANCE PROBABILITY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}, mb="xs"),
                                dcc.Graph(id=f"analysis-{metric_prefix}-exceedance", style={"height": "350px"})
                            ],
                            p="md",
                            shadow="xs"
                        )
                    ],
                    span=6
                )
            ],
            gutter="md",
            mb="md"
        ),
        dmc.Grid(
            [
                dmc.GridCol(
                    [
                        create_impact_summary(metric_prefix)  # Create fresh instance with unique IDs per tab
                    ],
                    span=6
                    ),
                dmc.GridCol(
                    [
                        dmc.Paper(
                            [
                                dmc.Text("IMPACT PERCENTILES", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}, mb="xs"),
                                html.Div(id=f"analysis-{metric_prefix}-percentiles")
                            ],
                            p="md",
                            shadow="xs"
                        )
                    ],
                    span=6
                )
            ],
            gutter="md",
            mb="md"
        ),
    ]

# Selector section
selectors_section = dmc.Paper([
    dmc.Group(
        [
            dmc.Stack(
                [
                    dmc.Text("Country", size="xs", fw=600, c="dimmed"),
                    dmc.Select(
                        id="analysis-country-select",
                        placeholder="Select country...",
                        data=COUNTRY_OPTIONS,
                        value=DEFAULT_COUNTRY,
                        style={"width": "100%", "minWidth": "150px"}
                    )
                ],
                gap="xs",
                style={"flex": "1"}
            ),
            dmc.Stack(
                [
                    dmc.Text("Date", size="xs", fw=600, c="dimmed"),
                    dmc.Select(
                        id="analysis-forecast-date",
                        placeholder="Date...",
                        data=[],
                        value=None,
                        style={"width": "100%", "minWidth": "120px"}
                    )
                ],
                gap="xs",
                style={"flex": "1"}
            ),
            dmc.Stack(
                [
                    dmc.Text("Time", size="xs", fw=600, c="dimmed"),
                    dmc.Select(
                        id="analysis-forecast-time",
                        placeholder="Time...",
                        data=[],
                        value=None,
                        style={"width": "100%", "minWidth": "120px"}
                    )
                ],
                gap="xs",
                style={"flex": "1"}
            ),
            dmc.Stack(
                [
                    dmc.Text("Hurricane", size="xs", fw=600, c="dimmed"),
                    dmc.Select(
                        id="analysis-storm-select",
                        placeholder="Hurricane...",
                        data=[],
                        value=None,
                        style={"width": "100%", "minWidth": "120px"}
                    )
                ],
                gap="xs",
                style={"flex": "1"}
            ),
        ],
        gap="md",
        grow=True,
        align="flex-start"
    )
],
p="sm",
shadow="xs",
mb="0"
)


# Impact Summary Section - Create as function to return fresh instances for each tab with unique IDs
def create_impact_summary(tab_suffix=""):
    """Create a fresh instance of the impact summary table for each tab with unique IDs per tab"""
    suffix = f"-{tab_suffix}" if tab_suffix else ""
    return dmc.Paper([
                    dmc.Group([
                        dmc.Text("IMPACT SUMMARY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                    ], justify="flex-start", gap="sm", mb="sm"),
                    
                    # Impact Summary Table
                    dmc.Table(
                        [
                            dmc.TableThead([
                                dmc.TableTr([
                                    dmc.TableTh([
                                        dmc.Text("Metric", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                        dmc.Text("at Risk", style={"margin": 0, "fontSize": "0.85em", "fontWeight": 400, "color": "#6c757d"}, c="dimmed")
                                    ], style={"fontWeight": 700, "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "height": "60px", "verticalAlign": "top", "paddingTop": "8px"}),
                                    dmc.TableTh([
                                        dmc.Text("DET", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                        dmc.Badge("#51", id=f"analysis-deterministic-badge{suffix}", size="xs", color="blue", variant="light", style={"marginTop": "2px"})
                                    ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"}),
                                    dmc.TableTh("Expected", style={"fontWeight": 700, "textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "paddingTop": "8px", "height": "60px", "verticalAlign": "top"}),
                                    dmc.TableTh([
                                        dmc.Text("Worst", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                        dmc.Badge("Member", id=f"analysis-high-impact-badge{suffix}", size="xs", color="red", variant="light", style={"marginTop": "2px"})
                                    ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"})
                                ])
                            ]),
                            dmc.TableTbody([
                                dmc.TableTr([
                                    dmc.TableTd("Population", style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-population-count-low{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-population-count-probabilistic{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-population-count-high{suffix}", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd(dmc.Group([dmc.Text(size="xs", c="dimmed"), dmc.Text("Age 5-15", style={"fontStyle": "italic", "fontSize": "0.95em"})], gap=0), style={"fontWeight": 500, "paddingLeft": "15px"}),
                                    dmc.TableTd("N/A", id=f"analysis-children-affected-low{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-children-affected-probabilistic{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-children-affected-high{suffix}", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd(dmc.Group([dmc.Text(size="xs", c="dimmed"), dmc.Text("Age 0-5", style={"fontStyle": "italic", "fontSize": "0.95em"})], gap=0), style={"fontWeight": 500, "paddingLeft": "15px"}),
                                    dmc.TableTd("N/A", id=f"analysis-infant-affected-low{suffix}",
                                                style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-infant-affected-probabilistic{suffix}",
                                                style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-infant-affected-high{suffix}",
                                                style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Schools", style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-schools-count-low{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-schools-count-probabilistic{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-schools-count-high{suffix}", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Health Centers", style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-health-count-low{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-health-count-probabilistic{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-health-count-high{suffix}", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd([
                                        html.Span("Built Surface m"),
                                        html.Sup("2"),
                                    ], style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-bsm2-count-low{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-bsm2-count-probabilistic{suffix}", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id=f"analysis-bsm2-count-high{suffix}", style={"textAlign": "center", "fontWeight": 500})
                                ])
                            ])
                        ],
                        striped=True,
                        highlightOnHover=True,
                        withTableBorder=True,
                        withColumnBorders=True
                    )
                ],
                p="sm",
                shadow="xs",
                style={"marginBottom": "12px"}
            )

# Threshold options constant to avoid duplication
THRESHOLD_OPTIONS = [
    {"value": "34", "label": "34kt - Tropical storm"},
    {"value": "40", "label": "40kt - Strong tropical"},
    {"value": "50", "label": "50kt - Very strong tropical"},
    {"value": "64", "label": "64kt - Cat 1 hurricane"},
    {"value": "83", "label": "83kt - Cat 2 hurricane"},
    {"value": "96", "label": "96kt - Cat 3 hurricane"},
    {"value": "113", "label": "113kt - Cat 4 hurricane"},
    {"value": "137", "label": "137kt - Cat 5 hurricane"},
]

def make_single_page_layout():
    """Create the forecast analysis dashboard layout"""
    return dmc.Stack(
        [
            # Selectors at top
            selectors_section,
            
            # Main content area
            dmc.Paper(
                [
                    # Store component to track selected wind threshold from nested tabs
                    dcc.Store(id="analysis-wind-threshold-store", data="34"),
                    
                    dmc.Group([
                        DashIconify(icon="carbon:chart-box-plot", width=24),
                        dmc.Title("PROBABILISTIC FORECAST ANALYSIS", order=4)
                    ], gap="sm", mb="md"),
                    
                    # Tabs for individual metrics
                    dmc.Tabs(
                        [
                            dmc.TabsList(
                                [
                                    dmc.TabsTab("Population", value="population"),
                                    dmc.TabsTab("Age 5-15", value="children"),
                                    dmc.TabsTab("Age 0-5", value="infants"),
                                    dmc.TabsTab("Schools", value="schools"),
                                    dmc.TabsTab("Health Centers", value="health"),
                                    dmc.TabsTab("Built Surface", value="built_surface"),
                                ]
                            ),
                            # Population Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("population", "Population"),
                                value="population"
                            ),
                            # Children Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("children", "Age 5-15"),
                                value="children"
                            ),
                            # Infants Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("infants", "Age 0-5"),
                                value="infants"
                            ),
                            # Schools Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("schools", "Schools"),
                                value="schools"
                            ),
                            # Health Centers Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("health", "Health Centers"),
                                value="health"
                            ),
                            # Built Surface Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("built-surface", "Built Surface (m²)"),
                                value="built_surface"
                            )
                        ],
                        id="analysis-metrics-tabs",
                        value="population",
                        mb="md"
                    ),
                    
                    # Status message
                    html.Div(id="analysis-plots-status")
                ],
                p="md",
                shadow="sm"
            )
        ],
        gap="xs",
        style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto", "padding": "4px", "paddingTop": "2px"}
    )

def make_single_page_appshell():
    """Create appshell with custom header using existing footer and appshell structure"""
    
    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_custom_header(), px=15, zIndex=2000),
            dmc.AppShellMain(
                make_single_page_layout(),
            ),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id="analysis-shell",
        header={"height": "67"},
        padding=0,
        footer={"height": "80"},
    )

# Use the single-page appshell
layout = make_single_page_appshell()

# =============================================================================
# CALLBACKS FOR HURRICANE SELECTORS
# =============================================================================

# Pre-compute date options for efficiency
if unique_dates:
    date_options = []
    for date_str in unique_dates:
        date_obj = pd.to_datetime(date_str).date()
        date_options.append({
            "value": date_str,
            "label": date_obj.strftime('%b %d, %Y')
        })
    default_date = date_options[0]['value'] if date_options else None
else:
    date_options = []
    default_date = None

@callback(
    Output("analysis-forecast-date", "data"),
    Output("analysis-forecast-date", "value"),
    Input("analysis-country-select", "value"),
    prevent_initial_call=False
)
def update_forecast_dates(country):
    """Get available forecast dates from pre-loaded data and set most recent as default"""
    return date_options, default_date

@callback(
    Output("analysis-forecast-time", "data"),
    Output("analysis-forecast-time", "value", allow_duplicate=True),
    Input("analysis-forecast-date", "value"),
    prevent_initial_call='initial_duplicate'
)
def update_forecast_times(selected_date):
    """Get available forecast times for selected date, with most recent time as default"""
    all_possible_times = ["00:00", "06:00", "12:00", "18:00"]
    
    if not selected_date or metadata_df.empty:
        return [{"value": t, "label": f"{t} UTC", "disabled": True} for t in all_possible_times], "00:00"
    
    # Get available times for selected date (metadata_df already has DATE and TIME columns)
    available_times = sorted(metadata_df[metadata_df['DATE'] == selected_date]['TIME'].unique())
    
    # Create options with all possible times, marking unavailable ones as disabled
    time_options = [
        {"value": t, "label": f"{t} UTC", "disabled": t not in available_times}
        for t in all_possible_times
    ]
    
    # Set default to most recent available time
    default_time = available_times[-1] if available_times else "00:00"
    return time_options, default_time

@callback(
    Output("analysis-storm-select", "data"),
    Output("analysis-storm-select", "value", allow_duplicate=True),
    [Input("analysis-country-select", "value"),
     Input("analysis-forecast-date", "value"),
     Input("analysis-forecast-time", "value")],
    prevent_initial_call='initial_duplicate'
)
def update_storm_options(country, forecast_date, forecast_time):
    """Update available storms based on country, date, and time selection - show only available storms and set most recent as default"""
    if not forecast_date or not forecast_time or metadata_df.empty:
        return [], None
    
    # Get available storms for selected date and time (metadata_df already has DATE and TIME columns)
    available_storms = sorted(
        metadata_df[(metadata_df['DATE'] == forecast_date) & (metadata_df['TIME'] == forecast_time)]['TRACK_ID'].unique()
    )
    
    # Create options and set default to most recent storm
    storm_options = [{"value": storm, "label": storm} for storm in available_storms]
    default_storm = available_storms[-1] if available_storms else None
    return storm_options, default_storm

# Callback to update wind threshold store when any threshold selector changes
@callback(
    Output("analysis-wind-threshold-store", "data"),
    [Input("analysis-population-threshold-selector", "value"),
     Input("analysis-children-threshold-selector", "value"),
     Input("analysis-infants-threshold-selector", "value"),
     Input("analysis-schools-threshold-selector", "value"),
     Input("analysis-health-threshold-selector", "value"),
     Input("analysis-built-surface-threshold-selector", "value")],
    prevent_initial_call=False
)
def update_wind_threshold_store(pop_val, children_val, infants_val, schools_val, health_val, built_surface_val):
    """Update store when any threshold selector changes"""
    # Get the active input (triggered one)
    ctx = dash.callback_context
    if not ctx.triggered:
        return "34" if not pop_val else pop_val
    
    # Extract threshold from the triggered input
    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
    triggered_value = ctx.triggered[0]["value"]
    
    # All selectors are SegmentedControls now, return the value directly
    if triggered_id.endswith("-threshold-selector"):
        return triggered_value if triggered_value else "34"
    
    # Default fallback
    return pop_val if pop_val else "34"

@callback(
    [Output("analysis-population-threshold-selector", "data"),
     Output("analysis-population-threshold-selector", "value", allow_duplicate=True),
     Output("analysis-children-threshold-selector", "data"),
     Output("analysis-children-threshold-selector", "value", allow_duplicate=True),
     Output("analysis-infants-threshold-selector", "data"),
     Output("analysis-infants-threshold-selector", "value", allow_duplicate=True),
     Output("analysis-schools-threshold-selector", "data"),
     Output("analysis-schools-threshold-selector", "value", allow_duplicate=True),
     Output("analysis-health-threshold-selector", "data"),
     Output("analysis-health-threshold-selector", "value", allow_duplicate=True),
     Output("analysis-built-surface-threshold-selector", "data"),
     Output("analysis-built-surface-threshold-selector", "value", allow_duplicate=True)],
    [Input("analysis-storm-select", "value"),
     Input("analysis-forecast-date", "value"),
     Input("analysis-forecast-time", "value")],
    [State("analysis-population-threshold-selector", "value")],
    prevent_initial_call='initial_duplicate'
)
def update_threshold_selectors(storm, date, time, current_threshold):
    """Update all threshold SegmentedControls based on available thresholds from Snowflake"""
    
    if not all([storm, date, time]):
        # Return all thresholds enabled if no storm selected
        return (
            THRESHOLD_OPTIONS, "34",  # population
            THRESHOLD_OPTIONS, "34",  # children
            THRESHOLD_OPTIONS, "34",  # infants
            THRESHOLD_OPTIONS, "34",  # schools
            THRESHOLD_OPTIONS, "34",  # health
            THRESHOLD_OPTIONS, "34"   # built-surface
        )
    
    try:
        # Get available wind thresholds from Snowflake
        forecast_datetime = f"{date} {time}:00"
        available_thresholds = get_available_wind_thresholds(storm, forecast_datetime)
        
        # Filter options to only include available thresholds
        filtered_options = [opt for opt in THRESHOLD_OPTIONS if opt["value"] in available_thresholds]
        
        # Set default threshold - preserve user selection if still available, otherwise prefer 50kt or first available
        if not filtered_options:
            filtered_options = THRESHOLD_OPTIONS
            default_threshold = "34"
        elif current_threshold and current_threshold in available_thresholds:
            default_threshold = current_threshold
        elif "50" in available_thresholds:
            default_threshold = "50"
        else:
            default_threshold = str(sorted([int(t) for t in available_thresholds])[0])
        
        # Return same options and default for all selectors (they should stay in sync)
        return (
            filtered_options, default_threshold,  # population
            filtered_options, default_threshold,  # children
            filtered_options, default_threshold,  # infants
            filtered_options, default_threshold,  # schools
            filtered_options, default_threshold,  # health
            filtered_options, default_threshold   # built-surface
        )
        
    except Exception as e:
        print(f"Analysis: Error getting wind threshold options: {e}")
        return (
            THRESHOLD_OPTIONS, "34",  # population
            THRESHOLD_OPTIONS, "34",  # children
            THRESHOLD_OPTIONS, "34",  # infants
            THRESHOLD_OPTIONS, "34",  # schools
            THRESHOLD_OPTIONS, "34",  # health
            THRESHOLD_OPTIONS, "34"   # built-surface
        )

# =============================================================================
# CALLBACK FOR IMPACT SUMMARY
# =============================================================================

@callback(
    # Outputs for population tab
    [Output("analysis-population-count-low-population", "children"),
     Output("analysis-population-count-probabilistic-population", "children"),
     Output("analysis-population-count-high-population", "children"),
     Output("analysis-children-affected-low-population", "children"),
     Output("analysis-children-affected-probabilistic-population", "children"),
     Output("analysis-children-affected-high-population", "children"),
     Output("analysis-infant-affected-low-population", "children"),
     Output("analysis-infant-affected-probabilistic-population", "children"),
     Output("analysis-infant-affected-high-population", "children"),
     Output("analysis-schools-count-low-population", "children"),
     Output("analysis-schools-count-probabilistic-population", "children"),
     Output("analysis-schools-count-high-population", "children"),
     Output("analysis-health-count-low-population", "children"),
     Output("analysis-health-count-probabilistic-population", "children"),
     Output("analysis-health-count-high-population", "children"),
     Output("analysis-bsm2-count-low-population", "children"),
     Output("analysis-bsm2-count-probabilistic-population", "children"),
     Output("analysis-bsm2-count-high-population", "children"),
     Output("analysis-high-impact-badge-population", "children"),
     # Outputs for children tab
     Output("analysis-population-count-low-children", "children"),
     Output("analysis-population-count-probabilistic-children", "children"),
     Output("analysis-population-count-high-children", "children"),
     Output("analysis-children-affected-low-children", "children"),
     Output("analysis-children-affected-probabilistic-children", "children"),
     Output("analysis-children-affected-high-children", "children"),
     Output("analysis-infant-affected-low-children", "children"),
     Output("analysis-infant-affected-probabilistic-children", "children"),
     Output("analysis-infant-affected-high-children", "children"),
     Output("analysis-schools-count-low-children", "children"),
     Output("analysis-schools-count-probabilistic-children", "children"),
     Output("analysis-schools-count-high-children", "children"),
     Output("analysis-health-count-low-children", "children"),
     Output("analysis-health-count-probabilistic-children", "children"),
     Output("analysis-health-count-high-children", "children"),
     Output("analysis-bsm2-count-low-children", "children"),
     Output("analysis-bsm2-count-probabilistic-children", "children"),
     Output("analysis-bsm2-count-high-children", "children"),
     Output("analysis-high-impact-badge-children", "children"),
     # Outputs for infants tab
     Output("analysis-population-count-low-infants", "children"),
     Output("analysis-population-count-probabilistic-infants", "children"),
     Output("analysis-population-count-high-infants", "children"),
     Output("analysis-children-affected-low-infants", "children"),
     Output("analysis-children-affected-probabilistic-infants", "children"),
     Output("analysis-children-affected-high-infants", "children"),
     Output("analysis-infant-affected-low-infants", "children"),
     Output("analysis-infant-affected-probabilistic-infants", "children"),
     Output("analysis-infant-affected-high-infants", "children"),
     Output("analysis-schools-count-low-infants", "children"),
     Output("analysis-schools-count-probabilistic-infants", "children"),
     Output("analysis-schools-count-high-infants", "children"),
     Output("analysis-health-count-low-infants", "children"),
     Output("analysis-health-count-probabilistic-infants", "children"),
     Output("analysis-health-count-high-infants", "children"),
     Output("analysis-bsm2-count-low-infants", "children"),
     Output("analysis-bsm2-count-probabilistic-infants", "children"),
     Output("analysis-bsm2-count-high-infants", "children"),
     Output("analysis-high-impact-badge-infants", "children"),
     # Outputs for schools tab
     Output("analysis-population-count-low-schools", "children"),
     Output("analysis-population-count-probabilistic-schools", "children"),
     Output("analysis-population-count-high-schools", "children"),
     Output("analysis-children-affected-low-schools", "children"),
     Output("analysis-children-affected-probabilistic-schools", "children"),
     Output("analysis-children-affected-high-schools", "children"),
     Output("analysis-infant-affected-low-schools", "children"),
     Output("analysis-infant-affected-probabilistic-schools", "children"),
     Output("analysis-infant-affected-high-schools", "children"),
     Output("analysis-schools-count-low-schools", "children"),
     Output("analysis-schools-count-probabilistic-schools", "children"),
     Output("analysis-schools-count-high-schools", "children"),
     Output("analysis-health-count-low-schools", "children"),
     Output("analysis-health-count-probabilistic-schools", "children"),
     Output("analysis-health-count-high-schools", "children"),
     Output("analysis-bsm2-count-low-schools", "children"),
     Output("analysis-bsm2-count-probabilistic-schools", "children"),
     Output("analysis-bsm2-count-high-schools", "children"),
     Output("analysis-high-impact-badge-schools", "children"),
     # Outputs for health tab
     Output("analysis-population-count-low-health", "children"),
     Output("analysis-population-count-probabilistic-health", "children"),
     Output("analysis-population-count-high-health", "children"),
     Output("analysis-children-affected-low-health", "children"),
     Output("analysis-children-affected-probabilistic-health", "children"),
     Output("analysis-children-affected-high-health", "children"),
     Output("analysis-infant-affected-low-health", "children"),
     Output("analysis-infant-affected-probabilistic-health", "children"),
     Output("analysis-infant-affected-high-health", "children"),
     Output("analysis-schools-count-low-health", "children"),
     Output("analysis-schools-count-probabilistic-health", "children"),
     Output("analysis-schools-count-high-health", "children"),
     Output("analysis-health-count-low-health", "children"),
     Output("analysis-health-count-probabilistic-health", "children"),
     Output("analysis-health-count-high-health", "children"),
     Output("analysis-bsm2-count-low-health", "children"),
     Output("analysis-bsm2-count-probabilistic-health", "children"),
     Output("analysis-bsm2-count-high-health", "children"),
     Output("analysis-high-impact-badge-health", "children"),
     # Outputs for built-surface tab
     Output("analysis-population-count-low-built-surface", "children"),
     Output("analysis-population-count-probabilistic-built-surface", "children"),
     Output("analysis-population-count-high-built-surface", "children"),
     Output("analysis-children-affected-low-built-surface", "children"),
     Output("analysis-children-affected-probabilistic-built-surface", "children"),
     Output("analysis-children-affected-high-built-surface", "children"),
     Output("analysis-infant-affected-low-built-surface", "children"),
     Output("analysis-infant-affected-probabilistic-built-surface", "children"),
     Output("analysis-infant-affected-high-built-surface", "children"),
     Output("analysis-schools-count-low-built-surface", "children"),
     Output("analysis-schools-count-probabilistic-built-surface", "children"),
     Output("analysis-schools-count-high-built-surface", "children"),
     Output("analysis-health-count-low-built-surface", "children"),
     Output("analysis-health-count-probabilistic-built-surface", "children"),
     Output("analysis-health-count-high-built-surface", "children"),
     Output("analysis-bsm2-count-low-built-surface", "children"),
     Output("analysis-bsm2-count-probabilistic-built-surface", "children"),
     Output("analysis-bsm2-count-high-built-surface", "children"),
     Output("analysis-high-impact-badge-built-surface", "children")],
   [Input("analysis-storm-select", "value"),
    Input("analysis-wind-threshold-store", "data"),
    Input("analysis-population-threshold-selector", "value"),
    Input("analysis-children-threshold-selector", "value"),
    Input("analysis-infants-threshold-selector", "value"),
    Input("analysis-schools-threshold-selector", "value"),
    Input("analysis-health-threshold-selector", "value"),
    Input("analysis-built-surface-threshold-selector", "value"),
    Input("analysis-country-select", "value"),
    Input("analysis-forecast-date", "value"),
    Input("analysis-forecast-time", "value"),
    Input("analysis-metrics-tabs", "value")],  # Trigger on tab switch to force update
    prevent_initial_call=True
)
def update_impact_metrics(storm, wind_threshold_store, pop_thresh, children_thresh, infants_thresh, schools_thresh, health_thresh, built_surface_thresh, country, forecast_date, forecast_time, active_tab):
    """Update impact metrics for all three scenarios when Load Impact Summary button is clicked"""
    
    # Use the store value as primary
    wind_threshold = wind_threshold_store or pop_thresh or children_thresh or infants_thresh or schools_thresh or health_thresh or built_surface_thresh or "34"
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        na_values = ("N/A",) * 19  # 18 metrics + 1 badge = 19 outputs per tab
        return na_values * 6  # Return for all 6 tabs
    
    # Calculate probabilistic impact metrics
    try:
        # Construct the filename for the tiles impact view
        date_str = forecast_date.replace('-', '')  # Convert "2025-10-15" to "20251015"
        time_str = forecast_time.replace(':', '')  # Convert "00:00" to "0000"
        forecast_datetime = f"{date_str}{time_str}00"  # Add seconds: "20251015000000"
        
        filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}_{ZOOM_LEVEL}.csv"
        filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "mercator_views", filename)
        
        print(f"Analysis Impact metrics: Looking for file {filename}")
        
        # Initialize all scenario results
        low_results = {"children": "N/A", "infant": "N/A", "schools": "N/A", "health": "N/A", "population": "N/A", "built_surface_m2":"N/A"}
        probabilistic_results = {"children": "N/A", "infant": "N/A", "schools": "N/A", "health": "N/A", "population": "N/A", "built_surface_m2":"N/A"}
        high_results = {"children": "N/A", "infant": "N/A", "schools": "N/A", "health": "N/A", "population": "N/A", "built_surface_m2":"N/A"}
        
        # Initialize member badge
        high_member_badge = "N/A"
        
        if giga_store.file_exists(filepath):
            try:
                df = read_dataset(giga_store, filepath)
                
                # Calculate PROBABILISTIC scenario (from tiles data)
                if 'E_school_age_population' in df.columns and not df['E_school_age_population'].isna().all():
                    probabilistic_results["children"] = df['E_school_age_population'].sum()
                else:
                    probabilistic_results["children"] = "N/A"

                if 'E_infant_population' in df.columns and not df['E_infant_population'].isna().all():
                    probabilistic_results["infant"] = df['E_infant_population'].sum()
                else:
                    probabilistic_results["infant"] = "N/A"
                
                probabilistic_results["schools"] = df['E_num_schools'].sum() if 'E_num_schools' in df.columns else "N/A"
                probabilistic_results["health"] = df['E_num_hcs'].sum() if 'E_num_hcs' in df.columns else "N/A"
                probabilistic_results["population"] = df['E_population'].sum() if 'E_population' in df.columns else "N/A"
                probabilistic_results["built_surface_m2"] = df['E_built_surface_m2'].sum() if 'E_built_surface_m2' in df.columns else "N/A"
                
                # Calculate DETERMINISTIC (member 51) and HIGH scenarios (from track data)
                tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
                
                if giga_store.file_exists(tracks_filepath):
                    gdf_tracks = read_dataset(giga_store, tracks_filepath)
                    
                    if 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
                        # Use deterministic member 51 (always member 51)
                        deterministic_member = 51
                        # Find ensemble member with highest impact
                        member_totals = gdf_tracks.groupby('zone_id')['severity_population'].sum()
                        high_impact_member = member_totals.idxmax()
                        
                        # Set member badge text (deterministic is always #51, no need to update)
                        high_member_badge = f"#{high_impact_member}"
                        
                        # Get deterministic scenario data (member 51)
                        low_scenario_data = gdf_tracks[gdf_tracks['zone_id'] == deterministic_member]
                        high_scenario_data = gdf_tracks[gdf_tracks['zone_id'] == high_impact_member]
                        
                        # Check if health center data is available for this time slot
                        hc_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                        hc_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', hc_filename)
                        hc_data_available = giga_store.file_exists(hc_filepath)
                        
                        # DETERMINISTIC scenario (member 51)
                        if not low_scenario_data.empty:
                            low_results["children"] = low_scenario_data[
                                'severity_school_age_population'].sum() if 'severity_school_age_population' in low_scenario_data.columns else "N/A"
                            low_results["infant"] = low_scenario_data[
                                'severity_infant_population'].sum() if 'severity_infant_population' in low_scenario_data.columns else "N/A"
                            low_results["schools"] = low_scenario_data['severity_schools'].sum() if 'severity_schools' in low_scenario_data.columns else "N/A"
                            low_results["population"] = low_scenario_data['severity_population'].sum() if 'severity_population' in low_scenario_data.columns else "N/A"
                            low_results["health"] = low_scenario_data['severity_hcs'].sum() if ('severity_hcs' in low_scenario_data.columns and hc_data_available) else "N/A"
                            low_results["built_surface_m2"] = low_scenario_data['severity_built_surface_m2'].sum() if ('severity_built_surface_m2' in low_scenario_data.columns and hc_data_available) else "N/A"
                        else:
                            # Member 51 not found in data (badge will still show #51 as static value)
                            pass

                        # HIGH scenario
                        high_results["children"] = high_scenario_data[
                            'severity_school_age_population'].sum() if 'severity_school_age_population' in high_scenario_data.columns else "N/A"
                        high_results["infant"] = high_scenario_data[
                            'severity_infant_population'].sum() if 'severity_infant_population' in high_scenario_data.columns else "N/A"
                        high_results["schools"] = high_scenario_data['severity_schools'].sum() if 'severity_schools' in high_scenario_data.columns else "N/A"
                        high_results["population"] = high_scenario_data['severity_population'].sum() if 'severity_population' in high_scenario_data.columns else "N/A"
                        high_results["health"] = high_scenario_data['severity_hcs'].sum() if ('severity_hcs' in high_scenario_data.columns and hc_data_available) else "N/A"
                        high_results["built_surface_m2"] = high_scenario_data['severity_built_surface_m2'].sum() if ('severity_built_surface_m2' in high_scenario_data.columns and hc_data_available) else "N/A"
                
            except Exception as e:
                print(f"Analysis Impact metrics: Error reading file {filename}: {e}")
        else:
            print(f"Analysis Impact metrics: File not found {filename}")
        
        # Format results
        def format_value(value):
            return str(value) if isinstance(value, str) else f"{value:,.0f}"
        
        # Create the single set of values (19 outputs per tab)
        tab_values = (
            # Population count
            format_value(low_results["population"]),
            format_value(probabilistic_results["population"]),
            format_value(high_results["population"]),
            # Children affected (part of population)
            format_value(low_results["children"]),
            format_value(probabilistic_results["children"]),
            format_value(high_results["children"]),
            # Infants affected (part of population)
            format_value(low_results["infant"]),
            format_value(probabilistic_results["infant"]),
            format_value(high_results["infant"]),
            # Schools count
            format_value(low_results["schools"]),
            format_value(probabilistic_results["schools"]),
            format_value(high_results["schools"]),
            # Health count
            format_value(low_results["health"]),
            format_value(probabilistic_results["health"]),
            format_value(high_results["health"]),
            # Built Surface m2
            format_value(low_results["built_surface_m2"]),
            format_value(probabilistic_results["built_surface_m2"]),
            format_value(high_results["built_surface_m2"]),
            # Member badge (deterministic badge is static #51, doesn't need updating)
            high_member_badge
        )
        
        # Return the same values for all 6 tabs (6 * 19 = 114 outputs)
        return tab_values * 6
            
    except Exception as e:
        print(f"Analysis Impact metrics: Error updating metrics: {e}")
        na_values = ("N/A",) * 19  # 18 metrics + 1 badge = 19 outputs per tab
        return na_values * 6  # Return for all 6 tabs


# =============================================================================
# CALLBACK FOR BOX PLOTS - IMPACT DISTRIBUTION BY ENSEMBLE MEMBER
# =============================================================================

@callback(
    [Output("analysis-population-plot", "figure"),
     Output("analysis-population-exceedance", "figure"),
     Output("analysis-population-percentiles", "children"),
     Output("analysis-children-plot", "figure"),
     Output("analysis-children-exceedance", "figure"),
     Output("analysis-children-percentiles", "children"),
     Output("analysis-infants-plot", "figure"),
     Output("analysis-infants-exceedance", "figure"),
     Output("analysis-infants-percentiles", "children"),
     Output("analysis-schools-plot", "figure"),
     Output("analysis-schools-exceedance", "figure"),
     Output("analysis-schools-percentiles", "children"),
     Output("analysis-health-plot", "figure"),
     Output("analysis-health-exceedance", "figure"),
     Output("analysis-health-percentiles", "children"),
     Output("analysis-built-surface-plot", "figure"),
     Output("analysis-built-surface-exceedance", "figure"),
     Output("analysis-built-surface-percentiles", "children"),
     Output("analysis-plots-status", "children")],
    [Input("analysis-storm-select", "value"),
     Input("analysis-wind-threshold-store", "data"),
     Input("analysis-country-select", "value"),
     Input("analysis-forecast-date", "value"),
     Input("analysis-forecast-time", "value")],
    prevent_initial_call=False
)
def update_box_plots(storm, wind_threshold, country, forecast_date, forecast_time):
    """Generate horizontal box plot showing population impact distribution across ensemble members"""
    
    # Create empty figure as default
    empty_fig = go.Figure()
    empty_fig.add_annotation(
        text="No data available. Please select all fields.",
        xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=14, color="gray")
    )
    empty_fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=20, b=20)
    )
    
    # Create empty children components for spread metrics and percentiles
    empty_children = html.Div("No data available", style={"padding": "10px", "color": "gray", "fontSize": "12px"})
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        status_msg = dmc.Alert(
            "Please select all required fields to view plots.",
            title="Missing Selections",
            color="orange",
            variant="light"
        )
        # Return: 6 metrics * (2 figures + 1 children) = 18 outputs
        # Pattern: box, exceedance (figures), percentiles (children)
        return (
            empty_fig, empty_fig, empty_children,  # population
            empty_fig, empty_fig, empty_children,  # children
            empty_fig, empty_fig, empty_children,  # infants
            empty_fig, empty_fig, empty_children,  # schools
            empty_fig, empty_fig, empty_children,  # health
            empty_fig, empty_fig, empty_children,  # built_surface
            status_msg
        )
    
    try:
        # Initialize higher threshold data (will be populated if available)
        higher_threshold_data = {}
        
        # Construct the filename for track data
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime = f"{date_str}{time_str}00"
        
        tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
        
        if not giga_store.file_exists(tracks_filepath):
            status_msg = dmc.Alert(
                "Track data file not found. Please ensure the storm data has been processed.",
                title="Data Not Found",
                color="orange",
                variant="light"
            )
            return (
                empty_fig, empty_fig, empty_children,  # population
                empty_fig, empty_fig, empty_children,  # children
                empty_fig, empty_fig, empty_children,  # infants
                empty_fig, empty_fig, empty_children,  # schools
                empty_fig, empty_fig, empty_children,  # health
                empty_fig, empty_fig, empty_children,  # built_surface
                status_msg
            )
        
        # Load track data
        gdf_tracks = read_dataset(giga_store, tracks_filepath)
        
        if 'zone_id' not in gdf_tracks.columns:
            status_msg = dmc.Alert(
                "Track data does not contain ensemble member information.",
                title="Invalid Data",
                color="orange",
                variant="light"
            )
            return (
                empty_fig, empty_fig, empty_children,  # population
                empty_fig, empty_fig, empty_children,  # children
                empty_fig, empty_fig, empty_children,  # infants
                empty_fig, empty_fig, empty_children,  # schools
                empty_fig, empty_fig, empty_children,  # health
                empty_fig, empty_fig, empty_children,  # built_surface
                status_msg
            )
        
        # Check if health center data is available
        hc_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
        hc_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', hc_filename)
        hc_data_available = giga_store.file_exists(hc_filepath)
        
        # Get available wind thresholds for this storm
        try:
            forecast_datetime_str = f"{forecast_date} {forecast_time}:00"
            available_wind_thresholds = get_available_wind_thresholds(storm, forecast_datetime_str)
        except Exception as e:
            print(f"Error getting available wind thresholds: {e}")
            available_wind_thresholds = []
        
        # Calculate totals per ensemble member for all metrics
        member_data = []
        unique_members = gdf_tracks['zone_id'].unique()
        for member_id in unique_members:
            member_data_subset = gdf_tracks[gdf_tracks['zone_id'] == member_id]
            
            member_row = {'member': member_id}
            
            # Population metrics
            member_row['population'] = member_data_subset['severity_population'].sum() if 'severity_population' in member_data_subset.columns else 0
            member_row['children'] = member_data_subset['severity_school_age_population'].sum() if 'severity_school_age_population' in member_data_subset.columns else 0
            member_row['infants'] = member_data_subset['severity_infant_population'].sum() if 'severity_infant_population' in member_data_subset.columns else 0
            
            # Infrastructure metrics
            member_row['schools'] = member_data_subset['severity_schools'].sum() if 'severity_schools' in member_data_subset.columns else 0
            member_row['health'] = member_data_subset['severity_hcs'].sum() if ('severity_hcs' in member_data_subset.columns and hc_data_available) else 0
            member_row['built_surface'] = member_data_subset['severity_built_surface_m2'].sum() if ('severity_built_surface_m2' in member_data_subset.columns and hc_data_available) else 0
            
            member_data.append(member_row)
        
        member_df = pd.DataFrame(member_data)
        
        # Load data for higher wind thresholds
        higher_threshold_data = {}  # Structure: {metric_name: {threshold: values_array}}
        if available_wind_thresholds and wind_threshold:
            current_thresh_int = int(wind_threshold)
            higher_thresholds = [t for t in available_wind_thresholds if t.isdigit() and int(t) > current_thresh_int]
            
            for higher_thresh in sorted(higher_thresholds, key=int):
                try:
                    higher_tracks_filename = f"{country}_{storm}_{forecast_datetime}_{higher_thresh}.parquet"
                    higher_tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', higher_tracks_filename)
                    
                    if giga_store.file_exists(higher_tracks_filepath):
                        higher_gdf_tracks = read_dataset(giga_store, higher_tracks_filepath)
                        
                        if 'zone_id' in higher_gdf_tracks.columns and len(higher_gdf_tracks) > 0:
                            # Calculate totals per ensemble member for higher threshold
                            higher_member_data = []
                            for member_id in higher_gdf_tracks['zone_id'].unique():
                                higher_member_subset = higher_gdf_tracks[higher_gdf_tracks['zone_id'] == member_id]
                                
                                higher_member_row = {'member': member_id}
                                higher_member_row['population'] = higher_member_subset['severity_population'].sum() if 'severity_population' in higher_member_subset.columns else 0
                                higher_member_row['children'] = higher_member_subset['severity_school_age_population'].sum() if 'severity_school_age_population' in higher_member_subset.columns else 0
                                higher_member_row['infants'] = higher_member_subset['severity_infant_population'].sum() if 'severity_infant_population' in higher_member_subset.columns else 0
                                higher_member_row['schools'] = higher_member_subset['severity_schools'].sum() if 'severity_schools' in higher_member_subset.columns else 0
                                higher_member_row['health'] = higher_member_subset['severity_hcs'].sum() if ('severity_hcs' in higher_member_subset.columns and hc_data_available) else 0
                                higher_member_row['built_surface'] = higher_member_subset['severity_built_surface_m2'].sum() if ('severity_built_surface_m2' in higher_member_subset.columns and hc_data_available) else 0
                                
                                higher_member_data.append(higher_member_row)
                            
                            higher_member_df = pd.DataFrame(higher_member_data)
                            
                            # Store values for each metric
                            metric_names = ['population', 'children', 'infants', 'schools', 'health', 'built_surface']
                            for metric in metric_names:
                                if metric not in higher_threshold_data:
                                    higher_threshold_data[metric] = {}
                            
                            if not higher_member_df.empty:
                                for metric in metric_names:
                                    higher_threshold_data[metric][higher_thresh] = higher_member_df[metric].values
                except Exception as e:
                    print(f"Error loading higher threshold {higher_thresh}kt data: {e}")
                    continue
        
        if member_df.empty:
            status_msg = dmc.Alert(
                "No ensemble member data found.",
                title="No Data",
                color="orange",
                variant="light"
            )
            return (
                empty_fig, empty_fig, empty_children,  # population
                empty_fig, empty_fig, empty_children,  # children
                empty_fig, empty_fig, empty_children,  # infants
                empty_fig, empty_fig, empty_children,  # schools
                empty_fig, empty_fig, empty_children,  # health
                empty_fig, empty_fig, empty_children,  # built_surface
                status_msg
            )
        
        # Helper function to create box plot for a metric
        def create_box_plot(values, member_ids, metric_name, color, x_axis_title):
            fig = go.Figure()
            
            # Box plot without points (just the box)
            fillcolor_rgba = hex_to_rgba(color, 0.2)
            
            fig.add_trace(go.Box(
                x=values,
                y=[0] * len(values),  # Position box at y=0
                name=metric_name,
                fillcolor=fillcolor_rgba,
                line=dict(color=color, width=2),
                boxpoints=False,  # Don't show points on box plot
                orientation='h',
                notched=False
            ))
            
            # Add all points manually with controlled jitter so Member 51 is in the same distribution
            import random
            random.seed(42)  # Fixed seed for reproducible jitter
            
            # Prepare data for scatter plot
            x_points = []
            y_points = []
            point_colors = []
            point_sizes = []
            point_line_widths = []
            custom_data = []
            
            # Position points to the side of the box (not overlapping)
            base_y = -1  # Position to the left of the box
            
            for idx, (val, mid) in enumerate(zip(values, member_ids)):
                x_points.append(val)
                # Create jittered y position (reproducible with seed, smaller range)
                random.seed(42 + idx)  # Different seed per point for reproducibility
                jitter_y = random.uniform(-0.15, 0.15)  # Smaller jitter range
                y_points.append(base_y + jitter_y)
                
                # Color and size based on member
                if mid == 51:
                    point_colors.append('#ff6b35')  # Orange for member 51
                    point_sizes.append(10)
                    point_line_widths.append(2)
                else:
                    point_colors.append(color)
                    point_sizes.append(6)
                    point_line_widths.append(1)
                
                custom_data.append(mid)
            
            # Add all points as scatter plot
            fig.add_trace(go.Scatter(
                x=x_points,
                y=y_points,
                mode='markers',
                marker=dict(
                    color=point_colors,
                    size=point_sizes,
                    opacity=0.7,
                    line=dict(color='white', width=point_line_widths)
                ),
                hovertemplate=f'<b>{metric_name}</b><br>Member #%{{customdata}}<br>Value: %{{x:,.0f}}<extra></extra>',
                customdata=custom_data,
                name=metric_name,
                showlegend=False
            ))
            
            fig.update_layout(
                xaxis=dict(
                    title=dict(text=x_axis_title, font=dict(size=11)),
                    tickformat=",.0f",
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True,
                    linecolor='rgba(200, 200, 200, 0.5)'
                ),
                yaxis=dict(
                    showticklabels=False,
                    showgrid=False,
                    showline=False,
                    range=[-2.0, 0.5]  # Range to accommodate box at 0 and points at -1.5
                ),
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                plot_bgcolor='rgba(250, 250, 250, 1)',
                paper_bgcolor='white',
                margin=dict(l=60, r=40, t=30, b=40),
                height=350
            )
            return fig
        
        # Helper function to convert hex color to rgba
        def hex_to_rgba(hex_color, alpha=0.2):
            r = int(hex_color[1:3], 16)
            g = int(hex_color[3:5], 16)
            b = int(hex_color[5:7], 16)
            return f'rgba({r}, {g}, {b}, {alpha})'
        
        # Cache member 51 index if present
        member_51_idx = None
        if 51 in member_df['member'].values:
            member_51_idx = member_df['member'].values.tolist().index(51)
        
        # Define metrics with their colors and labels
        metrics_config = {
            'population': {'color': '#1cabe2', 'label': 'Population Affected', 'x_label': 'Affected Population'},
            'children': {'color': '#3498db', 'label': 'Children Affected', 'x_label': 'Affected Children'},
            'infants': {'color': '#2980b9', 'label': 'Infants Affected', 'x_label': 'Affected Infants'},
            'schools': {'color': '#27ae60', 'label': 'Schools Affected', 'x_label': 'Number of Schools'},
            'health': {'color': '#e74c3c', 'label': 'Health Centers Affected', 'x_label': 'Number of Health Centers'},
            'built_surface': {'color': '#f39c12', 'label': 'Built Surface Affected', 'x_label': 'Built Surface (m²)'}
        }
        
        # Create plots for each metric
        pop_box = create_box_plot(
            member_df['population'].values,
            member_df['member'].values,
            'Population Affected',
            metrics_config['population']['color'],
            metrics_config['population']['x_label']
        )
        
        children_box = create_box_plot(
            member_df['children'].values,
            member_df['member'].values,
            'Children Affected',
            metrics_config['children']['color'],
            metrics_config['children']['x_label']
        )
        
        infants_box = create_box_plot(
            member_df['infants'].values,
            member_df['member'].values,
            'Infants Affected',
            metrics_config['infants']['color'],
            metrics_config['infants']['x_label']
        )
        
        schools_box = create_box_plot(
            member_df['schools'].values,
            member_df['member'].values,
            'Schools Affected',
            metrics_config['schools']['color'],
            metrics_config['schools']['x_label']
        )
        
        health_box = create_box_plot(
            member_df['health'].values,
            member_df['member'].values,
            'Health Centers Affected',
            metrics_config['health']['color'],
            metrics_config['health']['x_label']
        )
        
        built_surface_box = create_box_plot(
            member_df['built_surface'].values,
            member_df['member'].values,
            'Built Surface Affected',
            metrics_config['built_surface']['color'],
            metrics_config['built_surface']['x_label']
        )
        
        # Helper functions for uncertainty metrics
        def create_exceedance_plot(values, metric_name, color, x_axis_title, wind_threshold=None, available_thresholds=None, higher_threshold_data=None):
            """Create exceedance probability plot for a metric with wind threshold indicators and higher threshold overlays"""
            fig = go.Figure()
            
            if len(values) == 0:
                return fig
            
            # Create probability levels from 0% to 100%
            n_probabilities = 100
            probability_levels = np.linspace(0, 100, n_probabilities)
            
            # For each probability level, find the impact threshold where P(X > threshold) = probability
            # The threshold for probability p% is the (100-p)th percentile
            # (e.g., for 50% probability, we want the 50th percentile where 50% exceed it)
            impact_thresholds = []
            for prob in probability_levels:
                # Convert probability to percentile: percentile = 100 - probability
                percentile = 100 - prob
                threshold = np.percentile(values, percentile)
                impact_thresholds.append(threshold)
            
            fillcolor_rgba = hex_to_rgba(color, 0.2)
            
            # Add main curve (current wind threshold)
            fig.add_trace(go.Scatter(
                x=probability_levels,
                y=impact_thresholds,
                mode='lines',
                name=f"{metric_name} ({wind_threshold}kt)" if wind_threshold else metric_name,
                line=dict(color=color, width=2.5),
                fill='tozerox',  # Fill to zero on x-axis (left side)
                fillcolor=fillcolor_rgba,
                hovertemplate='<b>Probability:</b> %{x:.1f}%<br>Impact Threshold: %{y:,.0f}<extra></extra>'
            ))
            
            # Add curves for higher wind thresholds if available
            if higher_threshold_data:
                # Define colors for different thresholds (progressively darker/more intense)
                higher_threshold_colors = {
                    "40": "#5dade2",
                    "50": "#3498db",
                    "64": "#2980b9",
                    "83": "#1f618d",
                    "96": "#1a5490",
                    "113": "#154360",
                    "137": "#0b2638"
                }
                
                # Define threshold labels
                threshold_labels = {
                    "34": "34kt TS",
                    "40": "40kt Strong TS",
                    "50": "50kt V.Strong TS",
                    "64": "64kt Cat 1",
                    "83": "83kt Cat 2",
                    "96": "96kt Cat 3",
                    "113": "113kt Cat 4",
                    "137": "137kt Cat 5"
                }
                
                for higher_thresh, higher_values in higher_threshold_data.items():
                    if len(higher_values) > 0:
                        # Calculate exceedance curve for this higher threshold
                        higher_impact_thresholds = []
                        for prob in probability_levels:
                            percentile = 100 - prob
                            threshold = np.percentile(higher_values, percentile)
                            higher_impact_thresholds.append(threshold)
                        
                        # Get color for this threshold
                        trace_color = higher_threshold_colors.get(higher_thresh, "#888888")
                        
                        # Add as dashed line to show it's a different threshold
                        fig.add_trace(go.Scatter(
                            x=probability_levels,
                            y=higher_impact_thresholds,
                            mode='lines',
                            name=threshold_labels.get(higher_thresh, f"{higher_thresh}kt"),
                            line=dict(color=trace_color, width=2, dash='dash'),
                            hovertemplate=f'<b>{threshold_labels.get(higher_thresh, higher_thresh+"kt")}:</b><br>Probability: %{{x:.1f}}%<br>Impact Threshold: %{{y:,.0f}}<extra></extra>',
                            legendgroup="higher_thresholds",
                            showlegend=True
                        ))
            
            # Add horizontal line for deterministic value
            if member_51_idx is not None:
                member_51_val = values[member_51_idx] if len(values) > member_51_idx else None
                if member_51_val is not None:
                    exceedance_prob_51 = np.sum(values > member_51_val) / len(values) * 100
                    fig.add_hline(
                        y=member_51_val,
                        line_dash="dash",
                        line_color="#ff6b35",
                        line_width=2,
                        annotation_text=f"Deterministic ({exceedance_prob_51:.1f}%)",
                        annotation_position="top right"
                    )
            
            fig.update_layout(
                xaxis=dict(
                    title=dict(text="Probability of Exceeding Threshold (%)", font=dict(size=12)),
                    range=[0, 100],
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                yaxis=dict(
                    title=dict(text=f"Impact Threshold ({x_axis_title})", font=dict(size=12)),
                    tickformat=",.0f",
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                plot_bgcolor='rgba(250, 250, 250, 1)',
                paper_bgcolor='white',
                margin=dict(l=60, r=120, t=30, b=40),  # Increased right margin for legend
                height=350,
                showlegend=True,
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=1,
                    xanchor="right",
                    x=1.02,
                    font=dict(size=10),
                    bgcolor="rgba(255, 255, 255, 0.8)",
                    bordercolor="#cccccc",
                    borderwidth=1
                )
            )
            return fig
        
        def create_percentiles_display(values, color):
            """Create percentile display for a metric"""
            if len(values) == 0:
                return html.Div("No data available")
            
            percentile_data = [
                ('10th Percentile', np.percentile(values, 10)),
                ('25th Percentile (Q1)', np.percentile(values, 25)),
                ('50th Percentile (Median)', np.percentile(values, 50)),
                ('75th Percentile (Q3)', np.percentile(values, 75)),
                ('90th Percentile', np.percentile(values, 90)),
                ('Mean', np.mean(values)),
                ('Standard Deviation', np.std(values)),
            ]
            
            if member_51_idx is not None:
                member_51_val = values[member_51_idx] if len(values) > member_51_idx else None
                if member_51_val is not None:
                    percentile_data.append(('Member 51 (Deterministic)', member_51_val))
            
            percentile_rows = []
            for label, value in percentile_data:
                percentile_rows.append(
                    dmc.Group([
                        dmc.Text(label, size="sm", style={"width": "200px", "fontWeight": 500}),
                        dmc.Text(f"{value:,.0f}", size="sm", style={"fontWeight": 600, "color": color})
                    ], justify="space-between", mb="xs")
                )
            
            return dmc.Stack(percentile_rows, gap="sm", p="sm")
        
        # Generate all uncertainty metrics for each metric
        pop_exceedance = create_exceedance_plot(
            member_df['population'].values,
            'Population',
            metrics_config['population']['color'],
            metrics_config['population']['x_label'],
            wind_threshold=wind_threshold,
            available_thresholds=available_wind_thresholds,
            higher_threshold_data=higher_threshold_data.get('population', {})
        )
        pop_percentiles = create_percentiles_display(
            member_df['population'].values,
            metrics_config['population']['color']
        )
        
        print(f"Generated plots for population: box={type(pop_box)}, exceedance={type(pop_exceedance)}")
        
        children_exceedance = create_exceedance_plot(
            member_df['children'].values,
            'Children',
            metrics_config['children']['color'],
            metrics_config['children']['x_label'],
            wind_threshold=wind_threshold,
            available_thresholds=available_wind_thresholds,
            higher_threshold_data=higher_threshold_data.get('children', {})
        )
        children_percentiles = create_percentiles_display(
            member_df['children'].values,
            metrics_config['children']['color']
        )
        
        infants_exceedance = create_exceedance_plot(
            member_df['infants'].values,
            'Infants',
            metrics_config['infants']['color'],
            metrics_config['infants']['x_label'],
            wind_threshold=wind_threshold,
            available_thresholds=available_wind_thresholds,
            higher_threshold_data=higher_threshold_data.get('infants', {})
        )
        infants_percentiles = create_percentiles_display(
            member_df['infants'].values,
            metrics_config['infants']['color']
        )
        
        schools_exceedance = create_exceedance_plot(
            member_df['schools'].values,
            'Schools',
            metrics_config['schools']['color'],
            metrics_config['schools']['x_label'],
            wind_threshold=wind_threshold,
            available_thresholds=available_wind_thresholds,
            higher_threshold_data=higher_threshold_data.get('schools', {})
        )
        schools_percentiles = create_percentiles_display(
            member_df['schools'].values,
            metrics_config['schools']['color']
        )
        
        health_exceedance = create_exceedance_plot(
            member_df['health'].values,
            'Health Centers',
            metrics_config['health']['color'],
            metrics_config['health']['x_label'],
            wind_threshold=wind_threshold,
            available_thresholds=available_wind_thresholds,
            higher_threshold_data=higher_threshold_data.get('health', {})
        )
        health_percentiles = create_percentiles_display(
            member_df['health'].values,
            metrics_config['health']['color']
        )
        
        built_surface_exceedance = create_exceedance_plot(
            member_df['built_surface'].values,
            'Built Surface',
            metrics_config['built_surface']['color'],
            metrics_config['built_surface']['x_label'],
            wind_threshold=wind_threshold,
            available_thresholds=available_wind_thresholds,
            higher_threshold_data=higher_threshold_data.get('built_surface', {})
        )
        built_surface_percentiles = create_percentiles_display(
            member_df['built_surface'].values,
            metrics_config['built_surface']['color']
        )
        
        status_msg = dmc.Alert(
            f"Showing distribution for {len(member_df)} ensemble members.",
            title="Plots Updated",
            color="green",
            variant="light"
        )
        
        return (
            pop_box, pop_exceedance, pop_percentiles,
            children_box, children_exceedance, children_percentiles,
            infants_box, infants_exceedance, infants_percentiles,
            schools_box, schools_exceedance, schools_percentiles,
            health_box, health_exceedance, health_percentiles,
            built_surface_box, built_surface_exceedance, built_surface_percentiles,
            status_msg
        )
        
    except Exception as e:
        print(f"Error generating box plots: {e}")
        import traceback
        traceback.print_exc()
        status_msg = dmc.Alert(
            f"Error generating plots: {str(e)}",
            title="Error",
            color="red",
            variant="light"
        )
        # Create empty figure and children if not already created
        empty_fig = go.Figure()
        empty_fig.update_layout(
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor="white",
            margin=dict(l=20, r=20, t=20, b=20)
        )
        empty_children = html.Div("No data available", style={"padding": "10px", "color": "gray", "fontSize": "12px"})
        return (
            empty_fig, empty_fig, empty_fig, empty_children,  # population
            empty_fig, empty_fig, empty_fig, empty_children,  # children
            empty_fig, empty_fig, empty_fig, empty_children,  # infants
            empty_fig, empty_fig, empty_fig, empty_children,  # schools
            empty_fig, empty_fig, empty_fig, empty_children,  # health
            empty_fig, empty_fig, empty_fig, empty_children,  # built_surface
            status_msg
        )

