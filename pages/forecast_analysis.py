from dash import html, dcc, Input, Output, State, callback
import dash
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import numpy as np
import os
import warnings
import plotly.graph_objects as go

# Optional scipy import for PDF estimation
try:
    from scipy.stats import gaussian_kde
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config
from components.ui.header import make_header
from components.ui.footer import footer
from components.data.snowflake_utils import get_snowflake_connection, get_available_wind_thresholds
from gigaspatial.core.io.readers import read_dataset
from components.data.data_store_utils import get_data_store

# Constants
ZOOM_LEVEL = 14
VIEWS_DIR = config.VIEWS_DIR or "aos_views"
ROOT_DATA_DIR = config.ROOT_DATA_DIR or "geodb"

# Initialize data store
data_store = get_data_store()
giga_store = data_store

dash.register_page(
    __name__, path="/analysis", name="Forecast Analysis"
)

# Load hurricane metadata from Snowflake
def get_snowflake_data():
    """Get hurricane metadata directly from Snowflake"""
    try:
        conn = get_snowflake_connection()
        
        # Get unique storm/forecast combinations from TC_TRACKS
        query = '''
        SELECT DISTINCT 
            TRACK_ID,
            FORECAST_TIME,
            COUNT(DISTINCT ENSEMBLE_MEMBER) as ENSEMBLE_COUNT
        FROM TC_TRACKS
        GROUP BY TRACK_ID, FORECAST_TIME
        ORDER BY FORECAST_TIME DESC, TRACK_ID
        '''
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"Error getting Snowflake data: {str(e)}")
        return pd.DataFrame({'TRACK_ID': [], 'FORECAST_TIME': [], 'ENSEMBLE_COUNT': []})

# Load initial metadata
metadata_df = get_snowflake_data()
# Parse dates and times from metadata
if not metadata_df.empty:
    metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date
    metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')
    
    # Get unique dates and times
    unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
    unique_times = sorted(metadata_df['TIME'].unique())
else:
    unique_dates = []
    unique_times = []

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
            data=[
                {"value": "34", "label": "34kt - Tropical storm"},
                {"value": "40", "label": "40kt - Strong tropical"},
                {"value": "50", "label": "50kt - Very strong tropical"},
                {"value": "64", "label": "64kt - Cat 1 hurricane"},
                {"value": "83", "label": "83kt - Cat 2 hurricane"},
                {"value": "96", "label": "96kt - Cat 3 hurricane"},
                {"value": "113", "label": "113kt - Cat 4 hurricane"},
                {"value": "137", "label": "137kt - Cat 5 hurricane"},
            ],
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
                        impact_summary
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

# Selector section - full width, evenly distributed
selectors_section = dmc.Paper([
    dmc.Group(
        [
            dmc.Stack(
                [
                    dmc.Text("Country", size="xs", fw=600, c="dimmed"),
                    dmc.Select(
                        id="analysis-country-select",
                        placeholder="Select country...",
                        data=[
                            {"value": "AIA", "label": "Anguilla"},
                            {"value": "ATG", "label": "Antigua and Barbuda"},
                            {"value": "BLZ", "label": "Belize"},
                            {"value": "VGB", "label": "British Virgin Islands"},
                            {"value": "CUB", "label": "Cuba"},
                            {"value": "DMA", "label": "Dominica"},
                            {"value": "DOM", "label": "Dominican Republic"},
                            {"value": "GRD", "label": "Grenada"},
                            {"value": "JAM", "label": "Jamaica"},
                            {"value": "MSR", "label": "Montserrat"},
                            {"value": "NIC", "label": "Nicaragua"},
                            {"value": "KNA", "label": "Saint Kitts and Nevis"},
                            {"value": "LCA", "label": "Saint Lucia"},
                            {"value": "VCT", "label": "Saint Vincent and the Grenadines"}
                        ],
                        value="JAM",
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


# Impact Summary Section
impact_summary = dmc.Paper([
                    dmc.Group([
                        dmc.Text("IMPACT SUMMARY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                    ], justify="flex-start", gap="sm", mb="sm"),
                    
                    dmc.Text("Hurricane impact scenarios and metrics", size="xs", c="dimmed", mb="md"),
                    
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
                                        dmc.Text("Deterministic", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                        dmc.Badge("#51", id="analysis-deterministic-badge", size="xs", color="blue", variant="light", style={"marginTop": "2px"})
                                    ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"}),
                                    dmc.TableTh("Expected", style={"fontWeight": 700, "textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "paddingTop": "8px", "height": "60px", "verticalAlign": "top"}),
                                    dmc.TableTh([
                                        dmc.Text("Worst", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                        dmc.Badge("Member", id="analysis-high-impact-badge", size="xs", color="red", variant="light", style={"marginTop": "2px"})
                                    ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"})
                                ])
                            ]),
                            dmc.TableTbody([
                                dmc.TableTr([
                                    dmc.TableTd("Population", style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-population-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-population-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-population-count-high", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd(dmc.Group([dmc.Text(size="xs", c="dimmed"), dmc.Text("Age 5-15", style={"fontStyle": "italic", "fontSize": "0.95em"})], gap=0), style={"fontWeight": 500, "paddingLeft": "15px"}),
                                    dmc.TableTd("N/A", id="analysis-children-affected-low", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-children-affected-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-children-affected-high", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd(dmc.Group([dmc.Text(size="xs", c="dimmed"), dmc.Text("Age 0-5", style={"fontStyle": "italic", "fontSize": "0.95em"})], gap=0), style={"fontWeight": 500, "paddingLeft": "15px"}),
                                    dmc.TableTd("N/A", id="analysis-infant-affected-low",
                                                style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-infant-affected-probabilistic",
                                                style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-infant-affected-high",
                                                style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Schools", style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-schools-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-schools-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-schools-count-high", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Health Centers", style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-health-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-health-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-health-count-high", style={"textAlign": "center", "fontWeight": 500})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd([
                                        html.Span("Built Surface m"),
                                        html.Sup("2"),
                                    ], style={"fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-bsm2-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-bsm2-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                    dmc.TableTd("N/A", id="analysis-bsm2-count-high", style={"textAlign": "center", "fontWeight": 500})
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

def make_single_page_layout():
    """Create the forecast analysis dashboard layout"""
    return dmc.Stack(
        [
            # Selectors at top - full width
            selectors_section,
            
            # Main content area - full width
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
                                [
                                    # Wind threshold selector tabs - just for selecting, graphs are outside
                                    dmc.SegmentedControl(
                                        id="analysis-population-threshold-selector",
                                        value="34",
                                        data=[
                                            {"value": "34", "label": "34kt - Tropical storm"},
                                            {"value": "40", "label": "40kt - Strong tropical"},
                                            {"value": "50", "label": "50kt - Very strong tropical"},
                                            {"value": "64", "label": "64kt - Cat 1 hurricane"},
                                            {"value": "83", "label": "83kt - Cat 2 hurricane"},
                                            {"value": "96", "label": "96kt - Cat 3 hurricane"},
                                            {"value": "113", "label": "113kt - Cat 4 hurricane"},
                                            {"value": "137", "label": "137kt - Cat 5 hurricane"},
                                        ],
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
                                                            dmc.Text("POPULATION AFFECTED", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}, mb="xs"),
                                                            dcc.Graph(id="analysis-population-plot", style={"height": "350px"})
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
                                                            dcc.Graph(id="analysis-population-exceedance", style={"height": "350px"})
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
                                                    impact_summary
                                                ],
                                                span=6
                                            ),
                                            dmc.GridCol(
                                                [
                                                    dmc.Paper(
                                                        [
                                                            dmc.Text("IMPACT PERCENTILES", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}, mb="xs"),
                                                            html.Div(id="analysis-population-percentiles")
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
                                ],
                                value="population"
                            ),
                            # Children Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("children", "Children (Age 5-15)"),
                                value="children"
                            ),
                            # Infants Tab
                            dmc.TabsPanel(
                                create_wind_threshold_tabs_content("infants", "Infants (Age 0-5)"),
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

@callback(
    Output("analysis-forecast-date", "data"),
    Output("analysis-forecast-date", "value"),
    Input("analysis-country-select", "value"),
    prevent_initial_call=False
)
def update_forecast_dates(country):
    """Get available forecast dates from pre-loaded data and set most recent as default"""
    print(f"Analysis: update_forecast_dates called with country: {country}")
    
    if not metadata_df.empty:
        # Format dates and create options
        date_options = []
        for date in unique_dates:
            formatted_date = date.strftime('%Y-%m-%d')
            display_date = date.strftime('%b %d, %Y')
            date_options.append({
                "value": formatted_date,
                "label": display_date
            })
        
        # Set default to most recent date (first in the list since unique_dates is sorted reverse=True)
        default_date = date_options[0]['value'] if date_options else None
        print(f"Analysis: Returning {len(date_options)} date options from pre-loaded data")
        return date_options, default_date
    else:
        print("Analysis: No metadata available, returning fallback dates")
        # Return some fallback data for testing
        fallback_dates = [
            {"value": "2025-10-20", "label": "Oct 20, 2025"},
            {"value": "2025-10-15", "label": "Oct 15, 2025"},
            {"value": "2025-10-10", "label": "Oct 10, 2025"},
            {"value": "2025-10-05", "label": "Oct 05, 2025"},
            {"value": "2025-09-30", "label": "Sep 30, 2025"}
        ]
        return fallback_dates, fallback_dates[0]['value']

@callback(
    Output("analysis-forecast-time", "data"),
    Output("analysis-forecast-time", "value", allow_duplicate=True),
    [Input("analysis-forecast-date", "value")],
    prevent_initial_call='initial_duplicate'
)
def update_forecast_times(selected_date):
    """Get available forecast times for selected date, with most recent time as default"""
    if not selected_date or metadata_df.empty:
        # Return all possible times with unavailable ones grayed out
        all_times = ["00:00", "06:00", "12:00", "18:00"]
        return [{"value": t, "label": f"{t} UTC", "disabled": True} for t in all_times], "00:00"
    
    # Filter metadata for selected date
    df = metadata_df.copy()
    df['DATE'] = pd.to_datetime(df['FORECAST_TIME']).dt.date.astype(str)
    df['TIME'] = pd.to_datetime(df['FORECAST_TIME']).dt.strftime('%H:%M')
    
    # Get available times for selected date
    available_times = sorted(df[df['DATE'] == selected_date]['TIME'].unique())
    
    # Create options with all possible times, marking unavailable ones as disabled
    all_possible_times = ["00:00", "06:00", "12:00", "18:00"]
    time_options = []
    
    for time in all_possible_times:
        is_available = time in available_times
        time_options.append({
            "value": time,
            "label": f"{time} UTC",
            "disabled": not is_available
        })
    
    # Set default to most recent available time (last in sorted list)
    default_time = available_times[-1] if available_times else "00:00"
    print(f"Analysis: Forecast times for {selected_date}: available={available_times}, default (most recent)={default_time}")
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
    
    # Filter metadata for selected date and time
    df = metadata_df.copy()
    df['DATE'] = pd.to_datetime(df['FORECAST_TIME']).dt.date.astype(str)
    df['TIME'] = pd.to_datetime(df['FORECAST_TIME']).dt.strftime('%H:%M')
    
    # Get available storms for selected date and time
    available_storms = sorted(df[(df['DATE'] == forecast_date) & (df['TIME'] == forecast_time)]['TRACK_ID'].unique())
    
    # Create options with only available storms (no grayed out options)
    storm_options = []
    for storm in available_storms:
        storm_options.append({
            "value": storm,
            "label": storm
        })
    
    # Set default to most recent storm (last in sorted list)
    default_storm = available_storms[-1] if available_storms else None
    print(f"Analysis: Storms for {forecast_date} {forecast_time}: available={available_storms}, options={len(storm_options)}, default (most recent)={default_storm}")
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
    Output("analysis-wind-threshold-select", "data"),
    Output("analysis-wind-threshold-select", "value", allow_duplicate=True),
    [Input("analysis-storm-select", "value"),
     Input("analysis-forecast-date", "value"),
     Input("analysis-forecast-time", "value")],
    [State("analysis-wind-threshold-select", "value")],
    prevent_initial_call='initial_duplicate'
)
def update_wind_threshold_options(storm, date, time, current_threshold):
    """Update wind threshold dropdown based on selected storm, date, and time - set most recent available as default"""
    if not all([storm, date, time]):
        # Return all thresholds if no storm selected
        all_thresholds = [
            {"value": "34", "label": "34kt - Tropical storm force (17.49 m/s)"},
            {"value": "40", "label": "40kt - Strong tropical storm (20.58 m/s)"},
            {"value": "50", "label": "50kt - Very strong tropical storm (25.72 m/s)"},
            {"value": "64", "label": "64kt - Category 1 hurricane (32.92 m/s)"},
            {"value": "83", "label": "83kt - Category 2 hurricane (42.70 m/s)"},
            {"value": "96", "label": "96kt - Category 3 hurricane (49.39 m/s)"},
            {"value": "113", "label": "113kt - Category 4 hurricane (58.12 m/s)"},
            {"value": "137", "label": "137kt - Category 5 hurricane (70.48 m/s)"}
        ]
        return all_thresholds, "34"  # Default to 34kt
    
    try:
        # Get available wind thresholds from Snowflake
        forecast_datetime = f"{date} {time}:00"
        available_thresholds = get_available_wind_thresholds(storm, forecast_datetime)
        
        # Define all possible thresholds with labels
        all_thresholds = {
            "34": "34kt - Tropical storm force (17.49 m/s)",
            "40": "40kt - Strong tropical storm (20.58 m/s)",
            "50": "50kt - Very strong tropical storm (25.72 m/s)",
            "64": "64kt - Category 1 hurricane (32.92 m/s)",
            "83": "83kt - Category 2 hurricane (42.70 m/s)",
            "96": "96kt - Category 3 hurricane (49.39 m/s)",
            "113": "113kt - Category 4 hurricane (58.12 m/s)",
            "137": "137kt - Category 5 hurricane (70.48 m/s)"
        }
        
        # Create options list, marking unavailable ones as disabled
        options = []
        for threshold, label in all_thresholds.items():
            is_available = threshold in available_thresholds
            options.append({
                "value": threshold,
                "label": label,
                "disabled": not is_available
            })
        
        # Set default threshold - preserve user selection if still available, otherwise default to 50kt
        default_threshold = None
        if available_thresholds:
            # If user's current selection is still available, keep it
            if current_threshold and current_threshold in available_thresholds:
                default_threshold = current_threshold
            else:
                # Otherwise, prefer 50kt if available, otherwise use the highest available
                if "50" in available_thresholds:
                    default_threshold = "50"
                else:
                    sorted_thresholds = sorted([int(t) for t in available_thresholds], reverse=True)
                    default_threshold = str(sorted_thresholds[0])
        else:
            default_threshold = "50"  # Fallback default
        
        print(f"Analysis: Wind thresholds for {storm} at {forecast_datetime}: available={available_thresholds}, current={current_threshold}, default={default_threshold}")
        return options, default_threshold
        
    except Exception as e:
        print(f"Analysis: Error getting wind threshold options: {e}")
        # Return all thresholds on error
        all_thresholds = [
            {"value": "34", "label": "34kt - Tropical storm force (17.49 m/s)"},
            {"value": "40", "label": "40kt - Strong tropical storm (20.58 m/s)"},
            {"value": "50", "label": "50kt - Very strong tropical storm (25.72 m/s)"},
            {"value": "64", "label": "64kt - Category 1 hurricane (32.92 m/s)"},
            {"value": "83", "label": "83kt - Category 2 hurricane (42.70 m/s)"},
            {"value": "96", "label": "96kt - Category 3 hurricane (49.39 m/s)"},
            {"value": "113", "label": "113kt - Category 4 hurricane (58.12 m/s)"},
            {"value": "137", "label": "137kt - Category 5 hurricane (70.48 m/s)"}
        ]
        return all_thresholds, "50"

# =============================================================================
# CALLBACK FOR IMPACT SUMMARY
# =============================================================================

@callback(
    [Output("analysis-population-count-low", "children"),
     Output("analysis-population-count-probabilistic", "children"),
     Output("analysis-population-count-high", "children"),
     Output("analysis-children-affected-low", "children"),
     Output("analysis-children-affected-probabilistic", "children"),
     Output("analysis-children-affected-high", "children"),
     Output("analysis-infant-affected-low", "children"),
     Output("analysis-infant-affected-probabilistic", "children"),
     Output("analysis-infant-affected-high", "children"),
     Output("analysis-schools-count-low", "children"),
     Output("analysis-schools-count-probabilistic", "children"),
     Output("analysis-schools-count-high", "children"),
     Output("analysis-health-count-low", "children"),
     Output("analysis-health-count-probabilistic", "children"),
     Output("analysis-health-count-high", "children"),
     Output("analysis-bsm2-count-low", "children"),
     Output("analysis-bsm2-count-probabilistic", "children"),
     Output("analysis-bsm2-count-high", "children"),
     Output("analysis-high-impact-badge", "children")],
   [Input("analysis-storm-select", "value"),
    Input("analysis-wind-threshold-store", "data"),
    Input("analysis-country-select", "value"),
    Input("analysis-forecast-date", "value"),
    Input("analysis-forecast-time", "value")],
    prevent_initial_call=True
)
def update_impact_metrics(storm, wind_threshold, country, forecast_date, forecast_time):
    """Update impact metrics for all three scenarios when Load Impact Summary button is clicked"""
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        return ("N/A",) * 18 + ("N/A",)  # 18 metrics + 1 badge = 19 outputs
    
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
        
        # Initialize member badge (deterministic is always #51, doesn't need updating)
        high_member_badge = "N/A"
        status_msg = None
        
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
                
                print(f"Analysis Impact metrics: Successfully loaded {len(df)} features")
                status_msg = dmc.Alert("Impact summary loaded successfully", title="Success", color="green", variant="light")
                
            except Exception as e:
                print(f"Analysis Impact metrics: Error reading file {filename}: {e}")
                status_msg = dmc.Alert(f"Error loading impact data: {str(e)}", title="Error", color="red", variant="light")
        else:
            print(f"Analysis Impact metrics: File not found {filename}")
            status_msg = dmc.Alert("Impact data file not found. Please ensure the storm data has been processed.", title="Data Not Found", color="orange", variant="light")
        
        # Format results
        def format_value(value):
            return str(value) if isinstance(value, str) else f"{value:,.0f}"
        
        return (
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
            
    except Exception as e:
        print(f"Analysis Impact metrics: Error updating metrics: {e}")
        return ("N/A",) * 18 + ("N/A",)  # 18 metrics + 1 badge = 19 outputs


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
    
    # Debug logging
    print(f"update_box_plots called with: storm={storm}, wind_threshold={wind_threshold}, country={country}, date={forecast_date}, time={forecast_time}")
    
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
        
        # Calculate totals per ensemble member for all metrics
        print(f"Loading data from: {tracks_filepath}")
        print(f"Found {len(gdf_tracks)} track records with {len(gdf_tracks['zone_id'].unique())} unique members")
        
        member_data = []
        for member_id in gdf_tracks['zone_id'].unique():
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
        print(f"Created member_df with {len(member_df)} rows")
        print(f"Columns: {member_df.columns.tolist()}")
        print(f"Sample data:\n{member_df.head()}")
        
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
            # Convert hex color to rgba
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            fillcolor_rgba = f'rgba({r}, {g}, {b}, 0.2)'
            
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
        
        # Helper function to create CDF plot for a metric
        def create_cdf_plot(values, metric_name, color, x_axis_title):
            sorted_values = np.sort(values)
            n = len(sorted_values)
            cdf_y = np.arange(1, n + 1) / n * 100
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=sorted_values,
                y=cdf_y,
                mode='lines',
                name=metric_name,
                line=dict(color=color, width=2.5),
                hovertemplate=f'<b>{metric_name}</b><br>Value: %{{x:,.0f}}<br>Probability: %{{y:.1f}}%<extra></extra>'
            ))
            
            # Highlight member 51 if present
            if 51 in member_df['member'].values:
                member_51_idx = member_df['member'].values.tolist().index(51)
                member_51_value = values[member_51_idx]
                member_51_cdf = (np.sum(sorted_values <= member_51_value) / n) * 100
                fig.add_trace(go.Scatter(
                    x=[member_51_value],
                    y=[member_51_cdf],
                    mode='markers',
                    marker=dict(
                        symbol='circle',
                        size=10,
                        color='#ff6b35',
                        line=dict(width=2, color='white')
                    ),
                    name='Member 51 (Deterministic)',
                    hovertemplate=f'<b>Member 51:</b><br>Value: %{{x:,.0f}}<br>Percentile: ~%{{y:.1f}}%<extra></extra>',
                    showlegend=True
                ))
            
            fig.update_layout(
                xaxis=dict(
                    title=dict(text=x_axis_title, font=dict(size=11)),
                    tickformat=",.0f",
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                yaxis=dict(
                    title=dict(text="Cumulative Probability (%)", font=dict(size=11)),
                    range=[0, 100],
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                plot_bgcolor='rgba(250, 250, 250, 1)',
                paper_bgcolor='white',
                margin=dict(l=60, r=40, t=30, b=40),
                height=350
            )
            return fig
        
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
        def create_exceedance_plot(values, metric_name, color, x_axis_title):
            """Create exceedance probability plot for a metric"""
            fig = go.Figure()
            
            if len(values) == 0:
                return fig
            
            max_val = np.max(values)
            min_val = np.max([0, np.min(values)])
            
            # Create threshold values
            n_thresholds = 100
            thresholds = np.linspace(min_val, max_val * 1.1, n_thresholds)
            
            # Calculate exceedance probability for each threshold
            exceedance_probs = []
            for threshold in thresholds:
                prob = np.sum(values > threshold) / len(values) * 100
                exceedance_probs.append(prob)
            
            # Convert hex color to rgba
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            fillcolor_rgba = f'rgba({r}, {g}, {b}, 0.2)'
            
            fig.add_trace(go.Scatter(
                x=thresholds,
                y=exceedance_probs,
                mode='lines',
                name=metric_name,
                line=dict(color=color, width=2.5),
                fill='tozeroy',
                fillcolor=fillcolor_rgba,
                hovertemplate='<b>Threshold:</b> %{x:,.0f}<br>Probability: %{y:.1f}%<extra></extra>'
            ))
            
            # Add vertical line for deterministic value
            if 51 in member_df['member'].values:
                member_51_idx = member_df['member'].values.tolist().index(51)
                member_51_val = values[member_51_idx] if len(values) > member_51_idx else None
                if member_51_val is not None:
                    exceedance_prob_51 = np.sum(values > member_51_val) / len(values) * 100
                    fig.add_vline(
                        x=member_51_val,
                        line_dash="dash",
                        line_color="#ff6b35",
                        line_width=2,
                        annotation_text=f"Deterministic ({exceedance_prob_51:.1f}%)",
                        annotation_position="top right"
                    )
            
            fig.update_layout(
                xaxis=dict(
                    title=dict(text=f"Impact Threshold ({x_axis_title})", font=dict(size=12)),
                    tickformat=",.0f",
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                yaxis=dict(
                    title=dict(text="Probability of Exceeding Threshold (%)", font=dict(size=12)),
                    range=[0, 100],
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                plot_bgcolor='rgba(250, 250, 250, 1)',
                paper_bgcolor='white',
                margin=dict(l=60, r=40, t=30, b=40),
                height=350
            )
            return fig
        
        def create_pdf_plot(values, metric_name, color, x_axis_title):
            """Create PDF plot for a metric"""
            fig = go.Figure()
            
            if len(values) == 0:
                return fig
            
            # Use KDE to estimate PDF if scipy is available
            if SCIPY_AVAILABLE:
                try:
                    kde = gaussian_kde(values)
                    x_pdf = np.linspace(np.max([0, np.min(values) * 0.9]), np.max(values) * 1.1, 200)
                    y_pdf = kde(x_pdf) * len(values)
                    
                    # Convert hex color to rgba
                    r = int(color[1:3], 16)
                    g = int(color[3:5], 16)
                    b = int(color[5:7], 16)
                    fillcolor_rgba = f'rgba({r}, {g}, {b}, 0.3)'
                    
                    fig.add_trace(go.Scatter(
                        x=x_pdf,
                        y=y_pdf,
                        mode='lines',
                        name=f'{metric_name} PDF',
                        line=dict(color=color, width=2.5),
                        fill='tozeroy',
                        fillcolor=fillcolor_rgba,
                        hovertemplate='<b>Value:</b> %{x:,.0f}<br>Density: %{y:.2f}<extra></extra>'
                    ))
                    
                    # Highlight deterministic value
                    if 51 in member_df['member'].values:
                        member_51_idx = member_df['member'].values.tolist().index(51)
                        member_51_val = values[member_51_idx] if len(values) > member_51_idx else None
                        if member_51_val is not None:
                            pdf_51 = kde(member_51_val) * len(values)
                            fig.add_trace(go.Scatter(
                                x=[member_51_val],
                                y=[pdf_51],
                                mode='markers',
                                marker=dict(symbol='circle', size=12, color='#ff6b35', line=dict(width=2, color='white')),
                                name='Member 51 (Deterministic)',
                                hovertemplate=f'<b>Member 51:</b> %{{x:,.0f}}<extra></extra>',
                                showlegend=True
                            ))
                except:
                    # Fallback to histogram
                    fig.add_trace(go.Histogram(
                        x=values, histnorm='probability density', name=f'{metric_name} PDF',
                        marker_color=color, opacity=0.7,
                        hovertemplate='<b>Value:</b> %{x:,.0f}<br>Density: %{y:.4f}<extra></extra>'
                    ))
            else:
                fig.add_trace(go.Histogram(
                    x=values, histnorm='probability density', name=f'{metric_name} PDF',
                    marker_color=color, opacity=0.7,
                    hovertemplate='<b>Value:</b> %{x:,.0f}<br>Density: %{y:.4f}<extra></extra>'
                ))
            
            fig.update_layout(
                xaxis=dict(
                    title=dict(text=x_axis_title, font=dict(size=12)),
                    tickformat=",.0f",
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                yaxis=dict(
                    title=dict(text="Probability Density", font=dict(size=12)),
                    gridcolor='rgba(200, 200, 200, 0.3)',
                    showline=True
                ),
                showlegend=True,
                legend=dict(x=1.02, y=1),
                plot_bgcolor='rgba(250, 250, 250, 1)',
                paper_bgcolor='white',
                margin=dict(l=60, r=100, t=30, b=40),
                height=350
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
            
            if 51 in member_df['member'].values:
                member_51_idx = member_df['member'].values.tolist().index(51)
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
            metrics_config['population']['x_label']
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
            metrics_config['children']['x_label']
        )
        children_percentiles = create_percentiles_display(
            member_df['children'].values,
            metrics_config['children']['color']
        )
        
        infants_exceedance = create_exceedance_plot(
            member_df['infants'].values,
            'Infants',
            metrics_config['infants']['color'],
            metrics_config['infants']['x_label']
        )
        infants_percentiles = create_percentiles_display(
            member_df['infants'].values,
            metrics_config['infants']['color']
        )
        
        schools_exceedance = create_exceedance_plot(
            member_df['schools'].values,
            'Schools',
            metrics_config['schools']['color'],
            metrics_config['schools']['x_label']
        )
        schools_percentiles = create_percentiles_display(
            member_df['schools'].values,
            metrics_config['schools']['color']
        )
        
        health_exceedance = create_exceedance_plot(
            member_df['health'].values,
            'Health Centers',
            metrics_config['health']['color'],
            metrics_config['health']['x_label']
        )
        health_percentiles = create_percentiles_display(
            member_df['health'].values,
            metrics_config['health']['color']
        )
        
        built_surface_exceedance = create_exceedance_plot(
            member_df['built_surface'].values,
            'Built Surface',
            metrics_config['built_surface']['color'],
            metrics_config['built_surface']['x_label']
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
        
        print(f"About to return plots. Total outputs: 6 metrics * 3 = 18 plots + 1 status = 19")
        print(f"Population values: pop={member_df['population'].sum():.0f}, len={len(member_df['population'])}")
        print(f"Plot types: pop_box={type(pop_box)}, pop_exceedance={type(pop_exceedance)}")
        
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

