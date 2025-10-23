import numpy as np
import pandas as pd
import dash
from dash import Output, Input, State, callback, dcc, html
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_leaflet as dl
import geopandas as gpd
import os
import warnings

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config

from dash_extensions.javascript import assign

# JavaScript styling for hurricane tracks
style_tracks = assign("""
function(feature, context) {
    const member_type = feature.properties?.member_type;
    if (member_type === 'control') {
        return {color: '#ff0000', weight: 4, opacity: 1.0};
    } else {
        return {color: '#1cabe2', weight: 2, opacity: 0.8};
    }
}
""")

# JavaScript styling for schools and health centers with probability-based coloring
style_schools_health = assign("""
function(feature, context) {
    const props = feature.properties || {};
    const color = props._color || '#808080';
    const radius = props._radius || 5;
    const opacity = props._opacity || 0.8;
    const weight = props._weight || 1;
    const fillOpacity = props._fillOpacity || 0.7;
    
    return {
        color: color,
        weight: weight,
        opacity: opacity,
        fillColor: color,
        fillOpacity: fillOpacity,
        radius: radius
    };
}
""")

# JavaScript point-to-layer function for schools and health centers
point_to_layer_schools_health = assign("""
function(feature, latlng, context) {
    const props = feature.properties || {};
    const color = props._color || '#808080';
    const radius = props._radius || 5;
    const opacity = props._opacity || 0.8;
    const weight = props._weight || 1;
    const fillOpacity = props._fillOpacity || 0.7;
    
    return L.circleMarker(latlng, {
        radius: radius,
        fillColor: color,
        color: color,
        weight: weight,
        opacity: opacity,
        fillOpacity: fillOpacity
    });
}
""")

style_envelopes = assign("""
function(feature, context) {
    const wind_threshold = feature.properties?.wind_threshold;
    if (wind_threshold === 34) {
        return {color: '#ffff00', weight: 2, fillColor: '#ffff00', fillOpacity: 0.3};
    } else if (wind_threshold === 40) {
        return {color: '#ff8800', weight: 2, fillColor: '#ff8800', fillOpacity: 0.3};
    } else if (wind_threshold === 50) {
        return {color: '#ff0000', weight: 2, fillColor: '#ff0000', fillOpacity: 0.3};
    } else {
        return {color: '#888888', weight: 2, fillColor: '#888888', fillOpacity: 0.3};
    }
}
""")

# Load hurricane metadata at startup (like hurricanes page)

from components.ui.appshell import make_default_appshell
import dash_leaflet as dl
import geopandas as gpd
from components.map.map_config import map_config, mapbox_token
from components.map.home_map import make_empty_map
from components.data.snowflake_utils import get_available_wind_thresholds, get_latest_forecast_time_overall, get_snowflake_connection, get_envelope_data_snowflake

#### Metadata 
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.processing.geo import convert_to_geodataframe
from components.data.data_store_utils import get_data_store

##### env variables #####
RESULTS_DIR = config.RESULTS_DIR
BBOX_FILE = config.BBOX_FILE
VIEWS_DIR = config.VIEWS_DIR
ROOT_DATA_DIR = config.ROOT_DATA_DIR
#########################

# Initialize data store using centralized utility
data_store = get_data_store()
giga_store = data_store


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
metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date
metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')

# Get unique dates and times
unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
unique_times = sorted(metadata_df['TIME'].unique())
##########################

#### Get current hurricanes
latest = (metadata_df.assign(dt=pd.to_datetime(metadata_df["DATE"].astype(str) + " " + metadata_df["TIME"]))
            .sort_values(["TRACK_ID","dt"])
            .drop_duplicates("TRACK_ID", keep="last"))

def get_lat_lons(row):
    """Get latitude and longitude for a hurricane track from Snowflake"""
    try:
        conn = get_snowflake_connection()
        
        # Get any available track data at lead time 0
        query = '''
        SELECT LATITUDE, LONGITUDE
        FROM TC_TRACKS
        WHERE TRACK_ID = %s AND FORECAST_TIME = %s 
        AND LEAD_TIME = 0
        LIMIT 1
        '''
        
        df_latlon = pd.read_sql(query, conn, params=[row['TRACK_ID'], str(row['FORECAST_TIME'])])
        
        conn.close()
        
        if len(df_latlon) > 0:
            lat = df_latlon.iloc[0]['LATITUDE']
            lon = df_latlon.iloc[0]['LONGITUDE']
        else:
            lat = np.nan
            lon = np.nan
            
        return pd.Series([lat, lon], index=["latitude", "longitude"])
            
    except Exception as e:
        print(f"Error getting lat/lon from Snowflake: {str(e)}")
        return pd.Series([np.nan, np.nan], index=["latitude", "longitude"])

latest[["latitude","longitude"]] = latest.apply(get_lat_lons, axis=1)

# Convert timestamp columns to strings for JSON serialization
latest_clean = latest.dropna().copy()
if 'FORECAST_TIME' in latest_clean.columns:
    latest_clean['FORECAST_TIME'] = latest_clean['FORECAST_TIME'].astype(str)
if 'DATE' in latest_clean.columns:
    latest_clean['DATE'] = latest_clean['DATE'].astype(str)
if 'TIME' in latest_clean.columns:
    latest_clean['TIME'] = latest_clean['TIME'].astype(str)

gdf_latest = convert_to_geodataframe(latest_clean)
##########################

user_name = "UNICEF-User"

interactive_options = dmc.Group(
    [
        dmc.NavLink(
            label="Hurricane Track Explorer",
            rightSection=DashIconify(icon="mdi:chevron-right", width=24),
            p="md",
            # description="Additional information",
            href="/hurricanes",
            style={
                "border": "1px solid #e0e0e0",  # Light grey border around each NavLink
                "borderRadius": "5px",  # Rounded corners
                "height": "8vh",
            },
        ),
        dmc.NavLink(
            label="Schools",
            rightSection=DashIconify(icon="mdi:chevron-right", width=24),
            p="md",
            # description="Additional information",
            href="/schools",
            style={
                "border": "1px solid #e0e0e0",  # Light grey border around each NavLink
                "borderRadius": "5px",  # Rounded corners
                "height": "8vh",
            },
        ),
        dmc.NavLink(
            label="Health Centers",
            rightSection=DashIconify(icon="mdi:chevron-right", width=24),
            p="md",
            # description="Additional information",
            href="/health",
            style={
                "border": "1px solid #e0e0e0",  # Light grey border around each NavLink
                "borderRadius": "5px",  # Rounded corners
                "height": "8vh",
            },
        ),
        dmc.NavLink(
            label="Tiles",
            rightSection=DashIconify(icon="mdi:chevron-right", width=24),
            p="md",
            # description="Additional information",
            href="/tiles",
            style={
                "border": "1px solid #e0e0e0",  # Light grey border around each NavLink
                "borderRadius": "5px",  # Rounded corners
                "height": "8vh",
            },
        ),
        dmc.NavLink(
            label="Envelopes",
            rightSection=DashIconify(icon="mdi:chevron-right", width=24),
            p="md",
            # description="Additional information",
            href="/envelopes",
            style={
                "border": "1px solid #e0e0e0",  # Light grey border around each NavLink
                "borderRadius": "5px",  # Rounded corners
                "height": "8vh",
            },
        ),
    ],
    gap="sm",
    style={"marginTop": "40px"},
)


def make_welcome(user):
    return dmc.Group(
        [
            dmc.Text("Welcome, ", style={"fontSize": "1vw"}),
            dmc.Text(f"{user}!", fw=700, style={"fontSize": "1vw", "marginLeft": -10}),
        ],
    )


def make_status_indicators(values):
    return dmc.Stack(
        [
            dmc.Group(
                [
                    DashIconify(icon="mdi:alert", color="red", width=20),
                    dmc.Text(
                        f"{values[0]:,} Schools at risk",
                        style={"fontSize": "0.7vw"},
                    ),
                ],
                gap="xs",
            ),
            dmc.Group(
                [
                    DashIconify(icon="mdi:alert", color="red", width=20),
                    dmc.Text(
                        f"{values[1]:,} Health centers at risk",
                        style={"fontSize": "0.7vw"},
                    ),
                ],
                gap="xs",
            ),
            dmc.Group(
                [
                    DashIconify(icon="mdi:alert", color="red", width=20),
                    dmc.Text(
                        f"{values[2]:,} Population at risk",
                        style={"fontSize": "0.7vw"},
                    ),
                ],
                gap="xs",
            ),
        ],
        gap="sm",
        style={"marginTop": "20px"},
    )


def make_navbar(user, values):
    return dmc.Container(
        [
            # Welcome Text
            make_welcome(user),
            # Status Indicators
            make_status_indicators(values),
            # Interactive Options
            interactive_options,
        ],
        fluid=True,
        px=0,
        style={
            "width": "100%",
            "height": "100%",
        },
    )


# Single-page dashboard layout
def make_single_page_layout():
    """Create the three-panel single-page dashboard layout"""
    return dmc.Grid(
        [
            # Left Panel - Configuration
            dmc.GridCol(
                [
                    dmc.Paper(
                        [
                            # Step 1: Country Selection
                            dmc.Paper(
                                [
                                    dmc.Group([
                                        dmc.Badge("1", size="sm", color="#1cabe2", variant="filled"),
                                        dmc.Text("COUNTRY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                                    ], mb="xs", justify="flex-start"),
                                    dmc.Select(
                                        id="country-select",
                                        placeholder="Select country...",
                                        data=[
                                            {"value": "VNM", "label": "Vietnam"}, 
                                            {"value": "DOM", "label": "Dominican Republic"},
                                            {"value": "NIC", "label": "Nicaragua"}
                                        ],
                                        value="NIC",
                                        mb="xs"
                                    )
                                ],
                                p="sm",
                                shadow="xs",
                                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "12px"}
                            ),
                            
                            # Step 2: Hurricane Exploration (Snowflake Data)
                            dmc.Paper(
                                [
                                    dmc.Group([
                                        dmc.Badge("2", size="sm", color="#1cabe2", variant="filled"),
                                        dmc.Text("HURRICANE", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}),
                                    ], justify="flex-start", gap="sm", mb="xs"),
                                    
                                    # Forecast Selection (compact)
                                    dmc.Select(
                                        id="forecast-date",
                                        placeholder="Select forecast date...",
                                        data=[],  # Will be populated dynamically
                                        value=None,
                                        mb="xs"
                                    ),
                                    dmc.Select(
                                        id="forecast-time",
                                        placeholder="Select forecast time...",
                                        data=[],  # Will be populated based on selected date
                                        value=None,
                                        mb="xs"
                                    ),
                                    dmc.Select(
                                        id="storm-select",
                                        placeholder="Select hurricane...",
                                        data=[],  # Will be populated based on date and time
                                        value=None,
                                        mb="xs"
                                    ),
                                    
                                    # Wind Threshold (compact)
                                    dmc.Select(
                                        id="wind-threshold-select",
                                        placeholder="Select wind threshold...",
                                        data=[
                                            {"value": "34", "label": "34kt - Tropical storm force (17.49 m/s)"},
                                            {"value": "40", "label": "40kt - Strong tropical storm (20.58 m/s)"},
                                            {"value": "50", "label": "50kt - Very strong tropical storm (25.72 m/s)"},
                                            {"value": "64", "label": "64kt - Category 1 hurricane (32.92 m/s)"},
                                            {"value": "83", "label": "83kt - Category 2 hurricane (42.70 m/s)"},
                                            {"value": "96", "label": "96kt - Category 3 hurricane (49.39 m/s)"},
                                            {"value": "113", "label": "113kt - Category 4 hurricane (58.12 m/s)"},
                                            {"value": "137", "label": "137kt - Category 5 hurricane (70.48 m/s)"}
                                        ],
                                        value="50",
                                        mb="xs"
                                    ),
                                    
                                ],
                                p="sm",
                                shadow="xs",
                                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "12px"}
                            ),
                            
                            # Step 3: Load Layers Button
                            dmc.Paper(
                                [
                                    dmc.Group([
                                        dmc.Badge("3", size="sm", color="#1cabe2", variant="filled"),
                                        dmc.Text("LOAD LAYERS", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}),
                                    ], justify="flex-start", gap="sm", mb="sm"),
                                    
                                    dmc.Text("Load all available data layers for the selected hurricane", size="xs", c="dimmed", mb="md"),
                                    
                                    dmc.Button(
                                        "Load Layers",
                                        id="load-layers-btn",
                                        leftSection=DashIconify(icon="carbon:download", width=20),
                                        variant="filled",
                                        color="#1cabe2",
                                        fullWidth=True,
                                        mb="md"
                                    ),
                                    
                                    dmc.Text("Status: Not loaded", id="load-status", size="xs", c="dimmed", mb="md")
                                ],
                                p="md",
                                shadow="xs",
                                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
                            ),
                            
                            # Step 4: Layer Controls
                            dmc.Paper(
                                [
                                    dmc.Group([
                                        dmc.Badge("4", size="sm", color="#1cabe2", variant="filled"),
                                        dmc.Text("LAYER CONTROLS", size="sm", fw=700, c="dark", ta="left", style={"letterSpacing": "0.5px"}),
                                    ], justify="flex-start", gap="sm", mb="sm"),
                                    
                                    dmc.Text("Toggle layers on/off to explore different data types", size="xs", c="dimmed", mb="md"),
                                    
                                    # Layer Selection
                                    dmc.Stack([
                                        dmc.Text("Hurricane Data", size="sm", fw=600, mb="xs"),
                                        dmc.Checkbox(id="hurricane-tracks-toggle", label="Hurricane Tracks", checked=False, mb="xs", disabled=True),
                                        dmc.Checkbox(id="hurricane-envelopes-toggle", label="Hurricane Envelopes", checked=False, mb="xs", disabled=True),
                                        
                                        dmc.Text("Impact Data", size="sm", fw=600, mb="xs", mt="md"),
                                        dmc.Checkbox(id="schools-layer", label="Schools Impact", checked=False, mb="xs", disabled=True),
                                        dmc.Checkbox(id="health-layer", label="Health Centers Impact", checked=False, mb="xs", disabled=True),
                                        dmc.Checkbox(id="tiles-layer", label="Population Tiles", checked=False, mb="xs", disabled=True),
                                        
                                        dmc.Text("Note: Load layers first to enable toggles", size="xs", c="dimmed", mb="md")
                                    ])
                                ],
                                p="md",
                                shadow="xs",
                                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
                            ),
                            
                        ],
                        p="md",
                        shadow="sm"
                    )
                ],
                span=3,
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"}
            ),
            
            # Center Panel - Map (Dash Leaflet with all layers)
            dmc.GridCol(
                html.Div([
                    dcc.Store(id="map-state-store", data={}),
                    dcc.Store(id="envelope-data-store", data={}),
                    dcc.Store(id="schools-data-store", data={}),
                    dcc.Store(id="health-data-store", data={}),
                    dcc.Store(id="tiles-data-store", data={}),
                    dcc.Store(id="tracks-data-store", data={}),
                    dcc.Store(id="layers-loaded-store", data=False),
                    dl.Map(
                        [
                            dl.TileLayer(
                                url=f"https://api.mapbox.com/styles/v1/mapbox/light-v11/tiles/{{z}}/{{x}}/{{y}}?access_token={mapbox_token}",
                                attribution="mapbox",
                            ),
                            # Hurricane Tracks Layer
                            dl.GeoJSON(
                                id="hurricane-tracks-json",
                                data={},
                                zoomToBounds=True,
                                style=style_tracks
                            ),
                            # Hurricane Envelopes Layer
        dl.GeoJSON(
            id="envelopes-json-test",
            data={},
            zoomToBounds=True,
            style=style_envelopes
        ),
                            # Schools Impact Layer
                            dl.GeoJSON(
                                id="schools-json-test",
                                data={},
                                zoomToBounds=True,
                                style=style_schools_health,
                                pointToLayer=point_to_layer_schools_health
                            ),
                            # Health Centers Impact Layer
                            dl.GeoJSON(
                                id="health-json-test",
                                data={},
                                zoomToBounds=True,
                                style=style_schools_health,
                                pointToLayer=point_to_layer_schools_health
                            ),
                            # Population Tiles Layer
                            dl.GeoJSON(
                                id="tiles-json-test",
                                data={},
                                zoomToBounds=True
                            ),
                            dl.FullScreenControl(),
                            dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
                        ],
                        center=[map_config.center["lat"], map_config.center["lon"]],
                        zoom=map_config.zoom,
                        scrollWheelZoom=True,
                        style={
                            "height": "calc(100vh - 147px)",
                            "width": "100%",
                            "position": "relative",
                            "zIndex": 1,
                            "overflow": "hidden"
                        }
                    )
                ]),
                span=6,
                style={"height": "100%", "minHeight": 0}
            ),
            
            # Right Panel - Impact Metrics
            dmc.GridCol(
                [
                    dmc.Paper(
                        [
                            dmc.Text("Impact Summary", size="lg", fw=700, mb="md"),
                            
                            # Scenario Selection
                            dmc.Stack([
                                dmc.Text("Scenario", size="sm", fw=500),
                                dmc.SegmentedControl(
                                    id="scenario-select",
                                    data=[
                                        {"value": "low", "label": "Low"},
                                        {"value": "probabilistic", "label": "Probabilistic"},
                                        {"value": "high", "label": "High"}
                                    ],
                                    value="probabilistic",
                                    mb="md"
                                )
                            ]),
                            
                            # Main Impact Number
                            dmc.Center(
                                dmc.Stack([
                                    dmc.Text("Children Affected", size="sm", ta="center"),
                                    dmc.Text("0", id="children-affected", size="xl", fw=700, ta="center", c="red")
                                ]),
                                mb="lg"
                            ),
                            
                            # Critical Infrastructure
                            dmc.Stack([
                                dmc.Text("Critical Infrastructure", size="sm", fw=500, mb="sm"),
                                dmc.Group([
                                    dmc.Text("Schools:", size="sm"),
                                    dmc.Text("0", id="schools-count", size="sm", fw=500)
                                ], justify="space-between"),
                                dmc.Group([
                                    dmc.Text("Health Centers:", size="sm"),
                                    dmc.Text("0", id="health-count", size="sm", fw=500)
                                ], justify="space-between"),
                                dmc.Group([
                                    dmc.Text("Population at Risk:", size="sm"),
                                    dmc.Text("0", id="population-count", size="sm", fw=500)
                                ], justify="space-between"),
                            ])
                        ],
                        p="md",
                        shadow="sm"
                    )
                ],
                span=3,
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"}
            )
        ],
        gutter="md",
        style={"height": "100%", "margin": 0}
    )

# Callbacks for interactive functionality
@callback(
    [Output("children-affected", "children"),
     Output("schools-count", "children"),
     Output("health-count", "children"),
     Output("population-count", "children")],
    [Input("storm-select", "value"),
     Input("wind-threshold-select", "value"),
     Input("scenario-select", "value"),
     Input("country-select", "value"),
     Input("forecast-date", "value"),
     Input("forecast-time", "value")]
)
def update_impact_metrics(storm, wind_threshold, scenario, country, forecast_date, forecast_time):
    """Update impact metrics based on storm, wind threshold, scenario, and country selection"""
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        return "0", "0", "0", "0"
    
    try:
        # Construct the filename for the tiles impact view (like tiles layer callback)
        date_str = forecast_date.replace('-', '')  # Convert "2025-10-15" to "20251015"
        time_str = forecast_time.replace(':', '')  # Convert "00:00" to "0000"
        forecast_datetime = f"{date_str}{time_str}00"  # Add seconds: "20251015000000"
        
        filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}_15.parquet"
        filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "mercator_views", filename)
        
        if os.path.exists(filepath):
            gdf = read_dataset(giga_store, filepath)
            
            # Calculate scenario-based metrics from the tiles data
            if scenario == "low":
                # Use lowest impact values (lowest wind threshold)
                children_affected = gdf['school_age_population'].sum() if 'school_age_population' in gdf.columns else 0
                schools_count = gdf['num_schools'].sum() if 'num_schools' in gdf.columns else 0
                health_count = gdf['num_hcs'].sum() if 'num_hcs' in gdf.columns else 0
                population_count = gdf['population'].sum() if 'population' in gdf.columns else 0
            elif scenario == "high":
                # Use highest impact values (highest wind threshold)
                children_affected = gdf['school_age_population'].sum() if 'school_age_population' in gdf.columns else 0
                schools_count = gdf['num_schools'].sum() if 'num_schools' in gdf.columns else 0
                health_count = gdf['num_hcs'].sum() if 'num_hcs' in gdf.columns else 0
                population_count = gdf['population'].sum() if 'population' in gdf.columns else 0
            else:  # probabilistic
                # Use current wind threshold values
                children_affected = gdf['school_age_population'].sum() if 'school_age_population' in gdf.columns else 0
                schools_count = gdf['num_schools'].sum() if 'num_schools' in gdf.columns else 0
                health_count = gdf['num_hcs'].sum() if 'num_hcs' in gdf.columns else 0
                population_count = gdf['population'].sum() if 'population' in gdf.columns else 0
            
            return (
                f"{children_affected:,.0f}",
                f"{schools_count:,.0f}",
                f"{health_count:,.0f}",
                f"{population_count:,.0f}"
            )
        else:
            return "0", "0", "0", "0"
            
    except Exception as e:
        print(f"Error updating metrics: {e}")
        return "0", "0", "0", "0"

# Function to get envelope data from Snowflake (matching the working envelopes.py)
# Callback to populate available forecast dates
@callback(
    Output("forecast-date", "data"),
    Output("forecast-date", "value"),
    Input("country-select", "value"),
    prevent_initial_call=False
)
def update_forecast_dates(country):
    """Get available forecast dates from pre-loaded data and set most recent as default"""
    print(f"update_forecast_dates called with country: {country}")
    
    if not metadata_df.empty:
        # Format dates and create options (like hurricanes page)
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
        print(f"Returning {len(date_options)} date options from pre-loaded data: {date_options}")
        print(f"Default date (most recent): {default_date}")
        return date_options, default_date
    else:
        print("No metadata available, returning fallback dates")
        # Return some fallback data for testing
        fallback_dates = [
            {"value": "2025-10-20", "label": "Oct 20, 2025"},
            {"value": "2025-10-15", "label": "Oct 15, 2025"},
            {"value": "2025-10-10", "label": "Oct 10, 2025"},
            {"value": "2025-10-05", "label": "Oct 05, 2025"},
            {"value": "2025-09-30", "label": "Sep 30, 2025"}
        ]
        return fallback_dates, fallback_dates[0]['value']

# Callback to update forecast times based on selected date
@callback(
    Output("forecast-time", "data"),
    Output("forecast-time", "value", allow_duplicate=True),
    [Input("forecast-date", "value")],
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
    print(f"Forecast times for {selected_date}: available={available_times}, options={len(time_options)}, default (most recent)={default_time}")
    return time_options, default_time


# Note: Storm selection is now handled directly in update_storm_options callback

@callback(
    Output("storm-select", "data"),
    Output("storm-select", "value", allow_duplicate=True),
    [Input("country-select", "value"),
     Input("forecast-date", "value"),
     Input("forecast-time", "value")],
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
    print(f"Storms for {forecast_date} {forecast_time}: available={available_storms}, options={len(storm_options)}, default (most recent)={default_storm}")
    return storm_options, default_storm

# Callback to update wind threshold options based on storm, date, and time selection
@callback(
    Output("wind-threshold-select", "data"),
    Output("wind-threshold-select", "value", allow_duplicate=True),
    [Input("storm-select", "value"),
     Input("forecast-date", "value"),
     Input("forecast-time", "value")],
    prevent_initial_call='initial_duplicate'
)
def update_wind_threshold_options(storm, date, time):
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
        return all_thresholds, "50"  # Default to 50kt
    
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
        
        # Set default to most recent available threshold (highest available)
        default_threshold = None
        if available_thresholds:
            # Sort available thresholds numerically and take the highest
            sorted_thresholds = sorted([int(t) for t in available_thresholds], reverse=True)
            default_threshold = str(sorted_thresholds[0])
        else:
            default_threshold = "50"  # Fallback default
        
        print(f"Wind thresholds for {storm} at {forecast_datetime}: available={available_thresholds}, default (highest)={default_threshold}")
        return options, default_threshold
        
    except Exception as e:
        print(f"Error getting wind threshold options: {e}")
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

# Use the standard header with Last Updated timestamp (now included in main header)
def make_custom_header():
    """Use the standard header which now includes Last Updated timestamp"""
    from components.ui.header import make_header
    return make_header(active_tab="tab-home")


# Create single-page appshell using existing components
def make_single_page_appshell():
    """Create appshell with custom header using existing footer and appshell structure"""
    from components.ui.footer import footer
    
    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_custom_header(), px=15, zIndex=2000),
            dmc.AppShellMain(
                make_single_page_layout(),
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "hidden"}
            ),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id="single-page-shell",
        header={"height": "67"},
        padding=0,  # Remove all padding
        footer={"height": "80"},
    )

# Use the single-page appshell
layout = make_single_page_appshell()

# Load all layers callback
@callback(
    [Output('tracks-data-store', 'data'),
     Output('envelope-data-store', 'data'),
     Output('schools-data-store', 'data'),
     Output('health-data-store', 'data'),
     Output('tiles-data-store', 'data'),
     Output('layers-loaded-store', 'data'),
     Output('load-status', 'children'),
     Output('hurricane-tracks-toggle', 'disabled'),
     Output('hurricane-envelopes-toggle', 'disabled'),
     Output('schools-layer', 'disabled'),
     Output('health-layer', 'disabled'),
     Output('tiles-layer', 'disabled')],
    [Input('load-layers-btn', 'n_clicks')],
    State('country-select', 'value'),
    State('storm-select', 'value'),
    State('forecast-date', 'value'),
    State('forecast-time', 'value'),
    State('wind-threshold-select', 'value'),
    prevent_initial_call=True
)
def load_all_layers(n_clicks, country, storm, forecast_date, forecast_time, wind_threshold):
    """Load all available layers when Load Layers button is clicked"""
    print(f"Loading all layers for {country}_{storm}_{forecast_date}_{forecast_time}_{wind_threshold}")
    
    if not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        return {}, {}, {}, {}, {}, False, "Status: Missing selections", True, True, True, True, True
    
    try:
        # Initialize empty data stores
        tracks_data = {}
        envelope_data = {}
        schools_data = {}
        health_data = {}
        tiles_data = {}
        
        # Load Hurricane Tracks
        try:
            conn = get_snowflake_connection()
            forecast_datetime = f"{forecast_date} {forecast_time}:00"
            
            print(f"Loading tracks for storm={storm}, forecast_time={forecast_datetime}")
            
            query = '''
            SELECT 
                ENSEMBLE_MEMBER,
                VALID_TIME,
                LEAD_TIME,
                LATITUDE,
                LONGITUDE,
                WIND_SPEED_KNOTS,
                PRESSURE_HPA
            FROM TC_TRACKS
            WHERE TRACK_ID = %s AND FORECAST_TIME = %s
            ORDER BY ENSEMBLE_MEMBER, VALID_TIME
            '''
            
            df_tracks = pd.read_sql(query, conn, params=[storm, forecast_datetime])
            conn.close()
            
            if not df_tracks.empty:
                # Create LineString features for each ensemble member
                features = []
                for member in df_tracks['ENSEMBLE_MEMBER'].unique():
                    member_data = df_tracks[df_tracks['ENSEMBLE_MEMBER'] == member].sort_values('LEAD_TIME')
                    coordinates = [[row['LONGITUDE'], row['LATITUDE']] for _, row in member_data.iterrows()]
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coordinates
                        },
                        "properties": {
                            "ensemble_member": member,
                            "member_type": "control" if member in [51, 52] else "ensemble"
                        }
                    }
                    features.append(feature)
                
                tracks_data = {
                    "type": "FeatureCollection",
                    "features": features
                }
        except Exception as e:
            print(f"Error loading tracks: {e}")
        
        # Load Hurricane Envelopes
        try:
            envelope_df = get_envelope_data_snowflake(storm, forecast_datetime)
            
            if not envelope_df.empty:
                envelope_data = {
                    'track_id': storm,
                    'forecast_time': forecast_datetime,
                    'data': envelope_df.to_dict('records')
                }
        except Exception as e:
            print(f"Error loading envelopes: {e}")
        
        # Load Impact Data (if files exist)
        try:
            date_str = forecast_date.replace('-', '')
            time_str = forecast_time.replace(':', '')
            forecast_datetime_str = f"{date_str}{time_str}00"
            
            print(f"Looking for impact data files with pattern: {country}_{storm}_{forecast_datetime_str}_{wind_threshold}")
            
            # Schools
            schools_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            schools_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', schools_file)
            if giga_store.file_exists(schools_path):
                try:
                    gdf_schools = read_dataset(giga_store, schools_path)
                    df_schools = gdf_schools.drop(columns=['geometry'])
                    schools_data = df_schools.to_dict("records")#__geo_interface__
                except Exception as e:
                    print(f"Error reading schools file: {e}")
                    schools_data = {}
            
            # Health Centers
            health_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            health_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', health_file)
            if giga_store.file_exists(health_path):
                try:
                    gdf_health = read_dataset(giga_store, health_path)
                    health_data = gdf_health.__geo_interface__
                except Exception as e:
                    print(f"Error reading health file: {e}")
                    health_data = {}
            
            # Tiles
            tiles_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_15.parquet"
            tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', tiles_file)
            if giga_store.file_exists(tiles_path):
                try:
                    gdf_tiles = read_dataset(giga_store, tiles_path)
                    tiles_data = gdf_tiles.__geo_interface__
                except Exception as e:
                    print(f"Error reading tiles file: {e}")
                    tiles_data = {}
                
        except Exception as e:
            print(f"Error loading impact data: {e}")
        
        # Enable all toggles
        return (tracks_data, envelope_data, schools_data, health_data, tiles_data, 
                True, "Status: All layers loaded successfully", 
                False, False, False, False, False)
        
    except Exception as e:
        print(f"Error in load_all_layers: {e}")
        return {}, {}, {}, {}, {}, False, f"Status: Error loading layers: {str(e)}", True, True, True, True, True

# Simple toggle callbacks - just show/hide pre-loaded data
@callback(
    Output("hurricane-tracks-json", "data"),
    Output("hurricane-tracks-json", "zoomToBounds"),
    [Input("hurricane-tracks-toggle", "checked")],
    State("tracks-data-store", "data"),
    prevent_initial_call=False
)
def toggle_tracks_layer(checked, tracks_data):
    """Toggle hurricane tracks layer visibility"""
    if checked and tracks_data:
        return tracks_data, True
    return {"type": "FeatureCollection", "features": []}, False

@callback(
    Output("envelopes-json-test", "data"),
    Output("envelopes-json-test", "zoomToBounds"),
    [Input("hurricane-envelopes-toggle", "checked")],
    State("envelope-data-store", "data"),
    State("wind-threshold-select", "value"),
    prevent_initial_call=False
)
def toggle_envelopes_layer(checked, envelope_data, wind_threshold):
    """Toggle hurricane envelopes layer visibility"""
    if not checked or not envelope_data or not envelope_data.get('data'):
        return {"type": "FeatureCollection", "features": []}, False
    
    try:
        df = pd.DataFrame(envelope_data['data'])
        if df.empty:
            return {"type": "FeatureCollection", "features": []}, False
        
        # Filter by wind threshold
        if wind_threshold:
            wth_int = int(wind_threshold)
            df = df[df['wind_threshold'] == wth_int]
        
        if df.empty:
            return {"type": "FeatureCollection", "features": []}, False
        
        # Convert to GeoDataFrame
        if df['geometry'].iloc[0].startswith('{'):
            from shapely.geometry import shape
            import json
            geometries = []
            for geom_str in df['geometry']:
                geom_dict = json.loads(geom_str)
                geometries.append(shape(geom_dict))
            gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries)
        else:
            gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))
        
        return gdf.__geo_interface__, True
        
    except Exception as e:
        print(f"Error toggling envelopes: {e}")
        return {"type": "FeatureCollection", "features": []}, False

@callback(
    Output("schools-json-test", "data"),
    Output("schools-json-test", "zoomToBounds"),
    [Input("schools-layer", "checked")],
    State("schools-data-store", "data"),
    prevent_initial_call=False
)
def toggle_schools_layer(checked, schools_data):
    """Toggle schools layer visibility with probability-based coloring"""
    if not checked or not schools_data:
        return {"type": "FeatureCollection", "features": []}, False
    
    try:
        # Add probability-based styling to each feature
        if 'features' in schools_data:
            for feature in schools_data['features']:
                if 'properties' in feature and 'probability' in feature['properties']:
                    prob = feature['properties']['probability']
                    
                    # Color scale: grey (0%) -> yellow -> orange -> red with 7 granular classes
                    if prob == 0 or prob is None:
                        color = '#808080'  # Grey for no impact
                    elif prob <= 0.15:
                        color = '#FFFF00'  # Yellow for very low impact
                    elif prob <= 0.30:
                        color = '#FFD700'  # Gold for low impact
                    elif prob <= 0.45:
                        color = '#FFA500'  # Orange for low-medium impact
                    elif prob <= 0.60:
                        color = '#FF8C00'  # Dark orange for medium impact
                    elif prob <= 0.75:
                        color = '#FF4500'  # Orange-red for high impact
                    elif prob <= 0.90:
                        color = '#DC143C'  # Crimson for very high impact
                    else:
                        color = '#8B0000'  # Dark red for extreme impact
                    
                    radius = 5  # Fixed radius for all markers
                    
                    # Add styling properties
                    feature['properties']['_color'] = color
                    feature['properties']['_radius'] = radius
                    feature['properties']['_opacity'] = 0.8
                    feature['properties']['_weight'] = 1
                    feature['properties']['_fillOpacity'] = 0.7


        df_schools = pd.DataFrame(schools_data)
        gdf_schools = convert_to_geodataframe(df_schools)
        
        return gdf_schools.__geo_interface__, True
        return schools_data, True
        
    except Exception as e:
        print(f"Error styling schools layer: {e}")
        return schools_data, True

@callback(
    Output("health-json-test", "data"),
    Output("health-json-test", "zoomToBounds"),
    [Input("health-layer", "checked")],
    State("health-data-store", "data"),
    prevent_initial_call=False
)
def toggle_health_layer(checked, health_data):
    """Toggle health centers layer visibility with probability-based coloring"""
    if not checked or not health_data:
        return {"type": "FeatureCollection", "features": []}, False
    
    try:
        # Add probability-based styling to each feature
        if 'features' in health_data:
            for feature in health_data['features']:
                if 'properties' in feature and 'probability' in feature['properties']:
                    prob = feature['properties']['probability']
                    
                    # Color scale: grey (0%) -> yellow -> orange -> red with 7 granular classes
                    if prob == 0 or prob is None:
                        color = '#808080'  # Grey for no impact
                    elif prob <= 0.15:
                        color = '#FFFF00'  # Yellow for very low impact
                    elif prob <= 0.30:
                        color = '#FFD700'  # Gold for low impact
                    elif prob <= 0.45:
                        color = '#FFA500'  # Orange for low-medium impact
                    elif prob <= 0.60:
                        color = '#FF8C00'  # Dark orange for medium impact
                    elif prob <= 0.75:
                        color = '#FF4500'  # Orange-red for high impact
                    elif prob <= 0.90:
                        color = '#DC143C'  # Crimson for very high impact
                    else:
                        color = '#8B0000'  # Dark red for extreme impact
                    
                    radius = 5  # Fixed radius for all markers
                    
                    # Add styling properties
                    feature['properties']['_color'] = color
                    feature['properties']['_radius'] = radius
                    feature['properties']['_opacity'] = 0.8
                    feature['properties']['_weight'] = 1
                    feature['properties']['_fillOpacity'] = 0.7
        
        return health_data, True
        
    except Exception as e:
        print(f"Error styling health layer: {e}")
        return health_data, True

@callback(
    Output("tiles-json-test", "data"),
    Output("tiles-json-test", "zoomToBounds"),
    [Input("tiles-layer", "checked")],
    State("tiles-data-store", "data"),
    prevent_initial_call=False
)
def toggle_tiles_layer(checked, tiles_data):
    """Toggle tiles layer visibility"""
    if checked and tiles_data:
        return tiles_data, True
    return {"type": "FeatureCollection", "features": []}, False





# Register the page as the home page
dash.register_page(__name__, path="/", name="Ahead of the Storm")

