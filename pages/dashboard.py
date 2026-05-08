import math
import numpy as np
import pandas as pd
import dash
from dash import Output, Input, State, callback, dcc, html, callback_context, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_leaflet as dl
import geopandas as gpd
import os
import warnings
import json
from shapely import wkt
import copy
import hashlib
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor
import time

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config
from components.ui.styling import all_colors, create_legend_divs, update_tile_features, precompute_all_colors, compute_layer_stats
from components.map.javascript import (
    style_tracks, style_tiles, point_to_layer_schools_health,
    style_envelopes, tooltip_tracks, tooltip_envelopes,
    tooltip_schools, tooltip_health, tooltip_tiles, tooltip_admin,
    tooltip_shelters, tooltip_wash
)

from dash_extensions.javascript import assign

# Import map config early for use in constants
from components.map.map_config import map_config, mapbox_token, get_tile_layer_url

# Import Snowflake utilities for country loading
from components.data.snowflake_utils import (
    get_active_countries, get_available_wind_thresholds, get_latest_forecast_time_overall,
    get_snowflake_connection, get_envelope_data_snowflake, get_snowflake_data,
    get_lat_lons, get_lat_lons_bulk,
    get_base_tiles, get_base_schools, get_base_hcs, get_base_shelters, get_base_wash, get_base_admin,
)

#### Constant - add as selector at some point
ZOOM_LEVEL = 14

# Run the three independent startup queries in parallel — reduces cold-start time
# from ~3× a single query to ~1× (they hit different tables and use separate connections).
with ThreadPoolExecutor(max_workers=3) as _startup_pool:
    _f_countries = _startup_pool.submit(get_active_countries)
    _f_metadata  = _startup_pool.submit(get_snowflake_data)
    _f_latlons   = _startup_pool.submit(get_lat_lons_bulk)
    countries_df = _f_countries.result()
    metadata_df  = _f_metadata.result()
    _latlon_bulk = _f_latlons.result()

# Build country-specific map centers and zoom levels from Snowflake data
COUNTRY_MAP_CONFIG = {}
if not countries_df.empty:
    for _, row in countries_df.iterrows():
        country_code = row['COUNTRY_CODE']
        center_lat = row['CENTER_LAT'] if pd.notna(row['CENTER_LAT']) else map_config.center["lat"]
        center_lon = row['CENTER_LON'] if pd.notna(row['CENTER_LON']) else map_config.center["lon"]
        view_zoom = row['VIEW_ZOOM'] if pd.notna(row['VIEW_ZOOM']) else map_config.zoom
        
        COUNTRY_MAP_CONFIG[country_code] = {
            "center": [center_lat, center_lon],
            "zoom": int(view_zoom) if pd.notna(view_zoom) else map_config.zoom
        }
    print(f"✓ Loaded {len(COUNTRY_MAP_CONFIG)} countries into COUNTRY_MAP_CONFIG")
else:
    print("⚠ No countries loaded - using default map config only")

# Default map config if country not found
DEFAULT_MAP_CONFIG = {"center": [map_config.center["lat"], map_config.center["lon"]], "zoom": map_config.zoom}

# Build country options list for dropdowns
COUNTRY_OPTIONS = []
REGION_MEMBERS = {}  # {'ECA': [{'value': 'AIA', 'label': 'Anguilla'}, ...]}


def _get_base_multi(fn, country, *args):
    """Call a get_base_* function for a country or each member of a region, then concat.

    Individual per-country results are lru_cached, so repeated calls are free.
    Returns same type as the single-country function (DataFrame or GeoDataFrame).
    """
    codes = ([item['value'] for item in REGION_MEMBERS[country]]
             if country in REGION_MEMBERS else [country])
    frames = [fn(c, *args) for c in codes]
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return frames[0]
    result = pd.concat(non_empty, ignore_index=True)
    if isinstance(non_empty[0], gpd.GeoDataFrame):
        result = gpd.GeoDataFrame(result, geometry='geometry', crs=non_empty[0].crs)
    return result


if not countries_df.empty:
    sql_mode = config.IMPACT_DATA_SOURCE == 'SQL'

    regions   = countries_df[countries_df['IS_REGION'] == True]
    countries = countries_df[countries_df['IS_REGION'] != True]

    # Build region member options for the drill-down selector
    code_to_name = dict(zip(countries_df['COUNTRY_CODE'], countries_df['COUNTRY_NAME']))
    if sql_mode:
        for _, row in regions.iterrows():
            members = row.get('MEMBER_CODES')
            if members:
                if isinstance(members, str):
                    members = json.loads(members)
                REGION_MEMBERS[row['COUNTRY_CODE']] = [
                    {"value": m, "label": code_to_name.get(m, m)} for m in members
                ]

    # Exclude region member countries from the main dropdown (accessible via drill-down)
    member_codes = {item['value'] for opts in REGION_MEMBERS.values() for item in opts}
    standalone = countries[~countries['COUNTRY_CODE'].isin(member_codes)]
    country_items = [{"value": r['COUNTRY_CODE'], "label": r['COUNTRY_NAME']} for _, r in standalone.iterrows()]

    if sql_mode and not regions.empty:
        region_items = [{"value": r['COUNTRY_CODE'], "label": r['COUNTRY_NAME']} for _, r in regions.iterrows()]
        COUNTRY_OPTIONS = [
            {"group": "Regions",   "items": region_items},
            {"group": "Countries", "items": country_items},
        ]
    else:
        COUNTRY_OPTIONS = country_items

    all_codes = countries_df['COUNTRY_CODE'].tolist()
    DEFAULT_COUNTRY = "JAM" if "JAM" in all_codes else (all_codes[0] if all_codes else None)
else:
    DEFAULT_COUNTRY = None
    print("No country options available - country dropdown will be empty")




# =============================================================================
# SECTION 1: DATA LOADING AND METADATA
# =============================================================================
# Initialize data store and load hurricane metadata

from components.ui.appshell import make_default_appshell
import dash_leaflet as dl
import geopandas as gpd
from components.map.home_map import make_empty_map

#### Metadata 
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.processing.geo import convert_to_geodataframe
from components.data.data_store_utils import get_data_store, get_impact_data

##### env variables #####
RESULTS_DIR = config.RESULTS_DIR or "results"
VIEWS_DIR = config.VIEWS_DIR or "aos_views"
ROOT_DATA_DIR = config.ROOT_DATA_DIR or "geodb"
#########################

# Initialize data store using centralized utility
data_store = get_data_store()
giga_store = data_store
##############################################

###########################################

###### Process metadata loaded during parallel startup
# Parse dates and times from metadata
metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date
metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')

# Get unique dates and times
unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
unique_times = sorted(metadata_df['TIME'].unique())

#### Get current hurricanes
latest = (metadata_df.assign(dt=pd.to_datetime(metadata_df["DATE"].astype(str) + " " + metadata_df["TIME"]))
            .sort_values(["TRACK_ID","dt"])
            .drop_duplicates("TRACK_ID", keep="last"))

latest['TRACK_ID'] = latest['TRACK_ID'].astype(str)
_latlon_bulk['TRACK_ID'] = _latlon_bulk['TRACK_ID'].astype(str)
latest = latest.merge(_latlon_bulk[['TRACK_ID', 'FORECAST_TIME', 'latitude', 'longitude']],
                      on=['TRACK_ID', 'FORECAST_TIME'], how='left')

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




# =============================================================================
# SECTION 2: LAYOUT CREATION
# =============================================================================

######## Sections as variables

# Step 1: Country Selection
country_selection = dmc.Paper([
                        dmc.Group([
                            dmc.Badge("1", size="sm", color="#1cabe2", variant="filled"),
                            dmc.Text("COUNTRY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                        ], mb="xs", justify="flex-start"),
                        dmc.Select(
                            id="country-select",
                            placeholder="Select country...",
                            data=COUNTRY_OPTIONS,
                            value=DEFAULT_COUNTRY,
                            mb="xs"
                        ),
                        dmc.Select(
                            id="individual-country-select",
                            placeholder="Individual country (optional)...",
                            data=[],
                            value=None,
                            clearable=True,
                            style={"display": "none"},
                        ),
                    ],
                    p="sm",
                    shadow="xs",
                    style={"borderLeft": "3px solid #1cabe2", "marginBottom": "12px"}
                )

# Step 2: Hurricane Exploration (Snowflake Data)
hurricane_exploration = dmc.Paper([
                            dmc.Group([
                                dmc.Badge("2", size="sm", color="#1cabe2", variant="filled"),
                                dmc.Text("HURRICANE", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}),
                            ], justify="flex-start", gap="sm", mb="xs"),
                            
                            # Forecast Selection (compact)
                            dmc.Select(
                                id="forecast-date",
                                placeholder="Select forecast date...",
                                data=[],  # Will be populated dynamically
                                value=None,  # Let callback set most recent
                                mb="xs"
                            ),
                            dmc.Select(
                                id="forecast-time",
                                placeholder="Select forecast time...",
                                data=[],  # Will be populated based on selected date
                                value=None,  # Let callback set most recent
                                mb="xs"
                            ),
                            dmc.Select(
                                id="storm-select",
                                placeholder="Select hurricane...",
                                data=[],  # Will be populated based on date and time
                                value=None,  # Let callback set most recent
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
                                value="34",
                                mb="xs"
                            ),
                            
                        ],
                        p="sm",
                        shadow="xs",
                        style={"borderLeft": "3px solid #1cabe2", "marginBottom": "12px"}
                    )

# Step 3: Load Layers Button
load_layers_button = dmc.Paper([
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
                            mb="md",
                            loaderProps={"type": "dots"}
                        ),
                        
                        html.Div("Status: Not loaded", id="load-status", style={"fontSize": "12px", "color": "#868e96", "marginBottom": "16px"})
                    ],
                    p="md",
                    shadow="xs",
                    style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
                )

# Hurrican selection
hurricane_selection = dmc.Box([
                        dmc.Text("Hurricane Data", size="sm", fw=600, mb="xs"),
                        dmc.Checkbox(id="hurricane-tracks-toggle", label="Hurricane Tracks", checked=False, mb="xs", disabled=True),
                        dmc.Checkbox(id="hurricane-envelopes-toggle", label="Hurricane Envelopes", checked=False, mb="xs", disabled=True),
                    ],id='hurrican_selection_box')

# Infrastructure/poi selection
infrastructure_impact = dmc.Box([
                            dmc.Text("Infrastructure Impact", size="sm", fw=600, mb="xs", mt="md"),
                            dmc.Checkbox(id="schools-layer", label="Schools", checked=False, mb="xs", disabled=True),
                            dmc.Grid([
                                dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                                dmc.GridCol(span=8, children=[
                                    html.Div(style={
                                        "width": "100%", 
                                        "height": "10px", 
                                        "background": "linear-gradient(to right, #808080, #FFFF00, #FFD700, #FFA500, #FF8C00, #FF4500, #DC143C, #8B0000)",
                                        "border": "1px solid #ccc",
                                        "borderRadius": "1px"
                                    })
                                ]),
                                dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                            ], id="schools-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                            dmc.Checkbox(id="health-layer", label="Health Centers", checked=False, mb="xs", disabled=True),
                            dmc.Grid([
                                dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                                dmc.GridCol(span=8, children=[
                                    html.Div(style={
                                        "width": "100%", 
                                        "height": "10px", 
                                        "background": "linear-gradient(to right, #808080, #FFFF00, #FFD700, #FFA500, #FF8C00, #FF4500, #DC143C, #8B0000)",
                                        "border": "1px solid #ccc",
                                        "borderRadius": "1px"
                                    })
                                ]),
                                dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                            ], id="health-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                            dmc.Checkbox(id="shelters-layer", label="Shelters", checked=False, mb="xs", disabled=True),
                            dmc.Grid([
                                dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                                dmc.GridCol(span=8, children=[
                                    html.Div(style={
                                        "width": "100%",
                                        "height": "10px",
                                        "background": "linear-gradient(to right, #808080, #FFFF00, #FFD700, #FFA500, #FF8C00, #FF4500, #DC143C, #8B0000)",
                                        "border": "1px solid #ccc",
                                        "borderRadius": "1px"
                                    })
                                ]),
                                dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                            ], id="shelters-infra-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                            dmc.Checkbox(id="wash-layer", label="WASH Facilities", checked=False, mb="xs", disabled=True),
                            dmc.Grid([
                                dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                                dmc.GridCol(span=8, children=[
                                    html.Div(style={
                                        "width": "100%",
                                        "height": "10px",
                                        "background": "linear-gradient(to right, #808080, #FFFF00, #FFD700, #FFA500, #FF8C00, #FF4500, #DC143C, #8B0000)",
                                        "border": "1px solid #ccc",
                                        "borderRadius": "1px"
                                    })
                                ]),
                                dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                            ], id="wash-infra-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                        ],id='infrastructure_impact_box')

# Probability layer for tiles
probability_layer_tiles = dmc.Box([
                        # Discrete probability legend with buckets
                        dmc.Checkbox(id="probability-tiles-layer", label="Impact Probability", checked=False, mb="xs", disabled=True),
                        html.Div(id="probability-legend", children=[
                            dmc.Grid([
                                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-min", children="0%", size="xs", c="dimmed")]),
                                dmc.GridCol(span=9, children=html.Div(
                                    create_legend_divs('probability'),
                                    style={"display": "flex", "width": "100%"}
                                )),
                                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-max", children="100%", size="xs", c="dimmed")]),
                            ], gutter="xs", mb="xs")
                        ], style={"display": "none"}),
                    ],id='probability_layer_tiles_box')

# Probability layer for admin
probability_layer_admin = dmc.Box([
                        # Discrete probability legend with buckets
                        dmc.Checkbox(id="probability-admin-layer", label="Impact Probability", checked=False, mb="xs", disabled=True),
                        html.Div(id="probability-legend-admin", children=[
                            dmc.Grid([
                                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-admin-min", children="0%", size="xs", c="dimmed")]),
                                dmc.GridCol(span=9, children=html.Div(
                                    create_legend_divs('probability'),
                                    style={"display": "flex", "width": "100%"}
                                )),
                                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-admin-max", children="100%", size="xs", c="dimmed")]),
                            ], gutter="xs", mb="xs")
                        ], style={"display": "none"}),
                    ],id='probability_layer_admin_box')

# Tiles radiogroup
tiles_radiogroup = dmc.RadioGroup([
                        dmc.Radio(id="none-tiles-layer", label="No Tile Layer (just Probability)", value="none", mb="xs"),
                        dmc.Radio(id="population-tiles-layer", label="Population", value="population", mb="xs"),
                        dmc.Radio(id="children-total-tiles-layer", label="Children (total)", value="children-total", mb="xs"),
                        dmc.Radio(id="infant-tiles-layer", label=html.Span("Age 0–4", style={"paddingLeft": "12px", "color": "#666", "fontSize": "0.92em"}), value="infant", mb="xs"),
                        dmc.Radio(id="school-age-tiles-layer", label=html.Span("Age 5–14", style={"paddingLeft": "12px", "color": "#666", "fontSize": "0.92em"}), value="school-age", mb="xs"),
                        dmc.Radio(id="adolescent-tiles-layer", label=html.Span("Age 15–19", style={"paddingLeft": "12px", "color": "#666", "fontSize": "0.92em"}), value="adolescent", mb="xs"),
                        dmc.Radio(id="built-surface-tiles-layer", label="Built Surface Area", value="built-surface", mb="xs"),
                        dmc.Radio(id="cci-tiles-layer", label="CCI (Child Cyclone Index)", value="cci", mb="xs"),
                        dmc.Divider(mb="xs", mt="xs"),
                        dmc.Text("Context Data", size="xs", fw=600, c="dimmed", mb="xs", style={"textTransform": "uppercase", "letterSpacing": "1px"}),
                        dmc.Radio(id="settlement-tiles-layer", label="Settlement Classification", value="settlement", mb="xs"),
                        dmc.Radio(id="rwi-tiles-layer", label="Relative Wealth Index", value="rwi", mb="xs"),
                        dmc.Radio(id="moderate-poverty-tiles-layer", label="Moderate Child Poverty Rate", value="moderate-poverty", mb="xs"),
                        dmc.Radio(id="severe-poverty-tiles-layer", label="Severe Child Poverty Rate", value="severe-poverty", mb="xs"),
                        dmc.Divider(mb="xs", mt="xs"),
                    ], id="tiles-layer-group", value="none")

# Legend grids for each layer
tiles_legends = dmc.Box([
                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="population-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="population-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="population-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('children_total'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="children-total-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[
                            dmc.Text(id="infant-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('infant_population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[
                            dmc.Text(id="infant-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="infant-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('school_age_population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="school-age-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('adolescent_population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="adolescent-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-legend-min", children="Min", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('built_surface_m2'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="built-surface-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-legend-min", children="Min", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs(config.CCI_COL),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="cci-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#d3d3d3", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("No Data", size="xs", c="dimmed", ta="center")
                        ]),
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#dda0dd", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("Rural", size="xs", c="dimmed", ta="center")
                        ]),
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#9370db", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("Urban Clusters", size="xs", c="dimmed", ta="center")
                        ]),
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#4b0082", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("Urban Centers", size="xs", c="dimmed", ta="center")
                        ]),
                    ], id="settlement-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text("-1", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('rwi'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text("+1", size="xs", c="dimmed")]),
                    ], id="rwi-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('moderate_poverty_prob'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                    ], id="moderate-poverty-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('severe_poverty_prob'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                    ], id="severe-poverty-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                ], id='tiles_legends_box')

# Admin radiogroup
admin_radiogroup = dmc.RadioGroup([
                        dmc.Radio(id="none-admin-layer", label="No Region Layer (just Probability)", value="none", mb="xs"),
                        dmc.Radio(id="population-admin-layer", label="Population", value="population", mb="xs"),
                        dmc.Radio(id="children-total-admin-layer", label="Children (total)", value="children-total", mb="xs"),
                        dmc.Radio(id="infant-admin-layer", label=html.Span("Age 0–4", style={"paddingLeft": "12px", "color": "#666", "fontSize": "0.92em"}), value="infant", mb="xs"),
                        dmc.Radio(id="school-age-admin-layer", label=html.Span("Age 5–14", style={"paddingLeft": "12px", "color": "#666", "fontSize": "0.92em"}), value="school-age", mb="xs"),
                        dmc.Radio(id="adolescent-admin-layer", label=html.Span("Age 15–19", style={"paddingLeft": "12px", "color": "#666", "fontSize": "0.92em"}), value="adolescent", mb="xs"),
                        dmc.Radio(id="built-surface-admin-layer", label="Built Surface Area", value="built-surface", mb="xs"),
                        dmc.Radio(id="cci-admin-layer", label="CCI (Child Cyclone Index)", value="cci", mb="xs"),
                        dmc.Divider(mb="xs", mt="xs"),
                        dmc.Text("Context Data", size="xs", fw=600, c="dimmed", mb="xs", style={"textTransform": "uppercase", "letterSpacing": "1px"}),
                        dmc.Radio(id="settlement-admin-layer", label="Settlement Classification", value="settlement", mb="xs"),
                        dmc.Radio(id="rwi-admin-layer", label="Relative Wealth Index", value="rwi", mb="xs"),
                        dmc.Radio(id="moderate-poverty-admin-layer", label="Moderate Child Poverty Rate", value="moderate-poverty", mb="xs"),
                        dmc.Radio(id="severe-poverty-admin-layer", label="Severe Child Poverty Rate", value="severe-poverty", mb="xs"),
                        dmc.Divider(mb="xs", mt="xs"),
                    ], id="admin-layer-group", value="none")

# Legend grids for each region
admin_legends = dmc.Box([
                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="population-admin-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="population-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="population-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-admin-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('children_total'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="children-total-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[
                            dmc.Text(id="infant-admin-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('infant_population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[
                            dmc.Text(id="infant-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="infant-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-admin-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('school_age_population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="school-age-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-admin-legend-min", children="0", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('adolescent_population'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="adolescent-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-admin-legend-min", children="Min", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('built_surface_m2'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="built-surface-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-admin-legend-min", children="Min", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs(config.CCI_COL),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-admin-legend-max", children="Max", size="xs", c="dimmed")]),
                    ], id="cci-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#d3d3d3", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("No Data", size="xs", c="dimmed", ta="center")
                        ]),
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#dda0dd", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("Rural", size="xs", c="dimmed", ta="center")
                        ]),
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#9370db", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("Urban Clusters", size="xs", c="dimmed", ta="center")
                        ]),
                        dmc.GridCol(span=3, children=[
                            html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#4b0082", "border": "1px solid #ccc", "borderRadius": "1px"}),
                            dmc.Text("Urban Centers", size="xs", c="dimmed", ta="center")
                        ]),
                    ], id="settlement-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text("-1", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('rwi'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text("+1", size="xs", c="dimmed")]),
                    ], id="rwi-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('moderate_poverty_prob'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                    ], id="moderate-poverty-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                    dmc.Grid([
                        dmc.GridCol(span=1.5, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                        dmc.GridCol(span=9, children=html.Div(
                            create_legend_divs('severe_poverty_prob'),
                            style={"display": "flex", "width": "100%"}
                        )),
                        dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                    ], id="severe-poverty-admin-legend", style={"display": "none"}, gutter="xs", mb="xs"),

                ], id='admin_legends_box')

# Unified Population & Infrastructure section with mode selector
population_infrastructure_selection = dmc.Box([
                    dmc.Text("Population & Infrastructure", size="sm", fw=600, mb="xs", mt="md"),
                    
                    # Mode selector: Tiles vs By Region
                    dmc.SegmentedControl(
                        id="layer-mode-selector",
                        value="tiles",
                        data=[
                            {"value": "tiles", "label": "Tiles (Rasters)"},
                            {"value": "admin", "label": "By Region"}
                        ],
                        mb="md",
                        fullWidth=True
                    ),
                    
                    # Tiles mode components
                    dmc.Box([
                        probability_layer_tiles,
                        dmc.Space(h="xl"),
                        tiles_radiogroup,
                        tiles_legends,
                    ], id="tiles-mode-box"),
                    
                    # Admin mode components
                    dmc.Box([
                        probability_layer_admin,
                        dmc.Space(h="xl"),
                        admin_radiogroup,
                        admin_legends,
                    ], id="admin-mode-box", style={"display": "none"}),

                ],id='population_infrastructure_selection_box')

# Layer Selection
layer_selection = dmc.Stack([

                        hurricane_selection,

                        infrastructure_impact,

                        html.Div(id="layer-no-data-warning"),

                        population_infrastructure_selection,
                        
                        # Disclaimer
                        dmc.Text('Note: When "Impact Probability" is enabled, "Population Density", "School-Age Population", "Adolescent Population (Age 15–19)", and "Built Surface Area" show expected impact (base value × probability). Context Data layers cannot be selected when "Impact Probability" is active.', size="xs", c="dimmed", mb="md", mt="md")
                    ],id='layer_selection_stack')

# Step 4: Layer Controls
layers_controls = dmc.Paper([
                    dmc.Group([
                        dmc.Badge("4", size="sm", color="#1cabe2", variant="filled"),
                        dmc.Text("LAYER CONTROLS", size="sm", fw=700, c="dark", ta="left", style={"letterSpacing": "0.5px"}),
                    ], justify="flex-start", gap="sm", mb="sm"),
                    
                    dmc.Text("Toggle layers on/off to explore different data types", size="xs", c="dimmed", mb="md"),
                    
                    layer_selection,
                ],
                p="md",
                shadow="xs",
                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
            )


# Left Panel Configurations
left_panel = dmc.GridCol(
                [
                    dmc.Paper(
                        [
                            country_selection,
                            
                            hurricane_exploration,
                            
                            load_layers_button,
                            
                            layers_controls,
                            
                        ],
                        p="md",
                        shadow="sm"
                    )
                ],
                span=3,
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"}
            )


# Center Panel - Map (Dash Leaflet with all layers)

center_panel = dmc.GridCol(
                html.Div([
                    dcc.Store(id="effective-country-store", data=DEFAULT_COUNTRY),
                    dcc.Store(id="map-state-store", data={}),
                    dcc.Store(id="envelope-data-store", data={}),
                    dcc.Store(id="schools-data-store", data={}),
                    dcc.Store(id="health-data-store", data={}),
                    dcc.Store(id="shelters-data-store", data={}),
                    dcc.Store(id="wash-data-store", data={}),
                    dcc.Store(id="population-tiles-data-store", data={}),
                    dcc.Store(id="population-admin-data-store", data={}),
                    dcc.Store(id="tiles-stats-store", data={}),
                    dcc.Store(id="admin-stats-store", data={}),
                    dcc.Store(id="tracks-data-store", data={}),
                    dcc.Store(id="layers-loaded-store", data=False),
                    dcc.Store(id="layer-availability-store", data={}),
                    dl.Map(
                        [
                            dl.LayersControl(
                                [
                                    dl.BaseLayer(
                                        dl.TileLayer(
                                            url=get_tile_layer_url(),
                                            attribution='© <a href="https://www.mapbox.com/about/maps/">Mapbox</a> © <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a><br>The boundaries and names shown and the designations used on this map do not imply official endorsement or acceptance by the United Nations.' if mapbox_token else '© <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors<br>The boundaries and names shown and the designations used on this map do not imply official endorsement or acceptance by the United Nations.'
                                        ),
                                        name="Mapbox Light" if mapbox_token else "OpenStreetMap",
                                        checked=True
                                    ),
                                    dl.BaseLayer(
                                        dl.TileLayer(
                                            url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}",
                                            attribution='Tiles &copy; <a href="https://services.arcgisonline.com/">Esri</a> &mdash; Source: Esri, HERE, Garmin, FAO, NOAA, USGS<br>The boundaries and names shown and the designations used on this map do not imply official endorsement or acceptance by the United Nations.'
                                        ),
                                        name="Esri Topographic",
                                        checked=False
                                    ),
                                    dl.BaseLayer(
                                        dl.TileLayer(
                                            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                                            attribution='© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors<br>The boundaries and names shown and the designations used on this map do not imply official endorsement or acceptance by the United Nations.'
                                        ),
                                        name="OpenStreetMap",
                                        checked=False
                                    ),
                                ],
                                position="topright"
                            ),
                            # Hurricane Tracks Layer
                            dl.GeoJSON(
                                id="hurricane-tracks-json",
                                data={},
                                zoomToBounds=False,
                                style=style_tracks,
                                onEachFeature=tooltip_tracks
                            ),
                            # Hurricane Envelopes Layer
                            dl.GeoJSON(
                                id="envelopes-json-test",
                                data={},
                                zoomToBounds=False,
                                style=style_envelopes,
                                onEachFeature=tooltip_envelopes
                            ),
                            # Schools Impact Layer
                            dl.GeoJSON(
                                id="schools-overlay-json",
                                data={},
                                zoomToBounds=False,
                                pointToLayer=point_to_layer_schools_health,
                                onEachFeature=tooltip_schools
                            ),
                            # Health Centers Impact Layer
                            dl.GeoJSON(
                                id="health-overlay-json",
                                data={},
                                zoomToBounds=False,
                                pointToLayer=point_to_layer_schools_health,
                                onEachFeature=tooltip_health
                            ),
                            # Shelters Overlay Layer
                            dl.GeoJSON(
                                id="shelters-overlay-json",
                                data={},
                                zoomToBounds=False,
                                pointToLayer=point_to_layer_schools_health,
                                onEachFeature=tooltip_shelters
                            ),
                            # WASH Overlay Layer
                            dl.GeoJSON(
                                id="wash-overlay-json",
                                data={},
                                zoomToBounds=False,
                                pointToLayer=point_to_layer_schools_health,
                                onEachFeature=tooltip_wash
                            ),
                            # Population Density Tiles Layer
                            dl.GeoJSON(
                                id="population-tiles-json",
                                data={},
                                zoomToBounds=False,
                                style=style_tiles,
                                onEachFeature=tooltip_tiles,
                                hideout={"hidden": True}
                            ),

                            # Population Density Admin Layer
                            dl.GeoJSON(
                                id="population-admin-json",
                                data={},
                                zoomToBounds=False,
                                style=style_tiles,
                                onEachFeature=tooltip_admin,
                                hideout={"hidden": True}
                            ),

                            # Impact Probability Tiles Layer
                            dl.GeoJSON(
                                id="probability-tiles-json",
                                data={},
                                zoomToBounds=False,
                                style=style_tiles,
                                onEachFeature=tooltip_tiles,
                                hideout={"hidden": True}
                            ),

                            # Impact Probability Admin Layer
                            dl.GeoJSON(
                                id="probability-admin-json",
                                data={},
                                zoomToBounds=False,
                                style=style_tiles,
                                onEachFeature=tooltip_admin,
                                hideout={"hidden": True}
                            ),
                            
                            dl.FullScreenControl(),
                            dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
                        ],
                        id="main-map",
                        center=[map_config.center["lat"], map_config.center["lon"]],
                        zoom=map_config.zoom,
                        viewport={"center": [map_config.center["lat"], map_config.center["lon"]], "zoom": map_config.zoom},
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
            )

#impact summary
# Impact Summary Section
impact_summary = dmc.Paper([
                    dmc.Group([
                        dmc.Text("IMPACT SUMMARY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                    ], justify="flex-start", gap="sm", mb="sm"),
                    
                    dmc.Text("Hurricane impact scenarios and metrics", size="xs", c="dimmed", mb="md"),
                    
                    # Impact Summary Table
                    html.Div(style={"overflowX": "auto"}, children=[
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
                                        dmc.Badge("#51", id="deterministic-badge", size="xs", color="blue", variant="light", style={"marginTop": "2px"})
                                    ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px", }),
                                    dmc.TableTh("Expected", style={"fontWeight": 700, "textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "paddingTop": "8px", "height": "60px", "verticalAlign": "top", }),
                                    dmc.TableTh([
                                        dmc.Text("Worst", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                        dmc.Badge("Member", id="high-impact-badge", size="xs", color="red", variant="light", style={"marginTop": "2px"})
                                    ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px", })
                                ])
                            ]),
                            dmc.TableTbody([
                                dmc.TableTr([
                                    dmc.TableTd("Population"),
                                    dmc.TableTd("0", id="population-count-low", style={"textAlign": "center", }),
                                    dmc.TableTd("2,482", id="population-count-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("59,678", id="population-count-high", style={"textAlign": "center", })
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd([
                                        html.Span("Children"),
                                        html.Span(" (total)", style={"fontSize": "0.8em", "color": "#888", "marginLeft": "3px"})
                                    ]),
                                    dmc.TableTd("N/A", id="children-total-low", style={"textAlign": "center", }),
                                    dmc.TableTd("N/A", id="children-total-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("N/A", id="children-total-high", style={"textAlign": "center", })
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Age 0–4", style={"fontStyle": "italic", "fontSize": "0.93em", "color": "#888", "paddingLeft": "18px"}),
                                    dmc.TableTd("N/A", id="infant-affected-low", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}),
                                    dmc.TableTd("N/A", id="infant-affected-probabilistic", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}),
                                    dmc.TableTd("N/A", id="infant-affected-high", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Age 5–14", style={"fontStyle": "italic", "fontSize": "0.93em", "color": "#888", "paddingLeft": "18px"}),
                                    dmc.TableTd("N/A", id="children-affected-low", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}),
                                    dmc.TableTd("N/A", id="children-affected-probabilistic", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}),
                                    dmc.TableTd("N/A", id="children-affected-high", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Age 15–19", style={"fontStyle": "italic", "fontSize": "0.93em", "color": "#888", "paddingLeft": "18px"}),
                                    dmc.TableTd("N/A", id="adolescent-affected-low", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}),
                                    dmc.TableTd("N/A", id="adolescent-affected-probabilistic", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}),
                                    dmc.TableTd("N/A", id="adolescent-affected-high", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"})
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Schools"),
                                    dmc.TableTd("0", id="schools-count-low", style={"textAlign": "center", }),
                                    dmc.TableTd("2", id="schools-count-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("39", id="schools-count-high", style={"textAlign": "center", })
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Health Centers"),
                                    dmc.TableTd("0", id="health-count-low", style={"textAlign": "center", }),
                                    dmc.TableTd("1", id="health-count-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("0", id="health-count-high", style={"textAlign": "center", })
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("Shelters"),
                                    dmc.TableTd("N/A", id="shelters-count-low", style={"textAlign": "center", }),
                                    dmc.TableTd("N/A", id="shelters-count-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("N/A", id="shelters-count-high", style={"textAlign": "center", })
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd("WASH Facilities"),
                                    dmc.TableTd("N/A", id="wash-count-low", style={"textAlign": "center", }),
                                    dmc.TableTd("N/A", id="wash-count-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("N/A", id="wash-count-high", style={"textAlign": "center", })
                                ]),
                                dmc.TableTr([
                                    dmc.TableTd([
                                        html.Span("Built Surface m"),
                                        html.Sup("2"),
                                    ]),
                                    dmc.TableTd("0", id="bsm2-count-low", style={"textAlign": "center", }),
                                    dmc.TableTd("2,482", id="bsm2-count-probabilistic", style={"textAlign": "center", }),
                                    dmc.TableTd("59,678", id="bsm2-count-high", style={"textAlign": "center", })
                                ])
                            ])
                        ],
                        striped=True,
                        highlightOnHover=True,
                        withTableBorder=True,
                        withColumnBorders=True,
                        horizontalSpacing="xs",
                        style={"tableLayout": "fixed", "width": "100%"}
                    )
                    ]),
                ],
                p="md",
                shadow="xs",
                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
            )

# Specific Track View Section
specific_track_view = dmc.Paper([
                        dmc.Group([
                            dmc.Text("SPECIFIC TRACK VIEW", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                        ], justify="flex-start", gap="sm", mb="sm"),
                        
                        dmc.Text("Visualize individual hurricane track scenarios", size="xs", c="dimmed", mb="md"),
                        
                        # Specific Track Controls
                        dmc.Stack([
                            dmc.Button(
                                dmc.Group([
                                    DashIconify(icon="mdi:map-marker-path", width=16),
                                    dmc.Text("Show Specific Track", ml="xs")
                                ]),
                                id="show-specific-track-btn",
                                variant="outline",
                                size="sm",
                                disabled=True,
                                mb="md"
                            ),
                            
                            dmc.Select(
                                id="specific-track-select",
                                label="Select Track Scenario",
                                placeholder="Choose ensemble member...",
                                data=[],
                                mb="md",
                                disabled=True,
                                style={"display": "none"}
                            ),
                            dmc.Text(
                                "Members are ordered by total population impacted (deterministic first).",
                                id="specific-track-order-note",
                                size="xs",
                                c="dimmed",
                                mb="sm",
                                style={"display": "none"}
                            ),
                            
                            dmc.Checkbox(
                                id="show-all-envelopes-toggle", 
                                label="Show Higher Wind Thresholds", 
                                checked=True, 
                                mb="md",
                                disabled=True,
                                description="Display all wind thresholds that are higher than the selected threshold for this track."
                            ),
                            
                            dmc.Text(
                                "Load layers first, then select a specific track to see exact impact numbers",
                                size="xs",
                                c="dimmed",
                                id="specific-track-info"
                            )
                        ])
                    ],
                    p="md",
                    shadow="xs",
                    style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
                )

# Exceedance Probability Chart Section
exceedance_chart = dmc.Paper([
                    dmc.Group([
                        dmc.Text("EXCEEDANCE PROBABILITY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
                    ], justify="flex-start", gap="sm", mb="sm"),
                    
                    dmc.Text("Probability of exceeding different impact thresholds", size="xs", c="dimmed", mb="md"),
                    
                    dcc.Graph(id="exceedance-probability-chart", style={"height": "400px", "width": "100%"}, responsive=True),
                    
                    # Custom 3-column legend
                    html.Div(id="exceedance-legend", style={"marginTop": "10px", "marginBottom": "10px"}),
                    
                    dmc.Text(
                        "Load layers to view exceedance probability based on ensemble forecasts.",
                        size="xs",
                        c="dimmed",
                        id="exceedance-chart-info"
                    )
                ],
                p="md",
                shadow="xs",
                style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
            )

# Right Panel - Impact Metrics
right_panel = dmc.GridCol(
                [
                    dmc.Paper(
                        [
                            impact_summary,
                            
                            specific_track_view,
                            
                            exceedance_chart,
                        ],
                        p="md",
                        shadow="sm"
                    )
                ],
                span=3,
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"}
            )

# Create the three-panel dashboard layout (left controls, center map, right metrics)
def make_single_page_layout():
    """Create the three-panel single-page dashboard layout"""
    return dmc.Grid(
        [
            left_panel,
            
            center_panel,
            
            right_panel,
        ],
        gutter="md",
        style={"height": "100%", "margin": 0}
    )

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




# =============================================================================
# SECTION 3: CALLBACKS
# =============================================================================
# All Dash callbacks that handle user interactions and data updates

# -----------------------------------------------------------------------------
# 3.1: CALLBACKS - IMPACT METRICS
# -----------------------------------------------------------------------------
# Calculate and display impact metrics for deterministic (member 51)/probabilistic/high scenarios

@callback(
    [Output("population-count-low", "children"),
     Output("population-count-probabilistic", "children"),
     Output("population-count-high", "children"),
     Output("children-total-low", "children"),
     Output("children-total-probabilistic", "children"),
     Output("children-total-high", "children"),
     Output("infant-affected-low", "children"),
     Output("infant-affected-probabilistic", "children"),
     Output("infant-affected-high", "children"),
     Output("children-affected-low", "children"),
     Output("children-affected-probabilistic", "children"),
     Output("children-affected-high", "children"),
     Output("adolescent-affected-low", "children"),
     Output("adolescent-affected-probabilistic", "children"),
     Output("adolescent-affected-high", "children"),
     Output("schools-count-low", "children"),
     Output("schools-count-probabilistic", "children"),
     Output("schools-count-high", "children"),
     Output("health-count-low", "children"),
     Output("health-count-probabilistic", "children"),
     Output("health-count-high", "children"),
     Output("shelters-count-low", "children"),
     Output("shelters-count-probabilistic", "children"),
     Output("shelters-count-high", "children"),
     Output("wash-count-low", "children"),
     Output("wash-count-probabilistic", "children"),
     Output("wash-count-high", "children"),
     Output("bsm2-count-low", "children"),
     Output("bsm2-count-probabilistic", "children"),
     Output("bsm2-count-high", "children"),
     Output("high-impact-badge", "children"),],
   [Input("storm-select", "value"),
    Input("wind-threshold-select", "value"),
    Input("effective-country-store", "data"),
    Input("forecast-date", "value"),
    Input("forecast-time", "value"),
    Input("layers-loaded-store", "data")],
    prevent_initial_call=True
)
def update_impact_metrics(storm, wind_threshold, country, forecast_date, forecast_time, layers_loaded):
    """Update impact metrics for all three scenarios based on storm, wind threshold, and country selection"""
    
    # Only compute after user has loaded layers to avoid startup churn
    if not layers_loaded:
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        # Return all scenarios with default values (population, children, infants, schools, health, built surface, badge)
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
    
    # Calculate probabilistic impact metrics
    
    try:
        # Construct the filename for the tiles impact view
        date_str = forecast_date.replace('-', '')  # Convert "2025-10-15" to "20251015"
        time_str = forecast_time.replace(':', '')  # Convert "00:00" to "0000"
        forecast_datetime = f"{date_str}{time_str}00"  # Add seconds: "20251015000000"
        
        filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}_{ZOOM_LEVEL}.csv"
        filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "mercator_views", filename)

        print(f"Impact metrics: Looking for file {filename}")
        print(f"Impact metrics: Full path = {filepath}")
        print(f"Impact metrics: ROOT_DATA_DIR = {ROOT_DATA_DIR}")

        # Initialize all scenario results
        low_results = {"children": "N/A", "infant": "N/A", "adolescent": "N/A", "children_total": "N/A", "schools": "N/A", "health": "N/A", "shelters": "N/A", "wash": "N/A", "population": "N/A", "built_surface_m2": "N/A"}
        probabilistic_results = {"children": "N/A", "infant": "N/A", "adolescent": "N/A", "children_total": "N/A", "schools": "N/A", "health": "N/A", "shelters": "N/A", "wash": "N/A", "population": "N/A", "built_surface_m2": "N/A"}
        high_results = {"children": "N/A", "infant": "N/A", "adolescent": "N/A", "children_total": "N/A", "schools": "N/A", "health": "N/A", "shelters": "N/A", "wash": "N/A", "population": "N/A", "built_surface_m2": "N/A"}

        # Initialize member badge (deterministic is always #51, doesn't need updating)
        high_member_badge = "N/A"

        if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(filepath):
            # Retry logic for reading tiles CSV file
            df = None
            max_retries = 3
            retry_delay = 1.0
            
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        delay = retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                        print(f"Impact metrics: Retry attempt {attempt + 1}/{max_retries} after {delay:.1f}s delay...")
                        time.sleep(delay)
                    
                    df = get_impact_data('tile', giga_store, filepath,
                                         country=country, storm=storm,
                                         forecast_date=forecast_datetime,
                                         wind_threshold=int(wind_threshold))
                    break  # Success, exit retry loop

                except Exception as e:
                    error_msg = str(e)
                    is_retryable = (
                        "FileNotFoundError" in error_msg or
                        "No such file or directory" in error_msg or
                        "connection" in error_msg.lower() or
                        "timeout" in error_msg.lower() or
                        "253002" in error_msg or
                        "parquet magic bytes" in error_msg.lower() or
                        "arrowinvalid" in error_msg.lower() or
                        "could not open parquet" in error_msg.lower()
                    )
                    
                    if attempt < max_retries - 1 and is_retryable:
                        print(f"Impact metrics: Retryable error (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")
                        continue  # Retry
                    else:
                        # Final attempt failed or non-retryable error
                        print(f"Impact metrics: Error reading file {filename}: {error_msg}")
                        if attempt == max_retries - 1:
                            print(f"Impact metrics: Failed after {max_retries} attempts")
                        df = None
                        break
            
            if df is not None:
                try:
                    # Calculate PROBABILISTIC scenario (from tiles data)
                    if 'E_school_age_population' in df.columns and not df['E_school_age_population'].isna().all():
                        probabilistic_results["children"] = df['E_school_age_population'].sum()#(gdf['probability'] * gdf['school_age_population']).sum()
                    else:
                        probabilistic_results["children"] = "N/A"

                    if 'E_infant_population' in df.columns and not df['E_infant_population'].isna().all():
                        probabilistic_results["infant"] = df['E_infant_population'].sum()
                    else:
                        probabilistic_results["infant"] = "N/A"

                    if 'E_adolescent_population' in df.columns and not df['E_adolescent_population'].isna().all():
                        probabilistic_results["adolescent"] = df['E_adolescent_population'].sum()
                    else:
                        probabilistic_results["adolescent"] = "N/A"

                    # Children total = sum of all non-N/A age sub-groups
                    _child_parts = [v for v in [probabilistic_results["infant"], probabilistic_results["children"], probabilistic_results["adolescent"]] if v != "N/A"]
                    probabilistic_results["children_total"] = sum(_child_parts) if _child_parts else "N/A"

                    probabilistic_results["schools"] = df['E_num_schools'].sum() if ('E_num_schools' in df.columns and not df['E_num_schools'].isna().all()) else "N/A"
                    probabilistic_results["health"] = df['E_num_hcs'].sum() if ('E_num_hcs' in df.columns and not df['E_num_hcs'].isna().all()) else "N/A"
                    probabilistic_results["shelters"] = df['E_num_shelters'].sum() if ('E_num_shelters' in df.columns and not df['E_num_shelters'].isna().all()) else "N/A"
                    probabilistic_results["wash"] = df['E_num_wash'].sum() if ('E_num_wash' in df.columns and not df['E_num_wash'].isna().all()) else "N/A"
                    probabilistic_results["population"] = df['E_population'].sum() if 'E_population' in df.columns else "N/A"
                    probabilistic_results["built_surface_m2"] = df['E_built_surface_m2'].sum() if 'E_built_surface_m2' in df.columns else "N/A"
                    
                    # Calculate DETERMINISTIC (member 51) and HIGH scenarios (from track data)
                    tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                    tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)

                    if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
                        try:
                            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                                          country=country, storm=storm,
                                                          forecast_date=forecast_datetime,
                                                          wind_threshold=int(wind_threshold))
                        except Exception as e:
                            print(f"Error reading track file {tracks_filepath}: {e}")
                            gdf_tracks = pd.DataFrame()  # Empty dataframe to skip processing
                        
                        if not gdf_tracks.empty and 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
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
                            hc_data_available = (config.IMPACT_DATA_SOURCE == 'SQL') or giga_store.file_exists(hc_filepath)
                            
                            def _col(df, col):
                                """N/A if column absent or all-NaN; otherwise sum."""
                                if col not in df.columns or df[col].isna().all():
                                    return "N/A"
                                return df[col].sum()

                            def _fill_scenario(data, hc_ok):
                                """Compute all impact metrics for one scenario subset."""
                                r = {}
                                r["population"]     = _col(data, 'severity_population')
                                r["children"]       = _col(data, 'severity_school_age_population')
                                r["infant"]         = _col(data, 'severity_infant_population')
                                r["adolescent"]     = _col(data, 'severity_adolescent_population')
                                _parts = [v for v in [r["infant"], r["children"], r["adolescent"]] if v != "N/A"]
                                r["children_total"] = sum(_parts) if _parts else "N/A"
                                r["shelters"]       = _col(data, 'severity_num_shelters')
                                r["wash"]           = _col(data, 'severity_num_wash')
                                r["built_surface_m2"] = _col(data, 'severity_built_surface_m2') if hc_ok else "N/A"
                                # If population is N/A the member didn't reach this country — all
                                # metrics are 0, not N/A (0 is the correct answer, not "unknown").
                                if r["population"] == "N/A":
                                    return {**{k: 0 for k in r}, "schools": 0, "health": 0}
                                r["schools"] = _col(data, 'severity_schools')
                                r["health"]  = "N/A" if not hc_ok else _col(data, 'severity_hcs')
                                return r

                            # DETERMINISTIC scenario (member 51)
                            if not low_scenario_data.empty:
                                low_results.update(_fill_scenario(low_scenario_data, hc_data_available))
                            elif not gdf_tracks.empty:
                                # Member 51 has no rows — track didn't reach this country, impact is 0
                                low_results.update({k: 0 for k in low_results})

                            # HIGH scenario
                            high_results.update(_fill_scenario(high_scenario_data, hc_data_available))
                    
                    print(f"Impact metrics: Successfully loaded {len(df)} features")
                except Exception as e:
                    print(f"Impact metrics: Error processing file {filename}: {e}")
            else:
                # df is None - file read failed after all retries
                print(f"Impact metrics: Could not read file {filename} after {max_retries} attempts")
        else:
            print(f"Impact metrics: File not found {filename}")
        
        # Format results — always round up (ceiling) so counts are never understated
        def format_value(value):
            if isinstance(value, str):
                return value
            ceiled = math.ceil(value)
            formatted = f"{ceiled:,}"
            if ceiled >= 100_000_000:  # 9+ digits
                return html.Span(formatted, style={"fontSize": "0.85em"})
            return formatted
        
        return (
            # Population count
            format_value(low_results["population"]),
            format_value(probabilistic_results["population"]),
            format_value(high_results["population"]),
            # Children total (all age groups combined)
            format_value(low_results["children_total"]),
            format_value(probabilistic_results["children_total"]),
            format_value(high_results["children_total"]),
            # Infants Age 0–4
            format_value(low_results["infant"]),
            format_value(probabilistic_results["infant"]),
            format_value(high_results["infant"]),
            # Children Age 5–14
            format_value(low_results["children"]),
            format_value(probabilistic_results["children"]),
            format_value(high_results["children"]),
            # Adolescents Age 15–19
            format_value(low_results["adolescent"]),
            format_value(probabilistic_results["adolescent"]),
            format_value(high_results["adolescent"]),
            # Schools count
            format_value(low_results["schools"]),
            format_value(probabilistic_results["schools"]),
            format_value(high_results["schools"]),
            # Health count
            format_value(low_results["health"]),
            format_value(probabilistic_results["health"]),
            format_value(high_results["health"]),
            # Shelters count
            format_value(low_results["shelters"]),
            format_value(probabilistic_results["shelters"]),
            format_value(high_results["shelters"]),
            # WASH Facilities count
            format_value(low_results["wash"]),
            format_value(probabilistic_results["wash"]),
            format_value(high_results["wash"]),
            # Built Surface m2
            format_value(low_results["built_surface_m2"]),
            format_value(probabilistic_results["built_surface_m2"]),
            format_value(high_results["built_surface_m2"]),
            # Member badge
            high_member_badge
        )
            
    except Exception as e:
        print(f"Impact metrics: Error updating metrics: {e}")
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")



# -----------------------------------------------------------------------------
# 3.2: CALLBACKS - SPECIFIC TRACK CONTROLS
# -----------------------------------------------------------------------------

@callback(
    Output("show-specific-track-btn", "disabled"),
    [Input("layers-loaded-store", "data")],
    prevent_initial_call=True
)
def enable_specific_track_button(layers_loaded):
    """Enable specific track button when layers are loaded"""
    return not layers_loaded

# Callback to populate specific track selector when layers are loaded
@callback(
    [Output("specific-track-select", "data"),
     Output("specific-track-select", "style"),
     Output("specific-track-order-note", "style")],
    [Input("layers-loaded-store", "data")],
    [State("effective-country-store", "data"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value"),
     State("wind-threshold-select", "value")],
    prevent_initial_call=True
)
def populate_specific_track_options(layers_loaded, country, storm, forecast_date, forecast_time, wind_threshold):
    """Populate specific track selector with available ensemble members"""
    
    if not layers_loaded or not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        return [], {"display": "none"}, {"display": "none"}
    
    try:
        # Convert date and time to YYYYMMDDHHMMSS format
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"
        tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
        
        if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
            try:
                gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                              country=country, storm=storm,
                                              forecast_date=forecast_datetime_str,
                                              wind_threshold=int(wind_threshold))
            except Exception as e:
                print(f"Error reading track file {tracks_filepath}: {e}")
                gdf_tracks = pd.DataFrame()  # Empty dataframe to skip processing

            if not gdf_tracks.empty and 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
                # Sort ensemble members by total impacted population (descending)
                member_totals = (
                    gdf_tracks[["zone_id", "severity_population"]]
                    .groupby("zone_id")["severity_population"]
                    .sum()
                    .fillna(0)
                )
                sorted_members = member_totals.sort_values(ascending=False).index.tolist()
                # Ensure deterministic (51) appears first when present
                ordered_members = ([51] if 51 in sorted_members else []) + [m for m in sorted_members if m != 51]
                
                # Find low and high impact members
                low_impact_member = member_totals.idxmin()
                high_impact_member = member_totals.idxmax()
                
                # Create options with member type labels and impact indicators
                deterministic_items = []
                ensemble_items = []
                for member in ordered_members:
                    is_deterministic = (member == 51)
                    label_prefix = "Deterministic #51" if is_deterministic else f"Ensemble #{member}"
                    
                    # Add impact scenario indicators
                    impact_indicator = ""
                    if member == low_impact_member:
                        impact_indicator = " (LOW IMPACT)"
                    elif member == high_impact_member:
                        impact_indicator = " (HIGH IMPACT)"
                    
                    item = {
                        "value": str(member),
                        "label": f"{label_prefix}{impact_indicator}"
                    }
                    if is_deterministic:
                        deterministic_items.append(item)
                    else:
                        ensemble_items.append(item)

                # Group options so a visual divider appears after deterministic
                if deterministic_items:
                    options = [
                        {"group": "Deterministic", "items": deterministic_items},
                        {"group": "Ensemble Members (by impact)", "items": ensemble_items},
                    ]
                else:
                    options = ensemble_items

                return options, {"display": "block"}, {"display": "block"}
        
        return [], {"display": "none"}, {"display": "none"}
        
    except Exception as e:
        print(f"Error loading specific track options: {e}")
        return [], {"display": "none"}, {"display": "none"}



# -----------------------------------------------------------------------------
# 3.3: CALLBACKS - SELECTORS (Date, Time, Storm, Wind Threshold)
# -----------------------------------------------------------------------------
# Update dropdown options based on available data

@callback(
    Output("effective-country-store", "data"),
    Output("country-store", "data"),
    Output("country-is-region-store", "data"),
    Input("country-select", "value"),
    Input("individual-country-select", "value"),
)
def update_effective_country_store(country, individual):
    effective = individual if individual else country
    is_region = effective in REGION_MEMBERS and not individual
    return effective, effective, is_region

@callback(
    Output("storm-store", "data"),
    Input("storm-select", "value"),
)
def update_storm_store(storm):
    return storm

@callback(
    Output("date-store", "data"),
    Input("forecast-date", "value"),
    Input("forecast-time", "value"),
    State("forecast-date", "value"),
    State("forecast-time", "value"),
)
def update_date_store(i_date, i_time, s_date, s_time):
    if s_date and s_time:
        return f"{s_date.replace('-', '')}{s_time.replace(':', '')}00"
    return dash.no_update

@callback(
    Output("individual-country-select", "data"),
    Output("individual-country-select", "style"),
    Output("individual-country-select", "value"),
    Input("country-select", "value"),
    prevent_initial_call=True,
)
def update_individual_country_select(country):
    if country and country in REGION_MEMBERS:
        return REGION_MEMBERS[country], {"display": "block"}, None
    return [], {"display": "none"}, None

# Callback to update map view based on country selection
@callback(
    Output("main-map", "viewport"),
    Input("effective-country-store", "data"),
    prevent_initial_call=False
)
def update_map_view(country):
    """Update map center and zoom based on selected country"""
    print(f"update_map_view called with country: {country}, type: {type(country)}")
    print(f"Available countries in config: {list(COUNTRY_MAP_CONFIG.keys())}")
    if country and country in COUNTRY_MAP_CONFIG:
        config = COUNTRY_MAP_CONFIG[country]
        print(f"Found config for {country}: center={config['center']}, zoom={config['zoom']}")
        return {"center": config["center"], "zoom": config["zoom"]}
    print(f"Country {country} not found in config, using default: center={DEFAULT_MAP_CONFIG['center']}, zoom={DEFAULT_MAP_CONFIG['zoom']}")
    return {"center": DEFAULT_MAP_CONFIG["center"], "zoom": DEFAULT_MAP_CONFIG["zoom"]}

@callback(
    Output("forecast-date", "data"),
    Output("forecast-date", "value"),
    Input("effective-country-store", "data"),
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
    [Input("effective-country-store", "data"),
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
    [State("wind-threshold-select", "value")],  # Add current value as State
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
        
        print(f"Wind thresholds for {storm} at {forecast_datetime}: available={available_thresholds}, current={current_threshold}, default={default_threshold}")
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



# -----------------------------------------------------------------------------
# 3.4: CALLBACKS - DATA LOADING
# -----------------------------------------------------------------------------
# Load all data layers when user clicks "Load Layers" button

@callback(
    [Output('tracks-data-store', 'data'),
     Output('envelope-data-store', 'data'),
     Output('schools-data-store', 'data'),
     Output('health-data-store', 'data'),
     Output('shelters-data-store', 'data'),
     Output('wash-data-store', 'data'),
     Output('tiles-stats-store', 'data'),
     Output('admin-stats-store', 'data'),
     Output('layers-loaded-store', 'data'),
     Output('using-base-layers-store', 'data'),
     Output('load-status', 'children'),
     # GeoJSON layers — written directly to avoid browser round-trip
     Output('population-tiles-json', 'data', allow_duplicate=True),
     Output('population-tiles-json', 'zoomToBounds', allow_duplicate=True),
     Output('population-tiles-json', 'key', allow_duplicate=True),
     Output('population-tiles-json', 'hideout', allow_duplicate=True),
     Output('probability-tiles-json', 'data', allow_duplicate=True),
     Output('probability-tiles-json', 'zoomToBounds', allow_duplicate=True),
     Output('probability-tiles-json', 'key', allow_duplicate=True),
     Output('probability-tiles-json', 'hideout', allow_duplicate=True),
     Output('population-admin-json', 'data', allow_duplicate=True),
     Output('population-admin-json', 'zoomToBounds', allow_duplicate=True),
     Output('population-admin-json', 'key', allow_duplicate=True),
     Output('population-admin-json', 'hideout', allow_duplicate=True),
     Output('probability-admin-json', 'data', allow_duplicate=True),
     Output('probability-admin-json', 'zoomToBounds', allow_duplicate=True),
     Output('probability-admin-json', 'key', allow_duplicate=True),
     Output('probability-admin-json', 'hideout', allow_duplicate=True),
     # Hurricane section
     Output('hurricane-tracks-toggle', 'disabled'),
     Output('hurricane-envelopes-toggle', 'disabled'),
     Output('show-all-envelopes-toggle', 'disabled'),
     # Infrastructure Impact
     Output('schools-layer', 'disabled'),
     Output('health-layer', 'disabled'),
     Output('shelters-layer', 'disabled'),
     Output('wash-layer', 'disabled'),
     # Tile layers
     Output('probability-tiles-layer', 'disabled'),
     Output('population-tiles-layer', 'disabled', allow_duplicate=True),
     Output('children-total-tiles-layer', 'disabled', allow_duplicate=True),
     Output('infant-tiles-layer', 'disabled', allow_duplicate=True),
     Output('school-age-tiles-layer', 'disabled', allow_duplicate=True),
     Output('adolescent-tiles-layer', 'disabled', allow_duplicate=True),
     Output('built-surface-tiles-layer', 'disabled', allow_duplicate=True),
     Output('cci-tiles-layer', 'disabled', allow_duplicate=True),
     Output('settlement-tiles-layer', 'disabled', allow_duplicate=True),
     Output('rwi-tiles-layer', 'disabled', allow_duplicate=True),
     Output('moderate-poverty-tiles-layer', 'disabled', allow_duplicate=True),
     Output('severe-poverty-tiles-layer', 'disabled', allow_duplicate=True),
     # Admin layers
     Output('probability-admin-layer', 'disabled'),
     Output('population-admin-layer', 'disabled', allow_duplicate=True),
     Output('children-total-admin-layer', 'disabled', allow_duplicate=True),
     Output('infant-admin-layer', 'disabled', allow_duplicate=True),
     Output('school-age-admin-layer', 'disabled', allow_duplicate=True),
     Output('adolescent-admin-layer', 'disabled', allow_duplicate=True),
     Output('built-surface-admin-layer', 'disabled', allow_duplicate=True),
     Output('cci-admin-layer', 'disabled', allow_duplicate=True),
     Output('settlement-admin-layer', 'disabled', allow_duplicate=True),
     Output('rwi-admin-layer', 'disabled', allow_duplicate=True),
     Output('moderate-poverty-admin-layer', 'disabled', allow_duplicate=True),
     Output('severe-poverty-admin-layer', 'disabled', allow_duplicate=True),
     Output('layer-availability-store', 'data')],
    [Input('load-layers-btn', 'n_clicks')],
    State('effective-country-store', 'data'),
    State('storm-select', 'value'),
    State('forecast-date', 'value'),
    State('forecast-time', 'value'),
    State('wind-threshold-select', 'value'),
    State('tiles-layer-group', 'value'),
    State('probability-tiles-layer', 'checked'),
    State('admin-layer-group', 'value'),
    State('probability-admin-layer', 'checked'),
    prevent_initial_call=True,
    running=[(Output("load-layers-btn", "loading"), True, False)]
)
def load_all_layers(n_clicks, country, storm, forecast_date, forecast_time, wind_threshold,
                    tiles_layer_group, prob_tiles_checked, admin_layer_group, prob_admin_checked):
    """Load all available layers when Load Layers button is clicked"""
    print(f"=== LOAD ALL LAYERS CALLBACK STARTED ===")
    print(f"Loading all layers for {country}_{storm}_{forecast_date}_{forecast_time}_{wind_threshold}")
    print(f"Callback context: {callback_context.triggered}")
    
    # Show loading indicator immediately
    loading_indicator = dmc.Loader(
        color="blue",
        size="md", 
        type="dots"
    )
    
    _empty_fc = {"type": "FeatureCollection", "features": []}
    _hidden = {"hidden": True}
    if not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        print("=== MISSING SELECTIONS - RETURNING EARLY ===")
        return ({}, {}, {}, {}, {}, {}, {}, {}, False, False,
                dmc.Alert("Missing selections", title="Warning", color="orange", variant="light"),
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                {})
    try:
        # Initialize empty data stores
        tracks_data = {}
        envelope_data = {}
        schools_data = {}
        health_data = {}
        shelters_data = {}
        wash_data = {}
        tiles_data = {}
        admin_data = {}
        using_base_layers = False

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
            envelope_start = time.time()
            envelope_df = get_envelope_data_snowflake(storm, forecast_datetime)
            
            if not envelope_df.empty:
                # Filter out obviously invalid geometries (empty/null) without parsing
                if 'geometry' in envelope_df.columns:
                    # Quick filter - just check if not null/empty, actual parsing happens later when needed
                    envelope_df = envelope_df[envelope_df['geometry'].notna() & (envelope_df['geometry'].astype(str).str.strip() != '')]
                
                # Pre-process envelopes for multiple wind thresholds in parallel to speed up display
                # Pre-process selected threshold + most common ones (50kt, 64kt) for instant switching
                preprocessed_envelopes = {}
                if wind_threshold and 'wind_threshold' in envelope_df.columns:
                    try:
                        wth_int = int(wind_threshold)
                        # Pre-process selected threshold + common ones (50, 64) in parallel
                        thresholds_to_preprocess = [wth_int]
                        if wth_int != 50:
                            thresholds_to_preprocess.append(50)
                        if wth_int != 64:
                            thresholds_to_preprocess.append(64)
                        # Remove duplicates
                        thresholds_to_preprocess = list(set(thresholds_to_preprocess))
                        
                        def preprocess_threshold(thresh):
                            """Pre-process envelopes for a specific wind threshold"""
                            try:
                                df_filtered = envelope_df[envelope_df['wind_threshold'].astype(int) == thresh].copy()
                                
                                if df_filtered.empty:
                                    return thresh, None
                                
                                parse_start = time.time()
                                # Check geometry format - could be WKT or GeoJSON
                                first_geom = df_filtered['geometry'].iloc[0] if len(df_filtered) > 0 else None
                                if first_geom and isinstance(first_geom, str):
                                    if first_geom.strip().startswith('{') or first_geom.strip().startswith('['):
                                        # GeoJSON format - parse using shapely.geometry.shape
                                        from shapely.geometry import shape
                                        geometries = []
                                        for geom_str in df_filtered['geometry']:
                                            if pd.notna(geom_str) and isinstance(geom_str, str):
                                                try:
                                                    if geom_str.strip().startswith('{'):
                                                        geom_dict = json.loads(geom_str)
                                                        geometries.append(shape(geom_dict))
                                                    else:
                                                        geometries.append(wkt.loads(geom_str))
                                                except:
                                                    geometries.append(None)
                                            else:
                                                geometries.append(None)
                                        gdf = gpd.GeoDataFrame(df_filtered.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
                                    else:
                                        # WKT format - use optimized bulk parsing
                                        gdf = gpd.GeoDataFrame(df_filtered, geometry=gpd.GeoSeries.from_wkt(df_filtered['geometry'], crs='EPSG:4326'))
                                else:
                                    gdf = gpd.GeoDataFrame(df_filtered, geometry=gpd.GeoSeries.from_wkt(df_filtered['geometry'], crs='EPSG:4326'))
                                
                                gdf = gdf[gdf.geometry.notna()]
                                
                                # Simplify geometries for faster rendering (reduce vertices by ~10%)
                                # This makes rendering much faster without noticeable visual difference
                                if len(gdf) > 0:
                                    try:
                                        # Simplify with tolerance of 0.0001 degrees (~11 meters)
                                        gdf['geometry'] = gdf['geometry'].simplify(0.0001, preserve_topology=True)
                                    except:
                                        pass  # If simplification fails, use original
                                
                                # Try to add impact data from track_views if available
                                if country and storm:
                                    date_str = forecast_date.replace('-', '')
                                    time_str = forecast_time.replace(':', '')
                                    forecast_datetime_str = f"{date_str}{time_str}00"
                                    tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{thresh}.parquet"
                                    tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
                                    
                                    if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
                                        try:
                                            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                                                          country=country, storm=storm,
                                                                          forecast_date=forecast_datetime_str,
                                                                          wind_threshold=int(thresh))
                                            if 'zone_id' in gdf_tracks.columns and 'wind_threshold' in gdf_tracks.columns:
                                                tracks_thresh = gdf_tracks[gdf_tracks['wind_threshold'] == thresh]
                                                if not tracks_thresh.empty:
                                                    ensemble_col = 'ENSEMBLE_MEMBER' if 'ENSEMBLE_MEMBER' in gdf.columns else 'ensemble_member'
                                                    if ensemble_col in gdf.columns:
                                                        # Use NaN-preserving sum for optional cols (no-data ≠ zero impact)
                                                        _opt_sum = lambda s: s.sum() if s.notna().any() else float('nan')
                                                        _opt_cols = {'severity_num_shelters', 'severity_num_wash'}
                                                        agg_cols = {
                                                            c: (_opt_sum if c in _opt_cols else 'sum')
                                                            for c in [
                                                                'severity_population',
                                                                'severity_school_age_population',
                                                                'severity_infant_population',
                                                                'severity_adolescent_population',
                                                                'severity_schools',
                                                                'severity_hcs',
                                                                'severity_num_shelters',
                                                                'severity_num_wash',
                                                                'severity_built_surface_m2',
                                                            ] if c in tracks_thresh.columns
                                                        }
                                                        impact_summary = tracks_thresh.groupby('zone_id').agg(agg_cols).reset_index()
                                                        impact_summary.columns = ['ensemble_member'] + [col for col in impact_summary.columns if col != 'zone_id']

                                                        if ensemble_col != 'ensemble_member':
                                                            gdf['ensemble_member'] = gdf[ensemble_col].astype(int)

                                                        gdf = gdf.merge(impact_summary, on='ensemble_member', how='left')
                                                        impact_cols = [c for c in impact_summary.columns if c != 'ensemble_member']
                                                        _no_fill = {'severity_num_shelters', 'severity_num_wash', 'severity_schools', 'severity_hcs', 'severity_built_surface_m2'}
                                                        for col in impact_cols:
                                                            if col in gdf.columns and col not in _no_fill:
                                                                gdf[col] = gdf[col].fillna(0)
                                        except Exception as e:
                                            pass  # Impact data is optional
                                
                                # Convert to GeoJSON and store
                                geo_dict = gdf.__geo_interface__
                                
                                # Calculate max population for relative scaling
                                if 'severity_population' in gdf.columns and gdf['severity_population'].max() > 0:
                                    max_pop = gdf['severity_population'].max()
                                    for feature in geo_dict.get('features', []):
                                        if 'properties' in feature:
                                            feature['properties']['max_population'] = max_pop
                                
                                parse_elapsed = time.time() - parse_start
                                print(f"Pre-processed {len(gdf)} envelopes for {thresh}kt in {parse_elapsed:.2f}s")
                                return thresh, geo_dict
                            except Exception as e:
                                print(f"Error pre-processing threshold {thresh}: {e}")
                                return thresh, None
                        
                        # Pre-process multiple thresholds in parallel
                        with ThreadPoolExecutor(max_workers=3) as executor:
                            futures = {executor.submit(preprocess_threshold, thresh): thresh for thresh in thresholds_to_preprocess}
                            for future in futures:
                                thresh, geo_dict = future.result()
                                if geo_dict:
                                    preprocessed_envelopes[str(thresh)] = geo_dict
                        
                        # Fallback: if parallel processing didn't work, do selected threshold synchronously
                        if str(wth_int) not in preprocessed_envelopes:
                            thresh, geo_dict = preprocess_threshold(wth_int)
                            if geo_dict:
                                preprocessed_envelopes[str(wth_int)] = geo_dict
                    except Exception as e:
                        print(f"Error pre-processing envelopes: {e}")
                
                envelope_data = {
                    'track_id': storm,
                    'forecast_time': forecast_datetime,
                    'data': envelope_df.to_dict('records'),
                    'preprocessed': preprocessed_envelopes  # Store pre-processed GeoJSON by wind threshold
                }
                envelope_elapsed = time.time() - envelope_start
                print(f"Loaded {len(envelope_df)} envelopes from Snowflake in {envelope_elapsed:.2f}s")
        except Exception as e:
            print(f"Error loading envelopes: {e}")
        
        # Load Impact Data (if files exist)
        # Check if data files exist for the selected time
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"

        print(f"Looking for impact data files with pattern: {country}_{storm}_{forecast_datetime_str}_{wind_threshold}")
        print(f"DEBUG: ROOT_DATA_DIR = {ROOT_DATA_DIR}")
        print(f"DEBUG: VIEWS_DIR = {VIEWS_DIR}")
        print(f"DEBUG: Full base path = {os.path.join(ROOT_DATA_DIR, VIEWS_DIR)}")
        
        # Debug: Check if mount point exists and list directory contents
        base_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR)
        print(f"DEBUG: Checking if base path exists: {base_path}")
        if os.path.exists(base_path):
            print(f"DEBUG: Base path exists! Listing contents...")
            try:
                contents = os.listdir(base_path)
                print(f"DEBUG: Found {len(contents)} items in {base_path}: {contents}")
            except Exception as e:
                print(f"DEBUG: Error listing directory: {e}")
        else:
            print(f"DEBUG: Base path does NOT exist!")
        
        # Debug: Check mercator_views directory specifically
        mercator_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views')
        print(f"DEBUG: Checking mercator_views path: {mercator_path}")
        if os.path.exists(mercator_path):
            print(f"DEBUG: mercator_views directory exists!")
            try:
                mercator_files = [f for f in os.listdir(mercator_path) if f.endswith('.csv')]
                print(f"DEBUG: Found {len(mercator_files)} CSV files in mercator_views")
                # Show first 10 files matching the pattern
                pattern = f"{country}_{storm}_{forecast_datetime_str[:8]}"
                matching = [f for f in mercator_files if pattern in f]
                print(f"DEBUG: Files matching pattern '{pattern}': {matching[:10]}")
            except Exception as e:
                print(f"DEBUG: Error listing mercator_views: {e}")
        else:
            print(f"DEBUG: mercator_views directory does NOT exist!")
        
        if config.IMPACT_DATA_SOURCE != 'SQL':
            # Check for data file availability (STAGE/LOCAL/BLOB mode only)
            data_files_found = []
            missing_files = []

            schools_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            schools_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', schools_file)
            print(f"DEBUG: Checking schools file at: {schools_path}")
            print(f"DEBUG: Using giga_store.file_exists() - result: {giga_store.file_exists(schools_path)}")
            print(f"DEBUG: Using os.path.exists() - result: {os.path.exists(schools_path)}")
            if giga_store.file_exists(schools_path):
                data_files_found.append("schools")
            else:
                missing_files.append("schools")

            health_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            health_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', health_file)
            if giga_store.file_exists(health_path):
                data_files_found.append("health centers")
            else:
                missing_files.append("health centers")

            tiles_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_{ZOOM_LEVEL}.csv"
            tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', tiles_file)
            print(f"DEBUG: Checking tiles file at: {tiles_path}")
            print(f"DEBUG: Using giga_store.file_exists() - result: {giga_store.file_exists(tiles_path)}")
            print(f"DEBUG: Using os.path.exists() - result: {os.path.exists(tiles_path)}")
            if giga_store.file_exists(tiles_path):
                data_files_found.append("infrastructure tiles")
            else:
                missing_files.append("infrastructure tiles")

            admin_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_admin1.csv"
            admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', admin_file)
            if giga_store.file_exists(admin_path):
                data_files_found.append("infrastructure admins")
            else:
                missing_files.append("infrastructure admins")

            if not data_files_found:
                status_alert = dmc.Alert(
                    f"No data files found for {forecast_time}. Please select a different time or generate data for this forecast time.",
                    title="No Data Available",
                    color="orange",
                    variant="light"
                )
            elif missing_files:
                status_alert = dmc.Alert(
                    f"Partial data loaded. Missing: {', '.join(missing_files)}. Available: {', '.join(data_files_found)}.",
                    title="Partial Data Loaded",
                    color="yellow",
                    variant="light"
                )
            else:
                status_alert = dmc.Alert(
                    "All layers loaded successfully",
                    title="Success",
                    color="green",
                    variant="light"
                )
            print(f"Data availability: Found={data_files_found}, Missing={missing_files}")
        
        load_start_time = time.time()
        
        try:
            # Helper function to load a dataset with retry logic
            def load_dataset(file_path, dataset_name, max_retries=3, retry_delay=1.0):
                """
                Load a dataset and return its geo_interface with retry logic.
                
                Args:
                    file_path: Path to the file
                    dataset_name: Name of the dataset (for logging)
                    max_retries: Maximum number of retry attempts (default: 3)
                    retry_delay: Initial delay between retries in seconds (default: 1.0)
                
                Returns:
                    dict: GeoJSON-like geo_interface or empty dict on failure
                """
                last_error = None
                for attempt in range(max_retries):
                    try:
                        start = time.time()
                        # Add small delay for retries to avoid connection pool exhaustion
                        if attempt > 0:
                            delay = retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                            print(f"Retry attempt {attempt + 1}/{max_retries} for {dataset_name} after {delay:.1f}s delay...")
                            time.sleep(delay)
                        
                        df = read_dataset(giga_store, file_path)
                        
                        # Ensure we have a GeoDataFrame
                        if isinstance(df, gpd.GeoDataFrame):
                            gdf = df
                        elif isinstance(df, pd.DataFrame):
                            # Try to convert to GeoDataFrame if geometry column exists
                            if 'geometry' in df.columns:
                                # Check geometry column type and sample
                                geom_dtype = df['geometry'].dtype
                                geom_sample = df['geometry'].iloc[0] if len(df) > 0 else None
                                
                                # Handle different geometry formats
                                # First check if sample is bytes (WKB format) - this takes priority
                                # Note: bytes can have dtype 'object' in pandas, so check isinstance first
                                if geom_sample is not None and isinstance(geom_sample, bytes):
                                    # Binary format (WKB) - convert from WKB
                                    try:
                                        from shapely import wkb
                                        geometries = gpd.GeoSeries([wkb.loads(g) if isinstance(g, bytes) else g for g in df['geometry']], crs='EPSG:4326')
                                        gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
                                    except Exception as e:
                                        print(f"Warning: Failed to convert WKB geometry for {dataset_name}: {e}")
                                        # Try convert_to_geodataframe as fallback
                                        try:
                                            gdf = convert_to_geodataframe(df)
                                        except:
                                            print(f"Error: Could not convert WKB geometry for {dataset_name}")
                                            return {}
                                elif geom_dtype == 'object' and geom_sample is not None:
                                    # String format - could be WKT or already Shapely objects
                                    if geom_sample is not None:
                                        # Check if it's WKT string
                                        if isinstance(geom_sample, str):
                                            try:
                                                from shapely import wkt
                                                # Try WKT conversion
                                                geometries = gpd.GeoSeries.from_wkt(df['geometry'], crs='EPSG:4326')
                                                gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
                                            except:
                                                # If WKT fails, might be already Shapely objects
                                                try:
                                                    # Try as Shapely objects
                                                    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                                                except:
                                                    # Last resort: convert using convert_to_geodataframe
                                                    try:
                                                        gdf = convert_to_geodataframe(df)
                                                    except:
                                                        print(f"Error: Could not convert geometry for {dataset_name}")
                                                        return {}
                                        else:
                                            # Not a string or bytes, try as Shapely objects
                                            try:
                                                gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                                            except:
                                                try:
                                                    gdf = convert_to_geodataframe(df)
                                                except:
                                                    print(f"Error: Could not convert geometry for {dataset_name}")
                                                    return {}
                                    else:
                                        try:
                                            gdf = convert_to_geodataframe(df)
                                        except:
                                            print(f"Error: Could not convert geometry for {dataset_name}")
                                            return {}
                                else:
                                    # Numeric or other type - try direct conversion
                                    try:
                                        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                                    except:
                                        gdf = convert_to_geodataframe(df)
                            else:
                                # No geometry column - this shouldn't happen for spatial data
                                print(f"Warning: {dataset_name} file has no geometry column, converting...")
                                gdf = convert_to_geodataframe(df)
                        else:
                            # Unknown type, try to convert
                            gdf = convert_to_geodataframe(df)
                    
                        geo_data = gdf.__geo_interface__
                        elapsed = time.time() - start
                        print(f"Loaded {dataset_name} in {elapsed:.2f}s ({len(gdf)} features)")
                        return geo_data
                    except (IOError, FileNotFoundError, ValueError, ConnectionError, Exception) as e:
                        last_error = e
                        error_msg = str(e)
                        
                        # Check if this is a retryable error
                        # Include parquet/arrow errors as they can be transient Snowflake stage issues
                        is_retryable = (
                            "FileNotFoundError" in error_msg or
                            "No such file or directory" in error_msg or
                            "connection" in error_msg.lower() or
                            "timeout" in error_msg.lower() or
                            "253002" in error_msg or  # Snowflake file transfer error
                            "parquet magic bytes" in error_msg.lower() or  # Can be transient when file is being read during transfer
                            "arrowinvalid" in error_msg.lower() or  # Arrow/Parquet read errors can be transient
                            "could not open parquet" in error_msg.lower()  # Parquet file access errors can be transient
                        )
                        
                        if attempt < max_retries - 1 and is_retryable:
                            print(f"Retryable error reading {dataset_name} file (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")
                            continue  # Retry
                        else:
                            # Final attempt failed or non-retryable error
                            print(f"Error reading {dataset_name} file: {error_msg}")
                            if attempt == max_retries - 1:
                                print(f"Failed after {max_retries} attempts for {dataset_name}")
                            import traceback
                            traceback.print_exc()
                            return {}
                
                # If we get here, all retries failed
                if last_error:
                    print(f"Failed to load {dataset_name} after {max_retries} attempts. Last error: {last_error}")
                return {}
            
            # Infrastructure point layers — schools, health centres, shelters, WASH
            impact_fname = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            
            schools_path  = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views',   impact_fname)
            health_path   = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views',        impact_fname)
            shelters_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'shelter_views',   impact_fname)
            wash_path     = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'wash_views',      impact_fname)

            schools_data = {}
            health_data  = {}
            shelters_data = {}
            wash_data    = {}

            if config.IMPACT_DATA_SOURCE == 'SQL':
                sql_kwargs = dict(country=country, storm=storm,
                                  forecast_date=forecast_datetime_str,
                                  wind_threshold=int(wind_threshold))
                for data_type, path, var_name in [
                    ('school',   schools_path,  'schools'),
                    ('hc',       health_path,   'health'),
                    ('shelter',  shelters_path, 'shelters'),
                    ('wash',     wash_path,     'wash'),
                ]:
                    try:
                        df = get_impact_data(data_type, giga_store, path, **sql_kwargs)
                        if not df.empty and 'latitude' in df.columns:
                            gdf = gpd.GeoDataFrame(
                                df,
                                geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
                                crs='EPSG:4326'
                            )
                            if var_name == 'schools':   schools_data  = gdf.__geo_interface__
                            elif var_name == 'health':  health_data   = gdf.__geo_interface__
                            elif var_name == 'shelters': shelters_data = gdf.__geo_interface__
                            elif var_name == 'wash':    wash_data     = gdf.__geo_interface__
                    except Exception as e:
                        print(f"Error loading {var_name} from SQL: {e}")

                # Fall back to base layer MAT tables for any facility with no impact data
                def _fac_empty(store):
                    return not store or not store.get('features')
                base_fac_map = [
                    ('schools',  schools_data,  get_base_schools),
                    ('health',   health_data,   get_base_hcs),
                    ('shelters', shelters_data, get_base_shelters),
                    ('wash',     wash_data,     get_base_wash),
                ]
                with ThreadPoolExecutor(max_workers=4) as _pool:
                    _futures = {vn: _pool.submit(_get_base_multi, fn, country) for vn, store, fn in base_fac_map if _fac_empty(store)}
                for var_name, store, fn in base_fac_map:
                    if not _fac_empty(store):
                        continue
                    try:
                        df_base = _futures[var_name].result()
                        if not df_base.empty and 'latitude' in df_base.columns:
                            gdf = gpd.GeoDataFrame(df_base, geometry=gpd.points_from_xy(df_base['longitude'], df_base['latitude']), crs='EPSG:4326')
                            geojson = gdf.__geo_interface__
                            if var_name == 'schools':   schools_data  = geojson
                            elif var_name == 'health':  health_data   = geojson
                            elif var_name == 'shelters': shelters_data = geojson
                            elif var_name == 'wash':    wash_data     = geojson
                    except Exception as e:
                        print(f"Base {var_name} fallback error: {e}")
            else:
                # STAGE path: load all four in parallel
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = {}
                    for name, path in [
                        ('schools',  schools_path),
                        ('health',   health_path),
                        ('shelters', shelters_path),
                        ('wash',     wash_path),
                    ]:
                        if giga_store.file_exists(path):
                            futures[name] = executor.submit(load_dataset, path, name)
                            time.sleep(0.1)  # stagger to avoid overwhelming connection pool

                    for name, future in futures.items():
                        try:
                            result = future.result(timeout=30)
                            if result and isinstance(result, dict) and len(result) > 0:
                                if name == 'schools':    schools_data  = result
                                elif name == 'health':   health_data   = result
                                elif name == 'shelters': shelters_data = result
                                elif name == 'wash':     wash_data     = result
                            else:
                                print(f"Warning: {name} loaded but returned empty result")
                        except TimeoutError:
                            print(f"Error: Timeout loading {name} (exceeded 30s)")
                        except Exception as e:
                            error_msg = str(e)
                            print(f"Error in parallel load for {name}: {error_msg[:300]}")
                            if "connection" in error_msg.lower() or "253002" in error_msg:
                                print(f"  This appears to be a connection/network issue.")

            # Tiles
            tiles_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_{ZOOM_LEVEL}.csv"
            tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', tiles_file)
            if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tiles_path):
                if config.IMPACT_DATA_SOURCE == 'SQL':
                    # SQL mode: geometry from BASE_MERCATOR_TILE_MAT (lru_cache hit if base fallback
                    # already ran), impact columns from MERCATOR_TILE_IMPACT_MAT. No parquet read.
                    try:
                        df_tiles = get_impact_data('tile', giga_store, tiles_path,
                                                    country=country, storm=storm,
                                                    forecast_date=forecast_datetime_str,
                                                    wind_threshold=int(wind_threshold))
                        df_tiles = df_tiles.rename(columns={'zone_id': 'tile_id'})
                        gdf_base_tiles = _get_base_multi(get_base_tiles, country, ZOOM_LEVEL)
                        if not gdf_base_tiles.empty and not df_tiles.empty:
                            gdf_base_tiles['tile_id'] = gdf_base_tiles['tile_id'].astype(str)
                            df_tiles['tile_id'] = df_tiles['tile_id'].astype(str)
                            tmp = pd.merge(gdf_base_tiles, df_tiles, on="tile_id", how="left")
                            # CCI
                            cci_tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views',
                                f"{country}_{storm}_{forecast_datetime_str}_{ZOOM_LEVEL}_cci.csv")
                            try:
                                df_cci = get_impact_data('tile_cci', giga_store, cci_tiles_path,
                                                         country=country, storm=storm,
                                                         forecast_date=forecast_datetime_str,
                                                         zoom_level=ZOOM_LEVEL)
                                df_cci = df_cci.rename(columns={'zone_id': 'tile_id'})
                                df_cci['tile_id'] = df_cci['tile_id'].astype(str)
                                tmp = pd.merge(tmp, df_cci, on="tile_id", how="left")
                            except Exception as e:
                                print(f'Cannot merge tile CCI: {e}')
                            gdf_tiles = gpd.GeoDataFrame(tmp, geometry="geometry", crs=gdf_base_tiles.crs)
                            tiles_data = gdf_tiles.__geo_interface__
                    except Exception as e:
                        print(f"Error loading tiles (SQL): {e}")
                        tiles_data = {}
                else:
                    # STAGE / LOCAL / BLOB mode: read base parquet from file store
                    base_tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views',
                                                    f"{country}_{ZOOM_LEVEL}.parquet")
                    if giga_store.file_exists(base_tiles_path):
                        try:
                            df_tiles = get_impact_data('tile', giga_store, tiles_path,
                                                        country=country, storm=storm,
                                                        forecast_date=forecast_datetime_str,
                                                        wind_threshold=int(wind_threshold))
                            df_tiles = df_tiles.rename(columns={'zone_id': 'tile_id'})
                            gdf_base_tiles = read_dataset(giga_store, base_tiles_path)
                            if 'tile_id' in gdf_base_tiles.columns and 'tile_id' in df_tiles.columns:
                                gdf_base_tiles['tile_id'] = gdf_base_tiles['tile_id'].astype(int)
                                df_tiles['tile_id'] = df_tiles['tile_id'].astype(int)
                            elif 'tile_id' in df_tiles.columns and 'tile_id' not in gdf_base_tiles.columns:
                                gdf_base_tiles['tile_id'] = df_tiles['tile_id'].astype(int)
                                df_tiles['tile_id'] = df_tiles['tile_id'].astype(int)
                            tmp = pd.merge(gdf_base_tiles, df_tiles, on="tile_id", how="left")
                            # CCI
                            cci_tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views',
                                f"{country}_{storm}_{forecast_datetime_str}_{ZOOM_LEVEL}_cci.csv")
                            if giga_store.file_exists(cci_tiles_path):
                                try:
                                    df_cci = get_impact_data('tile_cci', giga_store, cci_tiles_path,
                                                             country=country, storm=storm,
                                                             forecast_date=forecast_datetime_str,
                                                             zoom_level=ZOOM_LEVEL)
                                    df_cci = df_cci.rename(columns={'zone_id': 'tile_id'})
                                    if 'Unnamed: 0' in df_cci.columns:
                                        df_cci = df_cci.drop(columns=['Unnamed: 0'])
                                    df_cci['tile_id'] = df_cci['tile_id'].astype(int)
                                    tmp = pd.merge(tmp, df_cci, on="tile_id", how="left")
                                except Exception as e:
                                    print(f'Cannot merge tile CCI: {e}')
                            else:
                                print('CCI tile file not found')
                            gdf_tiles = gpd.GeoDataFrame(tmp, geometry="geometry", crs=gdf_base_tiles.crs)
                            tiles_data = gdf_tiles.__geo_interface__
                        except Exception as e:
                            print(f"Error reading tiles file: {e}")
                            tiles_data = {}

            # Admin
            admin_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_admin1.csv"
            admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', admin_file)
            if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(admin_path):
                if config.IMPACT_DATA_SOURCE == 'SQL':
                    # SQL mode: geometry from BASE_ADMIN_GEOM_MAT, impact from ADMIN_ALL_IMPACT_MAT.
                    try:
                        df_admin = get_impact_data('admin_impact', giga_store, admin_path,
                                                    country=country, storm=storm,
                                                    forecast_date=forecast_datetime_str,
                                                    wind_threshold=int(wind_threshold))
                        if df_admin.empty:
                            raise ValueError('empty admin impact data')
                        df_admin = df_admin.rename(columns={'zone_id': 'tile_id'})
                        # Drop name from impact data — base table is authoritative for region names
                        df_admin = df_admin.drop(columns=[c for c in ['name', 'admin_level'] if c in df_admin.columns])
                        gdf_base_admin = _get_base_multi(get_base_admin, country, 1)
                        if not gdf_base_admin.empty:
                            gdf_base_admin['tile_id'] = gdf_base_admin['tile_id'].astype(str)
                            df_admin['tile_id'] = df_admin['tile_id'].astype(str)
                            tmp = pd.merge(gdf_base_admin, df_admin, on="tile_id", how="left")
                            # CCI
                            cci_admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                f"{country}_{storm}_{forecast_datetime_str}_admin1_cci.csv")
                            try:
                                df_cci_admin = get_impact_data('admin_cci', giga_store, cci_admin_path,
                                                               country=country, storm=storm,
                                                               forecast_date=forecast_datetime_str)
                                df_cci_admin = df_cci_admin.rename(columns={'zone_id': 'tile_id'})
                                df_cci_admin['tile_id'] = df_cci_admin['tile_id'].astype(str)
                                tmp = pd.merge(tmp, df_cci_admin, on="tile_id", how="left")
                            except Exception as e:
                                print(f'Cannot merge admin CCI: {e}')
                            gdf_admin = gpd.GeoDataFrame(tmp, geometry="geometry", crs=gdf_base_admin.crs)
                            admin_data = gdf_admin.__geo_interface__
                    except Exception as e:
                        print(f"Error loading admin (SQL): {e}")
                        admin_data = {}
                else:
                    # STAGE / LOCAL / BLOB mode: read base parquet from file store
                    base_admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                                    f"{country}_admin1.parquet")
                    if giga_store.file_exists(base_admin_path):
                        try:
                            df_admin = get_impact_data('admin_impact', giga_store, admin_path,
                                                        country=country, storm=storm,
                                                        forecast_date=forecast_datetime_str,
                                                        wind_threshold=int(wind_threshold))
                            if df_admin.empty:
                                print(f'No admin impact data for {country}/{storm}/{forecast_datetime_str}')
                                raise ValueError('empty admin data')
                            df_admin = df_admin.rename(columns={'zone_id': 'tile_id'})
                            gdf_base_admin = read_dataset(giga_store, base_admin_path)
                            if 'tile_id' in gdf_base_admin.columns and 'tile_id' in df_admin.columns:
                                gdf_base_admin['tile_id'] = gdf_base_admin['tile_id'].astype(str)
                                df_admin['tile_id'] = df_admin['tile_id'].astype(str)
                            tmp = pd.merge(gdf_base_admin, df_admin, on="tile_id", how="left")
                            # CCI
                            cci_admin_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views',
                                f"{country}_{storm}_{forecast_datetime_str}_admin1_cci.csv")
                            if giga_store.file_exists(cci_admin_path):
                                try:
                                    df_cci_admin = get_impact_data('admin_cci', giga_store, cci_admin_path,
                                                                   country=country, storm=storm,
                                                                   forecast_date=forecast_datetime_str)
                                    df_cci_admin = df_cci_admin.rename(columns={'zone_id': 'tile_id'})
                                    if 'Unnamed: 0' in df_cci_admin.columns:
                                        df_cci_admin = df_cci_admin.drop(columns=['Unnamed: 0'])
                                    df_cci_admin['tile_id'] = df_cci_admin['tile_id'].astype(str)
                                    tmp = pd.merge(tmp, df_cci_admin, on="tile_id", how="left")
                                except Exception as e:
                                    print(f'Cannot merge admin CCI: {e}')
                            else:
                                print('CCI admin file not found')
                            gdf_admin = gpd.GeoDataFrame(tmp, geometry="geometry", crs=gdf_base_admin.crs)
                            admin_data = gdf_admin.__geo_interface__
                        except Exception as e:
                            print(f"Error reading admin file: {e}")
                            admin_data = {}
                
        except Exception as e:
            print(f"Error loading impact data: {e}")
            status_alert = dmc.Alert(
                f"Error loading layers: {str(e)}",
                title="Error",
                color="red",
                variant="light"
            )
        
        # Fall back to base MAT tables if no impact data loaded (SQL mode only)
        if config.IMPACT_DATA_SOURCE == 'SQL':
            if not tiles_data or not tiles_data.get('features'):
                try:
                    gdf_base = _get_base_multi(get_base_tiles, country, ZOOM_LEVEL)
                    if not gdf_base.empty:
                        tiles_data = gdf_base.__geo_interface__
                        using_base_layers = True
                        print(f"✓ Base tile fallback: {len(gdf_base)} tiles for {country}")
                except Exception as e:
                    print(f"Base tile fallback error: {e}")

            if not admin_data or not admin_data.get('features'):
                try:
                    gdf_base_admin = _get_base_multi(get_base_admin, country, 1)
                    if not gdf_base_admin.empty:
                        admin_data = gdf_base_admin.__geo_interface__
                        using_base_layers = True
                        print(f"✓ Base admin fallback: {len(gdf_base_admin)} regions for {country}")
                except Exception as e:
                    print(f"Base admin fallback error: {e}")

        # Show warning if we fell back to base layers; success if SQL loaded impact data normally
        if using_base_layers:
            status_alert = dmc.Alert(
                "No impact data found for this storm and forecast time. Showing base context layers only.",
                title="Base Layers Only — No Impact Data Available",
                color="yellow",
                variant="light",
            )
        elif config.IMPACT_DATA_SOURCE == 'SQL':
            status_alert = dmc.Alert(
                "All layers loaded successfully",
                title="Success",
                color="green",
                variant="light",
            )

        # Create independent copies for each tile layer
        if not tiles_data or not isinstance(tiles_data, dict) or not 'features' in tiles_data:
            tiles_data = {"type": "FeatureCollection", "features": []}

        if not admin_data or not isinstance(admin_data, dict) or not 'features' in admin_data:
            admin_data = {"type": "FeatureCollection", "features": []}

        # Pre-compute all color variants for fast toggle without server round-trip
        tiles_data = precompute_all_colors(tiles_data)
        admin_data = precompute_all_colors(admin_data)
        tiles_stats = compute_layer_stats(tiles_data)
        admin_stats = compute_layer_stats(admin_data)

        # Compute initial hideouts for GeoJSON layers based on current UI state
        def _hideouts(layer_group, prob_checked):
            pop_hidden = (not layer_group or layer_group == "none") or (
                prob_checked and layer_group in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]
            )
            prop = _LAYER_TO_PROP.get(layer_group, "population")
            e_prop = _LAYER_TO_E_PROP.get(layer_group, "probability")
            pop_h = {"hidden": True} if pop_hidden else {"prop": prop}
            prob_h = {"prop": e_prop} if prob_checked else {"hidden": True}
            return pop_h, prob_h

        tiles_pop_h, tiles_prob_h = _hideouts(tiles_layer_group, prob_tiles_checked)
        admin_pop_h, admin_prob_h = _hideouts(admin_layer_group, prob_admin_checked)
        layer_key = str(time.time())

        load_elapsed = time.time() - load_start_time
        print(f"=== LOAD ALL LAYERS CALLBACK COMPLETED SUCCESSFULLY in {load_elapsed:.2f}s ===")

        # Hurricane/probability/CCI controls require impact data
        dis_hurricane = using_base_layers   # tracks, envelopes, show-all
        dis_prob      = using_base_layers   # probability-tiles-layer, probability-admin-layer
        dis_cci       = using_base_layers   # cci-tiles-layer, cci-admin-layer

        # Determine which optional layers have actual data
        # Single-pass scan to find which tile/admin properties have at least one non-null value
        def _available_props(fc):
            found = set()
            for f in fc.get('features', []):
                for k, v in (f.get('properties') or {}).items():
                    if v is not None:
                        found.add(k)
            return found

        tile_props  = _available_props(tiles_data)
        admin_props = _available_props(admin_data)

        _prop_map = {
            "population":    "population",
            "children-total":"children_total",
            "infant":        "infant_population",
            "school-age":    "school_age_population",
            "adolescent":    "adolescent_population",
            "built-surface": "built_surface_m2",
            "cci":           config.CCI_COL,
            "settlement":       "smod_class",
            "rwi":              "rwi",
            "moderate-poverty": "moderate_poverty_prob",
            "severe-poverty":   "severe_poverty_prob",
        }

        layer_availability = {
            "using_base_layers": using_base_layers,
            # Infrastructure point layers
            "schools":   bool(schools_data  and schools_data.get('features')),
            "health":    bool(health_data   and health_data.get('features')),
            "shelters":  bool(shelters_data and shelters_data.get('features')),
            "wash":      bool(wash_data     and wash_data.get('features')),
            # Hurricane overlays
            "tracks":    bool(tracks_data    and tracks_data.get('features')),
            # envelope_data uses 'data' key (list of records), not 'features'
            "envelopes": bool(envelope_data  and envelope_data.get('data')),
        }
        # Tile and admin property layers
        for layer_key, prop_name in _prop_map.items():
            layer_availability[f"tile_{layer_key}"]  = prop_name in tile_props
            layer_availability[f"admin_{layer_key}"] = prop_name in admin_props

        return (tracks_data, envelope_data, schools_data, health_data,
                shelters_data, wash_data,
                tiles_stats, admin_stats,
                True, using_base_layers, status_alert,
                tiles_data, False, layer_key, tiles_pop_h,
                tiles_data, False, layer_key, tiles_prob_h,
                admin_data, False, layer_key, admin_pop_h,
                admin_data, False, layer_key, admin_prob_h,
                # hurricane: tracks, envelopes, show-all; facilities: schools, health, shelters, wash
                dis_hurricane, dis_hurricane, dis_hurricane, False, False, False, False,
                # tile layers: prob, pop, children, infant, school-age, adolescent, built-surface, cci, settlement, rwi, mod-poverty, sev-poverty
                dis_prob, False, False, False, False, False, False, dis_cci, False, False, False, False,
                # admin layers: same
                dis_prob, False, False, False, False, False, False, dis_cci, False, False, False, False,
                layer_availability)

    except Exception as e:
        print(f"Error in load_all_layers: {e}")
        return ({}, {}, {}, {}, {}, {}, {}, {}, False, False,
                dmc.Alert(f"Error loading layers: {str(e)}", title="Error", color="red", variant="light"),
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                {})


# Callback to warn when selectors change after layers are loaded
@callback(
    Output('load-status', 'children', allow_duplicate=True),
    [Input('effective-country-store', 'data'),
     Input('storm-select', 'value'),
     Input('forecast-date', 'value'),
     Input('forecast-time', 'value'),
     Input('wind-threshold-select', 'value')],
    [State('layers-loaded-store', 'data')],
    prevent_initial_call='initial_duplicate'
)
def warn_on_selector_change(country, storm, forecast_date, forecast_time, wind_threshold, layers_loaded):
    """Show warning when selectors change after layers are loaded"""
    # Only show warning if layers are already loaded
    if layers_loaded:
        return dmc.Alert(
            "Selection changed. Please reload layers to see updated data.",
            title="Reload Required",
            color="orange",
            variant="light"
        )
    # Otherwise, don't interfere with normal status updates
    return dash.no_update


# -----------------------------------------------------------------------------
# 3.5: CALLBACKS - LAYER TOGGLES
# -----------------------------------------------------------------------------
# Toggle visibility of different map layers (tracks, envelopes, schools, etc.)

@callback(
    Output("hurricane-tracks-json", "data"),
    Output("hurricane-tracks-json", "zoomToBounds"),
    Output("hurricane-tracks-json","key"),
    [Input("hurricane-tracks-toggle", "checked"),
     Input("specific-track-select", "value")],
    State("tracks-data-store", "data"),
    prevent_initial_call=True
)
def toggle_tracks_layer(checked, selected_track, tracks_data_in):
    """Toggle hurricane tracks layer visibility with optional specific track filtering"""
    if not checked or not tracks_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    tracks_data = copy.deepcopy(tracks_data_in)
    key = hashlib.md5(json.dumps(tracks_data, sort_keys=True).encode()).hexdigest()
    
    # If specific track is selected, filter to show only that track
    if selected_track and 'features' in tracks_data:
        filtered_tracks = {"type": "FeatureCollection", "features": []}
        for feature in tracks_data['features']:
            if feature.get('properties', {}).get('ensemble_member') == int(selected_track):
                filtered_tracks['features'].append(feature)
        return filtered_tracks, False, key
    
    # Otherwise show all tracks
    return tracks_data, False, key

@callback(
    Output("envelopes-json-test", "data"),
    Output("envelopes-json-test", "zoomToBounds"),
    Output("envelopes-json-test","key"),
    [Input("hurricane-envelopes-toggle", "checked"),
     Input("show-all-envelopes-toggle", "checked"),
     Input("specific-track-select", "value")],
    [State("envelope-data-store", "data"),
     State("wind-threshold-select", "value"),
     State("effective-country-store", "data"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value")],
    prevent_initial_call=True
)
def toggle_envelopes_layer(checked, show_all_envelopes, selected_track, envelope_data_in, wind_threshold, country, storm, forecast_date, forecast_time):
    """Toggle hurricane envelopes layer visibility with optional specific track filtering"""
    
    if not checked:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    envelope_data = copy.deepcopy(envelope_data_in)
    key = hashlib.md5(json.dumps(envelope_data, sort_keys=True).encode()).hexdigest()
    
    # Construct datetime string for file paths
    date_str = forecast_date.replace('-', '') if forecast_date else ''
    time_str = forecast_time.replace(':', '') if forecast_time else ''
    forecast_datetime_str = f"{date_str}{time_str}00"
    
    # If specific track is selected AND "Show All Envelopes" is checked, show all higher wind threshold envelopes
    if selected_track and show_all_envelopes:
        try:
            # Get the selected wind threshold as integer
            wth_int = int(wind_threshold) if wind_threshold else 50
            
            if not envelope_data or not envelope_data.get('data'):
                # Fallback: try to load from track_views if envelope data not available
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            # Load envelope data from Snowflake for the specific track
            df = pd.DataFrame(envelope_data['data'])
            if df.empty:
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            # Filter for the specific ensemble member (selected track)
            # ensemble_member could be in different column names
            ensemble_col = None
            if 'ENSEMBLE_MEMBER' in df.columns:
                ensemble_col = 'ENSEMBLE_MEMBER'
            elif 'ensemble_member' in df.columns:
                ensemble_col = 'ensemble_member'
            
            if ensemble_col:
                df_filtered = df[df[ensemble_col].astype(int) == int(selected_track)]
            else:
                # Fallback: assume envelope data doesn't have ensemble member info
                df_filtered = df
            
            if df_filtered.empty:
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            # Filter for wind thresholds >= selected threshold (all higher thresholds)
            wind_thresh_col = 'wind_threshold' if 'wind_threshold' in df_filtered.columns else 'WIND_THRESHOLD'
            if wind_thresh_col in df_filtered.columns:
                df_filtered = df_filtered[df_filtered[wind_thresh_col].astype(int) >= wth_int]
            
            if df_filtered.empty:
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            # Convert to GeoDataFrame - handle both WKT and GeoJSON formats
            geom_col = 'geometry' if 'geometry' in df_filtered.columns else 'ENVELOPE_REGION'
            
            if len(df_filtered) == 0:
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            # Check geometry format - could be WKT or GeoJSON
            first_geom = df_filtered[geom_col].iloc[0] if len(df_filtered) > 0 else None
            parse_start = time.time()
            
            if first_geom and isinstance(first_geom, str):
                if first_geom.strip().startswith('{') or first_geom.strip().startswith('['):
                    # GeoJSON format - parse using shapely.geometry.shape
                    print("Detected GeoJSON format in stacked envelope geometries")
                    from shapely.geometry import shape
                    geometries = []
                    for geom_str in df_filtered[geom_col]:
                        if pd.notna(geom_str) and isinstance(geom_str, str):
                            try:
                                if geom_str.strip().startswith('{') or geom_str.strip().startswith('['):
                                    geom_dict = json.loads(geom_str)
                                    geometries.append(shape(geom_dict))
                                else:
                                    # Try WKT as fallback
                                    from shapely import wkt
                                    geometries.append(wkt.loads(geom_str))
                            except Exception as e:
                                print(f"Error parsing geometry: {e}")
                                geometries.append(None)
                        else:
                            geometries.append(None)
                    gdf = gpd.GeoDataFrame(df_filtered.drop(geom_col, axis=1), geometry=geometries, crs='EPSG:4326')
                else:
                    # WKT format - use optimized bulk parsing
                    print("Detected WKT format in stacked envelope geometries")
                    try:
                        gdf = gpd.GeoDataFrame(df_filtered, geometry=gpd.GeoSeries.from_wkt(df_filtered[geom_col], crs='EPSG:4326'))
                    except Exception as e:
                        print(f"Error with bulk WKT parsing, trying individual: {e}")
                        # Fallback: parse individually
                        from shapely import wkt as shapely_wkt
                        geometries = []
                        for wkt_str in df_filtered[geom_col]:
                            if pd.notna(wkt_str) and isinstance(wkt_str, str):
                                try:
                                    geometries.append(shapely_wkt.loads(wkt_str))
                                except:
                                    geometries.append(None)
                            else:
                                geometries.append(None)
                        gdf = gpd.GeoDataFrame(df_filtered.drop(geom_col, axis=1), geometry=geometries, crs='EPSG:4326')
            else:
                # Unknown format, try WKT
                print("Unknown geometry format in stacked envelopes, trying WKT")
                try:
                    gdf = gpd.GeoDataFrame(df_filtered, geometry=gpd.GeoSeries.from_wkt(df_filtered[geom_col], crs='EPSG:4326'))
                except Exception as e:
                    print(f"Error parsing geometries: {e}")
                    return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            gdf = gdf[gdf.geometry.notna()]
            parse_elapsed = time.time() - parse_start
            print(f"Parsed {len(gdf)} envelope geometries in {parse_elapsed:.2f}s")
            
            # Try to add impact data from track_views if available
            try:
                if country and storm and forecast_datetime_str:
                    # Define all possible wind thresholds
                    all_thresholds = [34, 40, 50, 64, 83, 96, 113, 137]
                    available_thresholds = [t for t in all_thresholds if t >= wth_int]
                    
                    # Load track_views files in parallel for better performance
                    def load_impact_data_for_threshold(thresh):
                        """Load impact data for a specific wind threshold"""
                        try:
                            tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{thresh}.parquet"
                            tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
                            
                            if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
                                try:
                                    gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                                                  country=country, storm=storm,
                                                                  forecast_date=forecast_datetime_str,
                                                                  wind_threshold=int(thresh))
                                    track_data = gdf_tracks[gdf_tracks['zone_id'] == int(selected_track)]
                                except Exception as e:
                                    print(f"Error reading track file {tracks_filepath}: {e}")
                                    track_data = pd.DataFrame()  # Empty dataframe
                                if not track_data.empty and 'wind_threshold' in track_data.columns:
                                    track_data_filtered = track_data[track_data['wind_threshold'] == thresh]
                                    if not track_data_filtered.empty:
                                        def _col_sum(col):
                                            if col not in track_data_filtered.columns or track_data_filtered[col].isna().all():
                                                return None
                                            return track_data_filtered[col].sum()
                                        return {
                                            'wind_threshold': thresh,
                                            'severity_population': _col_sum('severity_population'),
                                            'severity_school_age_population': _col_sum('severity_school_age_population'),
                                            'severity_infant_population': _col_sum('severity_infant_population'),
                                            'severity_adolescent_population': _col_sum('severity_adolescent_population'),
                                            'severity_num_shelters': _col_sum('severity_num_shelters'),
                                            'severity_num_wash': _col_sum('severity_num_wash'),
                                            'severity_schools': _col_sum('severity_schools'),
                                            'severity_hcs': _col_sum('severity_hcs'),
                                            'severity_built_surface_m2': _col_sum('severity_built_surface_m2'),
                                        }
                        except Exception as e:
                            print(f"Error loading impact data for threshold {thresh}: {e}")
                        return None
                    
                    # Load impact data in parallel
                    impact_data_list = []
                    with ThreadPoolExecutor(max_workers=4) as executor:
                        futures = {executor.submit(load_impact_data_for_threshold, thresh): thresh for thresh in available_thresholds}
                        for future in futures:
                            result = future.result()
                            if result:
                                impact_data_list.append(result)
                    
                    # Add impact data to each envelope based on its wind threshold
                    if impact_data_list:
                        impact_df = pd.DataFrame(impact_data_list)
                        wind_thresh_col_gdf = 'wind_threshold' if 'wind_threshold' in gdf.columns else 'WIND_THRESHOLD'
                        if wind_thresh_col_gdf in gdf.columns:
                            gdf = gdf.merge(impact_df, on=wind_thresh_col_gdf, how='left', suffixes=('', '_from_tracks'))

            except Exception as e:
                print(f"Could not add impact data to stacked envelopes: {e}")
            
            # Convert to GeoJSON and return
            geo_dict = gdf.__geo_interface__
            
            # Calculate max population for relative scaling across all thresholds
            if 'severity_population' in gdf.columns and gdf['severity_population'].max() > 0:
                max_pop = gdf['severity_population'].max()
                for feature in geo_dict.get('features', []):
                    if 'properties' in feature:
                        feature['properties']['max_population'] = max_pop
                        # Mark as stacked for higher opacity in visualization
                        feature['properties']['is_stacked'] = True
            
            # Mark all features as stacked if not already marked
            for feature in geo_dict.get('features', []):
                if 'properties' in feature and 'is_stacked' not in feature['properties']:
                    feature['properties']['is_stacked'] = True
            
            print(f"Showing stacked envelopes for track {selected_track} at wind thresholds >= {wth_int} ({len(gdf)} envelopes)")
            return geo_dict, False, key
            
        except Exception as e:
            print(f"Error creating stacked envelope view: {e}")
            import traceback
            traceback.print_exc()
            return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    # If specific track is selected but "Show All Envelopes" is NOT checked, show only selected wind threshold
    if selected_track and not show_all_envelopes:
        try:
            # Load specific track data for selected wind threshold only
            tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
            
            if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
                try:
                    gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                                  country=country, storm=storm,
                                                  forecast_date=forecast_datetime_str,
                                                  wind_threshold=int(wind_threshold))
                    specific_track_data = gdf_tracks[gdf_tracks['zone_id'] == int(selected_track)]
                except Exception as e:
                    print(f"Error reading track file {tracks_filepath}: {e}")
                    return f"Error loading track data: {str(e)}"
                
                if not specific_track_data.empty:
                    # Create specific track envelope
                    specific_envelope = {"type": "FeatureCollection", "features": []}
                    for _, row in specific_track_data.iterrows():
                        # Convert geometry to proper GeoJSON format
                        if isinstance(row['geometry'], str):
                            if row['geometry'].startswith('{'):
                                geometry = json.loads(row['geometry'])
                            else:
                                # WKT format - convert to GeoJSON
                                geom_obj = wkt.loads(row['geometry'])
                                geometry = geom_obj.__geo_interface__
                        else:
                            # Already a Shapely geometry object - convert to GeoJSON
                            geometry = row['geometry'].__geo_interface__
                        
                        def _prop_float(col):
                            return float(row[col]) if col in row.index and pd.notna(row[col]) else None
                        feature = {
                            "type": "Feature",
                            "geometry": geometry,
                            "properties": {
                                "zone_id": int(row['zone_id']),
                                "ensemble_member": int(row['zone_id']),
                                "wind_threshold": int(row['wind_threshold']),
                                "severity_population": _prop_float('severity_population'),
                                "severity_schools": _prop_float('severity_schools'),
                                "severity_hcs": _prop_float('severity_hcs'),
                                "severity_built_surface_m2": _prop_float('severity_built_surface_m2'),
                                "severity_school_age_population": _prop_float('severity_school_age_population'),
                                "severity_infant_population": _prop_float('severity_infant_population'),
                                "severity_adolescent_population": _prop_float('severity_adolescent_population'),
                                "severity_num_shelters": _prop_float('severity_num_shelters'),
                                "severity_num_wash": _prop_float('severity_num_wash'),
                            }
                        }
                        specific_envelope['features'].append(feature)
                    return specific_envelope, False, key
        except Exception as e:
            print(f"Error creating specific track envelope: {e}")
    
    # Default probabilistic envelope behavior - now with impact data!
    if not envelope_data or not envelope_data.get('data'):
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    # Check if we have pre-processed envelopes for this wind threshold (fast path)
    if wind_threshold and envelope_data.get('preprocessed') and str(wind_threshold) in envelope_data['preprocessed']:
        print(f"Using pre-processed envelopes for {wind_threshold}kt (fast path)")
        return envelope_data['preprocessed'][str(wind_threshold)], False, key
    
    # Fallback: process on-the-fly (slower, but handles edge cases)
    try:
        df = pd.DataFrame(envelope_data['data'])
        if df.empty:
            return {"type": "FeatureCollection", "features": []}, False, dash.no_update
        
        # Filter by wind threshold
        if wind_threshold:
            wth_int = int(wind_threshold)
            df = df[df['wind_threshold'] == wth_int]
        
        if df.empty:
            return {"type": "FeatureCollection", "features": []}, False, dash.no_update
        
        # When "Show All Envelopes" is checked, display all ensemble member envelopes for this wind threshold
        
        # Convert to GeoDataFrame - handle both WKT and GeoJSON formats
        parse_start = time.time()
        # Check geometry format - could be WKT or GeoJSON
        first_geom = df['geometry'].iloc[0] if len(df) > 0 else None
        if first_geom and isinstance(first_geom, str):
            if first_geom.strip().startswith('{') or first_geom.strip().startswith('['):
                # GeoJSON format - parse using shapely.geometry.shape
                print("Detected GeoJSON format in envelope geometries")
                from shapely.geometry import shape
                geometries = []
                for geom_str in df['geometry']:
                    if pd.notna(geom_str) and isinstance(geom_str, str):
                        try:
                            if geom_str.strip().startswith('{') or geom_str.strip().startswith('['):
                                geom_dict = json.loads(geom_str)
                                geometries.append(shape(geom_dict))
                            else:
                                # Try WKT as fallback
                                from shapely import wkt
                                geometries.append(wkt.loads(geom_str))
                        except Exception as e:
                            print(f"Error parsing geometry: {e}")
                            geometries.append(None)
                    else:
                        geometries.append(None)
                gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
            else:
                # WKT format - use optimized bulk parsing
                print("Detected WKT format in envelope geometries")
                try:
                    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry'], crs='EPSG:4326'))
                except Exception as e:
                    print(f"Error with bulk WKT parsing, trying individual: {e}")
                    # Fallback: parse individually
                    from shapely import wkt as shapely_wkt
                    geometries = []
                    for wkt_str in df['geometry']:
                        if pd.notna(wkt_str) and isinstance(wkt_str, str):
                            try:
                                geometries.append(shapely_wkt.loads(wkt_str))
                            except:
                                geometries.append(None)
                        else:
                            geometries.append(None)
                    gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
        else:
            # Unknown format, try WKT
            print("Unknown geometry format, trying WKT")
            try:
                gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry'], crs='EPSG:4326'))
            except Exception as e:
                print(f"Error parsing geometries: {e}")
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
        
        gdf = gdf[gdf.geometry.notna()]
        parse_elapsed = time.time() - parse_start
        print(f"Parsed {len(gdf)} envelope geometries in {parse_elapsed:.2f}s (fallback path)")
        
        # Try to add impact data from track_views if available
        try:
            # Only try to load impact data if we have all required parameters
            if country and storm and forecast_datetime_str and wind_threshold:
                tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
                tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
                
                if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
                    try:
                        gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                                      country=country, storm=storm,
                                                      forecast_date=forecast_datetime_str,
                                                      wind_threshold=int(wind_threshold))
                    except Exception as e:
                        print(f"Error reading track file {tracks_filepath}: {e}")
                        gdf_tracks = pd.DataFrame()  # Empty dataframe to skip processing

                    if not gdf_tracks.empty and 'zone_id' in gdf_tracks.columns and 'wind_threshold' in gdf_tracks.columns:
                        # Sum impact metrics per ensemble member (zone_id is ensemble_member in track data)
                        # Filter by wind threshold
                        tracks_thresh = gdf_tracks[gdf_tracks['wind_threshold'] == wth_int]
                        
                        if not tracks_thresh.empty:
                            # Aggregate impact data by ensemble member
                            _opt = lambda s: s.sum() if s.notna().any() else float('nan')
                            _opt_cols = {'severity_num_shelters', 'severity_num_wash'}
                            agg_dict = {
                                c: (_opt if c in _opt_cols else 'sum')
                                for c in [
                                    'severity_school_age_population',
                                    'severity_infant_population',
                                    'severity_adolescent_population',
                                    'severity_population',
                                    'severity_schools',
                                    'severity_hcs',
                                    'severity_num_shelters',
                                    'severity_num_wash',
                                    'severity_built_surface_m2',
                                ] if c in tracks_thresh.columns
                            }

                            impact_summary = tracks_thresh.groupby('zone_id').agg(agg_dict).reset_index()
                            impact_summary.columns = ['ensemble_member'] + [col for col in impact_summary.columns if col != 'zone_id']
                            
                            # Merge with envelope data
                            # Get ensemble_member from envelope data - could be in ENSEMBLE_MEMBER column
                            if 'ENSEMBLE_MEMBER' in gdf.columns:
                                gdf['ensemble_member'] = gdf['ENSEMBLE_MEMBER']
                            
                            # Merge impact data
                            gdf = gdf.merge(impact_summary, on='ensemble_member', how='left')

                            # Calculate max population for relative scaling
                            if 'severity_population' in gdf.columns and gdf['severity_population'].max() > 0:
                                max_pop = gdf['severity_population'].max()
                                # Add max_population to each feature properties for relative scaling
                                geo_dict = gdf.__geo_interface__
                                for feature in geo_dict.get('features', []):
                                    if 'properties' in feature:
                                        feature['properties']['max_population'] = max_pop
        except Exception as e:
            print(f"Could not add impact data to envelopes: {e}")
        
        # Convert to geo_interface if not already
        if isinstance(gdf, gpd.GeoDataFrame):
            geo_dict = gdf.__geo_interface__
            # If we didn't add max_population yet, calculate it
            if any('max_population' in f.get('properties', {}) for f in geo_dict.get('features', [])):
                pass  # Already added
            elif 'severity_population' in gdf.columns and gdf['severity_population'].max() > 0:
                max_pop = gdf['severity_population'].max()
                for feature in geo_dict.get('features', []):
                    if 'properties' in feature:
                        feature['properties']['max_population'] = max_pop
            return geo_dict, False, key
        
        return gdf.__geo_interface__, False, key
        
    except Exception as e:
        print(f"Error toggling envelopes: {e}")
        return {"type": "FeatureCollection", "features": []}, False, key

def _style_point_layer(geo_data, base_color):
    """Convert GeoJSON features to styled point markers based on impact probability.

    All four infrastructure layers (schools, HCs, shelters, WASH) share the same
    yellow→red impact scale; only the base_color (no-impact dot) differs per layer.
    """
    from shapely.geometry import shape
    point_features = []
    for feature in geo_data.get('features', []):
        if 'properties' not in feature or 'geometry' not in feature:
            continue
        prob = feature['properties'].get('probability') or 0
        if prob == 0:
            color, radius = base_color, 4
        elif prob <= 0.15:
            color, radius = '#FFFF00', 10  # Yellow
        elif prob <= 0.30:
            color, radius = '#FFD700', 12  # Gold
        elif prob <= 0.45:
            color, radius = '#FFA500', 15  # Orange
        elif prob <= 0.60:
            color, radius = '#FF8C00', 18  # Dark orange
        elif prob <= 0.75:
            color, radius = '#FF4500', 20  # Orange-red
        elif prob <= 0.90:
            color, radius = '#DC143C', 22  # Crimson
        else:
            color, radius = '#8B0000', 25  # Dark red
        try:
            centroid = shape(feature['geometry']).centroid
            point_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [centroid.x, centroid.y]},
                "properties": {
                    **feature['properties'],
                    "_color": color,
                    "_radius": radius,
                    "_opacity": 0.8,
                    "_weight": 2,
                    "_fillOpacity": 0.7
                }
            })
        except Exception as e:
            print(f"Error converting to point: {e}")
    return point_features


@callback(
    Output("schools-overlay-json", "data"),
    Output("schools-overlay-json", "zoomToBounds"),
    Output("schools-overlay-json","key"),
    [Input("schools-layer", "checked")],
    State("schools-data-store", "data"),
    prevent_initial_call=True
)
def toggle_schools_overlay(checked, schools_data_in):
    """Toggle schools layer visibility with probability-based coloring and variable radius"""
    if not checked or not schools_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    schools_data = copy.deepcopy(schools_data_in)
    key = hashlib.md5(json.dumps(schools_data, sort_keys=True).encode()).hexdigest()
    try:
        schools_data['features'] = _style_point_layer(schools_data, '#ADD8E6')  # Light blue
        return schools_data, False, key
    except Exception as e:
        print(f"Error styling schools layer: {e}")
        return schools_data, False, key

@callback(
    Output("health-overlay-json", "data"),
    Output("health-overlay-json", "zoomToBounds"),
    Output("health-overlay-json","key"),
    [Input("health-layer", "checked")],
    State("health-data-store", "data"),
    prevent_initial_call=True
)
def toggle_health_overlay(checked, health_data_in):
    """Toggle health centers layer visibility with probability-based coloring and variable radius"""
    if not checked or not health_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    health_data = copy.deepcopy(health_data_in)
    key = hashlib.md5(json.dumps(health_data, sort_keys=True).encode()).hexdigest()
    try:
        health_data['features'] = _style_point_layer(health_data, '#90EE90')  # Light green
        return health_data, False, key
    except Exception as e:
        print(f"Error styling health layer: {e}")
        return health_data, False, key

@callback(
    Output("shelters-overlay-json", "data"),
    Output("shelters-overlay-json", "zoomToBounds"),
    Output("shelters-overlay-json", "key"),
    [Input("shelters-layer", "checked")],
    State("shelters-data-store", "data"),
    prevent_initial_call=True
)
def toggle_shelters_overlay(checked, shelters_data_in):
    """Toggle shelters layer visibility with probability-based coloring and variable radius"""
    if not checked or not shelters_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    shelters_data = copy.deepcopy(shelters_data_in)
    key = hashlib.md5(json.dumps(shelters_data, sort_keys=True).encode()).hexdigest()
    try:
        shelters_data['features'] = _style_point_layer(shelters_data, '#E91E8C')  # Pink
        return shelters_data, False, key
    except Exception as e:
        print(f"Error styling shelters layer: {e}")
        return shelters_data, False, key

@callback(
    Output("wash-overlay-json", "data"),
    Output("wash-overlay-json", "zoomToBounds"),
    Output("wash-overlay-json", "key"),
    [Input("wash-layer", "checked")],
    State("wash-data-store", "data"),
    prevent_initial_call=True
)
def toggle_wash_overlay(checked, wash_data_in):
    """Toggle WASH facilities layer visibility with probability-based coloring and variable radius"""
    if not checked or not wash_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    wash_data = copy.deepcopy(wash_data_in)
    key = hashlib.md5(json.dumps(wash_data, sort_keys=True).encode()).hexdigest()
    try:
        wash_data['features'] = _style_point_layer(wash_data, '#40E0D0')  # Turquoise
        return wash_data, False, key
    except Exception as e:
        print(f"Error styling WASH layer: {e}")
        return wash_data, False, key


_LAYER_DISPLAY_NAMES = {
    "schools":        "Schools",
    "health":         "Health Centers",
    "shelters":       "Shelters",
    "wash":           "WASH Facilities",
    "tracks":         "Hurricane Tracks",
    "envelopes":      "Hurricane Envelopes",
    "population":     "Population",
    "children-total": "Children (Total)",
    "infant":         "Age 0–4",
    "school-age":     "Age 5–14",
    "adolescent":     "Age 15–19",
    "built-surface":  "Built Surface Area",
    "cci":            "CCI (Child Cyclone Index)",
    "settlement":       "Settlement Classification",
    "rwi":              "Relative Wealth Index (RWI)",
    "moderate-poverty": "Moderate Child Poverty Rate",
    "severe-poverty":   "Severe Child Poverty Rate",
}

@callback(
    Output("layer-no-data-warning", "children"),
    Input("schools-layer", "checked"),
    Input("health-layer", "checked"),
    Input("shelters-layer", "checked"),
    Input("wash-layer", "checked"),
    Input("hurricane-tracks-toggle", "checked"),
    Input("hurricane-envelopes-toggle", "checked"),
    Input("tiles-layer-group", "value"),
    Input("admin-layer-group", "value"),
    State("layer-availability-store", "data"),
    prevent_initial_call=True,
)
def show_layer_no_data_warning(schools_checked, health_checked, shelters_checked, wash_checked,
                                tracks_checked, envelopes_checked, tiles_layer, admin_layer,
                                availability):
    """Show a warning when the user enables a layer that has no data for the current country."""
    if not availability:
        return None

    missing = []

    # Infrastructure and hurricane checkboxes
    checkbox_layers = [
        ("schools",   schools_checked),
        ("health",    health_checked),
        ("shelters",  shelters_checked),
        ("wash",      wash_checked),
        ("tracks",    tracks_checked),
        ("envelopes", envelopes_checked),
    ]
    for key, checked in checkbox_layers:
        if checked and not availability.get(key):
            missing.append(_LAYER_DISPLAY_NAMES[key])

    # Tile and admin radio group selections (skip "none" and base-layer-only mode for tile props)
    using_base = availability.get("using_base_layers", False)
    for prefix, selected in [("tile", tiles_layer), ("admin", admin_layer)]:
        if not selected or selected == "none":
            continue
        avail_key = f"{prefix}_{selected}"
        # In base-layer-only mode, impact-derived props are expected to be absent — don't warn
        if using_base and selected not in ("population", "children-total", "infant",
                                           "school-age", "adolescent", "built-surface", "settlement", "rwi",
                                           "moderate-poverty", "severe-poverty"):
            continue
        if not availability.get(avail_key, True):
            label = _LAYER_DISPLAY_NAMES.get(selected, selected)
            entry = f"{label} ({'tiles' if prefix == 'tile' else 'admin regions'})"
            if entry not in missing:
                missing.append(entry)

    if missing:
        return dmc.Alert(
            f"No data available for: {', '.join(missing)}.",
            title="Layer Has No Data",
            color="yellow",
            variant="light",
            withCloseButton=True,
        )
    return None


# -----------------------------------------------------------------------------
# 3.6: CALLBACKS - TILE LAYER STYLING
# -----------------------------------------------------------------------------
# Handle tile layer display and styling based on selected property
# Data is written directly by load_all_layers; these callbacks only update hideout.

_LAYER_TO_PROP = {
    "population": "population", "children-total": "children_total",
    "infant": "infant_population", "school-age": "school_age_population",
    "adolescent": "adolescent_population", "built-surface": "built_surface_m2",
    "cci": config.CCI_COL, "settlement": "smod_class", "rwi": "rwi",
    "moderate-poverty": "moderate_poverty_prob", "severe-poverty": "severe_poverty_prob",
}
_LAYER_TO_E_PROP = {
    "population": "E_population", "children-total": "E_children_total",
    "infant": "E_infant_population", "school-age": "E_school_age_population",
    "adolescent": "E_adolescent_population", "built-surface": "E_built_surface_m2",
    "cci": config.E_CCI_COL, "settlement": "probability", "rwi": "probability",
    "moderate-poverty": "probability", "severe-poverty": "probability",
    "none": "probability", None: "probability",
}

def _compute_layer_toggle_outputs(selected_layer, prob_checked, base_layers_only):
    """Shared hideout + disabled computation for both tile and admin layer toggle callbacks."""
    if prob_checked:
        pop_dis = inf_dis = sch_dis = blt_dis = ado_dis = chi_dis = False
        set_dis = rwi_dis = mod_pov_dis = sev_pov_dis = True
    else:
        pop_dis = inf_dis = sch_dis = blt_dis = ado_dis = chi_dis = False
        set_dis = rwi_dis = mod_pov_dis = sev_pov_dis = False
    cci_dis = bool(base_layers_only)  # CCI requires impact data

    pop_hidden = (not selected_layer or selected_layer == "none") or (
        prob_checked and selected_layer in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]
    )
    prop = _LAYER_TO_PROP.get(selected_layer, "population")
    e_prop = _LAYER_TO_E_PROP.get(selected_layer, "probability")
    pop_hideout = {"hidden": True} if pop_hidden else {"prop": prop}
    prob_hideout = {"prop": e_prop} if prob_checked else {"hidden": True}
    return pop_hideout, prob_hideout, pop_dis, chi_dis, inf_dis, sch_dis, ado_dis, blt_dis, cci_dis, set_dis, rwi_dis, mod_pov_dis, sev_pov_dis


@callback(
    Output("population-tiles-json", "hideout", allow_duplicate=True),
    Output("probability-tiles-json", "hideout", allow_duplicate=True),
    Output('population-tiles-layer', 'disabled', allow_duplicate=True),
    Output('children-total-tiles-layer', 'disabled', allow_duplicate=True),
    Output('infant-tiles-layer', 'disabled', allow_duplicate=True),
    Output('school-age-tiles-layer', 'disabled', allow_duplicate=True),
    Output('adolescent-tiles-layer', 'disabled', allow_duplicate=True),
    Output('built-surface-tiles-layer', 'disabled', allow_duplicate=True),
    Output('cci-tiles-layer', 'disabled', allow_duplicate=True),
    Output('settlement-tiles-layer', 'disabled', allow_duplicate=True),
    Output('rwi-tiles-layer', 'disabled', allow_duplicate=True),
    Output('moderate-poverty-tiles-layer', 'disabled', allow_duplicate=True),
    Output('severe-poverty-tiles-layer', 'disabled', allow_duplicate=True),
    Input('tiles-layer-group', 'value'),
    Input('probability-tiles-layer', 'checked'),
    State('using-base-layers-store', 'data'),
    prevent_initial_call=True,
)
def juggle_toggles_tiles_layer(selected_layer, prob_checked, base_layers_only):
    return _compute_layer_toggle_outputs(selected_layer, prob_checked, base_layers_only)


@callback(
    Output("population-admin-json", "hideout", allow_duplicate=True),
    Output("probability-admin-json", "hideout", allow_duplicate=True),
    Output('population-admin-layer', 'disabled', allow_duplicate=True),
    Output('children-total-admin-layer', 'disabled', allow_duplicate=True),
    Output('infant-admin-layer', 'disabled', allow_duplicate=True),
    Output('school-age-admin-layer', 'disabled', allow_duplicate=True),
    Output('adolescent-admin-layer', 'disabled', allow_duplicate=True),
    Output('built-surface-admin-layer', 'disabled', allow_duplicate=True),
    Output('cci-admin-layer', 'disabled', allow_duplicate=True),
    Output('settlement-admin-layer', 'disabled', allow_duplicate=True),
    Output('rwi-admin-layer', 'disabled', allow_duplicate=True),
    Output('moderate-poverty-admin-layer', 'disabled', allow_duplicate=True),
    Output('severe-poverty-admin-layer', 'disabled', allow_duplicate=True),
    Input('admin-layer-group', 'value'),
    Input('probability-admin-layer', 'checked'),
    State('using-base-layers-store', 'data'),
    prevent_initial_call=True,
)
def juggle_toggles_admin_layer(selected_layer, prob_checked, base_layers_only):
    return _compute_layer_toggle_outputs(selected_layer, prob_checked, base_layers_only)

# Callback for Impact Probability layer
@callback(
    Output("probability-tiles-json", "hideout", allow_duplicate=True),
    Output("probability-legend", "style", allow_duplicate=True),
    Output("probability-legend-min", "children", allow_duplicate=True),
    Output("probability-legend-max", "children", allow_duplicate=True),
    Input('probability-tiles-layer','checked'),
    Input('tiles-layer-group','value'),
    State('tiles-stats-store','data'),
    prevent_initial_call = True,
)
def toggle_probability_tiles_layer(prob_checked, selected_layer, tiles_stats):
    """Handle Impact Probability layer hideout and legend — data pre-loaded by juggle callback"""
    legend_style = {"display": "block"} if prob_checked else {"display": "none"}

    if not prob_checked:
        return {"hidden": True}, legend_style, "0%", "100%"

    property_map = {
        "population":     "E_population",
        "children-total": "E_children_total",
        "infant":         "E_infant_population",
        "school-age":     "E_school_age_population",
        "adolescent":     "E_adolescent_population",
        "built-surface":  "E_built_surface_m2",
        "cci":            config.E_CCI_COL,
        "settlement":     None,
        "rwi":            None,
        "none":           "probability",
        None:             "probability",
    }

    property_name = property_map.get(selected_layer, "probability")
    if property_name is None:
        property_name = "probability"

    hideout = {"prop": property_name}

    # Legend min/max from pre-computed stats (no feature iteration)
    min_val = "0"
    max_val = "100%"
    if property_name != "probability" and tiles_stats and property_name in tiles_stats:
        try:
            max_val_num = tiles_stats[property_name]["max"]

            def format_number(val):
                if val >= 1000000:
                    return f"{val / 1000000:.1f}M".replace('.0M', 'M')
                elif val >= 1000:
                    return f"{val / 1000:.1f}k".replace('.0k', 'k')
                else:
                    return f"{math.ceil(val):,}"

            max_val = format_number(max_val_num)
        except Exception as e:
            print(f"Error reading legend from stats: {e}")

    return hideout, legend_style, min_val, max_val

# Callback for Impact Probability layer for admins
@callback(
    Output("probability-admin-json", "hideout", allow_duplicate=True),
    Output("probability-legend-admin", "style", allow_duplicate=True),
    Output("probability-legend-admin-min", "children", allow_duplicate=True),
    Output("probability-legend-admin-max", "children", allow_duplicate=True),
    Input('probability-admin-layer','checked'),
    Input('admin-layer-group','value'),
    State('admin-stats-store','data'),
    prevent_initial_call = True,
)
def toggle_probability_admin_layer(prob_checked, selected_layer, admin_stats):
    """Handle Impact Probability admin layer hideout and legend — data pre-loaded by juggle callback"""
    legend_style = {"display": "block"} if prob_checked else {"display": "none"}

    if not prob_checked:
        return {"hidden": True}, legend_style, "0%", "100%"

    property_map = {
        "population":     "E_population",
        "children-total": "E_children_total",
        "infant":         "E_infant_population",
        "school-age":     "E_school_age_population",
        "adolescent":     "E_adolescent_population",
        "built-surface":  "E_built_surface_m2",
        "cci":            config.E_CCI_COL,
        "settlement":     None,
        "rwi":            None,
        "none":           "probability",
        None:             "probability",
    }

    property_name = property_map.get(selected_layer, "probability")
    if property_name is None:
        property_name = "probability"

    hideout = {"prop": property_name}

    # Legend min/max from pre-computed stats (no feature iteration)
    min_val = "0"
    max_val = "100%"
    if property_name != "probability" and admin_stats and property_name in admin_stats:
        try:
            max_val_num = admin_stats[property_name]["max"]

            def format_number(val):
                if val >= 1000000:
                    return f"{val / 1000000:.1f}M".replace('.0M', 'M')
                elif val >= 1000:
                    return f"{val / 1000:.1f}k".replace('.0k', 'k')
                else:
                    return f"{math.ceil(val):,}"

            max_val = format_number(max_val_num)
        except Exception as e:
            print(f"Error reading legend from stats: {e}")

    return hideout, legend_style, min_val, max_val



# -----------------------------------------------------------------------------
# 3.7: CALLBACKS - SPECIFIC TRACK SELECTION
# -----------------------------------------------------------------------------
# Handle selection and display of individual hurricane track scenarios

@callback(
    Output("specific-track-info", "children"),
    [Input("specific-track-select", "value")],
    [State("effective-country-store", "data"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value"),
     State("wind-threshold-select", "value")],
    prevent_initial_call=True
)
def update_specific_track_info(selected_track, country, storm, forecast_date, forecast_time, wind_threshold):
    """Update info text with specific track impact numbers"""
    
    if not selected_track:
        return "Load layers first, then select a specific track to see exact impact numbers"
    
    try:
        # Load specific track data
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"
        tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
        
        if config.IMPACT_DATA_SOURCE != 'SQL' and not giga_store.file_exists(tracks_filepath):
            return "Track data not found"

        # Load track data
        try:
            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                          country=country, storm=storm,
                                          forecast_date=forecast_datetime_str,
                                          wind_threshold=int(wind_threshold))
        except Exception as e:
            print(f"Error reading track file {tracks_filepath}: {e}")
            return f"Error loading track data: {str(e)}"

        # Filter for specific track
        specific_track_data = gdf_tracks[gdf_tracks['zone_id'] == int(selected_track)]
        
        if specific_track_data.empty:
            return f"No data found for track {selected_track}"
        
        # Calculate impact numbers for info text
        total_population = specific_track_data['severity_population'].sum()
        total_schools = specific_track_data['severity_schools'].sum()
        total_health = specific_track_data['severity_hcs'].sum()
        
        return f"Track {selected_track}: {total_population:,.0f} people, {total_schools:,.0f} schools, {total_health:,.0f} health centers affected"
        
    except Exception as e:
        print(f"Error loading specific track info: {e}")
        return f"Error: {str(e)}"

# Callback to handle "Show Specific Track" button click
@callback(
    [Output("specific-track-select", "disabled"),
     Output("show-specific-track-btn", "children")],
    [Input("show-specific-track-btn", "n_clicks")],
    [State("specific-track-select", "disabled")],
    prevent_initial_call=True
)
def toggle_specific_track_mode(n_clicks, currently_disabled):
    """Enable/disable specific track selector when button is clicked"""
    if n_clicks and n_clicks > 0:
        if currently_disabled:
            # Currently disabled, so enable it
            return False, dmc.Group([
                DashIconify(icon="mdi:map-marker-path", width=16),
                dmc.Text("Hide Specific Track", ml="xs")
            ])
        else:
            # Currently enabled, so disable it
            return True, dmc.Group([
                DashIconify(icon="mdi:map-marker-path", width=16),
                dmc.Text("Show Specific Track", ml="xs")
            ])
    return currently_disabled, dmc.Group([
        DashIconify(icon="mdi:map-marker-path", width=16),
        dmc.Text("Show Specific Track", ml="xs")
    ])

# Callback to clear specific track selection when mode is disabled
@callback(
    Output("specific-track-select", "value"),
    [Input("specific-track-select", "disabled")],
    prevent_initial_call=True
)
def clear_specific_track_when_disabled(is_disabled):
    """Clear specific track selection when dropdown is disabled"""
    if is_disabled:
        return None
    return dash.no_update


# Keep "Show Higher Wind Thresholds" disabled unless Specific Track mode is active
@callback(
    Output("show-all-envelopes-toggle", "disabled", allow_duplicate=True),
    [Input("specific-track-select", "disabled"),
     Input("layers-loaded-store", "data")],
    prevent_initial_call='initial_duplicate'
)
def sync_show_higher_winds_disabled(specific_track_disabled, _layers_loaded):
    """Ensure the higher-winds checkbox is enabled only when specific track is enabled."""
    return bool(specific_track_disabled)


# -----------------------------------------------------------------------------
# 3.7.1: CALLBACKS - EXCEEDANCE PROBABILITY CHART
# -----------------------------------------------------------------------------
# Generate exceedance probability chart based on ensemble member data

@callback(
    [Output("exceedance-probability-chart", "figure"),
     Output("exceedance-chart-info", "children"),
     Output("exceedance-legend", "children")],
    [Input("storm-select", "value"),
     Input("wind-threshold-select", "value"),
     Input("effective-country-store", "data"),
     Input("forecast-date", "value"),
     Input("forecast-time", "value"),
     Input("layers-loaded-store", "data")],
    prevent_initial_call=True
)
def update_exceedance_probability_chart(storm, wind_threshold, country, forecast_date, forecast_time, layers_loaded):
    """Generate exceedance probability plot showing population impact distribution"""
    
    # Create empty figure as default
    empty_fig = go.Figure()
    empty_fig.add_annotation(
        text="Load layers to view exceedance probability chart.",
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
    
    empty_legend = html.Div()
    
    if not layers_loaded:
        return empty_fig, "Load layers to view exceedance probability based on ensemble forecasts.", empty_legend
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        return empty_fig, "Please select all required fields (country, storm, date, time, wind threshold) and load layers.", empty_legend
    
    try:
        # Construct the filename for track data
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime = f"{date_str}{time_str}00"
        
        tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
        
        if config.IMPACT_DATA_SOURCE != 'SQL' and not giga_store.file_exists(tracks_filepath):
            empty_fig.add_annotation(
                text="Track data file not found. Please ensure the storm data has been processed.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=12, color="orange")
            )
            return empty_fig, "Track data not found for the selected storm and wind threshold.", empty_legend

        # Load track data
        try:
            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                          country=country, storm=storm,
                                          forecast_date=forecast_datetime,
                                          wind_threshold=int(wind_threshold))
        except Exception as e:
            empty_fig.add_annotation(
                text="Track data could not be read. This may be a temporary issue.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=12, color="orange")
            )
            return empty_fig, f"Track data could not be read: {str(e)}", empty_legend
        
        if 'zone_id' not in gdf_tracks.columns or 'severity_population' not in gdf_tracks.columns:
            empty_fig.add_annotation(
                text="Track data does not contain ensemble member information.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=12, color="orange")
            )
            return empty_fig, "Invalid track data structure.", empty_legend
        
        # Calculate totals per ensemble member
        member_data = []
        unique_members = gdf_tracks['zone_id'].unique()
        for member_id in unique_members:
            member_data_subset = gdf_tracks[gdf_tracks['zone_id'] == member_id]
            total_population = member_data_subset['severity_population'].sum() if 'severity_population' in member_data_subset.columns else float('nan')
            member_data.append({'member': member_id, 'population': total_population})
        
        member_df = pd.DataFrame(member_data)
        
        if member_df.empty:
            return empty_fig, "No ensemble member data found.", empty_legend
        
        values = member_df['population'].values
        member_ids = member_df['member'].values
        
        # Get available wind thresholds for higher threshold curves
        try:
            forecast_datetime_str = f"{forecast_date} {forecast_time}:00"
            available_wind_thresholds = get_available_wind_thresholds(storm, forecast_datetime_str)
        except Exception as e:
            print(f"Error getting available wind thresholds: {e}")
            available_wind_thresholds = []
        
        # Load data for higher wind thresholds
        higher_threshold_data = {}
        if available_wind_thresholds and wind_threshold:
            current_thresh_int = int(wind_threshold)
            higher_thresholds = [t for t in available_wind_thresholds if t.isdigit() and int(t) > current_thresh_int]
            
            for higher_thresh in sorted(higher_thresholds, key=int):
                try:
                    higher_tracks_filename = f"{country}_{storm}_{forecast_datetime}_{higher_thresh}.parquet"
                    higher_tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', higher_tracks_filename)
                    
                    if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(higher_tracks_filepath):
                        higher_gdf_tracks = get_impact_data('track', giga_store, higher_tracks_filepath,
                                                             country=country, storm=storm,
                                                             forecast_date=forecast_datetime,
                                                             wind_threshold=int(higher_thresh))

                        if 'zone_id' in higher_gdf_tracks.columns and len(higher_gdf_tracks) > 0:
                            higher_member_data = []
                            for member_id in higher_gdf_tracks['zone_id'].unique():
                                higher_member_subset = higher_gdf_tracks[higher_gdf_tracks['zone_id'] == member_id]
                                higher_total = higher_member_subset['severity_population'].sum() if 'severity_population' in higher_member_subset.columns else float('nan')
                                higher_member_data.append(higher_total)
                            
                            if higher_member_data:
                                higher_threshold_data[higher_thresh] = np.array(higher_member_data)
                except Exception as e:
                    print(f"Error loading higher threshold {higher_thresh}kt data: {e}")
                    continue
        
        # Create exceedance probability plot
        fig = go.Figure()
        
        if len(values) == 0:
            return empty_fig, "No data available for exceedance probability calculation.", empty_legend
        
        # Create probability levels from 0% to 100%
        n_probabilities = 100
        probability_levels = np.linspace(0, 100, n_probabilities)
        
        # For each probability level, find the impact threshold where P(X > threshold) = probability
        impact_thresholds = []
        for prob in probability_levels:
            percentile = 100 - prob
            threshold = np.nanpercentile(values, percentile)
            impact_thresholds.append(threshold)
        
        # Helper function to convert hex color to rgba
        def hex_to_rgba(hex_color, alpha=0.2):
            r = int(hex_color[1:3], 16)
            g = int(hex_color[3:5], 16)
            b = int(hex_color[5:7], 16)
            return f'rgba({r}, {g}, {b}, {alpha})'
        
        color = '#1cabe2'  # UNICEF blue
        fillcolor_rgba = hex_to_rgba(color, 0.2)
        
        # Collect legend items for custom 3-column legend
        legend_items = []
        
        # Add main curve (current wind threshold) - use shorter label for legend
        main_label = f"{wind_threshold}kt"
        fig.add_trace(go.Scatter(
            x=probability_levels,
            y=impact_thresholds,
            mode='lines',
            name=main_label,
            line=dict(color=color, width=2.5),
            fill='tozerox',
            fillcolor=fillcolor_rgba,
            hovertemplate=f'<b>Population ({wind_threshold}kt):</b><br>Probability: %{{x:.1f}}%<br>Impact Threshold: %{{y:,.0f}}<extra></extra>',
            showlegend=False  # Hide from Plotly legend
        ))
        legend_items.append({"label": main_label, "color": color, "line_style": "solid"})
        
        # Add curves for higher wind thresholds if available
        if higher_threshold_data:
            higher_threshold_colors = {
                "40": "#5dade2",
                "50": "#3498db",
                "64": "#2980b9",
                "83": "#1f618d",
                "96": "#1a5490",
                "113": "#154360",
                "137": "#0b2638"
            }
            
            threshold_labels = {
                "34": "34kt",
                "40": "40kt",
                "50": "50kt",
                "64": "64kt",
                "83": "83kt",
                "96": "96kt",
                "113": "113kt",
                "137": "137kt"
            }
            
            for higher_thresh, higher_values in higher_threshold_data.items():
                if len(higher_values) > 0:
                    higher_impact_thresholds = []
                    for prob in probability_levels:
                        percentile = 100 - prob
                        threshold = np.nanpercentile(higher_values, percentile)
                        higher_impact_thresholds.append(threshold)
                    
                    trace_color = higher_threshold_colors.get(higher_thresh, "#888888")
                    
                    higher_label = threshold_labels.get(higher_thresh, f"{higher_thresh}kt")
                    fig.add_trace(go.Scatter(
                        x=probability_levels,
                        y=higher_impact_thresholds,
                        mode='lines',
                        name=higher_label,
                        line=dict(color=trace_color, width=2, dash='dash'),
                        hovertemplate=f'<b>{higher_label}:</b><br>Probability: %{{x:.1f}}%<br>Impact Threshold: %{{y:,.0f}}<extra></extra>',
                        showlegend=False  # Hide from Plotly legend
                    ))
                    legend_items.append({"label": higher_label, "color": trace_color, "line_style": "dash"})
        
        # Add horizontal line for deterministic value (Member 51)
        member_51_idx = None
        if 51 in member_ids:
            member_51_idx = np.where(member_ids == 51)[0]
            if len(member_51_idx) > 0:
                member_51_val = values[member_51_idx[0]]
                exceedance_prob_51 = np.sum(values > member_51_val) / len(values) * 100
                fig.add_hline(
                    y=member_51_val,
                    line_dash="dash",
                    line_color="#ff6b35",
                    line_width=2,
                    annotation_text=f"Deterministic ({exceedance_prob_51:.1f}%)",
                    annotation_position="top right"
                )
        
        # Get y-axis range for custom tick formatting
        y_min = min(impact_thresholds) if impact_thresholds else 0
        y_max = max(impact_thresholds) if impact_thresholds else 1000000
        
        fig.update_layout(
            xaxis=dict(
                title=dict(text="Probability of Exceeding Threshold (%)", font=dict(size=11)),
                range=[0, 100],
                gridcolor='rgba(200, 200, 200, 0.3)',
                showline=True
            ),
            yaxis=dict(
                title=dict(text="Impact Threshold (Affected Population)", font=dict(size=11)),
                tickformat='.2s',  # Use SI prefixes (K, M, etc.) with 2 significant digits
                gridcolor='rgba(200, 200, 200, 0.3)',
                showline=True
            ),
            plot_bgcolor='rgba(250, 250, 250, 1)',
            paper_bgcolor='white',
            margin=dict(l=45, r=20, t=20, b=50),  # Reduced bottom margin since legend is separate
            height=400,
            showlegend=False  # Hide Plotly legend, use custom legend below
        )
        
        # Create custom 3-column legend
        legend_cols = []
        items_per_col = (len(legend_items) + 2) // 3  # Divide into 3 columns
        
        for col_idx in range(3):
            col_items = []
            start_idx = col_idx * items_per_col
            end_idx = min(start_idx + items_per_col, len(legend_items))
            
            for item in legend_items[start_idx:end_idx]:
                # Determine line style CSS
                if item['line_style'] == 'solid':
                    line_style_css = f"3px solid {item['color']}"
                else:  # dash
                    line_style_css = f"2px dashed {item['color']}"
                
                col_items.append(
                    dmc.Group([
                        html.Div(style={
                            "width": "25px",
                            "height": "2px",
                            "borderTop": line_style_css,
                            "marginRight": "8px"
                        }),
                        dmc.Text(item['label'], size="xs", style={"fontSize": "9px"})
                    ], gap="xs", align="center", style={"marginBottom": "4px"})
                )
            
            if col_items:
                legend_cols.append(
                    dmc.GridCol(
                        dmc.Stack(col_items, gap="xs"),
                        span=4  # 3 columns = 12/3 = 4 spans each
                    )
                )
        
        custom_legend = dmc.Grid(legend_cols, gutter="sm") if legend_cols else html.Div()
        
        info_text = f"Showing exceedance probability for {len(member_df)-1} ensemble members at {wind_threshold}kt wind threshold."
        return fig, info_text, custom_legend
        
    except Exception as e:
        print(f"Error generating exceedance probability chart: {e}")
        import traceback
        traceback.print_exc()
        empty_fig.add_annotation(
            text=f"Error generating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=12, color="red")
        )
        return empty_fig, f"Error: {str(e)}", empty_legend



# -----------------------------------------------------------------------------
# 3.8: CALLBACKS - LEGEND VISIBILITY
# -----------------------------------------------------------------------------
# Show/hide legends based on which layers are visible

# Callback to toggle between tiles and admin mode
@callback(
    [Output("tiles-mode-box", "style"),
     Output("admin-mode-box", "style")],
    Input("layer-mode-selector", "value"),
    prevent_initial_call=False
)
def toggle_layer_mode(selected_mode):
    """Toggle between tiles (rasters) and admin (by region) mode"""
    if selected_mode == "tiles":
        return {"display": "block"}, {"display": "none"}
    else:
        return {"display": "none"}, {"display": "block"}

@callback(
    Output("schools-legend", "style"),
    [Input("schools-layer", "checked")],
    prevent_initial_call=True
)
def toggle_schools_legend(checked):
    """Show/hide schools legend based on checkbox state"""
    return {"display": "block" if checked else "none"}

@callback(
    Output("health-legend", "style"),
    [Input("health-layer", "checked")],
    prevent_initial_call=True
)
def toggle_health_legend(checked):
    """Show/hide health centers legend based on checkbox state"""
    return {"display": "block" if checked else "none"}

@callback(
    Output("shelters-infra-legend", "style"),
    [Input("shelters-layer", "checked")],
    prevent_initial_call=True
)
def toggle_shelters_infra_legend(checked):
    """Show/hide shelters legend based on checkbox state"""
    return {"display": "block" if checked else "none"}

@callback(
    Output("wash-infra-legend", "style"),
    [Input("wash-layer", "checked")],
    prevent_initial_call=True
)
def toggle_wash_infra_legend(checked):
    """Show/hide WASH facilities legend based on checkbox state"""
    return {"display": "block" if checked else "none"}

@callback(
    [Output("population-legend", "style"),
     Output("children-total-legend", "style"),
     Output("infant-legend", "style"),
     Output("school-age-legend", "style"),
     Output("adolescent-legend", "style"),
     Output("built-surface-legend", "style"),
     Output("cci-legend", "style"),
     Output("settlement-legend", "style"),
     Output("rwi-legend", "style"),
     Output("moderate-poverty-legend", "style"),
     Output("severe-poverty-legend", "style"),
     Output("population-legend-min", "children"),
     Output("population-legend-max", "children"),
     Output("children-total-legend-min", "children"),
     Output("children-total-legend-max", "children"),
     Output("infant-legend-min", "children"),
     Output("infant-legend-max", "children"),
     Output("school-age-legend-min", "children"),
     Output("school-age-legend-max", "children"),
     Output("adolescent-legend-min", "children"),
     Output("adolescent-legend-max", "children"),
     Output("built-surface-legend-min", "children"),
     Output("built-surface-legend-max", "children"),
     Output("cci-legend-min", "children"),
     Output("cci-legend-max", "children"),
     ],
    [Input("tiles-layer-group", "value"),
     Input("probability-tiles-layer", "checked")],
    State("tiles-stats-store", "data"),
    prevent_initial_call=True
)
def toggle_tiles_legend(selected_value, prob_checked, tiles_stats):
    """Show/hide tile legends and update labels from pre-computed stats"""
    def format_number(val):
        if val >= 1000000:
            return f"{val / 1000000:.1f}M".replace('.0M', 'M')
        elif val >= 1000:
            return f"{val / 1000:.1f}k".replace('.0k', 'k')
        else:
            return f"{val:,.0f}"

    def get_stats(prop):
        if tiles_stats and prop in tiles_stats:
            return f"{tiles_stats[prop]['min']:,.0f}", format_number(tiles_stats[prop]['max'])
        return "Min", "Max"

    pop_min, pop_max                         = get_stats('population')
    children_total_min, children_total_max   = get_stats('children_total')
    infant_min, infant_max                   = get_stats('infant_population')
    school_min, school_max                   = get_stats('school_age_population')
    adolescent_min, adolescent_max           = get_stats('adolescent_population')
    built_min, built_max                     = get_stats('built_surface_m2')
    cci_min, cci_max                         = get_stats(config.CCI_COL)

    _none = {"display": "none"}
    _show = {"display": "block"}
    # Order matches Output declarations: population, children-total, infant, school-age, adolescent, built-surface, cci, settlement, rwi, moderate-poverty, severe-poverty
    _all_vals = (pop_min, pop_max, children_total_min, children_total_max, infant_min, infant_max, school_min, school_max, adolescent_min, adolescent_max, built_min, built_max, cci_min, cci_max)

    # Hide regular layer legends when probability is checked and a layer with expected values is selected
    if prob_checked and selected_value in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]:
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals

    if selected_value == "population":
        return _show, _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "children-total":
        return _none, _show, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "infant":
        return _none, _none, _show, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "school-age":
        return _none, _none, _none, _show, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "adolescent":
        return _none, _none, _none, _none, _show, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "built-surface":
        return _none, _none, _none, _none, _none, _show, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "cci":
        return _none, _none, _none, _none, _none, _none, _show, _none, _none, _none, _none, *_all_vals
    elif selected_value == "settlement":
        return _none, _none, _none, _none, _none, _none, _none, _show, _none, _none, _none, *_all_vals
    elif selected_value == "rwi":
        return _none, _none, _none, _none, _none, _none, _none, _none, _show, _none, _none, *_all_vals
    elif selected_value == "moderate-poverty":
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _show, _none, *_all_vals
    elif selected_value == "severe-poverty":
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, _show, *_all_vals
    else:
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals

@callback(
    [Output("population-admin-legend", "style"),
     Output("children-total-admin-legend", "style"),
     Output("infant-admin-legend", "style"),
     Output("school-age-admin-legend", "style"),
     Output("adolescent-admin-legend", "style"),
     Output("built-surface-admin-legend", "style"),
     Output("cci-admin-legend", "style"),
     Output("settlement-admin-legend", "style"),
     Output("rwi-admin-legend", "style"),
     Output("moderate-poverty-admin-legend", "style"),
     Output("severe-poverty-admin-legend", "style"),
     Output("population-admin-legend-min", "children"),
     Output("population-admin-legend-max", "children"),
     Output("children-total-admin-legend-min", "children"),
     Output("children-total-admin-legend-max", "children"),
     Output("infant-admin-legend-min", "children"),
     Output("infant-admin-legend-max", "children"),
     Output("school-age-admin-legend-min", "children"),
     Output("school-age-admin-legend-max", "children"),
     Output("adolescent-admin-legend-min", "children"),
     Output("adolescent-admin-legend-max", "children"),
     Output("built-surface-admin-legend-min", "children"),
     Output("built-surface-admin-legend-max", "children"),
     Output("cci-admin-legend-min", "children"),
     Output("cci-admin-legend-max", "children"),
     ],
    [Input("admin-layer-group", "value"),
     Input("probability-admin-layer", "checked")],
    State("admin-stats-store", "data"),
    prevent_initial_call=True
)
def toggle_admin_legend(selected_value, prob_checked, admin_stats):
    """Show/hide admin legends and update labels from pre-computed stats"""
    def format_number(val):
        if val >= 1000000:
            return f"{val / 1000000:.1f}M".replace('.0M', 'M')
        elif val >= 1000:
            return f"{val / 1000:.1f}k".replace('.0k', 'k')
        else:
            return f"{val:,.0f}"

    def get_stats(prop):
        if admin_stats and prop in admin_stats:
            return f"{admin_stats[prop]['min']:,.0f}", format_number(admin_stats[prop]['max'])
        return "Min", "Max"

    pop_min, pop_max                         = get_stats('population')
    children_total_min, children_total_max   = get_stats('children_total')
    infant_min, infant_max                   = get_stats('infant_population')
    school_min, school_max                   = get_stats('school_age_population')
    adolescent_min, adolescent_max           = get_stats('adolescent_population')
    built_min, built_max                     = get_stats('built_surface_m2')
    cci_min, cci_max                         = get_stats(config.CCI_COL)

    _none = {"display": "none"}
    _show = {"display": "block"}
    # Order matches Output declarations: population, children-total, infant, school-age, adolescent, built-surface, cci, settlement, rwi, moderate-poverty, severe-poverty
    _all_vals = (pop_min, pop_max, children_total_min, children_total_max, infant_min, infant_max, school_min, school_max, adolescent_min, adolescent_max, built_min, built_max, cci_min, cci_max)

    # Hide regular layer legends when probability is checked and a layer with expected values is selected
    if prob_checked and selected_value in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]:
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals

    if selected_value == "population":
        return _show, _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "children-total":
        return _none, _show, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "infant":
        return _none, _none, _show, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "school-age":
        return _none, _none, _none, _show, _none, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "adolescent":
        return _none, _none, _none, _none, _show, _none, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "built-surface":
        return _none, _none, _none, _none, _none, _show, _none, _none, _none, _none, _none, *_all_vals
    elif selected_value == "cci":
        return _none, _none, _none, _none, _none, _none, _show, _none, _none, _none, _none, *_all_vals
    elif selected_value == "settlement":
        return _none, _none, _none, _none, _none, _none, _none, _show, _none, _none, _none, *_all_vals
    elif selected_value == "rwi":
        return _none, _none, _none, _none, _none, _none, _none, _none, _show, _none, _none, *_all_vals
    elif selected_value == "moderate-poverty":
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _show, _none, *_all_vals
    elif selected_value == "severe-poverty":
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, _show, *_all_vals
    else:
        return _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, _none, *_all_vals



# =============================================================================
# SECTION 4: PAGE REGISTRATION
# =============================================================================

dash.register_page(__name__, path="/", name="Ahead of the Storm")

