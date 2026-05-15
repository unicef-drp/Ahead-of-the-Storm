"""
Dashboard page — Ahead of the Storm.

Entry point for the Dash multi-page app. Loads country/storm metadata at startup,
defines the full layout via layouts/panels.py, owns the selector callbacks
(country → date → time → storm → wind threshold), and drives the central
`load_all_layers` callback that fetches tracks, envelopes, and impact GeoJSON from
Snowflake / the file store and distributes them to the map components.

Data sources: Snowflake (TC_TRACKS, TC_ENVELOPES_COMBINED, PIPELINE_COUNTRIES),
AOTS_ANALYSIS stage files (Parquet/CSV impact data), FastAPI tile server (port 8001).
"""

# =============================================================================
# SECTION 1 — IMPORTS AND CONFIGURATION
# Third-party and internal imports, environment variable resolution.
# =============================================================================

import logging
import pandas as pd
import dash
from dash import Output, Input, State, callback, callback_context
import dash_mantine_components as dmc
import geopandas as gpd
import os
import warnings
import json
from shapely import wkt
import copy
import hashlib
from concurrent.futures import ThreadPoolExecutor
import time
import threading
import requests

logger = logging.getLogger(__name__)

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config

# Import map config for startup COUNTRY_MAP_CONFIG construction
from components.map.map_config import map_config

# Import Snowflake utilities for country loading
from components.data.snowflake_utils import (
    get_active_countries, get_available_wind_thresholds, get_latest_forecast_time_overall,
    get_snowflake_connection, get_envelope_data_snowflake, get_snowflake_data,
    get_lat_lons_bulk,
    get_base_tiles, get_base_admin,
)

ZOOM_LEVEL = 14  # tile zoom level baked into mercator CSV filenames


# =============================================================================
# SECTION 2 — STARTUP DATA LOADING
# Three Snowflake queries run in parallel at module import time to minimise
# cold-start latency. Results are stored in module-level globals used by layout
# and selector callbacks.
# =============================================================================

# Run the three independent startup queries in parallel — reduces cold-start time
# from ~3× a single query to ~1× (they hit different tables and use separate connections).
with ThreadPoolExecutor(max_workers=3) as _startup_pool:
    _f_countries = _startup_pool.submit(get_active_countries)
    _f_metadata  = _startup_pool.submit(get_snowflake_data)
    _f_latlons   = _startup_pool.submit(get_lat_lons_bulk)
    countries_df = _f_countries.result()
    metadata_df  = _f_metadata.result()
    _latlon_bulk = _f_latlons.result()

# =============================================================================
# SECTION 3 — COUNTRY AND REGION CONFIGURATION
# Build per-country map centres/zooms (COUNTRY_MAP_CONFIG), region member
# lookup (REGION_MEMBERS), and the country dropdown options list
# (COUNTRY_OPTIONS) from startup data.
# =============================================================================

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
    logger.info(f"✓ Loaded {len(COUNTRY_MAP_CONFIG)} countries into COUNTRY_MAP_CONFIG")
else:
    logger.warning("⚠ No countries loaded - using default map config only")

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
    logger.info("No country options available - country dropdown will be empty")




# =============================================================================
# SECTION 4 — IMPACT DATA STORE AND METADATA
# Initialise the file/blob data store and parse forecast metadata into sorted
# date/time lists.
# =============================================================================

# Metadata
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.processing.geo import convert_to_geodataframe
from components.data.data_store_utils import get_data_store, get_impact_data

# Layout module — constants are defined there and re-imported here for use in callbacks
from layouts.panels import (
    make_single_page_appshell,
    PRIMARY_COLOR, _DISPLAY_NONE, _IMPACT_GRADIENT,
    _SWITCH_TRACK_BASE, _SWITCH_LABEL_BASE, _UN_DISCLAIMER,
)
from callbacks.tiles_and_admin import _LAYER_TO_PROP, _LAYER_TO_E_PROP

VIEWS_DIR = config.VIEWS_DIR or "aos_views"
ROOT_DATA_DIR = config.ROOT_DATA_DIR or "geodb"

# Initialize data store using centralized utility
data_store = get_data_store()
giga_store = data_store
# Process metadata loaded during parallel startup
# Parse dates and times from metadata
metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date
metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')

# Get unique dates and times
unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
unique_times = sorted(metadata_df['TIME'].unique())

# Get current hurricanes — latest forecast time per track
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


# =============================================================================
# SECTION 5 — LAYOUT
# Single entry point for the page layout. All layout components live in
# layouts/panels.py.
# =============================================================================

layout = make_single_page_appshell(COUNTRY_OPTIONS, DEFAULT_COUNTRY)


# =============================================================================
# SECTION 6 — SELECTOR CALLBACKS
# Country, storm, date, time, and wind-threshold dropdowns. These callbacks
# have no startup-data write dependency and run on every user interaction.
# =============================================================================

# -----------------------------------------------------------------------------
# Country and storm selectors
# -----------------------------------------------------------------------------

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
)
def update_date_store(date, time):
    if date and time:
        return f"{date.replace('-', '')}{time.replace(':', '')}00"
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
    logger.info(f"update_map_view called with country: {country}, type: {type(country)}")
    logger.info(f"Available countries in config: {list(COUNTRY_MAP_CONFIG.keys())}")
    if country and country in COUNTRY_MAP_CONFIG:
        config = COUNTRY_MAP_CONFIG[country]
        logger.info(f"Found config for {country}: center={config['center']}, zoom={config['zoom']}")
        return {"center": config["center"], "zoom": config["zoom"]}
    logger.warning(f"Country {country} not found in config, using default: center={DEFAULT_MAP_CONFIG['center']}, zoom={DEFAULT_MAP_CONFIG['zoom']}")
    return {"center": DEFAULT_MAP_CONFIG["center"], "zoom": DEFAULT_MAP_CONFIG["zoom"]}

@callback(
    Output("forecast-date", "data"),
    Output("forecast-date", "value"),
    Input("effective-country-store", "data"),
    Input("metadata-refresh-interval", "n_intervals"),
    prevent_initial_call=False
)
def update_forecast_dates(country, n_intervals):
    """Get available forecast dates; refreshes Snowflake metadata every 15 min via interval."""
    global metadata_df, unique_dates, unique_times
    if n_intervals:
        get_snowflake_data.cache_clear()
        metadata_df = get_snowflake_data()
        metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date
        metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')
        unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
        unique_times = sorted(metadata_df['TIME'].unique())
        logger.info(f"Metadata refreshed: {len(unique_dates)} dates, triggered by interval tick {n_intervals}")

    logger.info(f"update_forecast_dates called with country: {country}")

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
        logger.info(f"Returning {len(date_options)} date options from pre-loaded data: {date_options}")
        logger.info(f"Default date (most recent): {default_date}")
        return date_options, default_date
    else:
        logger.info("No metadata available, returning fallback dates")
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
    logger.info(f"Forecast times for {selected_date}: available={available_times}, options={len(time_options)}, default (most recent)={default_time}")
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
    logger.info(f"Storms for {forecast_date} {forecast_time}: available={available_storms}, options={len(storm_options)}, default (most recent)={default_storm}")
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
        
        logger.info(f"Wind thresholds for {storm} at {forecast_datetime}: available={available_thresholds}, current={current_threshold}, default={default_threshold}")
        return options, default_threshold
        
    except Exception as e:
        logger.error(f"Error getting wind threshold options: {e}")
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
# SECTION 7 — DATA LOADING CALLBACK
# The primary data-load callback. Triggered by the 'Load Layers' button. Reads
# all impact layers from the data store, builds the MapLibre tile config, and
# writes everything to dcc.Stores in one atomic update.
# =============================================================================

@callback(
    [Output('tracks-data-store', 'data'),
     Output('envelope-data-store', 'data'),
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
     Output('in-need-tiles-switch', 'disabled'),
     Output('in-need-children-tiles-switch', 'disabled'),
     Output('in-need-admin-switch', 'disabled'),
     Output('in-need-children-admin-switch', 'disabled'),
     Output('maplibre-tile-config-store', 'data'),
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
    """Fetch and distribute all map layers for the selected country/storm/threshold.

    Triggered by the Load Layers button. Runs three data-loading phases in parallel:
    1. Hurricane data — TC_TRACKS and TC_ENVELOPES_COMBINED from Snowflake
    2. Infrastructure — schools, health centres, shelters, WASH from Snowflake or stage files
    3. Impact tiles/admin — Parquet/CSV from AOTS_ANALYSIS stage via giga_store

    Returns a 64-tuple written atomically to dcc.Stores and GeoJSON components. Early
    returns (missing selections or exception) fill all outputs with empty/disabled defaults.
    `using_base_layers` is True when no impact files are found — disables impact-derived layers.
    """
    logger.info(f"=== LOAD ALL LAYERS CALLBACK STARTED ===")
    logger.info(f"Loading all layers for {country}_{storm}_{forecast_date}_{forecast_time}_{wind_threshold}")
    logger.info(f"Callback context: {callback_context.triggered}")
    
    _empty_fc = {"type": "FeatureCollection", "features": []}
    _hidden = {"hidden": True}
    if not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        logger.info("=== MISSING SELECTIONS - RETURNING EARLY ===")
        return ({}, {}, {}, {}, False, False,
                dmc.Alert("Missing selections", title="Warning", color="orange", variant="light"),
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                True, True, True, True,
                {}, {})
    try:
        # Initialize empty data stores
        tracks_data = {}
        envelope_data = {}
        tiles_data = {}
        admin_data = {}
        using_base_layers = False
        status_alert = None
        tiles_stats = {}
        admin_stats = {}

        # Load Hurricane Tracks
        try:
            conn = get_snowflake_connection()
            forecast_datetime = f"{forecast_date} {forecast_time}:00"
            
            logger.info(f"Loading tracks for storm={storm}, forecast_time={forecast_datetime}")
            
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
            logger.error(f"Error loading tracks: {e}")
        
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
                                                except Exception:
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
                                    except Exception:
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
                                logger.info(f"Pre-processed {len(gdf)} envelopes for {thresh}kt in {parse_elapsed:.2f}s")
                                return thresh, geo_dict
                            except Exception as e:
                                logger.error(f"Error pre-processing threshold {thresh}: {e}")
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
                        logger.error(f"Error pre-processing envelopes: {e}")
                
                envelope_data = {
                    'track_id': storm,
                    'forecast_time': forecast_datetime,
                    'data': envelope_df.to_dict('records'),
                    'preprocessed': preprocessed_envelopes  # Store pre-processed GeoJSON by wind threshold
                }
                envelope_elapsed = time.time() - envelope_start
                logger.info(f"Loaded {len(envelope_df)} envelopes from Snowflake in {envelope_elapsed:.2f}s")
        except Exception as e:
            logger.error(f"Error loading envelopes: {e}")
        
        # Load Impact Data (if files exist)
        # Check if data files exist for the selected time
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"

        logger.info(f"Looking for impact data files with pattern: {country}_{storm}_{forecast_datetime_str}_{wind_threshold}")
        logger.debug(f"DEBUG: ROOT_DATA_DIR = {ROOT_DATA_DIR}")
        logger.debug(f"DEBUG: VIEWS_DIR = {VIEWS_DIR}")
        logger.debug(f"DEBUG: Full base path = {os.path.join(ROOT_DATA_DIR, VIEWS_DIR)}")
        
        # Debug: Check if mount point exists and list directory contents
        base_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR)
        logger.debug(f"DEBUG: Checking if base path exists: {base_path}")
        if os.path.exists(base_path):
            logger.debug(f"DEBUG: Base path exists! Listing contents...")
            try:
                contents = os.listdir(base_path)
                logger.debug(f"DEBUG: Found {len(contents)} items in {base_path}: {contents}")
            except Exception as e:
                logger.debug(f"DEBUG: Error listing directory: {e}")
        else:
            logger.debug(f"DEBUG: Base path does NOT exist!")
        
        # Debug: Check mercator_views directory specifically
        mercator_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views')
        logger.debug(f"DEBUG: Checking mercator_views path: {mercator_path}")
        if os.path.exists(mercator_path):
            logger.debug(f"DEBUG: mercator_views directory exists!")
            try:
                mercator_files = [f for f in os.listdir(mercator_path) if f.endswith('.csv')]
                logger.debug(f"DEBUG: Found {len(mercator_files)} CSV files in mercator_views")
                # Show first 10 files matching the pattern
                pattern = f"{country}_{storm}_{forecast_datetime_str[:8]}"
                matching = [f for f in mercator_files if pattern in f]
                logger.debug(f"DEBUG: Files matching pattern '{pattern}': {matching[:10]}")
            except Exception as e:
                logger.debug(f"DEBUG: Error listing mercator_views: {e}")
        else:
            logger.debug(f"DEBUG: mercator_views directory does NOT exist!")
        
        if config.IMPACT_DATA_SOURCE != 'SQL':
            # Check for data file availability (STAGE/LOCAL/BLOB mode only)
            data_files_found = []
            missing_files = []

            schools_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            schools_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', schools_file)
            logger.debug(f"DEBUG: Checking schools file at: {schools_path}")
            logger.debug(f"DEBUG: Using giga_store.file_exists() - result: {giga_store.file_exists(schools_path)}")
            logger.debug(f"DEBUG: Using os.path.exists() - result: {os.path.exists(schools_path)}")
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
            logger.debug(f"DEBUG: Checking tiles file at: {tiles_path}")
            logger.debug(f"DEBUG: Using giga_store.file_exists() - result: {giga_store.file_exists(tiles_path)}")
            logger.debug(f"DEBUG: Using os.path.exists() - result: {os.path.exists(tiles_path)}")
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
            logger.info(f"Data availability: Found={data_files_found}, Missing={missing_files}")
        
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
                            logger.info(f"Retry attempt {attempt + 1}/{max_retries} for {dataset_name} after {delay:.1f}s delay...")
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
                                        logger.error(f"Warning: Failed to convert WKB geometry for {dataset_name}: {e}")
                                        # Try convert_to_geodataframe as fallback
                                        try:
                                            gdf = convert_to_geodataframe(df)
                                        except Exception:
                                            logger.error(f"Error: Could not convert WKB geometry for {dataset_name}")
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
                                            except Exception:
                                                # If WKT fails, might be already Shapely objects
                                                try:
                                                    # Try as Shapely objects
                                                    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                                                except Exception:
                                                    # Last resort: convert using convert_to_geodataframe
                                                    try:
                                                        gdf = convert_to_geodataframe(df)
                                                    except Exception:
                                                        logger.error(f"Error: Could not convert geometry for {dataset_name}")
                                                        return {}
                                        else:
                                            # Not a string or bytes, try as Shapely objects
                                            try:
                                                gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                                            except Exception:
                                                try:
                                                    gdf = convert_to_geodataframe(df)
                                                except Exception:
                                                    logger.error(f"Error: Could not convert geometry for {dataset_name}")
                                                    return {}
                                    else:
                                        try:
                                            gdf = convert_to_geodataframe(df)
                                        except Exception:
                                            logger.error(f"Error: Could not convert geometry for {dataset_name}")
                                            return {}
                                else:
                                    # Numeric or other type - try direct conversion
                                    try:
                                        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                                    except Exception:
                                        gdf = convert_to_geodataframe(df)
                            else:
                                # No geometry column - this shouldn't happen for spatial data
                                logger.warning(f"Warning: {dataset_name} file has no geometry column, converting...")
                                gdf = convert_to_geodataframe(df)
                        else:
                            # Unknown type, try to convert
                            gdf = convert_to_geodataframe(df)
                    
                        geo_data = gdf.__geo_interface__
                        elapsed = time.time() - start
                        logger.info(f"Loaded {dataset_name} in {elapsed:.2f}s ({len(gdf)} features)")
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
                            logger.warning(f"Retryable error reading {dataset_name} file (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")
                            continue  # Retry
                        else:
                            # Final attempt failed or non-retryable error
                            logger.error(f"Error reading {dataset_name} file: {error_msg}")
                            if attempt == max_retries - 1:
                                logger.warning(f"Failed after {max_retries} attempts for {dataset_name}")
                            return {}
                
                # If we get here, all retries failed
                if last_error:
                    logger.error(f"Failed to load {dataset_name} after {max_retries} attempts. Last error: {last_error}")
                return {}
            
            # Phase 2: MapLibre fetches PBF tiles on-demand from the tile sidecar.
            # Facility layers (schools, health, shelters, WASH) are also fetched on-demand
            # by async clientside callbacks from /geojson/facilities/ on the tile server.
            # Skip full GeoJSON loading — only fetch aggregate stats for the legend.
            import urllib.request as _urllib_req
            import urllib.parse as _urllib_parse
            # For regions, expand to '+'-joined member codes for the tile server.
            _tile_country = ('+'.join(m['value'] for m in REGION_MEMBERS[country])
                             if country in REGION_MEMBERS else country)
            try:
                _stats_url = (
                    f"{config.TILE_SERVER_URL}/stats/{_urllib_parse.quote(_tile_country)}/"
                    f"{_urllib_parse.quote(storm)}/{_urllib_parse.quote(forecast_datetime_str)}"
                    f"?wind_threshold={int(wind_threshold)}"
                )
                with _urllib_req.urlopen(_stats_url, timeout=15) as _resp:
                    tiles_stats = json.loads(_resp.read().decode())
                if "probability" in tiles_stats:
                    logger.info(f"✓ Impact data for {country}/{storm}/{forecast_datetime_str}: {list(tiles_stats.keys())[:4]}…")
                else:
                    using_base_layers = True
                    logger.info(f"No impact data for {country}/{storm}/{forecast_datetime_str} — base layers only")
                # Fetch admin-level stats separately (admin regions have much higher pop than H3 tiles)
                try:
                    _admin_stats_url = (
                        f"{config.TILE_SERVER_URL}/admin-stats/{_urllib_parse.quote(_tile_country)}/"
                        f"{_urllib_parse.quote(storm)}/{_urllib_parse.quote(forecast_datetime_str)}"
                        f"?wind_threshold={int(wind_threshold)}&admin_level=1"
                    )
                    with _urllib_req.urlopen(_admin_stats_url, timeout=15) as _ar:
                        admin_stats = json.loads(_ar.read().decode())
                    if not admin_stats:
                        admin_stats = tiles_stats
                except Exception:
                    admin_stats = tiles_stats
            except Exception as _stats_err:
                logger.info(f"Tile server stats unavailable: {_stats_err}")
                tiles_stats = {}
                admin_stats = {}
        except Exception as e:
            logger.error(f"Error loading impact data: {e}")
            status_alert = dmc.Alert(
                f"Error loading layers: {str(e)}",
                title="Error",
                color="red",
                variant="light"
            )
        
        # Phase 2: tiles/admin data is served by the tile sidecar — no GeoJSON needed here.
        tiles_data = {"type": "FeatureCollection", "features": []}
        admin_data = {"type": "FeatureCollection", "features": []}

        if using_base_layers:
            status_alert = dmc.Alert(
                "No impact data found for this storm and forecast time. Showing base context layers only.",
                title="Base Layers Only — No Impact Data Available",
                color="yellow",
                variant="light",
            )
        elif config.IMPACT_DATA_SOURCE == 'SQL' and status_alert is None:
            status_alert = dmc.Alert(
                "All layers loaded successfully",
                title="Success",
                color="green",
                variant="light",
            )

        # Compute initial hideouts for GeoJSON layers based on current UI state
        def _hideouts(layer_group, prob_checked, stats):
            pop_hidden = (not layer_group or layer_group == "none") or (
                prob_checked and layer_group in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]
            )
            prop = _LAYER_TO_PROP.get(layer_group, "population")
            e_prop = _LAYER_TO_E_PROP.get(layer_group, "probability")
            def _h(p):
                h = {"prop": p}
                if stats and p in stats:
                    h["min_val"] = stats[p].get("min")
                    h["max_val"] = stats[p].get("max")
                return h
            pop_h = {"hidden": True} if pop_hidden else _h(prop)
            prob_h = _h(e_prop) if prob_checked else {"hidden": True}
            return pop_h, prob_h

        tiles_pop_h, tiles_prob_h = _hideouts(tiles_layer_group, prob_tiles_checked, tiles_stats)
        admin_pop_h, admin_prob_h = _hideouts(admin_layer_group, prob_admin_checked, admin_stats)
        layer_key = str(time.time())

        load_elapsed = time.time() - load_start_time
        logger.info(f"=== LOAD ALL LAYERS CALLBACK COMPLETED SUCCESSFULLY in {load_elapsed:.2f}s ===")

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

        # Phase 2: derive available props from tile server stats (tiles_data is empty GeoJSON).
        # Base columns always present in BASE_MERCATOR_TILE_MAT; impact columns only for storm runs.
        # Poverty is present in tile (raster) stats but NOT in admin stats — BASE_ADMIN_GEOM_MAT
        # has no poverty data (populated only at z=14 tile level, not aggregated to admin boundaries).
        # Poverty availability in admin view therefore comes from admin_stats keys, not _BASE_PROPS.
        _BASE_PROPS = {
            'population', 'infant_population', 'school_age_population', 'adolescent_population',
            'children_total', 'built_surface_m2', 'smod_class', 'rwi', config.CCI_COL,
        }
        # Impact columns always present when storm impact data exists.
        _IMPACT_PROPS = {'E_num_shelters', 'E_num_wash'}
        tile_props  = (set(tiles_stats.keys()) | _BASE_PROPS | (_IMPACT_PROPS if not using_base_layers else set())) if tiles_stats else set()
        admin_props = (set(admin_stats.keys()) | _BASE_PROPS | (_IMPACT_PROPS if not using_base_layers else set())) if admin_stats else set()

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
            # Facility layers served by tile server on-demand — always available
            "schools": True, "health": True, "shelters": True, "wash": True,
            # Hurricane overlays
            "tracks":    bool(tracks_data    and tracks_data.get('features')),
            # envelope_data uses 'data' key (list of records), not 'features'
            "envelopes": bool(envelope_data  and envelope_data.get('data')),
        }
        # Tile and admin property layers
        for layer_key, prop_name in _prop_map.items():
            layer_availability[f"tile_{layer_key}"]  = prop_name in tile_props
            layer_availability[f"admin_{layer_key}"] = prop_name in admin_props

        vuln_tile_available  = 'E_people_in_need' in tile_props
        vuln_admin_available = 'E_people_in_need' in admin_props

        _map_cfg = COUNTRY_MAP_CONFIG.get(country, DEFAULT_MAP_CONFIG)
        # For regions, pass '+'-joined member codes so the tile server can query all members.
        tile_country = ('+'.join(m['value'] for m in REGION_MEMBERS[country])
                        if country in REGION_MEMBERS else country)
        maplibre_config = {
            "country": tile_country,
            "storm": storm,
            "forecast_date": forecast_datetime_str,
            "wind_threshold": int(wind_threshold),
            "tile_server_url": "" if config.SPCS_RUN else config.TILE_SERVER_URL,
            "stats": tiles_stats,
            "admin_stats": admin_stats,
            "center": _map_cfg["center"],
            "zoom": _map_cfg["zoom"],
            "tile_prop":  (_LAYER_TO_PROP.get(tiles_layer_group)
                           if tiles_layer_group and tiles_layer_group != "none" else None),
            "admin_prop": (_LAYER_TO_PROP.get(admin_layer_group)
                           if admin_layer_group and admin_layer_group != "none" else None),
        }

        return (tracks_data, envelope_data,
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
                True, True,   # in-need switches always start disabled; dedicated callback enables them
                True, True,
                maplibre_config,
                layer_availability)

    except Exception as e:
        logger.error(f"Error in load_all_layers: {e}")
        return ({}, {}, {}, {}, False, False,
                dmc.Alert(f"Error loading layers: {str(e)}", title="Error", color="red", variant="light"),
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                _empty_fc, False, dash.no_update, _hidden,
                True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                True, True, True, True, True, True, True, True, True, True, True, True,
                True, True, True, True,
                {}, {})


# =============================================================================
# SECTION 8 — HURRICANE LAYER TOGGLES
# Toggle visibility of hurricane track and envelope GeoJSON layers. Reads from
# dcc.Stores populated by load_all_layers.
# =============================================================================

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
                    logger.info("Detected GeoJSON format in stacked envelope geometries")
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
                                logger.error(f"Error parsing geometry: {e}")
                                geometries.append(None)
                        else:
                            geometries.append(None)
                    gdf = gpd.GeoDataFrame(df_filtered.drop(geom_col, axis=1), geometry=geometries, crs='EPSG:4326')
                else:
                    # WKT format - use optimized bulk parsing
                    logger.info("Detected WKT format in stacked envelope geometries")
                    try:
                        gdf = gpd.GeoDataFrame(df_filtered, geometry=gpd.GeoSeries.from_wkt(df_filtered[geom_col], crs='EPSG:4326'))
                    except Exception as e:
                        logger.error(f"Error with bulk WKT parsing, trying individual: {e}")
                        # Fallback: parse individually
                        from shapely import wkt as shapely_wkt
                        geometries = []
                        for wkt_str in df_filtered[geom_col]:
                            if pd.notna(wkt_str) and isinstance(wkt_str, str):
                                try:
                                    geometries.append(shapely_wkt.loads(wkt_str))
                                except Exception:
                                    geometries.append(None)
                            else:
                                geometries.append(None)
                        gdf = gpd.GeoDataFrame(df_filtered.drop(geom_col, axis=1), geometry=geometries, crs='EPSG:4326')
            else:
                # Unknown format, try WKT
                logger.info("Unknown geometry format in stacked envelopes, trying WKT")
                try:
                    gdf = gpd.GeoDataFrame(df_filtered, geometry=gpd.GeoSeries.from_wkt(df_filtered[geom_col], crs='EPSG:4326'))
                except Exception as e:
                    logger.error(f"Error parsing geometries: {e}")
                    return {"type": "FeatureCollection", "features": []}, False, dash.no_update
            
            gdf = gdf[gdf.geometry.notna()]
            parse_elapsed = time.time() - parse_start
            logger.info(f"Parsed {len(gdf)} envelope geometries in {parse_elapsed:.2f}s")
            
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
                                    logger.error(f"Error reading track file {tracks_filepath}: {e}")
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
                            logger.error(f"Error loading impact data for threshold {thresh}: {e}")
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
                logger.error(f"Could not add impact data to stacked envelopes: {e}")
            
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
            
            logger.info(f"Showing stacked envelopes for track {selected_track} at wind thresholds >= {wth_int} ({len(gdf)} envelopes)")
            return geo_dict, False, key
            
        except Exception as e:
            logger.error(f"Error creating stacked envelope view: {e}")
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
                    logger.error(f"Error reading track file {tracks_filepath}: {e}")
                    return {"type": "FeatureCollection", "features": []}, False, dash.no_update
                
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
            logger.error(f"Error creating specific track envelope: {e}")
    
    # Default probabilistic envelope behavior - now with impact data!
    if not envelope_data or not envelope_data.get('data'):
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    # Check if we have pre-processed envelopes for this wind threshold (fast path)
    if wind_threshold and envelope_data.get('preprocessed') and str(wind_threshold) in envelope_data['preprocessed']:
        logger.info(f"Using pre-processed envelopes for {wind_threshold}kt (fast path)")
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
                logger.info("Detected GeoJSON format in envelope geometries")
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
                            logger.error(f"Error parsing geometry: {e}")
                            geometries.append(None)
                    else:
                        geometries.append(None)
                gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
            else:
                # WKT format - use optimized bulk parsing
                logger.info("Detected WKT format in envelope geometries")
                try:
                    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry'], crs='EPSG:4326'))
                except Exception as e:
                    logger.error(f"Error with bulk WKT parsing, trying individual: {e}")
                    # Fallback: parse individually
                    from shapely import wkt as shapely_wkt
                    geometries = []
                    for wkt_str in df['geometry']:
                        if pd.notna(wkt_str) and isinstance(wkt_str, str):
                            try:
                                geometries.append(shapely_wkt.loads(wkt_str))
                            except Exception:
                                geometries.append(None)
                        else:
                            geometries.append(None)
                    gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries, crs='EPSG:4326')
        else:
            # Unknown format, try WKT
            logger.info("Unknown geometry format, trying WKT")
            try:
                gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry'], crs='EPSG:4326'))
            except Exception as e:
                logger.error(f"Error parsing geometries: {e}")
                return {"type": "FeatureCollection", "features": []}, False, dash.no_update
        
        gdf = gdf[gdf.geometry.notna()]
        parse_elapsed = time.time() - parse_start
        logger.info(f"Parsed {len(gdf)} envelope geometries in {parse_elapsed:.2f}s (fallback path)")
        
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
                        logger.error(f"Error reading track file {tracks_filepath}: {e}")
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
            logger.error(f"Could not add impact data to envelopes: {e}")
        
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
        logger.error(f"Error toggling envelopes: {e}")
        return {"type": "FeatureCollection", "features": []}, False, key



# =============================================================================
# SECTION 9 — MAPLIBRE CLIENTSIDE CALLBACKS
# JavaScript clientside callbacks that drive the MapLibre GL tile layer. These
# run entirely in the browser and have no Python round-trip.
# =============================================================================

from dash import clientside_callback

# Push tile config to the maplibre map whenever the store updates.
# Also accepts current radio selections as State so it can apply them immediately after config loads,
# avoiding the race where the radio callback already fired before the source existed.
clientside_callback(
    """
    function(config, tiles_val, admin_val, prob_tiles, prob_admin,
             in_need_pop_t, in_need_chi_t, in_need_pop_a, in_need_chi_a,
             tiles_stats, admin_stats) {
        if (window.applyTileConfig) {
            window.applyTileConfig(config);
        }
        const propMap   = window._AOTS_PROP_MAP    || {};
        const ePropMap  = window._AOTS_E_PROP_MAP  || {};
        const inNeedMap = window._AOTS_IN_NEED_MAP || {};
        if (window.setTileLayerProp && window.setTileLayerVisibility) {
            const inNeedT = (tiles_val === 'population' && in_need_pop_t) || (tiles_val === 'children-total' && in_need_chi_t);
            const inNeedA = (admin_val === 'population' && in_need_pop_a) || (admin_val === 'children-total' && in_need_chi_a);
            let tileProp, adminProp;
            if (inNeedT && inNeedMap[tiles_val]) {
                tileProp = inNeedMap[tiles_val];
            } else {
                const tMap = prob_tiles ? ePropMap : propMap;
                tileProp = (tiles_val && tiles_val !== 'none') ? tMap[tiles_val] : (prob_tiles ? 'probability' : null);
            }
            if (inNeedA && inNeedMap[admin_val]) {
                adminProp = inNeedMap[admin_val];
            } else {
                const aMap = prob_admin ? ePropMap : propMap;
                adminProp = (admin_val && admin_val !== 'none') ? aMap[admin_val] : (prob_admin ? 'probability' : null);
            }
            const stats  = tiles_stats  || {};
            const aStats = admin_stats  || {};
            setTimeout(function() {
                if (tileProp)  window.setTileLayerProp('aots-tiles-layer', 'tiles', tileProp, stats);
                else           window.setTileLayerVisibility('aots-tiles-layer', false);
                if (adminProp) window.setTileLayerProp('aots-admin-layer', 'admin', adminProp, aStats);
                else           window.setTileLayerVisibility('aots-admin-layer', false);
            }, 300);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output('maplibre-container', 'data-config', allow_duplicate=True),
    Input('maplibre-tile-config-store', 'data'),
    State('tiles-layer-group', 'value'),
    State('admin-layer-group', 'value'),
    State('probability-tiles-layer', 'checked'),
    State('probability-admin-layer', 'checked'),
    State('in-need-tiles-switch', 'checked'),
    State('in-need-children-tiles-switch', 'checked'),
    State('in-need-admin-switch', 'checked'),
    State('in-need-children-admin-switch', 'checked'),
    State('tiles-stats-store', 'data'),
    State('admin-stats-store', 'data'),
    prevent_initial_call=True,
)

# Sync layer visibility / active property to the maplibre map when the user changes the radio
# selection OR toggles the probability checkbox. Switches between base and E_ props accordingly.
clientside_callback(
    """
    function(tiles_val, admin_val, prob_tiles, prob_admin,
             in_need_pop_t, in_need_chi_t, in_need_pop_a, in_need_chi_a,
             tiles_stats, admin_stats) {
        const propMap   = window._AOTS_PROP_MAP    || {};
        const ePropMap  = window._AOTS_E_PROP_MAP  || {};
        const inNeedMap = window._AOTS_IN_NEED_MAP || {};

        if (!window.setTileLayerProp || !window.setTileLayerVisibility)
            return window.dash_clientside.no_update;

        const stats  = tiles_stats  || {};
        const aStats = admin_stats  || {};

        const inNeedT = (tiles_val === 'population' && in_need_pop_t) || (tiles_val === 'children-total' && in_need_chi_t);
        const inNeedA = (admin_val === 'population' && in_need_pop_a) || (admin_val === 'children-total' && in_need_chi_a);

        let tileProp, adminProp;
        if (inNeedT && inNeedMap[tiles_val]) {
            tileProp = inNeedMap[tiles_val];
        } else {
            const tMap = prob_tiles ? ePropMap : propMap;
            tileProp = (tiles_val && tiles_val !== 'none') ? tMap[tiles_val] : (prob_tiles ? 'probability' : null);
        }
        if (inNeedA && inNeedMap[admin_val]) {
            adminProp = inNeedMap[admin_val];
        } else {
            const aMap = prob_admin ? ePropMap : propMap;
            adminProp = (admin_val && admin_val !== 'none') ? aMap[admin_val] : (prob_admin ? 'probability' : null);
        }

        if (tileProp) {
            window.setTileLayerProp('aots-tiles-layer', 'tiles', tileProp, stats);
        } else {
            window.setTileLayerVisibility('aots-tiles-layer', false);
        }

        if (adminProp) {
            window.setTileLayerProp('aots-admin-layer', 'admin', adminProp, aStats);
        } else {
            window.setTileLayerVisibility('aots-admin-layer', false);
        }

        return window.dash_clientside.no_update;
    }
    """,
    Output('maplibre-container', 'data-layer', allow_duplicate=True),
    Input('tiles-layer-group', 'value'),
    Input('admin-layer-group', 'value'),
    Input('probability-tiles-layer', 'checked'),
    Input('probability-admin-layer', 'checked'),
    Input('in-need-tiles-switch', 'checked'),
    Input('in-need-children-tiles-switch', 'checked'),
    Input('in-need-admin-switch', 'checked'),
    Input('in-need-children-admin-switch', 'checked'),
    State('tiles-stats-store', 'data'),
    State('admin-stats-store', 'data'),
    prevent_initial_call=True,
)

# Viewport sync fallback: fires on every Leaflet moveend (and on initial load).
# The primary real-time sync is via dl.Map eventHandlers → sync_maplibre_on_move.
# This ensures MapLibre is aligned even on initial render and after country changes.
clientside_callback(
    """function(viewport) {
        if (!window._aots_maplibre || !viewport || !viewport.center)
            return window.dash_clientside.no_update;
        var lat = viewport.center[0], lng = viewport.center[1], z = (viewport.zoom || 6) - 1;
        if (window._aots_maplibre_ready) {
            window._aots_maplibre.jumpTo({ center: [lng, lat], zoom: z });
        } else {
            window._aots_initial_viewport = { center: [lng, lat], zoom: z };
        }
        return window.dash_clientside.no_update;
    }""",
    Output('maplibre-container', 'data-viewport', allow_duplicate=True),
    Input('main-map', 'viewport'),
    prevent_initial_call='initial_duplicate',
)


# Pre-warm tile server cache when a storm is loaded (fire-and-forget HTTP GET)
clientside_callback(
    """
    function(config) {
        if (!config || !config.country || !config.storm) return window.dash_clientside.no_update;
        var base = (config.tile_server_url != null && config.tile_server_url !== '') ? config.tile_server_url : window.location.origin;
        var url = base + '/preload/'
            + encodeURIComponent(config.country) + '/'
            + encodeURIComponent(config.storm) + '/'
            + encodeURIComponent(config.forecast_date)
            + '?wind_threshold=' + config.wind_threshold;
        fetch(url).catch(function() {});  // fire-and-forget, ignore errors
        return window.dash_clientside.no_update;
    }
    """,
    Output('maplibre-container', 'data-preload', allow_duplicate=True),
    Input('maplibre-tile-config-store', 'data'),
    prevent_initial_call=True,
)


# =============================================================================
# SECTION 10 — TILE SERVER CACHE PRELOAD
# Background HTTP call to warm the tile-server DataFrame cache when the user
# changes their selection, so tiles load instantly when 'Load Layers' is clicked.
# =============================================================================

# -----------------------------------------------------------------------------
# Preload helper
# Fires whenever the user changes country, storm, forecast date/time, or wind
# threshold. A daemon thread calls the tile server's /preload endpoint so the
# DataFrame cache is warm before the user clicks "Load Layers".
# The callback itself returns nothing visible — it only triggers the side-effect.
# -----------------------------------------------------------------------------

def _do_preload(country, storm, forecast_date, forecast_time, wind_threshold):
    """Fire-and-forget: warm the tile server cache for the given selection."""
    try:
        # Tile server expects compact YYYYMMDDHHMMSS format matching FORECAST_DATE in Snowflake
        date_compact = forecast_date.replace('-', '')   # '2026-05-10' → '20260510'
        time_compact = forecast_time.replace(':', '')   # '00:00' → '0000'
        forecast_date_str = f"{date_compact}{time_compact}00"  # '20260510000000'
        threshold = int(wind_threshold) if wind_threshold else 34
        # For regions, expand to '+'-joined member codes the tile server understands
        tile_country = ('+'.join(m['value'] for m in REGION_MEMBERS[country])
                        if country in REGION_MEMBERS else country)
        url = f"http://127.0.0.1:8001/preload/{tile_country}/{storm}/{forecast_date_str}"
        requests.get(url, params={"wind_threshold": threshold}, timeout=30)
    except Exception:
        pass  # Best-effort — never propagate errors back to Dash


@callback(
    Output('preload-dummy-store', 'data'),
    [Input('effective-country-store', 'data'),
     Input('storm-select', 'value'),
     Input('forecast-date', 'value'),
     Input('forecast-time', 'value'),
     Input('wind-threshold-select', 'value')],
    prevent_initial_call=True,
)
def trigger_tile_preload(country, storm, forecast_date, forecast_time, wind_threshold):
    """Warm the tile server cache in the background whenever the selection changes."""
    if not all([country, storm, forecast_date, forecast_time]):
        return dash.no_update

    t = threading.Thread(
        target=_do_preload,
        args=(country, storm, forecast_date, forecast_time, wind_threshold or "34"),
        daemon=True,
    )
    t.start()
    return dash.no_update


# =============================================================================
# SECTION 11 — PAGE REGISTRATION
# Register this module as a Dash page and import callback modules so their
# @callback decorators fire.
# =============================================================================

# Register callback modules — importing them causes @callback decorators to fire
from callbacks import overlays, tiles_and_admin, metrics  # noqa: F401, E402

dash.register_page(__name__, path="/", name="Ahead of the Storm")

