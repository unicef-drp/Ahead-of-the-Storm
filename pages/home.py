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

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config

from dash_extensions.javascript import assign

#### Constant - add as selector at some point
ZOOM_LEVEL = 14

##############################################

###### Colors for the tiles - move to a different file

all_colors = {
    # probability colors: starts at light yellow and increases to dark red
    'probability': ['transparent',
                    '#ffffcc','#fff3b0','#ffe680','#ffd34d','#ffc300',
                    '#ffb000','#ff8c00','#ff6a00','#ff4500','#e31a1c','#b10026'],
    'population': ['transparent','#87ceeb', '#4682b4', '#1e90ff', '#0000cd', '#000080', '#191970'],
    'E_population': ['transparent',
                    '#ffffcc','#fff3b0','#ffe680','#ffd34d','#ffc300',
                    '#ffb000','#ff8c00','#ff6a00','#ff4500','#e31a1c','#b10026'],
    'school_age_population':['transparent','#90ee90','#32cd32','#228b22','#006400','#2e8b57','#1c4a1c'],
    'E_school_age_population':['transparent',
                    '#ffffcc','#fff3b0','#ffe680','#ffd34d','#ffc300',
                    '#ffb000','#ff8c00','#ff6a00','#ff4500','#e31a1c','#b10026'],
    'built_surface_m2':['transparent','#d3d3d3','#a9a9a9','#808080','#8b4513','#654321','#2f1b14'],
    'E_built_surface_m2':['transparent',
                    '#ffffcc','#fff3b0','#ffe680','#ffd34d','#ffc300',
                    '#ffb000','#ff8c00','#ff6a00','#ff4500','#e31a1c','#b10026'],
    'smod_class': ['transparent','#dda0dd','#9370db', '#4b0082'],
    # RWI: 9 colors from negative (red/yellow) to neutral (gray) to positive (green)
    # Format: transparent, 4 negative colors (red to yellow), gray (neutral at 0), 4 positive colors (light green to dark green)
    'rwi':['transparent','#d73027','#f46d43','#fdae61','#fee08b','#808080','#d9ef8b','#a6d96a','#66bd63','#1a9850'],
}

def create_legend_divs(color_key, skip_transparent=True):
    """Generate legend HTML divs from all_colors dictionary
    
    Args:
        color_key: Key in all_colors dict (e.g., 'population', 'probability')
        skip_transparent: Whether to skip the first color (usually 'transparent')
    
    Returns:
        List of HTML div elements for legend
    """
    if color_key not in all_colors:
        return []
    
    colors = all_colors[color_key]
    
    if skip_transparent and colors and colors[0] == 'transparent':
        actual_colors = colors[1:]
    else:
        actual_colors = colors
    
    if not actual_colors:
        return []
    
    # Calculate width percentage for each color block
    width_pct = 100 / len(actual_colors)
    
    legend_divs = []
    for i, color in enumerate(actual_colors):
        # Last item doesn't need right margin
        margin_right = "1px" if i < len(actual_colors) - 1 else ""
        legend_divs.append(
            html.Div(style={
                "width": f"{width_pct}%",
                "height": "12px",
                "backgroundColor": color,
                "border": "1px solid #ccc",
                "display": "inline-block",
                "marginRight": margin_right
            })
        )
    
    return legend_divs

##############
def update_tile_features(tiles_data_in,property):
    if not tiles_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    # Ensure tiles_data has proper GeoJSON structure
    if not isinstance(tiles_data_in, dict) or 'features' not in tiles_data_in:
        print(f"ERROR: Invalid tiles_data structure: {type(tiles_data)}")
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    tiles_data = copy.deepcopy(tiles_data_in)
    key = hashlib.md5(json.dumps(tiles_data, sort_keys=True).encode()).hexdigest()

    try:
        colors = all_colors[property]
        buckets = len(colors)
        if property=='smod_class':
            # Settlement classification: discrete categories (0=no data, 10=rural, 20=urban cluster, 30=urban center)
            # Divide by 10 to get category (10->1, 20->2, 30->3)
            values = [int(f["properties"][property]/10) if not pd.isna(f["properties"][property]) else 0 for f in tiles_data["features"]]
        else:
            values = [f["properties"][property] for f in tiles_data["features"]]
        
        clean_values = [v for v in values if not pd.isna(v)]
        nan_count = len(values)-len(clean_values)
        zero_count = len([v for v in clean_values if v==0])
        
        # For probability, use a fixed scale (0-1 or 0-100) regardless of actual max value
        if property == 'probability':
            max_val = 1.0  # Probability is always 0-1
        else:
            max_val = max(clean_values) if clean_values else 0
        
        # Separate colors into actual color buckets (excluding transparent)
        actual_colors = colors[1:]  # Skip transparent
        actual_buckets = len(actual_colors)
        
        if not clean_values:
            color_prop = [colors[0]] * len(values)  # All values are NaN - use transparent
        elif max_val == 0 and property != 'rwi':
            color_prop = [colors[0]] * len(values)  # All values are 0 - use transparent (not for rwi)
        else:
            color_prop = []
            if property == 'rwi':
                # Fixed symmetric scale for RWI from -1 to 1; 0 should be mid color (not transparent)
                min_val = -1.0
                max_val_fixed = 1.0
                denom = (max_val_fixed - min_val) if (max_val_fixed - min_val) != 0 else 1.0
                for val in values:
                    if pd.isna(val):
                        color_prop.append(colors[0])  # transparent for NaN only
                    else:
                        norm = (float(val) - min_val) / denom  # 0..1
                        idx = int(min(max(round(norm * (actual_buckets - 1)), 0), actual_buckets - 1))
                        color_prop.append(actual_colors[idx])
            elif property == 'smod_class':
                # Settlement classification: categorical mapping (0=transparent, 1=first color, 2=second, 3=third)
                for val in values:
                    if pd.isna(val):
                        color_prop.append(colors[0])  # transparent for NaN
                    elif val == 0:
                        color_prop.append(colors[0])  # transparent for 0 (no data)
                    elif val in [1, 2, 3]:
                        index = int(val) - 1  # Map 1->0, 2->1, 3->2
                        color_prop.append(actual_colors[index])  # Use actual color for category
                    else:
                        color_prop.append(colors[0])  # transparent for unknown values
            else:
                # Use actual color buckets (excluding transparent); transparent for 0 or NaN
                step = max_val / actual_buckets if actual_buckets > 0 else 1.0
                for val in values:
                    if pd.isna(val) or val == 0:
                        color_prop.append(colors[0])  # Use transparent for NaN or 0
                    else:
                        index = min(int(val / step), actual_buckets - 1)
                        color_prop.append(actual_colors[index])  # Use actual color bucket
        
        for feature, color in zip(tiles_data["features"], color_prop):
            feature['properties']['_color'] = color
            if color==colors[0]:
                feature['properties']['_fillOpacity'] = 0.0
            else:
                feature['properties']['_fillOpacity'] = 0.7
            feature['properties']['_weight'] = 1
            feature['properties']['_opacity'] = 0.8
        
        # Debug output
        if values:
            print(f"{property} tiles debug - Total tiles: {len(values)}")
            print(f"Zero values: {zero_count}, NaN values: {nan_count}")
            print(f"Min {property}: {min(values)}, Max {property}: {max(values)}")
            print(f"Sample values: {sorted(set(values))[:10]}")
        
        # Debug: Check data structure
        if tiles_data:
            print(f"Tiles data type: {type(tiles_data)}")
            if isinstance(tiles_data, dict):
                print(f"Tiles data keys: {list(tiles_data.keys())}")
                if 'features' in tiles_data:
                    print(f"Features count: {len(tiles_data['features'])}")
                else:
                    print("ERROR: No 'features' key in tiles_data!")
            else:
                print(f"ERROR: tiles_data is not a dict, it's {type(tiles_data)}")
        
        return tiles_data, True, key
    except Exception as e:
        print(f"Error styling {property} tiles: {e}")
        return tiles_data, True, key
##############

###############################################

###### Javascript functions - move to a different file

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

# JavaScript styling for tiles with value-based coloring
style_tiles = assign("""
function(feature, context) {
    const props = feature.properties || {};
    const color = props._color || '#808080';
    const fillOpacity = props._fillOpacity || 0.7;
    const weight = props._weight || 1;
    const opacity = props._opacity || 0.8;
    
    return {
        color: color,
        weight: weight,
        opacity: opacity,
        fillColor: color,
        fillOpacity: fillOpacity
    };
}
""")

# JavaScript point-to-layer function for schools and health centers
point_to_layer_schools_health = assign("""
function(feature, latlng, context) {
    const props = feature.properties || {};
    const color = props._color || '#808080';
    const radius = props._radius || 12;
    const opacity = props._opacity || 0.8;
    const weight = props._weight || 2;
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
    const props = feature.properties || {};
    const severity_population = props.severity_population || 0;
    const max_population = props.max_population || 1;
    
    // Gray for no data or zero impact
    if (!severity_population || severity_population === 0) {
        return {color: '#808080', weight: 2, fillColor: '#808080', fillOpacity: 0.3};
    }
    
    // Calculate relative severity (0 to 1)
    const relativeSeverity = Math.min(severity_population / max_population, 1);
    
    // Smooth gradient from yellow to red using color interpolation
    // Using cubic easing for smoother transitions
    const easedSeverity = relativeSeverity * relativeSeverity * relativeSeverity;
    
    // Color interpolation helper
    const interpolateColor = (startColor, endColor, fraction) => {
        const start = parseInt(startColor.slice(1), 16);
        const end = parseInt(endColor.slice(1), 16);
        const r = Math.round(((start >> 16) & 0xff) * (1 - fraction) + ((end >> 16) & 0xff) * fraction);
        const g = Math.round(((start >> 8) & 0xff) * (1 - fraction) + ((end >> 8) & 0xff) * fraction);
        const b = Math.round((start & 0xff) * (1 - fraction) + (end & 0xff) * fraction);
        return '#' + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
    };
    
    // Interpolate between yellow (#FFFF00) and dark red (#8B0000)
    const color = interpolateColor('#FFFF00', '#8B0000', easedSeverity);
    
    // Opacity also increases with severity
    const fillOpacity = 0.3 + (easedSeverity * 0.6);
    
    return {color: color, weight: 2, fillColor: color, fillOpacity: fillOpacity};
}
""")

# Tooltip functions for displaying data on hover
tooltip_tracks = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const member = props.ensemble_member || 'N/A';
    const type = props.member_type || 'N/A';
    
    const label = type === 'control' ? 'Control Track' : 'Ensemble Track';
    
    const content = `
        <div style="font-size: 13px; font-weight: 600; color: #1cabe2; margin-bottom: 5px;">
            ${label}
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Ensemble Member:</strong> #${member}
        </div>
    `;
    
    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_envelopes = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const wind_threshold = props.wind_threshold || props.WIND_THRESHOLD || 'N/A';
    const ensemble_member = props.ensemble_member || props.ENSEMBLE_MEMBER || 'N/A';
    const severity_population = props.severity_population || 0;
    const severity_schools = props.severity_schools || 0;
    const severity_hcs = props.severity_hcs || 0;
    const severity_built_surface_m2 = props.severity_built_surface_m2 || 0;
    const severity_children = props.severity_children || 0;
    
    const formatNumber = (num) => {
        if (typeof num === 'number') {
            return new Intl.NumberFormat('en-US').format(Math.round(num));
        }
        return num;
    };
    
    // Always show same structure, use N/A when data not available
    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #ff0000; margin-bottom: 5px;">
            Hurricane Envelope
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Wind Threshold:</strong> ${wind_threshold}
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Ensemble Member:</strong> ${ensemble_member !== 'N/A' ? '#' + ensemble_member : 'N/A'}
        </div>
    `;
    
    // Always show impact section
    content += `
        <hr style="margin: 5px 0; border: none; border-top: 1px solid #ddd;">
        <div style="font-size: 11px; color: #777; margin-top: 5px;">
            <strong>Impact:</strong>
        </div>
        <div style="font-size: 11px; color: #555;">
            Children: ${severity_children > 0 ? formatNumber(severity_children) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Population: ${severity_population > 0 ? formatNumber(severity_population) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Schools: ${severity_schools > 0 ? formatNumber(severity_schools) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Health Centers: ${severity_hcs > 0 ? formatNumber(severity_hcs) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Built Surface: ${severity_built_surface_m2 > 0 ? formatNumber(severity_built_surface_m2) + ' m²' : 'N/A'}
        </div>
    `;
    
    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_schools = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const probability = props.probability || 0;
    const school_id = props.school_id_giga || props.school_id || 'N/A';
    const school_name = props.school_name || props.name || props.school || 'N/A';
    
    const formatPercent = (prob) => {
        if (typeof prob === 'number') {
            return (prob * 100).toFixed(1) + '%';
        }
        return 'N/A';
    };
    
    const content = `
        <div style="font-size: 13px; font-weight: 600; color: #4169E1; margin-bottom: 5px;">
            School
        </div>
        ${school_name !== 'N/A' ? `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${school_name}</div>` : ''}
        <div style="font-size: 12px; color: #555;">
            <strong>Impact Probability:</strong> ${formatPercent(probability)}
        </div>
    `;
    
    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_health = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const probability = props.probability || 0;
    const osm_id = props.osm_id || 'N/A';
    const facility_name = props.facility_name || props.name || props.amenity_name || 'N/A';
    const facility_type = props.facility_type || props.amenity_type || props.type || 'N/A';
    
    const formatPercent = (prob) => {
        if (typeof prob === 'number') {
            return (prob * 100).toFixed(1) + '%';
        }
        return 'N/A';
    };
    
    const content = `
        <div style="font-size: 13px; font-weight: 600; color: #228B22; margin-bottom: 5px;">
            Health Facility
        </div>
        ${facility_name !== 'N/A' ? `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${facility_name}</div>` : ''}
        ${facility_type !== 'N/A' ? `<div style="font-size: 11px; color: #777;"><strong>Type:</strong> ${facility_type}</div>` : ''}
        <div style="font-size: 12px; color: #555;">
            <strong>Impact Probability:</strong> ${formatPercent(probability)}
        </div>
    `;
    
    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_tiles = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    
    const formatNumber = (num) => {
        if (typeof num === 'number') {
            return new Intl.NumberFormat('en-US').format(Math.round(num));
        }
        return num;
    };
    
    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #4169E1; margin-bottom: 5px;">
            Tile Statistics
        </div>
    `;
    
    // Expected impact values (from hurricane envelopes)
    const E_population = props.E_population || props.expected_population || 0;
    const E_built_surface_m2 = props.E_built_surface_m2 || props.expected_built_surface || 0;
    const E_num_schools = props.E_num_schools || 0;
    const E_school_age_population = props.E_school_age_population || 0;
    const E_num_hcs = props.E_num_hcs || 0;
    const E_rwi = props.E_rwi || 0;
    const probability = props.probability || 0;
    
    // Base infrastructure values
    const population = props.population || 0;
    const built_surface = props.built_surface_m2 || 0;
    const num_schools = props.num_schools || 0;
    const school_age_pop = props.school_age_population || 0;
    const num_hcs = props.num_hcs || 0;
    const rwi = props.rwi || 0;
    const smod_class = props.smod_class || 'N/A';
    
    // Settlement classification mapping (values are 0, 10, 20, 30)
    const getSettlementLabel = (classNum) => {
        if (classNum === null || classNum === undefined || classNum === '' || classNum === 0) return 'No Data';
        // Handle both original (10, 20, 30) and processed (1, 2, 3) values
        if (classNum === 1 || classNum === 10) return 'Rural';
        if (classNum === 2 || classNum === 20) return 'Urban Clusters';
        if (classNum === 3 || classNum === 30) return 'Urban Centers';
        return 'N/A';
    };
    
    // Formatting helper functions
    const formatValue = (val) => {
        if (val === null || val === undefined || (typeof val === 'number' && isNaN(val))) return 'N/A';
        if (typeof val === 'number') return formatNumber(val);
        return val === '' ? 'N/A' : val;
    };
    
    const formatSettlement = (val) => {
        if (val === null || val === undefined || val === '' || (typeof val === 'number' && isNaN(val))) return 'N/A';
        if (typeof val === 'number') return getSettlementLabel(val);
        return 'N/A';
    };
    
    const formatDecimal = (val) => {
        if (val === null || val === undefined || (typeof val === 'number' && isNaN(val)) || val === '') return 'N/A';
        return val.toFixed(2);
    };
    
    // Show expected impact if available
    if (probability > 0) {
        content += `
        <div style="font-size: 11px; color: #dc143c; margin-top: 5px; font-weight: 600;">
            Expected Impact:
        </div>
        <div style="font-size: 11px; color: #555;">
            Hurricane Impact Probability: ${(probability * 100).toFixed(1)}%
        </div>
        <hr style="margin: 5px 0; border: none; border-top: 1px solid #ddd;">
        `;
    }
    
    // Show tile data - always show all fields
    content += `
    <div style="font-size: 11px; color: #777; margin-top: 5px;">
        <strong>Tile Base Data:</strong>
    </div>
    <div style="font-size: 11px; color: #555;">
        Total Population: ${formatValue(population)}
    </div>
    <div style="font-size: 11px; color: #555;">
        School-Age Population: ${formatValue(school_age_pop)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Schools: ${formatValue(num_schools)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Health Centers: ${formatValue(num_hcs)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Built Surface: ${built_surface > 0 ? formatNumber(built_surface) + ' m²' : 'N/A'}
    </div>
    <div style="font-size: 11px; color: #555;">
        Settlement: ${formatSettlement(smod_class)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Relative Wealth Index: ${formatDecimal(rwi)}
    </div>
    `;
    
    layer.bindTooltip(content, {sticky: true});
}
""")

##############################################

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
##############################################

#### Snowflake functions - move to data ####
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
    
###########################################

###### Load initial metadata
metadata_df = get_snowflake_data()
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

###### DASH components #########

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
                                            {"value": "AIA", "label": "Anguilla"},
                                            {"value": "ATG", "label": "Antigua and Barbuda"},
                                            {"value": "BLZ", "label": "Belize"},
                                            {"value": "VGB", "label": "British Virgin Islands"},
                                            {"value": "DMA", "label": "Dominica"},
                                            {"value": "DOM", "label": "Dominican Republic"},
                                            {"value": "GRD", "label": "Grenada"},
                                            {"value": "MSR", "label": "Montserrat"},
                                            {"value": "NIC", "label": "Nicaragua"},
                                            {"value": "KNA", "label": "Saint Kitts and Nevis"},
                                            {"value": "LCA", "label": "Saint Lucia"},
                                            {"value": "VCT", "label": "Saint Vincent and the Grenadines"}
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
                                        value="2025-10-22",  # Default to most recent date
                                        mb="xs"
                                    ),
                                    dmc.Select(
                                        id="forecast-time",
                                        placeholder="Select forecast time...",
                                        data=[],  # Will be populated based on selected date
                                        value="18:00",  # Default to most recent time
                                        mb="xs"
                                    ),
                                    dmc.Select(
                                        id="storm-select",
                                        placeholder="Select hurricane...",
                                        data=[],  # Will be populated based on date and time
                                        value="FENGSHEN",  # Default to most recent storm
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
                                        mb="md",
                                        loaderProps={"type": "dots"}
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
                                        
                                        dmc.Text("Infrastructure Impact", size="sm", fw=600, mb="xs", mt="md"),
                                        dmc.Checkbox(id="schools-layer", label="Schools Impact", checked=False, mb="xs", disabled=True),
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
                                        
                                        dmc.Checkbox(id="health-layer", label="Health Centers Impact", checked=False, mb="xs", disabled=True),
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
                                        
                                        dmc.Text("Population & Infrastructure Tiles", size="sm", fw=600, mb="xs", mt="md"),

                                        dmc.Checkbox(id="probability-tiles-layer", label="Impact Probability", checked=False, mb="xs", disabled=True),
                                        # Discrete probability legend with buckets
                                        html.Div(id="probability-legend", children=[
                                            dmc.Grid([
                                                dmc.GridCol(span=1.5, children=[dmc.Text("0%", size="xs", c="dimmed")]),
                                                dmc.GridCol(span=9, children=html.Div(
                                                    create_legend_divs('probability'),
                                                    style={"display": "flex", "width": "100%"}
                                                )),
                                                dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
                                            ], gutter="xs", mb="xs")
                                        ], style={"display": "none"}),

                                        dmc.Divider(),

                                        dmc.RadioGroup([
                                            dmc.Radio(id="none-tiles-layer", label="None", value="none", mb="xs"),
                                            dmc.Divider(mb="xs", mt="xs"),
                                            dmc.Radio(id="population-tiles-layer", label="Population Density", value="population", mb="xs"),
                                            dmc.Radio(id="school-age-tiles-layer", label="School-Age Population", value="school-age", mb="xs"),
                                            dmc.Radio(id="built-surface-tiles-layer", label="Built Surface Area", value="built-surface", mb="xs"),
                                            dmc.Divider(mb="xs", mt="xs"),
                                            dmc.Text("Context Data", size="xs", fw=600, c="dimmed", mb="xs", style={"textTransform": "uppercase", "letterSpacing": "1px"}),
                                            dmc.Radio(id="settlement-tiles-layer", label="Settlement Classification", value="settlement", mb="xs"),
                                            dmc.Radio(id="rwi-tiles-layer", label="Relative Wealth Index", value="rwi", mb="xs"),
                                        ], id="tiles-layer-group", value="none"),

                                        # Legend grids for each layer
                                        dmc.Grid([
                                            dmc.GridCol(span=1.5, children=[dmc.Text("0", size="xs", c="dimmed")]),
                                            dmc.GridCol(span=9, children=html.Div(
                                                create_legend_divs('population'),
                                                style={"display": "flex", "width": "100%"}
                                            )),
                                            dmc.GridCol(span=1.5, children=[dmc.Text("5000+", size="xs", c="dimmed")]),
                                        ], id="population-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                                        
                                        dmc.Grid([
                                            dmc.GridCol(span=1.5, children=[dmc.Text("0", size="xs", c="dimmed")]),
                                            dmc.GridCol(span=9, children=html.Div(
                                                create_legend_divs('school_age_population'),
                                                style={"display": "flex", "width": "100%"}
                                            )),
                                            dmc.GridCol(span=1.5, children=[dmc.Text("750+", size="xs", c="dimmed")]),
                                        ], id="school-age-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                                        
                                        dmc.Grid([
                                            dmc.GridCol(span=1.5, children=[dmc.Text("0", size="xs", c="dimmed")]),
                                            dmc.GridCol(span=9, children=html.Div(
                                                create_legend_divs('built_surface_m2'),
                                                style={"display": "flex", "width": "100%"}
                                            )),
                                            dmc.GridCol(span=1.5, children=[dmc.Text("50k+", size="xs", c="dimmed")]),
                                        ], id="built-surface-legend", style={"display": "none"}, gutter="xs", mb="xs"),
                                        
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
                                        
                                        dmc.Text("Note: Select one tile layer to view on the map", size="xs", c="dimmed", mb="md")
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
                    dcc.Store(id="population-tiles-data-store", data={}),
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
                                style=style_tracks,
                                onEachFeature=tooltip_tracks
                            ),
                            # Hurricane Envelopes Layer
                            dl.GeoJSON(
                                id="envelopes-json-test",
                                data={},
                                zoomToBounds=True,
                                style=style_envelopes,
                                onEachFeature=tooltip_envelopes
                            ),
                            # Schools Impact Layer
                            dl.GeoJSON(
                                id="schools-json-test",
                                data={},
                                zoomToBounds=True,
                                pointToLayer=point_to_layer_schools_health,
                                onEachFeature=tooltip_schools
                            ),
                            # Health Centers Impact Layer
                            dl.GeoJSON(
                                id="health-json-test",
                                data={},
                                zoomToBounds=True,
                                pointToLayer=point_to_layer_schools_health,
                                onEachFeature=tooltip_health
                            ),
                            # Population Density Tiles Layer
                            dl.GeoJSON(
                                id="population-tiles-json",
                                data={},
                                zoomToBounds=True,
                                style=style_tiles,
                                onEachFeature=tooltip_tiles
                            ),
                            
                            # Impact Probability Tiles Layer
                            dl.GeoJSON(
                                id="probability-tiles-json",
                                data={},
                                zoomToBounds=False,
                                style=style_tiles,
                                onEachFeature=tooltip_tiles
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
                    # Impact Summary Section
                    dmc.Paper(
                        [
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
                                                dmc.Text("Low", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                                dmc.Badge("Member", id="low-impact-badge", size="xs", color="blue", variant="light", style={"marginTop": "2px"})
                                            ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"}),
                                            dmc.TableTh("Probabilistic", style={"fontWeight": 700, "textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "paddingTop": "8px", "height": "60px", "verticalAlign": "top"}),
                                            dmc.TableTh([
                                                dmc.Text("High", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                                dmc.Badge("Member", id="high-impact-badge", size="xs", color="red", variant="light", style={"marginTop": "2px"})
                                            ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"})
                                        ])
                                    ]),
                                    dmc.TableTbody([
                                        dmc.TableTr([
                                            dmc.TableTd("Children", style={"fontWeight": 500}),
                                            dmc.TableTd("N/A", id="children-affected-low", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("N/A", id="children-affected-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("N/A", id="children-affected-high", style={"textAlign": "center", "fontWeight": 500})
                                        ]),
                                        dmc.TableTr([
                                            dmc.TableTd("Schools", style={"fontWeight": 500}),
                                            dmc.TableTd("0", id="schools-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("2", id="schools-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("39", id="schools-count-high", style={"textAlign": "center", "fontWeight": 500})
                                        ]),
                                        dmc.TableTr([
                                            dmc.TableTd("Health Centers", style={"fontWeight": 500}),
                                            dmc.TableTd("0", id="health-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("1", id="health-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("0", id="health-count-high", style={"textAlign": "center", "fontWeight": 500})
                                        ]),
                                        dmc.TableTr([
                                            dmc.TableTd("Population", style={"fontWeight": 500}),
                                            dmc.TableTd("0", id="population-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("2,482", id="population-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("59,678", id="population-count-high", style={"textAlign": "center", "fontWeight": 500})
                                        ]),
                                        dmc.TableTr([
                                            dmc.TableTd([
                                                html.Span("Built Surface m"),
                                                html.Sup("2"),
                                            ], style={"fontWeight": 500}),
                                            dmc.TableTd("0", id="bsm2-count-low", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("2,482", id="bsm2-count-probabilistic", style={"textAlign": "center", "fontWeight": 500}),
                                            dmc.TableTd("59,678", id="bsm2-count-high", style={"textAlign": "center", "fontWeight": 500})
                                        ])
                                    ])
                                ],
                                striped=True,
                                highlightOnHover=True,
                                withTableBorder=True,
                                withColumnBorders=True
                            )
                        ],
                        p="md",
                        shadow="xs",
                        style={"borderLeft": "3px solid #1cabe2", "marginBottom": "16px"}
                    ),
                    
                    # Specific Track View Section
                    dmc.Paper(
                        [
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
                ],
                span=3,
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"}
            )
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


########################### Callbacks ####################################

# Callbacks for interactive functionality
@callback(
    [Output("children-affected-low", "children"),
     Output("children-affected-probabilistic", "children"),
     Output("children-affected-high", "children"),
     Output("schools-count-low", "children"),
     Output("schools-count-probabilistic", "children"),
     Output("schools-count-high", "children"),
     Output("health-count-low", "children"),
     Output("health-count-probabilistic", "children"),
     Output("health-count-high", "children"),
     Output("population-count-low", "children"),
     Output("population-count-probabilistic", "children"),
     Output("population-count-high", "children"),
     Output("bsm2-count-low", "children"),
     Output("bsm2-count-probabilistic", "children"),
     Output("bsm2-count-high", "children"),
     Output("low-impact-badge", "children"),
     Output("high-impact-badge", "children"),],
    [Input("storm-select", "value"),
     Input("wind-threshold-select", "value"),
     Input("country-select", "value"),
     Input("forecast-date", "value"),
     Input("forecast-time", "value")],
    prevent_initial_call=True
)
def update_impact_metrics(storm, wind_threshold, country, forecast_date, forecast_time):
    """Update impact metrics for all three scenarios based on storm, wind threshold, and country selection"""
    
    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        # Return all scenarios with default values
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
    
    # Calculate probabilistic impact metrics
    
    try:
        # Construct the filename for the tiles impact view
        date_str = forecast_date.replace('-', '')  # Convert "2025-10-15" to "20251015"
        time_str = forecast_time.replace(':', '')  # Convert "00:00" to "0000"
        forecast_datetime = f"{date_str}{time_str}00"  # Add seconds: "20251015000000"
        
        filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}_{ZOOM_LEVEL}.csv"
        filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "mercator_views", filename)
        
        print(f"Impact metrics: Looking for file {filename}")
        
        # Initialize all scenario results
        low_results = {"children": "N/A", "schools": "N/A", "health": "N/A", "population": "N/A", "built_surface_m2":"N/A"}
        probabilistic_results = {"children": "N/A", "schools": "N/A", "health": "N/A", "population": "N/A", "built_surface_m2":"N/A"}
        high_results = {"children": "N/A", "schools": "N/A", "health": "N/A", "population": "N/A", "built_surface_m2":"N/A"}
        
        # Initialize member badges
        low_member_badge = "N/A"
        high_member_badge = "N/A"
        
        if giga_store.file_exists(filepath):
            try:
                df = read_dataset(giga_store, filepath)
                
                # Calculate PROBABILISTIC scenario (from tiles data)
                if 'E_school_age_population' in df.columns and not df['E_school_age_population'].isna().all():
                    probabilistic_results["children"] = df['E_school_age_population'].sum()#(gdf['probability'] * gdf['school_age_population']).sum()
                else:
                    probabilistic_results["children"] = "N/A"
                
                probabilistic_results["schools"] = df['E_num_schools'].sum() if 'E_num_schools' in df.columns else "N/A"
                probabilistic_results["health"] = df['E_num_hcs'].sum() if 'E_num_hcs' in df.columns else "N/A"
                probabilistic_results["population"] = df['E_population'].sum() if 'E_population' in df.columns else "N/A"
                probabilistic_results["built_surface_m2"] = df['E_built_surface_m2'].sum() if 'E_built_surface_m2' in df.columns else "N/A"
                
                # Calculate LOW and HIGH scenarios (from track data)
                tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
                
                if giga_store.file_exists(tracks_filepath):
                    gdf_tracks = read_dataset(giga_store, tracks_filepath)
                    
                    if 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
                        # Find ensemble members with lowest and highest impact
                        member_totals = gdf_tracks.groupby('zone_id')['severity_population'].sum()
                        low_impact_member = member_totals.idxmin()
                        high_impact_member = member_totals.idxmax()
                        
                        # Set member badge text
                        low_member_badge = f"#{low_impact_member}"
                        high_member_badge = f"#{high_impact_member}"
                        
                        low_scenario_data = gdf_tracks[gdf_tracks['zone_id'] == low_impact_member]
                        high_scenario_data = gdf_tracks[gdf_tracks['zone_id'] == high_impact_member]
                        
                        # Check if health center data is available for this time slot
                        hc_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                        hc_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', hc_filename)
                        hc_data_available = giga_store.file_exists(hc_filepath)
                        
                        # LOW scenario
                        low_results["schools"] = low_scenario_data['severity_schools'].sum() if 'severity_schools' in low_scenario_data.columns else "N/A"
                        low_results["population"] = low_scenario_data['severity_population'].sum() if 'severity_population' in low_scenario_data.columns else "N/A"
                        low_results["health"] = low_scenario_data['severity_hcs'].sum() if ('severity_hcs' in low_scenario_data.columns and hc_data_available) else "N/A"
                        low_results["built_surface_m2"] = low_scenario_data['severity_built_surface_m2'].sum() if ('severity_built_surface_m2' in low_scenario_data.columns and hc_data_available) else "N/A"

                        # HIGH scenario
                        high_results["schools"] = high_scenario_data['severity_schools'].sum() if 'severity_schools' in high_scenario_data.columns else "N/A"
                        high_results["population"] = high_scenario_data['severity_population'].sum() if 'severity_population' in high_scenario_data.columns else "N/A"
                        high_results["health"] = high_scenario_data['severity_hcs'].sum() if ('severity_hcs' in high_scenario_data.columns and hc_data_available) else "N/A"
                        high_results["built_surface_m2"] = high_scenario_data['severity_built_surface_m2'].sum() if ('severity_built_surface_m2' in high_scenario_data.columns and hc_data_available) else "N/A"
                
                print(f"Impact metrics: Successfully loaded {len(df)} features")
                
            except Exception as e:
                print(f"Impact metrics: Error reading file {filename}: {e}")
        else:
            print(f"Impact metrics: File not found {filename}")
        
        # Format results
        def format_value(value):
            return str(value) if isinstance(value, str) else f"{value:,.0f}"
        
        return (
            # Children affected
            format_value(low_results["children"]),
            format_value(probabilistic_results["children"]),
            format_value(high_results["children"]),
            # Schools count
            format_value(low_results["schools"]),
            format_value(probabilistic_results["schools"]),
            format_value(high_results["schools"]),
            # Health count
            format_value(low_results["health"]),
            format_value(probabilistic_results["health"]),
            format_value(high_results["health"]),
            # Population count
            format_value(low_results["population"]),
            format_value(probabilistic_results["population"]),
            format_value(high_results["population"]),
            # Built Surface m2
            format_value(low_results["built_surface_m2"]),
            format_value(probabilistic_results["built_surface_m2"]),
            format_value(high_results["built_surface_m2"]),
            # Member badges
            low_member_badge,
            high_member_badge
        )
            
    except Exception as e:
        print(f"Impact metrics: Error updating metrics: {e}")
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")

# Callback to enable/disable specific track button when layers are loaded
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
     Output("specific-track-select", "style")],
    [Input("layers-loaded-store", "data")],
    [State("country-select", "value"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value"),
     State("wind-threshold-select", "value")],
    prevent_initial_call=True
)
def populate_specific_track_options(layers_loaded, country, storm, forecast_date, forecast_time, wind_threshold):
    """Populate specific track selector with available ensemble members"""
    
    if not layers_loaded or not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        return [], {"display": "none"}
    
    try:
        # Convert date and time to YYYYMMDDHHMMSS format
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"
        tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
        
        if giga_store.file_exists(tracks_filepath):
            gdf_tracks = read_dataset(giga_store, tracks_filepath)
            
            if 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
                # Get unique ensemble members
                ensemble_members = sorted(gdf_tracks['zone_id'].unique())
                
                # Find low and high impact members
                member_totals = gdf_tracks.groupby('zone_id')['severity_population'].sum()
                low_impact_member = member_totals.idxmin()
                high_impact_member = member_totals.idxmax()
                
                # Create options with member type labels and impact indicators
                options = []
                for member in ensemble_members:
                    member_type = "Control" if member in [51, 52] else f"Ensemble {member}"
                    
                    # Add impact scenario indicators
                    impact_indicator = ""
                    if member == low_impact_member:
                        impact_indicator = " (LOW IMPACT)"
                    elif member == high_impact_member:
                        impact_indicator = " (HIGH IMPACT)"
                    
                    options.append({
                        "value": str(member),
                        "label": f"{member_type} (ID: {member}){impact_indicator}"
                    })
                
                return options, {"display": "block"}
        
        return [], {"display": "none"}
        
    except Exception as e:
        print(f"Error loading specific track options: {e}")
        return [], {"display": "none"}

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

# Load all layers callback with integrated loading indicator
@callback(
    [Output('tracks-data-store', 'data'),
     Output('envelope-data-store', 'data'),
     Output('schools-data-store', 'data'),
     Output('health-data-store', 'data'),
     Output('population-tiles-data-store', 'data'),
     Output('layers-loaded-store', 'data'),
     Output('load-status', 'children'),
     Output('hurricane-tracks-toggle', 'disabled'),
     Output('hurricane-envelopes-toggle', 'disabled'),
     Output('schools-layer', 'disabled'),
     Output('health-layer', 'disabled'),
     Output('probability-tiles-layer', 'disabled'),
     Output('population-tiles-layer', 'disabled', allow_duplicate=True),
     Output('school-age-tiles-layer', 'disabled', allow_duplicate=True),
     Output('built-surface-tiles-layer', 'disabled', allow_duplicate=True),
     Output('settlement-tiles-layer', 'disabled', allow_duplicate=True),
     Output('rwi-tiles-layer', 'disabled', allow_duplicate=True)],
    [Input('load-layers-btn', 'n_clicks')],
    State('country-select', 'value'),
    State('storm-select', 'value'),
    State('forecast-date', 'value'),
    State('forecast-time', 'value'),
    State('wind-threshold-select', 'value'),
    prevent_initial_call=True,
    running=[(Output("load-layers-btn", "loading"), True, False)]
)
def load_all_layers(n_clicks, country, storm, forecast_date, forecast_time, wind_threshold):
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
    
    if not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        print("=== MISSING SELECTIONS - RETURNING EARLY ===")
        return {}, {}, {}, {}, {}, False, dmc.Alert("Missing selections", title="Warning", color="orange", variant="light"), True, True, True, True, True, True, True, True, True, True
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
        # Check if data files exist for the selected time
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"
        
        print(f"Looking for impact data files with pattern: {country}_{storm}_{forecast_datetime_str}_{wind_threshold}")
        
        # Check for data file availability
        data_files_found = []
        missing_files = []
        
        # Check schools file
        schools_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        schools_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', schools_file)
        if giga_store.file_exists(schools_path):
            data_files_found.append("schools")
        else:
            missing_files.append("schools")
        
        # Check health centers file
        health_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        health_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', health_file)
        if giga_store.file_exists(health_path):
            data_files_found.append("health centers")
        else:
            missing_files.append("health centers")
        
        # Check tiles file
        tiles_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_{ZOOM_LEVEL}.csv"
        tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', tiles_file)
        if giga_store.file_exists(tiles_path):
            data_files_found.append("infrastructure tiles")
        else:
            missing_files.append("infrastructure tiles")
        
        # Generate status alert based on data availability
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
        
        try:
            # Load available data files
            # Schools
            schools_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            schools_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', schools_file)
            if giga_store.file_exists(schools_path):
                try:
                    gdf_schools = read_dataset(giga_store, schools_path)
                    schools_data = gdf_schools.__geo_interface__
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
            tiles_file = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}_{ZOOM_LEVEL}.csv"
            tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', tiles_file)
            if giga_store.file_exists(tiles_path):
                base_tiles_file = f"{country}_{ZOOM_LEVEL}.parquet"
                base_tiles_path = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', base_tiles_file)
                if giga_store.file_exists(base_tiles_path):
                    try:
                        df_tiles = read_dataset(giga_store, tiles_path)
                        df_tiles = df_tiles.rename(columns={'zone_id':'tile_id'})
                        gdf_base_tiles = read_dataset(giga_store, base_tiles_path)
                        gdf_base_tiles['tile_id'] = df_tiles['tile_id'].astype(int)
                        tmp = pd.merge(gdf_base_tiles, df_tiles, on="tile_id", how="left")
                        gdf_tiles = gpd.GeoDataFrame(tmp, geometry="geometry", crs=gdf_base_tiles.crs)
                        tiles_data = gdf_tiles.__geo_interface__
                    except Exception as e:
                        print(f"Error reading tiles file: {e}")
                        tiles_data = {}
                
        except Exception as e:
            print(f"Error loading impact data: {e}")
            status_alert = dmc.Alert(
                f"Error loading layers: {str(e)}",
                title="Error",
                color="red",
                variant="light"
            )
        
        # Create independent copies for each tile layer
        if not tiles_data or not isinstance(tiles_data, dict) or not 'features' in tiles_data:
            tiles_data = {"type": "FeatureCollection", "features": []}
        
        print(f"=== LOAD ALL LAYERS CALLBACK COMPLETED SUCCESSFULLY ===")
        return (tracks_data, envelope_data, schools_data, health_data, 
                tiles_data,
                True, status_alert, 
                False, False, False, False, False, False, False, False, False, False)
        
    except Exception as e:
        print(f"Error in load_all_layers: {e}")
        return {}, {}, {}, {}, {}, False, dmc.Alert(f"Error loading layers: {str(e)}", title="Error", color="red", variant="light"), True, True, True, True, True, True, True, True, True, True

# Simple toggle callbacks - just show/hide pre-loaded data
@callback(
    Output("hurricane-tracks-json", "data"),
    Output("hurricane-tracks-json", "zoomToBounds"),
    Output("hurricane-tracks-json","key"),
    [Input("hurricane-tracks-toggle", "checked"),
     Input("specific-track-select", "value")],
    State("tracks-data-store", "data"),
    prevent_initial_call=False
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
        return filtered_tracks, True, key
    
    # Otherwise show all tracks
    return tracks_data, True, key

@callback(
    Output("envelopes-json-test", "data"),
    Output("envelopes-json-test", "zoomToBounds"),
    Output("envelopes-json-test","key"),
    [Input("hurricane-envelopes-toggle", "checked"),
     Input("specific-track-select", "value")],
    [State("envelope-data-store", "data"),
     State("wind-threshold-select", "value"),
     State("country-select", "value"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value")],
    prevent_initial_call=False
)
def toggle_envelopes_layer(checked, selected_track, envelope_data_in, wind_threshold, country, storm, forecast_date, forecast_time):
    """Toggle hurricane envelopes layer visibility with optional specific track filtering"""
    
    if not checked:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    envelope_data = copy.deepcopy(envelope_data_in)
    key = hashlib.md5(json.dumps(envelope_data, sort_keys=True).encode()).hexdigest()
    
    # Construct datetime string for file paths
    date_str = forecast_date.replace('-', '') if forecast_date else ''
    time_str = forecast_time.replace(':', '') if forecast_time else ''
    forecast_datetime_str = f"{date_str}{time_str}00"
    
    # If specific track is selected, create specific track envelope
    if selected_track:
        try:
            # Load specific track data
            tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
            tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
            
            if giga_store.file_exists(tracks_filepath):
                gdf_tracks = read_dataset(giga_store, tracks_filepath)
                specific_track_data = gdf_tracks[gdf_tracks['zone_id'] == int(selected_track)]
                
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
                        
                        feature = {
                            "type": "Feature",
                            "geometry": geometry,
                            "properties": {
                                "zone_id": int(row['zone_id']),
                                "ensemble_member": int(row['zone_id']),  # Use zone_id as ensemble_member for specific tracks
                                "wind_threshold": int(row['wind_threshold']),
                                "severity_population": float(row['severity_population']),
                                "severity_schools": int(row['severity_schools']),
                                "severity_hcs": int(row['severity_hcs']),
                                "severity_built_surface_m2": float(row['severity_built_surface_m2']),
                                "severity_children": float(row['severity_children']) if 'severity_children' in row else 0
                            }
                        }
                        specific_envelope['features'].append(feature)
                    return specific_envelope, True, key
        except Exception as e:
            print(f"Error creating specific track envelope: {e}")
    
    # Default probabilistic envelope behavior - now with impact data!
    if not envelope_data or not envelope_data.get('data'):
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
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
        
        # Convert to GeoDataFrame
        if df['geometry'].iloc[0].startswith('{'):
            from shapely.geometry import shape
            geometries = []
            for geom_str in df['geometry']:
                geom_dict = json.loads(geom_str)
                geometries.append(shape(geom_dict))
            gdf = gpd.GeoDataFrame(df.drop('geometry', axis=1), geometry=geometries)
        else:
            gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))
        
        # Try to add impact data from track_views if available
        try:
            # Only try to load impact data if we have all required parameters
            if country and storm and forecast_datetime_str and wind_threshold:
                tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
                tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)
                
                if giga_store.file_exists(tracks_filepath):
                    gdf_tracks = read_dataset(giga_store, tracks_filepath)
                    
                    # Sum impact metrics per ensemble member (zone_id is ensemble_member in track data)
                    if 'zone_id' in gdf_tracks.columns and 'wind_threshold' in gdf_tracks.columns:
                        # Filter by wind threshold
                        tracks_thresh = gdf_tracks[gdf_tracks['wind_threshold'] == wth_int]
                        
                        if not tracks_thresh.empty:
                            # Aggregate impact data by ensemble member
                            agg_dict = {
                                'severity_population': 'sum',
                                'severity_schools': 'sum',
                                'severity_hcs': 'sum',
                                'severity_built_surface_m2': 'sum'
                            }
                            
                            # Add children column if it exists
                            if 'severity_children' in tracks_thresh.columns:
                                agg_dict['severity_children'] = 'sum'
                            
                            impact_summary = tracks_thresh.groupby('zone_id').agg(agg_dict).reset_index()
                            
                            # Build column names list dynamically
                            col_names = ['ensemble_member', 'severity_population', 'severity_schools', 'severity_hcs', 'severity_built_surface_m2']
                            if 'severity_children' in agg_dict:
                                col_names.insert(1, 'severity_children')
                            impact_summary.columns = col_names
                            
                            # Merge with envelope data
                            # Get ensemble_member from envelope data - could be in ENSEMBLE_MEMBER column
                            if 'ENSEMBLE_MEMBER' in gdf.columns:
                                gdf['ensemble_member'] = gdf['ENSEMBLE_MEMBER']
                            
                            # Merge impact data
                            gdf = gdf.merge(impact_summary, on='ensemble_member', how='left')
                            
                            # Fill NaN values with 0
                            impact_cols = ['severity_population', 'severity_schools', 'severity_hcs', 'severity_built_surface_m2']
                            if 'severity_children' in impact_summary.columns:
                                impact_cols.insert(0, 'severity_children')
                            for col in impact_cols:
                                if col in gdf.columns:
                                    gdf[col] = gdf[col].fillna(0)
                            
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
            return geo_dict, True, key
        
        return gdf.__geo_interface__, True, key
        
    except Exception as e:
        print(f"Error toggling envelopes: {e}")
        return {"type": "FeatureCollection", "features": []}, False, key

@callback(
    Output("schools-json-test", "data"),
    Output("schools-json-test", "zoomToBounds"),
    Output("schools-json-test","key"),
    [Input("schools-layer", "checked")],
    State("schools-data-store", "data"),
    prevent_initial_call=False
)
def toggle_schools_layer(checked, schools_data_in):
    """Toggle schools layer visibility with probability-based coloring and variable radius"""
    if not checked or not schools_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    schools_data = copy.deepcopy(schools_data_in)
    key = hashlib.md5(json.dumps(schools_data, sort_keys=True).encode()).hexdigest()
    try:
        # Convert polygons to point markers
        if 'features' in schools_data:
            from shapely.geometry import shape
            point_features = []
            
            for feature in schools_data['features']:
                if 'properties' in feature and 'geometry' in feature:
                    prob = feature['properties'].get('probability', 0)
                    
                    # Calculate color and radius based on probability
                    if prob == 0 or prob is None:
                        color = '#ADD8E6'  # Light blue for schools when not impacted
                        radius = 4  # Smaller for no impact
                    elif prob <= 0.15:
                        color = '#FFFF00'  # Yellow
                        radius = 10
                    elif prob <= 0.30:
                        color = '#FFD700'  # Gold
                        radius = 12
                    elif prob <= 0.45:
                        color = '#FFA500'  # Orange
                        radius = 15
                    elif prob <= 0.60:
                        color = '#FF8C00'  # Dark orange
                        radius = 18
                    elif prob <= 0.75:
                        color = '#FF4500'  # Orange-red
                        radius = 20
                    elif prob <= 0.90:
                        color = '#DC143C'  # Crimson
                        radius = 22
                    else:
                        color = '#8B0000'  # Dark red
                        radius = 25
                    
                    # Convert polygon to point (centroid)
                    try:
                        geom_shape = shape(feature['geometry'])
                        centroid = geom_shape.centroid
                        
                        # Create point feature with probability-based styling
                        point_feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [centroid.x, centroid.y]
                            },
                            "properties": {
                                **feature['properties'],
                                "_color": color,
                                "_radius": radius,
                                "_opacity": 0.8,
                                "_weight": 2,
                                "_fillOpacity": 0.7
                            }
                        }
                        point_features.append(point_feature)
                    except Exception as e:
                        print(f"Error converting to point: {e}")
                        continue
            
            schools_data['features'] = point_features
        
        return schools_data, True, key
    except Exception as e:
        print(f"Error styling schools layer: {e}")
        return schools_data, True, key

@callback(
    Output("health-json-test", "data"),
    Output("health-json-test", "zoomToBounds"),
    Output("health-json-test","key"),
    [Input("health-layer", "checked")],
    State("health-data-store", "data"),
    prevent_initial_call=False
)
def toggle_health_layer(checked, health_data_in):
    """Toggle health centers layer visibility with probability-based coloring and variable radius"""
    if not checked or not health_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    health_data = copy.deepcopy(health_data_in)
    key = hashlib.md5(json.dumps(health_data, sort_keys=True).encode()).hexdigest()
    
    try:
        # Convert polygons to point markers
        if 'features' in health_data:
            from shapely.geometry import shape
            point_features = []
            
            for feature in health_data['features']:
                if 'properties' in feature and 'geometry' in feature:
                    prob = feature['properties'].get('probability', 0)
                    
                    # Calculate color and radius based on probability
                    if prob == 0 or prob is None:
                        color = '#90EE90'  # Light green for health centers when not impacted
                        radius = 4  # Smaller for no impact
                    elif prob <= 0.15:
                        color = '#FFFF00'  # Yellow
                        radius = 10
                    elif prob <= 0.30:
                        color = '#FFD700'  # Gold
                        radius = 12
                    elif prob <= 0.45:
                        color = '#FFA500'  # Orange
                        radius = 15
                    elif prob <= 0.60:
                        color = '#FF8C00'  # Dark orange
                        radius = 18
                    elif prob <= 0.75:
                        color = '#FF4500'  # Orange-red
                        radius = 20
                    elif prob <= 0.90:
                        color = '#DC143C'  # Crimson
                        radius = 22
                    else:
                        color = '#8B0000'  # Dark red
                        radius = 25
                    
                    # Convert polygon to point (centroid)
                    try:
                        geom_shape = shape(feature['geometry'])
                        centroid = geom_shape.centroid
                        
                        # Create point feature with probability-based styling
                        point_feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [centroid.x, centroid.y]
                            },
                            "properties": {
                                **feature['properties'],
                                "_color": color,
                                "_radius": radius,
                                "_opacity": 0.8,
                                "_weight": 2,
                                "_fillOpacity": 0.7
                            }
                        }
                        point_features.append(point_feature)
                    except Exception as e:
                        print(f"Error converting to point: {e}")
                        continue
            
            health_data['features'] = point_features
        
        return health_data, True, key
        
    except Exception as e:
        print(f"Error styling health layer: {e}")
        return health_data, True, key


# Callbacks for toggle tiles - now using radio group
@callback(
    Output("population-tiles-json", "data", allow_duplicate=True),
    Output("population-tiles-json", "zoomToBounds", allow_duplicate=True),
    Output("population-tiles-json", "key", allow_duplicate=True),
    Output('population-tiles-layer', 'disabled', allow_duplicate=True),
    Output('school-age-tiles-layer', 'disabled', allow_duplicate=True),
    Output('built-surface-tiles-layer', 'disabled', allow_duplicate=True),
    Output('settlement-tiles-layer', 'disabled', allow_duplicate=True),
    Output('rwi-tiles-layer', 'disabled', allow_duplicate=True),
    Input('tiles-layer-group','value'),
    Input('probability-tiles-layer','checked'),
    State('probability-tiles-layer','checked'),
    State('population-tiles-data-store','data'),
    prevent_initial_call = True,
)
def juggle_toggles_tiles_layer(selected_layer, prob_checked_trigger, prob_checked_val, tiles_data_in):
    """Handle tile layer display based on radio selection"""
    # Determine which layer is selected
    active_layer = selected_layer
    
    # Context data layers should be disabled when Impact Probability is on
    # and regular layers should be enabled at all times
    if prob_checked_val:
        # When Impact Probability is on, disable context data radios
        population_enabled, school_age_enabled, built_enabled, settlement_enabled, rwi_enabled = False, False, False, True, True
    else:
        # When Impact Probability is off, all radios are enabled
        population_enabled, school_age_enabled, built_enabled, settlement_enabled, rwi_enabled = False, False, False, False, False
    
    radios_enabled = (population_enabled, school_age_enabled, built_enabled, settlement_enabled, rwi_enabled)
    
    # If no layer is selected or "none" is selected, return empty data (to show only Impact Probability)
    if not active_layer or active_layer == "none":
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update, *radios_enabled
    
    # Determine what data to show based on active layer
    if active_layer == "population":
        if prob_checked_val:
            tiles, zoom, key = update_tile_features(tiles_data_in, 'E_population')
        else:
            tiles, zoom, key = update_tile_features(tiles_data_in, 'population')
        return tiles, zoom, key, *radios_enabled
    
    elif active_layer == "school-age":
        if prob_checked_val:
            tiles, zoom, key = update_tile_features(tiles_data_in, 'E_school_age_population')
        else:
            tiles, zoom, key = update_tile_features(tiles_data_in, 'school_age_population')
        return tiles, zoom, key, *radios_enabled
    
    elif active_layer == "built-surface":
        if prob_checked_val:
            tiles, zoom, key = update_tile_features(tiles_data_in, 'E_built_surface_m2')
        else:
            tiles, zoom, key = update_tile_features(tiles_data_in, 'built_surface_m2')
        return tiles, zoom, key, *radios_enabled
    
    elif active_layer == "settlement":
        tiles, zoom, key = update_tile_features(tiles_data_in, 'smod_class')
        return tiles, zoom, key, *radios_enabled
    
    elif active_layer == "rwi":
        tiles, zoom, key = update_tile_features(tiles_data_in, 'rwi')
        return tiles, zoom, key, *radios_enabled
    
    # Default: return empty data
    return {"type": "FeatureCollection", "features": []}, False, dash.no_update, *radios_enabled

# Callback for Impact Probability layer
@callback(
    Output("probability-tiles-json", "data", allow_duplicate=True),
    Output("probability-tiles-json", "zoomToBounds", allow_duplicate=True),
    Output("probability-tiles-json", "key", allow_duplicate=True),
    Output("probability-legend", "style", allow_duplicate=True),
    Input('probability-tiles-layer','checked'),
    Input('tiles-layer-group','value'),
    State('population-tiles-data-store','data'),
    prevent_initial_call = True,
)
def toggle_probability_tiles_layer(prob_checked, selected_layer, tiles_data_in):
    """Handle Impact Probability layer display - only show when checkbox is on and radio is 'none'"""
    # Show probability legend whenever checkbox is on; show probability layer only when radio is 'none'
    legend_style = {"display": "block"} if prob_checked else {"display": "none"}
    should_show = prob_checked and (selected_layer is None or selected_layer == "none") and tiles_data_in
    
    if not should_show or not tiles_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update, legend_style
    
    # Show probability data
    tiles, zoom, key = update_tile_features(tiles_data_in, 'probability')
    return tiles, zoom, key, legend_style

# Callback to update info text when specific track is selected
@callback(
    Output("specific-track-info", "children"),
    [Input("specific-track-select", "value")],
    [State("country-select", "value"),
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
        
        if not giga_store.file_exists(tracks_filepath):
            return "Track data not found"
        
        # Load track data
        gdf_tracks = read_dataset(giga_store, tracks_filepath)
        
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

# Callback to update forecast dates based on country selection



# Legend visibility callbacks
@callback(
    Output("schools-legend", "style"),
    [Input("schools-layer", "checked")],
    prevent_initial_call=False
)
def toggle_schools_legend(checked):
    """Show/hide schools legend based on checkbox state"""
    return {"display": "block" if checked else "none"}

@callback(
    Output("health-legend", "style"),
    [Input("health-layer", "checked")],
    prevent_initial_call=False
)
def toggle_health_legend(checked):
    """Show/hide health centers legend based on checkbox state"""
    return {"display": "block" if checked else "none"}

@callback(
    [Output("population-legend", "style"),
     Output("school-age-legend", "style"),
     Output("built-surface-legend", "style"),
     Output("settlement-legend", "style"),
     Output("rwi-legend", "style")],
    [Input("tiles-layer-group", "value")],
    prevent_initial_call=False
)
def toggle_tiles_legend(selected_value):
    """Show/hide tile legends based on radio button selection"""
    if selected_value == "population":
        return {"display": "block"}, {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}
    elif selected_value == "school-age":
        return {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}, {"display": "none"}
    elif selected_value == "built-surface":
        return {"display": "none"}, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}
    elif selected_value == "settlement":
        return {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "block"}, {"display": "none"}
    elif selected_value == "rwi":
        return {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "block"}
    else:
        return {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}

# Register the page as the home page
dash.register_page(__name__, path="/", name="Ahead of the Storm")

