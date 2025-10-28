"""
Dashboard Styling Configuration
Contains color schemes, legend helpers, and tile processing functions
"""
import pandas as pd
from dash import html
import dash


# =============================================================================
# COLOR SCHEMES
# =============================================================================
# Define color palettes for different data types and tile visualizations

all_colors = {
    # probability colors: starts at light yellow and increases to dark red
    'probability': ['transparent',
                    '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c',
                    '#fc4e2a', '#f03b20', '#e31a1c', '#bd0026', '#800026'],
    'population': ['transparent',
                    '#add8e6', '#8cc5d3', '#6bb2c0', '#4a9bad', '#33849a',
                    '#216d87', '#165674', '#0d3f51', '#06283d', '#011129'],
    'E_population': ['transparent',
                    '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c',
                    '#fc4e2a', '#f03b20', '#e31a1c', '#bd0026', '#800026'],
    'school_age_population': ['transparent',
                    '#a8e6cf', '#7ed3b8', '#5ec0a1', '#40ad8a', '#2d9a73',
                    '#228759', '#177440', '#0f5127', '#083310', '#001107'],
    'E_school_age_population':['transparent',
                    '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c',
                    '#fc4e2a', '#f03b20', '#e31a1c', '#bd0026', '#800026'],
    'infant_population': ['transparent',
                    '#d6e8ff', '#b3d9ff', '#8ac8ff', '#66b7ff', '#42a6ff',
                    '#1e95ff', '#1685e6', '#0f75cc', '#0765b3', '#005599'],
    'E_infant_population': ['transparent',
                    '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c',
                    '#fc4e2a', '#f03b20', '#e31a1c', '#bd0026', '#800026'],
    'built_surface_m2': ['transparent',
                    '#f6e6d1', '#e8d4b8', '#dac29f', '#ccb086', '#be9e6d',
                    '#b08854', '#a2723b', '#945c22', '#864609', '#783000'],
    'E_built_surface_m2':['transparent',
                    '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c',
                    '#fc4e2a', '#f03b20', '#e31a1c', '#bd0026', '#800026'],
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


def update_tile_features(tiles_data_in, property):
    """
    Update tile features with color styling based on property values
    
    Args:
        tiles_data_in: GeoJSON FeatureCollection with tile data
        property: Property name to use for coloring (e.g., 'population', 'probability')
    
    Returns:
        tuple: (styled_tiles_data, should_zoom, cache_key)
    """
    import copy
    import json
    import hashlib
    from dash_extensions.javascript import assign
    
    if not tiles_data_in:
        return {"type": "FeatureCollection", "features": []}, False, dash.no_update
    
    # Ensure tiles_data has proper GeoJSON structure
    if not isinstance(tiles_data_in, dict) or 'features' not in tiles_data_in:
        print(f"ERROR: Invalid tiles_data structure: {type(tiles_data_in)}")
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
                # Use log scale for population, school_age_population, infant_population, built_surface_m2, and their E_ equivalents; linear for others
                if property in ['population', 'school_age_population', 'infant_population', 'built_surface_m2',
                                'E_population', 'E_school_age_population', 'E_infant_population', 'E_built_surface_m2']:
                    # Log scale: transform values using log10
                    import math
                    clean_positive_values = [v for v in clean_values if v > 0]
                    if clean_positive_values:
                        log_min = math.log10(min(clean_positive_values))
                        log_max = math.log10(max_val) if max_val > 0 else log_min
                        log_step = (log_max - log_min) / actual_buckets if log_max != log_min else 1.0
                        
                        for val in values:
                            if pd.isna(val) or val == 0:
                                color_prop.append(colors[0])  # Use transparent for NaN or 0
                            else:
                                log_val = math.log10(val)
                                index = min(int((log_val - log_min) / log_step) if log_step > 0 else 0, actual_buckets - 1)
                                color_prop.append(actual_colors[index])
                    else:
                        color_prop = [colors[0]] * len(values)
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

