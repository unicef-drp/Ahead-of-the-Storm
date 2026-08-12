"""
Dashboard Styling Configuration
Contains the color palette registry and legend helper for the map UI.
all_colors is loaded from tile_palettes.json, the single source of truth for
all palette data shared between Python (legends) and JavaScript (MapLibre).
"""
import json
import os
from dash import html

_PALETTES_PATH = os.path.join(os.path.dirname(__file__), '..', 'map', 'tile_palettes.json')

def _load_all_colors():
    with open(_PALETTES_PATH) as f:
        data = json.load(f)
    # Prepend 'transparent' to each palette's color list so create_legend_divs
    # can skip it (transparent = no-data sentinel, not shown in legend).
    return {key: ['transparent'] + entry['colors'] for key, entry in data['palettes'].items()}

all_colors = _load_all_colors()


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



