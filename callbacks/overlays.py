"""
Infrastructure overlay callbacks.

Handles the four point-layer overlays (schools, health centers, shelters, WASH)
and the cross-layer "no data" warning banner. Each overlay callback reads pre-loaded
GeoJSON from a dcc.Store, converts polygon features to centroid points, applies a
shared yellow→red probability colour scale, and pushes the result to a
dash_leaflet GeoJSON component.
"""
import copy
import hashlib
import json
import logging

import dash
from dash import Output, Input, State, callback
import dash_mantine_components as dmc

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# Human-readable display names for all layer IDs. Used by
# show_layer_no_data_warning to build the alert message.
# =============================================================================

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


# =============================================================================
# HELPERS
# Internal utilities used by the overlay toggle callbacks.
# =============================================================================

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
        # 8-band yellow→red scale (0–15%, 15–30%, ..., 90–100%) matching the map tile colour ramp.
        # Radius grows with probability to give higher-impact features additional visual weight.
        if prob == 0:
            color, radius = base_color, 4
        elif prob <= 0.15:
            color, radius = '#FFFF00', 10
        elif prob <= 0.30:
            color, radius = '#FFD700', 12
        elif prob <= 0.45:
            color, radius = '#FFA500', 15
        elif prob <= 0.60:
            color, radius = '#FF8C00', 18
        elif prob <= 0.75:
            color, radius = '#FF4500', 20
        elif prob <= 0.90:
            color, radius = '#DC143C', 22
        else:
            color, radius = '#8B0000', 25
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
            logger.error(f"Error converting to point: {e}")
    return point_features


# =============================================================================
# INFRASTRUCTURE OVERLAY TOGGLE CALLBACKS
# One callback per layer — registered via factory to avoid code duplication.
# Each reads pre-loaded GeoJSON from a dcc.Store, converts polygon features
# to centroid points with probability-scaled colour/radius, and writes the
# result to the corresponding dash_leaflet GeoJSON component.
# =============================================================================

# ---------------------------------------------------------------------------
# Layer registry
# ---------------------------------------------------------------------------
_OVERLAY_LAYERS = [
    # (layer_id, base_color)   — layer_id matches both the checkbox and store IDs
    ("schools",  "#ADD8E6"),   # Light blue
    ("health",   "#90EE90"),   # Light green
    ("shelters", "#E91E8C"),   # Pink
    ("wash",     "#40E0D0"),   # Turquoise
]


def _register_overlay_toggle(layer_id, base_color):
    """Register the toggle callback for one infrastructure overlay layer.

    Uses a closure so the correct layer_id and base_color are captured at registration
    time. Called once per entry in _OVERLAY_LAYERS during module import.
    """
    @callback(
        Output(f"{layer_id}-overlay-json", "data"),
        Output(f"{layer_id}-overlay-json", "zoomToBounds"),
        Output(f"{layer_id}-overlay-json", "key"),
        Input(f"{layer_id}-layer", "checked"),
        State(f"{layer_id}-data-store", "data"),
        prevent_initial_call=True
    )
    def _toggle(checked, data_in):
        if not checked or not data_in:
            return {"type": "FeatureCollection", "features": []}, False, dash.no_update
        data = copy.deepcopy(data_in)
        key = hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
        try:
            data["features"] = _style_point_layer(data, base_color)
            return data, False, key
        except Exception as e:
            logger.error(f"Error styling {layer_id} layer: {e}")
            return data, False, key

for _lid, _col in _OVERLAY_LAYERS:
    _register_overlay_toggle(_lid, _col)


# =============================================================================
# NO-DATA WARNING CALLBACK
# Cross-layer warning banner. Fires whenever any layer checkbox or radio
# changes, checks layer-availability-store, and renders a yellow alert
# listing missing layers.
# =============================================================================

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
    """Show a yellow alert when the user enables a layer that has no data for the current selection.

    `availability` is a dict written by `load_all_layers` in dashboard.py; keys are layer IDs
    (e.g. "schools", "tile_population") and values are booleans. Base-layer-only mode suppresses
    warnings for impact-derived properties that are expected to be absent.
    """
    if not availability:
        return None

    missing = []

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

    using_base = availability.get("using_base_layers", False)
    for prefix, selected in [("tile", tiles_layer), ("admin", admin_layer)]:
        if not selected or selected == "none":
            continue
        avail_key = f"{prefix}_{selected}"
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
