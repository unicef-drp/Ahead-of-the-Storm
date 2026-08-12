"""
Infrastructure overlay callbacks.

Handles the four point-layer overlays (schools, health centers, shelters, WASH)
and the cross-layer "no data" warning banner.

Each overlay uses an async clientside callback that fetches styled GeoJSON directly
from the tile server, bypassing Dash's callback POST channel (which has a ~10 MB
size limit that large countries exceed). Data flows: tile server → browser → Leaflet
component, with no server round-trip through Dash.
"""
from dash import Output, Input, State, callback, clientside_callback
import dash_mantine_components as dmc


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
# INFRASTRUCTURE OVERLAY TOGGLE CALLBACKS
# One async clientside callback per layer. When the checkbox is toggled OR a new
# storm is loaded (maplibre-tile-config-store changes), the callback fetches styled
# GeoJSON directly from the tile server and updates the Leaflet GeoJSON component.
#
# Why clientside + async fetch instead of a server callback:
#   A server callback sends GeoJSON through Dash's _dash-update-component POST,
#   which nginx caps at 50 MB and Dash serialises synchronously. Large countries
#   (e.g. Mexico with ~30 k schools) exceed the limit and return HTTP 413.
#   An async fetch runs entirely in the browser; Dash never touches the payload.
# =============================================================================

_EMPTY_FC = '{"type":"FeatureCollection","features":[]}'

def _register_overlay_toggle(layer_id: str) -> None:
    clientside_callback(
        f"""
        async function(checked, config) {{
            if (!checked || !config || !config.country || !config.storm) {{
                return [{_EMPTY_FC}, window.dash_clientside.no_update];
            }}
            var base = (config.tile_server_url != null && config.tile_server_url !== '')
                ? config.tile_server_url
                : window.location.origin;
            var url = base + '/geojson/facilities/{layer_id}/'
                + encodeURIComponent(config.country) + '/'
                + encodeURIComponent(config.storm) + '/'
                + encodeURIComponent(config.forecast_date)
                + '?wind_threshold=' + config.wind_threshold;
            try {{
                var resp = await fetch(url);
                if (!resp.ok) return [{_EMPTY_FC}, window.dash_clientside.no_update];
                var geojson = await resp.json();
                return [geojson, Date.now().toString()];
            }} catch(e) {{
                console.error('[AoTS] Failed to fetch {layer_id}:', e);
                return [{_EMPTY_FC}, window.dash_clientside.no_update];
            }}
        }}
        """,
        [Output(f"{layer_id}-overlay-json", "data"),
         Output(f"{layer_id}-overlay-json", "key")],
        Input(f"{layer_id}-layer", "checked"),
        Input("maplibre-tile-config-store", "data"),
        prevent_initial_call=True,
    )

for _lid in ("schools", "health", "shelters", "wash"):
    _register_overlay_toggle(_lid)


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
