"""
Placeholder map shown during initial page load before the full dashboard map renders.
Provides a minimal Dash Leaflet map (tile layer + fullscreen/locate controls) with the
same viewport dimensions as the main map so the layout does not shift on hydration.
Not used for data display — contains no GeoJSON layers or callbacks.
"""
import dash_leaflet as dl

from .map_config import map_config, mapbox_token, get_tile_layer_url


def make_empty_map():
    return dl.Map(
        [
            dl.TileLayer(
                url=get_tile_layer_url(),
                attribution="© OpenStreetMap contributors" if not mapbox_token else "mapbox",
            ),
            dl.FullScreenControl(),
            dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
        ],
        center=[map_config.center["lat"], map_config.center["lon"]],
        zoom=map_config.zoom,
        style={
            "height": "calc(100vh - 235px)",
            "width": "100%",
            "position": "relative",
            "paddingRight": "15px",
            "zIndex": 0,
        },
    )
