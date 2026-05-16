import logging
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_leaflet as dl
from pydantic import BaseModel
import os

logger = logging.getLogger(__name__)

# Mapbox access token for map visualization
mapbox_token = os.environ.get("MAPBOX_ACCESS_TOKEN") or None

if mapbox_token:
    logger.info("Mapbox token found (length: %d characters)", len(mapbox_token))
else:
    logger.warning("Mapbox token not found — will use OpenStreetMap fallback")

def get_tile_layer_url():
    """Get the appropriate tile layer URL based on whether Mapbox token is available."""
    if mapbox_token:
        return f"https://api.mapbox.com/styles/v1/mapbox/light-v11/tiles/{{z}}/{{x}}/{{y}}?access_token={mapbox_token}"
    logger.debug("Using OpenStreetMap tiles (Mapbox token not available)")
    return "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"

class MapConfig(BaseModel):

    marker_size: int = 5
    marker_opacity: float = 0.95
    title_x: float = 0.5
    title_y: float = 0.95
    legend_x: float = 0.1
    legend_y: float = 0.925
    legend_bgcolor: str = "#262624"
    legend_width: int = 75  # px
    legend_font_color: str = "white"
    colorscale_font_color: str = "white"
    legend_border_color: str = "#262624"
    legend_border_width: int = 1
    center: dict = {"lon": -73.967590, "lat": 40.749191}
    zoom: float = 2


map_config = MapConfig()
