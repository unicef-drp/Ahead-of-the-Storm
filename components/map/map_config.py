import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_leaflet as dl
from pydantic import BaseModel
import os

# Support both MAPBOX_ACCESS_TOKEN and MAPBOX_TOKEN for compatibility
mapbox_token = os.environ.get("MAPBOX_ACCESS_TOKEN") or os.environ.get("MAPBOX_TOKEN") or None

# Debug: Log token status (without exposing the actual token)
if mapbox_token:
    print(f"✓ Mapbox token found (length: {len(mapbox_token)} characters)")
else:
    print("⚠ Mapbox token not found - will use OpenStreetMap fallback")

# Fallback tile layer URL if Mapbox token is not available
def get_tile_layer_url():
    """Get the appropriate tile layer URL based on whether Mapbox token is available"""
    if mapbox_token:
        return f"https://api.mapbox.com/styles/v1/mapbox/light-v11/tiles/{{z}}/{{x}}/{{y}}?access_token={mapbox_token}"
    else:
        # Fallback to OpenStreetMap if no Mapbox token
        print("Using OpenStreetMap tiles (Mapbox token not available)")
        return "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"

class MapConfig(BaseModel):

    marker_size: int = 5
    marker_opacity: float = 0.95
    title_x: float = 0.5
    title_y: float = 0.95
    legend_x: float = 0.1
    legend_y: float = 0.925
    legend_bgcolor: str = "#262624"
    legend_width: str = 75  # px
    legend_font_color: str = "white"
    colorscale_font_color: str = "white"
    legend_border_color: str = "#262624"
    legend_border_width: int = 1
    center: dict = {"lon": -73.967590, "lat": 40.749191}
    zoom: float = 2


map_config = MapConfig()
