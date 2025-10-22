import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_leaflet as dl
from pydantic import BaseModel
import os

mapbox_token = os.environ.get("MAPBOX_ACCESS_TOKEN")

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
