import logging
from pydantic import BaseModel
import os

logger = logging.getLogger(__name__)

# Mapbox access token for map visualization
mapbox_token = os.environ.get("MAPBOX_ACCESS_TOKEN") or None

if mapbox_token:
    logger.info("Mapbox token found (length: %d characters)", len(mapbox_token))
else:
    logger.warning("Mapbox token not found, will use OpenStreetMap fallback")

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
    # lat nudged north from the true equatorial-ish midpoint (0-15 range) so
    # the default Global zoom-2 view sits ~60S behind the footer bar instead
    # of showing empty ocean/Antarctica edge there.
    center: dict = {"lon": 20.0, "lat": -6.0}
    zoom: float = 2


map_config = MapConfig()
