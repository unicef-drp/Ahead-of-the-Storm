import sys
import json
import dash_leaflet as dl
import dash_leaflet.express as dlx
from dash_extensions.javascript import arrow_function, assign
from dash import html

from .map_config import map_config, mapbox_token, get_tile_layer_url



hover_style = assign("function(){ return {weight:3, color:'#e53935'}; }")

on_each_feature = assign("""
        function(feature, layer){
            const props = feature.properties || {};
            const rows = Object.keys(props).map(k => 
                `<tr><th style="text-align:left;padding-right:6px;">${k}</th><td>${props[k]}</td></tr>`
            ).join('');
            const html = `<div style="font-size:12px;"><table>${rows}</table></div>`;
            if (layer && layer.bindTooltip) {
                layer.bindTooltip(html, {sticky: true});
            }
        }
        """
    )


def make_empty_map():
    return dl.Map(
        [
            dl.TileLayer(
                url=get_tile_layer_url(),
                attribution="© OpenStreetMap contributors" if not mapbox_token else "mapbox",
            ),
            #admin1_layer,
            #admin2_layer,
            #school_layer,
            dl.FullScreenControl(),
            dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
            #make_floating_dropdowns(admin1_options_dict=admin1_options_dict),
        ],
        center=[map_config.center["lat"], map_config.center["lon"]],
        zoom=map_config.zoom,
        style={
            "height": "calc(100vh - 235px)",
            "width": "100%",
            "position": "relative",
            "paddingRight": "15px",
            "zIndex": 0,  # Ensure the map is behind the control panel
        },
    )

def make_map_layers(gdf):

    cols = ["TRACK_ID","FORECAST_TIME"]  # all non-geometry columns
    # Convert Timestamp objects to strings for JSON serialization
    gdf_copy = gdf.copy()
    gdf_copy['FORECAST_TIME'] = gdf_copy['FORECAST_TIME'].astype(str)
    layer = json.loads(gdf_copy[cols + ["geometry"]].to_json())

    

    layer_h = dl.GeoJSON(
        data=layer,#.__geo_interface__,
        id="hurricanes-json",
        onEachFeature=on_each_feature,
        #zoomToBounds=True,
        #options=dict(onEachFeature=on_each_feature),   # <- bind tooltip per feature
        hoverStyle=hover_style                 # optional hover highlight
        # hideout=dict(admin1_filter=[], admin2_filter=[])
    )

    return dl.Map(
        [
            dl.TileLayer(
                url=get_tile_layer_url(),
                attribution="© OpenStreetMap contributors" if not mapbox_token else "mapbox",
            ),
            layer_h,
            dl.FullScreenControl(),
            dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
            #make_floating_dropdowns(admin1_options_dict=admin1_options_dict),
        ],
        center=[map_config.center["lat"], map_config.center["lon"]],
        zoom=map_config.zoom,
        style={
            "height": "calc(100vh - 235px)",
            "width": "100%",
            "position": "relative",
            "paddingRight": "15px",
            "zIndex": 0,  # Ensure the map is behind the control panel
        },
    )