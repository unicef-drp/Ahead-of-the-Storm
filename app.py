"""
Dash application entry point for Ahead of the Storm.

Initialises the Dash app, wires external stylesheets/scripts, defines global
dcc.Store components, and serves the JS map-static assets. Page layouts and
page-specific callbacks are registered lazily by the pages/ modules via
dash.page_container / use_pages=True.
"""
import os
import logging
import dash
import dash_mantine_components as dmc
from dash import Dash, _dash_renderer, dcc, callback, Input, Output, State
from dotenv import load_dotenv
import json as _json
from flask import send_from_directory, Response
from flask_compress import Compress

load_dotenv()

_dash_renderer._set_react_version("18.2.0")

app = Dash(
    __name__,
    meta_tags=[
        {"name": "AoS Hurricane Impact", "content": "width=device-width, initial-scale=1"}
    ],
    external_stylesheets=[
        dmc.styles.ALL,
        "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css",
    ],
    external_scripts=[
        "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js",
        "/map-static/tile_palettes.js",
        "/map-static/maplibre_tiles.js",
    ],
    use_pages=True,
)

app.config["suppress_callback_exceptions"] = True
app.title = "AoS Hurricane Impact"
app._favicon = "img/aots_icon.png"
server = app.server
Compress(server)

_MAP_COMPONENTS_DIR = os.path.join(os.path.dirname(__file__), "components", "map")
_PALETTES_JSON = os.path.join(_MAP_COMPONENTS_DIR, "tile_palettes.json")

@server.route("/map-static/tile_palettes.js")
def serve_tile_palettes_js():
    try:
        with open(_PALETTES_JSON) as f:
            data = _json.load(f)
        js = (
            "// Auto-generated from tile_palettes.json — edit that file, not this endpoint.\n"
            "window._AOTS_PALETTES = "   + _json.dumps(data["palettes"])   + ";\n"
            "window._AOTS_PROP_MAP = "   + _json.dumps(data["prop_map"])   + ";\n"
            "window._AOTS_E_PROP_MAP = " + _json.dumps(data["e_prop_map"]) + ";\n"
            "window._AOTS_IN_NEED_MAP = "+ _json.dumps(data["in_need_map"])+ ";\n"
        )
        return Response(js, mimetype="application/javascript")
    except (FileNotFoundError, _json.JSONDecodeError, KeyError) as e:
        logging.getLogger(__name__).error("Failed to serve tile_palettes.js: %s", e)
        return Response("// tile_palettes.json unavailable\n", mimetype="application/javascript", status=500)

@server.route("/map-static/<path:filename>")
def serve_map_static(filename):
    from flask import make_response
    resp = make_response(send_from_directory(_MAP_COMPONENTS_DIR, filename))
    resp.headers["Cache-Control"] = "no-store"
    return resp


app.layout = dmc.MantineProvider(
    [
        dash.page_container,
        dcc.Store("country-store", data=""),
        dcc.Store("country-is-region-store", data=False),
        dcc.Store("storm-store", data=""),
        dcc.Store("date-store", data=""),
        dcc.Store("using-base-layers-store", data=False),
    ],
    id="mantine-provider",
    forceColorScheme="light",
    theme={"fontFamily": "'Open Sans', sans-serif"},
)


@callback(
    Output("app-shell", "navbar"),
    Input("burger-button", "opened"),
    State("app-shell", "navbar"),
    prevent_initial_call=True,
)
def navbar_is_open(opened, navbar):
    navbar["collapsed"] = {"mobile": not opened}
    return navbar


if __name__ == "__main__":
    app.run(debug=True)
