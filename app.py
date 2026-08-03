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
    # "no-cache" (not "no-store") — still hits the server on every page load
    # to revalidate, so active JS edits are never served stale, but Flask's
    # send_from_directory sets ETag/Last-Modified by default, so an
    # unchanged file returns a tiny 304 instead of re-transferring the full
    # ~40KB body every time (2026-08 performance audit found this was
    # unconditionally re-fetched on every fresh page load).
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@server.route("/alert-email/<track_id>/<forecast_time>/<country_code>")
def serve_alert_email(track_id, forecast_time, country_code):
    """Real Alert email HTML (AOTS.TC_ECMWF.ALERT_SENT_LOG.EMAIL_BODY), served
    as a normal page — backs both the alert-email-modal iframe's `src` and
    its "Open in new tab" link (pages/map_shell_concept.py's
    _open_alert_email_detail). Real bug found+fixed here: this used to be a
    giant `data:text/html;charset=utf-8,<percent-encoded ~700KB body>` URI
    built client-side — real emails are large enough (embedded base64 map
    images) that the percent-encoded URI could balloon past practical
    browser limits for a fresh top-level navigation, so "Open in new tab"
    opened a blank tab that only rendered after a manual refresh. A real
    HTTP GET has no such size quirk."""
    from components.data.snowflake_utils import get_alert_email_body
    body = get_alert_email_body(track_id, forecast_time, country_code)
    if body is None:
        return Response("Alert email not found.", mimetype="text/plain", status=404)
    return Response(body, mimetype="text/html")


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
    # dev_tools_ui=False: several pages (map_shell_concept.py) inject
    # components into controls-body reactively (Country Analysis mode only) —
    # callbacks referencing their ids are legitimately absent from the very
    # first layout snapshot, which the renderer flags as a "nonexistent
    # object" ReferenceError even though the components mount and work
    # correctly once that mode is entered (confirmed: zero such errors once
    # Country Analysis mode has rendered once, and every control keeps
    # working). dev_tools_props_check (prop type/value validation) and
    # dev_tools_validate_callbacks (circular-dependency checks) are separate
    # flags that don't gate this specific renderer warning or its overlay —
    # only dev_tools_ui does. Hot reload/dev_tools_hot_reload is untouched.
    # dev_tools_ui=False: several pages (map_shell_concept.py) inject
    # components into controls-body reactively (Country Analysis mode only) —
    # callbacks referencing their ids are legitimately absent from the very
    # first layout snapshot, which the renderer flags as a "nonexistent
    # object" ReferenceError even though the components mount and work
    # correctly once that mode is entered (confirmed: zero such errors once
    # Country Analysis mode has rendered once, and every control keeps
    # working). dev_tools_props_check (prop type/value validation) and
    # dev_tools_validate_callbacks (circular-dependency checks) are separate
    # flags that don't gate this specific renderer warning or its overlay —
    # only dev_tools_ui does. Hot reload/dev_tools_hot_reload is untouched.
    app.run(debug=True, dev_tools_ui=False)
