"""
Dashboard layout definitions.
All visual components — left controls panel, center map panel, right metrics panel —
assembled into the three-panel AppShell returned by make_single_page_appshell().

Accepts startup-computed data (country_options, default_country) as parameters so
this module has no dependency on pages/dashboard.py (no circular imports).
"""
from dash import dcc, html
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_leaflet as dl

from components.config import config
from components.map.map_config import map_config, mapbox_token
from components.ui.styling import create_legend_divs
from components.map.javascript import (
    style_tracks, point_to_layer_schools_health,
    style_envelopes, tooltip_tracks, tooltip_envelopes,
    tooltip_schools, tooltip_health,
    tooltip_shelters, tooltip_wash,
    sync_maplibre_on_load, sync_maplibre_on_move,
)

# ---------------------------------------------------------------------------
# Shared style constants — imported by dashboard.py for use in callbacks too
# ---------------------------------------------------------------------------
_SWITCH_TRACK_BASE = {"minWidth": "5.5rem"}
_SWITCH_LABEL_BASE = {"fontSize": "11px", "fontWeight": 600, "paddingLeft": "12px", "paddingRight": "4px"}
PRIMARY_COLOR = "#1cabe2"
_DISPLAY_NONE = {"display": "none"}
_IMPACT_GRADIENT = (
    "linear-gradient(to right, #808080, #FFFF00, #FFD700, #FFA500,"
    " #FF8C00, #FF4500, #DC143C, #8B0000)"
)
_UN_DISCLAIMER = (
    'The boundaries and names shown and the designations used on this map '
    'do not imply official endorsement or acceptance by the United Nations.'
)


def make_single_page_appshell(country_options, default_country):
    """Build and return the complete dashboard AppShell.

    Args:
        country_options: List of country/region options for the country dropdown.
        default_country: Default selected country code (e.g. 'JAM').
    """
    # -------------------------------------------------------------------------
    # Step 1: Country Selection
    # -------------------------------------------------------------------------
    country_selection = dmc.Paper([
        dmc.Group([
            dmc.Badge("1", size="sm", color=PRIMARY_COLOR, variant="filled"),
            dmc.Text("COUNTRY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
        ], mb="xs", justify="flex-start"),
        dmc.Select(
            id="country-select",
            placeholder="Select country...",
            data=country_options,
            value=default_country,
            mb="xs"
        ),
        dmc.Select(
            id="individual-country-select",
            placeholder="Individual country (optional)...",
            data=[],
            value=None,
            clearable=True,
            style=_DISPLAY_NONE,
        ),
    ],
    p="sm",
    shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "12px"}
    )

    # -------------------------------------------------------------------------
    # Step 2: Hurricane Exploration
    # -------------------------------------------------------------------------
    hurricane_exploration = dmc.Paper([
        dmc.Group([
            dmc.Badge("2", size="sm", color=PRIMARY_COLOR, variant="filled"),
            dmc.Text("HURRICANE", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}),
        ], justify="flex-start", gap="sm", mb="xs"),
        dmc.Select(
            id="forecast-date",
            placeholder="Select forecast date...",
            data=[],
            value=None,
            mb="xs"
        ),
        dmc.Select(
            id="forecast-time",
            placeholder="Select forecast time...",
            data=[],
            value=None,
            mb="xs"
        ),
        dmc.Select(
            id="storm-select",
            placeholder="Select hurricane...",
            data=[],
            value=None,
            mb="xs"
        ),
        dmc.Select(
            id="wind-threshold-select",
            placeholder="Select wind threshold...",
            data=[
                {"value": "34",  "label": "34kt - Tropical storm force (17.49 m/s)"},
                {"value": "40",  "label": "40kt - Strong tropical storm (20.58 m/s)"},
                {"value": "50",  "label": "50kt - Very strong tropical storm (25.72 m/s)"},
                {"value": "64",  "label": "64kt - Category 1 hurricane (32.92 m/s)"},
                {"value": "83",  "label": "83kt - Category 2 hurricane (42.70 m/s)"},
                {"value": "96",  "label": "96kt - Category 3 hurricane (49.39 m/s)"},
                {"value": "113", "label": "113kt - Category 4 hurricane (58.12 m/s)"},
                {"value": "137", "label": "137kt - Category 5 hurricane (70.48 m/s)"},
            ],
            value="34",
            mb="xs"
        ),
    ],
    p="sm",
    shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "12px"}
    )

    # -------------------------------------------------------------------------
    # Step 3: Load Layers Button
    # -------------------------------------------------------------------------
    load_layers_button = dmc.Paper([
        dmc.Group([
            dmc.Badge("3", size="sm", color=PRIMARY_COLOR, variant="filled"),
            dmc.Text("LOAD LAYERS", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"}),
        ], justify="flex-start", gap="sm", mb="sm"),
        dmc.Text("Load all available data layers for the selected hurricane", size="xs", c="dimmed", mb="md"),
        dmc.Button(
            "Load Layers",
            id="load-layers-btn",
            leftSection=DashIconify(icon="carbon:download", width=20),
            variant="filled",
            color=PRIMARY_COLOR,
            fullWidth=True,
            mb="md",
            loaderProps={"type": "dots"}
        ),
        html.Div("Status: Not loaded", id="load-status", style={"fontSize": "12px", "color": "#868e96", "marginBottom": "16px"})
    ],
    p="md",
    shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "16px"}
    )

    # -------------------------------------------------------------------------
    # Layer sub-sections
    # -------------------------------------------------------------------------
    hurricane_selection = dmc.Box([
        dmc.Text("Hurricane Data", size="sm", fw=600, mb="xs"),
        dmc.Checkbox(id="hurricane-tracks-toggle",   label="Hurricane Tracks",    checked=False, mb="xs", disabled=True),
        dmc.Checkbox(id="hurricane-envelopes-toggle", label="Hurricane Envelopes", checked=False, mb="xs", disabled=True),
    ], id='hurricane_selection_box')

    infrastructure_impact = dmc.Box([
        dmc.Text("Infrastructure Impact", size="sm", fw=600, mb="xs", mt="xs"),
        dmc.Checkbox(id="schools-layer", label="Schools", checked=False, mb="xs", disabled=True),
        dmc.Grid([
            dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
            dmc.GridCol(span=8, children=[
                html.Div(style={"width": "100%", "height": "10px", "background": _IMPACT_GRADIENT,
                                "border": "1px solid #ccc", "borderRadius": "1px"})
            ]),
            dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="schools-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),
        dmc.Checkbox(id="health-layer", label="Health Centers", checked=False, mb="xs", disabled=True),
        dmc.Grid([
            dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
            dmc.GridCol(span=8, children=[
                html.Div(style={"width": "100%", "height": "10px", "background": _IMPACT_GRADIENT,
                                "border": "1px solid #ccc", "borderRadius": "1px"})
            ]),
            dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="health-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),
        dmc.Checkbox(id="shelters-layer", label="Shelters", checked=False, mb="xs", disabled=True),
        dmc.Grid([
            dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
            dmc.GridCol(span=8, children=[
                html.Div(style={"width": "100%", "height": "10px", "background": _IMPACT_GRADIENT,
                                "border": "1px solid #ccc", "borderRadius": "1px"})
            ]),
            dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="shelters-infra-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),
        dmc.Checkbox(id="wash-layer", label="WASH Facilities", checked=False, mb="xs", disabled=True),
        dmc.Grid([
            dmc.GridCol(span=2, children=[dmc.Text("0%", size="xs", c="dimmed")]),
            dmc.GridCol(span=8, children=[
                html.Div(style={"width": "100%", "height": "10px", "background": _IMPACT_GRADIENT,
                                "border": "1px solid #ccc", "borderRadius": "1px"})
            ]),
            dmc.GridCol(span=2, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="wash-infra-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),
    ], id='infrastructure_impact_box')

    probability_layer_tiles = dmc.Box([
        dmc.Checkbox(id="probability-tiles-layer", label="Impact Probability", checked=False, mb="xs", disabled=True),
        html.Div(id="probability-legend", children=[
            dmc.Grid([
                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-min", children="0%", size="xs", c="dimmed")]),
                dmc.GridCol(span=9,   children=html.Div(create_legend_divs('probability'), style={"display": "flex", "width": "100%"})),
                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-max", children="100%", size="xs", c="dimmed")]),
            ], gutter="xs", mb="xs")
        ], style=_DISPLAY_NONE),
    ], id='probability_layer_tiles_box')

    probability_layer_admin = dmc.Box([
        dmc.Checkbox(id="probability-admin-layer", label="Impact Probability", checked=False, mb="xs", disabled=True),
        html.Div(id="probability-legend-admin", children=[
            dmc.Grid([
                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-admin-min", children="0%", size="xs", c="dimmed")]),
                dmc.GridCol(span=9,   children=html.Div(create_legend_divs('probability'), style={"display": "flex", "width": "100%"})),
                dmc.GridCol(span=1.5, children=[dmc.Text(id="probability-legend-admin-max", children="100%", size="xs", c="dimmed")]),
            ], gutter="xs", mb="xs")
        ], style=_DISPLAY_NONE),
    ], id='probability_layer_admin_box')

    # -------------------------------------------------------------------------
    # Tiles radio group + legends
    # -------------------------------------------------------------------------
    _sub_radio_row = {"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "8px"}

    tiles_radiogroup = dmc.RadioGroup([
        dmc.Radio(id="none-tiles-layer", label="No Tile Layer (just Probability)", value="none", mb="md"),
        html.Div([
            dmc.Radio(id="population-tiles-layer", label="Population", value="population"),
            dmc.Switch(id="in-need-tiles-switch", checked=False, disabled=True,
                       size="md", color=PRIMARY_COLOR,
                       onLabel="In need", offLabel="At risk",
                       styles={"track": _SWITCH_TRACK_BASE, "trackLabel": _SWITCH_LABEL_BASE}),
        ], style=_sub_radio_row),
        html.Div([
            dmc.Radio(id="children-total-tiles-layer", label="Children (total)", value="children-total"),
            dmc.Switch(id="in-need-children-tiles-switch", checked=False, disabled=True,
                       size="md", color=PRIMARY_COLOR,
                       onLabel="In need", offLabel="At risk",
                       styles={"track": _SWITCH_TRACK_BASE, "trackLabel": _SWITCH_LABEL_BASE}),
        ], style=_sub_radio_row),
        dmc.Radio(id="infant-tiles-layer",     label=html.Span("Age 0–4",   style={"paddingLeft": "12px", "color": "#888", "fontSize": "0.88em"}), value="infant",     mb=6),
        dmc.Radio(id="school-age-tiles-layer", label=html.Span("Age 5–14",  style={"paddingLeft": "12px", "color": "#888", "fontSize": "0.88em"}), value="school-age", mb=6),
        dmc.Radio(id="adolescent-tiles-layer", label=html.Span("Age 15–19", style={"paddingLeft": "12px", "color": "#888", "fontSize": "0.88em"}), value="adolescent", mb="md"),
        dmc.Radio(id="built-surface-tiles-layer", label="Built Surface Area",          value="built-surface", mb="xs"),
        dmc.Radio(id="cci-tiles-layer",           label="CCI (Child Cyclone Index)",   value="cci",           mb="xs"),
        dmc.Divider(mb="xs", mt="xs"),
        dmc.Text("Context Data", size="xs", fw=600, c="dimmed", mb="xs", style={"textTransform": "uppercase", "letterSpacing": "1px"}),
        dmc.Radio(id="settlement-tiles-layer",       label="Settlement Classification",   value="settlement",       mb="xs"),
        dmc.Radio(id="rwi-tiles-layer",              label="Relative Wealth Index",        value="rwi",              mb="xs"),
        dmc.Radio(id="moderate-poverty-tiles-layer", label="Moderate Child Poverty Rate", value="moderate-poverty", mb="xs"),
        dmc.Radio(id="severe-poverty-tiles-layer",   label="Severe Child Poverty Rate",   value="severe-poverty",   mb="xs"),
        dmc.Divider(mb="xs", mt="xs"),
    ], id="tiles-layer-group", value="none")

    _flex_full = {"display": "flex", "width": "100%"}
    tiles_legends = dmc.Box([
        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="population-legend-min",      children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('population'),               style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="population-legend-max",      children="Max", size="xs", c="dimmed")]),
        ], id="population-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-legend-min",  children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('children_total'),             style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-legend-max",  children="Max", size="xs", c="dimmed")]),
        ], id="children-total-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="infant-legend-min",          children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('infant_population'),          style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="infant-legend-max",          children="Max", size="xs", c="dimmed")]),
        ], id="infant-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-legend-min",      children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('school_age_population'),      style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-legend-max",      children="Max", size="xs", c="dimmed")]),
        ], id="school-age-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-legend-min",      children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('adolescent_population'),      style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-legend-max",      children="Max", size="xs", c="dimmed")]),
        ], id="adolescent-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-legend-min",   children="Min", size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('built_surface_m2'),           style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-legend-max",   children="Max", size="xs", c="dimmed")]),
        ], id="built-surface-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-legend-min",             children="Min", size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs(config.CCI_COL),               style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-legend-max",             children="Max", size="xs", c="dimmed")]),
        ], id="cci-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        # Settlement: categorical, not a continuous scale
        dmc.Grid([
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#d3d3d3", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("No Data",       size="xs", c="dimmed", ta="center")]),
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#dda0dd", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("Rural",         size="xs", c="dimmed", ta="center")]),
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#9370db", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("Urban Clusters", size="xs", c="dimmed", ta="center")]),
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#4b0082", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("Urban Centers",  size="xs", c="dimmed", ta="center")]),
        ], id="settlement-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text("-1",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('rwi'), style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text("+1",   size="xs", c="dimmed")]),
        ], id="rwi-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text("0%",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('moderate_poverty_prob'), style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="moderate-poverty-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text("0%",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('severe_poverty_prob'), style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="severe-poverty-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),
    ], id='tiles_legends_box')

    # -------------------------------------------------------------------------
    # Admin radio group + legends
    # -------------------------------------------------------------------------
    admin_radiogroup = dmc.RadioGroup([
        dmc.Radio(id="none-admin-layer", label="No Region Layer (just Probability)", value="none", mb="md"),
        html.Div([
            dmc.Radio(id="population-admin-layer", label="Population", value="population"),
            dmc.Switch(id="in-need-admin-switch", checked=False, disabled=True,
                       size="md", color=PRIMARY_COLOR,
                       onLabel="In need", offLabel="At risk",
                       styles={"track": _SWITCH_TRACK_BASE, "trackLabel": _SWITCH_LABEL_BASE}),
        ], style=_sub_radio_row),
        html.Div([
            dmc.Radio(id="children-total-admin-layer", label="Children (total)", value="children-total"),
            dmc.Switch(id="in-need-children-admin-switch", checked=False, disabled=True,
                       size="md", color=PRIMARY_COLOR,
                       onLabel="In need", offLabel="At risk",
                       styles={"track": _SWITCH_TRACK_BASE, "trackLabel": _SWITCH_LABEL_BASE}),
        ], style=_sub_radio_row),
        dmc.Radio(id="infant-admin-layer",     label=html.Span("Age 0–4",   style={"paddingLeft": "12px", "color": "#888", "fontSize": "0.88em"}), value="infant",     mb=6),
        dmc.Radio(id="school-age-admin-layer", label=html.Span("Age 5–14",  style={"paddingLeft": "12px", "color": "#888", "fontSize": "0.88em"}), value="school-age", mb=6),
        dmc.Radio(id="adolescent-admin-layer", label=html.Span("Age 15–19", style={"paddingLeft": "12px", "color": "#888", "fontSize": "0.88em"}), value="adolescent", mb="xs"),
        dmc.Radio(id="built-surface-admin-layer", label="Built Surface Area",         value="built-surface", mb="xs"),
        dmc.Radio(id="cci-admin-layer",           label="CCI (Child Cyclone Index)",  value="cci",           mb="xs"),
        dmc.Divider(mb="xs", mt="xs"),
        dmc.Text("Context Data", size="xs", fw=600, c="dimmed", mb="xs", style={"textTransform": "uppercase", "letterSpacing": "1px"}),
        dmc.Radio(id="settlement-admin-layer",       label="Settlement Classification",   value="settlement",       mb="xs"),
        dmc.Radio(id="rwi-admin-layer",              label="Relative Wealth Index",        value="rwi",              mb="xs"),
        dmc.Radio(id="moderate-poverty-admin-layer", label="Moderate Child Poverty Rate", value="moderate-poverty", mb="xs"),
        dmc.Radio(id="severe-poverty-admin-layer",   label="Severe Child Poverty Rate",   value="severe-poverty",   mb="xs"),
        dmc.Divider(mb="xs", mt="xs"),
    ], id="admin-layer-group", value="none")

    admin_legends = dmc.Box([
        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="population-admin-legend-min",      children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('population'),                       style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="population-admin-legend-max",      children="Max", size="xs", c="dimmed")]),
        ], id="population-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-admin-legend-min",  children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('children_total'),                   style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="children-total-admin-legend-max",  children="Max", size="xs", c="dimmed")]),
        ], id="children-total-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="infant-admin-legend-min",          children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('infant_population'),                style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="infant-admin-legend-max",          children="Max", size="xs", c="dimmed")]),
        ], id="infant-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-admin-legend-min",      children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('school_age_population'),            style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="school-age-admin-legend-max",      children="Max", size="xs", c="dimmed")]),
        ], id="school-age-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-admin-legend-min",      children="0",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('adolescent_population'),            style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="adolescent-admin-legend-max",      children="Max", size="xs", c="dimmed")]),
        ], id="adolescent-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-admin-legend-min",   children="Min", size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('built_surface_m2'),                 style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="built-surface-admin-legend-max",   children="Max", size="xs", c="dimmed")]),
        ], id="built-surface-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-admin-legend-min",             children="Min", size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs(config.CCI_COL),                     style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text(id="cci-admin-legend-max",             children="Max", size="xs", c="dimmed")]),
        ], id="cci-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#d3d3d3", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("No Data",       size="xs", c="dimmed", ta="center")]),
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#dda0dd", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("Rural",         size="xs", c="dimmed", ta="center")]),
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#9370db", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("Urban Clusters", size="xs", c="dimmed", ta="center")]),
            dmc.GridCol(span=3, children=[html.Div(style={"width": "100%", "height": "10px", "backgroundColor": "#4b0082", "border": "1px solid #ccc", "borderRadius": "1px"}), dmc.Text("Urban Centers",  size="xs", c="dimmed", ta="center")]),
        ], id="settlement-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text("-1",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('rwi'), style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text("+1",   size="xs", c="dimmed")]),
        ], id="rwi-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text("0%",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('moderate_poverty_prob'), style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="moderate-poverty-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),

        dmc.Grid([
            dmc.GridCol(span=1.5, children=[dmc.Text("0%",   size="xs", c="dimmed")]),
            dmc.GridCol(span=9,   children=html.Div(create_legend_divs('severe_poverty_prob'), style=_flex_full)),
            dmc.GridCol(span=1.5, children=[dmc.Text("100%", size="xs", c="dimmed")]),
        ], id="severe-poverty-admin-legend", style=_DISPLAY_NONE, gutter="xs", mb="xs"),
    ], id='admin_legends_box')

    # -------------------------------------------------------------------------
    # Population & Infrastructure combined (tiles vs admin toggle)
    # -------------------------------------------------------------------------
    population_infrastructure_selection = dmc.Box([
        dmc.Text("Population & Infrastructure", size="sm", fw=600, mb="xs", mt="xs"),
        dmc.SegmentedControl(
            id="layer-mode-selector",
            value="tiles",
            data=[
                {"value": "tiles", "label": "Tiles (Rasters)"},
                {"value": "admin", "label": "By Region"},
            ],
            mb="md",
            fullWidth=True,
        ),
        dmc.Box([probability_layer_tiles, tiles_radiogroup, tiles_legends], id="tiles-mode-box"),
        dmc.Box([probability_layer_admin, admin_radiogroup, admin_legends], id="admin-mode-box", style=_DISPLAY_NONE),
    ], id='population_infrastructure_selection_box')

    layer_selection = dmc.Stack([
        hurricane_selection,
        infrastructure_impact,
        html.Div(id="layer-no-data-warning"),
        population_infrastructure_selection,
        dmc.Text(
            'Note: When "Impact Probability" is enabled, "Population Density", "School-Age Population", '
            '"Adolescent Population (Age 15–19)", and "Built Surface Area" show expected impact '
            '(base value × probability). Context Data layers cannot be selected when "Impact Probability" is active.',
            size="xs", c="dimmed", mb="md", mt="xs"
        ),
    ], gap="xs", id='layer_selection_stack')

    # -------------------------------------------------------------------------
    # Step 4: Layer Controls
    # -------------------------------------------------------------------------
    layers_controls = dmc.Paper([
        dmc.Group([
            dmc.Badge("4", size="sm", color=PRIMARY_COLOR, variant="filled"),
            dmc.Text("LAYER CONTROLS", size="sm", fw=700, c="dark", ta="left", style={"letterSpacing": "0.5px"}),
        ], justify="flex-start", gap="sm", mb="sm"),
        dmc.Text("Toggle layers on/off to explore different data types", size="xs", c="dimmed", mb="md"),
        layer_selection,
    ],
    p="md",
    shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "16px"}
    )

    # -------------------------------------------------------------------------
    # Left Panel
    # -------------------------------------------------------------------------
    left_panel = dmc.GridCol(
        [dmc.Paper([country_selection, hurricane_exploration, load_layers_button, layers_controls],
                   p="md", shadow="sm")],
        span=3,
        style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"},
    )

    # -------------------------------------------------------------------------
    # Center Panel — MapLibre (bottom) + Leaflet (top)
    # -------------------------------------------------------------------------
    center_panel = dmc.GridCol(
        html.Div([
            dcc.Store(id="effective-country-store", data=default_country),
            dcc.Interval(id="metadata-refresh-interval", interval=15 * 60 * 1000, n_intervals=0),
            dcc.Store(id="map-state-store",               data={}),
            dcc.Store(id="envelope-data-store",           data={}),
            dcc.Store(id="schools-data-store",            data={}),
            dcc.Store(id="health-data-store",             data={}),
            dcc.Store(id="shelters-data-store",           data={}),
            dcc.Store(id="wash-data-store",               data={}),
            dcc.Store(id="population-tiles-data-store",   data={}),
            dcc.Store(id="population-admin-data-store",   data={}),
            dcc.Store(id="tiles-stats-store",             data={}),
            dcc.Store(id="admin-stats-store",             data={}),
            dcc.Store(id="tracks-data-store",             data={}),
            dcc.Store(id="layers-loaded-store",           data=False),
            dcc.Store(id="preload-dummy-store",           data=None),
            dcc.Store(id="layer-availability-store",      data={}),
            dcc.Store(id="maplibre-tile-config-store",    data={}),
            html.Div([
                # MapLibre canvas — renders tile/admin layers underneath Leaflet
                html.Div(
                    id="maplibre-container",
                    **{"data-mapbox-token": mapbox_token or ""},
                    style={
                        "height": "calc(100vh - 147px)",
                        "width": "100%",
                        "position": "absolute",
                        "top": 0, "left": 0, "zIndex": 0,
                    },
                ),
                # Leaflet map — tracks, envelopes, schools, etc. on top
                dl.Map(
                    [
                        dl.LayersControl(
                            [
                                # opacity=0 = transparent; Leaflet shows nothing, MapLibre provides basemap.
                                # Attribution strings are still read by Leaflet's attribution control.
                                # baselayerchange → swapMaplibreBasemap in maplibre_tiles.js.
                                dl.BaseLayer(
                                    dl.TileLayer(
                                        opacity=0,
                                        attribution=(
                                            ('© <a href="https://www.mapbox.com/about/maps/">Mapbox</a> '
                                             '© <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a><br>'
                                             + _UN_DISCLAIMER)
                                            if mapbox_token else
                                            ('© <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors<br>'
                                             + _UN_DISCLAIMER)
                                        ),
                                    ),
                                    name="Mapbox Light" if mapbox_token else "OpenStreetMap",
                                    checked=False,
                                ),
                                dl.BaseLayer(
                                    dl.TileLayer(opacity=0, attribution='© <a href="https://carto.com/attributions">CARTO</a><br>' + _UN_DISCLAIMER),
                                    name="CartoDB Light",
                                    checked=True,
                                ),
                                dl.BaseLayer(
                                    dl.TileLayer(opacity=0, attribution='© <a href="https://carto.com/attributions">CARTO</a> © <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a><br>' + _UN_DISCLAIMER),
                                    name="CartoDB Dark",
                                    checked=False,
                                ),
                                dl.BaseLayer(
                                    dl.TileLayer(opacity=0, attribution='Tiles &copy; <a href="https://services.arcgisonline.com/">Esri</a> &mdash; Source: Esri, Maxar, Earthstar Geographics<br>' + _UN_DISCLAIMER),
                                    name="Satellite",
                                    checked=False,
                                ),
                            ],
                            position="topright",
                        ),
                        dl.GeoJSON(id="hurricane-tracks-json", data={}, zoomToBounds=False, style=style_tracks,                 onEachFeature=tooltip_tracks),
                        dl.GeoJSON(id="envelopes-json-test",   data={}, zoomToBounds=False, style=style_envelopes,              onEachFeature=tooltip_envelopes),
                        dl.GeoJSON(id="schools-overlay-json",  data={}, zoomToBounds=False, pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_schools),
                        dl.GeoJSON(id="health-overlay-json",   data={}, zoomToBounds=False, pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_health),
                        dl.GeoJSON(id="shelters-overlay-json", data={}, zoomToBounds=False, pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_shelters),
                        dl.GeoJSON(id="wash-overlay-json",     data={}, zoomToBounds=False, pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_wash),
                        # These four hold empty GeoJSON — MapLibre renders the actual tiles.
                        # They exist only as Dash state containers for hideout props (tile coloring).
                        dl.GeoJSON(id="population-tiles-json",  data={}, zoomToBounds=False, hideout={"hidden": True}),
                        dl.GeoJSON(id="population-admin-json",  data={}, zoomToBounds=False, hideout={"hidden": True}),
                        dl.GeoJSON(id="probability-tiles-json", data={}, zoomToBounds=False, hideout={"hidden": True}),
                        dl.GeoJSON(id="probability-admin-json", data={}, zoomToBounds=False, hideout={"hidden": True}),
                        dl.FullScreenControl(),
                        dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
                    ],
                    id="main-map",
                    center=[map_config.center["lat"], map_config.center["lon"]],
                    zoom=map_config.zoom,
                    viewport={"center": [map_config.center["lat"], map_config.center["lon"]], "zoom": map_config.zoom},
                    scrollWheelZoom=True,
                    preferCanvas=True,
                    eventHandlers={"load": sync_maplibre_on_load, "move": sync_maplibre_on_move},
                    style={
                        "height": "calc(100vh - 147px)",
                        "width": "100%",
                        "position": "absolute",
                        "top": 0, "left": 0, "zIndex": 1,
                        "background": "transparent",
                    },
                ),
            ], style={"position": "relative", "height": "calc(100vh - 147px)", "width": "100%"}),
        ]),
        span=6,
        style={"height": "100%", "minHeight": 0},
    )

    # -------------------------------------------------------------------------
    # Right Panel — Impact metrics, specific track, exceedance chart
    # -------------------------------------------------------------------------
    impact_summary = dmc.Paper([
        dmc.Group([
            dmc.Text("IMPACT SUMMARY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
        ], justify="flex-start", gap="sm", mb="sm"),
        dmc.Text("Hurricane impact scenarios and metrics", size="xs", c="dimmed", mb="md"),
        html.Div(style={"overflowX": "auto"}, children=[
            dmc.Table(
                [
                    dmc.TableThead([
                        dmc.TableTr([
                            dmc.TableTh([
                                dmc.Text("Metric",    style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                dmc.Text("at Risk",   style={"margin": 0, "fontSize": "0.85em", "fontWeight": 400, "color": "#6c757d"}, c="dimmed"),
                            ], style={"fontWeight": 700, "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "height": "60px", "verticalAlign": "top", "paddingTop": "8px"}),
                            dmc.TableTh([
                                dmc.Text("DET", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                dmc.Badge("#51", id="deterministic-badge", size="xs", color="blue", variant="light", style={"marginTop": "2px"}),  # must match DETERMINISTIC_MEMBER_ID in callbacks/metrics.py
                            ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"}),
                            dmc.TableTh("Expected",   style={"fontWeight": 700, "textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "paddingTop": "8px", "height": "60px", "verticalAlign": "top"}),
                            dmc.TableTh([
                                dmc.Text("Worst", style={"fontWeight": 700, "margin": 0, "fontSize": "inherit"}),
                                dmc.Badge("Member", id="high-impact-badge", size="xs", color="red", variant="light", style={"marginTop": "2px"}),
                            ], style={"textAlign": "center", "backgroundColor": "#f8f9fa", "color": "#495057", "borderBottom": "2px solid #dee2e6", "verticalAlign": "top", "paddingTop": "8px", "height": "60px"}),
                        ])
                    ]),
                    dmc.TableTbody([
                        dmc.TableTr([dmc.TableTd("Population"),         dmc.TableTd("0",     id="population-count-low",          style={"textAlign": "center"}), dmc.TableTd("2,482",  id="population-count-probabilistic",  style={"textAlign": "center"}), dmc.TableTd("59,678",  id="population-count-high",          style={"textAlign": "center"})]),
                        dmc.TableTr([dmc.TableTd([html.Span("Children"), html.Span(" (total)", style={"fontSize": "0.8em", "color": "#888", "marginLeft": "3px"})]),
                                     dmc.TableTd("N/A", id="children-total-low",          style={"textAlign": "center"}), dmc.TableTd("N/A", id="children-total-probabilistic",  style={"textAlign": "center"}), dmc.TableTd("N/A", id="children-total-high",          style={"textAlign": "center"})]),
                        dmc.TableTr([dmc.TableTd("Age 0–4",  style={"fontStyle": "italic", "fontSize": "0.93em", "color": "#888", "paddingLeft": "18px"}), dmc.TableTd("N/A", id="infant-affected-low",       style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}), dmc.TableTd("N/A", id="infant-affected-probabilistic",   style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}), dmc.TableTd("N/A", id="infant-affected-high",       style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"})]),
                        dmc.TableTr([dmc.TableTd("Age 5–14", style={"fontStyle": "italic", "fontSize": "0.93em", "color": "#888", "paddingLeft": "18px"}), dmc.TableTd("N/A", id="children-affected-low",     style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}), dmc.TableTd("N/A", id="children-affected-probabilistic", style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}), dmc.TableTd("N/A", id="children-affected-high",     style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"})]),
                        dmc.TableTr([dmc.TableTd("Age 15–19",style={"fontStyle": "italic", "fontSize": "0.93em", "color": "#888", "paddingLeft": "18px"}), dmc.TableTd("N/A", id="adolescent-affected-low",   style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}), dmc.TableTd("N/A", id="adolescent-affected-probabilistic",style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"}), dmc.TableTd("N/A", id="adolescent-affected-high",   style={"textAlign": "center", "fontSize": "0.93em", "whiteSpace": "nowrap", "color": "#888"})]),
                        dmc.TableTr([dmc.TableTd("Schools"),             dmc.TableTd("0",     id="schools-count-low",             style={"textAlign": "center"}), dmc.TableTd("2",      id="schools-count-probabilistic",     style={"textAlign": "center"}), dmc.TableTd("39",      id="schools-count-high",             style={"textAlign": "center"})]),
                        dmc.TableTr([dmc.TableTd("Health Centers"),      dmc.TableTd("0",     id="health-count-low",              style={"textAlign": "center"}), dmc.TableTd("1",      id="health-count-probabilistic",      style={"textAlign": "center"}), dmc.TableTd("0",       id="health-count-high",              style={"textAlign": "center"})]),
                        dmc.TableTr([dmc.TableTd("Shelters"),            dmc.TableTd("N/A",   id="shelters-count-low",            style={"textAlign": "center"}), dmc.TableTd("N/A",    id="shelters-count-probabilistic",    style={"textAlign": "center"}), dmc.TableTd("N/A",     id="shelters-count-high",            style={"textAlign": "center"})]),
                        dmc.TableTr([dmc.TableTd("WASH Facilities"),     dmc.TableTd("N/A",   id="wash-count-low",                style={"textAlign": "center"}), dmc.TableTd("N/A",    id="wash-count-probabilistic",        style={"textAlign": "center"}), dmc.TableTd("N/A",     id="wash-count-high",                style={"textAlign": "center"})]),
                        dmc.TableTr([dmc.TableTd([html.Span("Built Surface m"), html.Sup("2")]),
                                     dmc.TableTd("0",     id="bsm2-count-low",               style={"textAlign": "center"}), dmc.TableTd("2,482",  id="bsm2-count-probabilistic",        style={"textAlign": "center"}), dmc.TableTd("59,678",  id="bsm2-count-high",                style={"textAlign": "center"})]),
                    ]),
                ],
                striped=True,
                highlightOnHover=True,
                withTableBorder=True,
                withColumnBorders=True,
                horizontalSpacing="xs",
                style={"tableLayout": "fixed", "width": "100%"},
            )
        ]),
    ],
    p="md", shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "16px"},
    )

    specific_track_view = dmc.Paper([
        dmc.Group([
            dmc.Text("SPECIFIC TRACK VIEW", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
        ], justify="flex-start", gap="sm", mb="sm"),
        dmc.Text("Visualize individual hurricane track scenarios", size="xs", c="dimmed", mb="md"),
        dmc.Stack([
            dmc.Button(
                dmc.Group([DashIconify(icon="mdi:map-marker-path", width=16), dmc.Text("Show Specific Track", ml="xs")]),
                id="show-specific-track-btn",
                variant="outline", size="sm", disabled=True, mb="md",
            ),
            dmc.Select(id="specific-track-select", label="Select Track Scenario",
                       placeholder="Choose ensemble member...", data=[], mb="md", disabled=True,
                       style=_DISPLAY_NONE),
            dmc.Text("Members are ordered by total population impacted (deterministic first).",
                     id="specific-track-order-note", size="xs", c="dimmed", mb="sm", style=_DISPLAY_NONE),
            dmc.Checkbox(id="show-all-envelopes-toggle", label="Show Higher Wind Thresholds",
                         checked=True, mb="md", disabled=True,
                         description="Display all wind thresholds that are higher than the selected threshold for this track."),
            dmc.Text("Load layers first, then select a specific track to see exact impact numbers",
                     size="xs", c="dimmed", id="specific-track-info"),
        ]),
    ],
    p="md", shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "16px"},
    )

    exceedance_chart = dmc.Paper([
        dmc.Group([
            dmc.Text("EXCEEDANCE PROBABILITY", size="sm", fw=700, c="dark", style={"letterSpacing": "0.5px"})
        ], justify="flex-start", gap="sm", mb="sm"),
        dmc.Text("Probability of exceeding different impact thresholds", size="xs", c="dimmed", mb="md"),
        dcc.Graph(id="exceedance-probability-chart", style={"height": "400px", "width": "100%"}, responsive=True),
        html.Div(id="exceedance-legend", style={"marginTop": "10px", "marginBottom": "10px"}),
        dmc.Text("Load layers to view exceedance probability based on ensemble forecasts.",
                 size="xs", c="dimmed", id="exceedance-chart-info"),
    ],
    p="md", shadow="xs",
    style={"borderLeft": f"3px solid {PRIMARY_COLOR}", "marginBottom": "16px"},
    )

    right_panel = dmc.GridCol(
        [dmc.Paper([impact_summary, specific_track_view, exceedance_chart], p="md", shadow="sm")],
        span=3,
        style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"},
    )

    # -------------------------------------------------------------------------
    # Assemble AppShell
    # -------------------------------------------------------------------------
    grid = dmc.Grid(
        [left_panel, center_panel, right_panel],
        gutter="md",
        style={"height": "100%", "margin": 0},
    )

    from components.ui.header import make_header
    from components.ui.footer import footer

    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_header(active_tab="tab-home"), px=15, zIndex=2000),
            dmc.AppShellMain(grid, style={"height": "calc(100vh - 67px - 80px)", "overflow": "hidden"}),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id="single-page-shell",
        header={"height": "67"},
        padding=0,
        footer={"height": "80"},
    )
