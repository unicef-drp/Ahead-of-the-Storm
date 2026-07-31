"""
Concept preview: Simple / Expert mode split for the main dashboard.

NOT wired to Snowflake, the tile server, or MapLibre — every number, narrative
sentence, and map area on this page is a hardcoded placeholder. Purpose: let a
real Mantine/Dash render of the Simple-vs-Expert idea (and a placeholder
narrative panel sourced from a hypothetical per-storm narrative table) be
judged in the actual app shell before touching pages/dashboard.py or
layouts/panels.py. Not linked from the header nav on purpose — reachable only
by visiting /concept directly.
"""
import dash
import dash_mantine_components as dmc
from dash import html, dcc, Input, Output, State, callback
from dash_iconify import DashIconify

from components.ui.header import make_header
from components.ui.footer import footer

dash.register_page(__name__, path="/concept", name="Concept (WIP)")

PRIMARY_COLOR = "#1cabe2"
_DISPLAY_NONE = {"display": "none"}

_HAZARD_COLORS = {"wind": PRIMARY_COLOR, "gust": "orange", "river": "teal", "rain": "grape"}

# ---------------------------------------------------------------------------
# Hardcoded example content only — stands in for a future per-storm narrative
# table (see conversation: "AI generates small narratives, stored in a table,
# pulled into the dashboard"). No such table exists yet; this is layout only.
# ---------------------------------------------------------------------------
_NARRATIVE = {
    "headline": "Tropical Storm BAVI is expected to pass north-east of Luzon.",
    "summary": "Rainfall remains the main concern, with potential river and flash flooding. "
               "Confidence in the forecast is medium.",
    "updated": "Updated 1 hour ago",
}

_HAZARD_OVERVIEW = [
    {"key": "rain",  "label": "Rainfall",         "risk": "High",     "color": "red",
     "note": "High probability of extreme 24–72h rainfall in parts of Northern Luzon."},
    {"key": "river", "label": "River Flood",       "risk": "Moderate", "color": "orange",
     "note": "Several basins may exceed historical return periods."},
    {"key": "runoff","label": "Runoff / Flash Flood","risk": "High",   "color": "red",
     "note": "Elevated runoff potential in mountainous regions."},
    {"key": "wind",  "label": "Wind",              "risk": "Moderate", "color": "orange",
     "note": "Tropical storm force winds possible in coastal areas."},
]

_EXPOSURE = [
    ("Children", "520K"), ("Schools", "3.1K"), ("Health Centers", "420"),
    ("WASH Facilities", "680"), ("Population", "1.2M"),
]


def _risk_badge(risk, color):
    return dmc.Badge(risk, color=color, variant="filled", size="sm", radius="sm")


_TREND_META = {
    "up":   {"icon": "carbon:arrow-up",   "color": "#c0392b", "word": "increasing"},
    "down": {"icon": "carbon:arrow-down", "color": "#2f9e64", "word": "decreasing"},
    "flat": {"icon": "carbon:subtract",   "color": "#8a97a3", "word": "steady"},
}


def _trend_icon(direction):
    m = _TREND_META[direction]
    return DashIconify(icon=m["icon"], color=m["color"], width=14)


def _section_header(title, subtitle):
    return dmc.Paper([
        dmc.Text(title, size="xs", fw=700, c=PRIMARY_COLOR, style={"letterSpacing": "0.06em"}, mb=4),
        dmc.Text(subtitle, size="sm", c="dimmed"),
    ], p="md", shadow="xs", withBorder=True)


# ---------------------------------------------------------------------------
# SIMPLE MODE — narrative-first briefing, modeled on the reference concept.
# Six persistent, always-reachable destinations (dmc.Tabs, not an accordion —
# every destination is visible up front, none require knowing they're there).
# ---------------------------------------------------------------------------
_HAZARD_TREND = {"rain": "up", "river": "flat", "runoff": "up", "wind": "down"}

_SCENARIOS = [
    ("Most Likely", "blue", "Most likely",
     "The single most probable outcome across all ensemble members.", "1.2M"),
    ("Worse Than Expected", "orange", "~20% chance",
     "A plausible, more severe outcome — track shifts closer to the coast.", "2.1M"),
    ("Best Case", "teal", "~15% chance",
     "A plausible, less severe outcome — storm weakens or veers away.", "480K"),
]

_CHILDREN_STATS = [
    ("Children Affected", "520K"), ("Children In Need", "180K"), ("Schools At Risk", "3.1K"),
    ("Age 0–4", "95K"), ("Age 5–14", "310K"), ("Age 15–19", "115K"),
]

_CHANGES = [
    ("Rainfall risk", "Moderate", "High", "up"),
    ("River Flood risk", "Low", "Moderate", "up"),
    ("Wind risk", "High", "Moderate", "down"),
    ("Children potentially affected", "410K", "520K", "up"),
    ("Track confidence", "Low", "Medium", "up"),
]

_DATA_SOURCES = ["ECMWF ENS", "GloFAS", "WorldPop", "UN GeoRepo", "GEOGLOWS"]


def _scenario_card(name, badge_color, prob_label, desc, pop):
    return dmc.Paper([
        dmc.Group([dmc.Text(name, size="sm", fw=700), dmc.Badge(prob_label, color=badge_color, variant="light", size="sm")],
                  justify="space-between", mb=6),
        dmc.Text(desc, size="xs", c="dimmed", mb=10),
        dmc.Text(pop, size="lg", fw=700, ff="monospace"),
        dmc.Text("people potentially affected", size="xs", c="dimmed"),
    ], p="sm", withBorder=True, radius="md")


def _tab_briefing():
    return dmc.Stack([_simple_current_situation(), _simple_hazard_overview(), _simple_map_placeholder()], gap="md")


def _tab_scenario_explorer():
    return dmc.Stack([
        _section_header("SCENARIO EXPLORER",
                         "Instead of 51 individual ensemble members, forecasts are grouped into three "
                         "scenarios you can compare directly."),
        dmc.SimpleGrid([_scenario_card(*s) for s in _SCENARIOS], cols=3, spacing="sm"),
    ], gap="md")


def _tab_impact_children():
    return dmc.Stack([
        _section_header("IMPACT ON CHILDREN",
                         "Every number below is specific to children — not total population — using "
                         "UNICEF's Child Cyclone Index (CCI) to flag the most vulnerable areas."),
        dmc.SimpleGrid([
            dmc.Paper([dmc.Text(k, size="xs", c="dimmed", fw=700, tt="uppercase"),
                       dmc.Text(v, size="xl", fw=700, ff="monospace")], p="sm", withBorder=True, radius="md")
            for k, v in _CHILDREN_STATS
        ], cols=3, spacing="sm"),
    ], gap="md")


def _tab_hazard_overview():
    cards = []
    for hz in _HAZARD_OVERVIEW:
        trend = _HAZARD_TREND[hz["key"]]
        cards.append(dmc.Paper([
            dmc.Group([dmc.Text(hz["label"], size="sm", fw=700), _trend_icon(trend)], justify="space-between", mb=6),
            _risk_badge(hz["risk"], hz["color"]),
            dmc.Text(hz["note"], size="xs", c="dimmed", mt=6),
            dmc.Text(f"Since last run: {_TREND_META[trend]['word']}", size="xs", c="dimmed", fs="italic", mt=6),
        ], p="sm", withBorder=True, radius="md"))
    return dmc.Stack([
        _section_header("HAZARD OVERVIEW",
                         "All four hazards this storm could bring, with trend since the previous forecast run."),
        dmc.SimpleGrid(cards, cols=4, spacing="sm"),
    ], gap="md")


def _tab_what_changed():
    rows = []
    for label, before, after, direction in _CHANGES:
        rows.append(dmc.Group([
            dmc.Text(label, size="sm", fw=600, style={"flex": 1}),
            dmc.Text(before, size="sm", c="dimmed"),
            DashIconify(icon="carbon:arrow-right", width=14, color="#8a97a3"),
            dmc.Text(after, size="sm", fw=700),
            _trend_icon(direction),
        ], gap=8, wrap="nowrap"))
    return dmc.Stack([
        _section_header("WHAT CHANGED", "Comparing this forecast run (Jul 5, 06:00) to the previous one (Jul 5, 00:00)."),
        dmc.Paper(dmc.Stack(rows, gap=10), p="md", shadow="xs", withBorder=True),
    ], gap="md")


def _tab_data_sources():
    return dmc.Stack([
        dmc.Paper([
            dmc.Text("DATA & SOURCES", size="xs", fw=700, c=PRIMARY_COLOR, style={"letterSpacing": "0.06em"}, mb="xs"),
            dmc.Group([dmc.Badge(s, variant="light", color="gray") for s in _DATA_SOURCES], gap=6, mb="md"),
            dmc.Text("Model run: v2.4.1 · Jul 5, 2026 06:00 UTC · 51-member ensemble", size="xs", c="dimmed", mb=4),
            dmc.Text("Impact estimates are probabilistic and subject to uncertainty. Always refer to local "
                      "early warning information.", size="xs", c="dimmed", fs="italic"),
        ], p="md", shadow="xs", withBorder=True),
    ], gap="md")


def _simple_nav_and_content():
    tabs = [
        ("briefing", "Briefing", _tab_briefing),
        ("scenario", "Scenario Explorer", _tab_scenario_explorer),
        ("children", "Impact on Children", _tab_impact_children),
        ("hazards", "Hazard Overview", _tab_hazard_overview),
        ("changed", "What Changed", _tab_what_changed),
        ("sources", "Data & Sources", _tab_data_sources),
    ]
    # "Switch to Expert Mode" isn't repeated here — the SegmentedControl in the
    # top bar already does that job, visibly on every tab; a second copy nested
    # in the nav would be a redundant, easy-to-miss-or-duplicate control.
    return dmc.Tabs([
        dmc.TabsList([
            dmc.TabsTab(label, value=key, style={"justifyContent": "flex-start"}) for key, label, _ in tabs
        ]),
    ] + [dmc.TabsPanel(builder(), value=key, pl="md") for key, _, builder in tabs],
        value="briefing", orientation="vertical", color=PRIMARY_COLOR, variant="pills",
    )


def _simple_current_situation():
    return dmc.Paper([
        dmc.Text("CURRENT SITUATION", size="xs", fw=700, c=PRIMARY_COLOR, style={"letterSpacing": "0.06em"}, mb="xs"),
        dmc.Text(_NARRATIVE["headline"], size="md", fw=700, mb="xs"),
        dmc.Text(_NARRATIVE["summary"], size="sm", c="dimmed", mb="sm"),
        dmc.Group([
            DashIconify(icon="carbon:time", width=14, color="#8a97a3"),
            dmc.Text(_NARRATIVE["updated"], size="xs", c="dimmed"),
        ], gap=4),
    ], p="md", shadow="xs", withBorder=True)


def _simple_hazard_overview():
    cards = []
    for hz in _HAZARD_OVERVIEW:
        cards.append(
            dmc.Paper([
                dmc.Group([
                    dmc.Text(hz["label"], size="sm", fw=700),
                ], justify="space-between", mb=6),
                _risk_badge(hz["risk"], hz["color"]),
                dmc.Text(hz["note"], size="xs", c="dimmed", mt=6),
            ], p="sm", withBorder=True, radius="md")
        )
    return dmc.Paper([
        dmc.Text("HAZARD OVERVIEW", size="xs", fw=700, c="dimmed", style={"letterSpacing": "0.06em"}, mb="sm"),
        dmc.SimpleGrid(cards, cols=4, spacing="sm"),
    ], p="md", shadow="xs", withBorder=True)


def _simple_map_placeholder():
    return dmc.Paper([
        dmc.Group([
            dmc.Text("MOST LIKELY SCENARIO", size="xs", fw=700, c="dimmed", style={"letterSpacing": "0.06em"}),
        ], justify="space-between", mb="sm"),
        html.Div(
            "Map preview — not connected to live tile server",
            style={
                "height": "360px", "borderRadius": "8px",
                "background": "linear-gradient(135deg, #eef6fa 0%, #dcecf4 40%, #f5e9d8 70%, #f0d9c8 100%)",
                "display": "flex", "alignItems": "center", "justifyContent": "center",
                "color": "#8a97a3", "fontSize": "13px", "fontWeight": 600, "border": "1px solid #dde6ec",
            },
        ),
    ], p="md", shadow="xs", withBorder=True)


def _simple_summary_sidebar():
    hazard_rows = [
        ("Rainfall", "High", "red"), ("River Flood", "Moderate", "orange"),
        ("Runoff / Flash Flood", "High", "red"), ("Wind", "Moderate", "orange"),
        ("Storm Surge", "Low", "green"),
    ]
    return dmc.Paper([
        dmc.Text("SITUATION SUMMARY", size="xs", fw=700, c="dimmed", style={"letterSpacing": "0.06em"}, mb="sm"),
        dmc.Text("Hazards", size="xs", fw=700, c="dimmed", mb=6),
        dmc.Stack([
            dmc.Group([dmc.Text(name, size="sm"), _risk_badge(risk, color)], justify="space-between")
            for name, risk, color in hazard_rows
        ], gap=6, mb="md"),
        dmc.Text("Exposure (Potential)", size="xs", fw=700, c="dimmed", mb=6),
        dmc.Stack([
            dmc.Group([dmc.Text(name, size="sm"), dmc.Text(val, size="sm", fw=700)], justify="space-between")
            for name, val in _EXPOSURE
        ], gap=6, mb="md"),
        dmc.Divider(mb="md"),
        dmc.Text("Uncertainty", size="xs", fw=700, c="dimmed", mb=6),
        dmc.Text("Track uncertainty remains moderate. Rainfall varies across ensemble members.",
                  size="xs", c="dimmed"),
    ], p="md", shadow="xs", withBorder=True)


def _simple_mode():
    # The Situation Summary sidebar stays constant across all six tabs — it's
    # the persistent "at a glance" panel, not tied to whichever detail view
    # you're currently reading.
    return dmc.Grid([
        dmc.GridCol(_simple_nav_and_content(), span=9),
        dmc.GridCol(_simple_summary_sidebar(), span=3),
    ], gutter="md")


# ---------------------------------------------------------------------------
# EXPERT MODE — reuses the validated hazard-rail pattern (checkbox + slider +
# plain-language readout, no accordions) plus a real-lead-time timeline
# ---------------------------------------------------------------------------
def _hazard_row(hz_key, label, checked, preview=False):
    color = _HAZARD_COLORS[hz_key]
    marks_count = 8 if hz_key in ("wind", "gust") else 6
    children = [
        dmc.Group([
            dmc.Checkbox(id=f"concept-{hz_key}-on", checked=checked, color=color),
            html.Div(style={"width": "8px", "height": "8px", "borderRadius": "50%", "background": color}),
            dmc.Text(label, size="sm", fw=600),
        ] + ([dmc.Badge("preview", size="xs", variant="light", color=color)] if preview else []), gap=8),
        html.Div(
            dmc.Slider(
                id=f"concept-{hz_key}-slider", min=0, max=marks_count - 1, step=1,
                value=(marks_count - 1) // 2,
                marks=[{"value": i} for i in range(marks_count)],
                size="sm", color=color, mb=4,
            ),
            style={"marginLeft": "22px", "marginTop": "8px"},
        ),
        html.Div(id=f"concept-{hz_key}-readout", style={"marginLeft": "22px", "fontSize": "11px", "color": "#495057"}),
    ]
    return dmc.Box(children, mb="md")


def _expert_left_rail():
    return dmc.Paper([
        dmc.Text("HAZARD LAYERS", size="xs", fw=700, c="dimmed", style={"letterSpacing": "0.06em"}, mb="sm"),
        _hazard_row("wind", "Sustained Wind", True),
        _hazard_row("gust", "Gust", False),
        _hazard_row("river", "River Flooding", False, preview=True),
        _hazard_row("rain", "Rainfall", False, preview=True),
        dmc.Text(
            "Window follows the timeline below.", size="xs", c="dimmed", fs="italic",
            style={"marginLeft": "22px", "marginTop": "-8px"},
        ),
        dmc.Divider(my="md"),
        dmc.Text("INFRASTRUCTURE", size="xs", fw=700, c="dimmed", style={"letterSpacing": "0.06em"}, mb="sm"),
        dmc.Stack([
            dmc.Group([dmc.Checkbox(label=name, size="sm"), dmc.Text("loads with layers", size="xs", c="dimmed", fs="italic")],
                      justify="space-between")
            for name in ["Schools", "Health Centers", "Shelters", "WASH Facilities"]
        ], gap=6),
    ], p="md", shadow="xs", withBorder=True)


def _expert_map_and_timeline():
    timeline_marks = [
        {"value": 0, "label": "Now"}, {"value": 1, "label": "+6h"}, {"value": 2, "label": "+24h"},
        {"value": 3, "label": "+72h"}, {"value": 4, "label": "+120h"},
    ]
    return dmc.Stack([
        dmc.Paper([
            dmc.Group([
                dmc.Text("Hazards", size="xs", fw=700, c="dimmed"),
                dmc.SegmentedControl(
                    id="concept-detail-toggle", value="tiles",
                    data=[{"value": "tiles", "label": "Tiles"}, {"value": "admin", "label": "Regions"}],
                    size="xs",
                ),
            ], justify="space-between"),
        ], p="xs", shadow="xs", withBorder=True),
        dmc.Paper([
            html.Div(
                "Map preview — not connected to live tile server",
                style={
                    "height": "420px", "borderRadius": "8px",
                    "background": "linear-gradient(135deg, #eef6fa 0%, #dcecf4 40%, #f5e9d8 70%, #f0d9c8 100%)",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                    "color": "#8a97a3", "fontSize": "13px", "fontWeight": 600, "border": "1px solid #dde6ec",
                },
            ),
        ], p="xs", shadow="xs", withBorder=True),
        dmc.Paper([
            html.Div(id="concept-timeline-caption", style={"fontSize": "13px", "fontWeight": 600, "marginBottom": "10px"}),
            dmc.Slider(
                id="concept-timeline-slider", min=0, max=4, step=1, value=0,
                marks=timeline_marks, color=PRIMARY_COLOR, mb="md",
            ),
        ], p="md", shadow="xs", withBorder=True),
    ], gap="md")


def _expert_right_rail():
    return dmc.Paper([
        dmc.Text("IMPACT SUMMARY", size="xs", fw=700, c="dimmed", style={"letterSpacing": "0.06em"}, mb="sm"),
        dmc.SimpleGrid([
            dmc.Paper([dmc.Text(k, size="xs", c="dimmed", fw=700, tt="uppercase"),
                       dmc.Text(v, size="xl", fw=700, ff="monospace")], p="sm", withBorder=True, radius="md")
            for k, v in [("People Affected", "640K"), ("Children Affected", "218K"),
                         ("Schools at Risk", "185"), ("Health Centers", "46")]
        ], cols=2, spacing="sm"),
    ], p="md", shadow="xs", withBorder=True)


def _expert_mode():
    return dmc.Grid([
        dmc.GridCol(_expert_left_rail(), span=3),
        dmc.GridCol(_expert_map_and_timeline(), span=6),
        dmc.GridCol(_expert_right_rail(), span=3),
    ], gutter="md")


# ---------------------------------------------------------------------------
# Page assembly
# ---------------------------------------------------------------------------
def _topbar():
    return dmc.Group([
        dmc.Group([
            dmc.Badge("PROTOTYPE", color="grape", variant="filled", size="sm"),
            dmc.Text("Simple / Expert mode concept — no live data connected", size="sm", fw=600, c="dimmed"),
        ], gap=10),
        dmc.Group([
            dmc.Text("Philippines · Typhoon BAVI · Jul 5, 2026 06:00", size="xs", c="dimmed"),
            dmc.SegmentedControl(
                id="concept-mode-toggle", value="simple",
                data=[{"value": "simple", "label": "Simple"}, {"value": "expert", "label": "Expert"}],
                color=PRIMARY_COLOR,
            ),
        ], gap=16),
    ], justify="space-between", wrap="wrap", p="sm", style={"borderBottom": "1px solid #dde6ec"})


def make_single_page_layout():
    return dmc.Stack([
        _topbar(),
        html.Div(_simple_mode(), id="concept-simple-wrap", style={"padding": "16px"}),
        html.Div(_expert_mode(), id="concept-expert-wrap", style={"padding": "16px", **_DISPLAY_NONE}),
    ], gap=0)


def make_single_page_appshell():
    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_header(active_tab="tab-home"), px=15, zIndex=2000),
            dmc.AppShellMain(
                make_single_page_layout(),
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "auto"},
            ),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id="concept-page-shell",
        header={"height": "67"},
        padding=0,
        footer={"height": "80"},
    )


layout = make_single_page_appshell()


# ---------------------------------------------------------------------------
# Callbacks — layout/state only, no data
# ---------------------------------------------------------------------------
@callback(
    Output("concept-simple-wrap", "style"),
    Output("concept-expert-wrap", "style"),
    Input("concept-mode-toggle", "value"),
    prevent_initial_call=True,
)
def _toggle_mode(mode):
    base = {"padding": "16px"}
    if mode == "expert":
        return {**base, **_DISPLAY_NONE}, base
    return base, {**base, **_DISPLAY_NONE}


_WIND_CATS = [
    ("Minor", "Tropical Storm", 34, 17), ("Minor", "Strong Trop. Storm", 40, 21),
    ("Moderate", "Severe Trop. Storm", 50, 26), ("Significant", "Category 1 Hurricane", 64, 33),
    ("Major", "Category 2 Hurricane", 83, 43), ("Severe", "Category 3 Hurricane", 96, 49),
    ("Extreme", "Category 4 Hurricane", 113, 58), ("Catastrophic", "Category 5 Hurricane", 137, 70),
]
_RIVER_CATS = [
    ("Happens most years", "1-in-2-year flood"), ("Happens every few years", "1-in-5-year flood"),
    ("Uncommon flood", "1-in-10-year flood"), ("Rare flood", "1-in-20-year flood"),
    ("Very rare flood", "1-in-50-year flood"), ("Once-in-a-generation flood", "1-in-100-year flood"),
]
_RAIN_MM_BY_WINDOW = {6: [25, 50, 75], 24: [35, 70, 103], 72: [45, 90, 133], 120: [50, 100, 150]}
_RAIN_TIERS = ["Moderate rain", "Heavy rain", "Extreme rain"]
_TIMELINE = [
    (None, "Now — BAVI is a Category 3 hurricane 140km east of Batangas."),
    (6, "+6h — Outer bands reach the coast, flash-flood risk elevated."),
    (24, "+24h — Landfall expected as a Category 2 storm."),
    (72, "+72h — Weakening inland, flood risk persists from saturated ground."),
    (120, "+120h — System dissipates, residual upland flood risk."),
]


@callback(Output("concept-wind-readout", "children"), Input("concept-wind-slider", "value"))
def _wind_readout(idx):
    c = _WIND_CATS[idx or 0]
    return f"{c[0]} — {c[1]} · {c[2]}kt sustained wind"


@callback(Output("concept-gust-readout", "children"), Input("concept-gust-slider", "value"))
def _gust_readout(idx):
    c = _WIND_CATS[idx or 0]
    return f"{c[0]} — {c[1]} · {c[3]} m/s gusts"


@callback(Output("concept-river-readout", "children"), Input("concept-river-slider", "value"))
def _river_readout(idx):
    c = _RIVER_CATS[idx or 0]
    return f"{c[0]} · {c[1]}"


@callback(
    Output("concept-rain-readout", "children"),
    Input("concept-rain-slider", "value"),
    Input("concept-timeline-slider", "value"),
)
def _rain_readout(tier_idx, timeline_idx):
    window_h = _TIMELINE[timeline_idx or 0][0] or 6
    tier_idx = tier_idx or 0
    mm = _RAIN_MM_BY_WINDOW[window_h][tier_idx]
    return f"{_RAIN_TIERS[tier_idx]} · ≥ {mm}mm over {window_h}h"


@callback(Output("concept-timeline-caption", "children"), Input("concept-timeline-slider", "value"))
def _timeline_caption(idx):
    return _TIMELINE[idx or 0][1]
