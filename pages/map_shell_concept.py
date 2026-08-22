"""
Map-first shell (full-bleed map, floating dismissible panels, mode-aware
Controls, two-tier timeline): the main dashboard, promoted from the
/map-shell concept exploration. Synthesizes pieces from three earlier
Artifact mockups: global_zoom_navigation.html (top bar shape, Global-mode
content: simple worldwide checkboxes + Active Storms list),
weatherlab_style_navigation.html (floating-panel treatment), and
dashboard_synthesis_v2.html (always-visible Impact Summary panel).

Now wired to a real MapLibre/Leaflet dual-layer map (same stack as
layouts/panels.py) for base-layer rendering, zoom/pan, and basemap
switching. The synthetic canvas placeholder is gone. Hazard/tile/facility
DATA layers are still separate follow-up work (see the project plan). The
old dashboard (pages/dashboard.py) is kept at /legacy as a working
reference/fallback. Deliberately skips the app's usual make_header/AppShell
chrome: this page's own full-bleed shell replaces it, not the other way
around.
"""
import hashlib
import json
import logging
import math
import os
import threading
import time
import urllib.parse
import urllib.request

import dash
import dash_mantine_components as dmc
import dash_leaflet as dl
import pandas as pd
import plotly.graph_objects as go
import requests
from dash import html, dcc, Input, Output, State, callback, clientside_callback
from dash_iconify import DashIconify
from shapely import wkt
from shapely.geometry import mapping as shapely_mapping
from urllib.parse import quote, unquote

from components.config import config
from components.map.map_config import map_config, mapbox_token
from components.map.javascript import (
    sync_maplibre_on_load, sync_maplibre_on_move,
    style_tracks, style_envelopes, tooltip_tracks, tooltip_envelopes,
    point_to_layer_schools_health, tooltip_schools, tooltip_health,
    tooltip_shelters, tooltip_wash,
)
from components.data.snowflake_utils import (
    get_active_countries, get_snowflake_data, get_active_storm_countries,
    get_latest_forecast_time_overall, get_default_forecast_cycle, get_available_wind_thresholds,
    get_country_totals, get_snowflake_connection,
    get_envelope_data_snowflake, get_gust_envelope_data_snowflake, get_storms_for_country_date,
    get_storms_and_countries_for_date, get_storms_and_countries_examined_for_date,
    get_track_impacts, get_gust_track_impacts,
    get_latest_river_forecast_time, get_latest_rain_forecast_time, ttl_cache,
    get_precip_forecast_time_near, get_river_extent_forecast_time_for_date,
    get_multi_storm_tracks, get_track_ids_for_date, get_gust_track_ids_for_date, get_tracks_for_storm,
    get_tile_impacts, get_gust_tile_impacts, get_river_tile_impacts, get_rain_tile_impacts,
    get_countries_with_river_impact_at, get_countries_with_precip_impact_at,
    get_countries_examined_for_river_at, get_countries_examined_for_precip_at,
    get_storms_with_alert_emails_at, get_alert_emails_for_storm, get_alert_email_body,
    get_storms_with_warning_emails_at, get_warning_emails_for_storm, get_warning_email_body,
    get_recent_forecast_dates, get_admin_impacts, get_facility_source,
    get_query_executor, get_member_fetch_executor, get_tile_impact_totals_by_threshold, get_data_availability,
    HAZARD_SOURCE_ECMWF, HAZARD_SOURCE_GLOFAS,
)
from components.data.data_store_utils import get_data_store, get_impact_data

logger = logging.getLogger(__name__)

dash.register_page(__name__, path="/", name="Ahead of the Storm")

# ---------------------------------------------------------------------------
# Real Snowflake-backed reference data, loaded once at module import time,
# same "parallel startup queries" pattern as pages/dashboard.py SECTION 2.
# Feeds _country_options()/_STORMS below (cross-cutting reference data, same
# for every visitor) plus the per-country helper functions further down
# (_get_country_stats/_get_country_pin_pct/_get_country_totals/
# _get_data_availability_real), which run their own per-country/per-storm
# queries reactively on country/storm selection, same module-level-vs-
# per-callback split pages/dashboard.py and callbacks/metrics.py already use.
# ---------------------------------------------------------------------------
# Uses the shared, long-lived query executor rather than a throwaway
# ThreadPoolExecutor: a throwaway pool pays a fresh connect() handshake per
# worker thread and leaks the connection attached to each thread once the
# pool exits. The shared executor's threads persist for the app's whole
# lifetime, so their threading.local() connections (get_snowflake_
# connection in snowflake_utils.py) get reused by later fan-outs too.
_f_countries = get_query_executor().submit(get_active_countries)
_f_metadata = get_query_executor().submit(get_snowflake_data)
_f_active_storm_codes = get_query_executor().submit(get_active_storm_countries)
_countries_df = _f_countries.result()
_metadata_df = _f_metadata.result()
_active_storm_country_codes = _f_active_storm_codes.result()

# Lazy-ish singleton (created once here, at import time), mirrors
# pages/dashboard.py's own module-level `data_store = get_data_store()`.
_giga_store = get_data_store()

# COUNTRY_NAME (e.g. "Jamaica"), not COUNTRY_CODE, is used as the dropdown
# VALUE below. Every existing dict-keyed-by-display-name lookup elsewhere
# on this page (country selection values were always display names, e.g.
# "Philippines") keeps working unchanged against real data.
_CODE_TO_NAME = (dict(zip(_countries_df['COUNTRY_CODE'], _countries_df['COUNTRY_NAME']))
                 if not _countries_df.empty else {})
_NAME_TO_CODE = {v: k for k, v in _CODE_TO_NAME.items()}
_ACTIVE_STORM_COUNTRY_NAMES = [_CODE_TO_NAME.get(code, code) for code in _active_storm_country_codes]

# A region row's own COUNTRY_CODE (e.g. "ECA") is a synthetic code, not a
# real ISO3 the tile server understands. Its real member codes live in
# PIPELINE_COUNTRIES.MEMBER_CODES as a JSON array string (e.g.
# '["ATG","BRB","DMA",...]'), analogous to pages/dashboard.py's own
# REGION_MEMBERS dict (which this page doesn't import/build, since its
# country picker already stores a flat list of individually-selected
# names (plain countries and regions both by NAME) rather than
# dashboard.py's single-dropdown-value-expands-to-members model).
_CODE_TO_MEMBER_CODES = (
    dict(zip(_countries_df['COUNTRY_CODE'], _countries_df.get('MEMBER_CODES', pd.Series(dtype=object))))
    if not _countries_df.empty and 'MEMBER_CODES' in _countries_df.columns else {}
)

# NAME -> (lat, lon, zoom) real per-country map view, straight from
# get_active_countries()'s own CENTER_LAT/CENTER_LON/VIEW_ZOOM columns
# (PIPELINE_COUNTRIES), same _countries_df already used for _CODE_TO_NAME/
# _NAME_TO_CODE above. Feeds the ms-main-map viewport-follows-selection
# callback further down; entries with a missing/NaN center are simply
# omitted (that country falls back to not moving the map, not a crash).
_NAME_TO_CENTER = {}
if not _countries_df.empty and {'CENTER_LAT', 'CENTER_LON'}.issubset(_countries_df.columns):
    for _, _row in _countries_df.iterrows():
        if pd.notna(_row.get('CENTER_LAT')) and pd.notna(_row.get('CENTER_LON')):
            _NAME_TO_CENTER[_row['COUNTRY_NAME']] = (
                float(_row['CENTER_LAT']), float(_row['CENTER_LON']),
                float(_row['VIEW_ZOOM']) if pd.notna(_row.get('VIEW_ZOOM')) else 6,
            )


def _resolve_tile_codes(countries):
    """Expand selected country/region display names into a flat, deduplicated
    list of real ISO3 codes for the tile server's '+'-joined country param.

    Plain countries resolve to their own COUNTRY_CODE; region rows (IS_REGION)
    expand via their own MEMBER_CODES JSON array instead of their synthetic
    COUNTRY_CODE. Selecting several countries/regions at once just unions all
    of their codes. The tile server's own `_country_in_clause` (tile_server.py)
    already treats a '+'-joined string as an arbitrary multi-country IN (...)
    list, not something with region-specific meaning of its own.
    """
    codes: list[str] = []
    for name in (countries or []):
        code = _NAME_TO_CODE.get(name)
        if not code:
            continue
        member_codes_raw = _CODE_TO_MEMBER_CODES.get(code)
        expanded = None
        if member_codes_raw and isinstance(member_codes_raw, str) and member_codes_raw.strip():
            try:
                expanded = json.loads(member_codes_raw)
            except Exception as e:
                logger.warning("Could not parse MEMBER_CODES for %s: %s", code, e)
        if expanded:
            codes.extend(c for c in expanded if c)
        else:
            codes.append(code)
    seen, out = set(), []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

# Saffir-Simpson-style wind-speed tiers (severity word, category label, kt,
# m/s), moved up here from further down in the file (where the wind-
# severity slider's own readout still uses it) so the real _STORMS-building
# code just below can also use it, at module-import time, to derive a
# storm's rough category from its highest available wind-envelope
# threshold (get_available_wind_thresholds). There's no dedicated "storm
# category" column/function anywhere in snowflake_utils.py.
_WIND_CATS = [
    ("Minor", "Tropical Storm", 34, 17), ("Minor", "Strong Trop. Storm", 40, 21),
    ("Moderate", "Severe Trop. Storm", 50, 26), ("Significant", "Category 1 Hurricane", 64, 33),
    ("Major", "Category 2 Hurricane", 83, 43), ("Severe", "Category 3 Hurricane", 96, 49),
    ("Extreme", "Category 4 Hurricane", 113, 58), ("Catastrophic", "Category 5 Hurricane", 137, 70),
]
# PREVIEW ONLY: illustrative per-tier ratio, NOT wired into the real Impact
# Summary/table numbers anywhere (see _hazard_breakdown, which still only
# reads the checkbox on/off state, not this). Index 2 (Severe Trop. Storm,
# today's default slider position) = 1.0, i.e. exactly today's baseline
# number. Every other tier is a ratio relative to that, decreasing as
# severity increases (a higher wind threshold clears a smaller area, so
# fewer people are exposed at that threshold). This is "Option A" from the
# threshold-visualization exploration, shown here as a visual preview only.
_WIND_TIER_FACTOR = [1.9, 1.45, 1.0, 0.64, 0.36, 0.21, 0.09, 0.04]


def _category_label_from_kt(max_kt):
    """Highest _WIND_CATS tier reached by `max_kt` kt (e.g. 96 -> 'Category
    3 Hurricane'), or None if `max_kt` is below every tier's threshold."""
    label = None
    for _sev, cat_label, kt, _ms in _WIND_CATS:
        if max_kt >= kt:
            label = cat_label
    return label


@ttl_cache(ttl_seconds=900, maxsize=256)
def _ensemble_max_kt(storm_name, forecast_time_str):
    """Real max wind speed (kt) across ALL ensemble members, worst-case/
    upper-bound statistic (can be driven by a single outlier member; see
    _category_label_ensemble_max's own docstring for the GENEVIEVE example
    where this differs a lot from a single-member or median reading).

    Returns None if TC_TRACKS has no rows at all for this storm/forecast.

    Cached because this is a live per-storm/forecast_time query
    called once per country from _resolve_storm_for_country, which
    _fetch_real_combined_tile_totals calls, which in turn gets invoked up to
    ~20x per single Global Hazard Contribution popup open (_resolve_stat_
    value's global branch calls _combined_stats + _combined_in_need_total
    twice, each iterating every country), the dominant cost behind popups
    taking 6-8s to open."""
    try:
        conn = get_snowflake_connection()
        df = pd.read_sql(
            "SELECT MAX(WIND_SPEED_KNOTS) AS MAX_KT FROM TC_TRACKS "
            "WHERE TRACK_ID=%s AND FORECAST_TIME=%s",
            conn, params=[storm_name, forecast_time_str],
        )
        if df.empty or pd.isna(df['MAX_KT'].iloc[0]):
            return None
        return float(df['MAX_KT'].iloc[0])
    except Exception as e:
        logger.warning("Could not load ensemble-max wind speed for %s/%s: %s", storm_name, forecast_time_str, e)
        return None


def _category_label_ensemble_max(storm_name, forecast_time_str):
    """CAT badge label derived from the worst-case max wind speed across ALL
    ensemble members (see _ensemble_max_kt): "TS" (not a granular Tropical
    Storm/Strong Trop. Storm/Severe Trop. Storm sub-label) for anything below
    Category 1's 64kt threshold, the real _category_label_from_kt tier label
    otherwise. Returns "Unknown" when TC_TRACKS has no real data at all for
    this storm/forecast.

    Note: this is a worst-case statistic, not a central estimate, e.g.
    GENEVIEVE's real ensemble max of 63.96kt (just under Cat 1) came from a
    single outlier member at one moment; the median across all 51 members
    was ~51kt (Severe Tropical Storm) and member 51 alone was 48kt. Kept as
    max (not member-51-only or median) per explicit product decision.
    """
    max_kt = _ensemble_max_kt(storm_name, forecast_time_str)
    if max_kt is None:
        return "Unknown"
    if max_kt < 64:
        return "TS"
    return _category_label_from_kt(max_kt)


def _build_real_storms(latest_time):
    """Real replacement for the old hardcoded 4-entry _STORMS mock.

    Combines three independently-real signals, none of which alone matches
    the old mock's exact (name/countries/date/cat) shape, per the task's own
    "adapt the mock shape to match reality" guidance:
      - get_snowflake_data(): distinct TRACK_ID/FORECAST_TIME combos already
        in TC_TRACKS (a storm's name + when its forecast was issued).
      - latest_time (get_latest_forecast_time_overall(), computed once at
        module level and shared with _DEFAULT_FORECAST_DATE/_RUN below, see
        there for why): the current forecast cycle, so only storms from the
        LATEST run are shown, not every historical run ever ingested.
      - get_active_storm_countries(): which countries have genuine
        (non-zero) storm impact right now.
      - _category_label_ensemble_max(): real worst-case max wind speed across
        all ensemble members from TC_TRACKS, see its own docstring for the
        "TS" sub-64kt labeling rule.

    Documented limitation (not silently papered over): none of these
    functions join a specific storm to specific countries: only "which
    countries have SOME active storm impact right now". With the common
    case of a single currently-active storm this is exact; if two+ storms
    ever shared the same latest forecast cycle, every active country would
    be attributed to every one of them here.
    """
    if _metadata_df.empty or not _ACTIVE_STORM_COUNTRY_NAMES or latest_time is None:
        return []
    latest_runs = _metadata_df[_metadata_df['FORECAST_TIME'] == latest_time]
    storms = []
    for _, row in latest_runs.iterrows():
        track_id = row['TRACK_ID']
        forecast_time = row['FORECAST_TIME']
        cat = _category_label_ensemble_max(track_id, str(forecast_time))
        try:
            date_str = pd.Timestamp(forecast_time).strftime("%a %-d %b %Y")
        except Exception:
            date_str = str(forecast_time)
        storms.append({"name": track_id, "countries": _ACTIVE_STORM_COUNTRY_NAMES, "date": date_str, "cat": cat})
    return storms

# Two color families instead of four unrelated hues, Tropical Cyclone
# (Tracks/Sustained Wind/Gust) reads as one group of orange shades, Flood
# Hazards (River/Rainfall) as one group of blue shades. Darker = the
# default-on/primary layer within each family, lighter = the secondary one.
TRACKS = "#b5480a"
WIND = "#e8590c"
GUST = "#ffa94d"
RIVER = "#1864ab"
RAIN = "#4dabf7"
# Teal, not another blue, distinct from RIVER/RAIN so Storm Surge reads as
# its own hazard within the Flood family, not a third shade of the same two.
SURGE = "#0c8599"
# Neutral slate, not a blend of WIND/RIVER (which comes out a muddy brown)
# and not PIN_COLOR's purple (already means "in need" elsewhere), reads as
# "neither hazard on its own", i.e. the Tropical Cyclone/Flood overlap.
HAZARD_BOTH_COLOR = "#6c7a89"
# Darker shade of the same slate, not an unrelated hue, reads as "the same
# 'shared risk' concept as HAZARD_BOTH_COLOR, just more of it" (all 3 of a
# family's own members at once, vs. any 2).
HAZARD_TRIPLE_COLOR = "#3d4550"
# The PATTERN itself escalates with hazard count, not just the color: a
# single diagonal hatch for "2 at once", a criss-cross (two hatch
# directions layered) for "all 3 at once", white translucent lines over
# each solid base color, not literal per-member color mixing (which pair of
# the 3 members is "double" varies, so there's no single fixed 2-color mix
# to stripe).
_DOUBLE_OVERLAP_PATTERN = (
    "repeating-linear-gradient(45deg, rgba(255,255,255,0.3) 0px, rgba(255,255,255,0.3) 2px, transparent 2px, transparent 6px), "
    f"linear-gradient({HAZARD_BOTH_COLOR}, {HAZARD_BOTH_COLOR})"
)
_TRIPLE_OVERLAP_PATTERN = (
    "repeating-linear-gradient(45deg, rgba(255,255,255,0.3) 0px, rgba(255,255,255,0.3) 2px, transparent 2px, transparent 6px), "
    "repeating-linear-gradient(135deg, rgba(255,255,255,0.3) 0px, rgba(255,255,255,0.3) 2px, transparent 2px, transparent 6px), "
    f"linear-gradient({HAZARD_TRIPLE_COLOR}, {HAZARD_TRIPLE_COLOR})"
)
# Matches the real app's own vulnerability-lens purple (assets/custom.css
# .runoff-swatch), a third, deliberately distinct color family for
# "in need" (PIN/CHIN), which isn't a hazard, isn't exposure, it's the
# vulnerability lens on top of both.
PIN_COLOR = "#7c5cbf"
PIN_COLOR_LIGHT = "#a78bda"

# Same tile_palettes.json app.py's own /map-static/tile_palettes.js route
# serves to the browser (window._AOTS_PALETTES), loaded here too so the map
# legend's raster-hazard gradient swatches (_map_legend_body below) can reuse
# the EXACT real colors MapLibre paints with, rather than a second
# hand-copied color list that could silently drift out of sync.
try:
    with open(os.path.join(os.path.dirname(__file__), "..", "components", "map", "tile_palettes.json")) as _f:
        _TILE_PALETTES_DATA = json.load(_f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    logger.error("Could not load tile_palettes.json for map legend: %s", e)
    _TILE_PALETTES_DATA = {"palettes": {}, "prop_map": {}, "e_prop_map": {}, "in_need_map": {}}
_AOTS_PALETTES = _TILE_PALETTES_DATA["palettes"]

# ---------------------------------------------------------------------------
# i18n, module-level current language, set once per request by layout()
# (a function, so Dash's page router can pass ?lang=es as a kwarg) and read
# by every _t() call made while building that same response. Internal dict
# KEYS/VALUES (_COUNTRY_STATS, _STAT_ICONS, ids, etc.) always stay English,
# only the point where a string is actually rendered gets wrapped in _t().
# Good enough for this single-user concept page; a real multi-user page
# would need request-scoped state instead of a bare module global.
# ---------------------------------------------------------------------------
_LANG = "en"

_TRANSLATIONS = {
    "es": {
        "N/A": "N/D",
        # Basemap
        "Light": "Claro", "Dark": "Oscuro", "Satellite": "Satélite", "OSM": "OSM",
        # Countries
        "Philippines": "Filipinas", "Vietnam": "Vietnam", "Mozambique": "Mozambique",
        "Pacific Islands": "Islas del Pacífico",
        "Pacific Islands (region — several small nations bundled together)":
            "Islas del Pacífico (región — varias naciones pequeñas agrupadas)",
        "Antigua and Barbuda": "Antigua y Barbuda", "Saint Lucia": "Santa Lucía",
        "Saint Vincent and the Grenadines": "San Vicente y las Granadinas",
        "ECA — East Caribbean Area (region — several small nations bundled together)":
            "ECA — Área del Caribe Oriental (región — varias naciones pequeñas agrupadas)",
        "Countries": "Países", "Regions": "Regiones",
        "Per Country": "Por País", "Combined Total": "Total Combinado", "Split": "Separado", "Total": "Total",
        "Combined — {n} countries": "Combinado — {n} países",
        # Demo scenarios
        "GENEVIEVE — Jamaica, has alert email (Jul 22, 06Z)": "GENEVIEVE — Jamaica, con correo de alerta (22 jul, 06Z)",
        "HELIO — Mozambique (Jul 6, 06Z)": "HELIO — Mozambique (6 jul, 06Z)",
        "Jamaica, no active storm (Jul 10, 06Z)": "Jamaica, sin tormenta activa (10 jul, 06Z)",
        "Global overview (Jul 5, 06Z)": "Vista global (5 jul, 06Z)",
        # Ensemble members / influencing factor
        "Probabilistic": "Probabilístico", "Ensemble Members": "Miembros del Conjunto",
        "Control (deterministic)": "Control (determinista)", "Member {n}": "Miembro {n}",
        "None": "Ninguno", "Children (total)": "Niños (total)", "Built-up Area": "Área Urbanizada",
        "Hazard Probability": "Probabilidad de Riesgo",
        "Classification": "Clasificación", "Raw": "Sin Procesar",
        "Hazard Render Mode": "Modo de Visualización de Riesgo",
        "How checked hazards render — independent of the Exposure tab's own selection.":
            "Cómo se muestran los riesgos marcados — independiente de la selección de la pestaña Exposición.",
        'Population/Children/etc. have no visible effect while Hazard Render Mode is "{mode}" — switch it back to "Probability" (top of the Hazard tab) to use these.':
            'Población/Niños/etc. no tienen ningún efecto visible mientras el Modo de Visualización de Riesgo sea "{mode}" — vuelva a "Probabilidad" (parte superior de la pestaña Riesgo) para usarlos.',
        "Other": "Otro", "Deterministic": "Determinista", "Compare Worst Case By": "Comparar Peor Caso Por",
        "Show": "Mostrar", "Combined": "Combinado", "Print": "Imprimir",
        # Stat labels
        "People": "Personas", "Children": "Niños", "Schools": "Escuelas",
        "Health Centers": "Centros de Salud", "Shelters": "Refugios", "WASH Facilities": "Instalaciones WASH",
        "At Risk": "En Riesgo", "In Need": "En Necesidad",
        "(shown as At Risk above In Need for People & Children)": "(mostrado como En Riesgo sobre En Necesidad para Personas y Niños)",
        "(estimated Tropical Cyclone only / Both / Flood only share of each value — illustrative, not real per-pixel overlap data)":
            "(participación estimada Solo Ciclón Tropical / Ambos / Solo Inundación de cada valor — ilustrativo, no son datos reales de superposición por píxel)",
        "Share": "Parte", "People at Risk": "Personas en Riesgo", "Children at Risk": "Niños en Riesgo",
        "Schools at Risk": "Escuelas en Riesgo", "Health Centers at Risk": "Centros de Salud en Riesgo",
        "Shelters at Risk": "Refugios en Riesgo", "WASH Facilities at Risk": "Instalaciones WASH en Riesgo",
        "People in Need": "Personas en Necesidad", "Children in Need": "Niños en Necesidad",
        "People At Risk": "Personas en Riesgo", "People In Need": "Personas en Necesidad",
        "Children At Risk": "Niños en Riesgo", "Children In Need": "Niños en Necesidad",
        "Population": "Población",
        # Age bands
        "Age 0–4 (Infant)": "0–4 años (Infantes)", "Age 5–14 (School-age)": "5–14 años (Edad Escolar)",
        "Age 15–19 (Adolescent)": "15–19 años (Adolescentes)",
        # Data availability
        "Pop": "Pob", "Settlement": "Asentamiento", "Mod. Poverty": "Pobreza Mod.", "Sev. Poverty": "Pobreza Sev.",
        "Select a country to see data availability.": "Selecciona un país para ver la disponibilidad de datos.",
        "{c}: no availability data.": "{c}: sin datos de disponibilidad.",
        "{schools} Schools · {health_centers} HCs · {shelters} Shelters · {wash} WASH":
            "{schools} Escuelas · {health_centers} Centros de Salud · {shelters} Refugios · {wash} WASH",
        # Controls panel sections
        "Tropical Cyclone": "Ciclón Tropical", "Flood": "Inundación",
        "At Risk by Hazard Type": "En Riesgo por Tipo de Peligro",
        "Storm Tracks": "Trayectorias de la Tormenta",
        "Sustained Wind": "Viento Sostenido", "Gust": "Ráfaga", "Flood Hazards": "Peligros de Inundación",
        "Extent": "Extensión", "Probability Raster": "Ráster de Probabilidad",
        "Mean": "Media", "Probability": "Probabilidad",
        "River Flooding": "Inundación Fluvial", "Rainfall": "Precipitación", "Proxies": "Aproximaciones",
        "Real forecasted indicators (river return-period tiers, rainfall accumulation) that contribute to flood potential — not a direct forecast of flood extent or depth.":
            "Indicadores reales pronosticados (niveles de período de retorno fluvial, acumulación de lluvia) que contribuyen al potencial de inundación, no un pronóstico directo de la extensión o profundidad de la inundación.",
        "A return period of N years means roughly a 1-in-N chance of a flood this severe occurring in any given year.":
            "Un período de retorno de N años significa aproximadamente una probabilidad de 1 en N de que ocurra una inundación de esta gravedad en un año determinado.",
        # Map legend
        "Legend": "Leyenda", "No layers active": "Ninguna capa activa", "Tracks": "Trayectorias",
        "Control member": "Miembro de control", "Ensemble member": "Miembro del conjunto",
        "Lower impact": "Menor impacto", "Higher impact": "Mayor impacto",
        "Intensity": "Intensidad", "Raw global layer": "Capa global cruda",
        "Source: {source}": "Fuente: {source}",
        "Facilities": "Instalaciones",
        "Darker red = higher hazard probability at that facility.": "Rojo más oscuro = mayor probabilidad de riesgo en esa instalación.",
        "Flat fill only — no single country to attribute per-member impact to in Global mode.":
            "Relleno plano únicamente — no hay un solo país al que atribuir el impacto por miembro en modo Global.",
        "No real data for this exact selection yet.": "Aún no hay datos reales para esta selección exacta.",
        "10% of members": "10% de los miembros", "≥80% of members": "≥80% de los miembros",
        "Coming Soon": "Próximamente",
        "No real forecast data available for this country.": "No hay datos reales de pronóstico disponibles para este país.",
        "No real forecast for this exact date/time (raw layer).": "No hay pronóstico real para esta fecha/hora exacta (capa cruda).",
        "Raw layer: no real river flood-extent forecast for {date} (any run).":
            "Capa cruda: no hay pronóstico real de inundación fluvial para {date} (ninguna corrida).",
        "Raw layer: no real precipitation forecast at {date} {run}Z.":
            "Capa cruda: no hay pronóstico real de precipitación a las {date} {run}Z.",
        "Exposure": "Exposición", "Hazard": "Peligro", "Infrastructure": "Infraestructura",
        "Context Data": "Datos de Contexto", "Settlement Classification": "Clasificación de Asentamiento",
        "Relative Wealth Index": "Índice de Riqueza Relativa",
        "Moderate Child Poverty Rate": "Tasa de Pobreza Infantil Moderada",
        "Severe Child Poverty Rate": "Tasa de Pobreza Infantil Severa",
        "Data Availability": "Disponibilidad de Datos", "View As": "Ver Como",
        "In Need is only available for Population/Children.": "En Necesidad solo está disponible para Población/Niños.",
        "In Need (Coming Soon)": "En Necesidad (Próximamente)",
        "In Need is not available yet for this view.": "En Necesidad aún no está disponible para esta vista.",
        "In Need currently only reflects Tropical Cyclone (wind) impact.": "\"En Necesidad\" actualmente solo refleja el impacto del Ciclón Tropical (viento).",
        "In Need: at risk, narrowed to the part of the population already flagged vulnerable.":
            "En Necesidad: en riesgo, limitado a la parte de la población ya marcada como vulnerable.",
        "Shown as plain locations with no hazard active; colored by impact probability once a hazard is toggled on below.":
            "Se muestran como ubicaciones simples sin ningún peligro activo; coloreadas por probabilidad de impacto al activar un peligro abajo.",
        # Top bar
        "Last Updated:": "Última Actualización:", "Global": "Global", "Country Analysis": "Análisis por País",
        "Demo Scenarios": "Escenarios Demo",
        "Storm:": "Tormenta:", "Forecast issued:": "Pronóstico emitido:",
        "Countries:": "Países:", "Report generated:": "Informe generado:",
        "Search storms available {date}…": "Buscar tormentas disponibles el {date}…",
        "Select countries…": "Seleccionar países…",
        # Active storms
        "Active Storms — {date}": "Tormentas Activas — {date}",
        "Active Storm — {country}": "Tormenta Activa — {country}",
        "Active Storms — {countries}": "Tormentas Activas — {countries}",
        "Currently tracking {names}.": "Actualmente rastreando {names}.",
        "Alert Emails": "Correos de Alerta", "Alert Emails — {storm}": "Correos de Alerta — {storm}",
        "No alert emails found for this storm at the selected date/time.": "No se encontraron correos de alerta para esta tormenta en la fecha/hora seleccionada.",
        # Impact panel
        "Impact Summary": "Resumen de Impacto",
        "Global — worldwide totals across visible hazards": "Global — totales mundiales de los peligros visibles",
        "Global — worldwide totals across all hazards": "Global — totales mundiales combinando todos los peligros",
        "Reflects only countries currently initialized in the database.":
            "Solo incluye los países actualmente inicializados en la base de datos.",
        "None of the initialized countries show impact at the currently selected hazard configuration (severity threshold/tier). This does not mean there is no real impact overall — a different threshold may show real impact, and potentially affected countries may not yet be in the database.":
            "Ningún país inicializado muestra impacto con la configuración de peligro actualmente seleccionada (umbral/nivel de gravedad). Esto no significa que no haya un impacto real en general: un umbral diferente podría mostrar impacto real, y los países potencialmente afectados pueden no estar aún en la base de datos.",
        "No impact at the currently selected hazard configuration (severity threshold/tier) for this selection. This does not mean there is no real impact overall — a different threshold may show real impact.":
            "Sin impacto con la configuración de peligro actualmente seleccionada (umbral/nivel de gravedad) para esta selección. Esto no significa que no haya un impacto real en general: un umbral diferente podría mostrar impacto real.",
        "Full Impact Breakdown": "Desglose Completo de Impacto", "Hazard Contribution": "Contribución por Peligro",
        "Alert Email": "Correo de Alerta", "Warning Email": "Correo de Advertencia", "Open in new tab ↗": "Abrir en nueva pestaña ↗",
        "Total: {value}": "Total: {value}",
        "Illustrative split — a real implementation would compute this from actual per-hazard overlap.":
            "División ilustrativa — una implementación real calcularía esto a partir del solapamiento real por peligro.",
        "By Country": "Por país",
        "Tropical Cyclone only": "Solo Ciclón Tropical", "Flood only": "Solo Inundación", "Both": "Ambos",
        "Double overlap (any 2)": "Doble Superposición (2 cualquiera)",
        "Triple overlap (all 3)": "Triple Superposición (los 3)",
        "Hazards included:": "Peligros Incluidos:",
        "Threshold sensitivity (preview) — People at Risk:": "Sensibilidad al Umbral (vista previa) — Personas en Riesgo:",
        "Rows: depth tier · Columns: accumulation window — ringed cell is current ({window}, {tier})":
            "Filas: nivel de intensidad · Columnas: ventana de acumulación — la celda marcada es la actual ({window}, {tier})",
        "None — toggle a hazard on the map to see impact numbers.":
            "Ninguno — active un peligro en el mapa para ver los números de impacto.",
        "Tropical Cyclone and Flood risk overlap — this isn't two separate groups of people.":
            "El riesgo de Ciclón Tropical e Inundación se superpone — no son dos grupos de personas separados.",
        "{country} — no active tropical cyclone": "{country} — sin ciclón tropical activo",
        "{n} countries selected: {list}": "{n} países seleccionados: {list}",
        "Metric": "Métrica", "Region": "Región",
        "Admin Level 1 Breakdown — {country}": "Desglose de Nivel Administrativo 1 — {country}",
        # Command bar
        "Hazards": "Peligros", "River": "Río", "View": "Vista", "Tiles": "Cuadrículas", "Regions": "Regiones",
        # Wind categories
        "Minor": "Menor", "Moderate": "Moderado", "Significant": "Significativo", "Major": "Mayor",
        "Severe": "Severo", "Extreme": "Extremo", "Catastrophic": "Catastrófico",
        # River Flooding classification (FloodHub-style, see _RIVER_CURVE_LABELS)
        "Warning": "Alerta", "Danger": "Peligro", "Historic": "Histórico",
        "Tropical Storm": "Tormenta Tropical", "Strong Trop. Storm": "Tormenta Trop. Fuerte",
        "Severe Trop. Storm": "Tormenta Trop. Severa", "Category 1 Hurricane": "Huracán Categoría 1",
        "Category 2 Hurricane": "Huracán Categoría 2", "Category 3 Hurricane": "Huracán Categoría 3",
        "Category 4 Hurricane": "Huracán Categoría 4", "Category 5 Hurricane": "Huracán Categoría 5",
        "sustained wind": "viento sostenido", "gusts": "ráfagas",
        # River return periods
        "1-in-2-year flood": "Inundación de 1 en 2 años", "1-in-5-year flood": "Inundación de 1 en 5 años",
        "1-in-10-year flood": "Inundación de 1 en 10 años", "1-in-20-year flood": "Inundación de 1 en 20 años",
        "1-in-50-year flood": "Inundación de 1 en 50 años", "1-in-100-year flood": "Inundación de 1 en 100 años",
        "(not natively computed — RP10 used as an upper-bound estimate)": "(no calculado de forma nativa — RP10 usado como estimación de límite superior)",
        # Rain tiers
        "Moderate rain": "Lluvia Moderada", "Heavy rain": "Lluvia Fuerte", "Extreme rain": "Lluvia Extrema",
        # Storm Surge (placeholder hazard + tiers)
        "Storm Surge": "Marea de Tormenta",
        "Minor surge (0.3–1m)": "Marea menor (0,3–1m)", "Moderate surge (1–2m)": "Marea moderada (1–2m)",
        "Major surge (2–3m)": "Marea mayor (2–3m)", "Extreme surge (>3m)": "Marea extrema (>3m)",
        # Ensemble member tooltip
        "Track, wind envelope, and precip layer show the probability-weighted view across all members.":
            "La trayectoria, el sobre de viento y la capa de precipitación muestran la vista ponderada por probabilidad de todos los miembros.",
        "the control run": "la corrida de control",
        "Track, wind envelope, and precip layer switch to {label}'s own forecast — not the Probabilistic view.":
            "La trayectoria, el sobre de viento y la capa de precipitación cambian al pronóstico propio de {label} — no la vista Probabilística.",
        # Search
        "No storms match.": "Ninguna tormenta coincide.",
        # Footer
        "Supported by": "Con el apoyo de",
        "Powered by": "Con tecnología de",
        # UN disclaimer
        ("The boundaries and names shown and the designations used on this map "
         "do not imply official endorsement or acceptance by the United Nations."):
            "Los límites y nombres mostrados y las designaciones utilizadas en este mapa "
            "no implican reconocimiento o aceptación oficial por parte de las Naciones Unidas.",
        # Experimental-tool disclaimer
        "Experimental tool. Outputs should not be used without expert review.":
            "Herramienta experimental. Los resultados no deben utilizarse sin revisión de un experto.",
        # Language switcher
        "Language": "Idioma",
        "No country impact yet": "Aún sin impacto en el país",
        "No real gust forecast data for this storm/date.":
            "No hay datos reales de pronóstico de ráfagas para esta tormenta/fecha.",
        "5 days": "5 días",
        "{tier} · ≥ {mm}mm over {hours}h": "{tier} · ≥ {mm}mm en {hours}h",
        "Cat {n}": "Cat {n}",
        "Unknown": "Desconocido",
        "{name} Severity": "Gravedad de {name}",
        "Color = that ensemble member's own population impact. Faint fill = no impact data for that member.":
            "Color = el impacto poblacional propio de ese miembro del conjunto. Relleno tenue = sin datos de impacto para ese miembro.",
        "ECMWF ensemble tropical cyclone forecast (51 members + control), ECMWF Open Data":
            "Pronóstico de ciclón tropical por conjunto de ECMWF (51 miembros + control), ECMWF Open Data",
        "ECMWF ensemble forecast, 10m wind gust (10fg field)":
            "Pronóstico por conjunto de ECMWF, ráfaga de viento a 10 m (campo 10fg)",
        "GloFAS v4.0 ensemble forecast (Copernicus CEMS) x JRC Global River Flood Hazard Maps v2.1":
            "Pronóstico por conjunto de GloFAS v4.0 (Copernicus CEMS) combinado con los Mapas Globales de "
            "Riesgo de Inundación Fluvial del JRC v2.1",
        "ECMWF ensemble forecast, total precipitation (tp field)":
            "Pronóstico por conjunto de ECMWF, precipitación total (campo tp)",
        "No real data pipeline yet — preview only": "Aún sin canal de datos real — solo vista previa",
        "UNICEF Giga school-location API": "API de ubicación de escuelas Giga de UNICEF",
        "OpenStreetMap (social_facility=shelter tag)": "OpenStreetMap (etiqueta social_facility=shelter)",
        "OpenStreetMap (humanitarian WASH tags)": "OpenStreetMap (etiquetas humanitarias WASH)",
        "WorldPop population estimates": "Estimaciones de población de WorldPop",
        "EU JRC Global Human Settlement Layer (GHS-BUILT-S)":
            "Capa Global de Asentamientos Humanos del JRC de la UE (GHS-BUILT-S)",
        "EU JRC Global Human Settlement Layer (GHS-SMOD)":
            "Capa Global de Asentamientos Humanos del JRC de la UE (GHS-SMOD)",
        "Meta/Facebook Relative Wealth Index (hosted on HDX)":
            "Índice de Riqueza Relativa de Meta/Facebook (alojado en HDX)",
        "Modeled from Meta/Facebook RWI, calibrated to real UNICEF child poverty survey rates":
            "Modelado a partir del RWI de Meta/Facebook, calibrado con tasas reales de encuestas de "
            "pobreza infantil de UNICEF",
        "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom":
            "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — personalizado",
        "Jamaica Ministry of Health facility registry — custom":
            "Registro de instalaciones del Ministerio de Salud de Jamaica — personalizado",
        "supportjamaica.gov.jm (Government of Jamaica official shelter registry) — custom":
            "supportjamaica.gov.jm (registro oficial de refugios del Gobierno de Jamaica) — personalizado",
        "National Water Commission / Government of Jamaica (NWC/GOJ ArcGIS) — custom":
            "Comisión Nacional de Aguas / Gobierno de Jamaica (NWC/GOJ ArcGIS) — personalizado",
        "In Need reflects Sustained Wind exposure only, and does not change with the wind severity threshold selected.":
            "\"En Necesidad\" refleja únicamente la exposición al Viento Sostenido, y no cambia con el "
            "umbral de severidad del viento seleccionado.",
        "In Need is only available for Population, Children (total), and the age bands.":
            "\"En Necesidad\" solo está disponible para Población, Niños (total) y los grupos de edad.",
        "Infants in Need": "Infantes en Necesidad", "School-age in Need": "Edad Escolar en Necesidad",
        "Adolescents in Need": "Adolescentes en Necesidad",
        "Custom source: {source}": "Fuente personalizada: {source}",
        "* custom data source": "* fuente de datos personalizada",
    
        "0-4": "0-4",
        "15-19": "15-19",
        "2+ hazards overlap": "Superposición de 2+ peligros",
        "24h": "24h",
        "5-14": "5-14",
        "6h": "6h",
        "72h": "72h",
        "Adolescents": "Adolescentes",
        "Anguilla": "Anguila",
        "Aruba": "Aruba",
        "BAVI period — Global flood hazards preview (2 Jul 2026, 06Z)": "Período BAVI — Vista previa de peligros de inundación global (2 Jul 2026, 06Z)",
        "Bahamas": "Bahamas",
        "Bangladesh": "Bangladés",
        "Barbados": "Barbados",
        "Belize": "Belice",
        "British Virgin Islands": "Islas Vírgenes Británicas",
        "Child Climate Index": "Índice Climático Infantil",
        "Combined Hazard Probability": "Probabilidad de Riesgo Combinada",
        "Combined Probability": "Probabilidad Combinada",
        "Combined across {n} active hazards — see map tooltip.": "Combinado en {n} peligros activos — consulte la información emergente del mapa.",
        "Costa Rica": "Costa Rica",
        "Cuba": "Cuba",
        "Curaçao": "Curazao",
        "Dominica": "Dominica",
        "Dominican Republic": "República Dominicana",
        "East Caribbean Area": "Área del Caribe Oriental",
        "El Salvador": "El Salvador",
        "Expected Adolescent Impact": "Impacto Esperado en Adolescentes",
        "Expected Built-up Impact": "Impacto Esperado en Área Urbanizada",
        "Expected Child Climate Index Impact": "Impacto Esperado en el Índice Climático Infantil",
        "Expected Children Impact": "Impacto Esperado en Niños",
        "Expected Infant Impact": "Impacto Esperado en Infantes",
        "Expected Population Impact": "Impacto Esperado en la Población",
        "Expected School-age Impact": "Impacto Esperado en Edad Escolar",
        "Fewest members agree": "Menos miembros coinciden",
        "Flooded under this member": "Inundado según este miembro",
        "Grenada": "Granada",
        "Guatemala": "Guatemala",
        "HCs": "CS",
        "Haiti": "Haití",
        "Hazard Classification": "Clasificación de Riesgo",
        "Honduras": "Honduras",
        "In Need is unavailable — no real Sustained Wind data for the current selection.": "En Necesidad no está disponible — no hay datos reales de Viento Sostenido para la selección actual.",
        "Infants": "Infantes",
        "Jamaica": "Jamaica",
        "MELISSA — Jamaica (28 Oct 2025, 00Z)": "MELISSA — Jamaica (28 Oct 2025, 00Z)",
        "Mapbox Light": "Mapbox Light",
        "Mexico": "México",
        "Moderate Poverty Probability": "Probabilidad de Pobreza Moderada",
        "Montserrat": "Montserrat",
        "Most members agree": "Más miembros coinciden",
        "Nicaragua": "Nicaragua",
        "No real admin-level breakdown available for this storm/threshold selection.": "No hay un desglose real a nivel administrativo disponible para esta selección de tormenta/umbral.",
        "RWI": "RWI",
        "Real ensemble-member fraction across {n} active hazards — see map tooltip.": "Fracción real de miembros del conjunto en {n} peligros activos — consulte la información emergente del mapa.",
        "Real per-member Tropical Cyclone only / Both / Flood only split of each value (falls back to an illustrative estimate only when no real split data resolves).": "División real por miembro de cada valor en Solo Ciclón Tropical / Ambos / Solo Inundación (recurre a una estimación ilustrativa solo cuando no se resuelven datos reales de división).",
        "Real per-member figure — \"Both\" counts only ensemble members genuinely hit by both Tropical Cyclone and Flood hazards at the same tile, not an estimate.": "Cifra real por miembro — \"Ambos\" cuenta solo los miembros del conjunto realmente afectados por los peligros de Ciclón Tropical e Inundación en la misma celda, no una estimación.",
        "Real per-member union, not a sum: checks each of the 51 real ensemble members and counts it once if ANY active hazard reaches this tile, then divides by 51. Example: River alone hits 10 members (~20%), Rain alone hits 10 members (~20%), 5 of them the same members. Combined = 15 unique members ÷ 51 ≈ 29% — not 20%+20%=40%, and not just the larger single number.": "Unión real por miembro, no una suma: comprueba cada uno de los 51 miembros reales del conjunto y lo cuenta una vez si CUALQUIER peligro activo alcanza esta celda, luego divide entre 51. Ejemplo: Fluvial por sí solo afecta a 10 miembros (~20%), Precipitación por sí sola afecta a 10 miembros (~20%), 5 de ellos son los mismos miembros. Combinado = 15 miembros únicos ÷ 51 ≈ 29% — no 20%+20%=40%, y no solo el número individual más grande.",
        "Saint Kitts and Nevis": "San Cristóbal y Nieves",
        "School-age": "Edad Escolar",
        "Settlement Type": "Tipo de Asentamiento",
        "Severe Poverty Probability": "Probabilidad de Pobreza Severa",
        "Trinidad and Tobago": "Trinidad y Tobago",
        "Turks and Caicos Islands": "Islas Turcas y Caicos",
        "WASH": "WASH",
        "Wind": "Viento",
        "{d}d": "{d}d",
    
        "No real data for this hazard/selection.": "No hay datos reales para este peligro/selección.",
        "No real per-threshold rainfall data for this metric.": "No hay datos reales de precipitación por umbral para esta métrica.",
        "No real split data for this cell.": "No hay datos reales de división para esta celda.",
        "Real per-hazard split unavailable for this selection — Tropical Cyclone and Flood contributions cannot be separated right now.": "División real por peligro no disponible para esta selección — las contribuciones de Ciclón Tropical e Inundación no se pueden separar en este momento.",
        "Real per-member Tropical Cyclone only / Both / Flood only split of each value.": "División real por miembro de cada valor en Solo Ciclón Tropical / Ambos / Solo Inundación.",
    },
    "fr": {
        "N/A": "N/D",
        "Light": "Clair", "Dark": "Sombre", "Satellite": "Satellite", "OSM": "OSM",
        "Philippines": "Philippines", "Vietnam": "Vietnam", "Mozambique": "Mozambique",
        "Pacific Islands": "Îles du Pacifique",
        "Pacific Islands (region — several small nations bundled together)":
            "Îles du Pacifique (région — plusieurs petites nations regroupées)",
        "Antigua and Barbuda": "Antigua-et-Barbuda", "Saint Lucia": "Sainte-Lucie",
        "Saint Vincent and the Grenadines": "Saint-Vincent-et-les-Grenadines",
        "ECA — East Caribbean Area (region — several small nations bundled together)":
            "ECA — Zone des Caraïbes orientales (région — plusieurs petites nations regroupées)",
        "Countries": "Pays", "Regions": "Régions",
        "Per Country": "Par Pays", "Combined Total": "Total Combiné", "Split": "Séparé", "Total": "Total",
        "Combined — {n} countries": "Combiné — {n} pays",
        "GENEVIEVE — Jamaica, has alert email (Jul 22, 06Z)": "GENEVIEVE — Jamaïque, avec e-mail d'alerte (22 juil., 06Z)",
        "HELIO — Mozambique (Jul 6, 06Z)": "HELIO — Mozambique (6 juil., 06Z)",
        "Jamaica, no active storm (Jul 10, 06Z)": "Jamaïque, aucune tempête active (10 juil., 06Z)",
        "Global overview (Jul 5, 06Z)": "Vue d'ensemble mondiale (5 juil., 06Z)",
        "Probabilistic": "Probabiliste", "Ensemble Members": "Membres de l'ensemble",
        "Control (deterministic)": "Contrôle (déterministe)", "Member {n}": "Membre {n}",
        "None": "Aucun", "Children (total)": "Enfants (total)", "Built-up Area": "Zone bâtie",
        "Hazard Probability": "Probabilité de risque",
        "Classification": "Classification", "Raw": "Brut",
        "Hazard Render Mode": "Mode d'affichage des aléas",
        "How checked hazards render — independent of the Exposure tab's own selection.":
            "Comment les aléas cochés s'affichent — indépendant de la sélection de l'onglet Exposition.",
        'Population/Children/etc. have no visible effect while Hazard Render Mode is "{mode}" — switch it back to "Probability" (top of the Hazard tab) to use these.':
            'Population/Enfants/etc. n\'ont aucun effet visible tant que le Mode d\'affichage des aléas est "{mode}" — repassez sur "Probabilité" (haut de l\'onglet Aléas) pour les utiliser.',
        "Other": "Autre", "Deterministic": "Déterministe", "Compare Worst Case By": "Comparer le pire scénario par",
        "Show": "Afficher", "Combined": "Combiné",
        "People": "Personnes", "Children": "Enfants", "Schools": "Écoles",
        "Health Centers": "Centres de santé", "Shelters": "Abris", "WASH Facilities": "Installations EAH",
        "At Risk": "À risque", "In Need": "Dans le besoin",
        "(shown as At Risk above In Need for People & Children)": "(affiché comme À risque au-dessus de Dans le besoin pour Personnes et Enfants)",
        "(estimated Tropical Cyclone only / Both / Flood only share of each value — illustrative, not real per-pixel overlap data)":
            "(part estimée Cyclone tropical seulement / Les deux / Inondation seulement de chaque valeur — illustratif, pas des données réelles de chevauchement par pixel)",
        "Share": "Part", "People at Risk": "Personnes à risque", "Children at Risk": "Enfants à risque",
        "Schools at Risk": "Écoles à risque", "Health Centers at Risk": "Centres de santé à risque",
        "Shelters at Risk": "Abris à risque", "WASH Facilities at Risk": "Installations EAH à risque",
        "People in Need": "Personnes dans le besoin", "Children in Need": "Enfants dans le besoin",
        "People At Risk": "Personnes à risque", "People In Need": "Personnes dans le besoin",
        "Children At Risk": "Enfants à risque", "Children In Need": "Enfants dans le besoin",
        "Population": "Population",
        "Age 0–4 (Infant)": "0–4 ans (Nourrissons)", "Age 5–14 (School-age)": "5–14 ans (Âge scolaire)",
        "Age 15–19 (Adolescent)": "15–19 ans (Adolescents)",
        "Pop": "Pop", "Settlement": "Peuplement", "Mod. Poverty": "Pauvreté mod.", "Sev. Poverty": "Pauvreté sév.",
        "Select a country to see data availability.": "Sélectionnez un pays pour voir la disponibilité des données.",
        "{c}: no availability data.": "{c} : aucune donnée de disponibilité.",
        "{schools} Schools · {health_centers} HCs · {shelters} Shelters · {wash} WASH":
            "{schools} écoles · {health_centers} centres de santé · {shelters} abris · {wash} EAH",
        "Tropical Cyclone": "Cyclone tropical", "Flood": "Inondation",
        "At Risk by Hazard Type": "À risque par type de risque",
        "Storm Tracks": "Trajectoires de la tempête",
        "Sustained Wind": "Vent soutenu", "Gust": "Rafale", "Flood Hazards": "Risques d'inondation",
        "Extent": "Étendue", "Probability Raster": "Raster de probabilité",
        "Mean": "Moyenne", "Probability": "Probabilité",
        "River Flooding": "Inondation fluviale", "Rainfall": "Précipitations", "Proxies": "Approximations",
        "Real forecasted indicators (river return-period tiers, rainfall accumulation) that contribute to flood potential — not a direct forecast of flood extent or depth.":
            "Indicateurs réels prévus (niveaux de période de retour fluviale, accumulation de précipitations) qui contribuent au potentiel d'inondation — pas une prévision directe de l'étendue ou de la profondeur de l'inondation.",
        "A return period of N years means roughly a 1-in-N chance of a flood this severe occurring in any given year.":
            "Une période de retour de N ans signifie environ 1 chance sur N qu'une inondation de cette gravité se produise au cours d'une année donnée.",
        # Map legend
        "Legend": "Légende", "No layers active": "Aucune couche active", "Tracks": "Trajectoires",
        "Control member": "Membre de contrôle", "Ensemble member": "Membre de l'ensemble",
        "Lower impact": "Impact plus faible", "Higher impact": "Impact plus élevé",
        "Intensity": "Intensité", "Raw global layer": "Couche brute globale",
        "Source: {source}": "Source : {source}",
        "Facilities": "Installations",
        "Darker red = higher hazard probability at that facility.": "Rouge plus foncé = probabilité de risque plus élevée pour cette installation.",
        "Flat fill only — no single country to attribute per-member impact to in Global mode.":
            "Remplissage uni uniquement — aucun pays unique auquel attribuer l'impact par membre en mode Global.",
        "No real data for this exact selection yet.": "Aucune donnée réelle pour cette sélection exacte pour le moment.",
        "10% of members": "10 % des membres", "≥80% of members": "≥80 % des membres",
        "Coming Soon": "Bientôt disponible",
        "No real forecast data available for this country.": "Aucune donnée de prévision réelle disponible pour ce pays.",
        "No real forecast for this exact date/time (raw layer).": "Aucune prévision réelle pour cette date/heure exacte (couche brute).",
        "Raw layer: no real river flood-extent forecast for {date} (any run).":
            "Couche brute : aucune prévision réelle d'inondation fluviale pour {date} (aucune exécution).",
        "Raw layer: no real precipitation forecast at {date} {run}Z.":
            "Couche brute : aucune prévision réelle de précipitations à {date} {run}Z.",
        "Exposure": "Exposition", "Hazard": "Risque", "Infrastructure": "Infrastructure",
        "Context Data": "Données contextuelles", "Settlement Classification": "Classification du peuplement",
        "Relative Wealth Index": "Indice de richesse relative",
        "Moderate Child Poverty Rate": "Taux de pauvreté infantile modérée",
        "Severe Child Poverty Rate": "Taux de pauvreté infantile sévère",
        "Data Availability": "Disponibilité des données", "View As": "Afficher comme",
        "In Need is only available for Population/Children.": "Dans le besoin n'est disponible que pour Population/Enfants.",
        "In Need (Coming Soon)": "Dans le besoin (Bientôt disponible)",
        "In Need is not available yet for this view.": "Dans le besoin n'est pas encore disponible pour cette vue.",
        "In Need currently only reflects Tropical Cyclone (wind) impact.": "« Dans le besoin » ne reflète actuellement que l'impact du cyclone tropical (vent).",
        "In Need: at risk, narrowed to the part of the population already flagged vulnerable.":
            "Dans le besoin : à risque, limité à la partie de la population déjà signalée comme vulnérable.",
        "Shown as plain locations with no hazard active; colored by impact probability once a hazard is toggled on below.":
            "Affichés comme simples emplacements sans risque actif ; colorés selon la probabilité d'impact une fois un risque activé ci-dessous.",
        "Last Updated:": "Dernière mise à jour :", "Global": "Mondial", "Country Analysis": "Analyse par pays",
        "Demo Scenarios": "Scénarios de démonstration",
        "Storm:": "Tempête :", "Forecast issued:": "Prévision émise :",
        "Countries:": "Pays :", "Report generated:": "Rapport généré :",
        "Search storms available {date}…": "Rechercher les tempêtes disponibles le {date}…",
        "Select countries…": "Sélectionner des pays…",
        "Active Storms — {date}": "Tempêtes actives — {date}",
        "Active Storm — {country}": "Tempête active — {country}",
        "Active Storms — {countries}": "Tempêtes actives — {countries}",
        "Currently tracking {names}.": "Suivi actuel : {names}.",
        "Alert Emails": "E-mails d'alerte", "Alert Emails — {storm}": "E-mails d'alerte — {storm}",
        "No alert emails found for this storm at the selected date/time.": "Aucun e-mail d'alerte trouvé pour cette tempête à la date/heure sélectionnée.",
        "Impact Summary": "Résumé de l'impact",
        "Global — worldwide totals across visible hazards": "Mondial — totaux mondiaux des risques visibles",
        "Global — worldwide totals across all hazards": "Mondial — totaux mondiaux combinant tous les risques",
        "Reflects only countries currently initialized in the database.":
            "Ne reflète que les pays actuellement initialisés dans la base de données.",
        "None of the initialized countries show impact at the currently selected hazard configuration (severity threshold/tier). This does not mean there is no real impact overall — a different threshold may show real impact, and potentially affected countries may not yet be in the database.":
            "Aucun pays initialisé ne montre d'impact avec la configuration de danger actuellement sélectionnée (seuil/niveau de gravité). Cela ne signifie pas qu'il n'y a pas d'impact réel en général : un seuil différent pourrait montrer un impact réel, et des pays potentiellement touchés peuvent ne pas encore figurer dans la base de données.",
        "No impact at the currently selected hazard configuration (severity threshold/tier) for this selection. This does not mean there is no real impact overall — a different threshold may show real impact.":
            "Aucun impact avec la configuration de danger actuellement sélectionnée (seuil/niveau de gravité) pour cette sélection. Cela ne signifie pas qu'il n'y a pas d'impact réel en général : un seuil différent pourrait montrer un impact réel.",
        "Full Impact Breakdown": "Répartition complète de l'impact", "Hazard Contribution": "Contribution par risque",
        "Alert Email": "E-mail d'alerte", "Warning Email": "E-mail d'avertissement", "Open in new tab ↗": "Ouvrir dans un nouvel onglet ↗",
        "Total: {value}": "Total : {value}",
        "Illustrative split — a real implementation would compute this from actual per-hazard overlap.":
            "Répartition illustrative — une implémentation réelle calculerait ceci à partir du chevauchement réel par risque.",
        "By Country": "Par pays",
        "Tropical Cyclone only": "Cyclone tropical seulement", "Flood only": "Inondation seulement", "Both": "Les deux",
        "Double overlap (any 2)": "Double Chevauchement (2 quelconques)",
        "Triple overlap (all 3)": "Triple Chevauchement (les 3)",
        "Hazards included:": "Risques Inclus :",
        "Threshold sensitivity (preview) — People at Risk:": "Sensibilité au Seuil (aperçu) — Personnes à Risque :",
        "Rows: depth tier · Columns: accumulation window — ringed cell is current ({window}, {tier})":
            "Lignes : niveau d'intensité · Colonnes : fenêtre d'accumulation — la cellule entourée est actuelle ({window}, {tier})",
        "None — toggle a hazard on the map to see impact numbers.":
            "Aucun — activez un risque sur la carte pour voir les chiffres d'impact.",
        "Tropical Cyclone and Flood risk overlap — this isn't two separate groups of people.":
            "Les risques de cyclone tropical et d'inondation se chevauchent — ce ne sont pas deux groupes de personnes distincts.",
        "{country} — no active tropical cyclone": "{country} — aucun cyclone tropical actif",
        "{n} countries selected: {list}": "{n} pays sélectionnés : {list}",
        "Metric": "Indicateur", "Region": "Région",
        "Admin Level 1 Breakdown — {country}": "Répartition de niveau administratif 1 — {country}",
        "Hazards": "Risques", "River": "Rivière", "View": "Vue", "Tiles": "Tuiles", "Regions": "Régions",
        "Minor": "Mineur", "Moderate": "Modéré", "Significant": "Significatif", "Major": "Majeur",
        "Severe": "Sévère", "Extreme": "Extrême", "Catastrophic": "Catastrophique",
        "Warning": "Alerte", "Danger": "Danger", "Historic": "Historique",
        "Tropical Storm": "Tempête tropicale", "Strong Trop. Storm": "Tempête trop. forte",
        "Severe Trop. Storm": "Tempête trop. sévère", "Category 1 Hurricane": "Ouragan de catégorie 1",
        "Category 2 Hurricane": "Ouragan de catégorie 2", "Category 3 Hurricane": "Ouragan de catégorie 3",
        "Category 4 Hurricane": "Ouragan de catégorie 4", "Category 5 Hurricane": "Ouragan de catégorie 5",
        "sustained wind": "vent soutenu", "gusts": "rafales",
        "1-in-2-year flood": "Crue de type 1 sur 2 ans", "1-in-5-year flood": "Crue de type 1 sur 5 ans",
        "1-in-10-year flood": "Crue de type 1 sur 10 ans", "1-in-20-year flood": "Crue de type 1 sur 20 ans",
        "1-in-50-year flood": "Crue de type 1 sur 50 ans", "1-in-100-year flood": "Crue de type 1 sur 100 ans",
        "(not natively computed — RP10 used as an upper-bound estimate)": "(non calculé nativement — RP10 utilisé comme estimation de limite supérieure)",
        "Moderate rain": "Pluie modérée", "Heavy rain": "Pluie forte", "Extreme rain": "Pluie extrême",
        "Storm Surge": "Onde de Tempête",
        "Minor surge (0.3–1m)": "Onde mineure (0,3–1m)", "Moderate surge (1–2m)": "Onde modérée (1–2m)",
        "Major surge (2–3m)": "Onde majeure (2–3m)", "Extreme surge (>3m)": "Onde extrême (>3m)",
        "Track, wind envelope, and precip layer show the probability-weighted view across all members.":
            "La trajectoire, l'enveloppe de vent et la couche de précipitations montrent la vue pondérée par probabilité de tous les membres.",
        "the control run": "la simulation de contrôle",
        "Track, wind envelope, and precip layer switch to {label}'s own forecast — not the Probabilistic view.":
            "La trajectoire, l'enveloppe de vent et la couche de précipitations basculent vers la prévision propre de {label} — pas la vue probabiliste.",
        "No storms match.": "Aucune tempête ne correspond.",
        "Supported by": "Avec le soutien de",
        "Powered by": "Propulsé par",
        ("The boundaries and names shown and the designations used on this map "
         "do not imply official endorsement or acceptance by the United Nations."):
            "Les frontières et noms indiqués ainsi que les désignations utilisées sur cette carte "
            "n'impliquent pas de reconnaissance ou d'acceptation officielle par les Nations Unies.",
        "Experimental tool. Outputs should not be used without expert review.":
            "Outil expérimental. Les résultats ne doivent pas être utilisés sans l'examen d'un expert.",
        "Language": "Langue",
        "No country impact yet": "Aucun impact national pour l'instant",
        "No real gust forecast data for this storm/date.":
            "Aucune donnée réelle de prévision de rafales pour cette tempête/date.",
        "5 days": "5 jours",
        "{tier} · ≥ {mm}mm over {hours}h": "{tier} · ≥ {mm}mm sur {hours}h",
        "Cat {n}": "Cat {n}",
        "Unknown": "Inconnu",
        "{name} Severity": "Gravité de {name}",
        "Color = that ensemble member's own population impact. Faint fill = no impact data for that member.":
            "Couleur = impact démographique propre à ce membre de l'ensemble. Remplissage pâle = "
            "aucune donnée d'impact pour ce membre.",
        "ECMWF ensemble tropical cyclone forecast (51 members + control), ECMWF Open Data":
            "Prévision d'ensemble de cyclone tropical ECMWF (51 membres + membre de contrôle), ECMWF Open Data",
        "ECMWF ensemble forecast, 10m wind gust (10fg field)":
            "Prévision d'ensemble ECMWF, rafale de vent à 10 m (champ 10fg)",
        "GloFAS v4.0 ensemble forecast (Copernicus CEMS) x JRC Global River Flood Hazard Maps v2.1":
            "Prévision d'ensemble GloFAS v4.0 (Copernicus CEMS) combinée aux cartes mondiales de risque "
            "d'inondation fluviale du JRC v2.1",
        "ECMWF ensemble forecast, total precipitation (tp field)":
            "Prévision d'ensemble ECMWF, précipitations totales (champ tp)",
        "No real data pipeline yet — preview only": "Aucun pipeline de données réel pour l'instant — aperçu uniquement",
        "UNICEF Giga school-location API": "API de localisation des écoles Giga de l'UNICEF",
        "OpenStreetMap (social_facility=shelter tag)": "OpenStreetMap (balise social_facility=shelter)",
        "OpenStreetMap (humanitarian WASH tags)": "OpenStreetMap (balises humanitaires WASH)",
        "WorldPop population estimates": "Estimations de population WorldPop",
        "EU JRC Global Human Settlement Layer (GHS-BUILT-S)":
            "Couche mondiale d'occupation humaine du JRC de l'UE (GHS-BUILT-S)",
        "EU JRC Global Human Settlement Layer (GHS-SMOD)":
            "Couche mondiale d'occupation humaine du JRC de l'UE (GHS-SMOD)",
        "Meta/Facebook Relative Wealth Index (hosted on HDX)":
            "Indice de richesse relative Meta/Facebook (hébergé sur HDX)",
        "Modeled from Meta/Facebook RWI, calibrated to real UNICEF child poverty survey rates":
            "Modélisé à partir du RWI de Meta/Facebook, calibré sur les taux réels d'enquêtes UNICEF "
            "sur la pauvreté infantile",
        "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom":
            "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — personnalisé",
        "Jamaica Ministry of Health facility registry — custom":
            "Registre des établissements du ministère de la Santé de la Jamaïque — personnalisé",
        "supportjamaica.gov.jm (Government of Jamaica official shelter registry) — custom":
            "supportjamaica.gov.jm (registre officiel des abris du gouvernement jamaïcain) — personnalisé",
        "National Water Commission / Government of Jamaica (NWC/GOJ ArcGIS) — custom":
            "Commission nationale de l'eau / gouvernement de la Jamaïque (NWC/GOJ ArcGIS) — personnalisé",
        "In Need reflects Sustained Wind exposure only, and does not change with the wind severity threshold selected.":
            "« Dans le besoin » ne reflète que l'exposition au Vent Soutenu, et ne change pas avec le "
            "seuil de gravité du vent sélectionné.",
        "In Need is only available for Population, Children (total), and the age bands.":
            "« Dans le besoin » n'est disponible que pour Population, Enfants (total) et les tranches d'âge.",
        "Infants in Need": "Nourrissons dans le besoin", "School-age in Need": "Âge scolaire dans le besoin",
        "Adolescents in Need": "Adolescents dans le besoin",
        "Custom source: {source}": "Source personnalisée : {source}",
        "* custom data source": "* source de données personnalisée",
    
        "0-4": "0-4",
        "15-19": "15-19",
        "2+ hazards overlap": "2+ risques se chevauchent",
        "24h": "24h",
        "5-14": "5-14",
        "6h": "6h",
        "72h": "72h",
        "Adolescents": "Adolescents",
        "Anguilla": "Anguilla",
        "Aruba": "Aruba",
        "BAVI period — Global flood hazards preview (2 Jul 2026, 06Z)": "Période BAVI — Aperçu des risques d'inondation mondiaux (2 juil. 2026, 06Z)",
        "Bahamas": "Bahamas",
        "Bangladesh": "Bangladesh",
        "Barbados": "Barbade",
        "Belize": "Belize",
        "British Virgin Islands": "Îles Vierges britanniques",
        "Child Climate Index": "Indice climatique infantile",
        "Combined Hazard Probability": "Probabilité de risque combinée",
        "Combined Probability": "Probabilité combinée",
        "Combined across {n} active hazards — see map tooltip.": "Combiné sur {n} risques actifs — voir l'infobulle de la carte.",
        "Costa Rica": "Costa Rica",
        "Cuba": "Cuba",
        "Curaçao": "Curaçao",
        "Dominica": "Dominique",
        "Dominican Republic": "République dominicaine",
        "East Caribbean Area": "Région des Caraïbes orientales",
        "El Salvador": "Salvador",
        "Expected Adolescent Impact": "Impact attendu sur les adolescents",
        "Expected Built-up Impact": "Impact attendu sur les zones bâties",
        "Expected Child Climate Index Impact": "Impact attendu sur l'indice climatique infantile",
        "Expected Children Impact": "Impact attendu sur les enfants",
        "Expected Infant Impact": "Impact attendu sur les nourrissons",
        "Expected Population Impact": "Impact attendu sur la population",
        "Expected School-age Impact": "Impact attendu sur les enfants d'âge scolaire",
        "Fewest members agree": "Accord le plus faible entre les membres",
        "Flooded under this member": "Inondé selon ce membre",
        "Grenada": "Grenade",
        "Guatemala": "Guatemala",
        "HCs": "CS",
        "Haiti": "Haïti",
        "Hazard Classification": "Classification de risque",
        "Honduras": "Honduras",
        "In Need is unavailable — no real Sustained Wind data for the current selection.": "Dans le besoin n'est pas disponible — aucune donnée réelle de Vent soutenu pour la sélection actuelle.",
        "Infants": "Nourrissons",
        "Jamaica": "Jamaïque",
        "MELISSA — Jamaica (28 Oct 2025, 00Z)": "MELISSA — Jamaïque (28 oct. 2025, 00Z)",
        "Mapbox Light": "Mapbox Light",
        "Mexico": "Mexique",
        "Moderate Poverty Probability": "Probabilité de pauvreté modérée",
        "Montserrat": "Montserrat",
        "Most members agree": "Accord le plus fort entre les membres",
        "Nicaragua": "Nicaragua",
        "No real admin-level breakdown available for this storm/threshold selection.": "Aucune ventilation réelle au niveau administratif disponible pour cette sélection de tempête/seuil.",
        "RWI": "RWI",
        "Real ensemble-member fraction across {n} active hazards — see map tooltip.": "Fraction réelle de membres de l'ensemble sur {n} risques actifs — voir l'infobulle de la carte.",
        "Real per-member Tropical Cyclone only / Both / Flood only split of each value (falls back to an illustrative estimate only when no real split data resolves).": "Répartition réelle par membre entre Cyclone tropical seulement / Les deux / Inondation seulement pour chaque valeur (revient à une estimation illustrative uniquement lorsqu'aucune donnée de répartition réelle n'est disponible).",
        "Real per-member figure — \"Both\" counts only ensemble members genuinely hit by both Tropical Cyclone and Flood hazards at the same tile, not an estimate.": "Chiffre réel par membre — « Les deux » ne compte que les membres de l'ensemble réellement touchés à la fois par les risques Cyclone tropical et Inondation sur la même tuile, et non une estimation.",
        "Real per-member union, not a sum: checks each of the 51 real ensemble members and counts it once if ANY active hazard reaches this tile, then divides by 51. Example: River alone hits 10 members (~20%), Rain alone hits 10 members (~20%), 5 of them the same members. Combined = 15 unique members ÷ 51 ≈ 29% — not 20%+20%=40%, and not just the larger single number.": "Union réelle par membre, et non une somme : vérifie chacun des 51 membres réels de l'ensemble et le compte une seule fois si N'IMPORTE QUEL risque actif atteint cette tuile, puis divise par 51. Exemple : la rivière seule touche 10 membres (~20%), la pluie seule touche 10 membres (~20%), dont 5 membres communs. Combiné = 15 membres uniques ÷ 51 ≈ 29% — pas 20%+20%=40%, et pas simplement le chiffre le plus élevé.",
        "Saint Kitts and Nevis": "Saint-Kitts-et-Nevis",
        "School-age": "Âge scolaire",
        "Settlement Type": "Type de peuplement",
        "Severe Poverty Probability": "Probabilité de pauvreté sévère",
        "Trinidad and Tobago": "Trinité-et-Tobago",
        "Turks and Caicos Islands": "Îles Turques-et-Caïques",
        "WASH": "EAH",
        "Wind": "Vent",
        "{d}d": "{d}d",
    
        "No real data for this hazard/selection.": "Aucune donnée réelle pour ce risque/cette sélection.",
        "No real per-threshold rainfall data for this metric.": "Aucune donnée réelle de précipitations par seuil pour cet indicateur.",
        "No real split data for this cell.": "Aucune donnée réelle de répartition pour cette cellule.",
        "Real per-hazard split unavailable for this selection — Tropical Cyclone and Flood contributions cannot be separated right now.": "Répartition réelle par risque indisponible pour cette sélection — les contributions du Cyclone tropical et de l'Inondation ne peuvent pas être séparées pour le moment.",
        "Real per-member Tropical Cyclone only / Both / Flood only split of each value.": "Répartition réelle par membre entre Cyclone tropical seulement / Les deux / Inondation seulement pour chaque valeur.",
    },
    "bn": {
        "N/A": "প্রযোজ্য নয়",
        "Light": "হালকা", "Dark": "গাঢ়", "Satellite": "স্যাটেলাইট", "OSM": "OSM",
        "Philippines": "ফিলিপাইন", "Vietnam": "ভিয়েতনাম", "Mozambique": "মোজাম্বিক",
        "Pacific Islands": "প্রশান্ত মহাসাগরীয় দ্বীপপুঞ্জ",
        "Pacific Islands (region — several small nations bundled together)":
            "প্রশান্ত মহাসাগরীয় দ্বীপপুঞ্জ (অঞ্চল — বেশ কয়েকটি ছোট দেশ একত্রিত)",
        "Antigua and Barbuda": "অ্যান্টিগুয়া ও বার্বুডা", "Saint Lucia": "সেন্ট লুসিয়া",
        "Saint Vincent and the Grenadines": "সেন্ট ভিনসেন্ট ও গ্রেনাডাইন্স",
        "ECA — East Caribbean Area (region — several small nations bundled together)":
            "ECA — পূর্ব ক্যারিবিয়ান অঞ্চল (অঞ্চল — বেশ কয়েকটি ছোট দেশ একত্রিত)",
        "Countries": "দেশসমূহ", "Regions": "অঞ্চলসমূহ",
        "Per Country": "প্রতি দেশ", "Combined Total": "সম্মিলিত মোট", "Split": "পৃথক", "Total": "মোট",
        "Combined — {n} countries": "সম্মিলিত — {n}টি দেশ",
        "GENEVIEVE — Jamaica, has alert email (Jul 22, 06Z)": "GENEVIEVE — জ্যামাইকা, সতর্কতা ইমেইল আছে (২২ জুলাই, ০৬Z)",
        "HELIO — Mozambique (Jul 6, 06Z)": "HELIO — মোজাম্বিক (৬ জুলাই, ০৬Z)",
        "Jamaica, no active storm (Jul 10, 06Z)": "জ্যামাইকা, কোনো সক্রিয় ঝড় নেই (১০ জুলাই, ০৬Z)",
        "Global overview (Jul 5, 06Z)": "বৈশ্বিক পর্যালোচনা (৫ জুলাই, ০৬Z)",
        "Probabilistic": "সম্ভাব্যতাভিত্তিক", "Ensemble Members": "এনসেম্বল সদস্য",
        "Control (deterministic)": "কন্ট্রোল (নির্ধারক)", "Member {n}": "সদস্য {n}",
        "None": "কোনোটি না", "Children (total)": "শিশু (মোট)", "Built-up Area": "নির্মিত এলাকা",
        "Hazard Probability": "ঝুঁকির সম্ভাবনা",
        "Classification": "শ্রেণীবিভাগ", "Raw": "কাঁচা",
        "Hazard Render Mode": "ঝুঁকি প্রদর্শন মোড",
        "How checked hazards render — independent of the Exposure tab's own selection.":
            "চেক করা ঝুঁকিগুলি কীভাবে প্রদর্শিত হয় — এক্সপোজার ট্যাবের নিজস্ব নির্বাচন থেকে স্বতন্ত্র।",
        'Population/Children/etc. have no visible effect while Hazard Render Mode is "{mode}" — switch it back to "Probability" (top of the Hazard tab) to use these.':
            'জনসংখ্যা/শিশু/ইত্যাদির উপর কোনো দৃশ্যমান প্রভাব নেই যতক্ষণ ঝুঁকি প্রদর্শন মোড "{mode}" থাকে — এগুলি ব্যবহার করতে "সম্ভাব্যতা"-এ ফিরে যান (ঝুঁকি ট্যাবের শীর্ষে)।',
        "Other": "অন্যান্য", "Deterministic": "নির্ধারক", "Compare Worst Case By": "সবচেয়ে খারাপ পরিস্থিতি তুলনা করুন",
        "Show": "দেখান", "Combined": "সম্মিলিত",
        "People": "মানুষ", "Children": "শিশু", "Schools": "স্কুল",
        "Health Centers": "স্বাস্থ্যকেন্দ্র", "Shelters": "আশ্রয়কেন্দ্র", "WASH Facilities": "WASH সুবিধা",
        "At Risk": "ঝুঁকিতে", "In Need": "প্রয়োজনে",
        "(shown as At Risk above In Need for People & Children)": "(মানুষ ও শিশুর জন্য ঝুঁকিতে-এর নিচে প্রয়োজনে হিসেবে দেখানো হয়েছে)",
        "(estimated Tropical Cyclone only / Both / Flood only share of each value — illustrative, not real per-pixel overlap data)":
            "(প্রতিটি মানের আনুমানিক শুধু গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় / উভয়ই / শুধু বন্যা অংশ — দৃষ্টান্তমূলক, প্রকৃত পিক্সেল-ভিত্তিক ওভারল্যাপ ডেটা নয়)",
        "Share": "অংশ", "People at Risk": "ঝুঁকিতে থাকা মানুষ", "Children at Risk": "ঝুঁকিতে থাকা শিশু",
        "Schools at Risk": "ঝুঁকিতে থাকা স্কুল", "Health Centers at Risk": "ঝুঁকিতে থাকা স্বাস্থ্যকেন্দ্র",
        "Shelters at Risk": "ঝুঁকিতে থাকা আশ্রয়কেন্দ্র", "WASH Facilities at Risk": "ঝুঁকিতে থাকা WASH সুবিধা",
        "People in Need": "প্রয়োজনে থাকা মানুষ", "Children in Need": "প্রয়োজনে থাকা শিশু",
        "People At Risk": "ঝুঁকিতে থাকা মানুষ", "People In Need": "প্রয়োজনে থাকা মানুষ",
        "Children At Risk": "ঝুঁকিতে থাকা শিশু", "Children In Need": "প্রয়োজনে থাকা শিশু",
        "Population": "জনসংখ্যা",
        "Age 0–4 (Infant)": "বয়স ০–৪ (শিশু)", "Age 5–14 (School-age)": "বয়স ৫–১৪ (স্কুল বয়স)",
        "Age 15–19 (Adolescent)": "বয়স ১৫–১৯ (কিশোর)",
        "Pop": "জনসংখ্যা", "Settlement": "বসতি", "Mod. Poverty": "মাঝারি দারিদ্র্য", "Sev. Poverty": "তীব্র দারিদ্র্য",
        "Select a country to see data availability.": "তথ্যের প্রাপ্যতা দেখতে একটি দেশ নির্বাচন করুন।",
        "{c}: no availability data.": "{c}: কোনো প্রাপ্যতা তথ্য নেই।",
        "{schools} Schools · {health_centers} HCs · {shelters} Shelters · {wash} WASH":
            "{schools} স্কুল · {health_centers} স্বাস্থ্যকেন্দ্র · {shelters} আশ্রয়কেন্দ্র · {wash} WASH",
        "Tropical Cyclone": "গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড়", "Flood": "বন্যা",
        "At Risk by Hazard Type": "ঝুঁকির ধরন অনুযায়ী ঝুঁকিতে",
        "Storm Tracks": "ঝড়ের গতিপথ",
        "Sustained Wind": "স্থায়ী বাতাস", "Gust": "দমকা হাওয়া", "Flood Hazards": "বন্যা ঝুঁকি",
        "Extent": "ব্যাপ্তি", "Probability Raster": "সম্ভাব্যতা র‍্যাস্টার",
        "Mean": "গড়", "Probability": "সম্ভাব্যতা",
        "River Flooding": "নদীর বন্যা", "Rainfall": "বৃষ্টিপাত", "Proxies": "প্রক্সি",
        "Real forecasted indicators (river return-period tiers, rainfall accumulation) that contribute to flood potential — not a direct forecast of flood extent or depth.":
            "প্রকৃত পূর্বাভাসকৃত সূচক (নদীর প্রত্যাবর্তন সময়কাল স্তর, বৃষ্টিপাত সঞ্চয়) যা বন্যার সম্ভাবনায় অবদান রাখে — বন্যার ব্যাপ্তি বা গভীরতার সরাসরি পূর্বাভাস নয়।",
        "A return period of N years means roughly a 1-in-N chance of a flood this severe occurring in any given year.":
            "N বছরের একটি প্রত্যাবর্তন সময়কাল মানে যেকোনো নির্দিষ্ট বছরে এই তীব্রতার বন্যা হওয়ার আনুমানিক N-এ-১ সম্ভাবনা।",
        # Map legend
        "Legend": "সূচক", "No layers active": "কোনো স্তর সক্রিয় নেই", "Tracks": "গতিপথ",
        "Control member": "কন্ট্রোল সদস্য", "Ensemble member": "এনসেম্বল সদস্য",
        "Lower impact": "কম প্রভাব", "Higher impact": "বেশি প্রভাব",
        "Intensity": "তীব্রতা", "Raw global layer": "কাঁচা বৈশ্বিক স্তর",
        "Source: {source}": "উৎস: {source}",
        "Facilities": "সুবিধাসমূহ",
        "Darker red = higher hazard probability at that facility.": "গাঢ় লাল = সেই সুবিধায় ঝুঁকির সম্ভাবনা বেশি।",
        "Flat fill only — no single country to attribute per-member impact to in Global mode.":
            "শুধু সমতল ভরাট — গ্লোবাল মোডে প্রতি-সদস্য প্রভাব দায়ী করার মতো কোনো একক দেশ নেই।",
        "No real data for this exact selection yet.": "এই নির্দিষ্ট নির্বাচনের জন্য এখনও কোনো প্রকৃত তথ্য নেই।",
        "10% of members": "১০% সদস্য", "≥80% of members": "≥৮০% সদস্য",
        "Coming Soon": "শীঘ্রই আসছে",
        "No real forecast data available for this country.": "এই দেশের জন্য কোনো প্রকৃত পূর্বাভাস তথ্য উপলব্ধ নেই।",
        "No real forecast for this exact date/time (raw layer).": "এই সঠিক তারিখ/সময়ের জন্য কোনো প্রকৃত পূর্বাভাস নেই (কাঁচা স্তর)।",
        "Raw layer: no real river flood-extent forecast for {date} (any run).":
            "কাঁচা স্তর: {date} তারিখের জন্য (কোনো রানে) কোনো প্রকৃত নদী বন্যা পূর্বাভাস নেই।",
        "Raw layer: no real precipitation forecast at {date} {run}Z.":
            "কাঁচা স্তর: {date} {run}Z-এ কোনো প্রকৃত বৃষ্টিপাতের পূর্বাভাস নেই।",
        "Exposure": "এক্সপোজার", "Hazard": "ঝুঁকি", "Infrastructure": "অবকাঠামো",
        "Context Data": "প্রাসঙ্গিক তথ্য", "Settlement Classification": "বসতি শ্রেণিবিন্যাস",
        "Relative Wealth Index": "আপেক্ষিক সম্পদ সূচক",
        "Moderate Child Poverty Rate": "মাঝারি শিশু দারিদ্র্যের হার",
        "Severe Child Poverty Rate": "তীব্র শিশু দারিদ্র্যের হার",
        "Data Availability": "তথ্যের প্রাপ্যতা", "View As": "যেভাবে দেখুন",
        "In Need is only available for Population/Children.": "প্রয়োজনে শুধুমাত্র জনসংখ্যা/শিশুর জন্য উপলব্ধ।",
        "In Need (Coming Soon)": "প্রয়োজনে (শীঘ্রই আসছে)",
        "In Need is not available yet for this view.": "এই দৃশ্যের জন্য প্রয়োজনে এখনও উপলব্ধ নয়।",
        "In Need currently only reflects Tropical Cyclone (wind) impact.": "\"প্রয়োজনে\" বর্তমানে শুধুমাত্র গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড়ের (বাতাসের) প্রভাব প্রতিফলিত করে।",
        "In Need: at risk, narrowed to the part of the population already flagged vulnerable.":
            "প্রয়োজনে: ঝুঁকিতে থাকা, যারা ইতিমধ্যে ঝুঁকিপূর্ণ চিহ্নিত জনসংখ্যার অংশ।",
        "Shown as plain locations with no hazard active; colored by impact probability once a hazard is toggled on below.":
            "কোনো ঝুঁকি সক্রিয় না থাকলে সাধারণ অবস্থান হিসেবে দেখানো হয়; নিচে কোনো ঝুঁকি চালু করলে প্রভাবের সম্ভাবনা অনুযায়ী রঙিন করা হয়।",
        "Last Updated:": "সর্বশেষ হালনাগাদ:", "Global": "বৈশ্বিক", "Country Analysis": "দেশ বিশ্লেষণ",
        "Demo Scenarios": "ডেমো দৃশ্যকল্প",
        "Storm:": "ঝড়:", "Forecast issued:": "পূর্বাভাস জারি:",
        "Countries:": "দেশসমূহ:", "Report generated:": "প্রতিবেদন তৈরি:",
        "Search storms available {date}…": "{date} তারিখে উপলব্ধ ঝড় খুঁজুন…",
        "Select countries…": "দেশ নির্বাচন করুন…",
        "Active Storms — {date}": "সক্রিয় ঝড় — {date}",
        "Active Storm — {country}": "সক্রিয় ঝড় — {country}",
        "Active Storms — {countries}": "সক্রিয় ঝড় — {countries}",
        "Currently tracking {names}.": "বর্তমানে ট্র্যাক করা হচ্ছে: {names}।",
        "Alert Emails": "সতর্কতা ইমেইল", "Alert Emails — {storm}": "সতর্কতা ইমেইল — {storm}",
        "No alert emails found for this storm at the selected date/time.": "নির্বাচিত তারিখ/সময়ে এই ঝড়ের জন্য কোনো সতর্কতা ইমেইল পাওয়া যায়নি।",
        "Impact Summary": "প্রভাবের সারসংক্ষেপ",
        "Global — worldwide totals across visible hazards": "বৈশ্বিক — দৃশ্যমান ঝুঁকিসমূহের বিশ্বব্যাপী মোট",
        "Global — worldwide totals across all hazards": "বৈশ্বিক — সকল ঝুঁকির সম্মিলিত বিশ্বব্যাপী মোট",
        "Reflects only countries currently initialized in the database.":
            "শুধুমাত্র ডেটাবেসে বর্তমানে যুক্ত দেশগুলো প্রতিফলিত করে।",
        "None of the initialized countries show impact at the currently selected hazard configuration (severity threshold/tier). This does not mean there is no real impact overall — a different threshold may show real impact, and potentially affected countries may not yet be in the database.":
            "যুক্ত দেশগুলোর কোনোটিই বর্তমানে নির্বাচিত ঝুঁকির কনফিগারেশনে (তীব্রতার সীমা/স্তর) প্রভাব দেখাচ্ছে না। এর অর্থ এই নয় যে সামগ্রিকভাবে প্রকৃত কোনো প্রভাব নেই — ভিন্ন একটি সীমা প্রকৃত প্রভাব দেখাতে পারে, এবং সম্ভাব্য প্রভাবিত দেশগুলো এখনও ডেটাবেসে যুক্ত নাও হতে পারে।",
        "No impact at the currently selected hazard configuration (severity threshold/tier) for this selection. This does not mean there is no real impact overall — a different threshold may show real impact.":
            "এই নির্বাচনের জন্য বর্তমানে নির্বাচিত ঝুঁকির কনফিগারেশনে (তীব্রতার সীমা/স্তর) কোনো প্রভাব নেই। এর অর্থ এই নয় যে সামগ্রিকভাবে প্রকৃত কোনো প্রভাব নেই — ভিন্ন একটি সীমা প্রকৃত প্রভাব দেখাতে পারে।",
        "Full Impact Breakdown": "সম্পূর্ণ প্রভাব বিভাজন", "Hazard Contribution": "ঝুঁকির অবদান",
        "Alert Email": "সতর্কতা ইমেইল", "Warning Email": "সতর্কীকরণ ইমেইল", "Open in new tab ↗": "নতুন ট্যাবে খুলুন ↗",
        "Total: {value}": "মোট: {value}",
        "Illustrative split — a real implementation would compute this from actual per-hazard overlap.":
            "দৃষ্টান্তমূলক বিভাজন — প্রকৃত বাস্তবায়নে এটি প্রতিটি ঝুঁকির প্রকৃত ওভারল্যাপ থেকে গণনা করা হবে।",
        "By Country": "দেশ অনুযায়ী",
        "Tropical Cyclone only": "শুধু গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড়", "Flood only": "শুধু বন্যা", "Both": "উভয়ই",
        "Double overlap (any 2)": "দ্বিগুণ ওভারল্যাপ (যেকোনো ২টি)",
        "Triple overlap (all 3)": "ত্রিগুণ ওভারল্যাপ (৩টিই)",
        "Hazards included:": "অন্তর্ভুক্ত ঝুঁকি:",
        "Threshold sensitivity (preview) — People at Risk:": "থ্রেশহোল্ড সংবেদনশীলতা (পূর্বরূপ) — ঝুঁকিতে থাকা মানুষ:",
        "Rows: depth tier · Columns: accumulation window — ringed cell is current ({window}, {tier})":
            "সারি: তীব্রতার স্তর · কলাম: সঞ্চয়ের সময়কাল — বৃত্তাকার ঘরটি বর্তমান ({window}, {tier})",
        "None — toggle a hazard on the map to see impact numbers.":
            "কোনোটি নয় — প্রভাবের সংখ্যা দেখতে মানচিত্রে একটি ঝুঁকি চালু করুন।",
        "Tropical Cyclone and Flood risk overlap — this isn't two separate groups of people.":
            "গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় ও বন্যার ঝুঁকি ওভারল্যাপ করে — এরা দুটি পৃথক জনগোষ্ঠী নয়।",
        "{country} — no active tropical cyclone": "{country} — কোনো সক্রিয় গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় নেই",
        "{n} countries selected: {list}": "{n}টি দেশ নির্বাচিত: {list}",
        "Metric": "সূচক", "Region": "অঞ্চল",
        "Admin Level 1 Breakdown — {country}": "প্রশাসনিক স্তর ১ বিভাজন — {country}",
        "Hazards": "ঝুঁকিসমূহ", "River": "নদী", "View": "দৃশ্য", "Tiles": "টাইলস", "Regions": "অঞ্চলসমূহ",
        "Minor": "সামান্য", "Moderate": "মাঝারি", "Significant": "উল্লেখযোগ্য", "Major": "বড়",
        "Severe": "তীব্র", "Extreme": "চরম", "Catastrophic": "বিপর্যয়কর",
        "Warning": "সতর্কতা", "Danger": "বিপদ", "Historic": "ঐতিহাসিক",
        "Tropical Storm": "গ্রীষ্মমন্ডলীয় ঝড়", "Strong Trop. Storm": "শক্তিশালী গ্রীষ্মমন্ডলীয় ঝড়",
        "Severe Trop. Storm": "তীব্র গ্রীষ্মমন্ডলীয় ঝড়", "Category 1 Hurricane": "শ্রেণী ১ হারিকেন",
        "Category 2 Hurricane": "শ্রেণী ২ হারিকেন", "Category 3 Hurricane": "শ্রেণী ৩ হারিকেন",
        "Category 4 Hurricane": "শ্রেণী ৪ হারিকেন", "Category 5 Hurricane": "শ্রেণী ৫ হারিকেন",
        "sustained wind": "স্থায়ী বাতাস", "gusts": "দমকা হাওয়া",
        "1-in-2-year flood": "১-এ-২-বছরের বন্যা", "1-in-5-year flood": "১-এ-৫-বছরের বন্যা",
        "1-in-10-year flood": "১-এ-১০-বছরের বন্যা", "1-in-20-year flood": "১-এ-২০-বছরের বন্যা",
        "1-in-50-year flood": "১-এ-৫০-বছরের বন্যা", "1-in-100-year flood": "১-এ-১০০-বছরের বন্যা",
        "(not natively computed — RP10 used as an upper-bound estimate)": "(স্বাভাবিকভাবে গণনা করা হয়নি — ঊর্ধ্বসীমা হিসেবে RP10 ব্যবহার করে)",
        "Moderate rain": "মাঝারি বৃষ্টি", "Heavy rain": "ভারী বৃষ্টি", "Extreme rain": "চরম বৃষ্টি",
        "Storm Surge": "ঝড়ের জলোচ্ছ্বাস",
        "Minor surge (0.3–1m)": "সামান্য জলোচ্ছ্বাস (০.৩–১মি)", "Moderate surge (1–2m)": "মাঝারি জলোচ্ছ্বাস (১–২মি)",
        "Major surge (2–3m)": "বড় জলোচ্ছ্বাস (২–৩মি)", "Extreme surge (>3m)": "চরম জলোচ্ছ্বাস (>৩মি)",
        "Track, wind envelope, and precip layer show the probability-weighted view across all members.":
            "গতিপথ, বায়ু আবরণ এবং বৃষ্টিপাত স্তর সকল সদস্যের সম্ভাবনা-ভারযুক্ত দৃশ্য দেখায়।",
        "the control run": "কন্ট্রোল রান",
        "Track, wind envelope, and precip layer switch to {label}'s own forecast — not the Probabilistic view.":
            "গতিপথ, বায়ু আবরণ এবং বৃষ্টিপাত স্তর {label}-এর নিজস্ব পূর্বাভাসে পরিবর্তিত হয় — সম্ভাব্যতাভিত্তিক দৃশ্য নয়।",
        "No storms match.": "কোনো ঝড় মেলেনি।",
        "Supported by": "সহায়তায়",
        "Powered by": "চালিত",
        ("The boundaries and names shown and the designations used on this map "
         "do not imply official endorsement or acceptance by the United Nations."):
            "এই মানচিত্রে দেখানো সীমানা ও নাম এবং ব্যবহৃত পদবি জাতিসংঘের সরকারি অনুমোদন বা গ্রহণযোগ্যতা বোঝায় না।",
        "Experimental tool. Outputs should not be used without expert review.":
            "পরীক্ষামূলক টুল। বিশেষজ্ঞ পর্যালোচনা ছাড়া ফলাফল ব্যবহার করা উচিত নয়।",
        "Language": "ভাষা",
        "No country impact yet": "এখনও কোনো দেশে প্রভাব নেই",
        "No real gust forecast data for this storm/date.":
            "এই ঝড়/তারিখের জন্য প্রকৃত ঝাপটা পূর্বাভাসের তথ্য নেই।",
        "5 days": "৫ দিন",
        "{tier} · ≥ {mm}mm over {hours}h": "{tier} · ≥ {mm}mm, {hours} ঘণ্টায়",
        "Cat {n}": "ক্যাট {n}",
        "Unknown": "অজানা",
        "{name} Severity": "{name} তীব্রতা",
        "Color = that ensemble member's own population impact. Faint fill = no impact data for that member.":
            "রঙ = সেই এনসেম্বল সদস্যের নিজস্ব জনসংখ্যা প্রভাব। ফিকে ভরাট = সেই সদস্যের জন্য কোনো প্রভাবের তথ্য নেই।",
        "ECMWF ensemble tropical cyclone forecast (51 members + control), ECMWF Open Data":
            "ECMWF এনসেম্বল ক্রান্তীয় ঘূর্ণিঝড় পূর্বাভাস (৫১ সদস্য + কন্ট্রোল), ECMWF Open Data",
        "ECMWF ensemble forecast, 10m wind gust (10fg field)":
            "ECMWF এনসেম্বল পূর্বাভাস, ১০ মিটার বায়ু ঝাপটা (10fg ফিল্ড)",
        "GloFAS v4.0 ensemble forecast (Copernicus CEMS) x JRC Global River Flood Hazard Maps v2.1":
            "GloFAS v4.0 এনসেম্বল পূর্বাভাস (Copernicus CEMS) x JRC গ্লোবাল রিভার ফ্লাড হ্যাজার্ড ম্যাপস v2.1",
        "ECMWF ensemble forecast, total precipitation (tp field)":
            "ECMWF এনসেম্বল পূর্বাভাস, মোট বৃষ্টিপাত (tp ফিল্ড)",
        "No real data pipeline yet — preview only": "এখনও কোনো প্রকৃত ডেটা পাইপলাইন নেই — শুধুমাত্র প্রিভিউ",
        "UNICEF Giga school-location API": "UNICEF Giga স্কুল-অবস্থান API",
        "OpenStreetMap (social_facility=shelter tag)": "OpenStreetMap (social_facility=shelter ট্যাগ)",
        "OpenStreetMap (humanitarian WASH tags)": "OpenStreetMap (মানবিক WASH ট্যাগ)",
        "WorldPop population estimates": "WorldPop জনসংখ্যা অনুমান",
        "EU JRC Global Human Settlement Layer (GHS-BUILT-S)":
            "EU JRC গ্লোবাল হিউম্যান সেটেলমেন্ট লেয়ার (GHS-BUILT-S)",
        "EU JRC Global Human Settlement Layer (GHS-SMOD)":
            "EU JRC গ্লোবাল হিউম্যান সেটেলমেন্ট লেয়ার (GHS-SMOD)",
        "Meta/Facebook Relative Wealth Index (hosted on HDX)":
            "Meta/Facebook আপেক্ষিক সম্পদ সূচক (HDX-এ হোস্ট করা)",
        "Modeled from Meta/Facebook RWI, calibrated to real UNICEF child poverty survey rates":
            "Meta/Facebook RWI থেকে মডেল করা, UNICEF-এর প্রকৃত শিশু দারিদ্র্য জরিপ হারের সাথে ক্যালিব্রেট করা",
        "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom":
            "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — কাস্টম",
        "Jamaica Ministry of Health facility registry — custom":
            "জ্যামাইকা স্বাস্থ্য মন্ত্রণালয়ের সুবিধা নিবন্ধন — কাস্টম",
        "supportjamaica.gov.jm (Government of Jamaica official shelter registry) — custom":
            "supportjamaica.gov.jm (জ্যামাইকা সরকারের সরকারি আশ্রয় নিবন্ধন) — কাস্টম",
        "National Water Commission / Government of Jamaica (NWC/GOJ ArcGIS) — custom":
            "জাতীয় জল কমিশন / জ্যামাইকা সরকার (NWC/GOJ ArcGIS) — কাস্টম",
        "In Need reflects Sustained Wind exposure only, and does not change with the wind severity threshold selected.":
            "\"প্রয়োজনে\" শুধুমাত্র সাসটেইনড উইন্ড এক্সপোজার প্রতিফলিত করে, এবং নির্বাচিত বায়ু তীব্রতার "
            "থ্রেশহোল্ডের সাথে পরিবর্তিত হয় না।",
        "In Need is only available for Population, Children (total), and the age bands.":
            "\"প্রয়োজনে\" শুধুমাত্র জনসংখ্যা, শিশু (মোট) এবং বয়স গ্রুপগুলির জন্য উপলব্ধ।",
        "Infants in Need": "প্রয়োজনে শিশুরা (০-৪)", "School-age in Need": "প্রয়োজনে স্কুল-বয়সী",
        "Adolescents in Need": "প্রয়োজনে কিশোর-কিশোরী",
        "Custom source: {source}": "কাস্টম উৎস: {source}",
        "* custom data source": "* কাস্টম ডেটা উৎস",
    
        "0-4": "0-4",
        "15-19": "15-19",
        "2+ hazards overlap": "2+ ঝুঁকির ওভারল্যাপ",
        "24h": "24h",
        "5-14": "5-14",
        "6h": "6h",
        "72h": "72h",
        "Adolescents": "কিশোর",
        "Anguilla": "অ্যাঙ্গুইলা",
        "Aruba": "আরুবা",
        "BAVI period — Global flood hazards preview (2 Jul 2026, 06Z)": "BAVI সময়কাল — বিশ্বব্যাপী বন্যা ঝুঁকির প্রাকদর্শন (2 Jul 2026, 06Z)",
        "Bahamas": "বাহামা",
        "Bangladesh": "বাংলাদেশ",
        "Barbados": "বার্বাডোস",
        "Belize": "বেলিজ",
        "British Virgin Islands": "ব্রিটিশ ভার্জিন দ্বীপপুঞ্জ",
        "Child Climate Index": "শিশু জলবায়ু সূচক",
        "Combined Hazard Probability": "সম্মিলিত ঝুঁকির সম্ভাবনা",
        "Combined Probability": "সম্মিলিত সম্ভাব্যতা",
        "Combined across {n} active hazards — see map tooltip.": "{n}টি সক্রিয় ঝুঁকি জুড়ে সম্মিলিত — মানচিত্র টুলটিপ দেখুন।",
        "Costa Rica": "কোস্টা রিকা",
        "Cuba": "কিউবা",
        "Curaçao": "কুরাসাও",
        "Dominica": "ডমিনিকা",
        "Dominican Republic": "ডোমিনিকান প্রজাতন্ত্র",
        "East Caribbean Area": "পূর্ব ক্যারিবিয়ান অঞ্চল",
        "El Salvador": "এল সালভাদোর",
        "Expected Adolescent Impact": "প্রত্যাশিত কিশোর প্রভাব",
        "Expected Built-up Impact": "প্রত্যাশিত নির্মিত প্রভাব",
        "Expected Child Climate Index Impact": "প্রত্যাশিত শিশু জলবায়ু সূচক প্রভাব",
        "Expected Children Impact": "প্রত্যাশিত শিশু প্রভাব",
        "Expected Infant Impact": "প্রত্যাশিত শিশু প্রভাব",
        "Expected Population Impact": "প্রত্যাশিত জনসংখ্যা প্রভাব",
        "Expected School-age Impact": "প্রত্যাশিত স্কুল বয়স প্রভাব",
        "Fewest members agree": "সবচেয়ে কম সংখ্যক সদস্য সম্মত",
        "Flooded under this member": "এই সদস্যের অধীনে প্লাবিত",
        "Grenada": "গ্রেনাডা",
        "Guatemala": "গুয়াতেমালা",
        "HCs": "HCs",
        "Haiti": "হাইতি",
        "Hazard Classification": "ঝুঁকি শ্রেণীবিভাগ",
        "Honduras": "হন্ডুরাস",
        "In Need is unavailable — no real Sustained Wind data for the current selection.": "প্রয়োজনে অনুপলব্ধ — বর্তমান নির্বাচনের জন্য কোনো প্রকৃত স্থায়ী বাতাসের তথ্য নেই।",
        "Infants": "শিশু",
        "Jamaica": "জামাইকা",
        "MELISSA — Jamaica (28 Oct 2025, 00Z)": "MELISSA — জামাইকা (28 Oct 2025, 00Z)",
        "Mapbox Light": "Mapbox Light",
        "Mexico": "মেক্সিকো",
        "Moderate Poverty Probability": "মাঝারি দারিদ্র্যের সম্ভাব্যতা",
        "Montserrat": "মন্টসেরাট",
        "Most members agree": "সবচেয়ে বেশি সংখ্যক সদস্য সম্মত",
        "Nicaragua": "নিকারাগুয়া",
        "No real admin-level breakdown available for this storm/threshold selection.": "এই ঝড়/থ্রেশহোল্ড নির্বাচনের জন্য কোনো প্রকৃত প্রশাসনিক-স্তরের বিভাজন উপলভ্য নেই।",
        "RWI": "RWI",
        "Real ensemble-member fraction across {n} active hazards — see map tooltip.": "{n}টি সক্রিয় ঝুঁকি জুড়ে প্রকৃত এনসেম্বল সদস্য ভগ্নাংশ — মানচিত্র টুলটিপ দেখুন।",
        "Real per-member Tropical Cyclone only / Both / Flood only split of each value (falls back to an illustrative estimate only when no real split data resolves).": "প্রতিটি মানের প্রকৃত প্রতি-সদস্য শুধু গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় / উভয়ই / শুধু বন্যা বিভাজন (শুধুমাত্র প্রকৃত বিভাজন তথ্য পাওয়া না গেলে একটি দৃষ্টান্তমূলক অনুমানে ফিরে যায়)।",
        "Real per-member figure — \"Both\" counts only ensemble members genuinely hit by both Tropical Cyclone and Flood hazards at the same tile, not an estimate.": "প্রকৃত প্রতি-সদস্য পরিসংখ্যান — \"উভয়ই\" শুধুমাত্র সেই এনসেম্বল সদস্যদের গণনা করে যারা একই টাইলে সত্যিকারভাবে গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় এবং বন্যা উভয় ঝুঁকিতেই আক্রান্ত হয়েছে, কোনো অনুমান নয়।",
        "Real per-member union, not a sum: checks each of the 51 real ensemble members and counts it once if ANY active hazard reaches this tile, then divides by 51. Example: River alone hits 10 members (~20%), Rain alone hits 10 members (~20%), 5 of them the same members. Combined = 15 unique members ÷ 51 ≈ 29% — not 20%+20%=40%, and not just the larger single number.": "প্রকৃত প্রতি-সদস্য ইউনিয়ন, যোগফল নয়: এটি 51টি প্রকৃত এনসেম্বল সদস্যের প্রতিটি পরীক্ষা করে এবং যদি কোনো সক্রিয় ঝুঁকি এই টাইলে পৌঁছায় তবে এটিকে একবার গণনা করে, তারপর 51 দিয়ে ভাগ করে। উদাহরণ: শুধু নদী 10 জন সদস্যকে প্রভাবিত করে (~20%), শুধু বৃষ্টি 10 জন সদস্যকে প্রভাবিত করে (~20%), তাদের মধ্যে 5 জন একই সদস্য। সম্মিলিত = 15 জন অনন্য সদস্য ÷ 51 ≈ 29% — 20%+20%=40% নয়, এবং শুধু বড় একক সংখ্যাও নয়।",
        "Saint Kitts and Nevis": "সেন্ট কিটস ও নেভিস",
        "School-age": "স্কুল বয়স",
        "Settlement Type": "বসতির ধরন",
        "Severe Poverty Probability": "তীব্র দারিদ্র্যের সম্ভাব্যতা",
        "Trinidad and Tobago": "ত্রিনিদাদ ও টোবাগো",
        "Turks and Caicos Islands": "টার্কস ও কাইকোস দ্বীপপুঞ্জ",
        "WASH": "WASH",
        "Wind": "বাতাস",
        "{d}d": "{d}d",
    
        "No real data for this hazard/selection.": "এই ঝুঁকি/নির্বাচনের জন্য কোনো প্রকৃত তথ্য নেই।",
        "No real per-threshold rainfall data for this metric.": "এই সূচকের জন্য থ্রেশহোল্ড অনুযায়ী কোনো প্রকৃত বৃষ্টিপাতের তথ্য নেই।",
        "No real split data for this cell.": "এই কক্ষের জন্য কোনো প্রকৃত বিভাজন তথ্য নেই।",
        "Real per-hazard split unavailable for this selection — Tropical Cyclone and Flood contributions cannot be separated right now.": "এই নির্বাচনের জন্য প্রতি-ঝুঁকি প্রকৃত বিভাজন উপলব্ধ নেই — গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় এবং বন্যার অবদান এই মুহূর্তে আলাদা করা যাচ্ছে না।",
        "Real per-member Tropical Cyclone only / Both / Flood only split of each value.": "প্রতিটি মানের প্রকৃত প্রতি-সদস্য শুধু গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় / উভয়ই / শুধু বন্যা বিভাজন।",
    },
}


def _t(text, **kwargs):
    """Look up `text` in the current language's dict (falls back to the
    English original if untranslated), then apply any {placeholder} kwargs.
    """
    translated = _TRANSLATIONS.get(_LANG, {}).get(text, text)
    return translated.format(**kwargs) if kwargs else translated


# Vocabulary for the map's own hover tooltips, tooltip_tracks/tooltip_
# envelopes/tooltip_schools/tooltip_health/tooltip_shelters/tooltip_wash
# (components/map/javascript.py) and _buildTileTooltip/_buildRawLayerTooltip
# (assets/maplibre_tiles.js) are raw browser-side JS with no access to
# _TRANSLATIONS/_t() (those only exist server-side, while Dash builds the
# Python component tree), so this vocabulary is served to the browser
# separately. Deliberately SHORT, colon-free keys (JS composes the trailing
# ":"/parens itself) so the same ~50-entry vocabulary covers all 8 functions'
# real overlap (e.g. "Population"/"Schools"/"Age 0–4" each appear in several)
# instead of one entry per literal on-screen string. Reuses _TRANSLATIONS'
# own existing values verbatim wherever the concept already has one
# elsewhere in the app (Population/Schools/Health Centers/Shelters/WASH
# Facilities/Children (total)/In Need/Settlement/Region/Probability/River
# Flooding/Rainfall) so this vocabulary never drifts from the rest of the
# app's own translations.
_MAP_TOOLTIP_TRANSLATIONS = {
    "es": {
        "Ensemble Member": "Miembro del Conjunto", "Control Track": "Trayectoria de Control",
        "Ensemble Track": "Trayectoria del Conjunto", "Gust Extent": "Extensión de Ráfagas",
        "Sustained Wind Extent": "Extensión de Viento Sostenido", "Sustained Wind": "Viento Sostenido", "Gust Threshold": "Umbral de Ráfaga",
        "Wind Threshold": "Umbral de Viento", "Impact": "Impacto",
        "Population": "Población", "Children (total)": "Niños (total)",
        "Age 0–4": "0–4 años", "Age 5–14": "5–14 años", "Age 15–19": "15–19 años",
        "Schools": "Escuelas", "Health Centers": "Centros de Salud", "Shelters": "Refugios",
        "WASH Facilities": "Instalaciones WASH", "Built Surface": "Superficie Construida",
        "In Need": "En Necesidad", "In Need (across all wind speeds)": "En Necesidad (en todas las velocidades de viento)",
        "School": "Escuela", "Health Facility": "Centro de Salud", "Shelter": "Refugio",
        "WASH Facility": "Instalación WASH", "Name": "Nombre", "Type": "Tipo", "Category": "Categoría",
        "Impact Probability": "Probabilidad de Impacto",
        "Base location (no impact data)": "Ubicación base (sin datos de impacto)",
        "Region Statistics": "Estadísticas de la Región", "Tile Statistics": "Estadísticas de la Celda",
        "Region": "Región", "Tile": "Celda", "Base Data": "Datos Base",
        "Expected Impact": "Impacto Esperado", "Hazard Probability": "Probabilidad de Peligro",
        "(reference only, not counted toward the red at-risk numbers below)": "(solo de referencia, no cuenta para las cifras rojas de riesgo de abajo)",
        "Tropical Cyclone": "Ciclón Tropical", "Gust": "Ráfaga", "Combined": "Combinado",
        "Settlement": "Asentamiento", "Wealth Index (RWI)": "Índice de Riqueza (RWI)",
        "Moderate Child Poverty": "Pobreza Infantil Moderada", "Severe Child Poverty": "Pobreza Infantil Severa",
        "Rural": "Rural", "Urban Clusters": "Grupos Urbanos", "Urban Centers": "Centros Urbanos",
        "N/A": "N/D", "River Flooding": "Inundación Fluvial", "Rainfall": "Precipitación",
        "Return period": "Período de retorno", "Member agreement": "Concordancia de miembros",
        "Ensemble-mean rate": "Tasa media del conjunto", "Probability": "Probabilidad",
        "Real GloFAS discharge exceeding the return-period threshold, matched against JRC's historical flood-extent map — not a simulated depth/extent for this specific event.":
            "Descarga real de GloFAS que supera el umbral del período de retorno, contrastada con el mapa histórico de extensión de inundación del JRC — no una profundidad/extensión simulada para este evento específico.",
        "Real forecasted precipitation — a flood-risk indicator, not a flood forecast.":
            "Precipitación real pronosticada — un indicador de riesgo de inundación, no un pronóstico de inundación.",
    
        "Combined = highest individual hazard probability, not a joint measurement.": "Combinado = la probabilidad de peligro individual más alta, no una medición conjunta.",
        "Flooded under this member": "Inundado según este miembro",
        "Member": "Miembro",
        "Not flooded under this member": "No inundado según este miembro",
        "Real GloFAS discharge exceeding the return-period threshold, matched against JRC's historical flood-extent map, not a simulated depth/extent for this specific event.": "Caudal real de GloFAS que supera el umbral del período de retorno, comparado con el mapa histórico de extensión de inundación de JRC, no una profundidad/extensión simulada para este evento específico.",
        "Real forecasted precipitation (a flood-risk indicator, not a flood forecast).": "Precipitación pronosticada real (un indicador de riesgo de inundación, no un pronóstico de inundación).",
        "This member's rate": "Tasa de este miembro",
        "accumulation window": "ventana de acumulación",
    },
    "fr": {
        "Ensemble Member": "Membre de l'ensemble", "Control Track": "Trajectoire de contrôle",
        "Ensemble Track": "Trajectoire de l'ensemble", "Gust Extent": "Étendue des rafales",
        "Sustained Wind Extent": "Étendue du vent soutenu", "Sustained Wind": "Vent soutenu", "Gust Threshold": "Seuil de rafale",
        "Wind Threshold": "Seuil de vent", "Impact": "Impact",
        "Population": "Population", "Children (total)": "Enfants (total)",
        "Age 0–4": "0–4 ans", "Age 5–14": "5–14 ans", "Age 15–19": "15–19 ans",
        "Schools": "Écoles", "Health Centers": "Centres de santé", "Shelters": "Abris",
        "WASH Facilities": "Installations EAH", "Built Surface": "Surface bâtie",
        "In Need": "Dans le besoin", "In Need (across all wind speeds)": "Dans le besoin (toutes vitesses de vent confondues)",
        "School": "École", "Health Facility": "Centre de santé", "Shelter": "Abri",
        "WASH Facility": "Installation EAH", "Name": "Nom", "Type": "Type", "Category": "Catégorie",
        "Impact Probability": "Probabilité d'impact",
        "Base location (no impact data)": "Emplacement de base (aucune donnée d'impact)",
        "Region Statistics": "Statistiques de la région", "Tile Statistics": "Statistiques de la cellule",
        "Region": "Région", "Tile": "Cellule", "Base Data": "Données de base",
        "Expected Impact": "Impact attendu", "Hazard Probability": "Probabilité de risque",
        "(reference only, not counted toward the red at-risk numbers below)": "(référence uniquement, non compté dans les chiffres rouges de risque ci-dessous)",
        "Tropical Cyclone": "Cyclone tropical", "Gust": "Rafale", "Combined": "Combiné",
        "Settlement": "Peuplement", "Wealth Index (RWI)": "Indice de richesse (RWI)",
        "Moderate Child Poverty": "Pauvreté infantile modérée", "Severe Child Poverty": "Pauvreté infantile sévère",
        "Rural": "Rural", "Urban Clusters": "Groupes urbains", "Urban Centers": "Centres urbains",
        "N/A": "N/D", "River Flooding": "Inondation fluviale", "Rainfall": "Précipitations",
        "Return period": "Période de retour", "Member agreement": "Accord des membres",
        "Ensemble-mean rate": "Taux moyen de l'ensemble", "Probability": "Probabilité",
        "Real GloFAS discharge exceeding the return-period threshold, matched against JRC's historical flood-extent map — not a simulated depth/extent for this specific event.":
            "Débit réel GloFAS dépassant le seuil de période de retour, comparé à la carte historique d'étendue d'inondation du JRC — pas une profondeur/étendue simulée pour cet événement spécifique.",
        "Real forecasted precipitation — a flood-risk indicator, not a flood forecast.":
            "Précipitations réelles prévues — un indicateur de risque d'inondation, pas une prévision d'inondation.",
    
        "Combined = highest individual hazard probability, not a joint measurement.": "Combiné = probabilité de risque individuelle la plus élevée, pas une mesure conjointe.",
        "Flooded under this member": "Inondé selon ce membre",
        "Member": "Membre",
        "Not flooded under this member": "Non inondé selon ce membre",
        "Real GloFAS discharge exceeding the return-period threshold, matched against JRC's historical flood-extent map, not a simulated depth/extent for this specific event.": "Débit GloFAS réel dépassant le seuil de période de retour, comparé à la carte historique d'étendue des inondations du JRC, et non une profondeur/étendue simulée pour cet événement spécifique.",
        "Real forecasted precipitation (a flood-risk indicator, not a flood forecast).": "Précipitations prévues réelles (un indicateur de risque d'inondation, pas une prévision d'inondation).",
        "This member's rate": "Taux de ce membre",
        "accumulation window": "fenêtre d'accumulation",
    },
    "bn": {
        "Ensemble Member": "এনসেম্বল সদস্য", "Control Track": "নিয়ন্ত্রণ ট্র্যাক",
        "Ensemble Track": "এনসেম্বল ট্র্যাক", "Gust Extent": "দমকা হাওয়ার ব্যাপ্তি",
        "Sustained Wind Extent": "স্থায়ী বাতাসের ব্যাপ্তি", "Sustained Wind": "স্থায়ী বাতাস", "Gust Threshold": "দমকা হাওয়ার সীমা",
        "Wind Threshold": "বাতাসের সীমা", "Impact": "প্রভাব",
        "Population": "জনসংখ্যা", "Children (total)": "শিশু (মোট)",
        "Age 0–4": "বয়স ০–৪", "Age 5–14": "বয়স ৫–১৪", "Age 15–19": "বয়স ১৫–১৯",
        "Schools": "স্কুল", "Health Centers": "স্বাস্থ্যকেন্দ্র", "Shelters": "আশ্রয়কেন্দ্র",
        "WASH Facilities": "WASH সুবিধা", "Built Surface": "নির্মিত পৃষ্ঠ",
        "In Need": "প্রয়োজনে", "In Need (across all wind speeds)": "প্রয়োজনে (সকল বাতাসের গতিতে)",
        "School": "স্কুল", "Health Facility": "স্বাস্থ্য সুবিধা", "Shelter": "আশ্রয়কেন্দ্র",
        "WASH Facility": "WASH সুবিধা", "Name": "নাম", "Type": "ধরন", "Category": "শ্রেণী",
        "Impact Probability": "প্রভাবের সম্ভাব্যতা",
        "Base location (no impact data)": "মূল অবস্থান (কোনো প্রভাব ডেটা নেই)",
        "Region Statistics": "অঞ্চলের পরিসংখ্যান", "Tile Statistics": "টাইলের পরিসংখ্যান",
        "Region": "অঞ্চল", "Tile": "টাইল", "Base Data": "মূল ডেটা",
        "Expected Impact": "প্রত্যাশিত প্রভাব", "Hazard Probability": "বিপদের সম্ভাব্যতা",
        "(reference only, not counted toward the red at-risk numbers below)": "(শুধুমাত্র তথ্যসূত্র, নিচের লাল ঝুঁকিপূর্ণ সংখ্যায় গণনা করা হয়নি)",
        "Tropical Cyclone": "গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড়", "Gust": "দমকা হাওয়া", "Combined": "সম্মিলিত",
        "Settlement": "বসতি", "Wealth Index (RWI)": "সম্পদ সূচক (RWI)",
        "Moderate Child Poverty": "মাঝারি শিশু দারিদ্র্য", "Severe Child Poverty": "তীব্র শিশু দারিদ্র্য",
        "Rural": "গ্রামীণ", "Urban Clusters": "শহুরে গুচ্ছ", "Urban Centers": "শহুরে কেন্দ্র",
        "N/A": "প্রযোজ্য নয়", "River Flooding": "নদীর বন্যা", "Rainfall": "বৃষ্টিপাত",
        "Return period": "প্রত্যাবর্তন সময়কাল", "Member agreement": "সদস্য সম্মতি",
        "Ensemble-mean rate": "এনসেম্বল-গড় হার", "Probability": "সম্ভাব্যতা",
        "Real GloFAS discharge exceeding the return-period threshold, matched against JRC's historical flood-extent map — not a simulated depth/extent for this specific event.":
            "প্রত্যাবর্তন-সময়কালের সীমা অতিক্রমকারী প্রকৃত GloFAS নিঃসরণ, JRC-এর ঐতিহাসিক বন্যা-ব্যাপ্তি মানচিত্রের সাথে মিলিত — এই নির্দিষ্ট ঘটনার জন্য কোনো সিমুলেটেড গভীরতা/ব্যাপ্তি নয়।",
        "Real forecasted precipitation — a flood-risk indicator, not a flood forecast.":
            "প্রকৃত পূর্বাভাসকৃত বৃষ্টিপাত — একটি বন্যা-ঝুঁকি সূচক, বন্যার পূর্বাভাস নয়।",
    
        "Combined = highest individual hazard probability, not a joint measurement.": "সম্মিলিত = সর্বোচ্চ পৃথক ঝুঁকির সম্ভাবনা, যৌথ পরিমাপ নয়।",
        "Flooded under this member": "এই সদস্যের অধীনে প্লাবিত",
        "Member": "সদস্য",
        "Not flooded under this member": "এই সদস্যের অধীনে প্লাবিত নয়",
        "Real GloFAS discharge exceeding the return-period threshold, matched against JRC's historical flood-extent map, not a simulated depth/extent for this specific event.": "প্রকৃত GloFAS প্রবাহ যা প্রত্যাবর্তন সময়কাল থ্রেশহোল্ড অতিক্রম করে, JRC-এর ঐতিহাসিক বন্যা-বিস্তৃতি মানচিত্রের সাথে মিলিত করা হয়েছে, এই নির্দিষ্ট ঘটনার জন্য কোনো সিমুলেটেড গভীরতা/বিস্তৃতি নয়।",
        "Real forecasted precipitation (a flood-risk indicator, not a flood forecast).": "প্রকৃত পূর্বাভাসিত বৃষ্টিপাত (একটি বন্যা-ঝুঁকি নির্দেশক, বন্যার পূর্বাভাস নয়)।",
        "This member's rate": "এই সদস্যের হার",
        "accumulation window": "সঞ্চয়ন সময়কাল",
    },
}


_PANEL_STYLE = {
    "position": "absolute", "background": "rgba(255,255,255,0.94)", "backdropFilter": "blur(10px)",
    "border": "1px solid #dde6ec", "borderRadius": "12px",
    "boxShadow": "0 8px 28px rgba(0,0,0,0.18), 0 1px 2px rgba(0,0,0,0.08)", "zIndex": 30,
}

# Unified spacing system for every floating panel/row in this shell, ONE
# margin applied consistently to every gap: topbar-to-panel-top, panel-to-
# viewport-edge, panel-bottom-to-bottom-row-top, bottom-row-to-footer,
# command-bar-to-topbar, command-bar-to-side-panels, disclaimer-to-footer,
# AND the topbar/footer's own horizontal content padding (so the "AHEAD OF
# THE STORM" logo/"Supported by" text and the language switcher/GitHub
# icon line up with the side panels' own left/right edges, not a
# different, wider 24px padding). So the whole shell reads as one
# consistent grid instead of several independently-tuned offsets.
#
# Every gap must be changed together, on both sides at once, changing
# only one side of a gap, or shrinking the side panels' own maxHeight to
# make room for a bigger margin, breaks the shared grid. Panel height is a
# real content constraint (users need to see as many Infrastructure/stat
# rows as possible) and must NOT shrink just to make the margins prettier;
# the margin itself is kept small instead. _PANEL_MAX_HEIGHT stays the
# original "100vh - 210", and 15px is small enough that even at that
# taller cap, the panel's bottom edge still clears the basemap-row/legend
# beneath it with margin to spare, see the derivation below.
#
# Measured: topbar ~58.6px tall, footer ~67.3px tall, the bottom-left
# basemap+demo row ~40.6px tall, the collapsed map legend ~37.5px tall.
# Solving gap_top == gap_left == gap_bottom == gap_footer == M for the
# panel's "100vh-210" cap gives M ≈ 14.5-15.5 (the two bottom rows differ
# slightly in height), 15 satisfies both with a couple of spare pixels
# either way (no overlap at true max height).
_UI_MARGIN = 15
_PANEL_TOP = "74px"  # topbar height (~58.6) + _UI_MARGIN, rounded
_BOTTOM_ROW_OFFSET = "82px"  # footer height (~67.3) + _UI_MARGIN, rounded
# See this block's own comment above on why panel height must not shrink
# just to make the margins symmetric.
_PANEL_MAX_HEIGHT = "calc(100vh - 210px)"

# Same frosted-glass look as the floating panels (_PANEL_STYLE), applied to
# dmc.Modal via its Styles API so popups read as part of the same design
# language instead of a plain default Mantine dialog.
_MODAL_PANEL_STYLES = {
    "content": {
        "background": "rgba(255,255,255,0.94)", "backdropFilter": "blur(14px)",
        "border": "1px solid #dde6ec",
        "boxShadow": "0 12px 40px rgba(0,0,0,0.22), 0 1px 2px rgba(0,0,0,0.08)",
    },
    "header": {"background": "transparent", "borderBottom": "1px solid #eef2f5", "paddingBottom": "12px"},
    "title": {"fontWeight": 700, "fontSize": "15px", "color": "#16232c"},
    "close": {"color": "#8ea0ab"},
    # Fixed max height + internal scroll, without this, centered=True keeps
    # recentering/regrowing the WHOLE modal every time a collapsible section
    # (like Admin Level 1 Breakdown) expands or collapses, which reads as
    # the modal growing from the bottom rather than sitting still while its
    # content scrolls.
    # overflowX:auto here too (not just on the custom div wrapping the
    # breakdown table), Mantine's own Modal.Content box clips overflow for
    # its rounded corners, so if THIS body element (the ancestor between
    # that clipping box and my own table wrapper) doesn't also explicitly
    # allow horizontal scroll, wide content can end up hard-clipped by the
    # content box before it ever reaches my wrapper's own overflow rule.
    "body": {"maxHeight": "72vh", "overflowY": "auto", "overflowX": "auto"},
}

# Same wording as the real app's map (layouts/panels.py _UN_DISCLAIMER),
# there attached to every Leaflet base-layer's attribution string.
_UN_DISCLAIMER = (
    "The boundaries and names shown and the designations used on this map "
    "do not imply official endorsement or acceptance by the United Nations."
)

# Shown in the footer (see _compact_footer below) so it's visible on every
# load regardless of mode/date/country selection, same always-present
# placement as "Supported by" right next to it, not a one-time dismissible
# banner that a returning user could miss having already closed.
_EXPERIMENTAL_DISCLAIMER = (
    "Experimental tool. Outputs should not be used without expert review."
)

# 1x1 transparent GIF, inlined as a data: URI, used as the `url` for every
# opacity=0 Leaflet base layer below (MapLibre renders the real, visible
# basemap; these exist only to drive dl.LayersControl's selection UI/
# attribution text and fire baselayerchange). dl.TileLayer's own default
# `url` is the real OpenStreetMap tile server, so leaving it unset meant the
# currently-selected invisible layer silently fetched real (never-shown) OSM
# tiles on every pan/zoom, this constant needs no network request at all.
_BLANK_TILE_URL = "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw=="

# Same 4 named options as the real app's dl.LayersControl (layouts/panels.py):
# CartoDB Light (default), CartoDB Dark, Satellite, OpenStreetMap/Mapbox
# Light. This pill is the only control the user interacts with, its
# clientside_callback below clicks the matching (visually hidden) native
# Leaflet LayersControl radio input, so Leaflet's own 'baselayerchange'
# event fires and swapMaplibreBasemap (maplibre_tiles.js) does the real
# tile swap, exactly as it already does for panels.py's own LayersControl.
def _basemap_options():
    osm_label = _t("Mapbox Light") if mapbox_token else _t("OSM")
    return [
        {"value": "cartodb-light", "label": _t("Light")},
        {"value": "cartodb-dark", "label": _t("Dark")},
        {"value": "satellite", "label": _t("Satellite")},
        {"value": "osm", "label": osm_label},
    ]

# Shared with _topbar's own DatePickerInput/SegmentedControl initial values
# and _breakdown_new_tab_href's default query params, so the print page's
# "Forecast issued" line always matches the live app's own default state
# instead of a second hardcoded literal that could drift out of sync.
# BAVI/PHL 2026-07-02 is the only country+date combination with real data
# for Wind (all 4 runs), River (00Z), AND Rainfall (06Z) at once, making it
# the richest available real test point for exercising every wired hazard
# layer. The other test scenario worth knowing about for manual testing:
# MELISSA/Jamaica at 2025-10-28 00Z (single-country, wind-only, 17,920 real
# tiles), a genuine single-storm/single-country case, not set as the
# default here since only one date/run pair can be.
# Real, not a hardcoded literal, always the most recent forecast cycle
# Snowflake actually has, computed once here and shared with _build_real_storms()
# below (same query, not run twice). Falls back to "now" only when there is
# genuinely no data anywhere (a fresh/empty environment), rather than ever
# silently drifting behind a stale hardcoded date.
try:
    _LATEST_FORECAST_TIME = get_latest_forecast_time_overall()
except Exception as e:
    logger.warning("Could not load latest forecast time: %s", e)
    _LATEST_FORECAST_TIME = None

# _DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN (the topbar's own initial
# landing date/run) deliberately do NOT just reuse _LATEST_FORECAST_TIME
# above: that's the latest raw TRACK ingestion (TC_TRACKS), which
# TC-ECMWF-Forecast-Pipeline can publish hours before DATAPIPELINE's own
# wind/precip impact computation for that exact cycle actually finishes
# (real, live-confirmed gap: 19 Aug 2026, a 06Z cycle was raw-ingested at
# 08:26 but MERCATOR_TILE_PRECIP_MAT still hadn't caught up over 6.5 hours
# later, and a Databricks run was separately still processing a genuinely
# new storm cycle for over an hour). get_default_forecast_cycle() instead
# walks backwards from the latest track until it finds a cycle where wind
# (+gust) AND precip are BOTH genuinely done (real materialized MAT output
# for wind/gust, AMBIENT_HAZARD_RUN_LOG completion bookkeeping for precip,
# not raw ingestion; see _wind_gust_ready_at's own docstring for why wind/
# gust readiness checks real output tables directly, not a completion log),
# so a user isn't dropped by default onto a half-computed cycle with a
# confusing/wrong-looking Impact Summary and no explanation. River is
# deliberately NOT gated here (see that function's own docstring: its real
# staleness right now is a separate, already-known, accepted gap, the
# GloFAS ingestion task has been suspended for over a month; gating on it
# would make this fix a permanent no-op in today's real data state).
#
# _LATEST_FORECAST_TIME itself is intentionally left untouched above: the
# date/time picker's own "don't let a user pick a future/nonexistent date"
# cutoff (_max_allowed_run_for_date/_time_options_for_date) and the
# Active Storms list (_STORMS = _build_real_storms(_LATEST_FORECAST_TIME)
# below) are both legitimately about real TRACK existence, not impact-
# computation completeness, changing those would be a different, wider
# fix than what was actually asked for here.
def _resolve_default_forecast_date_run():
    """Live (date_str, run_str) default for the topbar, see
    get_default_forecast_cycle's own docstring for the full "why" behind
    the wind/gust+precip readiness walk this wraps.

    get_default_forecast_cycle() is itself @ttl_cache'd (15 min, same
    _META_TTL as every other single-value "storm list, forecast times"
    getter in snowflake_utils.py). That means this function is safe to
    call fresh on every page load (see layout() below), not just once at
    process start: the first visitor in each 15-minute window pays one
    real multi-query Snowflake round-trip, every other visitor in that
    same window gets the cached result instantly, not a fresh query each
    -- N concurrent visitors don't turn into N queries.

    Falls back to the raw latest TRACK ingestion (_LATEST_FORECAST_TIME)
    if the real resolution fails or returns nothing (a genuinely fresh/
    empty environment), and further to wall-clock UTC "today" at 00Z if
    even that is unavailable."""
    try:
        cycle_ts = get_default_forecast_cycle()
    except Exception as e:
        logger.warning("Could not resolve default forecast cycle: %s", e)
        cycle_ts = None
    if cycle_ts is None and _LATEST_FORECAST_TIME is not None:
        cycle_ts = _LATEST_FORECAST_TIME
    if cycle_ts is not None:
        ts = pd.Timestamp(cycle_ts)
        # Snap to the nearest synoptic run (00/06/12/18Z, the only values
        # ms-topbar-time's SegmentedControl offers), real forecast times
        # are always exactly on one of these already, this is just a
        # defensive floor.
        return ts.strftime("%Y-%m-%d"), f"{(ts.hour // 6) * 6:02d}"
    return pd.Timestamp.utcnow().strftime("%Y-%m-%d"), "00"


_DEFAULT_FORECAST_DATE, _DEFAULT_FORECAST_RUN = _resolve_default_forecast_date_run()

# "Future" here means later than the latest REAL forecast_time in the
# database (_LATEST_FORECAST_TIME/_DEFAULT_FORECAST_DATE+RUN above), not
# later than wall-clock "now", real forecast data can genuinely lag behind
# today's actual date, so wall-clock time would be the wrong reference and
# would incorrectly greyed out/allow the wrong runs.
_RUN_VALUES = ["00", "06", "12", "18"]


def _max_allowed_run_for_date(date_str, latest_forecast_time=_LATEST_FORECAST_TIME,
                               default_forecast_date=_DEFAULT_FORECAST_DATE,
                               default_forecast_run=_DEFAULT_FORECAST_RUN):
    """Highest run ('00'/'06'/'12'/'18') selectable for date_str without
    going past the latest real forecast_time in the database. Returns None
    when date_str is entirely past the latest available date (nothing on
    it is selectable) or when there's no known latest date at all (fresh/
    empty environment, nothing to restrict against).

    The three ceiling params default to the module-import-time globals
    (correct for the very first layout render, and for every other existing
    call site that doesn't pass anything), but callers that need this to
    reflect data that landed AFTER the process started should pass live
    values from _live_forecast_ceiling() instead. See
    _refresh_forecast_ceiling below for why the frozen globals alone are
    not enough."""
    if latest_forecast_time is None or not date_str:
        return _RUN_VALUES[-1]
    if date_str < default_forecast_date:
        return _RUN_VALUES[-1]
    if date_str == default_forecast_date:
        return default_forecast_run
    return None


def _time_options_for_date(date_str, latest_forecast_time=_LATEST_FORECAST_TIME,
                            default_forecast_date=_DEFAULT_FORECAST_DATE,
                            default_forecast_run=_DEFAULT_FORECAST_RUN):
    """topbar-time's SegmentedControl `data`, runs later than
    _max_allowed_run_for_date(date_str) get a dimmed, non-interactive-
    looking label (Mantine's SegmentedControl has no native per-item
    disabled state; _guard_future_forecast_run below is what actually
    blocks selecting one, this just makes that same boundary visible).
    Same live-ceiling-override params as _max_allowed_run_for_date, for the
    same reason."""
    max_run = _max_allowed_run_for_date(date_str, latest_forecast_time, default_forecast_date, default_forecast_run)
    data = []
    for r in _RUN_VALUES:
        if max_run is not None and int(r) <= int(max_run):
            data.append({"value": r, "label": f"{r}Z"})
        else:
            data.append({"value": r, "label": html.Span(
                f"{r}Z", style={"opacity": 0.35, "cursor": "not-allowed"})})
    return data


def _live_forecast_ceiling():
    """Live (uncached) re-fetch of the latest real forecast_time ceiling,
    same query/pattern _update_last_updated already uses correctly below
    (get_latest_forecast_time_overall() has no @ttl_cache/@lru_cache at
    all, so this is always a fresh Snowflake round-trip, never stale).

    _LATEST_FORECAST_TIME/_DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN
    above are computed exactly ONCE, at module import time, and then never
    change again for the rest of the process's life -- correct for the
    very first layout render, but a real staleness bug for everything
    else: a locally-running process started before a later real forecast
    cycle lands (e.g. process starts while 00Z is latest, a real 18Z cycle
    completes hours later) stays stuck offering only up to 00Z in the
    topbar for its entire remaining lifetime, even though "Last Updated"
    (which already calls this same live query on its own 15-min interval)
    correctly shows 18Z the whole time. _refresh_forecast_ceiling below is
    what actually closes that gap, on the same interval."""
    try:
        latest_time = get_latest_forecast_time_overall()
    except Exception as e:
        logger.warning("Could not live-refresh latest forecast time: %s", e)
        return None, None, None
    if latest_time is None:
        return None, None, None
    ts = pd.Timestamp(latest_time)
    return latest_time, ts.strftime("%Y-%m-%d"), f"{(ts.hour // 6) * 6:02d}"


# NOTE: the two GLOBAL, country/storm-INDEPENDENT raw hazard layers (raw
# precip-rate raster + raw river flood-extent raster, see
# services/tile_server.py's own "Global raw precipitation-rate endpoints"/
# "Global raw river endpoints" sections) do NOT resolve their own "latest"
# forecast_time here at import time. Both layers' forecast_time must follow
# the topbar's date+time selection instead of always "latest" (see
# get_precip_forecast_time_near/get_river_extent_forecast_time_for_date in
# snowflake_utils.py), and _build_global_raw_config below already fires on
# page load (prevent_initial_call=False) with the topbar's own default
# date/run (_DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN, set just above),
# so a separate import-time resolution here would just be redundant work
# whose result nothing else read.

# See _build_real_storms() near the top of the file for exactly how this is
# derived from Snowflake. "countries" is a LIST, a real storm's track can
# genuinely affect more than one nation, and impact MAT tables are computed
# per-country, so a single TRACK_ID having real data for several countries
# at once isn't hypothetical.
_STORMS = _build_real_storms(_LATEST_FORECAST_TIME)
# Keyed by full category label (get_available_wind_thresholds-derived, via
# _category_label_from_kt), not a bare "Category 1/2/3", _cat_badge below
# still extracts just the digit for its "Cat N" text, so visually this is
# unchanged for hurricane-strength storms; sub-hurricane ones (Tropical
# Storm/Strong Trop. Storm/Severe Trop. Storm) and "Unknown" simply fall
# back to the neutral default color.
_CAT_COLORS = {
    "Category 1 Hurricane": "#f0ac52", "Category 2 Hurricane": "#e8793f",
    "Category 3 Hurricane": "#d94f3c", "Category 4 Hurricane": "#c0392b",
    "Category 5 Hurricane": "#8e1a0f",
}

# Quick-jump presets for testing, sets date, time, country selection, and
# mode all at once. Both correspond to real data (not illustrative/mock):
# MELISSA/Jamaica is a genuine single-storm, wind-only case (17,920 real
# tiles); the BAVI period's own 2026-07-02 06Z cycle is the one date/run
# with real River AND Rainfall data at once (River resolves by calendar
# date alone so any run works; 06Z is Rainfall's own real cycle for this
# date, see get_precip_forecast_time_near/
# get_river_extent_forecast_time_for_date in snowflake_utils.py).
_DEMO_SCENARIOS = [
    {"label": "MELISSA — Jamaica (28 Oct 2025, 00Z)", "date": "2025-10-28", "time": "00",
     "countries": ["Jamaica"], "mode": "zoom"},
    # Global mode (countries=[]), not zoomed into a country, to exercise
    # the two GLOBAL, country/storm-independent raw hazard layers (raw
    # river flood-extent + raw precip-rate rasters, see
    # _build_global_raw_config's own header comment) instead of a
    # country-scoped Gust demo. River's real forecast_time resolves by
    # calendar date alone (any run matches); 06Z specifically is Rainfall's
    # own real cycle for this date, so this is the one date/run
    # combination where both raw flood layers show real data at once in
    # Global mode.
    {"label": "BAVI period — Global flood hazards preview (2 Jul 2026, 06Z)", "date": "2026-07-02", "time": "06",
     "countries": [], "mode": "global"},
    # Same real forecast cycle as the Global entry above, zoomed into
    # Bangladesh specifically (Country Analysis mode): a real flood-only
    # scenario (River Flooding + Rainfall both real and active, no active
    # tropical cyclone), the exact case this session's own Admin Level 1
    # Breakdown / combined-hazard work was built and verified against:
    # all 8 real Bangladesh divisions, real per-region TC-only/Both/
    # Flood-only splits (100% Flood-only here, since no storm is active).
    {"label": "BAVI period — Bangladesh (2 Jul 2026, 06Z)", "date": "2026-07-02", "time": "06",
     "countries": ["Bangladesh"], "mode": "zoom"},
]


def _demo_scenarios_menu():
    # Icon-only, living inside _bottom_left_controls() right next to the
    # basemap switcher, sharing that row instead of its own separate
    # bottom-right floating button, a separate button would leave an
    # empty-looking gap at bottom-right in Country Analysis mode (where
    # this is hidden) and compete with the map legend for the same corner
    # in Global mode. No id/absolute style of its own; _toggle_demo_
    # scenarios_menu below just shows/hides this small wrapper inline
    # within that shared row.
    return html.Div(
        # position="top-start": this row sits only _UI_MARGIN (15px) above
        # the footer, so Mantine's own default Menu position (opens
        # downward) has nowhere near enough room and the dropdown's lower
        # items would render hidden behind/under the footer. Opening
        # upward instead gives it the whole panel column above to work
        # with. zIndex above the footer's own 1000 so it's never visually
        # clipped underneath it either.
        dmc.Menu([
            dmc.MenuTarget(
                dmc.ActionIcon(DashIconify(icon="mdi:flask-outline", width=15),
                                 size="md", variant="white", color=WIND),
            ),
            dmc.MenuDropdown([
                dmc.MenuItem(_t(s["label"]), id={"type": "demo-scenario", "index": i})
                for i, s in enumerate(_DEMO_SCENARIOS)
            ]),
        ], position="top-start", withinPortal=True, zIndex=1001),
        id="demo-scenarios-menu",
    )


def _bottom_left_controls():
    # Basemap switcher + Demo Scenarios icon share one floating row, a
    # separate bottom-right panel for Demo Scenarios would leave an
    # empty-looking gap there whenever it's hidden (Country Analysis mode)
    # and directly collide with the map legend's own natural bottom-right
    # spot in Global mode. There's genuinely enough room in this row
    # (basemap switcher's own segments don't span the full panel width)
    # for both.
    # bottom offset from the shared _BOTTOM_ROW_OFFSET spacing system (see
    # _PANEL_MAX_HEIGHT's own comment), controls-panel's maxHeight is
    # derived FROM this same row's real height, so the two can never
    # overlap regardless of which side changes.
    return html.Div([
        dmc.SegmentedControl(
            id="basemap-select",
            # Matches initMaplibre()'s own token-conditional default tiles
            # (maplibre_tiles.js): with a real Mapbox token, the map already
            # renders Mapbox Light tiles on first load regardless of this
            # value, so the pill needs to start on "osm" (→ "Mapbox Light")
            # to avoid showing "Light" selected while different tiles are
            # actually on screen.
            value="osm" if mapbox_token else "cartodb-light",
            data=_basemap_options(),
            size="xs",
        ),
        _demo_scenarios_menu(),
    ], style={**_PANEL_STYLE, "bottom": _BOTTOM_ROW_OFFSET, "left": f"{_UI_MARGIN}px", "padding": "4px",
               "display": "flex", "alignItems": "center", "gap": "6px"})


_LANGUAGES = [
    {"code": "en", "label": "English"},
    {"code": "es", "label": "Español"},
    {"code": "fr", "label": "Français"},
    {"code": "bn", "label": "বাংলা"},
]


def _language_switcher(lang):
    # Each option is a plain <a href>, not a Dash callback, switching
    # language re-invokes layout(lang=...) fresh on full page load, so every
    # static string on the page (built once, at layout-build time) picks up
    # the new language too, not just whatever a reactive callback touches.
    # Lives inline as the last item in the top bar (top-right corner), not a
    # separate floating panel, same tier as the other top-bar controls.
    current = next((l for l in _LANGUAGES if l["code"] == lang), _LANGUAGES[0])
    return dmc.Menu([
        dmc.MenuTarget(dmc.Button(
            current["label"], size="xs", variant="white", color="dark",
            leftSection=DashIconify(icon="carbon:language", width=13),
        )),
        dmc.MenuDropdown([
            dmc.MenuItem(
                dcc.Link(l["label"], href=f"/?lang={l['code']}", refresh=True,
                          style={"color": "inherit", "textDecoration": "none"}),
                disabled=(l["code"] == lang),
            ) for l in _LANGUAGES
        ]),
    # Same z-index bump as the date picker/country select above, for the
    # same reason, the top bar itself sits at zIndex 1000, which was
    # cutting into this dropdown's own (lower, Mantine-default) z-index.
    ], zIndex=2000)

# Real REGION_MEMBERS bundle (snowflake/mat_tables/02b_add_regional_group.sql:34-48,
# a "registered 2026-04-17, example for reference" template), East
# Caribbean Area, member ISO codes AIA/ATG/BRB/VGB/DMA/GRD/MSR/KNA/LCA/VCT/
# TTO/TCA (12 countries). Illustrated here with 3 of the real 12 (the ones
# ELARA (see _STORMS) already affects), not the full list, to keep the
# mock data manageable; the pattern (aggregate region + drillable members)
# is the same either way.
_ECA_MEMBERS = ["Antigua and Barbuda", "Saint Lucia", "Saint Vincent and the Grenadines"]

# Multi-select: the country picker holds a LIST of selected countries.
# Selecting a multi-country storm (BAVI) selects all its countries at once;
# the Impact Summary panel then shows one stat block per selected country,
# side by side, rather than forcing a single arbitrary pick.
#
# Real REGION_MEMBERS pattern (Pacific Islands, ECA, see get_countries in
# components/data/snowflake_utils.py + pages/dashboard.py's COUNTRY_OPTIONS):
# a fixed bundle queried/selected as ONE unit, grouped separately from plain
# countries in the dropdown ("Regions" vs "Countries"). Kept in the SAME
# multi-select as individual countries (not the real app's separate
# individual-country-select drill-down) so the region and the flexible
# ad-hoc multi-select both stay available from one control, pick "ECA" for
# the aggregate, or pick "Saint Lucia" on its own, or mix both.
def _country_options():
    """Real country/region dropdown, built from get_active_countries()
    (PIPELINE_COUNTRIES) at module import time, same source pages/dashboard.py
    uses for its own COUNTRY_OPTIONS. COUNTRY_NAME is used as both value and
    label (not COUNTRY_CODE) so every dict-keyed-by-display-name lookup
    elsewhere on this page keeps working unchanged. IS_REGION splits real
    regional bundles (e.g. ECA) into their own group, same as dashboard.py.
    """
    if _countries_df.empty:
        return [{"group": _t("Countries"), "items": []}]
    is_region = _countries_df['IS_REGION'] if 'IS_REGION' in _countries_df.columns else False
    regions_df = _countries_df[is_region == True]
    countries_only_df = _countries_df[is_region != True]
    country_items = [{"value": r['COUNTRY_NAME'], "label": _t(r['COUNTRY_NAME'])}
                      for _, r in countries_only_df.iterrows()]
    groups = [{"group": _t("Countries"), "items": country_items}]
    if not regions_df.empty:
        region_items = [{"value": r['COUNTRY_NAME'], "label": _t(r['COUNTRY_NAME'])}
                         for _, r in regions_df.iterrows()]
        groups.append({"group": _t("Regions"), "items": region_items})
    return groups

# Fallback baseline only, used when NOTHING has been selected yet at all
# (no country picked in Global mode), an illustrative "here's the shape of
# this panel" skeleton for that specific pre-selection state, same as the
# rest of this page's "no real data available" fallbacks (e.g. metrics.py's
# own _NA_VALUE).
#
# Do NOT use this as _get_country_stats/_get_country_pin_pct's own fallback
# for a REAL, already-selected country/storm/hazard query that happens to
# produce no frames (e.g. a wind severity threshold this specific storm's
# forecast never reached), that's a genuine, real ZERO, not "nothing
# selected yet," and showing this mock there would fabricate a materially
# wrong number (see _get_country_stats's own
# docstring), _ZERO_STATS/_ZERO_PIN_PCT below are for that case instead.
_DEFAULT_STATS = {"Children at Risk": "218K", "People at Risk": "640K", "Schools at Risk": "185", "Health Centers at Risk": "46", "Shelters at Risk": "62", "WASH Facilities at Risk": "134"}
_ZERO_STATS = {k: "0" for k in _DEFAULT_STATS}

# Real per-age labels for the Full Impact Breakdown table's child-age-band
# rows, sourced from real per-age Snowflake columns
# (E_infant_population/E_school_age_population/E_adolescent_population,
# and their *_in_need siblings), which are fetched and summed into one
# "children" scalar by _fetch_real_combined_tile_totals_uncached/
# _real_member_stats. See _get_country_age_split/
# _real_member_age_split/_combined_age_split for the per-age breakdown.
_CHILD_AGE_BANDS = ["Age 0–4 (Infant)", "Age 5–14 (School-age)", "Age 15–19 (Adolescent)"]
_ZERO_AGE_SPLIT = {label: {"at_risk": 0.0, "in_need_pct": 0, "in_need_abs": 0.0} for label in _CHILD_AGE_BANDS}

# Icon per stat, Material Design Icons (mdi:*) via Iconify, since carbon's
# set (used for the rest of this page's chrome icons) doesn't cover
# school/hospital/shelter/water-specific glyphs consistently.
_STAT_ICONS = {
    "People at Risk": "mdi:account-group",
    "Children at Risk": "mdi:human-child",
    "Schools at Risk": "mdi:school",
    "Health Centers at Risk": "mdi:hospital-box",
    "Shelters at Risk": "mdi:home-group",
    "WASH Facilities at Risk": "mdi:water-pump",
}


def _mat_forecast_date(date, run):
    """topbar-date value ('YYYY-MM-DD') + topbar-time value (run:
    '00'/'06'/'12'/'18') -> the 'YYYYMMDDHH24MISS' string the *_MAT tables'
    own FORECAST_DATE column actually uses (e.g.
    '20251028000000' for MELISSA/Jamaica 28 Oct 2025 00Z)."""
    return f"{date.replace('-', '')}{run}0000"


def _resolve_storm_for_country(country, date=None, run=None):
    """REACTIVE replacement for `next((s for s in _STORMS if country in
    s["countries"]), None)`, answers "what storm has real impact data for
    THIS country at THIS selected topbar date/run", not "what's currently
    active" (that question is still correctly answered by _STORMS/
    _build_real_storms, used only by the Global-mode Active Storms list).

    Falls back to _DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN (the latest
    real forecast cycle, same default the topbar itself opens on) when date/
    run aren't supplied, e.g. for callers outside a callback that has the
    live topbar-date/topbar-time values.

    Returns a dict shaped like a _STORMS entry ({"name", "cat"}) plus the two
    date-string forms downstream callers need: "forecast_time"
    ("YYYY-MM-DD HH:MM:SS", matching TC_TRACKS/TC_ENVELOPES_COMBINED's own
    FORECAST_TIME timestamp column and get_available_wind_thresholds' own
    expected format) and "mat_forecast_date" ("YYYYMMDDHH24MISS", matching
    the *_MAT tables' own FORECAST_DATE string column), or None when there's
    genuinely no real data for this country/date/run combination.
    """
    code = _NAME_TO_CODE.get(country)
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    if not code or not date or run is None:
        return None
    mat_date = _mat_forecast_date(date, run)
    try:
        storms = get_storms_for_country_date(code, mat_date)
    except Exception as e:
        logger.warning("Could not resolve storm for %s/%s: %s", country, mat_date, e)
        return None
    if not storms:
        return None
    storm_name = storms[0]
    forecast_time_str = f"{date} {run}:00:00"
    cat = _category_label_ensemble_max(storm_name, forecast_time_str)
    return {"name": storm_name, "cat": cat, "forecast_time": forecast_time_str, "mat_forecast_date": mat_date}


@ttl_cache(ttl_seconds=900, maxsize=64)
def _resolve_storms_for_date(date=None, run=None):
    """Every REAL storm with impact data at the given topbar date/run,
    across ALL countries, the date-reactive replacement for filtering the
    frozen `_STORMS`/`_build_real_storms` "active right now" snapshot in
    `_active_storms_section`. This is what lets a historical Demo Scenario
    date (e.g. MELISSA on 28 Oct 2025) show the exact same bordered
    storm-row box (name, category badge, alert-email icon) the Global-mode
    Active Storms list already shows for genuinely-live storms, same UI,
    just sourced from "what's real on this date" instead of "what's
    happening in the last 12h".

    Falls back to _DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN when date/run
    aren't supplied, same convention as _resolve_storm_for_country.

    Returns a list shaped exactly like `_STORMS` ({"name", "countries",
    "date", "cat"}), one entry per distinct storm, countries as real
    display names (via _CODE_TO_NAME), PLUS two extra keys every entry
    already carries internally: "forecast_time" ("YYYY-MM-DD HH:MM:SS",
    matching TC_TRACKS' own FORECAST_TIME column) and "mat_forecast_date"
    ("YYYYMMDDHH24MISS", matching the *_MAT tables' own FORECAST_DATE
    column), same two extra keys _resolve_storm_for_country already
    returns, so callers that need to fetch this storm's own real track/
    envelope data (e.g. the Global-mode multi-storm track query) don't have
    to re-derive them. All storms from one call currently share the same
    forecast_time (this function resolves ONE mat_date from the given
    date/run and queries every storm real at that exact date/run), but
    each entry still carries its own copy rather than callers assuming a
    single shared value, in case that ever changes. Empty list when
    there's genuinely no real data for this date.

    Cached because this is called (with the same date/run) by
    _active_storms_section, _hurricane_family, _flood_hazards_family, and
    _resolve_stat_value's own "global" branch, several of which run inside
    the same request via _controls_global/_controls_zoom's own concurrent
    fan-out. @ttl_cache (single-flight per key, see its own docstring in
    snowflake_utils.py) means the second-and-later callers for the same
    (date, run) hit cache instead of redoing this function's own Snowflake
    round-trips.
    """
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    if not date or run is None:
        return []
    mat_date = _mat_forecast_date(date, run)
    forecast_time_str = f"{date} {run}:00:00"
    try:
        # 34kt (not the 50kt used elsewhere for precise impact numbers),
        # the loosest/broadest tropical-storm-force wind threshold, so this
        # "who's potentially impacted" list casts as wide a net as the real
        # forecast data supports, rather than only surfacing countries that
        # clear a higher, more severe bar.
        pairs = get_storms_and_countries_for_date(mat_date, wind_threshold=34)
    except Exception as e:
        logger.warning("Could not resolve storms for date %s: %s", mat_date, e)
        pairs = []
    by_storm = {}
    for row in pairs:
        by_storm.setdefault(row["STORM"], []).append(row["COUNTRY"])
    # Real storms that have a track for this exact date/run but haven't hit
    # measurable country impact yet (e.g. still at sea) are otherwise
    # invisible here even though the Tropical Cyclone header already shows
    # them as active (get_track_ids_for_date, the same track-existence
    # resolver, zero impact requirement, see get_track_ids_for_date's own
    # docstring). Add them with an empty countries list rather than dropping
    # them, so this list never disagrees with the header dot above it and
    # so real, currently-active storms with a track but no measurable
    # impact yet still appear in Active Storms.
    try:
        for name in get_track_ids_for_date(forecast_time_str):
            by_storm.setdefault(name, [])
    except Exception as e:
        logger.warning("Could not resolve track-only storms for %s: %s", forecast_time_str, e)
    if not by_storm:
        return []
    try:
        date_str = pd.Timestamp(forecast_time_str).strftime("%a %-d %b %Y")
    except Exception:
        date_str = date

    def _build_storm_entry(item):
        storm_name, codes = item
        cat = _category_label_ensemble_max(storm_name, forecast_time_str)
        country_names = [_CODE_TO_NAME.get(c, c) for c in codes]
        return {
            "name": storm_name, "countries": country_names, "date": date_str, "cat": cat,
            "forecast_time": forecast_time_str, "mat_forecast_date": mat_date,
        }

    # Real storms with a track for the same date/run are independent of
    # each other, so fetching each one's _category_label_ensemble_max
    # (-> _ensemble_max_kt, its own Snowflake round-trip on a cold cache)
    # concurrently via the shared executor turns N sequential round-trips
    # into ~1, rather than calling it once per storm sequentially.
    return list(get_query_executor().map(_build_storm_entry, by_storm.items()))


def _resolve_wind_kt(wind_idx):
    """Real currently-selected wind-severity threshold (kt), same
    resolution _build_hazard_tile_config already uses to derive the map's
    own wind tile layer (ms-wind-slider's index, default 2 == 'Severe Trop.
    Storm', into _WIND_CATS). Threaded into _fetch_real_tile_totals (and
    everything that calls it) so dragging the slider actually changes the
    real numbers shown on the page, rather than always using a fixed
    50kt regardless of the slider's value."""
    wind_idx = wind_idx if wind_idx is not None else 2
    return _WIND_CATS[wind_idx][2]


def _resolve_gust_kt(gust_idx):
    """Real currently-selected gust-severity threshold (kt), _WIND_CATS rows
    are (label, name, wind_kt, gust_kt), so this is index [3], NOT [2] (that's
    wind's own kt). Using wind's kt value (index [2]) for the GUST_THRESHOLD
    column filter would be wrong, wind kt values (34/40/50/64/83/96/
    113/137) and real gust kt values (17/21/26/33/43/49/58/70) never overlap,
    so that join would never match a single real row (e.g. BAVI/PHL
    2026-07-02 has 0 gust rows at kt=50, but real nonzero rows at kt=26,
    the correct index-[3] value for the same slider position)."""
    gust_idx = gust_idx if gust_idx is not None else 2
    return _WIND_CATS[gust_idx][3]


# "Multi-source ready" preparation: only one real source per hazard exists
# today, but the STATE MODEL is built so a future second source per
# hazard, e.g. Google FloodHub alongside GloFAS for River, Google
# WeatherNext alongside ECMWF for Wind, is additive later, not a retrofit
# across every hz-reading call site. Every hz dict below carries a
# `{hazard}_source` key alongside its existing `{hazard}_on`/threshold
# keys, always set to today's one real source. A single-option preview
# Select exists per hazard in the controls panel (see
# _hazard_source_select below), deliberately not yet wired to actually
# change what data loads, since that needs the real pipeline/methodology
# work described below first.
#
# HAZARD_SOURCE_ECMWF/HAZARD_SOURCE_GLOFAS are imported from
# snowflake_utils rather than re-hardcoded as separate literal strings
# here, that module owns the actual validation (get_wind_tile_bitmask
# etc. each raise NotImplementedError for any other value), so importing
# its constants means the two can never silently drift apart the way two
# independently-typed "ecmwf" strings could.
#
# Deliberately UNSOLVED problem, flagged not hidden: River today shares
# GloFAS, which is itself driven by the SAME ECMWF ensemble Wind/Rain use,
# so a real member (e.g. "member 17") means
# the same physical realization across all three today, that's what
# makes the per-member bitmask union (_flood_combine_active et al.)
# correct. A genuinely different future source (FloodHub is NOT
# ECMWF-driven) would very likely NOT share that same member identity, so
# combining "Wind from ECMWF member 17" with "River from FloodHub" would
# need a real, separate decision about what "combining" even means then,
# not solved by this plumbing, deliberately deferred.
_DEFAULT_HAZARD_SOURCES = {
    "wind": HAZARD_SOURCE_ECMWF, "gust": HAZARD_SOURCE_ECMWF,
    "river": HAZARD_SOURCE_GLOFAS, "rain": HAZARD_SOURCE_ECMWF,
}


def _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx=None, gust_idx=None,
               river_idx=None, rain_idx=None, rain_window=None, river_window=None):
    """Builds the real multi-hazard `hz` dict _fetch_real_combined_tile_totals
    (via _get_country_stats/_get_country_pin_pct/_combined_stats/
    _combined_in_need_total) expects, resolving each hazard's own slider
    index into its real threshold value, exactly the same resolution
    _build_hazard_tile_config already uses for the map's own tile layers, so
    the Impact Summary/Breakdown numbers always match what the map itself is
    currently showing for wind_kt/gust_kt/rp_tier/threshold_mm.

    `river_window`: River's real per-country impact numbers
    (population/schools/HCs/shelters/WASH) carry a real STEP_H column
    meaning a CUMULATIVE lead-time window (24/72/120/168h),
    not a single-day snapshot or an unconditional "worst case across the
    entire horizon", mirrors `rain_window`'s own threading exactly.
    Defaults to _RIVER_WINDOW_DEFAULT (the full 168h horizon) when None,
    same EXACT-not-approximate backward-compat reasoning as tile_server.py's
    own _RIVER_WINDOW_DEFAULT.
    """
    # Default index 2 == "rp10", the real, non-placeholder river return-
    # period tier (rp2/rp5 are IS_STANDIN=True placeholders, see
    # ms-river-slider's own default and _RIVER_RP_TIERS).
    river_idx = river_idx if river_idx is not None else 2
    # ms-river-window's own Dash value is
    # a STRING ("72", from its SegmentedControl `data`, same shape as
    # ms-river-window's own UI options), unlike rain_window (which stays a
    # string throughout this whole app, since _RAIN_MM_BY_WINDOW's keys are
    # strings), river_window is used purely as a real int downstream (SQL
    # STEP_H bind params, cache-key tuples alongside _RIVER_WINDOW_DEFAULT
    # itself, which IS an int), cast explicitly here, the one place every
    # hz-dict consumer's river_window ultimately comes from, rather than
    # risking a str/int type mismatch silently splitting cache entries or
    # reaching a SQL bind param inconsistently typed.
    river_window = int(river_window) if river_window else _RIVER_WINDOW_DEFAULT
    # "6"/2 ("6h"/"Extreme rain", 75mm), matching ms-rain-window/
    # ms-rain-slider's own live UI defaults, so every fallback used when a
    # caller has no live slider state (Global scope defaults, the print
    # page, etc.) mirrors the same default.
    rain_window = rain_window or "6"
    rain_idx = rain_idx if rain_idx is not None else 2
    return {
        "wind_on": bool(wind_on), "gust_on": bool(gust_on),
        "river_on": bool(river_on), "rain_on": bool(rain_on),
        "wind_kt": _resolve_wind_kt(wind_idx),
        "gust_kt": _resolve_gust_kt(gust_idx),
        "rp_tier": _RIVER_RP_TIERS[river_idx],
        "river_window": river_window,
        "rain_mm": _RAIN_MM_BY_WINDOW[rain_window][rain_idx],
        "rain_window": rain_window,
        # "Multi-source ready" plumbing, see _DEFAULT_HAZARD_SOURCES's own
        # comment above. No selector exists yet; always today's one real
        # source per hazard.
        "wind_source": _DEFAULT_HAZARD_SOURCES["wind"],
        "gust_source": _DEFAULT_HAZARD_SOURCES["gust"],
        "river_source": _DEFAULT_HAZARD_SOURCES["river"],
        "rain_source": _DEFAULT_HAZARD_SOURCES["rain"],
    }


def _wind_only_hz(wind_kt=None):
    """Default hazard-state bundle for every call site that hasn't been
    updated to pass a real multi-hazard `hz`, preserves the exact old
    wind-only behavior (see _get_country_stats/_get_country_pin_pct below)."""
    return {"wind_on": True, "gust_on": False, "river_on": False, "rain_on": False,
            "wind_kt": wind_kt, "gust_kt": None, "rp_tier": None, "river_window": None,
            "rain_mm": None, "rain_window": None,
            # "Multi-source ready" plumbing, see _build_hz's own
            # _DEFAULT_HAZARD_SOURCES comment.
            "wind_source": _DEFAULT_HAZARD_SOURCES["wind"], "gust_source": _DEFAULT_HAZARD_SOURCES["gust"],
            "river_source": _DEFAULT_HAZARD_SOURCES["river"], "rain_source": _DEFAULT_HAZARD_SOURCES["rain"]}


def _river_only_hz(rp_tier=None, river_window=None):
    """River-only counterpart to _wind_only_hz, used by _hazard_curve_row's
    real per-return-period-tier threshold curve (see that function's own
    docstring for why each hazard's curve isolates that ONE hazard rather
    than reusing whatever combined hz the popup itself was opened with).
    `river_window` defaults to _RIVER_WINDOW_DEFAULT downstream (see
    _build_hz's own comment) whenever left None here; cast to int here too
    (see _build_hz's own comment on why, this is a second, independent
    entry point for the same str-from-Dash value)."""
    return {"wind_on": False, "gust_on": False, "river_on": True, "rain_on": False,
            "wind_kt": None, "gust_kt": None, "rp_tier": rp_tier,
            "river_window": int(river_window) if river_window else None,
            "rain_mm": None, "rain_window": None,
            # "Multi-source ready" plumbing, see _build_hz's own
            # _DEFAULT_HAZARD_SOURCES comment.
            "wind_source": _DEFAULT_HAZARD_SOURCES["wind"], "gust_source": _DEFAULT_HAZARD_SOURCES["gust"],
            "river_source": _DEFAULT_HAZARD_SOURCES["river"], "rain_source": _DEFAULT_HAZARD_SOURCES["rain"]}


def _global_flood_availability(date, run, river_idx=None, rain_idx=None, rain_window=None, river_window=None):
    """Real per-date River/Rain availability for Global scope.

    Fixes two real bugs found live (PHL/BGD showing zero impact in the
    Global Impact Summary despite genuinely having real, large Rainfall
    exposure that same forecast cycle):

    BUG A (country roster was wind/TC-track-only): `all_country_names` used
    to come ONLY from _resolve_storms_for_date, itself built from
    MERCATOR_TILE_IMPACT_MAT (wind) + TC_TRACKS -- there was no path for a
    country whose only real exposure is River/Rain to ever be counted, even
    though this function's own river_avail/rain_avail correctly identified
    River/Rain as globally available that date. Now unions in every real
    country get_countries_with_river_impact_at/get_countries_with_precip_
    impact_at themselves return, so a flood-only country (no active/tracked
    storm at all) is no longer silently dropped from the Global total.

    BUG B (availability checked the wrong table): river_avail/rain_avail
    used to come from get_river_extent_forecast_time_for_date/
    get_precip_forecast_time_near, which check RIVER_FORECASTS/MET_FORECASTS
    -- the RAW ingestion-log tables -- not MERCATOR_TILE_RIVER_MAT/
    MERCATOR_TILE_PRECIP_MAT, the real per-country IMPACT tables
    combined_country_totals (services/tile_server.py) actually reads from.
    Those two layers can genuinely disagree (the raw layer shows a cycle
    ingested while the downstream impact MAT hasn't caught up to that exact
    cycle yet), so "the raw layer has this date" was never sufficient proof
    the real per-country numbers this function's own callers go on to fetch
    would come back non-empty. Now river_avail/rain_avail are derived from
    whether get_countries_with_river_impact_at/get_countries_with_precip_
    impact_at themselves returned at least one real country at the EXACT
    (forecast_time, rp_tier/threshold_mm, window) combination that will
    actually be queried -- the raw-layer resolvers are still used (correctly)
    to resolve WHICH forecast_time to check, just no longer trusted alone to
    answer "is there real impact data" for it.

    `river_idx`/`rain_idx`/`rain_window`/`river_window` (all optional, same
    meaning as _build_hz's own params) let this resolve the EXACT threshold
    the caller's own subsequent _build_hz call will use, rather than a
    threshold-independent existence check that could drift from what's
    actually queried. Duplicates _build_hz's own tiny default-resolution
    snippet rather than delegating to it: _build_hz's signature bundles
    that resolution together with wind_on/river_on/rain_on flags this
    function doesn't have yet -- it's what DECIDES river_on/rain_on for the
    caller's own _build_hz call right after this returns, so computing them
    from a river_on/rain_on-dependent call would be circular.

    Returns (all_country_names, river_avail, rain_avail), shared by both
    Global's own Impact Summary total and the Hazard Contribution popup so
    they never disagree on what's real for a given date.

    BUG C (fixed, real quiet-cycle case): `all_country_names` used to come
    ONLY from countries that already clear a non-zero-impact HAVING gate
    (wind_country_names from _resolve_storms_for_date's own impact-gated
    `countries`, river/rain from get_countries_with_river_impact_at/get_
    countries_with_precip_impact_at). On a real cycle where every active
    hazard genuinely produces zero exposure everywhere (a real, not
    fabricated, "quiet" answer, confirmed live for Aug 21 2026 00Z: SAUDEL/
    LALA both have real tile rows for PHL/BGD with population exposure of
    exactly 0.0 at every wind threshold, and real precip data with zero
    countries clearing the default 75mm/6h bar), that made ALL THREE
    sources empty simultaneously, so `all_country_names` came back [], and
    _combined_stats' own `if not countries: return {k: None...}` early-
    return (correct for its OWN contract: "no countries to even consider")
    fired -- producing N/A across every Impact Summary tile even though
    real countries genuinely WERE examined and genuinely DO have a real
    zero. Now uses get_storms_and_countries_examined_for_date/get_
    countries_examined_for_river_at/get_countries_examined_for_precip_at
    (this file's own "examined" siblings of the impact-gated functions
    above): every country with ANY real tile row for this exact hazard/
    date/threshold, regardless of whether the resulting exposure happens
    to be exactly zero, so a real worldwide-quiet cycle now correctly
    shows "0" through _combined_stats' own has_real tracking instead of
    N/A. river_avail/rain_avail switch to the same "examined" source for
    the identical reason: whether real river/rain DATA was genuinely
    computed for this cycle, not whether someone currently shows nonzero
    risk, so Global's own combined hz still marks a genuinely-quiet hazard
    "on" (and reports its real 0) rather than treating it as unavailable
    and omitting it entirely."""
    mat_date = _mat_forecast_date(date, run) if (date and run is not None) else None
    wind_pairs = get_storms_and_countries_examined_for_date(mat_date) if mat_date else []
    wind_country_names = {_CODE_TO_NAME.get(row["COUNTRY"], row["COUNTRY"]) for row in wind_pairs}

    rp_tier = _RIVER_RP_TIERS[river_idx if river_idx is not None else 2]
    river_window_resolved = int(river_window) if river_window else _RIVER_WINDOW_DEFAULT
    # "6"/2, matching ms-rain-window/ms-rain-slider's own live UI
    # defaults (see _build_hz's own comment on this same fallback).
    rain_window_resolved = rain_window or "6"
    rain_mm = _RAIN_MM_BY_WINDOW[rain_window_resolved][rain_idx if rain_idx is not None else 2]

    river_forecast_time = get_river_extent_forecast_time_for_date(date, rp_tier) if date else None
    river_countries = (
        get_countries_examined_for_river_at(_mat_forecast_date(date, "00"), rp_tier, river_window_resolved)
        if river_forecast_time else []
    )
    rain_forecast_time = get_precip_forecast_time_near(date, run) if (date and run is not None) else None
    rain_countries = (
        get_countries_examined_for_precip_at(_mat_forecast_date(date, run), rain_mm, int(rain_window_resolved))
        if rain_forecast_time else []
    )

    river_avail = bool(river_countries)
    rain_avail = bool(rain_countries)
    flood_country_names = {_CODE_TO_NAME.get(c, c) for c in river_countries + rain_countries}
    all_country_names = sorted(wind_country_names | flood_country_names)
    return all_country_names, river_avail, rain_avail


# Common exposure-column name set every hazard's own tile dataframe gets
# normalized/renamed into (see _norm_hazard_tile_df) before being merged in
# _fetch_real_combined_tile_totals, lets wind/gust/river/rain dataframes
# (different underlying MAT tables, different join keys, different real
# column coverage) be outer-merged on zone_id and combined without one
# hazard's columns colliding with another's.
_HAZARD_TILE_EXPOSURE_COLS = [
    'E_population', 'E_infant_population', 'E_school_age_population', 'E_adolescent_population',
    'E_num_schools', 'E_num_hcs', 'E_num_shelters', 'E_num_wash',
]


def _norm_hazard_tile_df(df):
    """Uppercase-E_-prefix + lowercase-rest column normalization, same
    convention get_impact_data() already applies to wind's own tile data
    (components/data/data_store_utils.py's _norm), reused here so every
    hazard's raw Snowflake dataframe (columns come back all-uppercase, e.g.
    E_POPULATION) ends up using the same 'E_population'/'zone_id' names before
    being merged."""
    if df is None or df.empty:
        return None
    out = df.copy()
    out.columns = [('E_' + c[2:].lower()) if c.upper().startswith('E_') else c.lower() for c in out.columns]
    return out


def _fetch_real_combined_tile_totals(country, date=None, run=None, hz=None):
    """Cached entry point for _fetch_real_combined_tile_totals_impl below.

    `hz` is a plain dict, not hashable, so it can't be passed straight
    through @ttl_cache, converted here to a sorted tuple-of-items key
    instead. This avoids an N+1 hotspot: a single Global Hazard
    Contribution popup open calls this function (via _combined_stats +
    _combined_in_need_total, each called twice, once for people, once
    for children) up to ~5x PER COUNTRY, so caching means those calls
    reuse the outer-join/row-wise-MAX pandas merge result rather than
    redoing it from scratch each time, as long as every underlying
    Snowflake query it depends on is already warm in cache.
    """
    hz = hz or _wind_only_hz()
    hz_key = tuple(sorted(hz.items()))
    return _fetch_real_combined_tile_totals_impl(country, date, run, hz_key)


@ttl_cache(ttl_seconds=900, maxsize=512)
def _fetch_real_combined_tile_totals_impl(country, date, run, hz_key):
    return _fetch_real_combined_tile_totals_uncached(country, date, run, dict(hz_key))


def _fetch_real_combined_tile_totals_uncached(country, date=None, run=None, hz=None):
    """Real per-country 'at risk' aggregates COMBINED across every ACTIVE
    hazard counted toward a total (Wind/River/Rain; Storm Surge has no real
    backend anywhere, see ms-surge-on's own comment, and never contributes
    here). Gust deliberately never reaches this function's own `params`
    dict at all, regardless of `hz["gust_on"]"/`hz["gust_kt"]` -- see
    services/tile_server.py's own `_UNION_EXCLUDED_HAZARDS` for the full
    rationale (gust is a reference-only hazard layer, excluded from every
    combined impact number). This is a client-side optimization on top of
    that server-side exclusion, not a second, independent gate: the server
    would discard gust's contribution from the union either way, so this
    function skips resolving a gust threshold and paying for a real
    per-member gust bitmask fetch that would only be thrown away. Restoring
    gust here is only meaningful once `_UNION_EXCLUDED_HAZARDS` no longer
    excludes it server-side too.

    Combination method: delegates to services/tile_server.py's
    combined_country_totals, which computes the real per-tile bitmask
    union (_combine_bitmask_aware, `tile_mask=None`, every real z14 tile
    in the country) and multiplies by the RAW population/facility columns,
    summed, a true expected-value total under "the real fraction of the
    51-member ensemble where AT LEAST ONE COUNTED active hazard hits this
    tile", the SAME methodology the combined raster/facility-marker/tile-
    tooltip paths already use (those paths do still resolve and send a real
    gust threshold when gust is checked, for its own reference-row display,
    but the server-side union they feed excludes it the same way). See that
    endpoint's own docstring for the full methodology writeup.

    People/Children In Need (PIN/CHIN) stays wind-only regardless of which
    other hazards are active, no vulnerability/CCI pipeline exists for gust/
    river/rain (services/tile_server.py's MERCATOR_TILE_GUST_MAT comment), so
    there's no real per-hazard in-need number to combine; if wind itself
    isn't active, PIN/CHIN is 0 (no other real source for it exists).
    Computed HERE (not delegated), directly off get_tile_impacts.

    `hz` is a dict from _wind_only_hz() (or an equivalent multi-hazard dict
    built by a real hazard-aware caller), {"wind_on", "gust_on", "river_on",
    "rain_on", "wind_kt", "gust_kt", "rp_tier", "rain_mm", "rain_window"}.
    `gust_on`/`gust_kt` are still accepted (the map's own tile/raster/
    tooltip requests need them for their own reference-row resolution) but
    read nowhere below.

    Returns None (callers fall back to _DEFAULT_STATS/_DEFAULT_PIN_PCT, same
    convention as before) when no hazard is active, or none of the active
    hazards resolve to any real data for this country/date.
    """
    code = _NAME_TO_CODE.get(country)
    if not code:
        return None

    people_in_need = 0.0
    children_in_need = 0.0
    age_in_need = {"Age 0–4 (Infant)": 0.0, "Age 5–14 (School-age)": 0.0, "Age 15–19 (Adolescent)": 0.0}
    _AGE_IN_NEED_COL = {"Age 0–4 (Infant)": "E_infant_in_need", "Age 5–14 (School-age)": "E_school_age_in_need",
                          "Age 15–19 (Adolescent)": "E_adolescent_in_need"}

    # Only wind_on gates storm_info here (not gust_on too): gust never
    # contributes to this function's own combined total (see this
    # function's own docstring), so there is no reason to pay for a real
    # storm-lookup on gust_on's account alone when wind is off.
    storm_info = _resolve_storm_for_country(country, date, run) if hz["wind_on"] else None

    def _resolve_threshold(name, target_kt):
        try:
            thresholds = get_available_wind_thresholds(storm_info["name"], str(storm_info["forecast_time"]))
            numeric = sorted(int(t) for t in thresholds if t.isdigit())
        except Exception as e:
            logger.warning("Could not load %s thresholds for %s: %s", name, country, e)
            return None
        if not numeric:
            return None
        target_kt = target_kt if target_kt is not None else 50
        # Must NOT silently fall back to numeric[0] (the LOWEST available
        # threshold) when the requested target_kt isn't one of this
        # storm's own real, computed thresholds, e.g. selecting Category
        # 5 (137kt) for a storm whose real envelope data only goes up to
        # Category 4 (113kt, because it never forecast to reach Cat 5
        # anywhere) would then silently return the 34kt result MISLABELED
        # as the 137kt one, fabricating a materially wrong number (this
        # would surface as the SAME "at risk" figure appearing at every
        # threshold above the storm's real maximum, an impossible
        # non-monotonic artifact, see _hazard_curve_row, which probes
        # this same resolution across every threshold for the real
        # per-hazard threshold curve). A threshold this storm's real
        # forecast never reached genuinely has no data to report, return
        # None (no frame contributed for this hazard at this specific
        # threshold) rather than substituting a different, lower
        # threshold's real number under the wrong label.
        return target_kt if target_kt in numeric else None

    wind_threshold = None
    if hz["wind_on"] and storm_info:
        wt = _resolve_threshold("wind", hz["wind_kt"])
        if wt is not None:
            wind_threshold = wt
            df = _norm_hazard_tile_df(get_tile_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], wt, 14))
            if df is not None and not df.empty:
                # Column EXISTENCE alone ('E_people_in_need' in df.columns)
                # is not enough to treat a value as a confirmed zero:
                # E_people_in_need/E_children_in_need can genuinely exist
                # as real columns for a country but be 100% NULL across
                # every row (a genuine missing-vulnerability-data gap for
                # that country), and pandas .sum() silently treats an
                # all-NaN column as 0.0, indistinguishable from "we
                # checked and confirmed zero people in need". .notna().any()
                # distinguishes "has at least one real value" (real sum,
                # even if that sum happens to be exactly 0) from "entirely
                # empty" (None, rendered as N/A downstream, see
                # _simple_breakdown_table's/_admin1_table's own N/A
                # handling). The two OTHER real-zero cases, wind being off
                # entirely, or this specific threshold/storm genuinely
                # having zero rows at all (this block never runs), stay
                # real, intentional zeros; only "we have real rows but this
                # ONE in-need column is empty" is distinguished here.
                if 'E_people_in_need' in df.columns:
                    people_in_need = float(df['E_people_in_need'].sum()) if df['E_people_in_need'].notna().any() else None
                if 'E_children_in_need' in df.columns:
                    children_in_need = float(df['E_children_in_need'].sum()) if df['E_children_in_need'].notna().any() else None
                for age_label, col in _AGE_IN_NEED_COL.items():
                    if col in df.columns:
                        age_in_need[age_label] = float(df[col].sum()) if df[col].notna().any() else None

    # Gust deliberately has no threshold resolved here at all -- see this
    # function's own docstring (_UNION_EXCLUDED_HAZARDS in
    # services/tile_server.py is the real, server-side exclusion; this is
    # just not paying for a fetch that would be discarded there anyway).

    # river/rain forecast_date resolved here in MAT format (same convention
    # ms-tile-config-store already uses for the raster/facility/tooltip
    # paths), for the ACTUAL selected topbar date/run -- get_river_extent_
    # forecast_time_for_date/get_precip_forecast_time_near, the SAME real,
    # already-correct resolvers the raw layers use, NOT get_latest_river_
    # forecast_time/get_latest_rain_forecast_time (both MAX(FORECAST_TIME)
    # for the country, ignoring `date`/`run` entirely -- a real bug: this
    # function's own combined totals would silently use whatever the
    # country's all-time-latest River/Rain cycle happens to be regardless
    # of which historical date/run was actually selected, a completely
    # different real-world event, not "the requested date's data, stale by
    # a few hours"). River has one real cycle per calendar day (always
    # 00Z), so it resolves once per date regardless of `run`; Rain resolves
    # to the EXACT selected run, matching the raw layer's own exact-match,
    # no-silent-substitution convention. combined_country_totals' own
    # internal _mat_date_to_river_date/_mat_date_to_rain_date conversion
    # (shared with the raster/facility paths) still applies to this MAT-
    # format value unchanged.
    river_resolved = (
        get_river_extent_forecast_time_for_date(date, hz["rp_tier"])
        if (hz["river_on"] and hz["rp_tier"] and date) else None
    )
    river_forecast_time = _mat_forecast_date(date, '00') if river_resolved else None
    rain_resolved = (
        get_precip_forecast_time_near(date, run)
        if (hz["rain_on"] and hz["rain_mm"] is not None and hz["rain_window"] is not None
            and date and run is not None) else None
    )
    rain_forecast_time = _mat_forecast_date(date, run) if rain_resolved else None

    if wind_threshold is None and not river_forecast_time and not rain_forecast_time:
        return None

    params = {}
    if wind_threshold is not None and storm_info:
        params["wind_on"] = True
        params["wind_forecast_date"] = storm_info["mat_forecast_date"]
        params["wind_threshold"] = wind_threshold
    # No gust_on/gust_threshold key is ever added to params: see this
    # function's own docstring.
    if river_forecast_time:
        params["river_on"] = True
        params["river_forecast_date"] = river_forecast_time
        params["rp_tier"] = hz["rp_tier"]
        params["river_window"] = hz["river_window"]
    if rain_forecast_time:
        params["rain_on"] = True
        params["rain_forecast_date"] = rain_forecast_time
        params["threshold_mm"] = hz["rain_mm"]
        params["window_h"] = hz["rain_window"]

    try:
        resp = requests.get(
            f"{config.TILE_SERVER_URL}/impact/combined-totals/{code}/{storm_info['name'] if storm_info else 'NONE'}",
            params=params, timeout=_MEMBER_IMPACT_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        totals = resp.json() or None
    except Exception as e:
        logger.warning("Could not load combined country totals for %s: %s", country, e)
        totals = None

    if not totals:
        return None

    def _sum_or_none(key):
        v = totals.get(key)
        return float(v) if v is not None else None

    population = totals.get("population") or 0.0
    # Unlike E_population itself (real, expected PARTIAL per-tile coverage
    # in every country checked, correctly handled by _sum's own skipna
    # sum), one specific age-band column can be entirely NULL for a whole
    # country while its siblings and E_population both have real data
    # (e.g. Curaçao's own E_adolescent_population). Same "sum only real
    # components" pattern as the facility columns above: a fully-empty
    # band is excluded from `children` (not folded in as a fabricated 0)
    # and rendered as its own N/A wherever that band's own row is shown.
    age_population = {"Age 0–4 (Infant)": _sum_or_none('infant_population'),
                        "Age 5–14 (School-age)": _sum_or_none('school_age_population'),
                        "Age 15–19 (Adolescent)": _sum_or_none('adolescent_population')}
    children = sum(v for v in age_population.values() if v is not None)
    stats = {
        "Children at Risk": _format_stat_number(children),
        "People at Risk": _format_stat_number(population),
        "Schools at Risk": _format_stat_number(_sum_or_none('num_schools')),
        "Health Centers at Risk": _format_stat_number(_sum_or_none('num_hcs')),
        "Shelters at Risk": _format_stat_number(_sum_or_none('num_shelters')),
        "WASH Facilities at Risk": _format_stat_number(_sum_or_none('num_wash')),
    }
    # None (not 0) whenever the real numerator itself is None, i.e. this
    # country genuinely has no real in-need data (see the .notna().any()
    # fix above), propagated all the way to display (_format_stat_number/
    # _value_td render this as "N/A", not a confirmed real zero).
    #
    # Must NOT math.ceil() the percentage to an integer HERE, before
    # _value_td/_pin_arc_charts_block_from/etc multiply it back against a
    # base count to get the displayed absolute number, that would be a
    # second ceil on top of the one that already happens at that final
    # multiplication, violating this project's own "apply ceil ONCE, at
    # the true final display step" convention for displayed counts.
    # Rounding e.g. 46.1% up to 47% barely moves a single country's own
    # number (base and numerator come from the same population), but for
    # the Combined column, whose base is the SUM across every selected
    # country, including ones with zero real in-need data, that same
    # +0.9-point rounding gets multiplied against the whole inflated base,
    # not just one country's own smaller share, turning a small rounding
    # nudge into a much larger one. Kept as a raw float (still clamped
    # 0-100) so the one real ceil happens only where an absolute count is
    # finally computed.
    pin_pct = {
        "people": (max(0.0, min(100.0, people_in_need / population * 100)) if population > 0 else 0.0) if people_in_need is not None else None,
        "children": (max(0.0, min(100.0, children_in_need / children * 100)) if children > 0 else 0.0) if children_in_need is not None else None,
        # People/Children In Need (E_people_in_need/E_children_in_need)
        # come from a wind-only vulnerability assessment that does NOT
        # scale with the selected wind-severity threshold (see this
        # project's own established In Need note), while At Risk exposure
        # (population) shrinks sharply as the threshold rises. Consumers
        # of pin_pct must NOT reconstruct the displayed In Need count as
        # at_risk * pct / 100, that's correct only while at_risk stays
        # above the real in_need count; once a high threshold shrinks
        # at_risk below the country's genuinely near-fixed in_need total,
        # the real ratio exceeds 100%, the min(100.0, ...) clamp above
        # caps pct at exactly 100%, and the reconstruction collapses to In
        # Need == At Risk, a systematic artifact of deriving an absolute
        # count from a clamped percentage instead of showing the real
        # number. people_abs/children_abs are the real, un-derived,
        # un-clamped absolute counts (raw floats, ceil'd only once at final
        # display, same convention as pin_pct's own comment above), every
        # display surface (the breakdown table, arc charts, Combined
        # column, per-member comparisons) reads these directly instead of
        # reconstructing via pct, so a real In Need count can correctly
        # exceed At Risk without being misrepresented as equal to it.
        "people_abs": people_in_need,
        "children_abs": children_in_need,
    }
    # Real per-age at-risk/in-need breakdown, see _CHILD_AGE_BANDS's own
    # comment.
    age_split = {
        label: {
            # None (not 0) when this age band's own population column is
            # genuinely missing (see age_population's own comment above).
            "at_risk": age_population[label],
            # Same raw-float fix as pin_pct above, see its own comment.
            # Also None (can't compute a real percentage without a real
            # denominator) whenever at_risk itself is None.
            "in_need_pct": ((max(0.0, min(100.0, age_in_need[label] / age_population[label] * 100)) if age_population[label] > 0 else 0.0)
                             if age_in_need[label] is not None and age_population[label] is not None else None),
            # Real, un-derived absolute in-need count for this age band,
            # same "read directly, don't reconstruct via a clamped pct"
            # fix as pin_pct's own people_abs/children_abs above.
            "in_need_abs": age_in_need[label],
        }
        for label in _CHILD_AGE_BANDS
    }
    # Surfaces combined_country_totals' own real TC(Wind|Gust)-vs-
    # Flood(River|Rain) family split (real per-member joint bitmask check,
    # not the independence formula) so _hazard_contribution_content can use
    # it instead of computing its own separate, cruder estimate. None when
    # only one family is active (no real "both" concept possible) or the
    # endpoint didn't return one (e.g. an older cached response).
    #
    # flood_split is the SAME real per-member bitmask methodology applied
    # one level deeper, WITHIN Flood (River Flooding vs Rainfall vs both),
    # None whenever River Flooding and Rainfall aren't BOTH simultaneously
    # active (see combined_country_totals' own has_flood_split comment in
    # tile_server.py), independent of whether TC is active at all.
    return {"stats": stats, "pin_pct": pin_pct, "age_split": age_split,
            "family_split": totals.get("family_split"),
            "flood_split": totals.get("flood_split")}


def _get_country_stats(country, date=None, run=None, wind_kt=None, hz=None):
    """`date`/`run` (topbar values) thread through to _fetch_real_combined_tile_totals
    for reactive resolution; omit them to fall back to the default forecast cycle.
    `wind_kt` (see _resolve_wind_kt) threads the real wind-severity slider value
    through when `hz` isn't given (wind-only callers). Pass a real multi-hazard
    `hz` dict (see _fetch_real_combined_tile_totals's own docstring) to get the
    real combined-across-active-hazards total instead of wind-only.

    Falls back to _ZERO_STATS (all real zeros), not an illustrative mock,
    whenever _fetch_real_combined_tile_totals returns None. That None case
    is NOT "this country was never wired up", in practice it almost
    always means "every active hazard genuinely produced zero real rows
    for this exact query" (e.g. a wind severity threshold this specific
    storm's real forecast never reached, such as selecting Category 5 for
    a storm whose real envelope data only goes up to Category 4). A
    country this reactive to real Snowflake data should never show a
    materially wrong, fabricated illustrative number in place of an
    honest zero."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz or _wind_only_hz(wind_kt))
    return real["stats"] if real else _ZERO_STATS


def _get_country_age_split(country, date=None, run=None, wind_kt=None, hz=None):
    """Real per-age at-risk/in-need breakdown for `country`. Returns
    {label: {"at_risk": float, "in_need_pct": int}} for each of
    _CHILD_AGE_BANDS, same real/zero-fallback contract as
    _get_country_stats (see its own docstring)."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz or _wind_only_hz(wind_kt))
    return real["age_split"] if real else _ZERO_AGE_SPLIT


def _get_country_pin_pct(country, date=None, run=None, wind_kt=None, hz=None):
    """See _get_country_stats above re: date/run/wind_kt/hz, and re: why this
    falls back to _ZERO_PIN_PCT (real zeros) rather than an illustrative
    mock."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz or _wind_only_hz(wind_kt))
    return real["pin_pct"] if real else _ZERO_PIN_PCT


# The real TC(Wind|Gust)-vs-Flood(River|Rain) family split (from
# combined_country_totals' own real per-member bitmask family split) feeds
# _hazard_contribution_content instead of a separate independence-formula
# estimate. Metric names here match the SAME "People at Risk"/"Children
# at Risk"/etc. keys _resolve_stat_value/_STAT_KEY_TO_FACTOR already use,
# mapped to family_split's own raw-column keys: "children" isn't a
# single family_split key (it's infant+school_age+adolescent, same
# 3-column sum _fetch_real_combined_tile_totals_uncached's own `children`
# does), handled specially in _sum_family_split_metric below rather than
# forcing family_split itself to duplicate that sum 3 ways.
_METRIC_TO_FAMILY_SPLIT_KEY = {
    "People at Risk": "population", "Schools at Risk": "num_schools",
    "Health Centers at Risk": "num_hcs", "Shelters at Risk": "num_shelters",
    "WASH Facilities at Risk": "num_wash",
}


def _sum_family_split_metric(family_split, metric):
    """Real value for ONE metric out of a family_split dict's own 3 buckets
    (tc_only/flood_only/both), summed the same way _fetch_real_combined_
    tile_totals_uncached's own `children` does for the 3-age-band case.
    Returns {"tc_only": v, "flood_only": v, "both": v}, any bucket None
    only when EVERY country contributing summed a real all-None column
    (e.g. a country-wide facility-data gap), never a fabricated 0."""
    if not family_split:
        return None
    out = {}
    for bucket in ("tc_only", "flood_only", "both"):
        b = family_split.get(bucket) or {}
        if metric == "Children at Risk":
            parts = [b.get(k) for k in ("infant_population", "school_age_population", "adolescent_population")]
            real_parts = [p for p in parts if p is not None]
            out[bucket] = sum(real_parts) if real_parts else None
        else:
            out[bucket] = b.get(_METRIC_TO_FAMILY_SPLIT_KEY.get(metric, ""))
    return out


def _get_country_family_split(country, date=None, run=None, hz=None):
    """Single-country real family split, see _METRIC_TO_FAMILY_SPLIT_KEY's
    own comment. Returns the raw family_split dict (all 9 raw columns x 3
    buckets) or None when no real data resolves."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz)
    return real.get("family_split") if real else None


def _combined_family_split(countries, date=None, run=None, hz=None):
    """Real family split SUMMED across `countries`, same real per-country
    fan-out + sum pattern _combined_stats/_combined_in_need_total already
    use, applied to family_split's own nested (bucket -> raw_col -> value)
    shape instead of a flat stats dict. A raw_col stays None in the sum
    only when EVERY contributing country's own value for it was None (a
    genuine, real "no country has this data" case, not a fabricated 0,
    same convention _sum_or_none uses elsewhere)."""
    buckets = ("tc_only", "flood_only", "both")
    raw_cols = ("population", "infant_population", "school_age_population", "adolescent_population",
                "built_surface_m2", "num_schools", "num_hcs", "num_shelters", "num_wash")
    totals = {b: {c: None for c in raw_cols} for b in buckets}
    any_real = False
    # Fanned out via get_query_executor().map (same shared-pool pattern
    # used throughout this file for per-country fan-outs, e.g.
    # _combined_stats' own `_fetch`), not a plain serial for loop.
    # _get_country_family_split sits behind _fetch_real_combined_tile_
    # totals' own @ttl_cache, so a call site whose `hz` already matches a
    # just-warmed key (e.g. _impact_breakdown_content's own combined_
    # family_split, called right after _combined_stats with the identical
    # hz) pays almost nothing extra here either way; but a call site that
    # builds its OWN fresh, narrower hz (e.g. _hazard_contribution_
    # content's real_estimate branch, `combined_hz = _build_hz(True,
    # False, ...)`, deliberately different from whatever hz the popup's
    # own headline stats already warmed) is a genuinely COLD cache key,
    # and previously paid N sequential Snowflake round trips here, one per
    # country in the current selection/scope -- now genuinely real given
    # real precip/river bitmask data across ~29 countries, where this used
    # to be a fast no-op against mostly-empty precip tables.
    per_country = list(get_query_executor().map(
        lambda country: _get_country_family_split(country, date, run, hz), countries))
    for fs in per_country:
        if not fs:
            continue
        any_real = True
        for b in buckets:
            bucket_vals = fs.get(b) or {}
            for c in raw_cols:
                v = bucket_vals.get(c)
                if v is None:
                    continue
                totals[b][c] = (totals[b][c] or 0.0) + v
    return totals if any_real else None


def _sum_flood_split_metric(flood_split, metric):
    """Real value for ONE metric out of a flood_split dict's own 3 buckets
    (river_only/rain_only/both), same shape and same "None only when every
    contributing bucket is genuinely all-None" rule as
    _sum_family_split_metric above, applied one level deeper (WITHIN Flood,
    River Flooding vs Rainfall, instead of TC vs Flood)."""
    if not flood_split:
        return None
    out = {}
    for bucket in ("river_only", "rain_only", "both"):
        b = flood_split.get(bucket) or {}
        if metric == "Children at Risk":
            parts = [b.get(k) for k in ("infant_population", "school_age_population", "adolescent_population")]
            real_parts = [p for p in parts if p is not None]
            out[bucket] = sum(real_parts) if real_parts else None
        else:
            out[bucket] = b.get(_METRIC_TO_FAMILY_SPLIT_KEY.get(metric, ""))
    return out


def _get_country_flood_split(country, date=None, run=None, hz=None):
    """Single-country real within-Flood split (River Flooding vs Rainfall
    vs both), see _get_country_family_split's own docstring for the
    sibling TC-vs-Flood version this mirrors. Returns None whenever River
    Flooding and Rainfall aren't BOTH active in `hz`, or no real data
    resolves."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz)
    return real.get("flood_split") if real else None


def _combined_flood_split(countries, date=None, run=None, hz=None):
    """Real within-Flood split SUMMED across `countries`, same real
    per-country fan-out + sum pattern _combined_family_split already uses,
    applied to flood_split's own (river_only/rain_only/both) bucket shape
    instead of (tc_only/flood_only/both). Fanned out via
    get_query_executor().map, same reasoning as _combined_family_split's
    own comment above (this function's only caller,
    _hazard_contribution_content's flood_split_real branch, always builds
    its own fresh river+rain-only `flood_hz`, a genuinely cold cache key,
    not a warm one)."""
    buckets = ("river_only", "rain_only", "both")
    raw_cols = ("population", "infant_population", "school_age_population", "adolescent_population",
                "built_surface_m2", "num_schools", "num_hcs", "num_shelters", "num_wash")
    totals = {b: {c: None for c in raw_cols} for b in buckets}
    any_real = False
    per_country = list(get_query_executor().map(
        lambda country: _get_country_flood_split(country, date, run, hz), countries))
    for fs in per_country:
        if not fs:
            continue
        any_real = True
        for b in buckets:
            bucket_vals = fs.get(b) or {}
            for c in raw_cols:
                v = bucket_vals.get(c)
                if v is None:
                    continue
                totals[b][c] = (totals[b][c] or 0.0) + v
    return totals if any_real else None


# _simple_breakdown_table's own inline hazard-split line
# (_hazard_split_line) must NOT apply ONE flat, illustrative
# _HAZARD_OVERLAP_FRAC-derived `breakdown` to EVERY cell in the table,
# regardless of which metric (Population/Children/Schools/...) or which
# column (country) that cell belongs to, each cell gets its own real
# per-metric split instead, same as the Hazard Contribution popup above.
_AGE_BAND_TO_FAMILY_SPLIT_KEY = {
    "Age 0–4 (Infant)": "infant_population",
    "Age 5–14 (School-age)": "school_age_population",
    "Age 15–19 (Adolescent)": "adolescent_population",
}


def _breakdown_from_split(tc_only_n, flood_only_n, both_n, fallback_breakdown):
    """Real tc_pct/flood_pct/both_pct/tc_only_pct/flood_only_pct from one
    metric's own real 3-way family split, same shape _hazard_contribution_
    content's own real-estimate block builds, factored out here so both
    that popup and _simple_breakdown_table's per-cell split share ONE
    real percentage-deriving formula.

    Returns None (NOT the flat illustrative fallback_breakdown) whenever
    the real total is non-positive, a genuine "no real split to show" case
    (e.g. this exact metric has no real data for either family). This
    function NEVER substitutes a fabricated illustrative percentage split,
    callers must render an honest "no real split" state
    instead (see _hazard_split_line's own None-handling)."""
    real_total = (tc_only_n or 0.0) + (both_n or 0.0) + (flood_only_n or 0.0)
    if real_total <= 0:
        return None
    return {**fallback_breakdown,
        "tc_pct": ((tc_only_n or 0.0) + (both_n or 0.0)) / real_total * 100,
        "flood_pct": ((flood_only_n or 0.0) + (both_n or 0.0)) / real_total * 100,
        "both_pct": (both_n or 0.0) / real_total * 100,
        "tc_only_pct": (tc_only_n or 0.0) / real_total * 100,
        "flood_only_pct": (flood_only_n or 0.0) / real_total * 100,
    }


def _compute_breakdown_by_metric(family_split, breakdown):
    """Real per-metric breakdown map, {metric_or_age_band_label:
    breakdown_dict}, built from ONE real family_split fetch (see
    _get_country_family_split/_combined_family_split, already cached by
    the SAME underlying _fetch_real_combined_tile_totals call this
    column's own base stats already trigger, no extra real round trip).
    Empty (not partially-filled) whenever only one family is active at
    all, same tc_active/flood_active gate _hazard_contribution_content's
    own real-estimate block uses, since there's no real split concept
    with only one family active regardless of per-metric data."""
    if not family_split or not (breakdown.get("tc_active") and breakdown.get("flood_active")):
        return {}
    out = {}
    for metric in list(_METRIC_TO_FAMILY_SPLIT_KEY) + ["Children at Risk"]:
        split = _sum_family_split_metric(family_split, metric)
        if split is None:
            continue
        # _breakdown_from_split returns None for a genuine no-real-split
        # case (real_total<=0), left OUT of this dict entirely rather than
        # stored as a fabricated fallback, same "absent means no data"
        # contract this dict already uses for the single-family case, see
        # _value_td's own cell_breakdown resolution for how a caller must
        # treat a metric missing from this dict.
        result = _breakdown_from_split(split.get("tc_only"), split.get("flood_only"), split.get("both"), breakdown)
        if result is not None:
            out[metric] = result
    for age_label, raw_key in _AGE_BAND_TO_FAMILY_SPLIT_KEY.items():
        tc_only = (family_split.get("tc_only") or {}).get(raw_key)
        flood_only = (family_split.get("flood_only") or {}).get(raw_key)
        both = (family_split.get("both") or {}).get(raw_key)
        if tc_only is None and flood_only is None and both is None:
            continue
        result = _breakdown_from_split(tc_only, flood_only, both, breakdown)
        if result is not None:
            out[age_label] = result
    return out


def _get_data_availability_real(country):
    """Real per-country data-availability snapshot, the same facility-count
    + dataset-boolean check Ahead-of-the-Storm-ORCHESTRATION's own
    08_utilities/check_baseline_data.py already runs against Snowflake.
    Returns None (same as the old dict's `.get()` miss) when the country has
    no base-layer data in Snowflake at all yet.

    Delegates to get_data_availability, which computes the same 4 facility
    sums and 8 non-null checks in one Snowflake aggregate query (SUM/COUNT,
    no geometry involved) rather than pulling every BASE_MERCATOR_TILE_MAT
    row for the country plus a Python loop reconstructing quadkey geometry
    per row, same return shape (see get_data_availability's own
    docstring), so every caller here works unchanged.
    """
    code = _NAME_TO_CODE.get(country)
    if not code:
        return None
    try:
        return get_data_availability(code)
    except Exception as e:
        logger.warning("Could not load data availability for %s: %s", country, e)
        return None


# Real per-member track granularity (TC_TRACKS has member_type/member id,
# see components/map/javascript.py's style_tracks control/ensemble
# distinction): "Probabilistic" is today's real default (probability-
# weighted across the whole ensemble); picking a specific member selects
# that member's own track, wind envelope, and precip layer instead of the
# aggregate. Grouped so "Probabilistic" reads as a distinct choice, not
# just one more item in the same list as the individual members.
#
# Wired into _load_ms_tracks_and_envelopes (map tracks + envelopes filter
# to just the selected member) via _resolve_ensemble_member below. Real
# per-member data exists for this: TC_TRACKS/TRACK_MAT both carry a real
# ENSEMBLE_MEMBER/ZONE_ID column, numbered 1-51, member 51 is ECMWF's real
# deterministic control run (50 perturbed + 1 control = 51 total), matching
# _build_ms_track_features' own existing `member in (51, 52)` control check.
# The full 1-50 list is shown, since real data supports all of them, 51
# itself is deliberately excluded from this range (shown once, as
# "Control", not duplicated as "Member 51").
_CONTROL_MEMBER = 51


def _ensemble_members():
    return [
        {"value": "combined", "label": _t("Probabilistic")},
        {"group": _t("Ensemble Members"), "items":
            [{"value": "control", "label": _t("Control (deterministic)")}]
            + [{"value": f"member-{i}", "label": _t("Member {n}", n=i)} for i in range(1, _CONTROL_MEMBER)]},
    ]


def _resolve_ensemble_member(value):
    """Map ensemble-member-select's dropdown value to a real ENSEMBLE_MEMBER
    int (TC_TRACKS/TRACK_MAT's own numbering), or None for "combined" (the
    probability-weighted aggregate across every member, no filter)."""
    if not value or value == "combined":
        return None
    if value == "control":
        return _CONTROL_MEMBER
    if value.startswith("member-"):
        try:
            return int(value.split("-", 1)[1])
        except ValueError:
            return None
    return None

# Fallback only for the "nothing selected yet" skeleton (see _DEFAULT_STATS's
# own comment for why this is NOT _get_country_pin_pct's own real-query
# fallback anymore, that's _ZERO_PIN_PCT below).
_DEFAULT_PIN_PCT = {"people": 34, "children": 41, "people_abs": None, "children_abs": None}
_ZERO_PIN_PCT = {"people": 0, "children": 0, "people_abs": 0.0, "children_abs": 0.0}

# Fallback only (see _get_country_totals below), population/children
# denominator for the arc charts' outer "Population" ring, used only when
# get_country_totals() has no real BASE_MERCATOR_TILE_MAT data for a country.
# Real ZERO fallback (not a fabricated illustrative mock), same convention
# as _ZERO_STATS/_ZERO_PIN_PCT/_ZERO_AGE_SPLIT above. _make_arc_chart's own
# `no_data = total is None or total == 0` check already renders this as an
# explicit no-data ring state, not a misleading confirmed-zero one.
_ZERO_TOTALS = {"population": 0, "children": 0}


def _get_country_totals(country):
    """Total population/children for `country`, sourced from
    BASE_MERCATOR_TILE_MAT via get_country_totals() in snowflake_utils.py
    (the same function callbacks/metrics.py's own In Need arc charts use
    for this exact outer-ring denominator).

    Falls back to _ZERO_TOTALS (a real, honest zero) per field, not a
    fabricated illustrative mock, whenever Snowflake has no real total for
    a country, same convention as the sibling functions immediately above
    (_get_country_stats/_get_country_pin_pct/_get_country_age_split, see
    their own docstrings). Never a fake non-zero number presented as if it
    were real data.

    Note: get_country_totals() also returns a real total_built_surface_m2
    (see its own docstring in snowflake_utils.py), not included in this
    function's own return value, since nothing in this file reads it (the
    real per-tile bitmask unions compute built-surface exposure directly
    where needed instead).
    """
    code = _NAME_TO_CODE.get(country)
    if not code:
        return _ZERO_TOTALS
    totals = get_country_totals(code)
    return {
        "population": totals["total_population"] if totals["total_population"] is not None else 0,
        "children": totals["total_children"] if totals["total_children"] is not None else 0,
    }

# Illustrative, how much each active hazard family contributes to a given
# at-risk/in-need number. One shared split reused across metrics (mock only);
# a real implementation would compute this per metric from actual overlap.
_HAZARD_CONTRIBUTION = [
    ("Sustained Wind", WIND, 45, "mdi:weather-windy"),
    ("Gust", GUST, 15, "mdi:weather-windy-variant"),
    # Flood's own overall share is 40, decomposed into three sub-hazards
    # (River Flooding 20, Rainfall 10, Storm Surge 10). Storm Surge itself
    # is a placeholder (no real pipeline/data behind it yet, see
    # tc_ecmwf_additional_layers work elsewhere) but contributes to the
    # Flood total here the same as any other member.
    ("River Flooding", RIVER, 20, "mdi:waves"),
    ("Rainfall", RAIN, 10, "mdi:weather-pouring"),
    ("Storm Surge", SURGE, 10, "mdi:tsunami"),
]

# The two overarching families used both in the tile-click Hazard
# Contribution popup and the Full Impact Breakdown table's own inline
# hazard-split line, same Tropical Cyclone (orange)/Flood (blue) grouping
# already used everywhere else on this page (_hurricane_family/
# _flood_hazards_family), just applied here to the illustrative
# contribution split too instead of only listing the hazards flat.
# Tropical Cyclone deliberately only includes Sustained Wind, not Gust, in
# BOTH this popup and the table split-line below (both read from the same
# _HAZARD_TC_MEMBERS). Gust stays in _HAZARD_CONTRIBUTION's data (its real
# gust-envelope layer elsewhere on this page is unaffected) but isn't
# grouped under any hazard family here, so its 15% share isn't shown or
# counted in either breakdown.
_HAZARD_GROUPS = [
    ("Tropical Cyclone", WIND, "mdi:hurricane", ["Sustained Wind"]),
    ("Flood", RIVER, "mdi:home-flood", ["River Flooding", "Rainfall", "Storm Surge"]),
]

_HAZARD_TC_NAME, _HAZARD_TC_COLOR, _HAZARD_TC_ICON, _HAZARD_TC_MEMBERS = _HAZARD_GROUPS[0]
_HAZARD_FLOOD_NAME, _HAZARD_FLOOD_COLOR, _HAZARD_FLOOD_ICON, _HAZARD_FLOOD_MEMBERS = _HAZARD_GROUPS[1]
_HAZARD_BY_NAME = {name: (color, pct, icon) for name, color, pct, icon in _HAZARD_CONTRIBUTION}

# Illustrative, what fraction of the SMALLER hazard family's footprint also
# falls within the other family's footprint (e.g. coastal households hit by
# both storm-force wind and the same storm's flooding). A real
# implementation would derive this from actual per-pixel/facility overlap
# between the two hazards' exposure geometries, not a flat constant. Shared
# by the tile-click Hazard Contribution popup's overlap bar AND the Full
# Impact Breakdown tables' own hazard-split line, so both breakdowns agree
# with each other instead of each inventing their own overlap number.
_HAZARD_OVERLAP_FRAC = 0.25


def _hazard_breakdown(wind_on=True, river_on=True, rain_on=True, surge_on=True):
    """Computes every hazard-scoping number this page needs (the Impact
    Summary/table scale, the table's inline hazard-split, and the
    tile-click popup's own breakdown) from EXACTLY which individual hazards
    are toggled on, not just "is Tropical Cyclone or Flood active at all".
    Only the checked members contribute, at their own raw share: "some
    flood hazard is on" must NOT count the WHOLE Flood family, all three
    members, at their full combined share, when only one of the three is
    actually checked.

    Gust is permanently excluded (not in _HAZARD_TC_MEMBERS at all, see
    _HAZARD_GROUPS's own note), so the maximum achievable total, every one
    of the 4 tracked checkboxes on at once, is less than the original mock
    number (their combined illustrative share, minus overlap, is 75%, not
    100%; Gust's 15% is simply never counted). This is a deliberate
    consequence of NOT renormalizing the percentages, same reasoning as
    before: they're illustrative shares of a fixed baseline, not a real
    per-hazard decomposition that's guaranteed to sum to 100.
    """
    active_flags = {"Sustained Wind": wind_on, "River Flooding": river_on,
                      "Rainfall": rain_on, "Storm Surge": surge_on}

    def _family_pct(member_names):
        return sum(_HAZARD_BY_NAME[name][1] for name in member_names if active_flags.get(name))

    tc_pct = _family_pct(_HAZARD_TC_MEMBERS)
    flood_pct = _family_pct(_HAZARD_FLOOD_MEMBERS)
    tc_active, flood_active = tc_pct > 0, flood_pct > 0
    # both_pct must NOT be math.ceil()'d HERE, before tc_only_pct/
    # flood_only_pct are derived by subtracting it and all three later get
    # multiplied against a real people-at-risk total and ceil'd AGAIN at
    # display (_hazard_row/_hazard_curve_row/_legend_item/
    # _hazard_split_line), that would be the same premature-rounding
    # pattern to avoid as in the in-need/Combined pipeline elsewhere in
    # this file (see _fetch_real_combined_tile_totals_uncached's own
    # pin_pct comment for the full "why"). Kept as a raw float here; every
    # consumer already does the one real ceil at its own final
    # _format_stat_number() display step, and _hazard_row is the only
    # place that shows this AS a percentage (not an absolute count), it
    # rounds for that display separately, see its own comment.
    both_pct = (_HAZARD_OVERLAP_FRAC * min(tc_pct, flood_pct)) if (tc_active and flood_active) else 0.0
    tc_only_pct = tc_pct - both_pct
    flood_only_pct = flood_pct - both_pct
    scale = (tc_only_pct + both_pct + flood_only_pct) / 100

    return {
        "scale": scale, "tc_active": tc_active, "flood_active": flood_active,
        "tc_pct": tc_pct, "flood_pct": flood_pct,
        "both_pct": both_pct, "tc_only_pct": tc_only_pct, "flood_only_pct": flood_only_pct,
        "active_tc_members": [m for m in _HAZARD_TC_MEMBERS if active_flags.get(m)],
        "active_flood_members": [m for m in _HAZARD_FLOOD_MEMBERS if active_flags.get(m)],
    }


def _scale_stats_by_hazard(base_stats, scale):
    if scale == 1.0:
        return base_stats
    return {k: _format_stat_number(_parse_stat_number(v) * scale) for k, v in base_stats.items()}


def _active_hazards_indicator(breakdown):
    # Shown at the top of the Full Impact Breakdown (modal + print page) so
    # it's clear the table below isn't always the full picture, it's
    # scoped to whichever hazards are currently toggled on. Grouped under
    # Tropical Cyclone/Flood (own icon + label), not a flat list of hazard
    # chips, a flat list didn't make clear which hazards belong to which
    # family, same reasoning as the tile-click popup's own grouped rows.
    by_name = {name: (color, icon) for name, color, _, icon in _HAZARD_CONTRIBUTION}

    def _hazard_chip(name):
        return dmc.Group([DashIconify(icon=by_name[name][1], width=13, color=by_name[name][0]),
                            dmc.Text(_t(name), size="11px", c="dimmed")], gap=4)

    def _family_group(family_name, family_color, family_icon, member_names):
        if not member_names:
            return None
        # Pill (tinted background + border in the family's own color), same
        # visual language as the command-bar's own hazard pills, so a group
        # reads as one contained unit instead of just extra gaps between
        # plain text runs.
        return html.Div(dmc.Group([
            dmc.Group([DashIconify(icon=family_icon, width=14, color=family_color),
                        dmc.Text(_t(family_name), size="11px", fw=700, c=family_color)], gap=4),
        ] + [_hazard_chip(name) for name in member_names], gap=10, wrap="nowrap"),
            style={"padding": "6px 14px", "borderRadius": "999px", "border": f"1px solid {family_color}",
                    "background": f"{family_color}15"})

    groups = [g for g in (
        _family_group(_HAZARD_TC_NAME, _HAZARD_TC_COLOR, _HAZARD_TC_ICON, breakdown["active_tc_members"]),
        _family_group(_HAZARD_FLOOD_NAME, _HAZARD_FLOOD_COLOR, _HAZARD_FLOOD_ICON, breakdown["active_flood_members"]),
    ) if g is not None]

    if not groups:
        return dmc.Group([
            dmc.Text(_t("Hazards included:"), size="11px", fw=700, c="dimmed"),
            dmc.Text(_t("None — toggle a hazard on the map to see impact numbers."), size="11px", c="red", fs="italic"),
        ], gap=12, mt=10, mb=18)

    # mt gives this row breathing room instead of sitting flush against
    # the divider/title right above it.
    return html.Div([
        dmc.Text(_t("Hazards included:"), size="11px", fw=700, c="dimmed", mb=6),
        dmc.Group(groups, gap=20, wrap="wrap"),
    ], style={"marginTop": "10px", "marginBottom": "18px"})


def _hazard_split_line(n, breakdown, font_size="9.5px", real_split=None):
    # Shared by _simple_breakdown_table and _admin1_table so both value
    # tables show the same Tropical Cyclone-only/Both/Flood-only illustrative
    # split under every number, using the same percentages (and the same
    # overlap fraction) as the tile-click Hazard Contribution popup's own
    # overlap bar, three numbers, not the earlier two-number TC-share/
    # Flood-share split, which read as if they were separate people instead
    # of overlapping risk.
    #
    # `breakdown` (from _hazard_breakdown) reflects EXACTLY which individual
    # hazard checkboxes are on, if Flood has nothing toggled on, there's no
    # Flood-only or Both share to show (there's nothing for Tropical Cyclone
    # to overlap WITH), so this drops to a single plain number instead of a
    # 3-way split; same the other way around if Tropical Cyclone is off.
    # Neither active means n is already 0, nothing meaningful to split.
    # The single-family branches must NOT render a SECOND, colored copy of
    # the exact same number (n * 100%, since there's only one family to
    # attribute it to) with no caption, _hazard_split_legend (the only
    # thing that would explain the color) deliberately renders nothing
    # unless BOTH families are active, so a single-family selection (the
    # common case, e.g. only Sustained Wind) would show an unexplained
    # duplicate. A duplicate of a number directly above it conveys zero
    # real information regardless of caption, same principle
    # _admin1_table's own docstring applies ("no per-hazard split to
    # explain" once only one real hazard exists), so this renders nothing
    # in the single-family case too, instead of a redundant colored copy.
    # None means "both families active, but no real per-metric/per-cell
    # split resolved" (see _value_td's own cell_breakdown resolution,
    # three-way: real split / no-real-split / single-family), a genuinely
    # different case from "nothing to explain" above: must NOT render the
    # illustrative flat percentages here either, that was the exact
    # "nothing should ever fall back to illustrative" bug this whole
    # function's caller-side fix addresses, see _breakdown_from_split's
    # own comment for the source-side half of this same fix.
    if breakdown is None:
        return html.Div(_t("No real split data for this cell."),
                          style={"fontSize": font_size, "marginTop": "1px", "color": "#adb5bd", "fontStyle": "italic"})
    tc_active, flood_active = breakdown["tc_active"], breakdown["flood_active"]
    if (tc_active and not flood_active) or (flood_active and not tc_active) or (not tc_active and not flood_active):
        return html.Div()
    # real_split: optional {"tc_only": v, "both": v, "flood_only": v} of
    # REAL un-rounded per-metric values, when the caller already has them
    # (e.g. the tile-click popup's By Country block, which has real
    # c_split values sitting right next to `n` -- see that call site's own
    # comment). Bypasses the `n * pct / 100` reconstruction below when
    # given: `n` there is itself a re-parse of an already-K/M-abbreviated
    # display string (_get_country_stats -> _format_stat_number, then
    # _parse_stat_number back), the exact "round-trip a rounded display
    # string, then reconstruct" precision loss _hazard_row's own
    # abs_value param exists to avoid one level up (see its own comment
    # for the full "why": a real 0.6/0.4 split can flip to a displayed
    # "1/1", or two real numbers can look mutually contradictory). Callers
    # whose own `n` is already a real unrounded value (_simple_breakdown_
    # table/_admin1_table, both pass a real un-formatted number) don't
    # need this and can omit it, the reconstruction is exact for them.
    if real_split is not None:
        tc_n = real_split.get("tc_only") or 0.0
        both_n = real_split.get("both") or 0.0
        flood_n = real_split.get("flood_only") or 0.0
    else:
        tc_n = n * breakdown["tc_only_pct"] / 100
        both_n = n * breakdown["both_pct"] / 100
        flood_n = n * breakdown["flood_only_pct"] / 100
    return html.Div([
        html.Span(_format_stat_number(tc_n), style={"color": _HAZARD_TC_COLOR, "fontWeight": 600}),
        html.Span("/", style={"color": "#c3ccd2", "margin": "0 1px"}),
        html.Span(_format_stat_number(both_n), style={"color": HAZARD_BOTH_COLOR, "fontWeight": 600}),
        html.Span("/", style={"color": "#c3ccd2", "margin": "0 1px"}),
        html.Span(_format_stat_number(flood_n), style={"color": _HAZARD_FLOOD_COLOR, "fontWeight": 600}),
    ], style={"fontSize": font_size, "marginTop": "1px"})


def _hazard_split_legend(breakdown):
    # Shared legend, same markup used under both tables. Only meaningful
    # when BOTH families are active, a single-family view already shows a
    # single plain-colored number (see _hazard_split_line), nothing to
    # explain via a 3-dot legend.
    #
    # The TC-only/Both/Flood-only split under each value is real
    # (per-metric, per-column, from the real per-tile bitmask family
    # split, see _compute_breakdown_by_metric), not the flat
    # _HAZARD_CONTRIBUTION/_HAZARD_OVERLAP_FRAC illustrative estimate. A
    # cell with no real split data NO LONGER falls back to that flat
    # illustrative split (see _breakdown_from_
    # split's own comment and _value_td's own cell_breakdown resolution);
    # it shows an honest "no real split data" state instead
    # (_hazard_split_line's own None-handling branch), so this caption can
    # now state plainly that every visible split is real, with no
    # illustrative-fallback caveat left to disclose.
    if not (breakdown["tc_active"] and breakdown["flood_active"]):
        return html.Div()
    return dmc.Group([
        dmc.Group([html.Span(style={"width": "7px", "height": "7px", "borderRadius": "50%", "background": _HAZARD_TC_COLOR}),
                    dmc.Text(_t("Tropical Cyclone only"), size="10px", c="dimmed")], gap=5),
        dmc.Group([html.Span(style={"width": "7px", "height": "7px", "borderRadius": "50%", "background": HAZARD_BOTH_COLOR}),
                    dmc.Text(_t("Both"), size="10px", c="dimmed")], gap=5),
        dmc.Group([html.Span(style={"width": "7px", "height": "7px", "borderRadius": "50%", "background": _HAZARD_FLOOD_COLOR}),
                    dmc.Text(_t("Flood only"), size="10px", c="dimmed")], gap=5),
        dmc.Text(_t("Real per-member Tropical Cyclone only / Both / Flood only split of each value."),
                   size="10px", c="dimmed", fs="italic"),
    ], gap=14, mt=10)

# Not a raw member picker, the user picks which EXPOSURE/FACILITY property
# should drive the comparison, and the member that's "worst" FOR THAT
# PROPERTY is looked up automatically. Different properties can genuinely
# point at different members (the scenario with the worst population impact
# isn't necessarily the one with the worst schools impact). Matches the
# actual exposure/infrastructure properties elsewhere on this page, not
# "Hazard Type", which is already its own separate selection (the hazard
# checkboxes), not a property to compare worst-case scenarios by.
# Country-Analysis-only; "None" shows no comparison at all.
def _influencing_factors():
    return [
        {"value": "none", "label": _t("None")},
        {"value": "population", "label": _t("Population")},
        {"value": "children", "label": _t("Children (total)")},
        {"value": "built_up", "label": _t("Built-up Area")},
        {"value": "schools", "label": _t("Schools")},
        {"value": "health_centers", "label": _t("Health Centers")},
        {"value": "shelters", "label": _t("Shelters")},
        {"value": "wash", "label": _t("WASH Facilities")},
        # Separated at the bottom, comparing against the deterministic/control
        # run specifically is a real but occasional need, not a normal default.
        {"group": _t("Other"), "items": [{"value": "deterministic", "label": _t("Deterministic")}]},
    ]

# Deliberately does NOT hardcode the SAME "worst" member for every property
# regardless of country/storm/date/threshold (e.g. "population" -> always
# "member-10"), maps an influencing_factor to the real TRACK_MAT column
# (get_track_impacts, lowercased, see _member_track_impacts) whose real
# per-member max determines the real worst member, for THIS country/storm/
# threshold specifically.
_INFLUENCING_FACTOR_SEVERITY_COL = {
    "population": "severity_population", "built_up": "severity_built_surface_m2",
    "schools": "severity_schools", "health_centers": "severity_hcs",
    "shelters": "severity_num_shelters", "wash": "severity_num_wash",
}


def _member_track_impacts(country, date, run, wind_kt):
    """Real per-member severity DataFrame (get_track_impacts, columns
    lowercased, Snowflake normalizes unquoted identifiers to uppercase
    regardless of how the SQL aliased them, same gotcha
    _build_ms_envelope_geojson already documents) for `country` at the
    currently resolved storm/date/wind_kt. None when no real storm resolves
    or the query fails, callers must treat that as "no real comparison
    available", not fall back to a fake one."""
    if wind_kt is None:
        return None
    code = _NAME_TO_CODE.get(country)
    storm_info = _resolve_storm_for_country(country, date, run)
    if not code or not storm_info:
        return None
    try:
        df = get_track_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], wind_kt)
    except Exception as e:
        logger.warning("Could not load per-member impacts for %s: %s", country, e)
        return None
    if df is None or df.empty:
        return None
    return df.rename(columns=lambda c: c.lower())


def _member_gust_track_impacts(country, date, run, gust_kt):
    """Gust mirror of _member_track_impacts, get_gust_track_impacts/
    TRACK_GUST_MAT. Only ever has zone_id + severity_population (no
    facility/children columns exist for gust, confirmed via that
    function's own SQL, components/data/snowflake_utils.py). Currently
    unused: the cross-hazard combination reads Gust through
    combined_member_impacts's own bitmask-based path instead (see
    _member_combined_impacts_impl), not through this function."""
    if gust_kt is None:
        return None
    code = _NAME_TO_CODE.get(country)
    storm_info = _resolve_storm_for_country(country, date, run)
    if not code or not storm_info:
        return None
    try:
        df = get_gust_track_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], gust_kt)
    except Exception as e:
        logger.warning("Could not load per-member gust impacts for %s: %s", country, e)
        return None
    if df is None or df.empty:
        return None
    return df.rename(columns=lambda c: c.lower())


# A COLD river bitmask build for a real country can genuinely take longer
# than a plain API round-trip should, up to ~39s for Bangladesh's own
# real extent_rp10_bymember parquet (see _RiverExtentCache's own module
# comment for why: a full row-group scan, not an indexed lookup). A short
# client timeout would silently swallow that as "no data" instead of a
# real, expected cold-start cost, matches the SAME real cold-Zarr cost
# _PrecipRawCache.ensure_precip_raw already documents for the aggregate
# raw layer. A subsequent request against the now-warm tile_server-side
# cache (TTL-bounded, see _RIVER_EXTENT_TTL/_PRECIP_RAW_TTL) is fast
# either way, this timeout only matters for a genuinely cold cache.
_MEMBER_IMPACT_HTTP_TIMEOUT = 120


def _member_combined_impacts(country, date, run, hz):
    """Real per-member Wind+River+Rain COMBINED E_* DataFrame (Gust
    deliberately excluded, same as _HAZARD_TC_MEMBERS/the Hazard
    Contribution popup's Tropical Cyclone family, see
    _member_combined_impacts_impl's own comment for why), server-to-server
    HTTP call to services/tile_server.py's /impact/combined-member endpoint
    (see that endpoint's own docstring for the full methodology). Returns a
    TRUE tile-level union, flooded-OR-exceeded-OR-wind-covered, per member,
    not independent per-hazard totals combined via a coarse max() or an
    independence-assumption formula afterward.

    Resolves each active hazard's own forecast_time/storm INDEPENDENTLY
    (different source tables/resolvers per hazard, River/Rain via
    get_river_extent_forecast_time_for_date/get_precip_forecast_time_near,
    same two resolvers _build_global_raw_config already uses for those raw
    layers; Wind via _resolve_storm_for_country, the SAME resolver
    _member_track_impacts already uses) and passes whichever ones are real
    to the joint endpoint, a country/date where only some of them resolve
    still gets a correct answer (just those hazards' own real union, no
    artificial "union with nothing"). Returns None only when NOTHING is
    active/resolvable at all.

    Callers should only reach for this when a flood hazard (River/Rain) is
    also active, see _fetch_family_member_frames's own "wind" vs
    "combined" gate: a pure wind-only request stays on the older,
    TRACK_MAT-scalar-only path (_member_track_impacts) instead, so it has
    zero dependency on whether TILE_WIND_BITMASK_MAT has real rows yet for
    a given historical storm (a real, deliberate transition-safety choice,
    see this function's own call site).

    @ttl_cache'd (via the _impl split below, same hz-dict-isn't-hashable
    pattern _fetch_real_combined_tile_totals already uses), since a single
    Country Analysis render can call this up to 3x for the SAME (country,
    hz) (worst-member resolution, PIN arc chart, compare_stats), and
    unlike _member_track_impacts (a thin wrapper over the already-
    @ttl_cache'd get_track_impacts), this makes its own real HTTP round
    trip with no cache underneath it otherwise."""
    hz_key = tuple(sorted((hz or {}).items()))
    return _member_combined_impacts_impl(country, date, run, hz_key)


@ttl_cache(ttl_seconds=900, maxsize=256)
def _member_combined_impacts_impl(country, date, run, hz_key):
    hz = dict(hz_key)
    code = _NAME_TO_CODE.get(country)
    if not code:
        return None
    wind_on = bool(hz.get("wind_on") and hz.get("wind_kt") is not None)
    # Gust is deliberately EXCLUDED from this endpoint, same as it's
    # deliberately excluded from _HAZARD_TC_MEMBERS/the Hazard Contribution
    # popup's Tropical Cyclone family (see _HAZARD_GROUPS's own comment).
    # "Compare Worst Case By" and the wider multi-hazard combination stay
    # scoped to Wind (not Wind+Gust) for the same reason: Gust has its own
    # independent toggle/layer elsewhere on this page, and folding it into
    # this combined number would make "worst member" answer a different,
    # unlabeled question depending on whether Gust happened to be checked.
    # The server endpoint (combined_member_impacts) technically accepts a
    # gust_threshold, but nothing here ever passes one, deliberately.
    river_on = bool(hz.get("river_on") and hz.get("rp_tier"))
    rain_on = bool(hz.get("rain_on") and hz.get("rain_mm") is not None)
    if not wind_on and not river_on and not rain_on:
        return None
    params = {}
    if wind_on:
        storm_info = _resolve_storm_for_country(country, date, run)
        if storm_info:
            params["storm"] = storm_info["name"]
            params["forecast_date"] = storm_info["mat_forecast_date"]
            params["wind_threshold"] = hz["wind_kt"]
    if river_on:
        try:
            resolved = get_river_extent_forecast_time_for_date(date, hz["rp_tier"])
        except Exception as e:
            logger.warning("Could not resolve river-raw forecast time for %s: %s", country, e)
            resolved = None
        if resolved:
            params["river_forecast_time"] = resolved[0]
            params["rp_tier"] = hz["rp_tier"]
            params["step_h"] = int(hz["river_window"]) if hz.get("river_window") else _RIVER_WINDOW_DEFAULT
    if rain_on:
        try:
            resolved = get_precip_forecast_time_near(date, run)
        except Exception as e:
            logger.warning("Could not resolve precip-raw forecast time for %s: %s", country, e)
            resolved = None
        if resolved:
            params["rain_forecast_time"] = resolved[0]
            params["window_h"] = int(hz["rain_window"]) if hz.get("rain_window") else 6
            params["threshold_mm"] = hz["rain_mm"]
    if not any(k in params for k in ("wind_threshold", "river_forecast_time", "rain_forecast_time")):
        return None
    try:
        resp = requests.get(
            f"{config.TILE_SERVER_URL}/impact/combined-member/{code}",
            params=params, timeout=_MEMBER_IMPACT_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        members = (resp.json() or {}).get("members") or {}
    except Exception as e:
        logger.warning("Could not load per-member combined impacts for %s: %s", country, e)
        return None
    if not members:
        return None
    return pd.DataFrame([{"zone_id": int(m), **vals} for m, vals in members.items()])


# Cross-hazard per-member combination for the "Compare Worst Case By"
# feature (_resolve_worst_member_multi/_real_member_stats below).
#
# Combining a Wind-only scalar total against an already-unioned Flood
# (River+Rain) total via an independence-assumption formula would
# overstate true combined exposure, since same-storm hazards are
# correlated and treating their footprints as statistically independent
# overstates the union. Since Wind/Gust also have real per-tile
# per-member data (see services/tile_server.py's own
# combined_member_impacts), there is nothing to approximate: whenever a
# flood hazard is active, every currently-toggled hazard (Wind, Gust when
# its own toggle is on, River, Rain) is folded into ONE already-unioned
# "combined" frame server-side, and this module's own job shrinks to
# reading it, no inclusion-exclusion math, no country-total normalization.
# Known limitation: In Need stays Wind-only.
_STAT_KEY_TO_FACTOR = {
    "People at Risk": "population", "Children at Risk": "children",
    "Schools at Risk": "schools", "Health Centers at Risk": "health_centers",
    "Shelters at Risk": "shelters", "WASH Facilities at Risk": "wash",
}
_CHILDREN_TRACK_COLS = ("severity_infant_population", "severity_school_age_population", "severity_adolescent_population")
# Read off the "combined" (Wind+River+Rain union) frame, see
# services/tile_server.py's combined_member_impacts docstring for why all
# three real per-tile age columns are sampled the same way for the union,
# not just population.
_CHILDREN_COMBINED_COLS = ("E_infant_population", "E_school_age_population", "E_adolescent_population")
# Metric -> the E_* column the "combined" (Wind+River+Rain union) frame
# carries for it. "children" has its own multi-column sum handled
# separately (see _hazard_key_totals) rather than a single column here. A
# metric genuinely missing a column on a given response (e.g. a real
# per-tile data gap) still safely falls through _hazard_key_totals' own
# `col not in df.columns` skip, treated as "this hazard doesn't
# contribute" here, not a fabricated 0.
_COMBINED_METRIC_COL = {
    "population": "E_population", "schools": "E_num_schools",
    "health_centers": "E_num_hcs", "shelters": "E_num_shelters",
    "wash": "E_num_wash", "built_up": "E_built_surface_m2",
}


def _flood_hazard_active(hz):
    """Real gate for "is a flood hazard (River or Rain) toggled on in the
    UI, with a real selection", River needs BOTH `river_on` AND a
    selected `rp_tier`; Rain needs BOTH `rain_on` AND a selected
    `rain_mm` threshold. This is a UI-STATE-ONLY check, see
    _flood_combine_active below for the stricter, DATA-aware version the
    actual per-member combine logic uses; this bare function is still
    used on its own by _stat_grid's own compare-badge labeling (which
    only needs to know what the user selected, not whether that selection
    happens to have real data behind it for today's date).

    _fetch_family_member_frames must route on this stricter check, not the
    looser `hz.get("river_on") or hz.get("rain_on")` (UI toggle state
    alone), _member_combined_impacts's own internal gating requires
    river_on AND rp_tier, or rain_on AND rain_mm is not None. A UI state
    where River is toggled on but no tier is selected yet (or an unpicked
    rain threshold) would otherwise route through the bitmask-based
    "combined" path for a request that only Wind actually resolves,
    losing the wind-only path's proven TRACK_MAT-scalar safety net (see
    _fetch_family_member_frames's own docstring below on why that
    matters) for zero real benefit, and compounding the separate
    "TILE_WIND_BITMASK_MAT not yet backfilled for this storm" gap into a
    silently-empty result instead of a real wind-only one."""
    return bool((hz.get("river_on") and hz.get("rp_tier"))
                or (hz.get("rain_on") and hz.get("rain_mm") is not None))


def _flood_data_resolves(date, run, hz):
    """Real check: does River or Rain have REAL DATA for this exact
    topbar date/run, not just "is the toggle on" (that's
    _flood_hazard_active above, a pure UI-state check). Calls the SAME
    @ttl_cache'd resolvers _member_combined_impacts_impl itself calls
    (get_river_extent_forecast_time_for_date/get_precip_forecast_time_
    near), cheap after the first real call for a given date, and
    genuinely country-independent (both resolvers key only on
    date/rp_tier/run, never on country, River/Rain are storm-independent
    global layers), so this only needs to run ONCE per request, not once
    per selected country.

    Real per-member flood data only exists for a small, bounded set of
    real dates (e.g. one 2026-07-02 river+rain cycle for PHL/BAVI), not
    "every historical storm" the way TRACK_MAT's 1380 real storm-date keys
    span. _flood_hazard_active alone only reflects the UI TOGGLE state,
    not whether flood data can ACTUALLY be found for the
    currently-selected date. Without this check, toggling River/Rain on
    for ANY date (even one with zero real flood data anywhere nearby)
    would route to the bitmask-union "combined" path regardless, which
    (since TILE_WIND_BITMASK_MAT is ALSO still mostly unbackfilled, see
    _fetch_family_member_frames's own docstring) would then silently
    produce an all-empty/near-zero result instead of falling back to
    Wind's own real, complete TRACK_MAT-scalar numbers. See
    _flood_combine_active below, the actual gate used by the combine
    logic."""
    river_on = bool(hz.get("river_on") and hz.get("rp_tier"))
    rain_on = bool(hz.get("rain_on") and hz.get("rain_mm") is not None)
    if river_on:
        try:
            if get_river_extent_forecast_time_for_date(date, hz["rp_tier"]):
                return True
        except Exception as e:
            logger.warning("Could not resolve river-raw forecast time for %s: %s", date, e)
    if rain_on:
        try:
            if get_precip_forecast_time_near(date, run):
                return True
        except Exception as e:
            logger.warning("Could not resolve precip-raw forecast time for %s: %s", date, e)
    return False


def _flood_combine_active(date, run, hz):
    """The ONE real gate the per-member combine logic actually uses,
    both a real UI selection (_flood_hazard_active) AND real data behind
    it for this exact date (_flood_data_resolves). Shared by
    _fetch_family_member_frames's own routing decision, _real_member_
    stats's own `active_flood` gate, and _real_member_age_split's own
    combined-mode branch, so none of them can ever disagree about which
    path is in play. Kept as one function (rather than each of those 3
    call sites ANDing the two checks inline) specifically to avoid a
    repeat of the exact drift bug _flood_hazard_active's own docstring
    describes, one canonical predicate, not N independent copies of the
    same logic to keep in sync by hand."""
    return _flood_hazard_active(hz) and _flood_data_resolves(date, run, hz)


def _parallel_member_fetch(countries, fn):
    """Runs fn(c) for each country concurrently via get_member_fetch_
    executor()'s own dedicated pool, deliberately NOT the shared
    get_query_executor() pool.

    Must NOT submit through the shared, bounded 24-worker
    get_query_executor() pool: _fetch_family_member_frames (this
    function's only caller) is itself already called from INSIDE that
    same pool's own worker threads in several places (_combined_stats._fetch,
    _country_compare_bundle, _update_impact_summary._build_block, all
    their own get_query_executor().map(...) callbacks). Nesting a
    blocking .map() call onto the IDENTICAL bounded pool from within one
    of its own workers is a real deadlock risk under realistic
    concurrency (a genuine multi-country event, e.g. MELISSA across 4+
    countries, with several Dash callbacks in flight at once can reach
    the 24-outer-task ceiling; the pool has no way to grow to serve its
    own nested work).

    Must NOT use a throwaway `with ThreadPoolExecutor(...) as ex:`
    created fresh per call, that closes the deadlock, but
    reintroduces the exact connection-leak/cold-handshake anti-pattern
    get_query_executor()'s own module comment in snowflake_utils.py
    documents avoiding (each throwaway worker thread pays a fresh 0.47-0.65s
    connect() on a cold _thread_local, then leaks that open connection
    when the pool tears down at the end of the `with` block, a routine
    path here, not an edge case, since every Impact Summary render and
    Full Breakdown open for a multi-country selection goes through this
    function). get_member_fetch_executor() is a SECOND process-wide,
    long-lived pool (see its own docstring), isolated from the shared
    pool (no deadlock risk) AND long-lived (each worker thread's
    connection is created once and reused, same as the primary pool).
    Single-country case still skips pool submission entirely (the common
    case, most selections are 1 country)."""
    if not countries:
        return {}
    if len(countries) == 1:
        return {countries[0]: fn(countries[0])}
    return dict(zip(countries, get_member_fetch_executor().map(fn, countries)))


def _fetch_family_member_frames(countries, date, run, hz):
    """One round trip per (country x active path), NOT per metric,
    fetched concurrently across countries via _parallel_member_fetch's own
    dedicated local pool (see that function's own docstring for why this
    is NOT the shared get_query_executor() pool). Returns EXACTLY ONE of:
      {"wind": {country: df|None}}, no flood hazard active
      {"combined": {country: df|None}}, a flood hazard is active

    A real Wind+Gust+River+Rain tile-level union is available (see
    services/tile_server.py's combined_member_impacts and this module's
    own _member_combined_impacts) whenever a flood hazard is active, so
    THAT path is used whenever _flood_combine_active(date, run, hz) is
    true (both toggled on AND real data resolves for this date, see that
    function's own docstring), folding Wind's own real contribution in
    too, with no separate scalar Wind fetch to later combine via an
    independence formula.

    The pure wind-only case (no flood hazard active) deliberately stays
    on the OLDER, TRACK_MAT-scalar-only path (_member_track_impacts)
    instead of also routing through the bitmask-based endpoint, a
    deliberate transition-safety choice: this path has zero dependency on
    whether TILE_WIND_BITMASK_MAT has real rows yet for a given
    historical storm (that table is only populated going forward from a
    real DATAPIPELINE pipeline run), so the already-working wind-only
    worst-case feature can never regress because of backfill state.

    Gust is excluded from BOTH paths, deliberately, as a policy choice (same
    as _HAZARD_TC_MEMBERS/the Hazard Contribution popup's Tropical Cyclone
    family), not a data-shape limitation of either one. `_member_combined_
    impacts`/`_member_combined_impacts_impl` never pass a `gust_threshold`
    to the server even though the endpoint technically accepts one, see
    that function's own comment for why."""
    if _flood_combine_active(date, run, hz):
        return {"combined": _parallel_member_fetch(
            countries, lambda c: _member_combined_impacts(c, date, run, hz))}
    if hz.get("wind_on"):
        return {"wind": _parallel_member_fetch(
            countries, lambda c: _member_track_impacts(c, date, run, hz.get("wind_kt")))}
    return {}


def _hazard_key_totals(frames, hazard_key, factor, countries):
    """Sums the ONE hazard-key _fetch_family_member_frames actually
    populated ("wind" or "combined") into a single {member: value} dict,
    SUMMED across countries (matches _resolve_worst_member_multi's
    pre-existing multi-country contract).

    "wind": OLD TRACK_MAT-scalar columns (severity_*), the wind-only
    fallback path, see _fetch_family_member_frames's own docstring for
    why it's kept separate.
    "combined": real per-tile union (E_* columns) across every
    currently-active hazard except Gust, computed server-side by
    services/tile_server.py's combined_member_impacts.

    Each hazard_key here maps to exactly ONE already-correct source, with
    no max() across hazard keys within a "family" (Wind vs Gust, River vs
    Rain) and no independence-formula combination across families needed
    on top of it."""
    totals = {}
    for c in countries:
        df = frames.get(hazard_key, {}).get(c)
        if df is None or 'zone_id' not in df.columns:
            continue
        if factor == "children":
            cols = _CHILDREN_TRACK_COLS if hazard_key == "wind" else _CHILDREN_COMBINED_COLS
            cols = [col for col in cols if col in df.columns]
            if not cols:
                continue
            series = df[cols].sum(axis=1)
        elif hazard_key == "wind":
            col = _INFLUENCING_FACTOR_SEVERITY_COL.get(factor)
            if not col or col not in df.columns:
                continue
            series = df[col]
        else:  # combined (real Wind+River+Rain tile-level union)
            col = _COMBINED_METRIC_COL.get(factor)
            if not col or col not in df.columns:
                continue
            series = df[col]
        for zid, val in zip(df['zone_id'], series):
            if pd.isna(zid) or pd.isna(val):
                continue
            key = int(zid)
            totals[key] = totals.get(key, 0.0) + float(val)
    return totals


def _combine_metric_series(frames, factor, countries):
    """Real per-member Series for ONE metric, a thin read of whichever
    ONE hazard-key _fetch_family_member_frames actually populated for
    this request ("wind" or "combined", see that function's own
    docstring). Pure in-memory pandas, no I/O, `frames` is already
    fetched.

    There is only ever ONE hazard-key populated per request (see
    _fetch_family_member_frames), so this reads it directly rather than
    combining two separate marginal estimates (Wind's own scalar total,
    Flood's own already-unioned total) via an independence-assumption
    formula normalized against a real country total."""
    hazard_key = "combined" if frames.get("combined") else "wind"
    return pd.Series(_hazard_key_totals(frames, hazard_key, factor, countries), dtype=float)


def _resolve_worst_member(influencing_factor, country, date, run, wind_kt, hz=None):
    """The real ensemble member with the highest real severity for
    `influencing_factor`, for THIS country/storm/date/threshold. Returns
    None (no comparison) when nothing resolves: "deterministic" always
    resolves to the real control member (_CONTROL_MEMBER) without needing
    a query, since that's an identity, not a data-driven maximum.

    Single-country thin wrapper around _resolve_worst_member_multi
    (cross-hazard-aware worst-member identification, see that function's
    own docstring). `hz` defaults to `_wind_only_hz(wind_kt)` when
    omitted, preserving wind-only behavior for any caller that doesn't
    pass it."""
    return _resolve_worst_member_multi(influencing_factor, [country], date, run, wind_kt,
                                          hz=hz or _wind_only_hz(wind_kt))


def _resolve_worst_member_multi(influencing_factor, countries, date, run, wind_kt, hz=None):
    """Multi-country version of _resolve_worst_member, the Full Impact
    Breakdown modal shows ONE worst-member tag (e.g. "#6") across every
    selected country's own column, so this sums each real member's own
    severity for `influencing_factor` ACROSS all selected countries first,
    then picks the real overall worst member, not just the first
    country's own worst member, which could differ country to country.

    "Worst member" is genuinely CROSS-HAZARD-AWARE, whenever a flood
    hazard is active, folds in a real tile-level union of every
    currently-toggled hazard (Wind, Gust when its own toggle is on,
    River, Rain; fetched on-demand, see _fetch_family_member_frames/
    _combine_metric_series's own docstrings) instead of considering Wind
    severity alone. `hz` defaults to `_wind_only_hz(wind_kt)` when
    omitted, every existing caller that doesn't pass it gets wind-only
    behavior.

    Explicit limits: Wind has a real tile-level union too whenever a
    flood hazard is active AND TILE_WIND_BITMASK_MAT has real rows for
    that storm, a pure wind-only request always stays on the proven
    TRACK_MAT-scalar path regardless, see _fetch_family_member_frames's
    own docstring; In Need stays Wind-only."""
    if not influencing_factor or influencing_factor == "none":
        return None
    if influencing_factor == "deterministic":
        return "control"
    countries = countries or []
    if not countries:
        return None
    hz = hz or _wind_only_hz(wind_kt)
    # Each per-country fetch is an independent round-trip, so for a
    # multi-country storm selection (e.g. MELISSA across Turks and
    # Caicos/Jamaica/Cuba/Nicaragua), _fetch_family_member_frames fetches
    # every ACTIVE hazard concurrently across countries
    # (thread-local connections per components/data/snowflake_utils.py's
    # own get_connection, safe to call from multiple threads), turning N
    # sequential round-trips into ~1 per hazard.
    frames = _fetch_family_member_frames(countries, date, run, hz)
    series = _combine_metric_series(frames, influencing_factor, countries)
    if series.empty:
        return None
    best_member = int(series.idxmax())
    return "control" if best_member == _CONTROL_MEMBER else f"member-{best_member}"


def _member_label(value):
    for entry in _ensemble_members():
        if entry.get("value") == value:
            return entry["label"]
        for item in entry.get("items", []):
            if item["value"] == value:
                return item["label"]
    return value


def _in_need_note(size="11px", mt=8):
    """Shared clarification note wherever a real People/Children In Need
    number is shown (Impact Summary panel arc charts, Full Impact Breakdown
    table, Admin Level 1 breakdown), two real, verified properties of this
    data (see _update_view_as's own docstring for the exact confirmed
    numbers): (1) E_people_in_need/E_children_in_need only ever come from
    wind's own MAT-table columns, never gust/river/rain, regardless of
    which other hazards are also toggled on; (2) these columns do NOT scale
    with the selected wind severity threshold the way At Risk exposure
    does, so the In Need SHARE looks proportionally larger at higher
    thresholds without any real change in vulnerable population, worth
    knowing before reading a jump in that percentage as a real change.

    Same size/weight/line-height as this page's other dimmed inline notes
    (e.g. the Infrastructure section's own "Shown as plain locations..."),
    plain, not italic, so the two read as one consistent style when
    both appear together.

    Deliberate asymmetry: the "Compare Worst Case By" AT-RISK numbers
    next to this note DO combine Wind with River/Rain's own per-member
    impact (see _resolve_worst_member_multi/_real_member_stats, Gust
    stays excluded from the combination), but this IN-NEED note's own
    claim stays wind-only, no real vulnerability/in-need pipeline exists
    for gust/river/rain, so combining them here would fabricate data
    rather than reflect it."""
    return dmc.Text(
        _t("In Need reflects Sustained Wind exposure only, and does not change with the wind severity threshold selected."),
        size=size, c="dimmed", mt=mt, style={"lineHeight": 1.5},
    )


def _member_short_label(value):
    """Compact tag for inline use next to a comparison number: 'member-6' -> '#6'."""
    if not value:
        return ""
    if value == "control":
        return "Control"
    if value.startswith("member-"):
        return f"#{value.split('-')[-1]}"
    return value


def _parse_stat_number(v):
    """'640K' -> 640000, '185' -> 185. None -> None (propagates a genuine
    "no real data" value through unchanged, see _format_stat_number's own
    None handling for the counterpart on the way back out)."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    v = v.strip()
    if v.endswith("K"):
        return math.ceil(float(v[:-1]) * 1_000)
    if v.endswith("M"):
        return math.ceil(float(v[:-1]) * 1_000_000)
    return math.ceil(float(v))


def _format_stat_number(n):
    """640000 -> '640K', 185 -> '185'.

    Ceils the raw value to the next whole number EXACTLY ONCE (real people/
    impact counts must never be undercounted, e.g. 42081.2 -> 42082, never
    42081), but the K/M abbreviation of that already-ceiled integer must
    use ordinary nearest-rounding, not a second ceil: 42082 abbreviates to
    "42K" (round(42.082) == 42), NOT "43K". Ceiling the abbreviation too
    would be a second, unwanted round-up on top of the first,
    abbreviating for compact display isn't itself a population-
    undercounting concern the way the raw count is. None -> None (genuine
    "no real data" passthrough, see _parse_stat_number's own comment)."""
    if n is None:
        return None
    n = math.ceil(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{round(n / 1000)}K"
    return str(n)


def _real_member_stats(country, date, run, wind_kt, member, hz=None):
    """Real replacement for _scaled_stats, that ONE real ensemble member's
    own real severity numbers (get_track_impacts/TRACK_MAT), not
    Combined/Probabilistic multiplied by a fake per-member factor. Returns
    None when no real data resolves for this member (no storm, member not
    present in this country's real rows, etc.), callers should treat that
    as "no comparison available" (matching the old function's "unknown
    member -> no scaling" behavior), not silently substitute anything.

    When `hz` shows a flood hazard is active (River/Rain), branches to the
    same cross-hazard combination `_resolve_worst_member_multi` uses
    (_fetch_family_member_frames + _combine_metric_series) instead of the
    TRACK_MAT-only body below, a real Wind+River+Rain tile-level union,
    not just Wind/Gust's own scalar total. `hz=None` (any caller that
    doesn't pass it) preserves wind-only behavior.

    `active_flood` uses the SAME _flood_combine_active(date, run, hz)
    predicate _fetch_family_member_frames's own routing decision uses,
    checks both a real UI selection AND real data behind it for this
    exact date (see that function's own docstring for the concrete
    failure a toggle-only check would cause)."""
    if not member or member in ("combined", "none"):
        return None
    member_int = _CONTROL_MEMBER if member == "control" else _resolve_ensemble_member(member)
    if member_int is None:
        return None

    active_flood = bool(hz) and _flood_combine_active(date, run, hz)
    if active_flood:
        frames = _fetch_family_member_frames([country], date, run, hz)
        result = {}
        any_real = False
        for stat_key, factor in _STAT_KEY_TO_FACTOR.items():
            series = _combine_metric_series(frames, factor, [country])
            val = series.get(member_int)
            if val is not None:
                any_real = True
            result[stat_key] = _format_stat_number(val) if val is not None else None
        return result if any_real else None

    df = _member_track_impacts(country, date, run, wind_kt)
    if df is None or 'zone_id' not in df.columns:
        return None
    row = df[df['zone_id'] == member_int]
    if row.empty:
        return None
    r = row.iloc[0]

    def _v(col):
        val = r.get(col)
        return float(val) if pd.notna(val) else 0.0

    def _v_or_none(col):
        # None (not 0) when this member's own facility column is genuinely
        # missing, same pattern as _fetch_real_combined_tile_totals_
        # uncached's own _sum_or_none (e.g. Turks and Caicos Islands' real
        # all-NULL severity_num_shelters).
        val = r.get(col)
        return float(val) if pd.notna(val) else None

    children = _v('severity_infant_population') + _v('severity_school_age_population') + _v('severity_adolescent_population')
    return {
        "People at Risk": _format_stat_number(_v('severity_population')),
        "Children at Risk": _format_stat_number(children),
        "Schools at Risk": _format_stat_number(_v_or_none('severity_schools')),
        "Health Centers at Risk": _format_stat_number(_v_or_none('severity_hcs')),
        "Shelters at Risk": _format_stat_number(_v_or_none('severity_num_shelters')),
        "WASH Facilities at Risk": _format_stat_number(_v_or_none('severity_num_wash')),
    }


def _real_member_pin_pct(country, date, run, wind_kt, member):
    """Real replacement for _scaled_pin_pct, real people/children in-need
    PERCENTAGE for this one real ensemble member (severity_people_in_need/
    severity_population etc, same ratio pattern
    _fetch_real_combined_tile_totals_uncached's own pin_pct already uses at
    country level). Returns None when unresolvable, same contract as
    _real_member_stats above."""
    if not member or member in ("combined", "none"):
        return None
    df = _member_track_impacts(country, date, run, wind_kt)
    if df is None or 'zone_id' not in df.columns:
        return None
    member_int = _CONTROL_MEMBER if member == "control" else _resolve_ensemble_member(member)
    if member_int is None:
        return None
    row = df[df['zone_id'] == member_int]
    if row.empty:
        return None
    r = row.iloc[0]
    population = float(r.get('severity_population') or 0)
    children = (float(r.get('severity_infant_population') or 0) + float(r.get('severity_school_age_population') or 0)
                + float(r.get('severity_adolescent_population') or 0))
    # None (not 0) when this member's own in-need column is genuinely
    # missing, same pattern as _fetch_real_combined_tile_totals_
    # uncached's own pin_pct (see that function's own comment for the full
    # "why", a country with no real vulnerability data at all must not
    # silently render the same as a confirmed real zero).
    _pin_raw = r.get('severity_people_in_need')
    _chin_raw = r.get('severity_children_in_need')
    people_in_need = float(_pin_raw) if pd.notna(_pin_raw) else None
    children_in_need = float(_chin_raw) if pd.notna(_chin_raw) else None
    # Raw float (not ceil'd to an integer), see _fetch_real_combined_tile_
    # totals_uncached's own pin_pct comment for why a premature ceil here
    # corrupts the Combined column's reconstructed absolute number.
    return {
        "people": ((max(0.0, min(100.0, people_in_need / population * 100)) if population > 0 else 0.0)
                    if people_in_need is not None else None),
        "children": ((max(0.0, min(100.0, children_in_need / children * 100)) if children > 0 else 0.0)
                      if children_in_need is not None else None),
        # Real, un-derived absolute in-need counts, same fix as
        # _fetch_real_combined_tile_totals_uncached's own pin_pct comment
        # (people_abs/children_abs there): every display now reads these
        # directly instead of reconstructing via at_risk * pct / 100, which
        # silently collapses to In Need == At Risk once pct gets clamped at
        # 100%.
        "people_abs": people_in_need,
        "children_abs": children_in_need,
    }


_AGE_COMBINED_COLS = {
    "Age 0–4 (Infant)": "E_infant_population",
    "Age 5–14 (School-age)": "E_school_age_population",
    "Age 15–19 (Adolescent)": "E_adolescent_population",
}


def _real_member_age_split(country, date, run, wind_kt, member, hz=None):
    """Real per-age at-risk/in-need breakdown for this one real ensemble
    member, real replacement for applying the old fixed _CHILD_AGE_SPLIT
    share to _real_member_stats's combined "Children at Risk" number.
    Returns None when unresolvable, same contract as _real_member_stats.

    When `hz` shows a flood hazard is active, `_real_member_stats`'s own
    "Children at Risk" total comes from the combined Wind+River+Rain
    union (_hazard_key_totals' "children" factor, summing the SAME 3 real
    E_* age columns used below), this function must branch on the SAME
    _flood_combine_active(date, run, hz) gate _real_member_stats itself
    uses, reading each of the 3 real E_* age columns SEPARATELY (not
    summed) from the identical "combined" frame, so the 3 displayed age
    rows always sum to exactly the same total as the "Children at Risk"
    card shown right above them for the identical member.

    In-need percentages/absolutes stay None (not fabricated) in combined
    mode, same deliberate Wind-only asymmetry _in_need_note documents
    for the country-level aggregate: no real vulnerability pipeline
    exists for River/Rain, so there is no real combined in-need number to
    show, only a real combined at-risk one. `hz=None` (any caller that
    doesn't pass it) preserves wind-only behavior."""
    if not member or member in ("combined", "none"):
        return None
    member_int = _CONTROL_MEMBER if member == "control" else _resolve_ensemble_member(member)
    if member_int is None:
        return None

    if hz and _flood_combine_active(date, run, hz):
        frames = _fetch_family_member_frames([country], date, run, hz)
        df = frames.get("combined", {}).get(country)
        if df is None or 'zone_id' not in df.columns:
            return None
        row = df[df['zone_id'] == member_int]
        if row.empty:
            return None
        r = row.iloc[0]
        result = {}
        any_real = False
        for label, col in _AGE_COMBINED_COLS.items():
            val = r.get(col)
            has_real = pd.notna(val)
            any_real = any_real or has_real
            result[label] = {
                "at_risk": float(val) if has_real else 0.0,
                "in_need_pct": None,
                "in_need_abs": None,
            }
        return result if any_real else None

    df = _member_track_impacts(country, date, run, wind_kt)
    if df is None or 'zone_id' not in df.columns:
        return None
    member_int = _CONTROL_MEMBER if member == "control" else _resolve_ensemble_member(member)
    if member_int is None:
        return None
    row = df[df['zone_id'] == member_int]
    if row.empty:
        return None
    r = row.iloc[0]

    def _v(col):
        val = r.get(col)
        return float(val) if pd.notna(val) else 0.0

    def _v_or_none(col):
        # None (not 0) when this member's own in-need column is genuinely
        # missing, see _real_member_pin_pct's own comment for the full
        # "why".
        val = r.get(col)
        return float(val) if pd.notna(val) else None

    _AGE_COLS = {"Age 0–4 (Infant)": ("severity_infant_population", "severity_infant_in_need"),
                  "Age 5–14 (School-age)": ("severity_school_age_population", "severity_school_age_in_need"),
                  "Age 15–19 (Adolescent)": ("severity_adolescent_population", "severity_adolescent_in_need")}
    result = {}
    for label, (pop_col, need_col) in _AGE_COLS.items():
        at_risk = _v(pop_col)
        need_val = _v_or_none(need_col)
        # Raw float, same fix as _real_member_pin_pct's own return above.
        result[label] = {
            "at_risk": at_risk,
            "in_need_pct": ((max(0.0, min(100.0, need_val / at_risk * 100)) if at_risk > 0 else 0.0)
                             if need_val is not None else None),
            # Real, un-derived absolute in-need count, same fix as
            # _fetch_real_combined_tile_totals_uncached's own age_split
            # comment.
            "in_need_abs": need_val,
        }
    return result


# "Combined Total" mode for the Impact Summary/Full Breakdown, sums each
# selected country's own (optionally worst-case-scaled) numbers rather than
# averaging a percentage, so a combined In Need total always equals the sum
# of what each country's own tile would show individually.
def _combined_stats(countries, member=None, date=None, run=None, wind_kt=None, hz=None):
    keys = list(_DEFAULT_STATS.keys())
    # A genuinely empty `countries` list, no
    # real countries to even consider, is real "no data available", the
    # same N/A treatment as a per-country facility-column gap below, not a
    # confirmed "0" (that reads as "we checked and there are real zero
    # people/schools/etc," which isn't what this situation actually is).
    # _hazard_contribution_content's own is_global branch below shows the
    # SAME "None of the initialized countries show impact..." explanation
    # for this case as it already does for a real total==0, so clicking any
    # of these six tiles still lands on a clear, accurate message either way.
    if not countries:
        return {k: None for k in keys}
    totals = {k: 0.0 for k in keys}
    # Tracks whether ANY country contributed a real (non-None) value for
    # this metric, People/Children at Risk are always real (per-tile
    # population gaps already soft-propagate via _sum's own skipna sum,
    # see its own comment), but a facility metric (Schools/Health Centers/
    # Shelters/WASH at Risk) can be None for a country with a genuine
    # column-wide data gap (e.g. Turks and Caicos Islands' real all-NULL
    # E_num_shelters, see _fetch_real_combined_tile_totals_uncached's own
    # _sum_or_none comment). Same "sum only real countries" policy
    # _combined_in_need_total already uses, a metric with ZERO real-data
    # countries stays None (not a fabricated 0); with SOME real countries,
    # sums just those.
    has_real = {k: False for k in keys}

    def _fetch(c):
        scaled = None
        if member:
            # Real per-country per-member lookup (_real_member_stats), a
            # country this member's envelope doesn't reach at all still
            # falls back to that country's own base/Probabilistic stats
            # (matching the old function's own "unknown member -> no
            # scaling" contract), not a silent zero.
            scaled = _real_member_stats(c, date, run, wind_kt, member, hz=hz)
        if scaled is None:
            scaled = _get_country_stats(c, date, run, wind_kt, hz=hz)
        return scaled

    # Each _fetch(c) call is an
    # independent per-country Snowflake-backed round-trip, this helper is
    # shared by every Global/Combined call site in the file (the Impact
    # Summary panel, the Full Impact Breakdown modal, the Hazard
    # Contribution popup's threshold curves), several of which pass in
    # every active country (5-15+ during a real multi-country event like
    # MELISSA) with no parallelization, unlike the sibling per-country-
    # selected code paths elsewhere in this file. Fetching concurrently
    # turns N sequential round-trips into ~1.
    scaled_per_country = list(get_query_executor().map(_fetch, countries))
    for scaled in scaled_per_country:
        for k in keys:
            v = _parse_stat_number(scaled.get(k, "0"))
            if v is None:
                continue
            has_real[k] = True
            totals[k] += v
    # People at Risk/Children at Risk always end up has_real=True here
    # naturally (every real per-country `scaled` dict always has a real
    # value for these two, see this function's own top comment), since a
    # genuinely empty `countries` list is already handled by the early
    # return above before this loop ever runs.
    return {k: (_format_stat_number(totals[k]) if has_real[k] else None) for k in keys}


def _combined_in_need_total(countries, risk_key, pin_key, member=None, date=None, run=None, wind_kt=None, hz=None):
    """Sums only the countries with real in-need data for `pin_key`
    ("people"/"children"), silently excluding any country whose own abs
    count is None (genuinely missing, e.g. Cuba's real all-NULL
    E_PEOPLE_IN_NEED, see _fetch_real_combined_tile_totals_uncached's own
    comment). This is the "sum only real countries, note it's partial"
    policy, use _combined_in_need_is_partial alongside this to know
    whether any country was excluded, so the UI can flag the total as
    partial rather than silently understating it.

    Must NOT reconstruct each country's contribution as `at_risk * pct /
    100`, where pct is ALREADY clamped to [0, 100] one level down, same
    root cause as _fetch_real_combined_tile_totals_uncached's own pin_pct
    comment: a country whose at-risk population has shrunk below its own
    real (threshold-independent) in-need count would then silently
    contribute at_risk instead of its true, larger in-need count. Sums
    the real, un-derived `{pin_key}_abs` count directly instead,
    `risk_key`/`base` are not needed for this sum at all (kept as a
    parameter purely for this function's existing public signature; every
    call site still passes it). Returns a raw float, NOT ceil'd
    per-country before summing, ceiling each country's own contribution
    first (then summing already-ceiled parts) is its own small version of
    the same premature-rounding pattern _fetch_real_combined_tile_totals_
    uncached's own pin_pct comment describes; callers that need an
    absolute display count wrap this call in a single math.ceil()
    themselves."""
    def _fetch(c):
        scaled_pin = _real_member_pin_pct(c, date, run, wind_kt, member) if member else None
        if scaled_pin is None:
            # Sits behind one @ttl_cache'd _fetch_real_combined_tile_totals
            # per country (see _get_country_pin_pct's own docstring), so
            # this is a cache hit whenever _get_country_stats already ran
            # for the same country, the real per-country cost is the outer
            # loop below, parallelized same as _combined_stats.
            scaled_pin = _get_country_pin_pct(c, date, run, wind_kt, hz=hz)
        return scaled_pin

    bundles = list(get_query_executor().map(_fetch, countries))
    total = 0.0
    for scaled_pin in bundles:
        abs_val = scaled_pin.get(f"{pin_key}_abs")
        if abs_val is None:
            continue
        total += abs_val
    return total


def _combined_in_need_is_partial(countries, pin_key, member=None, date=None, run=None, wind_kt=None, hz=None):
    """True when at least one of `countries` has no real in-need data for
    `pin_key`, i.e. _combined_in_need_total's own sum silently excluded
    that country. Cheap: _get_country_pin_pct/_real_member_pin_pct both sit
    behind _fetch_real_combined_tile_totals's own @ttl_cache, so this is a
    cache hit, not a fresh Snowflake round-trip."""
    for c in countries:
        scaled_pin = None
        if member:
            scaled_pin = _real_member_pin_pct(c, date, run, wind_kt, member)
        if scaled_pin is None:
            scaled_pin = _get_country_pin_pct(c, date, run, wind_kt, hz=hz)
        if scaled_pin[pin_key] is None:
            return True
    return False


def _combined_in_need_pct(countries, risk_key, pin_key, base_value, member=None, date=None, run=None, wind_kt=None, hz=None):
    """Reconstructs the "Combined" column's own in-need PERCENTAGE (summed
    real in-need over `base_value`, the combined at-risk total). None only
    when NONE of `countries` has real in-need data for `pin_key` (a
    fabricated 0% would be exactly the same 0-vs-N/A bug
    _fetch_real_combined_tile_totals_uncached's own pin_pct fixes at the
    single-country level); a real total of zero among countries that DO
    report data still correctly returns 0, not None. Factored out of ~6
    near-identical inline call sites across _pin_arc_charts_block_combined/
    _impact_breakdown_content/the Global Impact Summary/the print report."""
    # An EMPTY `countries` list is real "no data
    # available" (None/N/A), same as _combined_stats's own empty-countries
    # handling above, not a confirmed 0%.
    if not countries:
        return None
    any_real = False
    for c in countries:
        scaled_pin = None
        if member:
            scaled_pin = _real_member_pin_pct(c, date, run, wind_kt, member)
        if scaled_pin is None:
            scaled_pin = _get_country_pin_pct(c, date, run, wind_kt, hz=hz)
        if scaled_pin[pin_key] is not None:
            any_real = True
            break
    if not any_real:
        return None
    # Raw float, not ceil'd, see _fetch_real_combined_tile_totals_uncached's
    # own pin_pct comment for why ceiling a percentage before it gets
    # multiplied back against a (possibly much larger) base count corrupts
    # the reconstructed absolute number; the one real ceil happens where
    # that reconstruction finally occurs (_value_td etc).
    total = _combined_in_need_total(countries, risk_key, pin_key, member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)
    return max(0.0, min(100.0, total / max(1, base_value) * 100))


def _combined_age_split(countries, member=None, date=None, run=None, wind_kt=None, hz=None):
    """Combined-across-countries real per-age at-risk/in-need breakdown,
    same sum-then-reconstruct-percentage pattern _combined_stats +
    _combined_in_need_total use together for People/Children (see e.g.
    _impact_breakdown_content's own combined_pin), applied per age band.
    Returns the SAME {label: {"at_risk": float, "in_need_pct": float|None}}
    shape _get_country_age_split does, reconstructing a combined percentage
    (summed in-need over summed at-risk) rather than an absolute in-need
    count keeps _simple_breakdown_table's consumption of this uniform
    regardless of whether a column is one country or the Combined total.
    in_need_pct is a raw float (not ceil'd here), see _combined_in_need_pct's
    own comment for why ceiling a percentage before it's multiplied back
    against the (larger) combined at-risk base corrupts the reconstructed
    absolute number; the one real ceil happens where that final
    reconstruction occurs (_value_td)."""
    # An EMPTY `countries` list is real "no data
    # available" (None/N/A) for both fields, same as _combined_stats's own
    # empty-countries handling above, not a confirmed 0.
    if not countries:
        return {label: {"at_risk": None, "in_need_pct": None, "in_need_abs": None} for label in _CHILD_AGE_BANDS}
    at_risk_totals = {label: 0.0 for label in _CHILD_AGE_BANDS}
    in_need_totals = {label: 0.0 for label in _CHILD_AGE_BANDS}
    # Tracks whether ANY country contributed real in-need data for this age
    # band, same None-vs-0 contract as _get_country_age_split/
    # _real_member_age_split (a band with zero real-data countries stays
    # None, not a fabricated 0; a band with SOME real countries sums just
    # those, same "sum only real countries" policy _combined_in_need_total
    # uses).
    has_real = {label: False for label in _CHILD_AGE_BANDS}
    # Same tracking for at_risk itself, a country's own age-band
    # population can now individually be None too (e.g. Curaçao's real
    # all-NULL E_adolescent_population, see _fetch_real_combined_tile_
    # totals_uncached's own comment), not just its in-need share.
    has_real_at_risk = {label: False for label in _CHILD_AGE_BANDS}

    def _fetch(c):
        scaled = None
        if member:
            scaled = _real_member_age_split(c, date, run, wind_kt, member, hz=hz)
        if scaled is None:
            scaled = _get_country_age_split(c, date, run, wind_kt, hz=hz)
        return scaled

    # Same reasoning as
    # _combined_stats/_combined_in_need_total above: parallelize the real
    # per-country fetch, aggregate sequentially.
    scaled_per_country = list(get_query_executor().map(_fetch, countries))
    for scaled in scaled_per_country:
        for label in _CHILD_AGE_BANDS:
            band = scaled[label]
            if band["at_risk"] is None:
                continue
            has_real_at_risk[label] = True
            at_risk_totals[label] += band["at_risk"]
            # Same root cause as _combined_in_need_total's own comment:
            # sums the real, un-derived in_need_abs directly instead of
            # reconstructing via `at_risk * in_need_pct / 100`, which
            # would silently corrupt once a country's own in_need_pct has
            # already been clamped to 100% one level down.
            if band.get("in_need_abs") is not None:
                has_real[label] = True
                in_need_totals[label] += band["in_need_abs"]
    return {
        label: {
            "at_risk": at_risk_totals[label] if has_real_at_risk[label] else None,
            "in_need_pct": ((max(0.0, min(100.0, in_need_totals[label] / at_risk_totals[label] * 100)) if at_risk_totals[label] > 0 else 0.0)
                             if has_real[label] else None),
            # Real, un-derived absolute in-need total for this age band,
            # same fix as pin_pct's own people_abs/children_abs.
            "in_need_abs": in_need_totals[label] if has_real[label] else None,
        }
        for label in _CHILD_AGE_BANDS
    }


# Copied near-verbatim from callbacks/metrics.py's _make_arc_chart for exact
# visual parity with the real dashboard's "PEOPLE IN NEED" panel
# (layouts/panels.py:871-889), same 270° 3-ring polar-bar gauge, same colors,
# same layout. Fed illustrative numbers here instead of a real Snowflake query.
def _make_arc_chart(total, exposed, in_need, exposed_label="People At Risk", in_need_label="People In Need", height=210):
    _FULL_DEG = 270.0
    _SCALE = _FULL_DEG / 100.0
    _GRAY = "#d0d5dd"
    _NAVY = "#1c3a6e"
    _BLUE = "#1cabe2"
    _ORANGE = "#f59f00"
    _RING_W = 12
    _BASES = [53, 35, 17]
    _MIN_DEG = 3.0

    def _pct_deg(pct):
        if not pct or pct <= 0:
            return 0.0
        return max(_MIN_DEG, min(_FULL_DEG, pct * _SCALE))

    no_data = total is None or total == 0
    # in_need being None means genuinely no real in-need data (see
    # _pin_arc_charts_block_from's own comment), NOT a real zero, the ring
    # renders as an explicit grey "N/A" state below, never a misleading
    # 0%-filled ring that would look identical to a real, confirmed zero.
    no_pin_data = in_need is None
    exp_pct = (exposed / total * 100) if (not no_data and exposed) else 0.0
    nee_pct = (in_need / total * 100) if (not no_data and not no_pin_data and in_need) else 0.0

    pop_deg = _FULL_DEG if not no_data else 0.0
    exp_deg = _pct_deg(exp_pct)
    nee_deg = 0.0 if no_pin_data else _pct_deg(nee_pct)

    _GAP_THETA = 357

    nee_color = _GRAY if no_pin_data else _ORANGE
    nee_name = (_t(in_need_label) + " (" + _t("N/A") + ")") if no_pin_data else _t(in_need_label)

    ring_specs = [
        (_BASES[0], pop_deg, _NAVY, _t("Population")),
        (_BASES[1], exp_deg, _BLUE, _t(exposed_label)),
        (_BASES[2], nee_deg, nee_color, nee_name),
    ]

    traces = []
    for base, fill_deg, fill_color, name in ring_specs:
        if fill_deg > 0:
            traces.append(go.Barpolar(
                r=[_RING_W], base=[base], theta=[fill_deg / 2], width=[fill_deg],
                marker_color=[fill_color], marker_line_width=0, showlegend=False, hoverinfo="skip",
            ))
        bg_deg = _FULL_DEG - fill_deg
        if bg_deg > 0:
            traces.append(go.Barpolar(
                r=[_RING_W], base=[base], theta=[fill_deg + bg_deg / 2], width=[bg_deg],
                marker_color=[_GRAY], marker_line_width=0, showlegend=False, hoverinfo="skip",
            ))

    for base, _fill, _color, name in ring_specs:
        r_mid = base + _RING_W / 2
        traces.append(go.Scatterpolar(
            r=[r_mid], theta=[_GAP_THETA], mode="text", text=[name + "   "],
            # Smaller than the real dashboard's own 10px, this chart runs at
            # a more compact height (150px vs. the real one's 210px) to fit
            # beside the table, and 10px labels were getting clipped there.
            textfont=dict(size=8, color="#333"), textposition="middle left",
            showlegend=False, hoverinfo="skip",
        ))

    tick_degs = [i * 27.0 for i in range(11)]
    tick_lbls = [str(i * 10) for i in range(11)]

    fig = go.Figure(data=traces)
    fig.update_layout(
        polar=dict(
            angularaxis=dict(visible=True, rotation=90, direction="clockwise",
                              tickvals=tick_degs, ticktext=tick_lbls, showgrid=True,
                              gridcolor="#dde3ea", griddash="dot",
                              tickfont=dict(size=8, color="#999"), showline=False, ticks=""),
            radialaxis=dict(visible=False, range=[0, 74]),
            bgcolor="rgba(0,0,0,0)", domain=dict(x=[0.0, 1.0], y=[0.0, 1.0]),
        ),
        # Transparent (not the real dashboard's solid white), this modal's
        # background is a tinted frosted-glass panel, not a plain white
        # Paper, so the chart needs to blend into it rather than sit in its
        # own white box.
        # Left margin wide enough for the longest ring label ("Children In
        # Need"/"People In Need") to not get clipped by the figure edge,
        # the labels extend leftward from their anchor point ("middle left").
        showlegend=False, margin=dict(l=55, r=20, t=16, b=20), paper_bgcolor="rgba(0,0,0,0)", height=height,
    )
    return fig



def _cat_badge(cat):
    # Handles both "Category N" (old mock shape) and the real
    # _category_label_from_kt shape ("Category N Hurricane", or a
    # sub-hurricane label like "Severe Trop. Storm", or "Unknown"), only
    # the first form abbreviates to "Cat N"; anything else is shown as-is
    # rather than crashing on a label with no trailing digit.
    # Color lookup below keys off the RAW (untranslated) label on purpose,
    # _CAT_COLORS' own keys are the English _WIND_CATS/"Unknown" forms, not
    # translated text, so translation must happen only in the displayed
    # `text`, after the color lookup, never before it.
    label = cat or "Unknown"
    parts = label.split()
    text = (_t("Cat {n}", n=parts[1]) if len(parts) >= 2 and parts[0] == "Category" and parts[1].isdigit()
            else _t(label))
    return dmc.Badge(text, variant="filled", size="sm",
                      style={"backgroundColor": _CAT_COLORS.get(label, "#8ea0ab"), "color": "#fff"})


def _storm_row(s, bordered=False, has_alert_email=False, has_warning_email=False):
    """Storm entry, used both in the top-bar search dropdown and the
    Active Storms list (Global mode, or a selected country). No separate
    'Select' button (matching global_zoom_navigation's row style, not the
    WeatherLab reference's button-per-row).

    Only the name/subtitle block itself is the "select this storm" click
    target (id={"type": "select-storm", ...}), NOT the whole row. The
    email icon must NOT be nested INSIDE that same clickable row div:
    Dash has no simple stopPropagation for native DOM click bubbling, so
    clicking the email icon would also fire select-storm and silently
    jump the app into Country Analysis mode as a side effect, even when
    the user only meant to open the storm's alert emails. Making the
    email icon a plain SIBLING of the clickable name block (not a
    descendant)
    means its click never reaches the select-storm div's own listener at
    all, so clicking it can no longer trigger storm selection.
    """
    style = {"padding": "9px 12px"}
    if bordered:
        style.update({"borderRadius": "8px", "border": "1px solid #eef2f5", "background": "#f6f9fb", "marginBottom": "8px"})
    else:
        style.update({"borderTop": "1px solid #eef2f5", "padding": "10px 14px"})

    right_side = [_cat_badge(s["cat"])]
    # Only in the Active Storms list (bordered=True), not the search
    # dropdown, avoids cluttering search results with an action button.
    #
    # has_alert_email/has_warning_email are resolved by the caller
    # (_active_storms_section) via get_storms_with_alert_emails_at()/
    # get_storms_with_warning_emails_at(), both ttl_cached real queries
    # SCOPED TO THE CURRENTLY SELECTED topbar date/run, must NOT check "has
    # this storm EVER had ANY alert/warning," which would let an icon appear
    # and then open an empty "no emails" popup for a date/time with nothing
    # real to show. Two separate icons (not merged into one): an Alert and a
    # Warning for the same storm/date are genuinely different real emails
    # (ALERT_SENT_LOG vs WATCH_SENT_LOG), a storm can have either, both, or
    # neither at a given date/time, and they open different modals/routes.
    # Colored differently (WIND blue for Alert, amber/yellow for Warning) so
    # they read as visually distinct severity tiers at a glance, not two
    # copies of the same control.
    if bordered and has_alert_email:
        right_side.append(dmc.ActionIcon(
            DashIconify(icon="carbon:email", width=14),
            id={"type": "alert-email-btn", "name": s["name"]}, n_clicks=0,
            variant="light", color=WIND, size="sm",
        ))
    if bordered and has_warning_email:
        right_side.append(dmc.ActionIcon(
            DashIconify(icon="carbon:email", width=14),
            id={"type": "warning-email-btn", "name": s["name"]}, n_clicks=0,
            variant="light", color="yellow", size="sm",
        ))

    # Real storms with a track but no measurable country impact yet (still
    # at sea) carry an empty `countries` list (see _resolve_storms_for_date),
    # say so explicitly rather than rendering a blank subtitle line.
    subtitle = ", ".join(s["countries"]) if s["countries"] else _t("No country impact yet")
    name_block = html.Div(
        [dmc.Text(s["name"], fw=700, size="sm"), dmc.Text(subtitle, size="xs", c="dimmed")],
        id={"type": "select-storm", "name": s["name"]}, n_clicks=0,
        style={"lineHeight": 1.3, "cursor": "pointer"},
    )
    return html.Div(
        dmc.Group([
            name_block,
            dmc.Group(right_side, gap=6, wrap="nowrap", align="center"),
        ], justify="space-between", align="center"),
        style=style,
    )


def _vdivider(color=None):
    return dmc.Divider(orientation="vertical", color=color)


# Lightweight, always-visible loading affordance for the two genuinely slow
# real round-trips on this page, a fresh country/storm/forecast_date
# selection (tracks+envelopes query + per-hazard stats/admin-stats tile-
# server calls, ~11s cold) and the Tiles<->Regions cmdbar-detail toggle
# (~3-5s, admin-stats re-fetch), both of which are already Inputs to
# _build_hazard_tile_config below.
#
# dcc.Loading's target_components mechanism does not reliably show this
# spinner even with the Store nested as a child of the target: it polls
# the DOM periodically, and across genuinely-slow real callback
# round-trips it can simply never catch a visible state. Dash's OWN
# generic top-level indicator (a `.dash-loading-callback`-classed div
# inserted directly under #react-entry-point) reliably appears for the
# same requests, so this reuses THAT signal via a plain MutationObserver
# (window.aotsInitLoadingIndicator, assets/maplibre_tiles.js) instead of
# relying on target_components.
#
# Lives in the always-visible top bar (rather than a floating panel) so it
# shows in both Global and Country Analysis mode, and doesn't compete for
# space with the four existing floating panels, which already occupy
# every corner of the map.
#
# Deliberately not scoped to only ms-tile-config-store's own requests
# (Dash's global indicator fires for ANY in-flight callback), a single
# generic "something is loading" badge is simpler and more honest than
# trying to isolate just this one store, and every other callback on this
# page is fast enough that this reads as correct in practice.
def _ms_loading_badge():
    return html.Div(
        dmc.Loader(size="sm", color="white", type="bars"),
        id="ms-global-loading-indicator",
        style={"display": "none"},
    )


def _topbar(initial_countries=None, default_date=_DEFAULT_FORECAST_DATE, default_run=_DEFAULT_FORECAST_RUN):
    # default_date/default_run default to the module-import-time globals
    # (correct for any call site that doesn't pass anything), but layout()
    # below passes a live-resolved value instead, see
    # _resolve_default_forecast_date_run's own docstring for why the
    # frozen globals alone aren't enough for a long-running server process.
    return html.Div([
        dmc.Group([
            # Plain html.A (real navigation, full reload), not dcc.Link,
            # deliberate: this page IS "/" itself, so a client-side dcc.Link
            # to the same pathname wouldn't force anything to reset (Dash's
            # routing only reacts to an actual pathname change). A real
            # reload genuinely resets every piece of state back to its
            # module-level default (topbar date/time/country selection/mode
            # all come from plain Python constants computed at import time,
            # e.g. _DEFAULT_FORECAST_DATE), which is what "back to the
            # start" means here.
            html.A(
                dmc.Text("AHEAD OF THE STORM", c="#ffffff",
                          style={"fontFamily": "'Handjet', sans-serif", "fontWeight": 800,
                                 "fontSize": "17px", "letterSpacing": "0.3px"}),
                href="/", style={"textDecoration": "none", "cursor": "pointer"},
            ),

            _vdivider(color="rgba(255,255,255,0.35)"),

            # Mirrors the real header's "Last Updated" (components/ui/header.py),
            # when the underlying data was last refreshed, distinct from the
            # forecast's own valid date/run picked below (see
            # _update_last_updated below), same get_latest_forecast_time_overall()
            # + 15-min dcc.Interval pattern as update_last_updated_header in
            # pages/dashboard.py, just under a distinct id/interval
            # ("ms-..." prefixed) to avoid colliding with layouts/panels.py's own
            # "metadata-refresh-interval", which is live in the same running app.
            dmc.Group([
                dmc.Text(_t("Last Updated:"), size="xs", c="white", opacity=0.8),
                dmc.Text("—", id="ms-last-updated", size="xs", fw=700, c="#ffffff"),
            ], gap=6, wrap="nowrap"),

            _vdivider(color="rgba(255,255,255,0.35)"),

            dmc.Group([
                dmc.DatePickerInput(
                    id="topbar-date", value=default_date, valueFormat="D MMM YYYY", size="xs", w=130,
                    leftSection=DashIconify(icon="carbon:calendar", width=13),
                    # Greys out/disables any calendar day later than the
                    # latest REAL forecast_time in the database (not
                    # wall-clock "today", see _max_allowed_run_for_date's
                    # own docstring) so a date with no real data can't be
                    # picked at all, rather than silently resolving to an
                    # empty page.
                    maxDate=default_date,
                    # The top bar itself sits at zIndex 1000 (_topbar's
                    # style) so it layers over the map/panels below it, but
                    # that also meant it was cutting into this popover's own
                    # (lower, Mantine-default) z-index when it opened right
                    # under the bar. Bump it above the bar explicitly.
                    popoverProps={"zIndex": 2000},
                ),
                dmc.SegmentedControl(
                    id="topbar-time", value=default_run,
                    data=_time_options_for_date(default_date, _LATEST_FORECAST_TIME, default_date, default_run),
                    size="xs",
                ),
            ], gap=8, wrap="nowrap"),

            _vdivider(color="rgba(255,255,255,0.35)"),

            dmc.SegmentedControl(
                id="topbar-mode", value="zoom" if initial_countries else "global",
                data=[{"value": "global", "label": _t("Global")}, {"value": "zoom", "label": _t("Country Analysis")}],
                size="xs",
            ),

            # Real multi-select: picking a storm that affects several countries
            # (e.g. BAVI, Philippines + Vietnam) selects all of them here at
            # once, and the Impact Summary panel shows one stat block per
            # selected country side by side, rather than forcing one arbitrary
            # pick or a separate comparison view.
            dmc.MultiSelect(
                id="topbar-country-select", value=initial_countries or [],
                placeholder=_t("Select countries…"), data=_country_options(),
                searchable=True, clearable=True, size="xs", w=260, maw=320,
                leftSection=DashIconify(icon="carbon:location", width=13),
                # Same z-index bump as the date picker above, for the same
                # reason, otherwise the dropdown opens partly under the bar.
                comboboxProps={"zIndex": 2000},
                # Keep selected-country pills on one line, side by side, no
                # matter how many are picked, never wrap onto a second row
                # and grow the top bar taller. Extra pills scroll
                # horizontally within the fixed-width box instead.
                styles={"pillsList": {"flexWrap": "nowrap", "overflowX": "auto"},
                         "pill": {"flexShrink": 0}},
            ),

            _vdivider(color="rgba(255,255,255,0.35)"),

            _ms_loading_badge(),

        ], gap=18, wrap="wrap", align="center", style={"paddingRight": "150px"}),
        # Absolutely positioned against the bar itself (not a marginLeft:auto
        # flex child), guarantees it stays pinned to the top-right corner
        # even when the rest of the bar's items wrap onto a second line on a
        # narrow viewport, which a flex auto-margin trick wouldn't survive.
        # right:24px (its own value, not _UI_MARGIN), must NOT align this
        # exactly with the panel edges (15px), which puts it visibly too
        # close to the screen's own edge; the topbar/footer's
        # own content wants more breathing room from the true screen edge
        # than the floating panels need from THEM, even though the panels
        # themselves stay at the tighter _UI_MARGIN.
        html.Div(_language_switcher(_LANG),
                  style={"position": "absolute", "top": "50%", "right": "20px", "transform": "translateY(-50%)"}),
    ],
        # Full-width fixed bar flush with the top edge, like the footer at
        # the bottom, not a floating rounded card (that was this shell's
        # earlier "map-first" treatment; the bar is chrome now, same tier as
        # the footer). 24px horizontal padding, its own value, see the
        # language-switcher comment just above for why this isn't _UI_MARGIN.
        style={"position": "fixed", "top": 0, "left": 0, "width": "100%",
               "backgroundColor": "#00AEEF", "padding": "14px 20px",
               "display": "flex", "alignItems": "center", "zIndex": 1000},
    )


# ---------------------------------------------------------------------------
# Controls panel, content depends on mode (Global vs Country Analysis), but
# Hurricane/Flood Hazards are IDENTICAL components in both (only one mode's
# content is ever mounted at a time, so reusing the same ids is safe). Global
# additionally lacks Exposure/Infrastructure, since those need a selected
# country to mean anything. There's no severity-exploration reason to dumb
# Global down to plain checkboxes, the same "what if this hits Category X"
# question is just as valid before you've zoomed into a place.
# ---------------------------------------------------------------------------
def _active_storms_section(countries=None, date=None, run=None):
    # Standalone, sits ABOVE the Tropical Cyclone family in both modes, not
    # nested inside it. Scoped to whichever countries are selected. A storm
    # matches if it affects ANY of the selected countries (not all), BAVI
    # shows up whether you've picked Philippines, Vietnam, or both.
    #
    # REACTIVE to the selected topbar date/run (_resolve_storms_for_date),
    # not the frozen "active right now" _STORMS snapshot. This is what makes
    # a historical Demo Scenario date (e.g. MELISSA on 28 Oct 2025) show the
    # exact same bordered storm-row box (name, category, alert-email icon) a
    # genuinely-live storm gets, real data either way. No "Historical"
    # badge distinguishing the two (removed, it read as wrong even for
    # currently-live storms, since "live" was derived from country-impact
    # membership, which a real storm with no impact yet never satisfies).
    #
    # Returns None (renders nothing) when there's genuinely no real storm for
    # this date, the panel then opens straight on the Exposure/Hazard switch
    # instead of spending the top slot on an empty placeholder message.
    # Callers must filter out this None (see _controls_global/_controls_zoom).
    countries = countries or []
    all_storms = _resolve_storms_for_date(date, run)
    scoped_storms = [s for s in all_storms if not countries or any(c in s["countries"] for c in countries)]
    if not scoped_storms:
        return None

    display_date = date or _DEFAULT_FORECAST_DATE
    display_run = run if run is not None else _DEFAULT_FORECAST_RUN
    try:
        date_label = f"{pd.Timestamp(display_date).strftime('%b %-d')}, {display_run}Z"
    except Exception:
        date_label = f"{display_date}, {display_run}Z"

    if not countries:
        label = _t("Active Storms — {date}", date=date_label)
    elif len(countries) == 1:
        label = _t("Active Storm — {country}", country=_t(countries[0]))
    else:
        label = _t("Active Storms — {countries}", countries=", ".join(_t(c) for c in countries))

    # Real, date/run-scoped check (see get_storms_with_alert_emails_at's own
    # docstring), one query for the whole list, not one per storm row.
    target_forecast_time = f"{display_date} {display_run}:00:00"
    storms_with_alerts = get_storms_with_alert_emails_at(target_forecast_time)
    storms_with_warnings = get_storms_with_warning_emails_at(target_forecast_time)
    content = html.Div([_storm_row(s, bordered=True, has_alert_email=(s["name"] in storms_with_alerts),
                                     has_warning_email=(s["name"] in storms_with_warnings))
                          for s in scoped_storms])
    children = [dmc.Text(label, size="10px", fw=700, c="dimmed", tt="uppercase", mb=10), content]
    if countries:
        names = ", ".join(s["name"] for s in scoped_storms)
        children.append(dmc.Text(_t("Currently tracking {names}.", names=names), size="11px", c="dimmed", mt=10,
                                   style={"lineHeight": 1.6}))
    # Bottom padding trimmed to 12px (from the section's own 20px top/side
    # padding), each storm row already carries its own 8px marginBottom
    # (_storm_row, bordered=True), so the last card's bottom edge otherwise
    # sat ~28px from the divider below, visibly more than the ~20px gap
    # every other section boundary on this panel uses.
    return html.Div(children, style={"padding": "20px 18px 12px", "borderTop": "1px solid #eef2f5"})



# Real, verified provenance for every toggleable map layer (not guessed,
# see each entry's own citation). Proper-noun product/organization names are
# NOT translated (same convention as this file's numeric legend values):
# only the "Source: " prefix passes through _t().
#   - Storm Tracks/Sustained Wind: ECMWF tropical cyclone ensemble track
#     forecasts, BUFR format, via ECMWF Open Data (Ahead-of-the-Storm-
#     ORCHESTRATION/04_data/04_tc_ecmwf_tables.sql:14, 42-46, 51 members +
#     control, NOT TIGGE).
#   - Gust: same ECMWF ensemble, 10m wind gust (10fg) field
#     (.../11_tc_gust_envelope_tables.sql:17-19).
#   - River Flooding: GloFAS v4.0 ensemble forecast (Copernicus CEMS)
#     matched against JRC's Global River Flood Hazard Maps v2.1
#     (.../10_glofas_tables.sql:12,45-46).
#   - Rainfall: same ECMWF ensemble, total precipitation (tp) field
#     (.../09_met_forecasts_table.sql:19-20,24-27).
#   - Storm Surge: no real pipeline exists yet (see this hazard's own
#     "Coming Soon" badge), no source to cite.
#   - Schools: UNICEF Giga school-location API (Ahead-of-the-Storm-
#     DATAPIPELINE/impact_analysis.py:728,76).
#   - Health Centers: HealthSites.io (impact_analysis.py:655,269).
#   - Shelters: OpenStreetMap, social_facility=shelter tag
#     (impact_analysis.py:766,788-790).
#   - WASH: OpenStreetMap, humanitarian WASH tags (impact_analysis.py:320).
#   - Population/Children/age bands: WorldPop population estimates, via the
#     gigaspatial library (impact_analysis.py:9-12).
#   - Built-up Area / Settlement Classification: EU JRC Global Human
#     Settlement Layer (GHS-BUILT-S built surface / GHS-SMOD settlement
#     classes respectively, impact_analysis.py:9-12).
#   - Relative Wealth Index: Meta/Facebook Relative Wealth Index, hosted on
#     HDX (impact_analysis.py:118; gigaspatial/handlers/rwi.py:4-5).
#   - Moderate/Severe Child Poverty Rate: NOT a separate raw dataset, a
#     modeled estimate derived from that same Meta/HDX RWI, calibrated via
#     PCHIP/Akima curve fitting to real UNICEF child poverty survey rates
#     (Ahead-of-the-Storm-DATAPIPELINE/impact_analysis.py:574-576, the
#     vulnerability/fetch_vulnerability_probs.py script's own docstring).
_LAYER_DATA_SOURCES = {
    "tracks": "ECMWF ensemble tropical cyclone forecast (51 members + control), ECMWF Open Data",
    "wind": "ECMWF ensemble tropical cyclone forecast (51 members + control), ECMWF Open Data",
    "gust": "ECMWF ensemble forecast, 10m wind gust (10fg field)",
    "river": "GloFAS v4.0 ensemble forecast (Copernicus CEMS) x JRC Global River Flood Hazard Maps v2.1",
    "rain": "ECMWF ensemble forecast, total precipitation (tp field)",
    "surge": "No real data pipeline yet — preview only",
    "schools": "UNICEF Giga school-location API",
    "health": "HealthSites.io",
    "shelters": "OpenStreetMap (social_facility=shelter tag)",
    "wash": "OpenStreetMap (humanitarian WASH tags)",
    "population": "WorldPop population estimates",
    "children": "WorldPop population estimates",
    "infant": "WorldPop population estimates",
    "school-age": "WorldPop population estimates",
    "adolescent": "WorldPop population estimates",
    "built": "EU JRC Global Human Settlement Layer (GHS-BUILT-S)",
    "settlement": "EU JRC Global Human Settlement Layer (GHS-SMOD)",
    "rwi": "Meta/Facebook Relative Wealth Index (hosted on HDX)",
    "moderate-poverty": "Modeled from Meta/Facebook RWI, calibrated to real UNICEF child poverty survey rates",
    "severe-poverty": "Modeled from Meta/Facebook RWI, calibrated to real UNICEF child poverty survey rates",
}

_HAZARD_SOURCE_DISPLAY_NAMES = {"ecmwf": "ECMWF", "glofas": "GloFAS"}


def _hazard_source_select(hazard_key):
    """A genuinely functioning dmc.Select (not a disabled mockup)
    reading from _DEFAULT_HAZARD_SOURCES (this file's own "multi-source
    ready" plumbing, see that dict's own comment near _build_hz), with
    exactly ONE real option today.

    Deliberately NOT wired to actually change what data loads yet: a
    genuinely different second source per hazard needs its own real
    ingestion pipeline AND a real methodology decision (see
    _DEFAULT_HAZARD_SOURCES's own comment, e.g. Google FloodHub is not
    ECMWF-ensemble-driven, so it would not share Wind/Rain's real member
    identity the way GloFAS does today, meaning the per-member bitmask
    union this app relies on could not just swap sources transparently).
    This control exists purely so the eventual multi-option version's
    real look/placement can be evaluated now, before any of that
    pipeline/methodology work is undertaken.

    Direct indexing (not a `.get()` with a blank fallback) into
    _DEFAULT_HAZARD_SOURCES: falling back to a blank source ("") on an
    unrecognized hazard_key would silently render an empty,
    non-functional combobox with no error, contradicting this same
    "multi-source ready" plumbing's own fail-loud convention
    (get_wind_tile_bitmask etc. all raise NotImplementedError for
    anything but today's real source). A future typo'd hazard_key at a
    new call site should fail immediately and visibly (KeyError) instead
    of shipping a silently-broken dropdown."""
    source = _DEFAULT_HAZARD_SOURCES[hazard_key]
    label = _HAZARD_SOURCE_DISPLAY_NAMES.get(source, source.upper())
    return dmc.Select(
        data=[{"value": source, "label": label}],
        value=source,
        size="xs",
        w=92,
        allowDeselect=False,
        comboboxProps={"withinPortal": True},
        styles={
            "input": {"fontSize": "10px", "height": "22px", "minHeight": "22px",
                       "paddingTop": 0, "paddingBottom": 0, "color": "#57707e"},
        },
    )


# Per-country facility overrides: some countries have a custom facility file
# (geodb/custom/<code>_<kind>.csv> in the DATAPIPELINE repo) that the pipeline
# uses INSTEAD of the standard API/OSM source above, checked by file
# presence, never recorded as a queryable column anywhere in Snowflake (see
# impact_analysis.py's fetch_schools/
# fetch_health_centers/fetch_shelters/fetch_wash). This dict is a static
# snapshot of the real custom files present on disk, it
# will silently go stale if a custom file is added/removed later, since
# there's no live way to check this from the dashboard (a real fix needs a
# new SOURCE column added to the MAT tables/pipeline).
# Keyed by the app's own ISO3 COUNTRY_CODE (not the DATAPIPELINE repo's
# alpha-2 filename prefixes, e.g. "DO_schools.csv" -> "DOM" here).
_FACILITY_CUSTOM_SOURCES = {
    ("DOM", "schools"): "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom",
    ("DOM", "health"): "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom",
    ("DOM", "wash"): "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom",
    ("HND", "schools"): "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom",
    ("HND", "health"): "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom",
    ("HND", "wash"): "SIASAR (Sistema de Información de Agua y Saneamiento Rural) — custom",
    ("JAM", "health"): "Jamaica Ministry of Health facility registry — custom",
    ("JAM", "shelters"): "supportjamaica.gov.jm (Government of Jamaica official shelter registry) — custom",
    ("JAM", "wash"): "National Water Commission / Government of Jamaica (NWC/GOJ ArcGIS) — custom",
}

# The 4 facility layer ids get_facility_source (snowflake_utils.py) can
# actually query, matches _FACILITY_SOURCE_TABLE there exactly.
_FACILITY_SOURCE_LAYERS = {"schools", "health", "shelters", "wash"}


def _resolve_facility_custom_source(code, source_key):
    """Real per-country/per-layer custom facility source override (raw,
    untranslated), or None when the standard source applies. Prefers the
    real live SOURCE column (get_facility_source) when it has real data for
    this country/layer, falls back to the static _FACILITY_CUSTOM_SOURCES
    snapshot (still accurate for DOM/HND/JAM until their pipeline is
    re-run), then None (standard source). Shared by _layer_label_with_info
    (Infrastructure checkbox tooltips) and _data_availability_table (which
    marks custom facility counts so a real "is this a standard source"
    question doesn't require opening a different panel to answer)."""
    if not code or source_key not in _FACILITY_SOURCE_LAYERS:
        return None
    override = None
    try:
        override = get_facility_source(code, source_key)
    except Exception as e:
        logger.warning("Could not load live facility source for %s/%s: %s", code, source_key, e)
    if not override:
        override = _FACILITY_CUSTOM_SOURCES.get((code, source_key))
    return override


def _layer_label_with_info(label_text, source_key, countries=None):
    """A layer's translated label text plus a small hover-only info icon
    surfacing its real data source (_LAYER_DATA_SOURCES), for use as a
    dmc.Checkbox's own `label` prop, which accepts any Dash component, not
    just a string. dmc.Tooltip needs no server round-trip; the hover
    interaction is handled entirely client-side by Mantine itself.

    For facility layers, a single selected country may use a custom source
    instead of the standard one, only shown when exactly one country is
    selected, since a multi-country/region selection can't unambiguously
    attribute one source to the whole layer. Prefers the REAL live SOURCE
    column (get_facility_source, BASE_SCHOOL_MAT/BASE_HC_MAT/
    BASE_SHELTER_MAT/BASE_WASH_MAT) when it has real data
    for this country/layer; falls back to the static _FACILITY_CUSTOM_
    SOURCES snapshot (still accurate for DOM/HND/JAM until their pipeline
    is re-run and actually populates the new column), then to the generic
    standard-source description."""
    override = None
    if countries and len(countries) == 1:
        # countries[0] is a display name (e.g. "Jamaica"), _FACILITY_CUSTOM_
        # SOURCES/get_facility_source are both keyed by the app's own ISO3
        # COUNTRY_CODE ("JAM"), so the raw display name must be converted
        # here or lookups on it would never match, silently falling back
        # to the generic OSM source instead of the real custom one.
        override = _resolve_facility_custom_source(_NAME_TO_CODE.get(countries[0]), source_key)
    source = _t(override or _LAYER_DATA_SOURCES.get(source_key, ""))
    return html.Span([
        label_text,
        dmc.Tooltip(
            label=_t("Source: {source}", source=source),
            multiline=True,
            w=230,
            withArrow=True,
            position="right",
            transitionProps={"duration": 0},
            styles={"tooltip": {"fontSize": "11px", "lineHeight": 1.4}},
            children=html.Span(
                DashIconify(icon="mdi:information-outline", width=12),
                style={"color": "#9aa7b0", "marginLeft": "5px", "cursor": "help",
                       "verticalAlign": "middle", "display": "inline-flex"},
            ),
        ),
    ], style={"display": "inline-flex", "alignItems": "center"})


def _hurricane_family(countries=None, expanded=True, date=None, run=None):
    # No storms list in here anymore, that's _active_storms_section above.
    # When nothing's active, collapse straight to grey: no body at all, not
    # a redundant "nothing here" message (the Active Storms section above
    # already said that), disabled checkboxes/sliders with nothing behind
    # them just read as broken, not "off".
    #
    # Header is a pure collapse (chevron), not a switch, collapsing the
    # section no longer implies turning the hazard off; Sustained Wind/Gust
    # keep whatever checked state they already have underneath.
    countries = countries or []
    is_global = not countries
    if countries:
        # REACTIVE: does any SELECTED country have real storm data for the
        # CURRENTLY selected topbar date/run (_resolve_storm_for_country),
        # not "is something active right now" (the frozen _STORMS snapshot,
        # correct only for the Global-mode Active Storms list below). This is
        # what lets Sustained Wind/Gust/Tracks stay enabled for a historical
        # date (date picker, Demo Scenarios) with no currently-active storm.
        has_storms = any(_resolve_storm_for_country(c, date, run) for c in countries)
        # Gust is storm-scoped, not country-scoped (unlike River/Rain), so
        # availability is "does the resolved storm(s) for the selected
        # countries have real gust data at this forecast_time", reuses
        # each country's already-resolved storm/forecast_time instead of a
        # fresh per-country query.
        _resolved = [_resolve_storm_for_country(c, date, run) for c in countries]
        gust_available = any(
            r and r["name"] in get_gust_track_ids_for_date(r["forecast_time"])
            for r in _resolved
        )
    else:
        # Global (nothing selected), REACTIVE to the selected topbar
        # date/run, matching _load_ms_tracks_and_envelopes's own Global-mode
        # tracks resolution exactly (get_track_ids_for_date, NOT the frozen
        # _STORMS snapshot, which is only ever "active in the last 12h as of
        # whenever this server process started" and was found live to say
        # "no storms" even when 2 real ones, DOLPHIN/GENEVIEVE, existed for
        # the selected date, this header must never disagree with what the
        # map itself is actually showing).
        _d = date or _DEFAULT_FORECAST_DATE
        _r = run if run is not None else _DEFAULT_FORECAST_RUN
        _forecast_time_str = f"{_d} {_r}:00:00" if _d and _r is not None else None
        has_storms = bool(get_track_ids_for_date(_forecast_time_str)) if _forecast_time_str else False
        gust_available = bool(get_gust_track_ids_for_date(_forecast_time_str)) if _forecast_time_str else False
    header = html.Div([
        html.Span(style={"width": "8px", "height": "8px", "borderRadius": "50%",
                          "background": WIND if has_storms else "#c3ccd2"}),
        dmc.Text(_t("Tropical Cyclone"), fw=700, size="sm",
                  style={"flex": 1, "color": "#16232c" if has_storms else "#a8b3bd"}),
        html.Span("▾", id="chev-hurricane", style={"fontSize": "10px",
                   "color": "#8ea0ab" if has_storms else "#c3ccd2"}),
    ], id="head-hurricane", style={"display": "flex", "alignItems": "center", "gap": "8px",
                                     "cursor": "pointer" if has_storms else "default"})

    def _slider_block(sub_key, color, max_val, default, disable_in_global=True, available=True):
        # wind's own threshold slider stays enabled in Global mode (unlike
        # gust), it genuinely drives which wind-severity tier's envelope
        # polygons render there (_load_ms_tracks_and_envelopes's Global
        # branch), so leaving it disabled would block the one control that
        # actually does something in Global mode.
        return html.Div([
            # label=None: Mantine's default
            # drag/hover thumb bubble shows the raw numeric INDEX (e.g. a
            # bare "2"), not a human-readable tier name, Dash can't pass a
            # JS value-formatter callable through this prop the way plain
            # React Mantine usage can, so a real "Category 2"-style bubble
            # isn't achievable here. The live readout div right below
            # already shows the full real label as the slider moves, so
            # suppressing the misleading bare-index bubble (rather than
            # showing it) is the honest fix.
            dmc.Slider(id=f"ms-{sub_key}-slider", min=0, max=max_val, step=1, value=default,
                       marks=[{"value": i} for i in range(max_val + 1)], size="sm", color=color, mb=4, label=None,
                       disabled=(not has_storms) or (is_global and disable_in_global) or (not available)),
            html.Div(id=f"ms-{sub_key}-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "16px"})

    # Always rendered (matching _flood_hazards_family's own unconditional
    # pattern), these ids are referenced unconditionally by several
    # callbacks (_update_impact_summary, _update_impact_breakdown,
    # _update_breakdown_new_tab_link, _open_hazard_contribution), so omitting
    # them when has_storms is False (the old behavior, harmless back when
    # _STORMS was a mock list that could never actually be empty) hard-crashes
    # Dash's client dispatcher the moment a real "no active storm anywhere"
    # state occurs, which, for real Snowflake-backed data, is the common
    # case, not an edge case. Disabling them (instead of omitting) keeps the
    # "nothing to toggle right now" signal without breaking every callback
    # that reads their state.
    body = html.Div([
        dmc.Checkbox(id="ms-tracks-on", label=_layer_label_with_info(_t("Storm Tracks"), "tracks"),
                      color=TRACKS, size="xs", mb=10, checked=True,
                      disabled=not has_storms),
        # Off by default in Global mode (still checkable, see
        # _load_ms_tracks_and_envelopes's own Global branch, which now
        # really does render every active storm's own real wind envelope
        # polygons at the selected threshold when this is checked, same
        # TC_ENVELOPES_COMBINED data as the Country-Analysis envelope view,
        # just without per-country severity coloring since there's no single
        # country to attribute population severity to here).
        dmc.Group([
            dmc.Checkbox(id="ms-wind-on", label=_layer_label_with_info(_t("Sustained Wind"), "wind"),
                          color=WIND, size="xs",
                          checked=not is_global, disabled=not has_storms),
            _hazard_source_select("wind"),
        ], justify="space-between", mb=10, wrap="nowrap"),
        _slider_block("wind", WIND, 7, 2, disable_in_global=False),  # index 2 == 26 m/s / 50kt
        # Must NOT be hard-disabled in Global mode on the assumption that
        # no gust envelope data exists, TC_GUST_ENVELOPES_COMBINED
        # (get_gust_envelope_data_snowflake) is a genuinely real,
        # separately deployed table mirroring TC_ENVELOPES_COMBINED with
        # GUST_THRESHOLD instead of WIND_THRESHOLD. Gust renders real
        # Global-mode envelope
        # polygons exactly like Wind (_load_ms_tracks_and_envelopes), so it
        # follows the same enable/default rule as Wind above.
        dmc.Group([
            dmc.Checkbox(id="ms-gust-on", label=_layer_label_with_info(_t("Gust"), "gust"),
                          color=GUST, size="xs", checked=False,
                          disabled=(not has_storms) or (not gust_available)),
            _hazard_source_select("gust"),
        ], justify="space-between", mb=10, wrap="nowrap"),
        # Gust envelope data is genuinely real but only exists for a
        # handful of historical storms/dates, TC_GUST_ENVELOPES_COMBINED
        # can have zero gust rows for a currently active storm even while
        # its wind envelope data is complete. Same "no real forecast data"
        # explanatory note as River/Rain's own _availability_note above,
        # so an ungreyed-but-disabled checkbox never reads as a bug.
        (dmc.Text(_t("No real gust forecast data for this storm/date."),
                   size="10px", c="dimmed", fs="italic", mb=8)
         if has_storms and not gust_available else None),
        _slider_block("gust", GUST, 7, 2, disable_in_global=False, available=gust_available),  # index 2 == 26 m/s / 50kt
        # Visible (Global mode) vs hidden-but-mounted (Country Analysis),
        # see this html.Div's own comment just below. Real behavior in
        # Country Analysis now comes from the Hazard tab's own
        # ms-hazard-render-mode switch (Raw/Probability/Classification,
        # _hazard_render_mode_switch), which drives THIS component's value
        # programmatically (see _sync_tc_view_as) rather than the user
        # clicking it directly there, this whole page's other 3 real
        # consumers of tc-view-as (_update_map_legend, the hazard-
        # contribution content callback, _load_ms_tracks_and_envelopes) are
        # completely unchanged, still just reading Input("tc-view-as",
        # "value") as before, so keeping the component itself (rather than
        # deleting it) is what avoids touching all three.
        html.Div([
            dmc.Text(_t("View As"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
            # Locked to Envelopes in Global mode, Probability Raster is
            # specifically the country-scoped wind/gust MapLibre raster
            # (_build_hazard_tile_config returns wind_visible=False whenever no
            # country is selected, by design), which never renders anything
            # without a selected country, unlike Envelopes above.
            #
            # Default value is MODE-DEPENDENT: Global mode
            # defaults to "envelopes"
            # (Global-mode envelope rendering is itself gated on
            # tc_view_as=="envelopes", see _load_ms_tracks_and_envelopes' own
            # docstring, so this default must stay put there), but Country
            # Analysis now defaults to "raster" (Probability) instead of
            # Envelopes, the standard first-look view for a selected country
            # should be the country-scoped probability raster, not the raw
            # ensemble envelope polygons. Since only one of Global/Country
            # Analysis is ever mounted at a time (this same function, same
            # component id, reused across both, see this function's own
            # header comment), a fresh render always picks the right default
            # for whichever mode is actually showing, immediately overridden
            # in Country Analysis by _sync_tc_view_as's own initial fire.
            dmc.SegmentedControl(
                id="tc-view-as", value=("envelopes" if is_global else "raster"), fullWidth=True, size="xs",
                disabled=(not has_storms) or is_global,
                data=[{"value": "envelopes", "label": _t("Extent")},
                      {"value": "raster", "label": _t("Probability")}],
            ),
        # Always hidden now, in both modes: Country Analysis already hid this
        # (display: none, driven programmatically instead by the Hazard
        # tab's own switch, see this Div's own comment above), and Global
        # mode's own copy is permanently disabled AND value-locked to
        # "envelopes" (line just above), never actually clickable there
        # either, so showing it in Global mode was just visual clutter with
        # no real control behind it, not a genuine option. The component
        # itself stays mounted either way (unchanged), only its wrapping
        # Div's visibility changes, this page's other 3 real consumers of
        # Input("tc-view-as", "value") are untouched.
        ], style={"display": "none"}),
    ], id="ms-hurricane-body", style={"marginTop": "16px"} if (has_storms and expanded) else {"marginTop": "16px", "display": "none"})

    return html.Div([header, body], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _collapsible_section(key, title, body, expanded=True):
    """Collapsible section WITHOUT a master switch, Exposure/Infrastructure
    aren't "is this visible" binaries the way Hurricane/Flood are. Neither has
    a WeatherLab equivalent at all: it only ever shows meteorological fields,
    never population/facility impact.
    """
    return html.Div([
        html.Div([
            dmc.Text(title, fw=700, size="sm", style={"flex": 1}),
            html.Span("▾", id=f"chev-{key}", style={"fontSize": "10px", "color": "#8ea0ab"}),
        ], id=f"head-{key}", style={"display": "flex", "alignItems": "center", "gap": "8px", "cursor": "pointer"}),
        html.Div(body, id=f"body-{key}", style={"marginTop": "16px"} if expanded else {"marginTop": "16px", "display": "none"}),
    ], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _data_availability_table(countries):
    # Real per-country availability overview, see _get_data_availability_real
    # above for the real check_baseline_data.py-style query this mirrors.
    countries = countries or []
    if not countries:
        return dmc.Text(_t("Select a country to see data availability."), size="11px",
                          c="dimmed", fs="italic")

    fields = [("population", _t("Pop")), ("age_0_4", _t("0-4")), ("age_5_14", _t("5-14")),
              ("age_15_19", _t("15-19")), ("rwi", _t("RWI")), ("settlement", _t("Settlement")),
              ("moderate_poverty", _t("Mod. Poverty")), ("severe_poverty", _t("Sev. Poverty"))]

    def _flag(ok):
        return html.Span("✓" if ok else "✕", style={"color": "#2f9e64" if ok else "#d94f3c", "fontWeight": 700})

    # Real per-country custom-source data exists
    # (_resolve_facility_custom_source, shared with the
    # Infrastructure checkboxes' own info-icon tooltips) and is surfaced
    # here as separate spans (not one plain joined sentence) so a
    # custom-sourced count can carry its own marker + tooltip
    # without disturbing the other three.
    facility_fields = [("schools", _t("Schools")), ("health", _t("HCs")),
                        ("shelters", _t("Shelters")), ("wash", _t("WASH"))]

    def _facility_line(code, d):
        count_key = {"schools": "schools", "health": "health_centers", "shelters": "shelters", "wash": "wash"}
        items = []
        any_custom = False
        for i, (layer_key, label) in enumerate(facility_fields):
            if i > 0:
                items.append(html.Span("·", style={"color": "#8ea0ab"}))
            # d[...] is None (not a confirmed real 0) when this country
            # genuinely has no real data for this facility type, see
            # _get_data_availability_real's own _total comment.
            _count = d[count_key[layer_key]]
            count_text = f"{_count:,} {label}" if _count is not None else f"{_t('N/A')} {label}"
            custom_source = _resolve_facility_custom_source(code, layer_key)
            if custom_source:
                any_custom = True
                items.append(dmc.Tooltip(
                    label=_t("Custom source: {source}", source=_t(custom_source)),
                    multiline=True, w=220, withArrow=True, position="top",
                    transitionProps={"duration": 0},
                    styles={"tooltip": {"fontSize": "11px", "lineHeight": 1.4}},
                    children=html.Span([
                        count_text,
                        html.Span(" *", style={"color": "#d9822b", "fontWeight": 700, "cursor": "help"}),
                    ]),
                ))
            else:
                items.append(html.Span(count_text))
        # dmc.Tooltip renders its own wrapper as a block-level element by
        # default, which breaks plain inline text flow (a dmc.Text with a
        # mixed list of html.Span/dmc.Tooltip children) every time one
        # appears mid-sentence. Setting style={"display": "inline"}
        # directly on the Tooltip itself does NOT fix this, that style
        # prop targets the floating popup content, not the reference
        # wrapper around its children. The fix is the same pattern
        # _layer_label_with_info already uses successfully: put the WHOLE
        # row in a flex container (dmc.Group, Mantine's own flex-row-with-
        # wrap primitive, already used for the ✓ flags row right below
        # this) instead of relying on native inline text flow, since
        # flexbox lays out any mix of inline/block children as a single
        # wrapping row regardless of each child's own default display type.
        # gap=4 (not 0) puts even spacing around every
        # item, including the bare "·" separators above, rather than a
        # baked-in " · " (spaces inside the string itself), which would
        # double up unevenly with a nonzero Group gap.
        return dmc.Group(items, gap=4, wrap="wrap", style={"rowGap": "2px"}), any_custom

    # _get_data_availability_real (the one-round-trip get_data_availability
    # aggregate, see its own docstring) is called nowhere else in the
    # codebase, so it's never prewarmed, a multi-country selection's
    # first render still pays N cold-cache round-trips here. Fetching
    # concurrently, rendering in order, same as every sibling per-country
    # loop in this file.
    availability_per_country = dict(zip(countries, get_query_executor().map(_get_data_availability_real, countries)))
    rows = []
    for c in countries:
        d = availability_per_country[c]
        if not d:
            rows.append(dmc.Text(_t("{c}: no availability data.", c=_t(c)), size="11px", c="dimmed", mb=8))
            continue
        code = _NAME_TO_CODE.get(c)
        facility_spans, any_custom = _facility_line(code, d)
        rows.append(html.Div([
            dmc.Text(_t(c), size="11px", fw=700, mb=4),
            dmc.Text(facility_spans, size="10.5px", c="dimmed", mb=2 if any_custom else 4),
            dmc.Text(_t("* custom data source"), size="9.5px", c="#d9822b", fs="italic", mb=4) if any_custom else None,
            dmc.Group([
                html.Span([_flag(d[key]), " ", label], style={"fontSize": "10.5px", "color": "#57707e"})
                for key, label in fields
            ], gap=10, style={"rowGap": "4px"}),
        ], style={"marginBottom": "14px"}))

    return html.Div(rows)


def _exposure_section():
    # No "Exposure" header here, it's the first thing shown under the
    # Exposure/Hazard switch above, and that switch already says "Exposure",
    # so a repeated header directly beneath it would be redundant.
    #
    # Age-band breakdown (infant/school-age/adolescent) matches the real
    # app's own property list (layouts/panels.py:399-435), flat radio
    # options in the same group as "Children (total)", just visually
    # indented/greyed to read as a breakdown of it, not structurally nested.
    #
    # No RadioGroup wrapper here anymore, these radios, Infrastructure, and
    # Context Data's radios all now share ONE exposure-property RadioGroup
    # built by the caller (_controls_zoom), so Context Data can sit after
    # Infrastructure while its properties still drive the same selection.
    def _sub_label(text):
        # #57707e (this file's own "secondary but readable" tone, e.g. every
        # slider readout below it), NOT #8ea0ab (this file's "dimmed/
        # disabled" tone, used for chevrons/disabled labels elsewhere).
        # These Age sub-items are genuinely selectable radios, not disabled
        # controls; the lighter #8ea0ab would make them read as unavailable
        # even though clicking one works fine.
        return html.Span(text, style={"paddingLeft": "12px", "color": "#57707e", "fontSize": "11px"})

    radios = [
        # Not a demographic/exposure tile at all, the raw hazard
        # probability raster itself (real app: the "probability" property in
        # _AOTS_PROP_MAP, same raster the tile sidecar already serves at
        # /tiles/raster/.../probability/...), included here so you can view
        # "how likely is impact here" on its own, independent of who/what is
        # exposed. Kept in the same flat radio group (same click-to-switch
        # behavior as every other property here), just visually separated by
        # its own divider since it isn't part of the population/children/
        # built-up family below it.
        # When 2+ hazards are checked, this renders one real combined-
        # probability raster (real per-tile bitmask union, no independence
        # formula, see _combine_bitmask_aware's own module comment)
        # instead of stacking
        # separate per-hazard rasters, see services/tile_server.py's
        # _fetch_combined_raster_tile. Which RENDER MODE (Raw/Probability/
        # Classification) actually applies is controlled by the Hazard
        # tab's own ms-hazard-render-mode switch (independent of this radio,
        # see that switch's own comment for why), not by this radio
        # itself; this radio only decides Population-vs-hazard-derived
        # property when render mode is "probability" (the default/neutral
        # mode where today's Exposure-driven behavior is unchanged).
        dmc.Radio(label=html.Span([
            _t("Hazard Probability"),
            dmc.Tooltip(
                label=_t(
                    "Real per-member union, not a sum: checks each of the 51 "
                    "real ensemble members and counts it once if ANY active "
                    "hazard reaches this tile, then divides by 51. Example: "
                    "River alone hits 10 members (~20%), Rain alone hits 10 "
                    "members (~20%), 5 of them the same members. Combined = "
                    "15 unique members ÷ 51 ≈ 29% — not 20%+20%=40%, and not "
                    "just the larger single number."
                ),
                multiline=True, w=280, withArrow=True, position="right",
                transitionProps={"duration": 0},
                styles={"tooltip": {"fontSize": "11px", "lineHeight": 1.4}},
                children=html.Span(
                    DashIconify(icon="mdi:information-outline", width=12),
                    style={"color": "#9aa7b0", "marginLeft": "4px", "cursor": "help",
                           "verticalAlign": "middle", "display": "inline-flex"},
                ),
            ),
        ], style={"display": "inline-flex", "alignItems": "center"}), value="probability", size="xs"),
        html.Div(style={"borderTop": "1px solid #eef2f5", "margin": "10px 0"}),
        dmc.Radio(label=_layer_label_with_info(_t("Population"), "population"), value="population", size="xs"),
        dmc.Radio(label=_layer_label_with_info(_t("Children (total)"), "children"), value="children", size="xs"),
        dmc.Radio(label=_layer_label_with_info(_sub_label(_t("Age 0–4 (Infant)")), "infant"), value="infant", size="xs"),
        dmc.Radio(label=_layer_label_with_info(_sub_label(_t("Age 5–14 (School-age)")), "school-age"),
                   value="school-age", size="xs"),
        dmc.Radio(label=_layer_label_with_info(_sub_label(_t("Age 15–19 (Adolescent)")), "adolescent"),
                   value="adolescent", size="xs"),
        dmc.Radio(id="exp-radio-built", label=_layer_label_with_info(_t("Built-up Area"), "built"), value="built", size="xs"),
    ]
    return html.Div([
        # Reactive (see _update_exposure_render_mode_note), whenever the
        # Hazard tab's ms-hazard-render-mode switch is "raw" or
        # "classification", NONE of the radios below have any visible
        # effect at all (every per-hazard MapLibre raster this whole
        # RadioGroup drives is either hidden ("raw") or replaced by the
        # combined classification raster ("classification") regardless
        # of which property is selected here; see applyHazardLayer's own
        # hazard_render_mode gate in maplibre_tiles.js). Surfaced here
        # rather than left to be silently discovered, matching this file's
        # own established "explain why a control is inert" convention
        # (e.g. _availability_note).
        html.Div(id="exposure-render-mode-note"),
        dmc.Stack(radios, gap=10, mb=16),
        # No "Baseline" option, with no hazard toggled below, the property
        # already shows plain/raw data (same rule as Infrastructure's
        # location-points-vs-impact-styling note); a separate "Baseline"
        # choice here would just duplicate "no hazard active."
        dmc.Text(_t("View As"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
        dmc.SegmentedControl(
            id="exposure-view-as", value="expected", fullWidth=True, size="xs",
            data=[{"value": "expected", "label": _t("At Risk")},
                  {"value": "inneed", "label": _t("In Need")}],
        ),
        dmc.Text(id="exposure-view-note", size="11px", c="dimmed", mt=8),
    ], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _infrastructure_section(countries=None):
    # Checkbox "checked" state drives layer visibility directly (see
    # _register_ms_facility_layer), the note below is about coloring/styling
    # once a hazard is toggled on, not about whether the layer shows at all.
    facilities = [("Schools", "schools"), ("Health Centers", "health"),
                  ("Shelters", "shelters"), ("WASH Facilities", "wash")]
    return _collapsible_section("infra", _t("Infrastructure"), [
        dmc.Stack([dmc.Checkbox(id=f"ms-facility-{lid}-on",
                                  label=_layer_label_with_info(_t(label), lid, countries=countries),
                                  size="xs", checked=False)
                   for label, lid in facilities], gap=10),
        dmc.Text(_t("Shown as plain locations with no hazard active; colored by impact probability once a hazard is toggled on below."),
                  size="11px", c="dimmed", mt=10, style={"lineHeight": 1.5}),
    ], expanded=True)


def _context_data_section(countries=None):
    # Settlement/RWI/Poverty, real properties too (layouts/panels.py:426-433),
    # tucked into their own collapsed sub-section since they're for review,
    # not everyday use, and placed after Infrastructure (last in the Exposure
    # pane). Radios still belong to the shared exposure-property RadioGroup
    # built by the caller, selecting one still drives the map property,
    # despite sitting after unrelated Infrastructure checkboxes in the DOM.
    return html.Div([
        html.Div([
            dmc.Text(_t("Context Data"), fw=700, size="10px", c="dimmed", tt="uppercase", style={"flex": 1}),
            html.Span("▾", id="chev-context", style={"fontSize": "10px", "color": "#8ea0ab"}),
        ], id="head-context", style={"display": "flex", "alignItems": "center", "gap": "8px", "cursor": "pointer"}),
        html.Div([
            dmc.Stack([
                dmc.Radio(id="exp-radio-settlement", label=_layer_label_with_info(_t("Settlement Classification"), "settlement"),
                           value="settlement", size="xs"),
                dmc.Radio(id="exp-radio-rwi", label=_layer_label_with_info(_t("Relative Wealth Index"), "rwi"), value="rwi", size="xs"),
                dmc.Radio(id="exp-radio-moderate-poverty", label=_layer_label_with_info(_t("Moderate Child Poverty Rate"), "moderate-poverty"),
                           value="moderate-poverty", size="xs"),
                dmc.Radio(id="exp-radio-severe-poverty", label=_layer_label_with_info(_t("Severe Child Poverty Rate"), "severe-poverty"),
                           value="severe-poverty", size="xs"),
            ], gap=12, mb=18),
            dmc.Text(_t("Data Availability"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=10),
            _data_availability_table(countries),
        ], id="body-context", style={"marginTop": "18px", "display": "none"}),
    ], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _flood_hazards_family(expanded=True, countries=None, date=None, run=None):
    # River Flooding is single-axis (return period only). Rainfall is
    # genuinely 2-axis, 4 real accumulation windows (PRECIP_WINDOWS_H in
    # DATAPIPELINE's main_pipeline.py), each with their own 3 depth tiers
    # (PRECIP_TP_THRESHOLDS_MM), so it needs its own window SegmentedControl
    # above the severity slider, not just a slider like the generic
    # _hazard_family helper gives every other hazard, hence its own
    # dedicated builder here rather than the generic helper.
    #
    # Header is a pure collapse (chevron), not a switch, same reasoning as
    # Tropical Cyclone: collapsing shouldn't imply River Flooding/Rainfall
    # turn off, only that they're checked/unchecked individually below.
    #
    # ms-river-on/ms-rain-on below do double duty: unchanged, they still
    # drive _build_hazard_tile_config's country-scoped, probability-only
    # impact system exactly as before. They ALSO gate the two GLOBAL,
    # country/storm-independent RAW hazard layers (raw precip-rate raster +
    # raw river-discharge raster, see services/tile_server.py's "Global raw
    # precipitation-rate endpoints"/"Global raw river-discharge endpoints"
    # sections) via the separate _build_global_raw_config callback further
    # down, which reads these same two checkbox ids as an independent,
    # additional Input (Dash allows multiple callbacks on the same
    # component/prop as Input), deliberately not a separate standalone
    # raw-layer checkbox + its own floating panel, to avoid duplicate
    # controls.
    #
    # These raw layers must NOT ALWAYS render global/uncropped in Country
    # Analysis mode (no country gating at all), that would bleed in
    # visibly around the selected country's own country-scoped rendering,
    # reading as broken since it looks unrelated to what's actually
    # selected. OFF by default the instant a country is selected (see
    # _build_global_raw_config's own country-gating comment), with an
    # explicit opt-back-in: the Hazard
    # tab's own ms-hazard-render-mode switch (Raw/Probability/Classification,
    # see _hazard_render_mode_switch), NOT a control local to this
    # section. Selecting "Raw" there shows the SAME raw layer Global mode
    # shows by default, still genuinely global/uncropped even while a
    # country is selected (not clipped to that country, the underlying
    # data has no country dimension at all, see this raw layer's own
    # "GLOBAL raw" section in tile_server.py), a different concept
    # entirely from the "Hazard Probability" Exposure radio (that one IS
    # country-scoped, per-tile derived impact probability from the MAT
    # tables, not this raw precip-rate/river flood-extent measurement).
    # flood-view-as
    # (Mean/Probability, nested under Rainfall's own controls below) controls
    # ONLY the raw precip-rate layer's aggregation and has no effect on the
    # country-scoped system, which stays probability-only forever. River has
    # no Mean mode at all (see River Flooding's own controls below), unlike
    # rain, it has no second independent quantity to toggle to.
    # REACTIVE (same pattern as _hurricane_family's own has_storms check,
    # re-evaluated here every time _switch_mode_content re-renders this
    # panel): River Flooding/Rainfall are NOT storm-scoped, each resolves
    # its own independent forecast_date straight from
    # MERCATOR_TILE_RIVER_MAT/MERCATOR_TILE_PRECIP_MAT (see
    # _build_hazard_tile_config), so "no data" here means the primary
    # selected country has genuinely NO row in that MAT table at all, not
    # merely a stale/mismatched date. get_latest_river_forecast_time/
    # get_latest_rain_forecast_time already return None for exactly that
    # case (their own docstrings) and are ttl_cached, so this reuses the
    # same signal _build_hazard_tile_config computes rather than adding a
    # second real query path. No countries selected (Global), nothing to
    # check against yet, so leave both available (matches _hurricane_
    # family's own "nothing selected" fallback).
    codes = _resolve_tile_codes(countries) if countries else []
    primary_code = codes[0] if codes else None
    river_available = (get_latest_river_forecast_time(primary_code) is not None) if primary_code else True
    rain_available = (get_latest_rain_forecast_time(primary_code) is not None) if primary_code else True

    # Combined with the RAW global layer's own date-exact availability (same
    # checkbox drives both systems, see the comment above this function) --
    # OR'd together: unchecking/greying out if EITHER the country-scoped
    # impact system has no data for the selected country, OR the raw global
    # layer has no real cycle at the exact selected date/time (river) /
    # exact date+run (precip). date/run default to the topbar's own current
    # values when this section renders before either is known.
    _d = date or _DEFAULT_FORECAST_DATE
    _r = run if run is not None else _DEFAULT_FORECAST_RUN
    river_country_available = river_available
    rain_country_available = rain_available
    river_raw_available = get_river_extent_forecast_time_for_date(_d) is not None if _d else True
    precip_raw_available = get_precip_forecast_time_near(_d, _r) is not None if _d and _r is not None else True
    river_available = river_available and river_raw_available
    rain_available = rain_available and precip_raw_available

    # Whole-section graying, mirroring _hurricane_family's own has_storms
    # dimming exactly: when NEITHER River Flooding NOR Rainfall has anything
    # real to show (for either the country-scoped system or the raw global
    # layer), the section header itself should read as inactive too, not
    # just leave two individually-greyed rows under a normal-looking header.
    has_flood_hazards = river_available or rain_available
    header = html.Div([
        html.Span(style={"width": "8px", "height": "8px", "borderRadius": "50%",
                          "background": RIVER if has_flood_hazards else "#c3ccd2"}),
        dmc.Text(_t("Flood Hazards"), fw=700, size="sm",
                  style={"flex": 1, "color": "#16232c" if has_flood_hazards else "#a8b3bd"}),
        # The badge carries hover elaboration explaining why River
        # Flooding/Rainfall are labeled "Proxies", NOT because the
        # underlying rain/river forecasts themselves aren't real (they
        # are), but because neither hazard has a real simulated FLOOD
        # extent/depth forecast behind it: rainfall accumulation and river
        # return-period tiers are real forecasted indicators that
        # CONTRIBUTE to flood risk, not a direct forecast of the flood
        # itself (see this file's own "Global raw river FLOOD-EXTENT
        # raster"/"Global raw precipitation-rate" section comments for the
        # full rationale).
        dmc.Tooltip(
            label=_t("Real forecasted indicators (river return-period tiers, rainfall accumulation) that contribute to flood potential — not a direct forecast of flood extent or depth."),
            multiline=True, w=230, withArrow=True, position="top",
            transitionProps={"duration": 0},
            styles={"tooltip": {"fontSize": "11px", "lineHeight": 1.4}},
            children=dmc.Badge(_t("Proxies"), size="xs", variant="light",
                                 color=RIVER if has_flood_hazards else "gray", mr=4,
                                 style={"cursor": "help"}),
        ),
        html.Span("▾", id="chev-flood", style={"fontSize": "10px",
                   "color": "#8ea0ab" if has_flood_hazards else "#c3ccd2"}),
    ], id="head-flood", style={"display": "flex", "alignItems": "center", "gap": "8px", "cursor": "pointer"})

    def _availability_note(available, country_available, raw_available):
        if available:
            return None
        # Self-contained (does NOT defer to ms-raw-date-availability-note,
        # the separate reactive callback further down) -- that note's own
        # condition reads the CURRENT checked state of this exact checkbox,
        # which this function just forced to False when raw_available is
        # False, so deferring to it would silently show no explanation at
        # all once the checkbox auto-unchecks. Show the precise reason
        # directly here instead: prefer the raw-layer-specific text when
        # that's what actually failed (country-scoped data IS real, just the
        # global raw layer has no exact-match cycle for this date/time) --
        # the generic country-based text would be actively misleading there
        # (blaming "this country" for a date/time gap unrelated to country
        # selection at all).
        if country_available and not raw_available:
            return dmc.Text(_t("No real forecast for this exact date/time (raw layer)."),
                              size="10px", c="dimmed", fs="italic", mb=8)
        return dmc.Text(_t("No real forecast data available for this country."),
                          size="10px", c="dimmed", fs="italic", mb=8)

    body = html.Div([c for c in [
        dmc.Group([
            dmc.Checkbox(id="ms-river-on", label=_layer_label_with_info(_t("River Flooding"), "river"),
                          color=RIVER, size="xs",
                          checked=river_available, disabled=not river_available),
            _hazard_source_select("river"),
        ], justify="space-between", mb=10, wrap="nowrap"),
        _availability_note(river_available, river_country_available, river_raw_available),
        html.Div([
            # The RAW (global) river-extent layer must NOT hardcode a
            # single lead time (T+24h), that lead time genuinely has zero
            # real flood signal for every country this app has ever
            # onboarded (see _RIVER_EXTENT_STEP_HOURS' own comment in
            # tile_server.py for the live data proving this). Instead a
            # real selectable lead-time control mirrors ms-rain-window's
            # exact SegmentedControl pattern, RAW-layer-only. Default "72"
            # (3 days): the shortest lead time DATAPIPELINE's own
            # downstream impact pipeline ever ingests at all (its own real
            # FILE_PATHs never go below 72h), with real, non-empty
            # coverage for both onboarded countries at that lead time.
            #
            # Renders a CUMULATIVE flood footprint (real member-flood
            # union across every real day from 24h through the selected
            # value, see _RIVER_EXTENT_STEP_HOURS' own "ACCUMULATION
            # SEMANTICS" comment in tile_server.py), not a single-day
            # snapshot, matches how Rainfall's own window genuinely
            # accumulates mm from T+0. Deliberately does NOT touch
            # Probability mode's own MERCATOR_TILE_RIVER_MAT query, that
            # real per-country impact-number path has its own real STEP_H
            # column but MAX()-aggregates across the ENTIRE forecast
            # horizon unconditionally (no window filter, no window control
            # in Country Analysis mode at all), a real, known, deliberate
            # scoping decision, not an oversight.
            dmc.SegmentedControl(
                id="ms-river-window", value="72", fullWidth=True, size="xs", color=RIVER, mb=10,
                disabled=not river_available,
                data=[{"value": str(h), "label": _t("{d}d", d=h // 24)} for h in _RIVER_EXTENT_STEP_HOURS],
            ),
            # Default index 2 == "rp10", the real, non-placeholder return-
            # period tier (rp2/rp5 are IS_STANDIN=True placeholders).
            # label=None, see _slider_block's own comment above for why
            # (Mantine's default raw-index drag bubble is misleading here;
            # the readout div right below already carries the real label).
            dmc.Slider(id="ms-river-slider", min=0, max=5, step=1, value=2,
                       marks=[{"value": i} for i in range(6)], size="sm", color=RIVER, mb=4, label=None,
                       disabled=not river_available),
            html.Div(id="ms-river-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "20px"}),

        dmc.Group([
            dmc.Checkbox(id="ms-rain-on", label=_layer_label_with_info(_t("Rainfall"), "rain"),
                          color=RAIN, size="xs",
                          checked=rain_available, disabled=not rain_available),
            _hazard_source_select("rain"),
        ], justify="space-between", mb=10, wrap="nowrap"),
        _availability_note(rain_available, rain_country_available, precip_raw_available),
        html.Div([
            dmc.SegmentedControl(
                # Default "6" (6h): shortest window is the default first
                # look. ms-rain-slider's own default (value=2 ==
                # "Extreme rain", see _RAIN_TIERS) pairs with this so both
                # window AND severity tier default to "6h - Extreme"
                # (75mm, the real dangerous-rainfall threshold this
                # pairing targets) rather than the previous richest-
                # setting default. This component is shared between
                # Global and Country Analysis (same id,
                # same function, see this function's own header comment),
                # so one change here covers both modes.
                id="ms-rain-window", value="6", fullWidth=True, size="xs", color=RAIN, mb=10,
                disabled=not rain_available,
                data=[{"value": "6", "label": _t("6h")}, {"value": "24", "label": _t("24h")},
                      {"value": "72", "label": _t("72h")}, {"value": "120", "label": _t("5 days")}],
            ),
            # label=None, see _slider_block's own comment above.
            dmc.Slider(id="ms-rain-slider", min=0, max=2, step=1, value=2,
                       marks=[{"value": i} for i in range(3)], size="sm", color=RAIN, mb=4, label=None,
                       disabled=not rain_available),
            html.Div(id="ms-rain-readout", style={"fontSize": "11px", "color": "#57707e"}),
            # Nested INSIDE Rainfall's own controls, not a shared control,
            # so it's visually unambiguous that this toggle only affects
            # Rainfall, River's raw layer has no Mean mode at all (see
            # _fetch_river_extent_raster_tile's own docstring in
            # services/tile_server.py): a river "Mean" would always
            # describe the identical probability fraction under a
            # different colour, not a genuinely different quantity the way
            # rain's real mm-vs-exceedance-probability split is.
            dmc.Text(_t("View As"), size="10px", fw=700, c="dimmed", tt="uppercase", mt=10, mb=6),
            dmc.SegmentedControl(
                id="flood-view-as", value="probability", fullWidth=True, size="xs",
                disabled=not rain_available,
                data=[{"value": "mean", "label": _t("Mean")},
                      {"value": "probability", "label": _t("Probability")}],
            ),
        ], style={"marginLeft": "22px", "marginBottom": "20px"}),

        # Placeholder, no real pipeline/data behind this yet (see
        # tc_ecmwf_additional_layers work elsewhere), single-axis like River
        # Flooding (height above normal tide, not a window+depth pair like
        # Rainfall) since storm surge doesn't have a separate accumulation
        # window concept. Permanently disabled/unchecked (not conditional on
        # any per-country availability check like River/Rain above, there's
        # no real backend for this hazard anywhere, ever, so it's always
        # "coming soon" rather than sometimes-available), this same
        # ms-surge-on.disabled flag also grays the top command-bar pill via
        # the generic _reflect_checkbox_on_pill loop, no separate wiring
        # needed there. See surge_visible's own comment in
        # _build_hazard_tile_config for the matching backend-side flag.
        dmc.Group([
            dmc.Checkbox(id="ms-surge-on", label=_layer_label_with_info(_t("Storm Surge"), "surge"),
                          color=SURGE, size="xs",
                          checked=False, disabled=True),
            dmc.Badge(_t("Coming Soon"), size="xs", variant="light", color="gray"),
        ], gap=8, mb=10),
        html.Div([
            dmc.Slider(id="ms-surge-slider", min=0, max=3, step=1, value=1,
                       marks=[{"value": i} for i in range(4)], size="sm", color=SURGE, mb=4, label=None,
                       disabled=True),
            html.Div(id="ms-surge-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "16px"}),

        # DATE-based "not available" note for the raw layers specifically,
        # populated reactively by _update_raw_date_availability_note (further
        # down, keyed on ms-global-raw-config-store) rather than computed
        # here at layout-build time, same "empty placeholder + dedicated
        # callback" pattern as ms-river-readout/ms-rain-readout just above.
        # Deliberately a SEPARATE element from _availability_note (COUNTRY-
        # based, for the checkboxes' other role driving the impact system),
        # see that callback's own docstring for why the two are never
        # merged into one ambiguous message.
        html.Div(id="ms-raw-date-availability-note", style={"marginTop": "8px"}),
    ] if c is not None], id="ms-flood-body", style={"marginTop": "16px"} if expanded else {"marginTop": "16px", "display": "none"})

    return html.Div([header, body], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _controls_global(date=None, run=None):
    # Collapsed by default (Global has less room to spare than a single
    # zoomed-in country's panel) but still active underneath, collapsing
    # is purely a display state now, not a deactivation.
    #
    # These three
    # section builders are fully independent of each other (none consumes
    # another's return value, each hits its own real Snowflake round-trips:
    # _resolve_storms_for_date/get_storms_with_alert_emails_at,
    # get_track_ids_for_date + per-storm _ensemble_max_kt,
    # get_gust_track_ids_for_date/get_river_extent_forecast_time_for_date/
    # get_precip_forecast_time_near). Fetching concurrently via the shared
    # executor collapses this 7-9-round-trip chain to roughly the slowest
    # single section, rather than running each strictly one after another.
    _builders = [lambda: _active_storms_section(date=date, run=run),
                  lambda: _hurricane_family(expanded=False, date=date, run=run),
                  lambda: _flood_hazards_family(expanded=False, date=date, run=run)]
    children = list(get_query_executor().map(lambda f: f(), _builders))
    return html.Div([c for c in children if c is not None], className="controls-stack")


def _view_toggle():
    # Switches which content is shown below, Exposure (+ Infrastructure) or
    # Hazard (Tropical Cyclone + Flood Hazards), instead of stacking both at
    # once, which is what made the panel feel squished. Exposure is the
    # default: "who/what is here" before "which hazard".
    return html.Div(
        dmc.SegmentedControl(
            id="controls-view-toggle", value="exposure", fullWidth=True, size="xs",
            data=[{"value": "exposure", "label": _t("Exposure")}, {"value": "hazard", "label": _t("Hazard")}],
        ),
        style={"padding": "14px 18px 4px", "borderTop": "1px solid #eef2f5"},
    )


def _hazard_render_mode_switch():
    # Country-Analysis-only, top of the Hazard tab, unified control for how
    # every checked hazard actually renders on the map: Sustained Wind/Gust's
    # own Envelopes-vs-Probability-Raster switch (tc-view-as, hidden here and
    # driven programmatically by this one, see _sync_tc_view_as), the
    # River/Rain raw cross-border layer's own on/off state (driven by
    # _build_global_raw_config reading THIS switch directly), and
    # Classification are all unified into ONE top-level mode here rather
    # than being separate/scattered controls.
    #
    # Deliberately independent of the Exposure tab's own selection
    # (Population/Children/.../Hazard Probability): "Raw" and
    # "Classification" both take over hazard rendering regardless of what
    # Exposure currently shows; "Probability" (the default) is the neutral
    # state where today's Exposure-driven behavior is completely unaffected.
    #
    # - "Raw": Wind/Gust show their ensemble envelope polygons (Leaflet, not
    #   a MapLibre raster at all); River/Rain show the SAME global,
    #   uncropped raw precip-rate/river flood-extent raster Global mode
    #   shows by default (not clipped to the selected country). Every
    #   per-hazard MapLibre raster/admin layer (the Exposure-driven
    #   Population/Children/.../Hazard-Probability system) is hidden.
    # - "Probability" (default): Exposure's own
    #   radio drives what's shown; "Hazard Probability" + 2 active hazards
    #   real-combines via the real per-tile bitmask union (no independence
    #   formula, see _fetch_combined_raster_tile's own
    #   _combine_bitmask_aware call).
    # - "Classification": one real combined raster, colored by WHICH
    #   hazard(s) hit each tile (solid color for 1, HAZARD_BOTH_COLOR/
    #   HAZARD_TRIPLE_COLOR + hatch for 2/3+, matches the Hazard
    #   Contribution popup's own convention), regardless of Exposure's
    #   selection.
    return html.Div([
        dmc.Text(_t("Hazard Render Mode"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
        dmc.SegmentedControl(
            id="ms-hazard-render-mode", value="probability", fullWidth=True, size="xs",
            data=[{"value": "raw", "label": _t("Raw")},
                  {"value": "probability", "label": _t("Probability")},
                  {"value": "classification", "label": _t("Classification")}],
        ),
        dmc.Text(_t("How checked hazards render — independent of the Exposure tab's own selection."),
                  size="10px", c="dimmed", mt=4),
    ], style={"padding": "16px 18px", "borderBottom": "1px solid #eef2f5"})


def _controls_zoom(countries=None, date=None, run=None):
    # Order: Active Storm(s) first if there are any (nothing rendered at all
    # otherwise, the switch below just becomes the first thing shown), then
    # the Exposure/Hazard switch, then whichever pane it's set to.
    #
    # Same reasoning as
    # _controls_global above: _active_storms_section/
    # _hurricane_family/_flood_hazards_family are independent of each other
    # and of the exposure pane build below.
    # Fetching the three concurrently via the shared executor.
    _builders = [lambda: _active_storms_section(countries=countries, date=date, run=run),
                  lambda: _hurricane_family(countries=countries, date=date, run=run),
                  lambda: _flood_hazards_family(countries=countries, date=date, run=run)]
    active_storms, hurricane_family, flood_hazards_family = get_query_executor().map(lambda f: f(), _builders)
    children = [
        active_storms,
        _view_toggle(),
        html.Div(
            dmc.RadioGroup(
                html.Div([_exposure_section(), _infrastructure_section(countries), _context_data_section(countries)]),
                id="exposure-property", value="population",
            ),
            id="controls-exposure-pane",
        ),
        html.Div([_hazard_render_mode_switch(), hurricane_family, flood_hazards_family],
                  id="controls-hazard-pane", style={"display": "none"}),
    ]
    return html.Div([c for c in children if c is not None], className="controls-stack")


@callback(
    Output("tc-view-as", "value"),
    Input("ms-hazard-render-mode-store", "data"),
    Input("selected-country-store", "data"),
    prevent_initial_call=False,
)
def _sync_tc_view_as(hazard_render_mode, countries):
    """Drives the now-hidden-in-Country-Analysis tc-view-as component (see
    _hurricane_family's own comment) from ms-hazard-render-mode-store (the
    always-present mirror of the Country-Analysis-only switch, see that
    store's own comment for why this reads the mirror, not the switch
    directly), so this page's other 3 real consumers of Input("tc-view-as",
    "value") (_update_map_legend, the Hazard Contribution content callback,
    _load_ms_tracks_and_envelopes) all keep working completely unchanged.

    Global mode (`not countries`) always resolves to "envelopes", matching
    that mode's own unrelated, unchanged forced-envelope behavior, the
    switch itself has no meaning there, regardless of whatever value
    happens to be mirrored in the store from a previous Country Analysis
    visit.
    """
    if not countries:
        return "envelopes"
    hazard_render_mode = hazard_render_mode or "probability"
    return "envelopes" if hazard_render_mode == "raw" else "raster"


def _controls_panel():
    # Always present, no dismiss button. This is the primary navigation
    # surface (hazards, exposure, infrastructure), not an optional overlay
    # like a search result or a one-off detail panel. No headline: whatever
    # renders first (Active Storms, or the Exposure/Hazard switch when
    # there's no storm) already says what this panel is for.
    #
    # Must NOT call _controls_global() directly here, building the WHOLE
    # real panel (7-9 Snowflake round-trips: _active_storms_section/
    # _hurricane_family/_flood_hazards_family) inline inside layout()'s own
    # call graph, which Dash Pages runs synchronously as part of the routing
    # callback, would block delivery of the entire page. _switch_mode_content
    # (controls-body's own Output, an initial callback, prevent_initial_
    # call=False, and every one of its Inputs (topbar-mode/selected-country-
    # store/topbar-date/topbar-time) already has a real value in this same
    # layout() tree, via their own id= definitions below) fires
    # immediately after mount regardless, and builds the exact same content.
    # An inline build here would be pure redundant duplicate work. A
    # lightweight skeleton here lets the page ship first; the real panel
    # still appears within one callback round-trip, same as it always has
    # for every OTHER country/date/run change on this page.
    return html.Div(
        html.Div(_controls_skeleton(), id="controls-body"),
        # top/maxHeight from the shared _PANEL_TOP/_PANEL_MAX_HEIGHT spacing
        # system (see their own comment), same values impact-panel and
        # command-bar use, so every gap in this shell (topbar-to-panel,
        # panel-to-edge, panel-to-bottom-row, bottom-row-to-footer) is the
        # same 16px margin.
        id="controls-panel", style={**_PANEL_STYLE, "top": _PANEL_TOP, "left": f"{_UI_MARGIN}px", "width": "290px",
                                     "maxHeight": _PANEL_MAX_HEIGHT, "overflowY": "auto"},
    )


def _controls_skeleton():
    # Pure-Python, zero Snowflake round-trips, see _controls_panel's own
    # comment for why this exists. Bare dmc.Loader (same component
    # _ms_loading_badge already uses elsewhere on this page) rather than a
    # placeholder shaped like the real panel, this is only ever on-screen
    # for one callback round-trip, not worth the upkeep of keeping a fake
    # skeleton layout in sync with the real one.
    return html.Div(
        dmc.Loader(size="sm", color="#8ea0ab", type="dots"),
        style={"display": "flex", "justifyContent": "center", "padding": "24px"},
    )


def _stat_grid(stats, label=None, extra_stats=None, extra_label=None, scope="global",
                compare_stats=None, compare_member=None, hz=None, flood_combine_active=None):
    """One country's (or the global default's) stat block. `label` renders a
    small heading above the grid, used to tell countries apart when several
    are shown side by side; omitted for the single/global case. `extra_stats`
    adds a second, labeled group below (In Need numbers, Country Analysis
    only). `scope` disambiguates click-to-breakdown ids when several of
    these grids render at once (side-by-side countries, or this panel vs the
    modal), must be unique per rendered grid. `compare_stats` (Country
    Analysis only) adds a second, colored number in each tile, the
    Influencing-Factor-determined member's value, right next to the
    Probabilistic one, tagged with `compare_member` (e.g. "#6") so it's
    clear which specific scenario the comparison number belongs to.

    `hz` (optional): when more than one hazard
    FAMILY is active, the tag also lists which families contributed to this
    combined comparison number, e.g. "#6 (Wind+Flood)", a plain "#6" alone
    would otherwise read as wind-only.

    `flood_combine_active` (optional): the caller's own already-resolved
    _flood_combine_active(date, run, hz) result, pass this whenever
    `date`/`run` are available (see _country_block's own real call site)
    so the "Flood" tag reflects whether flood data ACTUALLY contributed,
    not just whether it was toggled on. This must NOT derive "Flood"
    purely from `hz`'s own UI toggle state, which can (and, since real
    flood data only exists for a handful of real dates, often does)
    disagree with what _fetch_family_member_frames actually combined,
    that would let the badge claim "Wind+Flood" while the displayed
    numbers are genuinely wind-only, with no visible way to tell.
    `flood_combine_active
    =None` (every existing caller that hasn't been updated, or a caller
    with no real date/run to check against) falls back to the OLD
    toggle-only check, a real, honest degradation (may still claim a
    hazard that didn't contribute) rather than a crash, not a silent
    behavior change for those callers."""
    member_tag = _member_short_label(compare_member)
    if hz and member_tag:
        families = []
        # Gust is deliberately excluded from the compare-badge combination
        # (see _fetch_family_member_frames's own comment), checking only
        # wind_on here matches what _fetch_family_member_frames actually
        # fetches/uses, so this tag never claims a hazard contributed when
        # it didn't.
        if hz.get("wind_on"):
            families.append(_t("Wind"))
        # Uses flood_combine_active
        # when the caller provided it (real data-aware answer), falling
        # back to the looser _flood_hazard_active(hz) toggle-only check
        # only when it wasn't (e.g. no date/run context available), see
        # this function's own docstring above for the concrete failure a
        # toggle-only check would cause.
        flood_contributed = flood_combine_active if flood_combine_active is not None else _flood_hazard_active(hz)
        if flood_contributed:
            families.append(_t("Flood"))
        if len(families) > 1:
            member_tag = f"{member_tag} ({'+'.join(families)})"

    def _card(k, v):
        # v/compare_stats[k] are None (not a fabricated 0) when this metric
        # genuinely has no real data (facility columns only, Schools/
        # Health Centers/Shelters/WASH, see _fetch_real_combined_tile_
        # totals_uncached's own _sum_or_none comment; People/Children at
        # Risk are always real). Rendered as plain "N/A" text, no dedicated
        # N/A styling needed here the way _value_td has, since these tiles
        # don't already have a real-zero-vs-N/A visual distinction to
        # preserve (unlike the breakdown table, this compact panel doesn't
        # grey out real zeros either).
        content = [
            dmc.Text(v if v is not None else _t("N/A"), size="lg", fw=700, ff="monospace", mb=2),
            dmc.Group([
                DashIconify(icon=_STAT_ICONS.get(k, "mdi:help-circle-outline"), width=13, color="#8ea0ab"),
                dmc.Text(_t(k), size="10px", c="dimmed", fw=700, tt="uppercase"),
            ], gap=4, wrap="nowrap"),
        ]
        if compare_stats and k in compare_stats and compare_stats[k] is not None:
            content.insert(1, html.Div([
                html.Span(compare_stats[k], style={"fontWeight": 700, "fontFamily": "monospace"}),
                html.Span(f" {member_tag}", style={"fontSize": "9px", "fontWeight": 700, "marginLeft": "3px"}),
            ], style={"fontSize": "14px", "color": "#d94f3c", "marginBottom": "2px"}))
        return html.Div(
            dmc.Paper(content, p="sm", withBorder=True, radius="md"),
            id={"type": "stat-card", "metric": k, "scope": scope}, n_clicks=0,
            style={"cursor": "pointer"},
        )

    grid = dmc.SimpleGrid([_card(k, v) for k, v in stats.items()], cols=2, spacing="sm")
    children = [grid]
    if extra_stats:
        children.append(dmc.Text(_t(extra_label) if extra_label else _t("In Need"), size="10px", fw=700, c="dimmed",
                                   tt="uppercase", mt=12, mb=6))
        children.append(dmc.SimpleGrid([_card(k, v) for k, v in extra_stats.items()], cols=2, spacing="sm"))
    body = html.Div(children) if len(children) > 1 else grid
    if not label:
        return body
    # A plain 11px label with only 6px below it
    # reads as too small/quiet to tell whose numbers are whose once several
    # countries' blocks are stacked in the "split" view, same
    # size/weight/color as a real section header (matching e.g. the
    # panel's own "Impact Summary" title), with a small colored accent bar
    # so it reads as a distinct block start at a glance, not just another
    # line of text.
    return html.Div([
        html.Div([
            html.Span(style={"width": "4px", "height": "15px", "background": "#1cabe2",
                               "borderRadius": "2px", "display": "inline-block", "flexShrink": 0}),
            dmc.Text(_t(label), size="sm", fw=700, c="#16232c"),
        ], style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "10px"}),
        body,
    ], style={"marginBottom": "14px"})


def _fetch_real_combined_admin_totals(country, date=None, run=None, hz=None):
    """Real per-admin-1-region impact numbers for `country` (a display name,
    possibly a bundled region like "ECA"/"Pacific Islands"), replaces the
    old _ADMIN1_REGIONS illustrative fixed-share dict (mock region names,
    mock percentage splits of the country total) AND this function's own
    earlier "one real table per active hazard" replacement (which reused
    ADMIN_ALL_IMPACT_MAT/ADMIN_ALL_RIVER_MAT/ADMIN_ALL_PRECIP_MAT directly,
    one independent fetch per hazard, no cross-hazard combination) with a
    SINGLE real per-region COMBINED total, using the exact same per-tile
    bitmask union methodology as the country-level table above
    (services/tile_server.py's own _combined_bitmask_fracs, shared by
    combined_country_totals AND this endpoint's own
    _combined_admin_totals_cached, just grouped by ADMIN_ID at the final
    summation step instead of summed to one whole-country number). The
    Admin Level 1 Breakdown follows the SAME structure as the main
    country-level table: one real black total
    per cell, with the real TC-only/Both/Flood-only split rendered below
    it (_admin1_table's own _cells, mirroring _simple_breakdown_table's
    _value_td/_hazard_split_line), not one separate table per hazard with
    no real cross-hazard combination at all, which is what the earlier
    "was wind-only, ADMIN_ALL_RIVER_MAT/ADMIN_ALL_PRECIP_MAT already real"
    fix above still left standing.

    Each returned region dict carries `family_split` (real TC-vs-Flood,
    the SAME shape _get_country_family_split's own return already uses)
    and `flood_split` (real River-vs-Rain, only when both are
    simultaneously active), fed straight into _breakdown_from_split/
    _hazard_split_line by _admin1_table, no separate estimate computed
    here.

    People/Children In Need (PIN/CHIN) stays wind-only, same real
    limitation as the country-level table (_fetch_real_combined_tile_
    totals_uncached's own docstring: no vulnerability/CCI pipeline exists
    for gust/river/rain), merged in here from get_admin_impacts'
    E_PEOPLE_IN_NEED/E_CHILDREN_IN_NEED columns by ADMIN_ID, None when
    wind isn't active or this specific region's own in-need column is
    genuinely missing (same None-vs-0 convention as everywhere else in
    this file).

    Returns a flat list of region dicts, alphabetical by name, empty when
    no hazard is active or none resolve to real data. For a bundled
    region (IS_REGION, e.g. "ECA"), _resolve_tile_codes expands it into
    its real member ISO3 codes and each is fetched concurrently, its own
    real admin-1 regions concatenated into the same flat list (no
    ADMIN_ID collisions across countries, each is a real per-country
    admin-region code, e.g. 'BGD_0003_V1').
    """
    codes = _resolve_tile_codes([country])
    if not codes:
        return []
    hz = hz or {}

    def _fetch_one(code):
        member_name = _CODE_TO_NAME.get(code, country)
        # Same resolution as _fetch_real_combined_tile_totals_uncached's
        # own wind/river/rain threshold+forecast_time blocks (see that
        # function's own comments for the full "why" behind each),
        # duplicated here rather than threaded through as already-resolved
        # params, since this is a per-CODE resolution (a bundled region's
        # members can each independently have/lack a real active storm)
        # while that function resolves once for a single real country.
        # Gust is deliberately never resolved here either, same reason as
        # that function's own docstring (_UNION_EXCLUDED_HAZARDS in
        # services/tile_server.py).
        storm_info = _resolve_storm_for_country(member_name, date, run) if hz.get("wind_on") else None

        def _resolve_threshold(name, target_kt):
            try:
                thresholds = get_available_wind_thresholds(storm_info["name"], str(storm_info["forecast_time"]))
                numeric = sorted(int(t) for t in thresholds if t.isdigit())
            except Exception as e:
                logger.warning("Could not load %s thresholds for %s: %s", name, code, e)
                return None
            if not numeric:
                return None
            target_kt = target_kt if target_kt is not None else 50
            return target_kt if target_kt in numeric else None

        wind_threshold = _resolve_threshold("wind", hz.get("wind_kt")) if (hz.get("wind_on") and storm_info) else None
        river_resolved = (
            get_river_extent_forecast_time_for_date(date, hz.get("rp_tier"))
            if (hz.get("river_on") and hz.get("rp_tier") and date) else None
        )
        river_forecast_time = _mat_forecast_date(date, "00") if river_resolved else None
        rain_resolved = (
            get_precip_forecast_time_near(date, run)
            if (hz.get("rain_on") and hz.get("rain_mm") is not None and hz.get("rain_window") is not None
                and date and run is not None) else None
        )
        rain_forecast_time = _mat_forecast_date(date, run) if rain_resolved else None

        if wind_threshold is None and not river_forecast_time and not rain_forecast_time:
            return []

        params = {}
        if wind_threshold is not None and storm_info:
            params["wind_on"] = True
            params["wind_forecast_date"] = storm_info["mat_forecast_date"]
            params["wind_threshold"] = wind_threshold
        # No gust_on/gust_threshold key is ever added: see _fetch_one's own
        # comment above.
        if river_forecast_time:
            params["river_on"] = True
            params["river_forecast_date"] = river_forecast_time
            params["rp_tier"] = hz["rp_tier"]
            params["river_window"] = hz.get("river_window")
        if rain_forecast_time:
            params["rain_on"] = True
            params["rain_forecast_date"] = rain_forecast_time
            params["threshold_mm"] = hz.get("rain_mm")
            params["window_h"] = hz.get("rain_window")

        try:
            resp = requests.get(
                f"{config.TILE_SERVER_URL}/impact/combined-admin-totals/{code}/{storm_info['name'] if storm_info else 'NONE'}",
                params=params, timeout=_MEMBER_IMPACT_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            regions = resp.json() or []
        except Exception as e:
            logger.warning("Could not load combined admin totals for %s: %s", code, e)
            return []

        # Real wind-only in-need merge, by ADMIN_ID (get_admin_impacts'
        # own TILE_ID column, see that function's docstring: it really IS
        # the admin region code, same convention ADMIN_ALL_RIVER_MAT/
        # ADMIN_ALL_PRECIP_MAT/BASE_ADMIN_MAT all share).
        in_need_by_admin = {}
        if wind_threshold is not None and storm_info:
            try:
                df = get_admin_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], wind_threshold, admin_level=1)
                if df is not None and not df.empty:
                    for _, r in df.iterrows():
                        in_need_by_admin[r.get("TILE_ID")] = {
                            "people_in_need": (float(r["E_PEOPLE_IN_NEED"]) if pd.notna(r.get("E_PEOPLE_IN_NEED")) else None),
                            "children_in_need": (float(r["E_CHILDREN_IN_NEED"]) if pd.notna(r.get("E_CHILDREN_IN_NEED")) else None),
                        }
            except Exception as e:
                logger.warning("Could not load admin-1 in-need for %s: %s", code, e)

        return [{**region, **in_need_by_admin.get(region["admin_id"], {"people_in_need": None, "children_in_need": None})}
                 for region in regions]

    # A bundled region (e.g. "ECA", "Pacific Islands") fetches each real
    # ISO3 member concurrently. Must NOT submit through the shared, bounded
    # 24-worker get_query_executor() pool here: this function is itself
    # already called from INSIDE that same pool's own worker threads
    # (_admin1_section is fanned out via get_query_executor().map in
    # _impact_breakdown_content), so a nested blocking .map() call onto the
    # IDENTICAL bounded pool from within one of its own workers is a real
    # deadlock risk under realistic concurrency (a genuine multi-country
    # event with several Dash callbacks in flight can reach the 24-outer-
    # task ceiling; the pool has no way to grow to serve its own nested
    # work). get_member_fetch_executor() (see _parallel_member_fetch's own
    # identical reasoning, its only other caller) is a SECOND, dedicated,
    # isolated pool that exists exactly for this nested-fan-out case.
    rows = [row for code_rows in get_member_fetch_executor().map(_fetch_one, codes) for row in code_rows]
    # Alphabetical, not Snowflake's own return order, same reasoning as
    # this function's own earlier version (a bundled region's members
    # concatenated above would otherwise group by country before name).
    rows.sort(key=lambda row: row["name"])
    return rows


def _admin1_table(country, regions_real, breakdown):
    # Same tinted-header/rounded-card/zebra-stripe treatment as
    # _simple_breakdown_table, and the SAME cell structure too: a real
    # black total, with the real TC-only/Both/
    # Flood-only split rendered below it via _hazard_split_line, exactly
    # mirroring that table's own _value_td (see this function's own
    # docstring reasoning duplicated in _fetch_real_combined_admin_totals'
    # docstring for why this replaced the earlier "one table per hazard, no
    # cross-hazard combination" version). People/Children get their own
    # side-by-side At Risk/In Need sub-columns (colSpan=2), same pattern as
    # the main table's header; facility metrics (no In Need concept) keep a
    # single column.
    # `regions_real` (from _fetch_real_combined_admin_totals) carries real
    # ABSOLUTE numbers (and a real per-region family_split) per real admin-1
    # region already, no illustrative share/scale step needed here: this
    # must never fall back to fake fixed percentages of the country total,
    # under fake region names, that don't move with the real storm/
    # threshold selected.
    # `breakdown` (from _hazard_breakdown, threaded through from
    # _impact_breakdown_content via _admin1_section) is the SAME flat dict
    # the main table's own _value_td uses: its tc_active/flood_active gate
    # decides whether _hazard_split_line renders anything at all, and it's
    # also the fallback `**fallback_breakdown` spread _breakdown_from_split
    # applies on top of a region's own real percentages (both_pct/
    # tc_only_pct/etc. themselves always come from the real split, this
    # only contributes the unrelated flags/scale fields untouched by that
    # spread).
    metric_keys = list(_DEFAULT_STATS.keys())
    _PIN_FIELD = {"People at Risk": "people_in_need", "Children at Risk": "children_in_need"}
    # Keys match _fetch_real_combined_admin_totals' own region dict fields
    # (the same raw column names services/tile_server.py's own
    # _combined_admin_totals_cached returns), "Children at Risk" is handled
    # separately via _children_total below (summed from 3 real age-band
    # columns, no single "children" field exists on a region dict).
    _METRIC_FIELD = {"People at Risk": "population",
                       "Schools at Risk": "num_schools", "Health Centers at Risk": "num_hcs",
                       "Shelters at Risk": "num_shelters", "WASH Facilities at Risk": "num_wash"}
    # region["children"] doesn't exist on the new combined-endpoint region
    # dicts (they carry the 3 raw age-band columns separately, same shape
    # as the country-level endpoint's own totals), summed here once per
    # region the same "sum only real components" way _shape_admin1_rows'
    # own predecessor and _fetch_real_combined_tile_totals_uncached's own
    # age_population both already do, not folding a genuinely missing band
    # in as a fabricated 0.
    def _children_total(region):
        vals = [region.get(k) for k in ("infant_population", "school_age_population", "adolescent_population")]
        real_vals = [v for v in vals if v is not None]
        return sum(real_vals) if real_vals else None

    def _region_metric_breakdown(region, key):
        # Same real 3-way resolution as _simple_breakdown_table's own
        # _value_td (see that function's own comment for the full "why"),
        # gate-FIRST, same order as _value_td's own branch (which only ever
        # reaches a real metric_split because _compute_breakdown_by_metric
        # itself refuses to compute one unless both families are active,
        # see that function's own guard): (1) only one family active at all
        # -> the flat `breakdown` itself, immediately, never even attempting
        # a real split (inert: _hazard_split_line already renders nothing
        # for a single active family, see its own comment); (2) both
        # active, this region's own real per-metric split resolved -> use
        # it; (3) both active, but this metric's own split is genuinely
        # absent for this region (e.g. a real all-None facility column) ->
        # None, _hazard_split_line renders an honest "no real split" state,
        # never a fabricated illustrative one.
        if not (breakdown["tc_active"] and breakdown["flood_active"]):
            return breakdown
        family_split = region.get("family_split")
        split = _sum_family_split_metric(family_split, key) if family_split else None
        return _breakdown_from_split(split.get("tc_only"), split.get("flood_only"), split.get("both"), breakdown) if split else None
    # People/Children need two 75px sub-columns (150 total) for their At
    # Risk/In Need pair; Schools/Health Centers/Shelters/WASH are single
    # plain numbers and need far less, equal table-layout:fixed
    # distribution was squeezing all six into the same width, which
    # overlapped headers and clipped the combined-format cells.
    # Facility columns widened slightly vs. the earlier per-hazard version
    # (was 70/95/70/80), the 3-number TC-only/Both/Flood-only split line
    # below each total needs a bit more room than a single plain number did.
    _COL_WIDTHS = {"People at Risk": "150px", "Children at Risk": "150px", "Schools at Risk": "80px",
                    "Health Centers at Risk": "105px", "Shelters at Risk": "80px", "WASH Facilities at Risk": "90px"}
    # No whiteSpace:nowrap, table-layout:fixed below means a too-narrow
    # column can't grow the table to fit "HEALTH CENTERS" on one line; it
    # needs to be free to wrap instead.
    th_style = {"textAlign": "right", "padding": "8px 10px", "borderBottom": "1px solid #dde6ec",
                "fontSize": "9.5px", "color": "#57707e",
                "background": "#f6f9fb", "textTransform": "uppercase", "letterSpacing": "0.3px"}
    sub_th_style = {**th_style, "textAlign": "center", "fontWeight": 500, "fontSize": "9px",
                     "padding": "5px 8px", "textTransform": "none", "letterSpacing": "normal"}
    td_base = {"padding": "7px 10px", "fontSize": "11px", "textAlign": "right"}

    def _row_style(i):
        return {"background": "#fafcfd" if i % 2 else "#ffffff", "borderBottom": "1px solid #f1f4f6"}

    # A real confirmed zero should read as
    # visually lighter/less alarming than a real nonzero count, plain
    # black "0" next to bold black nonzero numbers made every zero region
    # look as prominent/urgent as an actually-affected one.
    def _num_style(val, base_style):
        return {**base_style, "color": "#adb5bd"} if val == 0 else base_style

    def _cells(key, region, td_style):
        # None (not 0) when this region's own facility/age-band column is
        # genuinely missing (e.g. Turks and Caicos Islands' real all-NULL
        # shelters), rendered as a distinct italic "N/A", same convention
        # as the in-need column pair.
        base_raw = _children_total(region) if key == "Children at Risk" else region.get(_METRIC_FIELD[key], 0)
        base_val = math.ceil(base_raw) if base_raw is not None else None
        # Real per-metric TC-only/Both/Flood-only split for this region
        # (see _region_metric_breakdown's own docstring), fed into
        # _hazard_split_line exactly like _simple_breakdown_table's own
        # _value_td does for the main country-level table, so this admin
        # table follows the SAME structure.
        cell_breakdown = _region_metric_breakdown(region, key) if base_val is not None else None
        pin_field = _PIN_FIELD.get(key)
        if pin_field is None:
            if base_val is None:
                return [html.Td(html.Div(_t("N/A"), style={"color": "#adb5bd", "fontStyle": "italic", "fontSize": "10px"}),
                                  style={**td_style, "textAlign": "center"})]
            return [html.Td([
                html.Div(_format_stat_number(base_val), style=_num_style(base_val, {"fontFamily": "monospace", "fontWeight": 700})),
                _hazard_split_line(base_val, cell_breakdown, font_size="8.5px"),
            ], style={**td_style, "textAlign": "center"})]
        # None (not 0) when this region's own in-need column is genuinely
        # missing (wind-only, see _fetch_real_combined_admin_totals' own
        # docstring), rendered as a distinct italic "N/A", same convention
        # as _simple_breakdown_table's own _value_td. No per-hazard split
        # under In Need either, same reasoning as that function's own
        # comment: In Need has no real per-hazard source to split in the
        # first place (wind-only vulnerability data, regardless of which
        # other hazards are active).
        in_need_raw = region.get(pin_field)
        in_need = math.ceil(in_need_raw) if in_need_raw is not None else None
        at_risk_td = (
            html.Td(html.Div(_t("N/A"), style={"color": "#adb5bd", "fontStyle": "italic", "fontSize": "10px"}),
                     style={**td_style, "textAlign": "center"})
            if base_val is None else
            html.Td([
                html.Div(_format_stat_number(base_val), style=_num_style(base_val, {"fontFamily": "monospace", "fontWeight": 700})),
                _hazard_split_line(base_val, cell_breakdown, font_size="8.5px"),
            ], style={**td_style, "textAlign": "center"})
        )
        in_need_td = (
            html.Td(html.Div(_t("N/A"), style={"color": "#adb5bd", "fontStyle": "italic", "fontSize": "10px"}),
                     style={**td_style, "textAlign": "center"})
            if in_need is None else
            html.Td(html.Div(_format_stat_number(in_need),
                               style=_num_style(in_need, {"fontFamily": "monospace", "fontWeight": 700})),
                     style={**td_style, "textAlign": "center"})
        )
        return [at_risk_td, in_need_td]

    # table-layout:fixed splits width equally across columns unless the
    # FIRST ROW gives one an explicit width, without this, "Region" got
    # squeezed to the same width as a single number column, which is far
    # too narrow for names like "Eastern Visayas".
    header_rows = [html.Tr(
        [html.Th(_t("Region"), style={**th_style, "textAlign": "left", "width": "140px"}, rowSpan=2)]
        + [html.Th(_t(k.replace(" at Risk", "")),
                    style={**th_style, "textAlign": "center", "width": _COL_WIDTHS[k]},
                    colSpan=2 if k in _PIN_FIELD else 1, rowSpan=1 if k in _PIN_FIELD else 2)
            for k in metric_keys]
    ), html.Tr(
        [html.Th(_t(lbl), style=sub_th_style) for k in metric_keys if k in _PIN_FIELD for lbl in ("At Risk", "In Need")]
    )]
    rows = []
    for i, region in enumerate(regions_real):
        rstyle = _row_style(i)
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(region["name"]), style={**td_style, "textAlign": "left", "color": "#16232c", "fontWeight": 600})]
        for k in metric_keys:
            cells.extend(_cells(k, region, td_style))
        rows.append(html.Tr(cells))

    # tableLayout:"fixed" is the actual fix for the table overflowing past
    # its container no matter how the wrapping divs were constrained,
    # table-layout:auto (the default) treats width:100% as a mere
    # suggestion and still grows the table past it whenever cell content
    # doesn't naturally fit; fixed layout makes the width a hard constraint,
    # wrapping cell content instead of growing the table.
    # minWidth (140 Region + 150 People + 150 Children + 70 Schools + 95
    # Health Centers + 70 Shelters + 80 WASH = 755), same reasoning as
    # _simple_breakdown_table: without it, table-layout:fixed would just
    # shrink all columns to fit a narrower container instead of ever
    # needing to scroll.
    table = html.Table([html.Thead(header_rows), html.Tbody(rows)],
                         style={"width": "100%", "minWidth": "755px", "tableLayout": "fixed", "borderCollapse": "collapse"})
    # maxWidth caps this at its own comfortable size (matching the 7 fixed
    # column widths above) regardless of how wide the MAIN table's modal
    # grows for many countries, without this, table width:100% stretched
    # to fill the whole (possibly 2000px) modal, leaving a wall of empty
    # space around a table that only ever needs ~900px.
    # width:"fit-content", this div's own default width:auto (=100% of its
    # parent) combined with overflow:"hidden" (for the rounded corners) was
    # clipping the table at THIS boundary before the ancestor scrollable
    # div ever got a chance to provide a scrollbar. fit-content makes this
    # div match its child table's actual (min-width-driven) size instead,
    # still capped by maxWidth below.
    return html.Div(table, style={"border": "1px solid #eef2f5", "borderRadius": "10px", "overflow": "hidden",
                                    "width": "fit-content", "maxWidth": "950px"})


def _admin1_body_content(country, date, run, hz, breakdown):
    """The real, potentially-slow part of an Admin Level 1 section: fetches
    _fetch_real_combined_admin_totals (an HTTP round trip to services/
    tile_server.py plus a get_admin_impacts Snowflake in-need merge) and
    renders either the real unified table (_admin1_table) or an honest
    "no real data" message. Factored out of _admin1_section so the
    INTERACTIVE modal (see that function's own docstring) can defer calling
    this until the user actually clicks to expand a section, instead of
    running it unconditionally for every selected country on every modal
    render/slider tweak regardless of whether that section is ever opened."""
    regions_real = _fetch_real_combined_admin_totals(country, date, run, hz)
    # No real per-admin-1 data for this country/storm/threshold at all
    # (e.g. no hazard is active, no storm resolves for the selected
    # date/run, or this country genuinely has no rows yet), say so plainly
    # rather than rendering an empty table or falling back to a fake one.
    if not regions_real:
        return [dmc.Text(_t("No real admin-level breakdown available for this storm/threshold selection."),
                           size="11px", c="dimmed", fs="italic")]
    # ONE real, combined table (total in black with the TC-only/Both/
    # Flood-only breakdown rendered below it), replaces the earlier "one
    # table per active
    # hazard, no cross-hazard combination" version (see
    # _fetch_real_combined_admin_totals' own docstring for the full "why"
    # this changed). No per-hazard label needed anymore, every active
    # hazard's real contribution is already combined into this one table's
    # own totals + split, same as the main table above.
    body_content = [_admin1_table(country, regions_real, breakdown)]
    # Same shared In Need clarification as the main table/Impact Summary
    # panel; still wind-only regardless of which other hazards are active
    # (see _fetch_real_combined_admin_totals' own docstring).
    body_content.append(_in_need_note(mt=10))
    return body_content


def _admin1_section(country, expanded=False, date=None, run=None, wind_kt=None, hz=None, breakdown=None):
    # Collapsed by default in the interactive modal, the country-level
    # table above already covers the everyday view, this is finer detail on
    # demand, not something to always show (avoids repeating the same row
    # structure 3-4x per country). expanded=True (used by the standalone
    # print page, which has no click-to-expand interaction a reader could
    # use) renders it already open, and drops the id-based
    # click/pattern-matching entirely so this static page carries no
    # dependency on the interactive modal's own toggle callback.
    #
    # date/run/wind_kt/hz thread through from _impact_breakdown_content (the
    # only caller) so this section's own numbers stay in sync with the main
    # breakdown table's, rather than resolving against the
    # frozen "active right now" storm/50kt default regardless of what the
    # user has selected, which would silently disagree with the table above it.
    # `breakdown` (from _hazard_breakdown, same instance the main table
    # above already uses) drives _admin1_table's own _hazard_split_line
    # calls, falls back to _hazard_breakdown()'s own all-active default
    # only for a caller that hasn't been updated to pass it through (there
    # are none left, kept as a safety net, not a real illustrative-data path
    # since tc_active/flood_active would only gate whether a split renders
    # at all, never fabricate the split's own real percentages).
    breakdown = breakdown if breakdown is not None else _hazard_breakdown()
    head_id = {} if expanded else {"id": {"type": "admin1-head", "country": country}}
    body_id = {} if expanded else {"id": {"type": "admin1-body", "country": country}}
    # The print page (expanded=True) has no click interaction at all, so it
    # must render the real content eagerly, same as before. The interactive
    # modal instead starts EMPTY and defers the real fetch to _load_admin1
    # (see that callback's own docstring for the full "why": this section
    # is collapsed by default, and the country-level table above it already
    # covers the everyday view, so unconditionally paying a real HTTP+
    # Snowflake round trip here on every modal render/slider tweak for a
    # section most opens never actually expand was pure avoidable latency).
    # _load_admin1 needs country/date/run/hz/breakdown to build the SAME
    # real content this function would have built eagerly; stashed in a
    # per-section dcc.Store (all JSON-serializable plain dicts/scalars)
    # rather than re-derived from raw UI state inside that callback, so
    # there's exactly ONE place (_impact_breakdown_content) that resolves
    # these values, not two independent copies that could drift apart.
    if expanded:
        body_content = _admin1_body_content(country, date, run, hz, breakdown)
        params_store = []
    else:
        body_content = []
        params_store = [dcc.Store(id={"type": "admin1-params", "country": country},
                                     data={"country": country, "date": date, "run": run, "hz": hz, "breakdown": breakdown})]
    return html.Div([
        html.Div([
            dmc.Text(_t("Admin Level 1 Breakdown — {country}", country=_t(country)), fw=700, size="11px", c="dimmed", style={"flex": 1}),
        ] + ([] if expanded else [html.Span("▾", style={"fontSize": "10px", "color": "#8ea0ab"})]),
           **head_id,
           style={"display": "flex", "alignItems": "center", "gap": "8px",
                   **({} if expanded else {"cursor": "pointer"})}),
        html.Div(body_content, **body_id,
           # width:"100%", same fix as the main breakdown table
           # (_impact_breakdown_content's main_row): overflowX:auto alone
           # does nothing on a div whose width is the default "auto", since
           # the div just grows to match its (wider) table child instead of
           # clipping/scrolling it. overflowY:"visible" (explicit, not left
           # unset), per the CSS overflow spec, setting overflow-x to
           # anything other than visible/clip while leaving overflow-y
           # unset computes overflow-y as "auto" too, which was adding a
           # redundant vertical scrollbar to each admin-1 table on top of
           # the modal's own single outer scroll.
           style={"marginTop": "10px", "display": "block" if expanded else "none", "width": "100%", "maxWidth": "100%",
                   "overflowX": "auto", "overflowY": "visible"}),
    ] + params_store,
    # marginTop/paddingTop (was 14/14), more breathing room between the
    # main breakdown table and the first Admin Level 1 section (and between
    # each subsequent one), so this reads as its own distinct section
    # rather than just another row butted up against the table above.
    style={"marginTop": "32px", "paddingTop": "22px", "borderTop": "1px solid #eef2f5"})


def _simple_breakdown_table(cols, breakdown, member="combined", pin_source=None, compare_source=None,
                              age_split_source=None, age_compare_source=None, breakdown_by_metric_by_col=None):
    # `breakdown_by_metric_by_col` (see
    # _compute_breakdown_by_metric's own docstring): {col_label: {metric_or_
    # age_band_label: real_breakdown_dict}}, one real per-metric bitmask
    # family split per COLUMN (country/Combined), replacing the flat
    # illustrative `breakdown` that otherwise applies uniformly to every
    # cell regardless of metric. Optional (defaults to `{}`, i.e. every
    # cell falls back to the flat `breakdown`) so this function still works
    # unchanged for any caller that hasn't computed real per-metric splits.
    #
    # `cols` is [(label, base_stats), ...]. `pin_source` (a {label: pin_pct}
    # dict) turns on the Country-Analysis extras: paired At Risk/In Need
    # columns + children age-band rows (now including their own In Need
    # share) + a second, colored worst-case NUMBER per cell (tagged with the
    # member, e.g. "#6") instead of a bare percentage, same real-numbers
    # pattern as the compact tiles, not an abstract delta. Soft dividers +
    # tinted header/stripes instead of a plain grid, matching the rest of
    # the page's card-like look rather than a bare default HTML table.
    # No whiteSpace:nowrap, table-layout:fixed below means a too-narrow
    # column can't grow the table to fit e.g. "WASH FACILITIES" on one
    # line; it needs to be free to wrap instead.
    #
    # `compare_source` (a {label: real_stats_dict} dict, real replacement
    # for the old _MEMBER_SCENARIO_FACTOR fake multiplier), precomputed by
    # the caller (_impact_breakdown_content), one entry per column in
    # `cols`, via _real_member_stats for a real country or _combined_stats
    # (member=...) for the synthetic "Combined" column. A label absent from
    # `compare_source` (or mapped to None) means no real per-member data
    # resolves for that column, that column shows no comparison number,
    # rather than a fabricated one.
    th_style = {"textAlign": "left", "padding": "9px 12px", "borderBottom": "1px solid #dde6ec",
                "fontSize": "10.5px", "color": "#57707e",
                "background": "#f6f9fb", "textTransform": "uppercase", "letterSpacing": "0.3px"}
    sub_th_style = {**th_style, "textAlign": "center", "fontWeight": 500, "fontSize": "9.5px",
                     "padding": "5px 12px", "textTransform": "none", "letterSpacing": "normal"}
    td_base = {"padding": "8px 12px", "fontSize": "12px"}
    td_dash_style = {**td_base, "color": "#c3ccd2", "textAlign": "center"}

    compare_source = compare_source or {}
    breakdown_by_metric_by_col = breakdown_by_metric_by_col or {}
    has_compare = bool(member and member not in (None, "combined", "none"))
    member_tag = _member_short_label(member) if has_compare else ""

    def _row_style(i):
        return {"background": "#fafcfd" if i % 2 else "#ffffff", "borderBottom": "1px solid #f1f4f6"}

    # A distinct divider between each country's own pair of columns, plain
    # adjacent cells with only the regular 1px grid lines made it hard to
    # tell at a glance where e.g. Vietnam's columns end and Combined's
    # begin, especially scrolled mid-table. Skipped for the very first
    # column (already bordered by the Metric column) and for is_combined
    # (which gets its own blue-tinted divider instead, see below).
    _GROUP_DIVIDER = {"borderLeft": "2px solid #dde3ea"}
    # Combined must NOT color its own NUMBERS blue (reusing RIVER) to read
    # as a distinct "total" column, RIVER is also the Flood color in the
    # hazard-split line under every value, so a blue base number next to a
    # blue Flood-share number would read as ambiguous ("is this blue number
    # the combined total or the flood share?"). A light RIVER-tinted
    # BACKGROUND across the whole column keeps the same color scheme/
    # identity without touching any text color, so numbers stay legible
    # and unambiguous.
    _COMBINED_DIVIDER = {"borderLeft": f"2px solid {RIVER}55", "background": f"{RIVER}12"}

    def _group_style(idx, is_combined):
        if idx == 0:
            return {"background": f"{RIVER}12"} if is_combined else {}
        return _COMBINED_DIVIDER if is_combined else _GROUP_DIVIDER

    def _combined_bg(is_combined):
        # Background only, no border, for cells (like the "In Need"
        # sub-header) that sit to the right of the group's leading edge and
        # so shouldn't repeat the divider, but still need the same tint so
        # the whole Combined column reads as one continuous block.
        return {"background": f"{RIVER}12"} if is_combined else {}

    # Same real-zero-reads-lighter treatment
    # _admin1_table's own _num_style already applies, a plain black "0" in
    # the main breakdown table would otherwise read as prominently/alarmingly
    # as an actually-affected nonzero count right next to it.
    def _num_style(val, base_style):
        return {**base_style, "color": "#adb5bd"} if val == 0 else base_style

    def _value_td(base_n, compare_n, td_style, metric_key=None, col_label=None):
        # Centered, directly under the "At Risk"/"In Need" header labels
        # (already centered via sub_th_style) instead of hugging the left
        # edge, which read as misaligned once the columns were narrowed.
        # base_n is None when this cell's own country/Combined genuinely has
        # no real data, either an In Need cell with no real in-need data,
        # or an At Risk facility cell (Schools/Health Centers/Shelters/
        # WASH) with a real all-NULL column (e.g. Turks and Caicos Islands'
        # shelters), see _fetch_real_combined_tile_totals_uncached's own
        # comments for both. Rendered as a distinct italic "N/A", not via
        # _num_style's grey-zero treatment, which is reserved for a
        # CONFIRMED real zero, the two must stay visually distinguishable.
        if base_n is None:
            return html.Td(html.Div(_t("N/A"), style={"color": "#adb5bd", "fontStyle": "italic", "fontSize": "11px"}),
                            style={**td_style, "textAlign": "center"})
        # Real per-metric, per-column breakdown when available (see
        # breakdown_by_metric_by_col's own comment). Three real cases, NOT
        # two: (1) a real per-metric split resolved -> use it; (2) both
        # families are active but no real split resolved for THIS metric
        # (a genuine data gap, breakdown_by_metric_by_col's own "absent
        # means no data" contract, see _compute_breakdown_by_metric) ->
        # None, _hazard_split_line renders an honest "no real split" state,
        # NEVER the flat illustrative percentages (see that function's own
        # comment for the full "why" this used to be a bug); (3) only one
        # family is active at all, nothing to split in the first place ->
        # the flat `breakdown` is itself real here (its own tc_active/
        # flood_active gate is what makes _hazard_split_line render nothing
        # for this case), not an illustrative substitute.
        if metric_key and col_label:
            metric_split = breakdown_by_metric_by_col.get(col_label, {}).get(metric_key)
            if metric_split is not None:
                cell_breakdown = metric_split
            elif breakdown["tc_active"] and breakdown["flood_active"]:
                cell_breakdown = None
            else:
                cell_breakdown = breakdown
        else:
            cell_breakdown = breakdown
        children = [
            html.Div(_format_stat_number(base_n), style=_num_style(base_n, {"fontWeight": 700, "fontFamily": "monospace"})),
            _hazard_split_line(base_n, cell_breakdown),
        ]
        if compare_n is not None:
            children.append(html.Div(f"{_format_stat_number(compare_n)} {member_tag}",
                                       style={"fontSize": "10px", "fontWeight": 700, "color": "#d94f3c", "marginTop": "2px"}))
        return html.Td(children, style={**td_style, "textAlign": "center"})

    has_pin = pin_source is not None
    # The Combined column (when present) is always the LAST one in `cols`,
    # see _impact_breakdown_content, which appends it after every real
    # country. combined_idx is None (no special styling) for the Global
    # table or a single/plain country selection with no Combined column.
    combined_idx = len(cols) - 1 if len(cols) > 1 and str(cols[-1][0]).startswith(_t("Combined")) else None
    # table-layout:fixed splits width equally across columns unless the
    # FIRST ROW gives one an explicit width, without this, "Metric" got
    # squeezed to the same width as a single At Risk/In Need sub-column,
    # far too narrow for labels like "Age 5–14 (School-age)".
    # Explicit width on each colSpan=2 header (not just Metric), under
    # table-layout:fixed, an unset width on a column-spanning cell just
    # gets divided evenly among ALL columns regardless of how many actually
    # need it, which squeezed "At Risk"/"In Need" down to illegible ~55px
    # sub-columns. 230px÷2 gives each sub-column enough room for e.g. "410K".
    header_rows = [html.Tr(
        [html.Th(_t("Metric"), style={**th_style, "width": "170px"}, rowSpan=2)]
        + [html.Th(_t(c), style={**th_style, "textAlign": "center", "width": "190px" if has_pin else "140px",
                                   **_group_style(idx, idx == combined_idx)},
                    colSpan=2 if has_pin else 1) for idx, (c, _) in enumerate(cols)]
    )]
    if has_pin:
        header_rows.append(html.Tr(
            [html.Th(_t(lbl), style={**sub_th_style,
                                        **(_group_style(idx, idx == combined_idx) if lbl == "At Risk"
                                            else _combined_bg(idx == combined_idx))})
              for idx, _ in enumerate(cols) for lbl in ("At Risk", "In Need")]
        ))

    rows = []
    row_i = 0
    age_split_source = age_split_source or {}
    age_compare_source = age_compare_source or {}

    for row_label, risk_key, pin_key in (("People", "People at Risk", "people"),
                                           ("Children (total)", "Children at Risk", "children")):
        rstyle = _row_style(row_i); row_i += 1
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(row_label), style={**td_style, "color": "#16232c", "fontWeight": 700})]
        for idx, (c, base_stats) in enumerate(cols):
            is_combined = idx == combined_idx
            group_style = {**td_style, **_group_style(idx, is_combined)}
            base_n = _parse_stat_number(base_stats.get(risk_key, "0"))
            comp_stats, comp_pin = (compare_source.get(c) or (None, None)) if has_compare else (None, None)
            compare_n = _parse_stat_number(comp_stats.get(risk_key, "0")) if comp_stats else None
            cells.append(_value_td(base_n, compare_n, group_style, metric_key=risk_key, col_label=c))
            if has_pin:
                # Real, un-derived absolute in-need count, read directly
                # instead of reconstructing via base_n * pct / 100, which
                # would silently collapse to In Need == At Risk
                # once pct has already been clamped to 100% one level down,
                # see _fetch_real_combined_tile_totals_uncached's own
                # pin_pct comment for the full mechanism.
                base_abs = pin_source.get(c, _DEFAULT_PIN_PCT).get(f"{pin_key}_abs")
                base_in_need = math.ceil(base_abs) if base_abs is not None else None
                compare_in_need = None
                if comp_stats and comp_pin and comp_pin.get(f"{pin_key}_abs") is not None:
                    compare_in_need = math.ceil(comp_pin[f"{pin_key}_abs"])
                cells.append(_value_td(base_in_need, compare_in_need,
                                         {**td_style, **_group_style(idx, is_combined)}))
        rows.append(html.Tr(cells))

    # Real per-age children breakdown, At Risk AND In Need, both real
    # numbers from age_split_source/age_compare_source (see
    # _get_country_age_split/_combined_age_split), not an illustrative share
    # of the Children (total) row above (see
    # _CHILD_AGE_BANDS's own comment).
    for age_label in _CHILD_AGE_BANDS:
        rstyle = _row_style(row_i); row_i += 1
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(age_label), style={**td_style, "color": "#8ea0ab", "paddingLeft": "24px"})]
        for idx, (c, base_stats) in enumerate(cols):
            is_combined = idx == combined_idx
            group_style = {**td_style, **_group_style(idx, is_combined)}
            band = age_split_source.get(c, {}).get(age_label) or _ZERO_AGE_SPLIT[age_label]
            # None (not a fabricated 0) when this age band's own population
            # column is genuinely missing for this country (e.g. Curaçao's
            # real all-NULL E_adolescent_population, see
            # _fetch_real_combined_tile_totals_uncached's own comment).
            base_age = math.ceil(band["at_risk"]) if band["at_risk"] is not None else None
            comp_band = (age_compare_source.get(c) or {}).get(age_label) if has_compare else None
            compare_age = (math.ceil(comp_band["at_risk"]) if comp_band and comp_band.get("at_risk") is not None else None)
            cells.append(_value_td(base_age, compare_age, group_style, metric_key=age_label, col_label=c))
            if has_pin:
                # Same real-abs-not-reconstructed fix as the People/Children
                # rows above.
                base_age_need = (math.ceil(band["in_need_abs"])
                                  if band.get("in_need_abs") is not None else None)
                compare_age_need = (math.ceil(comp_band["in_need_abs"])
                                     if comp_band and comp_band.get("in_need_abs") is not None else None)
                cells.append(_value_td(base_age_need, compare_age_need,
                                         {**td_style, **_group_style(idx, is_combined)}))
        rows.append(html.Tr(cells))

    # Remaining at-risk-only metrics (Schools/Health Centers/Shelters/WASH).
    for key in [k for k in _DEFAULT_STATS if k not in ("People at Risk", "Children at Risk")]:
        rstyle = _row_style(row_i); row_i += 1
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(key.replace(" at Risk", "")), style={**td_style, "color": "#57707e"})]
        for idx, (c, base_stats) in enumerate(cols):
            is_combined = idx == combined_idx
            group_style = {**td_style, **_group_style(idx, is_combined)}
            base_n = _parse_stat_number(base_stats.get(key, "0"))
            comp_stats, _ = (compare_source.get(c) or (None, None)) if has_compare else (None, None)
            compare_n = _parse_stat_number(comp_stats.get(key, "0")) if comp_stats else None
            cells.append(_value_td(base_n, compare_n, group_style, metric_key=key, col_label=c))
            if has_pin:
                cells.append(html.Td("—", style={**td_dash_style, **rstyle, **_group_style(idx, is_combined)}))
        rows.append(html.Tr(cells))

    # tableLayout:"fixed" is the actual fix for the table overflowing past
    # its container no matter how the wrapping divs were constrained,
    # table-layout:auto (the default) treats width:100% as a mere
    # suggestion and still grows the table past it whenever cell content
    # doesn't naturally fit; fixed layout makes the width a hard constraint,
    # wrapping cell content instead of growing the table.
    # minWidth is what actually makes horizontal scroll happen for many
    # countries, table-layout:fixed treats the explicit per-column widths
    # above as PROPORTIONAL WEIGHTS whenever the table's own width:100% is
    # narrower than their sum, so it was just shrinking all columns to fit
    # (illegibly, for 7-8 countries) rather than actually overflowing.
    # min-width forces the table to never shrink below what those columns
    # actually need, so once the container really is narrower, the parent's
    # overflowX:auto has something genuine to scroll.
    min_width_px = 170 + len(cols) * 190
    table = html.Table([html.Thead(header_rows), html.Tbody(rows)],
                         style={"width": "100%", "minWidth": f"{min_width_px}px",
                                 "tableLayout": "fixed", "borderCollapse": "collapse"})
    # width:"fit-content", same fix as _admin1_table's wrapper: this div's
    # default width:auto (100% of its parent) plus overflow:"hidden" (for
    # rounded corners) was clipping the table at THIS boundary before the
    # ancestor's own overflowX:auto ever got a chance to provide a
    # scrollbar. fit-content makes this div match the table's real
    # (min-width-driven) size so the ancestor can detect the real overflow.
    table_wrapper = html.Div(table, style={"border": "1px solid #eef2f5", "borderRadius": "10px",
                                             "overflow": "hidden", "width": "fit-content"})
    # Legend for the hazard-split line under every value, without it the
    # orange/blue pair reads as unexplained, same reasoning as the At Risk/
    # In Need legend under the Admin Level 1 breakdown.
    # _in_need_note only when this table actually HAS In Need columns at
    # all (has_pin, Global's own single-column table never does), same
    # clarification shown next to the Impact Summary panel's own arc
    # charts, repeated here since this table has its own separate People/
    # Children In Need columns a reader could see without ever opening
    # that panel.
    return html.Div([table_wrapper, _hazard_split_legend(breakdown)] + ([_in_need_note(mt=10)] if has_pin else []))


# Stacked (not side by side) in the Impact Summary panel's ~260px-wide
# content column, each chart gets the panel's full width instead of half
# of it, so it can afford to be taller too without looking squeezed.
_ARC_CHART_HEIGHT = 190


def _pin_arc_charts_block_from(label, base_stats, pin_pct, totals, member, compare_pop=None, compare_children=None,
                                  label_is_country=True, show_label=True):
    # Same 270° 3-ring polar-bar gauge as the real dashboard's "PEOPLE IN
    # NEED" panel (layouts/panels.py:871-889), see _make_arc_chart above.
    # Takes already-resolved stats/pin_pct/totals rather than a country
    # name, so the same renderer serves both a single country
    # (_pin_arc_charts_block) and a "Combined Total" aggregate
    # (_pin_arc_charts_block_combined) without duplicating this logic.
    # show_label=False is for embedding this directly under a _stat_grid
    # (the Impact Summary panel's own In Need visualization) that already
    # shows the country/"Combined" label itself, repeating it here would
    # just be redundant right below it.
    # The chart RINGS always reflect the base (Probabilistic) numbers, the
    # primary view stays stable regardless of the worst-case factor picked;
    # a second, colored number is added below (member-tagged, e.g. "#8")
    # exactly like the stat tiles above already do, rather than replacing
    # the primary number/rings outright.
    exposed_pop = _parse_stat_number(base_stats["People at Risk"])
    exposed_children = _parse_stat_number(base_stats["Children at Risk"])
    # None (not a fabricated 0) when this country/Combined genuinely has no
    # real in-need data for people/children, see
    # _fetch_real_combined_tile_totals_uncached's own comment. _make_arc_chart
    # renders this ring as an explicit "N/A" state, not a misleading
    # 0%-filled one.
    #
    # Real, un-derived absolute in-need count, read directly instead of
    # reconstructing via exposed_pop * pct / 100, which would silently
    # collapse to In Need == At Risk once pct has already been clamped to
    # 100% one level down, see _fetch_real_combined_tile_totals_uncached's
    # own pin_pct comment.
    in_need_pop = math.ceil(pin_pct["people_abs"]) if pin_pct.get("people_abs") is not None else None
    in_need_children = math.ceil(pin_pct["children_abs"]) if pin_pct.get("children_abs") is not None else None

    # compare_pop/compare_children (real per-member In Need numbers) are
    # computed by the caller (_pin_arc_charts_block/_pin_arc_charts_block_
    # combined, which have the country/date/run/wind_kt context needed for a
    # real get_track_impacts lookup) and passed straight through, this
    # function only renders, it doesn't compute the comparison itself.
    member_tag = _member_short_label(member) if member else ""

    fig_people = _make_arc_chart(totals["population"], exposed_pop, in_need_pop, "People At Risk", "People In Need",
                                   height=_ARC_CHART_HEIGHT)
    fig_children = _make_arc_chart(totals["children"], exposed_children, in_need_children,
                                     "Children At Risk", "Children In Need", height=_ARC_CHART_HEIGHT)
    _num_style = {"fontSize": "1.1em", "fontWeight": 700, "color": "#212529", "display": "block",
                  "lineHeight": "1.1", "marginBottom": "2px"}
    _compare_style = {"fontSize": "10px", "fontWeight": 700, "color": "#d94f3c", "marginBottom": "2px"}

    def _chart_col(col_label, number, compare_number, fig):
        col_children = [
            dmc.Text(_t(col_label), size="xs", fw=600, c="dark"),
            html.Span(number, style=_num_style),
        ]
        if compare_number is not None:
            col_children.append(html.Span(f"{_format_stat_number(compare_number)} {member_tag}", style=_compare_style))
        col_children.append(dcc.Graph(figure=fig, config={"displayModeBar": False},
                                        style={"height": f"{_ARC_CHART_HEIGHT}px", "width": "100%"}))
        return html.Div(col_children, style={"width": "100%"})

    children = []
    if show_label:
        children.append(dmc.Text(_t(label) if label_is_country else label, size="sm", fw=700, mb=6))
    # Stacked, not side by side, the Impact Summary panel is only ~260px
    # of usable width, which would squeeze two charts down to illegible
    # ~120px each; full-width stacked charts can actually be read. Children
    # first, People second, deliberately the reverse of the People/Children
    # tile order above them.
    children.append(html.Div([
        _chart_col("Children In Need", _format_stat_number(in_need_children) if in_need_children is not None else _t("N/A"),
                    compare_children, fig_children),
        _chart_col("People In Need", _format_stat_number(in_need_pop) if in_need_pop is not None else _t("N/A"),
                    compare_pop, fig_people),
    ], style={"display": "flex", "flexDirection": "column", "gap": "14px"}))
    # Shared clarification note (_in_need_note), stays visible
    # unconditionally rather than only when other hazards are toggled on,
    # since these particular numbers never reflect anything but Sustained
    # Wind, and never scale with the wind threshold, regardless of what
    # else is selected.
    children.append(_in_need_note())
    # Plain spacing here, not a borderTop line. The stat tiles and arc
    # charts below both belong to the SAME country/scope (just two
    # different views of its numbers), so a full divider line here would
    # read as if it marked a boundary between two different things (easy
    # to mistake for the boundary between two different COUNTRIES' blocks
    # once several are stacked in the "split" view), see
    # _update_impact_summary's own per-country block separator, which
    # owns that actual boundary instead.
    return html.Div(children, style={"marginTop": "14px"})


def _pin_arc_charts_block(country, member, show_label=True, scale=1.0, date=None, run=None, wind_kt=None, hz=None):
    # `scale`/_hazard_breakdown no longer applies here, _get_country_stats
    # already returns the real combined-across-active-hazards total when a
    # real `hz` is passed in (see _fetch_real_combined_tile_totals). Never
    # applies to _get_country_totals either way, that's the outer ring's
    # denominator (how many people COULD be there at all), which doesn't
    # depend on which hazards are currently toggled on.
    compare_pop = compare_children = None
    if member:
        # real_stats here is only a presence gate ("does a real member
        # scaling exist at all"), the actual displayed compare numbers
        # always come from real_pin below, which stays wind-only by design
        # (see _in_need_note's own "deliberate asymmetry" docstring note).
        real_stats = _real_member_stats(country, date, run, wind_kt, member, hz=hz)
        real_pin = _real_member_pin_pct(country, date, run, wind_kt, member)
        if real_stats is not None and real_pin is not None:
            # real_pin["people"]/["children"] individually None means this
            # member genuinely has no real in-need data (see
            # _real_member_pin_pct's own comment), leave compare_pop/
            # compare_children as None (no comparison number shown) rather
            # than crashing or fabricating one.
            # Real, un-derived absolute in-need counts, read directly
            # instead of reconstructing via at_risk * pct / 100 (see
            # _fetch_real_combined_tile_totals_uncached's own pin_pct
            # comment for why that reconstruction silently corrupts once
            # pct has already been clamped to 100%).
            if real_pin.get("people_abs") is not None:
                compare_pop = math.ceil(real_pin["people_abs"])
            if real_pin.get("children_abs") is not None:
                compare_children = math.ceil(real_pin["children_abs"])
    return _pin_arc_charts_block_from(
        country, _get_country_stats(country, date, run, wind_kt, hz=hz),
        _get_country_pin_pct(country, date, run, wind_kt, hz=hz),
        _get_country_totals(country), member,
        compare_pop=compare_pop, compare_children=compare_children, show_label=show_label,
    )


def _pin_arc_charts_block_combined(countries, member, show_label=True, scale=1.0, date=None, run=None, wind_kt=None, hz=None):
    combined_base = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
    _people_pct = _combined_in_need_pct(countries, "People at Risk", "people",
                                          _parse_stat_number(combined_base["People at Risk"]),
                                          date=date, run=run, wind_kt=wind_kt, hz=hz)
    _children_pct = _combined_in_need_pct(countries, "Children at Risk", "children",
                                            _parse_stat_number(combined_base["Children at Risk"]),
                                            date=date, run=run, wind_kt=wind_kt, hz=hz)
    combined_pin = {
        "people": _people_pct,
        "children": _children_pct,
        # Real, un-derived absolute in-need totals, _pin_arc_charts_block_
        # from now reads these directly instead of reconstructing via
        # exposed * pct / 100 (see that function's own comment). Same
        # None-ness as the already-correct pct fields above (both are
        # gated on the identical "does any selected country have real
        # in-need data" check inside _combined_in_need_pct), a genuine sum
        # of zero real countries' contributions still correctly computes
        # to 0.0, never fabricated when at least one country has real data.
        "people_abs": (_combined_in_need_total(countries, "People at Risk", "people",
                                                 date=date, run=run, wind_kt=wind_kt, hz=hz)
                        if _people_pct is not None else None),
        "children_abs": (_combined_in_need_total(countries, "Children at Risk", "children",
                                                    date=date, run=run, wind_kt=wind_kt, hz=hz)
                           if _children_pct is not None else None),
    }
    compare_pop = compare_children = None
    if member:
        # Sum of each real country's OWN real per-member number (same
        # "sum, don't average" contract _combined_stats/
        # _combined_in_need_total already use for the base numbers above).
        compare_pop = math.ceil(_combined_in_need_total(
            countries, "People at Risk", "people", member=member, date=date, run=run, wind_kt=wind_kt, hz=hz))
        compare_children = math.ceil(_combined_in_need_total(
            countries, "Children at Risk", "children", member=member, date=date, run=run, wind_kt=wind_kt, hz=hz))
    # No has_real-style None-guard needed here (unlike _combined_stats),
    # _get_country_totals always returns real ints (0 as its own honest
    # fallback, never a fabricated mock, see its own docstring), so an
    # unconditional sum here is already correct: a country with no real
    # base-layer data contributes a real, accurate 0.
    combined_totals = {"population": 0, "children": 0}
    for c in countries:
        t = _get_country_totals(c)
        combined_totals["population"] += t["population"]
        combined_totals["children"] += t["children"]
    return _pin_arc_charts_block_from(_t("Combined — {n} countries", n=len(countries)),
                                        combined_base, combined_pin, combined_totals, member,
                                        compare_pop=compare_pop, compare_children=compare_children,
                                        label_is_country=False, show_label=show_label)


def _breakdown_modal_width(countries):
    """Explicit pixel width for the Full Impact Breakdown modal, computed
    from how many columns _simple_breakdown_table is actually about to
    render, a real number Mantine can't misinterpret the way it apparently
    did with fit-content/vw values. Global has one plain column (no At
    Risk/In Need split); Country Analysis columns are twice as wide (paired
    At Risk + In Need sub-columns), plus one extra "Combined" column once
    there's more than one country.
    """
    countries = countries or []
    if not countries:
        return f"{max(480, min(170 + 140 + 120, 2000))}px"

    num_cols = len(countries) + (1 if len(countries) > 1 else 0)
    # 190px per data column, narrowed from 260 (the At Risk/In Need
    # sub-columns only ever hold short numbers like "640K", centering them
    # made the extra width just look like empty padding either side).
    # Matches the explicit width set on each colSpan=2 header in
    # _simple_breakdown_table.
    main_width = 170 + num_cols * 190 + 120

    # Whenever there's at least one country, _admin1_table also renders,
    # one PER selected country, but each is its own FIXED 7-column table
    # (Region 140 + People/Children 115 each + Schools/Shelters 70 each +
    # Health Centers 95 + WASH 80 = 685px raw, _admin1_table's own
    # _COL_WIDTHS), regardless of how many countries are selected overall.
    # That table kept getting cut off even after matching the raw column
    # widths exactly, real rendered width runs measurably wider than the
    # sum of declared column widths (cell padding/borders/table border adds
    # up across 7 columns), so this floor is deliberately padded well past
    # the raw 685px sum rather than trying to match it exactly again.
    admin1_width = 1150
    width = max(main_width, admin1_width)
    # No viewport-relative cap (maxWidth:95vw would clamp the modal BELOW
    # what the admin1 table actually needs on a non-maximized browser
    # window, making it look cut off even though the main table above it
    # fits fine). This value is
    # already capped at 2000px on its own; overflowX:auto (on the modal
    # body itself now, not just the table's own wrapper, see
    # _MODAL_PANEL_STYLES) is the fallback if a screen genuinely can't
    # show the full width.
    return f"{max(480, min(width, 2000))}px"


def _impact_breakdown_content(countries, influencing_factor, expand_admin1=False,
                                 wind_on=True, gust_on=False, river_on=True, rain_on=True, surge_on=True,
                                 wind_idx=None, gust_idx=None, river_idx=None, rain_idx=None, surge_idx=None, rain_window=None,
                                 river_window=None, date=None, run=None):
    # Global (no countries selected): exactly the original simple table,
    # no In Need rows, no worst-case comparison, no arc charts. Those are
    # all Country-Analysis-only.
    #
    # wind_on/river_on/rain_on/surge_on mirror the sidebar's own hazard
    # checkboxes (ms-wind-on/ms-river-on/ms-rain-on/ms-surge-on; Gust
    # excluded, see _hazard_breakdown), this breakdown is scoped to
    # whichever hazards are actually toggled on the map, not always the
    # full mock total regardless of what's active. _active_hazards_indicator
    # makes that scoping visible at the top instead of silently changing
    # numbers with no explanation.
    #
    # date/run (topbar-date/topbar-time's own values, threaded through from
    # both _update_impact_breakdown and the standalone print page) make the
    # country/admin1 numbers below REACTIVE to the selected forecast cycle,
    # see _resolve_storm_for_country's own docstring for why. wind_idx is
    # ALSO now resolved (via _resolve_wind_kt) into the real wind-severity
    # threshold used to fetch those same numbers, not just fed to the
    # threshold-preview widget as before.
    breakdown = _hazard_breakdown(wind_on, river_on, rain_on, surge_on)
    hazards_indicator = _active_hazards_indicator(breakdown)
    hazard_idx = {"Sustained Wind": wind_idx, "River Flooding": river_idx,
                   "Rainfall": rain_idx, "Storm Surge": surge_idx}
    wind_kt = _resolve_wind_kt(wind_idx)
    # Real per-hazard combined total (Wind/Gust/River/Rain, see _build_hz/
    # _fetch_real_combined_tile_totals), not an illustrative
    # percentage-scaled wind-only total. `breakdown`/`scale` still
    # drives the hazard-family indicator (tc_active/flood_active, which
    # hazards are even toggled on, unrelated to any split math), and the
    # TC-only/Both/Flood-only split itself (_hazard_split_line via
    # _simple_breakdown_table) is REAL too, a real per-metric,
    # per-column bitmask family split, not the flat illustrative
    # _HAZARD_OVERLAP_FRAC split applied uniformly to every cell (see
    # _compute_breakdown_by_metric).
    hz = _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx, gust_idx, river_idx, rain_idx, rain_window, river_window)
    countries = countries or []
    # Real replacement for the old _WORST_MEMBER_BY_FACTOR fixed dict, the
    # real ensemble member with the highest real severity for whichever
    # property the user picked, for THESE specific countries/storm/date/
    # threshold (None -> no comparison, shows Probabilistic only).
    member = _resolve_worst_member_multi(influencing_factor, countries, date, run, wind_kt, hz=hz) if countries else None
    if not countries:
        # Must NOT read _DEFAULT_STATS (an illustrative mock, scaled by
        # breakdown["scale"] on top) for the Global branch here, same
        # real pattern as _update_impact_summary's own Global branch:
        # _global_flood_availability + _build_hz + _combined_stats across
        # every real country with real impact data for the selected date.
        all_country_names, river_avail, rain_avail = _global_flood_availability(
            date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
        # wind_on=True (matches river/rain's own real-availability gating:
        # an active storm's own wind data is always real, no live checkbox
        # needed to decide "is there something to show"); gust_on=False
        # explicitly, not just left to whatever the checkbox says: Gust is
        # excluded from every combined total everywhere, not only here, see
        # _fetch_real_combined_tile_totals_uncached's own docstring.
        global_hz = _build_hz(True, False, river_avail, rain_avail, wind_idx, gust_idx, river_idx, rain_idx, rain_window, river_window)
        global_stats = _combined_stats(all_country_names, date=date, run=run,
                                         wind_kt=global_hz["wind_kt"], hz=global_hz)
        global_age_split = _combined_age_split(all_country_names, date=date, run=run,
                                                 wind_kt=global_hz["wind_kt"], hz=global_hz)
        # One real family_split fetch (shared cache with
        # global_stats' own underlying call, no extra round trip), giving
        # the table's inline hazard-split line real per-metric percentages
        # instead of the flat illustrative `breakdown` below.
        global_family_split = _combined_family_split(all_country_names, date=date, run=run, hz=global_hz)
        global_breakdown_by_metric = _compute_breakdown_by_metric(global_family_split, breakdown)
        # One real column PER country with real impact data (not just one
        # aggregated "Global" blob), same per-country + real TC-only/Both/
        # Flood-only split treatment Country Analysis's own table already
        # has below. Deliberately still skips In Need columns and
        # worst-case-member comparison (no `pin_source`/`compare_source`
        # passed to _simple_breakdown_table below): those were never part
        # of Global's own scope (see this function's own top comment), and
        # this is specifically about the per-country breakdown + hazard
        # split, not about expanding Global into every Country-Analysis-
        # only feature. Every real country shown, no cap. This can get wide
        # for a big multi-country event, same as an equally large Country
        # Analysis selection already can, the table's own overflowX:auto
        # handles it.
        #
        # Concurrent fan-out (get_query_executor().map), same reasoning as
        # Country Analysis's own _country_bundle just below: N real
        # Snowflake-backed fetches, one per country, must run in parallel,
        # not serially, for this to stay responsive on an active multi-
        # country day.
        def _global_country_bundle(c):
            return (c, _get_country_stats(c, date, run, global_hz["wind_kt"], hz=global_hz),
                       _get_country_age_split(c, date, run, global_hz["wind_kt"], hz=global_hz),
                       _get_country_family_split(c, date, run, hz=global_hz))
        _global_bundles = list(get_query_executor().map(_global_country_bundle, all_country_names))
        combined_label = _t("Combined — {n} countries", n=len(all_country_names))
        global_cols = [(c, stats) for c, stats, _age, _fs in _global_bundles] + [(combined_label, global_stats)]
        global_age_split_source = {c: age for c, _stats, age, _fs in _global_bundles}
        global_age_split_source[combined_label] = global_age_split
        global_breakdown_by_metric_by_col = {c: _compute_breakdown_by_metric(fs, breakdown) for c, _stats, _age, fs in _global_bundles}
        global_breakdown_by_metric_by_col[combined_label] = global_breakdown_by_metric
        table = _simple_breakdown_table(global_cols, breakdown,
                                          age_split_source=global_age_split_source,
                                          breakdown_by_metric_by_col=global_breakdown_by_metric_by_col)
        threshold_preview = _hazard_threshold_preview(breakdown, hazard_idx, _parse_stat_number(global_stats["People at Risk"]),
                                                         rain_window=rain_window, river_window=river_window, expanded=expand_admin1,
                                                         scope="combined", countries=all_country_names, date=date, run=run)
        return html.Div([hazards_indicator, threshold_preview, table], style={"marginTop": "20px"})

    # Unlike the compact panel (which only has room for one view at a time,
    # toggled via impact-aggregation-toggle), the modal has the space to
    # show everything: every selected country's own column, PLUS one extra
    # "Combined" column at the end when there's more than one, not an
    # either/or choice here.
    #
    # No arc charts in here anymore, a table-plus-350px-chart-column flex
    # layout was exactly what made this modal impossible to size sensibly
    # (too narrow for the table once there were several country + Combined
    # columns, too wide for a single country otherwise). The circular charts
    # now live in the Impact Summary panel instead (_update_impact_summary),
    # replacing its plain In Need number tiles; this modal is just the table
    # (plus Admin Level 1 below), which sizes far more predictably on its own.
    # _get_country_stats/_get_country_pin_pct/_get_country_age_split all
    # share one @ttl_cache'd _fetch_real_combined_tile_totals per country
    # (calling all 3 for the same country is effectively free after the
    # first), so the real cost is this OUTER per-country loop, for a
    # multi-country storm selection (e.g. MELISSA across Turks and
    # Caicos/Jamaica/Cuba/Nicaragua) it must run concurrently, not
    # serially. Fetching each country's bundle concurrently turns N
    # sequential cold-cache Snowflake round-trips into ~1.
    def _country_bundle(c):
        return (c, _get_country_stats(c, date, run, wind_kt, hz=hz),
                  _get_country_pin_pct(c, date, run, wind_kt, hz=hz),
                  _get_country_age_split(c, date, run, wind_kt, hz=hz),
                  # Shares the SAME cached
                  # _fetch_real_combined_tile_totals call the 3 fetches
                  # above already trigger for this country, no extra real
                  # round trip.
                  _get_country_family_split(c, date, run, hz=hz))
    _bundles = list(get_query_executor().map(_country_bundle, countries))
    cols = [(c, stats) for c, stats, _pin, _age, _fs in _bundles]
    pin_source = {c: pin for c, _stats, pin, _age, _fs in _bundles}
    age_split_source = {c: age for c, _stats, _pin, age, _fs in _bundles}
    breakdown_by_metric_by_col = {c: _compute_breakdown_by_metric(fs, breakdown) for c, _stats, _pin, _age, fs in _bundles}
    # Real per-column comparison data, one (stats, pin) tuple per real
    # country, via _real_member_stats/_real_member_pin_pct. A country this
    # member has no real data for (e.g. its own row absent from
    # TRACK_MAT) is simply omitted, _simple_breakdown_table already treats
    # a missing/None entry as "no comparison for this column."
    compare_source = {}
    age_compare_source = {}
    if member:
        # Same perf fix as _country_bundle above, _real_member_pin_pct
        # always shares one @ttl_cache'd _member_track_impacts per
        # country; _real_member_stats/_real_member_age_split do too
        # UNLESS a flood hazard is active, in which case they instead
        # share one @ttl_cache'd _member_combined_impacts per country
        # (see _flood_hazard_active's own docstring), either way, the
        # real cost is this outer per-country loop, so fetch concurrently.
        def _country_compare_bundle(c):
            comp_stats = _real_member_stats(c, date, run, wind_kt, member, hz=hz)
            comp_pin = _real_member_pin_pct(c, date, run, wind_kt, member)
            comp_age = _real_member_age_split(c, date, run, wind_kt, member, hz=hz)
            return (c, comp_stats, comp_pin, comp_age)
        _compare_bundles = list(get_query_executor().map(_country_compare_bundle, countries))
        for c, comp_stats, comp_pin, comp_age in _compare_bundles:
            # comp_pin is wind-only by design (see _in_need_note's own
            # "deliberate asymmetry" note) and can be None while comp_stats
            # is genuinely real (River/Rain-only worst-case, no active
            # wind storm), an AND-gate here would silently drop the
            # entire At-Risk comparison column in that case. The consumers
            # below (comp_stats/comp_pin unpacked at lines ~5425/5486)
            # already handle comp_pin being None on its own (in-need
            # comparison cells just show nothing), so storing the tuple
            # whenever comp_stats alone is real is sufficient.
            if comp_stats is not None:
                compare_source[c] = (comp_stats, comp_pin)
            if comp_age is not None:
                age_compare_source[c] = comp_age

    if len(countries) > 1:
        combined_label = _t("Combined — {n} countries", n=len(countries))
        combined_stats = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        _people_pct = _combined_in_need_pct(countries, "People at Risk", "people",
                                              _parse_stat_number(combined_stats["People at Risk"]),
                                              date=date, run=run, wind_kt=wind_kt, hz=hz)
        _children_pct = _combined_in_need_pct(countries, "Children at Risk", "children",
                                                _parse_stat_number(combined_stats["Children at Risk"]),
                                                date=date, run=run, wind_kt=wind_kt, hz=hz)
        combined_pin = {
            "people": _people_pct,
            "children": _children_pct,
            # Real, un-derived absolute in-need totals, see
            # _pin_arc_charts_block_combined's own identical comment.
            "people_abs": (_combined_in_need_total(countries, "People at Risk", "people",
                                                     date=date, run=run, wind_kt=wind_kt, hz=hz)
                            if _people_pct is not None else None),
            "children_abs": (_combined_in_need_total(countries, "Children at Risk", "children",
                                                        date=date, run=run, wind_kt=wind_kt, hz=hz)
                               if _children_pct is not None else None),
        }
        cols = cols + [(combined_label, combined_stats)]
        pin_source = {**pin_source, combined_label: combined_pin}
        age_split_source[combined_label] = _combined_age_split(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        # Same real per-metric family split as every other
        # column, summed across `countries` (see _combined_family_split's
        # own docstring).
        combined_family_split = _combined_family_split(countries, date=date, run=run, hz=hz)
        breakdown_by_metric_by_col[combined_label] = _compute_breakdown_by_metric(combined_family_split, breakdown)
        if member:
            combined_compare_stats = _combined_stats(countries, member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)
            _cmp_people_pct = _combined_in_need_pct(countries, "People at Risk", "people",
                                                       _parse_stat_number(combined_compare_stats["People at Risk"]),
                                                       member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)
            _cmp_children_pct = _combined_in_need_pct(countries, "Children at Risk", "children",
                                                         _parse_stat_number(combined_compare_stats["Children at Risk"]),
                                                         member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)
            combined_compare_pin = {
                "people": _cmp_people_pct,
                "children": _cmp_children_pct,
                "people_abs": (_combined_in_need_total(countries, "People at Risk", "people",
                                                         member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)
                                if _cmp_people_pct is not None else None),
                "children_abs": (_combined_in_need_total(countries, "Children at Risk", "children",
                                                            member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)
                                   if _cmp_children_pct is not None else None),
            }
            compare_source[combined_label] = (combined_compare_stats, combined_compare_pin)
            age_compare_source[combined_label] = _combined_age_split(countries, member=member, date=date, run=run, wind_kt=wind_kt, hz=hz)

    table = _simple_breakdown_table(cols, breakdown, member=member, pin_source=pin_source, compare_source=compare_source,
                                      age_split_source=age_split_source, age_compare_source=age_compare_source,
                                      breakdown_by_metric_by_col=breakdown_by_metric_by_col)
    # width:"100%" (not the default "auto") is what actually makes
    # overflowX:auto do anything here, a div with default width:auto just
    # grows to match the table's own natural (wider) content size instead
    # of clipping/scrolling it, which is exactly why the table kept
    # visibly overflowing past the modal's edge no matter how the modal
    # itself was sized. Giving this div a DEFINITE width is what makes the
    # overflow rule apply at all.
    main_row = html.Div(table, style={"marginTop": "20px", "width": "100%", "maxWidth": "100%",
                                        "overflowX": "auto", "overflowY": "visible"})
    # Admin Level 1 breakdown stays per-country only, admin-1 regions
    # inherently belong to one specific country, so a "Combined" version of
    # this wouldn't mean anything. Its own hazard-type split (per region,
    # real, same TC-only/Both/Flood-only methodology as the main table's
    # own cells, see _admin1_table's own _region_metric_breakdown) is now
    # inline in each region's own table row, not a separate section.
    # `breakdown` (this same flat _hazard_breakdown() instance the main
    # table above already uses) threads through so both tables' own
    # tc_active/flood_active gating agrees.
    # Same reasoning as _country_bundle above: each _admin1_section call is
    # an independent per-country real fetch
    # (_fetch_real_combined_admin_totals, itself an HTTP round trip to
    # services/tile_server.py plus a get_admin_impacts in-need merge);
    # building the HTML around it is cheap, so fetch concurrently.
    admin1_sections = html.Div(list(get_query_executor().map(
        lambda c: _admin1_section(c, expanded=expand_admin1, date=date, run=run, wind_kt=wind_kt, hz=hz, breakdown=breakdown), countries)))
    # cols[-1] is always the "representative" scope, the appended Combined
    # column for 2+ countries, or the single selected country otherwise.
    # _resolve_stat_value's own scope contract: "combined" (+ the real
    # countries list) for 2+, or the real single country name directly,
    # NOT cols[-1][0], which for the multi-country case is the display
    # LABEL text ("Combined, {n} countries"), not a value _resolve_stat_
    # value/_get_country_stats would ever recognize as a real scope.
    preview_scope = "combined" if len(countries) > 1 else countries[0]
    threshold_preview = _hazard_threshold_preview(breakdown, hazard_idx, _parse_stat_number(cols[-1][1]["People at Risk"]),
                                                     rain_window=rain_window, river_window=river_window, expanded=expand_admin1,
                                                     scope=preview_scope, countries=countries, date=date, run=run)
    return html.Div([hazards_indicator, threshold_preview, main_row, admin1_sections])


def _resolve_stat_value(metric, scope, countries=None, date=None, run=None, wind_kt=None, hz=None,
                          river_idx=None, rain_idx=None, rain_window=None, river_window=None):
    # Recomputes the exact Probabilistic number shown on the clicked
    # card (server-side, from scope+metric) rather than smuggling the
    # display value into the id itself. Always the base (unscaled-by-
    # worst-case-member) value: the click explains the primary number, not
    # whichever worst-case comparison happens to be showing alongside it.
    # scope=="combined" has no single-country entry to look up, its numbers
    # only exist as the sum of `countries` (the current selection, passed in
    # by the caller), same math as _update_impact_summary's own "Combined
    # Total" branch. `hz` (real multi-hazard state, see _build_hz) makes
    # this the real combined-across-active-hazards total for every scope,
    # including "global" (see the elif branch's own comment).
    #
    # date/run/wind_kt (threaded through from _open_hazard_contribution)
    # keep this in sync with whatever the tile/panel it was clicked from is
    # ALSO currently showing, rather than resolving against the
    # frozen "active right now" storm/50kt default regardless of the
    # selected topbar date/run or wind-severity slider.
    if scope == "combined" and countries:
        stats = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        _people_pct = _combined_in_need_pct(countries, "People at Risk", "people",
                                              _parse_stat_number(stats["People at Risk"]),
                                              date=date, run=run, wind_kt=wind_kt, hz=hz)
        _children_pct = _combined_in_need_pct(countries, "Children at Risk", "children",
                                                _parse_stat_number(stats["Children at Risk"]),
                                                date=date, run=run, wind_kt=wind_kt, hz=hz)
        pin_pct = {
            "people": _people_pct,
            "children": _children_pct,
            # Real, un-derived absolute in-need totals, see
            # _pin_arc_charts_block_combined's own identical comment.
            "people_abs": (_combined_in_need_total(countries, "People at Risk", "people",
                                                     date=date, run=run, wind_kt=wind_kt, hz=hz)
                            if _people_pct is not None else None),
            "children_abs": (_combined_in_need_total(countries, "Children at Risk", "children",
                                                        date=date, run=run, wind_kt=wind_kt, hz=hz)
                               if _children_pct is not None else None),
        }
    elif scope == "global":
        # Must NOT read _DEFAULT_STATS (an illustrative mock, rescaled via
        # _scale_stats_by_hazard) here, that would be disconnected from
        # the real per-hazard-combined system _update_impact_summary's own
        # Global branch uses, showing made-up numbers instead of either
        # the real total OR the "no initialized country impacted"
        # explanation, even when the real total genuinely is 0. Same
        # _global_flood_availability roster _update_impact_summary's own
        # Global branch now uses (storms UNION real river/rain-impact
        # countries, see that function's own docstring for the two real
        # bugs this fixes), so the popup can never disagree with the number
        # that was actually clicked. Was _resolve_storms_for_date alone
        # (storm/TC-track-only) until this fix: a River/Rain-only day (no
        # active tracked storm anywhere) correctly showed a real nonzero
        # total on the tile via _global_flood_availability's own roster,
        # but clicking it recomputed an EMPTY roster here, producing a
        # `None` total and an empty popup for a real, nonzero, just-clicked
        # number. This is what that looked like live for PHL/BGD Rainfall.
        all_country_names, _, _ = _global_flood_availability(
            date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
        stats = _combined_stats(all_country_names, date=date, run=run, wind_kt=wind_kt, hz=hz)
        _people_pct = _combined_in_need_pct(all_country_names, "People at Risk", "people",
                                              _parse_stat_number(stats["People at Risk"]),
                                              date=date, run=run, wind_kt=wind_kt, hz=hz)
        _children_pct = _combined_in_need_pct(all_country_names, "Children at Risk", "children",
                                                _parse_stat_number(stats["Children at Risk"]),
                                                date=date, run=run, wind_kt=wind_kt, hz=hz)
        pin_pct = {
            "people": _people_pct,
            "children": _children_pct,
            # Real, un-derived absolute in-need totals, see
            # _pin_arc_charts_block_combined's own identical comment.
            "people_abs": (_combined_in_need_total(all_country_names, "People at Risk", "people",
                                                     date=date, run=run, wind_kt=wind_kt, hz=hz)
                            if _people_pct is not None else None),
            "children_abs": (_combined_in_need_total(all_country_names, "Children at Risk", "children",
                                                        date=date, run=run, wind_kt=wind_kt, hz=hz)
                               if _children_pct is not None else None),
        }
    else:
        stats = _get_country_stats(scope, date, run, wind_kt, hz=hz)
        pin_pct = _get_country_pin_pct(scope, date, run, wind_kt, hz=hz)
    # None pin_pct means genuinely no real in-need data for this scope (see
    # _fetch_real_combined_tile_totals_uncached's own comment): "N/A", not
    # a fabricated 0, matching the same convention as the breakdown table.
    #
    # Real, un-derived absolute in-need count, read directly instead of
    # reconstructing via at_risk * pct / 100, which would silently collapse
    # to In Need == At Risk once pct has already been clamped to 100% one
    # level down, see _fetch_real_combined_tile_totals_uncached's own
    # pin_pct comment for the full mechanism.
    if metric == "People in Need":
        return (_format_stat_number(pin_pct["people_abs"])
                if pin_pct.get("people_abs") is not None else _t("N/A"))
    if metric == "Children in Need":
        return (_format_stat_number(pin_pct["children_abs"])
                if pin_pct.get("children_abs") is not None else _t("N/A"))
    return stats.get(metric, "—")


def _resolve_curve_countries(scope, countries, date, run, river_idx=None, rain_idx=None, rain_window=None, river_window=None):
    """Same real country-list resolution _resolve_stat_value itself applies
    per scope, factored out so _hazard_curve_totals below can resolve the
    identical country set without going through _resolve_stat_value's own
    metric/stats machinery. "global" recomputes the real affected-country
    list via _global_flood_availability (storms UNION real river/rain-
    impact countries, ignores `countries`, same as _resolve_stat_value's
    own "global" branch, see that function's own comment for the real
    empty-popup bug this fixes); "combined" uses `countries` as-is;
    anything else is a single country name."""
    if scope == "global":
        all_country_names, _, _ = _global_flood_availability(
            date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
        return all_country_names
    if scope == "combined":
        return countries or []
    return [scope] if scope else []


# metric -> the single get_tile_impact_totals_by_threshold column it reads.
# "Children at Risk" isn't here, it sums 3 separate age-band columns, see
# _curve_metric_value below. People/Children in Need aren't here either,
# no stat-card ever actually fires with those metrics today (extra_stats,
# the only place that would wire them up, is never passed to _stat_grid,
# confirmed via grep), and get_tile_impact_totals_by_threshold has no
# in-need column to derive them from per-threshold anyway (in-need % comes
# from MERCATOR_TILE_VULNERABILITY_MAT, wind-only and not part of this
# totals query, see its own docstring). _hazard_curve_totals returns None
# for those, and callers fall back to the old _resolve_stat_value path.
_CURVE_METRIC_COL = {
    "People at Risk": "E_POPULATION", "Schools at Risk": "E_NUM_SCHOOLS",
    "Health Centers at Risk": "E_NUM_HCS", "Shelters at Risk": "E_NUM_SHELTERS",
    "WASH Facilities at Risk": "E_NUM_WASH",
}
_CURVE_CHILD_AGE_COLS = ("E_INFANT_POPULATION", "E_SCHOOL_AGE_POPULATION", "E_ADOLESCENT_POPULATION")


def _curve_metric_value(metric, totals_row):
    """One threshold tier's real value for `metric`, from a single
    get_tile_impact_totals_by_threshold(...)["wind"|"river"][tier] row.
    None when `totals_row` itself is None (this hazard has no real data at
    all for this country) or the underlying column is genuinely all-NULL,
    same "don't fabricate a confirmed zero" contract _row_to_impact_totals
    already documents. "Children at Risk" sums only the real (non-None) age
    bands, matching _fetch_real_combined_tile_totals_uncached's own
    `children = sum(v for v in age_population.values() if v is not None)`
    (an all-None country still nets a real 0 here, not None, same quirk,
    kept for consistency with that existing behavior)."""
    if totals_row is None:
        return None
    if metric == "Children at Risk":
        bands = [totals_row.get(c) for c in _CURVE_CHILD_AGE_COLS]
        return sum(v for v in bands if v is not None)
    col = _CURVE_METRIC_COL.get(metric)
    return totals_row.get(col) if col else None


def _hazard_curve_totals(metric, hazard, scope, countries, date, run, river_window=None,
                           river_idx=None, rain_idx=None, rain_window=None):
    """Avoids the per-threshold _resolve_stat_value -> get_tile_impacts
    fan-out (8 full per-tile row fetches for wind alone, 472,848 rows
    transferred where one GROUP BY aggregate returns 8), ONE
    get_tile_impact_totals_by_threshold call per
    country (not per country PER THRESHOLD), summed across whichever
    countries this `scope` resolves to.

    `hazard` is "wind" or "river" (matches get_tile_impact_totals_by_
    threshold's own two 1D threshold-swept sections). Precip's own real
    per-threshold totals are also real (see get_tile_impact_totals_by_
    threshold's own "precip" section) but genuinely 2D (threshold_mm x
    window_h), its own dedicated aggregator is _precip_curve_totals below,
    not this function. Storm Surge has no real per-threshold backend at
    all, see _hazard_curve_row's own unchanged illustrative branch for that.

    Returns a list of real ints aligned to `_WIND_CATS`'s own kt order
    (hazard="wind") or `_RIVER_RP_TIERS`'s order (hazard="river"), a
    country/tier with no real contribution is simply excluded from that
    tier's sum (same "sum only real contributors" policy _combined_stats
    itself already uses), never fabricated as 0 unless every country is
    genuinely absent, in which case the tier legitimately sums to 0.

    Returns None when `metric` isn't one of the five real stat-card metrics
    this fast path covers (see _CURVE_METRIC_COL) or `scope` resolves to no
    real countries at all, callers fall back to the old per-threshold path.
    """
    if metric != "Children at Risk" and metric not in _CURVE_METRIC_COL:
        return None
    resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
    if not resolved_countries:
        return None

    def _fetch(country):
        code = _NAME_TO_CODE.get(country)
        if not code:
            return None
        # Wind needs a real storm to scope MERCATOR_TILE_IMPACT_MAT to,
        # no storm for this country/date means no real wind curve data at
        # all (matches _fetch_real_combined_tile_totals_uncached's own
        # `if hz["wind_on"] and storm_info` gate). River's own section
        # inside get_tile_impact_totals_by_threshold ignores storm/
        # forecast_date entirely (see its own docstring), resolves its
        # own forecast time independently (now via the real `date`/`run`
        # passed below, not "whatever's latest" -- see that function's own
        # docstring for the real headline-vs-grid mismatch bug this fixes),
        # so a missing storm_info still lets river's own section return
        # real data; "" placeholders here only affect the (harmless,
        # river-irrelevant) ttl_cache key.
        storm_info = _resolve_storm_for_country(country, date, run)
        if hazard == "wind" and storm_info is None:
            return None
        storm_name = storm_info["name"] if storm_info else ""
        mat_date = storm_info["mat_forecast_date"] if storm_info else ""
        return get_tile_impact_totals_by_threshold(code, storm_name, mat_date,
                                                     river_window=int(river_window) if river_window else _RIVER_WINDOW_DEFAULT,
                                                     date=date, run=run)

    per_country = list(get_query_executor().map(_fetch, resolved_countries))
    tiers = [wc[2] for wc in _WIND_CATS] if hazard == "wind" else _RIVER_RP_TIERS
    values = []
    for tier in tiers:
        total = 0
        for totals in per_country:
            if not totals or not totals.get(hazard):
                continue
            v = _curve_metric_value(metric, totals[hazard].get(tier))
            if v is not None:
                total += v
        values.append(total)
    return values


def _precip_curve_totals(metric, scope, countries, date, run, river_idx=None, rain_idx=None, rain_window=None, river_window=None):
    """Precip sibling of _hazard_curve_totals. Genuinely 2D (unlike
    wind/river's single threshold dimension), so
    it returns the FULL grid rather than one tier list, matching how
    _rain_threshold_grid itself always renders all 4 windows at once (not
    just the currently selected one), the "which cell is ringed as current"
    concern is the caller's, not this aggregator's.

    Returns {"6": [v25, v50, v75], "24": [...], "72": [...], "120": [...]}
    (values aligned to _RAIN_MM_BY_WINDOW[window]'s own order), or None when
    `scope` resolves to no real countries at all, caller falls back to the
    illustrative grid.

    Was previously restricted to "People at Risk" only, on the belief that
    "MERCATOR_TILE_PRECIP_MAT has NO real age-band or facility-count
    columns at all" -- confirmed live this was WRONG, a stale claim that
    had drifted out of sync with the actual code: _TOTALS_PRECIP_IMPACT_
    COLS (snowflake_utils.py) is `_TOTALS_IMPACT_COLS` again (the SAME full
    8-column set wind/river use), and get_tile_impact_totals_by_threshold's
    own precip section SELECTs and returns all 8 real columns already, not
    just E_POPULATION. Verified directly against Snowflake before lifting
    this restriction: Nicaragua's real MERCATOR_TILE_PRECIP_MAT has
    genuine non-null, nonzero E_NUM_SHELTERS/E_NUM_SCHOOLS/E_NUM_HCS/
    E_NUM_WASH AND E_INFANT_POPULATION/E_SCHOOL_AGE_POPULATION/
    E_ADOLESCENT_POPULATION across every real window/threshold combination
    (e.g. 72h/45mm: 47.2 real shelters, 966 real schools, 36,605 real
    infants -- 18,167 of 23,360 real tiles have real non-null age-band
    data). Every metric `_curve_metric_value` supports (including
    "Children at Risk", its own real "sum only real components" 3-age-
    band handling) is therefore genuinely real for precip too, same as
    wind/gust/river, not a metric-specific gap.
    """
    resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
    if not resolved_countries:
        return None

    def _fetch(country):
        code = _NAME_TO_CODE.get(country)
        if not code:
            return None
        # Precip is NOT storm-scoped (see get_tile_impact_totals_by_
        # threshold's own docstring), storm/forecast_date are irrelevant to
        # its own section, only affecting the (harmless) ttl_cache key.
        # `date`/`run` ARE real and required though (a separate axis from
        # storm/forecast_date): without them this silently fell back to
        # the country's absolute LATEST rain cycle regardless of what the
        # user actually selected -- a real bug (headline
        # "Total" built from the SELECTED cycle disagreeing with this
        # grid, built from a DIFFERENT, possibly still-mid-processing
        # cycle; e.g. Nicaragua's absolute-latest cycle had zero real rows
        # for the 120h window while the selected cycle's 120h window was
        # complete, making the grid's whole 120h column look like an
        # impossible "longer window has less accumulation" violation when
        # it was really just two different real snapshots being compared).
        return get_tile_impact_totals_by_threshold(code, "", "", date=date, run=run)

    per_country = list(get_query_executor().map(_fetch, resolved_countries))
    grid = {}
    for window, mm_tiers in _RAIN_MM_BY_WINDOW.items():
        values = []
        for mm in mm_tiers:
            total = 0
            for totals in per_country:
                if not totals or not totals.get("precip") or window not in totals["precip"]:
                    continue
                v = _curve_metric_value(metric, totals["precip"][window].get(mm))
                if v is not None:
                    total += v
            values.append(total)
        grid[window] = values
    return grid


def _country_river_rain_forecast_dates(country, date, run, rp_tier=None):
    """Real (river_forecast_time, rain_forecast_time) for `country` at the
    ACTUAL selected topbar `date`/`run`, the same resolution
    _fetch_real_combined_tile_totals_uncached's own river_forecast_time/
    rain_forecast_time locals use (get_river_extent_forecast_time_for_date/
    get_precip_forecast_time_near, NOT get_latest_river_forecast_time/
    get_latest_rain_forecast_time, which ignore `date`/`run` entirely and
    always return the country's all-time-latest cycle regardless of which
    historical date is actually selected -- a real bug this function used
    to have too, see _fetch_real_combined_tile_totals_uncached's own
    comment for the full "why"), factored out so _river_curve_totals_excl_
    rain/_rain_grid_totals_excl_river can hit the NEW batched sweep
    endpoints (one real HTTP round trip per country instead of one per
    tier/cell, see those functions' own "why" for the performance problem
    this fixes) without going through _fetch_real_combined_tile_totals_
    uncached's own wind/gust-specific machinery. Returns (None, None) when
    `country` doesn't resolve to a real code or `date` is missing."""
    code = _NAME_TO_CODE.get(country)
    if not code or not date:
        return None, None
    river_resolved = get_river_extent_forecast_time_for_date(date, rp_tier or "rp10")
    river_ft = _mat_forecast_date(date, '00') if river_resolved else None
    rain_resolved = get_precip_forecast_time_near(date, run) if run is not None else None
    rain_ft = _mat_forecast_date(date, run) if rain_resolved else None
    return river_ft, rain_ft


def _sum_flood_splits(flood_splits, metric):
    """Sums a list of raw flood_split dicts (river_only/rain_only/both
    buckets, e.g. one per contributing country) into one
    _sum_flood_split_metric-shaped {"river_only": v, "rain_only": v,
    "both": v} for `metric`, same real "only None when every contributor
    is genuinely None" rule _combined_flood_split itself already uses,
    applied here across a list already fetched via the batched sweep
    endpoints rather than via _combined_flood_split's own per-country
    Snowflake fan-out."""
    buckets = ("river_only", "rain_only", "both")
    out = {b: None for b in buckets}
    for fs in flood_splits:
        if not fs:
            continue
        ms = _sum_flood_split_metric(fs, metric)
        if ms is None:
            continue
        for b in buckets:
            v = ms.get(b)
            if v is None:
                continue
            out[b] = (out[b] or 0.0) + v
    return out


def _river_curve_totals_excl_rain(metric, scope, countries, date, run, rain_idx, rain_window, river_window, river_idx=None):
    """River-only-excluding-Rain-overlap counterpart to _hazard_curve_totals'
    own river branch, for the case where River Flooding AND Rainfall are
    BOTH active (is_real_river_rain in _hazard_contribution_content).

    _hazard_curve_totals' river branch is a MARGINAL per-tier total
    (river's own real exposure at that tier, regardless of whether Rain
    also hits the same tiles), which is a genuinely different question
    from the real WITHIN-Flood headline this popup now shows
    (river_only_n, river hits AND Rain does NOT, see flood_split_real's
    own comment) - the two are related (river_only + both ≈ the marginal
    total, small real-vs-real margin expected) but not the same number,
    so pairing the marginal curve under the joint-decomposed headline
    read as broken even though both numbers are real. This function
    fixes that by computing the SAME joint decomposition at EVERY River
    tier (not just the currently-selected one), holding Rain's own
    current threshold fixed, so every point on this curve reconciles with
    the headline by construction.

    ONE real HTTP round trip PER COUNTRY (via the tile server's own
    /impact/river-curve-excl-rain sweep endpoint, which loops all 6 RP
    tiers server-side), not 6 -- an earlier version of this function did
    6 separate _combined_flood_split calls (one per tier, parallelized
    client-side), which meant 6 real Snowflake-backed bitmask
    decompositions PLUS 6 HTTP round trips serialized through the tile
    server's single uvicorn process every time this popup opened,
    genuinely slow (the popup would sit "loading" for minutes on a cold
    cache). The sweep endpoint does the same 6 decompositions but inside
    ONE request, so tiers 2-6 reuse whatever tier 1 already warmed
    instead of racing 6 concurrent external requests against each other.

    Returns a list of 6 real values aligned to _RIVER_RP_TIERS' own
    order, or None when `metric` has no real flood_split coverage at all
    or no country resolves to real data."""
    resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
    if not resolved_countries:
        return None
    rain_mm = _RAIN_MM_BY_WINDOW[rain_window][rain_idx] if rain_idx is not None else _RAIN_MM_BY_WINDOW[rain_window][1]
    river_window_resolved = int(river_window) if river_window else _RIVER_WINDOW_DEFAULT

    def _fetch(country):
        code = _NAME_TO_CODE.get(country)
        river_ft, rain_ft = _country_river_rain_forecast_dates(country, date, run)
        if not code or not river_ft or not rain_ft:
            return None
        try:
            resp = requests.get(
                f"{config.TILE_SERVER_URL}/impact/river-curve-excl-rain/{code}/NONE",
                params={"river_forecast_date": river_ft, "river_window": river_window_resolved,
                         "rain_forecast_date": rain_ft, "threshold_mm": rain_mm, "window_h": int(rain_window)},
                timeout=_MEMBER_IMPACT_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning("Could not load river curve (excl rain) for %s: %s", country, e)
            return None

    per_country = list(get_query_executor().map(_fetch, resolved_countries))
    per_country = [r for r in per_country if r is not None]
    if not per_country:
        return None
    n_tiers = len(_RIVER_RP_TIERS)
    values = []
    any_real = False
    for i in range(n_tiers):
        flood_splits_at_tier = [r["flood_splits"][i] for r in per_country if i < len(r.get("flood_splits") or [])]
        summed = _sum_flood_splits(flood_splits_at_tier, metric)
        v = summed.get("river_only")
        if v is not None:
            any_real = True
        values.append(v or 0.0)
    return values if any_real else None


def _rain_grid_totals_excl_river(metric, scope, countries, date, run, river_idx, river_window, rain_idx=None, rain_window=None):
    """Rain-only-excluding-River-overlap counterpart to _precip_curve_totals,
    same real joint-decomposition fix as _river_curve_totals_excl_rain
    above, applied to the OTHER member: every cell of the window x
    depth-tier grid recomputed as Rain hits AND River does NOT, holding
    River's own current threshold fixed, so the grid's own "current" cell
    reconciles with the "Rainfall" headline row by construction, instead
    of showing Rain's raw marginal exposure (which, like the river curve,
    is a real but differently-scoped number, see that function's own
    comment for the full "why").

    ONE real HTTP round trip PER COUNTRY (via /impact/rain-grid-excl-river,
    which sweeps all 12 window x depth-tier cells server-side), same fix
    as _river_curve_totals_excl_rain's own comment -- an earlier version
    made 12 separate round trips, each potentially triggering its own
    real per-member precip Zarr download race across windows, genuinely
    slow enough that the popup could sit "loading" for minutes.

    Returns {"6": [v1,v2,v3], "24": [...], "72": [...], "120": [...]}
    (values aligned to _RAIN_MM_BY_WINDOW[window]'s own order), or None
    when no country resolves to real data.

    Like _precip_curve_totals (see its own docstring), this is NOT
    restricted to "People at Risk" -- both functions cover every metric in
    _DEFAULT_STATS. This function never touches MERCATOR_TILE_PRECIP_MAT
    directly, it goes through combined_country_totals' own
    _COUNTRY_TOTALS_RAW_COLS (population, 3 age bands, built-up area, 4
    facility counts, the SAME raw columns the headline row already
    correctly sums for any metric via _sum_flood_split_metric), so the
    same real per-tile weighting works for Children/Schools/Health
    Centers/Shelters/WASH at Risk exactly as it does for People at Risk.
    An earlier version of THIS function copied _precip_curve_totals' own
    then-restriction (based on a since-corrected stale claim that
    MERCATOR_TILE_PRECIP_MAT has no real age-band/facility columns at
    all): that was a real bug, silently falling back to the OLD
    illustrative grid for every metric except "People at Risk" and
    reproducing the exact headline-vs-grid mismatch this whole fix exists
    to eliminate, just for a different subset of metrics. Fixed here
    first; _precip_curve_totals' own restriction was later found to rest
    on the same wrong premise and removed too, so the two functions'
    metric coverage is consistent again."""
    resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
    if not resolved_countries:
        return None
    cells = [(window, mm) for window, mm_tiers in _RAIN_MM_BY_WINDOW.items() for mm in mm_tiers]
    cells_json = json.dumps(cells)
    river_tier = _RIVER_RP_TIERS[river_idx] if river_idx is not None else _RIVER_RP_TIERS[2]
    river_window_resolved = int(river_window) if river_window else _RIVER_WINDOW_DEFAULT

    def _fetch(country):
        code = _NAME_TO_CODE.get(country)
        river_ft, rain_ft = _country_river_rain_forecast_dates(country, date, run, rp_tier=river_tier)
        if not code or not river_ft or not rain_ft:
            return None
        try:
            resp = requests.get(
                f"{config.TILE_SERVER_URL}/impact/rain-grid-excl-river/{code}/NONE",
                params={"river_forecast_date": river_ft, "rp_tier": river_tier, "river_window": river_window_resolved,
                         "rain_forecast_date": rain_ft, "cells": cells_json},
                timeout=_MEMBER_IMPACT_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning("Could not load rain grid (excl river) for %s: %s", country, e)
            return None

    per_country = list(get_query_executor().map(_fetch, resolved_countries))
    per_country = [r for r in per_country if r is not None]
    if not per_country:
        return None
    grid = {}
    any_real = False
    for cell_i, (window, mm) in enumerate(cells):
        flood_splits_at_cell = [r["flood_splits"][cell_i] for r in per_country if cell_i < len(r.get("flood_splits") or [])]
        summed = _sum_flood_splits(flood_splits_at_cell, metric)
        v = summed.get("rain_only")
        if v is not None:
            any_real = True
        grid.setdefault(window, []).append(v or 0.0)
    if not any_real:
        return None
    return grid


def _hazard_overlap_bar(total, breakdown):
    # Segmented bar (Tropical Cyclone only / Both / Flood only), not a
    # separate "Overlap: X%" line floating apart from the two hazard
    # numbers, this way the bar's own width still adds up to the visible
    # TC/Flood union, instead of implying TC and Flood are simply additive
    # to separate people. The "Both" segment is striped (not a flat third
    # color) so it visibly reads as "shared", not a third distinct hazard
    # family. Same precomputed shares (from _hazard_breakdown, based on
    # exactly which hazards are toggled on) the table's own
    # _hazard_split_line uses, one source of truth for both. Only ever
    # called when both families are active (see _hazard_contribution_content),
    # so union_pct is always > 0 here.
    tc_only_pct, both_pct, flood_only_pct = breakdown["tc_only_pct"], breakdown["both_pct"], breakdown["flood_only_pct"]
    union_pct = tc_only_pct + both_pct + flood_only_pct  # == breakdown["tc_pct"] + breakdown["flood_pct"] - both_pct

    def _segment(pct, striped, color):
        style = {"width": f"{pct / union_pct * 100:.2f}%", "height": "100%"}
        if striped:
            style["backgroundImage"] = f"repeating-linear-gradient(45deg, {WIND}, {WIND} 4px, {RIVER} 4px, {RIVER} 8px)"
        else:
            style["background"] = color
        return html.Div(style=style)

    bar = html.Div([
        _segment(tc_only_pct, False, WIND),
        _segment(both_pct, True, None),
        _segment(flood_only_pct, False, RIVER),
    ], style={"display": "flex", "height": "16px", "borderRadius": "6px", "overflow": "hidden", "width": "100%"})

    def _legend_item(label, n, striped, color):
        swatch_style = {"width": "10px", "height": "10px", "borderRadius": "3px"}
        if striped:
            swatch_style["backgroundImage"] = f"repeating-linear-gradient(45deg, {WIND}, {WIND} 2px, {RIVER} 2px, {RIVER} 4px)"
        else:
            swatch_style["background"] = color
        return dmc.Group([
            html.Span(style=swatch_style),
            dmc.Text(_t(label), size="10px", c="dimmed"),
            dmc.Text(_format_stat_number(total * n / 100), size="10px", fw=700, ff="monospace"),
        ], gap=5, wrap="nowrap")

    legend = dmc.Group([
        _legend_item("Tropical Cyclone only", tc_only_pct, False, WIND),
        _legend_item("Both", both_pct, True, None),
        _legend_item("Flood only", flood_only_pct, False, RIVER),
    ], gap=14, mt=8, wrap="wrap")

    return html.Div([bar, legend])


# Illustrative, within a multi-member family (Flood: River Flooding/
# Rainfall/Storm Surge), what fraction of the family's own share is people
# facing 2+ of those members at once, not just one. Distinct from
# _HAZARD_OVERLAP_FRAC (which is about TC-vs-Flood overlap, a different
# relationship), a real implementation would derive both from actual
# exposure-geometry overlap, not flat constants.
_HAZARD_MULTI_FRAC = 0.20
# Of that "2+ at once" share, how much is ALL of the family's members at
# once (rarer) vs. any 2 of them (more common), e.g. facing river
# flooding, rainfall AND storm surge simultaneously vs. just two of the
# three.
_HAZARD_TRIPLE_FRAC = 0.3


def _hazard_contribution_content(value, breakdown, hazard_idx=None, rain_window=None, river_window=None, is_global=False,
                                    metric=None, scope=None, countries=None, date=None, run=None, hz=None):
    # Grouped under Tropical Cyclone/Flood (icon + bold, own subtotal) with
    # each family's actual hazards indented underneath (icon + lighter),
    # a flat list of 4 colored dots didn't make clear that Sustained Wind
    # and Gust are the SAME family, or give any per-hazard icon at all.
    #
    # `breakdown` (from _hazard_breakdown) reflects EXACTLY which individual
    # hazard checkboxes are on. A family with nothing toggled on is dropped
    # from this popup entirely (its group row + members), not shown at 0%,
    # since there's nothing being modeled for it right now. Within Flood,
    # only the individually-checked members (River Flooding/Rainfall/Storm
    # Surge) appear, toggling on just Rainfall shows only Rainfall, not
    # all three as if the whole family were active. When only one family
    # is active there's no TC/Flood overlap possible either, so the top
    # overlap bar/caption only appears when BOTH are active.
    tc_active, flood_active = breakdown["tc_active"], breakdown["flood_active"]
    total = _parse_stat_number(value)
    if not tc_active and not flood_active:
        return html.Div(dmc.Text(_t("None — toggle a hazard on the map to see impact numbers."),
                                    size="sm", c="dimmed", fs="italic"))
    # Global scope's tc_active/flood_active now reflect REAL data
    # availability for the resolved date (see _open_hazard_contribution's
    # own comment, river_avail/rain_avail come from a real Snowflake
    # check, not a blind "always on"), so "toggle a hazard" above CAN still
    # fire for Global (e.g. wind's own real total is 0 for some other
    # reason while flood genuinely has no data either). A genuinely zero
    # total (Global OR a single country/combined) is a DIFFERENT, more
    # specific fact than "nothing toggled on" and needs its own message,
    # BOTH variants spell out that a zero result is scoped to the
    # CURRENTLY SELECTED hazard configuration (wind/gust severity, river
    # return-period tier, rain window+threshold), not "this
    # location/storm has no real impact at any severity ever." A genuine,
    # real zero at one specific threshold is expected/correct behavior
    # (e.g. a wind category a storm's real forecast never reached, see
    # _get_country_stats's own docstring), so seeing "0" here for a real,
    # active storm should read as "not at this threshold," not as a
    # suspicious-looking blank result.
    #
    # total is None (not 0) whenever this metric/scope has no real data at
    # all, either a genuine per-country column gap (e.g. Turks and Caicos
    # Islands' real all-NULL shelters) or, in Global scope, simply zero
    # countries reaching the current configuration, see _combined_stats's
    # own comment for the full None-vs-0 contract that decides what the
    # TILE itself shows. The message here is deliberately the SAME for
    # both None and a real 0 (keep the 0-vs-N/A distinction
    # on the tile, but not in this explanation), either way, the honest
    # takeaway for a reader is identical: nothing shows up for this
    # metric/configuration right now, and that's not the same as "there's
    # no real impact, ever."
    if total is None or total == 0:
        if is_global:
            return html.Div(dmc.Text(
                _t("None of the initialized countries show impact at the currently selected hazard "
                    "configuration (severity threshold/tier). This does not mean there is no real impact "
                    "overall — a different threshold may show real impact, and potentially affected "
                    "countries may not yet be in the database."),
                size="sm", c="dimmed", fs="italic"))
        return html.Div(dmc.Text(
            _t("No impact at the currently selected hazard configuration (severity threshold/tier) for "
                "this selection. This does not mean there is no real impact overall — a different "
                "threshold may show real impact."),
            size="sm", c="dimmed", fs="italic"))
    if tc_active != flood_active:
        # When only ONE family genuinely has real data behind this total
        # (e.g. only Sustained Wind is checked, no River/Rain/Surge at
        # all), that family IS 100% of the real total, not the fixed
        # illustrative share (e.g. Wind's own "45%"), which describes a
        # fraction of an assumed-everything-active baseline that doesn't
        # apply once this total is a real, not-illustrative number.
        #
        # This override applies universally, in both Global and Country
        # Analysis, one active family is unambiguously 100% of the real
        # total regardless of scope. The illustrative split is only
        # meaningful when BOTH families really are active simultaneously
        # (the `else` case below, left untouched); when only ONE family is
        # checked there is no overlap to be illustrative ABOUT.
        breakdown = {**breakdown, "tc_pct": 100 if tc_active else 0,
                      "flood_pct": 100 if flood_active else 0}
    # When BOTH families are
    # genuinely active, this must NOT replace the fixed illustrative
    # 45/20/10/10 split with an independence-formula estimate
    # (`both = min(wind*flood/total_pop, wind, flood)`), that carries a
    # real, unquantified overstatement/understatement risk for correlated
    # same-storm hazards, the same class of error avoided elsewhere in
    # this codebase (_combine_bitmask_aware, combined_country_totals,
    # _paint_classification_tile). Uses the
    # REAL per-member joint bitmask check instead
    # (services/tile_server.py's combined_country_totals own family_split:
    # `both = popcount(tc_bits & flood_bits)/51`, a genuine "does the SAME
    # real ensemble member get hit by both families" fraction, not two
    # marginal totals multiplied together), no independence assumption,
    # no approximation, the same real methodology the raster/facility/
    # tooltip/Impact-Summary paths already use.
    #
    # `total` (used by every _hazard_row/_hazard_curve_row/_hazard_overlap_
    # bar call below via Python's own late-binding closures, and passed
    # explicitly to _hazard_overlap_bar) is reassigned here to this real
    # split's own total, for Country Analysis scope ONLY (single country):
    # the per-family blocks (_hazard_row's own `total * pct / 100`
    # reconstruction) and the pct values themselves both come from THIS
    # SAME family-split query, so reassigning keeps them internally
    # consistent with each other, deliberately NOT the same number as the
    # "Total: {value}" headline text above (which reads `value`/`total` as
    # they were BEFORE this reassignment, captured already in that
    # dmc.Text call). The two can still differ slightly on the margin
    # (this breakdown's own family-split query and the headline's own
    # union query are two separate real Snowflake round trips, not
    # guaranteed byte-identical), but both now use the SAME real bitmask
    # methodology, no more a real-union headline paired with an
    # independence-formula breakdown underneath it.
    #
    # Global scope deliberately does NOT reassign `total`: a real bug (74
    # vs 76 Shelters-at-Risk, found by precise per-country verification)
    # traced to exactly this reassignment when `value` is itself a SUM
    # ACROSS MULTIPLE COUNTRIES, each individually ceil'd first
    # (_get_country_stats -> _format_stat_number per country, matching the
    # By Country rows below and the tile's own displayed total, all three
    # "ceil each country, then sum" by construction) -- while `real_total`
    # here sums the SAME countries' raw un-ceiled floats FIRST and ceils
    # ONCE at the end ("sum, then ceil"). Summing N independently-ceiled
    # fractional values is mathematically guaranteed >= ceiling the raw
    # sum whenever any country carries a nonzero fractional remainder, so
    # the headline (if reassigned) and the By Country sum/tile total
    # (never reassigned) provably diverge on real multi-country data, not
    # just "slightly on the margin" -- this is deterministic, not
    # ensemble noise. Global scope's `blocks` (which DO consume the
    # reassigned total via `total * pct / 100`) are computed either way
    # but never rendered (see this function's own final return), so
    # skipping the reassignment for Global has no other effect beyond
    # fixing the headline/overlap-bar-legend numbers to agree with By
    # Country and the tile, which is the correct convention per this
    # project's own "apply ceil once, at the true final-display step"
    # rule -- the true final-display step for a per-country aggregate IS
    # each country's own ceil, not a second ceil of the raw cross-country
    # sum.
    real_estimate = False
    if tc_active and flood_active and metric is not None and scope is not None:
        resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=(hazard_idx or {}).get("River Flooding"), rain_idx=(hazard_idx or {}).get("Rainfall"), rain_window=rain_window, river_window=river_window)
        combined_hz = _build_hz(
            True, False,
            "River Flooding" in breakdown["active_flood_members"],
            "Rainfall" in breakdown["active_flood_members"],
            wind_idx=(hazard_idx or {}).get("Sustained Wind"),
            river_idx=(hazard_idx or {}).get("River Flooding"),
            rain_idx=(hazard_idx or {}).get("Rainfall"),
            rain_window=rain_window,
            river_window=river_window,
        )
        family_split = _combined_family_split(resolved_countries, date=date, run=run, hz=combined_hz)
        metric_split = _sum_family_split_metric(family_split, metric) if family_split else None
        if metric_split is not None:
            tc_only_n = metric_split.get("tc_only") or 0.0
            flood_only_n = metric_split.get("flood_only") or 0.0
            both_n = metric_split.get("both") or 0.0
            real_total = tc_only_n + both_n + flood_only_n
            if real_total > 0:
                real_estimate = True
                if not is_global:
                    total = real_total
                breakdown = {**breakdown,
                    "tc_pct": (tc_only_n + both_n) / real_total * 100,
                    "flood_pct": (flood_only_n + both_n) / real_total * 100,
                    "both_pct": both_n / real_total * 100,
                    "tc_only_pct": tc_only_n / real_total * 100,
                    "flood_only_pct": flood_only_n / real_total * 100,
                }
    # Both families are genuinely active and `total` (checked at the top of
    # this function) is a real, nonzero number, but the real per-member
    # joint bitmask split above couldn't resolve one (missing metric/scope
    # context, or the real query itself returned no data). This must NOT
    # fall back to the flat illustrative 45/20/10/10-derived percentages to
    # manufacture a plausible-looking TC/Flood breakdown of a real total,
    # same "no illustrative numbers ever" rule as _rain_threshold_grid's
    # own real_matrix requirement
    # above. Bails out with the real total and an honest explanation
    # instead of the per-family breakdown, rather than trying to keep the
    # rest of this function's rendering (family blocks/overlap bar/curves,
    # all of which assume a real or intentionally-100% breakdown) working
    # against a breakdown this function has no real data to justify.
    if tc_active and flood_active and not real_estimate:
        return html.Div([
            dmc.Text(_t("Total: {value}", value=value), size="sm", fw=700, mt=6, mb=10),
            dmc.Text(_t("Real per-hazard split unavailable for this selection — Tropical Cyclone and "
                        "Flood contributions cannot be separated right now."),
                       size="sm", c="dimmed", fs="italic"),
        ])

    # Real WITHIN-Flood split (River Flooding vs Rainfall vs both), same
    # real per-tile bitmask methodology as real_estimate above, one level
    # deeper. Independent of tc_active/real_estimate (Flood need not be
    # sharing this popup with Tropical Cyclone at all for its own two
    # members to be split correctly), fires whenever River Flooding AND
    # Rainfall are the ONLY two active Flood members. A 3-member Flood
    # selection (River+Rain+Storm Surge) deliberately does NOT use this:
    # Storm Surge has no real backend (see ms-surge-on's own comment), so
    # mixing 2 real numbers with 1 illustrative one under one bar would be
    # its own new kind of misleading, that case keeps the old flat
    # illustrative split entirely, same as before this fix.
    river_rain_only = breakdown["active_flood_members"] == ["River Flooding", "Rainfall"]
    flood_split_real = False
    river_only_n = rain_only_n = flood_both_n = 0.0
    if river_rain_only and metric is not None and scope is not None:
        flood_resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=(hazard_idx or {}).get("River Flooding"), rain_idx=(hazard_idx or {}).get("Rainfall"), rain_window=rain_window, river_window=river_window)
        flood_hz = _build_hz(
            False, False, True, True,
            river_idx=(hazard_idx or {}).get("River Flooding"),
            rain_idx=(hazard_idx or {}).get("Rainfall"),
            rain_window=rain_window,
            river_window=river_window,
        )
        flood_split = _combined_flood_split(flood_resolved_countries, date=date, run=run, hz=flood_hz)
        flood_metric_split = _sum_flood_split_metric(flood_split, metric) if flood_split else None
        if flood_metric_split is not None:
            river_only_n = flood_metric_split.get("river_only") or 0.0
            rain_only_n = flood_metric_split.get("rain_only") or 0.0
            flood_both_n = flood_metric_split.get("both") or 0.0
            flood_split_real = (river_only_n + rain_only_n + flood_both_n) > 0

    by_name = {name: (color, pct, icon) for name, color, pct, icon in _HAZARD_CONTRIBUTION}
    hazard_idx = hazard_idx or {}

    def _hazard_row(name, color, pct, icon, indent=False, mb=None, abs_value=None):
        # mb defaults to "there's a curve chart right below this row inside
        # the same card" (6/10px breathing room before it). Pass mb=0
        # explicitly when this row is the ONLY thing in its card (the
        # overlap rows below never have a curve), otherwise that default
        # bottom margin adds to the card's own bottom padding with nothing
        # above to balance it, so the row visually sits closer to the
        # card's top edge than its bottom one instead of centered.
        if mb is None:
            mb = 6 if indent else 10
        # abs_value: the real, un-derived absolute count, when the caller
        # already has one (river_only_n/rain_only_n/flood_both_n, see
        # is_real_river_rain's own comment in _family_members_block). Must
        # NOT be reconstructed as `total * pct / 100` in that case: `total`
        # itself comes from _parse_stat_number(value), a re-parse of the
        # stat card's OWN ALREADY-ROUNDED display string (e.g. "2K" ->
        # exactly 2000, discarding that the real value could be anywhere
        # in ~1500-2499). That round-trip error is invisible for large
        # populations but for tiny counts (Schools/HCs/Shelters/WASH,
        # often single digits) multiplying an already up-to-~25%-off total
        # by a small real ratio and re-ceiling can flip a real 0.6 into a
        # displayed "0" and a real 0.4 into "1" for a sibling row, making
        # two real numbers look mutually contradictory (e.g. River Flooding
        # showing 0 while Both, which River's own total must be at least
        # as large as, shows 1). Passing the real float straight through
        # keeps the ONE real ceil at this true final display step, per
        # this project's own established convention, instead of ceiling a
        # value already corrupted by an earlier display-string round-trip.
        display_n = abs_value if abs_value is not None else (total * pct / 100)
        return dmc.Group([
            DashIconify(icon=icon, width=15 if not indent else 13, color=color),
            dmc.Text(_t(name), size="xs" if not indent else "11px", fw=700 if not indent else 400,
                      c="dark" if not indent else "dimmed", style={"flex": 1, "minWidth": 0}),
            # w=42/72 + explicit whiteSpace:nowrap, narrower fixed widths sized
            # for a 2-3 digit "%"/short "K" number visually squish
            # together (barely any gap between them) once real numbers run
            # bigger (e.g. "273K"), since neither box has room to spare.
            # round() (display-only, not the impact-count ceil convention,
            # pct can now be a raw float, e.g. both_pct/tc_only_pct, see
            # _hazard_breakdown's own comment), the absolute count just
            # below still gets its one real ceil inside _format_stat_number.
            dmc.Text(f"{round(pct)}%", size="xs", c="dimmed", w=42, ta="right", style={"whiteSpace": "nowrap", "flexShrink": 0}),
            dmc.Text(_format_stat_number(display_n), size="xs", fw=700, ff="monospace",
                      w=72, ta="right", style={"whiteSpace": "nowrap", "flexShrink": 0}),
        ], gap=14, wrap="nowrap", mb=mb)

    def _hazard_card(color, children):
        # Tinted-border card around each individual sub-hazard's own row +
        # curve (and each overlap row), a flat stack of rows directly
        # against each other (the old layout) read as squished/hard to tell
        # apart at a glance, especially once a curve/heatmap sits between
        # each one. marginLeft (not the row's own paddingLeft) now supplies
        # the "nested under the family header" indent, so this card's own
        # border doesn't awkwardly start flush with the bold group row above.
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        return html.Div(children, style={
            "border": f"1px solid rgba({r},{g},{b},0.25)", "background": f"rgba({r},{g},{b},0.05)",
            "borderRadius": "10px", "padding": "8px 12px 8px", "marginLeft": "18px", "marginBottom": "8px",
        })

    # Populated by _hazard_curve_row below as a side effect, one real
    # True/False per member name that had a genuine per-tier query run
    # (Sustained Wind/River Flooding/Rainfall-with-a-window), consumed by
    # the family loop after it to decide whether to hide a toggled-on
    # family whose own real total is a genuine 0 at EVERY severity tier,
    # not just the currently selected one (see that loop's own comment for
    # the full "why"). A name never gets a True/False entry at all when its
    # curve is purely illustrative (Storm Surge, or Rainfall with no live
    # rain_window/metric/scope context, e.g. the print page). Those cases
    # have no independent real signal to check, so the family loop must
    # treat a MISSING entry as "unknown, don't hide", never as zero.
    _curve_all_zero_by_member = {}

    def _hazard_curve_row(name, color, pct):
        # None for Global scope, unconditionally: the overlap bar above
        # already covers the TC-vs-Flood split, so the per-hazard
        # breakdown share is redundant in the global view tile pop-up.
        # Wind/River/Rainfall per-threshold curves are all genuinely real
        # for every metric (Rainfall's own _precip_curve_totals no longer
        # restricts itself to "People at Risk" either, see that function's
        # own docstring), but showing 3+ full curve/grid cards per country
        # aggregate added visual noise the popup's own new By Country
        # section (below) already covers more usefully -- one real,
        # ranked, per-country breakdown for whatever metric is selected,
        # instead of several per-hazard-per-tier charts. Country Analysis
        # scope is untouched (is_global is False there), that popup has no
        # By Country section to substitute, so its own curves stay real
        # and visible for every metric.
        if is_global:
            return None
        # None if this hazard has no curve data or no live slider index was
        # passed in (e.g. the print page, which doesn't read slider state at
        # all).
        idx = hazard_idx.get(name)
        curve_data = _HAZARD_CURVE_DATA.get(name)
        if idx is None or curve_data is None:
            return None
        labels, factors = curve_data
        base_n = math.ceil(total * pct / 100)
        # Real per-tier query (Sustained Wind/River Flooding): re-resolves
        # the SAME real metric this popup is already showing (metric/scope/
        # countries/date/run, threaded from _open_hazard_contribution) at
        # EVERY threshold tier, isolating just this ONE hazard (via
        # _wind_only_hz/_river_only_hz, ignores whatever else is currently
        # toggled on, so this is always "this hazard's own real exposure at
        # each of its tiers", not a blend with other active hazards).
        #
        # This curve must NOT be entirely
        # illustrative (_WIND_TIER_FACTOR/_RIVER_TIER_FACTOR, a fixed ratio
        # applied to today's own number, not independently queried), a
        # ratio curve has no way to know a real threshold genuinely clears
        # no tiles at all, so it can only ever show a nonzero fraction of a
        # nonzero baseline, which can misrepresent a threshold with real
        # zero impact as having some.
        can_query_real = metric is not None and scope is not None
        # _hazard_curve_totals fetches
        # ONE get_tile_impact_totals_by_threshold aggregate per country
        # instead of a per-tier get_tile_impacts fan-out, replaces
        # 8 (wind) / 6 (river) separate Snowflake round-trips per country
        # with 1. Returns None for a metric it doesn't cover (People/
        # Children in Need, see its own docstring), in which case the
        # per-tier _resolve_stat_value fan-out below runs as a
        # fallback, via the shared long-lived executor (see
        # get_query_executor's own docstring in snowflake_utils.py).
        if name == "Sustained Wind" and can_query_real:
            real_values = _hazard_curve_totals(metric, "wind", scope, countries, date, run,
                                                  river_idx=(hazard_idx or {}).get("River Flooding"),
                                                  rain_idx=(hazard_idx or {}).get("Rainfall"), rain_window=rain_window)
            if real_values is None:
                raw_values = list(get_query_executor().map(
                    lambda wc: _resolve_stat_value(metric, scope, countries, date=date, run=run,
                                                      wind_kt=wc[2], hz=_wind_only_hz(wc[2]),
                                                      river_idx=(hazard_idx or {}).get("River Flooding"),
                                                      rain_idx=(hazard_idx or {}).get("Rainfall"),
                                                      rain_window=rain_window, river_window=river_window),
                    _WIND_CATS))
                # None only if this facility metric has no real data at ALL
                # for this country (a dataset-wide, not threshold-dependent,
                # gap, see _hazard_contribution_content's own top-level
                # None guard, which already keeps this whole curve from ever
                # building when the popup's own headline metric is None),
                # falls back to 0 here defensively.
                real_values = [_parse_stat_number(v) if v is not None else 0 for v in raw_values]
            _curve_all_zero_by_member[name] = all((v or 0) == 0 for v in real_values)
            chart = _threshold_curve_chart(labels, real_values, idx, color)
        elif name == "River Flooding" and can_query_real:
            # When River Flooding + Rainfall are BOTH active (flood_split_real),
            # the row this curve sits under shows the real WITHIN-Flood
            # split (river hits AND rain does NOT), not River's raw
            # marginal exposure, see _river_curve_totals_excl_rain's own
            # comment for the full "why" this needs its own real per-tier
            # fetch instead of reusing _hazard_curve_totals' marginal one
            # (947-vs-149-style mismatch otherwise: two real numbers, two
            # different questions, that read as contradicting each other).
            if river_rain_only and flood_split_real:
                real_values = _river_curve_totals_excl_rain(
                    metric, scope, countries, date, run,
                    rain_idx=(hazard_idx or {}).get("Rainfall"), rain_window=rain_window, river_window=river_window,
                    river_idx=(hazard_idx or {}).get("River Flooding"))
            else:
                real_values = _hazard_curve_totals(metric, "river", scope, countries, date, run, river_window=river_window,
                                                      river_idx=(hazard_idx or {}).get("River Flooding"),
                                                      rain_idx=(hazard_idx or {}).get("Rainfall"), rain_window=rain_window)
            if real_values is None:
                raw_values = list(get_query_executor().map(
                    lambda rp_tier: _resolve_stat_value(metric, scope, countries, date=date, run=run,
                                                           hz=_river_only_hz(rp_tier, river_window),
                                                           river_idx=(hazard_idx or {}).get("River Flooding"),
                                                           rain_idx=(hazard_idx or {}).get("Rainfall"),
                                                           rain_window=rain_window, river_window=river_window),
                    _RIVER_RP_TIERS))
                real_values = [_parse_stat_number(v) if v is not None else 0 for v in raw_values]
            _curve_all_zero_by_member[name] = all((v or 0) == 0 for v in real_values)
            chart = _threshold_curve_chart(labels, real_values, idx, color)
        elif name == "Rainfall":
            # Same real per-cell fetch pattern as Sustained Wind/River
            # Flooding above, via _precip_curve_totals (genuinely 2D, its
            # own dedicated aggregator, not _hazard_curve_totals, see that
            # function's own docstring). No real rain_window context (e.g.
            # the print page, which doesn't read live slider state) means
            # there's nothing to even query against, see _no_real_data_
            # chart's own comment for why this is an honest empty state,
            # never an illustrative number, same as every other hazard here.
            if rain_window is None:
                chart = _no_real_data_chart()
            else:
                # Same real WITHIN-Flood fix as River Flooding's own branch
                # above, mirrored onto the grid: when River Flooding is
                # ALSO active (river_rain_only/flood_split_real), every
                # cell must be Rain-and-NOT-River, not Rain's raw marginal
                # exposure, or the grid's own "current" cell contradicts
                # the real "Rainfall" headline row above it (see
                # _rain_grid_totals_excl_river's own comment).
                if river_rain_only and flood_split_real:
                    real_matrix = _rain_grid_totals_excl_river(
                        metric, scope, countries, date, run,
                        river_idx=(hazard_idx or {}).get("River Flooding"), river_window=river_window,
                        rain_idx=(hazard_idx or {}).get("Rainfall"), rain_window=rain_window)
                else:
                    real_matrix = _precip_curve_totals(
                        metric, scope, countries, date, run,
                        river_idx=(hazard_idx or {}).get("River Flooding"), rain_idx=(hazard_idx or {}).get("Rainfall"),
                        rain_window=rain_window, river_window=river_window) if can_query_real else None
                # real_matrix is None for the same "no real data path for
                # this metric/scope" reasons as the wind/river branches
                # above (see _precip_curve_totals' own docstring), leave
                # _curve_all_zero_by_member unset in that case (unknown, not
                # zero), same contract as everywhere else in this dict.
                if real_matrix is not None:
                    _curve_all_zero_by_member[name] = all(
                        (v or 0) == 0 for vals in real_matrix.values() for v in vals)
                chart = _rain_threshold_grid(labels, rain_window, idx, color, real_matrix)
        else:
            # Storm Surge (no real backend at all, see ms-surge-on's own
            # comment) or a real hazard missing metric/scope context
            # (defensive only, every real caller of
            # _hazard_contribution_content now passes both). Storm Surge
            # gets the SAME honest empty state as every other
            # no-real-data case, not its own illustrative exception, see
            # _no_real_data_chart's own comment.
            chart = _no_real_data_chart()
        return html.Div(chart, style={"marginTop": "4px"})

    def _segment(pct, family_pct, color, pattern=None):
        style = {"width": f"{max(pct, 0) / family_pct * 100:.2f}%", "height": "100%"}
        if pattern:
            style["backgroundImage"] = pattern
        else:
            style["background"] = color
        return html.Div(style=style)

    def _family_members_block(member_names, family_pct):
        # A family with only one member (Tropical Cyclone, for now) has
        # nothing to overlap with itself, falls back to the old flat row,
        # no bar, no overlap concept. Its own row+curve still gets wrapped
        # in a card for visual consistency with the multi-member case below.
        members = [(name,) + by_name[name] for name in member_names]  # (name, color, pct, icon)
        if len(members) < 2:
            # family_pct (not the member's own fixed illustrative share),
            # a single-member family's displayed share IS the family's own
            # share by definition; using the member's separately-tracked
            # constant here would let the two silently disagree, e.g.
            # showing a fixed "45%" for Sustained Wind even when it's
            # overridden to be 100% of a real Global total one level up.
            name, color, _member_pct, icon = members[0]
            pct = family_pct
            curve = _hazard_curve_row(name, color, pct)
            card_children = [_hazard_row(name, color, pct, icon, indent=True, mb=None if curve is not None else 0)]
            if curve is not None:
                card_children.append(curve)
            return [_hazard_card(color, card_children)]

        # Same idea as the TC/Flood overlap bar above, one level down:
        # people affected by 2+ of THIS family's own members (e.g. both
        # river flooding and rainfall at once) aren't a separate distinct
        # hazard, they're double-counted across the "only" segments unless
        # pulled out into their own share.
        # Raw floats through this whole derivation chain (multi_pct/
        # remaining_pct/only_pcts/triple_pct/double_pct below), same
        # premature-ceil bug already fixed in _hazard_breakdown's own
        # both_pct (see its comment for the full "why"): these percentages
        # get multiplied against a real people-at-risk total and ceil'd
        # ONCE at final display (_hazard_row/_hazard_curve_row), and
        # _hazard_row's own "%" text rounds separately for display, not via
        # a second premature ceil here.
        # River Flooding + Rainfall (only, no Storm Surge) uses the real
        # within-Flood split (flood_split_real, computed above via the
        # same per-tile bitmask methodology as real_estimate) instead of
        # the flat illustrative constants below. The two real per-member
        # numbers (river_only_n/rain_only_n/flood_both_n) are scaled onto
        # THIS family's own already-real family_pct/total, rather than
        # used as absolute numbers directly, so this row's displayed
        # number reconciles exactly with "Flood: {pct}% / {total}" above
        # it (same real ratios, not two independently-rounded real totals
        # disagreeing on the margin, see flood_split_real's own comment).
        is_real_river_rain = (member_names == ["River Flooding", "Rainfall"]) and flood_split_real
        if is_real_river_rain:
            real_total = river_only_n + rain_only_n + flood_both_n
            only_pcts = [(river_only_n / real_total) * family_pct, (rain_only_n / real_total) * family_pct]
            multi_pct = (flood_both_n / real_total) * family_pct
        else:
            multi_pct = _HAZARD_MULTI_FRAC * family_pct
            remaining_pct = family_pct - multi_pct
            weight_total = sum(m[2] for m in members)
            only_pcts = [remaining_pct * m[2] / weight_total for m in members]
        # abs_values: real un-derived absolute counts for the "only" rows,
        # aligned to `members`' own fixed order (River Flooding, Rainfall,
        # matching _HAZARD_FLOOD_MEMBERS), None per-member in the
        # illustrative case so _hazard_row falls back to its own
        # total*pct/100 reconstruction unchanged (see _hazard_row's own
        # abs_value comment for the full "why" this matters).
        only_abs = [river_only_n, rain_only_n] if is_real_river_rain else [None] * len(members)
        only_segments = [_segment(p, family_pct, m[1]) for p, m in zip(only_pcts, members)]
        only_cards = []
        for p, m, abs_v in zip(only_pcts, members, only_abs):
            m_name, m_color, _m_pct, m_icon = m
            curve = _hazard_curve_row(m_name, m_color, p)
            card_children = [_hazard_row(m_name, m_color, p, m_icon, indent=True, mb=None if curve is not None else 0, abs_value=abs_v)]
            if curve is not None:
                card_children.append(curve)
            only_cards.append(_hazard_card(m_color, card_children))

        if len(members) == 2:
            multi_segments = [_segment(multi_pct, family_pct, HAZARD_BOTH_COLOR, pattern=_DOUBLE_OVERLAP_PATTERN)]
            multi_cards = [_hazard_card(HAZARD_BOTH_COLOR,
                                          _hazard_row("Both", HAZARD_BOTH_COLOR, multi_pct, "mdi:vector-intersection", indent=True, mb=0,
                                                        abs_value=(flood_both_n if is_real_river_rain else None)))]
        else:
            # 3+ members (Flood: River Flooding/Rainfall/Storm Surge),
            # split the shared "2+ at once" pool further into exactly-2
            # vs. all-of-them-at-once, since those read as meaningfully
            # different severities, not one lump "some overlap" bucket.
            # Single diagonal hatch for "any 2" vs. a criss-cross (two
            # crossed diagonal directions) for "all 3", the pattern itself
            # escalates with how many hazards are stacked, not just the color.
            triple_pct = _HAZARD_TRIPLE_FRAC * multi_pct
            double_pct = multi_pct - triple_pct
            multi_segments = [_segment(double_pct, family_pct, HAZARD_BOTH_COLOR, pattern=_DOUBLE_OVERLAP_PATTERN),
                                _segment(triple_pct, family_pct, HAZARD_TRIPLE_COLOR, pattern=_TRIPLE_OVERLAP_PATTERN)]
            multi_cards = [
                _hazard_card(HAZARD_BOTH_COLOR,
                               _hazard_row("Double overlap (any 2)", HAZARD_BOTH_COLOR, double_pct, "mdi:vector-intersection", indent=True, mb=0)),
                _hazard_card(HAZARD_TRIPLE_COLOR,
                               _hazard_row("Triple overlap (all 3)", HAZARD_TRIPLE_COLOR, triple_pct, "mdi:vector-combine", indent=True, mb=0)),
            ]

        # width:"calc(100% - 18px)" + marginLeft:"18px", matches the cards'
        # own marginLeft below, so the bar's edges line up with the cards it
        # sits above instead of the (now-removed) per-row indent.
        bar = html.Div(
            only_segments + multi_segments,
            style={"display": "flex", "height": "10px", "borderRadius": "4px", "overflow": "hidden",
                    "width": "calc(100% - 18px)", "marginBottom": "10px", "marginLeft": "18px"},
        )
        return [bar] + only_cards + multi_cards

    # Recomputed after the loop below, once the all-zero hide decision is
    # known (see that loop's own comment). Starts as the plain toggle-state
    # value (both families checked) and narrows to "both actually rendered"
    # once a hidden all-zero family is found, so the overlap bar/caption
    # below doesn't explain an overlap against a family that isn't visible.
    both_active = tc_active and flood_active
    blocks = []
    # Iterate only the ACTIVE members of each family (not the family's full
    # membership), group_pct comes from `breakdown`, which already sums
    # only what's actually checked, not _HAZARD_GROUPS' fixed member list.
    for group_name, group_color, group_icon, active_members, group_pct, active in (
        (_HAZARD_TC_NAME, _HAZARD_TC_COLOR, _HAZARD_TC_ICON, breakdown["active_tc_members"], breakdown["tc_pct"], tc_active),
        (_HAZARD_FLOOD_NAME, _HAZARD_FLOOD_COLOR, _HAZARD_FLOOD_ICON, breakdown["active_flood_members"], breakdown["flood_pct"], flood_active),
    ):
        if not active:
            continue
        family_blocks = [_hazard_row(group_name, group_color, group_pct, group_icon)]
        # _family_members_block (via _hazard_curve_row) populates
        # _curve_all_zero_by_member as a side effect for every member with a
        # real per-tier signal, so this must run BEFORE the all-zero check
        # below, not after.
        family_blocks.extend(_family_members_block(active_members, group_pct))
        family_blocks.append(html.Div(style={"height": "16px"}))
        # Hide a toggled-on family entirely (same as if it were never
        # toggled) only when EVERY one of its active members reports a
        # real, definitive all-zero across every one of ITS OWN severity
        # tiers, not just the one currently selected: this is the "the
        # storm genuinely doesn't reach this country at ANY severity"
        # case, meaningfully different from "0 at this threshold, real
        # impact at others" (which _curve_all_zero_by_member reports as
        # False and must keep showing, per this popup's own original
        # design intent, see the top-of-function comment). A member with
        # no entry at all (None, e.g. Storm Surge's illustrative-only
        # curve, or missing metric/scope context like the print page) is
        # "unknown, not zero" and must NOT count toward hiding, or a
        # family with genuine real signal on one member would wrongly
        # disappear just because a sibling member's own check couldn't run.
        member_zero_flags = [_curve_all_zero_by_member.get(m) for m in active_members]
        if member_zero_flags and all(f is True for f in member_zero_flags):
            both_active = False
            continue
        blocks.extend(family_blocks)

    # `real_estimate` is guaranteed True by this point whenever both_active
    # is True: the both_active-but-not-real_estimate case now bails out
    # with its own early return well above (see that return's own
    # comment), so there is no longer an illustrative-estimate variant of
    # this caption to choose between: `real_estimate` is backed by the
    # real per-member joint bitmask check, not an independence formula, so
    # this caption must NOT warn readers that the "Both" figure likely
    # understates reality (that warning would only be honest for an
    # approximation): "Both" is a real, directly-observed ensemble
    # fraction here, not an estimate derived from an independence
    # assumption.
    overlap_note = (
        _t("Real per-member figure — \"Both\" counts only ensemble members genuinely hit by both "
            "Tropical Cyclone and Flood hazards at the same tile, not an estimate.")
    )
    overlap_children = [_hazard_overlap_bar(total, breakdown),
                          dmc.Text(overlap_note, size="10px", c="dimmed", mt=14, mb=26, fs="italic")] if both_active else []

    # Column header labeling what the two right-aligned numbers on every
    # hazard row actually are, without this, "45%" / "173K" reads as two
    # unlabeled numbers with no indication either is a share of the total
    # above vs. that hazard's own People at Risk estimate. Left spacer
    # (width matching the icon+gap _hazard_row's own name column starts
    # after) keeps "Hazard" lined up with the row names below it.
    column_header = dmc.Group([
        html.Div(style={"width": "15px"}),
        dmc.Text(_t("Hazard"), size="9px", fw=700, tt="uppercase", c="dimmed", style={"flex": 1, "minWidth": 0}),
        dmc.Text(_t("Share"), size="9px", fw=700, tt="uppercase", c="dimmed", w=42, ta="right"),
        dmc.Text(_t("People at Risk"), size="9px", fw=700, tt="uppercase", c="dimmed", w=72, ta="right"),
    ], gap=14, wrap="nowrap", mb=8)

    # The caption only makes sense when a real illustrative SPLIT is being
    # shown somewhere above. The TC-vs-Flood split itself is
    # real when `real_estimate` (own caption above, via overlap_note);
    # the both_active-but-not-real_estimate case no longer reaches this far
    # at all, it bails out with an honest "split unavailable" message
    # earlier in this function instead (see that early return's own
    # comment), so it's deliberately NOT one of the disjuncts below any
    # more. The WITHIN-Flood sub-split is ALSO real (flood_split_real, via
    # is_real_river_rain in _family_members_block above) whenever exactly
    # River Flooding + Rainfall are the active Flood members, so that case
    # is excluded here too. What's left as illustrative — and still a real,
    # open gap this popup's own "no illustrative numbers ever" fix hasn't
    # reached yet — is a 3-member Flood selection (River+Rain+Storm Surge,
    # Storm Surge has no real backend, see ms-surge-on's own comment)
    # still falling back to the old fixed-weight (_HAZARD_MULTI_FRAC/
    # _HAZARD_TRIPLE_FRAC) split entirely, via _family_members_block. Since
    # its own per-member curves already show an honest "no real data" state
    # (see _no_real_data_chart), only the numeric SHARE percentages there
    # are still fabricated, not the curves themselves. Tropical Cyclone
    # only ever has one member (Sustained Wind, Gust is permanently
    # excluded, see _hazard_breakdown's own comment), so a single active
    # hazard (the common Global case: only Sustained Wind has real data)
    # shows a plain, deterministic 100%, not a split of anything, and
    # calling that "illustrative" was misleading.
    has_illustrative_split = len(breakdown["active_flood_members"]) > 1 and not (river_rain_only and flood_split_real)
    caption = [dmc.Text(_t("Illustrative split — a real implementation would compute this from actual per-hazard overlap."),
                          size="10px", c="dimmed", mt=18, fs="italic")] if has_illustrative_split else []

    # Real per-country breakdown for THIS metric, Global scope only: the
    # tile-click popup itself (not only the
    # separate Full Impact Breakdown modal, which shows every metric at
    # once but requires navigating to a different button/section) should
    # show one row per real country, each with the same black-total +
    # TC-only/Both/Flood-only split pattern the rows above already use.
    # `metric in _DEFAULT_STATS` excludes People/Children in Need (no
    # `stats` dict key exists for those, they're handled separately via
    # pin_pct elsewhere, and no real stat-card ever fires with those
    # metrics today anyway, see _CURVE_METRIC_COL's own comment). Sorted
    # by real value, largest first, a ranked "who's most affected" list is
    # more useful here than alphabetical (unlike _admin1_table's own
    # always-multi-metric table, where alphabetical is the only sensible
    # single order across 6 different metrics at once).
    country_rows = []
    if is_global and metric in _DEFAULT_STATS and scope == "global":
        by_country_countries = _resolve_curve_countries(
            scope, countries, date, run,
            river_idx=(hazard_idx or {}).get("River Flooding"), rain_idx=(hazard_idx or {}).get("Rainfall"),
            rain_window=rain_window, river_window=river_window)

        def _fetch_country_metric(c):
            return c, _get_country_stats(c, date, run, (hz or {}).get("wind_kt"), hz=hz), _get_country_family_split(c, date, run, hz=hz)

        for c, c_stats, c_fs in get_query_executor().map(_fetch_country_metric, by_country_countries):
            c_val = _parse_stat_number(c_stats.get(metric, "0"))
            if not c_val:
                continue
            # Same real 3-way resolution as _admin1_table's own
            # _region_metric_breakdown / _simple_breakdown_table's own
            # _value_td: single-family active -> the flat breakdown itself
            # (inert, _hazard_split_line renders nothing for one family);
            # both active -> a real per-country split if one resolves, else
            # an honest None (never a fabricated illustrative one). c_split
            # (the real un-rounded tc_only/flood_only/both numbers, not
            # just their derived percentages) is carried through to
            # _hazard_split_line's own real_split param below, so THIS
            # row's own sub-numbers don't get reconstructed from c_val (a
            # re-parse of an already-K/M-abbreviated display string) --
            # same real precision-loss class of bug _hazard_row's own
            # abs_value param exists to avoid, see _hazard_split_line's
            # own comment for the full "why".
            c_split = None
            if breakdown["tc_active"] and breakdown["flood_active"]:
                c_split = _sum_family_split_metric(c_fs, metric) if c_fs else None
                c_breakdown = (_breakdown_from_split(c_split.get("tc_only"), c_split.get("flood_only"), c_split.get("both"), breakdown)
                                if c_split else None)
            else:
                c_breakdown = breakdown
            country_rows.append((c, c_val, c_breakdown, c_split))
        country_rows.sort(key=lambda r: r[1], reverse=True)

    by_country_block = []
    if country_rows:
        by_country_block = [
            dmc.Text(_t("By Country"), size="10px", fw=700, c="dimmed", mt=20, mb=8, tt="uppercase"),
            html.Div([
                html.Div([
                    html.Span(_t(c_name), style={"flex": 1, "fontSize": "12px", "color": "#16232c", "fontWeight": 600}),
                    html.Div([
                        html.Div(_format_stat_number(c_val), style={"fontFamily": "monospace", "fontWeight": 700, "fontSize": "12px"}),
                        _hazard_split_line(c_val, c_breakdown, font_size="9px", real_split=c_split),
                    ], style={"minWidth": "90px", "textAlign": "right"}),
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                           "padding": "5px 0", "borderBottom": "1px solid #f1f4f6"})
                for c_name, c_val, c_breakdown, c_split in country_rows
            ]),
        ]

    return html.Div([
        # `total` (not the original `value` string) by the time this
        # renders: single-family case, total is still exactly
        # _parse_stat_number(value) (never reassigned), so this is a no-op
        # there. Dual-family real_estimate case is scope-dependent (see the
        # `total = real_total` reassignment's own comment above for the
        # full "why", including the real 74-vs-76 bug this split avoids):
        # Country Analysis reassigns total to the real per-member
        # family-split query's own sum, which is what every row/bar below
        # this headline is actually built from there, keeping the headline
        # and every row internally consistent, one real number instead of
        # two disagreeing real ones (a headline "Total: 5.0M" next to a
        # "Flood 100% / 5.1M" row would otherwise be visibly
        # self-contradictory). Global scope does NOT reassign, so `total`
        # here is still the original per-country-ceiled-then-summed
        # number, matching the By Country rows and the tile itself.
        dmc.Text(_t("Total: {value}", value=_format_stat_number(total)), size="sm", fw=700, mt=6, mb=20),
        *overlap_children,
        # Global scope drops the per-hazard Share/People-at-Risk table
        # (column_header + blocks) entirely: the overlap bar above
        # (Tropical Cyclone only/Both/Flood only,
        # still real, still shown) already covers the TC-vs-Flood split at
        # a glance, and the new By Country section below is a more useful
        # detail view than the per-hazard rows/curves were once the popup
        # already lost its own per-threshold curves (is_global gate in
        # _hazard_curve_row above). Country Analysis scope is untouched,
        # its own popup has no By Country section to substitute and the
        # per-hazard rows/curves there are still real and useful.
        *([] if is_global else [column_header, html.Div(blocks[:-1]), *caption]),  # blocks[:-1] drops the trailing spacer
        *by_country_block,
    ])


def _panel_controls_row():
    # Both controls on one compact line: the aggregation toggle (short text,
    # not icons, the icon-only version was unclear) and the worst-case
    # factor Select side by side. The aggregation toggle has its OWN
    # wrapper (impact-aggregation-wrapper) so it can be hidden independently
    # when only 1 country is selected, while the row itself (and the factor
    # Select) stays visible with 1+ countries, see _toggle_controls_row/
    # _toggle_aggregation_wrapper below.
    return dmc.Group([
        html.Div(
            dmc.SegmentedControl(
                id="impact-aggregation-toggle", value="combined", size="xs",
                data=[{"value": "per-country", "label": _t("Split")},
                      {"value": "combined", "label": _t("Total")}],
            ),
            id="impact-aggregation-wrapper",
        ),
        dmc.Select(
            id="influencing-factor-select", value="none", size="xs", w=130,
            data=_influencing_factors(), allowDeselect=False,
            leftSection=DashIconify(icon="carbon:chart-relationship", width=13),
        ),
    ], id="impact-controls-row", gap=8, wrap="nowrap",
        style={"padding": "10px 18px", "borderTop": "1px solid #eef2f5", "display": "none"})


def _breakdown_new_tab_href(countries, lang=None, date=None, run=None,
                               wind_on=True, river_on=True, rain_on=True, surge_on=True,
                               wind_idx=None, river_idx=None, rain_idx=None, surge_idx=None, rain_window=None,
                               river_window=None):
    """URL for a standalone, plain/printable version of this exact
    breakdown (pages/map_shell_breakdown_print.py), NOT a reload of the
    whole interactive dashboard with the modal reopened on top of it (not
    something you'd ever want to print or hand someone as a link). Same
    idea as the alert email's own "Open in
    new tab" link, just to a dedicated report page instead of a mock file.
    date/run/hazard toggles/slider indices default to the topbar/sidebar's
    own defaults (not live state) when called from the initial layout
    build, where there's no State to read yet; the callback below overrides
    them with the live values so the print page always matches whatever the
    modal currently shows, hazard scoping (and its threshold-preview
    curves) included."""
    countries = countries or []
    lang = lang or _LANG
    date = date or _DEFAULT_FORECAST_DATE
    run = run or _DEFAULT_FORECAST_RUN
    query = (f"?lang={lang}&date={date}&run={run}"
              f"&wind={int(bool(wind_on))}&river={int(bool(river_on))}"
              f"&rain={int(bool(rain_on))}&surge={int(bool(surge_on))}")
    for key, idx in (("windidx", wind_idx), ("riveridx", river_idx), ("rainidx", rain_idx), ("surgeidx", surge_idx)):
        if idx is not None:
            query += f"&{key}={idx}"
    if rain_window is not None:
        query += f"&rainwindow={rain_window}"
    if river_window is not None:
        query += f"&riverwindow={river_window}"
    if countries:
        query += f"&zoom_countries={','.join(quote(c) for c in countries)}"
    return f"/breakdown-print{query}"


def _impact_panel(initial_countries=None, open_breakdown=False, default_date=None, default_run=None):
    # Always visible (no dismiss button), the app's real value-add over a
    # pure met-visualization tool like WeatherLab: population/school/health
    # impact, not just where the hazard is. Defaults to a worldwide framing
    # until countries are selected; one stat block per selected country when
    # there's more than one, genuinely side by side, not a forced single pick.
    # Every stat number is clickable → per-hazard contribution popup, in
    # both Global and Country Analysis (the one addition that stays useful
    # everywhere, unlike In Need numbers/arc charts/member comparison). The
    # Full Breakdown button itself is Country-Analysis-only, Global's
    # summary is already the whole (simple) picture, no fuller view needed.
    #
    # default_date/default_run: layout() passes its own live-resolved
    # values (_resolve_default_forecast_date_run, see that function's own
    # docstring), same fix as _topbar()'s own default_date/default_run.
    # Only matters for the modal's initial server-rendered content below
    # (open_breakdown=True, the "open in new tab" deep-link path); the
    # modal's own live-Input-driven callbacks (_update_impact_breakdown/
    # _update_breakdown_new_tab_link) already read topbar-date/topbar-time
    # directly and would self-correct within one round trip either way,
    # this just avoids a real, if brief, wrong-date flash on that one path.
    _pdate = default_date or _DEFAULT_FORECAST_DATE
    _prun = default_run or _DEFAULT_FORECAST_RUN
    return html.Div([
        dmc.Group([
            dmc.Text(_t("Impact Summary"), fw=700, size="sm"),
            dmc.ActionIcon(DashIconify(icon="carbon:table-split", width=15), id="impact-breakdown-btn",
                            variant="subtle", color="gray", size="sm", style={"display": "none"}),
        ], justify="space-between", style={"padding": "16px 18px 2px"}),
        dmc.Text(_t("Global — worldwide totals across visible hazards"), size="xs", c="dimmed",
                  id="impact-subtitle", style={"padding": "0 18px 10px"}),
        _panel_controls_row(),
        html.Div(_stat_grid(_DEFAULT_STATS, scope="global"), id="impact-body",
                  style={"padding": "4px 18px 18px"}),
        dmc.Modal(
            id="impact-breakdown-modal", opened=open_breakdown,
            # Title + "open in new tab" link side by side, same pattern as
            # the alert email modal, just built into the title slot here
            # since this Modal (unlike the alert email's) already has real
            # title text of its own.
            title=dmc.Group([
                dmc.Text(_t("Full Impact Breakdown"), fw=700, size="15px", c="#16232c"),
                dmc.Anchor(
                    DashIconify(icon="carbon:launch", width=15), id="breakdown-new-tab-link",
                    href=_breakdown_new_tab_href(initial_countries, date=_pdate, run=_prun), target="_blank",
                    style={"display": "flex", "alignItems": "center", "color": "#8ea0ab"},
                ),
            ], gap=8, wrap="nowrap"),
            centered=True, radius="lg", padding="lg", size=_breakdown_modal_width(initial_countries),
            styles=_MODAL_PANEL_STYLES,
            overlayProps={"backgroundOpacity": 0.35, "blur": 3},
            children=html.Div(_impact_breakdown_content(initial_countries, None,
                                                            date=_pdate, run=_prun),
                                id="impact-breakdown-body",
                                style={"width": "100%", "maxWidth": "100%", "overflowX": "auto", "overflowY": "visible"}),
        ),
        dmc.Modal(
            id="hazard-contribution-modal", opened=False, size="lg",
            title=html.Span(_t("Hazard Contribution"), id="hazard-contribution-title"),
            centered=True, radius="lg", padding="xl", styles=_MODAL_PANEL_STYLES,
            overlayProps={"backgroundOpacity": 0.35, "blur": 3},
            children=html.Div(id="hazard-contribution-body"),
        ),
    # top/maxHeight from the shared _PANEL_TOP/_PANEL_MAX_HEIGHT spacing
    # system, see _PANEL_MAX_HEIGHT's own comment for the full derivation.
    ], id="impact-panel", style={**_PANEL_STYLE, "top": _PANEL_TOP, "right": f"{_UI_MARGIN}px", "width": "300px",
                                  "maxHeight": _PANEL_MAX_HEIGHT, "overflowY": "auto"})


# NOTE: no timeline panel. Removed deliberately, nothing on this map has a
# real per-lead-time picture to scrub through (envelopes/impact tiles are
# aggregated across the whole forecast), and
# the "Init" stepper duplicated the top bar's own date/time controls. The one
# genuinely real progression (TC_TRACKS position/intensity per lead time, out
# to the real 144h horizon, not 120h) would be a narrower, separate feature,
# a track marker scrubber only, not a panel implying the whole map updates.


# NOTE: a bottom-right color-scale legend was removed here (was a static,
# unwired "Low -> Severe" gradient left over from the dashboard_synthesis_v2
# mockup, never labeled with which hazard/property it applied to, and never
# updated when toggling layers or changing the severity slider). Same
# reasoning as the removed timeline panel: don't keep UI that implies more
# than the mockup actually backs. A real legend belongs once there's an
# actual active hazard+property to describe, mirroring the real app's
# per-hazard MapLibre legend, not a decorative constant.


# Map-adjacent command bar (from dashboard_synthesis_v2.html): quick hazard
# toggle pills + Tiles/Regions view switch, sitting right above the map
# between the two side panels, mirrors the rail checkboxes (same ids), not a
# second source of truth.
_CMD_HAZARDS = [("wind", "Sustained Wind", WIND), ("gust", "Gust", GUST), ("river", "River", RIVER),
                 ("rain", "Rainfall", RAIN), ("surge", "Storm Surge", SURGE)]
_PILL_OFF_STYLE = {"padding": "6px 12px", "borderRadius": "999px", "cursor": "pointer", "whiteSpace": "nowrap",
                    "border": "1px solid #dde6ec", "background": "#f6f9fb", "color": "#57707e"}
_PILL_DISABLED_STYLE = {**_PILL_OFF_STYLE, "cursor": "not-allowed", "pointerEvents": "none", "opacity": 0.5}
# The command-bar "HAZARDS" eye icon (ms-hazards-hidden-store) must both
# hide the actual MapLibre hazard layers AND dim the pills right next to
# it, hiding the layers alone with no visual change to the pills gives no
# sign anything was toggled off. Dimmed
# (same opacity as _PILL_DISABLED_STYLE) but still clickable/hoverable
# (unlike _PILL_DISABLED_STYLE, this is a temporary preview state, not an
# actual "no real data" disable, so interacting with a pill/checkbox still
# works normally while hidden).
_PILL_HIDDEN_STYLE = {**_PILL_OFF_STYLE, "opacity": 0.5}
_DOT_OFF_STYLE = {"width": "7px", "height": "7px", "borderRadius": "50%", "background": "#8ea0ab"}


def _cmd_pill(key, label):
    return html.Div(
        dmc.Group([
            html.Span(id=f"cmd-{key}-dot", style=_DOT_OFF_STYLE),
            dmc.Text(label, size="xs", fw=700),
        ], gap=6, wrap="nowrap"),
        id=f"cmd-{key}-pill", n_clicks=0, style=_PILL_OFF_STYLE,
        **{"data-disabled": "false"},
    )


# left = controls-panel's own right edge (_UI_MARGIN + 290) + _UI_MARGIN;
# right = _UI_MARGIN + impact-panel's own width (300) + _UI_MARGIN, same
# _UI_MARGIN gap to both side panels as everything else in this spacing
# system. top uses the same _PANEL_TOP as both side panels.
_COMMAND_BAR_STYLE = {**_PANEL_STYLE, "top": _PANEL_TOP,
                      "left": f"{_UI_MARGIN + 290 + _UI_MARGIN}px", "right": f"{_UI_MARGIN + 300 + _UI_MARGIN}px",
                      "padding": "9px 14px", "zIndex": 35, "overflowX": "auto"}


def _command_bar():
    # "Tiles vs Regions" is a country-scoped rendering concept (population
    # tiles or admin polygons within one place), there's no such data at
    # global scale (just storm markers/tracks), so this bar only makes sense
    # in Country Analysis mode. Hidden by default since Global is the
    # starting mode.
    return html.Div(
        # width:"max-content" + minWidth:"100%": takes the LARGER of its
        # own content size and the full command-bar width, so on a wide
        # viewport (content smaller than the bar) it still fills the bar;
        # on a narrow viewport (content bigger than the bar) it grows past
        # the bar's own width instead of being capped to it (a plain
        # width:auto block box never exceeds its containing block), so
        # _COMMAND_BAR_STYLE's overflowX:auto has real overflow to scroll
        # rather than silently shrinking flex children to fit (same
        # fit-content/min-width fix _admin1_table's own wrapper uses for
        # the same reason, see that function's comment). The ensemble
        # select's own marginLeft:"auto" (not justify="space-between" on
        # this outer Group) is what actually pins it to the right edge --
        # an auto margin always consumes exactly the row's remaining free
        # space, so it holds the hard-right position on any width this
        # row resolves to, wide or narrow/overflowed.
        # gap=10 on the outer Group (same value the hazards-pills sub-Group
        # already uses) is a real MINIMUM gap between the two top-level
        # children, kept even when marginLeft:"auto" on the ensemble Select
        # below has zero free space left to consume (the narrow/overflowed
        # case, where the pills+divider+segmented-control already fill the
        # whole row) -- without an explicit gap here the select sits flush
        # against "Regions" with no breathing room at all once overflow
        # kicks in. On a wide viewport this gap is a small fraction of the
        # row's real free space, so marginLeft:"auto" still absorbs the
        # rest and the select still lands flush against the bar's own
        # right edge.
        dmc.Group([
            dmc.Group(
                [html.Div(
                    dmc.Group([
                        dmc.Text(_t("Hazards"), size="10px", fw=700, c="dimmed", tt="uppercase"),
                        DashIconify(icon="mdi:eye-outline", id="cmdbar-hazards-eye", width=13, color="#8ea0ab"),
                    ], gap=4, wrap="nowrap"),
                    id="cmdbar-hazards-label", n_clicks=0,
                    style={"cursor": "pointer", "userSelect": "none"},
                )]
                + [_cmd_pill(k, _t(l)) for k, l, _ in _CMD_HAZARDS]
                + [
                    _vdivider(),
                    dmc.Text(_t("View"), size="10px", fw=700, c="dimmed", tt="uppercase"),
                    dmc.SegmentedControl(
                        id="cmdbar-detail", value="tiles", size="xs",
                        data=[{"value": "tiles", "label": _t("Tiles")}, {"value": "admin", "label": _t("Regions")}],
                    ),
                ], gap=10, wrap="nowrap", align="center",
            ),
            # Just one control, so it lives on the right of this bar rather
            # than taking a whole left-panel section of its own. No hover
            # tooltip anymore, it fired on every hover over the select
            # (not just when open), which read as an intrusive popup rather
            # than a helpful hint. flexShrink:0 keeps this at its full
            # w=190 even under width pressure -- the hazard pills to its
            # left already have a natural shrink floor from their own
            # whiteSpace:nowrap labels, but this Select has no such floor
            # and would otherwise absorb most of any remaining compression.
            # marginLeft:"auto" (not this outer Group's justify prop) is
            # what actually pins it to the right edge, see this function's
            # own header comment for why.
            dmc.Select(
                id="ensemble-member-select", value="combined", data=_ensemble_members(),
                size="xs", w=190, searchable=True, style={"flexShrink": 0, "marginLeft": "auto"},
            ),
        ], gap=10, wrap="nowrap", align="center", style={"width": "max-content", "minWidth": "100%"}),
        id="command-bar",
        style={**_COMMAND_BAR_STYLE, "display": "none"},
    )


def _compact_footer():
    # Same logos/links as the real footer (components/ui/footer.py), sized
    # down for this shell's much shorter bar. Built independently rather than
    # reusing the `footer` object's nested children: those are actual
    # component instances shared by every other page, and mutating their
    # width/size in place would resize the real app's footer too.
    #
    # Chrome (fixed position/background/zIndex) lives on a plain html.Div
    # wrapper, not on the dmc.Group's own style, same pattern as _topbar().
    # Putting it directly on dmc.Group left the background looking off
    # (Mantine's own Group styles won by specificity in places), so the
    # background color here is now on an element with nothing else competing
    # for it.
    return html.Div(dmc.Group(
        [
            dmc.Group(
                [
                    dmc.Text(_t("Supported by"), size="xs", c="white", opacity=0.8, style={"marginRight": "8px"}),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/DID-logo-white.png", w=74),
                        href="https://www.unicef.org/digitalimpact/what-we-do/artificial-intelligence-children",
                        target="_blank", style={"marginRight": "12px"},
                    ),
                    _vdivider(color="rgba(255,255,255,0.6)"),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/FDN-UNICEF-logo_white.png", w=120),
                        href="http://frontierdatanetwork.org/", target="_blank", style={"marginLeft": "12px", "marginRight": "12px"},
                    ),
                    html.Div(style={"width": "18px", "height": "1px", "background": "rgba(255,255,255,0.6)",
                                      "marginRight": "12px"}),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/unicef-digital-inclusion_white.png", w=100),
                        href="https://www.unicef.org/digitalimpact/digital-inclusion", target="_blank", style={"marginRight": "14px"},
                    ),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/OoI_logo.png", w=95),
                        href="https://www.unicef.org/innovation/", target="_blank", style={"marginRight": "14px"},
                    ),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/ose_logo_white.png", w=70),
                        href="https://data.unicef.org/", target="_blank",
                    ),
                ],
                align="center", gap="sm",
            ),
            dmc.Group(
                [
                    # Always-visible experimental-tool disclaimer (see
                    # _EXPERIMENTAL_DISCLAIMER's own comment for why the
                    # footer, not a dismissible banner). Red pill, not the
                    # muted white/opacity text "Supported by" uses, this one
                    # is meant to read as an actual warning, not ambient
                    # footer text.
                    dmc.Badge(_t(_EXPERIMENTAL_DISCLAIMER), color="red", variant="filled",
                                size="md", radius="sm", style={"textTransform": "none", "fontWeight": 500}),
                    dmc.Stack(
                        [
                            dmc.Text(_t("Powered by"), size="9px", c="white", opacity=0.8, ta="center"),
                            dmc.Anchor(
                                dmc.Image(src="assets/img/gigaspatial_white_2x.png", w=75),
                                href="https://github.com/unicef/giga-spatial", target="_blank",
                            ),
                        ],
                        gap=2, align="center", style={"marginLeft": "16px"},
                    ),
                    dmc.Anchor(
                        dmc.ActionIcon(DashIconify(icon="carbon:logo-github", width=24), variant="transparent",
                                        style={"color": "#ffffff"}),
                        href="https://github.com/unicef-drp/Ahead-of-the-Storm", target="_blank",
                    ),
                ],
                align="center", gap="md",
            ),
        ],
        justify="space-between",
        style={"width": "100%"},
    # 24px horizontal padding, its own value, not _UI_MARGIN (see
    # _language_switcher's own comment in _topbar for why: the footer's
    # content wants more breathing room from the true screen edge than the
    # floating panels above it need from it).
    ), style={"width": "100%", "backgroundColor": "#00AEEF", "color": "#ffffff", "padding": "14px 20px",
              "position": "fixed", "bottom": 0, "left": 0, "zIndex": 1000, "boxSizing": "border-box"})


def _map_disclaimer():
    # Centered within the gap between the two side panels (same insets as
    # _COMMAND_BAR_STYLE caps its MAXIMUM width, so a very narrow viewport
    # still wraps/ellipsizes instead of overflowing the panels) but sized
    # to its own text via width:fit-content, not stretched to fill that
    # whole gap, a fixed left/right pairing would force the background
    # pill to span the entire gap width regardless of how short the
    # sentence actually is.
    return html.Div(
        dmc.Text(_t(_UN_DISCLAIMER), size="9px", c="#57707e",
                  style={"whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"}),
        style={"position": "absolute", "bottom": _BOTTOM_ROW_OFFSET, "left": "50%", "transform": "translateX(-50%)",
               "width": "fit-content",
               "maxWidth": f"calc(100% - {(_UI_MARGIN + 290 + _UI_MARGIN) + (_UI_MARGIN + 300 + _UI_MARGIN)}px)",
               "textAlign": "center", "zIndex": 25, "pointerEvents": "none",
               "background": "rgba(255,255,255,0.75)", "borderRadius": "6px", "padding": "4px 10px"},
    )


# The basemap switcher lives inside _bottom_left_controls() (near
# _demo_scenarios_menu(), which shares this same row), not as its own
# standalone function, so the basemap SegmentedControl and the Demo
# Scenarios icon can sit in one flex row together.


# ---------------------------------------------------------------------------
# Map legend, real, reactive (see the removed-mockup NOTE above this
# function for why a static one was deliberately deleted instead of kept).
# Small pill by default (Google Maps' own weather-legend convention),
# click-to-expand into a full card; anchored bottom-right, BELOW where the
# Global-only Demo Scenarios menu sits (bottom:74) so it never collides with
# it, and below where nothing else sits at all in Country Analysis mode,
# the one corner genuinely free in both modes without conditional styling.
# ---------------------------------------------------------------------------
# bottom:74px (not 20px), the fixed, zIndex:1000 _compact_footer spans the
# full viewport width and, despite looking empty at the far right, its own
# invisible Group element still captures clicks there, so anything below
# roughly bottom:60px collides with it. Demo Scenarios lives inside
# _bottom_left_controls() (an icon, next to the basemap switcher)
# specifically so bottom-right is free for this in BOTH modes, not just
# Country Analysis, no empty-looking gap in Country Analysis mode,
# no Global-mode collision.
#
# bottom offset from the shared _BOTTOM_ROW_OFFSET spacing system (see
# _PANEL_MAX_HEIGHT's own comment), impact-panel's maxHeight is derived
# FROM this same row's real height, so the two can never overlap
# regardless of which side changes.
_LEGEND_ANCHOR = {"position": "absolute", "bottom": _BOTTOM_ROW_OFFSET, "right": f"{_UI_MARGIN}px", "zIndex": 30}

_LEGEND_PROP_LABELS = {
    "probability": "Hazard Probability", "population": "Population",
    "children_total": "Children (total)", "infant_population": "Infants",
    "school_age_population": "School-age", "adolescent_population": "Adolescents",
    "built_surface_m2": "Built-up Area", "cci_children": "Child Climate Index",
    "smod_class": "Settlement Type", "rwi": "Relative Wealth Index",
    "moderate_poverty_prob": "Moderate Poverty Probability", "severe_poverty_prob": "Severe Poverty Probability",
    "E_population": "Expected Population Impact", "E_children_total": "Expected Children Impact",
    "E_infant_population": "Expected Infant Impact", "E_school_age_population": "Expected School-age Impact",
    "E_adolescent_population": "Expected Adolescent Impact", "E_built_surface_m2": "Expected Built-up Impact",
    "E_cci_children": "Expected Child Climate Index Impact",
    "E_people_in_need": "People in Need", "E_children_in_need": "Children in Need",
    "E_infant_in_need": "Infants in Need", "E_school_age_in_need": "School-age in Need",
    "E_adolescent_in_need": "Adolescents in Need",
    "E_num_shelters": "Shelters at Risk", "E_num_wash": "WASH Facilities at Risk",
    "E_num_schools": "Schools at Risk", "E_num_hcs": "Health Centers at Risk",
}

_LEGEND_HAZARD_LABELS = {"wind": "Sustained Wind", "gust": "Gust", "river": "River Flooding", "rain": "Rainfall"}

# Same real fixed (min, max) constants as services/tile_server.py's own
# _FIXED_SCALE_COLS / maplibre_tiles.js's own _AOTS_FIXED_SCALE_COLS (see
# either one's own comment for the full real-world grounding), duplicated
# here for the same import-boundary reason every other palette/prop-map
# constant in this file is duplicated rather than imported (pages/ is
# downstream of services/ in this app's own import graph).
#
# Only "probability" remains fixed here. The raw population-family props
# (population/children_total/infant_population/school_age_population/
# adolescent_population) were tried as a fixed scale, then reverted back
# to a real, data-driven per-country/per-cycle range -- their own
# _FIXED_SCALE_COLS/_AOTS_FIXED_SCALE_COLS entries were removed too, so
# the legend correctly falls through to its own dynamic-min/max default
# for them, matching what the map itself now renders. Their E_* impact/
# exposure siblings were also reverted, moved off this mechanism entirely
# onto "linear" scale.
#
# A real bug this dict's mere existence originally fixed
# (still true for "probability"): _legend_raster_info/_legend_combined_
# raster_info's own real min/max came from tile_config's stats_<hazard>
# dict (get_tile_stats/_fetch_tile_stats in tile_server.py), which
# computes its OWN independent real per-country/per-cycle MIN/MAX SQL
# aggregate -- entirely separate from _get_minmax, so it was NEVER
# touched by the fixed-scale rendering fix on its own, meaning the legend
# could show a different range than what the map actually painted
# against for any prop this dict doesn't cover.
_LEGEND_FIXED_SCALE_PROPS = {
    "probability": (1.0 / 51, 1.0),
}


def _legend_format_value(val, prop_key, palette):
    if val is None:
        return "—"
    if palette.get("fixed_max") == 1.0 or prop_key.endswith("_prob") or prop_key == "probability":
        # The raster's
        # own color scale for "probability" is log (see
        # _RASTER_PALETTES['PROBABILITY'] in tile_server.py), so its real
        # MIN endpoint is often well under 1% (e.g. a real country-wide
        # river rp10 min of 0.08%), always rounding to whole percent
        # would show a real, meaningful "0.08%" floor as a misleading "0%".
        pct = val * 100
        if 0 < pct < 1:
            return f"{pct:.2g}%"
        return f"{pct:.0f}%"
    if prop_key == "rwi":
        return f"{val:+.2f}"
    return _format_stat_number(val)


def _legend_swatch_row(color, label, shape="square"):
    swatch_style = {"width": "12px", "height": "12px", "flexShrink": 0,
                      "background": color, "border": "1px solid rgba(0,0,0,0.1)"}
    if shape == "circle":
        swatch_style["borderRadius"] = "50%"
    elif shape == "line":
        swatch_style = {"width": "16px", "height": "3px", "flexShrink": 0, "background": color, "borderRadius": "2px"}
    return dmc.Group([html.Span(style=swatch_style), dmc.Text(label, size="11px", c="#455a64")],
                       gap=8, wrap="nowrap", mb=5)


_LEGEND_LAYER_ORDER = ["wind", "gust", "river", "rain", "tracks"]


def _legend_color_swatch(color):
    # Compact-only, a plain color patch, deliberately with NO embedded
    # text label (unlike _legend_swatch_row, built for the full card's
    # stacked rows). Must NOT reuse _legend_swatch_row for the compact
    # strip: that would duplicate the hazard name (already shown as
    # the strip's own title) right next to it, and that second copy would
    # wrap onto 2 lines in the available width, the compact strip is
    # supposed to stay ONE short row no matter what.
    return html.Div(style={"height": "10px", "borderRadius": "5px", "background": color})


def _legend_raster_info(hazard, tile_config):
    """Info dict for hazard's ('wind'/'gust'/'river'/'rain') MapLibre
    raster color scale, or None if that raster isn't actually visible,
    real min/max from the SAME stats ms-tile-config-store already fetched
    (stats_<hazard>[tile_prop]), so this never describes numbers that don't
    match what's actually painted on the map."""
    if not tile_config.get(f"{hazard}_visible"):
        return None
    prop = tile_config.get("tile_prop") or "population"
    stats = tile_config.get(f"stats_{hazard}") or {}
    prop_stats = stats.get(prop) or {}
    palette = _AOTS_PALETTES.get(prop) or _AOTS_PALETTES.get("population", {"colors": ["#ffffcc", "#800026"]})
    colors = palette.get("colors", ["#ffffcc", "#800026"])
    min_v, max_v = prop_stats.get("min"), prop_stats.get("max")
    # `min_v`/`max_v` display the SAME fixed constants the map itself now
    # renders against (_LEGEND_FIXED_SCALE_PROPS), not tile_config's own
    # real per-country/per-cycle SQL min/max, whenever this prop has one --
    # real bug otherwise: the map paints against a fixed
    # scale but the legend kept showing the old dynamic range, e.g. a
    # "1 -- 25" label under a bar that was actually now scaled 1 -- 50,000.
    # Only overrides when real data genuinely exists here (min_v is not
    # None): a None min_v is this function's own "no real data at all"
    # signal a few lines below, must NOT be masked into looking like real
    # data by unconditionally substituting the fixed range.
    if min_v is not None:
        fixed_range = _LEGEND_FIXED_SCALE_PROPS.get(prop)
        if fixed_range is not None:
            min_v, max_v = fixed_range
    prop_label = _t(_LEGEND_PROP_LABELS.get(prop, prop.replace("_", " ").title()))
    # Must NOT always prefix the title with the hazard name whenever the
    # checkbox is checked/visible, that would be wrong when that hazard
    # has no real data for the current selection (e.g. Sustained Wind
    # checked on a quiet-date country with no active storm), where the
    # raster painted underneath is just the plain Population base layer
    # (see _build_hazard_tile_config's own any_hazard_on/wind_has_data
    # comment), so labeling it "Sustained Wind, Population" would falsely
    # imply real wind-weighted coloring.
    # {hazard}_has_data (same real-data signal any_hazard_on uses) gates
    # the hazard-name prefix, same underlying bar/stats either way, since
    # they already correctly show plain Population's own real min/max.
    has_data = tile_config.get(f"{hazard}_has_data", True)
    # A checked-but-no-data hazard, e.g. Sustained Wind default-checked
    # with no active storm, must NOT show its OWN phantom "Hazard
    # Probability / No real data" legend card: `probability` has no
    # raw/base fallback the way population/children/etc do, so with no
    # real PROBABILITY data at all, that hazard's own raster tile is fully
    # transparent (see _fetch_raster_tile's own `vals != 0` filter), it
    # paints literally nothing on the map, so a legend card describing it
    # would be pure clutter.
    #
    # E_* exposure props (Impact mode, e.g. "E_population") get the SAME
    # unconditional suppression, not just "probability" (a real, confirmed-
    # live bug: a no-storm Wind selection under "E_
    # population" still showed its own generic "Expected Population
    # Impact / No real data" card stacked right next to Rainfall's real
    # one). Confirmed directly against _fetch_raster_tile's own colorize
    # logic: "For all others (probability, poverty, E_* impact props), 0 =
    # no data -> transparent" applies to E_* exactly like probability, an
    # E_* column is raw_count x that hazard's OWN probability by
    # definition, so with no real probability, E_* is null/0 everywhere
    # and the raster paints literally nothing here too -- same "nothing on
    # screen to describe" reasoning, not a different case needing
    # cross-hazard lookahead logic.
    #
    # Plain (non-E_*, non-probability) props ("population"/"children_
    # total"/etc, the "At Risk" mode) are genuinely NOT affected by
    # either branch: those render the real raw base count, unweighted,
    # identically for EVERY hazard regardless of its own data state (not
    # hazard-conditional at all), so min_v is real there and this check
    # never triggers -- and since every checked hazard would show that
    # SAME real number, the "one card has data, one doesn't" confusion
    # this fixes can't arise for plain props in the first place.
    if (prop == "probability" or prop.startswith("E_")) and min_v is None:
        return None
    hazard_name = _t(_LEGEND_HAZARD_LABELS[hazard])
    return {
        "title": f"{hazard_name} — {prop_label}" if has_data else prop_label,
        # Short version for the compact strip: the full "Hazard —
        # Property" title comfortably fits the full card's own 300px width
        # on its own line, but not squeezed onto the same row as the bar
        # and chevron too.
        "compact_title": hazard_name if has_data else prop_label,
        "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                 "background": f"linear-gradient(to right, {', '.join(colors)})"}),
        "labels": (_legend_format_value(min_v, prop, palette), _legend_format_value(max_v, prop, palette)),
        "caption": None if min_v is not None else "No real data for this exact selection yet.",
    }


# Mirrors _EXPOSURE_E_PROP_MAP's own values further down this file (not
# imported directly, that dict is defined much later, after this section.
# Same cross-file/same-file duplication convention this repo already has
# for propMap/ePropMap),
# and maplibre_tiles.js's own _AOTS_COMBINABLE_EXPOSURE_PROPS /
# _COMBINED_EXPOSURE_RAW_COL in tile_server.py: the only Exposure props
# with a real combined-raster raw-count × probability formula.
_COMBINABLE_EXPOSURE_PROPS = frozenset({
    "E_population", "E_children_total", "E_infant_population",
    "E_school_age_population", "E_adolescent_population", "E_built_surface_m2",
})


def _combined_raster_active(tile_config):
    """Real check for whether the MAP is currently painting ONE combined
    raster (mirrors maplibre_tiles.js's own applyTileConfig useCombined
    exactly) rather than N separate per-hazard rasters, so the legend can
    match what's actually on screen instead of always describing each
    hazard independently."""
    # The HAZARDS
    # eye icon (ms-hazards-hidden-store) makes applyTileConfig in
    # maplibre_tiles.js return EARLY, hiding every hazard layer including
    # the combined one, BEFORE it ever computes its own useCombined at
    # all. This function must mirror that same early-exit, or the legend
    # could keep showing a "Combined Hazard Probability"/"Hazard
    # Classification" card while the eye icon has actually blanked the map.
    if tile_config.get("hazards_hidden"):
        return False
    mode = tile_config.get("hazard_render_mode") or "probability"
    if mode == "classification":
        return True
    if mode != "probability":
        return False
    prop = tile_config.get("tile_prop")
    if prop != "probability" and prop not in _COMBINABLE_EXPOSURE_PROPS:
        return False
    active = sum(1 for hz in ("wind", "gust", "river", "rain")
                  if tile_config.get(f"{hz}_visible") and tile_config.get(f"{hz}_has_data"))
    return active >= 2


def _legend_combined_raster_info(tile_config):
    """Must NOT show 3 separate cards, a dead generic one plus River's
    and Rain's own independent min/max, when the map itself is painting
    ONE real combined raster for those 2+ hazards, per
    _combined_raster_active above. Single "Expected Population Impact" (or Probability/
    Classification) entry describing that ONE raster, using the SAME
    color-scale source the raster itself actually paints with:
    - mode="classification": _CLASSIFICATION_HAZARD_RGBA-style swatches
      (which hazard(s) hit), not a gradient at all.
    - mode="probability" (tile_prop=="probability"): the union bound over
      the active hazards' own probability ranges (min of the mins, sum of
      the maxes capped at 1.0), which is the range the combined layer's own
      colour scale spans since P(A∪B) <= P(A)+P(B) always holds.
    - mode="exposure" (tile_prop is a combinable E_* prop): the RAW base
      column's own min/max (e.g. POPULATION, not E_POPULATION), the same
      scale the combined layer colours with, and the only self-consistent
      range once 2+ hazards are combined, since each hazard's own E_* range
      describes that hazard alone. Any active hazard's stats dict carries
      the same raw value (it is hazard-independent, from the shared base
      table), so whichever one has it works.

    Both gradient branches read whichever stats set the layer currently on
    screen is scaled by, see `stats_prefix` below.
    """
    mode = tile_config.get("hazard_render_mode") or "probability"
    prop = tile_config.get("tile_prop") or "population"
    active_hazards = [hz for hz in ("wind", "gust", "river", "rain")
                        if tile_config.get(f"{hz}_visible") and tile_config.get(f"{hz}_has_data")]
    # Which stats set the layer ON SCREEN is scaled by. Tiles view paints the
    # combined mercator raster, coloured from the z14 tile-level stats_{hz};
    # Regions view paints the combined admin polygons, whose fill expression
    # is built client-side from the region-level admin_stats_{hz} (see
    # _combinedAdminStats in maplibre_tiles.js). A region total and a single
    # z14 tile's value differ by orders of magnitude, so reading the wrong
    # set prints labels that describe a scale nothing on screen uses.
    # Classification stays on the raster in both views (a region has no
    # single honest classification), so it keeps the tile-level stats.
    stats_prefix = ("admin_stats_"
                    if tile_config.get("view_mode") == "admin" and mode != "classification"
                    else "stats_")
    if mode == "classification":
        hazard_colors = {"wind": WIND, "gust": GUST, "river": RIVER, "rain": RAIN}
        return {
            "title": _t("Hazard Classification"),
            "compact_title": _t("Classification"),
            "bar": html.Div([
                _legend_swatch_row(hazard_colors.get(hz, "#999"), _t(_LEGEND_HAZARD_LABELS[hz]), shape="square")
                for hz in active_hazards
            ] + [
                _legend_swatch_row("#6c7a89", _t("2+ hazards overlap"), shape="square"),
            ]),
            "compact_bar": html.Div(style={"height": "10px", "borderRadius": "5px", "background": "#6c7a89"}),
            "labels": None, "caption": None,
        }
    if prop == "probability":
        palette = _AOTS_PALETTES.get("probability", {"colors": ["#ffffcc", "#800026"]})
        colors = palette.get("colors", ["#ffffcc", "#800026"])
        # A constant 0-100% label range, matching what
        # services/tile_server.py's own combined-hazard raster path ALSO
        # now paints against (see that function's own _FIXED_SCALE_COLS
        # comment) -- an earlier version of this branch computed a real
        # union-bound range instead (min of every active hazard's own
        # min, sum of every active hazard's own max, capped at 1.0), which
        # was correct for the OLD dynamic renderer but is now stale: the
        # renderer no longer uses that union-bound range at all, so
        # keeping it here would show a legend range the map itself
        # doesn't paint against anymore.
        min_v, max_v = _LEGEND_FIXED_SCALE_PROPS["probability"]
        return {
            "title": _t("Combined Hazard Probability"),
            "compact_title": _t("Combined Probability"),
            "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                     "background": f"linear-gradient(to right, {', '.join(colors)})"}),
            "labels": (_legend_format_value(min_v, "probability", palette), _legend_format_value(max_v, "probability", palette)),
            "caption": _t("Real ensemble-member fraction across {n} active hazards — see map tooltip.", n=len(active_hazards)),
        }
    raw_prop = prop[2:] if prop.startswith("E_") else prop
    stats = next((tile_config.get(f"{stats_prefix}{hz}") or {} for hz in active_hazards
                    if (tile_config.get(f"{stats_prefix}{hz}") or {}).get(raw_prop)), {})
    prop_stats = stats.get(raw_prop) or {}
    palette = _AOTS_PALETTES.get(prop) or _AOTS_PALETTES.get("population", {"colors": ["#ffffcc", "#800026"]})
    colors = palette.get("colors", ["#ffffcc", "#800026"])
    min_v, max_v = prop_stats.get("min"), prop_stats.get("max")
    # Same fixed-scale override as _legend_raster_info's own (see that
    # function's own comment for the full "why"), keyed on `prop` (the
    # E_* combined-raster prop, e.g. "E_population"), not `raw_prop`: the
    # combined raster's own color scale is fixed on the E_* range (raw
    # count x fixed-1.0-max probability), not the raw column's range.
    if min_v is not None:
        fixed_range = _LEGEND_FIXED_SCALE_PROPS.get(prop)
        if fixed_range is not None:
            min_v, max_v = fixed_range
    prop_label = _t(_LEGEND_PROP_LABELS.get(prop, prop.replace("_", " ").title()))
    return {
        "title": prop_label,
        "compact_title": prop_label,
        "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                 "background": f"linear-gradient(to right, {', '.join(colors)})"}),
        "labels": (_legend_format_value(min_v, prop, palette), _legend_format_value(max_v, prop, palette)),
        "caption": (_t("Combined across {n} active hazards — see map tooltip.", n=len(active_hazards))
                    if min_v is not None else _t("No real data for this exact selection yet.")),
    }


# Legend entries for the GLOBAL, country/storm-independent raw river-extent/
# precip-rate rasters (see _build_global_raw_config's own header comment,
# an entirely separate rendering system from the per-country hazard-impact
# tiles above, sharing only the ms-river-on/ms-rain-on checkboxes as their
# on/off control). Must NOT have NO
# code path for these at all, toggling River Flooding/Rainfall on in
# Global mode (where _build_hazard_tile_config's own river_visible/
# rain_visible are ALWAYS forced False, see that function's "if not
# countries" early exit) would show a genuinely-painted flood/rain layer with
# no legend entry whatsoever. Fixed color ramps copied from
# services/tile_server.py's own _PRECIP_RATE_COLORS/_PRECIP_PROB_COLORS/
# _RIVER_EXTENT_PROB_COLORS/_RIVER_EXTENT_MASK_COLOR, these are the exact
# breakpoint colors the tile server paints, not independently invented ones.
_RAW_RIVER_PROB_COLORS = ["#B2EBF2", "#4DD0E1", "#00ACC1", "#006992", "#013260"]
_RAW_PRECIP_RATE_COLORS = ["#A8E691", "#3CB34B", "#FFDD33", "#FF8C00", "#DC143C"]
_RAW_PRECIP_PROB_COLORS = ["#DECBE4", "#BC95D5", "#9860C6", "#752FB1", "#4C0099"]

# Matches services/tile_server.py's own _PRECIP_RATE_BASE_BREAKS exactly (the
# base/6h radar ramp before per-window scaling).
_RAIN_RATE_BASE_BREAKS = [0.5, 5.0, 15.0, 30.0, 60.0]


def _rain_rate_range_for_window(window_h):
    """Real min/max of the Mean-mode radar ramp for this window, mirrors
    tile_server.py's own _precip_rate_breaks_for_window scaling (same real
    per-window depth-tier classification numbers, _RAIN_MM_BY_WINDOW below,
    single source of truth for both). Must NOT show a fixed "0.5mm"/"≥60mm"
    range for every window: longer accumulation windows genuinely reach
    much higher real totals (e.g. 120h/5-day accumulations routinely
    exceed 150mm, see that dict's own "120" entry), so a fixed range
    would render most of a 5-day forecast as one saturated "extreme rain"
    red blob with a legend that still claims the scale tops out at 60mm."""
    base_top = _RAIN_MM_BY_WINDOW["6"][-1]
    window_top = _RAIN_MM_BY_WINDOW.get(str(int(window_h)), _RAIN_MM_BY_WINDOW["6"])[-1]
    scale = window_top / base_top
    return _RAIN_RATE_BASE_BREAKS[0] * scale, _RAIN_RATE_BASE_BREAKS[-1] * scale


def _legend_raw_flood_info(layer, raw_config):
    """Info dict for the raw river-extent ('river') or precip-rate ('rain')
    global raster, or None if that raw layer isn't actually visible right
    now (checkbox off, or no real forecast cycle resolved for the selected
    date, per raw_config's own {river,precip}_visible flags)."""
    if not raw_config:
        return None
    visible_key = "river_visible" if layer == "river" else "precip_visible"
    if not raw_config.get(visible_key):
        return None
    hazard_name = _t(_LEGEND_HAZARD_LABELS[layer])
    title_suffix = _t("Raw global layer")

    # Picking a specific ensemble member via ensemble-member-select must
    # NOT silently keep showing the AGGREGATE legend (e.g. the "% of
    # members agree" gradient) while the map itself has switched to a
    # single member's own flat/real-rate rendering. A specific
    # member has no "member agreement" concept at all (that IS the
    # aggregate-across-members statistic), so this branches to its own
    # content entirely rather than reusing the probability-mode swatch
    # below with a relabeled title.
    member_key = "river_member" if layer == "river" else "precip_member"
    member = raw_config.get(member_key)
    if member is not None:
        member_label = _t("Control (deterministic)") if member == _CONTROL_MEMBER else _t("Member {n}", n=member)
        if layer == "river":
            # Same "RPn, <=Xh" param_label pattern the aggregate title below
            # uses, so a member's own flood extent doesn't lose
            # that context just because it swapped legend branches.
            rp_tier_m = raw_config.get("rp_tier") or "rp10"
            step_h_m = raw_config.get("river_step_h") or 72
            param_label_m = f"{rp_tier_m.upper()}, ≤{step_h_m}h"
            # Single fixed color (see services/tile_server.py's own
            # _RIVER_EXTENT_MEMBER_COLOR), a real boolean "does this member
            # flood here", not a gradient.
            return {
                "title": f"{hazard_name} ({param_label_m}) — {title_suffix} ({member_label})",
                "compact_title": hazard_name,
                "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                         "background": _RAW_RIVER_PROB_COLORS[-1]}),
                "compact_bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                                 "background": _RAW_RIVER_PROB_COLORS[-1]}),
                "labels": (_t("Flooded under this member"), ""),
                "caption": _t("(not natively computed — RP10 used as an upper-bound estimate)") if raw_config.get("rp_tier_is_standin") else None,
            }
        # Rain: still a real intensity ramp (that member's own real rate,
        # not an aggregate), same color scale as Mean mode (the numbers
        # are real mm either way), just window-scaled and re-titled so it
        # doesn't read as the ensemble mean.
        lo, hi = _rain_rate_range_for_window(raw_config.get("window_h") or 6)
        return {
            "title": f"{hazard_name} ({raw_config.get('window_h') or 6}h) — {title_suffix} ({member_label})",
            "compact_title": hazard_name,
            "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                     "background": f"linear-gradient(to right, {', '.join(_RAW_PRECIP_RATE_COLORS)})"}),
            "compact_bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                             "background": f"linear-gradient(to right, {', '.join(_RAW_PRECIP_RATE_COLORS)})"}),
            "labels": (f"{lo:g}mm", f"≥{hi:g}mm"),
            "caption": None,
        }

    # River has NO Mean mode, it always renders the one real per-cell
    # member-agreement fraction (Probability). River must NOT have its own
    # Mean toggle: unlike rain (real mm
    # intensity AND a real exceedance-probability, two genuinely different
    # quantities), river only ever has this one metric, so a "Mean" option
    # would always be just the same number under a different name. flood-view-as
    # (raw_config's own rain_mode) is rain-only.
    mode = "probability" if layer == "river" else (raw_config.get("rain_mode") or "probability")

    # Rain's active window/threshold (raw_config's own window_h/threshold_mm,
    # resolved by _build_global_raw_config) is now named ONCE, in the title,
    # exactly mirroring river's own "(RP10)" pattern below, instead of being
    # repeated inside BOTH the min and max labels: a "10% of members
    # >150mm/120h" / "≥80% of members >150mm/120h" label pair would
    # duplicate the same threshold/window text twice, wrapping onto its
    # own line in the legend card.
    window_h = raw_config.get("window_h") or 6
    threshold_mm = raw_config.get("threshold_mm")
    if threshold_mm is None:
        threshold_mm = 10.0

    mode_label = (_t("Intensity") if mode == "mean" else _t("Probability"))

    # Active-parameter suffix shown once in the title, right after the
    # hazard name, same position/format for both hazards.
    caption = None
    if layer == "river":
        rp_tier = raw_config.get("rp_tier") or "rp10"
        # Surfaces the
        # selectable lead time in the title too, see ms-river-window's
        # own comment for the full "why" (makes clear WHICH lead time
        # this raster is rendering, the same way rain's own window/depth
        # are already always visible in its title).
        #
        # "≤{step_h}h", not "+{step_h}h": "+72h" would read as a
        # single-day snapshot AT that lead time, but the underlying data
        # is a real CUMULATIVE union (see tile_server.py's own
        # "ACCUMULATION SEMANTICS" comment), so "≤{step_h}h" is what
        # honestly describes the real cumulative window being rendered
        # ("days 1 through 3 combined", not "day 3 only").
        step_h = raw_config.get("river_step_h") or 72
        param_label = f"{rp_tier.upper()}, ≤{step_h}h"
        if raw_config.get("rp_tier_is_standin"):
            caption = _t("(not natively computed — RP10 used as an upper-bound estimate)")
    else:
        param_label = (f"{window_h}h" if mode == "mean"
                        else f"{window_h}h, >{threshold_mm:g}mm")
    title = f"{hazard_name} ({param_label}) — {title_suffix} ({mode_label})"

    if layer == "rain" and mode == "mean":
        # Real, window-scaled range (not a fixed "0.5mm"/"≥60mm", see
        # _rain_rate_range_for_window's own docstring). Plain numeric
        # f-strings, not run through _t(): same precedent as param_label's
        # own window_h/threshold_mm formatting just above, a unit-suffixed
        # number needs no language translation.
        lo, hi = _rain_rate_range_for_window(window_h)
        colors, unit_labels = _RAW_PRECIP_RATE_COLORS, (f"{lo:g}mm", f"≥{hi:g}mm")
    else:
        # River always lands here (mode forced to "probability" above).
        # Relative labels, not fixed percentages: both hazards' raw
        # probability rasters now use a DYNAMIC per-request scale (real
        # min/max of this cycle's own nonzero values, see
        # services/tile_server.py's _fetch_river_extent_
        # raster_tile/_colorize_precip_probability own comments), so a
        # literal "10%"/"≥80%" would misdescribe the ramp's real endpoints,
        # which now vary per forecast cycle.
        colors = _RAW_RIVER_PROB_COLORS if layer == "river" else _RAW_PRECIP_PROB_COLORS
        unit_labels = (_t("Fewest members agree"), _t("Most members agree"))

    return {
        "title": title,
        "compact_title": hazard_name,
        "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                 "background": f"linear-gradient(to right, {', '.join(colors)})"}),
        "compact_bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                         "background": f"linear-gradient(to right, {', '.join(colors)})"}),
        "labels": unit_labels,
        "caption": caption,
    }


def _legend_layer_info(layer, wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, tile_config, is_global,
                         raw_config=None):
    """The single shared source of "what does this layer's color mean" for
    BOTH the always-visible compact strip and the full expanded card, one
    definition per layer so the two views can never say something
    different about the same layer. Returns None when `layer` isn't
    actually active/visible right now."""
    # The HAZARDS
    # eye icon (ms-hazards-hidden-store) blanks every per-hazard MapLibre
    # RASTER/admin layer client-side (_hideAllHazardLayers in
    # maplibre_tiles.js), so the legend must not keep showing those layers'
    # own cards unchanged while the map itself goes blank. River/rain are
    # ALWAYS raster (no envelope alternative), so hazards_hidden always
    # suppresses their card. Wind/gust only render as a MapLibre raster
    # while tc_view_as=="raster", in "envelopes" mode they're drawn via a
    # completely separate Leaflet GeoJSON layer the eye icon never touches
    # at all, so their own legend card must stay exactly as-is in that
    # case (checked again just below, per-branch, not short-circuited
    # here).
    if layer in ("river", "rain") and tile_config.get("hazards_hidden"):
        return None
    if layer == "tracks":
        if not tracks_on:
            return None
        return {
            "title": "Tracks",
            "bar": html.Div([
                _legend_swatch_row("#ff0000", _t("Control member"), shape="line"),
                _legend_swatch_row("#1cabe2", _t("Ensemble member"), shape="line"),
            ]),
            # Compact strip shows ONE representative color, not both rows,
            # the ensemble member color (the vast majority of drawn tracks,
            # ~50 vs. 1-2 control members), full detail (both colors) is
            # one click away in the full card via "bar" above. A solid bar
            # filling the available width (matching the gradient bars'
            # own height/shape), not a short fixed-width line floating in
            # empty space, same pill-shaped, edge-to-edge treatment
            # Google's own compact weather-layer strip uses.
            "compact_bar": html.Div(style={"height": "10px", "borderRadius": "5px", "background": "#1cabe2"}),
            "labels": None, "caption": None,
        }
    if layer in ("wind", "gust"):
        on = wind_on if layer == "wind" else gust_on
        if not on:
            return None
        if tc_view_as == "raster":
            # See this function's own top-of-function comment, only
            # relevant while tc_view_as=="raster" (the eye icon never
            # touches the separate Leaflet envelope layer used in
            # "envelopes" mode).
            if tile_config.get("hazards_hidden"):
                return None
            return _legend_raster_info(layer, tile_config)
        # Envelopes. Must NOT always show
        # the full severity gradient in Global mode: _build_ms_envelope_
        # geojson never has a country to attribute per-member population
        # severity to there (no get_track_impacts
        # call at all without one), so EVERY Global-mode envelope actually
        # renders as style_envelopes' flat low-opacity fallback (WIND/GUST
        # at ~10% fillOpacity), never the gradient. Country Analysis mode
        # genuinely earns the gradient (some members have real severity
        # data, some don't).
        name = _t(_LEGEND_HAZARD_LABELS[layer])
        if is_global:
            color = WIND if layer == "wind" else GUST
            return {
                "title": name,
                "compact_title": name,
                "bar": _legend_swatch_row(color, name, shape="square"),
                "compact_bar": _legend_color_swatch(color),
                "labels": None,
                "caption": _t("Flat fill only — no single country to attribute per-member impact to in Global mode."),
            }
        gradient = ["#FFFF00", "#8B0000"] if layer == "wind" else ["#FFF3BF", "#D9480F"]
        return {
            "title": _t("{name} Severity", name=name),
            "compact_title": name,
            "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                     "background": f"linear-gradient(to right, {', '.join(gradient)})"}),
            "labels": (_t("Lower impact"), _t("Higher impact")),
            "caption": _t("Color = that ensemble member's own population impact. Faint fill = no impact data for that member."),
        }
    if layer in ("river", "rain"):
        on = river_on if layer == "river" else rain_on
        if not on:
            return None
        # When
        # Hazard Render Mode is "raw", applyHazardLayer in maplibre_tiles.js
        # (gated on `hazard_render_mode !== 'raw'`) hides the per-country
        # stats-based raster entirely and shows the GLOBAL raw river/precip
        # layer instead (applyGlobalRawConfig), this must NOT still
        # prefer _legend_raster_info's per-country card whenever it has
        # real data, which would describe a layer that isn't actually on
        # screen and omit the one that is. wind/gust's own branch above
        # already gets this right by checking tc_view_as == "raster"
        # first; mirrors that same real-render-state check here instead of
        # only checking "does real data exist" (a signal orthogonal to
        # which layer is actually being painted).
        if tile_config.get("hazard_render_mode") == "raw":
            return _legend_raw_flood_info(layer, raw_config)
        stats_info = _legend_raster_info(layer, tile_config)
        # Prefer the per-country stats-based layer when it has REAL data
        # (most specific/relevant to the current selection), but that path
        # is unconditionally unavailable in Global mode (tile_config's own
        # river_visible/rain_visible are always forced False there) and can
        # also genuinely have no real per-country pipeline coverage for this
        # date even with a country selected. The exact same checkbox also
        # drives the completely separate global raw river/precip raster (see
        # _legend_raw_flood_info's own comment), fall back to THAT when it's
        # what's actually painting the map, rather than showing nothing (or
        # a generic "no real data" placeholder) while a real layer is visible.
        if stats_info and stats_info.get("caption") != "No real data for this exact selection yet.":
            return stats_info
        raw_info = _legend_raw_flood_info(layer, raw_config)
        return raw_info or stats_info
    return None


def _legend_section(title, children):
    # Generic version for sections that don't participate in the compact
    # strip's "one active layer" concept (Facilities, a set of point
    # layers, not a single color-scale/gradient layer).
    return html.Div([
        dmc.Text(_t(title), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
        html.Div(children),
    ], style={"marginBottom": "14px"})


def _legend_section_from_info(info):
    children = [dmc.Text(_t(info["title"]), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6), info["bar"]]
    if info["labels"]:
        children.append(dmc.Group([
            dmc.Text(info["labels"][0], size="9px", c="dimmed", ff="monospace"),
            dmc.Text(info["labels"][1], size="9px", c="dimmed", ff="monospace"),
        ], justify="space-between", mt=2))
    if info["caption"]:
        children.append(dmc.Text(_t(info["caption"]), size="9px", c="dimmed", fs="italic", mt=6))
    return html.Div(children, style={"marginBottom": "14px"})


def _legend_compact_from_info(info):
    # No labels/caption here on purpose, the whole point of the compact
    # strip (always visible) is staying ONE short row
    # that never grows taller, not a second copy of the full card. Uses
    # "compact_title"/"compact_bar" when a layer defines a shorter one,
    # the full title, e.g. "Sustained Wind
    # Envelope", would truncate mid-word here, AND _legend_swatch_row's own
    # embedded text label (meant for the full card's stacked rows)
    # would duplicate the hazard name a second time right next to it and
    # wrap onto 2 lines, silently growing this row's height, falls back
    # to "title"/"bar" for layers short enough to not need a shorter version.
    title = info.get("compact_title", info["title"])
    bar = info.get("compact_bar", info["bar"])
    return dmc.Group([
        dmc.Text(_t(title), size="11px", fw=600, c="#455a64",
                   style={"whiteSpace": "nowrap", "flexShrink": 0}),
        html.Div(bar, style={"flex": 1, "minWidth": "50px"}),
    ], gap=10, wrap="nowrap", align="center", style={"width": "100%"})


def _map_legend():
    # Two-tier, like Google's own weather-layer legend: a compact ALWAYS-
    # VISIBLE strip by default, must NOT require
    # a click just to see anything at all, and must fire in
    # Global mode too (see _update_map_legend's own docstring), showing just
    # the most-recently-toggled-on layer's color bar, one click (chevron)
    # away from the full multi-layer card. Both tiers share the exact same
    # width as the Impact Summary panel (300px, right:16px) directly above
    # them, and the compact strip stays a single short row, no expanding
    # in height until the user actually asks for the full card.
    return html.Div([
        html.Div([
            DashIconify(icon="mdi:map-legend", width=14, color="#57707e"),
            html.Div(id="ms-legend-compact-body", style={"flex": 1, "marginLeft": "10px", "minWidth": 0}),
            html.Span("▾", style={"fontSize": "10px", "color": "#8ea0ab", "marginLeft": "8px"}),
        ], id="ms-legend-collapsed", n_clicks=0,
            style={**_PANEL_STYLE, **_LEGEND_ANCHOR, "width": "300px", "padding": "10px 14px",
                    "cursor": "pointer", "display": "flex", "alignItems": "center", "boxSizing": "border-box"}),
        html.Div([
            html.Div(dmc.Group([
                dmc.Text(_t("Legend"), fw=700, size="sm"),
                html.Span("▴", style={"fontSize": "10px", "color": "#8ea0ab"}),
            ], justify="space-between"), id="ms-legend-header", n_clicks=0,
                style={"cursor": "pointer", "marginBottom": "10px"}),
            html.Div(id="ms-legend-body"),
        ], id="ms-legend-card", style={**_PANEL_STYLE, **_LEGEND_ANCHOR, "width": "300px",
                                          "maxHeight": "50vh", "overflowY": "auto",
                                          "padding": "14px 16px", "display": "none", "boxSizing": "border-box"}),
    ])


@callback(
    Output("ms-legend-collapsed", "style"),
    Output("ms-legend-card", "style"),
    Input("ms-legend-collapsed", "n_clicks"),
    Input("ms-legend-header", "n_clicks"),
    prevent_initial_call=True,
)
def _toggle_map_legend(_collapsed_clicks, _header_clicks):
    # Header click always collapses back to the compact strip; clicking the
    # strip always expands, whichever was actually clicked decides the new
    # state (both exist in the DOM at once, one hidden via style), so
    # callback_context.triggered_id is the reliable signal, not a stored
    # open/closed flag.
    expanded = dash.callback_context.triggered_id == "ms-legend-collapsed"
    collapsed_style = {**_PANEL_STYLE, **_LEGEND_ANCHOR, "width": "300px", "padding": "10px 14px",
                         "cursor": "pointer", "boxSizing": "border-box",
                         "display": "none" if expanded else "flex", "alignItems": "center"}
    card_style = {**_PANEL_STYLE, **_LEGEND_ANCHOR, "width": "300px", "maxHeight": "50vh",
                   "overflowY": "auto", "padding": "14px 16px", "boxSizing": "border-box",
                   "display": "block" if expanded else "none"}
    return collapsed_style, card_style


# Mantine's SegmentedControl/etc aside, plain checkboxes don't tell you
# which ONE of several just changed, this is what lets the compact strip
# show "whichever layer the user just turned on" (Google's own convention)
# instead of an arbitrary fixed priority order. Fires on every hazard/
# tracks checkbox everywhere on the page (both _controls_global and
# _controls_zoom render the same ids, so this works in both modes, unlike
# the facility checkboxes, these are never missing from the DOM).
@callback(
    Output("ms-legend-last-layer", "data"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-tracks-on", "checked"),
    State("ms-legend-last-layer", "data"),
)
def _track_last_toggled_layer(wind_on, gust_on, river_on, rain_on, tracks_on, prev):
    id_to_layer = {"ms-wind-on": "wind", "ms-gust-on": "gust", "ms-river-on": "river",
                    "ms-rain-on": "rain", "ms-tracks-on": "tracks"}
    checked = {"ms-wind-on": wind_on, "ms-gust-on": gust_on, "ms-river-on": river_on,
                "ms-rain-on": rain_on, "ms-tracks-on": tracks_on}
    triggered = dash.callback_context.triggered_id
    if triggered is None:
        # Initial call, seed with whatever's already checked by default
        # (ms-tracks-on=True out of the box), highest-priority first.
        for layer in _LEGEND_LAYER_ORDER:
            tid = next(k for k, v in id_to_layer.items() if v == layer)
            if checked.get(tid):
                return layer
        return None
    if checked.get(triggered):
        return id_to_layer[triggered]
    # The tracked layer just got switched OFF, fall back to any other
    # still-checked layer rather than leaving the strip pointed at a layer
    # that no longer exists on the map.
    if id_to_layer.get(triggered) == prev:
        for layer in _LEGEND_LAYER_ORDER:
            tid = next(k for k, v in id_to_layer.items() if v == layer)
            if checked.get(tid):
                return layer
        return None
    return prev


@callback(
    Output("ms-legend-compact-body", "children"),
    Input("ms-legend-last-layer", "data"),
    Input("ms-tile-config-store", "data"),
    Input("ms-global-raw-config-store", "data"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-tracks-on", "checked"),
    Input("tc-view-as", "value"),
    Input("selected-country-store", "data"),
)
def _update_map_legend_compact(last_layer, tile_config, raw_config, wind_on, gust_on, river_on, rain_on, tracks_on,
                                  tc_view_as, countries):
    tile_config = tile_config or {}
    raw_config = raw_config or {}
    tc_view_as = tc_view_as or "envelopes"
    is_global = not countries
    args = (wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, tile_config, is_global, raw_config)
    # Must NOT show separate River/Rain cards with their own independent
    # min/max when the map is painting ONE real combined raster for them,
    # see _combined_raster_active/_legend_combined_raster_info's own
    # docstrings. River/rain always fold into the combined raster when
    # it's active; wind/gust only while tc_view_as=="raster" (envelopes
    # mode draws their own individual severity gradient instead, genuinely
    # unaffected by river/rain's own combining).
    combined_active = _combined_raster_active(tile_config)

    def _combined_covers(layer):
        return combined_active and (layer in ("river", "rain")
                                     or (layer in ("wind", "gust") and tc_view_as == "raster"))

    info = None
    if last_layer:
        info = _legend_combined_raster_info(tile_config) if _combined_covers(last_layer) \
            else _legend_layer_info(last_layer, *args)
    if not info:
        # Fallback covers a timing edge case (this callback and
        # _track_last_toggled_layer firing in a different order than
        # expected) by just picking the first genuinely active layer.
        for layer in _LEGEND_LAYER_ORDER:
            info = _legend_combined_raster_info(tile_config) if _combined_covers(layer) \
                else _legend_layer_info(layer, *args)
            if info:
                break
    if not info:
        return dmc.Text(_t("No layers active"), size="11px", c="dimmed")
    return _legend_compact_from_info(info)


@callback(
    Output("ms-legend-body", "children"),
    Input("ms-tile-config-store", "data"),
    Input("ms-global-raw-config-store", "data"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-tracks-on", "checked"),
    Input("tc-view-as", "value"),
    Input("selected-country-store", "data"),
    Input("ms-facility-visibility-store", "data"),
)
def _update_map_legend(tile_config, raw_config, wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, countries,
                         facilities_on):
    tile_config = tile_config or {}
    raw_config = raw_config or {}
    tc_view_as = tc_view_as or "envelopes"
    # ms-facility-visibility-store, NOT the ms-facility-{id}-on checkboxes
    # directly, those checkboxes only exist in
    # the DOM in Country Analysis mode, and a Dash callback never fires at
    # all while any of its Inputs is missing from the layout (not just
    # "reads as None"). Referencing them directly would mean this ENTIRE
    # callback, including the Tracks/envelope/raster sections that have
    # nothing to do with facilities, silently never fires in Global mode,
    # despite ms-tracks-on defaulting to checked=True.
    # This store is always present (see _mirror_facility_visibility).
    facilities_on = facilities_on or {}
    is_global = not countries
    sections = []

    # Storm Category deliberately NOT included, _CAT_COLORS
    # only colors the "Cat N" badge in the Active Storms side-panel list
    # (_cat_badge/_storm_row); nothing on the MAP itself is colored by
    # category (tracks are colored by ensemble-member type, control vs
    # regular member, not by storm category). A map legend should only
    # explain what's actually drawn on the map.
    # Must NOT show separate River/Rain cards, plus a dead generic "no data"
    # card for whichever hazard has none, with their own independent
    # min/max, when the map is painting ONE real combined raster
    # for those 2+ hazards; see _combined_raster_active/
    # _legend_combined_raster_info's own docstrings. One combined card
    # replaces every raster-hazard card the combined raster actually
    # covers, river/rain always when combined, wind/gust only while
    # tc_view_as=="raster" (envelopes mode draws their own individual
    # severity gradient instead, genuinely unaffected by river/rain's own
    # combining).
    combined_active = _combined_raster_active(tile_config)
    if combined_active:
        combined_info = _legend_combined_raster_info(tile_config)
        if combined_info:
            sections.append(_legend_section_from_info(combined_info))

    for layer in _LEGEND_LAYER_ORDER:
        if combined_active and (layer in ("river", "rain")
                                  or (layer in ("wind", "gust") and tc_view_as == "raster")):
            continue
        info = _legend_layer_info(layer, wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, tile_config,
                                     is_global, raw_config)
        if info:
            sections.append(_legend_section_from_info(info))

    # Facilities, must NOT show
    # unconditionally: each facility type only actually renders on the
    # map while its OWN ms-facility-{id}-on checkbox is checked (see
    # _register_ms_facility_layer's clientside fetch, gated on exactly that
    # checkbox, all four default unchecked/off). Only include the ones
    # genuinely on screen, and drop the whole section when none are.
    facility_rows = []
    if facilities_on.get("schools"):
        facility_rows.append(_legend_swatch_row("#ADD8E6", _t("Schools"), shape="circle"))
    if facilities_on.get("health"):
        facility_rows.append(_legend_swatch_row("#90EE90", _t("Health Centers"), shape="circle"))
    if facilities_on.get("shelters"):
        facility_rows.append(_legend_swatch_row("#E91E8C", _t("Shelters"), shape="circle"))
    if facilities_on.get("wash"):
        facility_rows.append(_legend_swatch_row("#40E0D0", _t("WASH Facilities"), shape="circle"))
    if facility_rows:
        facility_rows.append(dmc.Text(_t("Darker red = higher hazard probability at that facility."),
                                        size="9px", c="dimmed", fs="italic", mt=2))
        sections.append(_legend_section("Facilities", facility_rows))

    return sections


# NOTE: the two GLOBAL, country/storm-INDEPENDENT raw hazard layers (raw
# precip-rate raster + raw river flood-extent raster, see
# services/tile_server.py's "Global raw precipitation-rate endpoints"/
# "Global raw river endpoints" sections) deliberately have no
# small standalone floating panel of their own, with dedicated raw-layer-only
# checkboxes, they
# reuse the EXISTING ms-river-on ("River Flooding")/ms-rain-on ("Rainfall")
# checkboxes in _flood_hazards_family below as their single on/off control
# (see that function's own comment for the full reuse rationale, and
# _build_global_raw_config further down for the config resolution that reads
# those same two checkbox ids as an independent additional consumer,
# alongside a new flood-view-as Mean/Probability toggle also added there).


def _alert_email_list_modal():
    """Lists a storm's real available Alert emails (ALERT_SENT_LOG), opened
    by clicking a storm row's email icon (_storm_row). One entry per real
    (forecast_time, country_code) pair; clicking an entry opens its full
    HTML in _alert_email_modal below (_open_alert_email_detail). A Warning
    email has no equivalent list step, see _warning_email_modal below
    (_open_warning_email_detail) for why."""
    return dmc.Modal(
        id="alert-email-list-modal", opened=False, size="md", title=_t("Alert Emails"),
        centered=True, radius="lg", padding="lg", styles=_MODAL_PANEL_STYLES,
        overlayProps={"backgroundOpacity": 0.35, "blur": 3},
        children=html.Div(id="alert-email-list-content"),
    )


def _alert_email_modal():
    # Real emails are full standalone HTML documents (own inline styling,
    # embedded map image), an iframe respects that instead of re-rendering
    # the content as Dash components. `src` (not `srcDoc`) points at the real
    # /alert-email/<track_id>/<forecast_time>/<country_code> Flask route
    # (app.py's serve_alert_email). Must NOT embed
    # the whole ~700KB HTML directly as this iframe's srcDoc AND separately
    # re-encode it into a giant data: URI for "Open in new tab", the data:
    # URI, once percent-encoded, can balloon past practical browser limits
    # for a fresh top-level navigation, opening a blank
    # tab that only renders after a manual refresh. A real
    # HTTP GET via a normal URL has no such size quirk, and both the iframe
    # and the link just point at the SAME URL instead of duplicating the
    # content two different ways.
    #
    # "Open in new tab" is a plain html.A, not dmc.Anchor, dmc.Anchor's
    # href attribute can silently fail to update in
    # the browser despite Dash's own callback response containing the
    # correct value (a dash-mantine-components quirk, not a Dash graph/logic
    # bug), so this avoids dmc.Anchor entirely for this link.
    return dmc.Modal(
        # The real email's own content is ~640px wide (its <table style=
        # "max-width:640px"> wrapper): "xl" gives it comfortable margin
        # inside the modal instead of squeezing against a narrower "lg".
        id="alert-email-modal", opened=False, size="xl", title=_t("Alert Email"),
        centered=True, radius="lg", padding="lg", styles=_MODAL_PANEL_STYLES,
        overlayProps={"backgroundOpacity": 0.35, "blur": 3},
        children=html.Div([
            # marginTop separates this from the Modal's own title/divider,
            # without it the link would sit almost flush against that
            # divider line.
            html.A(_t("Open in new tab ↗"), id="alert-email-new-tab-link", href="", target="_blank",
                    style={"display": "block", "marginTop": "10px", "marginBottom": "10px",
                           "fontSize": "12px", "color": "#1c7ed6"}),
            html.Iframe(id="alert-email-iframe", src="",
                         style={"width": "100%", "height": "70vh", "border": "1px solid #eef2f5",
                                "borderRadius": "8px"}),
        ]),
    )


def _warning_email_modal():
    # Same real-standalone-HTML-via-iframe reasoning as _alert_email_modal
    # above, `src` here points at /warning-email/<track_id>/<forecast_date>
    # (app.py's serve_warning_email) instead. No separate list modal (unlike
    # Alert): a Warning is always exactly one shared email per (track_id,
    # forecast_date) covering every affected country at once, never one per
    # country, so there's never a real choice to list -- see
    # _open_warning_email_detail's own docstring for the full reasoning.
    return dmc.Modal(
        id="warning-email-modal", opened=False, size="xl", title=_t("Warning Email"),
        centered=True, radius="lg", padding="lg", styles=_MODAL_PANEL_STYLES,
        overlayProps={"backgroundOpacity": 0.35, "blur": 3},
        children=html.Div([
            html.A(_t("Open in new tab ↗"), id="warning-email-new-tab-link", href="", target="_blank",
                    style={"display": "block", "marginTop": "10px", "marginBottom": "10px",
                           "fontSize": "12px", "color": "#1c7ed6"}),
            html.Iframe(id="warning-email-iframe", src="",
                         style={"width": "100%", "height": "70vh", "border": "1px solid #eef2f5",
                                "borderRadius": "8px"}),
        ]),
    )


def _map_stack():
    # Real MapLibre (bottom) + Leaflet (top) dual-layer map, same stack as
    # layouts/panels.py's center_panel, adapted for map-shell's full-bleed
    # shell: the map fills the entire viewport ("100%" here, not panels.py's
    # header-offset "calc(100vh - 147px)") behind the floating panels, rather
    # than sitting in one column of a 2-column grid. Hazard/tile/facility
    # DATA layers are deliberately left as empty placeholders, that's later,
    # separate follow-up work; this is purely the real interactive base map
    # replacing the old synthetic canvas.
    return html.Div(
        [
            # MapLibre canvas, will render tile/admin hazard layers underneath
            # Leaflet once that data is wired; for now just the basemap.
            html.Div(
                id="maplibre-container",
                **{"data-mapbox-token": mapbox_token or ""},
                style={
                    "height": "100%", "width": "100%",
                    "position": "absolute", "top": 0, "left": 0, "zIndex": 0,
                },
            ),
            # Leaflet map, owns zoom/pan/basemap-switching and (later) tracks,
            # envelopes, and facility overlays. opacity=0 tile layers below are
            # transparent; Leaflet shows nothing itself, MapLibre provides the
            # actual basemap tiles. baselayerchange -> swapMaplibreBasemap in
            # maplibre_tiles.js (see sync_basemap_select clientside_callback).
            #
            # url=_BLANK_TILE_URL: dl.TileLayer's own default `url` is the
            # real OpenStreetMap
            # tile server, so leaving it unset would mean the currently-selected
            # opacity=0 layer silently fetches real (never-shown) OSM
            # tiles on every pan/zoom, duplicate network traffic against the
            # tiles MapLibre is ALSO fetching for the real visible basemap,
            # and unnecessary load against OSM's own public tile servers. A
            # 1x1 transparent data: URI needs no network request at all and
            # still gives the LayersControl a real layer object to track
            # selection/fire baselayerchange, and the attribution text still
            # renders (Leaflet's attribution control reads the layer's own
            # `attribution` option, independent of whether any tile loaded).
            dl.Map(
                [
                    dl.LayersControl(
                        [
                            dl.BaseLayer(
                                dl.TileLayer(
                                    url=_BLANK_TILE_URL,
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
                                checked=bool(mapbox_token),
                            ),
                            dl.BaseLayer(
                                dl.TileLayer(url=_BLANK_TILE_URL, opacity=0, attribution='© <a href="https://carto.com/attributions">CARTO</a><br>' + _UN_DISCLAIMER),
                                name="CartoDB Light",
                                checked=not mapbox_token,
                            ),
                            dl.BaseLayer(
                                dl.TileLayer(url=_BLANK_TILE_URL, opacity=0, attribution='© <a href="https://carto.com/attributions">CARTO</a> © <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a><br>' + _UN_DISCLAIMER),
                                name="CartoDB Dark",
                                checked=False,
                            ),
                            dl.BaseLayer(
                                dl.TileLayer(url=_BLANK_TILE_URL, opacity=0, attribution='Tiles &copy; <a href="https://services.arcgisonline.com/">Esri</a> &mdash; Source: Esri, Maxar, Earthstar Geographics<br>' + _UN_DISCLAIMER),
                                name="Satellite",
                                checked=False,
                            ),
                        ],
                        position="topright",
                    ),
                    # Track/envelope/facility layers, data/key populated reactively
                    # by _load_ms_tracks_and_envelopes (server callback, keyed off
                    # ms-tile-config-store) and the ms-{layer}-json clientside
                    # facility fetches (direct browser -> tile server /geojson/
                    # facilities/..., same no-Dash-round-trip pattern as
                    # callbacks/overlays.py) further down this file. style/
                    # onEachFeature/pointToLayer mirror layouts/panels.py's own
                    # dl.GeoJSON(...) calls for these exact layers.
                    # Envelopes BEFORE tracks (Leaflet stacks later-added
                    # layers on top), tracks must always render above the
                    # wind envelope polygons, not underneath them.
                    dl.GeoJSON(id="ms-envelopes-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               style=style_envelopes, onEachFeature=tooltip_envelopes),
                    dl.GeoJSON(id="ms-tracks-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               style=style_tracks, onEachFeature=tooltip_tracks),
                    # cluster=True is deliberately NOT used here, grouping
                    # facility points into cluster bubbles doesn't fit how this
                    # app wants hazard exposure visualized (each facility's
                    # own color/radius already encodes its individual hazard
                    # probability; a cluster bubble collapses that
                    # per-facility signal into a plain count). Plain
                    # individual circleMarkers instead.
                    dl.GeoJSON(id="ms-schools-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_schools),
                    dl.GeoJSON(id="ms-health-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_health),
                    dl.GeoJSON(id="ms-shelters-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_shelters),
                    dl.GeoJSON(id="ms-wash-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_wash),
                    # These four hold empty GeoJSON, MapLibre renders the actual
                    # tiles and their tooltips (see maplibre_tiles.js's own
                    # _buildTileTooltip). They exist only as future Dash state
                    # containers for hideout props (tile coloring), not for
                    # rendering.
                    dl.GeoJSON(id="ms-population-tiles-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False, hideout={"hidden": True}),
                    dl.GeoJSON(id="ms-population-admin-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False, hideout={"hidden": True}),
                    dl.GeoJSON(id="ms-probability-tiles-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False, hideout={"hidden": True}),
                    dl.GeoJSON(id="ms-probability-admin-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False, hideout={"hidden": True}),
                    dl.FullScreenControl(),
                    dl.LocateControl(locateOptions={"enableHighAccuracy": True}),
                ],
                id="ms-main-map",
                center=[map_config.center["lat"], map_config.center["lon"]],
                zoom=map_config.zoom,
                viewport={"center": [map_config.center["lat"], map_config.center["lon"]], "zoom": map_config.zoom},
                scrollWheelZoom=True,
                preferCanvas=True,
                # "moveend" (in addition to panels.py's own "move") specifically
                # because the _nudge_map_ready programmatic viewport update below
                # fires 'moveend' but not always 'move' (Leaflet only fires
                # 'move' repeatedly DURING an animated transition; a direct
                # setView-driven jump can skip straight to 'moveend'), real
                # user mouse-drag panning fires both, which is why this was easy
                # to miss when the only earlier test was a manual drag.
                eventHandlers={"load": sync_maplibre_on_load, "move": sync_maplibre_on_move,
                                "moveend": sync_maplibre_on_move},
                style={
                    "height": "100%", "width": "100%",
                    "position": "absolute", "top": 0, "left": 0, "zIndex": 1,
                    "background": "transparent",
                },
            ),
        ],
        style={"position": "absolute", "inset": 0, "width": "100%", "height": "100%"},
    )


# dl.Map's "load" eventHandler
# (sync_maplibre_on_load, which sets window._leaflet_maps['main-map'], the
# thing everything else, including basemap switching, is gated on) races
# dash-leaflet's own async component bundle and does not reliably fire on a
# fresh page load. "move"/"moveend" also do not reliably fire from a
# Python-driven `viewport` prop update, dash-leaflet's internal reaction to
# a prop-diffed viewport does not dispatch real Leaflet DOM events the way
# genuine interaction does.
# What DOES reliably work: a real click on Leaflet's own native
# zoom +/- control buttons, same as a scroll-wheel zoom, so this simulates
# exactly that, entirely client-side, shortly after mount: zoom in then back
# out (net zero visual change) via the real buttons Leaflet already renders,
# which fires real 'move'/'zoomend' events through the same code path actual
# user interaction uses.
clientside_callback(
    """
    function(n) {
        var zoomIn = document.querySelector('.leaflet-control-zoom-in');
        var zoomOut = document.querySelector('.leaflet-control-zoom-out');
        if (zoomIn && zoomOut) {
            zoomIn.click();
            setTimeout(function () { zoomOut.click(); }, 120);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("map-init-nudge", "disabled"),
    Input("map-init-nudge", "n_intervals"),
    prevent_initial_call=True,
)


@callback(
    Output("ms-last-updated", "children"),
    Input("ms-metadata-refresh-interval", "n_intervals"),
    prevent_initial_call=False,
)
def _update_last_updated(_n):
    """Real "Last Updated" timestamp, same source/format as dashboard.py's
    own update_last_updated_header, so both pages agree on what "last
    updated" means. prevent_initial_call=False so this fires immediately on
    load too, not just after the first 15-minute interval."""
    try:
        latest_time = get_latest_forecast_time_overall()
        return latest_time.strftime("%b %d, %Y %H:%M UTC") if latest_time else "N/A"
    except Exception:
        return "N/A"


# Real fix for a real staleness bug: _LATEST_FORECAST_TIME/_DEFAULT_FORECAST_DATE/
# _DEFAULT_FORECAST_RUN (module-import-time globals, used as this callback's
# own default ceiling params below) never change again for the rest of the
# process's life, so topbar-date/topbar-time stayed stuck offering only
# whatever was "latest" at process start, even as _update_last_updated right
# above correctly showed newer real cycles the whole time (same underlying
# query, just called live on every interval instead of once at import).
# Same ms-metadata-refresh-interval/15-min cadence as _update_last_updated,
# and same intent (correct any staleness immediately on load too, not just
# after the first tick) -- but topbar-time's own `data` already has a
# plain-Output writer (_guard_future_forecast_run below), so this one must
# declare allow_duplicate=True, which Dash requires prevent_initial_call=True
# (or this special value) for, since duplicate-output firing order on the
# very first load isn't otherwise guaranteed. Deliberately does NOT touch
# topbar-time's own `value` here -- only widens/narrows what's selectable
# (maxDate, the dimmed/enabled data) to reflect newly-available real cycles;
# _guard_future_forecast_run is what snaps an already-selected value back if
# it's ever actually invalid, so the two callbacks stay cleanly separated by
# concern rather than duplicating each other's job.
@callback(
    Output("topbar-date", "maxDate"),
    Output("topbar-time", "data", allow_duplicate=True),
    Input("ms-metadata-refresh-interval", "n_intervals"),
    State("topbar-date", "value"),
    prevent_initial_call="initial_duplicate",
)
def _refresh_forecast_ceiling(_n, current_date):
    latest_time, live_date, live_run = _live_forecast_ceiling()
    if live_date is None:
        return dash.no_update, dash.no_update
    data = _time_options_for_date(current_date, latest_time, live_date, live_run)
    return live_date, data


# A function (not a static value), Dash's page router calls this with any
# URL query params as kwargs, e.g. /?lang=es -> layout(lang="es").
# Setting the module-level _LANG here, before building anything, is what lets
# every _t() call made while constructing this same tree pick up the right
# language, without threading a `lang` argument through every builder above.
def layout(lang="en", zoom_countries=None, open_breakdown=None, **kwargs):
    global _LANG
    _LANG = lang if lang in _TRANSLATIONS else "en"
    # zoom_countries/open_breakdown: set by the Full Impact Breakdown's own
    # "open in new tab" link (_breakdown_new_tab_href), a comma-separated,
    # URL-encoded country list plus a flag to auto-open the modal, so the
    # new tab lands exactly where the original one was instead of a blank
    # Global view.
    initial_countries = [unquote(c) for c in zoom_countries.split(",") if c] if zoom_countries else None
    should_open_breakdown = bool(open_breakdown) and bool(initial_countries)
    # Live per-request default (see _resolve_default_forecast_date_run's own
    # docstring): layout() is called fresh by Dash's page router on every
    # page load, so this is what actually closes the staleness gap
    # _refresh_forecast_ceiling below only partially covers -- that
    # callback live-widens what's SELECTABLE in the topbar every 15 min,
    # but deliberately never touches the already-rendered SELECTED value
    # for a tab that's already open. This is what makes a *new* page load
    # itself reflect real data within 15 minutes too, without ever needing
    # a process restart.
    _live_default_date, _live_default_run = _resolve_default_forecast_date_run()
    return html.Div([
        dcc.Store(id="selected-country-store", data=initial_countries),
        # Written by _select_storm below when the clicked storm has no real
        # country impact yet (still at sea, empty "countries"): a Leaflet
        # viewport dict flying the map to that storm's own real track
        # extent instead, since there's no country center to fly to and a
        # click on one of these storms would otherwise be a dead no-op.
        # Kept separate from selected-country-store/ms-main-map's other
        # viewport writer (_fly_map_to_selection) rather than merged into
        # either, so a storm WITH countries is untouched by this at all
        # (no risk of two callbacks racing to set the same viewport from
        # one click), see _select_storm's own comment for the full "why".
        dcc.Store(id="ms-storm-flyto-store", data=None),
        # Real hazard tile-config bridge (see the "Hazard tile-config bridge"
        # section near the bottom of this file's callbacks): assembled by a
        # reactive Python callback (country/storm selection + all 5 hazard
        # controls) and pushed to MapLibre via the existing
        # dash_clientside.maplibre.updateTileConfig bridge already defined in
        # components/map/maplibre_tiles.js. ms-prefixed, distinct from
        # layouts/panels.py's own "maplibre-tile-config-store" (same running
        # app, see this file's own module docstring for the ID convention).
        # Declared standalone, not nested inside _ms_loading_badge()'s
        # dcc.Loading (see _ms_loading_badge's own comment on why
        # target_components doesn't reliably work here), the loading
        # badge doesn't depend on this store's position in the tree at all.
        dcc.Store(id="ms-tile-config-store", data={}),
        # Mirrors the 4 ms-facility-{id}-on checkboxes (see
        # _mirror_facility_visibility below), always present regardless of
        # mode, unlike the checkboxes themselves, which only exist in the
        # DOM in Country Analysis mode (_controls_zoom's Infrastructure
        # section; _controls_global has no Infrastructure section at all).
        # _update_map_legend reads THIS store, not the checkboxes directly,
        # for exactly that reason, see that callback's own docstring.
        dcc.Store(id="ms-facility-visibility-store",
                   data={"schools": False, "health": False, "shelters": False, "wash": False}),
        # Same root cause as
        # ms-facility-visibility-store just above (see _mirror_facility_
        # visibility's own comment): ms-hazard-render-mode (the Hazard tab's
        # Raw/Probability/Classification switch) only exists in the DOM in
        # Country Analysis mode, _build_hazard_tile_config/_build_global_
        # raw_config/_sync_tc_view_as must NOT read it AS A DIRECT Input, or
        # Dash never fires ANY of those 3 callbacks at all in a session that
        # starts in (or has never left) Global mode, since Dash refuses to
        # fire a callback while ANY of its Inputs references a component
        # missing from the CURRENT rendered layout, not just resolves that
        # one value to None. Mirrored into this
        # always-present store (only fires when the switch DOES exist, i.e.
        # Country Analysis mode) decouples the two, exactly like the
        # facility store already does.
        dcc.Store(id="ms-hazard-render-mode-store", data="probability"),
        # Which hazard/tracks layer to show in the compact legend strip,
        # written by _track_last_toggled_layer, whichever checkbox the user
        # most recently turned ON (falls back to any other still-checked
        # layer if that one gets turned back off). None = nothing active.
        dcc.Store(id="ms-legend-last-layer", data=None),
        # Written only by a clientside debounce wrapper around the 4 hazard
        # sliders (~200ms after the drag settles), NOT a real Output of any
        # Python callback despite being declared as one (always returns
        # no_update there); the deferred write happens via
        # dash_clientside.set_props from a setTimeout. Checkboxes/segmented
        # control are cheap and un-debounced, so they don't go through this.
        dcc.Store(id="ms-slider-debounce-store", data=0),
        # Dummy sink for the clientside bridge below (Output required by
        # clientside_callback's API; nothing ever reads it), deliberately a
        # NEW ms-prefixed store rather than reusing pages/dashboard.py's own
        # Output('maplibre-container', 'data-config', ...) dummy-sink pattern,
        # to keep this page's callback graph fully independent of dashboard.py's.
        dcc.Store(id="ms-tile-config-applied-store", data=0),
        # Purely a client-side visual override, clicking the HAZARDS label
        # (see _command_bar) toggles this, which forces every hazard's
        # MapLibre layers invisible without touching the real checkboxes/
        # ms-tile-config-store at all, so un-hiding restores exactly what
        # was configured (see setHazardsHiddenOverride in maplibre_tiles.js).
        # A quick "just show me the base layer" preview, not a persisted
        # setting, any subsequent real hazard/checkbox change naturally
        # re-renders from the real config and drops this override, which is
        # the intended behavior for a temporary hide, not a bug.
        dcc.Store(id="ms-hazards-hidden-store", data=False),
        # Resolved config for the two GLOBAL, country/storm-independent raw
        # hazard layers (precip-raw raster + river-raw raster), see
        # _build_global_raw_config below, driven by the existing ms-river-on/
        # ms-rain-on checkboxes + flood-view-as toggle in
        # _flood_hazards_family (no dedicated checkboxes of its own).
        # Entirely separate from ms-tile-config-store/_build_hazard_tile_config
        # on purpose: those are gated on a selected country ("if not
        # countries: return ..." early exit) and this must render regardless
        # of country selection.
        dcc.Store(id="ms-global-raw-config-store", data={}),
        # Dummy sink for the clientside bridge below (same "Output required by
        # the API, nothing reads it" pattern as ms-tile-config-applied-store).
        dcc.Store(id="ms-global-raw-config-applied-store", data=0),
        # The map's own hover tooltips (tracks/envelopes/facilities/tile-stats/
        # raw-layer) need their own translation path:
        # tooltip_tracks/tooltip_envelopes/tooltip_schools/
        # tooltip_health/tooltip_shelters/tooltip_wash (components/map/
        # javascript.py) and _buildTileTooltip/_buildRawLayerTooltip
        # (assets/maplibre_tiles.js) are raw browser-side JS with zero access
        # to _LANG/_TRANSLATIONS/_t(), those are server-side-only Python
        # constructs, never serialized to the client. _MAP_TOOLTIP_
        # TRANSLATIONS (below _TRANSLATIONS) is a small, dedicated vocabulary
        # covering exactly what those 8 functions need, baked into this
        # Store ONCE at layout build time (same "computed from the request's
        # own _LANG, no callback needed" pattern _TRANSLATIONS itself already
        # uses), the clientside bridge just below copies it onto
        # window.AOTS_MAP_I18N, which those JS functions now read through a
        # shared _mapT() lookup helper.
        dcc.Store(id="ms-map-i18n-store", data=_MAP_TOOLTIP_TRANSLATIONS.get(_LANG, {})),
        dcc.Store(id="ms-map-i18n-applied-store", data=0),
        # One-shot, fires ~500ms after mount, see _nudge_map_ready below for
        # why this exists (basemap switching silently does nothing on a
        # fresh load until this fires or the user manually pans/zooms).
        dcc.Interval(id="map-init-nudge", interval=500, n_intervals=0, max_intervals=1),
        # Real "Last Updated" refresh, same 15-min cadence and
        # get_latest_forecast_time_overall() source as dashboard.py's own
        # update_last_updated_header, under an "ms-"-prefixed id so it
        # doesn't collide with layouts/panels.py's live
        # "metadata-refresh-interval" in this same running app.
        dcc.Interval(id="ms-metadata-refresh-interval", interval=15 * 60 * 1000, n_intervals=0),
        _map_stack(),
        _topbar(initial_countries=initial_countries, default_date=_live_default_date, default_run=_live_default_run),
        _controls_panel(),
        _command_bar(),
        _impact_panel(initial_countries=initial_countries, open_breakdown=should_open_breakdown,
                        default_date=_live_default_date, default_run=_live_default_run),
        _bottom_left_controls(),
        _map_legend(),
        _map_disclaimer(),
        _alert_email_list_modal(),
        _alert_email_modal(),
        _warning_email_modal(),
        _compact_footer(),
    ], style={"position": "relative", "width": "100%", "height": "100vh", "overflow": "hidden", "background": "#cfe3ee"})


# ---------------------------------------------------------------------------
# Callbacks, layout/state only, no data
# ---------------------------------------------------------------------------
# The pill UI (basemap-select) is the only thing the user interacts with,
# there's no separate "set the basemap" API to call, so instead this finds
# the matching native Leaflet LayersControl radio input (rendered into
# .leaflet-control-layers-base by dl.LayersControl in _map_stack) and clicks
# it. That fires Leaflet's own 'baselayerchange' event, which
# maplibre_tiles.js already listens for (swapMaplibreBasemap) to do the real
# tile swap, no new basemap-swapping JS needed, just reusing what's already
# there. The native control itself is visually hidden (assets/
# map_shell_concept.css) so the pill reads as the only basemap control.
clientside_callback(
    """
    function(basemapId) {
        var nameByValue = {
            "cartodb-light": "CartoDB Light",
            "cartodb-dark": "CartoDB Dark",
            "satellite": "Satellite",
            "osm": window._aots_mapbox_token ? "Mapbox Light" : "OpenStreetMap",
        };
        var targetName = nameByValue[basemapId];
        if (!targetName) { return basemapId; }
        var inputs = document.querySelectorAll(".leaflet-control-layers-base input[type=radio]");
        for (var i = 0; i < inputs.length; i++) {
            var input = inputs[i];
            var label = input.closest("label");
            var text = label ? label.textContent.trim() : "";
            if (text === targetName) {
                if (!input.checked) { input.click(); }
                break;
            }
        }
        return basemapId;
    }
    """,
    Output("basemap-select", "className"),
    Input("basemap-select", "value"),
)


@callback(
    Output("controls-body", "children"),
    Input("topbar-mode", "value"),
    Input("selected-country-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
)
def _switch_mode_content(mode, countries, date, run):
    # selected-country-store must be an Input, not State, picking a
    # different/additional country while ALREADY in zoom mode doesn't
    # necessarily change topbar-mode itself (see _country_selected's
    # no_update guard), so this needs to react to country changes directly
    # too, or Active Storms/Tropical Cyclone stay scoped to whatever country
    # was selected when zoom mode was first entered. topbar-date/topbar-time
    # are Inputs too now, for the same reason, _hurricane_family's
    # has_storms check is now reactive to the selected date/run (see its own
    # docstring), so changing the date without changing the country (or
    # applying a Demo Scenario, which sets both at once) needs to re-render
    # this too, or Sustained Wind/Gust/Tracks would stay stuck disabled/
    # enabled from whatever date was selected when zoom mode was first entered.
    if mode == "zoom":
        return _controls_zoom(countries=countries, date=date, run=run)
    return _controls_global(date=date, run=run)


@callback(Output("command-bar", "style"), Input("topbar-mode", "value"))
def _toggle_command_bar(mode):
    return _COMMAND_BAR_STYLE if mode == "zoom" else {**_COMMAND_BAR_STYLE, "display": "none"}


# No mode-based show/hide, Demo Scenarios stays visible in
# BOTH Global and Country Analysis mode. Hiding it in Country Analysis
# would just leave an inconsistent-looking gap in that row; every preset
# already sets its own mode via _apply_demo_scenario regardless of where
# it's clicked from, so there's no correctness reason to hide it either.


# clientside, pure style toggle, no server data dependency.
clientside_callback(
    """
    function(view) {
        if (view === "hazard") {
            return [{"display": "none"}, {"display": "block"}];
        }
        return [{"display": "block"}, {"display": "none"}];
    }
    """,
    Output("controls-exposure-pane", "style"),
    Output("controls-hazard-pane", "style"),
    Input("controls-view-toggle", "value"),
    prevent_initial_call=True,
)


# Tropical Cyclone / Flood Hazards headers are pure click-to-collapse now
# (chevron, no switch), same pattern as Infrastructure below, just against
# the ms-{fam}-body id those two already had rather than body-{key}.
for _fam in ("hurricane", "flood"):
    @callback(
        Output(f"ms-{_fam}-body", "style"),
        Input(f"head-{_fam}", "n_clicks"),
        State(f"ms-{_fam}-body", "style"),
        prevent_initial_call=True,
    )
    def _toggle_family(_n, current_style):
        is_hidden = bool(current_style) and current_style.get("display") == "none"
        return {"marginTop": "16px"} if is_hidden else {"marginTop": "16px", "display": "none"}


# Infrastructure and Exposure's Context Data both use the generic
# body-{key}/head-{key} id scheme, the rest of Exposure lost its header
# entirely (redundant with the switch above it), and Tropical Cyclone/Flood
# Hazards keep their own pre-existing ms-{fam}-body ids (handled in the loop
# above).
for _sec in ("infra", "context"):
    @callback(
        Output(f"body-{_sec}", "style"),
        Input(f"head-{_sec}", "n_clicks"),
        State(f"body-{_sec}", "style"),
        prevent_initial_call=True,
    )
    def _toggle_section(_n, current_style):
        is_hidden = bool(current_style) and current_style.get("display") == "none"
        return {"marginTop": "16px"} if is_hidden else {"marginTop": "16px", "display": "none"}


# Pattern-matching (dash.MATCH) since the Admin Level 1 breakdown renders
# once per selected country, a fixed id per section, like the loop above,
# doesn't work when the number of sections varies with the selection.
@callback(
    Output({"type": "admin1-body", "country": dash.MATCH}, "style"),
    Input({"type": "admin1-head", "country": dash.MATCH}, "n_clicks"),
    State({"type": "admin1-body", "country": dash.MATCH}, "style"),
    prevent_initial_call=True,
)
def _toggle_admin1(_n, current_style):
    is_hidden = bool(current_style) and current_style.get("display") == "none"
    # Must match the static style in _admin1_section exactly (width/maxWidth/
    # overflowY), this callback replaces the WHOLE style dict on every
    # click, so any fields missing here were silently lost the moment the
    # section was toggled even once.
    base = {"marginTop": "10px", "width": "100%", "maxWidth": "100%", "overflowX": "auto", "overflowY": "visible"}
    return base if is_hidden else {**base, "display": "none"}


# Same n_clicks Input as _toggle_admin1 above (a SEPARATE callback, not
# folded into that one: they write to different Output properties, style
# vs children, so Dash runs them independently with no conflict), but this
# one does the real work: the interactive Admin Level 1 section starts
# EMPTY (see _admin1_section's own docstring) and this callback lazily
# fills it in on the user's FIRST click only, using the country/date/run/
# hz/breakdown params _admin1_section stashed in the sibling admin1-params
# Store at render time. `current_children` (State, not Input, so reading it
# doesn't itself retrigger this callback) is the "already loaded" check: a
# non-empty list means a previous click already fetched and rendered the
# real content, in which case this returns dash.no_update rather than
# paying a second real HTTP+Snowflake round trip for a section that's just
# being collapsed/re-expanded, not freshly opened. Because
# _impact_breakdown_content rebuilds the WHOLE admin1_sections tree (fresh,
# empty children) on every hazard/threshold/date change while the modal is
# open, "already loaded" naturally resets alongside every other piece of
# this modal's own state on a genuine data-affecting change, so a click
# after such a change correctly re-fetches instead of showing stale content.
@callback(
    Output({"type": "admin1-body", "country": dash.MATCH}, "children"),
    Input({"type": "admin1-head", "country": dash.MATCH}, "n_clicks"),
    State({"type": "admin1-params", "country": dash.MATCH}, "data"),
    State({"type": "admin1-body", "country": dash.MATCH}, "children"),
    prevent_initial_call=True,
)
def _load_admin1(_n, params, current_children):
    if current_children:
        return dash.no_update
    if not params:
        return dash.no_update
    return _admin1_body_content(params["country"], params["date"], params["run"], params["hz"], params["breakdown"])


# Fixed ids (not dash.MATCH), unlike Admin Level 1, there's only ever ONE
# Threshold sensitivity preview section per breakdown, so no pattern-matching
# is needed. Doesn't exist at all when expanded=True (the print page), see
# _hazard_threshold_preview.
@callback(
    Output("body-threshold-preview", "style"),
    Output("head-threshold-preview", "children"),
    Input("head-threshold-preview", "n_clicks"),
    State("body-threshold-preview", "style"),
    State("head-threshold-preview", "children"),
    prevent_initial_call=True,
)
def _toggle_threshold_preview(_n, current_style, current_children):
    is_hidden = bool(current_style) and current_style.get("display") == "none"
    base_style = {"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(190px, 1fr))", "gap": "20px"}
    new_style = base_style if is_hidden else {**base_style, "display": "none"}
    # current_children[0] is the label text.Div, [1] is the chevron span,
    # only the chevron direction changes, the label stays put.
    new_chevron = html.Span("▴" if is_hidden else "▾", style={"fontSize": "10px", "color": "#8ea0ab"})
    return new_style, [current_children[0], new_chevron]


@callback(
    Output("exposure-view-as", "data"),
    Output("exposure-view-as", "value"),
    Output("exposure-view-note", "children"),
    Input("exposure-property", "value"),
    Input("selected-country-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    State("exposure-view-as", "value"),
)
def _update_view_as(prop, countries, date, run, current_view_as):
    # Enabled for
    # the five properties it has real data for (population/children/infant/
    # school-age/adolescent, see _EXPOSURE_IN_NEED_PROP_MAP), all wind-only
    # MAT-table columns (no gust/river/rain equivalent
    # exists). Still disabled for Built-up Area and every Context Data
    # property (settlement/rwi/poverty), which have no real *_in_need
    # column to show at all, not a partial-support gap, a genuine absence
    # of vulnerability data for those properties.
    prop_supported = prop in _EXPOSURE_IN_NEED_PROP_MAP

    # Same root cause as
    # _build_hazard_tile_config's own any_hazard_on gate: In Need is wind-
    # only (E_people_in_need/E_children_in_need come from Sustained Wind's
    # own MAT-table columns exclusively, see this function's own note
    # below), so this must NOT enable the option purely off the SELECTED
    # PROPERTY, with no check for whether the CURRENT country/date/run even
    # has a real storm resolved, a quiet-date country with no active storm
    # could otherwise switch to "In Need" and see a column that's NULL
    # everywhere, the same silent-NULL trap any_hazard_on avoids for the
    # map layer/legend. Same has_storms signal _hurricane_family already
    # computes to grey out the Sustained Wind checkbox itself.
    countries = countries or []
    if countries:
        has_storms = any(_resolve_storm_for_country(c, date, run) for c in countries)
    else:
        _d = date or _DEFAULT_FORECAST_DATE
        _r = run if run is not None else _DEFAULT_FORECAST_RUN
        _forecast_time_str = f"{_d} {_r}:00:00" if _d and _r is not None else None
        has_storms = bool(get_track_ids_for_date(_forecast_time_str)) if _forecast_time_str else False

    supported = prop_supported and has_storms
    data = [
        {"value": "expected", "label": _t("At Risk")},
        {"value": "inneed", "label": _t("In Need"), "disabled": not supported},
    ]
    # Prevents a stuck "In Need" selection: the segmented control has no
    # other path back to "expected" once its own option becomes disabled
    # mid-selection (e.g. switching from a country with a real storm to one
    # without, while In Need is already active), falls back automatically
    # rather than leaving the UI showing a disabled-but-still-selected value.
    value = dash.no_update
    if not supported and current_view_as == "inneed":
        value = "expected"
    if supported:
        # Two real properties of this data: (1)
        # E_people_in_need/E_children_in_need only ever come from wind's own
        # MAT-table columns, this reflects Sustained Wind exposure only,
        # regardless of which other hazards are also toggled on; (2) unlike
        # At Risk exposure (which shrinks sharply at higher wind-severity
        # thresholds, e.g. a real max falling from 37,854 to 4,695 across
        # 34/50/64kt for E_population), these columns do NOT scale with the
        # selected wind threshold at all, the same real E_people_in_need max
        # can appear at every threshold. The In Need SHARE
        # will therefore look proportionally larger at higher severity
        # thresholds purely because the At Risk denominator shrank, not
        # because more people actually became vulnerable, worth knowing
        # before reading a jump in that percentage as a real change.
        note = _t("In Need reflects Sustained Wind exposure only, and does not change with the wind severity threshold selected.")
    elif prop_supported and not has_storms:
        note = _t("In Need is unavailable — no real Sustained Wind data for the current selection.")
    else:
        note = _t("In Need is only available for Population, Children (total), and the age bands.")
    return data, value, note


# When "view as" In Need is
# active it must not be possible to switch to the built surface or any of
# the context layers, and the other way around: if any of these layers is
# already toggled on, it must not be possible to switch to "In
# Need". The second half is already implicitly true via _update_view_as's
# own `supported` check above (keyed off exposure-property's CURRENT value,
# so it already disables the In Need option itself whenever Built-up/
# Context Data is selected), this callback covers the first half, the
# direction _update_view_as's own eligibility check can't reach:
# disabling the 5 properties that have no real *_in_need column
# (Built-up Area, Settlement, RWI, Moderate/Severe Poverty) WHILE In Need is
# already active, so a user can't switch into an incompatible property and
# silently fall back to an ambiguous At-Risk-shaped number while the toggle
# still visually claims "In Need."
@callback(
    Output("exp-radio-built", "disabled"),
    Output("exp-radio-settlement", "disabled"),
    Output("exp-radio-rwi", "disabled"),
    Output("exp-radio-moderate-poverty", "disabled"),
    Output("exp-radio-severe-poverty", "disabled"),
    Input("exposure-view-as", "value"),
)
def _disable_in_need_incompatible_props(view_as):
    disabled = view_as == "inneed"
    return disabled, disabled, disabled, disabled, disabled


# _WIND_CATS/_WIND_TIER_FACTOR now live near the top of the file (moved
# there so the real _STORMS-building code at module-import time can also
# use _WIND_CATS to derive a storm's category, see that section's own
# comment for why).


# Shared fixed height for every hazard's curve/grid, see
# _threshold_curve_chart's own comment on why this needs to be one value
# both it and _rain_threshold_grid agree on. Kept compact so the
# tile-click popup's stacked cards don't run too tall.
_HAZARD_CURVE_HEIGHT = "128px"


# Shared "honest empty state" for every hazard curve/grid with no real
# per-threshold data source at all (Storm Surge, permanently, see
# ms-surge-on's own comment) or missing the context needed to query one
# (e.g. the print page, which doesn't read live slider state). This
# function NEVER shows a fabricated illustrative number here, even
# though a real backend simply not existing yet (Storm Surge) is a
# different underlying reason than a real backend existing but not being
# reached (the Rainfall bugs this same policy was written to fix, see
# _rain_threshold_grid's own comment). From a reader's point of view, both
# cases must look identical: a number here always means real data, never a
# guess.
def _no_real_data_chart():
    return html.Div(
        dmc.Text(_t("No real data for this hazard/selection."),
                   size="10px", c="dimmed", fs="italic", ta="center"),
        style={"minHeight": _HAZARD_CURVE_HEIGHT, "display": "flex", "alignItems": "center", "justifyContent": "center"})


def _threshold_curve_chart(labels, values, active_idx, color):
    # Same visual language as the exceedance-curve option explored earlier
    # (line + filled area + a marked point at the current selection, with
    # its own value called out), a real Plotly figure (like the arc
    # charts elsewhere on this page), not raw SVG, Dash's html module has
    # no SVG tag components to inject one directly.
    #
    # Ceil ONCE here, same project-wide convention _format_stat_number's
    # own docstring establishes (real counts always round up, never down),
    # applied to the value that DRIVES THE PLOT ITSELF, not just its text
    # label. Before this fix, `values` stayed raw/un-ceiled floats (real
    # weighted per-tile sums, e.g. Warning=0.9, Danger=0.05) while every
    # annotation's own text used _format_stat_number(v), which ceils
    # internally -- two genuinely different real values (0.9 and 0.05)
    # can both ceil to the same displayed "1", so the line/marker
    # POSITIONS (still 0.9 vs 0.05) visually read as "Danger is basically
    # zero" right under a label that says "1", looking broken even though
    # both numbers were individually real. Ceiling here makes the plotted
    # height and the displayed label always agree, same single real
    # number driving both.
    values = [math.ceil(v) if v is not None else 0 for v in values]
    n = len(values)
    marker_sizes = [13 if i == active_idx else 6 for i in range(n)]
    marker_colors = [color if i == active_idx else "#ffffff" for i in range(n)]
    # Plotly's fillcolor doesn't accept 8-digit (alpha-suffixed) hex, needs
    # an explicit rgba() string for the same translucency.
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    fig = go.Figure(go.Scatter(
        x=list(range(n)), y=values, mode="lines+markers",
        line=dict(color=color, width=2), fill="tozeroy", fillcolor=f"rgba({r},{g},{b},0.12)",
        marker=dict(size=marker_sizes, color=marker_colors, line=dict(color=color, width=2)),
        hoverinfo="skip",
    ))
    # Every tier's own number is shown now (not just the selected one), small
    # and gray so they read as reference context, not competing with the
    # selected tier's own bold black callout. Same idea as the Rainfall
    # matrix showing every cell's value, just adapted to a line chart: one
    # number stands out, the rest are still all visible for comparison.
    for i, v in enumerate(values):
        if i == active_idx:
            continue
        fig.add_annotation(
            x=i, y=v, text=_format_stat_number(v),
            showarrow=False, yshift=13, font=dict(size=8, color="#8ea0ab"),
        )
    fig.add_annotation(
        x=active_idx, y=values[active_idx], text=f"<b>{_format_stat_number(values[active_idx])}</b>",
        showarrow=False, yshift=16, font=dict(size=11, color="#16232c"),
    )
    fig.update_layout(
        height=52, margin=dict(l=2, r=2, t=16, b=10), showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False, range=[-0.3, n - 0.7]),
        yaxis=dict(visible=False, range=[0, max(values) * 1.15]),
    )
    # Fixed-height wrapper, centered, _rain_threshold_grid's heatmap is
    # naturally taller than this line chart (header row + 3 tier rows +
    # caption vs. one sparkline + one label row); without a shared height
    # the two ever sit at different sizes wherever they appear side by side
    # (_hazard_threshold_preview's grid) or stacked (the tile-click popup's
    # cards), _HAZARD_CURVE_HEIGHT is the one place both agree on.
    return html.Div(html.Div([
        dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "52px", "width": "100%"}),
        html.Div([html.Span(_t(l), style={"fontSize": "8px", "color": "#8ea0ab", "flex": 1, "textAlign": "center"}) for l in labels],
                   style={"display": "flex", "padding": "0 2px", "marginTop": "-4px"}),
    ]), style={"minHeight": _HAZARD_CURVE_HEIGHT, "display": "flex", "flexDirection": "column",
                "justifyContent": "center", "marginBottom": "0px"})


def _rain_threshold_grid(labels, active_window, active_idx, color, real_matrix):
    # Rainfall's own version of _threshold_curve_chart, a small heatmap
    # grid (window × depth tier), not a line chart: 4 overlapping lines
    # would tangle together, but genuinely 2D data (duration on one axis,
    # intensity on the other) is exactly the case a matrix/heatmap is the
    # standard, recognizable pattern for (weather dashboards use this same
    # grid shape for "accumulation over duration X at threshold Y", cell
    # color encodes magnitude, position encodes the two axes, no lines to
    # visually tangle together). Each cell shows its own number directly
    # rather than relying on hover, since this is a small static preview,
    # not an interactive chart; the current (window, tier) cell gets a
    # colored ring so "where am I" is still obvious at a glance.
    #
    # `real_matrix` is _precip_curve_totals's own
    # {"6": [v25, v50, v75], ...} shape when given, same real-vs-
    # illustrative pattern _hazard_curve_row already uses for Sustained
    # Wind/River Flooding.
    #
    # MUST NOT fall back to an illustrative ratio-scaled matrix when
    # real_matrix is None: this grid sits directly under a real "Rainfall:
    # X%/Y people" row computed from actual per-member data, and an
    # illustrative fallback here can show a DIFFERENT, contradicting
    # number in the same view (confirmed live: a "Children at Risk" grid
    # showed 7.4M in its ringed cell while the real headline read 3.2M --
    # at the time, _precip_curve_totals only covered the "People at Risk"
    # metric, a restriction since found to rest on a stale/wrong claim and
    # removed, see its own docstring, but back then every other metric
    # silently fell through to this illustrative branch). A real
    # per-disaster-response tool showing a fabricated number that merely
    # LOOKS like real data is worse than showing nothing, see this file's
    # own project-wide "no illustrative fallback" requirement. real_matrix
    # is None whenever there's genuinely no real per-cell data for this
    # metric/scope (e.g. scope resolves to zero countries), not a
    # metric-specific gap any more, so that's exactly when this must show
    # an honest "no real data" state instead of a chart.
    if real_matrix is None:
        return html.Div(
            dmc.Text(_t("No real per-threshold rainfall data for this metric."),
                       size="10px", c="dimmed", fs="italic", ta="center"),
            style={"minHeight": _HAZARD_CURVE_HEIGHT, "display": "flex", "alignItems": "center", "justifyContent": "center"})
    windows = list(_RAIN_WINDOW_SCALE.keys())
    matrix = real_matrix
    vmax = max(v for vals in matrix.values() for v in vals) or 1
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)

    header = [html.Div()] + [
        html.Div(_RAIN_WINDOW_LABELS[w], style={
            "fontSize": "9px", "fontWeight": 700 if w == active_window else 500,
            "color": color if w == active_window else "#8ea0ab", "textAlign": "center",
        }) for w in windows
    ]
    rows = []
    for i, tier in enumerate(labels):
        rows.append(html.Div(_t(tier), style={"fontSize": "9px", "color": "#57707e", "textAlign": "right",
                                                  "paddingRight": "6px", "whiteSpace": "nowrap", "alignSelf": "center"}))
        for w in windows:
            val = matrix[w][i]
            intensity = val / vmax
            is_active = (w == active_window and i == active_idx)
            # Color is now purely selection-driven (black+bold for the
            # current cell, gray for every other one) rather than tied to
            # each cell's own intensity, matches the line charts' own
            # treatment (_threshold_curve_chart), where the selected tier's
            # number stands out and every other tier's number is still shown
            # but small and gray. The background tint is the only thing left
            # encoding magnitude now.
            rows.append(html.Div(
                _format_stat_number(val),
                style={
                    "fontSize": "10px", "fontWeight": 700 if is_active else 500, "fontFamily": "monospace",
                    "textAlign": "center", "padding": "4px 2px", "borderRadius": "5px",
                    "background": f"rgba({r},{g},{b},{0.1 + intensity * 0.55:.2f})",
                    "color": "#16232c" if is_active else "#8ea0ab",
                    "border": f"2px solid {color}" if is_active else "2px solid transparent",
                },
            ))
    # Fixed-height wrapper, centered, same _HAZARD_CURVE_HEIGHT the line
    # chart uses (see _threshold_curve_chart's own comment), so every
    # hazard's visualization occupies the same space regardless of which
    # one (naturally taller heatmap vs. naturally shorter sparkline) it is.
    return html.Div(html.Div([
        html.Div(header + rows, style={
            "display": "grid",
            "gridTemplateColumns": f"38px repeat({len(windows)}, 1fr)",
            "gap": "3px", "alignItems": "center",
        }),
        html.Div(_t("Rows: depth tier · Columns: accumulation window — ringed cell is current ({window}, {tier})",
                     window=_RAIN_WINDOW_LABELS[active_window], tier=_t(labels[active_idx])),
                  style={"fontSize": "8px", "color": "#8ea0ab", "textAlign": "center", "marginTop": "4px"}),
    ]), style={"minHeight": _HAZARD_CURVE_HEIGHT, "display": "flex", "flexDirection": "column",
                "justifyContent": "center", "marginBottom": "0px"})
# Matches GloFAS's own return-period ladder (2/5/10/20/50/100-year), not a
# vaguer plain-English gloss, return period is the actual unit the
# underlying river-flood data is thresholded on.
_RIVER_CATS = ["1-in-2-year flood", "1-in-5-year flood", "1-in-10-year flood",
               "1-in-20-year flood", "1-in-50-year flood", "1-in-100-year flood"]
# Real MERCATOR_TILE_RIVER_MAT.RP_TIER values ('rp2'/'rp5'/
# 'rp10'/'rp20'/'rp50'/'rp100', a string not an int), same order as
# _RIVER_CATS/ms-river-slider so index i's tier label always matches index
# i's real backend RP_TIER value.
_RIVER_RP_TIERS = ["rp2", "rp5", "rp10", "rp20", "rp50", "rp100"]
# IS_STANDIN=True in RIVER_FORECASTS, the pipeline's own
# way of flagging "not yet independently computed, this file just reuses
# rp10's own real extent as a labelled UPPER-BOUND stand-in" until real
# rp2/rp5 computation exists (flood extent grows monotonically with return
# period, so RP10's extent conservatively overestimates RP2/RP5's true,
# smaller extent, see TC-ECMWF-Forecast-Pipeline's own
# glofas_extent_masking.py). Mirrors services/tile_server.py's own
# _RIVER_EXTENT_STANDIN_RP_TIERS exactly.
_RIVER_STANDIN_RP_TIERS = {"rp2", "rp5"}
# A DELIBERATE SUBSET of
# services/tile_server.py's own _RIVER_EXTENT_STEP_HOURS (all 7 real
# daily lead times the parquet carries, same cross-file duplication
# convention this repo already has for propMap/ePropMap),
# not an exact mirror, full
# daily granularity doesn't add real decision value in the UI (7 near-
# identical "Nd" pill buttons); mirrors Rainfall's own 4-option
# ms-rain-window shape (near-term/medium/longer/longest), just with river's
# own real daily values instead of rain's 6h-anchored ones. tile_server.py's own
# _RIVER_EXTENT_STEP_HOURS is unchanged (still the full real 7-value data-
# cadence documentation, every value below is honestly real 24h-step
# GloFAS data, just not every real value is exposed as its own button) and
# its `step_h` query param is unvalidated (any int still works server-side,
# so a direct API call can still request 48/96/144 if ever needed).
_RIVER_EXTENT_STEP_HOURS = [24, 72, 120, 168]

# Mirrors
# services/tile_server.py's own _RIVER_WINDOW_DEFAULT exactly (168h/the
# full real forecast horizon, see that constant's own comment for the
# full "why an EXACT, not approximate, backward-compat default"
# rationale). Used by _build_hz/_river_only_hz below as the fallback
# whenever no real river window has been selected yet. ms-river-window
# itself (the actual UI control, _RIVER_EXTENT_STEP_HOURS above) defaults
# to "72" for its own initial rendered value, this constant is the
# query-layer fallback for callers that pass river_window=None, a
# separate concern from what the control's own default value prop shows.
_RIVER_WINDOW_DEFAULT = 168
_RAIN_TIERS = ["Moderate rain", "Heavy rain", "Extreme rain"]
_RAIN_MM_BY_WINDOW = {"6": [25, 50, 75], "24": [35, 70, 103], "72": [45, 90, 133], "120": [50, 100, 150]}
# Height above normal tide, placeholder categories, no real pipeline behind
# these yet (see ms-surge-on's own comment).
_SURGE_TIERS = ["Minor surge (0.3–1m)", "Moderate surge (1–2m)", "Major surge (2–3m)", "Extreme surge (>3m)"]

# PREVIEW ONLY (see _WIND_TIER_FACTOR's own comment, same idea, one ratio
# list per hazard, index of today's default slider position = 1.0). All
# four hazards DECREASE with severity here, same direction, a return
# period is being read the same way as a wind/rain/surge intensity
# threshold: "how many people does THIS forecast put at risk of AT LEAST
# this severity", an exceedance question, not "how big is a hypothetical
# flood of exactly this return period on a static climatological map" (a
# different question, where extent genuinely grows with return period,
# easy to conflate the two, and an earlier version of this file did:
# rarer/bigger events are still harder for any ONE forecast to actually
# reach, so fewer people clear that bar, matching real GloFAS ensemble
# behavior too, fewer members/cells exceed RP20 than RP2 for a given
# forecast).
_RIVER_TIER_FACTOR = [2.6, 1.0, 0.42, 0.19, 0.07, 0.03]  # default index 1 (1-in-5yr)
_RAIN_TIER_FACTOR = [1.8, 1.0, 0.45]                      # default index 1 (Heavy)
_SURGE_TIER_FACTOR = [1.6, 1.0, 0.5, 0.22]                # default index 1 (Moderate)
# Rainfall is genuinely 2D (window × depth tier, see _RAIN_MM_BY_WINDOW),
# shown as one line per window instead of slicing at whichever window is
# selected, so all four are visible/comparable at once, not just the
# current one. Longer accumulation windows scale the whole tier-factor
# curve UP (more total rain accumulates given more time, even at the same
# nominal "Moderate/Heavy/Extreme" label, since each window's own mm
# thresholds are already calibrated to represent "moderate for that
# duration", see _RAIN_MM_BY_WINDOW). "6" (today's default window) = 1.0
# so nothing changes at the existing default combo (6h + Heavy).
_RAIN_WINDOW_SCALE = {"6": 1.0, "24": 1.4, "72": 1.9, "120": 2.3}
_RAIN_WINDOW_LABELS = {"6": "6h", "24": "24h", "72": "72h", "120": "120h"}

_WIND_CURVE_LABELS = [c[1].replace("Category ", "Cat").replace(" Hurricane", "").replace("Trop. Storm", "TS") for c in _WIND_CATS]
# Google FloodHub's own convention (per its public docs): "warning level" =
# 1-in-2yr, "danger level" = 1-in-5yr, "extreme level" = 1-in-20yr, plain
# severity names instead of a raw return-period ratio, since "1-in-5-year"
# means little at a glance to a reader who isn't already a hydrologist.
# Adapted here to all 6 of our tiers (FloodHub only names 3): keeps
# FloodHub's own three terms at their matching tiers (2/5/20yr) and adds
# Severe (10yr, between Danger and Extreme) plus Catastrophic/Historic
# (50/100yr, beyond Extreme) to complete the escalating ladder.
_RIVER_CURVE_LABELS = ["Warning", "Danger", "Severe", "Extreme", "Catastrophic", "Historic"]
_RAIN_CURVE_LABELS = [t.replace(" rain", "") for t in _RAIN_TIERS]
_SURGE_CURVE_LABELS = [t.split(" (")[0].replace(" surge", "") for t in _SURGE_TIERS]

# Per-hazard (labels, tier-factor ratios), keyed by the same names used in
# _HAZARD_CONTRIBUTION/_HAZARD_GROUPS, so any row for one of these hazards
# (wherever it appears, alone, or as one of Flood's several active members)
# can look up its own curve data by name.
_HAZARD_CURVE_DATA = {
    "Sustained Wind": (_WIND_CURVE_LABELS, _WIND_TIER_FACTOR),
    "River Flooding": (_RIVER_CURVE_LABELS, _RIVER_TIER_FACTOR),
    "Rainfall": (_RAIN_CURVE_LABELS, _RAIN_TIER_FACTOR),
    "Storm Surge": (_SURGE_CURVE_LABELS, _SURGE_TIER_FACTOR),
}


def _hazard_threshold_preview(breakdown, hazard_idx, total_people_at_risk, rain_window=None, river_window=None, expanded=False,
                                 scope=None, countries=None, date=None, run=None):
    # This must NOT stay ENTIRELY illustrative
    # (base_n * a fixed _WIND_TIER_FACTOR/_RIVER_TIER_FACTOR ratio) while
    # _hazard_curve_row (the tile-click Hazard Contribution popup's
    # own, separate copy of this same concept) uses
    # real per-tier data, the Full Impact Breakdown
    # modal has its OWN separate curve-building function here, and it needs
    # the same real-data treatment: an illustrative curve here can show a
    # figure that internally contradicts the real "People at Risk" total
    # directly below it in the main table for the exact same selection, not
    # just an inaccuracy.
    #
    # scope/countries/date/run (new params, threaded from
    # _impact_breakdown_content) enable the same real per-tier
    # _resolve_stat_value("People at Risk", scope, ...) calls
    # _hazard_curve_row already uses, isolating just ONE hazard at a time
    # via _wind_only_hz/_river_only_hz regardless of what else is active.
    # Rainfall (genuinely 2D, window x tier) now also gets the real
    # per-cell fetch _hazard_curve_row's own Rainfall branch already uses
    # (_precip_curve_totals), same can_query_real guard as Wind/River above.
    # Only Storm Surge (no real backend at all) stays illustrative.
    if not hazard_idx:
        return None
    can_query_real = scope is not None
    # Real WITHIN-Flood split (River Flooding vs Rainfall vs both), same
    # methodology _hazard_contribution_content's own river_rain_only/
    # flood_split_real block uses (see that block's own comment for the
    # full "why": River+Rain-only is the one case with a real per-tile
    # bitmask answer to "hit by River but NOT also Rain"). This function
    # was originally built (see its own top comment) to use the plain
    # marginal query for River/Rain instead, reasoned as "correct enough,
    # this view has no inline headline row to reconcile against", but that
    # reasoning was wrong in practice: even with no literal headline row
    # directly above it, the SAME real "River Flooding" curve showing a
    # materially different number here vs the Hazard Contribution popup
    # (confirmed live: ~1000x apart for the same Bangladesh/BAVI real
    # data, since River and Rain overlap so heavily there that "River's
    # raw marginal total" and "River exclusive of Rain" are wildly
    # different real quantities) reads as contradictory to a real user.
    # Computing the SAME exclusion-aware real_matrix here, whenever it
    # applies, makes both popups show the identical real number instead of
    # two different-but-both-real ones side by side.
    river_rain_only = breakdown["active_flood_members"] == ["River Flooding", "Rainfall"]
    flood_split_real = False
    if river_rain_only and can_query_real:
        flood_resolved_countries = _resolve_curve_countries(scope, countries, date, run, river_idx=hazard_idx.get("River Flooding"), rain_idx=hazard_idx.get("Rainfall"), rain_window=rain_window, river_window=river_window)
        flood_hz = _build_hz(
            False, False, True, True,
            river_idx=hazard_idx.get("River Flooding"),
            rain_idx=hazard_idx.get("Rainfall"),
            rain_window=rain_window,
            river_window=river_window,
        )
        flood_split = _combined_flood_split(flood_resolved_countries, date=date, run=run, hz=flood_hz)
        flood_metric_split = _sum_flood_split_metric(flood_split, "People at Risk") if flood_split else None
        if flood_metric_split is not None:
            river_only_n = flood_metric_split.get("river_only") or 0.0
            rain_only_n = flood_metric_split.get("rain_only") or 0.0
            flood_both_n = flood_metric_split.get("both") or 0.0
            flood_split_real = (river_only_n + rain_only_n + flood_both_n) > 0
    active_names = breakdown["active_tc_members"] + breakdown["active_flood_members"]
    rows = []
    for name in active_names:
        idx = hazard_idx.get(name)
        curve_data = _HAZARD_CURVE_DATA.get(name)
        if idx is None or curve_data is None:
            continue
        color, pct, icon = _HAZARD_BY_NAME[name]
        labels, factors = curve_data
        base_n = math.ceil(total_people_at_risk * pct / 100)
        if name == "Sustained Wind" and can_query_real:
            # Same
            # _hazard_curve_totals fast path as _hazard_curve_
            # row's own usage inside _hazard_contribution_content, see its
            # comment for the full "why". "People at Risk" is always one of
            # the metrics _hazard_curve_totals covers, so this never falls
            # through to the old per-tier fan-out.
            real_values = _hazard_curve_totals("People at Risk", "wind", scope, countries, date, run,
                                                  river_idx=hazard_idx.get("River Flooding"),
                                                  rain_idx=hazard_idx.get("Rainfall"), rain_window=rain_window)
            if real_values is None:
                raw_values = list(get_query_executor().map(
                    lambda wc: _resolve_stat_value("People at Risk", scope, countries, date=date, run=run,
                                                      wind_kt=wc[2], hz=_wind_only_hz(wc[2]),
                                                      river_idx=hazard_idx.get("River Flooding"),
                                                      rain_idx=hazard_idx.get("Rainfall"),
                                                      rain_window=rain_window, river_window=river_window),
                    _WIND_CATS))
                real_values = [_parse_stat_number(v) for v in raw_values]
            chart = _threshold_curve_chart(labels, real_values, idx, color)
        elif name == "River Flooding" and can_query_real:
            # Same real WITHIN-Flood fix as the Rainfall branch below: when
            # River+Rain are both active and a real split resolves, this
            # must be River-and-NOT-Rain (the SAME real quantity the Hazard
            # Contribution popup shows), not River's raw marginal total,
            # see this function's own river_rain_only/flood_split_real
            # comment above for the full "why".
            if river_rain_only and flood_split_real:
                real_values = _river_curve_totals_excl_rain(
                    "People at Risk", scope, countries, date, run,
                    rain_idx=hazard_idx.get("Rainfall"), rain_window=rain_window, river_window=river_window,
                    river_idx=hazard_idx.get("River Flooding"))
            else:
                real_values = _hazard_curve_totals("People at Risk", "river", scope, countries, date, run, river_window=river_window,
                                                      river_idx=hazard_idx.get("River Flooding"),
                                                      rain_idx=hazard_idx.get("Rainfall"), rain_window=rain_window)
            if real_values is None:
                raw_values = list(get_query_executor().map(
                    lambda rp_tier: _resolve_stat_value("People at Risk", scope, countries, date=date, run=run,
                                                           hz=_river_only_hz(rp_tier, river_window),
                                                           river_idx=hazard_idx.get("River Flooding"),
                                                           rain_idx=hazard_idx.get("Rainfall"),
                                                           rain_window=rain_window, river_window=river_window),
                    _RIVER_RP_TIERS))
                real_values = [_parse_stat_number(v) for v in raw_values]
            chart = _threshold_curve_chart(labels, real_values, idx, color)
        elif name == "Rainfall" and rain_window is not None:
            # Real per-cell fetch, same _precip_curve_totals aggregator
            # _hazard_curve_row's own Rainfall branch uses (see that
            # function's comment): this branch was the one hazard left
            # calling _rain_threshold_grid with no real_matrix at all,
            # silently falling through to its entirely-illustrative
            # base_n-ratio grid even though Wind/River right above it in
            # this same function were already real. Same real WITHIN-Flood
            # fix as River Flooding's own branch above, mirrored onto the
            # grid: when River+Rain are both active and a real split
            # resolves, every cell must be Rain-and-NOT-River (the SAME
            # real quantity the Hazard Contribution popup shows), not
            # Rain's raw marginal exposure, see this function's own
            # river_rain_only/flood_split_real comment above for the full
            # "why" this was wrong before (a materially different real
            # number here vs. the other popup reads as contradictory, even
            # with no inline headline row on THIS view to reconcile
            # against literally).
            if river_rain_only and flood_split_real:
                real_matrix = _rain_grid_totals_excl_river(
                    "People at Risk", scope, countries, date, run,
                    river_idx=hazard_idx.get("River Flooding"), river_window=river_window,
                    rain_idx=hazard_idx.get("Rainfall"), rain_window=rain_window)
            else:
                real_matrix = _precip_curve_totals(
                    "People at Risk", scope, countries, date, run,
                    river_idx=hazard_idx.get("River Flooding"), rain_idx=hazard_idx.get("Rainfall"),
                    rain_window=rain_window, river_window=river_window) if can_query_real else None
            chart = _rain_threshold_grid(labels, rain_window, idx, color, real_matrix)
        else:
            # Storm Surge (no real backend at all), Rainfall with no real
            # rain_window context, or a real hazard missing metric/scope
            # context. None of these show a fabricated illustrative
            # number, same honest empty state as
            # _hazard_curve_row's own identical else branch, see
            # _no_real_data_chart's own comment.
            chart = _no_real_data_chart()
        rows.append(html.Div([
            dmc.Group([DashIconify(icon=icon, width=13, color=color),
                        dmc.Text(_t(name), size="11px", fw=700, c="dark")], gap=4, mb=4),
            chart,
        ]))
    if not rows:
        return None
    head_id = {} if expanded else {"id": "head-threshold-preview"}
    body_id = {} if expanded else {"id": "body-threshold-preview"}
    head_children = [dmc.Text(_t("Threshold sensitivity (preview) — People at Risk:"), size="11px", fw=700, c="dimmed", style={"flex": 1})]
    if not expanded:
        head_children.append(html.Span("▾", style={"fontSize": "10px", "color": "#8ea0ab"}))
    return html.Div([
        html.Div(head_children, **head_id,
                   style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "12px",
                           **({} if expanded else {"cursor": "pointer"})}),
        html.Div(rows, **body_id,
                  style={"display": "grid" if expanded else "none",
                          "gridTemplateColumns": "repeat(auto-fit, minmax(190px, 1fr))", "gap": "20px"}),
    ], style={"marginTop": "6px", "marginBottom": "22px", "paddingBottom": "20px",
               "borderBottom": "1px solid #eef2f5"})


@callback(Output("ms-wind-readout", "children"), Input("ms-wind-slider", "value"))
def _wind_readout(idx):
    c = _WIND_CATS[idx or 0]
    # Backend threshold is still kt, but leading with the m/s equivalent (kt
    # in parentheses second) means you don't need to already know the
    # conversion to read the scrubber.
    return f"{_t(c[0])} — {_t(c[1])} · {c[3]} m/s ({c[2]}kt) {_t('sustained wind')}"




@callback(Output("ms-gust-readout", "children"), Input("ms-gust-slider", "value"))
def _gust_readout(idx):
    c = _WIND_CATS[idx or 0]
    return f"{_t(c[0])} — {_t(c[1])} · {c[3]} m/s ({c[2]}kt) {_t('gusts')}"


@callback(Output("ms-river-readout", "children"), Input("ms-river-slider", "value"))
def _river_readout(idx):
    idx = idx or 0
    label = _t(_RIVER_CATS[idx])
    # rp2/rp5 are
    # IS_STANDIN=True upstream,
    # this tier isn't independently computed yet, the pipeline just reuses
    # rp10's own real extent as a labelled stand-in/bound. Surfaced right on
    # the slider's own readout (both the raw global layer AND the
    # per-country hazard tile system share this same real data quality
    # limitation, not just the raw layer), so this never silently reads as
    # a real independent rp2/rp5 result.
    if _RIVER_RP_TIERS[idx] in _RIVER_STANDIN_RP_TIERS:
        label += " " + _t("(not natively computed — RP10 used as an upper-bound estimate)")
    # Unlike Wind/Gust
    # ("... m/s (kt) sustained wind/gusts") and Rainfall ("≥ Xmm over Yh"),
    # a "return period" readout ("1-in-10-year flood") is meaningless to a
    # first-time user with no explanation anywhere in the panel. Hover-only
    # tooltip on the readout itself, same convention as _layer_label_with_
    # info's icon tooltips elsewhere in this panel.
    return dmc.Tooltip(
        label=_t("A return period of N years means roughly a 1-in-N chance of a flood this severe occurring in any given year."),
        multiline=True, w=230, withArrow=True, position="top",
        transitionProps={"duration": 0},
        styles={"tooltip": {"fontSize": "11px", "lineHeight": 1.4}},
        children=html.Span(label, style={"cursor": "help", "borderBottom": "1px dotted #8ea0ab"}),
    )


@callback(Output("ms-surge-readout", "children"), Input("ms-surge-slider", "value"))
def _surge_readout(idx):
    return _t(_SURGE_TIERS[idx or 0])


@callback(
    Output("ms-rain-readout", "children"),
    Input("ms-rain-window", "value"),
    Input("ms-rain-slider", "value"),
)
def _rain_readout(window, idx):
    idx = idx or 0
    mm = _RAIN_MM_BY_WINDOW[window or "6"][idx]
    tier = _t(_RAIN_TIERS[idx])
    return _t("{tier} · ≥ {mm}mm over {hours}h", tier=tier, mm=mm, hours=window or "6")


# Real lat/lon bounding box across every ensemble member's full track for
# one storm, read from the already-loaded ms-tracks-json GeoJSON rather
# than a new Snowflake query: Global mode already loads every active
# storm's real track unconditionally, regardless of country impact (see
# _load_ms_tracks_and_envelopes's own docstring, "ALL real storms active at
# the selected topbar date/run... render as ONE combined tracks
# FeatureCollection"), each feature tagged properties.track_id matching a
# storm's exact display name (_build_ms_track_features). Used by
# _select_storm below to fly the map to a storm that has no country impact
# yet, no new query needed to do it. Returns None (caller no_updates the
# viewport) when the storm's track isn't in the already-loaded data at all,
# e.g. the Storm Tracks checkbox is off (ms-tracks-json is emptied, not
# just hidden, see _load_ms_tracks_and_envelopes's own tracks_on gate), a
# race with the track layer not having loaded yet, or (expected_forecast_
# time given and mismatched) real staleness: ms-tracks-json is refetched by
# a separately-scheduled callback off the same topbar-date/topbar-time
# Inputs, unordered relative to the storm row click, so a click on a still-
# visible row right after a date/run change could otherwise read the
# PREVIOUS cycle's track for a same-named multi-day storm with nothing to
# detect it -- rather than flying somewhere wrong (or to the wrong cycle),
# this returns None and the caller no-ops.
def _storm_track_bounds(tracks_geojson, storm_name, expected_forecast_time=None):
    if not tracks_geojson:
        return None
    lats, lons_raw = [], []
    for feature in tracks_geojson.get("features", []):
        props = feature.get("properties", {})
        if props.get("track_id") != storm_name:
            continue
        if expected_forecast_time and props.get("forecast_time") != expected_forecast_time:
            continue
        for lon, lat in feature.get("geometry", {}).get("coordinates", []):
            lats.append(lat)
            lons_raw.append(lon)
    if not lats:
        return None
    # Antimeridian check, same real technique _fly_map_to_selection already
    # uses a few lines below for the multi-country bounds case (see that
    # function's own comment for the full "why"): a naive min/max span can
    # be wildly wrong when an ensemble ACROSS MEMBERS straddles the date
    # line from opposite sides, even though each individual member's own
    # sequence (already unwrapped internally by _build_ms_track_features/
    # _unwrap_track_lons) never itself crosses 180 deg. Shift any negative
    # longitude by +360 and compare spans; use whichever is smaller.
    #
    # A single member crossing the date line exactly once combines fine
    # here: _unwrap_track_lons applies one +-360 correction regardless of
    # which member produced it, and that correction collapses to the same
    # canonical value as this function's own +360-if-negative step below,
    # independent of member or crossing direction. The real residual gap is
    # narrower: a member crossing the date line MORE than once within its
    # own sequence (offset accumulating past +-360), plus the same generic
    # "cut only at longitude 0" imprecision _fly_map_to_selection already
    # has and this file already accepts elsewhere (worst case, a wider than
    # strictly necessary but still locally-correct box). Neither is solved
    # here; both are pre-existing, shared limitations, not specific to this
    # function.
    naive_span = max(lons_raw) - min(lons_raw)
    if naive_span > 180:
        shifted = [lon + 360 if lon < 0 else lon for lon in lons_raw]
        shifted_span = max(shifted) - min(shifted)
        lons = shifted if shifted_span < naive_span else lons_raw
    else:
        lons = lons_raw
    # Same margin convention as _fly_map_to_selection's own multi-country
    # bounds case just below, kept in sync deliberately.
    margin_lat = max(0.5, (max(lats) - min(lats)) * 0.15)
    margin_lon = max(0.5, (max(lons) - min(lons)) * 0.15)
    return [[min(lats) - margin_lat, min(lons) - margin_lon],
            [max(lats) + margin_lat, max(lons) + margin_lon]]


# Handles the Global-mode Active Storms list rows, uses the
# {"type": "select-storm", "name": ...} pattern-matching id, since clicking
# a row does the same thing regardless of where it's clicked from: set the
# country picker's value to ALL of that storm's affected countries at once
# (a multi-country storm selects multiple countries here directly, no
# separate "also affects" switcher needed once the picker itself is
# multi-select), which the callback below reacts to (one source of truth for
# "selected countries", not a separate chip + a separate picker disagreeing).
@callback(
    Output("topbar-country-select", "value"),
    Output("ms-storm-flyto-store", "data"),
    Input({"type": "select-storm", "name": dash.ALL}, "n_clicks"),
    State("topbar-date", "value"),
    State("topbar-time", "value"),
    State("ms-tracks-json", "data"),
    prevent_initial_call=True,
)
def _select_storm(clicks, date, run, tracks_geojson):
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update
    triggered = dash.callback_context.triggered_id
    name = triggered["name"]
    # Storm rows are rendered by _active_storms_section from the date/run-
    # reactive _resolve_storms_for_date, not the frozen _STORMS snapshot,
    # look storms up the same way, or a click on a row that only exists for
    # the currently-selected date (a new storm, a dissipated one, or a
    # Demo Scenario's historical date) would silently miss and fall back to
    # the wrong storm/countries, same class of correctness concern as
    # _hurricane_family/_flood_hazards_family's has_storms/availability.
    storms = _resolve_storms_for_date(date, run)
    storm = next((s for s in storms if s["name"] == name), None)
    if storm is None:
        # Genuine lookup miss (a real race, e.g. date/run changed between
        # the row rendering and this click resolving). Previously fell back
        # to storms[0], silently selecting/flying to a completely unrelated
        # storm with no error and no visual indication. No-op instead,
        # matching the safe "return None rather than something wrong"
        # convention _storm_track_bounds itself already follows.
        return dash.no_update, dash.no_update
    countries = list(storm["countries"])
    if countries:
        # Real country impact: the existing selected-country-store ->
        # _fly_map_to_selection chain already flies to the real country
        # center(s), ms-storm-flyto-store must stay untouched here, not
        # emptied, two callbacks racing to set ms-main-map's viewport from
        # the same click would be a real bug, not just redundant.
        return countries, dash.no_update
    # No country impact yet (still at sea): fly to the storm's own real
    # track extent instead (see _storm_track_bounds above), so a click on
    # one of these storms isn't a dead no-op. topbar-mode still correctly
    # stays "global" either way, the clientside mode-switch callback below
    # only flips to "zoom" when countries is non-empty, this only ever
    # affects the map viewport. expected_forecast_time, same "date run:00:00"
    # shape _load_ms_tracks_and_envelopes itself builds forecast_time_str
    # with, guards against ms-tracks-json holding a previous cycle's data
    # for this same storm name (see _storm_track_bounds's own docstring).
    expected_forecast_time = f"{date} {run}:00:00" if date and run is not None else None
    bounds = _storm_track_bounds(tracks_geojson, name, expected_forecast_time)
    flyto = {"bounds": bounds, "transition": "flyToBounds"} if bounds else dash.no_update
    return countries, flyto


# Companion to _fly_map_to_selection just below: same Output, different
# trigger, each only ever produces a real value for the case the other
# leaves alone (see _select_storm's own comment), so the two can never
# actually race over the same click.
@callback(
    Output("ms-main-map", "viewport", allow_duplicate=True),
    Input("ms-storm-flyto-store", "data"),
    prevent_initial_call=True,
)
def _fly_map_to_storm_track(viewport):
    return viewport if viewport else dash.no_update


# The ONLY place "selected countries" actually gets set, whether picked
# directly from this multi-select or arrived via a storm click/search (which
# just sets this same picker's value, see _select_storm above). Guards mode
# with a State comparison rather than always writing it: this callback and
# _clear_countries_on_global below both touch topbar-mode/topbar-country-select,
# so writing a value that's already current (e.g. re-affirming "global") would
# re-trigger the other callback and ping-pong forever, only ever write mode
# when it's actually changing.
#
# This is the FIRST
# hop of a 4-hop chain on every country selection (_country_selected
# -> _update_view_as -> _build_hazard_tile_config -> _load_ms_tracks_and_
# envelopes), pure conditional logic with no Snowflake round-trip of its
# own, so there's no real reason it needs a server round-trip at all.
# Clientside removes that one Python round-trip from the front of the chain;
# logic is unchanged (same no_update-guarded mode write, same ping-pong
# avoidance described above).
clientside_callback(
    """
    function(countries, current_mode) {
        countries = countries || [];
        var newMode = countries.length ? "zoom" : "global";
        var modeOut = (newMode === current_mode) ? window.dash_clientside.no_update : newMode;
        return [countries, modeOut];
    }
    """,
    Output("selected-country-store", "data"),
    Output("topbar-mode", "value", allow_duplicate=True),
    Input("topbar-country-select", "value"),
    State("topbar-mode", "value"),
    prevent_initial_call=True,
)


# Flies the Leaflet map to the selected country's real center/zoom, until
# now, selecting a country updated the Impact Summary/mode but left the map
# viewport wherever it happened to already be, totally unrelated to the
# selection. _NAME_TO_CENTER (built above from get_active_countries()'s own
# CENTER_LAT/CENTER_LON/VIEW_ZOOM columns, the same PIPELINE_COUNTRIES data
# _CODE_TO_NAME/_NAME_TO_CODE already load), no new query. Single selected
# country (with a resolvable center): flies to that country's own
# center/zoom, exactly as before. 2+ selected countries (with 2+ resolvable
# centers): fits the viewport to a real bounding box across all of them,
# via dash-leaflet's own "bounds" viewport key (Map.viewport supports
# either center/zoom OR bounds, bounds takes precedence and does a real
# fitBounds/flyToBounds, no manual center+zoom math needed), must NOT
# fly to just the FIRST selected country's center, which would leave
# every other selected country off-screen. Empty/Global selection, or a
# selection with no resolvable center at all: no_update, leaves the
# viewport wherever it is rather than snapping back to the world default.
@callback(
    Output("ms-main-map", "viewport"),
    Input("selected-country-store", "data"),
    prevent_initial_call=True,
)
def _fly_map_to_selection(countries):
    centers = [_NAME_TO_CENTER[c] for c in (countries or []) if c in _NAME_TO_CENTER]
    if not centers:
        return dash.no_update
    if len(centers) == 1:
        lat, lon, zoom = centers[0]
        return {"center": [lat, lon], "zoom": zoom, "transition": "flyTo"}
    lats = [c[0] for c in centers]
    lons_raw = [c[1] for c in centers]
    # Antimeridian check: a naive min/max span can be wrong when the real
    # "short way around" actually crosses the dateline rather than passing
    # through the raw numeric range, e.g. Jamaica (-77°) and the
    # Philippines (+122°) naively span 199° through Africa/the Middle East,
    # but the real short way between them is only 161° through the Pacific.
    # Shift any negative longitude by +360 and compare spans; if the shifted
    # span is smaller, use it, Leaflet's fitBounds accepts lng values
    # outside -180..180 fine, it just fits the real rectangle either way.
    naive_span = max(lons_raw) - min(lons_raw)
    if naive_span > 180:
        shifted = [lon + 360 if lon < 0 else lon for lon in lons_raw]
        shifted_span = max(shifted) - min(shifted)
        lons = shifted if shifted_span < naive_span else lons_raw
    else:
        lons = lons_raw
    # Small margin (at least 0.5°, or 15% of the span) so the outermost
    # selected countries aren't flush against the viewport edge.
    margin_lat = max(0.5, (max(lats) - min(lats)) * 0.15)
    margin_lon = max(0.5, (max(lons) - min(lons)) * 0.15)
    bounds = [[min(lats) - margin_lat, min(lons) - margin_lon],
              [max(lats) + margin_lat, max(lons) + margin_lon]]
    return {"bounds": bounds, "transition": "flyToBounds"}


# Switching back to Global (directly via the mode toggle, not just by
# clearing the country picker) should drop the country selection, otherwise
# the picker/store keep stale countries that silently reappear if you flip
# back to Country Analysis. Only acts when mode becomes "global"; leaves
# everything alone when mode becomes "zoom" (that direction is driven by
# _country_selected above, and this callback must not clobber it).
@callback(
    Output("topbar-country-select", "value", allow_duplicate=True),
    Output("selected-country-store", "data", allow_duplicate=True),
    Input("topbar-mode", "value"),
    prevent_initial_call=True,
)
def _clear_countries_on_global(mode):
    if mode == "global":
        return [], []
    return dash.no_update, dash.no_update


# _update_map_legend must NOT read the 4
# ms-facility-{id}-on checkboxes directly as Inputs, those only exist
# in the DOM in Country Analysis mode (_controls_zoom's Infrastructure
# section), and Dash never fires a callback at all while ANY of its Inputs
# is missing from the current layout, not just returns None for that one
# value, which would silently break the legend in EVERY Global-
# mode render, including its Tracks/envelope/raster sections that have
# nothing to do with facilities. Mirroring into this always-present store
# (only fires when the checkboxes DO exist, i.e. Country Analysis mode)
# decouples the two.
@callback(
    Output("ms-facility-visibility-store", "data"),
    Input("ms-facility-schools-on", "checked"),
    Input("ms-facility-health-on", "checked"),
    Input("ms-facility-shelters-on", "checked"),
    Input("ms-facility-wash-on", "checked"),
)
def _mirror_facility_visibility(schools_on, health_on, shelters_on, wash_on):
    return {"schools": bool(schools_on), "health": bool(health_on),
            "shelters": bool(shelters_on), "wash": bool(wash_on)}


# Same mirror pattern as _mirror_facility_visibility above, for the same
# reason (see ms-hazard-render-mode-store's own comment where it's
# declared), _build_hazard_tile_config/_build_global_raw_config/
# _sync_tc_view_as all read THIS always-present store, never the
# Country-Analysis-only switch directly. Deliberately no
# reset-on-Global companion (unlike the facility store): "raw"/
# "classification" carries no per-country data, unlike a facility
# checkbox, which would show something visually wrong if left "on" in a
# mode with nothing to show, and every consumer of this store already
# gates its OWN Global-mode branch on `not countries` first, independent
# of whatever value happens to be here. Persisting the user's last-picked
# mode across a Global<->Country Analysis round-trip is a reasonable
# preference to keep, not a bug to guard against.
@callback(
    Output("ms-hazard-render-mode-store", "data"),
    Input("ms-hazard-render-mode", "value"),
)
def _mirror_hazard_render_mode(value):
    return value or "probability"


# The mirror callback above can't reset itself to all-False on switching to
# Global (its own Inputs no longer exist there to fire it), this is what
# keeps a facility checked before leaving Country Analysis from staying
# "on" in the store (and therefore in the legend) after switching to Global,
# where nothing about it is even shown.
@callback(
    Output("ms-facility-visibility-store", "data", allow_duplicate=True),
    Input("topbar-mode", "value"),
    prevent_initial_call=True,
)
def _reset_facility_visibility_on_global(mode):
    if mode == "global":
        return {"schools": False, "health": False, "shelters": False, "wash": False}
    return dash.no_update


# Mantine's SegmentedControl has no native per-item disabled state, so
# _time_options_for_date's dimmed labels above are cosmetic only, this is
# what actually blocks a future run from sticking. Fires on the date
# changing (rebuilds which runs are dimmed for the new date, and snaps the
# value down if it's now past the new date's own limit) AND on the run
# itself changing (covers a user managing to click a dimmed segment
# directly, snapping straight back to the max allowed run).
@callback(
    Output("topbar-time", "data"),
    Output("topbar-time", "value"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    prevent_initial_call=True,
)
def _guard_future_forecast_run(date, run):
    # Live ceiling (_live_forecast_ceiling, same helper _refresh_forecast_
    # ceiling already uses), not the frozen _LATEST_FORECAST_TIME/
    # _DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN globals this callback
    # used to fall back on by calling _time_options_for_date/_max_allowed_
    # run_for_date with only date_str. Real gap otherwise: once layout()
    # resolves a live per-request default, a page load can correctly land
    # on a newer run (e.g. 18Z) than the frozen globals still remember
    # (e.g. "00"), and any ordinary date/time interaction that re-fires
    # this callback for that same date would snap topbar-time's value
    # straight back down to the stale frozen ceiling, silently reverting
    # the live default for the rest of that session. Falls back to the
    # frozen globals (via _time_options_for_date/_max_allowed_run_for_
    # date's own defaults) only if the live re-fetch itself fails.
    latest_time, live_date, live_run = _live_forecast_ceiling()
    if live_date is None:
        data = _time_options_for_date(date)
        max_run = _max_allowed_run_for_date(date)
    else:
        data = _time_options_for_date(date, latest_time, live_date, live_run)
        max_run = _max_allowed_run_for_date(date, latest_time, live_date, live_run)
    if max_run is not None and run is not None and int(run) > int(max_run):
        return data, max_run
    return data, dash.no_update


# prop_id strings for the 5 hazard checkboxes, as dash.callback_context.
# triggered reports them ("<id>.<prop>"), see _update_impact_summary's own
# Global-mode short-circuit for why this exists.
_HAZARD_CHECKBOX_TRIGGER_PROPS = {
    "ms-wind-on.checked", "ms-gust-on.checked", "ms-river-on.checked",
    "ms-rain-on.checked", "ms-surge-on.checked",
}


@callback(
    Output("impact-body", "children"),
    Output("impact-subtitle", "children"),
    Input("selected-country-store", "data"),
    Input("influencing-factor-select", "value"),
    Input("impact-aggregation-toggle", "value"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-surge-on", "checked"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("ms-rain-window", "value"),
    Input("ms-river-window", "value"),
    # Debounced (~200ms after a drag settles, see the clientside wrapper
    # around ms-slider-debounce-store above) rather than direct Inputs on
    # the 4 hazard sliders, this callback calls _fetch_real_combined_tile_
    # totals per country, so a direct Input on the sliders would re-fire
    # the real Snowflake-backed combined-hazard fetch on every intermediate
    # drag tick, not just when the drag settles.
    Input("ms-slider-debounce-store", "data"),
    State("ms-wind-slider", "value"),
    State("ms-gust-slider", "value"),
    State("ms-river-slider", "value"),
    State("ms-rain-slider", "value"),
)
def _update_impact_summary(countries, influencing_factor, aggregation, wind_on, gust_on, river_on, rain_on, surge_on,
                             date, run, rain_window, river_window, _debounce_tick, wind_idx, gust_idx, river_idx, rain_idx):
    # Scoped to whichever hazards are toggled on (sidebar checkboxes/
    # command-bar pills), the at-risk numbers below are now the REAL
    # combined-across-active-hazards total (_build_hz/
    # _fetch_real_combined_tile_totals), not an illustrative percentage
    # scaling of wind's own total. topbar-date/topbar-time are Inputs, see
    # _resolve_storm_for_country's own docstring for why the real numbers
    # here need to react to date/run changes, not just country changes
    # (a Demo Scenario or the date picker can point at a historical date with
    # no currently-active storm at all). Every hazard's own slider is an
    # Input too, see _build_hz's own docstring for why the real numbers
    # here need to react to each hazard's own threshold, not just wind's.
    #
    # The Global branch just below (`not countries`)
    # deliberately ignores wind_on/gust_on/river_on/rain_on/surge_on
    # entirely, it always uses `_build_hz(True, False, river_avail,
    # rain_avail, ...)` (hardcoded wind True/gust False, river/rain from a
    # real Snowflake availability check, never from the checkbox args) for
    # the worldwide total, by explicit product decision (see that branch's
    # own comment: "Global mode's checkbox on/off state is still a pure
    # map-display concern... not what counts toward the worldwide total").
    # Gust's own False here isn't just "ignores the checkbox the same way
    # wind does" -- it never contributes regardless of what it's hardcoded
    # to, see _fetch_real_combined_tile_totals_uncached's own docstring.
    # So toggling a hazard checkbox while in Global mode (countries empty)
    # provably cannot change this callback's output, skip the multi-second
    # worldwide recompute for exactly that one case. Country Analysis scopes
    # (countries non-empty) are untouched, those totals genuinely still
    # depend on the checkboxes (real hz below).
    if not countries:
        triggered = dash.callback_context.triggered
        if triggered and all(t["prop_id"] in _HAZARD_CHECKBOX_TRIGGER_PROPS for t in triggered):
            return dash.no_update, dash.no_update
    wind_kt = _resolve_wind_kt(wind_idx)
    hz = _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx, gust_idx, river_idx, rain_idx, rain_window, river_window)
    countries = countries or []
    influencing_factor = influencing_factor or "none"
    if not countries:
        # Global: real worldwide total, sum of every country's own real
        # combined-hazard impact (_combined_stats, same combination already
        # used for multi-country Country Analysis), across every country
        # any REAL, currently-impactful storm affects (_resolve_storms_for_
        # date, impact-gated, correct here, unlike get_track_ids_for_
        # date's own track-existence-only check used for the header dot
        # above). Replaces the old _DEFAULT_STATS mock (a fixed hardcoded
        # number) with a genuinely zero total on a real quiet day.
        #
        # Global mode's checkbox on/off state is a
        # pure map-display concern (which layers paint on the map, not what
        # counts toward the worldwide total: Wind/River/Rain always
        # contribute here regardless of checkbox state; Gust never does,
        # checkbox or not, see _fetch_real_combined_tile_totals_uncached's
        # own docstring), but the total DOES react to each hazard's own
        # threshold slider exactly
        # like Country Analysis's own hz below does. Matches Country
        # Analysis's single-country/combined blocks in spirit (a real
        # multi-hazard total) without depending on which checkboxes happen
        # to be on.
        all_country_names, river_avail, rain_avail = _global_flood_availability(
            date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
        # wind_on=True (matches river/rain's own real-availability gating:
        # an active storm's own wind data is always real, no live checkbox
        # needed to decide "is there something to show"); gust_on=False
        # explicitly, not just left to whatever the checkbox says: Gust is
        # excluded from every combined total everywhere, not only here, see
        # _fetch_real_combined_tile_totals_uncached's own docstring.
        global_hz = _build_hz(True, False, river_avail, rain_avail, wind_idx, gust_idx, river_idx, rain_idx, rain_window, river_window)
        global_stats = _combined_stats(all_country_names, date=date, run=run,
                                         wind_kt=global_hz["wind_kt"], hz=global_hz)
        # impact-subtitle itself is a dmc.Text (renders a <p>), its own
        # "component" prop must stay untouched (setting component="div"
        # here crashed a Mantine clientside prop-transform on page load),
        # but plain children (Span/Br/Div) render into it fine. A plain
        # html.Div pill instead of dmc.Badge, Badge is built for short,
        # single-line labels (fixed line-height/overflow rules baked into
        # its own CSS class) and fighting that with inline style overrides
        # still rendered as an edge-to-edge rectangle, not a contained
        # pill. "width: fit-content" is what actually keeps it hugging its
        # own text instead of stretching to the panel's full width.
        subtitle = [
            html.Span(_t("Global — worldwide totals across all hazards")),
            html.Br(),
            html.Div(_t("Reflects only countries currently initialized in the database."),
                       style={"marginTop": "6px", "display": "inline-block", "width": "fit-content",
                               "maxWidth": "100%", "boxSizing": "border-box",
                               "padding": "4px 10px", "borderRadius": "8px",
                               "background": "#fff0f0", "color": "#c92a2a",
                               "fontSize": "11px", "fontWeight": 500, "fontStyle": "normal",
                               "lineHeight": 1.4, "whiteSpace": "normal"}),
        ]
        return _stat_grid(global_stats, scope="global"), subtitle

    # Real replacement for the old _WORST_MEMBER_BY_FACTOR fixed dict, see
    # _resolve_worst_member_multi's own docstring.
    compare_member = _resolve_worst_member_multi(influencing_factor, countries, date, run, wind_kt, hz=hz)
    # Resolved ONCE here (not
    # inside _stat_grid itself, which has no date/run) and passed through
    # to _country_block's own _stat_grid call below, so the compare badge's
    # "Flood" tag reflects whether flood data ACTUALLY contributed to
    # `compare_member`'s own numbers, not just whether it was toggled on,
    # see _stat_grid's own `flood_combine_active` param docstring.
    flood_combine_active = bool(hz) and _flood_combine_active(date, run, hz)

    # No "In Need" number tiles here (extra_stats), the
    # same circular PIN/CHIN gauges the Full Impact Breakdown modal shows
    # are used here instead. Each block below is the
    # base stat grid plus its matching arc-chart pair, show_label=False so
    # the country/"Combined" name isn't repeated a second time right under
    # the grid's own label. compare_stats/compare_pin (real per-member
    # numbers, real replacement for the old _scaled_stats/_scaled_pin_pct
    # fake-multiplier calls) are computed by each call site below, which has
    # the country/countries context needed for a real get_track_impacts
    # lookup, this closure only assembles the already-resolved numbers.
    def _country_block(base_stats, base_pin, arc_charts, compare_stats=None, compare_pin=None, label=None, scope="combined"):
        # Must NOT require BOTH compare_stats (At-Risk)
        # AND compare_pin (wind-only In Need, see _in_need_note's own
        # "deliberate asymmetry" docstring note) before showing ANY compare
        # badge at all: compare_stats can be genuinely real from
        # River/Rain alone (e.g. a country with no active wind storm at
        # all), while compare_pin stays None (wind-only, by design), an
        # AND-gate here would silently swallow the entire real At-Risk compare
        # badge in exactly that case. Each half populates
        # full_compare independently.
        full_compare = dict(compare_stats) if compare_stats is not None else None
        if compare_pin is not None:
            full_compare = full_compare if full_compare is not None else {}
            # None means genuinely no real in-need data for this member/
            # scope (see _fetch_real_combined_tile_totals_uncached's own
            # comment): "N/A", not a fabricated number.
            #
            # Real, un-derived absolute in-need count, read directly
            # instead of reconstructing via at_risk * pct / 100, see
            # _fetch_real_combined_tile_totals_uncached's own pin_pct
            # comment for the full mechanism.
            full_compare["People in Need"] = (
                _format_stat_number(compare_pin["people_abs"])
                if compare_pin.get("people_abs") is not None else _t("N/A"))
            full_compare["Children in Need"] = (
                _format_stat_number(compare_pin["children_abs"])
                if compare_pin.get("children_abs") is not None else _t("N/A"))
        grid = _stat_grid(base_stats, label=label, scope=scope,
                            compare_stats=full_compare, compare_member=compare_member, hz=hz,
                            flood_combine_active=flood_combine_active)
        return html.Div([grid, arc_charts])

    if len(countries) == 1:
        country = countries[0]
        storm_info = _resolve_storm_for_country(country, date, run)
        subtitle = f'{_t(country)} · {storm_info["cat"]}' if storm_info else _t("{country} — no active tropical cyclone", country=_t(country))
        arc_charts = _pin_arc_charts_block(country, compare_member, show_label=False, date=date, run=run, wind_kt=wind_kt, hz=hz)
        base_stats = _get_country_stats(country, date, run, wind_kt, hz=hz)
        compare_stats = _real_member_stats(country, date, run, wind_kt, compare_member, hz=hz) if compare_member else None
        compare_pin = _real_member_pin_pct(country, date, run, wind_kt, compare_member) if compare_member else None
        return _country_block(base_stats, _get_country_pin_pct(country, date, run, wind_kt, hz=hz),
                               arc_charts, compare_stats=compare_stats, compare_pin=compare_pin, scope=country), subtitle

    if aggregation == "combined":
        # Real per-country totals summed together (_combined_stats/
        # _combined_in_need_total), each already the real combined-across-
        # active-hazards total (via `hz`), not a re-derived aggregate, and
        # no more illustrative percentage scaling.
        combined_base = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        _people_pct = _combined_in_need_pct(countries, "People at Risk", "people",
                                              _parse_stat_number(combined_base["People at Risk"]),
                                              date=date, run=run, wind_kt=wind_kt, hz=hz)
        _children_pct = _combined_in_need_pct(countries, "Children at Risk", "children",
                                                _parse_stat_number(combined_base["Children at Risk"]),
                                                date=date, run=run, wind_kt=wind_kt, hz=hz)
        combined_pin = {
            "people": _people_pct,
            "children": _children_pct,
            # Real, un-derived absolute in-need totals, see
            # _pin_arc_charts_block_combined's own identical comment.
            "people_abs": (_combined_in_need_total(countries, "People at Risk", "people",
                                                     date=date, run=run, wind_kt=wind_kt, hz=hz)
                            if _people_pct is not None else None),
            "children_abs": (_combined_in_need_total(countries, "Children at Risk", "children",
                                                        date=date, run=run, wind_kt=wind_kt, hz=hz)
                               if _children_pct is not None else None),
        }
        subtitle = _t("Combined — {n} countries", n=len(countries))
        arc_charts = _pin_arc_charts_block_combined(countries, compare_member, show_label=False, date=date, run=run, wind_kt=wind_kt, hz=hz)
        compare_stats = compare_pin = None
        if compare_member:
            compare_stats = _combined_stats(countries, member=compare_member, date=date, run=run, wind_kt=wind_kt, hz=hz)
            _cmp_people_pct = _combined_in_need_pct(countries, "People at Risk", "people",
                                                       _parse_stat_number(compare_stats["People at Risk"]),
                                                       member=compare_member, date=date, run=run, wind_kt=wind_kt, hz=hz)
            _cmp_children_pct = _combined_in_need_pct(countries, "Children at Risk", "children",
                                                         _parse_stat_number(compare_stats["Children at Risk"]),
                                                         member=compare_member, date=date, run=run, wind_kt=wind_kt, hz=hz)
            compare_pin = {
                "people": _cmp_people_pct,
                "children": _cmp_children_pct,
                "people_abs": (_combined_in_need_total(countries, "People at Risk", "people",
                                                         member=compare_member, date=date, run=run, wind_kt=wind_kt, hz=hz)
                                if _cmp_people_pct is not None else None),
                "children_abs": (_combined_in_need_total(countries, "Children at Risk", "children",
                                                            member=compare_member, date=date, run=run, wind_kt=wind_kt, hz=hz)
                                   if _cmp_children_pct is not None else None),
            }
        return _country_block(combined_base, combined_pin, arc_charts,
                               compare_stats=compare_stats, compare_pin=compare_pin, scope="combined"), subtitle

    # Per country (default): one stat block per country, side by side, not
    # a single combined number and not forcing a pick between them.
    subtitle = _t("{n} countries selected: {list}", n=len(countries), list=", ".join(_t(c) for c in countries))
    # Each country's
    # block below is 5 independent per-country Snowflake-backed calls;
    # building them concurrently is what actually shows up in a click on
    # this panel (unlike the Full Impact Breakdown modal, which only
    # renders once opened, this "impact-body" panel is what the user sees
    # immediately after selecting a multi-country storm).
    def _build_block(c):
        return _country_block(
            _get_country_stats(c, date, run, wind_kt, hz=hz), _get_country_pin_pct(c, date, run, wind_kt, hz=hz),
            _pin_arc_charts_block(c, compare_member, show_label=False, date=date, run=run, wind_kt=wind_kt, hz=hz),
            compare_stats=_real_member_stats(c, date, run, wind_kt, compare_member, hz=hz) if compare_member else None,
            compare_pin=_real_member_pin_pct(c, date, run, wind_kt, compare_member) if compare_member else None,
            label=c, scope=c)
    blocks = list(get_query_executor().map(_build_block, countries))
    # Stacked country blocks must NOT butt straight
    # up against each other with no separation of their own (impact-body's
    # own container has no gap, see its layout() definition): the next
    # country's name label would sit directly under the previous country's
    # last arc chart with no breathing room, and _pin_arc_charts_block_
    # from's own internal divider (between one country's stat tiles and its
    # arc charts) would do double duty as the only visible "boundary" on
    # the page, reading ambiguously as if IT marked where one country ended
    # and the next began. This is the real country-to-country boundary
    # instead (a clearly heavier rule + real vertical gap) applied to
    # every block after the first (the first one already sits right under
    # the panel's own subtitle/controls, no extra separator needed there).
    blocks = [
        (b if i == 0 else html.Div(b, style={"borderTop": "1px solid #dde3ea", "marginTop": "24px", "paddingTop": "20px"}))
        for i, b in enumerate(blocks)
    ]
    return blocks, subtitle


@callback(
    Output("impact-breakdown-modal", "opened"),
    Input("impact-breakdown-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _open_impact_breakdown(_n):
    return True


# One clientside_callback, not 3 separate Python
# @callbacks each doing a pure style-dict toggle with zero Snowflake/i18n
# dependency (all sharing the same single Input), a pure-formatting
# callback with no server-side dependency should not pay a full HTTP round
# trip. Style values match what the equivalent Python callbacks would produce.
clientside_callback(
    """
    function(countries) {
        var hasCountries = !!(countries && countries.length);
        var breakdownBtnStyle = hasCountries ? {} : {"display": "none"};
        var controlsRowBase = {"padding": "10px 18px", "borderTop": "1px solid #eef2f5"};
        var controlsRowStyle = hasCountries ? controlsRowBase
            : Object.assign({}, controlsRowBase, {"display": "none"});
        // Only 2+ countries genuinely have anything to sum or split apart,
        // a single country has nothing for "Total" to do differently from
        // "Split", so just this control stays hidden until there's a real
        // choice to make.
        var aggregationStyle = (countries && countries.length > 1) ? {} : {"display": "none"};
        return [breakdownBtnStyle, controlsRowStyle, aggregationStyle];
    }
    """,
    Output("impact-breakdown-btn", "style"),
    Output("impact-controls-row", "style"),
    Output("impact-aggregation-wrapper", "style"),
    Input("selected-country-store", "data"),
)


@callback(
    Output("impact-breakdown-body", "children"),
    # Real Input (not State), see this callback's own opened-guard comment
    # for why: the modal being opened must ITSELF trigger a fresh
    # computation (so reopening after other Inputs changed while closed
    # still shows current data), not just gate against them.
    Input("impact-breakdown-modal", "opened"),
    Input("selected-country-store", "data"),
    Input("influencing-factor-select", "value"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-surge-on", "checked"),
    Input("ms-surge-slider", "value"),  # no real backend, never queries Snowflake, cheap, stays direct
    Input("ms-rain-window", "value"),
    # river_window was missing here entirely (rain_window's own direct
    # sibling right above it, present since this callback was first
    # written), so this whole modal (main table, per-cell TC/Flood split,
    # AND the threshold-sensitivity curves) silently ran every real river
    # query against _build_hz's own None-default (_RIVER_WINDOW_DEFAULT,
    # the full 168h horizon) regardless of the user's actual live
    # ms-river-window selection (e.g. "72h"), confirmed live: River
    # Flooding's own curve read ~1000x too high here vs the correctly-wired
    # Hazard Contribution popup's identical real query for the same
    # country/date. A real Input (not State), matching rain_window's own
    # treatment: changing the window is itself a reason to recompute, not
    # just context to read passively next time something else changes.
    Input("ms-river-window", "value"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    # Debounced, see _update_impact_summary's own comment on why the 4
    # hazard sliders go through ms-slider-debounce-store instead of firing
    # this Snowflake-backed callback on every intermediate drag tick.
    Input("ms-slider-debounce-store", "data"),
    State("ms-wind-slider", "value"),
    State("ms-gust-slider", "value"),
    State("ms-river-slider", "value"),
    State("ms-rain-slider", "value"),
)
def _update_impact_breakdown(opened, countries, influencing_factor, wind_on, gust_on, river_on, rain_on, surge_on,
                                surge_idx, rain_window, river_window, date, run, _debounce_tick,
                                wind_idx, gust_idx, river_idx, rain_idx):
    # Must NOT run the
    # heaviest per-country+comparison+admin1 Snowflake bundle in this file
    # on EVERY hazard/country/date/slider change regardless of whether the
    # modal is even open to see the result, e.g. dragging a threshold
    # slider with the modal closed would re-fire this exact same real query
    # bundle for no visible benefit. Skipped entirely while closed;
    # `opened` is a real Input (not just a State) specifically so reopening
    # the modal itself re-triggers a fresh computation, rather than showing
    # whatever was last computed before it was closed.
    if not opened:
        return dash.no_update
    # Not fed impact-aggregation-toggle, the modal always shows both
    # per-country AND Combined at once (see _impact_breakdown_content), so
    # unlike the compact panel it has nothing to switch between.
    #
    # topbar-date/topbar-time are Inputs too (matching
    # _update_impact_summary's own pattern), this modal's numbers must NOT
    # resolve against the frozen "active right now"
    # storm regardless of the selected date/run, which would silently
    # disagree with the Impact Summary panel right next to it.
    return _impact_breakdown_content(countries, influencing_factor, wind_on=wind_on, gust_on=gust_on,
                                        river_on=river_on, rain_on=rain_on, surge_on=surge_on,
                                        wind_idx=wind_idx, gust_idx=gust_idx, river_idx=river_idx, rain_idx=rain_idx, surge_idx=surge_idx,
                                        rain_window=rain_window, river_window=river_window, date=date, run=run)


@callback(
    Output("impact-breakdown-modal", "size"),
    Input("selected-country-store", "data"),
)
def _update_breakdown_modal_width(countries):
    return _breakdown_modal_width(countries)


@callback(
    Output("breakdown-new-tab-link", "href"),
    Input("selected-country-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("ms-wind-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-surge-on", "checked"),
    Input("ms-wind-slider", "value"),
    Input("ms-river-slider", "value"),
    Input("ms-rain-slider", "value"),
    Input("ms-surge-slider", "value"),
    Input("ms-rain-window", "value"),
    Input("ms-river-window", "value"),
)
def _update_breakdown_new_tab_link(countries, date, run, wind_on, river_on, rain_on, surge_on,
                                      wind_idx, river_idx, rain_idx, surge_idx, rain_window, river_window):
    return _breakdown_new_tab_href(countries, date=date, run=run, wind_on=wind_on,
                                      river_on=river_on, rain_on=rain_on, surge_on=surge_on,
                                      wind_idx=wind_idx, river_idx=river_idx, rain_idx=rain_idx, surge_idx=surge_idx,
                                      rain_window=rain_window, river_window=river_window)


@callback(
    Output("hazard-contribution-modal", "opened"),
    Output("hazard-contribution-title", "children"),
    Output("hazard-contribution-body", "children"),
    Input({"type": "stat-card", "metric": dash.ALL, "scope": dash.ALL}, "n_clicks"),
    State("selected-country-store", "data"),
    State("ms-wind-on", "checked"),
    State("ms-gust-on", "checked"),
    State("ms-river-on", "checked"),
    State("ms-rain-on", "checked"),
    State("ms-surge-on", "checked"),
    State("ms-wind-slider", "value"),
    State("ms-gust-slider", "value"),
    State("ms-river-slider", "value"),
    State("ms-rain-slider", "value"),
    State("ms-surge-slider", "value"),
    State("ms-rain-window", "value"),
    State("ms-river-window", "value"),
    State("topbar-date", "value"),
    State("topbar-time", "value"),
    prevent_initial_call=True,
)
def _open_hazard_contribution(clicks, countries, wind_on, gust_on, river_on, rain_on, surge_on,
                                 wind_idx, gust_idx, river_idx, rain_idx, surge_idx, rain_window, river_window, date, run):
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update
    triggered = dash.callback_context.triggered_id
    metric, scope = triggered["metric"], triggered["scope"]
    # Global's own Impact Summary total is
    # ALWAYS the real combination of every hazard (_update_impact_summary's
    # Global branch, independent of the sidebar checkboxes), so this
    # popup's breakdown must NOT read the LIVE checkbox state regardless of
    # scope: that would show "None — toggle a hazard..." for a Global stat
    # while every checkbox happens to be unchecked, even though the number
    # just clicked came from a real all-hazard total. Global scope forces
    # the same all-hazards-on view Global's own total already uses; Country
    # Analysis scopes (single-country/combined) are untouched, those
    # totals genuinely still depend on the checkboxes.
    is_global = (scope == "global")
    if is_global:
        # Must NOT force river/rain "on" for
        # the breakdown regardless of whether either genuinely has real
        # data for the resolved date, a historical storm with real WIND
        # data only (e.g. MELISSA/28 Oct 2025, well before River/Rain
        # pipeline coverage begins) would otherwise show "Flood only"/"Both"
        # sections in the popup, implying flood was part of the total when
        # it contributed nothing. Checks the SAME real per-country latest-
        # forecast-time functions the Flood Hazards checkboxes themselves
        # use, just against the countries this Global total actually
        # resolved (not the (always empty in Global mode) topbar
        # selection).
        _, river_avail, rain_avail = _global_flood_availability(
            date, run, river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
        breakdown = _hazard_breakdown(True, river_avail, rain_avail, surge_on)
    else:
        breakdown = _hazard_breakdown(wind_on, river_on, rain_on, surge_on)
    # topbar-date/topbar-time are States too, this popup must NOT
    # resolve against the frozen "active right now" storm/50kt
    # default, which could silently disagree with the tile that was clicked
    # to open it (see _resolve_stat_value's own docstring).
    wind_kt = _resolve_wind_kt(wind_idx)
    # Threshold-reactive for Global too,
    # same wind_idx/gust_idx/river_idx/rain_idx/rain_window Country
    # Analysis already uses, just still independent of which hazard
    # CHECKBOXES are on for Wind/River/Rain (every one of those three
    # always contributes to Global's total). Gust's own gust_on is
    # hardcoded False here, not True: it never contributes to any combined
    # total in either scope, see _fetch_real_combined_tile_totals_
    # uncached's own docstring for the full rationale.
    hz = _build_hz(True, False, river_avail, rain_avail, wind_idx, gust_idx, river_idx, rain_idx, rain_window, river_window) if is_global else \
        _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx, gust_idx, river_idx, rain_idx, rain_window, river_window)
    value = _resolve_stat_value(metric, scope, countries, date=date, run=run, wind_kt=wind_kt, hz=hz,
                                   river_idx=river_idx, rain_idx=rain_idx, rain_window=rain_window, river_window=river_window)
    title = f"{_t(metric)} — {_t('Combined')}" if scope == "combined" else (
        f"{_t(metric)} — {_t(scope)}" if scope != "global" else _t(metric))
    hazard_idx = {"Sustained Wind": wind_idx, "River Flooding": river_idx,
                   "Rainfall": rain_idx, "Storm Surge": surge_idx}
    # `countries` is passed through as-is even for scope=="global", matches
    # _resolve_stat_value's own behavior (its "global" branch ignores this
    # argument entirely and recomputes the real affected-country list itself
    # via _resolve_storms_for_date), so _hazard_curve_row's own real per-tier
    # _resolve_stat_value calls below stay correct for Global too.
    return True, title, _hazard_contribution_content(
        value, breakdown, hazard_idx=hazard_idx, rain_window=rain_window, river_window=river_window, is_global=is_global,
        metric=metric, scope=scope, countries=countries, date=date, run=run, hz=hz)


@callback(
    Output("alert-email-list-modal", "opened"),
    Output("alert-email-list-modal", "title"),
    Output("alert-email-list-content", "children"),
    Input({"type": "alert-email-btn", "name": dash.ALL}, "n_clicks"),
    State("topbar-date", "value"),
    State("topbar-time", "value"),
    prevent_initial_call=True,
)
def _open_alert_email_list(clicks, date, run):
    """Storm row's email icon (_storm_row) -> real list of that storm's
    available Alert emails (get_alert_emails_for_storm, ALERT_SENT_LOG),
    scoped to the CURRENTLY SELECTED topbar date/run only, must NOT
    show every alert ever sent for the storm regardless of
    what date/time is selected, which would read as stale/wrong emails
    appearing for "now." A multi-country storm can still show several entries here
    (one per affected country), just all for this one forecast run.
    Each entry is clickable (_open_alert_email_detail below opens its full
    HTML). Deliberately shows only the country name per entry, recipient
    count/sent timestamp are internal operational metadata, not surfaced
    here (see get_alert_emails_for_storm's own docstring)."""
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update
    storm_name = dash.callback_context.triggered_id["name"]
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    target_forecast_time = f"{date} {run}:00:00"
    emails = get_alert_emails_for_storm(storm_name, forecast_time=target_forecast_time)
    title = _t("Alert Emails — {storm}", storm=storm_name)
    if not emails:
        return True, title, dmc.Text(
            _t("No alert emails found for this storm at the selected date/time."), size="sm", c="dimmed")
    rows = []
    for e in emails:
        country_code = e["COUNTRY_CODE"]
        forecast_time = str(e["FORECAST_TIME"])
        rows.append(html.Div(
            dmc.Group([
                dmc.Text(_CODE_TO_NAME.get(country_code, country_code), fw=700, size="sm"),
                DashIconify(icon="carbon:chevron-right", width=16, color="#8ea0ab"),
            ], justify="space-between", align="center"),
            id={"type": "alert-email-item", "track_id": storm_name,
                 "forecast_time": forecast_time, "country_code": country_code},
            n_clicks=0,
            style={"padding": "10px 12px", "borderRadius": "8px", "border": "1px solid #eef2f5",
                    "marginBottom": "8px", "cursor": "pointer", "background": "#f6f9fb"},
        ))
    return True, title, html.Div(rows)


@callback(
    Output("alert-email-modal", "opened"),
    Output("alert-email-modal", "title"),
    Output("alert-email-iframe", "src"),
    Output("alert-email-new-tab-link", "href"),
    Output("alert-email-list-modal", "opened", allow_duplicate=True),
    Input({"type": "alert-email-item", "track_id": dash.ALL, "forecast_time": dash.ALL, "country_code": dash.ALL},
          "n_clicks"),
    prevent_initial_call=True,
)
def _open_alert_email_detail(clicks):
    """Clicking one list entry -> both the iframe's `src` and "Open in new
    tab"'s `href` point at the SAME real /alert-email/<track_id>/
    <forecast_time>/<country_code> Flask route (app.py's serve_alert_email,
    which fetches EMAIL_BODY from ALERT_SENT_LOG). Closes the list modal so
    the two don't stack.

    Must NOT embed the whole EMAIL_BODY
    directly as this callback's own Output twice (iframe srcDoc + a
    data:text/html;... URI re-encoding of the same content for the new-tab
    link), real emails can be large enough (embedded base64 map images,
    ~700KB HTML) that the percent-encoded data: URI could exceed practical
    browser limits for a fresh top-level navigation, leaving "Open in new
    tab" to open a blank tab that only renders after a manual page
    refresh. Pointing both at a real URL instead of embedding the
    content twice avoids that entirely, this callback doesn't
    fetch EMAIL_BODY itself at all, just confirms the row exists.
    """
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    triggered = dash.callback_context.triggered_id
    track_id, forecast_time, country_code = (
        triggered["track_id"], triggered["forecast_time"], triggered["country_code"])
    if get_alert_email_body(track_id, forecast_time, country_code) is None:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    country_name = _CODE_TO_NAME.get(country_code, country_code)
    title = f"{track_id} — {country_name} ({forecast_time})"
    email_url = (f"/alert-email/{urllib.parse.quote(track_id, safe='')}"
                 f"/{urllib.parse.quote(forecast_time, safe='')}/{urllib.parse.quote(country_code, safe='')}")
    return True, title, email_url, email_url, False


def _format_watch_forecast_date(forecast_date: str) -> str:
    """WATCH_SENT_LOG.FORECAST_DATE is a real compact 'YYYYMMDDHHMMSS'
    string (matches MERCATOR_TILE_IMPACT_MAT's own convention), not a real
    timestamp type the way ALERT_SENT_LOG.FORECAST_TIME is -- reformats it
    into the same "YYYY-MM-DD HH:MM:SS" shape _open_alert_email_detail's
    own title already shows, so the two modals read consistently instead
    of one showing a raw digit string. Falls back to the value unchanged
    if it doesn't match the expected 14-digit shape (defensive, should
    never happen for a real row)."""
    if len(forecast_date) == 14 and forecast_date.isdigit():
        return (f"{forecast_date[0:4]}-{forecast_date[4:6]}-{forecast_date[6:8]} "
                f"{forecast_date[8:10]}:{forecast_date[10:12]}:{forecast_date[12:14]}")
    return forecast_date


@callback(
    Output("warning-email-modal", "opened"),
    Output("warning-email-modal", "title"),
    Output("warning-email-iframe", "src"),
    Output("warning-email-new-tab-link", "href"),
    Input({"type": "warning-email-btn", "name": dash.ALL}, "n_clicks"),
    State("topbar-date", "value"),
    State("topbar-time", "value"),
    prevent_initial_call=True,
)
def _open_warning_email_detail(clicks, date, run):
    """Storm row's warning-email icon -> straight to the real Warning email
    (WATCH_SENT_LOG), no intermediate "pick one" list step: unlike Alert
    (genuinely one email PER COUNTRY, so a multi-country storm needs a real
    choice), a Warning is always exactly one shared email per (track_id,
    forecast_date) covering every affected country at once (see
    get_warning_emails_for_storm's own docstring) -- there is never a real
    choice to present, so the extra click Alert's own list modal requires
    would just be unnecessary friction here."""
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    storm_name = dash.callback_context.triggered_id["name"]
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    target_forecast_time = f"{date} {run}:00:00"
    emails = get_warning_emails_for_storm(storm_name, forecast_time=target_forecast_time)
    if not emails:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    e = emails[0]
    forecast_date = str(e["FORECAST_DATE"])
    if get_warning_email_body(storm_name, forecast_date) is None:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    countries_raw = [c.strip() for c in (e.get("COUNTRIES") or "").split(",") if c.strip()]
    countries_display = ", ".join(_CODE_TO_NAME.get(c, c) for c in countries_raw) or storm_name
    title = f"{storm_name} — {countries_display} ({_format_watch_forecast_date(forecast_date)})"
    email_url = (f"/warning-email/{urllib.parse.quote(storm_name, safe='')}"
                 f"/{urllib.parse.quote(forecast_date, safe='')}")
    return True, title, email_url, email_url


@callback(
    Output("topbar-date", "value", allow_duplicate=True),
    Output("topbar-time", "value", allow_duplicate=True),
    Output("topbar-country-select", "value", allow_duplicate=True),
    Input({"type": "demo-scenario", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _apply_demo_scenario(clicks):
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update
    # Only sets date/time/countries, mode and selected-country-store
    # already cascade from topbar-country-select changing, the same
    # mechanism a storm-row click uses (_select_storm -> _country_selected).
    scenario = _DEMO_SCENARIOS[dash.callback_context.triggered_id["index"]]
    return scenario["date"], scenario["time"], scenario["countries"]


# Command-bar pills mirror the rail checkboxes (ms-{hz}-on) rather than being
# a second source of truth: clicking a pill flips the checkbox, and the
# checkbox's own state (however it got set, pill, rail, anything else later)
# drives the pill's pressed/unpressed look. Two one-directional callbacks per
# hazard, not a loop: the pill click never writes its own style, only the
# checkbox; the style-reflecting callback only ever reads the checkbox.
#
# Both callbacks also read the checkbox's own "disabled" prop (set in
# _flood_hazards_family / _hurricane_family from real data-availability
# checks), the pill is a second entry point to the exact same checkbox, so
# it must honor the same gating instead of unconditionally flipping
# "checked" regardless of whether the rail says this hazard has no real data
# for the current country/storm.
for _hz_key, _hz_label, _hz_color in _CMD_HAZARDS:
    @callback(
        Output(f"ms-{_hz_key}-on", "checked"),
        Input(f"cmd-{_hz_key}-pill", "n_clicks"),
        State(f"ms-{_hz_key}-on", "checked"),
        State(f"ms-{_hz_key}-on", "disabled"),
        prevent_initial_call=True,
    )
    def _toggle_via_pill(_n, checked, disabled):
        if disabled:
            return dash.no_update
        return not checked

    @callback(
        Output(f"cmd-{_hz_key}-pill", "style"),
        Output(f"cmd-{_hz_key}-dot", "style"),
        Output(f"cmd-{_hz_key}-pill", "data-disabled"),
        Input(f"ms-{_hz_key}-on", "checked"),
        Input(f"ms-{_hz_key}-on", "disabled"),
        Input("ms-hazards-hidden-store", "data"),
    )
    def _reflect_checkbox_on_pill(checked, disabled, hazards_hidden, _color=_hz_color):
        if disabled:
            return _PILL_DISABLED_STYLE, _DOT_OFF_STYLE, "true"
        # The command-bar
        # eye icon's "temporarily hide all hazards" state must visually dim
        # every pill too, not just the underlying map layers, same reason
        # _build_hazard_tile_config's own any_hazard_on now factors in
        # ms-hazards-hidden-store (see that callback's own comment).
        if hazards_hidden:
            return _PILL_HIDDEN_STYLE, _DOT_OFF_STYLE, "false"
        if checked:
            return (
                {**_PILL_OFF_STYLE, "border": f"1px solid {_color}", "background": f"{_color}22", "color": _color},
                {**_DOT_OFF_STYLE, "background": _color},
                "false",
            )
        return _PILL_OFF_STYLE, _DOT_OFF_STYLE, "false"


# =============================================================================
# HAZARD TILE-CONFIG BRIDGE
# Wires the hazard checkboxes/sliders/rain-window control to real
# tile-server-backed MapLibre layers, Wind, Gust, River Flooding, and
# Rainfall for real; Storm Surge stays a visual-only preview (surge_visible
# is always False below, no real backend exists for it, no request is ever
# built for it). Reactive: every control change re-assembles the config and
# re-pushes it to MapLibre, no "Load Layers" button.
#
# Three pieces:
#   1. A clientside debounce wrapper around the 4 hazard sliders, writes
#      ms-slider-debounce-store ~200ms after a drag settles (via
#      dash_clientside.set_props, not this callback's own declared Output,
#      which always returns no_update) so a slider drag doesn't fire dozens
#      of Snowflake/tile-server requests.
#   2. _build_hazard_tile_config (plain Python callback), assembles the full
#      config dict from country selection + all 5 hazard controls, fetching
#      /stats and /admin-stats from the tile server for whichever hazards are
#      both checked "on" AND have a resolved forecast_date. Checkboxes/
#      ms-rain-window are direct (un-debounced) Inputs; the 4 sliders are
#      State, re-read only when Input("ms-slider-debounce-store") fires.
#   3. A clientside bridge pushing the assembled config to the existing
#      window.applyTileConfig (components/map/maplibre_tiles.js), the same
#      function dash_clientside.maplibre.updateTileConfig already wraps.
# =============================================================================

clientside_callback(
    """
    function(windIdx, gustIdx, riverIdx, rainIdx) {
        if (window._ms_slider_debounce_timer) {
            clearTimeout(window._ms_slider_debounce_timer);
        }
        window._ms_slider_debounce_timer = setTimeout(function () {
            window.dash_clientside.set_props('ms-slider-debounce-store', { data: Date.now() });
        }, 200);
        return window.dash_clientside.no_update;
    }
    """,
    Output("ms-slider-debounce-store", "data", allow_duplicate=True),
    Input("ms-wind-slider", "value"),
    Input("ms-gust-slider", "value"),
    Input("ms-river-slider", "value"),
    Input("ms-rain-slider", "value"),
    prevent_initial_call=True,
)


@ttl_cache(ttl_seconds=60, maxsize=256)
def _fetch_tile_server_json(url: str) -> dict:
    """GET a tile-server JSON endpoint (stats/admin-stats), tolerating
    failures the same way pages/dashboard.py's own load_all_layers does
    (missing data → empty dict, not a crash). Cached 60s, this callback is
    reactive (fires on every control change, unlike dashboard.py's
    button-triggered equivalent), so identical requests during a burst of
    unrelated control changes (e.g. toggling Rain while Wind's own
    threshold/stats are unchanged) don't re-hit Snowflake every time.
    """
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.info("Tile server stats unavailable (%s): %s", url, e)
        return {}


# Maps this page's own "exposure-property" radio values (see _exposure_section/
# _context_data_section) to the real GeoJSON/stats column names, analogous to
# callbacks/tiles_and_admin.py's _LAYER_TO_PROP, but keyed by THIS page's own radio
# value strings ("children"/"built"), not dashboard.py's ("children-total"/
# "built-surface"). Verified against components/map/tile_palettes.json's prop_map
# and services/tile_server.py's stats column mappings (the two real sources of
# truth for valid prop names) rather than trusted blindly.
_EXPOSURE_PROP_MAP = {
    "probability": "probability",
    "population": "population",
    "children": "children_total",
    "infant": "infant_population",
    "school-age": "school_age_population",
    "adolescent": "adolescent_population",
    "built": "built_surface_m2",
    "settlement": "smod_class",
    "rwi": "rwi",
    "moderate-poverty": "moderate_poverty_prob",
    "severe-poverty": "severe_poverty_prob",
}

# Hazard-weighted "expected impact" column names, keyed by THIS page's own
# exposure-property radio values, SWAPS a demographic layer from its raw
# count to this column whenever any hazard is active (see any_hazard_on
# below), rather than overlaying a second layer.
#
# settlement/rwi/moderate-poverty/
# severe-poverty must NOT resolve to the raw hazard "probability" column
# whenever ANY hazard is active, that's what /legacy dashboard does for
# those, but it's wrong for this page's own combination model. These
# four properties are already probability-derived per-tile metrics (not
# exposure counts a hazard could weight), so there's no real "combined"
# version of them to switch to, swapping to the unrelated hazard
# probability raster would replace a real context layer with a different,
# unrelated one the moment a hazard is toggled on, silently hiding the
# thing the user actually selected. Deliberately absent from this dict,
# any_hazard_on's own swap below only ever applies to a key that's
# actually present, so these four simply keep their raw
# _EXPOSURE_PROP_MAP value (smod_class/rwi/moderate_poverty_prob/
# severe_poverty_prob) regardless of hazard state, exactly like they
# already do with every hazard off.
_EXPOSURE_E_PROP_MAP = {
    "population": "E_population",
    "children": "E_children_total",
    "infant": "E_infant_population",
    "school-age": "E_school_age_population",
    "adolescent": "E_adolescent_population",
    "built": "E_built_surface_m2",
}

# "In Need" real column names, keyed by THIS page's own exposure-property
# radio values (see _exposure_section).
# MERCATOR_TILE_VULNERABILITY_MAT/ADMIN_ALL_VULNERABILITY_MAT/
# TRACK_VULNERABILITY_MAT all genuinely have E_INFANT_IN_NEED/
# E_SCHOOL_AGE_IN_NEED/E_ADOLESCENT_IN_NEED columns, selected in
# services/tile_server.py's raster/stats queries. Built-up Area
# and every Context Data property (settlement/rwi/poverty) genuinely have
# no *_in_need column at all, not a partial-support gap, a real absence of
# vulnerability data for those properties, so they stay excluded here (and
# see the ms-exposure-view-mutex callback below for the UI-side mutual
# exclusivity this implies).
_EXPOSURE_IN_NEED_PROP_MAP = {
    "population": "E_people_in_need",
    "children": "E_children_in_need",
    "infant": "E_infant_in_need",
    "school-age": "E_school_age_in_need",
    "adolescent": "E_adolescent_in_need",
}


def _hazard_stats(tile_country, hazard, path_storm, path_date, wind_threshold, extra_params):
    """Return (stats, admin_stats) for one hazard, or ({}, {}) if this
    hazard has no resolved forecast_date to query at all (e.g. a country with
    genuinely no river/rain data, see get_latest_river_forecast_time's own
    docstring; not a bug, a real coverage gap).

    /stats and
    /admin-stats are fetched
    concurrently via the shared executor, not as two sequential blocking
    urlopen calls, since neither depends on
    the other's result."""
    if not tile_country or not path_date:
        return {}, {}
    base_url = "" if config.SPCS_RUN else config.TILE_SERVER_URL
    common = {"wind_threshold": wind_threshold, "hazard": hazard, **extra_params}
    stats_qs = urllib.parse.urlencode(common)
    admin_qs = urllib.parse.urlencode({**common, "admin_level": 1})
    stats_url = (f"{base_url}/stats/{quote(tile_country)}/{quote(path_storm)}/{quote(path_date)}?{stats_qs}")
    admin_url = (f"{base_url}/admin-stats/{quote(tile_country)}/{quote(path_storm)}/{quote(path_date)}?{admin_qs}")
    stats, admin_stats = get_query_executor().map(_fetch_tile_server_json, [stats_url, admin_url])
    return stats, admin_stats


def _resolve_primary_storm_group(countries, date, run):
    """Cheap, ttl-cached storm/forecast_date resolution shared by
    _build_hazard_tile_config and _load_ms_tracks_and_envelopes.
    Deliberately excludes everything from
    _build_hazard_tile_config that isn't needed to draw a track/envelope,
    the per-hazard stats fan-out (_hazard_stats, the actual slow part,
    ~4s cold), so tracks/envelopes can resolve their own storm without
    waiting on that unrelated, slower callback's Output. _resolve_storm_
    for_country is itself @ttl_cache'd (single-flight), so when both
    callbacks call this for the same country/date/run concurrently, only
    one of them actually pays the Snowflake round trip.

    Returns (codes, primary_country_code, storm, forecast_date,
    country_storm_infos), country_storm_infos is exposed for
    _build_hazard_tile_config's own extra_groups_by_storm (multi-storm)
    logic, which needs the full per-country list, not just the primary.
    """
    codes = _resolve_tile_codes(countries or [])
    primary_country_code = codes[0] if codes else None
    # Resolved concurrently (get_query_executor().map, same shared pool
    # every other per-country fan-out in this file already uses), not a
    # plain serial for loop: _resolve_storm_for_country is itself
    # @ttl_cache'd/single-flight (repeat calls for a country already
    # resolved elsewhere in this request are free), but the FIRST cold
    # call for each of N genuinely different selected countries previously
    # paid N sequential ~0.15-0.36s Snowflake round trips back-to-back
    # here, on the critical path of BOTH _build_hazard_tile_config and
    # _load_ms_tracks_and_envelopes (this helper is shared by both, see
    # this function's own docstring above).
    _candidates = [(c, _NAME_TO_CODE.get(c)) for c in (countries or [])]
    _candidates = [(c, code) for c, code in _candidates if code]
    _storm_infos = (
        list(get_query_executor().map(lambda cc: _resolve_storm_for_country(cc[0], date, run), _candidates))
        if _candidates else []
    )
    country_storm_infos = [
        (code, storm_info) for (c, code), storm_info in zip(_candidates, _storm_infos) if storm_info
    ]
    storm = None
    forecast_date = None
    if country_storm_infos:
        storm = country_storm_infos[0][1]["name"]
        forecast_date = country_storm_infos[0][1]["mat_forecast_date"]
    return codes, primary_country_code, storm, forecast_date, country_storm_infos


@callback(
    Output("ms-tile-config-store", "data"),
    Input("selected-country-store", "data"),
    Input("exposure-property", "value"),
    Input("exposure-view-as", "value"),
    Input("tc-view-as", "value"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-surge-on", "checked"),
    Input("ms-rain-window", "value"),
    Input("ms-river-window", "value"),
    Input("ms-slider-debounce-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("cmdbar-detail", "value"),
    Input("ms-hazards-hidden-store", "data"),
    Input("ms-hazard-render-mode-store", "data"),
    State("ms-wind-slider", "value"),
    State("ms-gust-slider", "value"),
    State("ms-river-slider", "value"),
    State("ms-rain-slider", "value"),
)
def _build_hazard_tile_config(countries, exposure_prop, view_as, tc_view_as, wind_on, gust_on, river_on, rain_on, surge_on, rain_window,
                                river_window, _debounce_tick, date, run, view_mode, hazards_hidden, hazard_render_mode,
                                wind_idx, gust_idx, river_idx, rain_idx):
    countries = countries or []
    # cmdbar-detail ("tiles"/"admin") only ever renders in Country Analysis
    # mode (see _command_bar's own docstring) but stays mounted (just hidden)
    # in Global mode, so its value is always a real Input here, default to
    # "tiles" only for the pre-render tick where Dash hasn't set it yet.
    view_mode = view_mode or "tiles"
    tc_view_as = tc_view_as or "envelopes"
    hazard_render_mode = hazard_render_mode or "probability"
    # River's
    # own map tile/admin raster color + legend range respect this same
    # real cumulative window (see _build_hz's own comment for the full str-
    # from-Dash-needs-int-cast reasoning, identical here). Kept as a
    # SEPARATE config field from rain's own "window_h" below (not reused),
    # for the exact same reason _fetch_combined_facility_rows' own
    # river_window/window_h split exists in tile_server.py: River and Rain
    # can both be simultaneously visible with genuinely different windows.
    river_window = int(river_window) if river_window else _RIVER_WINDOW_DEFAULT
    if not countries:
        return {
            "country": None, "wind_visible": False, "gust_visible": False,
            "river_visible": False, "rain_visible": False, "surge_visible": False,
            "view_mode": view_mode, "tc_view_as": tc_view_as, "any_hazard_on": False,
            "wind_has_data": False, "gust_has_data": False, "river_has_data": False,
            "rain_has_data": False, "real_hazard_available": False,
        }

    resolved_prop = _EXPOSURE_PROP_MAP.get(exposure_prop, "population")
    # Matches /legacy's real "Probability" toggle semantics (callbacks/
    # tiles_and_admin.py's _compute_layer_toggle_outputs): a demographic
    # property is a plain basis layer (raw count) only while no hazard is
    # both checked AND has real resolved data behind it; the moment one
    # does, it SWITCHES (not overlays, each hazard's own MapLibre raster
    # source independently resolves this same prop name from its own table)
    # to the hazard-weighted "expected impact" column instead. Storm Surge
    # is excluded, it never carries real hazard state (surge_visible is
    # always False). any_hazard_on itself is computed further below, once
    # storm/river_forecast_date/rain_forecast_date are all resolved, see
    # that block's own comment for why checkbox-checked state alone isn't
    # enough anymore.

    # Wind/gust share the same real storm/forecast_date, resolved REACTIVELY
    # (first selected country with real impact data for the CURRENTLY
    # selected topbar-date/topbar-time, not "currently active right now")
    # via _resolve_storm_for_country, same reactive lookup
    # _fetch_real_tile_totals now uses for the Impact Summary panel. This is
    # what makes historical dates (date picker, Demo Scenarios) actually
    # drive real wind/gust tile layers and tracks/envelopes instead of
    # silently resolving to nothing.
    #
    # Selecting two countries hit by two
    # DIFFERENT real storms on the same date must NOT force EVERY selected
    # country's wind/gust tiles onto whichever storm this loop resolves
    # first, that would leave the other country's tiles silently coming
    # back empty (its own real storm never matching the shared
    # STORM+FORECAST_DATE filter). Every
    # country's own real storm is resolved once here (country_storm_infos,
    # in original selection order) and grouped below: the FIRST resolved
    # storm keeps the single "storm"/"forecast_date"/tile_country
    # shape (unchanged for the common case of one
    # shared storm), and any OTHER distinct storm among the remaining
    # countries becomes its own "extra_wind_groups"/"extra_gust_groups" entry
    # (see maplibre_tiles.js's own "MULTI-STORM GROUPS" section), a real,
    # separately-queried tile layer for that country's own real storm,
    # instead of silently reusing the primary one.
    #
    # This
    # resolution (codes/primary_code/storm/forecast_date) is shared via
    # _resolve_primary_storm_group with _load_ms_tracks_and_envelopes, so
    # tracks/envelopes can resolve the same storm independently instead of
    # waiting on this whole callback's slower Output (the stats fan-out
    # further below, not this cheap part).
    codes, primary_code, storm, forecast_date, country_storm_infos = \
        _resolve_primary_storm_group(countries, date, run)
    tile_country = "+".join(codes)
    # Selecting a
    # country with genuinely no active storm at all, e.g. Bangladesh on a
    # quiet date, must NOT show a completely blank map with not even the raw
    # Population base layer. The Population/Children/etc EXPOSURE layer
    # piggybacks on this SAME storm-scoped tile URL (there's no separate
    # storm-independent raster endpoint), with storm/forecast_date left
    # None, maplibre_tiles.js's own applyHazardLayer hides the WHOLE wind
    # layer, population included (`if (!country || !parts.forecast_date)`).
    # But the underlying SQL (_MERCATOR_FULL_SQL in tile_server.py) is a
    # real LEFT JOIN FROM BASE_MERCATOR_TILE_MAT, filtered only on
    # b.COUNTRY, any non-matching storm/forecast_date is completely safe
    # to pass (every E_*/impact column just comes back NULL, which is
    # exactly correct when there's genuinely no real impact). tile_storm/
    # tile_forecast_date below are this placeholder, used only for URL-
    # building and the population/exposure color-scale stats query below;
    # `storm`/`forecast_date` themselves stay real (possibly None) so
    # anything that means "is there a REAL storm" (gust's own stats gate,
    # _load_ms_tracks_and_envelopes, etc) still sees the honest answer.
    # Same "NONE" placeholder convention _hazardUrlParts already uses
    # client-side for river/rain's own inert storm segment.
    tile_storm = storm if storm is not None else "NONE"
    tile_forecast_date = forecast_date if forecast_date is not None else _mat_forecast_date(date, run)

    extra_groups_by_storm = {}
    for code, storm_info in country_storm_infos[1:]:
        if storm_info["name"] == storm:
            continue  # same storm as primary, already covered by tile_country/storm/forecast_date
        g = extra_groups_by_storm.setdefault(
            storm_info["name"], {"codes": [], "forecast_date": storm_info["mat_forecast_date"]})
        if code not in g["codes"]:
            g["codes"].append(code)

    wind_idx = wind_idx if wind_idx is not None else 2
    gust_idx = gust_idx if gust_idx is not None else 2
    wind_kt = _WIND_CATS[wind_idx][2]
    # index [3], NOT [2], _WIND_CATS rows are (label, name, wind_kt, gust_kt).
    # Must NOT read [2] (wind's own kt), the
    # GUST_THRESHOLD filter below would then never match a single real row
    # (wind kt values 34/40/50/64/83/96/113/137 vs real gust kt values
    # 17/21/26/33/43/49/58/70 never overlap), leaving the Gust map layer
    # silently empty. See _resolve_gust_kt's own docstring for the
    # verification (BAVI/PHL 2026-07-02: 0 rows at kt=50, real rows at kt=26).
    gust_kt = _WIND_CATS[gust_idx][3]

    # Default index 2 == "rp10" (see _build_hz's own comment on why).
    river_idx = river_idx if river_idx is not None else 2
    rp_tier = _RIVER_RP_TIERS[river_idx]

    # "6"/2, matching ms-rain-window/ms-rain-slider's own live UI
    # defaults (see _build_hz's own comment on this same fallback).
    rain_window = rain_window or "6"
    rain_idx = rain_idx if rain_idx is not None else 2
    threshold_mm = _RAIN_MM_BY_WINDOW[rain_window][rain_idx]

    # River/rain are NOT storm-scoped, each has its own independent
    # forecast_date (can genuinely lag behind the current wind storm's own
    # cycle) resolved straight from Snowflake, not derived from `storm` at
    # all -- and now for the ACTUAL selected topbar date/run (same fix as
    # _fetch_real_combined_tile_totals_uncached, see that function's own
    # comment for the full "why"; previously get_latest_river_forecast_
    # time/get_latest_rain_forecast_time ignored `date`/`run` entirely and
    # always returned the country's all-time-latest cycle, which is why the
    # combined/Probability layer could show a completely different real
    # forecast cycle than the raw layer for the identical selected date).
    river_resolved = (
        get_river_extent_forecast_time_for_date(date, rp_tier) if (primary_code and date) else None
    )
    river_forecast_date = _mat_forecast_date(date, '00') if river_resolved else None
    rain_resolved = (
        get_precip_forecast_time_near(date, run) if (primary_code and date and run is not None) else None
    )
    rain_forecast_date = _mat_forecast_date(date, run) if rain_resolved else None

    # Real per-hazard "does this hazard have REAL underlying data for the
    # current selection", wind/gust share the resolved storm above; river/
    # rain each resolve their own independent forecast_date just above too.
    # This mirrors the exact signal _hurricane_family/_flood_hazards_family
    # already use to grey out their own checkboxes (has_storms/
    # river_available/rain_available), any_hazard_on below is the tile-
    # config-side counterpart of that same "is there something real behind
    # this checkbox" check, not just whether the checkbox itself is checked.
    #
    # Must NOT switch
    # resolved_prop to the hazard-weighted E_* column the instant ANY
    # checkbox is checked, regardless of whether that hazard actually
    # resolves real data: that would show "Sustained Wind — Population"
    # for a country with no active storm at all (e.g. Bangladesh on a
    # quiet date). Sustained Wind stays CHECKED by default even
    # while ms-wind-on is DISABLED (Dash still reports the checkbox's true
    # `checked` value here, `disabled` only blocks user interaction), so a
    # quiet-date country with the default-checked Wind box would otherwise
    # switch the whole exposure layer to E_population, a column that's NULL
    # everywhere with no real storm, since MERCATOR_TILE_IMPACT_MAT never
    # has a matching row. Gating on real-data-per-hazard (not just checked)
    # keeps the exposure layer, its color-scale stats, and the map legend
    # all honestly showing plain Population whenever nothing real backs the
    # checked hazard(s).
    wind_has_data = bool(storm)
    gust_has_data = bool(storm)
    river_has_data = bool(river_forecast_date)
    rain_has_data = bool(rain_forecast_date)
    real_hazard_available = wind_has_data or gust_has_data or river_has_data or rain_has_data
    # Must NOT ignore ms-hazards-hidden-
    # store (the command bar's "HAZARDS" eye-icon temporary hide-all
    # toggle) entirely, clicking it only hides the wind/gust/river/rain
    # MapLibre layers themselves client-side (see setHazardsHiddenOverride
    # in maplibre_tiles.js), so without this check the Population/Children/
    # etc exposure layer would keep showing the hazard-weighted E_* column
    # underneath, unchanged. With hazards visually hidden there is nothing on screen to
    # justify "weighted by hazard", the exposure layer should fall back to
    # the raw base property, exactly as if every hazard checkbox were off.
    any_hazard_on = bool(
        (wind_on and wind_has_data) or (gust_on and gust_has_data) or
        (river_on and river_has_data) or (rain_on and rain_has_data)
    ) and not hazards_hidden
    # _register_ms_facility_layer's
    # fetch must NOT be hardcoded to `hazard=wind` (the FastAPI default),
    # never varying with which hazard checkbox is actually on, a
    # Rain-only or River-only selection with no active wind storm would
    # then always hit
    # SCHOOL_IMPACT_MAT/HC_IMPACT_MAT (wind's own tables) with a storm that
    # doesn't resolve, get zero rows back, and silently fall back to the
    # plain uncolored base layer, leaving infrastructure facility markers
    # (schools/health/shelters/wash) showing no impact at all whenever the
    # active hazard isn't Wind. Facility markers are single point circles
    # (one color per facility) so, unlike the stacked per-hazard MapLibre
    # raster layers, they can only ever reflect ONE hazard's real
    # probability at a time, priority below matches the Hazard tab's own
    # top-to-bottom ordering (Sustained Wind, Gust, River Flooding,
    # Rainfall). Falls back to "wind" when nothing real is active; harmless
    # since any_hazard_on already forces combine=false in that case (see
    # facility_geojson's own docstring in tile_server.py).
    if wind_on and wind_has_data:
        facility_hazard = "wind"
    elif gust_on and gust_has_data:
        facility_hazard = "gust"
    elif river_on and river_has_data:
        facility_hazard = "river"
    elif rain_on and rain_has_data:
        facility_hazard = "rain"
    else:
        facility_hazard = "wind"
    if any_hazard_on:
        resolved_prop = _EXPOSURE_E_PROP_MAP.get(exposure_prop, resolved_prop)
    # "In Need" only ever swaps population/children to their real E_*_in_need
    # column, every other prop (and "At Risk") is untouched, same
    # eligibility rule _update_view_as already enforces for the segmented
    # control itself. Takes precedence over the hazard-weighted switch above
    # (in_need already implies "hazard-weighted", just further vulnerability-
    # refined) regardless of any_hazard_on.
    if view_as == "inneed" and exposure_prop in _EXPOSURE_IN_NEED_PROP_MAP:
        resolved_prop = _EXPOSURE_IN_NEED_PROP_MAP[exposure_prop]

    # Computed HERE (not inline in the return dict below) so the stats
    # fetch just below can gate on the same real value, see its own
    # comment. See the returned "wind_base_fallback" field's own docstring
    # for the full "why": Wind is the single fallback carrier for the
    # plain Population/Children/etc base layer whenever nothing real is
    # actively weighting the display.
    wind_base_fallback = bool(not any_hazard_on and resolved_prop != "probability")

    # Path-segment placeholder for hazards with no real "storm" concept,
    # any non-empty string works (ignored server-side), reuses the real
    # storm name when one is resolved rather than adding a second required
    # config field only to fill an inert URL path segment. Same value as
    # tile_storm above (both exist for the identical reason); kept as its
    # own name here since river/rain's OWN forecast_date is independent of
    # wind's, unlike tile_forecast_date.
    placeholder_storm = tile_storm

    # wind's own stats query now uses tile_storm/tile_forecast_date (the
    # placeholder-aware pair) and is gated on wind_on alone, not "and
    # storm", see tile_storm's own comment above for why: population/
    # exposure min-max (this function's real purpose even with no storm)
    # comes from the SAME LEFT JOIN query and is safe/correct to fetch
    # regardless of whether a real storm resolved.
    #
    # Same tile_storm/tile_forecast_date + wind_on-style gating as wind's
    # own stats above (_MERCATOR_GUST_SQL is the identical LEFT-JOIN-FROM-
    # BASE_MERCATOR_TILE_MAT shape), gust's own raster URL
    # (_hazardUrlParts) already reads the SAME placeholder-aware "storm"/
    # "forecast_date" config fields, so its stats must use the same pair or
    # the population layer would render with no color-scale normalization
    # whenever Gust is checked but no real storm has resolved.
    #
    # These 4 hazards
    # are independent of each other (none reads another's stats), and each
    # itself makes 2 sequential urlopen calls
    # (see _hazard_stats' own comment), up to 8 serial round-trips before
    # ms-tile-config-store updates, which gates tracks, all 4 facility
    # layers, and the MapLibre tile fan-out. Fetching concurrently via the
    # shared executor turns that into ~1 hazard's worth of wall-clock time.
    _hazard_fetches = {}
    # `or wind_base_fallback`: the map itself now renders wind's raster
    # (real Population/Children/etc base layer) whenever wind_base_fallback
    # is true, even with wind_on False (see that flag's own docstring on
    # the return dict below) -- without this OR, stats_wind/admin_stats_wind
    # stay the {} default a few lines below, and _legend_raster_info has no
    # real min/max to show, a real regression this exact fix closes: the
    # map painted a real base layer with zero legend to explain it.
    if wind_on or wind_base_fallback:
        _hazard_fetches["wind"] = lambda: _hazard_stats(tile_country, "wind", tile_storm, tile_forecast_date, wind_kt, {})
    if gust_on:
        _hazard_fetches["gust"] = lambda: _hazard_stats(
            tile_country, "gust", tile_storm, tile_forecast_date, wind_kt, {"gust_threshold": gust_kt})
    if river_on:
        _hazard_fetches["river"] = lambda: _hazard_stats(
            tile_country, "river", placeholder_storm, river_forecast_date, wind_kt,
            {"rp_tier": rp_tier, "window_h": river_window})
    if rain_on:
        _hazard_fetches["rain"] = lambda: _hazard_stats(
            tile_country, "rain", placeholder_storm, rain_forecast_date, wind_kt,
            {"threshold_mm": threshold_mm, "window_h": rain_window})
    _hazard_keys = list(_hazard_fetches.keys())
    _hazard_values = list(get_query_executor().map(lambda k: _hazard_fetches[k](), _hazard_keys)) if _hazard_keys else []
    _hazard_results = dict(zip(_hazard_keys, _hazard_values))
    stats_wind, admin_stats_wind = _hazard_results.get("wind", ({}, {}))
    stats_gust, admin_stats_gust = _hazard_results.get("gust", ({}, {}))
    stats_river, admin_stats_river = _hazard_results.get("river", ({}, {}))
    stats_rain, admin_stats_rain = _hazard_results.get("rain", ({}, {}))

    # Real per-extra-storm-group tile stats, one extra "+"-joined
    # tile_country per distinct non-primary storm, each queried exactly like
    # the primary group above via the same _hazard_stats helper. Empty list
    # in the overwhelming common case (0 or 1 distinct storm across every
    # selected country), maplibre_tiles.js's _applyExtraHazardGroups is a
    # complete no-op when these are empty, zero behavior change there.
    #
    # Fanned out via get_query_executor().map (same shared pool/pattern the
    # primary group's own _hazard_values fetch above already uses, safe to
    # nest the same way _hazard_stats itself already nests its own
    # stats/admin_stats pair onto this pool (see _SHARED_QUERY_EXECUTOR's
    # own module comment in snowflake_utils.py for the bounded-nesting
    # reasoning), not a plain serial for loop. A genuinely multi-storm
    # Global-mode selection (e.g. two real simultaneously active storms,
    # confirmed to happen, see _hurricane_family's own DOLPHIN/GENEVIEVE
    # comment) previously paid N groups x up to 2 hazards x _hazard_stats'
    # own 2 sequential HTTP round-trips back-to-back, one extra storm group
    # away from the exact same serial-chain cost the original perf audit
    # already fixed for the primary group.
    extra_wind_groups, extra_gust_groups = [], []
    _extra_hazard_tasks = []
    for extra_storm, g in extra_groups_by_storm.items():
        group_country = "+".join(g["codes"])
        if wind_on:
            _extra_hazard_tasks.append(("wind", extra_storm, g, group_country))
        if gust_on:
            _extra_hazard_tasks.append(("gust", extra_storm, g, group_country))

    def _fetch_extra_hazard_group(task):
        kind, extra_storm, g, group_country = task
        if kind == "wind":
            return _hazard_stats(group_country, "wind", extra_storm, g["forecast_date"], wind_kt, {})
        return _hazard_stats(
            group_country, "gust", extra_storm, g["forecast_date"], wind_kt, {"gust_threshold": gust_kt})

    _extra_hazard_results = (
        list(get_query_executor().map(_fetch_extra_hazard_group, _extra_hazard_tasks))
        if _extra_hazard_tasks else []
    )
    for (kind, extra_storm, g, group_country), (g_stats, g_admin_stats) in zip(_extra_hazard_tasks, _extra_hazard_results):
        entry = {
            "country": group_country, "storm": extra_storm, "forecast_date": g["forecast_date"],
            "stats": g_stats, "admin_stats": g_admin_stats,
        }
        (extra_wind_groups if kind == "wind" else extra_gust_groups).append(entry)

    return {
        "country": tile_country or None,
        # Single ISO3 code (first selected country), distinct from "country"
        # above which is a "+"-joined multi-country string for tile-server
        # requests, get_track_impacts (real envelope severity/coloring,
        # see _load_ms_tracks_and_envelopes below) needs exactly one real
        # COUNTRY value to match TRACK_MAT/TRACK_VULNERABILITY_MAT rows
        # against, same as river/rain's own primary_code usage just above.
        "primary_country_code": primary_code,
        # Relative/same-origin whenever nginx fronts this container
        # (config.BEHIND_REVERSE_PROXY, set by entrypoint.sh: true on BOTH
        # SPCS and Azure Web App for Containers, since both run the exact
        # same Docker image/entrypoint.sh with nginx proxying Dash + tile
        # server on one public port). Deliberately NOT keyed on
        # config.SPCS_RUN, which only selects Snowflake auth mode: Azure
        # has SPCS_RUN=false but IS behind nginx just like SPCS, so an
        # SPCS_RUN-keyed value here silently sends the browser an absolute
        # http://localhost:8001 URL that resolves against the *viewer's*
        # own machine, not the server. Outside a proxied container (local
        # dev: `python app.py` on :8050 + a separately-run uvicorn tile
        # server on :8001, no nginx in front), config.TILE_SERVER_URL is used instead:
        # "" there is NOT same-origin, it resolves against the Dash app's
        # own port, not the tile server's, so real WebP tile requests
        # silently 200 as the Dash HTML shell instead of image bytes.
        "tile_server_url": "" if config.BEHIND_REVERSE_PROXY else config.TILE_SERVER_URL,
        "tile_prop": resolved_prop,
        "admin_prop": resolved_prop,
        # "raw" / "probability" / "classification", see
        # _hazard_render_mode_switch's own docstring for the full model.
        # Read directly by applyTileConfig/applyHazardLayer in
        # maplibre_tiles.js: "classification" always renders the combined
        # classification raster (regardless of exposure_prop); "raw" hides
        # every per-hazard MapLibre raster (wind/gust show Leaflet envelopes
        # instead (see tc_view_as below) and river/rain show the global
        # raw layer instead (see _build_global_raw_config); "probability"
        # (default) leaves today's Exposure-driven rendering completely
        # unaffected.
        "hazard_render_mode": hazard_render_mode,
        # Whether/how to combine 2+ simultaneously-active hazards into ONE
        # real raster instead of stacking N separate per-hazard rasters is
        # now resolved entirely client-side from tile_prop itself (see
        # maplibre_tiles.js's own isCombinableProp/_AOTS_COMBINABLE_EXPOSURE_
        # PROPS): "probability" combines via the real per-tile bitmask
        # union (no independence formula),
        # Population/Children/Infant/School-age/Adolescent/Built-up combine
        # via real raw-count × combined-probability expected impact (mode=
        # "exposure", see _fetch_combined_raster_tile's own docstring in
        # tile_server.py), everything else (settlement/RWI/poverty/In Need,
        # no real combined formula) keeps stacking per-hazard rasters
        # unchanged. This must NOT be gated on exposure_prop == "probability"
        # specifically, that would leave Population/
        # Children/Built-up never combining at all, visibly double-
        # rendering whenever 2+ hazards are active.
        # Facility points
        # (Schools/Health/Shelters/WASH, _register_ms_facility_layer below)
        # need the SAME "combine with hazard, or show plain?" signal the
        # exposure tile/admin layers above already resolve via
        # resolved_prop's own E_*-vs-plain switch. Single source of truth,
        # any_hazard_on ALREADY factors in ms-hazards-hidden-store (the eye
        # icon), so this one flag correctly covers "no hazard checked" AND
        # "hazards temporarily hidden" without duplicating that logic here.
        "any_hazard_on": any_hazard_on,
        # Real fix for "no hazard checked (or eye-icon hidden) => completely
        # blank map, not even plain Population": repro is literally all 4
        # hazard checkboxes off, AND the HAZARDS eye icon. Population/
        # Children/etc has no raster
        # source of its own, it only ever renders by piggybacking on one of
        # the 4 per-hazard MapLibre layers (see tile_storm's own comment
        # above), each independently gated client-side on
        # config[hazardKey + '_visible'] (applyHazardLayer in
        # maplibre_tiles.js) -- with every hazard off/hidden, every one of
        # those gates is false and nothing paints, even though resolved_prop
        # is already correctly the plain base column here (any_hazard_on is
        # False). True whenever there's a real plain base layer to show
        # (resolved_prop isn't the hazard-only "probability" column, which
        # is meaningless with no hazard active) and nothing is already
        # painting it via a real checked+active hazard. maplibre_tiles.js's
        # applyHazardLayer/setTileLayerProp OR this into wind's own
        # visibility check specifically (not gust/river/rain): Wind is the
        # existing single fallback carrier for "nothing real is active"
        # elsewhere in this function too (see facility_hazard's own
        # "Falls back to wind" comment above), one consistent convention
        # rather than a new one. Uses wind's placeholder-safe tile_storm/
        # tile_forecast_date (always set, never None) so the fallback
        # request is always valid even with zero real storm data. Computed
        # once, above (before the _hazard_fetches block), not re-derived
        # here: that same value also gates whether stats_wind/admin_stats_
        # wind get fetched at all, _legend_raster_info needs a real min/max
        # from there, a second independently-computed copy here risks
        # drifting out of sync with what was actually fetched.
        "wind_base_fallback": wind_base_fallback,
        # ms-hazards-hidden-store is a real Input to THIS callback (see the
        # signature above), so every eye-icon click round-trips through the
        # server and re-triggers applyTileConfig via ms-tile-config-store's
        # own Output. applyTileConfig/applyHazardLayer/useCombined must read
        # hazards_hidden from this config, not rely solely on the eye-icon's
        # OWN immediate client-side setHazardsHiddenOverride call (a
        # one-shot direct layer hide with no lasting state), the server
        # round-trip's applyTileConfig call runs AFTER that immediate
        # hide, so without this flag it would silently re-show every hazard
        # layer since nothing tells it to stay hidden. any_hazard_on already
        # factors hazards_hidden into the EXPOSURE-prop swap, but that
        # doesn't cover actual layer VISIBILITY. Carrying the real flag
        # through here makes the server-
        # computed config the single source of truth applyTileConfig can
        # unconditionally honor, instead of relying on call-ordering.
        "hazards_hidden": bool(hazards_hidden),
        # Which single hazard's real probability facility markers (Schools/
        # Health/Shelters/WASH) should query, see this function's own
        # comment on facility_hazard above.
        "facility_hazard": facility_hazard,
        # Real per-hazard "is there actual data behind this, or just a
        # checked-but-disabled checkbox", the map legend (_legend_raster_
        # info) reads these to avoid labeling a plain Population base layer
        # as e.g. "Sustained Wind, Population" when no real storm exists.
        # real_hazard_available (independent of checkbox state entirely) is
        # the client-side signal for whether the HAZARDS eye icon / hazard-
        # scoped "View As" controls have anything real to act on at all.
        "wind_has_data": wind_has_data, "gust_has_data": gust_has_data,
        "river_has_data": river_has_data, "rain_has_data": rain_has_data,
        "real_hazard_available": real_hazard_available,
        "view_mode": view_mode,
        # "envelopes" (default, today's behavior) or "raster", gates
        # ms-tracks-json/ms-envelopes-json in _load_ms_tracks_and_envelopes
        # below; the MapLibre wind/gust tile raster itself is untouched by
        # this and keeps rendering off wind_visible/gust_visible as before.
        "tc_view_as": tc_view_as,

        # tile_storm/tile_forecast_date (not the possibly-None storm/
        # forecast_date), see tile_storm's own comment above: JS's
        # applyHazardLayer/_hazardUrlParts read these two fields directly to
        # build the wind (and gust) raster/admin tile URLs, and hide the
        # WHOLE layer (population/exposure included) whenever
        # forecast_date is falsy. This keeps the Population base layer
        # rendering (real data, no hazard weighting) even when no real
        # storm is active for the selected country/date.
        "storm": tile_storm,
        "forecast_date": tile_forecast_date,
        "wind_threshold": wind_kt,
        "wind_visible": bool(wind_on),
        "stats_wind": stats_wind, "admin_stats_wind": admin_stats_wind,

        "gust_threshold": gust_kt,
        "gust_visible": bool(gust_on),
        "stats_gust": stats_gust, "admin_stats_gust": admin_stats_gust,

        # Any OTHER real storm among the selected countries beyond
        # the primary one above; see this function's own comment above and
        # maplibre_tiles.js's
        # _applyExtraHazardGroups. Empty in the common single-storm case.
        "extra_wind_groups": extra_wind_groups,
        "extra_gust_groups": extra_gust_groups,

        "river_forecast_date": river_forecast_date,
        "rp_tier": rp_tier,
        # River's own real cumulative window, kept as its own field, NOT
        # reusing "window_h" below (that's Rain's own, genuinely different
        # value/option-set, see this function's own top-of-body comment
        # for why sharing would be wrong whenever both are visible at once).
        # Read by maplibre_tiles.js's own _hazardUrlParts() to build River's
        # mercator/admin/raster URLs.
        "river_window": river_window,
        "river_visible": bool(river_on),
        "stats_river": stats_river, "admin_stats_river": admin_stats_river,

        "rain_forecast_date": rain_forecast_date,
        "threshold_mm": threshold_mm,
        "window_h": rain_window,
        "rain_visible": bool(rain_on),
        "stats_rain": stats_rain, "admin_stats_rain": admin_stats_rain,

        # Storm Surge, explicit visual-only preview, zero real
        # backend anywhere (see this file's own ms-surge-on comment): always
        # False, no tile-server request is ever built for it.
        "surge_visible": False,
    }


@callback(
    Output("exposure-render-mode-note", "children"),
    Input("ms-tile-config-store", "data"),
)
def _update_exposure_render_mode_note(tile_config):
    """Explains why Population/Children/Built-up/etc. do nothing visible
    while the Hazard tab's ms-hazard-render-mode switch is "raw" or
    "classification", see exposure-render-mode-note's own placement
    comment in _exposure_section for the full "why". Reads ms-tile-config-
    store (already carries hazard_render_mode, see _build_hazard_tile_
    config) rather than taking ms-hazard-render-mode as a second Input, so
    this stays correct even in the (Global-mode) case where that switch
    doesn't exist in the DOM at all, tile_config itself is None there.
    """
    if not tile_config:
        return None
    mode = tile_config.get("hazard_render_mode")
    if mode not in ("raw", "classification"):
        return None
    return dmc.Text(
        _t('Population/Children/etc. have no visible effect while Hazard Render Mode is "{mode}" — switch it back to "Probability" (top of the Hazard tab) to use these.',
            mode=_t("Raw") if mode == "raw" else _t("Classification")),
        size="10px", c="orange", fs="italic", mb=10, style={"lineHeight": 1.4},
    )


@callback(
    Output("ms-population-tiles-json", "hideout"),
    Output("ms-population-admin-json", "hideout"),
    Output("ms-probability-tiles-json", "hideout"),
    Output("ms-probability-admin-json", "hideout"),
    Input("ms-tile-config-store", "data"),
)
def _sync_ms_geojson_hideout(tile_config):
    """Reuses the real hideout mechanism from callbacks/tiles_and_admin.py's
    `_compute_layer_toggle_outputs`/`_compute_probability_outputs` (a
    {"prop": ..., "min_val": ..., "max_val": ...} dict, or {"hidden": True})
    for these four placeholder dl.GeoJSON layers.

    Note (see maplibre_tiles.js's own _buildTileTooltip, the real tile/admin
    tooltip source): these four layers hold EMPTY GeoJSON on this page, same
    as on /legacy, so this
    hideout currently has NO visible effect; it's wired here so a future
    consumer of these placeholder layers (unrelated to the real MapLibre
    raster/admin tile_prop/admin_prop, which DO already follow the
    exposure-property radio via _EXPOSURE_PROP_MAP in
    _build_hazard_tile_config above) can reuse this mechanism rather than
    re-deriving it from scratch. Picks whichever hazard is actually visible
    for its stats (first match in wind > gust > river > rain priority order,
    same as the hover-tooltip hazard priority in maplibre_tiles.js);
    {"hidden": True} when none are.
    """
    tile_config = tile_config or {}
    hazard = next((h for h in ("wind", "gust", "river", "rain") if tile_config.get(f"{h}_visible")), None)
    if not hazard:
        hidden = {"hidden": True}
        return hidden, hidden, hidden, hidden
    stats = tile_config.get(f"stats_{hazard}") or {}
    admin_stats = tile_config.get(f"admin_stats_{hazard}") or stats

    def _h(prop, s):
        h = {"prop": prop}
        if s and prop in s:
            h["min_val"] = s[prop].get("min")
            h["max_val"] = s[prop].get("max")
        return h

    pop_hideout = _h("population", stats)
    pop_admin_hideout = _h("population", admin_stats)
    prob_hideout = _h("probability", stats) if "probability" in stats else {"hidden": True}
    prob_admin_hideout = _h("probability", admin_stats) if "probability" in admin_stats else {"hidden": True}
    return pop_hideout, pop_admin_hideout, prob_hideout, prob_admin_hideout


clientside_callback(
    """
    function(tileConfig) {
        if (window.applyTileConfig) {
            window.applyTileConfig(tileConfig);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("ms-tile-config-applied-store", "data"),
    Input("ms-tile-config-store", "data"),
)


# =============================================================================
# GLOBAL RAW LAYER WIRING (precip-raw raster + river-raw raster)
# Entirely independent of ms-tile-config-store/_build_hazard_tile_config
# above, those are gated on a selected country (see that function's own
# "if not countries: return ..." early exit) whereas these two raw layers
# are GLOBAL (one worldwide file per forecast cycle, no COUNTRY/STORM column
# anywhere upstream, see services/tile_server.py's own "Global raw
# precipitation-rate endpoints"/"Global raw river endpoints" sections), so
# they must render in Global mode with nothing selected at all, not just
# once a country/storm is picked.
#
# Despite being "global", this must have real country-awareness: once a
# country is selected, its own ms-tile-config-store-driven layers switch to
# the country-scoped probability/exposure view, and this global raster must
# NOT keep rendering everywhere around it. This callback takes
# `selected-country-store`, and BOTH `precip_visible`/`river_visible` are
# False by default the instant any country is selected, so nothing bleeds
# in unasked for.
#
# `ms-hazard-render-mode` is the ONE unified
# Hazard-tab switch (see _hazard_render_mode_switch's own docstring) that
# provides the way to see this raw layer while a country is selected,
# Country-Analysis-only (absent from the DOM, and therefore always
# None/falsy, in Global mode, where `not countries` alone already makes
# this True regardless). Selecting "raw" renders `precip_visible`/
# `river_visible` exactly as they do in Global mode: genuinely
# global/uncropped, NOT clipped to the selected country (the underlying raw
# precip-rate/river-extent data has no country dimension at all), a
# different concept from the country-scoped "Hazard Probability" Exposure
# radio / the /tiles/raster-combined endpoint (those derive per-tile impact
# PROBABILITY from the MAT impact tables; this raw layer is the actual
# measurement (precip rate in mm, river flood extent) with its own
# Mean/Probability aggregation mode via flood-view-as, which the
# MAT-derived probability has no equivalent of).
#
# NO dedicated checkboxes here anymore, ms-river-on ("River Flooding") and
# ms-rain-on ("Rainfall") from _flood_hazards_family are reused as the single
# on/off control for these two raw layers (this callback is simply an extra,
# independent Input consumer of those same two checkbox ids, see that
# function's own header comment for the full rationale), and flood-view-as
# (nested under Rainfall's own controls in _flood_hazards_family, RAIN ONLY)
# resolves which aggregation mode (mean/probability) the precip-raw endpoint
# should render. River has no mode toggle at all, it always renders
# Probability (see
# _fetch_river_extent_raster_tile's own docstring in services/tile_server.py).
#
# forecast_time resolution for BOTH raw layers follows the
# topbar's own date+time selection (topbar-date/topbar-time) instead of
# always "latest", same as every other, country-scoped hazard layer already
# does via ms-tile-config-store. River (extent_rp10_bymember) is daily-only,
# so it matches on calendar date alone (get_river_extent_forecast_time_for_date);
# precip (tp) has irregular real cycle hours, so it matches to the single
# closest real cycle within a bounded window (get_precip_forecast_time_near),
# see both functions' own docstrings in snowflake_utils.py for the full
# matching-algorithm rationale. Either layer resolving to None (no real
# forecast within the matching window for the selected date) is a real,
# expected state, surfaced via precip_date_unavailable/river_date_unavailable
# below and rendered as a DATE-based "not available" note (ms-raw-date-
# availability-note, wired further down) that is deliberately worded/labeled
# differently from _flood_hazards_family's own COUNTRY-based
# _availability_note, so the two "unavailable" reasons are never conflated.
# =============================================================================

@callback(
    Output("ms-global-raw-config-store", "data"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("flood-view-as", "value"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("ms-metadata-refresh-interval", "n_intervals"),
    Input("ms-rain-window", "value"),
    Input("ms-river-window", "value"),
    Input("ms-slider-debounce-store", "data"),
    Input("selected-country-store", "data"),
    Input("ms-hazard-render-mode-store", "data"),
    Input("ensemble-member-select", "value"),
    State("ms-rain-slider", "value"),
    State("ms-river-slider", "value"),
    prevent_initial_call=False,
)
def _build_global_raw_config(river_on, rain_on, view_as, date, run, _n_intervals, rain_window, river_window,
                                _debounce_tick, countries, hazard_render_mode, member_select, rain_idx, river_idx):
    """Resolves the forecast_time for each global raw layer, matched to the
    selected topbar-date/topbar-time, NOT always "latest" (see this block's
    own header comment above), plus the RAIN-ONLY Mean/Probability
    aggregation mode from flood-view-as, then hands it all off to the
    clientside bridge just below.

    prevent_initial_call=False so this fires immediately on page load (the
    explicit "shown by default on first load, no country selected" ask),
    not just after the first ms-metadata-refresh-interval tick, using
    topbar-date/topbar-time's own initial values (_DEFAULT_FORECAST_DATE/
    _DEFAULT_FORECAST_RUN, the latest real forecast cycle, same default the
    topbar itself opens on). Re-fires on that same 15-minute interval
    afterwards (reusing it rather than adding a second dcc.Interval) so a new
    forecast cycle landing mid-session is picked up without a page reload,
    and immediately whenever either checkbox, the view-as toggle, or the
    topbar date/time changes (cheap: both get_*_forecast_time_* calls are
    ttl_cache'd).

    IMPORTANT: ms-river-on/ms-rain-on ALSO drive _build_hazard_tile_config's
    entirely separate, unchanged, probability-only country-scoped impact
    system (its own river_on/rain_on params), this callback is a second,
    independent consumer of the same two checkboxes, not a replacement for
    that wiring. "Mean" here is a raw-layer-only visualization concept and is
    never combined with that population-impact system.

    ms-rain-window/ms-rain-slider ALSO drive the raw precip-rate raster's
    window_h/threshold_mm, not a value hardcoded to 6h/10mm server-side (see
    services/tile_server.py's _PrecipRawCache). Same Input/State split
    as _build_hazard_tile_config above: ms-rain-window is a direct
    (un-debounced) Input (a SegmentedControl click, not a drag), while
    ms-rain-slider is read as State off the shared ms-slider-debounce-store
    tick so a slider drag doesn't re-push a new tile URL (and re-trigger a
    MapLibre tile reload) on every intermediate drag position.

    River has NO Mean mode (see
    _fetch_river_extent_raster_tile's own docstring in
    services/tile_server.py): it renders Probability unconditionally, so
    this store carries no river-side mode field at all, only rain's own
    `rain_mode`.

    ensemble-member-select ALSO drives
    BOTH raw layers here, mirroring what it already does for Wind/Gust's
    tracks/envelopes (raw map layer only, no impact-number side effects
    from this control alone; see _load_ms_tracks_and_envelopes). Resolved
    via the same `_resolve_ensemble_member()` helper used there. river_member/
    precip_member are None when "Probabilistic" is selected (the aggregate
    view, unchanged default), or an int 1-51 for a specific member.
    """
    rain_mode = view_as if view_as in ("mean", "probability") else "probability"
    member_int = _resolve_ensemble_member(member_select)
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    # "6"/2, matching ms-rain-window/ms-rain-slider's own live UI
    # defaults (see _build_hz's own comment on this same fallback).
    rain_window = rain_window or "6"
    rain_idx = rain_idx if rain_idx is not None else 2
    threshold_mm = _RAIN_MM_BY_WINDOW[rain_window][rain_idx]
    window_h = int(rain_window)
    # Must NOT always resolve/request rp10
    # regardless of ms-river-slider's own value, the raw river layer's
    # return-period tier is genuinely selectable, matching rain's own
    # window/threshold wiring above. Debounced the same way (State off the
    # shared ms-slider-debounce-store tick) so a slider drag doesn't
    # re-trigger a real Snowflake lookup + MapLibre tile reload per tick.
    river_idx = river_idx if river_idx is not None else 2  # index 2 == "rp10", matches ms-river-slider's own default
    rp_tier = _RIVER_RP_TIERS[river_idx]
    is_standin_tier = rp_tier in _RIVER_STANDIN_RP_TIERS
    # See ms-river-window's
    # own comment in _flood_hazards_family for the full "why" (a
    # CUMULATIVE window, real day-1-through-selected-day member-flood
    # union, not a single-day snapshot; see tile_server.py's own
    # "ACCUMULATION SEMANTICS" comment). Forecast_time resolution below is
    # unaffected by step_h (every real lead time lives in the SAME per-date
    # parquet, get_river_extent_forecast_time_for_date only needs to know
    # a real file exists for this date at all).
    river_step_h = int(river_window) if river_window else 72
    try:
        precip_latest = get_precip_forecast_time_near(date, run)
    except Exception as e:
        logger.warning("Could not resolve precip-raw forecast time near %s %sZ: %s", date, run, e)
        precip_latest = None
    try:
        river_latest = get_river_extent_forecast_time_for_date(date, rp_tier)
    except Exception as e:
        logger.warning("Could not resolve river-raw forecast time for %s (rp_tier=%s): %s", date, rp_tier, e)
        river_latest = None

    precip_forecast_time = precip_latest[0] if precip_latest else None
    river_forecast_time = river_latest[0] if river_latest else None

    # Both raster tile endpoints render fully pre-colored server-side (fixed
    # breakpoint ramps, see _colorize_precip_rate/_RIVER_EXTENT_PROB_COLORS
    # in tile_server.py), so no client-side stats call is needed here for
    # either layer's coloring.
    return {
        # See the sibling "tile_server_url" field earlier in this file:
        # this keys off config.BEHIND_REVERSE_PROXY, not config.SPCS_RUN.
        # (A prior version of this comment argued for keying off SPCS_RUN,
        # reasoning that local dev has no nginx so a bare "" resolves
        # against the Dash app's own port instead of the tile server's:
        # that part was correct, but SPCS_RUN was the wrong proxy for "is
        # nginx in front of me": Azure Web App for Containers also runs
        # this same entrypoint.sh behind nginx with SPCS_RUN=false, so an
        # SPCS_RUN-keyed value broke Azure to fix local dev. See
        # BEHIND_REVERSE_PROXY's own docstring in components/config.py.)
        "tile_server_url": "" if config.BEHIND_REVERSE_PROXY else config.TILE_SERVER_URL,
        # Rain-only, river has no Mean mode, always renders Probability
        # server-side regardless of this value.
        "rain_mode": rain_mode,
        "date": date,
        "run": run,
        # Rain-only, read by applyGlobalRawConfig in maplibre_tiles.js to
        # build the precip-raw tile URL's window_h/threshold_mm query params.
        "window_h": window_h,
        "threshold_mm": threshold_mm,
        "precip_forecast_time": precip_forecast_time,
        # False by default the instant any country is selected UNLESS
        # ms-hazard-render-mode is "raw" (see this block's own header
        # comment), always True in Global mode (`not countries`),
        # regardless of the switch's value there (it doesn't exist in the
        # DOM in Global mode, so `hazard_render_mode` is always None there,
        # harmless since `not countries` alone already makes this True).
        "precip_visible": bool(rain_on) and precip_forecast_time is not None and (not countries or hazard_render_mode == "raw"),
        # Checked ON but no real cycle matched within the window, a real,
        # date-specific "not available" state, distinct from
        # _flood_hazards_family's own country-based availability check.
        "precip_date_unavailable": bool(rain_on) and precip_forecast_time is None,
        "river_forecast_time": river_forecast_time,
        "river_visible": bool(river_on) and river_forecast_time is not None and (not countries or hazard_render_mode == "raw"),
        "river_date_unavailable": bool(river_on) and river_forecast_time is None,
        # River-only, the selected return-period tier + whether it's a real,
        # independently-computed tier or an IS_STANDIN=True placeholder that
        # reuses rp10's own extent as a labelled bound (see
        # _RIVER_STANDIN_RP_TIERS's own comment).
        "rp_tier": rp_tier,
        "rp_tier_is_standin": is_standin_tier,
        # The CUMULATIVE
        # forecast lead-time window (hours) the raw river-extent raster
        # renders, see ms-river-window's own comment for the full "why"
        # (a real multi-day union, not a single hardcoded 24h value or a
        # single-day snapshot, either of those would have zero real
        # signal for every onboarded country).
        "river_step_h": river_step_h,
        # A specific
        # ensemble member's own raw layer (None -> unchanged aggregate
        # view). Same resolved int for both, the underlying per-member
        # source data (extent_rp10_bymember / tp Zarr) shares the SAME
        # 51-member ECMWF ensemble numbering TRACK_MAT already uses (see
        # _resolve_ensemble_member's own docstring).
        "river_member": member_int,
        "precip_member": member_int,
    }


@callback(
    Output("ms-raw-date-availability-note", "children"),
    Input("ms-global-raw-config-store", "data"),
)
def _update_raw_date_availability_note(cfg):
    """DATE-based "not available" messaging for the two GLOBAL raw hazard
    layers (raw precip-rate / raw river flood-extent rasters).

    Deliberately SEPARATE from _flood_hazards_family's own
    _availability_note (COUNTRY-based, for the unrelated probability-only
    impact system that ms-river-on/ms-rain-on also drive), both notes can
    be showing at once, for two genuinely different reasons, so each is
    prefixed "Raw layer:" here to keep them unambiguous rather than reusing
    the exact same wording/disabled-state pattern for two different facts.
    """
    if not cfg:
        return None
    notes = []
    if cfg.get("river_date_unavailable"):
        notes.append(_t(
            "Raw layer: no real river flood-extent forecast for {date} (any run).",
            date=cfg.get("date", ""),
        ))
    if cfg.get("precip_date_unavailable"):
        notes.append(_t(
            "Raw layer: no real precipitation forecast at {date} {run}Z.",
            date=cfg.get("date", ""), run=cfg.get("run", ""),
        ))
    if not notes:
        return None
    return [dmc.Text(n, size="10px", c="orange", fs="italic", mb=4) for n in notes]


clientside_callback(
    """
    function(config) {
        if (window.applyGlobalRawConfig) {
            window.applyGlobalRawConfig(config);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("ms-global-raw-config-applied-store", "data"),
    Input("ms-global-raw-config-store", "data"),
)


# Map-tooltip i18n bridge, see ms-map-i18n-store's own comment for the full
# "why" (tooltip_tracks/tooltip_envelopes/tooltip_schools/tooltip_health/
# tooltip_shelters/tooltip_wash/_buildTileTooltip/_buildRawLayerTooltip have
# no server-side _t()/_LANG access, so this copies the request's own
# resolved vocabulary onto window.AOTS_MAP_I18N once at load, _LANG only
# changes via a full page reload (?lang= query param), so a one-shot copy is
# sufficient, no re-fire wiring needed.
clientside_callback(
    """
    function(i18n) {
        window.AOTS_MAP_I18N = i18n || {};
        return window.dash_clientside.no_update;
    }
    """,
    Output("ms-map-i18n-applied-store", "data"),
    Input("ms-map-i18n-store", "data"),
)


# =============================================================================
# TRACK / ENVELOPE LAYER WIRING
# Real hurricane track + wind-envelope GeoJSON for ms-tracks-json/
# ms-envelopes-json, reactive to ms-tile-config-store (the same
# country/storm/forecast_date/wind_threshold config the hazard tile layers
# already key off, see _build_hazard_tile_config above) rather than a
# "Load Layers" button, consistent with this page's reactive design.
# =============================================================================

_MS_EMPTY_FC = {"type": "FeatureCollection", "features": []}


def _ms_geojson_key(payload):
    """Content hash used as dl.GeoJSON's `key` prop so Leaflet actually
    re-renders on data change, same pattern pages/dashboard.py's own
    toggle_tracks_layer/toggle_envelopes_layer callbacks use."""
    try:
        return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    except (TypeError, ValueError):
        return str(id(payload))


def _unwrap_track_lons(lons):
    """A storm track
    crossing the antimeridian (±180°) must NOT render as a near-horizontal
    line stretching across the ENTIRE map, cutting across all of
    Europe/Asia. Leaflet draws a straight cartesian segment between
    consecutive LineString points, so a real `+179.4 -> -178.7` step
    between two adjacent track points (a genuine ~2° move across the date
    line) would otherwise be misread as a ~358° jump "the long way" around
    the globe instead.

    Unwraps the sequence, same real technique this file's own
    _fly_map_to_selection already uses for viewport fitBounds (see that
    function's own comment: "Leaflet's fitBounds accepts lng values
    outside -180..180 fine, it just fits the real rectangle either way",
    the same holds for drawing a LineString).
    Cumulatively shifts every point AFTER a >180° jump by ∓360° so the
    sequence stays numerically continuous (e.g. `179, 179.4, 181.3`
    instead of `179, 179.4, -178.7`), Leaflet then draws the real short
    segment across the date line instead of the long way around the
    whole globe. Real reproduction case: a track with consecutive
    points at lon 179.4 then -178.7."""
    if not lons:
        return lons
    out = [lons[0]]
    offset = 0.0
    for i in range(1, len(lons)):
        diff = lons[i] - lons[i - 1]
        if diff > 180:
            offset -= 360.0
        elif diff < -180:
            offset += 360.0
        out.append(lons[i] + offset)
    return out


def _unwrap_lon_seq(lons, state):
    """The antimeridian fix above must cover envelope POLYGONS too, not
    just TRACK lines, otherwise for a dateline-crossing storm the two
    would render in DIFFERENT world copies, ~360° apart, on screen.

    Same cumulative technique as `_unwrap_track_lons`, generalized to
    carry `state` (a real mutable `[offset, prev_raw_lon_or_None]` pair)
    ACROSS multiple calls rather than resetting per call, so every ring
    of every polygon feature processed within one _build_ms_envelope_geojson
    call stays mutually consistent with every other one, not just
    internally self-consistent. `state[1] is None` only on the very first
    point of the very first call in a run, mirroring
    `_unwrap_track_lons([lons[0]] + ...)`'s own "first point never shifts"
    behavior exactly when state is fresh.

    Known residual limitation, documented rather than silently assumed
    solved: this does NOT guarantee alignment with a separately-built
    track layer's own offset (_build_ms_track_features runs its own
    independent _unwrap_track_lons per member), only that all envelope
    polygons rendered together in one call stay internally coherent with
    each other (the more severe half of the original bug: a single ring
    self-crossing into a globe-spanning streak). Full track+envelope
    alignment would need a shared reference longitude threaded across
    both builder functions, real, deliberately deferred follow-up, not
    attempted here."""
    out = []
    for lon in lons:
        if state[1] is not None:
            diff = lon - state[1]
            if diff > 180:
                state[0] -= 360.0
            elif diff < -180:
                state[0] += 360.0
        state[1] = lon
        out.append(lon + state[0])
    return out


def _unwrap_geom_mapping_lons(mapping, state):
    """Recursively unwrap longitudes in a shapely_mapping()-style geometry
    dict's own 'coordinates', Polygon: list of rings (exterior + holes);
    MultiPolygon: list of polygons, each a list of rings. TC envelope
    geometries are always one of these two types in practice; anything
    else is returned unchanged (no known real case, safer than guessing
    at a coordinate shape this function doesn't recognize)."""
    gtype = mapping.get("type")

    def _unwrap_ring(ring):
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        return list(zip(_unwrap_lon_seq(lons, state), lats))

    if gtype == "Polygon":
        return {**mapping, "coordinates": [_unwrap_ring(ring) for ring in mapping["coordinates"]]}
    if gtype == "MultiPolygon":
        return {**mapping, "coordinates": [[_unwrap_ring(ring) for ring in poly] for poly in mapping["coordinates"]]}
    return mapping


def _build_ms_track_features(df_tracks, storm_name=None, forecast_time=None):
    """Build one LineString Feature per ensemble member from a TC_TRACKS
    query result. Mirrors pages/dashboard.py's load_all_layers track-building
    loop (same properties: ensemble_member, member_type), EXCEPT for the
    antimeridian unwrap below, which dashboard.py's
    own track loop does NOT have (that page is explicitly frozen, so this
    is a deliberate, documented divergence, not lost parity to restore).

    storm_name (optional): tags every feature's properties with
    `track_id` = storm_name, matching the property name tooltip_tracks
    (components/map/javascript.py) already reads to prefix its tooltip
    label. Not needed (left None) for the single-country path, where only
    one storm is ever on the map at once so there's no ambiguity, REQUIRED
    for the Global-mode multi-storm path (_load_ms_tracks_and_envelopes
    below), where several different storms' tracks can render
    simultaneously and would otherwise be indistinguishable on hover.

    forecast_time (optional): tags every feature's properties with the
    exact forecast_time string this fetch was for. Real gap otherwise:
    _select_storm reads ms-tracks-json as a State, but that store is
    refetched by a separately-scheduled callback off the same topbar-date/
    topbar-time Inputs, unordered relative to the storm row click target
    existing in the DOM; a click on a still-visible row right after a
    date/run change could otherwise read the PREVIOUS cycle's track for a
    same-named multi-day storm with no way to tell.
    _storm_track_bounds checks this against the currently-selected date/
    run before trusting a feature's coordinates, rather than trusting a
    separate side-channel timestamp that could itself race independently."""
    features = []
    for member in df_tracks['ENSEMBLE_MEMBER'].unique():
        member_data = df_tracks[df_tracks['ENSEMBLE_MEMBER'] == member].sort_values('LEAD_TIME')
        lats = member_data['LATITUDE'].tolist()
        lons = _unwrap_track_lons(member_data['LONGITUDE'].tolist())
        coordinates = list(zip(lons, lats))
        coordinates = [list(c) for c in coordinates]
        properties = {
            "ensemble_member": int(member) if pd.notna(member) else member,
            "member_type": "control" if member in (51, 52) else "ensemble",
        }
        if storm_name:
            properties["track_id"] = storm_name
        if forecast_time:
            properties["forecast_time"] = str(forecast_time)
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coordinates},
            "properties": properties,
        })
    return features


def _build_ms_envelope_geojson(envelope_df, wind_kt, country=None, storm=None, forecast_date=None, hazard="wind"):
    """Filter a get_envelope_data_snowflake()/get_gust_envelope_data_snowflake()
    result to the currently-selected threshold and parse its WKT geometries
    (ST_ASWKT output) into a FeatureCollection.

    `hazard` ("wind" or "gust") selects the real threshold column
    (wind_threshold/gust_threshold) and the real per-member severity source
    (get_track_impacts/TRACK_MAT vs get_gust_track_impacts/TRACK_GUST_MAT,
    both genuinely real, separately deployed tables). Every
    output feature is tagged "hazard" so style_envelopes/tooltip_envelopes
    (components/map/javascript.py) can color wind vs gust differently when
    both are shown at once.

    Real severity_population/max_population merge (when country/storm/
    forecast_date are given) via get_track_impacts()/get_gust_track_impacts(),
    matching pages/dashboard.py's own (file-based) track_views merge logic
    (its load_all_layers, ~line 2012-2071) but sourced from Snowflake directly
    instead of a track_views parquet file. No vulnerability join for gust (no
    TRACK_GUST_VULNERABILITY_MAT exists), so gust severity is
    population-only, same as every other gust-specific table in this app.

    Every member gets a real severity_population value whenever country/
    storm/forecast_date are given (attributing_severity below), defaulting
    to a CONFIRMED zero for any member absent from the query result or with
    a NULL SEVERITY_POPULATION (TRACK_MAT/TRACK_GUST_MAT frequently has NULL,
    not a literal 0, for a member whose envelope simply doesn't intersect
    the selected country's tiles at all, that IS zero impact for this
    country). Only when country/storm/forecast_date are ALL absent (Global
    mode, attribution never even attempted) is severity_population left
    off entirely. style_envelopes (components/map/javascript.py) renders
    these three cases distinctly: real nonzero severity (yellow->red
    gradient), confirmed zero (grey), and no attribution attempted at all
    (flat orange/yellow consensus fill)."""
    threshold_col = "gust_threshold" if hazard == "gust" else "wind_threshold"
    if envelope_df is None or envelope_df.empty or threshold_col not in envelope_df.columns:
        return dict(_MS_EMPTY_FC)
    try:
        df_f = envelope_df[envelope_df[threshold_col].astype(int) == int(wind_kt)]
    except (TypeError, ValueError):
        return dict(_MS_EMPTY_FC)

    severity_by_member = {}
    attributing_severity = bool(country and storm and forecast_date)
    if attributing_severity:
        try:
            track_impacts = (get_gust_track_impacts(country, storm, forecast_date, int(wind_kt)) if hazard == "gust"
                              else get_track_impacts(country, storm, forecast_date, int(wind_kt)))
            # Both functions' SQL alias columns lowercase ("AS zone_id" etc.),
            # but Snowflake normalizes unquoted identifiers to uppercase, so
            # the DataFrame this actually returns has UPPERCASE columns,
            # same gotcha get_impact_data's own _norm()
            # works around for every other SQL-mode query function.
            # Lowercased here rather than importing that helper, since none
            # of these columns have its special-cased E_ prefix.
            if track_impacts is not None and not track_impacts.empty:
                track_impacts = track_impacts.rename(columns=lambda c: c.lower())
                for _, r in track_impacts.iterrows():
                    member = r.get('zone_id')
                    pop = r.get('severity_population')
                    if pd.notna(member) and pd.notna(pop):
                        severity_by_member[int(member)] = float(pop)
        except Exception as e:
            logger.warning("Could not load real envelope severity for %s/%s/%s: %s", hazard, country, storm, e)
    max_population = max(severity_by_member.values()) if severity_by_member else 0

    features = []
    # See _unwrap_lon_seq's own
    # docstring: one shared, running unwrap `state` across EVERY feature
    # built in this call, so all envelope rings stay mutually consistent
    # with each other for a dateline-crossing storm, not just internally
    # coherent per-ring.
    _lon_unwrap_state = [0.0, None]
    for _, row in df_f.iterrows():
        geom_wkt = row.get('geometry')
        if not geom_wkt or not isinstance(geom_wkt, str) or not geom_wkt.strip():
            continue
        try:
            geom = wkt.loads(geom_wkt)
        except Exception:
            continue
        member = row.get('ensemble_member')
        member_int = int(member) if pd.notna(member) else None
        properties = {
            "ensemble_member": member_int,
            "wind_threshold": int(row[threshold_col]),
            "hazard": hazard,
            # severity_population/max_population deliberately absent when
            # attribution was never attempted at all (Global mode, no
            # country to attribute to), style_envelopes (components/map/
            # javascript.py) renders that case as a flat orange/yellow
            # consensus fill. When it WAS attempted (Country Analysis mode,
            # attributing_severity below), every member gets a real value,
            # defaulting to a CONFIRMED zero, see that branch's own
            # comment for the invariant this maintains.
        }
        if attributing_severity:
            # Must NOT only set the property
            # when member_int is a key in severity_by_member, that would
            # leave every OTHER member (no row at all, or SEVERITY_POPULATION
            # NULL, TRACK_MAT frequently has NULL, not a literal 0, for a
            # member whose envelope simply doesn't intersect the selected
            # country's population tiles at all) with the property entirely
            # absent, indistinguishable from Global mode's "never even tried
            # to attribute" case to style_envelopes, so these members would
            # render the same uninformative flat orange/yellow fill even
            # though attribution genuinely WAS
            # attempted and genuinely found zero for them (it is common for
            # the large majority of members to have NULL SEVERITY_POPULATION
            # rather than an explicit 0.0). A NULL/missing
            # row here means "this member's envelope doesn't reach this
            # country's tiles at all", that IS the correct definition of
            # zero impact for this country, so it defaults to 0.0 (a real,
            # confirmed zero, style_envelopes now renders this distinctly
            # grey), not "unknown."
            properties["severity_population"] = severity_by_member.get(member_int, 0.0) if member_int is not None else 0.0
            properties["max_population"] = max_population
        features.append({
            "type": "Feature",
            "geometry": _unwrap_geom_mapping_lons(shapely_mapping(geom), _lon_unwrap_state),
            "properties": properties,
        })
    # Sort so the HIGHEST-severity envelopes draw LAST (on top), Leaflet
    # renders GeoJSON features in array order, later features on top of
    # earlier ones, so the raw Snowflake ORDER BY ENSEMBLE_MEMBER order
    # otherwise draws them in an arbitrary z-order unrelated to which one
    # actually matters most. The same
    # severity_population value already used to COLOR each envelope
    # (darker red = higher severity) must also drive draw order, otherwise
    # a low-severity
    # member drawn last could fully occlude the high-severity one
    # underneath it. Missing-severity features (country=None/Global mode,
    # attribution never attempted at all) sort first/lowest via the `or 0`
    # default, same as their own orange/yellow consensus fill already
    # implies "nothing definitive known here". Confirmed-zero features
    # (attribution WAS attempted, genuinely found 0, see
    # attributing_severity above) sort the same way, which is fine: they're
    # now visually distinct (grey), so z-order among same-zero features
    # doesn't matter.
    features.sort(key=lambda f: f["properties"].get("severity_population") or 0)
    return {"type": "FeatureCollection", "features": features}


@callback(
    Output("ensemble-member-select", "data"),
    Input("selected-country-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("ms-slider-debounce-store", "data"),
    State("ms-wind-slider", "value"),
)
def _sort_ensemble_members_by_impact(countries, date, run, _debounce_tick, wind_idx):
    """Real per-member population-impact ordering,
    the default "Member 1".."Member 50" numeric order carries no real
    meaning; sorting by each member's own real severity_population
    (get_track_impacts/TRACK_MAT, the exact same real per-member data
    _build_ms_envelope_geojson's own severity coloring already uses) puts
    the members that matter most to THIS country/storm/threshold first,
    instead of an arbitrary ensemble-run numbering.

    Falls back to _ensemble_members()'s default (unsorted) list whenever no
    single real country+storm+threshold resolves, Global mode (no single
    country to query per-member impact for), a country with no active
    storm, or a Snowflake error."""
    countries = countries or []
    if len(countries) != 1:
        return _ensemble_members()
    country = countries[0]
    code = _NAME_TO_CODE.get(country)
    storm_info = _resolve_storm_for_country(country, date, run)
    if not code or not storm_info:
        return _ensemble_members()
    wind_kt = _resolve_wind_kt(wind_idx)
    try:
        df = get_track_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], wind_kt)
    except Exception as e:
        logger.warning("Could not load per-member impacts for ensemble sort (%s): %s", country, e)
        return _ensemble_members()
    if df is None or df.empty:
        return _ensemble_members()
    # get_track_impacts' SQL aliases columns
    # lowercase ("AS zone_id" etc.), but Snowflake normalizes unquoted
    # identifiers to uppercase, so the DataFrame this actually returns has
    # UPPERCASE columns (ZONE_ID/SEVERITY_POPULATION). A `'zone_id' not in
    # df.columns` check without the rename below would therefore always be
    # True, silently falling back to the unsorted default every time.
    # _build_ms_envelope_geojson already documents
    # and works around this exact gotcha for the same function, mirrored
    # here too.
    df = df.rename(columns=lambda c: c.lower())
    if 'zone_id' not in df.columns or 'severity_population' not in df.columns:
        return _ensemble_members()
    impact_by_member = {int(z): (p or 0.0) for z, p in zip(df['zone_id'], df['severity_population']) if pd.notna(z)}
    # Control (ZONE_ID _CONTROL_MEMBER, 51) is a real ensemble member with
    # its own real severity_population like any other, so it's included in
    # the same sort rather than being unconditionally pinned first -- a
    # storm where the deterministic run itself isn't the most severe member
    # should show it wherever its own real impact ranks it.
    all_members = sorted(range(1, _CONTROL_MEMBER + 1), key=lambda m: -impact_by_member.get(m, 0.0))
    return [
        {"value": "combined", "label": _t("Probabilistic")},
        {"group": _t("Ensemble Members"), "items":
            [({"value": "control", "label": _t("Control (deterministic)")} if m == _CONTROL_MEMBER
              else {"value": f"member-{m}", "label": _t("Member {n}", n=m)})
             for m in all_members]},
    ]


@callback(
    Output("ms-tracks-json", "data"),
    Output("ms-tracks-json", "key"),
    Output("ms-envelopes-json", "data"),
    Output("ms-envelopes-json", "key"),
    # Must NOT
    # depend on ms-tile-config-store.data alone, which would put tracks/
    # envelopes (which only ever need country/storm/forecast_date/
    # wind_kt/gust_kt/tc_view_as) behind _build_hazard_tile_config's
    # full per-hazard stats fan-out (the actual slow part, ~4s cold,
    # completely irrelevant to drawing a track line), a serial dependency
    # with no data reason behind it. Depends
    # directly on the same raw inputs _build_hazard_tile_config reads,
    # via the same shared _resolve_primary_storm_group helper (itself
    # backed by an already-ttl_cache'd lookup, so running both callbacks
    # concurrently doesn't double the real Snowflake cost), this callback
    # fires in PARALLEL with _build_hazard_tile_config off the same
    # trigger instead of waiting for its Output.
    Input("selected-country-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("ms-tracks-on", "checked"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("tc-view-as", "value"),
    Input("ensemble-member-select", "value"),
    # ms-wind-slider/
    # ms-gust-slider's own raw `value` must NOT be direct Inputs here
    # ALONGSIDE ms-tile-config-store, that would fire this Snowflake-
    # backed callback once per raw tick AND again when tile_config settles.
    # ms-tile-config-store's own producing callback (_build_hazard_tile_
    # config) IS already triggered by ms-slider-debounce-store, but its own
    # early-return path for Global mode ("if not countries: return {...}",
    # see that function's own comment) never includes wind_threshold/
    # gust_threshold at all, so a Global-mode slider drag settles the
    # debounce store, _build_hazard_tile_config re-runs, but its Output
    # never actually changes shape, and ms-tile-config-store.data alone is
    # not a reliable trigger for this callback in that mode.
    #
    # ms-slider-debounce-
    # store is a direct Input here specifically to
    # provide Global mode's own reactivity, the raw slider
    # `value` itself is not a live Input; only the already-debounced
    # settle event is, so this does not reintroduce a double-fire.
    Input("ms-slider-debounce-store", "data"),
    # wind_idx/gust_idx are used in BOTH branches below, this callback
    # does not receive tile_config at all (see comment above), so Country
    # Analysis mode must resolve its threshold from these State values
    # directly rather than from tile_config.
    State("ms-wind-slider", "value"),
    State("ms-gust-slider", "value"),
)
def _load_ms_tracks_and_envelopes(countries, date, run, tracks_on, wind_on, gust_on, tc_view_as, member_select,
                                     _debounce_tick, wind_idx, gust_idx):
    """Fetch real track/envelope GeoJSON for the placeholder ms-tracks-json/
    ms-envelopes-json layers whenever country/storm selection, date/run, the
    tracks/wind/gust checkboxes, or tc-view-as change, OR ms-slider-
    debounce-store settles (the debounced wind/gust threshold, for both
    Country Analysis and Global mode), no dedicated "Load Layers" button on
    this page, one of these fires on every relevant change in either mode.

    Must NOT
    depend solely on ms-tile-config-store.data, produced by
    _build_hazard_tile_config, a much heavier callback whose own slow part
    (the per-hazard stats fan-out for map tile coloring, ~4s cold) has
    nothing to do with drawing a track/envelope. Depending on it alone
    would make this callback
    wait behind an unrelated ~4s+ computation on every cold country/storm
    selection for no data reason. Country/storm/forecast_date are
    resolved directly here via the same _resolve_primary_storm_group helper
    _build_hazard_tile_config itself uses (backed by an already-ttl_cache'd,
    single-flight lookup, so this doesn't double the real Snowflake cost
    when both callbacks fire together), this callback runs in PARALLEL
    with _build_hazard_tile_config off the same trigger, instead of behind
    it.

    Reuses the exact TC_TRACKS query and get_envelope_data_snowflake()
    (components/data/snowflake_utils.py) pages/dashboard.py's own
    load_all_layers callback already uses for this, no new Snowflake
    queries are introduced here.

    Global mode (no country selected) is a separate branch below: ALL real
    storms active at the selected topbar date/run (_resolve_storms_for_date,
    the same resolver already powering the Global-mode Active Storms list)
    render as ONE combined tracks FeatureCollection, unconditionally
    (mirrors /legacy's own load_startup_tracks bypass-the-toggle precedent,
    tracks aren't gated on wind_visible/tc_view_as in Global mode, since
    there's no per-hazard checkbox governing "all storms" the way there is
    for a single selected storm). Envelopes DO render in Global mode when
    ms-wind-on is checked, real per-storm TC_ENVELOPES_COMBINED polygons
    for every active storm at the selected wind-severity threshold, just
    without per-country severity coloring (no single country to attribute
    population severity to here; falls back to flat gray, same as
    _build_ms_envelope_geojson already does whenever country isn't given).

    member_select (ensemble-member-select, only ever visible/meaningful in
    Country Analysis mode, see _command_bar's own docstring) filters the
    single-country branch's tracks/envelopes down to just that one real
    ensemble member when set to anything other than "combined"
    (_resolve_ensemble_member). Has no effect on the Global-mode branch
    above (multi-storm "all active storms" view has no single member
    concept to apply this to).
    """
    # "Storm Tracks" (ms-tracks-on) must NOT render
    # unconditionally regardless of its own checked state, it needs to be
    # an Input to this callback. Gates the TRACKS output only (both
    # branches below still compute tracks_data normally, then this checkbox
    # is applied right before each `return`), envelopes are a separate
    # concept (gated by tc_view_as=="envelopes" already) and are untouched
    # by this checkbox either way.
    tracks_on = tracks_on is not False

    codes, primary_country_code, storm, forecast_date, _infos = \
        _resolve_primary_storm_group(countries, date, run)

    if not primary_country_code:
        # get_track_ids_for_date, NOT _resolve_storms_for_date -- the latter
        # requires real nonzero MERCATOR_TILE_IMPACT_MAT impact (correct for
        # the Active Storms alert list, wrong here: a storm can have fully
        # real ensemble track data while still being far out at sea with
        # zero measurable country impact yet). This answers "does a real track exist",
        # matching /legacy's own load_startup_tracks precedent (queries
        # TC_TRACKS directly, no impact join at all).
        forecast_time_str = f"{date} {run}:00:00" if date and run is not None else None
        track_ids = get_track_ids_for_date(forecast_time_str) if forecast_time_str else []
        if not track_ids:
            # Genuinely quiet period for this date, same empty
            # FeatureCollection as any other "no data" gate here, not an
            # error state.
            return _MS_EMPTY_FC, dash.no_update, _MS_EMPTY_FC, dash.no_update
        # Tuple, not a list, get_multi_storm_tracks is now @ttl_cache'd and
        # needs every arg hashable.
        pairs = tuple((track_id, forecast_time_str) for track_id in track_ids)
        all_features = []
        try:
            df_all = get_multi_storm_tracks(pairs)
        except Exception as e:
            logger.error("Error loading multi-storm tracks for Global mode (%s): %s", pairs, e)
            df_all = pd.DataFrame()
        if not df_all.empty:
            for track_id, df_storm in df_all.groupby('TRACK_ID'):
                all_features.extend(_build_ms_track_features(df_storm, storm_name=track_id, forecast_time=forecast_time_str))
        tracks_data = {"type": "FeatureCollection", "features": all_features}
        if not tracks_on:
            tracks_data = dict(_MS_EMPTY_FC)

        # Real per-storm wind/gust envelope polygons, one Snowflake query per
        # active storm per checked hazard (get_envelope_data_snowflake /
        # get_gust_envelope_data_snowflake, both genuinely real, separately
        # deployed tables), no per-country severity coloring
        # (country=None), since a Global-mode storm can affect many
        # countries or none yet; _build_ms_envelope_geojson already falls
        # back to a low-opacity consensus fill in that case. Wind and Gust
        # combine into ONE FeatureCollection (each feature tagged "hazard")
        # rather than a second dl.GeoJSON layer, matching how both are a
        # single shared tc-view-as concept everywhere else on this page.
        envelope_features = []
        if wind_on:
            wind_kt = _resolve_wind_kt(wind_idx)
            for track_id in track_ids:
                try:
                    env_df = get_envelope_data_snowflake(track_id, forecast_time_str)
                except Exception as e:
                    logger.warning("Could not load Global-mode wind envelope for %s/%s: %s", track_id, forecast_time_str, e)
                    continue
                fc = _build_ms_envelope_geojson(env_df, wind_kt, storm=track_id, hazard="wind")
                envelope_features.extend(fc.get("features", []))
        if gust_on:
            gust_kt = _resolve_gust_kt(gust_idx)
            for track_id in track_ids:
                try:
                    gust_env_df = get_gust_envelope_data_snowflake(track_id, forecast_time_str)
                except Exception as e:
                    logger.warning("Could not load Global-mode gust envelope for %s/%s: %s", track_id, forecast_time_str, e)
                    continue
                fc = _build_ms_envelope_geojson(gust_env_df, gust_kt, storm=track_id, hazard="gust")
                envelope_features.extend(fc.get("features", []))
        envelope_features.sort(key=lambda f: f["properties"].get("severity_population") or 0)
        envelope_data = {"type": "FeatureCollection", "features": envelope_features} if envelope_features else dict(_MS_EMPTY_FC)
        return tracks_data, _ms_geojson_key(tracks_data), envelope_data, _ms_geojson_key(envelope_data)

    # storm/forecast_date/primary_country_code already resolved above (same
    # _resolve_primary_storm_group call the Global-mode gate used), not
    # read from tile_config, see this callback's own docstring.
    wind_kt = _resolve_wind_kt(wind_idx)
    gust_kt = _resolve_gust_kt(gust_idx)
    wind_on_cfg = bool(wind_on)
    gust_on_cfg = bool(gust_on)
    # Must NOT require wind_visible
    # specifically (`not tile_config.get("wind_visible")` alone), that
    # would hide TRACKS
    # and gust's own envelope too when unchecking "Sustained Wind" while
    # keeping "Gust" checked, even though gust has nothing to do with
    # that gate. Proceeds whenever EITHER hazard is on.
    #
    # storm == "NONE", defensive: _resolve_primary_storm_group only ever
    # returns a real storm name or None, never this placeholder string
    # (that placeholder is _build_hazard_tile_config's own tile_storm, a
    # DIFFERENT, MapLibre-URL-only value never exposed here), kept for
    # parity with the original tile_config-sourced check.
    if not storm or storm == "NONE" or not forecast_date or not (wind_on_cfg or gust_on_cfg):
        return _MS_EMPTY_FC, dash.no_update, _MS_EMPTY_FC, dash.no_update

    # "raster" view-as hides ONLY the envelope Leaflet layer (so the MapLibre
    # wind/gust probability raster, already rendering independently off
    # wind_visible/gust_visible, is the sole on-map representation of the
    # hazard), same empty-FeatureCollection short-circuit as before, just
    # scoped to envelopes only. This must NOT
    # be part of the SAME early-return gate as tracks above, that would
    # ALSO wipe tracks whenever tc_view_as=="raster" (Country Analysis's own
    # real default) even though
    # tracks are a
    # logically separate concept from envelopes (gated only by ms-tracks-on,
    # applied further below) with nothing to do with the envelope-vs-
    # raster visualization choice. A combined gate here would produce a
    # visible
    # "tracks render for a moment, then disappear" race, the FIRST
    # ms-tile-config-store update after selecting a country still has a
    # stale/empty config (so this callback's OTHER, Global-mode branch above
    # runs instead and renders tracks unconditionally), then a SECOND update
    # lands with the real resolved config (tc_view_as=="raster" by default),
    # which would wipe tracks too under a combined gate.
    show_envelopes = (tc_view_as or "envelopes") != "raster"

    try:
        forecast_dt_str = pd.to_datetime(forecast_date, format="%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as e:
        logger.error("Could not parse forecast_date %r for tracks/envelopes: %s", forecast_date, e)
        return _MS_EMPTY_FC, dash.no_update, _MS_EMPTY_FC, dash.no_update

    tracks_data = dict(_MS_EMPTY_FC)
    try:
        # get_tracks_for_storm (not a bare inline query anymore), cached on
        # (storm, forecast_dt_str), same perf fix as the envelope queries
        # below: tracks don't depend on the wind/gust threshold at all, so a
        # slider tick shouldn't re-fetch them from Snowflake.
        df_tracks = get_tracks_for_storm(storm, forecast_dt_str)
        if not df_tracks.empty:
            tracks_data = {"type": "FeatureCollection", "features": _build_ms_track_features(df_tracks)}
    except Exception as e:
        logger.error("Error loading tracks for map_shell_concept (%s/%s): %s", storm, forecast_dt_str, e)

    # Wind and Gust envelopes combine into ONE FeatureCollection (each
    # feature tagged "hazard") rather than a second dl.GeoJSON layer, see
    # _build_ms_envelope_geojson's own docstring. get_gust_envelope_data_
    # snowflake/TC_GUST_ENVELOPES_COMBINED and get_gust_track_impacts/
    # TRACK_GUST_MAT are both genuinely real, separately deployed tables.
    envelope_features = []
    if show_envelopes and wind_on_cfg and wind_kt is not None:
        try:
            envelope_df = get_envelope_data_snowflake(storm, forecast_dt_str)
            if not envelope_df.empty and 'geometry' in envelope_df.columns:
                envelope_df = envelope_df[envelope_df['geometry'].notna() & (envelope_df['geometry'].astype(str).str.strip() != '')]
                fc = _build_ms_envelope_geojson(
                    envelope_df, wind_kt,
                    country=primary_country_code, storm=storm, forecast_date=forecast_date, hazard="wind")
                envelope_features.extend(fc.get("features", []))
        except Exception as e:
            logger.error("Error loading wind envelopes for map_shell_concept (%s/%s): %s", storm, forecast_dt_str, e)
    if show_envelopes and gust_on_cfg and gust_kt is not None:
        try:
            gust_envelope_df = get_gust_envelope_data_snowflake(storm, forecast_dt_str)
            if not gust_envelope_df.empty and 'geometry' in gust_envelope_df.columns:
                gust_envelope_df = gust_envelope_df[gust_envelope_df['geometry'].notna() & (gust_envelope_df['geometry'].astype(str).str.strip() != '')]
                fc = _build_ms_envelope_geojson(
                    gust_envelope_df, gust_kt,
                    country=primary_country_code, storm=storm, forecast_date=forecast_date, hazard="gust")
                envelope_features.extend(fc.get("features", []))
        except Exception as e:
            logger.error("Error loading gust envelopes for map_shell_concept (%s/%s): %s", storm, forecast_dt_str, e)
    envelope_features.sort(key=lambda f: f["properties"].get("severity_population") or 0)
    envelope_geojson = {"type": "FeatureCollection", "features": envelope_features} if envelope_features else dict(_MS_EMPTY_FC)

    # Real ensemble-member filter (ensemble-member-select), both tracks and
    # envelopes already tag every feature with a real "ensemble_member" int
    # (_build_ms_track_features/_build_ms_envelope_geojson), so picking a
    # specific member here just keeps that one feature from each
    # FeatureCollection instead of all 51. "combined" (the default) filters
    # nothing, same as before this control existed.
    member = _resolve_ensemble_member(member_select)
    if member is not None:
        tracks_data = {"type": "FeatureCollection",
                        "features": [f for f in tracks_data.get("features", [])
                                       if f["properties"].get("ensemble_member") == member]}
        envelope_geojson = {"type": "FeatureCollection",
                             "features": [f for f in envelope_geojson.get("features", [])
                                            if f["properties"].get("ensemble_member") == member]}

    if not tracks_on:
        tracks_data = dict(_MS_EMPTY_FC)
    return tracks_data, _ms_geojson_key(tracks_data), envelope_geojson, _ms_geojson_key(envelope_geojson)


# =============================================================================
# FACILITY LAYER WIRING
# Direct-browser-fetch-to-tile-server pattern, same as callbacks/overlays.py's
# _register_overlay_toggle, bypasses Dash's callback POST channel (its ~10 MB
# size limit large countries exceed) entirely. Keyed off ms-tile-config-store
# and the matching ms-facility-{layer_id}-on checkbox, the fetch fires
# whenever country/storm/threshold changes OR the checkbox is toggled,
# short-circuiting to an empty FeatureCollection while unchecked.
# =============================================================================

def _register_ms_facility_layer(layer_id: str) -> None:
    clientside_callback(
        f"""
        async function(config, checked) {{
            if (!checked || !config || !config.country) {{
                return [{{"type":"FeatureCollection","features":[]}}, window.dash_clientside.no_update];
            }}
            // This fetch must NOT ALWAYS query wind's own facility
            // tables (SCHOOL_IMPACT_MAT/HC_IMPACT_MAT/...) regardless of which
            // hazard is actually checked, or infrastructure facility
            // markers would show no impact whenever the active hazard
            // isn't Wind. config.facility_hazard (see
            // _build_hazard_tile_config's own comment) picks the single real
            // hazard to color/size these markers by, and window._hazardUrlParts
            // (maplibre_tiles.js) resolves the SAME per-hazard storm/
            // forecast_date/query-string the raster layers already use,
            // river/rain in particular need their OWN independent forecast_date,
            // not config.forecast_date (wind's own placeholder-aware pair).
            var base = (config.tile_server_url != null && config.tile_server_url !== '')
                ? config.tile_server_url
                : window.location.origin;
            // Facility markers must NOT only ever reflect ONE hazard
            // (whichever facility_hazard's priority order wins) while the
            // raster background already shows a real combined view for 2+
            // active hazards. Whenever 2+ hazards are genuinely active
            // (config.any_hazard_on already folds in the eye icon's
            // hazards_hidden state, same signal the raster's own
            // useCombined uses), route to the combined-hazard endpoint
            // instead of picking a single hazard by priority.
            var activeCount = window._combinedActiveHazardCount ? window._combinedActiveHazardCount(config) : 0;
            // Facility
            // markers must NOT ignore hazard_render_mode entirely, staying
            // fully probability-tinted even in "Raw" mode, where every
            // per-hazard raster is hidden in favor of Wind/Gust's own
            // Leaflet envelopes and River/Rain's own global raw layer (see
            // applyHazardLayer's own hazard_render_mode gate in
            // maplibre_tiles.js). isRawMode forces facility markers to the
            // same plain/uncolored state combine=false already produces,
            // for consistency with what "Raw" means everywhere else on
            // this map.
            var isRawMode = config.hazard_render_mode === 'raw';
            var urls = [];
            if (!isRawMode && config.any_hazard_on && activeCount >= 2) {{
                var cStorm = config.storm || 'NONE';
                var buildCombinedUrl = function(gCountry, windOn, windForecastDate, gustOn) {{
                    return base + '/geojson/facilities-combined/{layer_id}/'
                        + encodeURIComponent(gCountry) + '/'
                        + encodeURIComponent(cStorm)
                        + '?wind_on=' + windOn
                        + '&wind_forecast_date=' + encodeURIComponent(windForecastDate || '')
                        + '&wind_threshold=' + config.wind_threshold
                        + '&gust_on=' + gustOn
                        + (config.gust_threshold != null ? '&gust_threshold=' + config.gust_threshold : '')
                        + '&river_on=' + (!!config.river_visible)
                        + '&river_forecast_date=' + encodeURIComponent(config.river_forecast_date || '')
                        + (config.rp_tier ? '&rp_tier=' + encodeURIComponent(config.rp_tier) : '')
                        // River's own real cumulative
                        // window, see services/tile_server.py's
                        // facility_geojson_combined's own docstring for why
                        // this is kept as a separate query param from
                        // window_h (Rain's own, below) rather than shared.
                        + (config.river_window != null ? '&river_window=' + config.river_window : '')
                        + '&rain_on=' + (!!config.rain_visible)
                        + '&rain_forecast_date=' + encodeURIComponent(config.rain_forecast_date || '')
                        + (config.threshold_mm != null ? '&threshold_mm=' + config.threshold_mm : '')
                        + (config.window_h != null ? '&window_h=' + config.window_h : '');
                }};
                urls.push(buildCombinedUrl(config.country, !!config.wind_visible, config.forecast_date, !!config.gust_visible));
                // This combined branch must NOT ignore extra_wind_groups/
                // extra_gust_groups entirely, unlike the single-hazard else
                // branch right below (which already patches this same
                // multi-storm gap), otherwise a second country hit by a
                // DIFFERENT real storm would get NO combined facility
                // markers at all (or silently reuse the primary country's
                // storm/date). Group by country since a single extra
                // storm+country can appear in both extra_wind_groups AND
                // extra_gust_groups when both are active for it, one
                // combined-endpoint request per extra country, not two
                // separate wind-only/gust-only ones that would each miss
                // the other's contribution to the SAME country's markers.
                var extraGroupMap = {{}};
                (config.extra_wind_groups || []).forEach(function(g) {{
                    if (!g || !g.country) return;
                    var e = extraGroupMap[g.country] || {{storm: g.storm, forecast_date: g.forecast_date, wind: false, gust: false}};
                    e.wind = true;
                    extraGroupMap[g.country] = e;
                }});
                (config.extra_gust_groups || []).forEach(function(g) {{
                    if (!g || !g.country) return;
                    var e = extraGroupMap[g.country] || {{storm: g.storm, forecast_date: g.forecast_date, wind: false, gust: false}};
                    e.gust = true;
                    extraGroupMap[g.country] = e;
                }});
                Object.keys(extraGroupMap).forEach(function(gCountry) {{
                    var g = extraGroupMap[gCountry];
                    urls.push(buildCombinedUrl(gCountry, g.wind && !!config.wind_visible, g.forecast_date, g.gust && !!config.gust_visible));
                }});
            }} else {{
                var haz = config.facility_hazard || 'wind';
                var parts = window._hazardUrlParts
                    ? window._hazardUrlParts(haz, config)
                    : {{storm: config.storm, forecast_date: config.forecast_date, qs: '&hazard=wind'}};
                if (!parts.forecast_date) {{
                    return [{{"type":"FeatureCollection","features":[]}}, window.dash_clientside.no_update];
                }}
                // combine=false when no hazard is genuinely active (every
                // checkbox off, OR the eye icon has hazards temporarily
                // hidden), facility points must show as plain, uncolored
                // locations in that state, not still tinted by whatever
                // wind_threshold happens to be selected underneath.
                var combineFlag = (!isRawMode && config.any_hazard_on) ? 'true' : 'false';
                var buildUrl = function(country, storm, forecastDate) {{
                    return base + '/geojson/facilities/{layer_id}/'
                        + encodeURIComponent(country) + '/'
                        + encodeURIComponent(storm) + '/'
                        + encodeURIComponent(forecastDate)
                        + '?wind_threshold=' + config.wind_threshold
                        + parts.qs
                        + '&combine=' + combineFlag;
                }};
                urls.push(buildUrl(config.country, parts.storm, parts.forecast_date));
                // A
                // second selected country hit by a DIFFERENT real storm
                // than the primary one must NOT get its facility markers
                // queried against the WRONG storm/date (0 rows, silently
                // falling back to uncolored base points), same multi-
                // storm gap the raster layer already solves via
                // extra_wind_groups/extra_gust_groups. Wind/Gust only,
                // matching the raster's own current scope, River/Rain
                // aren't storm-scoped and share this same "primary-country-
                // only forecast_date" limitation even in the raster today
                // (see _build_hazard_tile_config's own "wind/gust groups
                // only for this round" comment).
                var extraGroups = (haz === 'wind') ? config.extra_wind_groups
                    : (haz === 'gust') ? config.extra_gust_groups : null;
                if (extraGroups && extraGroups.length > 0) {{
                    extraGroups.forEach(function(g) {{
                        if (g && g.country && g.storm && g.forecast_date) {{
                            urls.push(buildUrl(g.country, g.storm, g.forecast_date));
                        }}
                    }});
                }}
            }}
            // Counts toward window._aots_pending_fetches, a SEPARATE
            // counter from window._aots_map_tiles_loading (MapLibre's own
            // dataloading/idle flag in maplibre_tiles.js) so the two
            // signals can't stomp on each other (e.g. this fetch finishing
            // first must not clear the badge while MapLibre tiles are still
            // mid-load). This is a real visible layer (schools/health/
            // shelters/wash points) loading via a direct browser fetch that
            // bypasses Dash's own callback dispatch entirely, so Dash's
            // generic _dash-loading-callback signal never covers it alone.
            window._aots_pending_fetches = (window._aots_pending_fetches || 0) + 1;
            if (window._aots_checkLoadingIndicator) window._aots_checkLoadingIndicator();
            try {{
                var resps = await Promise.all(urls.map(function(u) {{
                    return fetch(u).then(function(r) {{ return r.ok ? r.json() : null; }}).catch(function() {{ return null; }});
                }}));
                var features = [];
                resps.forEach(function(g) {{
                    if (g && g.features) features = features.concat(g.features);
                }});
                var geojson = {{"type": "FeatureCollection", "features": features}};
                return [geojson, Date.now().toString()];
            }} catch(e) {{
                console.error('[AoTS] Failed to fetch ms-{layer_id}:', e);
                return [{{"type":"FeatureCollection","features":[]}}, window.dash_clientside.no_update];
            }} finally {{
                window._aots_pending_fetches = Math.max(0, (window._aots_pending_fetches || 1) - 1);
                if (window._aots_checkLoadingIndicator) window._aots_checkLoadingIndicator();
            }}
        }}
        """,
        [Output(f"ms-{layer_id}-json", "data"),
         Output(f"ms-{layer_id}-json", "key")],
        Input("ms-tile-config-store", "data"),
        Input(f"ms-facility-{layer_id}-on", "checked"),
    )


for _ms_lid in ("schools", "health", "shelters", "wash"):
    _register_ms_facility_layer(_ms_lid)


# =============================================================================
# HAZARDS LABEL, TEMPORARY HIDE-ALL PREVIEW
# Clicking the "HAZARDS" label in the command bar (_command_bar) forces every
# hazard's MapLibre layers invisible purely client-side, without touching any
# real checkbox/store, un-hiding just re-applies window._aots_tile_config
# (the last real config applyTileConfig already caches), so nothing about the
# actual configuration is lost. See setHazardsHiddenOverride in
# maplibre_tiles.js for the actual show/hide mechanics.
# =============================================================================
clientside_callback(
    """
    function(n_clicks, hidden) {
        if (!n_clicks) return window.dash_clientside.no_update;
        var next = !hidden;
        if (window.setHazardsHiddenOverride) window.setHazardsHiddenOverride(next);
        return next;
    }
    """,
    Output("ms-hazards-hidden-store", "data"),
    Input("cmdbar-hazards-label", "n_clicks"),
    State("ms-hazards-hidden-store", "data"),
    prevent_initial_call=True,
)

clientside_callback(
    """
    function(hidden) {
        return hidden ? 'mdi:eye-off-outline' : 'mdi:eye-outline';
    }
    """,
    Output("cmdbar-hazards-eye", "icon"),
    Input("ms-hazards-hidden-store", "data"),
)

# When NO real hazard
# data exists at all for the current selection (real_hazard_available,
# see _build_hazard_tile_config's own wind_has_data/gust_has_data/
# river_has_data/rain_has_data comment), any_hazard_on is already forced
# False there regardless of this eye icon's own hidden/shown state, there
# is nothing real left for it to hide. This dims the eye icon + "HAZARDS"
# label and blocks clicks on it (pointerEvents:none, same disabled
# treatment _PILL_DISABLED_STYLE already gives an individual hazard pill
# with no real data), so the control itself honestly reflects that
# "hazards switched off" is already the map's real state, not a togglable
# choice, for this selection.
clientside_callback(
    """
    function(tileConfig) {
        var available = tileConfig && tileConfig.real_hazard_available;
        return available
            ? {cursor: 'pointer', userSelect: 'none'}
            : {cursor: 'not-allowed', userSelect: 'none', pointerEvents: 'none', opacity: 0.5};
    }
    """,
    Output("cmdbar-hazards-label", "style"),
    Input("ms-tile-config-store", "data"),
)


# =============================================================================
# TILE-SERVER CACHE PRE-WARMING
# Fires services/tile_server.py's GET /preload/ (background-thread pandas
# cache fill, see that endpoint's own docstring) for every real threshold
# value a currently-visible hazard's slider can reach, so scrubbing the
# slider afterward hits an already-warm cache instead of a cold Snowflake
# round-trip. Same "real country/storm/forecast_date present" gate and
# direct-browser-fetch-to-tile-server pattern as _register_ms_facility_layer
# above; side-effect only, reuses that callback's existing dummy-sink store
# (ms-tile-config-applied-store) rather than adding a new one.
#
# Threshold lists are embedded as JSON literals generated FROM the real
# Python module-level lists (_WIND_CATS/_RIVER_RP_TIERS/_RAIN_MM_BY_WINDOW)
# at import time, not hand-duplicated, keeps this the single source of
# truth those lists already are.
#
# Wind and Gust each read their OWN column out of the same _WIND_CATS rows
# (index [2] for wind kt, index [3] for gust kt, see _resolve_gust_kt's own
# docstring) and are independently-visible hazards with independent
# thresholds, so each is preloaded separately when visible.
# Rain only preloads the CURRENTLY selected rain_window's 3 mm tiers (not
# all 4 windows x 3 tiers), switching window is rare relative to scrubbing
# the depth-tier slider within a window, so the other 3 windows' 9
# combinations aren't worth the extra Snowflake load.
# =============================================================================

clientside_callback(
    f"""
    function(tileConfig) {{
        if (!tileConfig || !tileConfig.country || !tileConfig.storm || !tileConfig.forecast_date) {{
            return window.dash_clientside.no_update;
        }}
        var base = (tileConfig.tile_server_url != null && tileConfig.tile_server_url !== '')
            ? tileConfig.tile_server_url
            : window.location.origin;
        var WIND_KTS = {json.dumps([c[2] for c in _WIND_CATS])};
        // Real gust kt values (index [3]), NOT the same as wind's (index
        // [2]); the gust preload loop below must NOT reuse WIND_KTS, which
        // would never match a real GUST_THRESHOLD row (see _resolve_gust_kt's
        // own docstring).
        var GUST_KTS = {json.dumps([c[3] for c in _WIND_CATS])};
        var RIVER_TIERS = {json.dumps(_RIVER_RP_TIERS)};
        var RAIN_MM_BY_WINDOW = {json.dumps(_RAIN_MM_BY_WINDOW)};
        var windThreshold = tileConfig.wind_threshold;

        function fire(country, storm, forecastDate, params) {{
            if (!country || !storm || !forecastDate) return;
            var qs = new URLSearchParams(params).toString();
            var url = base + '/preload/' + encodeURIComponent(country) + '/'
                + encodeURIComponent(storm) + '/' + encodeURIComponent(forecastDate) + '?' + qs;
            fetch(url).then(function(resp) {{
                if (!resp.ok) console.warn('[AoTS] preload failed (' + resp.status + '):', url);
            }}).catch(function(e) {{
                console.warn('[AoTS] preload error:', url, e);
            }});
        }}

        if (tileConfig.wind_visible) {{
            WIND_KTS.forEach(function(kt) {{
                fire(tileConfig.country, tileConfig.storm, tileConfig.forecast_date,
                    {{wind_threshold: kt, hazard: 'wind'}});
            }});
        }}

        if (tileConfig.gust_visible) {{
            GUST_KTS.forEach(function(kt) {{
                fire(tileConfig.country, tileConfig.storm, tileConfig.forecast_date,
                    {{wind_threshold: windThreshold, hazard: 'gust', gust_threshold: kt}});
            }});
        }}

        if (tileConfig.river_visible) {{
            var riverStorm = tileConfig.storm || 'NONE';
            // Must NOT omit window_h/river_window entirely, unlike Rain's
            // own identical loop right below (which correctly reads
            // tileConfig.window_h), omitting it would always
            // warm the server's STEP_H=168 cache slot (ensure_mercator/
            // ensure_admin/ensure_facility's own `window_h or
            // _RIVER_WINDOW_DEFAULT` fallback) regardless of the user's
            // real selected window, so a user on window 24/72/120 would pay
            // a full cold-cache Snowflake round-trip on their real first
            // request despite the prewarm having just run, since the
            // effort was spent on the wrong cache key. Mirrors rain's
            // own pattern.
            RIVER_TIERS.forEach(function(tier) {{
                fire(tileConfig.country, riverStorm, tileConfig.river_forecast_date,
                    {{wind_threshold: windThreshold, hazard: 'river', rp_tier: tier, window_h: tileConfig.river_window}});
            }});
        }}

        if (tileConfig.rain_visible) {{
            var rainStorm = tileConfig.storm || 'NONE';
            var windowH = tileConfig.window_h || '6';
            var mmList = RAIN_MM_BY_WINDOW[windowH] || [];
            mmList.forEach(function(mm) {{
                fire(tileConfig.country, rainStorm, tileConfig.rain_forecast_date,
                    {{wind_threshold: windThreshold, hazard: 'rain', threshold_mm: mm, window_h: windowH}});
            }});
        }}

        return window.dash_clientside.no_update;
    }}
    """,
    Output("ms-tile-config-applied-store", "data", allow_duplicate=True),
    Input("ms-tile-config-store", "data"),
    prevent_initial_call=True,
)


# =============================================================================
# Demo Scenario tile-cache prewarm, closes the remaining "first load is
# slow" gap.
# =============================================================================
# The clientside_callback right above already warms EVERY threshold value
# for whichever country/storm/date is CURRENTLY selected, but only
# reactively, once ms-tile-config-store updates after a user (or a Demo
# Scenario click) has already picked it. That still leaves the FIRST combo
# itself paying a full cold Snowflake fetch inline (a fresh
# country/storm/date/threshold combo can take a few seconds cold, vs 1-6ms
# once warm). Since this app ships a small, fixed, known set of Demo Scenario
# presets (see _DEMO_SCENARIOS), warming exactly those specific combos
# proactively at process startup, before any real user has had time to
# click anything, means the app's own one-click demos are already warm by
# the time anyone actually uses them. Deliberately bounded to just this
# known list, not an attempt to warm every possible country/storm/date/
# threshold combination (that would be unbounded/combinatorial).
def _prewarm_urlopen_with_retry(url, timeout=15, max_attempts=6, initial_delay=2.0):
    """Self-healing replacement for a bare `urllib.request.urlopen(url,
    timeout=...).read()` in every prewarm function below, this Dash app
    and services/tile_server.py are two SEPARATE processes started
    independently (no shared startup barrier between them), so these
    daemon prewarm threads can and do fire their first request before
    uvicorn has finished binding its port, spuriously failing with
    "Connection refused" even though the tile server comes up correctly a
    moment later. Without a retry, a fresh `python app.py` +
    `uvicorn services.tile_server:app` restart can leave demo-scenario
    caches genuinely cold, a one-shot prewarm thread that has already
    exited by the time the tile server is ready never re-warms them
    without a manual re-trigger. Retries with linear backoff (2s, 4s, 6s...) up to
    max_attempts (worst case ~30s total) before giving up for real, bounded,
    not infinite, so a genuinely-unreachable tile server still fails and
    gets logged same as before, just not on the very first transient
    startup race."""
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return urllib.request.urlopen(url, timeout=timeout).read()
        except Exception as e:
            last_exc = e
            if attempt < max_attempts:
                time.sleep(initial_delay * attempt)
    raise last_exc


def _prewarm_demo_scenario_tile_cache() -> None:
    """Background, best-effort warm-up of the tile server's per-country
    pandas DataCache (services/tile_server.py's own /preload/ endpoint) for
    every REAL country affected by each _DEMO_SCENARIOS entry's storm.
    Fire-and-forget: runs in its own daemon thread so it never delays Dash
    startup/readiness, and every failure is logged and swallowed, a
    Snowflake hiccup here must never crash the app, same convention as
    tile_server.py's own _prewarm_raw_caches.

    Must NOT only warm _DEMO_SCENARIOS'
    own hand-listed single "countries" entry (e.g. just Jamaica for
    MELISSA), a multi-country storm's real affected countries (e.g.
    MELISSA also genuinely impacts Cuba/Nicaragua/Turks and Caicos Islands,
    all selectable together via the Global-mode Active Storms row click,
    same as _resolve_storms_for_date's own docstring describes) need
    warming too, or selecting the whole storm, not just its one demo-listed
    country, would still pay the full cold-cache tax for every country
    beyond the first. _resolve_storms_for_date already resolves the real, complete
    country list for whatever storm is real on this date; using ITS list
    here instead of _DEMO_SCENARIOS' own hint keeps this correct even if a
    future demo scenario's storm's real footprint changes."""
    base = "" if config.SPCS_RUN else config.TILE_SERVER_URL
    if not base:
        return
    wind_kt = _resolve_wind_kt(None)
    seen_dates = set()
    for scenario in _DEMO_SCENARIOS:
        date_key = (scenario["date"], scenario["time"])
        if date_key in seen_dates:
            continue  # multiple demo scenarios sharing the same date/run already resolved
        seen_dates.add(date_key)
        try:
            storms = _resolve_storms_for_date(scenario["date"], scenario["time"])
        except Exception as e:
            logger.warning("Prewarm: could not resolve storms for demo scenario date %s: %s", scenario["date"], e)
            continue
        for storm in storms:
            for country_name in storm["countries"]:
                try:
                    code = _NAME_TO_CODE.get(country_name)
                    if not code:
                        continue
                    url = (f"{base}/preload/{urllib.parse.quote(code)}/{urllib.parse.quote(storm['name'])}"
                           f"/{urllib.parse.quote(storm['mat_forecast_date'])}?wind_threshold={wind_kt}")
                    _prewarm_urlopen_with_retry(url)
                    logger.info("Prewarm: demo scenario %s/%s tile cache warm-up requested",
                                country_name, storm["name"])
                except Exception as e:
                    logger.warning("Prewarm: demo scenario %s/%s tile cache warm-up failed: %s",
                                    country_name, storm["name"], e)


threading.Thread(target=_prewarm_demo_scenario_tile_cache, daemon=True, name="demo-scenario-prewarm").start()


# Same cadence as services/tile_server.py's own _PREWARM_INTERVAL_SECONDS,
# not importable across the process boundary (this Dash app and the tile
# server run as separate processes/services), so redefined here as its own
# constant rather than sharing one.
_RECENT_PREWARM_INTERVAL_SECONDS = 60 * 60


def _prewarm_recent_tile_cache() -> None:
    """Background, ROLLING warm-up of the tile server's per-country pandas
    DataCache for the REAL latest 3 distinct forecast dates (get_recent_
    forecast_dates), re-run every _RECENT_PREWARM_INTERVAL_SECONDS, so the
    most recent 3 days are also warm for loading, in addition to the demo
    scenarios.

    _prewarm_demo_scenario_tile_cache above only warms the app's own fixed
    _DEMO_SCENARIOS presets (MELISSA/28 Oct 2025, BAVI period/2 Jul 2026),
    a real, currently-active storm a user actually clicks on (e.g. DOLPHIN
    today) is neither of those, so it still paid the full cold-cache tax on
    first click. This is a genuinely ROLLING prewarm (unlike the demo one,
    a one-shot at startup) since "the latest 3 days" is a moving target,
    mirrors tile_server.py's own _prewarm_raw_caches rolling-window pattern,
    just for the per-country wind/gust tile cache instead of the global raw
    river/precip rasters. No explicit eviction of aged-out dates: unlike
    the raw layer's own dense global grids, this per-country DataCache
    entry is small and _DataCache itself has no size bound to protect (see
    that class's own docstring), stale entries simply stop being re-warmed
    and age out via _TILE_TTL (15 min) the next time nobody's actively
    using them, same as the demo scenario entries already do.
    """
    base = "" if config.SPCS_RUN else config.TILE_SERVER_URL
    if not base:
        return
    wind_kt = _resolve_wind_kt(None)
    while True:
        try:
            recent = get_recent_forecast_dates(3)
        except Exception as e:
            logger.warning("Prewarm: could not resolve recent forecast dates: %s", e)
            recent = []
        for date_str, run_str in recent:
            try:
                storms = _resolve_storms_for_date(date_str, run_str)
            except Exception as e:
                logger.warning("Prewarm: could not resolve storms for recent date %s: %s", date_str, e)
                continue
            for storm in storms:
                for country_name in storm["countries"]:
                    try:
                        code = _NAME_TO_CODE.get(country_name)
                        if not code:
                            continue
                        url = (f"{base}/preload/{urllib.parse.quote(code)}/{urllib.parse.quote(storm['name'])}"
                               f"/{urllib.parse.quote(storm['mat_forecast_date'])}?wind_threshold={wind_kt}")
                        _prewarm_urlopen_with_retry(url)
                        logger.info("Prewarm: recent-date %s/%s/%s tile cache warm-up requested",
                                     date_str, country_name, storm["name"])
                    except Exception as e:
                        logger.warning("Prewarm: recent-date %s/%s/%s tile cache warm-up failed: %s",
                                        date_str, country_name, storm["name"], e)
        time.sleep(_RECENT_PREWARM_INTERVAL_SECONDS)


threading.Thread(target=_prewarm_recent_tile_cache, daemon=True, name="recent-date-tile-prewarm").start()


def _prewarm_demo_scenario_raw_layers() -> None:
    """Background, best-effort warm-up of the GLOBAL raw river-extent/
    precip-rate rasters (services/tile_server.py's own /preload/river-raw/
    and /preload/precip-raw/ endpoints, see _build_global_raw_config's own
    header comment for why these are a completely separate system from the
    per-country tile cache _prewarm_demo_scenario_tile_cache above warms)
    for each _DEMO_SCENARIOS date, across every real topbar run value
    (_RUN_VALUES), not just the one run each scenario happens to default to.

    The BAVI-period demo scenario's real Rainfall
    data only exists on the 06Z run for its date (its own default run, for
    the Global flood-hazards
    preview), a user manually switching to any OTHER run to explore must
    NOT pay the full ~1.2GB Zarr cold-download cost inline for that
    run, so every run for this date needs warming too.
    get_precip_forecast_time_near
    resolves several different (date, run) inputs to the SAME underlying
    forecast cycle, so distinct resolved forecast_times (not raw run
    values) are de-duplicated before firing the actual expensive download,
    this stays a small, bounded set
    (at most 4 runs x 2 demo dates x 2 hazards) rather than an unbounded
    "every possible time" sweep."""
    base = "" if config.SPCS_RUN else config.TILE_SERVER_URL
    if not base:
        return
    seen_dates = set()
    seen_precip_times, seen_river_times = set(), set()
    for scenario in _DEMO_SCENARIOS:
        if scenario["date"] in seen_dates:
            continue
        seen_dates.add(scenario["date"])
        for run in _RUN_VALUES:
            try:
                resolved = get_precip_forecast_time_near(scenario["date"], run)
            except Exception as e:
                logger.warning("Prewarm: could not resolve precip-raw time for %s %sZ: %s", scenario["date"], run, e)
                continue
            forecast_time = resolved[0] if resolved else None
            if not forecast_time or forecast_time in seen_precip_times:
                continue
            seen_precip_times.add(forecast_time)
            try:
                url = f"{base}/preload/precip-raw/{urllib.parse.quote(forecast_time)}"
                _prewarm_urlopen_with_retry(url)
                logger.info("Prewarm: demo scenario precip-raw %s warm-up requested", forecast_time)
            except Exception as e:
                logger.warning("Prewarm: demo scenario precip-raw %s warm-up failed: %s", forecast_time, e)
        try:
            resolved = get_river_extent_forecast_time_for_date(scenario["date"])
        except Exception as e:
            logger.warning("Prewarm: could not resolve river-raw time for %s: %s", scenario["date"], e)
            resolved = None
        forecast_time = resolved[0] if resolved else None
        if forecast_time and forecast_time not in seen_river_times:
            seen_river_times.add(forecast_time)
            try:
                url = f"{base}/preload/river-raw/{urllib.parse.quote(forecast_time)}"
                _prewarm_urlopen_with_retry(url)
                logger.info("Prewarm: demo scenario river-raw %s warm-up requested", forecast_time)
            except Exception as e:
                logger.warning("Prewarm: demo scenario river-raw %s warm-up failed: %s", forecast_time, e)


threading.Thread(target=_prewarm_demo_scenario_raw_layers, daemon=True, name="demo-scenario-raw-prewarm").start()


