"""
Map-first shell (full-bleed map, floating dismissible panels, mode-aware
Controls, two-tier timeline) — the main dashboard, promoted from the
/map-shell concept exploration. Synthesizes pieces from three earlier
Artifact mockups: global_zoom_navigation.html (top bar shape, Global-mode
content — simple worldwide checkboxes + Active Storms list),
weatherlab_style_navigation.html (floating-panel treatment), and
dashboard_synthesis_v2.html (always-visible Impact Summary panel).

Now wired to a real MapLibre/Leaflet dual-layer map (same stack as
layouts/panels.py) for base-layer rendering, zoom/pan, and basemap
switching — the synthetic canvas placeholder is gone. Hazard/tile/facility
DATA layers are still separate follow-up work (see the project plan). The
old dashboard (pages/dashboard.py) is kept at /legacy as a working
reference/fallback. Deliberately skips the app's usual make_header/AppShell
chrome — this page's own full-bleed shell replaces it, not the other way
around.
"""
import hashlib
import json
import logging
import os
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import dash
import dash_mantine_components as dmc
import dash_leaflet as dl
import pandas as pd
import plotly.graph_objects as go
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
    get_latest_forecast_time_overall, get_available_wind_thresholds,
    get_country_totals, get_base_tiles, get_snowflake_connection,
    get_envelope_data_snowflake, get_gust_envelope_data_snowflake, get_storms_for_country_date,
    get_storms_and_countries_for_date, get_track_impacts, get_gust_track_impacts,
    get_latest_river_forecast_time, get_latest_rain_forecast_time, ttl_cache,
    get_precip_forecast_time_near, get_river_extent_forecast_time_for_date,
    get_multi_storm_tracks, get_track_ids_for_date, get_gust_track_ids_for_date, get_tracks_for_storm,
    get_tile_impacts, get_gust_tile_impacts, get_river_tile_impacts, get_rain_tile_impacts,
)
from components.data.data_store_utils import get_data_store, get_impact_data

logger = logging.getLogger(__name__)

dash.register_page(__name__, path="/", name="Ahead of the Storm")

# ---------------------------------------------------------------------------
# Real Snowflake-backed reference data — loaded once at module import time,
# same "parallel startup queries" pattern as pages/dashboard.py SECTION 2.
# Feeds _country_options()/_STORMS below (cross-cutting reference data, same
# for every visitor) plus the per-country helper functions further down
# (_get_country_stats/_get_country_pin_pct/_get_country_totals/
# _get_data_availability_real), which run their own per-country/per-storm
# queries reactively on country/storm selection — same module-level-vs-
# per-callback split pages/dashboard.py and callbacks/metrics.py already use.
# ---------------------------------------------------------------------------
with ThreadPoolExecutor(max_workers=3) as _startup_pool:
    _f_countries = _startup_pool.submit(get_active_countries)
    _f_metadata = _startup_pool.submit(get_snowflake_data)
    _f_active_storm_codes = _startup_pool.submit(get_active_storm_countries)
    _countries_df = _f_countries.result()
    _metadata_df = _f_metadata.result()
    _active_storm_country_codes = _f_active_storm_codes.result()

# Lazy-ish singleton (created once here, at import time) — mirrors
# pages/dashboard.py's own module-level `data_store = get_data_store()`.
_giga_store = get_data_store()

# COUNTRY_NAME (e.g. "Jamaica"), not COUNTRY_CODE, is used as the dropdown
# VALUE below — every existing dict-keyed-by-display-name lookup elsewhere
# on this page (country selection values were always display names, e.g.
# "Philippines") keeps working unchanged against real data.
_CODE_TO_NAME = (dict(zip(_countries_df['COUNTRY_CODE'], _countries_df['COUNTRY_NAME']))
                 if not _countries_df.empty else {})
_NAME_TO_CODE = {v: k for k, v in _CODE_TO_NAME.items()}
_ACTIVE_STORM_COUNTRY_NAMES = [_CODE_TO_NAME.get(code, code) for code in _active_storm_country_codes]

# A region row's own COUNTRY_CODE (e.g. "ECA") is a synthetic code, not a
# real ISO3 the tile server understands — its real member codes live in
# PIPELINE_COUNTRIES.MEMBER_CODES as a JSON array string (confirmed live,
# e.g. '["ATG","BRB","DMA",...]'), analogous to pages/dashboard.py's own
# REGION_MEMBERS dict (which this page doesn't import/build, since its
# country picker already stores a flat list of individually-selected
# names — plain countries and regions both by NAME — rather than
# dashboard.py's single-dropdown-value-expands-to-members model).
_CODE_TO_MEMBER_CODES = (
    dict(zip(_countries_df['COUNTRY_CODE'], _countries_df.get('MEMBER_CODES', pd.Series(dtype=object))))
    if not _countries_df.empty and 'MEMBER_CODES' in _countries_df.columns else {}
)

# NAME -> (lat, lon, zoom) real per-country map view, straight from
# get_active_countries()'s own CENTER_LAT/CENTER_LON/VIEW_ZOOM columns
# (PIPELINE_COUNTRIES) — same _countries_df already used for _CODE_TO_NAME/
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
    of their codes — the tile server's own `_country_in_clause` (tile_server.py)
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
# m/s) — moved up here from further down in the file (where the wind-
# severity slider's own readout still uses it) so the real _STORMS-building
# code just below can also use it, at module-import time, to derive a
# storm's rough category from its highest available wind-envelope
# threshold (get_available_wind_thresholds) — there's no dedicated "storm
# category" column/function anywhere in snowflake_utils.py.
_WIND_CATS = [
    ("Minor", "Tropical Storm", 34, 17), ("Minor", "Strong Trop. Storm", 40, 21),
    ("Moderate", "Severe Trop. Storm", 50, 26), ("Significant", "Category 1 Hurricane", 64, 33),
    ("Major", "Category 2 Hurricane", 83, 43), ("Severe", "Category 3 Hurricane", 96, 49),
    ("Extreme", "Category 4 Hurricane", 113, 58), ("Catastrophic", "Category 5 Hurricane", 137, 70),
]
# PREVIEW ONLY — illustrative per-tier ratio, NOT wired into the real Impact
# Summary/table numbers anywhere (see _hazard_breakdown, which still only
# reads the checkbox on/off state, not this). Index 2 (Severe Trop. Storm,
# today's default slider position) = 1.0, i.e. exactly today's baseline
# number — every other tier is a ratio relative to that, decreasing as
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


def _ensemble_max_kt(storm_name, forecast_time_str):
    """Real max wind speed (kt) across ALL ensemble members — worst-case/
    upper-bound statistic (can be driven by a single outlier member; see
    _category_label_ensemble_max's own docstring for the GENEVIEVE example
    where this differs a lot from a single-member or median reading).

    Returns None if TC_TRACKS has no rows at all for this storm/forecast.
    """
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
    ensemble members (see _ensemble_max_kt) — "TS" (not a granular Tropical
    Storm/Strong Trop. Storm/Severe Trop. Storm sub-label) for anything below
    Category 1's 64kt threshold, the real _category_label_from_kt tier label
    otherwise. Returns "Unknown" when TC_TRACKS has no real data at all for
    this storm/forecast.

    Note: this is a worst-case statistic, not a central estimate — e.g.
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

    Combines three independently-real signals — none of which alone matches
    the old mock's exact (name/countries/date/cat) shape, per the task's own
    "adapt the mock shape to match reality" guidance:
      - get_snowflake_data(): distinct TRACK_ID/FORECAST_TIME combos already
        in TC_TRACKS (a storm's name + when its forecast was issued).
      - latest_time (get_latest_forecast_time_overall(), computed once at
        module level and shared with _DEFAULT_FORECAST_DATE/_RUN below — see
        there for why): the current forecast cycle, so only storms from the
        LATEST run are shown, not every historical run ever ingested.
      - get_active_storm_countries(): which countries have genuine
        (non-zero) storm impact right now.
      - _category_label_ensemble_max(): real worst-case max wind speed across
        all ensemble members from TC_TRACKS — see its own docstring for the
        "TS" sub-64kt labeling rule.

    Documented limitation (not silently papered over): none of these
    functions join a specific storm to specific countries — only "which
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

# Two color families instead of four unrelated hues — Tropical Cyclone
# (Tracks/Sustained Wind/Gust) reads as one group of orange shades, Flood
# Hazards (River/Rainfall) as one group of blue shades. Darker = the
# default-on/primary layer within each family, lighter = the secondary one.
TRACKS = "#b5480a"
WIND = "#e8590c"
GUST = "#ffa94d"
RIVER = "#1864ab"
RAIN = "#4dabf7"
# Teal, not another blue — distinct from RIVER/RAIN so Storm Surge reads as
# its own hazard within the Flood family, not a third shade of the same two.
SURGE = "#0c8599"
# Neutral slate, not a blend of WIND/RIVER (which comes out a muddy brown)
# and not PIN_COLOR's purple (already means "in need" elsewhere) — reads as
# "neither hazard on its own", i.e. the Tropical Cyclone/Flood overlap.
HAZARD_BOTH_COLOR = "#6c7a89"
# Darker shade of the same slate, not an unrelated hue — reads as "the same
# 'shared risk' concept as HAZARD_BOTH_COLOR, just more of it" (all 3 of a
# family's own members at once, vs. any 2).
HAZARD_TRIPLE_COLOR = "#3d4550"
# The PATTERN itself escalates with hazard count, not just the color: a
# single diagonal hatch for "2 at once", a criss-cross (two hatch
# directions layered) for "all 3 at once" — white translucent lines over
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
# .runoff-swatch) — a third, deliberately distinct color family for
# "in need" (PIN/CHIN), which isn't a hazard, isn't exposure, it's the
# vulnerability lens on top of both.
PIN_COLOR = "#7c5cbf"
PIN_COLOR_LIGHT = "#a78bda"

# Same tile_palettes.json app.py's own /map-static/tile_palettes.js route
# serves to the browser (window._AOTS_PALETTES) — loaded here too so the map
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
# i18n — module-level current language, set once per request by layout()
# (a function, so Dash's page router can pass ?lang=es as a kwarg) and read
# by every _t() call made while building that same response. Internal dict
# KEYS/VALUES (_COUNTRY_STATS, _STAT_ICONS, ids, etc.) always stay English —
# only the point where a string is actually rendered gets wrapped in _t().
# Good enough for this single-user concept page; a real multi-user page
# would need request-scoped state instead of a bare module global.
# ---------------------------------------------------------------------------
_LANG = "en"

_TRANSLATIONS = {
    "es": {
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
        "Other": "Otro", "Deterministic": "Determinista", "Compare Worst Case By": "Comparar Peor Caso Por",
        "Show": "Mostrar", "Combined": "Combinado", "Print": "Imprimir",
        # Stat labels
        "People": "Personas", "Children": "Niños", "Schools": "Escuelas",
        "Health Centers": "Centros de Salud", "Shelters": "Refugios", "WASH Facilities": "Instalaciones WASH",
        "At Risk": "En Riesgo", "In Need": "En Necesidad",
        "(shown as At Risk above In Need for People & Children)": "(mostrado como En Riesgo sobre En Necesidad para Personas y Niños)",
        "(shown as Tropical Cyclone only / Both / Flood only share of each value)":
            "(mostrado como la parte Solo Ciclón Tropical / Ambos / Solo Inundación de cada valor)",
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
        "Envelopes": "Envolventes", "Probability Raster": "Ráster de Probabilidad",
        "Mean": "Media", "Probability": "Probabilidad",
        "River Flooding": "Inundación Fluvial", "Rainfall": "Precipitación", "Proxies": "Aproximaciones",
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
        # Impact panel
        "Impact Summary": "Resumen de Impacto",
        "Global — worldwide totals across visible hazards": "Global — totales mundiales de los peligros visibles",
        "Global — worldwide totals across all hazards": "Global — totales mundiales combinando todos los peligros",
        "Reflects only countries currently initialized in the database.":
            "Solo incluye los países actualmente inicializados en la base de datos.",
        "None of the initialized countries are currently impacted. This does not mean there is no real impact — potentially affected countries may not yet be in the database.":
            "Ningún país inicializado está actualmente afectado. Esto no significa que no haya un impacto real: los países potencialmente afectados pueden no estar aún en la base de datos.",
        "Full Impact Breakdown": "Desglose Completo de Impacto", "Hazard Contribution": "Contribución por Peligro",
        "Alert Email": "Correo de Alerta", "Open in new tab ↗": "Abrir en nueva pestaña ↗",
        "Total: {value}": "Total: {value}",
        "Illustrative split — a real implementation would compute this from actual per-hazard overlap.":
            "División ilustrativa — una implementación real calcularía esto a partir del solapamiento real por peligro.",
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
        # UN disclaimer
        ("The boundaries and names shown and the designations used on this map "
         "do not imply official endorsement or acceptance by the United Nations."):
            "Los límites y nombres mostrados y las designaciones utilizadas en este mapa "
            "no implican reconocimiento o aceptación oficial por parte de las Naciones Unidas.",
        # Language switcher
        "Language": "Idioma",
    },
    "fr": {
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
        "Other": "Autre", "Deterministic": "Déterministe", "Compare Worst Case By": "Comparer le pire scénario par",
        "Show": "Afficher", "Combined": "Combiné",
        "People": "Personnes", "Children": "Enfants", "Schools": "Écoles",
        "Health Centers": "Centres de santé", "Shelters": "Abris", "WASH Facilities": "Installations EAH",
        "At Risk": "À risque", "In Need": "Dans le besoin",
        "(shown as At Risk above In Need for People & Children)": "(affiché comme À risque au-dessus de Dans le besoin pour Personnes et Enfants)",
        "(shown as Tropical Cyclone only / Both / Flood only share of each value)":
            "(affiché comme la part Cyclone tropical seulement / Les deux / Inondation seulement de chaque valeur)",
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
        "Envelopes": "Enveloppes", "Probability Raster": "Raster de probabilité",
        "Mean": "Moyenne", "Probability": "Probabilité",
        "River Flooding": "Inondation fluviale", "Rainfall": "Précipitations", "Proxies": "Approximations",
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
        "Impact Summary": "Résumé de l'impact",
        "Global — worldwide totals across visible hazards": "Mondial — totaux mondiaux des risques visibles",
        "Global — worldwide totals across all hazards": "Mondial — totaux mondiaux combinant tous les risques",
        "Reflects only countries currently initialized in the database.":
            "Ne reflète que les pays actuellement initialisés dans la base de données.",
        "None of the initialized countries are currently impacted. This does not mean there is no real impact — potentially affected countries may not yet be in the database.":
            "Aucun pays initialisé n'est actuellement touché. Cela ne signifie pas qu'il n'y a pas d'impact réel : des pays potentiellement touchés peuvent ne pas encore figurer dans la base de données.",
        "Full Impact Breakdown": "Répartition complète de l'impact", "Hazard Contribution": "Contribution par risque",
        "Alert Email": "E-mail d'alerte", "Open in new tab ↗": "Ouvrir dans un nouvel onglet ↗",
        "Total: {value}": "Total : {value}",
        "Illustrative split — a real implementation would compute this from actual per-hazard overlap.":
            "Répartition illustrative — une implémentation réelle calculerait ceci à partir du chevauchement réel par risque.",
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
        ("The boundaries and names shown and the designations used on this map "
         "do not imply official endorsement or acceptance by the United Nations."):
            "Les frontières et noms indiqués ainsi que les désignations utilisées sur cette carte "
            "n'impliquent pas de reconnaissance ou d'acceptation officielle par les Nations Unies.",
        "Language": "Langue",
    },
    "bn": {
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
        "Other": "অন্যান্য", "Deterministic": "নির্ধারক", "Compare Worst Case By": "সবচেয়ে খারাপ পরিস্থিতি তুলনা করুন",
        "Show": "দেখান", "Combined": "সম্মিলিত",
        "People": "মানুষ", "Children": "শিশু", "Schools": "স্কুল",
        "Health Centers": "স্বাস্থ্যকেন্দ্র", "Shelters": "আশ্রয়কেন্দ্র", "WASH Facilities": "WASH সুবিধা",
        "At Risk": "ঝুঁকিতে", "In Need": "প্রয়োজনে",
        "(shown as At Risk above In Need for People & Children)": "(মানুষ ও শিশুর জন্য ঝুঁকিতে-এর নিচে প্রয়োজনে হিসেবে দেখানো হয়েছে)",
        "(shown as Tropical Cyclone only / Both / Flood only share of each value)":
            "(প্রতিটি মানের শুধু গ্রীষ্মমন্ডলীয় ঘূর্ণিঝড় / উভয়ই / শুধু বন্যা অংশ হিসেবে দেখানো হয়েছে)",
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
        "Envelopes": "খাম", "Probability Raster": "সম্ভাব্যতা র‍্যাস্টার",
        "Mean": "গড়", "Probability": "সম্ভাব্যতা",
        "River Flooding": "নদীর বন্যা", "Rainfall": "বৃষ্টিপাত", "Proxies": "প্রক্সি",
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
        "Impact Summary": "প্রভাবের সারসংক্ষেপ",
        "Global — worldwide totals across visible hazards": "বৈশ্বিক — দৃশ্যমান ঝুঁকিসমূহের বিশ্বব্যাপী মোট",
        "Global — worldwide totals across all hazards": "বৈশ্বিক — সকল ঝুঁকির সম্মিলিত বিশ্বব্যাপী মোট",
        "Reflects only countries currently initialized in the database.":
            "শুধুমাত্র ডেটাবেসে বর্তমানে যুক্ত দেশগুলো প্রতিফলিত করে।",
        "None of the initialized countries are currently impacted. This does not mean there is no real impact — potentially affected countries may not yet be in the database.":
            "যুক্ত দেশগুলোর কোনোটিই বর্তমানে প্রভাবিত নয়। এর অর্থ এই নয় যে প্রকৃত কোনো প্রভাব নেই — সম্ভাব্য প্রভাবিত দেশগুলো এখনও ডেটাবেসে যুক্ত নাও হতে পারে।",
        "Full Impact Breakdown": "সম্পূর্ণ প্রভাব বিভাজন", "Hazard Contribution": "ঝুঁকির অবদান",
        "Alert Email": "সতর্কতা ইমেইল", "Open in new tab ↗": "নতুন ট্যাবে খুলুন ↗",
        "Total: {value}": "মোট: {value}",
        "Illustrative split — a real implementation would compute this from actual per-hazard overlap.":
            "দৃষ্টান্তমূলক বিভাজন — প্রকৃত বাস্তবায়নে এটি প্রতিটি ঝুঁকির প্রকৃত ওভারল্যাপ থেকে গণনা করা হবে।",
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
        ("The boundaries and names shown and the designations used on this map "
         "do not imply official endorsement or acceptance by the United Nations."):
            "এই মানচিত্রে দেখানো সীমানা ও নাম এবং ব্যবহৃত পদবি জাতিসংঘের সরকারি অনুমোদন বা গ্রহণযোগ্যতা বোঝায় না।",
        "Language": "ভাষা",
    },
}


def _t(text, **kwargs):
    """Look up `text` in the current language's dict (falls back to the
    English original if untranslated), then apply any {placeholder} kwargs.
    """
    translated = _TRANSLATIONS.get(_LANG, {}).get(text, text)
    return translated.format(**kwargs) if kwargs else translated


_PANEL_STYLE = {
    "position": "absolute", "background": "rgba(255,255,255,0.94)", "backdropFilter": "blur(10px)",
    "border": "1px solid #dde6ec", "borderRadius": "12px",
    "boxShadow": "0 8px 28px rgba(0,0,0,0.18), 0 1px 2px rgba(0,0,0,0.08)", "zIndex": 30,
}

# Unified spacing system for every floating panel/row in this shell — ONE
# margin applied consistently to every gap: topbar-to-panel-top, panel-to-
# viewport-edge, panel-bottom-to-bottom-row-top, bottom-row-to-footer,
# command-bar-to-topbar, command-bar-to-side-panels, disclaimer-to-footer,
# AND the topbar/footer's own horizontal content padding (so the "AHEAD OF
# THE STORM" logo/"Supported by" text and the language switcher/GitHub
# icon line up with the side panels' own left/right edges, not a
# different, wider 24px padding). So the whole shell reads as one
# consistent grid instead of several independently-tuned offsets.
#
# Real bug found+fixed here (twice already): earlier attempts either (a)
# changed ONE side of a gap without the other, or (b) shrunk the side
# panels' own maxHeight to make room for a bigger margin — panel height is
# a real content constraint (users need to see as many Infrastructure/
# stat rows as possible) and must NOT shrink just to make the margins
# prettier. The fix is a SMALLER margin instead: _PANEL_MAX_HEIGHT stays
# the ORIGINAL "100vh - 210" (unchanged), and 15px (not 16) is small
# enough that even at that original, taller cap, the panel's bottom edge
# still clears the basemap-row/legend beneath it with real margin to
# spare — see the derivation below.
#
# Measured live: topbar ~58.6px tall, footer ~67.3px tall, the bottom-left
# basemap+demo row ~40.6px tall, the collapsed map legend ~37.5px tall.
# Solving gap_top == gap_left == gap_bottom == gap_footer == M for the
# panel's ORIGINAL "100vh-210" cap gives M ≈ 14.5-15.5 (the two bottom
# rows differ slightly in height) — 15 satisfies both with a couple of
# spare pixels either way, confirmed live (no overlap at true max height).
_UI_MARGIN = 15
_PANEL_TOP = "74px"  # topbar height (~58.6) + _UI_MARGIN, rounded
_BOTTOM_ROW_OFFSET = "82px"  # footer height (~67.3) + _UI_MARGIN, rounded
# UNCHANGED from the original — see this block's own comment on why panel
# height must not shrink just to make the margins symmetric.
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
    # Fixed max height + internal scroll — without this, centered=True keeps
    # recentering/regrowing the WHOLE modal every time a collapsible section
    # (like Admin Level 1 Breakdown) expands or collapses, which reads as
    # the modal growing from the bottom rather than sitting still while its
    # content scrolls.
    # overflowX:auto here too (not just on the custom div wrapping the
    # breakdown table) — Mantine's own Modal.Content box clips overflow for
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

# Same 4 named options as the real app's dl.LayersControl (layouts/panels.py):
# CartoDB Light (default), CartoDB Dark, Satellite, OpenStreetMap/Mapbox
# Light. This pill is the only control the user interacts with — its
# clientside_callback below clicks the matching (visually hidden) native
# Leaflet LayersControl radio input, so Leaflet's own 'baselayerchange'
# event fires and swapMaplibreBasemap (maplibre_tiles.js) does the real
# tile swap, exactly as it already does for panels.py's own LayersControl.
def _basemap_options():
    return [
        {"value": "cartodb-light", "label": _t("Light")},
        {"value": "cartodb-dark", "label": _t("Dark")},
        {"value": "satellite", "label": _t("Satellite")},
        {"value": "osm", "label": _t("OSM")},
    ]

# Shared with _topbar's own DatePickerInput/SegmentedControl initial values
# and _breakdown_new_tab_href's default query params, so the print page's
# "Forecast issued" line always matches the live app's own default state
# instead of a second hardcoded literal that could drift out of sync.
# Real, verified test scenario (confirmed via a live Snowflake query) — BAVI/
# PHL 2026-07-02 is the only country+date combination with real data for
# Wind (all 4 runs), River (00Z), AND Rainfall (06Z) at once, making it the
# richest available real test point for exercising every wired hazard layer.
# The other real, verified test scenario worth knowing about for manual
# testing: MELISSA/Jamaica at 2025-10-28 00Z (single-country, wind-only,
# 17,920 real tiles) — a genuine single-storm/single-country case, not set
# as the default here since only one date/run pair can be.
# Real, not a hardcoded literal — always the most recent forecast cycle
# Snowflake actually has, computed once here and shared with _build_real_storms()
# below (same query, not run twice). Falls back to "now" only when there is
# genuinely no data anywhere (a fresh/empty environment), rather than ever
# silently drifting behind a stale hardcoded date.
try:
    _LATEST_FORECAST_TIME = get_latest_forecast_time_overall()
except Exception as e:
    logger.warning("Could not load latest forecast time: %s", e)
    _LATEST_FORECAST_TIME = None

if _LATEST_FORECAST_TIME is not None:
    _latest_ts = pd.Timestamp(_LATEST_FORECAST_TIME)
    _DEFAULT_FORECAST_DATE = _latest_ts.strftime("%Y-%m-%d")
    # Snap to the nearest synoptic run (00/06/12/18Z, the only values
    # ms-topbar-time's SegmentedControl offers) — real forecast times are
    # always exactly on one of these already, this is just a defensive floor.
    _DEFAULT_FORECAST_RUN = f"{(_latest_ts.hour // 6) * 6:02d}"
else:
    _DEFAULT_FORECAST_DATE = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    _DEFAULT_FORECAST_RUN = "00"

# "Future" here means later than the latest REAL forecast_time in the
# database (_LATEST_FORECAST_TIME/_DEFAULT_FORECAST_DATE+RUN above), not
# later than wall-clock "now" — real forecast data can genuinely lag behind
# today's actual date, so wall-clock time would be the wrong reference and
# would incorrectly greyed out/allow the wrong runs.
_RUN_VALUES = ["00", "06", "12", "18"]


def _max_allowed_run_for_date(date_str):
    """Highest run ('00'/'06'/'12'/'18') selectable for date_str without
    going past the latest real forecast_time in the database. Returns None
    when date_str is entirely past the latest available date (nothing on
    it is selectable) or when there's no known latest date at all (fresh/
    empty environment — nothing to restrict against)."""
    if _LATEST_FORECAST_TIME is None or not date_str:
        return _RUN_VALUES[-1]
    if date_str < _DEFAULT_FORECAST_DATE:
        return _RUN_VALUES[-1]
    if date_str == _DEFAULT_FORECAST_DATE:
        return _DEFAULT_FORECAST_RUN
    return None


def _time_options_for_date(date_str):
    """topbar-time's SegmentedControl `data` — runs later than
    _max_allowed_run_for_date(date_str) get a dimmed, non-interactive-
    looking label (Mantine's SegmentedControl has no native per-item
    disabled state; _guard_future_forecast_run below is what actually
    blocks selecting one, this just makes that same boundary visible)."""
    max_run = _max_allowed_run_for_date(date_str)
    data = []
    for r in _RUN_VALUES:
        if max_run is not None and int(r) <= int(max_run):
            data.append({"value": r, "label": f"{r}Z"})
        else:
            data.append({"value": r, "label": html.Span(
                f"{r}Z", style={"opacity": 0.35, "cursor": "not-allowed"})})
    return data


# NOTE: the two GLOBAL, country/storm-INDEPENDENT raw hazard layers (raw
# precip-rate raster + raw river flood-extent raster — see
# services/tile_server.py's own "Global raw precipitation-rate endpoints"/
# "Global raw river endpoints" sections) used to resolve their own "latest"
# forecast_time once here at import time. That's gone now (2026-07-31): both
# layers' forecast_time must follow the topbar's date+time selection instead
# of always "latest" (see get_precip_forecast_time_near/
# get_river_extent_forecast_time_for_date in snowflake_utils.py), and
# _build_global_raw_config below already fires on page load
# (prevent_initial_call=False) with the topbar's own default date/run
# (_DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN, set just above), so a
# separate import-time resolution here would just be redundant work whose
# result nothing else read.

# Real data (was a hardcoded 4-entry mock) — see _build_real_storms() near
# the top of the file for exactly how this is derived from Snowflake.
# "countries" is a LIST — a real storm's track can genuinely affect more than
# one nation, and impact MAT tables are computed per-country, so a single
# TRACK_ID having real data for several countries at once isn't hypothetical.
_STORMS = _build_real_storms(_LATEST_FORECAST_TIME)
# Keyed by full category label (get_available_wind_thresholds-derived, via
# _category_label_from_kt) rather than the old bare "Category 1/2/3" —
# _cat_badge below still extracts just the digit for its "Cat N" text, so
# visually this is unchanged for hurricane-strength storms; sub-hurricane
# ones (Tropical Storm/Strong Trop. Storm/Severe Trop. Storm) and "Unknown"
# simply fall back to the neutral default color.
_CAT_COLORS = {
    "Category 1 Hurricane": "#f0ac52", "Category 2 Hurricane": "#e8793f",
    "Category 3 Hurricane": "#d94f3c", "Category 4 Hurricane": "#c0392b",
    "Category 5 Hurricane": "#8e1a0f",
}

# Quick-jump presets for testing — sets date, time, country selection, and
# mode all at once. Both real, verified via a live Snowflake query (not
# illustrative/mock): MELISSA/Jamaica is a genuine single-storm, wind-only
# case (17,920 real tiles); BAVI/PHL 2026-07-02 is the one date with real
# Wind, River, AND Rainfall data all at once, for exercising the flood
# hazard layers specifically. Kept to just these two rather than the old
# 5-entry mock list, since every one of those referenced dates/storms that
# don't actually correspond to real data anymore.
_DEMO_SCENARIOS = [
    {"label": "MELISSA — Jamaica (28 Oct 2025, 00Z)", "date": "2025-10-28", "time": "00",
     "countries": ["Jamaica"], "mode": "zoom"},
    # Also the one real date with Gust data (MERCATOR_TILE_GUST_MAT has
    # real, non-empty rows ONLY for BAVI/PHL, both 00Z and 06Z on this exact
    # date — confirmed live) — Country-Analysis-only, no Global-mode
    # rendering path exists for Gust at all (unlike Wind, which now has real
    # Global-mode envelope polygons; Gust has no equivalent envelope data
    # source, see ms-gust-on's own Global-mode-disabled comment).
    {"label": "BAVI — Philippines, flood+gust test (2 Jul 2026, 00Z)", "date": "2026-07-02", "time": "00",
     "countries": ["Philippines"], "mode": "zoom"},
]


def _demo_scenarios_menu():
    # Icon-only now, living inside _bottom_left_controls() right next to the
    # basemap switcher — used to be its own separate bottom-right floating
    # button (own dmc.Button with a text label), but that meant an entire
    # empty-looking gap opened up at bottom-right in Country Analysis mode
    # (where this is hidden) and it competed with the map legend for the
    # same corner in Global mode. No id/absolute style of its own anymore;
    # _toggle_demo_scenarios_menu below just shows/hides this small wrapper
    # inline within that shared row.
    return html.Div(
        # position="top-start" — real bug found+fixed here: this row sits
        # only _UI_MARGIN (15px) above the footer, so Mantine's own default
        # Menu position (opens downward) had nowhere near enough room and
        # the dropdown's lower items rendered hidden behind/under the
        # footer. Opening upward instead gives it the whole panel column
        # above to work with. zIndex above the footer's own 1000 so it's
        # never visually clipped underneath it either.
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
    # Basemap switcher + Demo Scenarios icon share one floating row — real
    # bug found+fixed here: giving Demo Scenarios its own separate
    # bottom-right panel left an empty-looking gap there whenever it was
    # hidden (Country Analysis mode) and directly collided with the map
    # legend's own natural bottom-right spot in Global mode. There's
    # genuinely enough room in this row (basemap switcher's own segments
    # don't span the full panel width) for both.
    # bottom offset from the shared _BOTTOM_ROW_OFFSET spacing system (see
    # _PANEL_MAX_HEIGHT's own comment) — controls-panel's maxHeight is
    # derived FROM this same row's real height, so the two can never
    # overlap regardless of which side changes.
    return html.Div([
        dmc.SegmentedControl(id="basemap-select", value="cartodb-light", data=_basemap_options(), size="xs"),
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
    # Each option is a plain <a href>, not a Dash callback — switching
    # language re-invokes layout(lang=...) fresh on full page load, so every
    # static string on the page (built once, at layout-build time) picks up
    # the new language too, not just whatever a reactive callback touches.
    # Lives inline as the last item in the top bar (top-right corner), not a
    # separate floating panel — same tier as the other top-bar controls.
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
    # same reason — the top bar itself sits at zIndex 1000, which was
    # cutting into this dropdown's own (lower, Mantine-default) z-index.
    ], zIndex=2000)

# Real REGION_MEMBERS bundle (snowflake/mat_tables/02b_add_regional_group.sql:34-48,
# a "registered 2026-04-17, example for reference" template) — East
# Caribbean Area, member ISO codes AIA/ATG/BRB/VGB/DMA/GRD/MSR/KNA/LCA/VCT/
# TTO/TCA (12 countries). Illustrated here with 3 of the real 12 (the ones
# ELARA — see _STORMS — already affects), not the full list, to keep the
# mock data manageable; the pattern (aggregate region + drillable members)
# is the same either way.
_ECA_MEMBERS = ["Antigua and Barbuda", "Saint Lucia", "Saint Vincent and the Grenadines"]

# Multi-select: the country picker holds a LIST of selected countries.
# Selecting a multi-country storm (BAVI) selects all its countries at once;
# the Impact Summary panel then shows one stat block per selected country,
# side by side, rather than forcing a single arbitrary pick.
#
# Real REGION_MEMBERS pattern (Pacific Islands, ECA — see get_countries in
# components/data/snowflake_utils.py + pages/dashboard.py's COUNTRY_OPTIONS):
# a fixed bundle queried/selected as ONE unit, grouped separately from plain
# countries in the dropdown ("Regions" vs "Countries"). Kept in the SAME
# multi-select as individual countries (not the real app's separate
# individual-country-select drill-down) so the region and the flexible
# ad-hoc multi-select both stay available from one control — pick "ECA" for
# the aggregate, or pick "Saint Lucia" on its own, or mix both.
def _country_options():
    """Real country/region dropdown — built from get_active_countries()
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

# Fallback baseline only — used when there's no real active storm for a
# country yet (_fetch_real_tile_totals returns None in that case; see
# below), or for the "Global" scope, which has no single country to query.
# Not a live number: honestly illustrative, same as the rest of this page's
# "no real data available" fallbacks (e.g. metrics.py's own _NA_VALUE).
_DEFAULT_STATS = {"Children at Risk": "218K", "People at Risk": "640K", "Schools at Risk": "185", "Health Centers at Risk": "46", "Shelters at Risk": "62", "WASH Facilities at Risk": "134"}

# Icon per stat — Material Design Icons (mdi:*) via Iconify, since carbon's
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
    own FORECAST_DATE column actually uses (confirmed live, e.g.
    '20251028000000' for MELISSA/Jamaica 28 Oct 2025 00Z)."""
    return f"{date.replace('-', '')}{run}0000"


def _resolve_storm_for_country(country, date=None, run=None):
    """REACTIVE replacement for `next((s for s in _STORMS if country in
    s["countries"]), None)` — answers "what storm has real impact data for
    THIS country at THIS selected topbar date/run", not "what's currently
    active" (that question is still correctly answered by _STORMS/
    _build_real_storms, used only by the Global-mode Active Storms list).

    Falls back to _DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN (the latest
    real forecast cycle, same default the topbar itself opens on) when date/
    run aren't supplied — e.g. for callers outside a callback that has the
    live topbar-date/topbar-time values.

    Returns a dict shaped like a _STORMS entry ({"name", "cat"}) plus the two
    date-string forms downstream callers need — "forecast_time"
    ("YYYY-MM-DD HH:MM:SS", matching TC_TRACKS/TC_ENVELOPES_COMBINED's own
    FORECAST_TIME timestamp column and get_available_wind_thresholds' own
    expected format) and "mat_forecast_date" ("YYYYMMDDHH24MISS", matching
    the *_MAT tables' own FORECAST_DATE string column) — or None when there's
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


def _resolve_storms_for_date(date=None, run=None):
    """Every REAL storm with impact data at the given topbar date/run,
    across ALL countries — the date-reactive replacement for filtering the
    frozen `_STORMS`/`_build_real_storms` "active right now" snapshot in
    `_active_storms_section`. This is what lets a historical Demo Scenario
    date (e.g. MELISSA on 28 Oct 2025) show the exact same bordered
    storm-row box (name, category badge, alert-email icon) the Global-mode
    Active Storms list already shows for genuinely-live storms — same UI,
    just sourced from "what's real on this date" instead of "what's
    happening in the last 12h".

    Falls back to _DEFAULT_FORECAST_DATE/_DEFAULT_FORECAST_RUN when date/run
    aren't supplied, same convention as _resolve_storm_for_country.

    Returns a list shaped exactly like `_STORMS` ({"name", "countries",
    "date", "cat"}), one entry per distinct storm, countries as real
    display names (via _CODE_TO_NAME), PLUS two extra keys every entry
    already carries internally — "forecast_time" ("YYYY-MM-DD HH:MM:SS",
    matching TC_TRACKS' own FORECAST_TIME column) and "mat_forecast_date"
    ("YYYYMMDDHH24MISS", matching the *_MAT tables' own FORECAST_DATE
    column) — same two extra keys _resolve_storm_for_country already
    returns, so callers that need to fetch this storm's own real track/
    envelope data (e.g. the Global-mode multi-storm track query) don't have
    to re-derive them. All storms from one call currently share the same
    forecast_time (this function resolves ONE mat_date from the given
    date/run and queries every storm real at that exact date/run), but
    each entry still carries its own copy rather than callers assuming a
    single shared value, in case that ever changes. Empty list when
    there's genuinely no real data for this date.
    """
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    if not date or run is None:
        return []
    mat_date = _mat_forecast_date(date, run)
    forecast_time_str = f"{date} {run}:00:00"
    try:
        # 34kt (not the 50kt used elsewhere for precise impact numbers) —
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
    # resolver, zero impact requirement — see get_track_ids_for_date's own
    # docstring). Add them with an empty countries list rather than dropping
    # them, so this list never disagrees with the header dot above it. Real
    # bug found+fixed here: previously this whole function required nonzero
    # impact, so DOLPHIN/GENEVIEVE (2 genuinely real, currently-active storms
    # on 2026-07-31, confirmed live via TC_TRACKS) never appeared in Active
    # Storms at all.
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
    storms = []
    for storm_name, codes in by_storm.items():
        cat = _category_label_ensemble_max(storm_name, forecast_time_str)
        country_names = [_CODE_TO_NAME.get(c, c) for c in codes]
        storms.append({
            "name": storm_name, "countries": country_names, "date": date_str, "cat": cat,
            "forecast_time": forecast_time_str, "mat_forecast_date": mat_date,
        })
    return storms


def _resolve_wind_kt(wind_idx):
    """Real currently-selected wind-severity threshold (kt) — same
    resolution _build_hazard_tile_config already uses to derive the map's
    own wind tile layer (ms-wind-slider's index, default 2 == 'Severe Trop.
    Storm', into _WIND_CATS). Threaded into _fetch_real_tile_totals (and
    everything that calls it) so dragging the slider actually changes the
    real numbers shown on the page — previously hardcoded to 50kt
    regardless of the slider's value."""
    wind_idx = wind_idx if wind_idx is not None else 2
    return _WIND_CATS[wind_idx][2]


def _resolve_gust_kt(gust_idx):
    """Real currently-selected gust-severity threshold (kt) — _WIND_CATS rows
    are (label, name, wind_kt, gust_kt), so this is index [3], NOT [2] (that's
    wind's own kt). Real bug found and fixed here: _build_hazard_tile_config's
    existing `gust_kt = _WIND_CATS[gust_idx][2]` used wind's kt value for the
    GUST_THRESHOLD column filter — since wind kt values (34/40/50/64/83/96/
    113/137) and real gust kt values (17/21/26/33/43/49/58/70) never overlap,
    that join has never matched a single real row (confirmed live: BAVI/PHL
    2026-07-02 returns 0 gust rows at kt=50, but real nonzero rows at kt=26,
    the correct index-[3] value for the same slider position)."""
    gust_idx = gust_idx if gust_idx is not None else 2
    return _WIND_CATS[gust_idx][3]


def _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx=None, gust_idx=None,
               river_idx=None, rain_idx=None, rain_window=None):
    """Builds the real multi-hazard `hz` dict _fetch_real_combined_tile_totals
    (via _get_country_stats/_get_country_pin_pct/_combined_stats/
    _combined_in_need_total) expects — resolving each hazard's own slider
    index into its real threshold value, exactly the same resolution
    _build_hazard_tile_config already uses for the map's own tile layers, so
    the Impact Summary/Breakdown numbers always match what the map itself is
    currently showing for wind_kt/gust_kt/rp_tier/threshold_mm."""
    # Default index 2 == "rp10" — the real, non-placeholder river return-
    # period tier (rp2/rp5 are IS_STANDIN=True placeholders, see
    # ms-river-slider's own default and _RIVER_RP_TIERS).
    river_idx = river_idx if river_idx is not None else 2
    rain_window = rain_window or "6"
    rain_idx = rain_idx if rain_idx is not None else 1
    return {
        "wind_on": bool(wind_on), "gust_on": bool(gust_on),
        "river_on": bool(river_on), "rain_on": bool(rain_on),
        "wind_kt": _resolve_wind_kt(wind_idx),
        "gust_kt": _resolve_gust_kt(gust_idx),
        "rp_tier": _RIVER_RP_TIERS[river_idx],
        "rain_mm": _RAIN_MM_BY_WINDOW[rain_window][rain_idx],
        "rain_window": rain_window,
    }


def _wind_only_hz(wind_kt=None):
    """Default hazard-state bundle for every call site that hasn't been
    updated to pass a real multi-hazard `hz` — preserves the exact old
    wind-only behavior (see _get_country_stats/_get_country_pin_pct below)."""
    return {"wind_on": True, "gust_on": False, "river_on": False, "rain_on": False,
            "wind_kt": wind_kt, "gust_kt": None, "rp_tier": None, "rain_mm": None, "rain_window": None}


# Common exposure-column name set every hazard's own tile dataframe gets
# normalized/renamed into (see _norm_hazard_tile_df) before being merged in
# _fetch_real_combined_tile_totals — lets wind/gust/river/rain dataframes
# (different underlying MAT tables, different join keys, different real
# column coverage) be outer-merged on zone_id and combined without one
# hazard's columns colliding with another's.
_HAZARD_TILE_EXPOSURE_COLS = [
    'E_population', 'E_infant_population', 'E_school_age_population', 'E_adolescent_population',
    'E_num_schools', 'E_num_hcs', 'E_num_shelters', 'E_num_wash',
]


def _norm_hazard_tile_df(df):
    """Uppercase-E_-prefix + lowercase-rest column normalization — same
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
    """Real per-country 'at risk' aggregates COMBINED across every ACTIVE
    hazard (Wind/Gust/River/Rain — Storm Surge has no real backend anywhere,
    see ms-surge-on's own comment, and never contributes here) — the real
    replacement for the old illustrative _scale_stats_by_hazard/
    _hazard_breakdown percentage-scaled wind-only total.

    Combination method: each active hazard's own real per-tile exposure is
    queried independently (wind/gust share one storm+forecast_date; river/
    rain each have their own independent forecast_time — see
    _build_hazard_tile_config's own comment on why they're not storm-scoped),
    the resulting per-zone_id dataframes are OUTER-merged together, and for
    every exposure column the row-wise MAX across whichever hazards have data
    for that tile is taken before summing across tiles. MAX (not SUM) is what
    avoids double-counting the same population cell just because two active
    hazards both threaten it — the same "at least one hazard reaches this
    cell" semantics river's own MAX-across-STEP_H aggregation already uses
    server-side (services/tile_server.py's _MERCATOR_RIVER_SQL).

    People/Children In Need (PIN/CHIN) stays wind-only regardless of which
    other hazards are active — no vulnerability/CCI pipeline exists for gust/
    river/rain (services/tile_server.py's MERCATOR_TILE_GUST_MAT comment), so
    there's no real per-hazard in-need number to combine; if wind itself
    isn't active, PIN/CHIN is 0 (no other real source for it exists).

    Rain never contributes a real schools/health-centers/shelters/WASH count
    — MERCATOR_TILE_PRECIP_MAT only has a real hazard-conditional E_population
    column (see get_rain_tile_impacts's own docstring); including its facility
    columns here would just add the country's plain unconditional facility
    counts, not anything rain-specific, so they're excluded from rain's
    contribution rather than silently inflating the combined total.

    `hz` is a dict from _wind_only_hz() (or an equivalent multi-hazard dict
    built by a real hazard-aware caller) — {"wind_on", "gust_on", "river_on",
    "rain_on", "wind_kt", "gust_kt", "rp_tier", "rain_mm", "rain_window"}.

    Returns None (callers fall back to _DEFAULT_STATS/_DEFAULT_PIN_PCT, same
    convention as before) when no hazard is active, or none of the active
    hazards resolve to any real data for this country/date.
    """
    hz = hz or _wind_only_hz()
    code = _NAME_TO_CODE.get(country)
    if not code:
        return None

    frames = []
    people_in_need = 0.0
    children_in_need = 0.0

    storm_info = _resolve_storm_for_country(country, date, run) if (hz["wind_on"] or hz["gust_on"]) else None

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
        return target_kt if target_kt in numeric else numeric[0]

    if hz["wind_on"] and storm_info:
        wt = _resolve_threshold("wind", hz["wind_kt"])
        if wt is not None:
            df = _norm_hazard_tile_df(get_tile_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], wt, 14))
            if df is not None and not df.empty:
                frames.append(df[['zone_id'] + [c for c in _HAZARD_TILE_EXPOSURE_COLS if c in df.columns]])
                if 'E_people_in_need' in df.columns:
                    people_in_need = float(df['E_people_in_need'].sum())
                if 'E_children_in_need' in df.columns:
                    children_in_need = float(df['E_children_in_need'].sum())

    if hz["gust_on"] and storm_info:
        gt = _resolve_threshold("gust", hz["gust_kt"])
        if gt is not None:
            df = _norm_hazard_tile_df(get_gust_tile_impacts(code, storm_info["name"], storm_info["mat_forecast_date"], gt, 14))
            if df is not None and not df.empty:
                frames.append(df[['zone_id'] + [c for c in _HAZARD_TILE_EXPOSURE_COLS if c in df.columns]])

    if hz["river_on"] and hz["rp_tier"]:
        forecast_time = get_latest_river_forecast_time(code)
        if forecast_time:
            df = _norm_hazard_tile_df(get_river_tile_impacts(code, forecast_time, hz["rp_tier"]))
            if df is not None and not df.empty:
                frames.append(df[['zone_id'] + [c for c in _HAZARD_TILE_EXPOSURE_COLS if c in df.columns]])

    if hz["rain_on"] and hz["rain_mm"] is not None and hz["rain_window"] is not None:
        forecast_time = get_latest_rain_forecast_time(code)
        if forecast_time:
            df = _norm_hazard_tile_df(get_rain_tile_impacts(code, forecast_time, hz["rain_mm"], hz["rain_window"]))
            if df is not None and not df.empty and 'E_population' in df.columns:
                frames.append(df[['zone_id', 'E_population']])

    if not frames:
        return None

    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on='zone_id', how='outer', suffixes=('', '_dup'))
        # An outer merge gives a shared column a "_dup" sibling instead of
        # overwriting it — collapse each pair to its row-wise MAX (skipping
        # NaN) right away so the next merge iteration never accumulates
        # stale duplicate columns.
        for col in _HAZARD_TILE_EXPOSURE_COLS:
            dup = col + '_dup'
            if dup in merged.columns:
                merged[col] = merged[[col, dup]].max(axis=1, skipna=True)
                merged = merged.drop(columns=[dup])

    def _sum(col):
        return float(merged[col].sum()) if col in merged.columns and not merged[col].isna().all() else 0.0

    population = _sum('E_population')
    children = _sum('E_infant_population') + _sum('E_school_age_population') + _sum('E_adolescent_population')
    stats = {
        "Children at Risk": _format_stat_number(round(children)),
        "People at Risk": _format_stat_number(round(population)),
        "Schools at Risk": _format_stat_number(round(_sum('E_num_schools'))),
        "Health Centers at Risk": _format_stat_number(round(_sum('E_num_hcs'))),
        "Shelters at Risk": _format_stat_number(round(_sum('E_num_shelters'))),
        "WASH Facilities at Risk": _format_stat_number(round(_sum('E_num_wash'))),
    }
    pin_pct = {
        "people": max(0, min(100, round(people_in_need / population * 100))) if population > 0 else 0,
        "children": max(0, min(100, round(children_in_need / children * 100))) if children > 0 else 0,
    }
    return {"stats": stats, "pin_pct": pin_pct}


def _get_country_stats(country, date=None, run=None, wind_kt=None, hz=None):
    """Real replacement for the old `_COUNTRY_STATS.get(country, _DEFAULT_STATS)`.
    `date`/`run` (topbar values) thread through to _fetch_real_combined_tile_totals
    for reactive resolution; omit them to fall back to the default forecast cycle.
    `wind_kt` (see _resolve_wind_kt) threads the real wind-severity slider value
    through when `hz` isn't given (wind-only callers). Pass a real multi-hazard
    `hz` dict (see _fetch_real_combined_tile_totals's own docstring) to get the
    real combined-across-active-hazards total instead of wind-only."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz or _wind_only_hz(wind_kt))
    return real["stats"] if real else _DEFAULT_STATS


def _get_country_pin_pct(country, date=None, run=None, wind_kt=None, hz=None):
    """Real replacement for the old `_PIN_PCT.get(country, _DEFAULT_PIN_PCT)`.
    See _get_country_stats above re: date/run/wind_kt/hz."""
    real = _fetch_real_combined_tile_totals(country, date, run, hz or _wind_only_hz(wind_kt))
    return real["pin_pct"] if real else _DEFAULT_PIN_PCT


def _get_data_availability_real(country):
    """Real per-country data-availability snapshot — the same facility-count
    + dataset-boolean check Ahead-of-the-Storm-ORCHESTRATION's own
    08_utilities/check_baseline_data.py already runs against Snowflake,
    sourced here from BASE_MERCATOR_TILE_MAT (get_base_tiles in
    snowflake_utils.py) instead of the old hardcoded _DATA_AVAILABILITY mock.
    Returns None (same as the old dict's `.get()` miss) when the country has
    no base-layer data in Snowflake at all yet.
    """
    code = _NAME_TO_CODE.get(country)
    if not code:
        return None
    try:
        df = get_base_tiles(code)
    except Exception as e:
        logger.warning("Could not load base tiles for %s: %s", country, e)
        return None
    if df is None or df.empty:
        return None

    def _any_present(col):
        return col in df.columns and df[col].notna().any()

    def _total(col):
        return int(df[col].sum()) if col in df.columns and not df[col].isna().all() else 0

    return {
        "schools": _total('num_schools'), "health_centers": _total('num_hcs'),
        "shelters": _total('num_shelters'), "wash": _total('num_wash'),
        "population": _any_present('population'), "age_0_4": _any_present('infant_population'),
        "age_5_14": _any_present('school_age_population'), "age_15_19": _any_present('adolescent_population'),
        "rwi": _any_present('rwi'), "settlement": _any_present('smod_class'),
        "moderate_poverty": _any_present('moderate_poverty_prob'), "severe_poverty": _any_present('severe_poverty_prob'),
    }


# Real per-member track granularity (TC_TRACKS has member_type/member id,
# confirmed via components/map/javascript.py's style_tracks control/ensemble
# distinction) — "Probabilistic" mirrors today's real default (probability-
# weighted across the whole ensemble); picking a specific member is the new
# behavior this control illustrates: that member's own track, wind envelope,
# and precip layer, not the aggregate. Only 10 illustrative members shown
# here, not the real 51 — a real implementation would list them all.
# Grouped so "Probabilistic" reads as a distinct choice, not just one more
# item in the same list as the individual members.
def _ensemble_members():
    return [
        {"value": "combined", "label": _t("Probabilistic")},
        {"group": _t("Ensemble Members"), "items":
            [{"value": "control", "label": _t("Control (deterministic)")}]
            + [{"value": f"member-{i}", "label": _t("Member {n}", n=i)} for i in range(1, 11)]},
    ]

# Fallback only (see _get_country_pin_pct above) — % of "at risk" further
# flagged "in need" when there's no real per-country tile data available yet.
_DEFAULT_PIN_PCT = {"people": 34, "children": 41}

# Fallback only (see _get_country_totals below) — population/children
# denominator for the arc charts' outer "Population" ring, used only when
# get_country_totals() has no real BASE_MERCATOR_TILE_MAT data for a country.
_DEFAULT_TOTALS = {"population": 2_500_000, "children": 850_000}


def _get_country_totals(country):
    """Real replacement for the old hardcoded `_COUNTRY_TOTALS.get(country,
    _DEFAULT_TOTALS)` — total population/children for `country`, sourced
    from BASE_MERCATOR_TILE_MAT via get_country_totals() in
    snowflake_utils.py (the same function callbacks/metrics.py's own In Need
    arc charts use for this exact outer-ring denominator). Falls back to
    _DEFAULT_TOTALS only when Snowflake has no base-layer data at all for
    this country yet.
    """
    code = _NAME_TO_CODE.get(country)
    if not code:
        return _DEFAULT_TOTALS
    totals = get_country_totals(code)
    if totals["total_population"] is None and totals["total_children"] is None:
        return _DEFAULT_TOTALS
    return {
        "population": totals["total_population"] if totals["total_population"] is not None else _DEFAULT_TOTALS["population"],
        "children": totals["total_children"] if totals["total_children"] is not None else _DEFAULT_TOTALS["children"],
    }

# Illustrative — how much each active hazard family contributes to a given
# at-risk/in-need number. One shared split reused across metrics (mock only);
# a real implementation would compute this per metric from actual overlap.
_HAZARD_CONTRIBUTION = [
    ("Sustained Wind", WIND, 45, "mdi:weather-windy"),
    ("Gust", GUST, 15, "mdi:weather-windy-variant"),
    # River Flooding/Rainfall trimmed from 25/15 to 20/10 to make room for
    # Storm Surge (10) — Flood's own overall share stays 40, same as
    # before, just decomposed into three sub-hazards instead of two. Storm
    # Surge itself is a placeholder (no real pipeline/data behind it yet,
    # see tc_ecmwf_additional_layers work elsewhere) but contributes to the
    # Flood total here the same as any other member.
    ("River Flooding", RIVER, 20, "mdi:waves"),
    ("Rainfall", RAIN, 10, "mdi:weather-pouring"),
    ("Storm Surge", SURGE, 10, "mdi:tsunami"),
]

# The two overarching families used both in the tile-click Hazard
# Contribution popup and the Full Impact Breakdown table's own inline
# hazard-split line — same Tropical Cyclone (orange)/Flood (blue) grouping
# already used everywhere else on this page (_hurricane_family/
# _flood_hazards_family), just applied here to the illustrative
# contribution split too instead of only listing the hazards flat.
# Tropical Cyclone deliberately only includes Sustained Wind for now, not
# Gust — excluded per explicit request, in BOTH this popup and the table
# split-line below (both read from the same _HAZARD_TC_MEMBERS). Gust stays
# in _HAZARD_CONTRIBUTION's data (its real gust-envelope layer elsewhere on
# this page is unaffected) but isn't grouped under any hazard family here,
# so its 15% share isn't shown or counted in either breakdown.
_HAZARD_GROUPS = [
    ("Tropical Cyclone", WIND, "mdi:hurricane", ["Sustained Wind"]),
    ("Flood", RIVER, "mdi:home-flood", ["River Flooding", "Rainfall", "Storm Surge"]),
]

_HAZARD_TC_NAME, _HAZARD_TC_COLOR, _HAZARD_TC_ICON, _HAZARD_TC_MEMBERS = _HAZARD_GROUPS[0]
_HAZARD_FLOOD_NAME, _HAZARD_FLOOD_COLOR, _HAZARD_FLOOD_ICON, _HAZARD_FLOOD_MEMBERS = _HAZARD_GROUPS[1]
_HAZARD_BY_NAME = {name: (color, pct, icon) for name, color, pct, icon in _HAZARD_CONTRIBUTION}

# Illustrative — what fraction of the SMALLER hazard family's footprint also
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
    are toggled on — not just "is Tropical Cyclone or Flood active at all".
    An earlier version of this only checked family-level activity (e.g.
    "some flood hazard is on" counted the WHOLE Flood family, all three
    members, at their full combined share, even if only one of the three
    was actually checked) — that's fixed here: only the checked members
    contribute, at their own raw share.

    Gust is permanently excluded (not in _HAZARD_TC_MEMBERS at all — see
    _HAZARD_GROUPS's own note), so the maximum achievable total — every one
    of the 4 tracked checkboxes on at once — is less than the original mock
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
    both_pct = round(_HAZARD_OVERLAP_FRAC * min(tc_pct, flood_pct)) if (tc_active and flood_active) else 0
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
    return {k: _format_stat_number(round(_parse_stat_number(v) * scale)) for k, v in base_stats.items()}


def _active_hazards_indicator(breakdown):
    # Shown at the top of the Full Impact Breakdown (modal + print page) so
    # it's clear the table below isn't always the full picture — it's
    # scoped to whichever hazards are currently toggled on. Grouped under
    # Tropical Cyclone/Flood (own icon + label), not a flat list of hazard
    # chips — a flat list didn't make clear which hazards belong to which
    # family, same reasoning as the tile-click popup's own grouped rows.
    by_name = {name: (color, icon) for name, color, _, icon in _HAZARD_CONTRIBUTION}

    def _hazard_chip(name):
        return dmc.Group([DashIconify(icon=by_name[name][1], width=13, color=by_name[name][0]),
                            dmc.Text(_t(name), size="11px", c="dimmed")], gap=4)

    def _family_group(family_name, family_color, family_icon, member_names):
        if not member_names:
            return None
        # Pill (tinted background + border in the family's own color) — same
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

    # mt (was missing entirely) — this row otherwise sat flush against the
    # divider/title right above it with no breathing room at all.
    return html.Div([
        dmc.Text(_t("Hazards included:"), size="11px", fw=700, c="dimmed", mb=6),
        dmc.Group(groups, gap=20, wrap="wrap"),
    ], style={"marginTop": "10px", "marginBottom": "18px"})


def _hazard_split_line(n, breakdown, font_size="9.5px"):
    # Shared by _simple_breakdown_table and _admin1_table so both value
    # tables show the same Tropical Cyclone-only/Both/Flood-only illustrative
    # split under every number, using the same percentages (and the same
    # overlap fraction) as the tile-click Hazard Contribution popup's own
    # overlap bar — three numbers, not the earlier two-number TC-share/
    # Flood-share split, which read as if they were separate people instead
    # of overlapping risk.
    #
    # `breakdown` (from _hazard_breakdown) reflects EXACTLY which individual
    # hazard checkboxes are on — if Flood has nothing toggled on, there's no
    # Flood-only or Both share to show (there's nothing for Tropical Cyclone
    # to overlap WITH), so this drops to a single plain number instead of a
    # 3-way split; same the other way around if Tropical Cyclone is off.
    # Neither active means n is already 0, nothing meaningful to split.
    tc_active, flood_active = breakdown["tc_active"], breakdown["flood_active"]
    if tc_active and not flood_active:
        return html.Div(_format_stat_number(n), style={"fontSize": font_size, "marginTop": "1px",
                                                          "color": _HAZARD_TC_COLOR, "fontWeight": 600})
    if flood_active and not tc_active:
        return html.Div(_format_stat_number(n), style={"fontSize": font_size, "marginTop": "1px",
                                                          "color": _HAZARD_FLOOD_COLOR, "fontWeight": 600})
    if not tc_active and not flood_active:
        return html.Div()
    return html.Div([
        html.Span(_format_stat_number(round(n * breakdown["tc_only_pct"] / 100)), style={"color": _HAZARD_TC_COLOR, "fontWeight": 600}),
        html.Span("/", style={"color": "#c3ccd2", "margin": "0 1px"}),
        html.Span(_format_stat_number(round(n * breakdown["both_pct"] / 100)), style={"color": HAZARD_BOTH_COLOR, "fontWeight": 600}),
        html.Span("/", style={"color": "#c3ccd2", "margin": "0 1px"}),
        html.Span(_format_stat_number(round(n * breakdown["flood_only_pct"] / 100)), style={"color": _HAZARD_FLOOD_COLOR, "fontWeight": 600}),
    ], style={"fontSize": font_size, "marginTop": "1px"})


def _hazard_split_legend(breakdown):
    # Shared legend, same markup used under both tables. Only meaningful
    # when BOTH families are active — a single-family view already shows a
    # single plain-colored number (see _hazard_split_line), nothing to
    # explain via a 3-dot legend.
    if not (breakdown["tc_active"] and breakdown["flood_active"]):
        return html.Div()
    return dmc.Group([
        dmc.Group([html.Span(style={"width": "7px", "height": "7px", "borderRadius": "50%", "background": _HAZARD_TC_COLOR}),
                    dmc.Text(_t("Tropical Cyclone only"), size="10px", c="dimmed")], gap=5),
        dmc.Group([html.Span(style={"width": "7px", "height": "7px", "borderRadius": "50%", "background": HAZARD_BOTH_COLOR}),
                    dmc.Text(_t("Both"), size="10px", c="dimmed")], gap=5),
        dmc.Group([html.Span(style={"width": "7px", "height": "7px", "borderRadius": "50%", "background": _HAZARD_FLOOD_COLOR}),
                    dmc.Text(_t("Flood only"), size="10px", c="dimmed")], gap=5),
        dmc.Text(_t("(shown as Tropical Cyclone only / Both / Flood only share of each value)"), size="10px", c="dimmed", fs="italic"),
    ], gap=14, mt=10)

# Illustrative per-member scaling relative to "Combined" (1.0 = same as the
# probability-weighted view) — lets picking a member visibly move the
# Impact Summary numbers so they can be compared against Combined.
_MEMBER_SCENARIO_FACTOR = {
    "control": 1.05, "member-1": 0.72, "member-2": 0.85, "member-3": 0.93, "member-4": 1.02,
    "member-5": 1.10, "member-6": 1.18, "member-7": 0.65, "member-8": 1.25, "member-9": 0.95,
    "member-10": 1.35,
}
# Not a raw member picker — the user picks which EXPOSURE/FACILITY property
# should drive the comparison, and the member that's "worst" FOR THAT
# PROPERTY is looked up automatically. Different properties can genuinely
# point at different members (the scenario with the worst population impact
# isn't necessarily the one with the worst schools impact). Matches the
# actual exposure/infrastructure properties elsewhere on this page — not
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
        # Separated at the bottom — comparing against the deterministic/control
        # run specifically is a real but occasional need, not a normal default.
        {"group": _t("Other"), "items": [{"value": "deterministic", "label": _t("Deterministic")}]},
    ]
_WORST_MEMBER_BY_FACTOR = {
    "population": "member-10", "children": "member-8", "built_up": "member-6",
    "schools": "member-5", "health_centers": "member-4", "shelters": "member-6", "wash": "member-8",
    "deterministic": "control",
}


def _member_label(value):
    for entry in _ensemble_members():
        if entry.get("value") == value:
            return entry["label"]
        for item in entry.get("items", []):
            if item["value"] == value:
                return item["label"]
    return value


def _member_short_label(value):
    """Compact tag for inline use next to a comparison number — 'member-6' -> '#6'."""
    if not value:
        return ""
    if value == "control":
        return "Control"
    if value.startswith("member-"):
        return f"#{value.split('-')[-1]}"
    return value


def _parse_stat_number(v):
    """'640K' -> 640000, '185' -> 185."""
    if isinstance(v, (int, float)):
        return v
    v = v.strip()
    if v.endswith("K"):
        return round(float(v[:-1]) * 1_000)
    if v.endswith("M"):
        return round(float(v[:-1]) * 1_000_000)
    return round(float(v))


def _format_stat_number(n):
    """640000 -> '640K', 185 -> '185'."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{round(n / 1000)}K"
    return str(round(n))


def _scaled_stats(stats, member):
    factor = _MEMBER_SCENARIO_FACTOR.get(member, 1.0)  # "combined" (or unknown) -> unscaled
    if factor == 1.0:
        return dict(stats)
    return {k: _format_stat_number(round(_parse_stat_number(v) * factor)) for k, v in stats.items()}


def _scaled_pin_pct(pin_pct, member):
    factor = _MEMBER_SCENARIO_FACTOR.get(member, 1.0)
    if factor == 1.0:
        return dict(pin_pct)
    return {k: max(0, min(100, round(v * factor))) for k, v in pin_pct.items()}


# "Combined Total" mode for the Impact Summary/Full Breakdown — sums each
# selected country's own (optionally worst-case-scaled) numbers rather than
# averaging a percentage, so a combined In Need total always equals the sum
# of what each country's own tile would show individually.
def _combined_stats(countries, member=None, date=None, run=None, wind_kt=None, hz=None):
    keys = list(_DEFAULT_STATS.keys())
    totals = {k: 0 for k in keys}
    for c in countries:
        base = _get_country_stats(c, date, run, wind_kt, hz=hz)
        scaled = _scaled_stats(base, member) if member else base
        for k in keys:
            totals[k] += _parse_stat_number(scaled.get(k, "0"))
    return {k: _format_stat_number(v) for k, v in totals.items()}


def _combined_in_need_total(countries, risk_key, pin_key, member=None, date=None, run=None, wind_kt=None, hz=None):
    total = 0
    for c in countries:
        base_stats = _get_country_stats(c, date, run, wind_kt, hz=hz)
        pin_pct = _get_country_pin_pct(c, date, run, wind_kt, hz=hz)
        if member:
            base = _parse_stat_number(_scaled_stats(base_stats, member).get(risk_key, "0"))
            pct = _scaled_pin_pct(pin_pct, member)[pin_key]
        else:
            base = _parse_stat_number(base_stats.get(risk_key, "0"))
            pct = pin_pct[pin_key]
        total += round(base * pct / 100)
    return total


# Copied near-verbatim from callbacks/metrics.py's _make_arc_chart for exact
# visual parity with the real dashboard's "PEOPLE IN NEED" panel
# (layouts/panels.py:871-889) — same 270° 3-ring polar-bar gauge, same colors,
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
    exp_pct = (exposed / total * 100) if (not no_data and exposed) else 0.0
    nee_pct = (in_need / total * 100) if (not no_data and in_need) else 0.0

    pop_deg = _FULL_DEG if not no_data else 0.0
    exp_deg = _pct_deg(exp_pct)
    nee_deg = _pct_deg(nee_pct)

    _GAP_THETA = 357

    ring_specs = [
        (_BASES[0], pop_deg, _NAVY, _t("Population")),
        (_BASES[1], exp_deg, _BLUE, _t(exposed_label)),
        (_BASES[2], nee_deg, _ORANGE, _t(in_need_label)),
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
            # Smaller than the real dashboard's own 10px — this chart runs at
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
        # Transparent (not the real dashboard's solid white) — this modal's
        # background is a tinted frosted-glass panel, not a plain white
        # Paper, so the chart needs to blend into it rather than sit in its
        # own white box.
        # Left margin wide enough for the longest ring label ("Children In
        # Need"/"People In Need") to not get clipped by the figure edge —
        # the labels extend leftward from their anchor point ("middle left").
        showlegend=False, margin=dict(l=55, r=20, t=16, b=20), paper_bgcolor="rgba(0,0,0,0)", height=height,
    )
    return fig



def _cat_badge(cat):
    # Handles both "Category N" (old mock shape) and the real
    # _category_label_from_kt shape ("Category N Hurricane", or a
    # sub-hurricane label like "Severe Trop. Storm", or "Unknown") — only
    # the first form abbreviates to "Cat N"; anything else is shown as-is
    # rather than crashing on a label with no trailing digit.
    label = cat or "Unknown"
    parts = label.split()
    text = f"Cat {parts[1]}" if len(parts) >= 2 and parts[0] == "Category" and parts[1].isdigit() else label
    return dmc.Badge(text, variant="filled", size="sm",
                      style={"backgroundColor": _CAT_COLORS.get(label, "#8ea0ab"), "color": "#fff"})


# Illustrative only — which storms have a mock alert email available.
# Real emails are already generated as HTML and stored in Snowflake tables
# (SEND_ALERT/SEND_WARNING procedures), keyed by storm+country+date; a real
# implementation would query by that same combination instead of a fixed set.
# MELISSA included alongside GENEVIEVE so the real Jamaica/28 Oct 2025 demo
# scenario (a genuine named, classified storm that really did have an alert
# email sent) shows the same email affordance once storm resolution becomes
# reactive to historical dates (see the in-progress reactive-storm-resolution
# fix) — not just whatever happens to be "currently active".
_ALERT_EMAIL_AVAILABLE = {"GENEVIEVE", "MELISSA"}
_MOCK_ALERT_EMAIL_URL = "/assets/mock_alert_email.html"


def _storm_row(s, bordered=False):
    """Whole-row-clickable storm entry — used both in the top-bar search
    dropdown and the Active Storms list (Global mode, or a selected
    country). No separate 'Select' button (matching global_zoom_navigation's
    row style, not the WeatherLab reference's button-per-row).
    """
    style = {"padding": "9px 12px", "cursor": "pointer"}
    if bordered:
        style.update({"borderRadius": "8px", "border": "1px solid #eef2f5", "background": "#f6f9fb", "marginBottom": "8px"})
    else:
        style.update({"borderTop": "1px solid #eef2f5", "padding": "10px 14px"})

    right_side = [_cat_badge(s["cat"])]
    # Only in the Active Storms list (bordered=True), not the search dropdown
    # — avoids cluttering search results with an action button. Clicking it
    # also selects the storm as a side effect (it's nested inside the same
    # clickable row, and Dash has no simple stopPropagation) — acceptable
    # here since "open its alert email" already implies picking that storm.
    if bordered and s["name"] in _ALERT_EMAIL_AVAILABLE:
        right_side.append(dmc.ActionIcon(
            DashIconify(icon="carbon:email", width=14),
            id={"type": "alert-email-btn", "name": s["name"]}, n_clicks=0,
            variant="light", color=WIND, size="sm",
        ))

    # Real storms with a track but no measurable country impact yet (still
    # at sea) carry an empty `countries` list (see _resolve_storms_for_date)
    # — say so explicitly rather than rendering a blank subtitle line.
    subtitle = ", ".join(s["countries"]) if s["countries"] else _t("No country impact yet")
    return html.Div(
        dmc.Group([
            html.Div([dmc.Text(s["name"], fw=700, size="sm"), dmc.Text(subtitle, size="xs", c="dimmed")], style={"lineHeight": 1.3}),
            dmc.Group(right_side, gap=6, wrap="nowrap", align="center"),
        ], justify="space-between", align="center"),
        id={"type": "select-storm", "name": s["name"]}, n_clicks=0, style=style,
    )


def _vdivider(color=None):
    return dmc.Divider(orientation="vertical", color=color)


# Lightweight, always-visible loading affordance for the two genuinely slow
# real round-trips on this page — a fresh country/storm/forecast_date
# selection (tracks+envelopes query + per-hazard stats/admin-stats tile-
# server calls, ~11s cold) and the Tiles<->Regions cmdbar-detail toggle
# (~3-5s, admin-stats re-fetch) — both of which are already Inputs to
# _build_hazard_tile_config below.
#
# Real bug found+fixed here (confirmed live via Playwright, still broken
# despite this file's own PREVIOUS "fix" comment claiming it was resolved by
# nesting ms-tile-config-store as a child): dcc.Loading's target_components
# mechanism never actually shows this spinner — polled the DOM every 150ms
# through multiple genuinely-slow (1.6-3.0s measured) real callback
# round-trips and it never appeared, even with the Store nested as a child.
# Dash's OWN generic top-level indicator (a `.dash-loading-callback`-classed
# div inserted directly under #react-entry-point) DID reliably appear for
# the same requests in the same test — so this reuses THAT already-proven
# signal via a plain MutationObserver (window.aotsInitLoadingIndicator,
# assets/maplibre_tiles.js) instead of trusting target_components again.
#
# Lives in the always-visible top bar (rather than a floating panel) so it
# shows in both Global and Country Analysis mode, and doesn't compete for
# space with the four existing floating panels, which already occupy
# every corner of the map.
#
# Deliberately not scoped to only ms-tile-config-store's own requests
# (Dash's global indicator fires for ANY in-flight callback) — a single
# generic "something is loading" badge is simpler and more honest than
# trying to isolate just this one store, and every other callback on this
# page is fast enough that this reads as correct in practice.
def _ms_loading_badge():
    return html.Div(
        dmc.Loader(size="sm", color="white", type="bars"),
        id="ms-global-loading-indicator",
        style={"display": "none"},
    )


def _topbar(initial_countries=None):
    return html.Div([
        dmc.Group([
            dmc.Text("AHEAD OF THE STORM", c="#ffffff",
                      style={"fontFamily": "'Handjet', sans-serif", "fontWeight": 800, "fontSize": "17px", "letterSpacing": "0.3px"}),

            _vdivider(color="rgba(255,255,255,0.35)"),

            # Mirrors the real header's "Last Updated" (components/ui/header.py)
            # — when the underlying data was last refreshed, distinct from the
            # forecast's own valid date/run picked below. Real now (see
            # _update_last_updated below) — same get_latest_forecast_time_overall()
            # + 15-min dcc.Interval pattern as update_last_updated_header in the
            # (reverted) pages/dashboard.py, just under a distinct id/interval
            # ("ms-..." prefixed) to avoid colliding with layouts/panels.py's own
            # "metadata-refresh-interval", which is live in the same running app.
            dmc.Group([
                dmc.Text(_t("Last Updated:"), size="xs", c="white", opacity=0.8),
                dmc.Text("—", id="ms-last-updated", size="xs", fw=700, c="#ffffff"),
            ], gap=6, wrap="nowrap"),

            _vdivider(color="rgba(255,255,255,0.35)"),

            dmc.Group([
                dmc.DatePickerInput(
                    id="topbar-date", value=_DEFAULT_FORECAST_DATE, valueFormat="D MMM YYYY", size="xs", w=130,
                    leftSection=DashIconify(icon="carbon:calendar", width=13),
                    # Greys out/disables any calendar day later than the
                    # latest REAL forecast_time in the database (not
                    # wall-clock "today" — see _max_allowed_run_for_date's
                    # own docstring) so a date with no real data can't be
                    # picked at all, rather than silently resolving to an
                    # empty page.
                    maxDate=_DEFAULT_FORECAST_DATE,
                    # The top bar itself sits at zIndex 1000 (_topbar's
                    # style) so it layers over the map/panels below it — but
                    # that also meant it was cutting into this popover's own
                    # (lower, Mantine-default) z-index when it opened right
                    # under the bar. Bump it above the bar explicitly.
                    popoverProps={"zIndex": 2000},
                ),
                dmc.SegmentedControl(
                    id="topbar-time", value=_DEFAULT_FORECAST_RUN,
                    data=_time_options_for_date(_DEFAULT_FORECAST_DATE),
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
            # (e.g. BAVI — Philippines + Vietnam) selects all of them here at
            # once, and the Impact Summary panel shows one stat block per
            # selected country side by side, rather than forcing one arbitrary
            # pick or a separate comparison view.
            dmc.MultiSelect(
                id="topbar-country-select", value=initial_countries or [],
                placeholder=_t("Select countries…"), data=_country_options(),
                searchable=True, clearable=True, size="xs", w=260, maw=320,
                leftSection=DashIconify(icon="carbon:location", width=13),
                # Same z-index bump as the date picker above, for the same
                # reason — otherwise the dropdown opens partly under the bar.
                comboboxProps={"zIndex": 2000},
                # Keep selected-country pills on one line, side by side, no
                # matter how many are picked — never wrap onto a second row
                # and grow the top bar taller. Extra pills scroll
                # horizontally within the fixed-width box instead.
                styles={"pillsList": {"flexWrap": "nowrap", "overflowX": "auto"},
                         "pill": {"flexShrink": 0}},
            ),

            _vdivider(color="rgba(255,255,255,0.35)"),

            _ms_loading_badge(),

        ], gap=18, wrap="wrap", align="center", style={"paddingRight": "150px"}),
        # Absolutely positioned against the bar itself (not a marginLeft:auto
        # flex child) — guarantees it stays pinned to the top-right corner
        # even when the rest of the bar's items wrap onto a second line on a
        # narrow viewport, which a flex auto-margin trick wouldn't survive.
        # right:24px (its own value, not _UI_MARGIN) — real bug found+fixed
        # here: aligning this exactly with the panel edges (15px) put it
        # visibly too close to the screen's own edge; the topbar/footer's
        # own content wants more breathing room from the true screen edge
        # than the floating panels need from THEM, even though the panels
        # themselves stay at the tighter _UI_MARGIN.
        html.Div(_language_switcher(_LANG),
                  style={"position": "absolute", "top": "50%", "right": "20px", "transform": "translateY(-50%)"}),
    ],
        # Full-width fixed bar flush with the top edge — like the footer at
        # the bottom, not a floating rounded card (that was this shell's
        # earlier "map-first" treatment; the bar is chrome now, same tier as
        # the footer). 24px horizontal padding — its own value, see the
        # language-switcher comment just above for why this isn't _UI_MARGIN.
        style={"position": "fixed", "top": 0, "left": 0, "width": "100%",
               "backgroundColor": "#00AEEF", "padding": "14px 20px",
               "display": "flex", "alignItems": "center", "zIndex": 1000},
    )


# ---------------------------------------------------------------------------
# Controls panel — content depends on mode (Global vs Country Analysis), but
# Hurricane/Flood Hazards are IDENTICAL components in both (only one mode's
# content is ever mounted at a time, so reusing the same ids is safe). Global
# additionally lacks Exposure/Infrastructure, since those need a selected
# country to mean anything. There's no severity-exploration reason to dumb
# Global down to plain checkboxes — the same "what if this hits Category X"
# question is just as valid before you've zoomed into a place.
# ---------------------------------------------------------------------------
def _active_storms_section(countries=None, date=None, run=None):
    # Standalone, sits ABOVE the Tropical Cyclone family in both modes — not
    # nested inside it. Scoped to whichever countries are selected. A storm
    # matches if it affects ANY of the selected countries (not all) — BAVI
    # shows up whether you've picked Philippines, Vietnam, or both.
    #
    # REACTIVE to the selected topbar date/run (_resolve_storms_for_date) —
    # not the frozen "active right now" _STORMS snapshot. This is what makes
    # a historical Demo Scenario date (e.g. MELISSA on 28 Oct 2025) show the
    # exact same bordered storm-row box (name, category, alert-email icon) a
    # genuinely-live storm gets — real data either way. No "Historical"
    # badge distinguishing the two (removed — it read as wrong even for
    # currently-live storms, since "live" was derived from country-impact
    # membership, which a real storm with no impact yet never satisfies).
    #
    # Returns None (renders nothing) when there's genuinely no real storm for
    # this date — the panel then opens straight on the Exposure/Hazard switch
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

    content = html.Div([_storm_row(s, bordered=True) for s in scoped_storms])
    children = [dmc.Text(label, size="10px", fw=700, c="dimmed", tt="uppercase", mb=10), content]
    if countries:
        names = ", ".join(s["name"] for s in scoped_storms)
        children.append(dmc.Text(_t("Currently tracking {names}.", names=names), size="11px", c="dimmed", mt=10,
                                   style={"lineHeight": 1.6}))
    # Bottom padding trimmed to 12px (from the section's own 20px top/side
    # padding) — each storm row already carries its own 8px marginBottom
    # (_storm_row, bordered=True), so the last card's bottom edge otherwise
    # sat ~28px from the divider below, visibly more than the ~20px gap
    # every other section boundary on this panel uses.
    return html.Div(children, style={"padding": "20px 18px 12px", "borderTop": "1px solid #eef2f5"})



def _hurricane_family(countries=None, expanded=True, date=None, run=None):
    # No storms list in here anymore — that's _active_storms_section above.
    # When nothing's active, collapse straight to grey: no body at all, not
    # a redundant "nothing here" message (the Active Storms section above
    # already said that) — disabled checkboxes/sliders with nothing behind
    # them just read as broken, not "off".
    #
    # Header is a pure collapse (chevron), not a switch — collapsing the
    # section no longer implies turning the hazard off; Sustained Wind/Gust
    # keep whatever checked state they already have underneath.
    countries = countries or []
    is_global = not countries
    if countries:
        # REACTIVE: does any SELECTED country have real storm data for the
        # CURRENTLY selected topbar date/run (_resolve_storm_for_country) —
        # not "is something active right now" (the frozen _STORMS snapshot,
        # correct only for the Global-mode Active Storms list below). This is
        # what lets Sustained Wind/Gust/Tracks stay enabled for a historical
        # date (date picker, Demo Scenarios) with no currently-active storm.
        has_storms = any(_resolve_storm_for_country(c, date, run) for c in countries)
        # Gust is storm-scoped, not country-scoped (unlike River/Rain), so
        # availability is "does the resolved storm(s) for the selected
        # countries have real gust data at this forecast_time" — reuses
        # each country's already-resolved storm/forecast_time instead of a
        # fresh per-country query.
        _resolved = [_resolve_storm_for_country(c, date, run) for c in countries]
        gust_available = any(
            r and r["name"] in get_gust_track_ids_for_date(r["forecast_time"])
            for r in _resolved
        )
    else:
        # Global (nothing selected) — REACTIVE to the selected topbar
        # date/run, matching _load_ms_tracks_and_envelopes's own Global-mode
        # tracks resolution exactly (get_track_ids_for_date, NOT the frozen
        # _STORMS snapshot, which is only ever "active in the last 12h as of
        # whenever this server process started" and was found live to say
        # "no storms" even when 2 real ones, DOLPHIN/GENEVIEVE, existed for
        # the selected date — this header must never disagree with what the
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
        # gust) — it genuinely drives which wind-severity tier's envelope
        # polygons render there (_load_ms_tracks_and_envelopes's Global
        # branch), real bug found here: it was left disabled when that
        # Global envelope feature was added, blocking the one control that
        # actually does something in Global mode.
        return html.Div([
            dmc.Slider(id=f"ms-{sub_key}-slider", min=0, max=max_val, step=1, value=default,
                       marks=[{"value": i} for i in range(max_val + 1)], size="sm", color=color, mb=4,
                       disabled=(not has_storms) or (is_global and disable_in_global) or (not available)),
            html.Div(id=f"ms-{sub_key}-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "16px"})

    # Always rendered (matching _flood_hazards_family's own unconditional
    # pattern) — these ids are referenced unconditionally by several
    # callbacks (_update_impact_summary, _update_impact_breakdown,
    # _update_breakdown_new_tab_link, _open_hazard_contribution), so omitting
    # them when has_storms is False (the old behavior, harmless back when
    # _STORMS was a mock list that could never actually be empty) hard-crashes
    # Dash's client dispatcher the moment a real "no active storm anywhere"
    # state occurs — which, for real Snowflake-backed data, is the common
    # case, not an edge case. Disabling them (instead of omitting) keeps the
    # "nothing to toggle right now" signal without breaking every callback
    # that reads their state.
    body = html.Div([
        dmc.Checkbox(id="ms-tracks-on", label=_t("Storm Tracks"), color=TRACKS, size="xs", mb=10, checked=True,
                      disabled=not has_storms),
        # Off by default in Global mode (still checkable — see
        # _load_ms_tracks_and_envelopes's own Global branch, which now
        # really does render every active storm's own real wind envelope
        # polygons at the selected threshold when this is checked, same
        # TC_ENVELOPES_COMBINED data as the Country-Analysis envelope view,
        # just without per-country severity coloring since there's no single
        # country to attribute population severity to here).
        dmc.Checkbox(id="ms-wind-on", label=_t("Sustained Wind"), color=WIND, size="xs", mb=10,
                      checked=not is_global, disabled=not has_storms),
        _slider_block("wind", WIND, 7, 2, disable_in_global=False),  # index 2 == 26 m/s / 50kt
        # Real bug found+fixed here: this used to stay hard-disabled in
        # Global mode on the (wrong) assumption that no gust envelope data
        # exists at all — TC_GUST_ENVELOPES_COMBINED (get_gust_envelope_
        # data_snowflake) is a genuinely real, separately deployed table
        # (1804 rows, confirmed live) mirroring TC_ENVELOPES_COMBINED with
        # GUST_THRESHOLD instead of WIND_THRESHOLD — it was simply never
        # queried before. Gust now renders real Global-mode envelope
        # polygons exactly like Wind (_load_ms_tracks_and_envelopes), so it
        # follows the same enable/default rule as Wind above.
        dmc.Checkbox(id="ms-gust-on", label=_t("Gust"), color=GUST, size="xs", mb=10, checked=False,
                      disabled=(not has_storms) or (not gust_available)),
        # Real bug found+fixed here: gust envelope data is genuinely real
        # but only exists for a handful of historical storms/dates
        # (confirmed live: TC_GUST_ENVELOPES_COMBINED has rows only for
        # BAVI/MAYSAK/DOUGLAS between 2026-07-02 and 2026-07-05 — the
        # latest real forecast cycle, 2026-07-31, has zero gust rows for
        # either currently active storm). Same "no real forecast data"
        # explanatory note as River/Rain's own _availability_note above,
        # so an ungreyed-but-disabled checkbox never reads as a bug.
        (dmc.Text(_t("No real gust forecast data for this storm/date."),
                   size="10px", c="dimmed", fs="italic", mb=8)
         if has_storms and not gust_available else None),
        _slider_block("gust", GUST, 7, 2, disable_in_global=False, available=gust_available),  # index 2 == 26 m/s / 50kt
        dmc.Text(_t("View As"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
        # Locked to Envelopes in Global mode — Probability Raster is
        # specifically the country-scoped wind/gust MapLibre raster
        # (_build_hazard_tile_config returns wind_visible=False whenever no
        # country is selected, by design), which never renders anything
        # without a selected country, unlike Envelopes above.
        dmc.SegmentedControl(
            id="tc-view-as", value="envelopes", fullWidth=True, size="xs",
            disabled=(not has_storms) or is_global,
            data=[{"value": "envelopes", "label": _t("Envelopes")},
                  {"value": "raster", "label": _t("Probability Raster")}],
        ),
    ], id="ms-hurricane-body", style={"marginTop": "16px"} if (has_storms and expanded) else {"marginTop": "16px", "display": "none"})

    return html.Div([header, body], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _collapsible_section(key, title, body, expanded=True):
    """Collapsible section WITHOUT a master switch — Exposure/Infrastructure
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
    # Real per-country availability overview — see _get_data_availability_real
    # above for the real check_baseline_data.py-style query this mirrors.
    countries = countries or []
    if not countries:
        return dmc.Text(_t("Select a country to see data availability."), size="11px",
                          c="dimmed", fs="italic")

    fields = [("population", _t("Pop")), ("age_0_4", "0-4"), ("age_5_14", "5-14"),
              ("age_15_19", "15-19"), ("rwi", "RWI"), ("settlement", _t("Settlement")),
              ("moderate_poverty", _t("Mod. Poverty")), ("severe_poverty", _t("Sev. Poverty"))]

    def _flag(ok):
        return html.Span("✓" if ok else "✕", style={"color": "#2f9e64" if ok else "#d94f3c", "fontWeight": 700})

    rows = []
    for c in countries:
        d = _get_data_availability_real(c)
        if not d:
            rows.append(dmc.Text(_t("{c}: no availability data.", c=_t(c)), size="11px", c="dimmed", mb=8))
            continue
        rows.append(html.Div([
            dmc.Text(_t(c), size="11px", fw=700, mb=4),
            dmc.Text(_t("{schools} Schools · {health_centers} HCs · {shelters} Shelters · {wash} WASH",
                         schools=f"{d['schools']:,}", health_centers=f"{d['health_centers']:,}",
                         shelters=f"{d['shelters']:,}", wash=f"{d['wash']:,}"),
                      size="10.5px", c="dimmed", mb=4),
            dmc.Group([
                html.Span([_flag(d[key]), " ", label], style={"fontSize": "10.5px", "color": "#57707e"})
                for key, label in fields
            ], gap=10, style={"rowGap": "4px"}),
        ], style={"marginBottom": "14px"}))

    return html.Div(rows)


def _exposure_section():
    # No "Exposure" header here — it's the first thing shown under the
    # Exposure/Hazard switch above, and that switch already says "Exposure",
    # so a repeated header directly beneath it would be redundant.
    #
    # Age-band breakdown (infant/school-age/adolescent) matches the real
    # app's own property list (layouts/panels.py:399-435) — flat radio
    # options in the same group as "Children (total)", just visually
    # indented/greyed to read as a breakdown of it, not structurally nested.
    #
    # No RadioGroup wrapper here anymore — these radios, Infrastructure, and
    # Context Data's radios all now share ONE exposure-property RadioGroup
    # built by the caller (_controls_zoom), so Context Data can sit after
    # Infrastructure while its properties still drive the same selection.
    def _sub_label(text):
        return html.Span(text, style={"paddingLeft": "12px", "color": "#8ea0ab", "fontSize": "11px"})

    radios = [
        # Not a demographic/exposure tile at all — the raw hazard
        # probability raster itself (real app: the "probability" property in
        # _AOTS_PROP_MAP, same raster the tile sidecar already serves at
        # /tiles/raster/.../probability/...), included here so you can view
        # "how likely is impact here" on its own, independent of who/what is
        # exposed. Kept in the same flat radio group (same click-to-switch
        # behavior as every other property here), just visually separated by
        # its own divider since it isn't part of the population/children/
        # built-up family below it.
        dmc.Radio(label=_t("Hazard Probability"), value="probability", size="xs"),
        html.Div(style={"borderTop": "1px solid #eef2f5", "margin": "10px 0"}),
        dmc.Radio(label=_t("Population"), value="population", size="xs"),
        dmc.Radio(label=_t("Children (total)"), value="children", size="xs"),
        dmc.Radio(label=_sub_label(_t("Age 0–4 (Infant)")), value="infant", size="xs"),
        dmc.Radio(label=_sub_label(_t("Age 5–14 (School-age)")), value="school-age", size="xs"),
        dmc.Radio(label=_sub_label(_t("Age 15–19 (Adolescent)")), value="adolescent", size="xs"),
        dmc.Radio(label=_t("Built-up Area"), value="built", size="xs"),
    ]
    return html.Div([
        dmc.Stack(radios, gap=10, mb=16),
        # No "Baseline" option — with no hazard toggled below, the property
        # already shows plain/raw data (same rule as Infrastructure's
        # location-points-vs-impact-styling note); a separate "Baseline"
        # choice here would just duplicate "no hazard active."
        dmc.Text(_t("View As"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
        dmc.SegmentedControl(
            id="exposure-view-as", value="expected", fullWidth=True, size="xs",
            data=[{"value": "expected", "label": _t("At Risk")},
                  {"value": "inneed", "label": _t("In Need (Coming Soon)"), "disabled": True}],
        ),
        dmc.Text(id="exposure-view-note", size="11px", c="dimmed", mt=8),
    ], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _infrastructure_section():
    # Checkbox "checked" state drives layer visibility directly (see
    # _register_ms_facility_layer) — the note below is about coloring/styling
    # once a hazard is toggled on, not about whether the layer shows at all.
    facilities = [("Schools", "schools"), ("Health Centers", "health"),
                  ("Shelters", "shelters"), ("WASH Facilities", "wash")]
    return _collapsible_section("infra", _t("Infrastructure"), [
        dmc.Stack([dmc.Checkbox(id=f"ms-facility-{lid}-on", label=_t(label), size="xs", checked=False)
                   for label, lid in facilities], gap=10),
        dmc.Text(_t("Shown as plain locations with no hazard active; colored by impact probability once a hazard is toggled on below."),
                  size="11px", c="dimmed", mt=10, style={"lineHeight": 1.5}),
    ], expanded=True)


def _context_data_section(countries=None):
    # Settlement/RWI/Poverty — real properties too (layouts/panels.py:426-433),
    # tucked into their own collapsed sub-section since they're for review,
    # not everyday use, and placed after Infrastructure (last in the Exposure
    # pane). Radios still belong to the shared exposure-property RadioGroup
    # built by the caller — selecting one still drives the map property,
    # despite sitting after unrelated Infrastructure checkboxes in the DOM.
    return html.Div([
        html.Div([
            dmc.Text(_t("Context Data"), fw=700, size="10px", c="dimmed", tt="uppercase", style={"flex": 1}),
            html.Span("▾", id="chev-context", style={"fontSize": "10px", "color": "#8ea0ab"}),
        ], id="head-context", style={"display": "flex", "alignItems": "center", "gap": "8px", "cursor": "pointer"}),
        html.Div([
            dmc.Stack([
                dmc.Radio(label=_t("Settlement Classification"), value="settlement", size="xs"),
                dmc.Radio(label=_t("Relative Wealth Index"), value="rwi", size="xs"),
                dmc.Radio(label=_t("Moderate Child Poverty Rate"), value="moderate-poverty", size="xs"),
                dmc.Radio(label=_t("Severe Child Poverty Rate"), value="severe-poverty", size="xs"),
            ], gap=12, mb=18),
            dmc.Text(_t("Data Availability"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=10),
            _data_availability_table(countries),
        ], id="body-context", style={"marginTop": "18px", "display": "none"}),
    ], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _flood_hazards_family(expanded=True, countries=None, date=None, run=None):
    # River Flooding is single-axis (return period only). Rainfall is
    # genuinely 2-axis — 4 real accumulation windows (PRECIP_WINDOWS_H in
    # DATAPIPELINE's main_pipeline.py), each with their own 3 depth tiers
    # (PRECIP_TP_THRESHOLDS_MM) — so it needs its own window SegmentedControl
    # above the severity slider, not just a slider like the generic
    # _hazard_family helper gives every other hazard. Lost this when I
    # generalized the helper; restoring it here as a dedicated builder.
    #
    # Header is a pure collapse (chevron), not a switch — same reasoning as
    # Tropical Cyclone: collapsing shouldn't imply River Flooding/Rainfall
    # turn off, only that they're checked/unchecked individually below.
    #
    # ms-river-on/ms-rain-on below do double duty: unchanged, they still
    # drive _build_hazard_tile_config's country-scoped, probability-only
    # impact system exactly as before. They ALSO now drive the two GLOBAL,
    # country/storm-independent RAW hazard layers (raw precip-rate raster +
    # raw river-discharge raster — see services/tile_server.py's "Global raw
    # precipitation-rate endpoints"/"Global raw river-discharge endpoints"
    # sections) via the separate _build_global_raw_config callback further
    # down, which reads these same two checkbox ids as an independent,
    # additional Input (Dash allows multiple callbacks on the same
    # component/prop as Input) — this replaces the old standalone raw-layer
    # checkboxes + their own floating panel, removed per explicit user
    # direction against duplicate controls. The raw layers stay
    # global/uncropped even in Country Analysis mode (no country-bbox
    # filtering anywhere) — checking one of these boxes there shows the
    # always-global raw backdrop AND the country-scoped impact overlay at
    # once; they are two different things, not a conflict. flood-view-as
    # below (Mean/Probability) controls ONLY the raw layers' aggregation and
    # has no effect on the country-scoped system, which stays
    # probability-only forever.
    # REACTIVE (same pattern as _hurricane_family's own has_storms check,
    # re-evaluated here every time _switch_mode_content re-renders this
    # panel): River Flooding/Rainfall are NOT storm-scoped — each resolves
    # its own independent forecast_date straight from
    # MERCATOR_TILE_RIVER_MAT/MERCATOR_TILE_PRECIP_MAT (see
    # _build_hazard_tile_config), so "no data" here means the primary
    # selected country has genuinely NO row in that MAT table at all, not
    # merely a stale/mismatched date. get_latest_river_forecast_time/
    # get_latest_rain_forecast_time already return None for exactly that
    # case (their own docstrings) and are ttl_cached, so this reuses the
    # same signal _build_hazard_tile_config computes rather than adding a
    # second real query path. No countries selected (Global) — nothing to
    # check against yet, so leave both available (matches _hurricane_
    # family's own "nothing selected" fallback).
    is_global = not countries
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
        dmc.Badge(_t("Proxies"), size="xs", variant="light", color=RIVER if has_flood_hazards else "gray", mr=4),
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
        dmc.Checkbox(id="ms-river-on", label=_t("River Flooding"), color=RIVER, size="xs", mb=10,
                      checked=river_available, disabled=not river_available),
        _availability_note(river_available, river_country_available, river_raw_available),
        html.Div([
            # Default index 2 == "rp10" — the real, non-placeholder return-
            # period tier (rp2/rp5 are IS_STANDIN=True placeholders).
            dmc.Slider(id="ms-river-slider", min=0, max=5, step=1, value=2,
                       marks=[{"value": i} for i in range(6)], size="sm", color=RIVER, mb=4,
                       disabled=not river_available),
            html.Div(id="ms-river-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "20px"}),

        dmc.Checkbox(id="ms-rain-on", label=_t("Rainfall"), color=RAIN, size="xs", mb=10,
                      checked=rain_available, disabled=not rain_available),
        _availability_note(rain_available, rain_country_available, precip_raw_available),
        html.Div([
            dmc.SegmentedControl(
                id="ms-rain-window", value="6", fullWidth=True, size="xs", color=RAIN, mb=10,
                disabled=not rain_available,
                data=[{"value": "6", "label": "6h"}, {"value": "24", "label": "24h"},
                      {"value": "72", "label": "72h"}, {"value": "120", "label": _t("5 days")}],
            ),
            dmc.Slider(id="ms-rain-slider", min=0, max=2, step=1, value=1,
                       marks=[{"value": i} for i in range(3)], size="sm", color=RAIN, mb=4,
                       disabled=not rain_available),
            html.Div(id="ms-rain-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "20px"}),

        # Placeholder — no real pipeline/data behind this yet (see
        # tc_ecmwf_additional_layers work elsewhere), single-axis like River
        # Flooding (height above normal tide, not a window+depth pair like
        # Rainfall) since storm surge doesn't have a separate accumulation
        # window concept. Permanently disabled/unchecked (not conditional on
        # any per-country availability check like River/Rain above — there's
        # no real backend for this hazard anywhere, ever, so it's always
        # "coming soon" rather than sometimes-available) — this same
        # ms-surge-on.disabled flag also grays the top command-bar pill via
        # the generic _reflect_checkbox_on_pill loop, no separate wiring
        # needed there. See surge_visible's own comment in
        # _build_hazard_tile_config for the matching backend-side flag.
        dmc.Group([
            dmc.Checkbox(id="ms-surge-on", label=_t("Storm Surge"), color=SURGE, size="xs",
                          checked=False, disabled=True),
            dmc.Badge(_t("Coming Soon"), size="xs", variant="light", color="gray"),
        ], gap=8, mb=10),
        html.Div([
            dmc.Slider(id="ms-surge-slider", min=0, max=3, step=1, value=1,
                       marks=[{"value": i} for i in range(4)], size="sm", color=SURGE, mb=4,
                       disabled=True),
            html.Div(id="ms-surge-readout", style={"fontSize": "11px", "color": "#57707e"}),
        ], style={"marginLeft": "22px", "marginBottom": "16px"}),

        # Mirrors _hurricane_family's own tc-view-as toggle exactly (same
        # label styling + fullWidth/size="xs" SegmentedControl convention).
        # Unlike tc-view-as (disabled when there's no storm data at all),
        # this is never disabled — it only affects the always-global raw
        # precip-raw/river-raw layers (see the header comment above), which
        # render independent of any per-country River Flooding/Rainfall data
        # availability check.
        dmc.Text(_t("View As"), size="10px", fw=700, c="dimmed", tt="uppercase", mb=6),
        # Locked to Mean in Global mode (explicit product decision — the raw
        # layer's Probability is mechanically global-safe on its own, but
        # Global view has no country-scoped combination context, so Mean is
        # kept as the only Global-mode choice for consistency with the
        # Tropical Cyclone panel's own Global-mode lock above).
        dmc.SegmentedControl(
            id="flood-view-as", value="mean", fullWidth=True, size="xs",
            disabled=is_global,
            data=[{"value": "mean", "label": _t("Mean")},
                  {"value": "probability", "label": _t("Probability")}],
        ),
        # DATE-based "not available" note for the raw layers specifically —
        # populated reactively by _update_raw_date_availability_note (further
        # down, keyed on ms-global-raw-config-store) rather than computed
        # here at layout-build time, same "empty placeholder + dedicated
        # callback" pattern as ms-river-readout/ms-rain-readout just above.
        # Deliberately a SEPARATE element from _availability_note (COUNTRY-
        # based, for the checkboxes' other role driving the impact system)
        # — see that callback's own docstring for why the two are never
        # merged into one ambiguous message.
        html.Div(id="ms-raw-date-availability-note", style={"marginTop": "8px"}),
    ] if c is not None], id="ms-flood-body", style={"marginTop": "16px"} if expanded else {"marginTop": "16px", "display": "none"})

    return html.Div([header, body], style={"padding": "20px 18px", "borderTop": "1px solid #eef2f5"})


def _controls_global(date=None, run=None):
    # Collapsed by default (Global has less room to spare than a single
    # zoomed-in country's panel) but still active underneath — collapsing
    # is purely a display state now, not a deactivation.
    children = [
        _active_storms_section(date=date, run=run),
        _hurricane_family(expanded=False, date=date, run=run),
        _flood_hazards_family(expanded=False, date=date, run=run),
    ]
    return html.Div([c for c in children if c is not None], className="controls-stack")


def _view_toggle():
    # Switches which content is shown below — Exposure (+ Infrastructure) or
    # Hazard (Tropical Cyclone + Flood Hazards) — instead of stacking both at
    # once, which is what made the panel feel squished. Exposure is the
    # default: "who/what is here" before "which hazard".
    return html.Div(
        dmc.SegmentedControl(
            id="controls-view-toggle", value="exposure", fullWidth=True, size="xs",
            data=[{"value": "exposure", "label": _t("Exposure")}, {"value": "hazard", "label": _t("Hazard")}],
        ),
        style={"padding": "14px 18px 4px", "borderTop": "1px solid #eef2f5"},
    )


def _controls_zoom(countries=None, date=None, run=None):
    # Order: Active Storm(s) first if there are any (nothing rendered at all
    # otherwise — the switch below just becomes the first thing shown), then
    # the Exposure/Hazard switch, then whichever pane it's set to.
    children = [
        _active_storms_section(countries=countries, date=date, run=run),
        _view_toggle(),
        html.Div(
            dmc.RadioGroup(
                html.Div([_exposure_section(), _infrastructure_section(), _context_data_section(countries)]),
                id="exposure-property", value="population",
            ),
            id="controls-exposure-pane",
        ),
        html.Div([_hurricane_family(countries=countries, date=date, run=run),
                   _flood_hazards_family(countries=countries, date=date, run=run)],
                  id="controls-hazard-pane", style={"display": "none"}),
    ]
    return html.Div([c for c in children if c is not None], className="controls-stack")


def _controls_panel():
    # Always present — no dismiss button. This is the primary navigation
    # surface (hazards, exposure, infrastructure), not an optional overlay
    # like a search result or a one-off detail panel. No headline: whatever
    # renders first (Active Storms, or the Exposure/Hazard switch when
    # there's no storm) already says what this panel is for.
    return html.Div(
        html.Div(_controls_global(), id="controls-body"),
        # top/maxHeight from the shared _PANEL_TOP/_PANEL_MAX_HEIGHT spacing
        # system (see their own comment) — same values impact-panel and
        # command-bar use, so every gap in this shell (topbar-to-panel,
        # panel-to-edge, panel-to-bottom-row, bottom-row-to-footer) is the
        # same 16px margin.
        id="controls-panel", style={**_PANEL_STYLE, "top": _PANEL_TOP, "left": f"{_UI_MARGIN}px", "width": "290px",
                                     "maxHeight": _PANEL_MAX_HEIGHT, "overflowY": "auto"},
    )


def _stat_grid(stats, label=None, extra_stats=None, extra_label=None, scope="global",
                compare_stats=None, compare_member=None):
    """One country's (or the global default's) stat block. `label` renders a
    small heading above the grid — used to tell countries apart when several
    are shown side by side; omitted for the single/global case. `extra_stats`
    adds a second, labeled group below (In Need numbers — Country Analysis
    only). `scope` disambiguates click-to-breakdown ids when several of
    these grids render at once (side-by-side countries, or this panel vs the
    modal) — must be unique per rendered grid. `compare_stats` (Country
    Analysis only) adds a second, colored number in each tile — the
    Influencing-Factor-determined member's value, right next to the
    Probabilistic one, tagged with `compare_member` (e.g. "#6") so it's
    clear which specific scenario the comparison number belongs to.
    """
    member_tag = _member_short_label(compare_member)

    def _card(k, v):
        content = [
            dmc.Text(v, size="lg", fw=700, ff="monospace", mb=2),
            dmc.Group([
                DashIconify(icon=_STAT_ICONS.get(k, "mdi:help-circle-outline"), width=13, color="#8ea0ab"),
                dmc.Text(_t(k), size="10px", c="dimmed", fw=700, tt="uppercase"),
            ], gap=4, wrap="nowrap"),
        ]
        if compare_stats and k in compare_stats:
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
    return html.Div([dmc.Text(_t(label), size="11px", fw=700, mb=6), body], style={"marginBottom": "14px"})


# Illustrative share of "Children at Risk" per age band (sums to 1.0) —
# matches the real app's own breakdown (layouts/panels.py:399-435: Age 0-4 /
# 5-14 / 15-19, already mirrored in _exposure_section's radio list).
_CHILD_AGE_SPLIT = [("Age 0–4 (Infant)", 0.28), ("Age 5–14 (School-age)", 0.47), ("Age 15–19 (Adolescent)", 0.25)]

# Illustrative admin-level-1 shares per country (sum to 1.0) — real concept
# (the command bar's Tiles/Regions toggle + tile_server.py's /tiles/admin/
# MVT endpoint already aggregate by admin-1 boundary in the real app), mock
# numbers here. "Pacific Islands" is the one bundled REGION_MEMBERS entry
# (layouts/panels.py) — its "admin-1" units are the constituent countries
# within the bundle, not sub-national regions, since it's already one level
# up from a single country.
_ADMIN1_REGIONS = {
    "Philippines":      [("Bicol Region", 0.35), ("Eastern Visayas", 0.30), ("Caraga", 0.20), ("Davao Region", 0.15)],
    "Vietnam":          [("Central Coast", 0.45), ("Red River Delta", 0.30), ("Mekong Delta", 0.25)],
    "Jamaica":          [("Surrey County", 0.40), ("Middlesex County", 0.35), ("Cornwall County", 0.25)],
    "Mozambique":       [("Sofala Province", 0.40), ("Zambezia Province", 0.35), ("Nampula Province", 0.25)],
    "Pacific Islands":  [("Fiji", 0.50), ("Vanuatu", 0.30), ("Tonga", 0.20)],
    "Antigua and Barbuda":              [("Saint John Parish", 0.55), ("Saint George Parish", 0.45)],
    "Saint Lucia":                      [("Castries", 0.50), ("Vieux Fort", 0.30), ("Gros Islet", 0.20)],
    "Saint Vincent and the Grenadines": [("Kingstown", 0.60), ("The Grenadines", 0.40)],
    # Same pattern as Pacific Islands above — the region's "admin-1" units
    # are its constituent member countries (shares matching each member's
    # share of the region's own People at Risk total: 14K/33K, 10K/33K,
    # 9K/33K), not sub-national divisions within a single country.
    "ECA":              [("Saint Lucia", 0.42), ("Saint Vincent and the Grenadines", 0.30), ("Antigua and Barbuda", 0.28)],
}


def _admin1_table(country, base_stats, pin_pct, breakdown):
    # Same tinted-header/rounded-card/zebra-stripe treatment as
    # _simple_breakdown_table. People/Children get their own side-by-side At
    # Risk/In Need sub-columns (colSpan=2), same pattern as the main table's
    # header — there's enough width budget here to not need to stack them;
    # facility metrics (no In Need concept) keep a single column.
    regions = _ADMIN1_REGIONS.get(country, [])
    metric_keys = list(_DEFAULT_STATS.keys())
    _PIN_KEY = {"People at Risk": "people", "Children at Risk": "children"}
    # People/Children need two 75px sub-columns (150 total) for their At
    # Risk/In Need pair; Schools/Health Centers/Shelters/WASH are single
    # plain numbers and need far less — equal table-layout:fixed
    # distribution was squeezing all six into the same width, which
    # overlapped headers and clipped the combined-format cells.
    _COL_WIDTHS = {"People at Risk": "150px", "Children at Risk": "150px", "Schools at Risk": "70px",
                    "Health Centers at Risk": "95px", "Shelters at Risk": "70px", "WASH Facilities at Risk": "80px"}
    # No whiteSpace:nowrap — table-layout:fixed below means a too-narrow
    # column can't grow the table to fit "HEALTH CENTERS" on one line; it
    # needs to be free to wrap instead.
    th_style = {"textAlign": "right", "padding": "8px 10px", "borderBottom": "1px solid #dde6ec",
                "fontSize": "9.5px", "color": "#57707e",
                "background": "#f6f9fb", "textTransform": "uppercase", "letterSpacing": "0.3px"}
    sub_th_style = {**th_style, "textAlign": "center", "fontWeight": 500, "fontSize": "9px",
                     "padding": "5px 8px", "textTransform": "none", "letterSpacing": "normal"}
    td_base = {"padding": "7px 10px", "fontSize": "11px", "textAlign": "right"}
    td_center = {**td_base, "textAlign": "center"}

    def _row_style(i):
        return {"background": "#fafcfd" if i % 2 else "#ffffff", "borderBottom": "1px solid #f1f4f6"}

    def _cells(key, region_share, td_style):
        base_val = round(_parse_stat_number(base_stats.get(key, "0")) * region_share)
        pin_key = _PIN_KEY.get(key)
        if pin_key is None:
            return [html.Td([
                html.Div(_format_stat_number(base_val), style={"fontFamily": "monospace"}),
                _hazard_split_line(base_val, breakdown, font_size="9px"),
            ], style={**td_style, "textAlign": "center"})]
        in_need = round(base_val * pin_pct[pin_key] / 100)
        # Side-by-side At Risk/In Need cells, same hazard-split line as the
        # main table under each number, via the shared _hazard_split_line
        # helper so both tables stay in sync.
        return [
            html.Td([
                html.Div(_format_stat_number(base_val), style={"fontFamily": "monospace", "fontWeight": 700}),
                _hazard_split_line(base_val, breakdown, font_size="9px"),
            ], style={**td_style, "textAlign": "center"}),
            html.Td([
                # Plain black, same as At Risk — now that At Risk/In Need
                # are their own side-by-side, labeled sub-columns, the extra
                # PIN_COLOR (purple) text tint was redundant (the header
                # label already says which is which) and mismatched the
                # main table's own side-by-side columns, which don't tint
                # In Need either.
                html.Div(_format_stat_number(in_need), style={"fontFamily": "monospace", "fontWeight": 700}),
                _hazard_split_line(in_need, breakdown, font_size="9px"),
            ], style={**td_style, "textAlign": "center"}),
        ]

    # table-layout:fixed splits width equally across columns unless the
    # FIRST ROW gives one an explicit width — without this, "Region" got
    # squeezed to the same width as a single number column, which is far
    # too narrow for names like "Eastern Visayas".
    header_rows = [html.Tr(
        [html.Th(_t("Region"), style={**th_style, "textAlign": "left", "width": "140px"}, rowSpan=2)]
        + [html.Th(_t(k.replace(" at Risk", "")),
                    style={**th_style, "textAlign": "center", "width": _COL_WIDTHS[k]},
                    colSpan=2 if k in _PIN_KEY else 1, rowSpan=1 if k in _PIN_KEY else 2)
            for k in metric_keys]
    ), html.Tr(
        [html.Th(_t(lbl), style=sub_th_style) for k in metric_keys if k in _PIN_KEY for lbl in ("At Risk", "In Need")]
    )]
    rows = []
    for i, (region, share) in enumerate(regions):
        rstyle = _row_style(i)
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(region), style={**td_style, "textAlign": "left", "color": "#16232c", "fontWeight": 600})]
        for k in metric_keys:
            cells.extend(_cells(k, share, td_style))
        rows.append(html.Tr(cells))

    # tableLayout:"fixed" is the actual fix for the table overflowing past
    # its container no matter how the wrapping divs were constrained —
    # table-layout:auto (the default) treats width:100% as a mere
    # suggestion and still grows the table past it whenever cell content
    # doesn't naturally fit; fixed layout makes the width a hard constraint,
    # wrapping cell content instead of growing the table.
    # minWidth (140 Region + 150 People + 150 Children + 70 Schools + 95
    # Health Centers + 70 Shelters + 80 WASH = 755) — same reasoning as
    # _simple_breakdown_table: without it, table-layout:fixed would just
    # shrink all columns to fit a narrower container instead of ever
    # needing to scroll.
    table = html.Table([html.Thead(header_rows), html.Tbody(rows)],
                         style={"width": "100%", "minWidth": "755px", "tableLayout": "fixed", "borderCollapse": "collapse"})
    # maxWidth caps this at its own comfortable size (matching the 7 fixed
    # column widths above) regardless of how wide the MAIN table's modal
    # grows for many countries — without this, table width:100% stretched
    # to fill the whole (possibly 2000px) modal, leaving a wall of empty
    # space around a table that only ever needs ~900px.
    # width:"fit-content" — this div's own default width:auto (=100% of its
    # parent) combined with overflow:"hidden" (for the rounded corners) was
    # clipping the table at THIS boundary before the ancestor scrollable
    # div ever got a chance to provide a scrollbar. fit-content makes this
    # div match its child table's actual (min-width-driven) size instead,
    # still capped by maxWidth below.
    return html.Div(table, style={"border": "1px solid #eef2f5", "borderRadius": "10px", "overflow": "hidden",
                                    "width": "fit-content", "maxWidth": "950px"})


def _admin1_section(country, breakdown, expanded=False, date=None, run=None, wind_kt=None):
    # Collapsed by default in the interactive modal — the country-level
    # table above already covers the everyday view, this is finer detail on
    # demand, not something to always show (avoids repeating the same row
    # structure 3-4x per country). expanded=True (used by the standalone
    # print page, which has no click-to-expand interaction a reader could
    # use) renders it already open, and drops the id-based
    # click/pattern-matching entirely so this static page carries no
    # dependency on the interactive modal's own toggle callback.
    #
    # date/run/wind_kt thread through from _impact_breakdown_content (the
    # only caller) so this section's own numbers stay in sync with the main
    # breakdown table's — previously this always resolved against the
    # frozen "active right now" storm/50kt default regardless of what the
    # user had selected, which could silently disagree with the table above it.
    base_stats = _scale_stats_by_hazard(_get_country_stats(country, date, run, wind_kt), breakdown["scale"])
    pin_pct = _get_country_pin_pct(country, date, run, wind_kt)
    head_id = {} if expanded else {"id": {"type": "admin1-head", "country": country}}
    body_id = {} if expanded else {"id": {"type": "admin1-body", "country": country}}
    return html.Div([
        html.Div([
            dmc.Text(_t("Admin Level 1 Breakdown — {country}", country=_t(country)), fw=700, size="11px", c="dimmed", style={"flex": 1}),
        ] + ([] if expanded else [html.Span("▾", style={"fontSize": "10px", "color": "#8ea0ab"})]),
           **head_id,
           style={"display": "flex", "alignItems": "center", "gap": "8px",
                   **({} if expanded else {"cursor": "pointer"})}),
        html.Div([
            _admin1_table(country, base_stats, pin_pct, breakdown),
            # At Risk/In Need no longer need their own color-dot legend —
            # they're now separate, side-by-side, HEADER-labeled columns
            # (same as the main table), not a single colored/uncolored pair
            # inside one cell, so there's no color coding left to explain.
            # The hazard-split legend below is the only one still needed.
            _hazard_split_legend(breakdown),
        ], **body_id,
           # width:"100%" — same fix as the main breakdown table
           # (_impact_breakdown_content's main_row): overflowX:auto alone
           # does nothing on a div whose width is the default "auto", since
           # the div just grows to match its (wider) table child instead of
           # clipping/scrolling it. overflowY:"visible" (explicit, not left
           # unset) — per the CSS overflow spec, setting overflow-x to
           # anything other than visible/clip while leaving overflow-y
           # unset computes overflow-y as "auto" too, which was adding a
           # redundant vertical scrollbar to each admin-1 table on top of
           # the modal's own single outer scroll.
           style={"marginTop": "10px", "display": "block" if expanded else "none", "width": "100%", "maxWidth": "100%",
                   "overflowX": "auto", "overflowY": "visible"}),
    # marginTop/paddingTop (was 14/14) — more breathing room between the
    # main breakdown table and the first Admin Level 1 section (and between
    # each subsequent one), so this reads as its own distinct section
    # rather than just another row butted up against the table above.
    ], style={"marginTop": "32px", "paddingTop": "22px", "borderTop": "1px solid #eef2f5"})


def _simple_breakdown_table(cols, breakdown, member="combined", pin_source=None):
    # `cols` is [(label, base_stats), ...]. `pin_source` (a {label: pin_pct}
    # dict) turns on the Country-Analysis extras: paired At Risk/In Need
    # columns + children age-band rows (now including their own In Need
    # share) + a second, colored worst-case NUMBER per cell (tagged with the
    # member, e.g. "#6") instead of a bare percentage — same real-numbers
    # pattern as the compact tiles, not an abstract delta. Soft dividers +
    # tinted header/stripes instead of a plain grid, matching the rest of
    # the page's card-like look rather than a bare default HTML table.
    # No whiteSpace:nowrap — table-layout:fixed below means a too-narrow
    # column can't grow the table to fit e.g. "WASH FACILITIES" on one
    # line; it needs to be free to wrap instead.
    th_style = {"textAlign": "left", "padding": "9px 12px", "borderBottom": "1px solid #dde6ec",
                "fontSize": "10.5px", "color": "#57707e",
                "background": "#f6f9fb", "textTransform": "uppercase", "letterSpacing": "0.3px"}
    sub_th_style = {**th_style, "textAlign": "center", "fontWeight": 500, "fontSize": "9.5px",
                     "padding": "5px 12px", "textTransform": "none", "letterSpacing": "normal"}
    td_base = {"padding": "8px 12px", "fontSize": "12px"}
    td_dash_style = {**td_base, "color": "#c3ccd2", "textAlign": "center"}

    factor = _MEMBER_SCENARIO_FACTOR.get(member, 1.0)
    has_compare = factor != 1.0
    member_tag = _member_short_label(member) if has_compare else ""

    def _row_style(i):
        return {"background": "#fafcfd" if i % 2 else "#ffffff", "borderBottom": "1px solid #f1f4f6"}

    # A distinct divider between each country's own pair of columns — plain
    # adjacent cells with only the regular 1px grid lines made it hard to
    # tell at a glance where e.g. Vietnam's columns end and Combined's
    # begin, especially scrolled mid-table. Skipped for the very first
    # column (already bordered by the Metric column) and for is_combined
    # (which gets its own blue-tinted divider instead, see below).
    _GROUP_DIVIDER = {"borderLeft": "2px solid #dde3ea"}
    # Combined used to color its own NUMBERS blue (reusing RIVER) to read as
    # a distinct "total" column — but RIVER is also the Flood color in the
    # hazard-split line under every value, so a blue base number next to a
    # blue Flood-share number read as ambiguous ("is this blue number the
    # combined total or the flood share?"). A light RIVER-tinted BACKGROUND
    # across the whole column keeps the same color scheme/identity without
    # touching any text color, so numbers stay legible and unambiguous.
    _COMBINED_DIVIDER = {"borderLeft": f"2px solid {RIVER}55", "background": f"{RIVER}12"}

    def _group_style(idx, is_combined):
        if idx == 0:
            return {"background": f"{RIVER}12"} if is_combined else {}
        return _COMBINED_DIVIDER if is_combined else _GROUP_DIVIDER

    def _combined_bg(is_combined):
        # Background only, no border — for cells (like the "In Need"
        # sub-header) that sit to the right of the group's leading edge and
        # so shouldn't repeat the divider, but still need the same tint so
        # the whole Combined column reads as one continuous block.
        return {"background": f"{RIVER}12"} if is_combined else {}

    def _value_td(base_n, compare_n, td_style):
        # Centered — directly under the "At Risk"/"In Need" header labels
        # (already centered via sub_th_style) instead of hugging the left
        # edge, which read as misaligned once the columns were narrowed.
        children = [
            html.Div(_format_stat_number(base_n), style={"fontWeight": 700, "fontFamily": "monospace"}),
            _hazard_split_line(base_n, breakdown),
        ]
        if compare_n is not None:
            children.append(html.Div(f"{_format_stat_number(compare_n)} {member_tag}",
                                       style={"fontSize": "10px", "fontWeight": 700, "color": "#d94f3c", "marginTop": "2px"}))
        return html.Td(children, style={**td_style, "textAlign": "center"})

    def _scaled_num(base_val):
        return round(_parse_stat_number(base_val) * factor)

    has_pin = pin_source is not None
    # The Combined column (when present) is always the LAST one in `cols` —
    # see _impact_breakdown_content, which appends it after every real
    # country. combined_idx is None (no special styling) for the Global
    # table or a single/plain country selection with no Combined column.
    combined_idx = len(cols) - 1 if len(cols) > 1 and str(cols[-1][0]).startswith(_t("Combined")) else None
    # table-layout:fixed splits width equally across columns unless the
    # FIRST ROW gives one an explicit width — without this, "Metric" got
    # squeezed to the same width as a single At Risk/In Need sub-column,
    # far too narrow for labels like "Age 5–14 (School-age)".
    # Explicit width on each colSpan=2 header (not just Metric) — under
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
    # Track each country's total Children-in-Need (base + compare) so the
    # age-band rows below can share it proportionally.
    children_in_need_base, children_in_need_compare = {}, {}

    for row_label, risk_key, pin_key in (("People", "People at Risk", "people"),
                                           ("Children (total)", "Children at Risk", "children")):
        rstyle = _row_style(row_i); row_i += 1
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(row_label), style={**td_style, "color": "#16232c", "fontWeight": 700})]
        for idx, (c, base_stats) in enumerate(cols):
            is_combined = idx == combined_idx
            group_style = {**td_style, **_group_style(idx, is_combined)}
            base_n = _parse_stat_number(base_stats.get(risk_key, "0"))
            compare_n = _scaled_num(base_stats.get(risk_key, "0")) if has_compare else None
            cells.append(_value_td(base_n, compare_n, group_style))
            if has_pin:
                base_pct = pin_source.get(c, _DEFAULT_PIN_PCT)[pin_key]
                base_in_need = round(base_n * base_pct / 100)
                compare_in_need = None
                if has_compare:
                    compare_pct = _scaled_pin_pct(pin_source.get(c, _DEFAULT_PIN_PCT), member)[pin_key]
                    compare_in_need = round(compare_n * compare_pct / 100)
                if pin_key == "children":
                    children_in_need_base[c] = base_in_need
                    children_in_need_compare[c] = compare_in_need
                cells.append(_value_td(base_in_need, compare_in_need,
                                         {**td_style, **_group_style(idx, is_combined)}))
        rows.append(html.Tr(cells))

    # Children age-band breakdown — At Risk AND In Need, both a proportional
    # share of the Children (total) row above.
    for age_label, share in _CHILD_AGE_SPLIT:
        rstyle = _row_style(row_i); row_i += 1
        td_style = {**td_base, **rstyle}
        cells = [html.Td(_t(age_label), style={**td_style, "color": "#8ea0ab", "paddingLeft": "24px"})]
        for idx, (c, base_stats) in enumerate(cols):
            is_combined = idx == combined_idx
            group_style = {**td_style, **_group_style(idx, is_combined)}
            base_children = _parse_stat_number(base_stats.get("Children at Risk", "0"))
            base_age = round(base_children * share)
            compare_age = round(_scaled_num(base_stats.get("Children at Risk", "0")) * share) if has_compare else None
            cells.append(_value_td(base_age, compare_age, group_style))
            if has_pin:
                base_age_need = round(children_in_need_base.get(c, 0) * share)
                compare_age_need = (round(children_in_need_compare.get(c, 0) * share)
                                     if has_compare and children_in_need_compare.get(c) is not None else None)
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
            compare_n = _scaled_num(base_stats.get(key, "0")) if has_compare else None
            cells.append(_value_td(base_n, compare_n, group_style))
            if has_pin:
                cells.append(html.Td("—", style={**td_dash_style, **rstyle, **_group_style(idx, is_combined)}))
        rows.append(html.Tr(cells))

    # tableLayout:"fixed" is the actual fix for the table overflowing past
    # its container no matter how the wrapping divs were constrained —
    # table-layout:auto (the default) treats width:100% as a mere
    # suggestion and still grows the table past it whenever cell content
    # doesn't naturally fit; fixed layout makes the width a hard constraint,
    # wrapping cell content instead of growing the table.
    # minWidth is what actually makes horizontal scroll happen for many
    # countries — table-layout:fixed treats the explicit per-column widths
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
    # width:"fit-content" — same fix as _admin1_table's wrapper: this div's
    # default width:auto (100% of its parent) plus overflow:"hidden" (for
    # rounded corners) was clipping the table at THIS boundary before the
    # ancestor's own overflowX:auto ever got a chance to provide a
    # scrollbar. fit-content makes this div match the table's real
    # (min-width-driven) size so the ancestor can detect the real overflow.
    table_wrapper = html.Div(table, style={"border": "1px solid #eef2f5", "borderRadius": "10px",
                                             "overflow": "hidden", "width": "fit-content"})
    # Legend for the hazard-split line under every value — without it the
    # orange/blue pair reads as unexplained, same reasoning as the At Risk/
    # In Need legend under the Admin Level 1 breakdown.
    return html.Div([table_wrapper, _hazard_split_legend(breakdown)])


# Stacked (not side by side) in the Impact Summary panel's ~260px-wide
# content column — each chart gets the panel's full width instead of half
# of it, so it can afford to be taller too without looking squeezed.
_ARC_CHART_HEIGHT = 190


def _pin_arc_charts_block_from(label, base_stats, pin_pct, totals, member, label_is_country=True, show_label=True):
    # Same 270° 3-ring polar-bar gauge as the real dashboard's "PEOPLE IN
    # NEED" panel (layouts/panels.py:871-889) — see _make_arc_chart above.
    # Takes already-resolved stats/pin_pct/totals rather than a country
    # name, so the same renderer serves both a single country
    # (_pin_arc_charts_block) and a "Combined Total" aggregate
    # (_pin_arc_charts_block_combined) without duplicating this logic.
    # show_label=False is for embedding this directly under a _stat_grid
    # (the Impact Summary panel's own In Need visualization) that already
    # shows the country/"Combined" label itself — repeating it here would
    # just be redundant right below it.
    # The chart RINGS always reflect the base (Probabilistic) numbers — the
    # primary view stays stable regardless of the worst-case factor picked;
    # a second, colored number is added below (member-tagged, e.g. "#8")
    # exactly like the stat tiles above already do, rather than replacing
    # the primary number/rings outright.
    exposed_pop = _parse_stat_number(base_stats["People at Risk"])
    exposed_children = _parse_stat_number(base_stats["Children at Risk"])
    in_need_pop = round(exposed_pop * pin_pct["people"] / 100)
    in_need_children = round(exposed_children * pin_pct["children"] / 100)

    compare_pop = compare_children = None
    if member:
        scaled = _scaled_stats(base_stats, member)
        scaled_pin = _scaled_pin_pct(pin_pct, member)
        compare_pop = round(_parse_stat_number(scaled["People at Risk"]) * scaled_pin["people"] / 100)
        compare_children = round(_parse_stat_number(scaled["Children at Risk"]) * scaled_pin["children"] / 100)
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
    # Stacked, not side by side — the Impact Summary panel is only ~260px
    # of usable width, which squeezed two charts down to illegible ~120px
    # each; full-width stacked charts can actually be read. Children first,
    # People second, per explicit request (deliberately the reverse of the
    # People/Children tile order above them).
    children.append(html.Div([
        _chart_col("Children In Need", _format_stat_number(in_need_children), compare_children, fig_children),
        _chart_col("People In Need", _format_stat_number(in_need_pop), compare_pop, fig_people),
    ], style={"display": "flex", "flexDirection": "column", "gap": "14px"}))
    # In Need is only ever computed from wind's own MAT-table columns
    # (E_people_in_need/E_children_in_need, confirmed live -- gust/river/rain
    # have no equivalent) -- this note stays visible unconditionally rather
    # than only when other hazards are toggled on, since these particular
    # numbers never reflect anything but Tropical Cyclone regardless of what
    # else is checked.
    children.append(dmc.Text(_t("In Need currently only reflects Tropical Cyclone (wind) impact."),
                               size="10px", c="dimmed", fs="italic", mt=8))
    return html.Div(children, style={"borderTop": "1px solid #eef2f5", "paddingTop": "12px", "marginTop": "12px"})


def _pin_arc_charts_block(country, member, show_label=True, scale=1.0, date=None, run=None, wind_kt=None, hz=None):
    # `scale`/_hazard_breakdown no longer applies here — _get_country_stats
    # already returns the real combined-across-active-hazards total when a
    # real `hz` is passed in (see _fetch_real_combined_tile_totals). Never
    # applies to _get_country_totals either way — that's the outer ring's
    # denominator (how many people COULD be there at all), which doesn't
    # depend on which hazards are currently toggled on.
    return _pin_arc_charts_block_from(
        country, _get_country_stats(country, date, run, wind_kt, hz=hz),
        _get_country_pin_pct(country, date, run, wind_kt, hz=hz),
        _get_country_totals(country), member, show_label=show_label,
    )


def _pin_arc_charts_block_combined(countries, member, show_label=True, scale=1.0, date=None, run=None, wind_kt=None, hz=None):
    combined_base = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
    combined_pin = {
        "people": round(_combined_in_need_total(countries, "People at Risk", "people", date=date, run=run, wind_kt=wind_kt, hz=hz)
                         / max(1, _parse_stat_number(combined_base["People at Risk"])) * 100),
        "children": round(_combined_in_need_total(countries, "Children at Risk", "children", date=date, run=run, wind_kt=wind_kt, hz=hz)
                            / max(1, _parse_stat_number(combined_base["Children at Risk"])) * 100),
    }
    combined_totals = {"population": 0, "children": 0}
    for c in countries:
        t = _get_country_totals(c)
        combined_totals["population"] += t["population"]
        combined_totals["children"] += t["children"]
    return _pin_arc_charts_block_from(_t("Combined — {n} countries", n=len(countries)),
                                        combined_base, combined_pin,
                                        combined_totals, member, label_is_country=False, show_label=show_label)


def _breakdown_modal_width(countries):
    """Explicit pixel width for the Full Impact Breakdown modal, computed
    from how many columns _simple_breakdown_table is actually about to
    render — a real number Mantine can't misinterpret the way it apparently
    did with fit-content/vw values. Global has one plain column (no At
    Risk/In Need split); Country Analysis columns are twice as wide (paired
    At Risk + In Need sub-columns), plus one extra "Combined" column once
    there's more than one country.
    """
    countries = countries or []
    if not countries:
        return f"{max(480, min(170 + 140 + 120, 2000))}px"

    num_cols = len(countries) + (1 if len(countries) > 1 else 0)
    # 190px per data column — narrowed from 260 (the At Risk/In Need
    # sub-columns only ever hold short numbers like "640K", centering them
    # made the extra width just look like empty padding either side).
    # Matches the explicit width set on each colSpan=2 header in
    # _simple_breakdown_table.
    main_width = 170 + num_cols * 190 + 120

    # Whenever there's at least one country, _admin1_table also renders —
    # one PER selected country, but each is its own FIXED 7-column table
    # (Region 140 + People/Children 115 each + Schools/Shelters 70 each +
    # Health Centers 95 + WASH 80 = 685px raw, _admin1_table's own
    # _COL_WIDTHS), regardless of how many countries are selected overall.
    # That table kept getting cut off even after matching the raw column
    # widths exactly — real rendered width runs measurably wider than the
    # sum of declared column widths (cell padding/borders/table border adds
    # up across 7 columns), so this floor is deliberately padded well past
    # the raw 685px sum rather than trying to match it exactly again.
    admin1_width = 1150
    width = max(main_width, admin1_width)
    # No viewport-relative cap (maxWidth:95vw was tried and removed — on a
    # non-maximized browser window it clamped the modal BELOW what the
    # admin1 table actually needs, which is exactly what caused it to look
    # cut off even though the main table above it fit fine). This value is
    # already capped at 2000px on its own; overflowX:auto (on the modal
    # body itself now, not just the table's own wrapper — see
    # _MODAL_PANEL_STYLES) is the fallback if a screen genuinely can't
    # show the full width.
    return f"{max(480, min(width, 2000))}px"


def _impact_breakdown_content(countries, influencing_factor, expand_admin1=False,
                                 wind_on=True, gust_on=False, river_on=True, rain_on=True, surge_on=True,
                                 wind_idx=None, gust_idx=None, river_idx=None, rain_idx=None, surge_idx=None, rain_window=None,
                                 date=None, run=None):
    # Global (no countries selected): exactly the original simple table —
    # no In Need rows, no worst-case comparison, no arc charts. Those are
    # all Country-Analysis-only, per the user's own scoping.
    #
    # wind_on/river_on/rain_on/surge_on mirror the sidebar's own hazard
    # checkboxes (ms-wind-on/ms-river-on/ms-rain-on/ms-surge-on; Gust
    # excluded, see _hazard_breakdown) — this breakdown is scoped to
    # whichever hazards are actually toggled on the map, not always the
    # full mock total regardless of what's active. _active_hazards_indicator
    # makes that scoping visible at the top instead of silently changing
    # numbers with no explanation.
    #
    # date/run (topbar-date/topbar-time's own values, threaded through from
    # both _update_impact_breakdown and the standalone print page) make the
    # country/admin1 numbers below REACTIVE to the selected forecast cycle —
    # see _resolve_storm_for_country's own docstring for why. wind_idx is
    # ALSO now resolved (via _resolve_wind_kt) into the real wind-severity
    # threshold used to fetch those same numbers, not just fed to the
    # threshold-preview widget as before.
    breakdown = _hazard_breakdown(wind_on, river_on, rain_on, surge_on)
    hazards_indicator = _active_hazards_indicator(breakdown)
    hazard_idx = {"Sustained Wind": wind_idx, "River Flooding": river_idx,
                   "Rainfall": rain_idx, "Storm Surge": surge_idx}
    wind_kt = _resolve_wind_kt(wind_idx)
    # Real per-hazard combined total (Wind/Gust/River/Rain — see _build_hz/
    # _fetch_real_combined_tile_totals) replaces the old illustrative
    # percentage-scaled wind-only total below; `breakdown`/`scale` still
    # exists purely for the hazard-family indicator + TC-only/Both/Flood-only
    # split VISUALIZATION (_hazard_split_line via _simple_breakdown_table),
    # which stays illustrative for now — see this session's own review notes.
    hz = _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx, gust_idx, river_idx, rain_idx, rain_window)
    countries = countries or []
    member = _WORST_MEMBER_BY_FACTOR.get(influencing_factor or "none")  # None -> no scaling, shows Probabilistic only
    if not countries:
        global_stats = _scale_stats_by_hazard(_DEFAULT_STATS, breakdown["scale"])
        table = _simple_breakdown_table([("Global", global_stats)], breakdown)
        threshold_preview = _hazard_threshold_preview(breakdown, hazard_idx, _parse_stat_number(global_stats["People at Risk"]),
                                                         rain_window=rain_window, expanded=expand_admin1)
        return html.Div([hazards_indicator, threshold_preview, table], style={"marginTop": "20px"})

    # Unlike the compact panel (which only has room for one view at a time,
    # toggled via impact-aggregation-toggle), the modal has the space to
    # show everything: every selected country's own column, PLUS one extra
    # "Combined" column at the end when there's more than one — not an
    # either/or choice here.
    #
    # No arc charts in here anymore — a table-plus-350px-chart-column flex
    # layout was exactly what made this modal impossible to size sensibly
    # (too narrow for the table once there were several country + Combined
    # columns, too wide for a single country otherwise). The circular charts
    # now live in the Impact Summary panel instead (_update_impact_summary),
    # replacing its plain In Need number tiles; this modal is just the table
    # (plus Admin Level 1 below), which sizes far more predictably on its own.
    cols = [(c, _get_country_stats(c, date, run, wind_kt, hz=hz)) for c in countries]
    pin_source = {c: _get_country_pin_pct(c, date, run, wind_kt, hz=hz) for c in countries}

    if len(countries) > 1:
        combined_label = _t("Combined — {n} countries", n=len(countries))
        combined_stats = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        combined_pin = {
            "people": round(_combined_in_need_total(countries, "People at Risk", "people", date=date, run=run, wind_kt=wind_kt, hz=hz)
                             / max(1, _parse_stat_number(combined_stats["People at Risk"])) * 100),
            "children": round(_combined_in_need_total(countries, "Children at Risk", "children", date=date, run=run, wind_kt=wind_kt, hz=hz)
                                / max(1, _parse_stat_number(combined_stats["Children at Risk"])) * 100),
        }
        cols = cols + [(combined_label, combined_stats)]
        pin_source = {**pin_source, combined_label: combined_pin}

    table = _simple_breakdown_table(cols, breakdown, member=member, pin_source=pin_source)
    # width:"100%" (not the default "auto") is what actually makes
    # overflowX:auto do anything here — a div with default width:auto just
    # grows to match the table's own natural (wider) content size instead
    # of clipping/scrolling it, which is exactly why the table kept
    # visibly overflowing past the modal's edge no matter how the modal
    # itself was sized. Giving this div a DEFINITE width is what makes the
    # overflow rule apply at all.
    main_row = html.Div(table, style={"marginTop": "20px", "width": "100%", "maxWidth": "100%",
                                        "overflowX": "auto", "overflowY": "visible"})
    # Admin Level 1 breakdown stays per-country only — admin-1 regions
    # inherently belong to one specific country, so a "Combined" version of
    # this wouldn't mean anything. No hazard-type split here either — that
    # lives inline in the main table's own cells now (see _simple_breakdown_
    # table's _value_td), not as a separate section anywhere.
    admin1_sections = html.Div([_admin1_section(c, breakdown, expanded=expand_admin1, date=date, run=run, wind_kt=wind_kt) for c in countries])
    # cols[-1] is always the "representative" scope — the appended Combined
    # column for 2+ countries, or the single selected country otherwise.
    threshold_preview = _hazard_threshold_preview(breakdown, hazard_idx, _parse_stat_number(cols[-1][1]["People at Risk"]),
                                                     rain_window=rain_window, expanded=expand_admin1)
    return html.Div([hazards_indicator, threshold_preview, main_row, admin1_sections])


def _resolve_stat_value(metric, scope, countries=None, scale=1.0, date=None, run=None, wind_kt=None, hz=None):
    # Recomputes the exact Probabilistic number shown on the clicked
    # card — server-side, from scope+metric — rather than smuggling the
    # display value into the id itself. Always the base (unscaled-by-
    # worst-case-member) value: the click explains the primary number, not
    # whichever worst-case comparison happens to be showing alongside it.
    # scope=="combined" has no single-country entry to look up — its numbers
    # only exist as the sum of `countries` (the current selection, passed in
    # by the caller), same math as _update_impact_summary's own "Combined
    # Total" branch.
    #
    # `hz` (real multi-hazard state, see _build_hz) makes this the real
    # combined-across-active-hazards total — no more `scale` post-hoc
    # multiplication (kept as a param only because Global scope has no real
    # per-tile data to combine at all, so it still falls back to scaling
    # _DEFAULT_STATS illustratively).
    #
    # date/run/wind_kt (threaded through from _open_hazard_contribution)
    # keep this in sync with whatever the tile/panel it was clicked from is
    # ALSO currently showing — previously this always resolved against the
    # frozen "active right now" storm/50kt default regardless of the
    # selected topbar date/run or wind-severity slider.
    if scope == "combined" and countries:
        stats = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        pin_pct = {
            "people": round(_combined_in_need_total(countries, "People at Risk", "people", date=date, run=run, wind_kt=wind_kt, hz=hz)
                             / max(1, _parse_stat_number(stats["People at Risk"])) * 100),
            "children": round(_combined_in_need_total(countries, "Children at Risk", "children", date=date, run=run, wind_kt=wind_kt, hz=hz)
                                / max(1, _parse_stat_number(stats["Children at Risk"])) * 100),
        }
    else:
        stats = _DEFAULT_STATS if scope == "global" else _get_country_stats(scope, date, run, wind_kt, hz=hz)
        pin_pct = _DEFAULT_PIN_PCT if scope == "global" else _get_country_pin_pct(scope, date, run, wind_kt, hz=hz)
    if scope == "global":
        stats = _scale_stats_by_hazard(stats, scale)
    if metric == "People in Need":
        return _format_stat_number(round(_parse_stat_number(stats["People at Risk"]) * pin_pct["people"] / 100))
    if metric == "Children in Need":
        return _format_stat_number(round(_parse_stat_number(stats["Children at Risk"]) * pin_pct["children"] / 100))
    return stats.get(metric, "—")


# Illustrative — what fraction of the SMALLER hazard family's footprint
# also falls within the other family's footprint (e.g. coastal households
# hit by both storm-force wind and the same storm's flooding). A real
# implementation would derive this from actual per-pixel/facility overlap
# between the two hazards' exposure geometries, not a flat constant.
_HAZARD_OVERLAP_FRAC = 0.25


def _hazard_overlap_bar(total, breakdown):
    # Segmented bar (Tropical Cyclone only / Both / Flood only), not a
    # separate "Overlap: X%" line floating apart from the two hazard
    # numbers — this way the bar's own width still adds up to the visible
    # TC/Flood union, instead of implying TC and Flood are simply additive
    # to separate people. The "Both" segment is striped (not a flat third
    # color) so it visibly reads as "shared", not a third distinct hazard
    # family. Same precomputed shares (from _hazard_breakdown, based on
    # exactly which hazards are toggled on) the table's own
    # _hazard_split_line uses — one source of truth for both. Only ever
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
            dmc.Text(_format_stat_number(round(total * n / 100)), size="10px", fw=700, ff="monospace"),
        ], gap=5, wrap="nowrap")

    legend = dmc.Group([
        _legend_item("Tropical Cyclone only", tc_only_pct, False, WIND),
        _legend_item("Both", both_pct, True, None),
        _legend_item("Flood only", flood_only_pct, False, RIVER),
    ], gap=14, mt=8, wrap="wrap")

    return html.Div([bar, legend])


# Illustrative — within a multi-member family (Flood: River Flooding/
# Rainfall/Storm Surge), what fraction of the family's own share is people
# facing 2+ of those members at once, not just one. Distinct from
# _HAZARD_OVERLAP_FRAC (which is about TC-vs-Flood overlap, a different
# relationship) — a real implementation would derive both from actual
# exposure-geometry overlap, not flat constants.
_HAZARD_MULTI_FRAC = 0.20
# Of that "2+ at once" share, how much is ALL of the family's members at
# once (rarer) vs. any 2 of them (more common) — e.g. facing river
# flooding, rainfall AND storm surge simultaneously vs. just two of the
# three.
_HAZARD_TRIPLE_FRAC = 0.3


def _hazard_contribution_content(value, breakdown, hazard_idx=None, rain_window=None, is_global=False):
    # Grouped under Tropical Cyclone/Flood (icon + bold, own subtotal) with
    # each family's actual hazards indented underneath (icon + lighter) —
    # a flat list of 4 colored dots didn't make clear that Sustained Wind
    # and Gust are the SAME family, or give any per-hazard icon at all.
    #
    # `breakdown` (from _hazard_breakdown) reflects EXACTLY which individual
    # hazard checkboxes are on. A family with nothing toggled on is dropped
    # from this popup entirely (its group row + members), not shown at 0%,
    # since there's nothing being modeled for it right now. Within Flood,
    # only the individually-checked members (River Flooding/Rainfall/Storm
    # Surge) appear — toggling on just Rainfall shows only Rainfall, not
    # all three as if the whole family were active. When only one family
    # is active there's no TC/Flood overlap possible either, so the top
    # overlap bar/caption only appears when BOTH are active.
    tc_active, flood_active = breakdown["tc_active"], breakdown["flood_active"]
    total = _parse_stat_number(value)
    # Global scope now always forces every hazard "on" for this popup's own
    # breakdown (_open_hazard_contribution, matching _update_impact_
    # summary's Global branch, which is independent of the sidebar
    # checkboxes) — so tc_active/flood_active are effectively always True
    # here and "toggle a hazard" below never fires for Global. A genuinely
    # zero Global total means no INITIALIZED country is impacted right now,
    # which is a real but different fact from "nothing is toggled" and
    # needs its own message so it isn't misread as "there's no real
    # impact anywhere" — some potentially-affected countries may simply not
    # be in the database yet.
    if is_global and total == 0:
        return html.Div(dmc.Text(
            _t("None of the initialized countries are currently impacted. This does not mean "
                "there is no real impact — potentially affected countries may not yet be in the database."),
            size="sm", c="dimmed", fs="italic"))
    if not tc_active and not flood_active:
        return html.Div(dmc.Text(_t("None — toggle a hazard on the map to see impact numbers."),
                                    size="sm", c="dimmed", fs="italic"))
    by_name = {name: (color, pct, icon) for name, color, pct, icon in _HAZARD_CONTRIBUTION}
    hazard_idx = hazard_idx or {}

    def _hazard_row(name, color, pct, icon, indent=False, mb=None):
        # mb defaults to "there's a curve chart right below this row inside
        # the same card" (6/10px breathing room before it). Pass mb=0
        # explicitly when this row is the ONLY thing in its card (the
        # overlap rows below never have a curve) — otherwise that default
        # bottom margin adds to the card's own bottom padding with nothing
        # above to balance it, so the row visually sits closer to the
        # card's top edge than its bottom one instead of centered.
        if mb is None:
            mb = 6 if indent else 10
        return dmc.Group([
            DashIconify(icon=icon, width=15 if not indent else 13, color=color),
            dmc.Text(_t(name), size="xs" if not indent else "11px", fw=700 if not indent else 400,
                      c="dark" if not indent else "dimmed", style={"flex": 1, "minWidth": 0}),
            # w (was 36/56) + explicit whiteSpace:nowrap — fixed widths sized
            # for a 2-3 digit "%"/short "K" number got visually squished
            # together (barely any gap between them) once real numbers ran
            # bigger (e.g. "273K"), since neither box had room to spare.
            dmc.Text(f"{pct}%", size="xs", c="dimmed", w=42, ta="right", style={"whiteSpace": "nowrap", "flexShrink": 0}),
            dmc.Text(_format_stat_number(round(total * pct / 100)), size="xs", fw=700, ff="monospace",
                      w=72, ta="right", style={"whiteSpace": "nowrap", "flexShrink": 0}),
        ], gap=14, wrap="nowrap", mb=mb)

    def _hazard_card(color, children):
        # Tinted-border card around each individual sub-hazard's own row +
        # curve (and each overlap row) — a flat stack of rows directly
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

    def _hazard_curve_row(name, color, pct):
        # PREVIEW ONLY (see _WIND_TIER_FACTOR's own comment) — this hazard's
        # OWN number (round(total*pct/100)) is today's value at its slider's
        # default position (where its tier-factor is 1.0 by construction), so
        # it's used as-is as the curve's baseline, scaled by each tier's own
        # ratio. None if this hazard has no curve data or no live slider
        # index was passed in (e.g. the print page, which doesn't read
        # slider state at all).
        idx = hazard_idx.get(name)
        curve_data = _HAZARD_CURVE_DATA.get(name)
        if idx is None or curve_data is None:
            return None
        labels, factors = curve_data
        base_n = round(total * pct / 100)
        if name == "Rainfall":
            # Genuinely 2D (window × tier) — its own multi-line chart, see
            # _rain_threshold_chart's own comment. Falls back to the flat
            # single-line chart if no window was passed in (e.g. an older
            # caller), same as any other hazard.
            if rain_window is None:
                values = [base_n * f for f in factors]
                chart = _threshold_curve_chart(labels, values, idx, color)
            else:
                chart = _rain_threshold_grid(labels, base_n, rain_window, idx, color)
        else:
            values = [base_n * f for f in factors]
            chart = _threshold_curve_chart(labels, values, idx, color)
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
        # nothing to overlap with itself — falls back to the old flat row,
        # no bar, no overlap concept. Its own row+curve still gets wrapped
        # in a card for visual consistency with the multi-member case below.
        members = [(name,) + by_name[name] for name in member_names]  # (name, color, pct, icon)
        if len(members) < 2:
            name, color, pct, icon = members[0]
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
        multi_pct = round(_HAZARD_MULTI_FRAC * family_pct)
        remaining_pct = family_pct - multi_pct
        weight_total = sum(m[2] for m in members)
        only_pcts = [round(remaining_pct * m[2] / weight_total) for m in members]
        only_segments = [_segment(p, family_pct, m[1]) for p, m in zip(only_pcts, members)]
        only_cards = []
        for p, m in zip(only_pcts, members):
            m_name, m_color, _m_pct, m_icon = m
            curve = _hazard_curve_row(m_name, m_color, p)
            card_children = [_hazard_row(m_name, m_color, p, m_icon, indent=True, mb=None if curve is not None else 0)]
            if curve is not None:
                card_children.append(curve)
            only_cards.append(_hazard_card(m_color, card_children))

        if len(members) == 2:
            multi_segments = [_segment(multi_pct, family_pct, HAZARD_BOTH_COLOR, pattern=_DOUBLE_OVERLAP_PATTERN)]
            multi_cards = [_hazard_card(HAZARD_BOTH_COLOR,
                                          _hazard_row("Both", HAZARD_BOTH_COLOR, multi_pct, "mdi:vector-intersection", indent=True, mb=0))]
        else:
            # 3+ members (Flood: River Flooding/Rainfall/Storm Surge) —
            # split the shared "2+ at once" pool further into exactly-2
            # vs. all-of-them-at-once, since those read as meaningfully
            # different severities, not one lump "some overlap" bucket.
            # Single diagonal hatch for "any 2" vs. a criss-cross (two
            # crossed diagonal directions) for "all 3" — the pattern itself
            # escalates with how many hazards are stacked, not just the color.
            triple_pct = round(_HAZARD_TRIPLE_FRAC * multi_pct)
            double_pct = multi_pct - triple_pct
            multi_segments = [_segment(double_pct, family_pct, HAZARD_BOTH_COLOR, pattern=_DOUBLE_OVERLAP_PATTERN),
                                _segment(triple_pct, family_pct, HAZARD_TRIPLE_COLOR, pattern=_TRIPLE_OVERLAP_PATTERN)]
            multi_cards = [
                _hazard_card(HAZARD_BOTH_COLOR,
                               _hazard_row("Double overlap (any 2)", HAZARD_BOTH_COLOR, double_pct, "mdi:vector-intersection", indent=True, mb=0)),
                _hazard_card(HAZARD_TRIPLE_COLOR,
                               _hazard_row("Triple overlap (all 3)", HAZARD_TRIPLE_COLOR, triple_pct, "mdi:vector-combine", indent=True, mb=0)),
            ]

        # width:"calc(100% - 18px)" + marginLeft:"18px" — matches the cards'
        # own marginLeft below, so the bar's edges line up with the cards it
        # sits above instead of the (now-removed) per-row indent.
        bar = html.Div(
            only_segments + multi_segments,
            style={"display": "flex", "height": "10px", "borderRadius": "4px", "overflow": "hidden",
                    "width": "calc(100% - 18px)", "marginBottom": "10px", "marginLeft": "18px"},
        )
        return [bar] + only_cards + multi_cards

    both_active = tc_active and flood_active
    blocks = []
    # Iterate only the ACTIVE members of each family (not the family's full
    # membership) — group_pct comes from `breakdown`, which already sums
    # only what's actually checked, not _HAZARD_GROUPS' fixed member list.
    for group_name, group_color, group_icon, active_members, group_pct, active in (
        (_HAZARD_TC_NAME, _HAZARD_TC_COLOR, _HAZARD_TC_ICON, breakdown["active_tc_members"], breakdown["tc_pct"], tc_active),
        (_HAZARD_FLOOD_NAME, _HAZARD_FLOOD_COLOR, _HAZARD_FLOOD_ICON, breakdown["active_flood_members"], breakdown["flood_pct"], flood_active),
    ):
        if not active:
            continue
        blocks.append(_hazard_row(group_name, group_color, group_pct, group_icon))
        blocks.extend(_family_members_block(active_members, group_pct))
        blocks.append(html.Div(style={"height": "16px"}))

    overlap_children = [_hazard_overlap_bar(total, breakdown),
                          dmc.Text(_t("Tropical Cyclone and Flood risk overlap — this isn't two separate groups of people."),
                                    size="10px", c="dimmed", mt=14, mb=26, fs="italic")] if both_active else []

    # Column header labeling what the two right-aligned numbers on every
    # hazard row actually are — without this, "45%" / "173K" reads as two
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

    return html.Div([
        dmc.Text(_t("Total: {value}", value=value), size="sm", fw=700, mt=6, mb=20),
        *overlap_children,
        column_header,
        html.Div(blocks[:-1]),  # drop the trailing spacer
        dmc.Text(_t("Illustrative split — a real implementation would compute this from actual per-hazard overlap."),
                  size="10px", c="dimmed", mt=18, fs="italic"),
    ])


def _panel_controls_row():
    # Both controls on one compact line: the aggregation toggle (short text,
    # not icons — the icon-only version was unclear) and the worst-case
    # factor Select side by side. The aggregation toggle has its OWN
    # wrapper (impact-aggregation-wrapper) so it can be hidden independently
    # when only 1 country is selected, while the row itself (and the factor
    # Select) stays visible with 1+ countries — see _toggle_controls_row/
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
                               wind_idx=None, river_idx=None, rain_idx=None, surge_idx=None, rain_window=None):
    """URL for a standalone, plain/printable version of this exact
    breakdown (pages/map_shell_breakdown_print.py) — NOT a reload of the
    whole interactive dashboard with the modal reopened on top of it (that
    was the first attempt; not something you'd ever want to print or
    hand someone as a link). Same idea as the alert email's own "Open in
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
    if countries:
        query += f"&zoom_countries={','.join(quote(c) for c in countries)}"
    return f"/breakdown-print{query}"


def _impact_panel(initial_countries=None, open_breakdown=False):
    # Always visible (no dismiss button) — the app's real value-add over a
    # pure met-visualization tool like WeatherLab: population/school/health
    # impact, not just where the hazard is. Defaults to a worldwide framing
    # until countries are selected; one stat block per selected country when
    # there's more than one — genuinely side by side, not a forced single pick.
    # Every stat number is clickable → per-hazard contribution popup, in
    # both Global and Country Analysis (the one addition that stays useful
    # everywhere, unlike In Need numbers/arc charts/member comparison). The
    # Full Breakdown button itself is Country-Analysis-only — Global's
    # summary is already the whole (simple) picture, no fuller view needed.
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
            # Title + "open in new tab" link side by side — same pattern as
            # the alert email modal, just built into the title slot here
            # since this Modal (unlike the alert email's) already has real
            # title text of its own.
            title=dmc.Group([
                dmc.Text(_t("Full Impact Breakdown"), fw=700, size="15px", c="#16232c"),
                dmc.Anchor(
                    DashIconify(icon="carbon:launch", width=15), id="breakdown-new-tab-link",
                    href=_breakdown_new_tab_href(initial_countries), target="_blank",
                    style={"display": "flex", "alignItems": "center", "color": "#8ea0ab"},
                ),
            ], gap=8, wrap="nowrap"),
            centered=True, radius="lg", padding="lg", size=_breakdown_modal_width(initial_countries),
            styles=_MODAL_PANEL_STYLES,
            overlayProps={"backgroundOpacity": 0.35, "blur": 3},
            children=html.Div(_impact_breakdown_content(initial_countries, None), id="impact-breakdown-body",
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
    # system — see _PANEL_MAX_HEIGHT's own comment for the full derivation.
    ], id="impact-panel", style={**_PANEL_STYLE, "top": _PANEL_TOP, "right": f"{_UI_MARGIN}px", "width": "300px",
                                  "maxHeight": _PANEL_MAX_HEIGHT, "overflowY": "auto"})


# NOTE: no timeline panel. Removed deliberately — nothing on this map has a
# real per-lead-time picture to scrub through (envelopes/impact tiles are
# aggregated across the whole forecast, confirmed earlier this session), and
# the "Init" stepper duplicated the top bar's own date/time controls. The one
# genuinely real progression (TC_TRACKS position/intensity per lead time, out
# to the real 144h horizon, not 120h) would be a narrower, separate feature —
# a track marker scrubber only, not a panel implying the whole map updates.


# NOTE: a bottom-right color-scale legend was removed here (was a static,
# unwired "Low -> Severe" gradient left over from the dashboard_synthesis_v2
# mockup — never labeled with which hazard/property it applied to, and never
# updated when toggling layers or changing the severity slider). Same
# reasoning as the removed timeline panel: don't keep UI that implies more
# than the mockup actually backs. A real legend belongs once there's an
# actual active hazard+property to describe, mirroring the real app's
# per-hazard MapLibre legend, not a decorative constant.


# Map-adjacent command bar (from dashboard_synthesis_v2.html): quick hazard
# toggle pills + Tiles/Regions view switch, sitting right above the map
# between the two side panels — mirrors the rail checkboxes (same ids), not a
# second source of truth.
_CMD_HAZARDS = [("wind", "Sustained Wind", WIND), ("gust", "Gust", GUST), ("river", "River", RIVER),
                 ("rain", "Rainfall", RAIN), ("surge", "Storm Surge", SURGE)]
_PILL_OFF_STYLE = {"padding": "6px 12px", "borderRadius": "999px", "cursor": "pointer", "whiteSpace": "nowrap",
                    "border": "1px solid #dde6ec", "background": "#f6f9fb", "color": "#57707e"}
_PILL_DISABLED_STYLE = {**_PILL_OFF_STYLE, "cursor": "not-allowed", "pointerEvents": "none", "opacity": 0.5}
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
# right = _UI_MARGIN + impact-panel's own width (300) + _UI_MARGIN — same
# _UI_MARGIN gap to both side panels as everything else in this spacing
# system. top uses the same _PANEL_TOP as both side panels.
_COMMAND_BAR_STYLE = {**_PANEL_STYLE, "top": _PANEL_TOP,
                      "left": f"{_UI_MARGIN + 290 + _UI_MARGIN}px", "right": f"{_UI_MARGIN + 300 + _UI_MARGIN}px",
                      "padding": "9px 14px", "zIndex": 35, "overflowX": "auto"}


def _command_bar():
    # "Tiles vs Regions" is a country-scoped rendering concept (population
    # tiles or admin polygons within one place) — there's no such data at
    # global scale (just storm markers/tracks), so this bar only makes sense
    # in Country Analysis mode. Hidden by default since Global is the
    # starting mode.
    return html.Div(
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
            # tooltip anymore — it fired on every hover over the select
            # (not just when open), which read as an intrusive popup rather
            # than a helpful hint.
            dmc.Select(
                id="ensemble-member-select", value="combined", data=_ensemble_members(),
                size="xs", w=190, searchable=True,
            ),
        ], justify="space-between", wrap="nowrap", align="center"),
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
    # wrapper, not on the dmc.Group's own style — same pattern as _topbar().
    # Putting it directly on dmc.Group left the background looking off
    # (Mantine's own Group styles won by specificity in places), so the
    # background color here is now on an element with nothing else competing
    # for it.
    return html.Div(dmc.Group(
        [
            dmc.Group(
                [
                    dmc.Text(_t("Supported by"), size="xs", c="white", opacity=0.8, style={"marginRight": "10px"}),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/DID-logo-white.png", w=70),
                        href="https://www.unicef.org/digitalimpact/what-we-do/artificial-intelligence-children",
                        target="_blank", style={"marginRight": "16px"},
                    ),
                    _vdivider(color="rgba(255,255,255,0.6)"),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/FDN-UNICEF-logo_white.png", w=120),
                        href="http://frontierdatanetwork.org/", target="_blank", style={"marginLeft": "16px", "marginRight": "16px"},
                    ),
                    html.Div(style={"width": "18px", "height": "1px", "background": "rgba(255,255,255,0.6)",
                                      "marginRight": "16px"}),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/GIGA_lockup_white_horizontal.webp", w=120),
                        href="https://giga.global", target="_blank", style={"marginRight": "20px"},
                    ),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/OoI_logo.png", w=95),
                        href="https://www.unicef.org/innovation/", target="_blank", style={"marginRight": "20px"},
                    ),
                    dmc.Anchor(
                        dmc.Image(src="assets/img/ose_logo_white.png", w=70),
                        href="https://data.unicef.org/", target="_blank",
                    ),
                ],
                align="center", gap="sm",
            ),
            dmc.Group(
                dmc.Anchor(
                    dmc.ActionIcon(DashIconify(icon="carbon:logo-github", width=24), variant="transparent",
                                    style={"color": "#ffffff"}),
                    href="https://github.com/unicef-drp/Ahead-of-the-Storm", target="_blank",
                ),
                align="center",
            ),
        ],
        justify="space-between",
        style={"width": "100%"},
    # 24px horizontal padding — its own value, not _UI_MARGIN (see
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
    # whole gap — real bug found+fixed here: the old left:322/right:332
    # pairing forced the background pill to span the entire gap width
    # regardless of how short the sentence actually was.
    return html.Div(
        dmc.Text(_t(_UN_DISCLAIMER), size="9px", c="#57707e",
                  style={"whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"}),
        style={"position": "absolute", "bottom": _BOTTOM_ROW_OFFSET, "left": "50%", "transform": "translateX(-50%)",
               "width": "fit-content",
               "maxWidth": f"calc(100% - {(_UI_MARGIN + 290 + _UI_MARGIN) + (_UI_MARGIN + 300 + _UI_MARGIN)}px)",
               "textAlign": "center", "zIndex": 25, "pointerEvents": "none",
               "background": "rgba(255,255,255,0.75)", "borderRadius": "6px", "padding": "4px 10px"},
    )


# _basemap_switcher() used to live here as its own standalone function —
# folded into _bottom_left_controls() (near _demo_scenarios_menu(), which
# now shares this same row) so the basemap SegmentedControl and the Demo
# Scenarios icon can sit in one flex row together.


# ---------------------------------------------------------------------------
# Map legend — real, reactive (see the removed-mockup NOTE above this
# function for why a static one was deliberately deleted instead of kept).
# Small pill by default (Google Maps' own weather-legend convention),
# click-to-expand into a full card; anchored bottom-right, BELOW where the
# Global-only Demo Scenarios menu sits (bottom:74) so it never collides with
# it, and below where nothing else sits at all in Country Analysis mode —
# the one corner genuinely free in both modes without conditional styling.
# ---------------------------------------------------------------------------
# bottom:74px (not 20px) — real bug found+fixed here (confirmed live via
# Playwright hit-testing): the fixed, zIndex:1000 _compact_footer spans the
# full viewport width and, despite looking empty at the far right, its own
# invisible Group element still captures clicks there, so anything below
# roughly bottom:60px collides with it. Demo Scenarios moved into
# _bottom_left_controls() (an icon now, next to the basemap switcher)
# specifically so bottom-right is free for this in BOTH modes, not just
# Country Analysis — no more empty-looking gap in Country Analysis mode,
# no more Global-mode collision.
#
# bottom offset from the shared _BOTTOM_ROW_OFFSET spacing system (see
# _PANEL_MAX_HEIGHT's own comment) — impact-panel's maxHeight is derived
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
    "E_num_shelters": "Shelters at Risk", "E_num_wash": "WASH Facilities at Risk",
    "E_num_schools": "Schools at Risk", "E_num_hcs": "Health Centers at Risk",
}

_LEGEND_HAZARD_LABELS = {"wind": "Sustained Wind", "gust": "Gust", "river": "River Flooding", "rain": "Rainfall"}


def _legend_format_value(val, prop_key, palette):
    if val is None:
        return "—"
    if palette.get("fixed_max") == 1.0 or prop_key.endswith("_prob") or prop_key == "probability":
        return f"{val * 100:.0f}%"
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
    # Compact-only — a plain color patch, deliberately with NO embedded
    # text label (unlike _legend_swatch_row, built for the full card's
    # stacked rows). Real bug found+fixed here: reusing _legend_swatch_row
    # for the compact strip duplicated the hazard name (already shown as
    # the strip's own title) right next to it, and that second copy wrapped
    # onto 2 lines in the available width — the compact strip is supposed
    # to stay ONE short row no matter what.
    return html.Div(style={"height": "10px", "borderRadius": "5px", "background": color})


def _legend_raster_info(hazard, tile_config):
    """Info dict for hazard's ('wind'/'gust'/'river'/'rain') MapLibre
    raster color scale, or None if that raster isn't actually visible —
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
    prop_label = _t(_LEGEND_PROP_LABELS.get(prop, prop.replace("_", " ").title()))
    hazard_name = _t(_LEGEND_HAZARD_LABELS[hazard])
    return {
        "title": f"{hazard_name} — {prop_label}",
        # Short version for the compact strip — the full "Hazard —
        # Property" title comfortably fits the full card's own 300px width
        # on its own line, but not squeezed onto the same row as the bar
        # and chevron too.
        "compact_title": hazard_name,
        "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                 "background": f"linear-gradient(to right, {', '.join(colors)})"}),
        "labels": (_legend_format_value(min_v, prop, palette), _legend_format_value(max_v, prop, palette)),
        "caption": None if min_v is not None else "No real data for this exact selection yet.",
    }


def _legend_layer_info(layer, wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, tile_config, is_global):
    """The single shared source of "what does this layer's color mean" for
    BOTH the always-visible compact strip and the full expanded card — one
    definition per layer so the two views can never say something
    different about the same layer. Returns None when `layer` isn't
    actually active/visible right now."""
    if layer == "tracks":
        if not tracks_on:
            return None
        return {
            "title": "Tracks",
            "bar": html.Div([
                _legend_swatch_row("#ff0000", _t("Control member"), shape="line"),
                _legend_swatch_row("#1cabe2", _t("Ensemble member"), shape="line"),
            ]),
            # Compact strip shows ONE representative color, not both rows —
            # the ensemble member color (the vast majority of drawn tracks,
            # ~50 vs. 1-2 control members) — full detail (both colors) is
            # one click away in the full card via "bar" above. A solid bar
            # filling the available width (matching the gradient bars'
            # own height/shape), not a short fixed-width line floating in
            # empty space — same pill-shaped, edge-to-edge treatment
            # Google's own compact weather-layer strip uses.
            "compact_bar": html.Div(style={"height": "10px", "borderRadius": "5px", "background": "#1cabe2"}),
            "labels": None, "caption": None,
        }
    if layer in ("wind", "gust"):
        on = wind_on if layer == "wind" else gust_on
        if not on:
            return None
        if tc_view_as == "raster":
            return _legend_raster_info(layer, tile_config)
        # Envelopes. Real bug found+fixed here: this used to always show
        # the full severity gradient, even in Global mode — but
        # _build_ms_envelope_geojson never has a country to attribute
        # per-member population severity to there (no get_track_impacts
        # call at all without one), so EVERY Global-mode envelope actually
        # renders as style_envelopes' flat low-opacity fallback (WIND/GUST
        # at ~10% fillOpacity), never the gradient. Country Analysis mode
        # genuinely earns the gradient (some members have real severity
        # data, some don't).
        name = _t(_LEGEND_HAZARD_LABELS[layer])
        if is_global:
            color = WIND if layer == "wind" else GUST
            return {
                "title": f"{name} Envelope",
                "compact_title": name,
                "bar": _legend_swatch_row(color, name, shape="square"),
                "compact_bar": _legend_color_swatch(color),
                "labels": None,
                "caption": "Flat fill only — no single country to attribute per-member impact to in Global mode.",
            }
        gradient = ["#FFFF00", "#8B0000"] if layer == "wind" else ["#FFF3BF", "#D9480F"]
        return {
            "title": f"{name} Envelope Severity",
            "compact_title": name,
            "bar": html.Div(style={"height": "10px", "borderRadius": "5px",
                                     "background": f"linear-gradient(to right, {', '.join(gradient)})"}),
            "labels": (_t("Lower impact"), _t("Higher impact")),
            "caption": "Color = that ensemble member's own population impact. Faint fill = no impact data for that member.",
        }
    if layer in ("river", "rain"):
        on = river_on if layer == "river" else rain_on
        return _legend_raster_info(layer, tile_config) if on else None
    return None


def _legend_section(title, children):
    # Generic version for sections that don't participate in the compact
    # strip's "one active layer" concept (Facilities — a set of point
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
    # No labels/caption here on purpose — the whole point of the compact
    # strip (always visible, per user request) is staying ONE short row
    # that never grows taller, not a second copy of the full card. Uses
    # "compact_title"/"compact_bar" when a layer defines a shorter one
    # (real bug found+fixed here: the full title, e.g. "Sustained Wind
    # Envelope", truncated mid-word here, AND _legend_swatch_row's own
    # embedded text label — meant for the full card's stacked rows —
    # duplicated the hazard name a second time right next to it and wrapped
    # onto 2 lines, silently growing this row's height) — falls back to
    # "title"/"bar" for layers short enough to not need a shorter version.
    title = info.get("compact_title", info["title"])
    bar = info.get("compact_bar", info["bar"])
    return dmc.Group([
        dmc.Text(_t(title), size="11px", fw=600, c="#455a64",
                   style={"whiteSpace": "nowrap", "flexShrink": 0}),
        html.Div(bar, style={"flex": 1, "minWidth": "50px"}),
    ], gap=10, wrap="nowrap", align="center", style={"width": "100%"})


def _map_legend():
    # Two-tier, like Google's own weather-layer legend: a compact ALWAYS-
    # VISIBLE strip by default (real bug found+fixed: this used to require
    # a click just to see anything at all, and separately never fired in
    # Global mode — see _update_map_legend's own docstring) showing just
    # the most-recently-toggled-on layer's color bar, one click (chevron)
    # away from the full multi-layer card. Both tiers share the exact same
    # width as the Impact Summary panel (300px, right:16px) directly above
    # them, and the compact strip stays a single short row — no expanding
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
    # strip always expands — whichever was actually clicked decides the new
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
# which ONE of several just changed — this is what lets the compact strip
# show "whichever layer the user just turned on" (Google's own convention)
# instead of an arbitrary fixed priority order. Fires on every hazard/
# tracks checkbox everywhere on the page (both _controls_global and
# _controls_zoom render the same ids, so this works in both modes — unlike
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
        # Initial call — seed with whatever's already checked by default
        # (ms-tracks-on=True out of the box), highest-priority first.
        for layer in _LEGEND_LAYER_ORDER:
            tid = next(k for k, v in id_to_layer.items() if v == layer)
            if checked.get(tid):
                return layer
        return None
    if checked.get(triggered):
        return id_to_layer[triggered]
    # The tracked layer just got switched OFF — fall back to any other
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
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-tracks-on", "checked"),
    Input("tc-view-as", "value"),
    Input("selected-country-store", "data"),
)
def _update_map_legend_compact(last_layer, tile_config, wind_on, gust_on, river_on, rain_on, tracks_on,
                                  tc_view_as, countries):
    tile_config = tile_config or {}
    tc_view_as = tc_view_as or "envelopes"
    is_global = not countries
    args = (wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, tile_config, is_global)
    info = _legend_layer_info(last_layer, *args) if last_layer else None
    if not info:
        # Fallback covers a timing edge case (this callback and
        # _track_last_toggled_layer firing in a different order than
        # expected) by just picking the first genuinely active layer.
        for layer in _LEGEND_LAYER_ORDER:
            info = _legend_layer_info(layer, *args)
            if info:
                break
    if not info:
        return dmc.Text(_t("No layers active"), size="11px", c="dimmed")
    return _legend_compact_from_info(info)


@callback(
    Output("ms-legend-body", "children"),
    Input("ms-tile-config-store", "data"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-tracks-on", "checked"),
    Input("tc-view-as", "value"),
    Input("selected-country-store", "data"),
    Input("ms-facility-visibility-store", "data"),
)
def _update_map_legend(tile_config, wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, countries,
                         facilities_on):
    tile_config = tile_config or {}
    tc_view_as = tc_view_as or "envelopes"
    # ms-facility-visibility-store, NOT the ms-facility-{id}-on checkboxes
    # directly — real bug found+fixed here: those checkboxes only exist in
    # the DOM in Country Analysis mode, and a Dash callback never fires at
    # all while any of its Inputs is missing from the layout (not just
    # "reads as None"). Referencing them directly meant this ENTIRE
    # callback — including the Tracks/envelope/raster sections that have
    # nothing to do with facilities — silently never fired in Global mode
    # (confirmed live: zero ms-legend-body update requests on a fresh
    # Global-mode load, despite ms-tracks-on defaulting to checked=True).
    # This store is always present (see _mirror_facility_visibility).
    facilities_on = facilities_on or {}
    is_global = not countries
    sections = []

    # Storm Category deliberately NOT included — real finding: _CAT_COLORS
    # only colors the "Cat N" badge in the Active Storms side-panel list
    # (_cat_badge/_storm_row); nothing on the MAP itself is colored by
    # category (tracks are colored by ensemble-member type — control vs
    # regular member — not by storm category). A map legend should only
    # explain what's actually drawn on the map.
    for layer in _LEGEND_LAYER_ORDER:
        info = _legend_layer_info(layer, wind_on, gust_on, river_on, rain_on, tracks_on, tc_view_as, tile_config, is_global)
        if info:
            sections.append(_legend_section_from_info(info))

    # Facilities — real bug found+fixed here: this used to show
    # unconditionally, but each facility type only actually renders on the
    # map while its OWN ms-facility-{id}-on checkbox is checked (see
    # _register_ms_facility_layer's clientside fetch, gated on exactly that
    # checkbox — all four default unchecked/off). Only include the ones
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
# precip-rate raster + raw river flood-extent raster — see
# services/tile_server.py's "Global raw precipitation-rate endpoints"/
# "Global raw river endpoints" sections) used to have their own
# small standalone floating panel here, with two dedicated raw-layer-only
# checkboxes. Removed per explicit user direction ("why do we have two new
# checkboxes for this??") — they now
# reuse the EXISTING ms-river-on ("River Flooding")/ms-rain-on ("Rainfall")
# checkboxes in _flood_hazards_family below as their single on/off control
# (see that function's own comment for the full reuse rationale, and
# _build_global_raw_config further down for the config resolution that reads
# those same two checkbox ids as an independent additional consumer,
# alongside a new flood-view-as Mean/Probability toggle also added there).


def _alert_email_modal():
    # Real emails are full standalone HTML documents (own inline styling,
    # embedded map image) — an iframe respects that instead of re-rendering
    # the content as Dash components, and "Open in New Tab" is right there
    # for anyone who'd rather view it outside the modal entirely.
    return dmc.Modal(
        # The real email's own content is ~640px wide (its <table style=
        # "max-width:640px"> wrapper) — "xl" gives it comfortable margin
        # inside the modal instead of squeezing against a narrower "lg".
        id="alert-email-modal", opened=False, size="xl", title=_t("Alert Email"),
        centered=True, radius="lg", padding="lg", styles=_MODAL_PANEL_STYLES,
        overlayProps={"backgroundOpacity": 0.35, "blur": 3},
        children=html.Div([
            dmc.Anchor(_t("Open in new tab ↗"), id="alert-email-new-tab-link", href="", target="_blank",
                        size="xs", style={"display": "block", "marginBottom": "10px"}),
            html.Iframe(id="alert-email-iframe", src="",
                         style={"width": "100%", "height": "70vh", "border": "1px solid #eef2f5",
                                "borderRadius": "8px"}),
        ]),
    )


def _map_stack():
    # Real MapLibre (bottom) + Leaflet (top) dual-layer map — same stack as
    # layouts/panels.py's center_panel, adapted for map-shell's full-bleed
    # shell: the map fills the entire viewport ("100%" here, not panels.py's
    # header-offset "calc(100vh - 147px)") behind the floating panels, rather
    # than sitting in one column of a 2-column grid. Hazard/tile/facility
    # DATA layers are deliberately left as empty placeholders — that's later,
    # separate follow-up work; this is purely the real interactive base map
    # replacing the old synthetic canvas.
    return html.Div(
        [
            # MapLibre canvas — will render tile/admin hazard layers underneath
            # Leaflet once that data is wired; for now just the basemap.
            html.Div(
                id="maplibre-container",
                **{"data-mapbox-token": mapbox_token or ""},
                style={
                    "height": "100%", "width": "100%",
                    "position": "absolute", "top": 0, "left": 0, "zIndex": 0,
                },
            ),
            # Leaflet map — owns zoom/pan/basemap-switching and (later) tracks,
            # envelopes, and facility overlays. opacity=0 tile layers below are
            # transparent; Leaflet shows nothing itself, MapLibre provides the
            # actual basemap tiles. baselayerchange -> swapMaplibreBasemap in
            # maplibre_tiles.js (see sync_basemap_select clientside_callback).
            dl.Map(
                [
                    dl.LayersControl(
                        [
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
                    # Track/envelope/facility layers — data/key populated reactively
                    # by _load_ms_tracks_and_envelopes (server callback, keyed off
                    # ms-tile-config-store) and the ms-{layer}-json clientside
                    # facility fetches (direct browser -> tile server /geojson/
                    # facilities/..., same no-Dash-round-trip pattern as
                    # callbacks/overlays.py) further down this file. style/
                    # onEachFeature/pointToLayer mirror layouts/panels.py's own
                    # dl.GeoJSON(...) calls for these exact layers.
                    # Envelopes BEFORE tracks (Leaflet stacks later-added
                    # layers on top) — tracks must always render above the
                    # wind envelope polygons, not underneath them.
                    dl.GeoJSON(id="ms-envelopes-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               style=style_envelopes, onEachFeature=tooltip_envelopes),
                    dl.GeoJSON(id="ms-tracks-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               style=style_tracks, onEachFeature=tooltip_tracks),
                    dl.GeoJSON(id="ms-schools-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_schools),
                    dl.GeoJSON(id="ms-health-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_health),
                    dl.GeoJSON(id="ms-shelters-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_shelters),
                    dl.GeoJSON(id="ms-wash-json", data={"type": "FeatureCollection", "features": []}, zoomToBounds=False,
                               pointToLayer=point_to_layer_schools_health, onEachFeature=tooltip_wash),
                    # These four hold empty GeoJSON — MapLibre renders the actual
                    # tiles (see CLAUDE.md "CRITICAL: Where tooltips actually come
                    # from"). They exist only as future Dash state containers for
                    # hideout props (tile coloring), not for rendering.
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
                # setView-driven jump can skip straight to 'moveend') — real
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


# Real, confirmed-by-direct-testing bug (present on /legacy too — pre-existing,
# not introduced here, just far more visible on this page since there's no
# "Load Layers" button to incidentally mask it): dl.Map's "load" eventHandler
# (sync_maplibre_on_load, which sets window._leaflet_maps['main-map'] — the
# thing everything else, including basemap switching, is gated on) races
# dash-leaflet's own async component bundle and never fires on a fresh page
# load. Confirmed empirically that "move"/"moveend" don't fire either from a
# Python-driven `viewport` prop update (tried both, neither worked reliably —
# dash-leaflet's internal reaction to a prop-diffed viewport apparently
# doesn't dispatch real Leaflet DOM events the way genuine interaction does).
# What DOES reliably work (confirmed): a real click on Leaflet's own native
# zoom +/- control buttons, same as a scroll-wheel zoom — so this simulates
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
    """Real "Last Updated" timestamp — same source/format as dashboard.py's
    own update_last_updated_header, so both pages agree on what "last
    updated" means. prevent_initial_call=False so this fires immediately on
    load too, not just after the first 15-minute interval."""
    try:
        latest_time = get_latest_forecast_time_overall()
        return latest_time.strftime("%b %d, %Y %H:%M UTC") if latest_time else "N/A"
    except Exception:
        return "N/A"


# A function (not a static value) — Dash's page router calls this with any
# URL query params as kwargs, e.g. /?lang=es -> layout(lang="es").
# Setting the module-level _LANG here, before building anything, is what lets
# every _t() call made while constructing this same tree pick up the right
# language, without threading a `lang` argument through every builder above.
def layout(lang="en", zoom_countries=None, open_breakdown=None, **kwargs):
    global _LANG
    _LANG = lang if lang in _TRANSLATIONS else "en"
    # zoom_countries/open_breakdown: set by the Full Impact Breakdown's own
    # "open in new tab" link (_breakdown_new_tab_href) — a comma-separated,
    # URL-encoded country list plus a flag to auto-open the modal, so the
    # new tab lands exactly where the original one was instead of a blank
    # Global view.
    initial_countries = [unquote(c) for c in zoom_countries.split(",") if c] if zoom_countries else None
    should_open_breakdown = bool(open_breakdown) and bool(initial_countries)
    return html.Div([
        dcc.Store(id="selected-country-store", data=initial_countries),
        # Real hazard tile-config bridge (see the "Hazard tile-config bridge"
        # section near the bottom of this file's callbacks): assembled by a
        # reactive Python callback (country/storm selection + all 5 hazard
        # controls) and pushed to MapLibre via the existing
        # dash_clientside.maplibre.updateTileConfig bridge already defined in
        # components/map/maplibre_tiles.js. ms-prefixed — distinct from
        # layouts/panels.py's own "maplibre-tile-config-store" (same running
        # app, see this file's own module docstring / CLAUDE.md's ID rule).
        # Declared standalone (previously nested inside _ms_loading_badge()'s
        # dcc.Loading in an attempt to make its target_components mechanism
        # fire — that never actually worked, see _ms_loading_badge's own
        # current comment) — the loading badge no longer depends on this
        # store's position in the tree at all.
        dcc.Store(id="ms-tile-config-store", data={}),
        # Mirrors the 4 ms-facility-{id}-on checkboxes (see
        # _mirror_facility_visibility below) — always present regardless of
        # mode, unlike the checkboxes themselves, which only exist in the
        # DOM in Country Analysis mode (_controls_zoom's Infrastructure
        # section; _controls_global has no Infrastructure section at all).
        # _update_map_legend reads THIS store, not the checkboxes directly,
        # for exactly that reason — see that callback's own docstring.
        dcc.Store(id="ms-facility-visibility-store",
                   data={"schools": False, "health": False, "shelters": False, "wash": False}),
        # Which hazard/tracks layer to show in the compact legend strip —
        # written by _track_last_toggled_layer, whichever checkbox the user
        # most recently turned ON (falls back to any other still-checked
        # layer if that one gets turned back off). None = nothing active.
        dcc.Store(id="ms-legend-last-layer", data=None),
        # Written only by a clientside debounce wrapper around the 4 hazard
        # sliders (~200ms after the drag settles) — NOT a real Output of any
        # Python callback despite being declared as one (always returns
        # no_update there); the deferred write happens via
        # dash_clientside.set_props from a setTimeout. Checkboxes/segmented
        # control are cheap and un-debounced, so they don't go through this.
        dcc.Store(id="ms-slider-debounce-store", data=0),
        # Dummy sink for the clientside bridge below (Output required by
        # clientside_callback's API; nothing ever reads it) — deliberately a
        # NEW ms-prefixed store rather than reusing pages/dashboard.py's own
        # Output('maplibre-container', 'data-config', ...) dummy-sink pattern,
        # to keep this page's callback graph fully independent of dashboard.py's.
        dcc.Store(id="ms-tile-config-applied-store", data=0),
        # Purely a client-side visual override — clicking the HAZARDS label
        # (see _command_bar) toggles this, which forces every hazard's
        # MapLibre layers invisible without touching the real checkboxes/
        # ms-tile-config-store at all, so un-hiding restores exactly what
        # was configured (see setHazardsHiddenOverride in maplibre_tiles.js).
        # A quick "just show me the base layer" preview, not a persisted
        # setting — any subsequent real hazard/checkbox change naturally
        # re-renders from the real config and drops this override, which is
        # the intended behavior for a temporary hide, not a bug.
        dcc.Store(id="ms-hazards-hidden-store", data=False),
        # Resolved config for the two GLOBAL, country/storm-independent raw
        # hazard layers (precip-raw raster + river-raw raster) — see
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
        # One-shot, fires ~500ms after mount — see _nudge_map_ready below for
        # why this exists (basemap switching silently does nothing on a
        # fresh load until this fires or the user manually pans/zooms).
        dcc.Interval(id="map-init-nudge", interval=500, n_intervals=0, max_intervals=1),
        # Real "Last Updated" refresh — same 15-min cadence and
        # get_latest_forecast_time_overall() source as dashboard.py's own
        # update_last_updated_header, under an "ms-"-prefixed id so it
        # doesn't collide with layouts/panels.py's live
        # "metadata-refresh-interval" in this same running app.
        dcc.Interval(id="ms-metadata-refresh-interval", interval=15 * 60 * 1000, n_intervals=0),
        _map_stack(),
        _topbar(initial_countries=initial_countries),
        _controls_panel(),
        _command_bar(),
        _impact_panel(initial_countries=initial_countries, open_breakdown=should_open_breakdown),
        _bottom_left_controls(),
        _map_legend(),
        _map_disclaimer(),
        _alert_email_modal(),
        _compact_footer(),
    ], style={"position": "relative", "width": "100%", "height": "100vh", "overflow": "hidden", "background": "#cfe3ee"})


# ---------------------------------------------------------------------------
# Callbacks — layout/state only, no data
# ---------------------------------------------------------------------------
# The pill UI (basemap-select) is the only thing the user interacts with —
# there's no separate "set the basemap" API to call, so instead this finds
# the matching native Leaflet LayersControl radio input (rendered into
# .leaflet-control-layers-base by dl.LayersControl in _map_stack) and clicks
# it. That fires Leaflet's own 'baselayerchange' event, which
# maplibre_tiles.js already listens for (swapMaplibreBasemap) to do the real
# tile swap — no new basemap-swapping JS needed, just reusing what's already
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
    # selected-country-store must be an Input, not State — picking a
    # different/additional country while ALREADY in zoom mode doesn't
    # necessarily change topbar-mode itself (see _country_selected's
    # no_update guard), so this needs to react to country changes directly
    # too, or Active Storms/Tropical Cyclone stay scoped to whatever country
    # was selected when zoom mode was first entered. topbar-date/topbar-time
    # are Inputs too now, for the same reason — _hurricane_family's
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


# No mode-based show/hide anymore — Demo Scenarios now stays visible in
# BOTH Global and Country Analysis mode (used to hide in Country Analysis,
# which just left an inconsistent-looking gap in that row; every preset
# already sets its own mode via _apply_demo_scenario regardless of where
# it's clicked from, so there's no correctness reason to hide it either).


@callback(
    Output("controls-exposure-pane", "style"),
    Output("controls-hazard-pane", "style"),
    Input("controls-view-toggle", "value"),
    prevent_initial_call=True,
)
def _toggle_controls_view(view):
    if view == "hazard":
        return {"display": "none"}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}


# Tropical Cyclone / Flood Hazards headers are pure click-to-collapse now
# (chevron, no switch) — same pattern as Infrastructure below, just against
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
# body-{key}/head-{key} id scheme — the rest of Exposure lost its header
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
# once per selected country — a fixed id per section, like the loop above,
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
    # overflowY) — this callback replaces the WHOLE style dict on every
    # click, so any fields missing here were silently lost the moment the
    # section was toggled even once.
    base = {"marginTop": "10px", "width": "100%", "maxWidth": "100%", "overflowX": "auto", "overflowY": "visible"}
    return base if is_hidden else {**base, "display": "none"}


# Fixed ids (not dash.MATCH) — unlike Admin Level 1, there's only ever ONE
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
    # current_children[0] is the label text.Div, [1] is the chevron span —
    # only the chevron direction changes, the label stays put.
    new_chevron = html.Span("▴" if is_hidden else "▾", style={"fontSize": "10px", "color": "#8ea0ab"})
    return new_style, [current_children[0], new_chevron]


@callback(
    Output("exposure-view-as", "data"),
    Output("exposure-view-note", "children"),
    Input("exposure-property", "value"),
)
def _update_view_as(prop):
    # In Need permanently disabled for now (not prop-dependent anymore) --
    # E_people_in_need/E_children_in_need only exist in wind's own MAT-table
    # columns (confirmed live), not gust/river/rain, so a shared tile_prop
    # switching to it would silently render blank for whichever other
    # hazard(s) are also toggled on. Rather than partially support this
    # (correct only when wind happens to be the sole active hazard), it's
    # fully disabled here until a real per-hazard-scoped fix lands (see
    # feedback_check_snowflake-adjacent task tracking this) -- re-enabling
    # is a one-line revert of `disabled`/the label once that's real.
    data = [
        {"value": "expected", "label": _t("At Risk")},
        {"value": "inneed", "label": _t("In Need (Coming Soon)"), "disabled": True},
    ]
    note = _t("In Need is not available yet for this view.")
    return data, note


# _WIND_CATS/_WIND_TIER_FACTOR now live near the top of the file (moved
# there so the real _STORMS-building code at module-import time can also
# use _WIND_CATS to derive a storm's category — see that section's own
# comment for why).


# Shared fixed height for every hazard's curve/grid — see
# _threshold_curve_chart's own comment on why this needs to be one value
# both it and _rain_threshold_grid agree on. Trimmed from 170px per user
# feedback that the tile-click popup's stacked cards ran too tall.
_HAZARD_CURVE_HEIGHT = "128px"


def _threshold_curve_chart(labels, values, active_idx, color):
    # Same visual language as the exceedance-curve option explored earlier
    # (line + filled area + a marked point at the current selection, with
    # its own value called out) — a real Plotly figure (like the arc
    # charts elsewhere on this page), not raw SVG — Dash's html module has
    # no SVG tag components to inject one directly.
    n = len(values)
    marker_sizes = [13 if i == active_idx else 6 for i in range(n)]
    marker_colors = [color if i == active_idx else "#ffffff" for i in range(n)]
    # Plotly's fillcolor doesn't accept 8-digit (alpha-suffixed) hex — needs
    # an explicit rgba() string for the same translucency.
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    fig = go.Figure(go.Scatter(
        x=list(range(n)), y=values, mode="lines+markers",
        line=dict(color=color, width=2), fill="tozeroy", fillcolor=f"rgba({r},{g},{b},0.12)",
        marker=dict(size=marker_sizes, color=marker_colors, line=dict(color=color, width=2)),
        hoverinfo="skip",
    ))
    # Every tier's own number is shown now (not just the selected one) — small
    # and gray so they read as reference context, not competing with the
    # selected tier's own bold black callout. Same idea as the Rainfall
    # matrix showing every cell's value, just adapted to a line chart: one
    # number stands out, the rest are still all visible for comparison.
    for i, v in enumerate(values):
        if i == active_idx:
            continue
        fig.add_annotation(
            x=i, y=v, text=_format_stat_number(round(v)),
            showarrow=False, yshift=13, font=dict(size=8, color="#8ea0ab"),
        )
    fig.add_annotation(
        x=active_idx, y=values[active_idx], text=f"<b>{_format_stat_number(round(values[active_idx]))}</b>",
        showarrow=False, yshift=16, font=dict(size=11, color="#16232c"),
    )
    fig.update_layout(
        height=52, margin=dict(l=2, r=2, t=16, b=10), showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False, range=[-0.3, n - 0.7]),
        yaxis=dict(visible=False, range=[0, max(values) * 1.15]),
    )
    # Fixed-height wrapper, centered — _rain_threshold_grid's heatmap is
    # naturally taller than this line chart (header row + 3 tier rows +
    # caption vs. one sparkline + one label row); without a shared height
    # the two ever sit at different sizes wherever they appear side by side
    # (_hazard_threshold_preview's grid) or stacked (the tile-click popup's
    # cards) — _HAZARD_CURVE_HEIGHT is the one place both agree on.
    return html.Div(html.Div([
        dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "52px", "width": "100%"}),
        html.Div([html.Span(_t(l), style={"fontSize": "8px", "color": "#8ea0ab", "flex": 1, "textAlign": "center"}) for l in labels],
                   style={"display": "flex", "padding": "0 2px", "marginTop": "-4px"}),
    ]), style={"minHeight": _HAZARD_CURVE_HEIGHT, "display": "flex", "flexDirection": "column",
                "justifyContent": "center", "marginBottom": "0px"})


def _rain_threshold_grid(labels, base_n, active_window, active_idx, color):
    # Rainfall's own version of _threshold_curve_chart — a small heatmap
    # grid (window × depth tier), not a line chart. An earlier version drew
    # 4 overlapping lines here, but genuinely 2D data (duration on one axis,
    # intensity on the other) is exactly the case a matrix/heatmap is the
    # standard, recognizable pattern for (weather dashboards use this same
    # grid shape for "accumulation over duration X at threshold Y" — cell
    # color encodes magnitude, position encodes the two axes, no lines to
    # visually tangle together). Each cell shows its own number directly
    # rather than relying on hover, since this is a small static preview,
    # not an interactive chart; the current (window, tier) cell gets a
    # colored ring so "where am I" is still obvious at a glance.
    windows = list(_RAIN_WINDOW_SCALE.keys())
    matrix = {w: [base_n * f * scale for f in _RAIN_TIER_FACTOR] for w, scale in _RAIN_WINDOW_SCALE.items()}
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
            # each cell's own intensity — matches the line charts' own
            # treatment (_threshold_curve_chart), where the selected tier's
            # number stands out and every other tier's number is still shown
            # but small and gray. The background tint is the only thing left
            # encoding magnitude now.
            rows.append(html.Div(
                _format_stat_number(round(val)),
                style={
                    "fontSize": "10px", "fontWeight": 700 if is_active else 500, "fontFamily": "monospace",
                    "textAlign": "center", "padding": "4px 2px", "borderRadius": "5px",
                    "background": f"rgba({r},{g},{b},{0.1 + intensity * 0.55:.2f})",
                    "color": "#16232c" if is_active else "#8ea0ab",
                    "border": f"2px solid {color}" if is_active else "2px solid transparent",
                },
            ))
    # Fixed-height wrapper, centered — same _HAZARD_CURVE_HEIGHT the line
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
# vaguer plain-English gloss — return period is the actual unit the
# underlying river-flood data is thresholded on.
_RIVER_CATS = ["1-in-2-year flood", "1-in-5-year flood", "1-in-10-year flood",
               "1-in-20-year flood", "1-in-50-year flood", "1-in-100-year flood"]
# Real MERCATOR_TILE_RIVER_MAT.RP_TIER values (confirmed live: 'rp2'/'rp5'/
# 'rp10'/'rp20'/'rp50'/'rp100', a string not an int) — same order as
# _RIVER_CATS/ms-river-slider so index i's tier label always matches index
# i's real backend RP_TIER value.
_RIVER_RP_TIERS = ["rp2", "rp5", "rp10", "rp20", "rp50", "rp100"]
_RAIN_TIERS = ["Moderate rain", "Heavy rain", "Extreme rain"]
_RAIN_MM_BY_WINDOW = {"6": [25, 50, 75], "24": [35, 70, 103], "72": [45, 90, 133], "120": [50, 100, 150]}
# Height above normal tide — placeholder categories, no real pipeline behind
# these yet (see ms-surge-on's own comment).
_SURGE_TIERS = ["Minor surge (0.3–1m)", "Moderate surge (1–2m)", "Major surge (2–3m)", "Extreme surge (>3m)"]

# PREVIEW ONLY (see _WIND_TIER_FACTOR's own comment — same idea, one ratio
# list per hazard, index of today's default slider position = 1.0). All
# four hazards DECREASE with severity here, same direction — a return
# period is being read the same way as a wind/rain/surge intensity
# threshold: "how many people does THIS forecast put at risk of AT LEAST
# this severity", an exceedance question, not "how big is a hypothetical
# flood of exactly this return period on a static climatological map" (a
# different question, where extent genuinely grows with return period —
# easy to conflate the two, and an earlier version of this file did:
# rarer/bigger events are still harder for any ONE forecast to actually
# reach, so fewer people clear that bar, matching real GloFAS ensemble
# behavior too — fewer members/cells exceed RP20 than RP2 for a given
# forecast).
_RIVER_TIER_FACTOR = [2.6, 1.0, 0.42, 0.19, 0.07, 0.03]  # default index 1 (1-in-5yr)
_RAIN_TIER_FACTOR = [1.8, 1.0, 0.45]                      # default index 1 (Heavy)
_SURGE_TIER_FACTOR = [1.6, 1.0, 0.5, 0.22]                # default index 1 (Moderate)
# Rainfall is genuinely 2D (window × depth tier, see _RAIN_MM_BY_WINDOW) —
# shown as one line per window instead of slicing at whichever window is
# selected, so all four are visible/comparable at once, not just the
# current one. Longer accumulation windows scale the whole tier-factor
# curve UP (more total rain accumulates given more time, even at the same
# nominal "Moderate/Heavy/Extreme" label, since each window's own mm
# thresholds are already calibrated to represent "moderate for that
# duration" — see _RAIN_MM_BY_WINDOW). "6" (today's default window) = 1.0
# so nothing changes at the existing default combo (6h + Heavy).
_RAIN_WINDOW_SCALE = {"6": 1.0, "24": 1.4, "72": 1.9, "120": 2.3}
_RAIN_WINDOW_LABELS = {"6": "6h", "24": "24h", "72": "72h", "120": "120h"}

_WIND_CURVE_LABELS = [c[1].replace("Category ", "Cat").replace(" Hurricane", "").replace("Trop. Storm", "TS") for c in _WIND_CATS]
# Google FloodHub's own convention (per its public docs): "warning level" =
# 1-in-2yr, "danger level" = 1-in-5yr, "extreme level" = 1-in-20yr — plain
# severity names instead of a raw return-period ratio, since "1-in-5-year"
# means little at a glance to a reader who isn't already a hydrologist.
# Adapted here to all 6 of our tiers (FloodHub only names 3): keeps
# FloodHub's own three terms at their matching tiers (2/5/20yr) and adds
# Severe (10yr, between Danger and Extreme) plus Catastrophic/Historic
# (50/100yr, beyond Extreme) to complete the escalating ladder.
_RIVER_CURVE_LABELS = ["Warning", "Danger", "Severe", "Extreme", "Catastrophic", "Historic"]
_RAIN_CURVE_LABELS = [t.replace(" rain", "") for t in _RAIN_TIERS]
_SURGE_CURVE_LABELS = [t.split(" (")[0].replace(" surge", "") for t in _SURGE_TIERS]

# Per-hazard (labels, tier-factor ratios) — keyed by the same names used in
# _HAZARD_CONTRIBUTION/_HAZARD_GROUPS, so any row for one of these hazards
# (wherever it appears — alone, or as one of Flood's several active members)
# can look up its own curve data by name.
_HAZARD_CURVE_DATA = {
    "Sustained Wind": (_WIND_CURVE_LABELS, _WIND_TIER_FACTOR),
    "River Flooding": (_RIVER_CURVE_LABELS, _RIVER_TIER_FACTOR),
    "Rainfall": (_RAIN_CURVE_LABELS, _RAIN_TIER_FACTOR),
    "Storm Surge": (_SURGE_CURVE_LABELS, _SURGE_TIER_FACTOR),
}


def _hazard_threshold_preview(breakdown, hazard_idx, total_people_at_risk, rain_window=None, expanded=False):
    # PREVIEW ONLY — same curves as the tile-click popup's own per-hazard
    # rows (_hazard_contribution_content), but the Full Impact Breakdown
    # (modal + print page) has no single per-tile total to hang a
    # metric-specific version off of, so this scales every curve off
    # "People at Risk" specifically instead. Shared by the print page today;
    # nothing stops the interactive modal from using it too later.
    #
    # Collapsed by default in the interactive modal (same reasoning as the
    # Admin Level 1 sections below the table: it's supplementary detail, not
    # something to always show) — expanded=True (the print page, which has
    # no click-to-expand interaction a reader could use) renders it already
    # open and drops the click affordance entirely, same pattern as
    # _admin1_section.
    if not hazard_idx:
        return None
    active_names = breakdown["active_tc_members"] + breakdown["active_flood_members"]
    rows = []
    for name in active_names:
        idx = hazard_idx.get(name)
        curve_data = _HAZARD_CURVE_DATA.get(name)
        if idx is None or curve_data is None:
            continue
        color, pct, icon = _HAZARD_BY_NAME[name]
        labels, factors = curve_data
        base_n = round(total_people_at_risk * pct / 100)
        if name == "Rainfall" and rain_window is not None:
            chart = _rain_threshold_grid(labels, base_n, rain_window, idx, color)
        else:
            values = [base_n * f for f in factors]
            chart = _threshold_curve_chart(labels, values, idx, color)
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
    return _t(_RIVER_CATS[idx or 0])


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
    return f"{_t(_RAIN_TIERS[idx])} · ≥ {mm}mm over {window or '6'}h"


# Handles the Global-mode Active Storms list rows — uses the
# {"type": "select-storm", "name": ...} pattern-matching id, since clicking
# a row does the same thing regardless of where it's clicked from: set the
# country picker's value to ALL of that storm's affected countries at once
# (a multi-country storm selects multiple countries here directly — no
# separate "also affects" switcher needed once the picker itself is
# multi-select), which the callback below reacts to (one source of truth for
# "selected countries", not a separate chip + a separate picker disagreeing).
@callback(
    Output("topbar-country-select", "value"),
    Input({"type": "select-storm", "name": dash.ALL}, "n_clicks"),
    State("topbar-date", "value"),
    State("topbar-time", "value"),
    prevent_initial_call=True,
)
def _select_storm(clicks, date, run):
    if not clicks or not any(clicks):
        return dash.no_update
    triggered = dash.callback_context.triggered_id
    name = triggered["name"]
    # Storm rows are rendered by _active_storms_section from the date/run-
    # reactive _resolve_storms_for_date, not the frozen _STORMS snapshot —
    # look storms up the same way, or a click on a row that only exists for
    # the currently-selected date (a new storm, a dissipated one, or a
    # Demo Scenario's historical date) would silently miss and fall back to
    # the wrong storm/countries (the same class of bug already fixed for
    # _hurricane_family/_flood_hazards_family's has_storms/availability).
    storms = _resolve_storms_for_date(date, run)
    fallback = storms[0] if storms else {"countries": []}
    storm = next((s for s in storms if s["name"] == name), fallback)
    return list(storm["countries"])


# The ONLY place "selected countries" actually gets set — whether picked
# directly from this multi-select or arrived via a storm click/search (which
# just sets this same picker's value, see _select_storm above). Guards mode
# with a State comparison rather than always writing it: this callback and
# _clear_countries_on_global below both touch topbar-mode/topbar-country-select,
# so writing a value that's already current (e.g. re-affirming "global") would
# re-trigger the other callback and ping-pong forever — only ever write mode
# when it's actually changing.
@callback(
    Output("selected-country-store", "data"),
    Output("topbar-mode", "value", allow_duplicate=True),
    Input("topbar-country-select", "value"),
    State("topbar-mode", "value"),
    prevent_initial_call=True,
)
def _country_selected(countries, current_mode):
    countries = countries or []
    new_mode = "zoom" if countries else "global"
    mode_out = dash.no_update if new_mode == current_mode else new_mode
    return countries, mode_out


# Flies the Leaflet map to the selected country's real center/zoom — until
# now, selecting a country updated the Impact Summary/mode but left the map
# viewport wherever it happened to already be, totally unrelated to the
# selection. _NAME_TO_CENTER (built above from get_active_countries()'s own
# CENTER_LAT/CENTER_LON/VIEW_ZOOM columns, the same PIPELINE_COUNTRIES data
# _CODE_TO_NAME/_NAME_TO_CODE already load) — no new query. Single selected
# country (with a resolvable center): flies to that country's own
# center/zoom, exactly as before. 2+ selected countries (with 2+ resolvable
# centers): fits the viewport to a real bounding box across all of them,
# via dash-leaflet's own "bounds" viewport key (Map.viewport supports
# either center/zoom OR bounds — bounds takes precedence and does a real
# fitBounds/flyToBounds, no manual center+zoom math needed) — previously
# this always flew to just the FIRST selected country's center, leaving
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
    # through the raw numeric range — e.g. Jamaica (-77°) and the
    # Philippines (+122°) naively span 199° through Africa/the Middle East,
    # but the real short way between them is only 161° through the Pacific.
    # Shift any negative longitude by +360 and compare spans; if the shifted
    # span is smaller, use it — Leaflet's fitBounds accepts lng values
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
# clearing the country picker) should drop the country selection — otherwise
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


# Real bug found+fixed here: _update_map_legend used to read the 4
# ms-facility-{id}-on checkboxes directly as Inputs — but those only exist
# in the DOM in Country Analysis mode (_controls_zoom's Infrastructure
# section), and Dash never fires a callback at all while ANY of its Inputs
# is missing from the current layout, not just returns None for that one
# value. Confirmed live: this silently broke the legend in EVERY Global-
# mode render, including its Tracks/envelope/raster sections that have
# nothing to do with facilities — zero ms-legend-body update requests ever
# fired there. Mirroring into this always-present store (only fires when
# the checkboxes DO exist, i.e. Country Analysis mode) decouples the two.
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


# The mirror callback above can't reset itself to all-False on switching to
# Global (its own Inputs no longer exist there to fire it) — this is what
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
# _time_options_for_date's dimmed labels above are cosmetic only — this is
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
    data = _time_options_for_date(date)
    max_run = _max_allowed_run_for_date(date)
    if max_run is not None and run is not None and int(run) > int(max_run):
        return data, max_run
    return data, dash.no_update


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
    Input("ms-wind-slider", "value"),
    Input("ms-gust-slider", "value"),
    Input("ms-river-slider", "value"),
    Input("ms-rain-slider", "value"),
    Input("ms-rain-window", "value"),
)
def _update_impact_summary(countries, influencing_factor, aggregation, wind_on, gust_on, river_on, rain_on, surge_on,
                             date, run, wind_idx, gust_idx, river_idx, rain_idx, rain_window):
    # Scoped to whichever hazards are toggled on (sidebar checkboxes/
    # command-bar pills) — the at-risk numbers below are now the REAL
    # combined-across-active-hazards total (_build_hz/
    # _fetch_real_combined_tile_totals), not an illustrative percentage
    # scaling of wind's own total. topbar-date/topbar-time are Inputs — see
    # _resolve_storm_for_country's own docstring for why the real numbers
    # here need to react to date/run changes, not just country changes
    # (a Demo Scenario or the date picker can point at a historical date with
    # no currently-active storm at all). Every hazard's own slider is an
    # Input too — see _build_hz's own docstring for why the real numbers
    # here need to react to each hazard's own threshold, not just wind's.
    wind_kt = _resolve_wind_kt(wind_idx)
    hz = _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx, gust_idx, river_idx, rain_idx, rain_window)
    countries = countries or []
    influencing_factor = influencing_factor or "none"
    if not countries:
        # Global: real worldwide total — sum of every country's own real
        # combined-hazard impact (_combined_stats, same combination already
        # used for multi-country Country Analysis), across every country
        # any REAL, currently-impactful storm affects (_resolve_storms_for_
        # date, impact-gated — correct here, unlike get_track_ids_for_
        # date's own track-existence-only check used for the header dot
        # above). Replaces the old _DEFAULT_STATS mock (a fixed hardcoded
        # number) with a genuinely zero total on a real quiet day.
        #
        # Real behavior change here: this used to be wind-only AND reactive
        # to the sidebar's wind_on/wind_idx (the header even said "across
        # VISIBLE hazards") — but Global mode's checkboxes/sliders are a map-
        # display concern (which layers paint on the map), not a "what
        # counts toward the worldwide total" concern. Global's own total is
        # now always the real combination of EVERY hazard (wind+gust+river+
        # rain) at each hazard's own default severity tier, fully
        # independent of whatever's currently toggled/scrubbed on the map —
        # matches Country Analysis's single-country/combined blocks in
        # spirit (a real multi-hazard total) without depending on this
        # page's map-display selection at all. Country Analysis mode below
        # is untouched — it still reacts to wind_on/gust_on/.../each
        # hazard's own slider via the selection-driven `hz` computed above.
        all_storms = _resolve_storms_for_date(date, run)
        all_country_names = sorted({c for s in all_storms for c in s["countries"]})
        global_hz = _build_hz(True, True, True, True)
        global_stats = _combined_stats(all_country_names, date=date, run=run,
                                         wind_kt=global_hz["wind_kt"], hz=global_hz)
        # impact-subtitle itself is a dmc.Text (renders a <p>) — its own
        # "component" prop must stay untouched (setting component="div"
        # here crashed a Mantine clientside prop-transform on page load),
        # but plain children (Span/Br/Div) render into it fine. A plain
        # html.Div pill instead of dmc.Badge — Badge is built for short,
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

    compare_member = _WORST_MEMBER_BY_FACTOR.get(influencing_factor)

    # No more "In Need" number tiles here (extra_stats) — replaced by the
    # same circular PIN/CHIN gauges the Full Impact Breakdown modal used to
    # show in its own (width-breaking) side column. Each block below is the
    # base stat grid plus its matching arc-chart pair, show_label=False so
    # the country/"Combined" name isn't repeated a second time right under
    # the grid's own label.
    def _country_block(base_stats, base_pin, arc_charts, label=None, scope="combined"):
        compare_stats = None
        if compare_member:
            scaled = _scaled_stats(base_stats, compare_member)
            scaled_pin = _scaled_pin_pct(base_pin, compare_member)
            compare_stats = dict(scaled)
            compare_stats["People in Need"] = _format_stat_number(
                round(_parse_stat_number(scaled["People at Risk"]) * scaled_pin["people"] / 100))
            compare_stats["Children in Need"] = _format_stat_number(
                round(_parse_stat_number(scaled["Children at Risk"]) * scaled_pin["children"] / 100))
        grid = _stat_grid(base_stats, label=label, scope=scope,
                            compare_stats=compare_stats, compare_member=compare_member)
        return html.Div([grid, arc_charts])

    if len(countries) == 1:
        country = countries[0]
        storm_info = _resolve_storm_for_country(country, date, run)
        subtitle = f'{_t(country)} · {storm_info["cat"]}' if storm_info else _t("{country} — no active tropical cyclone", country=_t(country))
        arc_charts = _pin_arc_charts_block(country, compare_member, show_label=False, date=date, run=run, wind_kt=wind_kt, hz=hz)
        base_stats = _get_country_stats(country, date, run, wind_kt, hz=hz)
        return _country_block(base_stats, _get_country_pin_pct(country, date, run, wind_kt, hz=hz),
                               arc_charts, scope=country), subtitle

    if aggregation == "combined":
        # Real per-country totals summed together (_combined_stats/
        # _combined_in_need_total), each already the real combined-across-
        # active-hazards total (via `hz`) — not a re-derived aggregate, and
        # no more illustrative percentage scaling.
        combined_base = _combined_stats(countries, date=date, run=run, wind_kt=wind_kt, hz=hz)
        combined_pin = {
            "people": round(_combined_in_need_total(countries, "People at Risk", "people", date=date, run=run, wind_kt=wind_kt, hz=hz)
                             / max(1, _parse_stat_number(combined_base["People at Risk"])) * 100),
            "children": round(_combined_in_need_total(countries, "Children at Risk", "children", date=date, run=run, wind_kt=wind_kt, hz=hz)
                                / max(1, _parse_stat_number(combined_base["Children at Risk"])) * 100),
        }
        subtitle = _t("Combined — {n} countries", n=len(countries))
        arc_charts = _pin_arc_charts_block_combined(countries, compare_member, show_label=False, date=date, run=run, wind_kt=wind_kt, hz=hz)
        return _country_block(combined_base, combined_pin, arc_charts, scope="combined"), subtitle

    # Per country (default): one stat block per country, side by side — not
    # a single combined number and not forcing a pick between them.
    subtitle = _t("{n} countries selected: {list}", n=len(countries), list=", ".join(_t(c) for c in countries))
    blocks = [_country_block(_get_country_stats(c, date, run, wind_kt, hz=hz), _get_country_pin_pct(c, date, run, wind_kt, hz=hz),
                              _pin_arc_charts_block(c, compare_member, show_label=False, date=date, run=run, wind_kt=wind_kt, hz=hz),
                              label=c, scope=c) for c in countries]
    return blocks, subtitle


@callback(
    Output("impact-breakdown-modal", "opened"),
    Input("impact-breakdown-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _open_impact_breakdown(_n):
    return True


@callback(
    Output("impact-breakdown-btn", "style"),
    Input("selected-country-store", "data"),
)
def _toggle_breakdown_btn(countries):
    return {} if countries else {"display": "none"}


@callback(
    Output("impact-controls-row", "style"),
    Input("selected-country-store", "data"),
)
def _toggle_controls_row(countries):
    base = {"padding": "10px 18px", "borderTop": "1px solid #eef2f5"}
    return base if countries else {**base, "display": "none"}


@callback(
    Output("impact-aggregation-wrapper", "style"),
    Input("selected-country-store", "data"),
)
def _toggle_aggregation_wrapper(countries):
    # Only 2+ countries genuinely have anything to sum or split apart — a
    # single country has nothing for "Total" to do differently from
    # "Split", so just this one control (not the whole row — the worst-case
    # factor Select next to it is still useful for a single country) stays
    # hidden until there's a real choice to make.
    return {} if countries and len(countries) > 1 else {"display": "none"}


@callback(
    Output("impact-breakdown-body", "children"),
    Input("selected-country-store", "data"),
    Input("influencing-factor-select", "value"),
    Input("ms-wind-on", "checked"),
    Input("ms-gust-on", "checked"),
    Input("ms-river-on", "checked"),
    Input("ms-rain-on", "checked"),
    Input("ms-surge-on", "checked"),
    Input("ms-wind-slider", "value"),
    Input("ms-gust-slider", "value"),
    Input("ms-river-slider", "value"),
    Input("ms-rain-slider", "value"),
    Input("ms-surge-slider", "value"),
    Input("ms-rain-window", "value"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
)
def _update_impact_breakdown(countries, influencing_factor, wind_on, gust_on, river_on, rain_on, surge_on,
                                wind_idx, gust_idx, river_idx, rain_idx, surge_idx, rain_window, date, run):
    # Not fed impact-aggregation-toggle — the modal always shows both
    # per-country AND Combined at once (see _impact_breakdown_content), so
    # unlike the compact panel it has nothing to switch between.
    #
    # topbar-date/topbar-time are now Inputs too (matching
    # _update_impact_summary's own pattern) — this modal's numbers were
    # previously always resolved against the frozen "active right now"
    # storm regardless of the selected date/run, silently disagreeing with
    # the Impact Summary panel right next to it.
    return _impact_breakdown_content(countries, influencing_factor, wind_on=wind_on, gust_on=gust_on,
                                        river_on=river_on, rain_on=rain_on, surge_on=surge_on,
                                        wind_idx=wind_idx, gust_idx=gust_idx, river_idx=river_idx, rain_idx=rain_idx, surge_idx=surge_idx,
                                        rain_window=rain_window, date=date, run=run)


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
)
def _update_breakdown_new_tab_link(countries, date, run, wind_on, river_on, rain_on, surge_on,
                                      wind_idx, river_idx, rain_idx, surge_idx, rain_window):
    return _breakdown_new_tab_href(countries, date=date, run=run, wind_on=wind_on,
                                      river_on=river_on, rain_on=rain_on, surge_on=surge_on,
                                      wind_idx=wind_idx, river_idx=river_idx, rain_idx=rain_idx, surge_idx=surge_idx,
                                      rain_window=rain_window)


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
    State("topbar-date", "value"),
    State("topbar-time", "value"),
    prevent_initial_call=True,
)
def _open_hazard_contribution(clicks, countries, wind_on, gust_on, river_on, rain_on, surge_on,
                                 wind_idx, gust_idx, river_idx, rain_idx, surge_idx, rain_window, date, run):
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update
    triggered = dash.callback_context.triggered_id
    metric, scope = triggered["metric"], triggered["scope"]
    # Real bug found+fixed here: Global's own Impact Summary total is now
    # ALWAYS the real combination of every hazard (_update_impact_summary's
    # Global branch, independent of the sidebar checkboxes) — but this
    # popup's breakdown still read the LIVE checkbox state regardless of
    # scope, so clicking a Global stat while every checkbox happened to be
    # unchecked showed "None — toggle a hazard..." even though the number
    # just clicked came from a real all-hazard total. Global scope forces
    # the same all-hazards-on view Global's own total already uses; Country
    # Analysis scopes (single-country/combined) are untouched — those
    # totals genuinely still depend on the checkboxes.
    is_global = (scope == "global")
    breakdown = _hazard_breakdown(True, True, True, surge_on) if is_global else _hazard_breakdown(wind_on, river_on, rain_on, surge_on)
    # topbar-date/topbar-time are now States too — this popup previously
    # always resolved against the frozen "active right now" storm/50kt
    # default, which could silently disagree with the tile that was clicked
    # to open it (see _resolve_stat_value's own docstring).
    wind_kt = _resolve_wind_kt(wind_idx)
    hz = _build_hz(True, True, True, True) if is_global else \
        _build_hz(wind_on, gust_on, river_on, rain_on, wind_idx, gust_idx, river_idx, rain_idx, rain_window)
    value = _resolve_stat_value(metric, scope, countries, scale=breakdown["scale"], date=date, run=run, wind_kt=wind_kt, hz=hz)
    title = f"{_t(metric)} — {_t('Combined')}" if scope == "combined" else (
        f"{_t(metric)} — {_t(scope)}" if scope != "global" else _t(metric))
    hazard_idx = {"Sustained Wind": wind_idx, "River Flooding": river_idx,
                   "Rainfall": rain_idx, "Storm Surge": surge_idx}
    return True, title, _hazard_contribution_content(value, breakdown, hazard_idx=hazard_idx, rain_window=rain_window, is_global=is_global)


@callback(
    Output("alert-email-modal", "opened"),
    Output("alert-email-iframe", "src"),
    Output("alert-email-new-tab-link", "href"),
    Input({"type": "alert-email-btn", "name": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _open_alert_email(clicks):
    if not clicks or not any(clicks):
        return dash.no_update, dash.no_update, dash.no_update
    # Only one mock email exists right now (_ALERT_EMAIL_AVAILABLE), so every
    # button currently points at the same URL — a real version would look up
    # the stored HTML by (storm, country, date) instead.
    return True, _MOCK_ALERT_EMAIL_URL, _MOCK_ALERT_EMAIL_URL


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
    # Only sets date/time/countries — mode and selected-country-store
    # already cascade from topbar-country-select changing, the same
    # mechanism a storm-row click uses (_select_storm -> _country_selected).
    scenario = _DEMO_SCENARIOS[dash.callback_context.triggered_id["index"]]
    return scenario["date"], scenario["time"], scenario["countries"]


# Command-bar pills mirror the rail checkboxes (ms-{hz}-on) rather than being
# a second source of truth: clicking a pill flips the checkbox, and the
# checkbox's own state (however it got set — pill, rail, anything else later)
# drives the pill's pressed/unpressed look. Two one-directional callbacks per
# hazard, not a loop: the pill click never writes its own style, only the
# checkbox; the style-reflecting callback only ever reads the checkbox.
#
# Both callbacks also read the checkbox's own "disabled" prop (set in
# _flood_hazards_family / _hurricane_family from real data-availability
# checks) — the pill is a second entry point to the exact same checkbox, so
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
    )
    def _reflect_checkbox_on_pill(checked, disabled, _color=_hz_color):
        if disabled:
            return _PILL_DISABLED_STYLE, _DOT_OFF_STYLE, "true"
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
# tile-server-backed MapLibre layers — Wind, Gust, River Flooding, and
# Rainfall for real; Storm Surge stays a visual-only preview (surge_visible
# is always False below, no real backend exists for it, no request is ever
# built for it). Reactive: every control change re-assembles the config and
# re-pushes it to MapLibre, no "Load Layers" button.
#
# Three pieces:
#   1. A clientside debounce wrapper around the 4 hazard sliders — writes
#      ms-slider-debounce-store ~200ms after a drag settles (via
#      dash_clientside.set_props, not this callback's own declared Output,
#      which always returns no_update) so a slider drag doesn't fire dozens
#      of Snowflake/tile-server requests.
#   2. _build_hazard_tile_config (plain Python callback) — assembles the full
#      config dict from country selection + all 5 hazard controls, fetching
#      /stats and /admin-stats from the tile server for whichever hazards are
#      both checked "on" AND have a resolved forecast_date. Checkboxes/
#      ms-rain-window are direct (un-debounced) Inputs; the 4 sliders are
#      State, re-read only when Input("ms-slider-debounce-store") fires.
#   3. A clientside bridge pushing the assembled config to the existing
#      window.applyTileConfig (components/map/maplibre_tiles.js) — the same
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
    (missing data → empty dict, not a crash). Cached 60s — this callback is
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
# _context_data_section) to the real GeoJSON/stats column names — analogous to
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

# Hazard-weighted "expected impact" column names — mirrors callbacks/
# tiles_and_admin.py's real _LAYER_TO_E_PROP (the /legacy dashboard's own
# "Probability" toggle, which SWAPS a demographic layer from its raw count to
# this column rather than overlaying a second layer), keyed by THIS page's
# own exposure-property radio values. Props with no meaningful "expected
# impact" version (settlement/rwi/poverty, already probability-derived
# metrics rather than counts) resolve to the raw hazard "probability" column
# itself, same as /legacy's own choice for those.
_EXPOSURE_E_PROP_MAP = {
    "population": "E_population",
    "children": "E_children_total",
    "infant": "E_infant_population",
    "school-age": "E_school_age_population",
    "adolescent": "E_adolescent_population",
    "built": "E_built_surface_m2",
    "settlement": "probability",
    "rwi": "probability",
    "moderate-poverty": "probability",
    "severe-poverty": "probability",
}

# "In Need" real column names — copied verbatim from components/map/
# tile_palettes.json's own in_need_map (E_people_in_need / E_children_in_need),
# keyed here by THIS page's own exposure-property radio values ("population"/
# "children", see _exposure_section) rather than tile_palettes.json's own keys
# ("population"/"children-total"). Only these two props are ever eligible —
# same eligibility check as _update_view_as above.
_EXPOSURE_IN_NEED_PROP_MAP = {
    "population": "E_people_in_need",
    "children": "E_children_in_need",
}


def _hazard_stats(tile_country, hazard, path_storm, path_date, wind_threshold, extra_params):
    """Return (stats, admin_stats) for one hazard, or ({}, {}) if this
    hazard has no resolved forecast_date to query at all (e.g. a country with
    genuinely no river/rain data — see get_latest_river_forecast_time's own
    docstring; not a bug, a real coverage gap)."""
    if not tile_country or not path_date:
        return {}, {}
    base_url = "" if config.SPCS_RUN else config.TILE_SERVER_URL
    common = {"wind_threshold": wind_threshold, "hazard": hazard, **extra_params}
    stats_qs = urllib.parse.urlencode(common)
    admin_qs = urllib.parse.urlencode({**common, "admin_level": 1})
    stats_url = (f"{base_url}/stats/{quote(tile_country)}/{quote(path_storm)}/{quote(path_date)}?{stats_qs}")
    admin_url = (f"{base_url}/admin-stats/{quote(tile_country)}/{quote(path_storm)}/{quote(path_date)}?{admin_qs}")
    return _fetch_tile_server_json(stats_url), _fetch_tile_server_json(admin_url)


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
    Input("ms-slider-debounce-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("cmdbar-detail", "value"),
    State("ms-wind-slider", "value"),
    State("ms-gust-slider", "value"),
    State("ms-river-slider", "value"),
    State("ms-rain-slider", "value"),
)
def _build_hazard_tile_config(countries, exposure_prop, view_as, tc_view_as, wind_on, gust_on, river_on, rain_on, surge_on, rain_window,
                                _debounce_tick, date, run, view_mode, wind_idx, gust_idx, river_idx, rain_idx):
    countries = countries or []
    # cmdbar-detail ("tiles"/"admin") only ever renders in Country Analysis
    # mode (see _command_bar's own docstring) but stays mounted (just hidden)
    # in Global mode, so its value is always a real Input here — default to
    # "tiles" only for the pre-render tick where Dash hasn't set it yet.
    view_mode = view_mode or "tiles"
    tc_view_as = tc_view_as or "envelopes"
    if not countries:
        return {
            "country": None, "wind_visible": False, "gust_visible": False,
            "river_visible": False, "rain_visible": False, "surge_visible": False,
            "view_mode": view_mode, "tc_view_as": tc_view_as,
        }

    codes = _resolve_tile_codes(countries)
    tile_country = "+".join(codes)
    primary_code = codes[0] if codes else None
    resolved_prop = _EXPOSURE_PROP_MAP.get(exposure_prop, "population")
    # Matches /legacy's real "Probability" toggle semantics (callbacks/
    # tiles_and_admin.py's _compute_layer_toggle_outputs): a demographic
    # property is a plain basis layer (raw count) only while NO hazard is
    # active; the moment any real hazard is toggled on, it SWITCHES (not
    # overlays — each hazard's own MapLibre raster source independently
    # resolves this same prop name from its own table) to the hazard-
    # weighted "expected impact" column instead. Storm Surge is excluded —
    # it never carries real hazard state (surge_visible is always False).
    any_hazard_on = bool(wind_on or gust_on or river_on or rain_on)
    if any_hazard_on:
        resolved_prop = _EXPOSURE_E_PROP_MAP.get(exposure_prop, resolved_prop)
    # "In Need" only ever swaps population/children to their real E_*_in_need
    # column — every other prop (and "At Risk") is untouched, same
    # eligibility rule _update_view_as already enforces for the segmented
    # control itself. Takes precedence over the hazard-weighted switch above
    # (in_need already implies "hazard-weighted", just further vulnerability-
    # refined) regardless of any_hazard_on.
    if view_as == "inneed" and exposure_prop in _EXPOSURE_IN_NEED_PROP_MAP:
        resolved_prop = _EXPOSURE_IN_NEED_PROP_MAP[exposure_prop]

    # Wind/gust share the same real storm/forecast_date, resolved REACTIVELY
    # (first selected country with real impact data for the CURRENTLY
    # selected topbar-date/topbar-time — not "currently active right now")
    # via _resolve_storm_for_country, same reactive lookup
    # _fetch_real_tile_totals now uses for the Impact Summary panel. This is
    # what makes historical dates (date picker, Demo Scenarios) actually
    # drive real wind/gust tile layers and tracks/envelopes instead of
    # silently resolving to nothing.
    #
    # Real bug found+fixed here (#248): selecting two countries hit by two
    # DIFFERENT real storms on the same date used to force EVERY selected
    # country's wind/gust tiles onto whichever storm this loop resolved
    # first — the other country's tiles silently came back empty (its own
    # real storm never matched the shared STORM+FORECAST_DATE filter). Every
    # country's own real storm is now resolved once here (country_storm_infos,
    # in original selection order) and grouped below: the FIRST resolved
    # storm keeps today's exact single "storm"/"forecast_date"/tile_country
    # shape (zero behavior change for the overwhelming common case of one
    # shared storm), and any OTHER distinct storm among the remaining
    # countries becomes its own "extra_wind_groups"/"extra_gust_groups" entry
    # (see maplibre_tiles.js's own "MULTI-STORM GROUPS" section) — a real,
    # separately-queried tile layer for that country's own real storm,
    # instead of silently reusing the primary one.
    country_storm_infos = []
    for c in countries:
        code = _NAME_TO_CODE.get(c)
        if not code:
            continue
        storm_info = _resolve_storm_for_country(c, date, run)
        if storm_info:
            country_storm_infos.append((code, storm_info))

    storm = None
    forecast_date = None
    if country_storm_infos:
        storm = country_storm_infos[0][1]["name"]
        forecast_date = country_storm_infos[0][1]["mat_forecast_date"]

    extra_groups_by_storm = {}
    for code, storm_info in country_storm_infos[1:]:
        if storm_info["name"] == storm:
            continue  # same storm as primary — already covered by tile_country/storm/forecast_date
        g = extra_groups_by_storm.setdefault(
            storm_info["name"], {"codes": [], "forecast_date": storm_info["mat_forecast_date"]})
        if code not in g["codes"]:
            g["codes"].append(code)

    wind_idx = wind_idx if wind_idx is not None else 2
    gust_idx = gust_idx if gust_idx is not None else 2
    wind_kt = _WIND_CATS[wind_idx][2]
    # index [3], NOT [2] — _WIND_CATS rows are (label, name, wind_kt, gust_kt).
    # Real bug found+fixed here: this used to read [2] (wind's own kt), so the
    # GUST_THRESHOLD filter below never matched a single real row (wind kt
    # values 34/40/50/64/83/96/113/137 vs real gust kt values 17/21/26/33/43/
    # 49/58/70 never overlap) — the Gust map layer has been silently empty
    # since it shipped. See _resolve_gust_kt's own docstring for the live
    # verification (BAVI/PHL 2026-07-02: 0 rows at kt=50, real rows at kt=26).
    gust_kt = _WIND_CATS[gust_idx][3]

    # Default index 2 == "rp10" (see _build_hz's own comment on why).
    river_idx = river_idx if river_idx is not None else 2
    rp_tier = _RIVER_RP_TIERS[river_idx]

    rain_window = rain_window or "6"
    rain_idx = rain_idx if rain_idx is not None else 1
    threshold_mm = _RAIN_MM_BY_WINDOW[rain_window][rain_idx]

    # River/rain are NOT storm-scoped — each has its own independent
    # forecast_date (can genuinely lag behind the current wind storm's own
    # cycle, confirmed live) resolved straight from Snowflake, not derived
    # from `storm` at all.
    river_forecast_date = get_latest_river_forecast_time(primary_code) if primary_code else None
    rain_forecast_date = get_latest_rain_forecast_time(primary_code) if primary_code else None
    # Path-segment placeholder for hazards with no real "storm" concept —
    # any non-empty string works (ignored server-side), reuses the real
    # storm name when one is resolved rather than adding a second required
    # config field only to fill an inert URL path segment.
    placeholder_storm = storm or "NONE"

    stats_wind, admin_stats_wind = _hazard_stats(
        tile_country, "wind", storm, forecast_date, wind_kt, {}) if (wind_on and storm) else ({}, {})
    stats_gust, admin_stats_gust = _hazard_stats(
        tile_country, "gust", storm, forecast_date, wind_kt, {"gust_threshold": gust_kt}) if (gust_on and storm) else ({}, {})
    stats_river, admin_stats_river = _hazard_stats(
        tile_country, "river", placeholder_storm, river_forecast_date, wind_kt, {"rp_tier": rp_tier}) if river_on else ({}, {})
    stats_rain, admin_stats_rain = _hazard_stats(
        tile_country, "rain", placeholder_storm, rain_forecast_date, wind_kt,
        {"threshold_mm": threshold_mm, "window_h": rain_window}) if rain_on else ({}, {})

    # Real per-extra-storm-group tile stats (#248) — one extra "+"-joined
    # tile_country per distinct non-primary storm, each queried exactly like
    # the primary group above via the same _hazard_stats helper. Empty list
    # in the overwhelming common case (0 or 1 distinct storm across every
    # selected country) — maplibre_tiles.js's _applyExtraHazardGroups is a
    # complete no-op when these are empty, zero behavior change there.
    extra_wind_groups, extra_gust_groups = [], []
    for extra_storm, g in extra_groups_by_storm.items():
        group_country = "+".join(g["codes"])
        if wind_on:
            g_stats_wind, g_admin_stats_wind = _hazard_stats(group_country, "wind", extra_storm, g["forecast_date"], wind_kt, {})
            extra_wind_groups.append({
                "country": group_country, "storm": extra_storm, "forecast_date": g["forecast_date"],
                "stats": g_stats_wind, "admin_stats": g_admin_stats_wind,
            })
        if gust_on:
            g_stats_gust, g_admin_stats_gust = _hazard_stats(
                group_country, "gust", extra_storm, g["forecast_date"], wind_kt, {"gust_threshold": gust_kt})
            extra_gust_groups.append({
                "country": group_country, "storm": extra_storm, "forecast_date": g["forecast_date"],
                "stats": g_stats_gust, "admin_stats": g_admin_stats_gust,
            })

    return {
        "country": tile_country or None,
        # Single ISO3 code (first selected country), distinct from "country"
        # above which is a "+"-joined multi-country string for tile-server
        # requests — get_track_impacts (real envelope severity/coloring,
        # see _load_ms_tracks_and_envelopes below) needs exactly one real
        # COUNTRY value to match TRACK_MAT/TRACK_VULNERABILITY_MAT rows
        # against, same as river/rain's own primary_code usage just above.
        "primary_country_code": primary_code,
        "tile_server_url": "" if config.SPCS_RUN else config.TILE_SERVER_URL,
        "tile_prop": resolved_prop,
        "admin_prop": resolved_prop,
        "view_mode": view_mode,
        # "envelopes" (default, today's behavior) or "raster" — gates
        # ms-tracks-json/ms-envelopes-json in _load_ms_tracks_and_envelopes
        # below; the MapLibre wind/gust tile raster itself is untouched by
        # this and keeps rendering off wind_visible/gust_visible as before.
        "tc_view_as": tc_view_as,

        "storm": storm,
        "forecast_date": forecast_date,
        "wind_threshold": wind_kt,
        "wind_visible": bool(wind_on),
        "stats_wind": stats_wind, "admin_stats_wind": admin_stats_wind,

        "gust_threshold": gust_kt,
        "gust_visible": bool(gust_on),
        "stats_gust": stats_gust, "admin_stats_gust": admin_stats_gust,

        # #248 — any OTHER real storm among the selected countries beyond
        # the primary one above; see this function's own "Real bug
        # found+fixed here (#248)" comment and maplibre_tiles.js's
        # _applyExtraHazardGroups. Empty in the common single-storm case.
        "extra_wind_groups": extra_wind_groups,
        "extra_gust_groups": extra_gust_groups,

        "river_forecast_date": river_forecast_date,
        "rp_tier": rp_tier,
        "river_visible": bool(river_on),
        "stats_river": stats_river, "admin_stats_river": admin_stats_river,

        "rain_forecast_date": rain_forecast_date,
        "threshold_mm": threshold_mm,
        "window_h": rain_window,
        "rain_visible": bool(rain_on),
        "stats_rain": stats_rain, "admin_stats_rain": admin_stats_rain,

        # Storm Surge — explicit visual-only preview, confirmed zero real
        # backend anywhere (see this file's own ms-surge-on comment): always
        # False, no tile-server request is ever built for it.
        "surge_visible": False,
    }


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

    Note (see CLAUDE.md's "Where tooltips actually come from"): these four
    layers hold EMPTY GeoJSON on this page — same as on /legacy — so this
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
# above — those are gated on a selected country (see that function's own
# "if not countries: return ..." early exit) whereas these two raw layers
# are GLOBAL (one worldwide file per forecast cycle, no COUNTRY/STORM column
# anywhere upstream — see services/tile_server.py's own "Global raw
# precipitation-rate endpoints"/"Global raw river endpoints" sections), so
# they must render in Global mode with nothing selected at all, not just
# once a country/storm is picked.
#
# NO dedicated checkboxes here anymore — ms-river-on ("River Flooding") and
# ms-rain-on ("Rainfall") from _flood_hazards_family are reused as the single
# on/off control for these two raw layers (this callback is simply an extra,
# independent Input consumer of those same two checkbox ids — see that
# function's own header comment for the full rationale), and flood-view-as
# (also in _flood_hazards_family) resolves which aggregation mode
# (mean/probability) the now-mode-aware /tiles/raster/{precip-raw,river-raw}
# endpoints should render.
#
# (2026-07-31) forecast_time resolution for BOTH raw layers now follows the
# topbar's own date+time selection (topbar-date/topbar-time) instead of
# always "latest" — same as every other, country-scoped hazard layer already
# does via ms-tile-config-store. River (extent_rp10_bymember) is daily-only,
# so it matches on calendar date alone (get_river_extent_forecast_time_for_date);
# precip (tp) has irregular real cycle hours, so it matches to the single
# closest real cycle within a bounded window (get_precip_forecast_time_near)
# — see both functions' own docstrings in snowflake_utils.py for the full
# matching-algorithm rationale. Either layer resolving to None (no real
# forecast within the matching window for the selected date) is a real,
# expected state — surfaced via precip_date_unavailable/river_date_unavailable
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
    prevent_initial_call=False,
)
def _build_global_raw_config(river_on, rain_on, view_as, date, run, _n_intervals):
    """Resolves the forecast_time for each global raw layer — matched to the
    selected topbar-date/topbar-time, NOT always "latest" (see this block's
    own header comment above) — plus the shared Mean/Probability aggregation
    mode from flood-view-as, then hands it all off to the clientside bridge
    just below.

    prevent_initial_call=False so this fires immediately on page load (the
    explicit "shown by default on first load, no country selected" ask) —
    not just after the first ms-metadata-refresh-interval tick — using
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
    system (its own river_on/rain_on params) — this callback is a second,
    independent consumer of the same two checkboxes, not a replacement for
    that wiring. "Mean" here is a raw-layer-only visualization concept and is
    never combined with that population-impact system.
    """
    mode = view_as if view_as in ("mean", "probability") else "mean"
    date = date or _DEFAULT_FORECAST_DATE
    run = run if run is not None else _DEFAULT_FORECAST_RUN
    try:
        precip_latest = get_precip_forecast_time_near(date, run)
    except Exception as e:
        logger.warning("Could not resolve precip-raw forecast time near %s %sZ: %s", date, run, e)
        precip_latest = None
    try:
        river_latest = get_river_extent_forecast_time_for_date(date)
    except Exception as e:
        logger.warning("Could not resolve river-raw forecast time for %s: %s", date, e)
        river_latest = None

    precip_forecast_time = precip_latest[0] if precip_latest else None
    river_forecast_time = river_latest[0] if river_latest else None
    base_url = "" if config.SPCS_RUN else config.TILE_SERVER_URL

    # Both raster tile endpoints render fully pre-colored server-side (fixed
    # breakpoint ramps per mode — see _colorize_precip_rate/_colorize_river_extent_*
    # in tile_server.py), so no client-side stats call is needed here for
    # either layer's coloring.
    return {
        "tile_server_url": base_url,
        "mode": mode,
        "date": date,
        "run": run,
        "precip_forecast_time": precip_forecast_time,
        "precip_visible": bool(rain_on) and precip_forecast_time is not None,
        # Checked ON but no real cycle matched within the window — a real,
        # date-specific "not available" state, distinct from
        # _flood_hazards_family's own country-based availability check.
        "precip_date_unavailable": bool(rain_on) and precip_forecast_time is None,
        "river_forecast_time": river_forecast_time,
        "river_visible": bool(river_on) and river_forecast_time is not None,
        "river_date_unavailable": bool(river_on) and river_forecast_time is None,
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
    impact system that ms-river-on/ms-rain-on also drive) — both notes can
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


# =============================================================================
# TRACK / ENVELOPE LAYER WIRING
# Real hurricane track + wind-envelope GeoJSON for ms-tracks-json/
# ms-envelopes-json, reactive to ms-tile-config-store (the same
# country/storm/forecast_date/wind_threshold config the hazard tile layers
# already key off — see _build_hazard_tile_config above) rather than a
# "Load Layers" button, consistent with this page's reactive design.
# =============================================================================

_MS_EMPTY_FC = {"type": "FeatureCollection", "features": []}


def _ms_geojson_key(payload):
    """Content hash used as dl.GeoJSON's `key` prop so Leaflet actually
    re-renders on data change — same pattern pages/dashboard.py's own
    toggle_tracks_layer/toggle_envelopes_layer callbacks use."""
    try:
        return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    except (TypeError, ValueError):
        return str(id(payload))


def _build_ms_track_features(df_tracks, storm_name=None):
    """Build one LineString Feature per ensemble member from a TC_TRACKS
    query result. Mirrors pages/dashboard.py's load_all_layers track-building
    loop exactly (same properties: ensemble_member, member_type).

    storm_name (optional): tags every feature's properties with
    `track_id` = storm_name, matching the property name tooltip_tracks
    (components/map/javascript.py) already reads to prefix its tooltip
    label. Not needed (left None) for the single-country path, where only
    one storm is ever on the map at once so there's no ambiguity — REQUIRED
    for the Global-mode multi-storm path (_load_ms_tracks_and_envelopes
    below), where several different storms' tracks can render
    simultaneously and would otherwise be indistinguishable on hover."""
    features = []
    for member in df_tracks['ENSEMBLE_MEMBER'].unique():
        member_data = df_tracks[df_tracks['ENSEMBLE_MEMBER'] == member].sort_values('LEAD_TIME')
        coordinates = [[row['LONGITUDE'], row['LATITUDE']] for _, row in member_data.iterrows()]
        properties = {
            "ensemble_member": int(member) if pd.notna(member) else member,
            "member_type": "control" if member in (51, 52) else "ensemble",
        }
        if storm_name:
            properties["track_id"] = storm_name
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
    (get_track_impacts/TRACK_MAT vs get_gust_track_impacts/TRACK_GUST_MAT —
    both genuinely real, separately deployed tables; TC_GUST_ENVELOPES_COMBINED/
    TRACK_GUST_MAT were confirmed live after initially being missed). Every
    output feature is tagged "hazard" so style_envelopes/tooltip_envelopes
    (components/map/javascript.py) can color wind vs gust differently when
    both are shown at once.

    Real severity_population/max_population merge (when country/storm/
    forecast_date are given) via get_track_impacts()/get_gust_track_impacts()
    — matching pages/dashboard.py's own (file-based) track_views merge logic
    (its load_all_layers, ~line 2012-2071) but sourced from Snowflake directly
    instead of a track_views parquet file. No vulnerability join for gust (no
    TRACK_GUST_VULNERABILITY_MAT exists — confirmed live), so gust severity is
    population-only, same as every other gust-specific table in this app."""
    threshold_col = "gust_threshold" if hazard == "gust" else "wind_threshold"
    if envelope_df is None or envelope_df.empty or threshold_col not in envelope_df.columns:
        return dict(_MS_EMPTY_FC)
    try:
        df_f = envelope_df[envelope_df[threshold_col].astype(int) == int(wind_kt)]
    except (TypeError, ValueError):
        return dict(_MS_EMPTY_FC)

    severity_by_member = {}
    if country and storm and forecast_date:
        try:
            track_impacts = (get_gust_track_impacts(country, storm, forecast_date, int(wind_kt)) if hazard == "gust"
                              else get_track_impacts(country, storm, forecast_date, int(wind_kt)))
            # Both functions' SQL alias columns lowercase ("AS zone_id" etc.),
            # but Snowflake normalizes unquoted identifiers to uppercase, so
            # the DataFrame this actually returns has UPPERCASE columns
            # (confirmed live) — same gotcha get_impact_data's own _norm()
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
            # severity_population/max_population deliberately absent unless
            # real (below) — style_envelopes (components/map/javascript.py)
            # already renders any feature with no severity data as a
            # low-opacity consensus fill, whether that's because there's no
            # country to attribute severity to at all (Global mode) or
            # because this specific member genuinely has zero/unknown impact
            # for the selected country (Country Analysis mode) — no separate
            # flag needed to distinguish the two here.
        }
        if member_int is not None and member_int in severity_by_member:
            properties["severity_population"] = severity_by_member[member_int]
            properties["max_population"] = max_population
        features.append({
            "type": "Feature",
            "geometry": shapely_mapping(geom),
            "properties": properties,
        })
    # Sort so the HIGHEST-severity envelopes draw LAST (on top) — Leaflet
    # renders GeoJSON features in array order, later features on top of
    # earlier ones, so the raw Snowflake ORDER BY ENSEMBLE_MEMBER order
    # otherwise draws them in an arbitrary z-order unrelated to which one
    # actually matters most. Real bug found+fixed here: the same
    # severity_population value already used to COLOR each envelope
    # (darker red = higher severity) had no bearing on which envelope a
    # user could actually see when several overlapped — a low-severity
    # member drawn last could fully occlude the high-severity one
    # underneath it. Missing-severity features (country=None/Global mode,
    # or no real get_track_impacts match for that member) sort first/lowest
    # via the `or 0` default, same as their own gray/consensus fill already
    # implies "nothing definitive known here".
    features.sort(key=lambda f: f["properties"].get("severity_population") or 0)
    return {"type": "FeatureCollection", "features": features}


@callback(
    Output("ms-tracks-json", "data"),
    Output("ms-tracks-json", "key"),
    Output("ms-envelopes-json", "data"),
    Output("ms-envelopes-json", "key"),
    Input("ms-tile-config-store", "data"),
    Input("topbar-date", "value"),
    Input("topbar-time", "value"),
    Input("ms-tracks-on", "checked"),
    Input("ms-wind-on", "checked"),
    Input("ms-wind-slider", "value"),
    Input("ms-gust-on", "checked"),
    Input("ms-gust-slider", "value"),
)
def _load_ms_tracks_and_envelopes(tile_config, date, run, tracks_on, wind_on, wind_idx, gust_on, gust_idx):
    """Fetch real track/envelope GeoJSON for the placeholder ms-tracks-json/
    ms-envelopes-json layers whenever the shared ms-tile-config-store
    changes (country/storm selection or wind-threshold slider) — no
    dedicated "Load Layers" button on this page, this store already fires
    on every relevant input (_build_hazard_tile_config above).

    Reuses the exact TC_TRACKS query and get_envelope_data_snowflake()
    (components/data/snowflake_utils.py) pages/dashboard.py's own
    load_all_layers callback already uses for this — no new Snowflake
    queries are introduced here.

    Global mode (no country selected — tile_config["country"] is None, see
    _build_hazard_tile_config's own "if not countries" early return) is a
    separate branch below: ALL real storms active at the selected topbar
    date/run (_resolve_storms_for_date, the same resolver already powering
    the Global-mode Active Storms list) render as ONE combined tracks
    FeatureCollection, unconditionally (mirrors /legacy's own
    load_startup_tracks bypass-the-toggle precedent — tracks aren't gated on
    wind_visible/tc_view_as in Global mode, since there's no per-hazard
    checkbox governing "all storms" the way there is for a single selected
    storm). Envelopes DO render in Global mode when ms-wind-on is checked —
    real per-storm TC_ENVELOPES_COMBINED polygons for every active storm at
    the selected wind-severity threshold, just without per-country severity
    coloring (no single country to attribute population severity to here;
    falls back to flat gray, same as _build_ms_envelope_geojson already does
    whenever country isn't given). topbar-date/
    topbar-time are new Inputs added here (this callback previously only
    depended on ms-tile-config-store, which carries no date/run at all once
    "country" is None) purely to drive this new branch; they have no effect
    on the existing single-country branch below, which keeps reading
    date/forecast_date exclusively from tile_config as it always has.
    """
    tile_config = tile_config or {}
    # Real bug found+fixed here: "Storm Tracks" (ms-tracks-on) rendered
    # unconditionally regardless of its own checked state — it was never an
    # Input to this callback at all. Gates the TRACKS output only (both
    # branches below still compute tracks_data normally, then this checkbox
    # is applied right before each `return`) — envelopes are a separate
    # concept (gated by tc_view_as=="envelopes" already) and are untouched
    # by this checkbox either way.
    tracks_on = tracks_on is not False

    if not tile_config.get("country"):
        # get_track_ids_for_date, NOT _resolve_storms_for_date -- the latter
        # requires real nonzero MERCATOR_TILE_IMPACT_MAT impact (correct for
        # the Active Storms alert list, wrong here: a storm can have fully
        # real ensemble track data while still being far out at sea with
        # zero measurable country impact yet, confirmed live for 2026-07-31's
        # real DOLPHIN/GENEVIEVE). This answers "does a real track exist",
        # matching /legacy's own load_startup_tracks precedent (queries
        # TC_TRACKS directly, no impact join at all).
        forecast_time_str = f"{date} {run}:00:00" if date and run is not None else None
        track_ids = get_track_ids_for_date(forecast_time_str) if forecast_time_str else []
        if not track_ids:
            # Genuinely quiet period for this date — same empty
            # FeatureCollection as any other "no data" gate here, not an
            # error state.
            return _MS_EMPTY_FC, dash.no_update, _MS_EMPTY_FC, dash.no_update
        # Tuple, not a list — get_multi_storm_tracks is now @ttl_cache'd and
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
                all_features.extend(_build_ms_track_features(df_storm, storm_name=track_id))
        tracks_data = {"type": "FeatureCollection", "features": all_features}
        if not tracks_on:
            tracks_data = dict(_MS_EMPTY_FC)

        # Real per-storm wind/gust envelope polygons, one Snowflake query per
        # active storm per checked hazard (get_envelope_data_snowflake /
        # get_gust_envelope_data_snowflake — both genuinely real, separately
        # deployed tables; TC_GUST_ENVELOPES_COMBINED was initially missed,
        # confirmed live afterward) — no per-country severity coloring
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

    storm = tile_config.get("storm")
    forecast_date = tile_config.get("forecast_date")  # "YYYYMMDDHHMMSS"
    wind_kt = tile_config.get("wind_threshold")
    gust_kt = tile_config.get("gust_threshold")
    primary_country_code = tile_config.get("primary_country_code")
    wind_on_cfg = bool(tile_config.get("wind_visible"))
    gust_on_cfg = bool(tile_config.get("gust_visible"))
    # "raster" view-as hides these real track/envelope Leaflet layers so the
    # MapLibre wind/gust probability raster (already rendering independently
    # off wind_visible/gust_visible) is the sole on-map representation of the
    # hazard — same empty-FeatureCollection short-circuit as the other gates
    # here, not a separate visibility mechanism.
    #
    # Real bug found+fixed here: this used to require wind_visible
    # specifically (`not tile_config.get("wind_visible")` alone), so
    # unchecking "Sustained Wind" while keeping "Gust" checked hid TRACKS
    # and gust's own envelope too, even though gust had nothing to do with
    # that gate. Now proceeds whenever EITHER hazard is on.
    if not storm or not forecast_date or not (wind_on_cfg or gust_on_cfg) \
            or tile_config.get("tc_view_as", "envelopes") == "raster":
        return _MS_EMPTY_FC, dash.no_update, _MS_EMPTY_FC, dash.no_update

    try:
        forecast_dt_str = pd.to_datetime(forecast_date, format="%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as e:
        logger.error("Could not parse forecast_date %r for tracks/envelopes: %s", forecast_date, e)
        return _MS_EMPTY_FC, dash.no_update, _MS_EMPTY_FC, dash.no_update

    tracks_data = dict(_MS_EMPTY_FC)
    try:
        # get_tracks_for_storm (not a bare inline query anymore) — cached on
        # (storm, forecast_dt_str), same perf fix as the envelope queries
        # below: tracks don't depend on the wind/gust threshold at all, so a
        # slider tick shouldn't re-fetch them from Snowflake.
        df_tracks = get_tracks_for_storm(storm, forecast_dt_str)
        if not df_tracks.empty:
            tracks_data = {"type": "FeatureCollection", "features": _build_ms_track_features(df_tracks)}
    except Exception as e:
        logger.error("Error loading tracks for map_shell_concept (%s/%s): %s", storm, forecast_dt_str, e)

    # Wind and Gust envelopes combine into ONE FeatureCollection (each
    # feature tagged "hazard") rather than a second dl.GeoJSON layer — see
    # _build_ms_envelope_geojson's own docstring. get_gust_envelope_data_
    # snowflake/TC_GUST_ENVELOPES_COMBINED and get_gust_track_impacts/
    # TRACK_GUST_MAT are both genuinely real, separately deployed tables
    # (initially missed, confirmed live afterward — 1804/780 real rows).
    envelope_features = []
    if wind_on_cfg and wind_kt is not None:
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
    if gust_on_cfg and gust_kt is not None:
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

    if not tracks_on:
        tracks_data = dict(_MS_EMPTY_FC)
    return tracks_data, _ms_geojson_key(tracks_data), envelope_geojson, _ms_geojson_key(envelope_geojson)


# =============================================================================
# FACILITY LAYER WIRING
# Direct-browser-fetch-to-tile-server pattern, same as callbacks/overlays.py's
# _register_overlay_toggle — bypasses Dash's callback POST channel (its ~10 MB
# size limit large countries exceed) entirely. Keyed off ms-tile-config-store
# and the matching ms-facility-{layer_id}-on checkbox — the fetch fires
# whenever country/storm/threshold changes OR the checkbox is toggled,
# short-circuiting to an empty FeatureCollection while unchecked.
# =============================================================================

def _register_ms_facility_layer(layer_id: str) -> None:
    clientside_callback(
        f"""
        async function(config, checked) {{
            if (!checked || !config || !config.country || !config.storm || !config.forecast_date) {{
                return [{{"type":"FeatureCollection","features":[]}}, window.dash_clientside.no_update];
            }}
            var base = (config.tile_server_url != null && config.tile_server_url !== '')
                ? config.tile_server_url
                : window.location.origin;
            var url = base + '/geojson/facilities/{layer_id}/'
                + encodeURIComponent(config.country) + '/'
                + encodeURIComponent(config.storm) + '/'
                + encodeURIComponent(config.forecast_date)
                + '?wind_threshold=' + config.wind_threshold;
            // Counts toward window._aots_pending_fetches — a SEPARATE
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
                var resp = await fetch(url);
                if (!resp.ok) return [{{"type":"FeatureCollection","features":[]}}, window.dash_clientside.no_update];
                var geojson = await resp.json();
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
# HAZARDS LABEL — TEMPORARY HIDE-ALL PREVIEW
# Clicking the "HAZARDS" label in the command bar (_command_bar) forces every
# hazard's MapLibre layers invisible purely client-side, without touching any
# real checkbox/store — un-hiding just re-applies window._aots_tile_config
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


# =============================================================================
# TILE-SERVER CACHE PRE-WARMING
# Fires services/tile_server.py's GET /preload/ (background-thread pandas
# cache fill — see that endpoint's own docstring) for every real threshold
# value a currently-visible hazard's slider can reach, so scrubbing the
# slider afterward hits an already-warm cache instead of a cold Snowflake
# round-trip. Same "real country/storm/forecast_date present" gate and
# direct-browser-fetch-to-tile-server pattern as _register_ms_facility_layer
# above; side-effect only, reuses that callback's existing dummy-sink store
# (ms-tile-config-applied-store) rather than adding a new one.
#
# Threshold lists are embedded as JSON literals generated FROM the real
# Python module-level lists (_WIND_CATS/_RIVER_RP_TIERS/_RAIN_MM_BY_WINDOW)
# at import time, not hand-duplicated — keeps this the single source of
# truth those lists already are.
#
# Wind and Gust each read their OWN column out of the same _WIND_CATS rows
# (index [2] for wind kt, index [3] for gust kt — see _resolve_gust_kt's own
# docstring) and are independently-visible hazards with independent
# thresholds, so each is preloaded separately when visible.
# Rain only preloads the CURRENTLY selected rain_window's 3 mm tiers (not
# all 4 windows x 3 tiers) — switching window is rare relative to scrubbing
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
        // Real gust kt values (index [3]) — NOT the same as wind's (index
        // [2]); the gust preload loop below used to reuse WIND_KTS, which
        // never matches a real GUST_THRESHOLD row (see _resolve_gust_kt's
        // own docstring for the live-verified root cause).
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
            RIVER_TIERS.forEach(function(tier) {{
                fire(tileConfig.country, riverStorm, tileConfig.river_forecast_date,
                    {{wind_threshold: windThreshold, hazard: 'river', rp_tier: tier}});
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


