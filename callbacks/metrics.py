"""
Impact metrics and specific-track callbacks.

Computes and displays the three impact scenarios (deterministic/probabilistic/high) shown
in the right-hand metrics panel, controls the specific-track selector, and renders the
exceedance probability chart.

Data flow:
  load_all_layers (dashboard.py) → writes CSV/Parquet paths to stores
  update_impact_metrics           → reads tile CSV + track Parquet → fills metric cards
  update_exceedance_probability_chart → reads same track Parquet → builds plotly chart

`_giga_store` is a lazily-initialised singleton for the file/blob store. It is separate
from the `giga_store` in dashboard.py (two instances of the same underlying store — an
acknowledged limitation). Thread safety within this module is handled via double-checked
locking in `_get_store()`.
"""
import logging
import math
import os
import time

import dash
from dash import Output, Input, State, callback, html
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import numpy as np
import pandas as pd
import plotly.graph_objects as go

from components.config import config
from components.data.data_store_utils import get_data_store, get_impact_data
from components.data.snowflake_utils import get_available_wind_thresholds, get_country_totals

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION AND CONSTANTS
# Path constants derived from config and magic values extracted for maintainability.
# =============================================================================
VIEWS_DIR = config.VIEWS_DIR or "aos_views"
ROOT_DATA_DIR = config.ROOT_DATA_DIR or "geodb"
ZOOM_LEVEL = 14

# Deterministic ensemble member ID used by ECMWF EPS (always member 51)
DETERMINISTIC_MEMBER_ID = 51

# Retry settings for transient file-read failures (blob store, Parquet corruption)
_MAX_RETRIES = 3
_RETRY_DELAY = 1.0  # seconds; exponential back-off: 1s, 2s on retries 1 and 2

# Sentinel for missing / not-yet-computed impact values
_NA_VALUE = "N/A"

# Display style helpers used in specific-track callback outputs
_NONE = {"display": "none"}
_SHOW = {"display": "block"}

# =============================================================================
# DATA STORE
# Lazy singleton for the file/blob data store. Initialised once on first
# callback invocation. Thread-safe via double-checked locking.
# =============================================================================
import threading as _threading

_giga_store = None
_giga_store_lock = _threading.Lock()

def _get_store():
    """Return the module-level data store, initialising it once in a thread-safe way."""
    global _giga_store
    if _giga_store is None:
        with _giga_store_lock:
            if _giga_store is None:
                _giga_store = get_data_store()
    return _giga_store


# =============================================================================
# IMPACT METRICS CALLBACK
# Reads tile CSV (probabilistic scenario) and track Parquet (deterministic +
# high scenario) to populate the 31 metric card values in the right-hand panel.
# =============================================================================

@callback(
    [Output("population-count-low", "children"),
     Output("population-count-probabilistic", "children"),
     Output("population-count-high", "children"),
     Output("children-total-low", "children"),
     Output("children-total-probabilistic", "children"),
     Output("children-total-high", "children"),
     Output("infant-affected-low", "children"),
     Output("infant-affected-probabilistic", "children"),
     Output("infant-affected-high", "children"),
     Output("children-affected-low", "children"),
     Output("children-affected-probabilistic", "children"),
     Output("children-affected-high", "children"),
     Output("adolescent-affected-low", "children"),
     Output("adolescent-affected-probabilistic", "children"),
     Output("adolescent-affected-high", "children"),
     Output("schools-count-low", "children"),
     Output("schools-count-probabilistic", "children"),
     Output("schools-count-high", "children"),
     Output("health-count-low", "children"),
     Output("health-count-probabilistic", "children"),
     Output("health-count-high", "children"),
     Output("shelters-count-low", "children"),
     Output("shelters-count-probabilistic", "children"),
     Output("shelters-count-high", "children"),
     Output("wash-count-low", "children"),
     Output("wash-count-probabilistic", "children"),
     Output("wash-count-high", "children"),
     Output("bsm2-count-low", "children"),
     Output("bsm2-count-probabilistic", "children"),
     Output("bsm2-count-high", "children"),
     Output("high-impact-badge", "children"),
     Output("population-in-need-low", "children"),
     Output("population-in-need-probabilistic", "children"),
     Output("population-in-need-high", "children"),
     Output("children-total-in-need-low", "children"),
     Output("children-total-in-need-probabilistic", "children"),
     Output("children-total-in-need-high", "children"),
     Output("infant-in-need-low", "children"),
     Output("infant-in-need-probabilistic", "children"),
     Output("infant-in-need-high", "children"),
     Output("children-in-need-low", "children"),
     Output("children-in-need-probabilistic", "children"),
     Output("children-in-need-high", "children"),
     Output("adolescent-in-need-low", "children"),
     Output("adolescent-in-need-probabilistic", "children"),
     Output("adolescent-in-need-high", "children"),],
   [Input("storm-select", "value"),
    Input("wind-threshold-select", "value"),
    Input("effective-country-store", "data"),
    Input("forecast-date", "value"),
    Input("forecast-time", "value"),
    Input("layers-loaded-store", "data")],
    prevent_initial_call=True
)
def update_impact_metrics(storm, wind_threshold, country, forecast_date, forecast_time, layers_loaded):
    """Compute and return all 31 metric values for the three scenario columns.

    Reads two files per invocation (when available):
      - mercator_views/<country>_<storm>_<dt>_<kt>_<zoom>.csv  → probabilistic (E_*) columns
      - track_views/<country>_<storm>_<dt>_<kt>.parquet         → per-member severity columns

    Returns a 31-tuple matching the declared Outputs: (pop_low, pop_prob, pop_high, …, badge).
    Retries up to 3× on transient I/O errors with exponential back-off.
    """
    _NA46 = (_NA_VALUE,) * 46

    if not layers_loaded:
        return _NA46

    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        return _NA46

    giga_store = _get_store()

    try:
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime = f"{date_str}{time_str}00"

        filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}_{ZOOM_LEVEL}.csv"
        filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "mercator_views", filename)

        logger.debug(f"Impact metrics: Looking for file {filename}")
        logger.debug(f"Impact metrics: Full path = {filepath}")
        logger.debug(f"Impact metrics: ROOT_DATA_DIR = {ROOT_DATA_DIR}")

        low_results = {"children": _NA_VALUE, "infant": _NA_VALUE, "adolescent": _NA_VALUE, "children_total": _NA_VALUE, "schools": _NA_VALUE, "health": _NA_VALUE, "shelters": _NA_VALUE, "wash": _NA_VALUE, "population": _NA_VALUE, "built_surface_m2": _NA_VALUE}
        probabilistic_results = {"children": _NA_VALUE, "infant": _NA_VALUE, "adolescent": _NA_VALUE, "children_total": _NA_VALUE, "schools": _NA_VALUE, "health": _NA_VALUE, "shelters": _NA_VALUE, "wash": _NA_VALUE, "population": _NA_VALUE, "built_surface_m2": _NA_VALUE}
        high_results = {"children": _NA_VALUE, "infant": _NA_VALUE, "adolescent": _NA_VALUE, "children_total": _NA_VALUE, "schools": _NA_VALUE, "health": _NA_VALUE, "shelters": _NA_VALUE, "wash": _NA_VALUE, "population": _NA_VALUE, "built_surface_m2": _NA_VALUE}
        high_member_badge = _NA_VALUE
        df = None
        gdf_tracks = None
        high_impact_member = None

        if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(filepath):
            df = None
            max_retries = _MAX_RETRIES
            retry_delay = _RETRY_DELAY

            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        delay = retry_delay * (2 ** (attempt - 1))
                        logger.info(f"Impact metrics: Retry attempt {attempt + 1}/{max_retries} after {delay:.1f}s delay...")
                        time.sleep(delay)

                    df = get_impact_data('tile', giga_store, filepath,
                                         country=country, storm=storm,
                                         forecast_date=forecast_datetime,
                                         wind_threshold=int(wind_threshold))
                    break

                except Exception as e:
                    error_msg = str(e)
                    # 253002 = Snowflake stage file not yet visible after write (propagation lag)
                    # "parquet magic bytes" / "arrowinvalid" = file written but not fully flushed
                    is_retryable = (
                        "FileNotFoundError" in error_msg or
                        "No such file or directory" in error_msg or
                        "connection" in error_msg.lower() or
                        "timeout" in error_msg.lower() or
                        "253002" in error_msg or
                        "parquet magic bytes" in error_msg.lower() or
                        "arrowinvalid" in error_msg.lower() or
                        "could not open parquet" in error_msg.lower()
                    )

                    if attempt < max_retries - 1 and is_retryable:
                        logger.warning(f"Impact metrics: Retryable error (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")
                        continue
                    else:
                        logger.error(f"Impact metrics: Error reading file {filename}: {error_msg}")
                        if attempt == max_retries - 1:
                            logger.error(f"Impact metrics: Failed after {max_retries} attempts")
                        df = None
                        break

            if df is not None:
                try:
                    if 'E_school_age_population' in df.columns and not df['E_school_age_population'].isna().all():
                        probabilistic_results["children"] = df['E_school_age_population'].sum()
                    else:
                        probabilistic_results["children"] = _NA_VALUE

                    if 'E_infant_population' in df.columns and not df['E_infant_population'].isna().all():
                        probabilistic_results["infant"] = df['E_infant_population'].sum()
                    else:
                        probabilistic_results["infant"] = _NA_VALUE

                    if 'E_adolescent_population' in df.columns and not df['E_adolescent_population'].isna().all():
                        probabilistic_results["adolescent"] = df['E_adolescent_population'].sum()
                    else:
                        probabilistic_results["adolescent"] = _NA_VALUE

                    _child_parts = [v for v in [probabilistic_results["infant"], probabilistic_results["children"], probabilistic_results["adolescent"]] if v != _NA_VALUE]
                    probabilistic_results["children_total"] = sum(_child_parts) if _child_parts else _NA_VALUE

                    probabilistic_results["schools"] = df['E_num_schools'].sum() if ('E_num_schools' in df.columns and not df['E_num_schools'].isna().all()) else _NA_VALUE
                    probabilistic_results["health"] = df['E_num_hcs'].sum() if ('E_num_hcs' in df.columns and not df['E_num_hcs'].isna().all()) else _NA_VALUE
                    probabilistic_results["shelters"] = df['E_num_shelters'].sum() if ('E_num_shelters' in df.columns and not df['E_num_shelters'].isna().all()) else _NA_VALUE
                    probabilistic_results["wash"] = df['E_num_wash'].sum() if ('E_num_wash' in df.columns and not df['E_num_wash'].isna().all()) else _NA_VALUE
                    probabilistic_results["population"] = df['E_population'].sum() if ('E_population' in df.columns and not df['E_population'].isna().all()) else _NA_VALUE
                    probabilistic_results["built_surface_m2"] = df['E_built_surface_m2'].sum() if ('E_built_surface_m2' in df.columns and not df['E_built_surface_m2'].isna().all()) else _NA_VALUE

                    tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                    tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)

                    if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
                        try:
                            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                                          country=country, storm=storm,
                                                          forecast_date=forecast_datetime,
                                                          wind_threshold=int(wind_threshold))
                        except Exception as e:
                            logger.error(f"Error reading track file {tracks_filepath}: {e}")
                            gdf_tracks = pd.DataFrame()

                        if not gdf_tracks.empty and 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
                            member_totals = gdf_tracks.groupby('zone_id')['severity_population'].sum()
                            _idxmax = member_totals.idxmax()
                            high_impact_member = _idxmax if pd.notna(_idxmax) else None
                            high_member_badge = f"#{high_impact_member}" if high_impact_member is not None else ""

                            low_scenario_data = gdf_tracks[gdf_tracks['zone_id'] == DETERMINISTIC_MEMBER_ID]
                            high_scenario_data = gdf_tracks[gdf_tracks['zone_id'] == high_impact_member]

                            hc_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
                            hc_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', hc_filename)
                            hc_data_available = (config.IMPACT_DATA_SOURCE == 'SQL') or giga_store.file_exists(hc_filepath)

                            def _col(df, col):
                                """Sum a severity column; return _NA_VALUE if absent or all-null."""
                                if col not in df.columns or df[col].isna().all():
                                    return _NA_VALUE
                                return df[col].sum()

                            def _fill_scenario(data, hc_ok):
                                """Aggregate one scenario DataFrame into a flat result dict.

                                Sums each severity column; returns 0 for all keys when the
                                population column is N/A (data exists but all-null). `hc_ok`
                                gates health-centre and built-surface values — these require
                                the HC Parquet file to be present.
                                """
                                r = {}
                                r["population"]     = _col(data, 'severity_population')
                                r["children"]       = _col(data, 'severity_school_age_population')
                                r["infant"]         = _col(data, 'severity_infant_population')
                                r["adolescent"]     = _col(data, 'severity_adolescent_population')
                                _parts = [v for v in [r["infant"], r["children"], r["adolescent"]] if v != _NA_VALUE]
                                r["children_total"] = sum(_parts) if _parts else _NA_VALUE
                                r["shelters"]       = _col(data, 'severity_num_shelters')
                                r["wash"]           = _col(data, 'severity_num_wash')
                                r["built_surface_m2"] = _col(data, 'severity_built_surface_m2') if hc_ok else _NA_VALUE
                                if r["population"] == _NA_VALUE:
                                    return {**{k: 0 for k in r}, "schools": 0, "health": 0}
                                r["schools"] = _col(data, 'severity_schools')
                                r["health"]  = _NA_VALUE if not hc_ok else _col(data, 'severity_hcs')
                                return r

                            if not low_scenario_data.empty:
                                low_results.update(_fill_scenario(low_scenario_data, hc_data_available))
                            elif not gdf_tracks.empty:
                                low_results.update({k: 0 for k in low_results})

                            high_results.update(_fill_scenario(high_scenario_data, hc_data_available))

                    logger.info(f"Impact metrics: Successfully loaded {len(df)} features")
                except Exception as e:
                    logger.error(f"Impact metrics: Error processing file {filename}: {e}")
            else:
                logger.error(f"Impact metrics: Could not read file {filename} after {max_retries} attempts")
        else:
            logger.warning(f"Impact metrics: File not found {filename}")

        # PIN/CHIN sub-lines
        # SQL: in-need columns are already in df (MERCATOR_TILE_VULNERABILITY_MAT LEFT JOIN)
        #      and gdf_tracks (TRACK_VULNERABILITY_MAT LEFT JOIN) — no extra download needed.
        # Stage: read vulnerability CSV and tracks parquet directly from stage.
        pin_pop_prob = pin_children_prob = pin_infant_prob = pin_schoolage_prob = pin_adolescent_prob = ""
        pin_pop_low = pin_children_low = pin_infant_low = pin_schoolage_low = pin_adolescent_low = ""
        pin_pop_high = pin_children_high = pin_infant_high = pin_schoolage_high = pin_adolescent_high = ""
        _pin_high_impact_member = high_impact_member

        try:
            def _in_need_fmt(v):
                if v is None or isinstance(v, str):
                    return ""
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    return ""
                if not np.isfinite(fv) or fv <= 0:
                    return ""
                return f"{fv:,.0f}"

            def _row_val(row_df, col):
                if row_df.empty or col not in row_df.columns:
                    return None
                return row_df.iloc[0][col]

            if config.IMPACT_DATA_SOURCE == 'SQL':
                # Expected — from df (MERCATOR_TILE_IMPACT_MAT LEFT JOIN MERCATOR_TILE_VULNERABILITY_MAT)
                if df is not None:
                    pin_pop_prob        = _in_need_fmt(df['E_people_in_need'].sum()       if 'E_people_in_need'       in df.columns else None)
                    pin_children_prob   = _in_need_fmt(df['E_children_in_need'].sum()     if 'E_children_in_need'     in df.columns else None)
                    pin_infant_prob     = _in_need_fmt(df['E_infant_in_need'].sum()       if 'E_infant_in_need'       in df.columns else None)
                    pin_schoolage_prob  = _in_need_fmt(df['E_school_age_in_need'].sum()   if 'E_school_age_in_need'   in df.columns else None)
                    pin_adolescent_prob = _in_need_fmt(df['E_adolescent_in_need'].sum()   if 'E_adolescent_in_need'   in df.columns else None)

                # DET + Worst — from gdf_tracks (TRACK_MAT LEFT JOIN TRACK_VULNERABILITY_MAT)
                _gdf_vt = gdf_tracks if gdf_tracks is not None else pd.DataFrame()
                if not _gdf_vt.empty and 'zone_id' in _gdf_vt.columns:
                    det_row   = _gdf_vt[_gdf_vt['zone_id'] == DETERMINISTIC_MEMBER_ID]
                    worst_row = _gdf_vt[_gdf_vt['zone_id'] == _pin_high_impact_member] if isinstance(_pin_high_impact_member, (int, float, np.integer, np.floating)) else pd.DataFrame()

                    pin_pop_low        = _in_need_fmt(_row_val(det_row,   'severity_people_in_need'))
                    pin_children_low   = _in_need_fmt(_row_val(det_row,   'severity_children_in_need'))
                    pin_infant_low     = _in_need_fmt(_row_val(det_row,   'severity_infant_in_need'))
                    pin_schoolage_low  = _in_need_fmt(_row_val(det_row,   'severity_school_age_in_need'))
                    pin_adolescent_low = _in_need_fmt(_row_val(det_row,   'severity_adolescent_in_need'))

                    pin_pop_high        = _in_need_fmt(_row_val(worst_row, 'severity_people_in_need'))
                    pin_children_high   = _in_need_fmt(_row_val(worst_row, 'severity_children_in_need'))
                    pin_infant_high     = _in_need_fmt(_row_val(worst_row, 'severity_infant_in_need'))
                    pin_schoolage_high  = _in_need_fmt(_row_val(worst_row, 'severity_school_age_in_need'))
                    pin_adolescent_high = _in_need_fmt(_row_val(worst_row, 'severity_adolescent_in_need'))
            else:
                # Stage path — read vulnerability CSV and tracks parquet directly
                vuln_filename = f"{country}_{storm}_{forecast_datetime}_{ZOOM_LEVEL}_vulnerability.csv"
                vuln_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "mercator_views", vuln_filename)
                if giga_store.file_exists(vuln_filepath):
                    import io as _io
                    df_vuln = pd.read_csv(_io.BytesIO(giga_store.read_file(vuln_filepath)))
                    pin_pop_prob        = _in_need_fmt(df_vuln['E_people_in_need'].sum()       if 'E_people_in_need'       in df_vuln.columns else None)
                    pin_children_prob   = _in_need_fmt(df_vuln['E_children_in_need'].sum()     if 'E_children_in_need'     in df_vuln.columns else None)
                    pin_infant_prob     = _in_need_fmt(df_vuln['E_infant_in_need'].sum()       if 'E_infant_in_need'       in df_vuln.columns else None)
                    pin_schoolage_prob  = _in_need_fmt(df_vuln['E_school_age_in_need'].sum()   if 'E_school_age_in_need'   in df_vuln.columns else None)
                    pin_adolescent_prob = _in_need_fmt(df_vuln['E_adolescent_in_need'].sum()   if 'E_adolescent_in_need'   in df_vuln.columns else None)

                vuln_tracks_filename = f"{country}_{storm}_{forecast_datetime}_{ZOOM_LEVEL}_vulnerability_tracks.parquet"
                vuln_tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, "track_views", vuln_tracks_filename)
                if giga_store.file_exists(vuln_tracks_filepath):
                    import io as _io
                    df_vt = pd.read_parquet(_io.BytesIO(giga_store.read_file(vuln_tracks_filepath)))
                    if 'zone_id' in df_vt.columns:
                        det_row   = df_vt[df_vt['zone_id'] == DETERMINISTIC_MEMBER_ID]
                        worst_row = df_vt[df_vt['zone_id'] == _pin_high_impact_member] if isinstance(_pin_high_impact_member, (int, float, np.integer, np.floating)) else pd.DataFrame()

                        pin_pop_low        = _in_need_fmt(_row_val(det_row,   'severity_people_in_need'))
                        pin_children_low   = _in_need_fmt(_row_val(det_row,   'severity_children_in_need'))
                        pin_infant_low     = _in_need_fmt(_row_val(det_row,   'severity_infant_in_need'))
                        pin_schoolage_low  = _in_need_fmt(_row_val(det_row,   'severity_school_age_in_need'))
                        pin_adolescent_low = _in_need_fmt(_row_val(det_row,   'severity_adolescent_in_need'))

                        pin_pop_high        = _in_need_fmt(_row_val(worst_row, 'severity_people_in_need'))
                        pin_children_high   = _in_need_fmt(_row_val(worst_row, 'severity_children_in_need'))
                        pin_infant_high     = _in_need_fmt(_row_val(worst_row, 'severity_infant_in_need'))
                        pin_schoolage_high  = _in_need_fmt(_row_val(worst_row, 'severity_school_age_in_need'))
                        pin_adolescent_high = _in_need_fmt(_row_val(worst_row, 'severity_adolescent_in_need'))

        except Exception as e:
            logger.warning(f"Impact metrics: Could not load vulnerability in-need data: {e}")

        def format_value(value):
            """Format a numeric impact value for display; pass _NA_VALUE strings through unchanged.
            Shrinks font for 9-digit numbers to prevent card overflow.
            """
            if isinstance(value, str):
                return value
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return _NA_VALUE
            ceiled = math.ceil(value)
            formatted = f"{ceiled:,}"
            if ceiled >= 100_000_000:
                return html.Span(formatted, style={"fontSize": "0.85em"})
            return formatted

        return (
            format_value(low_results["population"]),
            format_value(probabilistic_results["population"]),
            format_value(high_results["population"]),
            format_value(low_results["children_total"]),
            format_value(probabilistic_results["children_total"]),
            format_value(high_results["children_total"]),
            format_value(low_results["infant"]),
            format_value(probabilistic_results["infant"]),
            format_value(high_results["infant"]),
            format_value(low_results["children"]),
            format_value(probabilistic_results["children"]),
            format_value(high_results["children"]),
            format_value(low_results["adolescent"]),
            format_value(probabilistic_results["adolescent"]),
            format_value(high_results["adolescent"]),
            format_value(low_results["schools"]),
            format_value(probabilistic_results["schools"]),
            format_value(high_results["schools"]),
            format_value(low_results["health"]),
            format_value(probabilistic_results["health"]),
            format_value(high_results["health"]),
            format_value(low_results["shelters"]),
            format_value(probabilistic_results["shelters"]),
            format_value(high_results["shelters"]),
            format_value(low_results["wash"]),
            format_value(probabilistic_results["wash"]),
            format_value(high_results["wash"]),
            format_value(low_results["built_surface_m2"]),
            format_value(probabilistic_results["built_surface_m2"]),
            format_value(high_results["built_surface_m2"]),
            high_member_badge,
            # PIN/CHIN sub-lines (15 new):
            pin_pop_low,        pin_pop_prob,        pin_pop_high,
            pin_children_low,   pin_children_prob,   pin_children_high,
            pin_infant_low,     pin_infant_prob,      pin_infant_high,
            pin_schoolage_low,  pin_schoolage_prob,  pin_schoolage_high,
            pin_adolescent_low, pin_adolescent_prob, pin_adolescent_high,
        )

    except Exception as e:
        logger.error(f"Impact metrics: Error updating metrics: {e}")
        return (_NA_VALUE,) * 46


# =============================================================================
# SPECIFIC TRACK CONTROLS
# Controls for the specific-track selector — enable/disable the button,
# populate member options, display impact numbers for the selected track.
# =============================================================================

@callback(
    Output("show-specific-track-btn", "disabled"),
    [Input("layers-loaded-store", "data")],
    prevent_initial_call=True
)
def enable_specific_track_button(layers_loaded):
    return not layers_loaded


@callback(
    [Output("specific-track-select", "data"),
     Output("specific-track-select", "style"),
     Output("specific-track-order-note", "style")],
    [Input("layers-loaded-store", "data")],
    [State("effective-country-store", "data"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value"),
     State("wind-threshold-select", "value")],
    prevent_initial_call=True
)
def populate_specific_track_options(layers_loaded, country, storm, forecast_date, forecast_time, wind_threshold):
    """Build the grouped option list for the specific-track selector.

    Members are sorted by total impacted population (descending) so the highest-impact
    tracks appear first. Member 51 (deterministic) is always pinned at the top of its
    own group. Returns empty list + hidden style when no track data is available.
    """
    giga_store = _get_store()

    if not layers_loaded or not all([country, storm, forecast_date, forecast_time, wind_threshold]):
        return [], _NONE, _NONE

    try:
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"
        tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)

        if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(tracks_filepath):
            try:
                gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                              country=country, storm=storm,
                                              forecast_date=forecast_datetime_str,
                                              wind_threshold=int(wind_threshold))
            except Exception as e:
                logger.error(f"Error reading track file {tracks_filepath}: {e}")
                gdf_tracks = pd.DataFrame()

            if not gdf_tracks.empty and 'zone_id' in gdf_tracks.columns and 'severity_population' in gdf_tracks.columns:
                member_totals = (
                    gdf_tracks[["zone_id", "severity_population"]]
                    .groupby("zone_id")["severity_population"]
                    .sum()
                    .fillna(0)
                )
                sorted_members = member_totals.sort_values(ascending=False).index.tolist()
                ordered_members = ([DETERMINISTIC_MEMBER_ID] if DETERMINISTIC_MEMBER_ID in sorted_members else []) + [m for m in sorted_members if m != DETERMINISTIC_MEMBER_ID]

                _idxmin = member_totals.idxmin()
                _idxmax = member_totals.idxmax()
                low_impact_member = _idxmin if pd.notna(_idxmin) else None
                high_impact_member = _idxmax if pd.notna(_idxmax) else None

                deterministic_items = []
                ensemble_items = []
                for member in ordered_members:
                    is_deterministic = (member == DETERMINISTIC_MEMBER_ID)
                    label_prefix = f"Deterministic #{DETERMINISTIC_MEMBER_ID}" if is_deterministic else f"Ensemble #{member}"

                    impact_indicator = ""
                    if member == low_impact_member:
                        impact_indicator = " (LOW IMPACT)"
                    elif member == high_impact_member:
                        impact_indicator = " (HIGH IMPACT)"

                    item = {"value": str(member), "label": f"{label_prefix}{impact_indicator}"}
                    if is_deterministic:
                        deterministic_items.append(item)
                    else:
                        ensemble_items.append(item)

                if deterministic_items:
                    options = [
                        {"group": "Deterministic", "items": deterministic_items},
                        {"group": "Ensemble Members (by impact)", "items": ensemble_items},
                    ]
                else:
                    options = ensemble_items

                return options, _SHOW, _SHOW

        return [], _NONE, _NONE

    except Exception as e:
        logger.error(f"Error loading specific track options: {e}")
        return [], _NONE, _NONE


# =============================================================================
# SELECTOR CHANGE WARNING
# Show an orange alert when the user changes any selector after layers are
# already loaded, prompting a reload.
# =============================================================================

@callback(
    Output('load-status', 'children', allow_duplicate=True),
    [Input('effective-country-store', 'data'),
     Input('storm-select', 'value'),
     Input('forecast-date', 'value'),
     Input('forecast-time', 'value'),
     Input('wind-threshold-select', 'value')],
    [State('layers-loaded-store', 'data')],
    prevent_initial_call='initial_duplicate'
)
def warn_on_selector_change(country, storm, forecast_date, forecast_time, wind_threshold, layers_loaded):
    if layers_loaded:
        return dmc.Alert(
            "Selection changed. Please reload layers to see updated data.",
            title="Reload Required",
            color="orange",
            variant="light"
        )
    return dash.no_update


@callback(
    Output("specific-track-info", "children"),
    [Input("specific-track-select", "value")],
    [State("effective-country-store", "data"),
     State("storm-select", "value"),
     State("forecast-date", "value"),
     State("forecast-time", "value"),
     State("wind-threshold-select", "value")],
    prevent_initial_call=True
)
def update_specific_track_info(selected_track, country, storm, forecast_date, forecast_time, wind_threshold):
    giga_store = _get_store()

    if not selected_track:
        return "Load layers first, then select a specific track to see exact impact numbers"

    try:
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime_str = f"{date_str}{time_str}00"
        tracks_filename = f"{country}_{storm}_{forecast_datetime_str}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)

        if config.IMPACT_DATA_SOURCE != 'SQL' and not giga_store.file_exists(tracks_filepath):
            return "Track data not found"

        try:
            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                          country=country, storm=storm,
                                          forecast_date=forecast_datetime_str,
                                          wind_threshold=int(wind_threshold))
        except Exception as e:
            logger.error(f"Error reading track file {tracks_filepath}: {e}")
            return f"Error loading track data: {str(e)}"

        specific_track_data = gdf_tracks[gdf_tracks['zone_id'] == int(selected_track)]

        if specific_track_data.empty:
            return f"No data found for track {selected_track}"

        total_population = specific_track_data['severity_population'].sum()
        total_schools = specific_track_data['severity_schools'].sum()
        total_health = specific_track_data['severity_hcs'].sum()

        return f"Track {selected_track}: {total_population:,.0f} people, {total_schools:,.0f} schools, {total_health:,.0f} health centers affected"

    except Exception as e:
        logger.error(f"Error loading specific track info: {e}")
        return f"Error: {str(e)}"


@callback(
    [Output("specific-track-select", "disabled"),
     Output("show-specific-track-btn", "children")],
    [Input("show-specific-track-btn", "n_clicks")],
    [State("specific-track-select", "disabled")],
    prevent_initial_call=True
)
def toggle_specific_track_mode(n_clicks, currently_disabled):
    if n_clicks and n_clicks > 0:
        if currently_disabled:
            return False, dmc.Group([
                DashIconify(icon="mdi:map-marker-path", width=16),
                dmc.Text("Hide Specific Track", ml="xs")
            ])
        else:
            return True, dmc.Group([
                DashIconify(icon="mdi:map-marker-path", width=16),
                dmc.Text("Show Specific Track", ml="xs")
            ])
    return currently_disabled, dmc.Group([
        DashIconify(icon="mdi:map-marker-path", width=16),
        dmc.Text("Show Specific Track", ml="xs")
    ])


@callback(
    Output("specific-track-select", "value"),
    [Input("specific-track-select", "disabled")],
    prevent_initial_call=True
)
def clear_specific_track_when_disabled(is_disabled):
    if is_disabled:
        return None
    return dash.no_update


@callback(
    Output("show-all-envelopes-toggle", "disabled", allow_duplicate=True),
    [Input("specific-track-select", "disabled"),
     Input("layers-loaded-store", "data")],
    prevent_initial_call='initial_duplicate'
)
def sync_show_higher_winds_disabled(specific_track_disabled, _layers_loaded):
    return bool(specific_track_disabled)


# =============================================================================
# EXCEEDANCE PROBABILITY CHART
# Build the plotly exceedance probability chart. Plots P(population > x) across
# all ensemble members and overlays higher wind-threshold curves when available.
# =============================================================================

@callback(
    [Output("exceedance-probability-chart", "figure"),
     Output("exceedance-chart-info", "children"),
     Output("exceedance-legend", "children")],
    [Input("storm-select", "value"),
     Input("wind-threshold-select", "value"),
     Input("effective-country-store", "data"),
     Input("forecast-date", "value"),
     Input("forecast-time", "value"),
     Input("layers-loaded-store", "data")],
    prevent_initial_call=True
)
def update_exceedance_probability_chart(storm, wind_threshold, country, forecast_date, forecast_time, layers_loaded):
    """Generate the exceedance probability chart and its custom legend.

    Plots P(affected population > x) for the selected wind threshold across all ensemble
    members. Additional curves are overlaid for higher thresholds when their Parquet files
    exist. The deterministic member (51) is marked with a horizontal dashed line.
    Returns (figure, info_text, legend_div).
    """
    giga_store = _get_store()

    empty_fig = go.Figure()
    empty_fig.add_annotation(
        text="Load layers to view exceedance probability chart.",
        xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=14, color="gray")
    )
    empty_fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=20, b=20)
    )
    empty_legend = html.Div()

    if not layers_loaded:
        return empty_fig, "Load layers to view exceedance probability based on ensemble forecasts.", empty_legend

    if not storm or not wind_threshold or not country or not forecast_date or not forecast_time:
        return empty_fig, "Please select all required fields (country, storm, date, time, wind threshold) and load layers.", empty_legend

    try:
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime = f"{date_str}{time_str}00"

        tracks_filename = f"{country}_{storm}_{forecast_datetime}_{wind_threshold}.parquet"
        tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', tracks_filename)

        if config.IMPACT_DATA_SOURCE != 'SQL' and not giga_store.file_exists(tracks_filepath):
            empty_fig.add_annotation(
                text="Track data file not found. Please ensure the storm data has been processed.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=12, color="orange")
            )
            return empty_fig, "Track data not found for the selected storm and wind threshold.", empty_legend

        try:
            gdf_tracks = get_impact_data('track', giga_store, tracks_filepath,
                                          country=country, storm=storm,
                                          forecast_date=forecast_datetime,
                                          wind_threshold=int(wind_threshold))
        except Exception as e:
            empty_fig.add_annotation(
                text="Track data could not be read. This may be a temporary issue.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=12, color="orange")
            )
            return empty_fig, f"Track data could not be read: {str(e)}", empty_legend

        if 'zone_id' not in gdf_tracks.columns or 'severity_population' not in gdf_tracks.columns:
            empty_fig.add_annotation(
                text="Track data does not contain ensemble member information.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=12, color="orange")
            )
            return empty_fig, "Invalid track data structure.", empty_legend

        member_data = []
        unique_members = gdf_tracks['zone_id'].unique()
        for member_id in unique_members:
            member_data_subset = gdf_tracks[gdf_tracks['zone_id'] == member_id]
            total_population = member_data_subset['severity_population'].sum() if 'severity_population' in member_data_subset.columns else float('nan')
            member_data.append({'member': member_id, 'population': total_population})

        member_df = pd.DataFrame(member_data)

        if member_df.empty:
            return empty_fig, "No ensemble member data found.", empty_legend

        values = member_df['population'].values
        member_ids = member_df['member'].values

        try:
            forecast_datetime_str = f"{forecast_date} {forecast_time}:00"
            available_wind_thresholds = get_available_wind_thresholds(storm, forecast_datetime_str)
        except Exception as e:
            logger.error(f"Error getting available wind thresholds: {e}")
            available_wind_thresholds = []

        higher_threshold_data = {}
        if available_wind_thresholds and wind_threshold:
            current_thresh_int = int(wind_threshold)
            higher_thresholds = [t for t in available_wind_thresholds if t.isdigit() and int(t) > current_thresh_int]

            for higher_thresh in sorted(higher_thresholds, key=int):
                try:
                    higher_tracks_filename = f"{country}_{storm}_{forecast_datetime}_{higher_thresh}.parquet"
                    higher_tracks_filepath = os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', higher_tracks_filename)

                    if config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(higher_tracks_filepath):
                        higher_gdf_tracks = get_impact_data('track', giga_store, higher_tracks_filepath,
                                                             country=country, storm=storm,
                                                             forecast_date=forecast_datetime,
                                                             wind_threshold=int(higher_thresh))

                        if 'zone_id' in higher_gdf_tracks.columns and len(higher_gdf_tracks) > 0:
                            higher_member_data = []
                            for member_id in higher_gdf_tracks['zone_id'].unique():
                                higher_member_subset = higher_gdf_tracks[higher_gdf_tracks['zone_id'] == member_id]
                                higher_total = higher_member_subset['severity_population'].sum() if 'severity_population' in higher_member_subset.columns else float('nan')
                                higher_member_data.append(higher_total)

                            if higher_member_data:
                                higher_threshold_data[higher_thresh] = np.array(higher_member_data)
                except Exception as e:
                    logger.error(f"Error loading higher threshold {higher_thresh}kt data: {e}")
                    continue

        fig = go.Figure()

        if len(values) == 0:
            return empty_fig, "No data available for exceedance probability calculation.", empty_legend

        n_probabilities = 100
        probability_levels = np.linspace(0, 100, n_probabilities)

        impact_thresholds = []
        for prob in probability_levels:
            percentile = 100 - prob
            threshold = np.nanpercentile(values, percentile)
            impact_thresholds.append(threshold)

        def hex_to_rgba(hex_color, alpha=0.2):
            r = int(hex_color[1:3], 16)
            g = int(hex_color[3:5], 16)
            b = int(hex_color[5:7], 16)
            return f'rgba({r}, {g}, {b}, {alpha})'

        color = '#1cabe2'
        fillcolor_rgba = hex_to_rgba(color, 0.2)
        legend_items = []

        main_label = f"{wind_threshold}kt"
        fig.add_trace(go.Scatter(
            x=probability_levels,
            y=impact_thresholds,
            mode='lines',
            name=main_label,
            line=dict(color=color, width=2.5),
            fill='tozerox',
            fillcolor=fillcolor_rgba,
            hovertemplate=f'<b>Population ({wind_threshold}kt):</b><br>Probability: %{{x:.1f}}%<br>Impact Threshold: %{{y:,.0f}}<extra></extra>',
            showlegend=False
        ))
        legend_items.append({"label": main_label, "color": color, "line_style": "solid"})

        if higher_threshold_data:
            higher_threshold_colors = {
                "40": "#5dade2", "50": "#3498db", "64": "#2980b9",
                "83": "#1f618d", "96": "#1a5490", "113": "#154360", "137": "#0b2638"
            }
            threshold_labels = {
                "34": "34kt", "40": "40kt", "50": "50kt", "64": "64kt",
                "83": "83kt", "96": "96kt", "113": "113kt", "137": "137kt"
            }

            for higher_thresh, higher_values in higher_threshold_data.items():
                if len(higher_values) > 0:
                    higher_impact_thresholds = []
                    for prob in probability_levels:
                        percentile = 100 - prob
                        threshold = np.nanpercentile(higher_values, percentile)
                        higher_impact_thresholds.append(threshold)

                    trace_color = higher_threshold_colors.get(higher_thresh, "#888888")
                    higher_label = threshold_labels.get(higher_thresh, f"{higher_thresh}kt")
                    fig.add_trace(go.Scatter(
                        x=probability_levels,
                        y=higher_impact_thresholds,
                        mode='lines',
                        name=higher_label,
                        line=dict(color=trace_color, width=2, dash='dash'),
                        hovertemplate=f'<b>{higher_label}:</b><br>Probability: %{{x:.1f}}%<br>Impact Threshold: %{{y:,.0f}}<extra></extra>',
                        showlegend=False
                    ))
                    legend_items.append({"label": higher_label, "color": trace_color, "line_style": "dash"})

        if DETERMINISTIC_MEMBER_ID in member_ids:
            member_51_idx = np.where(member_ids == DETERMINISTIC_MEMBER_ID)[0]
            if len(member_51_idx) > 0:
                member_51_val = values[member_51_idx[0]]
                exceedance_prob_51 = np.sum(values > member_51_val) / len(values) * 100
                fig.add_hline(
                    y=member_51_val,
                    line_dash="dash",
                    line_color="#ff6b35",
                    line_width=2,
                    annotation_text=f"Deterministic ({exceedance_prob_51:.1f}%)",
                    annotation_position="top right"
                )

        fig.update_layout(
            xaxis=dict(
                title=dict(text="Probability of Exceeding Threshold (%)", font=dict(size=11)),
                range=[0, 100],
                gridcolor='rgba(200, 200, 200, 0.3)',
                showline=True
            ),
            yaxis=dict(
                title=dict(text="Impact Threshold (Affected Population)", font=dict(size=11)),
                tickformat='.2s',
                gridcolor='rgba(200, 200, 200, 0.3)',
                showline=True
            ),
            plot_bgcolor='rgba(250, 250, 250, 1)',
            paper_bgcolor='white',
            margin=dict(l=45, r=20, t=20, b=50),
            height=400,
            showlegend=False
        )

        legend_cols = []
        items_per_col = (len(legend_items) + 2) // 3

        for col_idx in range(3):
            col_items = []
            start_idx = col_idx * items_per_col
            end_idx = min(start_idx + items_per_col, len(legend_items))

            for item in legend_items[start_idx:end_idx]:
                if item['line_style'] == 'solid':
                    line_style_css = f"3px solid {item['color']}"
                else:
                    line_style_css = f"2px dashed {item['color']}"

                col_items.append(
                    dmc.Group([
                        html.Div(style={
                            "width": "25px",
                            "height": "2px",
                            "borderTop": line_style_css,
                            "marginRight": "8px"
                        }),
                        dmc.Text(item['label'], size="xs", style={"fontSize": "9px"})
                    ], gap="xs", align="center", style={"marginBottom": "4px"})
                )

            if col_items:
                legend_cols.append(
                    dmc.GridCol(
                        dmc.Stack(col_items, gap="xs"),
                        span=4
                    )
                )

        custom_legend = dmc.Grid(legend_cols, gutter="sm") if legend_cols else html.Div()

        _ensemble_count = int(np.sum(member_ids != DETERMINISTIC_MEMBER_ID))
        info_text = f"Showing exceedance probability for {_ensemble_count} ensemble members at {wind_threshold}kt wind threshold."
        return fig, info_text, custom_legend

    except Exception as e:
        logger.error(f"Error generating exceedance probability chart: {e}")
        empty_fig.add_annotation(
            text=f"Error generating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=12, color="red")
        )
        return empty_fig, f"Error: {str(e)}", empty_legend


# =============================================================================
# IN-NEED ARC CHARTS
# Two concentric arc charts (Children / People):
#   outer ring  — country total population (100% = full 270° sweep)
#   middle ring — expected exposed ("at risk")
#   inner ring  — expected in need
# Angular axis: 0–100 % scale, 270° sweep, clockwise from top
# Big in-need count displayed above each chart in the panel HTML (ids:
#   in-need-number-children, in-need-number-people).
# =============================================================================

def _make_arc_chart(
    total: int | None,
    exposed: float | None,
    in_need: float | None,
    exposed_label: str = "People At Risk",
    in_need_label: str = "People In Need",
) -> go.Figure:
    """Build a 270° concentric-arc polar gauge with 3 rings."""
    _FULL_DEG = 270.0        # 100 % maps to 270 degrees
    _SCALE    = _FULL_DEG / 100.0
    _GRAY     = "#d0d5dd"
    _NAVY     = "#1c3a6e"
    _BLUE     = "#1cabe2"
    _ORANGE   = "#f59f00"
    _RING_W   = 12
    _BASES    = [53, 35, 17]  # outer / middle / inner ring inner radius
    _MIN_DEG  = 3.0           # minimum visible arc for any non-zero value

    def _pct_deg(pct: float) -> float:
        if not pct or pct <= 0:
            return 0.0
        return max(_MIN_DEG, min(_FULL_DEG, pct * _SCALE))

    no_data = total is None or total == 0
    exp_pct = (exposed / total * 100) if (not no_data and exposed) else 0.0
    nee_pct = (in_need / total * 100) if (not no_data and in_need) else 0.0

    pop_deg = _FULL_DEG if not no_data else 0.0
    exp_deg = _pct_deg(exp_pct)
    nee_deg = _pct_deg(nee_pct)

    _GAP_THETA = 357  # degrees — 3° into the gap past arc start; nearly vertical radial direction → right edges align

    ring_specs = [
        (_BASES[0], pop_deg, _NAVY,   "Population"),
        (_BASES[1], exp_deg, _BLUE,   exposed_label),
        (_BASES[2], nee_deg, _ORANGE, in_need_label),
    ]

    traces = []
    for base, fill_deg, fill_color, name in ring_specs:
        # Colored arc (filled portion)
        if fill_deg > 0:
            traces.append(go.Barpolar(
                r=[_RING_W], base=[base],
                theta=[fill_deg / 2], width=[fill_deg],
                marker_color=[fill_color], marker_line_width=0,
                showlegend=False, hoverinfo="skip",
            ))
        # Gray background (remaining portion up to 270°)
        bg_deg = _FULL_DEG - fill_deg
        if bg_deg > 0:
            traces.append(go.Barpolar(
                r=[_RING_W], base=[base],
                theta=[fill_deg + bg_deg / 2], width=[bg_deg],
                marker_color=[_GRAY], marker_line_width=0,
                showlegend=False, hoverinfo="skip",
            ))

    # Labels positioned inside the gap quarter (upper-left, 270°–360°)
    for base, _fill, _color, name in ring_specs:
        r_mid = base + _RING_W / 2
        traces.append(go.Scatterpolar(
            r=[r_mid], theta=[_GAP_THETA],
            mode="text",
            text=[name + "\u00a0\u00a0\u00a0"],
            textfont=dict(size=10, color="#333"),
            textposition="middle left",
            showlegend=False,
            hoverinfo="skip",
        ))

    tick_degs = [i * 27.0 for i in range(11)]
    tick_lbls = [str(i * 10) for i in range(11)]

    fig = go.Figure(data=traces)
    fig.update_layout(
        polar=dict(
            angularaxis=dict(
                visible=True,
                rotation=90,
                direction="clockwise",
                tickvals=tick_degs,
                ticktext=tick_lbls,
                showgrid=True,
                gridcolor="#dde3ea",
                griddash="dot",
                tickfont=dict(size=8, color="#999"),
                showline=False,
                ticks="",
            ),
            radialaxis=dict(visible=False, range=[0, 74]),
            bgcolor="white",
            domain=dict(x=[0.0, 1.0], y=[0.0, 1.0]),
        ),
        showlegend=False,
        margin=dict(l=8, r=32, t=16, b=20),
        paper_bgcolor="white",
        height=210,
    )
    return fig


def _fmt_in_need(n: float | None) -> str:
    """Format the big in-need headline number with commas (or M/B for very large)."""
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    n = int(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    return f"{n:,}"


_EMPTY_ARC = _make_arc_chart(None, None, None)


@callback(
    Output("in-need-arc-chart-people",    "figure"),
    Output("in-need-arc-chart-children",  "figure"),
    Output("in-need-number-people",       "children"),
    Output("in-need-number-children",     "children"),
    Input("storm-select",           "value"),
    Input("wind-threshold-select",  "value"),
    Input("effective-country-store", "data"),
    Input("forecast-date",          "value"),
    Input("forecast-time",          "value"),
    Input("layers-loaded-store",    "data"),
    prevent_initial_call=True,
)
def update_in_need_charts(storm, wind_threshold, country, forecast_date, forecast_time, layers_loaded):
    """Populate the two concentric-arc In Need charts and their headline numbers."""
    _empty = (_EMPTY_ARC, _EMPTY_ARC, "—", "—")
    if not (layers_loaded and storm and wind_threshold and country and forecast_date and forecast_time):
        return _empty

    try:
        date_str = forecast_date.replace('-', '')
        time_str = forecast_time.replace(':', '')
        forecast_datetime = f"{date_str}{time_str}00"

        giga_store = _get_store()
        filepath = os.path.join(
            ROOT_DATA_DIR, VIEWS_DIR, "mercator_views",
            f"{country}_{storm}_{forecast_datetime}_{wind_threshold}_{ZOOM_LEVEL}.csv",
        )

        if not (config.IMPACT_DATA_SOURCE == 'SQL' or giga_store.file_exists(filepath)):
            return _empty

        df = get_impact_data('tile', giga_store, filepath,
                             country=country, storm=storm,
                             forecast_date=forecast_datetime,
                             wind_threshold=int(wind_threshold))

        if df.empty:
            return _empty

        totals = get_country_totals(country)
        total_pop      = totals["total_population"]
        total_children = totals["total_children"]
        if total_pop is None and total_children is None:
            return _empty

        exposed_pop      = float(df['E_population'].sum()) if ('E_population' in df.columns and not df['E_population'].isna().all()) else None
        in_need_pop      = float(df['E_people_in_need'].sum())   if ('E_people_in_need'  in df.columns and not df['E_people_in_need'].isna().all())  else None
        _child_cols_present = any(c in df.columns for c in ['E_infant_population', 'E_school_age_population', 'E_adolescent_population'])
        exposed_children = (
            (float(df['E_infant_population'].sum())       if 'E_infant_population'     in df.columns else 0.0)
            + (float(df['E_school_age_population'].sum()) if 'E_school_age_population' in df.columns else 0.0)
            + (float(df['E_adolescent_population'].sum()) if 'E_adolescent_population' in df.columns else 0.0)
        ) if _child_cols_present else None
        in_need_children = float(df['E_children_in_need'].sum()) if ('E_children_in_need' in df.columns and not df['E_children_in_need'].isna().all()) else None

        fig_people   = _make_arc_chart(total_pop,      exposed_pop,      in_need_pop,
                                       "People At Risk",   "People In Need")
        fig_children = _make_arc_chart(total_children, exposed_children, in_need_children,
                                       "Children At Risk", "Children In Need")

        return fig_people, fig_children, _fmt_in_need(in_need_pop), _fmt_in_need(in_need_children)

    except Exception as e:
        logger.error(f"Error building in-need arc charts: {e}")
        return _empty
