#!/usr/bin/env python3
"""
Snowflake Utilities Module

This module provides utility functions for Snowflake operations and data retrieval.
It serves as a focused toolkit for connecting to Snowflake and retrieving hurricane data.

Key Components:
- Snowflake connection management (thread-local, SPCS OAuth + password auth)
- Hurricane track and envelope data retrieval
- Impact and base layer MAT table queries
"""

import logging
import time
import threading
import functools
from collections import OrderedDict
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import geopandas as gpd
import snowflake.connector
import warnings

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL-based cache
# ---------------------------------------------------------------------------
# Time-bounded expiry so the Dash app automatically serves fresh Snowflake
# data without a container restart.
#
# Sliding PER-ENTRY TTL, thread-safe LRU. The original version bucketed on
# int(time.time() // ttl_seconds), which meant every entry across every
# function decorated with this cache expired at the exact same instant every
# ttl_seconds — a cache stampede under any real concurrent traffic at that
# moment (found in the 2026-08 performance audit, already fixed in
# services/tile_server.py's own copy of this decorator; ported here for
# consistency since several functions here were newly wired into that same
# audit's N+1 fix). Each entry now expires ttl_seconds after IT was
# individually cached, so misses spread out over time instead of
# synchronizing.
#
# Per-key single-flight (2026-08 perf audit): a miss is still
# computed OUTSIDE the global `lock` (a slow cold key must never block
# lookups/hits on unrelated keys), but N concurrent callers on the SAME cold
# key used to each independently redo the full query (measured live: two
# identical concurrent cold calls both took 2754ms, zero sharing). `pending`
# holds one threading.Lock per key currently being computed; the first caller
# for a key becomes its "owner" and actually calls func(), every other
# concurrent caller for that exact key blocks on the owner's lock instead of
# re-querying, then re-checks the cache once unblocked (a real hit, since the
# owner just populated it). Entries are removed from `pending` as soon as
# their computation finishes (success or exception) so the dict never grows
# unbounded: it only ever holds keys with a computation genuinely in flight.

_META_TTL    = 15 * 60   # 15 min — storm list, forecast times (new storms appear promptly)
_IMPACT_TTL  = 15 * 60   # 15 min — impact queries (new pipeline output picked up within 15 min)
_BASE_TTL    = 60 * 60   # 60 min — base layers (schools/HCs/tiles — change only on re-init)


def ttl_cache(ttl_seconds: int, maxsize: int = 128):
    """LRU cache with a sliding per-entry TTL, thread-safe, single-flight per key."""
    def decorator(func):
        cache: "OrderedDict[tuple, tuple[float, object]]" = OrderedDict()
        lock = threading.Lock()
        pending: dict = {}  # key -> threading.Lock held by whichever caller is computing it

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            # Loop (not self-recursion) so a persistently-failing owner
            # under sustained concurrent load can't grow an unbounded
            # Python call stack in every still-waiting caller — each retry
            # below re-competes for ownership in place, in the same frame.
            while True:
                now = time.monotonic()
                with lock:
                    entry = cache.get(key)
                    if entry is not None and entry[0] > now:
                        cache.move_to_end(key)
                        return entry[1]
                    # Miss (or expired). Become this key's single-flight owner
                    # unless someone else already is; either way this whole
                    # branch only ever touches the dict lookups, never func()
                    # itself, so `lock` is held only briefly regardless of how
                    # slow the underlying query turns out to be.
                    key_lock = pending.get(key)
                    is_owner = key_lock is None
                    if is_owner:
                        key_lock = threading.Lock()
                        key_lock.acquire()
                        pending[key] = key_lock

                if not is_owner:
                    # Another caller is already computing this exact key;
                    # block on their lock instead of redoing the query.
                    key_lock.acquire()
                    key_lock.release()
                    with lock:
                        entry = cache.get(key)
                        if entry is not None and entry[0] > now:
                            cache.move_to_end(key)
                            return entry[1]
                    # Owner's computation raised (cache never got populated);
                    # loop back around, this time competing to become the
                    # new owner.
                    continue

                # Computed outside the lock — a slow Snowflake-backed miss on
                # one key must not block lookups/hits for every other key.
                try:
                    value = func(*args, **kwargs)
                    with lock:
                        cache[key] = (now + ttl_seconds, value)
                        cache.move_to_end(key)
                        while len(cache) > maxsize:
                            cache.popitem(last=False)
                    return value
                finally:
                    with lock:
                        pending.pop(key, None)
                    key_lock.release()

        def cache_clear():
            with lock:
                cache.clear()

        def cache_info():
            with lock:
                return {"size": len(cache), "maxsize": maxsize}

        wrapper.cache_clear = cache_clear
        wrapper.cache_info  = cache_info
        return wrapper
    return decorator

# Per-thread connection storage — each Gunicorn worker thread gets its own connection
_thread_local = threading.local()
_HEALTH_CHECK_INTERVAL = 300  # seconds — recheck liveness at most once every 5 min

# ---------------------------------------------------------------------------
# Shared query executor (2026-08 perf audit)
# ---------------------------------------------------------------------------
# Every Snowflake fan-out in this codebase (curve popups, per-country panels,
# multi-storm lookups) used to open its own throwaway `with
# ThreadPoolExecutor(...) as ex:` block at the call site: 13+ call sites.
# Each worker thread in a throwaway pool pays a fresh connect() handshake on
# a cold `_thread_local` (0.47-0.65s), and when the executor exits, that
# thread dies with its Snowflake connection still open; nothing ever calls
# .close() on it, so sessions leak until GC/server-side timeout. A cold curve
# popup alone used to spawn 8 short-lived threads = 8 handshakes + 8 leaked
# sessions.
#
# One shared, long-lived pool fixes both: worker threads never exit while
# the app runs, so each thread's `_thread_local.connection` is created once
# and reused by every future call routed to that thread, and there is no
# executor-shutdown moment to leak a connection at. Callers must use
# `get_query_executor().map(fn, items)` / `.submit(fn, item)` directly,
# NEVER as a context manager (`with get_query_executor() as ex:` would call
# `__exit__` -> `shutdown()` on this shared pool, killing it for every other
# concurrent caller in the process, not just the caller that opened it).
_SHARED_QUERY_EXECUTOR = ThreadPoolExecutor(max_workers=24, thread_name_prefix="sf-query")


def get_query_executor() -> ThreadPoolExecutor:
    """Process-wide, long-lived executor for fanning out concurrent Snowflake
    queries. Use `.map(fn, items)` or `.submit(fn, item)`; do not use as a
    context manager (see module comment above for why)."""
    return _SHARED_QUERY_EXECUTOR

def _is_connection_alive(conn):
    """Check if a Snowflake connection is still alive via a lightweight SELECT 1."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1")
        return True
    except Exception as e:
        logger.debug("Connection liveness check failed: %s", e)
        return False
    finally:
        cursor.close()


def _run_query(sql: str, params=None) -> pd.DataFrame:
    """
    Execute a SQL query against the thread-local Snowflake connection.
    On a connection-closed error (08003), resets the connection and retries once.
    """
    for attempt in range(2):
        try:
            conn = get_snowflake_connection()
            return pd.read_sql(sql, conn, params=params)
        except Exception as e:
            if attempt == 0 and ('08003' in str(e) or 'Connection is closed' in str(e)):
                _thread_local.connection = None
                _thread_local.last_health_check = 0.0
                continue
            raise

def get_snowflake_connection():
    """
    Get or create a Snowflake connection for the current thread.

    Uses threading.local() so each Gunicorn worker thread has its own
    independent connection — avoids race conditions when multiple threads
    run queries simultaneously.

    The liveness check (SELECT 1) is rate-limited to at most once every 5
    minutes per thread, rather than on every call, to avoid an extra
    Snowflake round-trip before each query.

    Supports two authentication methods:
    1. SPCS OAuth (for Snowflake Container Services):
       - Set SPCS_RUN=true
       - Token read from SPCS_TOKEN_PATH (default: /snowflake/session/token)
    2. Password Authentication (default):
       - Requires SNOWFLAKE_USER and SNOWFLAKE_PASSWORD
    """
    config.validate_snowflake_config()

    conn = getattr(_thread_local, 'connection', None)
    if conn is not None:
        last_check = getattr(_thread_local, 'last_health_check', 0.0)
        if time.monotonic() - last_check < _HEALTH_CHECK_INTERVAL:
            # Recent check passed — trust the connection
            return conn
        # Time for a periodic liveness check
        if _is_connection_alive(conn):
            _thread_local.last_health_check = time.monotonic()
            return conn
        # Connection is dead — close and fall through to reconnect
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.connection = None

    # Log once per thread on first connect
    first_connect = not getattr(_thread_local, 'connection_created', False)

    if config.SPCS_RUN:
        if first_connect:
            logger.info("Connecting to Snowflake with SPCS OAuth authentication...")
        try:
            with open(config.SPCS_TOKEN_PATH, 'r') as f:
                token = f.read().strip()
            conn_params = {
                'host': config.SNOWFLAKE_HOST,
                'port': config.SNOWFLAKE_PORT,
                'protocol': 'https',
                'account': config.SNOWFLAKE_ACCOUNT,
                'authenticator': 'oauth',
                'token': token,
                'warehouse': config.SNOWFLAKE_WAREHOUSE,
                'database': config.SNOWFLAKE_DATABASE,
                'schema': config.SNOWFLAKE_SCHEMA,
                'client_session_keep_alive': True
            }
            if first_connect:
                logger.info("Loaded OAuth token from %s", config.SPCS_TOKEN_PATH)
                logger.info("Using SPCS internal network: %s:%s", conn_params['host'], conn_params['port'])
        except Exception as e:
            raise ValueError(f"Failed to load OAuth token from {config.SPCS_TOKEN_PATH}: {str(e)}")
    else:
        if first_connect:
            logger.info("Connecting to Snowflake with password authentication...")
        conn_params = {
            'account': config.SNOWFLAKE_ACCOUNT,
            'user': config.SNOWFLAKE_USER,
            'password': config.SNOWFLAKE_PASSWORD,
            'warehouse': config.SNOWFLAKE_WAREHOUSE,
            'database': config.SNOWFLAKE_DATABASE,
            'schema': config.SNOWFLAKE_SCHEMA
        }

    try:
        conn = snowflake.connector.connect(**conn_params)
        # SPCS OAuth mode sometimes ignores the warehouse param in the connection
        # string — explicitly set it so every new thread session has a warehouse.
        if config.SPCS_RUN and config.SNOWFLAKE_WAREHOUSE:
            cur = conn.cursor()
            try:
                cur.execute(f"USE WAREHOUSE {config.SNOWFLAKE_WAREHOUSE}")
            except Exception as e:
                logger.warning("USE WAREHOUSE %s failed: %s", config.SNOWFLAKE_WAREHOUSE, e)
            finally:
                cur.close()
        if first_connect:
            logger.info("Connected to Snowflake (connection will be reused per thread)")
        _thread_local.connection = conn
        _thread_local.connection_created = True
        _thread_local.last_health_check = time.monotonic()
        return conn
    except Exception as e:
        logger.error("Failed to connect to Snowflake: %s", e)
        raise


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_available_wind_thresholds(storm, forecast_time):
    """
    Get available wind thresholds for a specific storm and forecast time from Snowflake
    
    Args:
        storm: Storm name (e.g., 'FENGSHEN')
        forecast_time: Forecast time string (e.g., '2025-10-20 00:00:00')
    
    Returns:
        List of available wind thresholds as strings, or empty list if none found
    """
    try:
        # Query to get distinct wind thresholds for the specific storm and forecast time
        query = """
        SELECT DISTINCT WIND_THRESHOLD 
        FROM TC_ENVELOPES_COMBINED 
        WHERE TRACK_ID = %s 
        AND FORECAST_TIME = %s
        ORDER BY WIND_THRESHOLD
        """
        
        df = _run_query(query, params=[storm, forecast_time])
        
        if not df.empty:
            # Convert to list of strings and sort
            thresholds = [str(int(th)) for th in df['WIND_THRESHOLD'].tolist() if pd.notna(th)]
            thresholds.sort(key=int)  # Sort numerically
            logger.debug("Found %d wind thresholds for %s at %s: %s", len(thresholds), storm, forecast_time, thresholds)
            return thresholds
        else:
            # Return empty list if no data found - don't use defaults
            logger.debug("No wind thresholds found for %s at %s", storm, forecast_time)
            return []

    except Exception as e:
        logger.error("Error getting wind thresholds from Snowflake: %s", e)
        # Return empty list on error - don't use defaults
        return []

@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_latest_forecast_time_overall():
    """
    Get the latest forecast issue time from Snowflake across all storms
    
    Returns:
        datetime: Latest forecast issue time (when the most recent forecast was issued), or None if no data found
    """
    try:
        # Query to get the most recent forecast time across all storms
        query = """
        SELECT MAX(FORECAST_TIME) as MAX_FORECAST_TIME
        FROM TC_TRACKS
        """
        
        df = _run_query(query)
        
        if not df.empty and pd.notna(df['MAX_FORECAST_TIME'].iloc[0]):
            latest_time = df['MAX_FORECAST_TIME'].iloc[0]
            return latest_time
        else:
            return None
            
    except Exception as e:
        logger.error("Error getting latest forecast time from Snowflake: %s", e)
        return None

@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_latest_river_forecast_time(country: str):
    """Latest FORECAST_TIME available in MERCATOR_TILE_RIVER_MAT for a country.

    River-flood data is NOT storm-scoped (see MERCATOR_TILE_RIVER_MAT's own schema —
    keyed by COUNTRY + FORECAST_TIME + RP_TIER only, no STORM/TRACK_ID column at all)
    so, unlike wind/gust, it can't be resolved from get_latest_forecast_time_overall()
    (that's TC_TRACKS-specific). Real production data can genuinely lag behind the
    latest wind storm cycle (confirmed live: PHL's only river data on 2026-07-30 was
    still the 2026-07-02 GloFAS run) — returns None (not a fallback date) when a
    country has no river data at all, same "don't paper over a real gap" convention
    as get_latest_forecast_time_overall's own None return.
    """
    try:
        df = _run_query(
            "SELECT MAX(FORECAST_TIME) AS MAX_FORECAST_TIME FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT WHERE COUNTRY = %s",
            params=[country],
        )
        if not df.empty and pd.notna(df['MAX_FORECAST_TIME'].iloc[0]):
            return str(df['MAX_FORECAST_TIME'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting latest river forecast time for %s: %s", country, e)
        return None


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_latest_rain_forecast_time(country: str):
    """Latest FORECAST_TIME available in MERCATOR_TILE_PRECIP_MAT for a country.

    Rainfall sibling of get_latest_river_forecast_time — same independence from
    storm/TC_TRACKS (keyed by COUNTRY + FORECAST_TIME + THRESHOLD_MM + WINDOW_H only).
    Returns None when the country has no precip data at all.
    """
    try:
        df = _run_query(
            "SELECT MAX(FORECAST_TIME) AS MAX_FORECAST_TIME FROM AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT WHERE COUNTRY = %s",
            params=[country],
        )
        if not df.empty and pd.notna(df['MAX_FORECAST_TIME'].iloc[0]):
            return str(df['MAX_FORECAST_TIME'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting latest rain forecast time for %s: %s", country, e)
        return None


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_latest_precip_forecast_time():
    """Latest FORECAST_TIME + STAGE_PATH for the raw global precip-rate Zarr.

    Unlike get_latest_river_forecast_time/get_latest_rain_forecast_time (both
    per-country, MAT-table-backed), this reads MET_FORECASTS directly — the
    raw tp Zarr is a single GLOBAL file per forecast cycle, not scoped to any
    country or storm at all (no COUNTRY/STORM column on MET_FORECASTS), so
    there's no country argument here. maxsize=1 matches get_latest_forecast_time_overall's
    own no-argument convention. Returns None (not a fallback date) when no
    PARAM='tp' row exists at all, same "don't paper over a real gap"
    convention as its per-country siblings.

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path) for the most
        recent PARAM='tp' row, or None if no data found.
    """
    try:
        df = _run_query(
            "SELECT FORECAST_TIME, STAGE_PATH FROM AOTS.TC_ECMWF.MET_FORECASTS "
            "WHERE PARAM = 'tp' ORDER BY FORECAST_TIME DESC LIMIT 1"
        )
        if not df.empty and pd.notna(df['FORECAST_TIME'].iloc[0]):
            return str(df['FORECAST_TIME'].iloc[0]), str(df['STAGE_PATH'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting latest precip forecast time: %s", e)
        return None


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_latest_river_raw_forecast_time():
    """Latest FORECAST_TIME + STAGE_PATH for the raw global river-discharge Zarr.

    NOT to be confused with get_latest_river_forecast_time (per-country, reads
    MERCATOR_TILE_RIVER_MAT for the pre-processed impact/flood-extent tiles).
    This one reads RIVER_FORECASTS directly, PARAM='dis24' only — the raw
    discharge Zarr is a single GLOBAL file per forecast cycle covering the
    whole world's river network cells, not scoped to any country or storm
    (mirrors get_latest_precip_forecast_time's own global/no-country
    convention for the tp Zarr).

    *** LEGACY (2026-07-31): services/tile_server.py's actual "river-raw"
    raster endpoint (/tiles/raster/river-raw/...) no longer uses PARAM='dis24'
    at all — it was switched to PARAM='extent_rp10_bymember' (see
    get_latest_river_extent_forecast_time below), a real RP10-matched
    flood-extent product instead of raw, unthresholded discharge. This
    function is kept only because it may still back other/legacy callers
    reading the raw dis24 series directly; it is NOT what the current
    river-raw map layer is keyed by any more. ***

    Deliberately excludes the 'extent_rp{N}_bymember' PARAM rows. maxsize=1
    matches get_latest_precip_forecast_time's own no-argument convention.
    Returns None (not a fallback date) when no PARAM='dis24' row exists at
    all, same "don't paper over a real gap" convention as its siblings.

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path) for the most
        recent PARAM='dis24' row, or None if no data found.
    """
    try:
        df = _run_query(
            "SELECT FORECAST_TIME, STAGE_PATH FROM AOTS.TC_ECMWF.RIVER_FORECASTS "
            "WHERE PARAM = 'dis24' ORDER BY FORECAST_TIME DESC LIMIT 1"
        )
        if not df.empty and pd.notna(df['FORECAST_TIME'].iloc[0]):
            return str(df['FORECAST_TIME'].iloc[0]), str(df['STAGE_PATH'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting latest river-raw forecast time: %s", e)
        return None


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_latest_river_extent_forecast_time():
    """Latest FORECAST_TIME + STAGE_PATH for the raw global river
    flood-extent per-member Parquet (PARAM='extent_rp10_bymember').

    This is the data source actually behind services/tile_server.py's
    current "river-raw" raster layer (/tiles/raster/river-raw/...) as of
    2026-07-31 — it REPLACES get_latest_river_raw_forecast_time's own
    PARAM='dis24' raw discharge with GloFAS discharge already matched
    against the real JRC historical flood-extent raster at the RP10 (10-year
    return period) tier. RP10 is used because it is the only tier confirmed
    genuinely computed (IS_STANDIN=False); RP2/RP5 are confirmed
    IS_STANDIN=True placeholder/extrapolated data and are deliberately never
    queried here.

    Unlike PARAM='dis24' (keyed by a full datetime FORECAST_TIME), this data
    is keyed by a plain DATE (e.g. "2026-07-14") — callers should not assume
    a time-of-day component is present.

    Like get_latest_river_raw_forecast_time/get_latest_precip_forecast_time,
    this is a single GLOBAL file per forecast cycle (no COUNTRY/STORM
    scoping), so there is no country argument; maxsize=1 matches their same
    no-argument convention. Returns None (not a fallback date) when no
    PARAM='extent_rp10_bymember' row exists at all.

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path) for the most
        recent PARAM='extent_rp10_bymember' row, or None if no data found.
    """
    try:
        df = _run_query(
            "SELECT FORECAST_TIME, STAGE_PATH FROM AOTS.TC_ECMWF.RIVER_FORECASTS "
            "WHERE PARAM = 'extent_rp10_bymember' ORDER BY FORECAST_TIME DESC LIMIT 1"
        )
        if not df.empty and pd.notna(df['FORECAST_TIME'].iloc[0]):
            return str(df['FORECAST_TIME'].iloc[0]), str(df['STAGE_PATH'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting latest river-extent forecast time: %s", e)
        return None


# How far the single closest real PARAM='tp' cycle is allowed to be from the
# requested topbar date/run before get_precip_forecast_time_near gives up and
# reports "not available" instead of silently showing a very stale cycle as
# if it were current. 3 days is generous enough to bridge tp's own irregular
# real cadence (confirmed live: cycles land at varying hours, sometimes days
# apart) while still refusing a cycle that's clearly unrelated to what was
# asked for.
@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_precip_forecast_time_near(target_date: str, target_time: str = "00"):
    """Real FORECAST_TIME + STAGE_PATH in MET_FORECASTS (PARAM='tp') for the
    EXACT topbar date/run requested, for the raw global precip-rate raster
    layer.

    Despite the function's own name (kept for caller-compatibility -- "near"
    is now a misnomer left over from an earlier fuzzy-matching design),
    matching is now EXACT date+time only, per explicit product decision: a
    real tp cycle either exists for precisely the selected date+run or the
    layer reports "not available" and gets greyed out/deselected -- no
    silent substitution of the nearest different cycle, which would show
    data for a time the user didn't actually select without any indication.

    Args:
        target_date: 'YYYY-MM-DD' (topbar-date's own value format).
        target_time: '00'/'06'/'12'/'18' (topbar-time's own run value
            format). Defaults to '00' only as a defensive floor — callers
            should always pass the live topbar-time value.

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path) for the exact
        real PARAM='tp' row at this date+run, or None if no real cycle
        exists at exactly that timestamp.
    """
    target_dt = f"{target_date} {target_time}:00:00"
    try:
        df = _run_query(
            "SELECT FORECAST_TIME, STAGE_PATH FROM AOTS.TC_ECMWF.MET_FORECASTS "
            "WHERE PARAM = 'tp' AND FORECAST_TIME = TO_TIMESTAMP_NTZ(%s) "
            "LIMIT 1",
            params=[target_dt],
        )
        if df.empty or pd.isna(df['FORECAST_TIME'].iloc[0]):
            return None
        return str(df['FORECAST_TIME'].iloc[0]), str(df['STAGE_PATH'].iloc[0])
    except Exception as e:
        logger.error("Error getting exact precip-raw forecast time for %s: %s", target_dt, e)
        return None


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_river_extent_forecast_time_for_date(target_date: str, rp_tier: str = "rp10"):
    """Real FORECAST_TIME + STAGE_PATH in RIVER_FORECASTS
    (PARAM='extent_{rp_tier}_bymember') for a given topbar date, for the raw
    global river flood-extent raster layer.

    `rp_tier` (default "rp10", matching the slider's own default): real bug
    fixed here — this used to be hardcoded to rp10 regardless of which
    return-period tier the ms-river-slider was actually set to. All 6 real
    tiers (rp2/rp5/rp10/rp20/rp50/rp100) are generated together per pipeline
    run for a given date (confirmed live), so in practice every tier
    resolves to the same forecast_time for the same date — but this is
    parameterized properly rather than assuming that always holds.

    Unlike precip's irregular cycle hours (see get_precip_forecast_time_near
    above), extent_rp10_bymember cycles are DAILY only (confirmed live: one
    real cycle per calendar day, e.g. 2026-07-02, 2026-07-13, 2026-07-14) —
    so all four topbar-time values (00Z/06Z/12Z/18Z) for a given topbar-date
    resolve to that SAME day's single real cycle when one exists; there's no
    "nearest hour" concept needed the way precip has. Matches on calendar
    date (CAST(...AS DATE), not a hardcoded literal list) so this keeps
    working generally as more real dates land, not just for today's three
    known ones.

    Args:
        target_date: 'YYYY-MM-DD' (topbar-date's own value format).

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path) — forecast_time
        is the EXACT value stored in RIVER_FORECASTS (a plain date string,
        e.g. '2026-07-14', matching get_latest_river_extent_forecast_time's
        own convention so tile_server.py's exact-match by-time lookup keeps
        working unchanged) for the real extent_rp10_bymember row matching
        target_date's calendar day, or None if no such real row exists.
    """
    try:
        df = _run_query(
            "SELECT FORECAST_TIME, STAGE_PATH FROM AOTS.TC_ECMWF.RIVER_FORECASTS "
            "WHERE PARAM = %s AND CAST(FORECAST_TIME AS DATE) = TO_DATE(%s) "
            "ORDER BY FORECAST_TIME DESC LIMIT 1",
            params=[f"extent_{rp_tier}_bymember", target_date],
        )
        if not df.empty and pd.notna(df['FORECAST_TIME'].iloc[0]):
            return str(df['FORECAST_TIME'].iloc[0]), str(df['STAGE_PATH'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting river-extent forecast time for date %s (rp_tier=%s): %s", target_date, rp_tier, e)
        return None


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_envelope_data_snowflake(track_id, forecast_time):
    """Get envelope data directly from Snowflake.

    Real perf bug found+fixed here: this query is NOT threshold-scoped — it
    always fetches every WIND_THRESHOLD x ENSEMBLE_MEMBER row for the given
    track/forecast_time (the caller, _build_ms_envelope_geojson, filters to
    one threshold client-side afterward) — yet had no caching at all, so
    every single threshold-slider tick re-ran this same full-dataset
    ST_ASWKT() query against Snowflake instead of reusing the identical
    already-fetched rows. Caching on (track_id, forecast_time) is exactly
    correct since the result never depends on the threshold at all.
    """
    try:
        # Use ST_ASWKT() to ensure we get WKT format, not raw GEOGRAPHY type
        query = '''
        SELECT
            ENSEMBLE_MEMBER,
            WIND_THRESHOLD,
            ST_ASWKT(ENVELOPE_REGION) AS ENVELOPE_REGION
        FROM TC_ENVELOPES_COMBINED
        WHERE TRACK_ID = %s AND FORECAST_TIME = %s
        ORDER BY ENSEMBLE_MEMBER, WIND_THRESHOLD
        '''
        df = _run_query(query, params=[track_id, str(forecast_time)])
        if not df.empty:
            df = df.rename(columns={'ENSEMBLE_MEMBER': 'ensemble_member', 'ENVELOPE_REGION': 'geometry', 'WIND_THRESHOLD': 'wind_threshold'})
            return df.copy()
        return pd.DataFrame()
    except Exception as e:
        logger.error("Error getting envelope data from Snowflake: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_gust_envelope_data_snowflake(track_id, forecast_time):
    """Real per-member GUST envelope polygons — TC_GUST_ENVELOPES_COMBINED,
    exact mirror of get_envelope_data_snowflake/TC_ENVELOPES_COMBINED above
    but keyed by GUST_THRESHOLD (confirmed live: a genuinely real, separately
    deployed table — 1804 rows, real BAVI/PHL rows at gust thresholds
    17-70kt — not a gap that was ever missing, just not queried by this app
    before now). Cached on (track_id, forecast_time) for the same reason as
    get_envelope_data_snowflake above — not threshold-scoped, so every
    gust-slider tick was re-fetching the identical full dataset."""
    try:
        query = '''
        SELECT
            ENSEMBLE_MEMBER,
            GUST_THRESHOLD,
            ST_ASWKT(ENVELOPE_REGION) AS ENVELOPE_REGION
        FROM TC_GUST_ENVELOPES_COMBINED
        WHERE TRACK_ID = %s AND FORECAST_TIME = %s
        ORDER BY ENSEMBLE_MEMBER, GUST_THRESHOLD
        '''
        df = _run_query(query, params=[track_id, str(forecast_time)])
        if not df.empty:
            df = df.rename(columns={'ENSEMBLE_MEMBER': 'ensemble_member', 'ENVELOPE_REGION': 'geometry', 'GUST_THRESHOLD': 'gust_threshold'})
            return df.copy()
        return pd.DataFrame()
    except Exception as e:
        logger.error("Error getting gust envelope data from Snowflake: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_gust_track_ids_for_date(forecast_time: str) -> list:
    """DISTINCT TRACK_IDs with real TC_GUST_ENVELOPES_COMBINED data at this
    exact forecast_time — the gust-availability sibling of
    get_track_ids_for_date (TC_TRACKS). Gust is storm-scoped (unlike River/
    Rain, which are country+date-scoped with no storm dimension at all), so
    "is gust available" means "does THIS storm at THIS forecast_time have
    any real gust envelope rows", not a country-wide latest-date lookup.

    Confirmed live: gust data only exists for BAVI/MAYSAK/DOUGLAS between
    2026-07-02 and 2026-07-05 — the latest real forecast cycle (2026-07-31)
    has zero TC_GUST_ENVELOPES_COMBINED rows for either active storm
    (DOLPHIN/GENEVIEVE), so Gust must show as genuinely unavailable there,
    not just "unchecked".

    Returns [] when there's genuinely no real gust data at this exact
    timestamp.
    """
    try:
        df = _run_query(
            "SELECT DISTINCT TRACK_ID FROM TC_GUST_ENVELOPES_COMBINED WHERE FORECAST_TIME = %s ORDER BY TRACK_ID",
            params=[forecast_time],
        )
        return df['TRACK_ID'].tolist() if not df.empty else []
    except Exception as e:
        logger.warning("get_gust_track_ids_for_date failed for %s: %s", forecast_time, e)
        return []


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_active_countries():
    """
    Get active countries from PIPELINE_COUNTRIES table in Snowflake
    
    Returns:
        pandas.DataFrame: DataFrame with columns COUNTRY_CODE, COUNTRY_NAME, CENTER_LAT, CENTER_LON, VIEW_ZOOM, ZOOM_LEVEL
        Returns empty DataFrame on error
    """
    try:
        # Get active countries from PIPELINE_COUNTRIES table
        query = '''
        SELECT
            COUNTRY_CODE,
            COUNTRY_NAME,
            CENTER_LAT,
            CENTER_LON,
            VIEW_ZOOM,
            ZOOM_LEVEL,
            COALESCE(IS_REGION, FALSE) AS IS_REGION,
            MEMBER_CODES
        FROM PIPELINE_COUNTRIES
        WHERE ACTIVE = TRUE
        ORDER BY COUNTRY_CODE
        '''
        
        df = _run_query(query)
        
        if not df.empty:
            logger.info("Loaded %d active countries from PIPELINE_COUNTRIES", len(df))
        else:
            logger.warning("No active countries found in PIPELINE_COUNTRIES table")

        return df.copy()

    except Exception as e:
        logger.error("Error getting active countries from Snowflake: %s", e, exc_info=True)
        return pd.DataFrame(columns=['COUNTRY_CODE', 'COUNTRY_NAME', 'CENTER_LAT', 'CENTER_LON', 'VIEW_ZOOM', 'ZOOM_LEVEL', 'IS_REGION', 'MEMBER_CODES'])


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_lat_lons_bulk() -> pd.DataFrame:
    """
    Fetch LATITUDE/LONGITUDE at LEAD_TIME=0 for every storm in TC_TRACKS.

    Single query replaces the N-per-storm loop used at dashboard startup.
    Cached so repeated calls (e.g. hot-reload) hit memory instead of Snowflake.

    Returns:
        pandas.DataFrame with columns: TRACK_ID, FORECAST_TIME, latitude, longitude
    """
    try:
        query = """
        SELECT DISTINCT TRACK_ID, FORECAST_TIME, LATITUDE, LONGITUDE
        FROM TC_TRACKS
        WHERE LEAD_TIME = 0
        """
        df = _run_query(query)
        df = df.rename(columns={'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'})
        logger.info("Loaded lat/lons for %d storm/forecast combinations in one query", len(df))
        return df.copy()
    except Exception as e:
        logger.error("Error in get_lat_lons_bulk: %s", e)
        return pd.DataFrame(columns=['TRACK_ID', 'FORECAST_TIME', 'latitude', 'longitude'])


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_multi_storm_tracks(storm_forecast_pairs) -> pd.DataFrame:
    """Fetch TC_TRACKS rows for MULTIPLE (storm, forecast_time) pairs in a
    single Snowflake round trip — the Global-mode ("no country selected")
    replacement for looping a single-storm TC_TRACKS query once per active
    storm.

    Real perf bug found+fixed here: uncached, so every Global-mode callback
    re-fire (e.g. a threshold-slider tick, which doesn't change which
    tracks exist at all) re-ran this query from scratch. Callers MUST pass
    a tuple, not a list, for storm_forecast_pairs — @ttl_cache's underlying
    lru_cache needs every arg hashable.

    Args:
        storm_forecast_pairs: tuple of (track_id, forecast_time) tuples.
            track_id is TC_TRACKS' own TRACK_ID (the storm name). forecast_time
            is "YYYY-MM-DD HH:MM:SS" (TC_TRACKS' own FORECAST_TIME column
            format). Each storm carries its OWN forecast_time here rather than
            one shared timestamp for all of them — different storms are not
            guaranteed to share one real forecast cycle even when both are
            "active" on the same selected calendar date/run.

    Builds one compound WHERE ((TRACK_ID = %s AND FORECAST_TIME = %s) OR ...)
    rather than issuing one query per storm, so this stays a single query
    regardless of how many storms are active at once.

    Returns:
        pandas.DataFrame with columns TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME,
        LEAD_TIME, LATITUDE, LONGITUDE, WIND_SPEED_KNOTS, PRESSURE_HPA — the
        extra TRACK_ID column (vs. the single-storm track query used
        elsewhere, which doesn't need it since it's already scoped to one
        storm) lets callers attribute each row back to its own storm when
        combining several storms' tracks into one FeatureCollection.
        Empty DataFrame (not an exception) when storm_forecast_pairs is empty
        or the query fails.
    """
    if not storm_forecast_pairs:
        return pd.DataFrame(columns=['TRACK_ID', 'ENSEMBLE_MEMBER', 'VALID_TIME', 'LEAD_TIME',
                                      'LATITUDE', 'LONGITUDE', 'WIND_SPEED_KNOTS', 'PRESSURE_HPA'])
    conditions = []
    params = []
    for track_id, forecast_time in storm_forecast_pairs:
        conditions.append("(TRACK_ID = %s AND FORECAST_TIME = %s)")
        params.extend([track_id, forecast_time])
    query = f'''
    SELECT
        TRACK_ID,
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME,
        LATITUDE,
        LONGITUDE,
        WIND_SPEED_KNOTS,
        PRESSURE_HPA
    FROM TC_TRACKS
    WHERE {" OR ".join(conditions)}
    ORDER BY TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME
    '''
    try:
        df = _run_query(query, params=params)
        logger.info("Loaded %d track rows for %d storm(s) in one query", len(df), len(storm_forecast_pairs))
        return df.copy()
    except Exception as e:
        logger.error("Error in get_multi_storm_tracks for %d storm(s): %s", len(storm_forecast_pairs), e)
        return pd.DataFrame(columns=['TRACK_ID', 'ENSEMBLE_MEMBER', 'VALID_TIME', 'LEAD_TIME',
                                      'LATITUDE', 'LONGITUDE', 'WIND_SPEED_KNOTS', 'PRESSURE_HPA'])


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_tracks_for_storm(storm: str, forecast_time: str) -> pd.DataFrame:
    """Single-storm TC_TRACKS query — the Country-Analysis-mode counterpart
    of get_multi_storm_tracks above (that one batches several storms into
    one query for Global mode; this is the single-storm case used once a
    country is selected).

    Real perf bug found+fixed here: this query used to be inlined directly
    in _load_ms_tracks_and_envelopes (pages/map_shell_concept.py) as a bare
    pd.read_sql call with no caching at all — tracks don't depend on the
    wind/gust threshold slider in any way, yet every slider tick re-fired
    this exact same query for the identical (storm, forecast_time), on top
    of the identically-uncached envelope queries (get_envelope_data_
    snowflake / get_gust_envelope_data_snowflake, also fixed alongside this
    one). Caching on (storm, forecast_time) is exactly correct since the
    result never depends on threshold at all.
    """
    try:
        query = '''
        SELECT
            ENSEMBLE_MEMBER,
            VALID_TIME,
            LEAD_TIME,
            LATITUDE,
            LONGITUDE,
            WIND_SPEED_KNOTS,
            PRESSURE_HPA
        FROM TC_TRACKS
        WHERE TRACK_ID = %s AND FORECAST_TIME = %s
        ORDER BY ENSEMBLE_MEMBER, VALID_TIME
        '''
        return _run_query(query, params=[storm, forecast_time])
    except Exception as e:
        logger.error("Error in get_tracks_for_storm (%s/%s): %s", storm, forecast_time, e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_track_ids_for_date(forecast_time: str) -> list:
    """DISTINCT TRACK_IDs with real TC_TRACKS data at this exact forecast_time
    ("YYYY-MM-DD HH:MM:SS") -- deliberately queries TC_TRACKS directly with NO
    impact-data requirement, unlike get_storms_and_countries_for_date (which
    requires real nonzero MERCATOR_TILE_IMPACT_MAT impact and is correctly
    scoped for the Active Storms alert-worthy list, not "does this storm
    exist"). A storm can have completely real ensemble track data while still
    being far out at sea with zero measurable country impact yet (confirmed
    live: 2026-07-31 00:00:00 has 2 real storms, DOLPHIN and GENEVIEVE, with
    zero MERCATOR_TILE_IMPACT_MAT rows for either) -- the Global-mode "show
    every real track" feature needs exactly this track-existence question,
    matching /legacy's own real precedent (load_startup_tracks, pages/
    dashboard.py:324-397, queries TC_TRACKS directly with no impact join at
    all).

    Returns [] when there's genuinely no real track data at this exact
    timestamp.
    """
    try:
        df = _run_query(
            "SELECT DISTINCT TRACK_ID FROM TC_TRACKS WHERE FORECAST_TIME = %s ORDER BY TRACK_ID",
            params=[forecast_time],
        )
        return df['TRACK_ID'].tolist() if not df.empty else []
    except Exception as e:
        logger.warning("get_track_ids_for_date failed for %s: %s", forecast_time, e)
        return []


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_snowflake_data():
    """Get hurricane metadata directly from Snowflake"""
    try:
        
        # Get unique storm/forecast combinations from TC_TRACKS
        query = '''
        SELECT DISTINCT 
            TRACK_ID,
            FORECAST_TIME,
            COUNT(DISTINCT ENSEMBLE_MEMBER) as ENSEMBLE_COUNT
        FROM TC_TRACKS
        GROUP BY TRACK_ID, FORECAST_TIME
        ORDER BY FORECAST_TIME DESC, TRACK_ID
        '''
        
        df = _run_query(query)
        
        return df.copy()
        
    except Exception as e:
        logger.error("Error getting Snowflake data: %s", e)
        return pd.DataFrame({'TRACK_ID': [], 'FORECAST_TIME': [], 'ENSEMBLE_COUNT': []})


# ---------------------------------------------------------------------------
# Active storm indicator
# ---------------------------------------------------------------------------

@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_active_storm_countries() -> list:
    """
    Return ISO3 country codes with meaningful storm impact in the last 12 hours (UTC).

    Criteria: most recent FORECAST_DATE within 12h of now AND at least one tile has
    non-zero expected impact (population, schools, or HCs) for that specific forecast
    date. Pure probability hits on uninhabited ocean tiles are excluded.
    Timezone-independent — comparison always done in UTC via CONVERT_TIMEZONE.

    Real perf fix (2026-08 performance audit): this used to have no WHERE
    predicate at all before the GROUP BY, so Snowflake scanned the entire
    206M-row MERCATOR_TILE_IMPACT_MAT and only narrowed to the last 12h
    afterwards, in the HAVING clause; it was the slowest of the 3 parallel
    startup-pool queries as a result. FORECAST_DATE is a 'YYYYMMDDHH24MISS'
    string, which sorts lexicographically the same as it sorts chronologically,
    so a plain string >= comparison against a cutoff computed the same way
    (DATEADD/CONVERT_TIMEZONE, still Snowflake's own clock, not the app
    server's) works as a real WHERE predicate and enables micro-partition
    pruning on both scans below. Any row that's a country's true MAX(FORECAST_DATE)
    and within 12h of now is, by definition, >= (now - 12h), so this WHERE
    cutoff can't exclude a country the old HAVING would have kept, which is
    why the HAVING in the `latest` CTE is now redundant and dropped rather
    than kept as a belt-and-suspenders check.
    """
    query = """
        WITH bounds AS (
            SELECT TO_VARCHAR(
                DATEADD('hour', -12, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())),
                'YYYYMMDDHH24MISS'
            ) AS cutoff
        ),
        latest AS (
            SELECT t.COUNTRY, MAX(t.FORECAST_DATE) AS latest_forecast
            FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT t
            CROSS JOIN bounds b
            WHERE t.FORECAST_DATE >= b.cutoff
            GROUP BY t.COUNTRY
        )
        SELECT l.COUNTRY
        FROM latest l
        JOIN AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT m
            ON m.COUNTRY = l.COUNTRY
           AND m.FORECAST_DATE = l.latest_forecast
        GROUP BY l.COUNTRY
        HAVING SUM(m.E_POPULATION) > 0
            OR SUM(m.E_NUM_SCHOOLS) > 0
            OR SUM(m.E_NUM_HCS) > 0
    """
    try:
        rows = _run_query(query)
        return [r['COUNTRY'] for r in rows.to_dict('records')] if not rows.empty else []
    except Exception as e:
        logger.warning("get_active_storm_countries failed: %s", e)
        return []


@ttl_cache(ttl_seconds=_META_TTL, maxsize=256)
def get_storms_for_country_date(country: str, forecast_date: str) -> list:
    """DISTINCT STORM values with real tile-impact data for `country` at
    `forecast_date` ("YYYYMMDDHH24MISS" string, e.g. "20251028000000").

    Answers "what storm/forecast applies to THIS selected country + date/run"
    reactively — a different question from get_active_storm_countries() above
    (which only ever answers "what's active in the last 12h", used for the
    Global-mode Active Storms list). This is the one to use for anything
    scoped to the currently-selected country + topbar date/run, including
    historical dates with no currently-active storm at all (e.g. a demo
    scenario or the date picker pointed at a past event).

    Returns [] (not a fallback) when there's genuinely no real data for that
    country/date combination.
    """
    try:
        df = _run_query(
            "SELECT DISTINCT STORM FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT "
            "WHERE COUNTRY = %s AND FORECAST_DATE = %s",
            params=[country, forecast_date],
        )
        return [r['STORM'] for r in df.to_dict('records')] if not df.empty else []
    except Exception as e:
        logger.warning("get_storms_for_country_date failed for %s/%s: %s", country, forecast_date, e)
        return []


@ttl_cache(ttl_seconds=_META_TTL, maxsize=256)
def get_storms_and_countries_for_date(forecast_date: str, wind_threshold: int = 50) -> list:
    """DISTINCT (STORM, COUNTRY) pairs with MEANINGFUL real tile-impact data
    at `forecast_date` ("YYYYMMDDHH24MISS" string) AND a single, specific
    `wind_threshold` (kt) — every storm affecting any country on this exact
    date/threshold, in one query, not looped per-country.

    Powers a date-reactive "Active Storms" list: unlike
    get_active_storm_countries() (always "last 12h", used only for the
    genuinely-live signal), this answers "what storms have real data on
    THIS specific date" for any date at all — including a historical one
    with nothing currently live (e.g. a Demo Scenario or the date picker
    pointed at a past event) — so the same bordered storm-row UI can show
    real historical storms too, not just live ones.

    `wind_threshold` MUST be pinned to a single value, not summed/grouped
    across every threshold this table has (34/40/50/64/83/96/113/137kt) —
    each is its own separate exceedance estimate (population exposed to AT
    LEAST that wind speed), so summing across all of them isn't a real
    total, it's 8 overlapping estimates added together (confirmed live: this
    inflated MELISSA/2025-10-28's Jamaica figure to a fabricated ~3.05M
    instead of the real ~237K at 50kt). Defaults to 50kt, this page's own
    established "today's baseline" convention (_resolve_wind_kt's default)
    — not reactive to ms-wind-slider here, since threading that in would
    create a circular Dash dependency (the slider itself is rendered INSIDE
    this same section's own output).

    Same non-zero-impact HAVING clause as get_active_storm_countries() above
    (SUM(E_population) > 0 OR SUM(E_num_schools) > 0 OR SUM(E_num_hcs) > 0) —
    without it, a wide low-probability forecast-cone tile technically exists
    for many nearby countries even when the real impact there is negligible
    (confirmed live: at 50kt this still correctly excludes Nicaragua, whose
    only nonzero signal was at the much looser 34kt threshold).

    Returns [] when there's genuinely no real data for this date/threshold.
    """
    try:
        df = _run_query(
            "SELECT STORM, COUNTRY FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT "
            "WHERE FORECAST_DATE = %s AND WIND_THRESHOLD = %s "
            "GROUP BY STORM, COUNTRY "
            "HAVING SUM(E_population) > 0 OR SUM(E_num_schools) > 0 OR SUM(E_num_hcs) > 0",
            params=[forecast_date, wind_threshold],
        )
        return df.to_dict('records') if not df.empty else []
    except Exception as e:
        logger.warning("get_storms_and_countries_for_date failed for %s: %s", forecast_date, e)
        return []


# ---------------------------------------------------------------------------
# Impact data queries — *_MAT tables
# ---------------------------------------------------------------------------

@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_school_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int) -> pd.DataFrame:
    """
    Query SCHOOL_IMPACT_MAT for school-level impact data.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        wind_threshold: Wind speed threshold in knots (e.g. 34)

    Returns:
        pandas.DataFrame with columns: SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY,
        ZONE_ID, LATITUDE, LONGITUDE, COUNTRY_ISO3_CODE
    """
    try:
        query = """
        SELECT
            SCHOOL_NAME,
            EDUCATION_LEVEL,
            PROBABILITY,
            ZONE_ID,
            LATITUDE,
            LONGITUDE,
            COUNTRY_ISO3_CODE
        FROM AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold])
        logger.info("Loaded %d school impact rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, wind_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying SCHOOL_IMPACT_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_hc_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int) -> pd.DataFrame:
    """
    Query HC_IMPACT_MAT for health centre impact data.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        wind_threshold: Wind speed threshold in knots (e.g. 34)

    Returns:
        pandas.DataFrame with columns: NAME, HEALTH_AMENITY_TYPE, AMENITY,
        OPERATIONAL_STATUS, BEDS, EMERGENCY, ELECTRICITY, OPERATOR_TYPE,
        PROBABILITY, ZONE_ID
    """
    try:
        query = """
        SELECT
            NAME,
            HEALTH_AMENITY_TYPE,
            AMENITY,
            OPERATIONAL_STATUS,
            BEDS,
            EMERGENCY,
            ELECTRICITY,
            OPERATOR_TYPE,
            PROBABILITY,
            ZONE_ID,
            ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LATITUDE,
            ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LONGITUDE
        FROM AOTS.TC_ECMWF.HC_IMPACT_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold])
        logger.info("Loaded %d HC impact rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, wind_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying HC_IMPACT_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_shelter_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int) -> pd.DataFrame:
    """
    Query SHELTER_IMPACT_MAT for shelter-level impact data.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        wind_threshold: Wind speed threshold in knots (e.g. 34)

    Returns:
        pandas.DataFrame with columns: NAME, TYPE, CATEGORY, PROBABILITY, ZONE_ID, LATITUDE, LONGITUDE
    """
    try:
        query = """
        SELECT
            NAME,
            SHELTER_TYPE,
            CATEGORY,
            PROBABILITY,
            ZONE_ID,
            LATITUDE,
            LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_IMPACT_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold])
        logger.info("Loaded %d shelter impact rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, wind_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying SHELTER_IMPACT_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_wash_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int) -> pd.DataFrame:
    """
    Query WASH_IMPACT_MAT for WASH facility impact data.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        wind_threshold: Wind speed threshold in knots (e.g. 34)

    Returns:
        pandas.DataFrame with columns: NAME, TYPE, CATEGORY, PROBABILITY, ZONE_ID, LATITUDE, LONGITUDE
    """
    try:
        query = """
        SELECT
            NAME,
            WASH_TYPE,
            CATEGORY,
            PROBABILITY,
            ZONE_ID,
            LATITUDE,
            LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_IMPACT_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold])
        logger.info("Loaded %d WASH impact rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, wind_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying WASH_IMPACT_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_tile_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int, zoom_level: int = 14) -> pd.DataFrame:
    """
    Query MERCATOR_TILE_IMPACT_MAT for probabilistic tile-level impact data.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        wind_threshold: Wind speed threshold in knots (e.g. 34)
        zoom_level: Mercator tile zoom level (default 14)

    Returns:
        pandas.DataFrame with columns: ZONE_ID, ADMIN_ID, PROBABILITY,
        E_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS, E_SCHOOL_AGE_POPULATION,
        E_INFANT_POPULATION, E_NUM_HCS, E_RWI, E_SMOD_CLASS
    """
    try:
        query = """
        SELECT
            t.ZONE_ID,
            t.ADMIN_ID,
            t.PROBABILITY,
            t.E_POPULATION,
            t.E_INFANT_POPULATION,
            t.E_SCHOOL_AGE_POPULATION,
            t.E_ADOLESCENT_POPULATION,
            t.E_BUILT_SURFACE_M2,
            t.E_NUM_SCHOOLS,
            t.E_NUM_HCS,
            t.E_NUM_SHELTERS,
            t.E_NUM_WASH,
            t.E_SMOD_CLASS,
            t.E_RWI,
            v.E_INFANT_IN_NEED,
            v.E_SCHOOL_AGE_IN_NEED,
            v.E_ADOLESCENT_IN_NEED,
            v.E_CHILDREN_IN_NEED,
            v.E_PEOPLE_IN_NEED
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT t
        LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_VULNERABILITY_MAT v
            ON  v.COUNTRY       = t.COUNTRY
            AND v.STORM         = t.STORM
            AND v.FORECAST_DATE = t.FORECAST_DATE
            AND v.ZOOM_LEVEL    = t.ZOOM_LEVEL
            AND v.ZONE_ID       = t.ZONE_ID
        WHERE t.COUNTRY = %s
          AND t.STORM = %s
          AND t.FORECAST_DATE = %s
          AND t.WIND_THRESHOLD = %s
          AND t.ZOOM_LEVEL = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold, zoom_level])
        logger.info("Loaded %d tile impact rows (%s/%s/%s/%dkt z=%d)", len(df), country, storm, forecast_date, wind_threshold, zoom_level)
        return df.copy()
    except Exception as e:
        logger.error("Error querying MERCATOR_TILE_IMPACT_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_gust_tile_impacts(country: str, storm: str, forecast_date: str, gust_threshold: int, zoom_level: int = 14) -> pd.DataFrame:
    """
    Real per-tile GUST impact totals — MERCATOR_TILE_GUST_MAT, same shape as
    get_tile_impacts (wind) above but keyed by GUST_THRESHOLD instead of
    WIND_THRESHOLD. No vulnerability join: E_PEOPLE_IN_NEED/E_CHILDREN_IN_NEED
    are computed from wind-ensemble envelope data specifically and don't exist
    for gust (mirrors services/tile_server.py's own MERCATOR_TILE_GUST_MAT
    comment on why that join is deliberately omitted there too).

    Returns pandas.DataFrame with columns: ZONE_ID, PROBABILITY, E_POPULATION,
    E_INFANT_POPULATION, E_SCHOOL_AGE_POPULATION, E_ADOLESCENT_POPULATION,
    E_NUM_SCHOOLS, E_NUM_HCS, E_NUM_SHELTERS, E_NUM_WASH.
    """
    try:
        query = """
        SELECT
            ZONE_ID, PROBABILITY, E_POPULATION, E_INFANT_POPULATION,
            E_SCHOOL_AGE_POPULATION, E_ADOLESCENT_POPULATION,
            E_NUM_SCHOOLS, E_NUM_HCS, E_NUM_SHELTERS, E_NUM_WASH
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s
          AND GUST_THRESHOLD = %s AND ZOOM_LEVEL = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, gust_threshold, zoom_level])
        logger.info("Loaded %d gust tile impact rows (%s/%s/%s/%dkt z=%d)",
                    len(df), country, storm, forecast_date, gust_threshold, zoom_level)
        return df.copy()
    except Exception as e:
        logger.error("Error querying MERCATOR_TILE_GUST_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_river_tile_impacts(country: str, forecast_time: str, rp_tier: str) -> pd.DataFrame:
    """
    Real per-tile RIVER flood-extent impact totals — MERCATOR_TILE_RIVER_MAT,
    keyed by COUNTRY + FORECAST_TIME + RP_TIER (not storm-scoped at all, see
    services/tile_server.py's own _MERCATOR_RIVER_SQL comment). MAX(...)-
    aggregated across STEP_H (multiple lead-time rows per tile for the same RP
    tier) — same "at least this severity, at some point in the forecast
    horizon" semantics the map's own river raster already uses server-side.

    Returns pandas.DataFrame with columns: ZONE_ID, PROBABILITY, E_POPULATION,
    E_INFANT_POPULATION, E_SCHOOL_AGE_POPULATION, E_ADOLESCENT_POPULATION,
    E_NUM_SCHOOLS, E_NUM_HCS, E_NUM_SHELTERS, E_NUM_WASH.
    """
    try:
        query = """
        SELECT
            ZONE_ID,
            MAX(PROBABILITY)             AS PROBABILITY,
            MAX(E_POPULATION)            AS E_POPULATION,
            MAX(E_INFANT_POPULATION)     AS E_INFANT_POPULATION,
            MAX(E_SCHOOL_AGE_POPULATION) AS E_SCHOOL_AGE_POPULATION,
            MAX(E_ADOLESCENT_POPULATION) AS E_ADOLESCENT_POPULATION,
            MAX(E_NUM_SCHOOLS)           AS E_NUM_SCHOOLS,
            MAX(E_NUM_HCS)               AS E_NUM_HCS,
            MAX(E_NUM_SHELTERS)          AS E_NUM_SHELTERS,
            MAX(E_NUM_WASH)              AS E_NUM_WASH
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
        GROUP BY ZONE_ID
        """
        df = _run_query(query, params=[country, forecast_time, rp_tier])
        logger.info("Loaded %d river tile impact rows (%s/%s/%s)", len(df), country, forecast_time, rp_tier)
        return df.copy()
    except Exception as e:
        logger.error("Error querying MERCATOR_TILE_RIVER_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_rain_tile_impacts(country: str, forecast_time: str, threshold_mm, window_h) -> pd.DataFrame:
    """
    Real per-tile RAINFALL impact totals — MERCATOR_TILE_PRECIP_MAT, keyed by
    COUNTRY + FORECAST_TIME + THRESHOLD_MM + WINDOW_H. Only E_POPULATION is a
    real hazard-conditional exposure column on this table (confirmed live —
    every other exposure column there is a bare, hazard-UNCONDITIONAL
    duplicate of the base layer, not a real rain-specific exposed count, see
    services/tile_server.py's own MERCATOR_TILE_PRECIP_MAT comment) — schools/
    health centers/shelters/WASH are deliberately NOT selected here; callers
    must treat rain as having no real facility-exposure signal at all, not
    silently substitute the unconditional base count.

    Returns pandas.DataFrame with columns: ZONE_ID, PROBABILITY, E_POPULATION.
    """
    try:
        query = """
        SELECT ZONE_ID, PROBABILITY, E_POPULATION
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
        """
        df = _run_query(query, params=[country, forecast_time, threshold_mm, window_h])
        logger.info("Loaded %d rain tile impact rows (%s/%s/%smm/%sh)",
                    len(df), country, forecast_time, threshold_mm, window_h)
        return df.copy()
    except Exception as e:
        logger.error("Error querying MERCATOR_TILE_PRECIP_MAT: %s", e)
        return pd.DataFrame()


# Canonical threshold tiers a per-threshold curve steps through. Mirrors
# _WIND_CATS (index [2] wind kt / index [3] gust kt) and _RIVER_RP_TIERS in
# pages/map_shell_concept.py, duplicated here (rather than imported) because
# snowflake_utils.py sits below pages/ in the import graph; if either list of
# tiers changes there, update these too so a real threshold never silently
# falls back to a fabricated 0 in the returned dict below.
_TOTALS_WIND_THRESHOLDS_KT = [34, 40, 50, 64, 83, 96, 113, 137]
_TOTALS_GUST_THRESHOLDS_KT = [17, 21, 26, 33, 43, 49, 58, 70]
_TOTALS_RIVER_RP_TIERS = ["rp2", "rp5", "rp10", "rp20", "rp50", "rp100"]

_TOTALS_IMPACT_COLS = [
    "E_POPULATION", "E_INFANT_POPULATION", "E_SCHOOL_AGE_POPULATION",
    "E_ADOLESCENT_POPULATION", "E_NUM_SCHOOLS", "E_NUM_HCS",
    "E_NUM_SHELTERS", "E_NUM_WASH",
]


def _zero_impact_totals() -> dict:
    return {col: 0 for col in _TOTALS_IMPACT_COLS}


def _row_to_impact_totals(row) -> dict:
    # A real, all-NULL column for this country (e.g. E_NUM_SHELTERS for a
    # country with no shelter dataset at all, see
    # _get_data_availability_real's own comment on this exact gap) stays
    # None here, same "don't fabricate a confirmed zero" convention used
    # throughout this file; only a threshold with genuinely zero matching
    # rows gets filled with real 0s, by _zero_impact_totals above.
    return {col: (int(row[col]) if pd.notna(row[col]) else None) for col in _TOTALS_IMPACT_COLS}


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_tile_impact_totals_by_threshold(country: str, storm: str, forecast_date: str, zoom_level: int = 14) -> dict:
    """
    One-round-trip-per-hazard replacement for fanning out get_tile_impacts/
    get_gust_tile_impacts/get_river_tile_impacts across every threshold tier
    just to sum them (the old pattern behind the Hazard Contribution popup's
    per-tier curve: 8 full per-tile fetches for wind alone, 472,848 rows
    transferred where a GROUP BY aggregate returns 8). Each hazard here is
    SUM(...) GROUP BY <threshold column> in a single statement covering every
    tier at once, cached on (country, storm, forecast_date) only, so every
    slider position after the first is a cache hit, not a new query.

    Returns:
        {
            "wind":  {34: {...}, 40: {...}, ..., 137: {...}},   # kt -> totals
            "gust":  {17: {...}, 21: {...}, ...,  70: {...}},   # kt -> totals
            "river": {"rp2": {...}, ..., "rp100": {...}},        # rp_tier -> totals
        }
    where each `{...}` is {"E_POPULATION": int|None, "E_INFANT_POPULATION": int|None,
    "E_SCHOOL_AGE_POPULATION": int|None, "E_ADOLESCENT_POPULATION": int|None,
    "E_NUM_SCHOOLS": int|None, "E_NUM_HCS": int|None, "E_NUM_SHELTERS": int|None,
    "E_NUM_WASH": int|None}. None only when that column is genuinely all-NULL
    for this country (a real dataset gap), 0 when the threshold tier simply
    has no matching rows (a real, confirmed-zero exposure at that tier).

    Every canonical threshold in _TOTALS_WIND_THRESHOLDS_KT/
    _TOTALS_GUST_THRESHOLDS_KT/_TOTALS_RIVER_RP_TIERS is always present as a
    key: a tier absent from the query result (genuinely 0 rows) is filled
    with _zero_impact_totals(), not omitted, so callers never have to
    special-case a missing key as "no data" when it really means "real zero".

    River is NOT storm-scoped (see get_river_tile_impacts's own docstring),
    so `storm` is ignored for the "river" section, and its own forecast time
    is resolved independently via get_latest_river_forecast_time(country)
    rather than reusing `forecast_date` (which is wind's cycle, and can
    genuinely differ from river's, confirmed live: PHL's own river data has
    lagged its wind cycle by weeks). The "river" key is {} (not zero-filled)
    when the country has no river data of any kind, a real dataset gap, not
    a per-tier zero.
    """
    result = {"wind": {}, "gust": {}, "river": {}}

    try:
        wind_df = _run_query(
            """
            SELECT WIND_THRESHOLD, """ + ", ".join(f"SUM({c}) AS {c}" for c in _TOTALS_IMPACT_COLS) + """
            FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
            WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND ZOOM_LEVEL = %s
            GROUP BY WIND_THRESHOLD
            """,
            params=[country, storm, forecast_date, zoom_level],
        )
        by_kt = {int(row["WIND_THRESHOLD"]): _row_to_impact_totals(row) for _, row in wind_df.iterrows()}
        result["wind"] = {kt: by_kt.get(kt, _zero_impact_totals()) for kt in _TOTALS_WIND_THRESHOLDS_KT}
    except Exception as e:
        logger.warning("get_tile_impact_totals_by_threshold wind failed for %s/%s/%s: %s", country, storm, forecast_date, e)

    try:
        gust_df = _run_query(
            """
            SELECT GUST_THRESHOLD, """ + ", ".join(f"SUM({c}) AS {c}" for c in _TOTALS_IMPACT_COLS) + """
            FROM AOTS.TC_ECMWF.MERCATOR_TILE_GUST_MAT
            WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND ZOOM_LEVEL = %s
            GROUP BY GUST_THRESHOLD
            """,
            params=[country, storm, forecast_date, zoom_level],
        )
        by_kt = {int(row["GUST_THRESHOLD"]): _row_to_impact_totals(row) for _, row in gust_df.iterrows()}
        result["gust"] = {kt: by_kt.get(kt, _zero_impact_totals()) for kt in _TOTALS_GUST_THRESHOLDS_KT}
    except Exception as e:
        logger.warning("get_tile_impact_totals_by_threshold gust failed for %s/%s/%s: %s", country, storm, forecast_date, e)

    try:
        river_forecast_time = get_latest_river_forecast_time(country)
        if river_forecast_time is not None:
            river_df = _run_query(
                """
                WITH per_zone_max AS (
                    SELECT RP_TIER, ZONE_ID, """ + ", ".join(f"MAX({c}) AS {c}" for c in _TOTALS_IMPACT_COLS) + """
                    FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT
                    WHERE COUNTRY = %s AND FORECAST_TIME = %s
                    GROUP BY RP_TIER, ZONE_ID
                )
                SELECT RP_TIER, """ + ", ".join(f"SUM({c}) AS {c}" for c in _TOTALS_IMPACT_COLS) + """
                FROM per_zone_max
                GROUP BY RP_TIER
                """,
                params=[country, river_forecast_time],
            )
            by_tier = {str(row["RP_TIER"]): _row_to_impact_totals(row) for _, row in river_df.iterrows()}
            result["river"] = {tier: by_tier.get(tier, _zero_impact_totals()) for tier in _TOTALS_RIVER_RP_TIERS}
    except Exception as e:
        logger.warning("get_tile_impact_totals_by_threshold river failed for %s: %s", country, e)

    return result


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_admin_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int, admin_level: int = 1) -> pd.DataFrame:
    """
    Query ADMIN_ALL_IMPACT_MAT for administrative-unit-level impact data.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        wind_threshold: Wind speed threshold in knots (e.g. 34)
        admin_level: Administrative level to query (default 1)

    Returns:
        pandas.DataFrame with columns: NAME, E_POPULATION, E_NUM_SCHOOLS,
        E_NUM_HCS, PROBABILITY, ZONE_ID
    """
    try:
        query = """
        SELECT
            t.TILE_ID,
            t.NAME,
            t.ADMIN_LEVEL,
            t.PROBABILITY,
            t.E_POPULATION,
            t.E_INFANT_POPULATION,
            t.E_SCHOOL_AGE_POPULATION,
            t.E_ADOLESCENT_POPULATION,
            t.E_BUILT_SURFACE_M2,
            t.E_NUM_SCHOOLS,
            t.E_NUM_HCS,
            t.E_NUM_SHELTERS,
            t.E_NUM_WASH,
            t.E_SMOD_CLASS,
            t.E_RWI,
            v.E_INFANT_IN_NEED,
            v.E_SCHOOL_AGE_IN_NEED,
            v.E_ADOLESCENT_IN_NEED,
            v.E_CHILDREN_IN_NEED,
            v.E_PEOPLE_IN_NEED
        FROM AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT t
        LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_VULNERABILITY_MAT v
            ON  v.COUNTRY       = t.COUNTRY
            AND v.STORM         = t.STORM
            AND v.FORECAST_DATE = t.FORECAST_DATE
            AND v.ADMIN_LEVEL   = t.ADMIN_LEVEL
            AND v.TILE_ID       = t.TILE_ID
        WHERE t.COUNTRY = %s
          AND t.STORM = %s
          AND t.FORECAST_DATE = %s
          AND t.WIND_THRESHOLD = %s
          AND t.ADMIN_LEVEL = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold, admin_level])
        logger.info("Loaded %d admin impact rows (%s/%s/%s/%dkt L%d)", len(df), country, storm, forecast_date, wind_threshold, admin_level)
        return df.copy()
    except Exception as e:
        logger.error("Error querying ADMIN_ALL_IMPACT_MAT: %s", e)
        return pd.DataFrame()



@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_tile_cci(country: str, storm: str, forecast_date: str, zoom_level: int = 14) -> pd.DataFrame:
    """
    Query MERCATOR_TILE_CCI_MAT for tile-level CCI data.
    Returns only zone_id + the two display columns to avoid merge conflicts.
    """
    try:
        query = """
        SELECT ZONE_ID, CCI_CHILDREN, E_CCI_CHILDREN
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND ZOOM_LEVEL = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, zoom_level])
        df.columns = [c.lower() for c in df.columns]   # zone_id, cci_children, E_cci_children
        # Normalise E_ prefix (E_cci_children stays as-is after lower)
        df = df.rename(columns={'e_cci_children': 'E_cci_children'})
        logger.info("Loaded %d tile CCI rows (%s/%s/%s z=%d)", len(df), country, storm, forecast_date, zoom_level)
        return df.copy()
    except Exception as e:
        logger.error("Error querying MERCATOR_TILE_CCI_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_admin_cci(country: str, storm: str, forecast_date: str, admin_level: int = 1) -> pd.DataFrame:
    """
    Query ADMIN_ALL_CCI_MAT for admin-level CCI data.
    Returns only tile_id + the two display columns to avoid merge conflicts.
    """
    try:
        query = """
        SELECT TILE_ID, CCI_CHILDREN, E_CCI_CHILDREN
        FROM AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND ADMIN_LEVEL = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, admin_level])
        df.columns = [c.lower() for c in df.columns]   # tile_id, cci_children, e_cci_children
        df = df.rename(columns={'e_cci_children': 'E_cci_children'})
        logger.info("Loaded %d admin CCI rows (%s/%s/%s L%d)", len(df), country, storm, forecast_date, admin_level)
        return df.copy()
    except Exception as e:
        logger.error("Error querying ADMIN_ALL_CCI_MAT: %s", e)
        return pd.DataFrame()



@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_track_impacts(country: str, storm: str, forecast_date: str, wind_threshold: int) -> gpd.GeoDataFrame:
    """
    Query TRACK_MAT and return a GeoDataFrame matching the structure of track_views parquet files.

    One row per ensemble member (ZONE_ID = member number 1–51), with severity columns
    and the wind-envelope geometry in EPSG:4326.

    Args:
        country: Country code (e.g. 'PNG')
        storm: Storm identifier (e.g. 'MAILA')
        forecast_date: Forecast date string in YYYYMMDDHHMMSS format (e.g. '20260405120000')
        wind_threshold: Wind threshold in knots (e.g. 50)

    Returns:
        geopandas.GeoDataFrame with columns matching track_views parquet files
    """
    try:
        from shapely import wkb as shapely_wkb
        query = """
        SELECT
            t.ZONE_ID                        AS zone_id,
            t.WIND_THRESHOLD                 AS wind_threshold,
            t.SEVERITY_POPULATION            AS severity_population,
            t.SEVERITY_SCHOOL_AGE_POPULATION AS severity_school_age_population,
            t.SEVERITY_INFANT_POPULATION     AS severity_infant_population,
            t.SEVERITY_ADOLESCENT_POPULATION AS severity_adolescent_population,
            t.SEVERITY_SCHOOLS               AS severity_schools,
            t.SEVERITY_HCS                   AS severity_hcs,
            t.SEVERITY_NUM_SHELTERS          AS severity_num_shelters,
            t.SEVERITY_NUM_WASH              AS severity_num_wash,
            t.SEVERITY_BUILT_SURFACE_M2      AS severity_built_surface_m2,
            t.GEOMETRY,
            v.SEVERITY_PEOPLE_IN_NEED        AS severity_people_in_need,
            v.SEVERITY_CHILDREN_IN_NEED      AS severity_children_in_need,
            v.SEVERITY_INFANT_IN_NEED        AS severity_infant_in_need,
            v.SEVERITY_SCHOOL_AGE_IN_NEED    AS severity_school_age_in_need,
            v.SEVERITY_ADOLESCENT_IN_NEED    AS severity_adolescent_in_need
        FROM AOTS.TC_ECMWF.TRACK_MAT t
        LEFT JOIN AOTS.TC_ECMWF.TRACK_VULNERABILITY_MAT v
            ON  v.COUNTRY       = t.COUNTRY
            AND v.STORM         = t.STORM
            AND v.FORECAST_DATE = t.FORECAST_DATE
            AND v.ZONE_ID       = t.ZONE_ID
        WHERE t.COUNTRY = %s
          AND t.STORM = %s
          AND t.FORECAST_DATE = %s
          AND t.WIND_THRESHOLD = %s
        ORDER BY t.ZONE_ID
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold])

        def _parse_wkb(g):
            if g is None:
                return None
            # Snowflake returns VARIANT binary as a JSON-quoted hex string
            hex_str = g.strip('"') if isinstance(g, str) else g.hex()
            return shapely_wkb.loads(bytes.fromhex(hex_str))

        df['geometry'] = df['GEOMETRY'].apply(_parse_wkb)
        df = df.drop(columns=['GEOMETRY'])
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
        logger.info("Loaded %d track rows (%s/%s/%s/%dkt)", len(gdf), country, storm, forecast_date, wind_threshold)
        return gdf.copy()
    except Exception as e:
        logger.error("Error querying TRACK_MAT: %s", e)
        return gpd.GeoDataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_gust_track_impacts(country: str, storm: str, forecast_date: str, gust_threshold: int) -> pd.DataFrame:
    """
    Real per-member GUST severity — TRACK_GUST_MAT, the gust mirror of
    get_track_impacts/TRACK_MAT above (confirmed live: a genuinely real,
    separately deployed table, e.g. 780 rows, real BAVI/PHL rows).

    Real perf bug found+fixed here: get_track_impacts (wind's own sibling,
    just above) already has this same @ttl_cache — this one was missing it,
    so every gust threshold change re-queried TRACK_GUST_MAT from scratch
    even on a cache hit for the identical (country, storm, forecast_date,
    gust_threshold) tuple (e.g. flicking a slider back and forth).

    Only ZONE_ID (member number) + SEVERITY_POPULATION are needed by this
    app's one real caller (_build_ms_envelope_geojson's severity-by-member
    lookup, which never reads geometry from this function at all — the
    real envelope polygon geometry comes from get_gust_envelope_data_snowflake
    instead) — a plain DataFrame, no WKB/GeoDataFrame parsing needed.

    No vulnerability join: there is no TRACK_GUST_VULNERABILITY_MAT (confirmed
    live — no such table exists), matching the same "no PIN/CHIN for gust"
    pattern already established for every other gust-specific table in this
    app (MERCATOR_TILE_GUST_MAT etc.).
    """
    try:
        query = """
        SELECT
            ZONE_ID              AS zone_id,
            SEVERITY_POPULATION  AS severity_population
        FROM AOTS.TC_ECMWF.TRACK_GUST_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND GUST_THRESHOLD = %s
        ORDER BY ZONE_ID
        """
        df = _run_query(query, params=[country, storm, forecast_date, gust_threshold])
        logger.info("Loaded %d gust track rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, gust_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying TRACK_GUST_MAT: %s", e)
        return pd.DataFrame()


# =============================================================================
# BASE LAYER QUERIES (no storm required)
# =============================================================================
# These functions query country-static MAT tables created in
# 02_setup_base_layer_tables.sql. They enable tile and facility layers to be
# displayed immediately on country selection, before any storm is loaded.
# Only used when IMPACT_DATA_SOURCE=SQL.
# =============================================================================

@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_tiles(country: str, zoom_level: int = 14) -> gpd.GeoDataFrame:
    """
    Query BASE_MERCATOR_TILE_MAT and reconstruct tile polygons from quadkeys.

    Returns GeoDataFrame with geometry (WGS84 bounding box per quadkey tile)
    and all available context columns: smod_class, rwi, population, facility counts.
    Missing columns (older pipeline format) are returned as NaN.
    """
    try:
        import mercantile
        from shapely.geometry import box as shapely_box
        t0 = time.time()
        query = """
        SELECT
            TILE_ID,
            ADMIN_ID,
            POPULATION,
            SCHOOL_AGE_POPULATION,
            INFANT_POPULATION,
            ADOLESCENT_POPULATION,
            BUILT_SURFACE_M2,
            SMOD_CLASS,
            SMOD_CLASS_L1,
            RWI,
            NUM_SCHOOLS,
            NUM_HCS,
            NUM_SHELTERS,
            NUM_WASH,
            MODERATE_POVERTY_PROB,
            SEVERE_POVERTY_PROB
        FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT
        WHERE COUNTRY = %s
          AND ZOOM_LEVEL = %s
        """
        df = _run_query(query, params=[country, zoom_level])
        if df.empty:
            return gpd.GeoDataFrame()

        df.columns = [c.lower() for c in df.columns]

        def _qk_to_geom(qk):
            try:
                t = mercantile.quadkey_to_tile(qk)
                b = mercantile.bounds(t)
                return shapely_box(b.west, b.south, b.east, b.north)
            except Exception as e:
                logger.debug("Invalid quadkey %s: %s", qk, e)
                return None

        df['geometry'] = df['tile_id'].apply(_qk_to_geom)
        df = df.dropna(subset=['geometry'])
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
        logger.info("Base tiles %s/z%d: %d tiles in %.1fs", country, zoom_level, len(gdf), time.time() - t0)
        return gdf.copy()
    except Exception as e:
        logger.error("Error querying BASE_MERCATOR_TILE_MAT: %s", e)
        return gpd.GeoDataFrame()


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_data_availability(country: str, zoom_level: int = 14) -> dict:
    """
    One-round-trip aggregate replacement for the Data Availability panel's
    old pattern of calling get_base_tiles(country): a full row pull (PHL:
    59,106 rows x 16 cols) plus a Python loop reconstructing quadkey geometry
    per row, just to compute 4 facility sums and 8 non-null checks. This
    does the exact same SUM/COUNT math in one Snowflake aggregate query
    instead, with no geometry involved at all.

    Same return shape as pages/map_shell_concept.py's own
    _get_data_availability_real(country) builds from get_base_tiles today:

        {
            "schools": int|None, "health_centers": int|None,
            "shelters": int|None, "wash": int|None,
            "population": bool, "age_0_4": bool,
            "age_5_14": bool, "age_15_19": bool,
            "rwi": bool, "settlement": bool,
            "moderate_poverty": bool, "severe_poverty": bool,
        }

    The 4 facility counts are None (not a fabricated 0) when that column is
    genuinely all-NULL for this country for the same reason
    _get_data_availability_real's own _total() treats it that way (live-
    verified real gap: Turks and Caicos Islands' NUM_SHELTERS is 100% NULL).
    SQL's own NULL-skipping SUM already returns NULL when every input row
    is NULL, so no extra COUNT(...) check is needed for those 4 fields. The
    8 boolean fields use COUNT(col) > 0 (a non-null row exists) since "any
    real data present at all" is the actual question there, not a sum.

    Returns None (same as get_base_tiles returning empty) when this country
    has no base-layer data in Snowflake at all yet for `zoom_level`.
    """
    try:
        query = """
        SELECT
            COUNT(*)                        AS N_ROWS,
            SUM(NUM_SCHOOLS)                AS TOTAL_SCHOOLS,
            SUM(NUM_HCS)                    AS TOTAL_HCS,
            SUM(NUM_SHELTERS)               AS TOTAL_SHELTERS,
            SUM(NUM_WASH)                   AS TOTAL_WASH,
            COUNT(POPULATION)               AS N_POPULATION,
            COUNT(INFANT_POPULATION)        AS N_INFANT,
            COUNT(SCHOOL_AGE_POPULATION)    AS N_SCHOOL_AGE,
            COUNT(ADOLESCENT_POPULATION)    AS N_ADOLESCENT,
            COUNT(RWI)                      AS N_RWI,
            COUNT(SMOD_CLASS)               AS N_SMOD_CLASS,
            COUNT(MODERATE_POVERTY_PROB)    AS N_MODERATE_POVERTY,
            COUNT(SEVERE_POVERTY_PROB)      AS N_SEVERE_POVERTY
        FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT
        WHERE COUNTRY = %s
          AND ZOOM_LEVEL = %s
        """
        df = _run_query(query, params=[country, zoom_level])
        if df.empty or int(df.iloc[0]["N_ROWS"]) == 0:
            return None
        row = df.iloc[0]

        def _total(col):
            return int(row[col]) if pd.notna(row[col]) else None

        def _any_present(col):
            return pd.notna(row[col]) and int(row[col]) > 0

        return {
            "schools": _total("TOTAL_SCHOOLS"), "health_centers": _total("TOTAL_HCS"),
            "shelters": _total("TOTAL_SHELTERS"), "wash": _total("TOTAL_WASH"),
            "population": _any_present("N_POPULATION"), "age_0_4": _any_present("N_INFANT"),
            "age_5_14": _any_present("N_SCHOOL_AGE"), "age_15_19": _any_present("N_ADOLESCENT"),
            "rwi": _any_present("N_RWI"), "settlement": _any_present("N_SMOD_CLASS"),
            "moderate_poverty": _any_present("N_MODERATE_POVERTY"), "severe_poverty": _any_present("N_SEVERE_POVERTY"),
        }
    except Exception as e:
        logger.error("Error querying data availability for %s: %s", country, e)
        return None


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def _get_country_totals_cached(country: str) -> dict:
    try:
        # Real bug found+fixed here (2026-08): summing
        # COALESCE(INFANT_POPULATION,0) + COALESCE(SCHOOL_AGE_POPULATION,0) +
        # COALESCE(ADOLESCENT_POPULATION,0) inside one row-level expression
        # collapsed NULLs to 0 BEFORE the outer SUM ever ran — a country
        # whose age-breakdown columns are 100% NULL (live-verified: real for
        # Curaçao's own E_ADOLESCENT_POPULATION) still produced a real SQL
        # 0.0, indistinguishable from a genuine zero. Three separate SUMs
        # let SQL's own NULL-skipping SUM propagate a true NULL when a whole
        # column is empty for this country, while still correctly summing
        # whichever age bands DO have real per-tile data (see below).
        query = """
        SELECT
            SUM(POPULATION)              AS total_population,
            SUM(INFANT_POPULATION)       AS total_infant,
            SUM(SCHOOL_AGE_POPULATION)   AS total_school_age,
            SUM(ADOLESCENT_POPULATION)   AS total_adolescent
        FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT
        WHERE COUNTRY = %s
        """
        df = _run_query(query, params=[country])
        if df.empty or df.iloc[0]["TOTAL_POPULATION"] is None:
            return {"total_population": None, "total_children": None}
        row = df.iloc[0]
        # Sum only the age bands with real data for this country — a band
        # that's genuinely all-NULL is excluded from the sum (not treated
        # as a real 0), and total_children itself is None only when ALL
        # THREE bands are missing, not just one.
        age_parts = [float(row[c]) for c in ("TOTAL_INFANT", "TOTAL_SCHOOL_AGE", "TOTAL_ADOLESCENT")
                      if pd.notna(row[c])]
        return {
            "total_population": int(row["TOTAL_POPULATION"]) if pd.notna(row["TOTAL_POPULATION"]) else None,
            "total_children":   int(sum(age_parts)) if age_parts else None,
        }
    except Exception as e:
        logger.error("Error querying country totals for %s: %s", country, e)
        return {"total_population": None, "total_children": None}


def get_country_totals(country: str) -> dict:
    """Return total population and total children for a country from BASE_MERCATOR_TILE_MAT.

    Returns dict with keys: total_population, total_children (int or None on error).
    Each call returns a fresh copy so callers cannot corrupt the cache.
    """
    return dict(_get_country_totals_cached(country))


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_schools(country: str) -> pd.DataFrame:
    """Query BASE_SCHOOL_MAT — all school locations for a country (no storm required)."""
    try:
        t0 = time.time()
        query = """
        SELECT
            SCHOOL_ID_GIGA  AS school_id_giga,
            SCHOOL_NAME     AS school_name,
            EDUCATION_LEVEL AS education_level,
            LATITUDE        AS latitude,
            LONGITUDE       AS longitude,
            COUNTRY_ISO3_CODE AS country_iso3_code
        FROM AOTS.TC_ECMWF.BASE_SCHOOL_MAT
        WHERE COUNTRY = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        """
        df = _run_query(query, params=[country])
        df.columns = [c.lower() for c in df.columns]
        logger.info("Base schools %s: %d rows in %.1fs", country, len(df), time.time() - t0)
        return df.copy()
    except Exception as e:
        logger.error("Error querying BASE_SCHOOL_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_hcs(country: str) -> pd.DataFrame:
    """Query BASE_HC_MAT — all health centre locations for a country (no storm required)."""
    try:
        t0 = time.time()
        query = """
        SELECT
            NAME                AS name,
            HEALTH_AMENITY_TYPE AS health_amenity_type,
            AMENITY             AS amenity,
            OPERATIONAL_STATUS  AS operational_status,
            BEDS                AS beds,
            EMERGENCY           AS emergency,
            ELECTRICITY         AS electricity,
            OPERATOR_TYPE       AS operator_type,
            LATITUDE            AS latitude,
            LONGITUDE           AS longitude
        FROM AOTS.TC_ECMWF.BASE_HC_MAT
        WHERE COUNTRY = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        """
        df = _run_query(query, params=[country])
        df.columns = [c.lower() for c in df.columns]
        logger.info("Base HCs %s: %d rows in %.1fs", country, len(df), time.time() - t0)
        return df.copy()
    except Exception as e:
        logger.error("Error querying BASE_HC_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_shelters(country: str) -> pd.DataFrame:
    """Query BASE_SHELTER_MAT — all shelter locations for a country (no storm required)."""
    try:
        t0 = time.time()
        query = """
        SELECT
            NAME         AS name,
            NAME_EN      AS name_en,
            SHELTER_TYPE AS shelter_type,
            CATEGORY     AS category,
            LATITUDE     AS latitude,
            LONGITUDE    AS longitude
        FROM AOTS.TC_ECMWF.BASE_SHELTER_MAT
        WHERE COUNTRY = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        """
        df = _run_query(query, params=[country])
        df.columns = [c.lower() for c in df.columns]
        logger.info("Base shelters %s: %d rows in %.1fs", country, len(df), time.time() - t0)
        return df.copy()
    except Exception as e:
        logger.error("Error querying BASE_SHELTER_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_wash(country: str) -> pd.DataFrame:
    """Query BASE_WASH_MAT — all WASH facility locations for a country (no storm required)."""
    try:
        t0 = time.time()
        query = """
        SELECT
            NAME      AS name,
            NAME_EN   AS name_en,
            WASH_TYPE AS wash_type,
            CATEGORY  AS category,
            LATITUDE  AS latitude,
            LONGITUDE AS longitude
        FROM AOTS.TC_ECMWF.BASE_WASH_MAT
        WHERE COUNTRY = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        """
        df = _run_query(query, params=[country])
        df.columns = [c.lower() for c in df.columns]
        logger.info("Base WASH %s: %d rows in %.1fs", country, len(df), time.time() - t0)
        return df.copy()
    except Exception as e:
        logger.error("Error querying BASE_WASH_MAT: %s", e)
        return pd.DataFrame()


_FACILITY_SOURCE_TABLE = {"schools": "BASE_SCHOOL_MAT", "health": "BASE_HC_MAT",
                          "shelters": "BASE_SHELTER_MAT", "wash": "BASE_WASH_MAT"}


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=64)
def get_facility_source(country: str, layer: str):
    """Real per-country facility data-source label — SOURCE column added
    2026-08 to BASE_SCHOOL_MAT/BASE_HC_MAT/BASE_SHELTER_MAT/BASE_WASH_MAT
    (impact_analysis.py's fetch_schools/fetch_health_centers/fetch_shelters/
    fetch_wash now populate it: the real custom-CSV source when a custom
    override is used, else the standard API/OSM source name).

    Returns the most common non-null SOURCE value for this country/layer
    (should be one consistent value per country in practice — a single
    country's facility file uses one source, not several), or None when the
    pipeline hasn't been re-run for this country since the SOURCE column
    was added (existing rows from before then read back as real NULL, not
    an error). Callers should fall back to a generic/static description
    when this returns None, not treat it as a failure.
    """
    table = _FACILITY_SOURCE_TABLE.get(layer)
    if not table:
        return None
    try:
        query = f"""
        SELECT SOURCE, COUNT(*) AS N
        FROM AOTS.TC_ECMWF.{table}
        WHERE COUNTRY = %s AND SOURCE IS NOT NULL
        GROUP BY SOURCE
        ORDER BY N DESC
        LIMIT 1
        """
        df = _run_query(query, params=[country])
        return str(df.iloc[0]["SOURCE"]) if not df.empty else None
    except Exception as e:
        logger.warning("Error querying facility source for %s/%s: %s", country, layer, e)
        return None


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_admin(country: str, admin_level: int = 1) -> gpd.GeoDataFrame:
    """
    Query BASE_ADMIN_GEOM_MAT — admin boundary polygons with demographics (no storm required).

    Geometry stored as GEOGRAPHY in Snowflake; returned as ST_ASGEOJSON and
    reconstructed into a GeoDataFrame for map rendering.
    """
    try:
        import json
        from shapely.geometry import shape
        t0 = time.time()
        query = """
        SELECT
            TILE_ID                  AS tile_id,
            NAME                     AS name,
            POPULATION               AS population,
            SCHOOL_AGE_POPULATION    AS school_age_population,
            INFANT_POPULATION        AS infant_population,
            ADOLESCENT_POPULATION    AS adolescent_population,
            BUILT_SURFACE_M2         AS built_surface_m2,
            SMOD_CLASS               AS smod_class,
            SMOD_CLASS_L1            AS smod_class_l1,
            RWI                      AS rwi,
            NUM_SCHOOLS              AS num_schools,
            NUM_HCS                  AS num_hcs,
            NUM_SHELTERS             AS num_shelters,
            NUM_WASH                 AS num_wash,
            MODERATE_POVERTY_PROB    AS moderate_poverty_prob,
            SEVERE_POVERTY_PROB      AS severe_poverty_prob,
            ST_ASGEOJSON(GEOMETRY)   AS geojson
        FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT
        WHERE COUNTRY = %s
          AND ADMIN_LEVEL = %s
          AND GEOMETRY IS NOT NULL
        """
        df = _run_query(query, params=[country, admin_level])
        if df.empty:
            return gpd.GeoDataFrame()

        df.columns = [c.lower() for c in df.columns]

        def _parse_geojson(s):
            try:
                return shape(json.loads(s))
            except Exception as e:
                logger.debug("GeoJSON parse error: %s", e)
                return None

        df['geometry'] = df['geojson'].apply(_parse_geojson)
        df = df.drop(columns=['geojson']).dropna(subset=['geometry'])
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
        logger.info("Base admin %s/L%d: %d regions in %.1fs", country, admin_level, len(gdf), time.time() - t0)
        return gdf.copy()
    except Exception as e:
        logger.error("Error querying BASE_ADMIN_GEOM_MAT: %s", e)
        return gpd.GeoDataFrame()


# ---------------------------------------------------------------------------
# Real, already-sent Alert emails (AOTS.TC_ECMWF.ALERT_SENT_LOG) — the "view
# past alert emails" feature on the dashboard's Global view. Deliberately
# ALERT-only (not Warning/watch): WATCH_SENT_LOG (the Warning dedup table —
# never renamed from its original "watch" name despite the procedure itself
# being called SEND_WARNING) has no EMAIL_BODY/HTML column at all — confirmed
# by reading 07b_alert_agent/02b_send_warning_procedure.sql directly (its
# CREATE TABLE and only INSERT both list just TRACK_ID/FORECAST_DATE/
# RECIPIENT_COUNT/COUNTRIES). A Warning's generated HTML is used once to call
# the email-send API and then discarded — there is nothing to fetch back for
# a past Warning, so it's out of scope until that changes upstream (needs the
# ORCHESTRATION repo's own explicit sign-off, not this app's to decide).
# ---------------------------------------------------------------------------

@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_storms_with_alert_emails_at(forecast_time: str) -> set:
    """Real set of TRACK_ID values with an alert email at this EXACT
    forecast_time (the topbar's selected date+run, e.g. "2026-08-02
    18:00:00") — used to decide whether a storm row's "view alert emails"
    icon should show AT ALL for the currently selected date/time, not just
    "this storm has ever had any alert" (real bug fixed here: the icon used
    to appear for a storm with alerts on a totally different date/run than
    the one currently selected, only to open an empty "no emails" popup —
    replacing an even older hardcoded demo set, {"GENEVIEVE", "MELISSA"})."""
    try:
        df = _run_query(
            "SELECT DISTINCT TRACK_ID FROM AOTS.TC_ECMWF.ALERT_SENT_LOG WHERE FORECAST_TIME = TO_TIMESTAMP_NTZ(%s)",
            params=[forecast_time],
        )
        return set(df['TRACK_ID'].tolist()) if not df.empty else set()
    except Exception as e:
        logger.error("Error querying storms with alert emails at %s: %s", forecast_time, e)
        return set()


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_alert_emails_for_storm(track_id: str, forecast_time: str = None):
    """Real list of available alert emails for a storm (ALERT_SENT_LOG), one
    entry per country — a multi-country storm can genuinely have several
    (one email per affected country) for the same forecast run.

    `forecast_time` (optional, e.g. "2026-08-02 18:00:00" — the topbar's
    selected date+run, NOT a free-text filter): when given, scopes results
    to that EXACT real forecast cycle only, matching the currently selected
    date/time instead of surfacing every historical alert ever sent for this
    storm (real feature — the popup used to show every alert regardless of
    the topbar's own date/time selection, which read as "why is this old
    email showing right now").

    Returns a list of dicts with keys TRACK_ID/FORECAST_TIME/COUNTRY_CODE/
    EMAIL_SUBJECT (RECIPIENT_COUNT/SENT_AT deliberately NOT selected —
    internal operational metadata, not something to surface in the UI.
    EMAIL_BODY itself is also NOT included here — fetch it separately via
    get_alert_email_body once a specific entry is picked, so listing a
    storm's emails stays cheap even when EMAIL_BODY is large).

    Real bug found+fixed here: ALERT_SENT_LOG's declared PRIMARY KEY
    (TRACK_ID, FORECAST_TIME, COUNTRY_CODE) is NOT actually enforced by
    Snowflake (PK/UNIQUE constraints there are informational only, never
    enforced) — confirmed live, MELISSA/JAM/2025-10-28 00:00:00 alone has
    20 real duplicate rows, all from repeated ORCHESTRATION test-harness
    runs over several months (SENT_AT ranging 2026-06-08 through
    2026-07-02), not 20 genuinely distinct alerts. Without dedup this
    listed "Jamaica" 20 times for one storm. QUALIFY + ROW_NUMBER keeps
    only the single most-recently-sent row per (track_id, forecast_time,
    country_code) — "if there are multiple versions from testing, only use
    the latest one", per explicit user request."""
    try:
        sql = (
            "SELECT TRACK_ID, FORECAST_TIME, COUNTRY_CODE, EMAIL_SUBJECT FROM AOTS.TC_ECMWF.ALERT_SENT_LOG "
            "WHERE TRACK_ID = %s"
        )
        params = [track_id]
        if forecast_time:
            sql += " AND FORECAST_TIME = TO_TIMESTAMP_NTZ(%s)"
            params.append(forecast_time)
        sql += (
            " QUALIFY ROW_NUMBER() OVER "
            "(PARTITION BY TRACK_ID, FORECAST_TIME, COUNTRY_CODE ORDER BY SENT_AT DESC) = 1"
            " ORDER BY COUNTRY_CODE"
        )
        df = _run_query(
            sql,
            params=params,
        )
        return df.to_dict('records') if not df.empty else []
    except Exception as e:
        logger.error("Error querying alert emails for storm %s: %s", track_id, e)
        return []


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_alert_email_body(track_id: str, forecast_time: str, country_code: str):
    """Real EMAIL_BODY HTML (a complete standalone <!DOCTYPE html> document)
    for one specific already-sent alert, or None if that exact
    (track_id, forecast_time, country_code) row doesn't exist.

    Real bug found+fixed here: ALERT_SENT_LOG's declared PRIMARY KEY isn't
    actually enforced by Snowflake (see get_alert_emails_for_storm's own
    docstring — confirmed live duplicate rows from repeated test-harness
    runs), so more than one row can genuinely match this exact key. Orders
    by SENT_AT DESC and takes the first — the single latest real send —
    rather than whatever arbitrary row order Snowflake happens to return.
    Content itself is otherwise immutable once sent, so caching the result
    (by these 3 args) is still safe."""
    try:
        df = _run_query(
            "SELECT EMAIL_BODY FROM AOTS.TC_ECMWF.ALERT_SENT_LOG "
            "WHERE TRACK_ID = %s AND FORECAST_TIME = TO_TIMESTAMP_NTZ(%s) AND COUNTRY_CODE = %s "
            "ORDER BY SENT_AT DESC LIMIT 1",
            params=[track_id, forecast_time, country_code],
        )
        if df.empty or pd.isna(df['EMAIL_BODY'].iloc[0]):
            return None
        return str(df['EMAIL_BODY'].iloc[0])
    except Exception as e:
        logger.error("Error querying alert email body for %s/%s/%s: %s", track_id, forecast_time, country_code, e)
        return None


@ttl_cache(ttl_seconds=_META_TTL, maxsize=1)
def get_recent_forecast_dates(n: int = 3):
    """Real, most-recent `n` distinct calendar dates with ANY real storm
    track in TC_TRACKS, each paired with that date's own latest real
    forecast cycle (run) — e.g. [("2026-08-02", "18"), ("2026-08-01", "12"),
    ("2026-07-31", "00")], newest first.

    Used to keep the per-country tile cache warm for whatever storms are
    ACTUALLY recent, not just the app's fixed demo scenarios — see
    _prewarm_recent_tile_cache in pages/map_shell_concept.py (added per
    explicit user request: "make sure the most recent 3 days are also warm
    for loading, in addition to the demo scenarios")."""
    try:
        df = _run_query(
            "SELECT CAST(FORECAST_TIME AS DATE) AS D, MAX(FORECAST_TIME) AS LATEST_TS "
            "FROM TC_TRACKS GROUP BY D ORDER BY D DESC LIMIT %s",
            params=[n],
        )
        out = []
        for _, row in df.iterrows():
            ts = row['LATEST_TS']
            if pd.isna(ts):
                continue
            ts = pd.Timestamp(ts)
            # Snap to the nearest synoptic run (00/06/12/18Z, the only real
            # cycle hours) — real forecast times are always exactly on one
            # of these already, this is just a defensive floor, same
            # convention pages/map_shell_concept.py's own
            # _DEFAULT_FORECAST_RUN resolution uses.
            run = (ts.hour // 6) * 6
            out.append((ts.strftime('%Y-%m-%d'), f"{run:02d}"))
        return out
    except Exception as e:
        logger.error("Error querying recent forecast dates: %s", e)
        return []