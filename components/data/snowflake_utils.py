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
# Hazard data-source vocabulary
# ---------------------------------------------------------------------------
# Single source of truth for the data-source names the `source` parameter on
# get_precip_forecast_time_near/get_river_extent_forecast_time_for_date/
# get_wind_tile_bitmask/get_gust_tile_bitmask validates against (each raises
# NotImplementedError for anything else). Exported so
# pages/map_shell_concept.py's own _DEFAULT_HAZARD_SOURCES (the UI-facing
# per-hazard default) can import these instead of re-hardcoding the same two
# literal strings independently, so the two modules stay in sync by
# construction rather than by coincidence.
HAZARD_SOURCE_ECMWF = "ecmwf"
HAZARD_SOURCE_GLOFAS = "glofas"

# ---------------------------------------------------------------------------
# TTL-based cache
# ---------------------------------------------------------------------------
# Time-bounded expiry so the Dash app automatically serves fresh Snowflake
# data without a container restart.
#
# Sliding PER-ENTRY TTL, thread-safe LRU. Each entry expires ttl_seconds
# after IT was individually cached (not on a shared int(time.time() //
# ttl_seconds) bucket boundary), so misses spread out over time instead of
# every entry across every function decorated with this cache expiring at
# the same instant and causing a cache stampede under concurrent traffic.
# services/tile_server.py carries its own copy of this same decorator for
# consistency.
#
# Per-key single-flight: a miss is computed OUTSIDE the global `lock` (a slow
# cold key must never block lookups/hits on unrelated keys). `pending` holds
# one threading.Lock per key currently being computed; the first caller for a
# key becomes its "owner" and actually calls func(), every other concurrent
# caller for that exact key blocks on the owner's lock instead of
# re-querying, then re-checks the cache once unblocked (a hit, since the
# owner just populated it). Entries are removed from `pending` as soon as
# their computation finishes (success or exception) so the dict never grows
# unbounded: it only ever holds keys with a computation genuinely in flight.

_META_TTL    = 15 * 60   # 15 min: storm list, forecast times (new storms appear promptly)
_IMPACT_TTL  = 15 * 60   # 15 min: impact queries (new pipeline output picked up within 15 min)
_BASE_TTL    = 60 * 60   # 60 min: base layers (schools/HCs/tiles; change only on re-init)


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
            # Python call stack in every still-waiting caller. Each retry
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

                # Computed outside the lock: a slow Snowflake-backed miss on
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

# Per-thread connection storage: each Gunicorn worker thread gets its own connection
_thread_local = threading.local()
_HEALTH_CHECK_INTERVAL = 300  # seconds: recheck liveness at most once every 5 min

# ---------------------------------------------------------------------------
# Shared query executor
# ---------------------------------------------------------------------------
# Every Snowflake fan-out in this codebase (curve popups, per-country panels,
# multi-storm lookups) routes through one shared, long-lived
# ThreadPoolExecutor rather than each call site opening its own throwaway
# `with ThreadPoolExecutor(...) as ex:` block. A throwaway pool pays a fresh
# connect() handshake per worker thread on a cold `_thread_local`
# (0.47-0.65s), and when the executor exits, that thread dies with its
# Snowflake connection still open; nothing ever calls .close() on it, so
# sessions leak until GC/server-side timeout.
#
# One shared, long-lived pool avoids both: worker threads never exit while
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


# Second, SEPARATE long-lived pool.
# pages/map_shell_concept.py's _fetch_family_member_frames is itself called
# from INSIDE _SHARED_QUERY_EXECUTOR's own worker threads in several places
# (_combined_stats._fetch, _country_compare_bundle, _update_impact_summary.
# _build_block), so it cannot submit its own per-country fan-out to that
# SAME bounded pool without risking a nested-pool deadlock (see that
# function's own docstring). A throwaway `with ThreadPoolExecutor(...) as
# ex:` per call would avoid the deadlock but reintroduce the same
# connection-leak/cold-handshake anti-pattern _SHARED_QUERY_EXECUTOR's own
# module comment above documents avoiding (each throwaway thread pays a
# fresh 0.47-0.65s connect() on a cold _thread_local, then leaks that open
# connection when the pool tears down at the end of the `with` block). A
# second module-level, never-torn-down pool gets both properties at once:
# isolated from _SHARED_QUERY_EXECUTOR (no deadlock risk), and long-lived
# (each worker thread's _thread_local connection is created once and reused
# across every future call routed to that thread, same as the primary
# pool). Sized smaller (8, not 24) since its own callers only ever fan out
# over a handful of selected countries, never a broad query fan-out.
_MEMBER_FETCH_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="member-fetch")


def get_member_fetch_executor() -> ThreadPoolExecutor:
    """Process-wide, long-lived executor DISTINCT from get_query_executor()'s
    own pool. See _MEMBER_FETCH_EXECUTOR's own comment for why a second
    pool exists at all. Use `.map(fn, items)`; do not use as a context
    manager (same reasoning as get_query_executor())."""
    return _MEMBER_FETCH_EXECUTOR

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
    independent connection: avoids race conditions when multiple threads
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
            # Recent check passed: trust the connection
            return conn
        # Time for a periodic liveness check
        if _is_connection_alive(conn):
            _thread_local.last_health_check = time.monotonic()
            return conn
        # Connection is dead: close and fall through to reconnect
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
        # string. Explicitly set it so every new thread session has a warehouse.
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

    River-flood data is NOT storm-scoped (see MERCATOR_TILE_RIVER_MAT's own schema:
    keyed by COUNTRY + FORECAST_TIME + RP_TIER only, no STORM/TRACK_ID column at all)
    so, unlike wind/gust, it can't be resolved from get_latest_forecast_time_overall()
    (that's TC_TRACKS-specific). Production GloFAS data can lag behind the latest wind
    storm cycle by multiple days, since the two are ingested on independent schedules.
    Returns None (not a fallback date) when a country has no river data at all, same
    "don't paper over a real gap" convention as get_latest_forecast_time_overall's own
    None return.
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

    Rainfall sibling of get_latest_river_forecast_time, same independence from
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
    per-country, MAT-table-backed), this reads MET_FORECASTS directly: the
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
    This one reads RIVER_FORECASTS directly, PARAM='dis24' only: the raw
    discharge Zarr is a single GLOBAL file per forecast cycle covering the
    whole world's river network cells, not scoped to any country or storm
    (mirrors get_latest_precip_forecast_time's own global/no-country
    convention for the tp Zarr).

    *** LEGACY: services/tile_server.py's "river-raw" raster endpoint
    (/tiles/raster/river-raw/...) does not use PARAM='dis24' at all; it is
    keyed by PARAM='extent_rp10_bymember' instead (see
    get_latest_river_extent_forecast_time below), an RP10-matched
    flood-extent product rather than raw, unthresholded discharge. This
    function is kept only because it may still back other callers reading
    the raw dis24 series directly; it is NOT what the current river-raw map
    layer is keyed by. ***

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

    This is the data source behind services/tile_server.py's current
    "river-raw" raster layer (/tiles/raster/river-raw/...); it replaces
    get_latest_river_raw_forecast_time's own PARAM='dis24' raw discharge
    with GloFAS discharge already matched against the JRC historical
    flood-extent raster at the RP10 (10-year
    return period) tier. RP10 is used because it is the only tier confirmed
    genuinely computed (IS_STANDIN=False); RP2/RP5 are confirmed
    IS_STANDIN=True placeholder/extrapolated data and are deliberately never
    queried here.

    Unlike PARAM='dis24' (keyed by a full datetime FORECAST_TIME), this data
    is keyed by a plain DATE (e.g. "2026-07-14"): callers should not assume
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
            # [:10]: RIVER_FORECASTS.FORECAST_TIME is a TIMESTAMP column (a
            # real per-row datetime64[ns] once read via pandas), so str() of
            # the raw value is "YYYY-MM-DD HH:MM:SS", not the plain
            # "YYYY-MM-DD" this function's own docstring promises. Every
            # downstream caller (tile_server.py's _RiverExtentCache, keyed
            # by this exact string) treats river-extent forecast_time as a
            # plain date; without this slice, this function's own callers
            # land under a DIFFERENT cache key than services/tile_server.py's
            # own _resolve_latest() (which normalizes the same way), causing
            # a real, measured duplicate ~10-30s parquet re-download for the
            # identical calendar date already warm in memory.
            return str(df['FORECAST_TIME'].iloc[0])[:10], str(df['STAGE_PATH'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting latest river-extent forecast time: %s", e)
        return None


# How far the single closest real PARAM='tp' cycle is allowed to be from the
# requested topbar date/run before get_precip_forecast_time_near gives up and
# reports "not available" instead of silently showing a very stale cycle as
# if it were current. 3 days is generous enough to bridge tp's own irregular
# cadence (cycles land at varying hours, sometimes days apart) while still
# refusing a cycle that's clearly unrelated to what was asked for.
@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_precip_forecast_time_near(target_date: str, target_time: str = "00", source: str = HAZARD_SOURCE_ECMWF):
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

    `source` (see pages/map_shell_concept.py's own _DEFAULT_HAZARD_SOURCES
    comment for the full "why"): ECMWF's own `tp` field is the only source
    implemented today. Accepted here (not silently ignored) so a caller
    passing anything else fails LOUDLY rather than silently getting ECMWF
    data under a different label; matches this project's own "hard
    structural gaps should raise loudly" convention. A future second source
    (e.g. a Google WeatherNext-derived rain field) would add a branch here,
    keyed the same way.

    Args:
        target_date: 'YYYY-MM-DD' (topbar-date's own value format).
        target_time: '00'/'06'/'12'/'18' (topbar-time's own run value
            format). Defaults to '00' only as a defensive floor: callers
            should always pass the live topbar-time value.
        source: only 'ecmwf' (the default) is implemented.

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path) for the exact
        real PARAM='tp' row at this date+run, or None if no real cycle
        exists at exactly that timestamp.
    """
    if source != HAZARD_SOURCE_ECMWF:
        raise NotImplementedError(f"get_precip_forecast_time_near: source={source!r} not implemented (only {HAZARD_SOURCE_ECMWF!r} exists today)")
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
def get_river_extent_forecast_time_for_date(target_date: str, rp_tier: str = "rp10", source: str = HAZARD_SOURCE_GLOFAS):
    """Real FORECAST_TIME + STAGE_PATH in RIVER_FORECASTS
    (PARAM='extent_{rp_tier}_bymember') for a given topbar date, for the raw
    global river flood-extent raster layer.

    `source` (see pages/map_shell_concept.py's own _DEFAULT_HAZARD_SOURCES
    comment for the full "why"): GloFAS is the only source implemented
    today. Accepted here (not silently ignored) so a caller passing
    anything else fails LOUDLY rather than silently getting GloFAS data
    under a different label; matches this project's own "hard structural
    gaps should raise loudly" convention. A future second source (e.g.
    Google FloodHub) would add a branch here; note it would very likely
    need its own, DIFFERENT per-member combination methodology too, not
    just a new table (see the same memory note above: FloodHub is not
    ECMWF-ensemble-driven, so it would not share Wind/Rain's member
    identity the way GloFAS does today).

    `rp_tier` (default "rp10", matching the slider's own default) is
    parameterized rather than hardcoded, since a caller may request any
    return-period tier the ms-river-slider is set to. All 6 tiers
    (rp2/rp5/rp10/rp20/rp50/rp100) are generated together per pipeline run
    for a given date, so in practice every tier resolves to the same
    forecast_time for the same date, but this is parameterized properly
    rather than assuming that always holds.

    Unlike precip's irregular cycle hours (see get_precip_forecast_time_near
    above), extent_rp10_bymember cycles are DAILY only (one cycle per
    calendar day), so all four topbar-time values (00Z/06Z/12Z/18Z) for a
    given topbar-date resolve to that SAME day's single cycle when one
    exists; there's no "nearest hour" concept needed the way precip has.
    Matches on calendar date (CAST(...AS DATE), not a hardcoded literal
    list) so this keeps working generally as new dates land.

    Args:
        target_date: 'YYYY-MM-DD' (topbar-date's own value format).
        source: only 'glofas' (the default) is implemented.

    Returns:
        tuple[str, str] | None: (forecast_time, stage_path): forecast_time
        is the EXACT value stored in RIVER_FORECASTS (a plain date string,
        e.g. '2026-07-14', matching get_latest_river_extent_forecast_time's
        own convention so tile_server.py's exact-match by-time lookup keeps
        working unchanged) for the real extent_rp10_bymember row matching
        target_date's calendar day, or None if no such real row exists.
    """
    if source != HAZARD_SOURCE_GLOFAS:
        raise NotImplementedError(f"get_river_extent_forecast_time_for_date: source={source!r} not implemented (only {HAZARD_SOURCE_GLOFAS!r} exists today)")
    try:
        df = _run_query(
            "SELECT FORECAST_TIME, STAGE_PATH FROM AOTS.TC_ECMWF.RIVER_FORECASTS "
            "WHERE PARAM = %s AND CAST(FORECAST_TIME AS DATE) = TO_DATE(%s) "
            "ORDER BY FORECAST_TIME DESC LIMIT 1",
            params=[f"extent_{rp_tier}_bymember", target_date],
        )
        if not df.empty and pd.notna(df['FORECAST_TIME'].iloc[0]):
            # [:10]: same TIMESTAMP-vs-plain-date normalization as
            # get_latest_river_extent_forecast_time's own comment above;
            # this function is the REAL default page-load path for the raw
            # River layer (called from pages/map_shell_concept.py on every
            # page load and topbar date/time change), so without this slice
            # every normal user session lands under a different cache key
            # than tile_server.py's own _resolve_latest()/prewarm loop keep
            # warm, defeating both.
            return str(df['FORECAST_TIME'].iloc[0])[:10], str(df['STAGE_PATH'].iloc[0])
        return None
    except Exception as e:
        logger.error("Error getting river-extent forecast time for date %s (rp_tier=%s): %s", target_date, rp_tier, e)
        return None


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_envelope_data_snowflake(track_id, forecast_time):
    """Get envelope data directly from Snowflake.

    This query is NOT threshold-scoped: it always fetches every
    WIND_THRESHOLD x ENSEMBLE_MEMBER row for the given track/forecast_time
    (the caller, _build_ms_envelope_geojson, filters to one threshold
    client-side afterward). Caching on (track_id, forecast_time) only (no
    threshold in the key) is correct since the result never depends on the
    threshold at all, so every threshold-slider tick reuses the same cached
    full-dataset result instead of re-running the ST_ASWKT() query.
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
    """Per-member GUST envelope polygons: TC_GUST_ENVELOPES_COMBINED, exact
    mirror of get_envelope_data_snowflake/TC_ENVELOPES_COMBINED above but
    keyed by GUST_THRESHOLD (a separately deployed table from
    TC_ENVELOPES_COMBINED). Cached on (track_id, forecast_time) for the same
    reason as get_envelope_data_snowflake above, not threshold-scoped, so
    every gust-slider tick reuses the identical cached full dataset."""
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
    exact forecast_time: the gust-availability sibling of
    get_track_ids_for_date (TC_TRACKS). Gust is storm-scoped (unlike River/
    Rain, which are country+date-scoped with no storm dimension at all), so
    "is gust available" means "does THIS storm at THIS forecast_time have
    any real gust envelope rows", not a country-wide latest-date lookup.
    Gust coverage can genuinely differ storm-by-storm and cycle-by-cycle
    from wind/track coverage, so a storm with active TC_TRACKS data may
    still have zero TC_GUST_ENVELOPES_COMBINED rows at the same
    forecast_time; Gust must show as genuinely unavailable in that case,
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
    single Snowflake round trip: the Global-mode ("no country selected")
    replacement for looping a single-storm TC_TRACKS query once per active
    storm.

    Cached, since which tracks exist doesn't depend on the threshold slider
    at all: a threshold-slider tick should hit the cache, not re-run this
    query. Callers MUST pass a tuple, not a list, for storm_forecast_pairs:
    @ttl_cache's underlying lru_cache needs every arg hashable.

    Args:
        storm_forecast_pairs: tuple of (track_id, forecast_time) tuples.
            track_id is TC_TRACKS' own TRACK_ID (the storm name). forecast_time
            is "YYYY-MM-DD HH:MM:SS" (TC_TRACKS' own FORECAST_TIME column
            format). Each storm carries its OWN forecast_time here rather than
            one shared timestamp for all of them: different storms are not
            guaranteed to share one real forecast cycle even when both are
            "active" on the same selected calendar date/run.

    Builds one compound WHERE ((TRACK_ID = %s AND FORECAST_TIME = %s) OR ...)
    rather than issuing one query per storm, so this stays a single query
    regardless of how many storms are active at once.

    Returns:
        pandas.DataFrame with columns TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME,
        LEAD_TIME, LATITUDE, LONGITUDE, WIND_SPEED_KNOTS, PRESSURE_HPA: the
        extra TRACK_ID column (vs. the single-storm track query used
        elsewhere, which doesn't need it since it's already scoped to one
        storm) lets callers attribute each row back to its own storm when
        combining several storms' tracks into one FeatureCollection.
        Empty DataFrame (not an exception) when storm_forecast_pairs is empty
        or the query fails.

    Filters LATITUDE/LONGITUDE IS NOT NULL at the query level, matching this
    file's other TC_TRACKS point-queries (e.g. get_lat_lons_bulk). A NULL
    LONGITUDE reaching pages/map_shell_concept.py's _unwrap_track_lons would
    produce a NaN diff, which fails both the >180 and <-180 antimeridian
    comparisons silently, letting a crossing straddling that row render as a
    globe-spanning line. Filtering here is simpler than teaching the unwrap
    function to skip/interpolate NaNs.
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
    WHERE ({" OR ".join(conditions)})
      AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
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
    """Single-storm TC_TRACKS query: the Country-Analysis-mode counterpart
    of get_multi_storm_tracks above (that one batches several storms into
    one query for Global mode; this is the single-storm case used once a
    country is selected).

    Cached on (storm, forecast_time), since tracks don't depend on the
    wind/gust threshold slider in any way: every slider tick should hit
    the cache for the identical (storm, forecast_time) rather than re-run
    this query, same as its envelope-query siblings
    (get_envelope_data_snowflake / get_gust_envelope_data_snowflake).
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
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
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
    being far out at sea with zero measurable country impact yet -- the
    Global-mode "show every real track" feature needs exactly this
    track-existence question, matching /legacy's own precedent
    (load_startup_tracks, pages/dashboard.py:324-397, queries TC_TRACKS
    directly with no impact join at all).

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
    Timezone-independent: comparison always done in UTC via CONVERT_TIMEZONE.

    Applies the 12h cutoff as a WHERE predicate before the GROUP BY, not only
    in a HAVING clause afterward, so it prunes the 206M-row
    MERCATOR_TILE_IMPACT_MAT scan up front rather than narrowing after a full
    scan. FORECAST_DATE is a 'YYYYMMDDHH24MISS' string, which sorts
    lexicographically the same as it sorts chronologically, so a plain string
    >= comparison against a cutoff computed the same way (DATEADD/
    CONVERT_TIMEZONE, still Snowflake's own clock, not the app server's)
    works as a real WHERE predicate and enables micro-partition pruning on
    both scans below. Any row that's a country's true MAX(FORECAST_DATE) and
    within 12h of now is, by definition, >= (now - 12h), so this WHERE
    cutoff can never exclude a country an equivalent HAVING clause would
    have kept: a separate HAVING check in the `latest` CTE would be
    redundant.
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
    reactively: a different question from get_active_storm_countries() above
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
    `wind_threshold` (kt): every storm affecting any country on this exact
    date/threshold, in one query, not looped per-country.

    Powers a date-reactive "Active Storms" list: unlike
    get_active_storm_countries() (always "last 12h", used only for the
    genuinely-live signal), this answers "what storms have real data on
    THIS specific date" for any date at all, including a historical one
    with nothing currently live (e.g. a Demo Scenario or the date picker
    pointed at a past event), so the same bordered storm-row UI can show
    real historical storms too, not just live ones.

    `wind_threshold` MUST be pinned to a single value, not summed/grouped
    across every threshold this table has (34/40/50/64/83/96/113/137kt):
    each is its own separate exceedance estimate (population exposed to AT
    LEAST that wind speed), so summing across all of them isn't a real
    total, it's 8 overlapping estimates added together. Defaults to 50kt,
    this page's own established "today's baseline" convention
    (_resolve_wind_kt's default), not reactive to ms-wind-slider here,
    since threading that in would create a circular Dash dependency (the
    slider itself is rendered INSIDE this same section's own output).

    Same non-zero-impact HAVING clause as get_active_storm_countries() above
    (SUM(E_population) > 0 OR SUM(E_num_schools) > 0 OR SUM(E_num_hcs) > 0):
    without it, a wide low-probability forecast-cone tile technically exists
    for many nearby countries even when the real impact there is negligible.

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
# Impact data queries: *_MAT tables
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
    Real per-tile GUST impact totals: MERCATOR_TILE_GUST_MAT, same shape as
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


# Mirrors services/tile_server.py's own _RIVER_WINDOW_DEFAULT exactly (that
# file's own comment has the full rationale: 168h/the full forecast horizon
# is a backward-compat-EXACT default, not an approximation, since the
# cumulative union at 168h already equals what an unconditional MAX()-
# across-every-STEP_H query would return). Duplicated here rather than
# imported since this module stays import-independent of the tile-server
# process (same convention already used for _PRECIP_RATE_WINDOWS_H etc.).
_RIVER_WINDOW_DEFAULT = 168


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_river_tile_impacts(country: str, forecast_time: str, rp_tier: str,
                            window_h: int = _RIVER_WINDOW_DEFAULT) -> pd.DataFrame:
    """
    Real per-tile RIVER flood-extent impact totals: MERCATOR_TILE_RIVER_MAT,
    keyed by COUNTRY + FORECAST_TIME + RP_TIER + STEP_H (not storm-scoped at
    all, see services/tile_server.py's own _MERCATOR_RIVER_IMPACT_ONLY_SQL
    comment).

    `window_h`: River's per-country impact numbers carry a STEP_H column
    meaning a CUMULATIVE window (24/72/120/168h), not a single-day snapshot.
    Each STEP_H row is already the correct union for that window, so this
    is a plain `= %s` filter, not a MAX()-across-everything collapse.
    Defaults to the full forecast horizon (168h) for any caller that
    doesn't pass a window: see _RIVER_WINDOW_DEFAULT's own comment for why
    that's an EXACT backward-compat default, not merely an approximation of
    an unconditional-MAX collapse.

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
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
        GROUP BY ZONE_ID
        """
        df = _run_query(query, params=[country, forecast_time, rp_tier, window_h or _RIVER_WINDOW_DEFAULT])
        logger.info("Loaded %d river tile impact rows (%s/%s/%s/%sh)", len(df), country, forecast_time, rp_tier, window_h)
        return df.copy()
    except Exception as e:
        logger.error("Error querying MERCATOR_TILE_RIVER_MAT: %s", e)
        return pd.DataFrame()


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_rain_tile_impacts(country: str, forecast_time: str, threshold_mm, window_h) -> pd.DataFrame:
    """
    Per-tile RAINFALL impact totals: MERCATOR_TILE_PRECIP_MAT, keyed by
    COUNTRY + FORECAST_TIME + THRESHOLD_MM + WINDOW_H.

    This table carries a full hazard-conditional E_* breakdown, same shape
    as wind's own MERCATOR_TILE_IMPACT_MAT (minus the E_*_IN_NEED columns,
    which come from a separate vulnerability table wind has and precip
    doesn't: see this file's own module docstring on precip's scope).

    Returns pandas.DataFrame with columns: ZONE_ID, ADMIN_ID, PROBABILITY,
    E_POPULATION, E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION,
    E_ADOLESCENT_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS, E_NUM_HCS,
    E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS, E_SMOD_CLASS_L1, E_RWI.
    """
    try:
        query = """
        SELECT ZONE_ID, ADMIN_ID, PROBABILITY, E_POPULATION,
               E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION, E_ADOLESCENT_POPULATION,
               E_BUILT_SURFACE_M2, E_NUM_SCHOOLS, E_NUM_HCS, E_NUM_SHELTERS, E_NUM_WASH,
               E_SMOD_CLASS, E_SMOD_CLASS_L1, E_RWI
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
# Duplicated from map_shell_concept.py's own _RAIN_MM_BY_WINDOW, same reason
# as the tier lists above (import-graph ordering); keep in sync.
_TOTALS_PRECIP_MM_BY_WINDOW = {"6": [25, 50, 75], "24": [35, 70, 103], "72": [45, 90, 133], "120": [50, 100, 150]}

_TOTALS_IMPACT_COLS = [
    "E_POPULATION", "E_INFANT_POPULATION", "E_SCHOOL_AGE_POPULATION",
    "E_ADOLESCENT_POPULATION", "E_NUM_SCHOOLS", "E_NUM_HCS",
    "E_NUM_SHELTERS", "E_NUM_WASH",
]
# MERCATOR_TILE_PRECIP_MAT carries the full hazard-conditional E_*
# breakdown, same shape as every other hazard, so this is just
# _TOTALS_IMPACT_COLS again.
_TOTALS_PRECIP_IMPACT_COLS = _TOTALS_IMPACT_COLS


def _zero_precip_impact_totals() -> dict:
    return {col: 0 for col in _TOTALS_PRECIP_IMPACT_COLS}


def _row_to_precip_impact_totals(row) -> dict:
    return {col: (int(row[col]) if pd.notna(row[col]) else None) for col in _TOTALS_PRECIP_IMPACT_COLS}


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
def get_tile_impact_totals_by_threshold(country: str, storm: str, forecast_date: str, zoom_level: int = 14,
                                          river_window: int = _RIVER_WINDOW_DEFAULT) -> dict:
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
            "wind":   {34: {...}, 40: {...}, ..., 137: {...}},   # kt -> totals
            "gust":   {17: {...}, 21: {...}, ...,  70: {...}},   # kt -> totals
            "river":  {"rp2": {...}, ..., "rp100": {...}},        # rp_tier -> totals
            "precip": {"6": {25: {...}, 50: {...}, 75: {...}}, "24": {...}, "72": {...}, "120": {...}},
        }
    where each wind/gust/river `{...}` is {"E_POPULATION": int|None,
    "E_INFANT_POPULATION": int|None, "E_SCHOOL_AGE_POPULATION": int|None,
    "E_ADOLESCENT_POPULATION": int|None, "E_NUM_SCHOOLS": int|None,
    "E_NUM_HCS": int|None, "E_NUM_SHELTERS": int|None, "E_NUM_WASH": int|None},
    and each precip `{...}` is just {"E_POPULATION": int|None} (see
    _TOTALS_PRECIP_IMPACT_COLS's own comment: MERCATOR_TILE_PRECIP_MAT has
    no real per-threshold facility/age columns). None only when that column
    is genuinely all-NULL for this country (a real dataset gap), 0 when the
    threshold tier simply has no matching rows (a real, confirmed-zero
    exposure at that tier).

    `river_window`: River's own per-RP-tier curve reflects this ONE
    cumulative window (24/72/120/168h, default the full 168h horizon): see
    get_river_tile_impacts's own
    docstring for the underlying STEP_H semantics. Unlike precip's own 2D
    "river" this stays a flat {rp_tier: {...}} shape at whichever single
    window is currently selected, not a window x tier grid.

    Every canonical threshold in _TOTALS_WIND_THRESHOLDS_KT/
    _TOTALS_GUST_THRESHOLDS_KT/_TOTALS_RIVER_RP_TIERS/
    _TOTALS_PRECIP_MM_BY_WINDOW is always present as a key: a tier absent
    from the query result (genuinely 0 rows) is filled with
    _zero_impact_totals()/_zero_precip_impact_totals(), not omitted, so
    callers never have to special-case a missing key as "no data" when it
    really means "real zero". Precip's own window keys ("6"/"24"/"72"/"120")
    are always all four, same convention one level up.

    River and precip are NOT storm-scoped (see get_river_tile_impacts's/
    get_rain_tile_impacts's own docstrings), so `storm` is ignored for those
    two sections, and each resolves its own forecast time independently via
    get_latest_river_forecast_time(country)/get_latest_rain_forecast_time(country)
    rather than reusing `forecast_date` (which is wind's cycle, and can
    genuinely differ from river's/precip's, since GloFAS and ECMWF precip
    are ingested on independent schedules from wind). The "river"/"precip"
    keys are {} (not zero-filled) when the country has no river/precip data
    of any kind, a real dataset gap, not a per-tier zero.

    Precip's own data is genuinely 2D (threshold_mm x window_h, see
    MERCATOR_TILE_PRECIP_MAT's own schema: 4 window_h values [6, 24, 72,
    120] x 3 threshold_mm tiers each): the whole
    grid is fetched in ONE query (GROUP BY WINDOW_H, THRESHOLD_MM), same
    "one round trip regardless of which tier the user currently has
    selected" design as wind/gust/river's own full-tier fetch, needed here
    specifically because the dashboard's own Rainfall heatmap
    (_rain_threshold_grid) shows all 4 windows at once, not just the
    currently selected one.
    """
    result = {"wind": {}, "gust": {}, "river": {}, "precip": {}}

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
            # Filters to the caller's cumulative `river_window` (default
            # 168h/the full horizon: see _RIVER_WINDOW_DEFAULT's own
            # comment for why that's an EXACT backward-compat default).
            # Deliberately NOT expanded into a full 2D (tier x window)
            # structure the way precip's own "river"-sibling key below is
            # 2D (window x mm): this feeds one curve (per-RP-tier) at
            # whichever ONE window is currently selected, not a picker
            # over every window at once; expanding to a full 2D curve
            # picker would be a separate UI feature.
            river_df = _run_query(
                """
                WITH per_zone_max AS (
                    SELECT RP_TIER, ZONE_ID, """ + ", ".join(f"MAX({c}) AS {c}" for c in _TOTALS_IMPACT_COLS) + """
                    FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT
                    WHERE COUNTRY = %s AND FORECAST_TIME = %s AND STEP_H = %s
                    GROUP BY RP_TIER, ZONE_ID
                )
                SELECT RP_TIER, """ + ", ".join(f"SUM({c}) AS {c}" for c in _TOTALS_IMPACT_COLS) + """
                FROM per_zone_max
                GROUP BY RP_TIER
                """,
                params=[country, river_forecast_time, river_window or _RIVER_WINDOW_DEFAULT],
            )
            by_tier = {str(row["RP_TIER"]): _row_to_impact_totals(row) for _, row in river_df.iterrows()}
            result["river"] = {tier: by_tier.get(tier, _zero_impact_totals()) for tier in _TOTALS_RIVER_RP_TIERS}
    except Exception as e:
        logger.warning("get_tile_impact_totals_by_threshold river failed for %s: %s", country, e)

    try:
        precip_forecast_time = get_latest_rain_forecast_time(country)
        if precip_forecast_time is not None:
            precip_df = _run_query(
                """
                SELECT WINDOW_H, THRESHOLD_MM, """ + ", ".join(f"SUM({c}) AS {c}" for c in _TOTALS_PRECIP_IMPACT_COLS) + """
                FROM AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT
                WHERE COUNTRY = %s AND FORECAST_TIME = %s
                GROUP BY WINDOW_H, THRESHOLD_MM
                """,
                params=[country, precip_forecast_time],
            )
            by_window_mm = {}
            for _, row in precip_df.iterrows():
                by_window_mm.setdefault(str(int(row["WINDOW_H"])), {})[int(row["THRESHOLD_MM"])] = _row_to_precip_impact_totals(row)
            result["precip"] = {
                window: {mm: by_window_mm.get(window, {}).get(mm, _zero_precip_impact_totals()) for mm in mm_tiers}
                for window, mm_tiers in _TOTALS_PRECIP_MM_BY_WINDOW.items()
            }
    except Exception as e:
        logger.warning("get_tile_impact_totals_by_threshold precip failed for %s: %s", country, e)

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
    Per-member GUST severity: TRACK_GUST_MAT, the gust mirror of
    get_track_impacts/TRACK_MAT above (a separately deployed table from
    TRACK_MAT).

    Cached with the same @ttl_cache as get_track_impacts (wind's own
    sibling, just above), so a repeated gust threshold change on the
    identical (country, storm, forecast_date, gust_threshold) tuple hits
    the cache instead of re-querying TRACK_GUST_MAT.

    Only ZONE_ID (member number) + SEVERITY_POPULATION are needed by this
    app's one caller (_build_ms_envelope_geojson's severity-by-member
    lookup, which never reads geometry from this function at all: the
    envelope polygon geometry comes from get_gust_envelope_data_snowflake
    instead), a plain DataFrame, no WKB/GeoDataFrame parsing needed.

    No vulnerability join: there is no TRACK_GUST_VULNERABILITY_MAT (no
    such table exists), matching the same "no PIN/CHIN for gust" pattern
    already established for every other gust-specific table in this app
    (MERCATOR_TILE_GUST_MAT etc.).
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


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_wind_tile_bitmask(country: str, storm: str, forecast_date: str, wind_threshold: int, source: str = HAZARD_SOURCE_ECMWF) -> pd.DataFrame:
    """
    Per-z14-tile, per-ensemble-member coverage bitmask: TILE_WIND_
    BITMASK_MAT. Bit `m-1` set <=> ensemble member `m`'s envelope covers
    that tile, same bit convention services/tile_server.py's own
    _RiverExtentCache uses for River's raw-layer bitmask.

    `source` (see pages/map_shell_concept.py's own _DEFAULT_HAZARD_SOURCES
    comment for the full "why"): ECMWF is the only source implemented
    today; TILE_WIND_BITMASK_MAT itself has no SOURCE column yet (a future
    source, e.g. Google WeatherNext, would need its own table, same
    "separate tables per hazard, combined only via the app layer"
    convention this repo's own ORCHESTRATION MAT tables already follow,
    not an ALTER TABLE onto this one). Accepted here (not silently
    ignored) so a caller passing anything else fails LOUDLY rather than
    silently getting ECMWF data under a different label.

    A plain SELECT against a small, already-materialized MAT table (an
    envelope-vs-tile spatial join computed once in DATAPIPELINE, see
    calculate_tile_member_bitmask() in that repo's impact_analysis.py): no
    heavy per-request file download/decode step the way River/Rain's own
    raw-layer caches need, so this is a plain @ttl_cache-decorated query
    function, not a new _DataCache/_RiverExtentCache-style class.

    Same (country, storm, forecast_date, wind_threshold) key shape as
    get_track_impacts/TRACK_MAT above: `forecast_date` must be the
    mat_forecast_date string (YYYYMMDDHHMMSS), not the raw-track
    forecast_time form (YYYY-MM-DD HH:MM:SS).

    Returns:
        DataFrame with columns TILE_ID (str, z14 quadkey), BITS (real
        Python int, Snowflake NUMBER(20,0) values arrive as Decimal/int,
        not float, so no precision loss for a 64-bit value). UPPERCASE:
        Snowflake normalizes unquoted column aliases to uppercase
        regardless of how the SQL below spells them (the same gotcha
        _build_ms_envelope_geojson's own docstring documents elsewhere in
        this codebase), so the `AS tile_id`/`AS bits` aliases below are
        purely cosmetic in the SQL text; the real DataFrame columns come
        back as TILE_ID/BITS: deliberately left uppercase here (not
        lowercased) so callers can merge this directly against
        services/tile_server.py's own TILE_ID/BITS convention (River's
        in-memory bitmask, _RiverExtentCache) with zero renaming. One row
        per DISTINCT tile with >=1 member's envelope covering it: a tile
        with zero coverage from every member simply has no row (sparse,
        same convention as River's own bitmask, not a fabricated 0 row).

        Returns None (not an empty DataFrame) specifically when the query
        itself raises, distinct from a legitimately-empty successful
        query (0 rows for this key, a common case e.g. a wind threshold
        this storm never reached). Keeping a Snowflake connection drop /
        permissions issue / renamed table distinguishable from "this
        hazard genuinely covers zero tiles" lets callers such as
        services/tile_server.py's own combined_member_impacts log the two
        cases distinctly via this None-vs-empty signal, though the
        CONTRIBUTION to the union stays the same all-zero matrix either
        way (fail-open, not fail-closed).
    """
    if source != HAZARD_SOURCE_ECMWF:
        raise NotImplementedError(f"get_wind_tile_bitmask: source={source!r} not implemented (only {HAZARD_SOURCE_ECMWF!r} exists today)")
    try:
        query = """
        SELECT
            TILE_ID AS tile_id,
            BITS    AS bits
        FROM AOTS.TC_ECMWF.TILE_WIND_BITMASK_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, wind_threshold])
        logger.info("Loaded %d wind tile-bitmask rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, wind_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying TILE_WIND_BITMASK_MAT: %s", e)
        return None


@ttl_cache(ttl_seconds=_IMPACT_TTL, maxsize=64)
def get_gust_tile_bitmask(country: str, storm: str, forecast_date: str, gust_threshold: int, source: str = HAZARD_SOURCE_ECMWF) -> pd.DataFrame:
    """Gust mirror of get_wind_tile_bitmask above: TILE_GUST_BITMASK_MAT,
    GUST_THRESHOLD instead of WIND_THRESHOLD. Same bit convention, same
    sparse "no row = no coverage" contract, same exception-vs-
    legitimately-empty None/DataFrame distinction, same `source` parameter
    (see get_wind_tile_bitmask's own docstring for the full "why" on
    both)."""
    if source != HAZARD_SOURCE_ECMWF:
        raise NotImplementedError(f"get_gust_tile_bitmask: source={source!r} not implemented (only {HAZARD_SOURCE_ECMWF!r} exists today)")
    try:
        query = """
        SELECT
            TILE_ID AS tile_id,
            BITS    AS bits
        FROM AOTS.TC_ECMWF.TILE_GUST_BITMASK_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND GUST_THRESHOLD = %s
        """
        df = _run_query(query, params=[country, storm, forecast_date, gust_threshold])
        logger.info("Loaded %d gust tile-bitmask rows (%s/%s/%s/%dkt)", len(df), country, storm, forecast_date, gust_threshold)
        return df.copy()
    except Exception as e:
        logger.error("Error querying TILE_GUST_BITMASK_MAT: %s", e)
        return None


# NOTE: there is no per-ADMIN-REGION member bitmask table
# (ADMIN_WIND_BITMASK_MAT / ADMIN_GUST_BITMASK_MAT do not exist): a
# per-region "did member m touch this polygon anywhere" mask cannot
# reproduce the admin layer's own PROBABILITY (which is the AREA-MEAN of
# z14 tile probabilities), so a probability derived from it would read
# higher than every one of its own marginals. The combined admin layer
# instead unions the z14 bitmasks above and aggregates down to regions;
# see services/tile_server.py::_combine_bitmask_aware_admin.


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
    _get_data_availability_real's own _total() treats it that way: a
    country whose base dataset never populated a given facility column
    (e.g. NUM_SHELTERS) should read as "unknown", not "confirmed zero".
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
        # Three separate SUMs (not a single row-level
        # COALESCE(INFANT_POPULATION,0) + COALESCE(SCHOOL_AGE_POPULATION,0) +
        # COALESCE(ADOLESCENT_POPULATION,0) expression) so SQL's own
        # NULL-skipping SUM propagates a true NULL when a whole age-band
        # column is empty for this country, rather than collapsing NULLs to
        # 0 before the outer SUM runs and producing a false 0.0
        # indistinguishable from a genuine zero, while still correctly
        # summing whichever age bands DO have real per-tile data (see
        # below).
        query = """
        SELECT
            SUM(POPULATION)              AS total_population,
            SUM(INFANT_POPULATION)       AS total_infant,
            SUM(SCHOOL_AGE_POPULATION)   AS total_school_age,
            SUM(ADOLESCENT_POPULATION)   AS total_adolescent,
            SUM(BUILT_SURFACE_M2)        AS total_built_surface_m2
        FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT
        WHERE COUNTRY = %s
        """
        df = _run_query(query, params=[country])
        if df.empty or df.iloc[0]["TOTAL_POPULATION"] is None:
            return {"total_population": None, "total_children": None, "total_built_surface_m2": None}
        row = df.iloc[0]
        # Sum only the age bands with real data for this country: a band
        # that's genuinely all-NULL is excluded from the sum (not treated
        # as a real 0), and total_children itself is None only when ALL
        # THREE bands are missing, not just one.
        age_parts = [float(row[c]) for c in ("TOTAL_INFANT", "TOTAL_SCHOOL_AGE", "TOTAL_ADOLESCENT")
                      if pd.notna(row[c])]
        return {
            "total_population": int(row["TOTAL_POPULATION"]) if pd.notna(row["TOTAL_POPULATION"]) else None,
            "total_children":   int(sum(age_parts)) if age_parts else None,
            # Country-wide total built-up surface: a generically useful
            # field of this function's own documented contract
            # (get_country_totals's own docstring). Same
            # None-not-fabricated-0 convention as total_population above.
            "total_built_surface_m2": float(row["TOTAL_BUILT_SURFACE_M2"]) if pd.notna(row["TOTAL_BUILT_SURFACE_M2"]) else None,
        }
    except Exception as e:
        logger.error("Error querying country totals for %s: %s", country, e)
        return {"total_population": None, "total_children": None, "total_built_surface_m2": None}


def get_country_totals(country: str) -> dict:
    """Return total population/children/built-up surface for a country from
    BASE_MERCATOR_TILE_MAT.

    Returns dict with keys: total_population, total_children,
    total_built_surface_m2 (int/float or None on error). Each call returns
    a fresh copy so callers cannot corrupt the cache.
    """
    return dict(_get_country_totals_cached(country))


@ttl_cache(ttl_seconds=_BASE_TTL, maxsize=32)
def get_base_schools(country: str) -> pd.DataFrame:
    """Query BASE_SCHOOL_MAT: all school locations for a country (no storm required)."""
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
    """Query BASE_HC_MAT: all health centre locations for a country (no storm required)."""
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
    """Query BASE_SHELTER_MAT: all shelter locations for a country (no storm required)."""
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
    """Query BASE_WASH_MAT: all WASH facility locations for a country (no storm required)."""
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
    """Per-country facility data-source label: the SOURCE column on
    BASE_SCHOOL_MAT/BASE_HC_MAT/BASE_SHELTER_MAT/BASE_WASH_MAT
    (impact_analysis.py's fetch_schools/fetch_health_centers/fetch_shelters/
    fetch_wash populate it: the custom-CSV source when a custom override
    is used, else the standard API/OSM source name).

    Returns the most common non-null SOURCE value for this country/layer
    (should be one consistent value per country in practice: a single
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
    Query BASE_ADMIN_GEOM_MAT: admin boundary polygons with demographics (no storm required).

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
# Real, already-sent Alert emails (AOTS.TC_ECMWF.ALERT_SENT_LOG): the "view
# past alert emails" feature on the dashboard's Global view. Deliberately
# ALERT-only (not Warning/watch): WATCH_SENT_LOG (the Warning dedup table,
# never renamed from its original "watch" name despite the procedure itself
# being called SEND_WARNING) has no EMAIL_BODY/HTML column at all: confirmed
# by reading 07b_alert_agent/02b_send_warning_procedure.sql directly (its
# CREATE TABLE and only INSERT both list just TRACK_ID/FORECAST_DATE/
# RECIPIENT_COUNT/COUNTRIES). A Warning's generated HTML is used once to call
# the email-send API and then discarded; there is nothing to fetch back for
# a past Warning, so it's out of scope until that changes upstream (needs the
# ORCHESTRATION repo's own explicit sign-off, not this app's to decide).
# ---------------------------------------------------------------------------

@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_storms_with_alert_emails_at(forecast_time: str) -> set:
    """Real set of TRACK_ID values with an alert email at this EXACT
    forecast_time (the topbar's selected date+run, e.g. "2026-08-02
    18:00:00"): used to decide whether a storm row's "view alert emails"
    icon should show AT ALL for the currently selected date/time, not just
    "this storm has ever had any alert". Scoping to the exact forecast_time
    avoids showing the icon for a storm whose only alerts are on a
    different date/run than the one currently selected, which would open
    an empty "no emails" popup.

    EMAIL_BODY IS NOT NULL filter: excludes metadata-only dedup marker rows
    (e.g. backfill dedup rows inserted to suppress SEND_ALERT() re-firing for
    historical/stale storm data during a bitmask backfill, see
    ALERT_SENT_LOG's own '[BACKFILL DEDUP MARKER]' EMAIL_SUBJECT convention)
    that were never a real sent email, so the icon never appears for a row
    with nothing real behind it."""
    try:
        df = _run_query(
            "SELECT DISTINCT TRACK_ID FROM AOTS.TC_ECMWF.ALERT_SENT_LOG "
            "WHERE FORECAST_TIME = TO_TIMESTAMP_NTZ(%s) AND EMAIL_BODY IS NOT NULL",
            params=[forecast_time],
        )
        return set(df['TRACK_ID'].tolist()) if not df.empty else set()
    except Exception as e:
        logger.error("Error querying storms with alert emails at %s: %s", forecast_time, e)
        return set()


@ttl_cache(ttl_seconds=_META_TTL, maxsize=64)
def get_alert_emails_for_storm(track_id: str, forecast_time: str = None):
    """Real list of available alert emails for a storm (ALERT_SENT_LOG), one
    entry per country: a multi-country storm can genuinely have several
    (one email per affected country) for the same forecast run.

    `forecast_time` (optional, e.g. "2026-08-02 18:00:00", the topbar's
    selected date+run, NOT a free-text filter): when given, scopes results
    to that EXACT real forecast cycle only, matching the currently selected
    date/time instead of surfacing every historical alert ever sent for this
    storm, so the popup never shows an alert unrelated to the topbar's
    current date/time selection.

    Returns a list of dicts with keys TRACK_ID/FORECAST_TIME/COUNTRY_CODE/
    EMAIL_SUBJECT (RECIPIENT_COUNT/SENT_AT deliberately NOT selected:
    internal operational metadata, not something to surface in the UI.
    EMAIL_BODY itself is also NOT included here: fetch it separately via
    get_alert_email_body once a specific entry is picked, so listing a
    storm's emails stays cheap even when EMAIL_BODY is large).

    ALERT_SENT_LOG's declared PRIMARY KEY (TRACK_ID, FORECAST_TIME,
    COUNTRY_CODE) is NOT actually enforced by Snowflake (PK/UNIQUE
    constraints there are informational only, never enforced), so more than
    one row can genuinely exist for the same key, e.g. repeated
    test-harness sends over time for the same storm/country/forecast_time.
    QUALIFY + ROW_NUMBER keeps only the single most-recently-sent row per
    (track_id, forecast_time, country_code), so a storm/country pair is
    never listed more than once for the same forecast cycle.

    EMAIL_BODY IS NOT NULL filter: same reasoning as
    get_storms_with_alert_emails_at's own docstring: excludes metadata-only
    backfill dedup marker rows that were never a real sent email, so this
    never lists an entry whose EMAIL_BODY fetch would come back empty."""
    try:
        sql = (
            "SELECT TRACK_ID, FORECAST_TIME, COUNTRY_CODE, EMAIL_SUBJECT FROM AOTS.TC_ECMWF.ALERT_SENT_LOG "
            "WHERE TRACK_ID = %s AND EMAIL_BODY IS NOT NULL"
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

    ALERT_SENT_LOG's declared PRIMARY KEY isn't actually enforced by
    Snowflake (see get_alert_emails_for_storm's own docstring), so more
    than one row can genuinely match this exact key. Orders by SENT_AT DESC
    and takes the first (the single latest send) rather than whatever
    arbitrary row order Snowflake happens to return. Content itself is
    otherwise immutable once sent, so caching the result (by these 3 args)
    is still safe."""
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
    forecast cycle (run), e.g. [("2026-08-02", "18"), ("2026-08-01", "12"),
    ("2026-07-31", "00")], newest first.

    Used to keep the per-country tile cache warm for whatever storms are
    ACTUALLY recent, not just the app's fixed demo scenarios: see
    _prewarm_recent_tile_cache in pages/map_shell_concept.py, which warms
    the most recent days in addition to the demo scenarios."""
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
            # cycle hours): real forecast times are always exactly on one
            # of these already, this is just a defensive floor, same
            # convention pages/map_shell_concept.py's own
            # _DEFAULT_FORECAST_RUN resolution uses.
            run = (ts.hour // 6) * 6
            out.append((ts.strftime('%Y-%m-%d'), f"{run:02d}"))
        return out
    except Exception as e:
        logger.error("Error querying recent forecast dates: %s", e)
        return []