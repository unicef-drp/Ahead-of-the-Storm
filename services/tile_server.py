"""
tile_server.py: FastAPI PBF tile sidecar for Ahead of the Storm

Mercator tiles: BASE_MERCATOR_TILE_MAT has no GEOMETRY column.
  TILE_ID is a quadkey string. Geometry reconstructed via mercantile.
  PBF encoded via mapbox_vector_tile Python library.

Admin tiles: BASE_ADMIN_GEOM_MAT has a GEOGRAPHY GEOMETRY column.
  ST_INTERSECTS filter in SQL; geometry parsed from GeoJSON returned
  by Snowflake connector; clipped to tile bbox via shapely.

Performance: on first tile request for a (country, storm, forecast_date,
  wind_threshold) combo, ONE bulk Snowflake query loads ALL rows into an
  in-memory pandas DataFrame. Subsequent tiles filter by binary search
  over a precomputed sorted TILE_ID index (_build_sorted_tile_index,
  _filter_by_tile_prefix), thread-safe.

Start:
    uvicorn services.tile_server:app --host 0.0.0.0 --port 8001 --reload
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import gzip
import io
import json
import logging
import math
import functools
import os
import tempfile
import threading
import time
from collections import OrderedDict
from typing import Callable, Optional

# Tile data and rendered tiles expire after this many seconds so new pipeline
# output is served without a container restart (matches snowflake_utils TTL).
_TILE_TTL = 15 * 60  # 15 minutes

# _DataCache size caps (see its own _evict_oldest_if_over), generous enough
# to hold several countries/storms/dates at once without ever growing
# unbounded across a long-running session. Facility entries (points, not a
# full tile grid) are cheaper per-key than mercator/admin, hence the higher
# cap.
#
# This is ONE global LRU pool shared across every (country, storm,
# forecast_date) + hazard-variant key, regardless of hazard. River's variant
# space is up to 24 (6 rp_tier x 4 window_h values, see _hazard_variant's
# river branch). A single country's worth of River exploration can fill a
# large share of the cache, evicting other hazards' or other countries'
# warm entries. Facility's key additionally splits by layer_type (schools/
# health/shelters/wash), so River's footprint there is up to 4x24=96
# entries. Caps are sized to comfortably hold River's full variant space
# for at least one country plus headroom for a few more (still bounded, not
# unbounded: each entry is a DataFrame, a few MB at most for a single
# country's tile grid, so this remains a modest, deliberate memory budget,
# not a leak).
_MERCATOR_CACHE_MAX = 64
_ADMIN_CACHE_MAX = 64
_FACILITY_CACHE_MAX = 128


def _ttl_cache(ttl_seconds: int, maxsize: int = 128):
    """LRU cache with a sliding PER-ENTRY TTL, thread-safe, single-flight.

    Each entry expires ttl_seconds after it was individually cached (not on
    a shared time.time() // ttl_seconds bucket), so misses spread out over
    time instead of every entry in the cache expiring at the same instant
    and causing a stampede under concurrent traffic.

    Single-flight: a `pending` dict tracks an in-flight Future per key; the
    first caller for a cold key computes it and resolves the Future for
    every other caller waiting on that same key, so N concurrent requests
    for the same cold key share one computation instead of each redoing the
    full (often Snowflake-backed) work. Callers for a DIFFERENT key are
    never blocked by it. The actual computation runs outside the lock.
    """
    def decorator(func):
        cache: "OrderedDict[tuple, tuple[float, object]]" = OrderedDict()
        pending: dict[tuple, concurrent.futures.Future] = {}
        lock = threading.Lock()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.monotonic()
            with lock:
                entry = cache.get(key)
                if entry is not None and entry[0] > now:
                    cache.move_to_end(key)
                    return entry[1]
                fut = pending.get(key)
                is_owner = fut is None
                if is_owner:
                    fut = concurrent.futures.Future()
                    pending[key] = fut
            if not is_owner:
                # Someone else is already computing this exact key, wait
                # for their result instead of redoing the same slow work.
                return fut.result()
            # Computed outside the lock: a slow Snowflake-backed miss on one
            # key must not block lookups/hits for every other key.
            try:
                value = func(*args, **kwargs)
            except BaseException as exc:
                with lock:
                    pending.pop(key, None)
                fut.set_exception(exc)
                raise
            with lock:
                cache[key] = (now + ttl_seconds, value)
                cache.move_to_end(key)
                while len(cache) > maxsize:
                    cache.popitem(last=False)
                pending.pop(key, None)
            fut.set_result(value)
            return value

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

from PIL import Image

import mapbox_vector_tile
import numpy as np
import mercantile
import pandas as pd
import pyarrow.parquet as pq
import snowflake.connector
import zarr
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from shapely.geometry import box, shape, Point
from shapely.ops import transform as _shapely_transform

load_dotenv()

# This module always runs as its own dedicated uvicorn process (see
# entrypoint.sh, never imported into the Dash/gunicorn process), so
# configuring the root logger here is safe and process-local. Without this,
# log.info(...) calls throughout this file (pre-existing and the pre-warm
# logging below) are silently dropped: uvicorn's own --log-level only
# configures its own "uvicorn"/"uvicorn.error" loggers, not the root logger,
# so the default root level (WARNING) filters INFO records before they ever
# reach a handler. basicConfig() is a no-op if the root logger already has
# handlers, so this cannot clobber any other configuration.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

log = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "AOTS")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "TC_ECMWF")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "AOTS_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "")

# SPCS OAuth: when running inside Snowflake Container Services the connector
# reads a short-lived OAuth token from a file mounted by the SPCS runtime.
# Locally, fall back to USER + PASSWORD env vars.
SPCS_RUN        = os.getenv("SPCS_RUN", "false").lower() == "true"
SPCS_TOKEN_PATH = os.getenv("SPCS_TOKEN_PATH", "/snowflake/session/token")
SNOWFLAKE_HOST  = os.getenv("SNOWFLAKE_HOST", "")
SNOWFLAKE_PORT  = int(os.getenv("SNOWFLAKE_PORT") or "443")

# Data source mode: controls where tile/admin/facility data is loaded from.
# SNOWFLAKE (default): all data from Snowflake MAT tables (SPCS production).
# LOCAL: read parquet/CSV files from ROOT_DATA_DIR/VIEWS_DIR on local disk.
# BLOB:  read parquet/CSV files from Azure Data Lake Storage (ADLS).
IMPACT_DATA_STORE   = os.getenv("IMPACT_DATA_STORE", "SNOWFLAKE").upper()
ROOT_DATA_DIR       = os.getenv("ROOT_DATA_DIR", "geodb")
VIEWS_DIR_NAME      = os.getenv("VIEWS_DIR", "aos_views")
ADLS_ACCOUNT_URL    = os.getenv("ADLS_ACCOUNT_URL", "")
ADLS_SAS_TOKEN      = os.getenv("ADLS_SAS_TOKEN", "")
ADLS_CONTAINER_NAME = os.getenv("ADLS_CONTAINER_NAME", "")

if not SPCS_RUN:
    if IMPACT_DATA_STORE == "SNOWFLAKE":
        SNOWFLAKE_USER     = os.environ["SNOWFLAKE_USER"]
        SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        # Snowflake credentials optional in LOCAL/BLOB mode: stats fall back to DataFrame.
        SNOWFLAKE_USER     = os.getenv("SNOWFLAKE_USER", "")
        SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")

MAT_ZOOM_LEVEL: int = 14

# Thread-local connections: one persistent Snowflake connection per FastAPI
# worker thread, mirroring components/data/snowflake_utils.py's own pattern.
# Each thread owns its own connection, so concurrent requests (several
# countries/users hitting an empty cache at once) execute their queries in
# parallel with no shared-cursor contention.
_thread_local = threading.local()
_CONN_HEALTH_CHECK_INTERVAL = 300  # seconds (matches snowflake_utils.py)


def _connect() -> snowflake.connector.SnowflakeConnection:
    log.info("Opening new Snowflake connection (SPCS=%s)…", SPCS_RUN)
    if SPCS_RUN:
        with open(SPCS_TOKEN_PATH, "r") as f:
            token = f.read().strip()
        kwargs: dict = dict(
            host=SNOWFLAKE_HOST,
            port=SNOWFLAKE_PORT,
            protocol="https",
            account=SNOWFLAKE_ACCOUNT,
            authenticator="oauth",
            token=token,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
        )
    else:
        kwargs = dict(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
        )
    if SNOWFLAKE_ROLE:
        kwargs["role"] = SNOWFLAKE_ROLE
    conn = snowflake.connector.connect(**kwargs)
    # The Snowflake connector sometimes ignores the warehouse param in the
    # connection string (confirmed under both SPCS OAuth and plain PAT/
    # password auth; see components/data/snowflake_utils.py's own
    # get_snowflake_connection() for the same fix). Explicitly set it so
    # every new thread session actually has an active warehouse.
    if SNOWFLAKE_WAREHOUSE:
        cur = conn.cursor()
        try:
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        except Exception as exc:
            log.warning("USE WAREHOUSE %s failed: %s", SNOWFLAKE_WAREHOUSE, exc)
        finally:
            cur.close()
    return conn


def get_connection() -> snowflake.connector.SnowflakeConnection:
    conn = getattr(_thread_local, "connection", None)
    if conn is not None:
        last_check = getattr(_thread_local, "last_health_check", 0.0)
        if time.monotonic() - last_check < _CONN_HEALTH_CHECK_INTERVAL:
            return conn
        try:
            if not conn.is_closed():
                _thread_local.last_health_check = time.monotonic()
                return conn
        except Exception as exc:
            log.debug("Stale connection check failed, reconnecting: %s", exc)
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.connection = None
    conn = _connect()
    _thread_local.connection = conn
    _thread_local.last_health_check = time.monotonic()
    return conn


def _run_query(sql: str, params: list) -> list[dict]:
    for attempt in range(2):
        conn = get_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                cols = [d[0].upper() for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            finally:
                cur.close()
        except snowflake.connector.errors.ProgrammingError as exc:
            if exc.errno == 390114 and attempt == 0:
                log.info("SPCS token expired, reconnecting with fresh token…")
                try:
                    conn.close()
                except Exception:
                    pass
                _thread_local.connection = None
                continue
            raise


def _run_query_df(sql: str, params: list) -> pd.DataFrame:
    """Same retry/reconnect behaviour as _run_query, but returns a DataFrame
    directly via fetch_pandas_all() (Arrow, columnar) instead of first
    materializing a Python list of per-row dicts and letting pandas rebuild
    a DataFrame from those, a real double conversion cost for the large
    bulk base-table loads (tens to hundreds of thousands of rows) this file
    issues (microbenched at ~0.94s CPU for 300k rows via the dict path).
    Reserved for those bulk loads; small facility/metadata queries keep
    using _run_query's dict path (a DataFrame is more overhead than benefit
    at that row count).
    """
    for attempt in range(2):
        conn = get_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                df = cur.fetch_pandas_all()
                df.columns = [c.upper() for c in df.columns]
                return df
            finally:
                cur.close()
        except snowflake.connector.errors.ProgrammingError as exc:
            if exc.errno == 390114 and attempt == 0:
                log.info("SPCS token expired, reconnecting with fresh token…")
                try:
                    conn.close()
                except Exception:
                    pass
                _thread_local.connection = None
                continue
            raise


def _country_in_clause(country: str) -> tuple[str, list[str]]:
    """Split a '+'-joined multi-country string into (SQL clause, [codes]).

    Single country  → "= %s",          ["PHL"]
    Region/multi    → "IN (%s, %s, …)", ["AIA", "ATG", …]

    The SQL clause is injected into the WHERE b.COUNTRY ... position; the codes
    are bound as parameterized values so there is no injection risk.
    """
    codes = [c.upper() for c in country.split('+') if c.strip()]
    if len(codes) == 1:
        return "= %s", codes
    ph = ', '.join(['%s'] * len(codes))
    return f"IN ({ph})", codes


def _make_transparent_tile() -> bytes:
    # 512×512 matches the size of data tiles: a 1×1 image may cause MapLibre
    # to treat the tile as malformed and still fall back to a parent tile.
    buf = io.BytesIO()
    Image.new('RGBA', (512, 512), (0, 0, 0, 0)).save(buf, 'WEBP', lossless=True)
    return buf.getvalue()

_TRANSPARENT_WEBP: bytes = _make_transparent_tile()


def _tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    b = mercantile.bounds(x, y, z)
    return b.west, b.south, b.east, b.north


_MERC_R = 6378137.0

def _to_merc(geom):
    """Project a shapely geometry from WGS84 (lon/lat degrees) to Web Mercator (metres)."""
    def _proj(lons, lats, zs=None):
        xs = np.array(lons, dtype=np.float64) * (math.pi / 180.0 * _MERC_R)
        ys = np.log(np.tan(np.pi / 4.0 + np.radians(np.array(lats, dtype=np.float64)) / 2.0)) * _MERC_R
        return (xs.tolist(), ys.tolist()) if zs is None else (xs.tolist(), ys.tolist(), list(zs))
    return _shapely_transform(_proj, geom)


def _quadkey_like_pattern(z: int, x: int, y: int) -> str:
    """LIKE pattern that selects all MAT_ZOOM_LEVEL=14 tiles within (z, x, y)."""
    qk = mercantile.quadkey(x, y, z)
    if z < MAT_ZOOM_LEVEL:
        return qk + "%"
    if z == MAT_ZOOM_LEVEL:
        return qk          # exact match via LIKE (no wildcard)
    return qk[:MAT_ZOOM_LEVEL]  # ancestor quadkey, exact match


# (country, zoom_level) -> (base_df_ref, lats, lons). A real bounded LRU
# (OrderedDict, not a plain dict): get_base_tiles() itself evicts by COUNT
# once more than maxsize=32 distinct (country, zoom_level) keys have EVER
# been queried in the process's lifetime, but that eviction is invisible to
# a separate, uncoupled plain dict here, i.e. a bound on one cache's
# concurrently-held entries does not bound another cache's cumulative
# distinct-key count over the life of a long-running SPCS process. Without
# its own real eviction, this dict would grow forever as new countries get
# onboarded (already 29 of the 32 as of this writing), each entry holding a
# strong reference to a full base-tiles GeoDataFrame (hundreds of MB for a
# large country like Mexico) that get_base_tiles() itself has long since
# forgotten. Capped at the SAME maxsize as get_base_tiles() for consistency.
_BASE_TILE_CENTROID_CACHE_MAX = 32
_BASE_TILE_CENTROID_CACHE: "OrderedDict[tuple, tuple]" = OrderedDict()
# Guards ONLY the dict bookkeeping (read/write/move_to_end/len-check/evict)
# below, never the expensive centroid computation itself (~237ms for a
# large country): rain_member_impacts/combined_member_impacts are
# synchronous FastAPI route handlers, dispatched to Starlette's own worker
# thread pool, so two DIFFERENT countries' cache misses genuinely run
# concurrently. Without this lock, two threads racing the read-then-write-
# then-len-check-then-evict sequence (e.g. one thread inserting a genuinely
# new key while another rewrites an already-cached key whose get_base_tiles
# entry expired) can both observe the SAME transient, larger len() before
# either evicts, causing two evictions for one net insertion and the cache
# trending below its configured cap under concurrent load, reproduced live
# during a review pass via a synchronized two-thread race. Held only for
# the few dict operations, not the numpy/shapely work, so this cannot
# serialize the actually expensive part across unrelated countries.
_BASE_TILE_CENTROID_CACHE_LOCK = threading.Lock()


def _get_base_tile_centroids(country: str, zoom_level: int, base) -> tuple[np.ndarray, np.ndarray]:
    """(lats, lons) numpy arrays of every row's centroid in `base`
    (get_base_tiles()'s own return value), computed once and reused across
    calls for the SAME underlying base-tiles DataFrame instead of
    recomputing shapely centroids from scratch on every request. Measured
    at 237ms for MEX's own 402k z14 base tiles, real, repeated cost for
    every rain_member_impacts/combined_member_impacts call otherwise, even
    though get_base_tiles() itself is already @ttl_cache'd and returns the
    identical DataFrame object until its own TTL expires.

    Keyed by (country, zoom_level), the same key shape get_base_tiles()
    uses, and the cache entry stores a strong reference to the exact
    `base` object the cached lats/lons were computed from (not just its
    id(), which Python can reuse for an unrelated object once the
    original is garbage collected): a stale entry is detected via
    `base_df_ref is base` failing, safe against get_base_tiles()'s own TTL
    expiry swapping in a fresh DataFrame for the same country."""
    key = (country, zoom_level)
    with _BASE_TILE_CENTROID_CACHE_LOCK:
        cached = _BASE_TILE_CENTROID_CACHE.get(key)
        if cached is not None and cached[0] is base:
            _BASE_TILE_CENTROID_CACHE.move_to_end(key)
            return cached[1], cached[2]
    # Outside the lock: the real, expensive work, so a concurrent cache
    # miss for a DIFFERENT country isn't serialized behind this one.
    lats = base.geometry.centroid.y.to_numpy(dtype=np.float64)
    lons = base.geometry.centroid.x.to_numpy(dtype=np.float64)
    with _BASE_TILE_CENTROID_CACHE_LOCK:
        # Another thread may have already written this exact key while we
        # were computing (e.g. two concurrent requests for the same
        # brand-new country); this is redundant work, not a correctness
        # issue, harmless to just overwrite with our own equally-valid
        # result. is_new_key gates the eviction check below on whether THIS
        # write actually grew the dict, so a stale-rewrite (same key,
        # already present) can never trigger a spurious eviction, and the
        # whole read-write-len-check-evict sequence is now atomic under one
        # lock acquisition, eliminating the cross-thread race entirely
        # (not just the single-key case get_base_tiles' own TTL-swap
        # scenario would hit).
        is_new_key = key not in _BASE_TILE_CENTROID_CACHE
        _BASE_TILE_CENTROID_CACHE[key] = (base, lats, lons)
        _BASE_TILE_CENTROID_CACHE.move_to_end(key)
        if is_new_key and len(_BASE_TILE_CENTROID_CACHE) > _BASE_TILE_CENTROID_CACHE_MAX:
            _BASE_TILE_CENTROID_CACHE.popitem(last=False)
        return lats, lons


def _build_sorted_tile_index(tile_ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """(sorted_ids, sorted_positions): `sorted_ids` is `tile_ids` sorted
    lexicographically, `sorted_positions` is the original row index each
    sorted entry came from (so `sorted_positions[i]` is a valid `.iloc[]`
    position into the DataFrame `tile_ids` was taken from). Computed once
    per DataFrame and reused for every subsequent prefix lookup against
    it, turning an O(n) linear scan per request into an O(log n) binary
    search plus an O(k) slice of the k matching rows.
    """
    positions = np.argsort(tile_ids, kind="quicksort")
    return tile_ids[positions], positions


def _tile_prefix_positions(sorted_ids: np.ndarray, sorted_positions: np.ndarray, prefix: str) -> np.ndarray:
    """Original-DataFrame row positions whose TILE_ID starts with `prefix`,
    found via binary search over a precomputed sorted view (see
    `_build_sorted_tile_index`) instead of a linear scan.

    Every real TILE_ID here is a fixed-length z14 quadkey over the digit
    alphabet '0'-'3' (see `_quadkey_like_pattern`), so `prefix + '4'` is a
    safe exclusive upper bound: '4' sorts after every digit a real
    quadkey can contain, so no real TILE_ID can equal or exceed it while
    still starting with `prefix`. An exact-match query (`prefix` already
    the full 14-character quadkey) is just the degenerate case of this
    same range and needs no special-casing.
    """
    lo = np.searchsorted(sorted_ids, prefix, side="left")
    hi = np.searchsorted(sorted_ids, prefix + "4", side="left")
    return sorted_positions[lo:hi]


def _filter_by_tile_prefix(df: pd.DataFrame, like_pat: str,
                            sorted_index: Optional[tuple[np.ndarray, np.ndarray]] = None) -> pd.DataFrame:
    """Real replacement for the repeated
    `df['TILE_ID'].str.startswith(prefix, na=False)` / `df['TILE_ID'] ==
    like_pat` scan pattern used across the raster/mercator tile paths.

    When `sorted_index` (from `_build_sorted_tile_index`, precomputed once
    per cached DataFrame) is available and still matches `df`'s current
    length, uses the O(log n) binary-search path. Falls back to the
    original O(n) scan for any DataFrame that never went through that
    precomputation (e.g. a hazard-bitmask frame that isn't one of
    `_DataCache`'s own cached mercator DataFrames). Same real correctness
    guarantee either way, only the algorithmic cost differs.
    """
    prefix = like_pat[:-1] if like_pat.endswith("%") else like_pat
    if sorted_index is not None:
        sorted_ids, sorted_positions = sorted_index
        if len(sorted_ids) == len(df):
            positions = _tile_prefix_positions(sorted_ids, sorted_positions, prefix)
            return df.iloc[positions]
    if like_pat.endswith("%"):
        mask = df["TILE_ID"].str.startswith(prefix, na=False)
    else:
        mask = df["TILE_ID"] == like_pat
    return df[mask]


def _tile_prefix_mask(df: pd.DataFrame, like_pat: str,
                       sorted_index: Optional[tuple[np.ndarray, np.ndarray]] = None) -> pd.Series:
    """Boolean-mask sibling of `_filter_by_tile_prefix`, for call sites that
    need `df.loc[mask, cols]` rather than an already-sliced frame (e.g.
    combining several hazards' own DataFrames tile-by-tile). Same real
    fast/fallback contract as `_filter_by_tile_prefix`.
    """
    prefix = like_pat[:-1] if like_pat.endswith("%") else like_pat
    if sorted_index is not None:
        sorted_ids, sorted_positions = sorted_index
        if len(sorted_ids) == len(df):
            positions = _tile_prefix_positions(sorted_ids, sorted_positions, prefix)
            mask = np.zeros(len(df), dtype=bool)
            mask[positions] = True
            return pd.Series(mask, index=df.index)
    if like_pat.endswith("%"):
        return df["TILE_ID"].str.startswith(prefix, na=False)
    return df["TILE_ID"] == like_pat


# ---------------------------------------------------------------------------
# LOCAL / BLOB file-reading helpers
# ---------------------------------------------------------------------------

def _read_file_local(rel_path: str) -> Optional[pd.DataFrame]:
    full = os.path.join(ROOT_DATA_DIR, VIEWS_DIR_NAME, rel_path)
    if not os.path.exists(full):
        log.debug("LOCAL: not found: %s", full)
        return None
    try:
        df = pd.read_parquet(full) if rel_path.endswith(".parquet") else pd.read_csv(full)
        return df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    except Exception as exc:
        log.warning("LOCAL: read failed %s: %s", full, exc)
        return None


def _read_file_blob(rel_path: str) -> Optional[pd.DataFrame]:
    try:
        from azure.storage.blob import BlobServiceClient
        import io as _io
        client = BlobServiceClient(account_url=ADLS_ACCOUNT_URL, credential=ADLS_SAS_TOKEN)
        blob_path = f"{ROOT_DATA_DIR}/{VIEWS_DIR_NAME}/{rel_path}"
        data = client.get_blob_client(ADLS_CONTAINER_NAME, blob_path).download_blob().readall()
        buf = _io.BytesIO(data)
        df = pd.read_parquet(buf) if rel_path.endswith(".parquet") else pd.read_csv(buf)
        return df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    except Exception as exc:
        log.debug("BLOB: read failed %s: %s", rel_path, exc)
        return None


def _read_file(rel_path: str) -> Optional[pd.DataFrame]:
    if IMPACT_DATA_STORE == "LOCAL":
        return _read_file_local(rel_path)
    if IMPACT_DATA_STORE == "BLOB":
        return _read_file_blob(rel_path)
    return None


def _wkb_to_geojson(geom_val) -> Optional[str]:
    """Convert WKB (bytes or hex string) to a GeoJSON geometry string."""
    if geom_val is None:
        return None
    if isinstance(geom_val, float):
        return None  # NaN from pandas
    try:
        from shapely import wkb as _wkb
        g = _wkb.loads(geom_val if isinstance(geom_val, bytes) else bytes.fromhex(str(geom_val)))
        return json.dumps(g.__geo_interface__)
    except Exception:
        return None


def _wkb_centroid(geom_val) -> tuple[Optional[float], Optional[float]]:
    """Return (lat, lon) of the centroid of a WKB geometry."""
    if geom_val is None:
        return None, None
    if isinstance(geom_val, float):
        return None, None  # NaN from pandas
    try:
        from shapely import wkb as _wkb
        g = _wkb.loads(geom_val if isinstance(geom_val, bytes) else bytes.fromhex(str(geom_val)))
        c = g.centroid
        return c.y, c.x
    except Exception:
        return None, None


def _norm_cols(df: pd.DataFrame, zone_id_to_tile_id: bool = True) -> pd.DataFrame:
    """Uppercase all column names; optionally rename ZONE_ID → TILE_ID; cast TILE_ID to str."""
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    if zone_id_to_tile_id and "TILE_ID" not in df.columns and "ZONE_ID" in df.columns:
        df = df.rename(columns={"ZONE_ID": "TILE_ID"})
    if "TILE_ID" in df.columns:
        df["TILE_ID"] = df["TILE_ID"].astype(str)
    return df


def _merge_no_collision(base: pd.DataFrame, right: pd.DataFrame, on: str) -> pd.DataFrame:
    """Merge right onto base, dropping any right columns that already exist in base (except join key)."""
    drop_cols = [c for c in right.columns if c != on and c in base.columns]
    if drop_cols:
        right = right.drop(columns=drop_cols)
    return base.merge(right, on=on, how="left")


def _add_children_total(df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
    cols = [f"{prefix}INFANT_POPULATION", f"{prefix}SCHOOL_AGE_POPULATION", f"{prefix}ADOLESCENT_POPULATION"]
    for c in cols:
        if c not in df.columns:
            df[c] = 0.0
    df[f"{prefix}CHILDREN_TOTAL"] = sum(
        pd.to_numeric(df[c], errors="coerce").fillna(0) for c in cols
    )
    return df


def _load_mercator_from_files(code: str, storm: str, forecast_date: str, wt: int) -> list[dict]:
    base = _read_file(f"mercator_views/{code}_{MAT_ZOOM_LEVEL}.parquet")
    if base is None or base.empty:
        log.warning("FILE: no base mercator for %s", code)
        return []
    base = _norm_cols(base)
    base = _add_children_total(base)

    impact = _read_file(f"mercator_views/{code}_{storm}_{forecast_date}_{wt}_{MAT_ZOOM_LEVEL}.csv")
    if impact is not None and not impact.empty:
        impact = _norm_cols(impact)
        impact = _add_children_total(impact, prefix="E_")
        base = _merge_no_collision(base, impact, on="TILE_ID")

    # CCI and vulnerability files have NO wind threshold in their filename: they aggregate all thresholds.
    vuln = _read_file(f"mercator_views/{code}_{storm}_{forecast_date}_{MAT_ZOOM_LEVEL}_vulnerability.csv")
    if vuln is not None and not vuln.empty:
        vuln = _norm_cols(vuln)
        keep = [c for c in ["TILE_ID", "E_PEOPLE_IN_NEED", "E_CHILDREN_IN_NEED", "E_INFANT_IN_NEED", "E_SCHOOL_AGE_IN_NEED", "E_ADOLESCENT_IN_NEED"] if c in vuln.columns]
        if len(keep) > 1:
            base = _merge_no_collision(base, vuln[keep], on="TILE_ID")

    cci = _read_file(f"mercator_views/{code}_{storm}_{forecast_date}_{MAT_ZOOM_LEVEL}_cci.csv")
    if cci is not None and not cci.empty:
        cci = _norm_cols(cci)
        keep = [c for c in ["TILE_ID", "CCI_CHILDREN", "E_CCI_CHILDREN"] if c in cci.columns]
        if len(keep) > 1:
            base = _merge_no_collision(base, cci[keep], on="TILE_ID")

    return base.to_dict("records")


def _load_admin_from_files(code: str, storm: str, forecast_date: str, wt: int, admin_level: int) -> list[dict]:
    base = _read_file(f"admin_views/{code}_admin{admin_level}.parquet")
    if base is None or base.empty:
        log.warning("FILE: no base admin for %s L%s", code, admin_level)
        return []
    base = _norm_cols(base)

    # Convert WKB geometry → GeoJSON string (matches SQL ST_ASGEOJSON output)
    for geom_col in ("GEOMETRY",):
        if geom_col in base.columns:
            base["GEOJSON"] = base[geom_col].apply(_wkb_to_geojson)
            base = base.drop(columns=[geom_col])
            break

    base = _add_children_total(base)

    impact = _read_file(f"admin_views/{code}_{storm}_{forecast_date}_{wt}_admin{admin_level}.csv")
    if impact is not None and not impact.empty:
        impact = _norm_cols(impact)
        impact = _add_children_total(impact, prefix="E_")
        base = _merge_no_collision(base, impact, on="TILE_ID")

    # CCI and vulnerability files have NO wind threshold: they aggregate all thresholds.
    vuln = _read_file(f"admin_views/{code}_{storm}_{forecast_date}_admin{admin_level}_vulnerability.csv")
    if vuln is not None and not vuln.empty:
        vuln = _norm_cols(vuln)
        keep = [c for c in ["TILE_ID", "E_PEOPLE_IN_NEED", "E_CHILDREN_IN_NEED", "E_INFANT_IN_NEED", "E_SCHOOL_AGE_IN_NEED", "E_ADOLESCENT_IN_NEED"] if c in vuln.columns]
        if len(keep) > 1:
            base = _merge_no_collision(base, vuln[keep], on="TILE_ID")

    cci = _read_file(f"admin_views/{code}_{storm}_{forecast_date}_admin{admin_level}_cci.csv")
    if cci is not None and not cci.empty:
        cci = _norm_cols(cci)
        keep = [c for c in ["TILE_ID", "CCI_CHILDREN", "E_CCI_CHILDREN"] if c in cci.columns]
        if len(keep) > 1:
            base = _merge_no_collision(base, cci[keep], on="TILE_ID")

    return base.to_dict("records")


_FACILITY_SUBDIR = {
    "schools":  "school_views",
    "health":   "hc_views",
    "shelters": "shelter_views",
    "wash":     "wash_views",
}
_FACILITY_BASE_FILENAME = {
    "schools":  "schools",
    "health":   "health_centers",   # actual filename: {country}_health_centers.parquet
    "shelters": "shelters",
    "wash":     "wash",
}


def _attach_latlon_from_geometry(df: pd.DataFrame) -> pd.DataFrame:
    """If LATITUDE/LONGITUDE missing (or all-null) but GEOMETRY present, extract from WKB centroid."""
    has_latlon = (
        "LATITUDE" in df.columns and "LONGITUDE" in df.columns
        and df["LATITUDE"].notna().any() and df["LONGITUDE"].notna().any()
    )
    if has_latlon or "GEOMETRY" not in df.columns:
        return df
    coords = [_wkb_centroid(v) for v in df["GEOMETRY"]]
    df = df.copy()
    df["LATITUDE"]  = [c[0] for c in coords]
    df["LONGITUDE"] = [c[1] for c in coords]
    df = df.drop(columns=["GEOMETRY"])
    return df


def _load_facility_from_files(layer_type: str, code: str, storm: str,
                              forecast_date: str, wt: int) -> list[dict]:
    subdir = _FACILITY_SUBDIR[layer_type]

    df = _read_file(f"{subdir}/{code}_{storm}_{forecast_date}_{wt}.parquet")
    if df is None or df.empty:
        base_name = _FACILITY_BASE_FILENAME[layer_type]
        df = _read_file(f"{subdir}/{code}_{base_name}.parquet")
        if df is None or df.empty:
            return []

    df = _norm_cols(df, zone_id_to_tile_id=False)
    if "PROBABILITY" not in df.columns:
        df["PROBABILITY"] = 0.0
    df = _attach_latlon_from_geometry(df)
    # Drop raw geometry bytes unconditionally: they're not JSON-serializable.
    if "GEOMETRY" in df.columns:
        df = df.drop(columns=["GEOMETRY"])

    # Normalize layer-specific column names to match SQL output
    if layer_type == "schools":
        if "SCHOOL_NAME" not in df.columns and "NAME" in df.columns:
            df = df.rename(columns={"NAME": "SCHOOL_NAME"})
    elif layer_type == "health":
        if "FACILITY_TYPE" not in df.columns:
            for alt in ("HEALTH_AMENITY_TYPE", "AMENITY", "HEALTHCARE", "TYPE"):
                if alt in df.columns:
                    df["FACILITY_TYPE"] = df[alt]
                    break

    df["PROBABILITY"] = pd.to_numeric(df["PROBABILITY"], errors="coerce").fillna(0.0)
    return df.to_dict("records")


# ---------------------------------------------------------------------------
# Stats helper: compute min/max from a cached DataFrame (LOCAL/BLOB mode)
# ---------------------------------------------------------------------------

_STATS_COL_MAP = [
    ("population",              "POPULATION",              False),
    ("children_total",          "CHILDREN_TOTAL",          False),
    ("infant_population",       "INFANT_POPULATION",       False),
    ("school_age_population",   "SCHOOL_AGE_POPULATION",   False),
    ("adolescent_population",   "ADOLESCENT_POPULATION",   False),
    ("built_surface_m2",        "BUILT_SURFACE_M2",        False),
    ("moderate_poverty_prob",   "MODERATE_POVERTY_PROB",   True),
    ("severe_poverty_prob",     "SEVERE_POVERTY_PROB",     True),
    ("rwi",                     "RWI",                     False),
    ("probability",             "PROBABILITY",             True),
    ("E_population",            "E_POPULATION",            True),
    ("E_children_total",        "E_CHILDREN_TOTAL",        True),
    ("E_infant_population",     "E_INFANT_POPULATION",     True),
    ("E_school_age_population", "E_SCHOOL_AGE_POPULATION", True),
    ("E_adolescent_population", "E_ADOLESCENT_POPULATION", True),
    ("E_built_surface_m2",      "E_BUILT_SURFACE_M2",      True),
    ("E_num_schools",           "E_NUM_SCHOOLS",           True),
    ("E_num_hcs",               "E_NUM_HCS",               True),
    ("E_num_shelters",          "E_NUM_SHELTERS",           True),
    ("E_num_wash",              "E_NUM_WASH",              True),
    ("E_people_in_need",        "E_PEOPLE_IN_NEED",        True),
    ("E_children_in_need",      "E_CHILDREN_IN_NEED",      True),
    ("E_infant_in_need",        "E_INFANT_IN_NEED",        True),
    ("E_school_age_in_need",    "E_SCHOOL_AGE_IN_NEED",    True),
    ("E_adolescent_in_need",    "E_ADOLESCENT_IN_NEED",    True),
    ("cci_children",            "CCI_CHILDREN",            True),
    ("E_cci_children",          "E_CCI_CHILDREN",          True),
]


def _py(v):
    """Convert numpy scalar to Python native for JSON serialization."""
    return v.item() if hasattr(v, "item") else v


def _safe_prop(v):
    """Return a JSON-safe scalar; None for NaN, None, or non-scalar types (e.g. arrays)."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (ValueError, TypeError):
        return None  # array/complex types raise on pd.isna()
    return _py(v)


def _stats_from_df(df: pd.DataFrame) -> dict:
    """Compute the same stats dict as the SQL stats queries, from a cached DataFrame."""
    result = {}
    for prop, col, positive_only in _STATS_COL_MAP:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if positive_only:
            s = s[s > 0]
        s = s.dropna()
        if s.empty:
            continue
        result[prop] = {"min": float(s.min()), "max": float(s.max())}
    return result


# ---------------------------------------------------------------------------
# Mercator tiles: reconstruct geometry from quadkey TILE_ID
# ---------------------------------------------------------------------------
#
# Base / impact split: the 15 base columns below are byte-identical across
# every threshold/hazard for a given country, only the small impact/
# vulnerability/CCI columns actually vary per (storm, forecast_date,
# threshold). _MERCATOR_BASE_SQL is queried once per (country, zoom_level)
# and cached with a long TTL (see _ensure_mercator_base_one); every
# hazard/threshold variant below queries ONLY its own small ZONE_ID-keyed
# impact columns and merges them onto the cached base DataFrame in pandas
# (_merge_no_collision) instead of re-running a whole-country 3-way LEFT
# JOIN per threshold. Bonus: the base query's bind params are identical
# across every threshold, so Snowflake's own 24h result cache can serve
# repeat base loads even across container restarts.

_MERCATOR_BASE_SQL = """
SELECT
    b.TILE_ID,
    b.ADMIN_ID,
    b.POPULATION,
    b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION AS CHILDREN_TOTAL,
    b.INFANT_POPULATION,
    b.SCHOOL_AGE_POPULATION,
    b.ADOLESCENT_POPULATION,
    b.BUILT_SURFACE_M2,
    b.SMOD_CLASS,
    b.RWI,
    b.MODERATE_POVERTY_PROB,
    b.SEVERE_POVERTY_PROB,
    b.NUM_SCHOOLS,
    b.NUM_HCS,
    b.NUM_SHELTERS,
    b.NUM_WASH
FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
WHERE b.COUNTRY    = %s
  AND b.ZOOM_LEVEL = %s
"""

# Wind impact-only siblings, MERCATOR_TILE_VULNERABILITY_MAT/
# MERCATOR_TILE_CCI_MAT are keyed by STORM+FORECAST_DATE only (no
# WIND_THRESHOLD), queried separately from the WIND_THRESHOLD-keyed impact
# table so their result is naturally reusable across every wind threshold of
# the same storm/date, not just the base columns.
_MERCATOR_IMPACT_ONLY_SQL = """
SELECT
    i.ZONE_ID AS TILE_ID,
    i.PROBABILITY,
    i.E_POPULATION,
    i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION AS E_CHILDREN_TOTAL,
    i.E_INFANT_POPULATION,
    i.E_SCHOOL_AGE_POPULATION,
    i.E_ADOLESCENT_POPULATION,
    i.E_BUILT_SURFACE_M2,
    i.E_NUM_SCHOOLS,
    i.E_NUM_HCS,
    i.E_NUM_SHELTERS,
    i.E_NUM_WASH
FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT i
WHERE i.COUNTRY        = %s
  AND i.ZOOM_LEVEL     = %s
  AND i.STORM          = %s
  AND i.FORECAST_DATE  = %s
  AND i.WIND_THRESHOLD = %s
"""

_MERCATOR_VULN_ONLY_SQL = """
SELECT
    v.ZONE_ID AS TILE_ID,
    v.E_PEOPLE_IN_NEED,
    v.E_CHILDREN_IN_NEED,
    v.E_INFANT_IN_NEED,
    v.E_SCHOOL_AGE_IN_NEED,
    v.E_ADOLESCENT_IN_NEED
FROM AOTS.TC_ECMWF.MERCATOR_TILE_VULNERABILITY_MAT v
WHERE v.COUNTRY       = %s
  AND v.ZOOM_LEVEL    = %s
  AND v.STORM         = %s
  AND v.FORECAST_DATE = %s
"""

_MERCATOR_CCI_ONLY_SQL = """
SELECT
    c.ZONE_ID AS TILE_ID,
    c.CCI_CHILDREN,
    c.E_CCI_CHILDREN
FROM AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT c
WHERE c.COUNTRY       = %s
  AND c.ZOOM_LEVEL    = %s
  AND c.STORM         = %s
  AND c.FORECAST_DATE = %s
"""

# ---------------------------------------------------------------------------
# Admin tiles: GEOMETRY column exists; use shapely for clipping
# ---------------------------------------------------------------------------
#
# Same base/impact split as mercator above, plus the ST_ASGEOJSON polygon
# transfer + shapely shape() parse + STRtree build, all threshold-invariant,
# cached once per (country, admin_level) (see _ensure_admin_base_one)
# instead of repeated per threshold.

_ADMIN_BASE_SQL = """
SELECT
    b.TILE_ID,
    b.NAME,
    b.ADMIN_LEVEL,
    b.POPULATION,
    b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION AS CHILDREN_TOTAL,
    b.INFANT_POPULATION,
    b.SCHOOL_AGE_POPULATION,
    b.ADOLESCENT_POPULATION,
    b.BUILT_SURFACE_M2,
    b.SMOD_CLASS,
    b.RWI,
    b.MODERATE_POVERTY_PROB,
    b.SEVERE_POVERTY_PROB,
    b.NUM_SCHOOLS,
    b.NUM_HCS,
    b.NUM_SHELTERS,
    b.NUM_WASH,
    ST_ASGEOJSON(b.GEOMETRY) AS GEOJSON
FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
WHERE b.COUNTRY     = %s
  AND b.ADMIN_LEVEL = %s
"""

_ADMIN_IMPACT_ONLY_SQL = """
SELECT
    i.TILE_ID,
    i.PROBABILITY,
    i.E_POPULATION,
    i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION AS E_CHILDREN_TOTAL,
    i.E_INFANT_POPULATION,
    i.E_SCHOOL_AGE_POPULATION,
    i.E_ADOLESCENT_POPULATION,
    i.E_BUILT_SURFACE_M2,
    i.E_NUM_SCHOOLS,
    i.E_NUM_HCS,
    i.E_NUM_SHELTERS,
    i.E_NUM_WASH
FROM AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT i
WHERE i.COUNTRY        = %s
  AND i.ADMIN_LEVEL    = %s
  AND i.STORM          = %s
  AND i.FORECAST_DATE  = %s
  AND i.WIND_THRESHOLD = %s
"""

_ADMIN_VULN_ONLY_SQL = """
SELECT
    v.TILE_ID,
    v.E_PEOPLE_IN_NEED,
    v.E_CHILDREN_IN_NEED,
    v.E_INFANT_IN_NEED,
    v.E_SCHOOL_AGE_IN_NEED,
    v.E_ADOLESCENT_IN_NEED
FROM AOTS.TC_ECMWF.ADMIN_ALL_VULNERABILITY_MAT v
WHERE v.COUNTRY       = %s
  AND v.ADMIN_LEVEL   = %s
  AND v.STORM         = %s
  AND v.FORECAST_DATE = %s
"""

_ADMIN_CCI_ONLY_SQL = """
SELECT
    c.TILE_ID,
    c.CCI_CHILDREN,
    c.E_CCI_CHILDREN
FROM AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT c
WHERE c.COUNTRY       = %s
  AND c.ADMIN_LEVEL   = %s
  AND c.STORM         = %s
  AND c.FORECAST_DATE = %s
"""

# ---------------------------------------------------------------------------
# Gust sibling queries: MERCATOR_TILE_GUST_MAT/ADMIN_ALL_GUST_MAT, keyed by
# STORM + FORECAST_DATE + GUST_THRESHOLD (same shape as wind, just a
# different threshold column). Deliberately OMIT the
# MERCATOR_TILE_VULNERABILITY_MAT/MERCATOR_TILE_CCI_MAT joins present in the
# wind query above: both E_PEOPLE_IN_NEED/E_CHILDREN_IN_NEED and
# CCI_CHILDREN/E_CCI_CHILDREN are computed from WIND ensemble envelope data
# specifically (wind-speed-band-weighted), not hazard-agnostic values.
# Displaying them under a gust view would misrepresent wind-derived numbers
# as gust data. No CCI/vulnerability pipeline exists for gust at all.
# ---------------------------------------------------------------------------

_MERCATOR_GUST_IMPACT_ONLY_SQL = """
SELECT
    i.ZONE_ID AS TILE_ID,
    i.PROBABILITY,
    i.E_POPULATION,
    i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION AS E_CHILDREN_TOTAL,
    i.E_INFANT_POPULATION,
    i.E_SCHOOL_AGE_POPULATION,
    i.E_ADOLESCENT_POPULATION,
    i.E_BUILT_SURFACE_M2,
    i.E_NUM_SCHOOLS,
    i.E_NUM_HCS,
    i.E_NUM_SHELTERS,
    i.E_NUM_WASH
FROM AOTS.TC_ECMWF.MERCATOR_TILE_GUST_MAT i
WHERE i.COUNTRY        = %s
  AND i.ZOOM_LEVEL     = %s
  AND i.STORM          = %s
  AND i.FORECAST_DATE  = %s
  AND i.GUST_THRESHOLD = %s
"""

_ADMIN_GUST_IMPACT_ONLY_SQL = """
SELECT
    i.TILE_ID,
    i.PROBABILITY,
    i.E_POPULATION,
    i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION AS E_CHILDREN_TOTAL,
    i.E_INFANT_POPULATION,
    i.E_SCHOOL_AGE_POPULATION,
    i.E_ADOLESCENT_POPULATION,
    i.E_BUILT_SURFACE_M2,
    i.E_NUM_SCHOOLS,
    i.E_NUM_HCS,
    i.E_NUM_SHELTERS,
    i.E_NUM_WASH
FROM AOTS.TC_ECMWF.ADMIN_ALL_GUST_MAT i
WHERE i.COUNTRY        = %s
  AND i.ADMIN_LEVEL    = %s
  AND i.STORM          = %s
  AND i.FORECAST_DATE  = %s
  AND i.GUST_THRESHOLD = %s
"""

# ---------------------------------------------------------------------------
# River-flood sibling queries: MERCATOR_TILE_RIVER_MAT/ADMIN_ALL_RIVER_MAT.
# NOT storm-scoped at all: keyed by COUNTRY + FORECAST_TIME + RP_TIER (a
# return-period tier string 'rp2'/'rp5'/'rp10'/'rp20'/'rp50'/'rp100'; the
# STORM/FORECAST_DATE path segments are ignored for this hazard, see
# tile_server.py's own endpoint functions and map_shell_concept.py's config
# assembly for how the (unused) storm segment is filled with a placeholder).
#
# MERCATOR_TILE_RIVER_MAT and every *_RIVER_MAT facility table carry MULTIPLE
# STEP_H rows per (COUNTRY, FORECAST_TIME, RP_TIER, tile/facility), e.g. rp10
# has STEP_H in {24, 72, 120, 168} for the same tile, each a different real
# CUMULATIVE lead-time window within the same forecast run.
#
# Each river query below filters `WHERE ... AND STEP_H = %s` (the
# requested window, `window_h or _RIVER_WINDOW_DEFAULT`, see that
# constant's own comment for why 168 is a safe, exact backward-compat
# default) rather than aggregating across every STEP_H. STEP_H rows store
# an already-cumulative union of pixels through that window (not a single
# day), so filtering to one STEP_H yields the correct cumulative figure for
# that window directly. MAX(...)/GROUP BY are kept as a defensive no-op
# (each ZONE_ID should be unique per (COUNTRY, FORECAST_TIME, RP_TIER,
# STEP_H) already, same assumption wind/gust's own tile MAT shape makes)
# rather than a plain SELECT, in case of an unexpected future duplicate row.
# BOOLOR_AGG folds the two boolean flag columns (true if true in ANY step
# up through the selected window, matching the union semantics the rest of
# this query reflects).
#
# This table has no stored E_CHILDREN_TOTAL column, so it's computed here
# the same way wind's own _MERCATOR_IMPACT_ONLY_SQL does, as a summed
# expression, not selected raw. Uses MAX(E_INFANT_POPULATION +
# E_SCHOOL_AGE_POPULATION + E_ADOLESCENT_POPULATION) (the row-level sum's
# own peak across STEP_H) rather than MAX(E_INFANT)+MAX(E_SCHOOL_AGE)+
# MAX(E_ADOLESCENT) (summing three independently-peaking steps), matching
# the "peak at any single point in the forecast horizon" semantics every
# other column in this query uses, and matching how
# _RIVER_MERCATOR_STATS_SQL's own e_chi_min/e_chi_max compute this same
# expression for color-scale normalization.
# ---------------------------------------------------------------------------

_MERCATOR_RIVER_IMPACT_ONLY_SQL = """
SELECT
    ZONE_ID AS TILE_ID,
    MAX(PROBABILITY)              AS PROBABILITY,
    BOOLOR_AGG(BELOW_MIN_BASIN)   AS BELOW_MIN_BASIN,
    BOOLOR_AGG(IS_STANDIN)        AS IS_STANDIN,
    MAX(E_POPULATION)             AS E_POPULATION,
    MAX(E_INFANT_POPULATION)      AS E_INFANT_POPULATION,
    MAX(E_SCHOOL_AGE_POPULATION)  AS E_SCHOOL_AGE_POPULATION,
    MAX(E_ADOLESCENT_POPULATION)  AS E_ADOLESCENT_POPULATION,
    MAX(E_INFANT_POPULATION + E_SCHOOL_AGE_POPULATION + E_ADOLESCENT_POPULATION) AS E_CHILDREN_TOTAL,
    MAX(E_BUILT_SURFACE_M2)       AS E_BUILT_SURFACE_M2,
    MAX(E_NUM_SCHOOLS)            AS E_NUM_SCHOOLS,
    MAX(E_NUM_HCS)                AS E_NUM_HCS,
    MAX(E_NUM_SHELTERS)           AS E_NUM_SHELTERS,
    MAX(E_NUM_WASH)               AS E_NUM_WASH
FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT
WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
GROUP BY ZONE_ID
"""

_ADMIN_RIVER_IMPACT_ONLY_SQL = """
SELECT
    TILE_ID,
    MAX(PROBABILITY)              AS PROBABILITY,
    BOOLOR_AGG(BELOW_MIN_BASIN)   AS BELOW_MIN_BASIN,
    BOOLOR_AGG(IS_STANDIN)        AS IS_STANDIN,
    MAX(E_POPULATION)             AS E_POPULATION,
    MAX(E_INFANT_POPULATION)      AS E_INFANT_POPULATION,
    MAX(E_SCHOOL_AGE_POPULATION)  AS E_SCHOOL_AGE_POPULATION,
    MAX(E_ADOLESCENT_POPULATION)  AS E_ADOLESCENT_POPULATION,
    MAX(E_INFANT_POPULATION + E_SCHOOL_AGE_POPULATION + E_ADOLESCENT_POPULATION) AS E_CHILDREN_TOTAL,
    MAX(E_BUILT_SURFACE_M2)       AS E_BUILT_SURFACE_M2,
    MAX(E_NUM_SCHOOLS)            AS E_NUM_SCHOOLS,
    MAX(E_NUM_HCS)                AS E_NUM_HCS,
    MAX(E_NUM_SHELTERS)           AS E_NUM_SHELTERS,
    MAX(E_NUM_WASH)               AS E_NUM_WASH
FROM AOTS.TC_ECMWF.ADMIN_ALL_RIVER_MAT
WHERE COUNTRY = %s AND ADMIN_LEVEL = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
GROUP BY TILE_ID
"""

# ---------------------------------------------------------------------------
# Rainfall sibling queries: MERCATOR_TILE_PRECIP_MAT/ADMIN_ALL_PRECIP_MAT.
# Also NOT storm-scoped: keyed by COUNTRY + FORECAST_TIME + THRESHOLD_MM +
# WINDOW_H (uses PRECIP_MAT, not the ratio-based PRECIPRATIO_MAT sibling;
# the app's ms-rain-slider/ms-rain-window controls are already threshold-mm
# based via _RAIN_MM_BY_WINDOW, not ratio based).
#
# MERCATOR_TILE_PRECIP_MAT/ADMIN_ALL_PRECIP_MAT carry the full E_* exposure
# breakdown, same shape as wind's own MERCATOR_TILE_IMPACT_MAT/
# ADMIN_ALL_IMPACT_MAT (see _MERCATOR_IMPACT_ONLY_SQL above), so the
# queries below select the same full set, including a computed
# E_CHILDREN_TOTAL (same E_INFANT_POPULATION + E_SCHOOL_AGE_POPULATION +
# E_ADOLESCENT_POPULATION expression river's own query above uses. Precip
# has no stored E_CHILDREN_TOTAL column either, same as river).
#
# ZONE_ID on this table is a valid mercantile z14 quadkey (decodes to real
# in-country coordinates), despite the presence of NATIVE_CELL_ROW/
# NATIVE_CELL_COL columns (that pair references the underlying
# meteorological native grid cell, a separate concept from the ZONE_ID
# display quadkey). _fetch_mercator_tile's bounds-reconstruction logic is
# reused unmodified.
#
# No STEP_H-style duplication for precip (one row per ZONE_ID per
# (COUNTRY, FORECAST_TIME, THRESHOLD_MM, WINDOW_H) combo). A plain filter
# suffices, unlike river's MAX(...) aggregation above.
# ---------------------------------------------------------------------------

_MERCATOR_PRECIP_IMPACT_ONLY_SQL = """
SELECT
    ZONE_ID AS TILE_ID,
    PROBABILITY,
    E_POPULATION,
    E_INFANT_POPULATION,
    E_SCHOOL_AGE_POPULATION,
    E_ADOLESCENT_POPULATION,
    E_INFANT_POPULATION + E_SCHOOL_AGE_POPULATION + E_ADOLESCENT_POPULATION AS E_CHILDREN_TOTAL,
    E_BUILT_SURFACE_M2,
    E_NUM_SCHOOLS,
    E_NUM_HCS,
    E_NUM_SHELTERS,
    E_NUM_WASH
FROM AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT
WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
"""

_ADMIN_PRECIP_IMPACT_ONLY_SQL = """
SELECT
    TILE_ID,
    PROBABILITY,
    E_POPULATION,
    E_INFANT_POPULATION,
    E_SCHOOL_AGE_POPULATION,
    E_ADOLESCENT_POPULATION,
    E_INFANT_POPULATION + E_SCHOOL_AGE_POPULATION + E_ADOLESCENT_POPULATION AS E_CHILDREN_TOTAL,
    E_BUILT_SURFACE_M2,
    E_NUM_SCHOOLS,
    E_NUM_HCS,
    E_NUM_SHELTERS,
    E_NUM_WASH
FROM AOTS.TC_ECMWF.ADMIN_ALL_PRECIP_MAT
WHERE COUNTRY = %s AND ADMIN_LEVEL = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
"""



# ---------------------------------------------------------------------------
# Pandas bulk cache: single z=14 DataFrame
# ---------------------------------------------------------------------------
# One bulk Snowflake query loads ALL z=14 tiles for a country at first request.
# Subsequent tiles filter by binary search over a precomputed sorted TILE_ID
# index (_build_sorted_tile_index/_filter_by_tile_prefix), not a linear scan.
# Quadkey prefix hierarchy guarantees every matched tile is fully contained in
# the requested map tile → intersection(tile_box) is always a no-op, skipped.
# All per-tile mercantile calls are hoisted to load time via numpy arrays.
# Thread-safe: pandas DataFrame reads are GIL-protected; writes use
# double-checked locking.

def _precompute_mercator_bounds(tile_ids: "pd.Series") -> pd.DataFrame:
    """Vectorised bounds computation: runs once at bulk-load time."""
    ws = np.empty(len(tile_ids), dtype=np.float64)
    ss = np.empty_like(ws); es = np.empty_like(ws); ns = np.empty_like(ws)
    for i, qk in enumerate(tile_ids):
        try:
            t = mercantile.quadkey_to_tile(qk)
            b = mercantile.bounds(t)
            ws[i], ss[i], es[i], ns[i] = b.west, b.south, b.east, b.north
        except Exception as exc:
            log.debug("Bad quadkey %s, skipping bounds: %s", qk, exc)
            ws[i] = ss[i] = es[i] = ns[i] = np.nan
    return pd.DataFrame({"BW": ws, "BS": ss, "BE": es, "BN": ns})


# Shared, long-lived fan-out pool for per-country Snowflake round-trips
# (ensure_mercator/ensure_admin/ensure_facility's own multi-country loops,
# e.g. a region selection like "AIA+ATG+..."). Using a shared pool instead
# of a throwaway `with ThreadPoolExecutor(...)` per call avoids paying a
# fresh Snowflake connect() handshake per worker thread on every call, and
# avoids leaking the connection attached to a short-lived executor's thread
# until GC/server-side timeout. Long-lived worker threads reuse their
# thread-local connection (see get_connection()) after the first call.
# Sized well above the largest per-call fan-out (`min(8, len(codes))`) so a
# handful of concurrent multi-country requests never queue behind each
# other's leaf queries.
_SHARED_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=16, thread_name_prefix="aots-tile-fanout",
)

# Separate, small, long-lived pool for the /preload/* endpoints' own
# top-level supervisor tasks (ensure_mercator/ensure_admin/ensure_facility
# per hazard). A plain `threading.Thread` per task (the previous approach)
# never closes its Snowflake connection on exit, leaking one connection
# and one OS socket per preload burst indefinitely. Long-lived pool workers
# reuse their thread-local connection across calls instead. Kept separate
# from _SHARED_EXECUTOR (not just reusing it) for the same reason
# _SHARED_EXECUTOR's own callers previously used raw threads here: each of
# these supervisor tasks itself submits multi-country work onto
# _SHARED_EXECUTOR, and a _SHARED_EXECUTOR worker blocking on its own
# pool's tasks would starve it. A handful of workers is enough since
# preload bursts are occasional, not a steady high-frequency path.
_PRELOAD_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=6, thread_name_prefix="aots-preload",
)

# TTL for the base (threshold/storm/hazard-independent) mercator/admin
# caches, see _ensure_mercator_base_one/_ensure_admin_base_one. Population,
# infrastructure counts, and admin-region geometry only change when the
# pipeline re-runs for a country (not per-forecast), so this is deliberately
# much longer-lived than _TILE_TTL (the per-threshold impact data).
_BASE_DATA_TTL = _TILE_TTL * 4  # 60 minutes
_MERCATOR_BASE_CACHE_MAX = 16
_ADMIN_BASE_CACHE_MAX = 16

# River's per-country impact numbers (MERCATOR_TILE_RIVER_MAT/ADMIN_ALL_
# RIVER_MAT/the 4 river facility tables) carry a STEP_H column meaning a
# CUMULATIVE window (24/72/120/168h, matches DATAPIPELINE's own
# RIVER_LEADTIME_STEPS_H and pages/map_shell_concept.py's own ms-river-
# window options exactly), not a single-day snapshot. 168h (the full
# forecast horizon) is the default for any caller that doesn't pass a
# window: since each window is a cumulative union, the 168h row equals the
# worst case across the entire horizon, as every smaller window's data is a
# subset of it.
_RIVER_WINDOW_DEFAULT = 168


def _hazard_variant(hazard: str, wind_threshold: int, gust_threshold: Optional[int],
                     rp_tier: Optional[str], threshold_mm: Optional[float],
                     window_h: Optional[int]) -> tuple:
    """Cache-key suffix uniquely identifying a hazard + its own threshold(s).

    Every _DataCache dict is keyed by (country, storm, forecast_date) + this
    variant tuple, so e.g. Wind@50kt, Gust@50kt, River@rp10, and Rain@25mm/6h
    for the exact same country/storm/forecast_date path segments are four
    completely separate cache entries, never collide, and can all be loaded
    and served simultaneously (independently toggleable hazard layers).
    """
    if hazard == "gust":
        return ("gust", gust_threshold)
    if hazard == "river":
        # `window_h` is reused here as river's own CUMULATIVE lead-time
        # window (24-168h): the same generic slot rain uses. River's
        # per-country impact numbers (MERCATOR_TILE_RIVER_MAT etc.) carry a
        # STEP_H column that means "cumulative through this many hours"
        # (see _RIVER_WINDOW_DEFAULT's own comment below). Folding it into
        # the cache-key variant here means Wind@50kt/River@rp10+72h/
        # River@rp10+168h are correctly three separate cache entries, never
        # collide.
        return ("river", rp_tier, window_h or _RIVER_WINDOW_DEFAULT)
    if hazard == "rain":
        return ("rain", threshold_mm, window_h)
    return ("wind", wind_threshold)


class _DataCache:
    """Bulk-loads from Snowflake; serves from pandas DataFrames.

    Load: ~2s Snowflake + ~0.5s bounds pre-computation (one-time per country/storm).
    Serve: binary search over a precomputed sorted TILE_ID index at all zoom levels.
    Thread-safe: reads are GIL-protected dict/DataFrame ops; writes use
    double-checked locking.
    TTL: entries older than _TILE_TTL seconds are reloaded on next access so
    new pipeline output is served without a container restart.

    hazard="wind" (default) preserves the exact original single-hazard
    behaviour for every existing caller. hazard="gust"/"river"/"rain" route to
    their own sibling SQL (see _MERCATOR_GUST_IMPACT_ONLY_SQL etc. above) and
    their own threshold param (gust_threshold/rp_tier/threshold_mm+window_h
    respectively) instead of wind_threshold, wind_threshold itself is still
    accepted (and harmlessly ignored) for non-wind hazards so callers never
    need to omit it.

    Base/impact split (SNOWFLAKE mode only): ensure_mercator/ensure_admin
    do not re-run a whole-country 3-way LEFT JOIN per threshold. Each
    first queries/caches its own
    threshold-independent BASE_MERCATOR_TILE_MAT/BASE_ADMIN_GEOM_MAT columns
    once per (country[, admin_level]) in its own long-lived
    _mercator_base/_admin_base dict with its own long TTL (_BASE_DATA_TTL),
    see _ensure_mercator_base_one/_ensure_admin_base_one. Per variant, it
    then queries only the small impact/vulnerability/CCI columns keyed by
    ZONE_ID/TILE_ID and merges them onto the cached base DataFrame
    (_merge_no_collision). Admin additionally reuses the cached base's
    already-parsed geometries and STRtree untouched for the common
    single-country case: only per-variant impact PROPS get rebuilt, not the
    geometry parse or the spatial index. The LOCAL/BLOB (file-based) path is
    untouched: it already reads a base parquet plus small per-threshold
    CSVs and merges them itself (cheap local/blob disk I/O, not the
    Snowflake round-trip cost this split targets).
    """

    def __init__(self) -> None:
        self._mercator: dict[tuple, pd.DataFrame] = {}
        # (sorted_tile_ids, sorted_positions) per self._mercator key, see
        # _build_sorted_tile_index/_filter_by_tile_prefix. Computed once
        # right after the DataFrame itself is cached, reused by every
        # subsequent tile request against that same cache entry instead of
        # each one re-scanning the whole country's TILE_ID column.
        self._mercator_sorted: dict[tuple, tuple] = {}
        self._admin: dict[tuple, pd.DataFrame] = {}
        self._admin_geoms: dict[tuple, tuple] = {}  # key → (geom_list, strtree, props_list)
        self._facility: dict[tuple, pd.DataFrame] = {}
        # Threshold/storm/hazard-independent base caches, own long TTL via
        # _BASE_DATA_TTL, see _ensure_mercator_base_one /
        # _ensure_admin_base_one. Key shapes ("mercator_base"/"admin_base"
        # prefixed) never collide with the per-variant caches above, sharing
        # the same _loaded_at/_key_locks infra (see _evict_oldest_if_over's
        # own comment on key-shape separation).
        self._mercator_base: dict[tuple, pd.DataFrame] = {}
        self._admin_base: dict[tuple, tuple] = {}  # key → (df, geoms, tree, tile_id_order)
        # Vulnerability (PIN/CHIN) rows: threshold-invariant like the base
        # caches above (MERCATOR_TILE_VULNERABILITY_MAT/ADMIN_TILE_
        # VULNERABILITY_MAT are keyed by country/storm/forecast_date only,
        # no WIND_THRESHOLD column), but were previously re-queried inside
        # _load_one on every single threshold change anyway, since that
        # query wasn't split out the same way BASE was. Measured cost:
        # 0.5-4s per redundant re-fetch (JAM: 3.96s cold; MEX: 1.35s cold),
        # paid again for every wind-threshold slider tick even though the
        # rows never change. Two SEPARATE dicts (not one dict shared by
        # both key prefixes): _evict_oldest_if_over bounds a dict purely by
        # its own len(), with no per-key-prefix filtering, so a single
        # shared dict would let mercator_vuln and admin_vuln entries evict
        # each other and compete for one capacity budget instead of each
        # independently getting the same _MERCATOR_BASE_CACHE_MAX/
        # _ADMIN_BASE_CACHE_MAX cap _mercator_base/_admin_base themselves
        # get. Genuinely separate dicts, same convention as those two.
        self._vuln_mercator: dict[tuple, pd.DataFrame] = {}
        self._vuln_admin: dict[tuple, pd.DataFrame] = {}
        # ZONE_ID -> (lat, lon) health-centre coordinate lookup, key →
        # dict[str, tuple[float, float]], same long _BASE_DATA_TTL as the
        # base caches above (see _ensure_hc_coords_one).
        self._hc_coords: dict[tuple, dict] = {}
        # Per-cache-key lock instead of one instance-wide lock: a single
        # process-wide lock would serialize EVERY ensure_mercator/
        # ensure_admin/ensure_facility call, so an unrelated cache miss (a
        # different hazard, admin level, or facility layer, even from a
        # different browser tab) would queue behind whichever load happened
        # to be running, regardless of key. _key_locks_meta_lock only
        # guards the tiny dict-of-locks itself, not the actual loads.
        self._key_locks: dict[tuple, threading.Lock] = {}
        self._key_locks_meta_lock = threading.Lock()
        self._loaded_at: dict[tuple, float] = {}  # key → epoch seconds when loaded

    def _lock_for(self, key: tuple) -> threading.Lock:
        with self._key_locks_meta_lock:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    def _is_fresh(self, key: tuple, ttl: int = _TILE_TTL) -> bool:
        return key in self._loaded_at and (time.time() - self._loaded_at[key]) < ttl

    def _evict_oldest_if_over(self, just_written_key: tuple, cache_dicts: list, max_size: int) -> None:
        """Bounds a cache dict (or a matched set of dicts sharing the same
        key space, e.g. _admin + _admin_geoms) to `max_size` entries:
        evicts the single oldest-loaded entry (by self._loaded_at) whenever
        a fresh write pushes the primary dict over the cap.

        _is_fresh only ever refreshes a stale key in place; nothing removes
        an old one on its own, so a long-running multi-country/multi-storm/
        multi-date session would grow these dicts unbounded without this:
        a single mercator entry alone can be 300k-470k rows plus a parsed
        admin geometry list + STRtree per key. Mirrors the "keep latest-N"
        eviction _PrecipRawCache/_RiverExtentCache do for their own caches,
        called under the same per-key lock every write already holds, so
        this never races with a concurrent load.
        """
        primary = cache_dicts[0]
        if len(primary) <= max_size:
            return
        # Oldest by _loaded_at among keys actually present in the primary
        # dict: _loaded_at is shared across every cache in this class (the
        # different caches' key SHAPES don't collide, see ensure_admin's
        # own admin_level-suffixed key), so this excludes keys belonging to
        # a different cache entirely.
        candidates = [k for k in primary if k in self._loaded_at]
        if not candidates:
            return
        oldest_key = min(candidates, key=lambda k: self._loaded_at[k])
        if oldest_key == just_written_key:
            return  # never evict the entry this same call just wrote
        for d in cache_dicts:
            d.pop(oldest_key, None)
        self._loaded_at.pop(oldest_key, None)
        with self._key_locks_meta_lock:
            self._key_locks.pop(oldest_key, None)

    # --- base (threshold/storm/hazard-independent) -----------------------

    def _ensure_mercator_base_one(self, code: str) -> pd.DataFrame:
        """Load+cache the base mercator DataFrame (BASE_MERCATOR_TILE_MAT
        columns + precomputed tile bounds) for ONE country code, the same
        base data serves every hazard/threshold variant of that country.
        SNOWFLAKE mode only (see class docstring for why LOCAL/BLOB skips
        this and returns an empty placeholder here, ensure_mercator's own
        _load_one falls back to _load_mercator_from_files directly instead).
        """
        base_key = ("mercator_base", code, MAT_ZOOM_LEVEL)
        if self._is_fresh(base_key, ttl=_BASE_DATA_TTL) and base_key in self._mercator_base:
            return self._mercator_base[base_key]
        with self._lock_for(base_key):
            if self._is_fresh(base_key, ttl=_BASE_DATA_TTL) and base_key in self._mercator_base:
                return self._mercator_base[base_key]
            log.info("Cache: bulk-loading mercator BASE %s [%s]…", code, IMPACT_DATA_STORE)
            if IMPACT_DATA_STORE == "SNOWFLAKE":
                df = _run_query_df(_MERCATOR_BASE_SQL, [code, MAT_ZOOM_LEVEL])
            else:
                df = pd.DataFrame(columns=["TILE_ID"])
            if not df.empty:
                df = pd.concat([df.reset_index(drop=True), _precompute_mercator_bounds(df["TILE_ID"])], axis=1)
            else:
                df = pd.DataFrame(columns=["TILE_ID", "BW", "BS", "BE", "BN"])
            self._mercator_base[base_key] = df
            self._loaded_at[base_key] = time.time()
            self._evict_oldest_if_over(base_key, [self._mercator_base], _MERCATOR_BASE_CACHE_MAX)
            log.info("  Cache: %d z=14 base tiles ready (country=%s)", len(df), code)
            return df

    def _ensure_admin_base_one(self, code: str, admin_level: int) -> tuple:
        """Load+cache (base_df, geoms, tree, tile_id_order) for ONE country
        code/admin_level. geoms/tree are parsed/built exactly once here;
        tile_id_order lines up positionally with geoms (only rows that had a
        usable GEOJSON, matching the original per-row skip). ensure_admin
        reuses geoms/tree UNCHANGED for the common single-country case:
        only the per-variant impact PROPS get rebuilt from a fresh small
        impact query merged onto base_df. SNOWFLAKE mode only, same
        reasoning as _ensure_mercator_base_one above.
        """
        from shapely.strtree import STRtree
        base_key = ("admin_base", code, admin_level)
        if self._is_fresh(base_key, ttl=_BASE_DATA_TTL) and base_key in self._admin_base:
            return self._admin_base[base_key]
        with self._lock_for(base_key):
            if self._is_fresh(base_key, ttl=_BASE_DATA_TTL) and base_key in self._admin_base:
                return self._admin_base[base_key]
            log.info("Cache: bulk-loading admin BASE %s L%s [%s]…", code, admin_level, IMPACT_DATA_STORE)
            if IMPACT_DATA_STORE == "SNOWFLAKE":
                df = _run_query_df(_ADMIN_BASE_SQL, [code, admin_level])
            else:
                df = pd.DataFrame(columns=["TILE_ID"])
            geoms, tile_id_order = [], []
            for _, row in df.iterrows():
                geojson_str = row.get("GEOJSON")
                if not geojson_str:
                    continue
                try:
                    geojson_data = json.loads(geojson_str) if isinstance(geojson_str, str) else geojson_str
                    geoms.append(shape(geojson_data))
                    tile_id_order.append(row["TILE_ID"])
                except Exception as e:
                    log.debug("Skip admin base geom: %s", e)
            tree = STRtree(geoms)
            entry = (df, geoms, tree, tile_id_order)
            self._admin_base[base_key] = entry
            self._loaded_at[base_key] = time.time()
            self._evict_oldest_if_over(base_key, [self._admin_base], _ADMIN_BASE_CACHE_MAX)
            log.info("  Cache: %d admin base regions parsed + indexed (country=%s L%s)",
                     len(geoms), code, admin_level)
            return entry

    def _ensure_vuln_mercator_one(self, code: str, storm: str, forecast_date: str) -> pd.DataFrame:
        """Load+cache MERCATOR_TILE_VULNERABILITY_MAT rows for ONE
        (country, storm, forecast_date): threshold-invariant, same reasoning
        as _ensure_mercator_base_one, just keyed by storm/forecast_date too
        (vuln data is real per-forecast-run data, unlike BASE's own
        population/geometry columns, which don't change between runs).
        SNOWFLAKE mode only, mirrors every other _ensure_*_one here.
        """
        vuln_key = ("mercator_vuln", code, storm, forecast_date)
        if self._is_fresh(vuln_key) and vuln_key in self._vuln_mercator:
            return self._vuln_mercator[vuln_key]
        with self._lock_for(vuln_key):
            if self._is_fresh(vuln_key) and vuln_key in self._vuln_mercator:
                return self._vuln_mercator[vuln_key]
            if IMPACT_DATA_STORE == "SNOWFLAKE":
                rows = _run_query(_MERCATOR_VULN_ONLY_SQL, [code, MAT_ZOOM_LEVEL, storm, forecast_date])
                df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["TILE_ID"])
            else:
                df = pd.DataFrame(columns=["TILE_ID"])
            self._vuln_mercator[vuln_key] = df
            self._loaded_at[vuln_key] = time.time()
            self._evict_oldest_if_over(vuln_key, [self._vuln_mercator], _MERCATOR_BASE_CACHE_MAX)
            return df

    def _ensure_vuln_admin_one(self, code: str, admin_level: int, storm: str, forecast_date: str) -> pd.DataFrame:
        """Admin-level sibling of _ensure_vuln_mercator_one above, same
        threshold-invariant reasoning, keyed by admin_level too."""
        vuln_key = ("admin_vuln", code, admin_level, storm, forecast_date)
        if self._is_fresh(vuln_key) and vuln_key in self._vuln_admin:
            return self._vuln_admin[vuln_key]
        with self._lock_for(vuln_key):
            if self._is_fresh(vuln_key) and vuln_key in self._vuln_admin:
                return self._vuln_admin[vuln_key]
            if IMPACT_DATA_STORE == "SNOWFLAKE":
                rows = _run_query(_ADMIN_VULN_ONLY_SQL, [code, admin_level, storm, forecast_date])
                df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["TILE_ID"])
            else:
                df = pd.DataFrame(columns=["TILE_ID"])
            self._vuln_admin[vuln_key] = df
            self._loaded_at[vuln_key] = time.time()
            self._evict_oldest_if_over(vuln_key, [self._vuln_admin], _ADMIN_BASE_CACHE_MAX)
            return df

    # --- mercator --------------------------------------------------------

    def ensure_mercator(self, country: str, storm: str, forecast_date: str,
                        wind_threshold: int, hazard: str = "wind",
                        gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                        threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> None:
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (country, storm, forecast_date) + variant
        if self._is_fresh(key) and key in self._mercator:
            return
        with self._lock_for(key):
            if self._is_fresh(key) and key in self._mercator:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]

            def _load_one(code: str) -> pd.DataFrame:
                log.info("Cache: bulk-loading mercator %s/%s/%s hazard=%s variant=%s [%s]…",
                         code, storm, forecast_date, hazard, variant, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    base_df = self._ensure_mercator_base_one(code)
                    if base_df.empty:
                        return base_df
                    merged = base_df
                    if hazard == "gust":
                        impact_rows = _run_query(_MERCATOR_GUST_IMPACT_ONLY_SQL, [
                            code, MAT_ZOOM_LEVEL, storm, forecast_date, gust_threshold,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                    elif hazard == "river":
                        impact_rows = _run_query(_MERCATOR_RIVER_IMPACT_ONLY_SQL, [
                            code, forecast_date, rp_tier, window_h or _RIVER_WINDOW_DEFAULT,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                    elif hazard == "rain":
                        impact_rows = _run_query(_MERCATOR_PRECIP_IMPACT_ONLY_SQL, [
                            code, forecast_date, threshold_mm, window_h,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                    else:
                        impact_rows = _run_query(_MERCATOR_IMPACT_ONLY_SQL, [
                            code, MAT_ZOOM_LEVEL, storm, forecast_date, wind_threshold,
                        ])
                        # Threshold-invariant, cached once per (country,
                        # storm, forecast_date) via _ensure_vuln_mercator_one
                        # instead of re-queried here on every threshold
                        # change, see that method's own docstring for the
                        # measured cost this avoids.
                        vuln_df = self._ensure_vuln_mercator_one(code, storm, forecast_date)
                        # CCI (Child Cyclone Index) stays in Snowflake for
                        # other consumers, but this app no longer queries or
                        # merges it: no UI path in the current dashboard
                        # displays it, so the query was pure redundant cost.
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                        if not vuln_df.empty:
                            merged = _merge_no_collision(merged, vuln_df, on="TILE_ID")
                    return merged
                elif hazard == "wind":
                    rows = _load_mercator_from_files(code, storm, forecast_date, wind_threshold)
                    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["TILE_ID"])
                else:
                    log.warning("%s STAGE (file-based) mercator loading not implemented, returning empty for %s",
                                hazard, code)
                    return pd.DataFrame(columns=["TILE_ID"])

            # Independent per-country Snowflake round-trips: safe to run
            # concurrently (pure network I/O), so a multi-country storm
            # selection loads every country's data in parallel instead of
            # serially. Uses the shared long-lived _SHARED_EXECUTOR (see
            # its own comment) instead of an ephemeral per-call executor.
            if len(codes) > 1:
                dfs = [df for df in _SHARED_EXECUTOR.map(_load_one, codes) if not df.empty]
            else:
                dfs = []
                for code in codes:
                    df = _load_one(code)
                    if not df.empty:
                        dfs.append(df)
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
                if "BW" not in df.columns:
                    # File-based fallback rows don't carry precomputed
                    # bounds yet (SNOWFLAKE-sourced rows already do, via
                    # the cached base, see _ensure_mercator_base_one).
                    df = pd.concat([df.reset_index(drop=True), _precompute_mercator_bounds(df["TILE_ID"])], axis=1)
            else:
                df = pd.DataFrame(columns=["TILE_ID", "BW", "BS", "BE", "BN"])
            log.info("  Cache: %d z=14 tiles ready (country=%s, hazard=%s)", len(df), country, hazard)
            self._mercator[key] = df
            self._mercator_sorted[key] = (_build_sorted_tile_index(df["TILE_ID"].to_numpy())
                                           if not df.empty and "TILE_ID" in df.columns else (np.array([]), np.array([])))
            self._loaded_at[key] = time.time()
            self._evict_oldest_if_over(key, [self._mercator, self._mercator_sorted], _MERCATOR_CACHE_MAX)

    def query_mercator(self, country: str, storm: str, forecast_date: str,
                       wind_threshold: int, like_pat: str, z: int, hazard: str = "wind",
                       gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                       threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> list[dict]:
        self.ensure_mercator(country, storm, forecast_date, wind_threshold, hazard,
                             gust_threshold, rp_tier, threshold_mm, window_h)
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (country, storm, forecast_date) + variant
        df = self._mercator.get(key)
        if df is None or df.empty:
            return []
        sub = _filter_by_tile_prefix(df, like_pat, self._mercator_sorted.get(key))
        return sub.to_dict("records")

    # --- admin -----------------------------------------------------------

    def ensure_admin(self, country: str, storm: str, forecast_date: str,
                     wind_threshold: int, admin_level: int, hazard: str = "wind",
                     gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                     threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> None:
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (country, storm, forecast_date) + variant + (admin_level,)
        if self._is_fresh(key) and key in self._admin_geoms:
            return
        with self._lock_for(key):
            if self._is_fresh(key) and key in self._admin_geoms:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]

            def _load_one(code: str) -> tuple:
                """Returns (merged_df, reuse_geoms, reuse_tree, tile_id_order).
                SNOWFLAKE mode reuses the cached base's geoms/tree/order
                untouched (only impact/vuln/cci columns are freshly queried
                and merged), reuse_geoms is None for the file-based
                fallback, signalling the caller to parse geometry fresh from
                merged_df['GEOJSON'] the original way.
                """
                log.info("Cache: bulk-loading admin %s/%s/%s hazard=%s variant=%s L%s [%s]…",
                         code, storm, forecast_date, hazard, variant, admin_level, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    base_df, geoms, tree, tile_id_order = self._ensure_admin_base_one(code, admin_level)
                    if base_df.empty:
                        return base_df, [], None, []
                    merged = base_df
                    if hazard == "gust":
                        impact_rows = _run_query(_ADMIN_GUST_IMPACT_ONLY_SQL, [
                            code, admin_level, storm, forecast_date, gust_threshold,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                    elif hazard == "river":
                        impact_rows = _run_query(_ADMIN_RIVER_IMPACT_ONLY_SQL, [
                            code, admin_level, forecast_date, rp_tier, window_h or _RIVER_WINDOW_DEFAULT,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                    elif hazard == "rain":
                        impact_rows = _run_query(_ADMIN_PRECIP_IMPACT_ONLY_SQL, [
                            code, admin_level, forecast_date, threshold_mm, window_h,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                    else:
                        impact_rows = _run_query(_ADMIN_IMPACT_ONLY_SQL, [
                            code, admin_level, storm, forecast_date, wind_threshold,
                        ])
                        # Threshold-invariant, cached once per (country,
                        # admin_level, storm, forecast_date) via
                        # _ensure_vuln_admin_one instead of re-queried here on
                        # every threshold change, see that method's own
                        # docstring for the measured cost this avoids.
                        vuln_df = self._ensure_vuln_admin_one(code, admin_level, storm, forecast_date)
                        # CCI is intentionally not queried here: it stays in
                        # Snowflake for other consumers, but this app never
                        # reads it. See ensure_mercator's own comment for the
                        # matching tile-level decision.
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                        if not vuln_df.empty:
                            merged = _merge_no_collision(merged, vuln_df, on="TILE_ID")
                    return merged, geoms, tree, tile_id_order
                elif hazard == "wind":
                    rows = _load_admin_from_files(code, storm, forecast_date, wind_threshold, admin_level)
                    merged = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["TILE_ID"])
                    return merged, None, None, None
                else:
                    log.warning("%s STAGE (file-based) admin loading not implemented, returning empty for %s",
                                hazard, code)
                    return pd.DataFrame(columns=["TILE_ID"]), None, None, None

            # See ensure_mercator's own comment, same safe-to-parallelize
            # reasoning, same shared executor.
            if len(codes) > 1:
                results = list(_SHARED_EXECUTOR.map(_load_one, codes))
            else:
                results = [_load_one(code) for code in codes]

            all_dfs: list[pd.DataFrame] = []
            all_geoms: list = []
            all_props: list = []
            # Only safe to reuse the base cache's own STRtree object as-is
            # when there's exactly one country AND the merge didn't drop any
            # of its geoms (see below), otherwise a fresh tree must be
            # built over the combined/filtered geometry list.
            single_code_tree = None
            for merged_df, reuse_geoms, reuse_tree, reuse_tile_id_order in results:
                if merged_df is None or merged_df.empty:
                    continue
                all_dfs.append(merged_df)
                if reuse_geoms is not None:
                    # SNOWFLAKE path, geoms already parsed once at base-load
                    # time (see _ensure_admin_base_one); only look up each
                    # geom's row (by TILE_ID) to build THIS variant's props.
                    merged_idx = merged_df.set_index("TILE_ID", drop=False)
                    code_geoms, code_props = [], []
                    for tile_id, geom in zip(reuse_tile_id_order, reuse_geoms):
                        if tile_id not in merged_idx.index:
                            continue
                        row = merged_idx.loc[tile_id]
                        if isinstance(row, pd.DataFrame):  # duplicate TILE_ID guard
                            row = row.iloc[0]
                        props = {k: _py(v) for k, v in row.items()
                                 if k != "GEOJSON" and _safe_prop(v) is not None}
                        code_geoms.append(geom)
                        code_props.append(props)
                    if len(results) == 1 and reuse_tree is not None and len(code_geoms) == len(reuse_geoms):
                        # Nothing dropped, single country: the cached base
                        # STRtree's own geometry set/order is unchanged, so
                        # reuse it directly instead of rebuilding it from
                        # scratch every threshold change (only PROPS
                        # actually vary).
                        single_code_tree = reuse_tree
                    all_geoms.extend(code_geoms)
                    all_props.extend(code_props)
                else:
                    # File-based fallback, parse geometry fresh, same as
                    # the original single-query implementation.
                    for _, row in merged_df.iterrows():
                        geojson_str = row.get("GEOJSON")
                        if not geojson_str:
                            continue
                        try:
                            geojson_data = json.loads(geojson_str) if isinstance(geojson_str, str) else geojson_str
                            geom = shape(geojson_data)
                            props = {k: _py(v) for k, v in row.items()
                                     if k != "GEOJSON" and _safe_prop(v) is not None}
                            all_geoms.append(geom)
                            all_props.append(props)
                        except Exception as e:
                            log.debug("Skip admin geom: %s", e)

            df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=["TILE_ID"])
            self._admin[key] = df
            if single_code_tree is not None:
                tree = single_code_tree
            else:
                from shapely.strtree import STRtree
                tree = STRtree(all_geoms)
            self._admin_geoms[key] = (all_geoms, tree, all_props)
            self._loaded_at[key] = time.time()
            self._evict_oldest_if_over(key, [self._admin, self._admin_geoms], _ADMIN_CACHE_MAX)
            log.info("  Cache: %d admin regions parsed + indexed", len(all_geoms))

    def query_admin(self, country: str, storm: str, forecast_date: str,
                    wind_threshold: int, admin_level: int,
                    tile_w: float, tile_s: float, tile_e: float, tile_n: float,
                    hazard: str = "wind", gust_threshold: Optional[int] = None,
                    rp_tier: Optional[str] = None, threshold_mm: Optional[float] = None,
                    window_h: Optional[int] = None) -> list[tuple]:
        """Returns list of (geom, props) for features that intersect the tile bbox."""
        self.ensure_admin(country, storm, forecast_date, wind_threshold, admin_level, hazard,
                          gust_threshold, rp_tier, threshold_mm, window_h)
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        entry = self._admin_geoms.get((country, storm, forecast_date) + variant + (admin_level,))
        if not entry:
            return []
        geoms, tree, props_list = entry
        tile_box = box(tile_w, tile_s, tile_e, tile_n)
        candidate_idxs = tree.query(tile_box, predicate='intersects')
        return [(geoms[i], props_list[i]) for i in candidate_idxs]

    # --- facility points -------------------------------------------------

    def _ensure_hc_coords_one(self, hazard: str, code: str, storm: Optional[str],
                              forecast_date: str) -> dict:
        """ZONE_ID -> (lat, lon) lookup for health-centre facilities, cached
        per (hazard, code, storm, forecast_date) with the same long
        _BASE_DATA_TTL as the mercator/admin base caches: ZONE_ID
        coordinates are threshold-independent within a forecast cycle: the
        same ZONE_ID resolves to an identical lat/lon across every
        threshold and even across the wind/gust sibling tables. Only
        called for hazard in (wind, gust, rain). River's HC_RIVER_MAT
        already does its own self-contained ZONE_ID GROUP BY.
        """
        key = ("hc_coords", hazard, code, storm, forecast_date)
        if self._is_fresh(key, ttl=_BASE_DATA_TTL) and key in self._hc_coords:
            return self._hc_coords[key]
        with self._lock_for(key):
            if self._is_fresh(key, ttl=_BASE_DATA_TTL) and key in self._hc_coords:
                return self._hc_coords[key]
            if hazard == "rain":
                rows = _run_query(_HC_COORDS_SQL[hazard], [code, forecast_date])
            else:
                rows = _run_query(_HC_COORDS_SQL[hazard], [code, storm, forecast_date])
            coords = {r["ZONE_ID"]: (r["LATITUDE"], r["LONGITUDE"]) for r in rows}
            self._hc_coords[key] = coords
            self._loaded_at[key] = time.time()
            self._evict_oldest_if_over(key, [self._hc_coords], _HC_COORDS_CACHE_MAX)
            log.info("  Cache: %d health-centre ZONE_ID coords (%s %s hazard=%s)",
                     len(coords), code, forecast_date, hazard)
            return coords

    def ensure_facility(self, layer_type: str, country: str, storm: str,
                        forecast_date: str, wind_threshold: int, hazard: str = "wind",
                        gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                        threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> None:
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (layer_type, country, storm, forecast_date) + variant
        if self._is_fresh(key) and key in self._facility:
            return
        with self._lock_for(key):
            if self._is_fresh(key) and key in self._facility:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]

            def _load_one(code: str) -> list[dict]:
                log.info("Cache: bulk-loading facility %s %s/%s/%s hazard=%s variant=%s [%s]…",
                         layer_type, code, storm, forecast_date, hazard, variant, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    if hazard == "gust":
                        rows = _run_query(_FACILITY_GUST_SQL[layer_type],
                                          [code, storm, forecast_date, gust_threshold])
                    elif hazard == "river":
                        rows = _run_query(_FACILITY_RIVER_SQL[layer_type],
                                          [code, forecast_date, rp_tier, window_h or _RIVER_WINDOW_DEFAULT])
                    elif hazard == "rain":
                        rows = _run_query(_FACILITY_PRECIP_SQL[layer_type],
                                          [code, forecast_date, threshold_mm, window_h])
                    else:
                        rows = _run_query(_FACILITY_IMPACT_SQL[layer_type],
                                          [code, storm, forecast_date, wind_threshold])
                    if not rows:
                        log.info("  No impact data for %s %s, using base layer", layer_type, code)
                        rows = _run_query(_FACILITY_BASE_SQL[layer_type], [code])
                    elif layer_type == "health" and hazard in ("wind", "gust", "rain"):
                        # The lean queries above carry no LATITUDE/
                        # LONGITUDE (no per-row ST_CENTROID). Resolve via
                        # the cached ZONE_ID lookup instead. Rows with no
                        # coord match (should never happen given both
                        # queries share the same geometry-not-null filter)
                        # are dropped rather than rendered with a missing/
                        # wrong location.
                        coords = self._ensure_hc_coords_one(hazard, code, storm, forecast_date)
                        enriched = []
                        missing = 0
                        for row in rows:
                            zid = row.pop("ZONE_ID", None)
                            lat_lon = coords.get(zid)
                            if lat_lon is None:
                                missing += 1
                                continue
                            row["LATITUDE"], row["LONGITUDE"] = lat_lon
                            enriched.append(row)
                        if missing:
                            log.warning("  %d/%d health rows had no ZONE_ID coord match (%s hazard=%s)",
                                       missing, len(rows), code, hazard)
                        rows = enriched
                    return rows
                elif hazard == "wind":
                    return _load_facility_from_files(layer_type, code, storm, forecast_date, wind_threshold)
                else:
                    log.warning("%s STAGE (file-based) facility loading not implemented, returning empty for %s",
                                hazard, code)
                    return []

            all_rows: list[dict] = []
            # See ensure_mercator's own comment, same safe-to-parallelize
            # reasoning, same shared executor.
            if len(codes) > 1:
                for rows in _SHARED_EXECUTOR.map(_load_one, codes):
                    all_rows.extend(rows)
            else:
                for code in codes:
                    all_rows.extend(_load_one(code))
            df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
                columns=["NAME", "PROBABILITY", "LATITUDE", "LONGITUDE"])
            if "PROBABILITY" in df.columns:
                df["PROBABILITY"] = pd.to_numeric(df["PROBABILITY"], errors="coerce").fillna(0.0)
            log.info("  Cache: %d %s points (country=%s, hazard=%s)", len(df), layer_type, country, hazard)
            self._facility[key] = df
            self._loaded_at[key] = time.time()
            self._evict_oldest_if_over(key, [self._facility], _FACILITY_CACHE_MAX)

    def get_facility_df(self, layer_type: str, country: str, storm: str,
                        forecast_date: str, wind_threshold: int, hazard: str = "wind",
                        gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                        threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> "pd.DataFrame":
        self.ensure_facility(layer_type, country, storm, forecast_date, wind_threshold, hazard,
                             gust_threshold, rp_tier, threshold_mm, window_h)
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        return self._facility.get((layer_type, country, storm, forecast_date) + variant,
                                  pd.DataFrame())

_cache = _DataCache()


# ---------------------------------------------------------------------------
# Facility SQL: impact tables (with PROBABILITY) and base fallbacks
#
# Health-centre coordinates: the *_HC_MAT impact tables carry no plain
# LATITUDE/LONGITUDE columns (unlike the school/shelter/WASH siblings), only
# a raw ALL_DATA:geometry blob. A NAME-keyed join onto BASE_HC_MAT (which has
# plain LATITUDE/LONGITUDE) was tried as a perf optimization but reverted:
# NAME is not a reliable key: a large share of health-centre NAMEs (roughly
# a fifth to two-fifths of rows, country-dependent, e.g. JPN) are shared by
# multiple facilities at genuinely different coordinates, so a NAME-only join
# silently collapses distinct facilities onto one arbitrary shared location.
#
# ZONE_ID, present on every *_HC_MAT table (HC_IMPACT_MAT/HC_GUST_MAT/
# HC_PRECIP_MAT), is a stable, collision-free per-facility identifier: the
# derived (ROUND(lat,6), ROUND(lon,6)) per ZONE_ID is stable across every
# storm/forecast_date/threshold combination and even across the separate
# wind/gust tables. So instead of every threshold-scoped query computing
# ST_Y/ST_X(ST_CENTROID(...)) per row (below, dropped from these three),
# coordinates are resolved once per (hazard, country, storm, forecast_date)
# via _ensure_hc_coords_one's own ZONE_ID-keyed GROUP BY query (see
# _HC_COORDS_SQL) and cached with the same long _BASE_DATA_TTL as the
# mercator/admin base caches, then merged onto each lean per-threshold row
# in ensure_facility's _load_one. River's own HC_RIVER_MAT
# (_FACILITY_RIVER_SQL below) already does its own self-contained ZONE_ID
# GROUP BY per query and is untouched by this. It doesn't need
# cross-query caching since it has no separate base/impact split.
# ---------------------------------------------------------------------------

_FACILITY_IMPACT_SQL: dict[str, str] = {
    "schools": """
        SELECT ZONE_ID, SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "health": """
        SELECT ZONE_ID, NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY
        FROM AOTS.TC_ECMWF.HC_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND ALL_DATA:geometry::STRING IS NOT NULL
    """,
    "shelters": """
        SELECT ZONE_ID, NAME, SHELTER_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT ZONE_ID, NAME, WASH_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

_FACILITY_GUST_SQL: dict[str, str] = {
    "schools": """
        SELECT ZONE_ID, SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND GUST_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "health": """
        SELECT ZONE_ID, NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY
        FROM AOTS.TC_ECMWF.HC_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND GUST_THRESHOLD = %s
          AND ALL_DATA:geometry::STRING IS NOT NULL
    """,
    "shelters": """
        SELECT ZONE_ID, NAME, SHELTER_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND GUST_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT ZONE_ID, NAME, WASH_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND GUST_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

# These queries filter `AND STEP_H = %s` (the requested cumulative window,
# see _RIVER_WINDOW_DEFAULT's own comment) rather than aggregating across
# every STEP_H (same reasoning documented above for
# _MERCATOR_RIVER_IMPACT_ONLY_SQL: STEP_H rows for the same facility are
# duplicates of the same underlying row at different cumulative windows).
# MAX(...)/GROUP BY are kept as the same defensive-no-op pattern the impact
# queries above use (should be one row per ZONE_ID per window already).
#
# GROUP BY is on ZONE_ID, not on name/type columns: grouping by descriptive
# columns (SCHOOL_NAME, NAME+TYPE+...) would collapse multiple genuinely
# distinct facilities that share identical descriptive metadata. ZONE_ID is
# a per-facility identifier present in every one of these tables and never
# maps to more than one distinct name within a single (country,
# forecast_time, rp_tier) slice for any of the four facility types: the
# correct, collision-free key to aggregate the STEP_H duplication away
# without merging distinct facilities.
_FACILITY_RIVER_SQL: dict[str, str] = {
    "schools": """
        SELECT ZONE_ID, MAX(SCHOOL_NAME) AS SCHOOL_NAME, MAX(EDUCATION_LEVEL) AS EDUCATION_LEVEL,
               MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY ZONE_ID
    """,
    "health": """
        SELECT ZONE_ID, NAME, FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY,
               ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(GEOM_STR, 'HEX')))) AS LATITUDE,
               ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(GEOM_STR, 'HEX')))) AS LONGITUDE
        FROM (
            SELECT ZONE_ID, MAX(NAME) AS NAME, MAX(HEALTH_AMENITY_TYPE) AS FACILITY_TYPE,
                   MAX(OPERATOR_TYPE) AS OPERATOR_TYPE,
                   MAX(PROBABILITY) AS PROBABILITY,
                   MAX(ALL_DATA:geometry::STRING) AS GEOM_STR
            FROM AOTS.TC_ECMWF.HC_RIVER_MAT
            WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
              AND ALL_DATA:geometry::STRING IS NOT NULL
            GROUP BY ZONE_ID
        )
    """,
    "shelters": """
        SELECT ZONE_ID, MAX(NAME) AS NAME, MAX(SHELTER_TYPE) AS SHELTER_TYPE, MAX(CATEGORY) AS CATEGORY,
               MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY ZONE_ID
    """,
    "wash": """
        SELECT ZONE_ID, MAX(NAME) AS NAME, MAX(WASH_TYPE) AS WASH_TYPE, MAX(CATEGORY) AS CATEGORY,
               MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY ZONE_ID
    """,
}

# Rain facility queries: no STEP_H duplication, plain SELECT.
_FACILITY_PRECIP_SQL: dict[str, str] = {
    "schools": """
        SELECT ZONE_ID, SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "health": """
        SELECT ZONE_ID, NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY
        FROM AOTS.TC_ECMWF.HC_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
          AND ALL_DATA:geometry::STRING IS NOT NULL
    """,
    "shelters": """
        SELECT ZONE_ID, NAME, SHELTER_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT ZONE_ID, NAME, WASH_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

# ZONE_ID -> (lat, lon) health-centre coordinate lookup, one query per
# (hazard, country, storm, forecast_date) regardless of how many thresholds
# get browsed, see the comment above _FACILITY_IMPACT_SQL and
# _DataCache._ensure_hc_coords_one. No threshold filter: pulls every ZONE_ID
# for the whole forecast cycle in one pass, GROUP BY collapses the (harmless
# , see comment above) per-threshold-row duplication.
_HC_COORDS_SQL: dict[str, str] = {
    "wind": """
        SELECT ZONE_ID,
               MAX(ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX'))))) AS LATITUDE,
               MAX(ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX'))))) AS LONGITUDE
        FROM AOTS.TC_ECMWF.HC_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s
          AND ALL_DATA:geometry::STRING IS NOT NULL
        GROUP BY ZONE_ID
    """,
    "gust": """
        SELECT ZONE_ID,
               MAX(ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX'))))) AS LATITUDE,
               MAX(ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX'))))) AS LONGITUDE
        FROM AOTS.TC_ECMWF.HC_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s
          AND ALL_DATA:geometry::STRING IS NOT NULL
        GROUP BY ZONE_ID
    """,
    "rain": """
        SELECT ZONE_ID,
               MAX(ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX'))))) AS LATITUDE,
               MAX(ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX'))))) AS LONGITUDE
        FROM AOTS.TC_ECMWF.HC_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s
          AND ALL_DATA:geometry::STRING IS NOT NULL
        GROUP BY ZONE_ID
    """,
}
_HC_COORDS_CACHE_MAX = 32

_FACILITY_BASE_SQL: dict[str, str] = {
    "schools": """
        SELECT SCHOOL_NAME, EDUCATION_LEVEL, 0.0 AS PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.BASE_SCHOOL_MAT
        WHERE COUNTRY = %s AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "health": """
        SELECT NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, 0.0 AS PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.BASE_HC_MAT
        WHERE COUNTRY = %s AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "shelters": """
        SELECT NAME, SHELTER_TYPE, CATEGORY, 0.0 AS PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.BASE_SHELTER_MAT
        WHERE COUNTRY = %s AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT NAME, WASH_TYPE, CATEGORY, 0.0 AS PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.BASE_WASH_MAT
        WHERE COUNTRY = %s AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

_FACILITY_BASE_COLORS: dict[str, str] = {
    "schools":  "#ADD8E6",
    "health":   "#90EE90",
    "shelters": "#E91E8C",
    "wash":     "#40E0D0",
}


# ---------------------------------------------------------------------------
# Tile fetch functions
# ---------------------------------------------------------------------------

_SKIP_COLS_MERCATOR = frozenset(("TILE_ID", "BW", "BS", "BE", "BN"))


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_mercator_tile(
    country: str,
    storm: str,
    forecast_date: str,
    wind_threshold: int,
    z: int,
    x: int,
    y: int,
    hazard: str = "wind",
    gust_threshold: Optional[int] = None,
    rp_tier: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
) -> bytes:
    like_pat = _quadkey_like_pattern(z, x, y)
    rows = _cache.query_mercator(country, storm, forecast_date, wind_threshold, like_pat, z,
                                 hazard, gust_threshold, rp_tier, threshold_mm, window_h)
    if not rows:
        return b""

    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)

    features = []
    for row in rows:
        w, s, e, n = row.get("BW"), row.get("BS"), row.get("BE"), row.get("BN")
        # Skip rows with invalid pre-computed bounds (NaN from failed quadkey parse)
        if w is None or pd.isna(w):
            continue
        # Quadkey hierarchy guarantees every matched z=14 tile is fully
        # contained in (z, x, y) → intersection with tile_box is always a
        # no-op; build the box directly from pre-computed bounds.
        geom = box(w, s, e, n)
        # Exclude internal/NaN columns and None/NaN property values.
        # v == v is False for float NaN: filters out Snowflake NULLs that
        # pandas converted to NaN (which would otherwise encode as opaque tiles).
        props = {k: _py(v) for k, v in row.items()
                 if k not in _SKIP_COLS_MERCATOR and _safe_prop(v) is not None}
        features.append({"geometry": geom.wkt, "properties": props})

    if not features:
        return b""

    pbf = mapbox_vector_tile.encode(
        [{"name": "tiles", "features": features}],
        default_options={"quantize_bounds": (tile_w, tile_s, tile_e, tile_n)},
    )
    raw = bytes(pbf) if not isinstance(pbf, bytes) else pbf
    return gzip.compress(raw, compresslevel=6)


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_admin_tile(
    country: str,
    storm: str,
    forecast_date: str,
    wind_threshold: int,
    admin_level: int,
    z: int,
    x: int,
    y: int,
    hazard: str = "wind",
    gust_threshold: Optional[int] = None,
    rp_tier: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
) -> bytes:
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    candidates = _cache.query_admin(country, storm, forecast_date, wind_threshold, admin_level,
                                   tile_w, tile_s, tile_e, tile_n,
                                   hazard, gust_threshold, rp_tier, threshold_mm, window_h)
    if not candidates:
        return b""

    # Clip in WGS84, then project to Web Mercator for PBF encoding so MapLibre
    # (which uses Mercator internally) renders polygons without lat distortion.
    merc_b = mercantile.xy_bounds(mercantile.Tile(x, y, z))
    tile_box_wgs = box(tile_w, tile_s, tile_e, tile_n)
    tile_box_merc = box(merc_b.left, merc_b.bottom, merc_b.right, merc_b.top)

    features = []
    for geom, props in candidates:
        try:
            clipped_wgs = geom.intersection(tile_box_wgs)
            if clipped_wgs.is_empty or clipped_wgs.geom_type == "GeometryCollection":
                continue
            clipped_merc = _to_merc(clipped_wgs).intersection(tile_box_merc)
            if clipped_merc.is_empty:
                continue
            features.append({"geometry": clipped_merc.wkt, "properties": props})
        except Exception as e:
            log.debug("Skip admin clip: %s", e)

    if not features:
        return b""

    pbf = mapbox_vector_tile.encode(
        [{"name": "admin", "features": features}],
        default_options={"quantize_bounds": (merc_b.left, merc_b.bottom, merc_b.right, merc_b.top)},
    )
    raw = bytes(pbf) if not isinstance(pbf, bytes) else pbf
    # gzip, matching _fetch_mercator_tile's own PBF output above (real
    # perf-audit finding: admin-region MVT was going out uncompressed while
    # its mercator sibling already gzipped, purely a historical gap, not a
    # deliberate choice, since admin-region MVT (property-per-feature,
    # repeated keys) compresses just as well). _fetch_admin_combined_tile
    # below has the identical fix for the same reason.
    return gzip.compress(raw, compresslevel=6)


# ---------------------------------------------------------------------------
# Raster palette definitions + color helpers
# ---------------------------------------------------------------------------

_RASTER_PALETTES: dict[str, dict] = {
    # 'log' for PROBABILITY and every E_* entry: for PROBABILITY, both
    # endpoints are now fully fixed constants (see _FIXED_SCALE_COLS),
    # unaffected by min_val anchoring at all. For every other log column
    # (population-family raw + E_*), min_val anchors at the TRUE minimum
    # (see `_get_minmax`'s own log branch and its comment for the full
    # real-data verification: a floor-raise was tried and
    # actively hurt these count-like columns, collapsing the vast
    # majority of real cells into one flattest color). `fixed_max` stays
    # omitted for non-fixed columns: max is fully dynamic (this
    # country/cycle's own real max).
    'PROBABILITY':             {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'POPULATION':              {'colors': ['#add8e6','#8cc5d3','#6bb2c0','#4a9bad','#33849a','#216d87','#165674','#0d3f51','#06283d','#011129'], 'scale': 'log'},
    'E_POPULATION':            {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'CHILDREN_TOTAL':          {'colors': ['#e8d5f5','#d0a8ed','#b87de5','#9e52dd','#8429d4','#6b1fb0','#53178c','#3c1068','#260844','#110022'], 'scale': 'log'},
    'E_CHILDREN_TOTAL':        {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'E_PEOPLE_IN_NEED':        {'colors': ['#fff7ec','#fee8c8','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#b30000','#7f0000'], 'scale': 'log'},
    'E_CHILDREN_IN_NEED':      {'colors': ['#fff7ec','#fee8c8','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#b30000','#7f0000'], 'scale': 'log'},
    'E_INFANT_IN_NEED':        {'colors': ['#fff7ec','#fee8c8','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#b30000','#7f0000'], 'scale': 'log'},
    'E_SCHOOL_AGE_IN_NEED':    {'colors': ['#fff7ec','#fee8c8','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#b30000','#7f0000'], 'scale': 'log'},
    'E_ADOLESCENT_IN_NEED':    {'colors': ['#fff7ec','#fee8c8','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#b30000','#7f0000'], 'scale': 'log'},
    'INFANT_POPULATION':       {'colors': ['#d6e8ff','#b3d9ff','#8ac8ff','#66b7ff','#42a6ff','#1e95ff','#1685e6','#0f75cc','#0765b3','#005599'], 'scale': 'log'},
    'E_INFANT_POPULATION':     {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'SCHOOL_AGE_POPULATION':   {'colors': ['#a8e6cf','#7ed3b8','#5ec0a1','#40ad8a','#2d9a73','#228759','#177440','#0f5127','#083310','#001107'], 'scale': 'log'},
    'E_SCHOOL_AGE_POPULATION': {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'ADOLESCENT_POPULATION':   {'colors': ['#cce0ff','#99c2ff','#66a3ff','#3385ff','#0066ff','#0052cc','#003d99','#002b66','#001a33','#000d1a'], 'scale': 'log'},
    'E_ADOLESCENT_POPULATION': {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'BUILT_SURFACE_M2':        {'colors': ['#f6e6d1','#e8d4b8','#dac29f','#ccb086','#be9e6d','#b08854','#a2723b','#945c22','#864609','#783000'], 'scale': 'log'},
    'E_BUILT_SURFACE_M2':      {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'CCI_CHILDREN':            {'colors': ['#ffcccb','#ff9999','#ff6666','#ff3333','#ff0000','#cc0000','#990000','#660000','#330000','#1a0000'], 'scale': 'log'},
    'E_CCI_CHILDREN':          {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'SMOD_CLASS':              {'colors': ['#dda0dd','#9370db','#4b0082'], 'scale': 'smod'},
    'RWI':                     {'colors': ['#d73027','#f46d43','#fdae61','#fee08b','#808080','#d9ef8b','#a6d96a','#66bd63','#1a9850'], 'scale': 'rwi'},
    'MODERATE_POVERTY_PROB':   {'colors': ['#fff4eb','#ffd8b3','#ffb960','#ff9b06','#e87b00','#d25a00','#b73800','#941600'], 'scale': 'linear', 'fixed_max': 1.0},
    'SEVERE_POVERTY_PROB':     {'colors': ['#ffebeb','#ffcdcd','#ffacac','#ff8585','#ff4f4f','#f70000','#c40000','#940000'], 'scale': 'linear', 'fixed_max': 1.0},
    'E_NUM_SHELTERS':          {'colors': ['#fde0dd','#fcc5c0','#fa9fb5','#f768a1','#dd3497','#ae017e','#7a0177','#49006a','#2d0040','#1a0026'], 'scale': 'log'},
    'E_NUM_WASH':              {'colors': ['#e5f5e0','#c7e9c0','#a1d99b','#74c476','#41ab5d','#238b45','#006d2c','#00441b','#002d12','#001a09'], 'scale': 'log'},
    'E_NUM_SCHOOLS':           {'colors': ['#fff7bc','#fee391','#fec44f','#fe9929','#ec7014','#cc4c02','#993404','#662506','#3d1604','#1a0900'], 'scale': 'log'},
    'E_NUM_HCS':               {'colors': ['#edf8fb','#ccece6','#99d8c9','#66c2a4','#41ae76','#238b45','#006d2c','#00441b','#002d12','#001a09'], 'scale': 'log'},
}


# Combined-exposure raster prop map: mirrors pages/map_shell_concept.py's
# own _EXPOSURE_E_PROP_MAP: every Exposure prop with a raw-count ×
# PROBABILITY relationship (each hazard's own per-threshold DataFrame
# already merges the SAME country-wide raw base column onto itself via
# _ensure_mercator_base_one, see ensure_mercator's own docstring, so no
# extra query is needed to fetch it). Deliberately does NOT cover "In
# Need" (E_*_IN_NEED). Those are vulnerability-weighted, not a simple
# raw*probability product, so there is no single formula to generalize to
# N simultaneous hazards; combining stays scoped to
# Probability/Classification/these props.
#
# The facility counts (E_NUM_SCHOOLS/E_NUM_HCS/E_NUM_SHELTERS/E_NUM_WASH)
# belong here for the same reason the population columns do: NUM_SCHOOLS
# etc. are plain raw counts on BASE_MERCATOR_TILE_MAT/BASE_ADMIN_GEOM_MAT,
# identical in shape to POPULATION, so raw × combined-probability is the
# same expected-impact quantity for them.
#
# This dict is intentionally WIDER than the client-side exposure lists it
# otherwise mirrors (maplibre_tiles.js's _AOTS_COMBINABLE_EXPOSURE_PROPS,
# map_shell_concept.py's _COMBINABLE_EXPOSURE_PROPS/_EXPOSURE_E_PROP_MAP),
# which cover only the six props the Exposure radio group can select. Those
# lists decide what the combined RASTER can be asked to paint: the raster
# colors one prop per request, so it can only ever be reached for a
# selectable prop, and this route validates `prop` against this dict's keys.
# The combined ADMIN tile has no such choice: its MVT carries every property
# at once for the hover tooltip, so every key here is computed from the union
# for it. Facilities are point layers rather than an Exposure radio option,
# so they reach the map only through that tooltip.
#
# Every consumer (_TileAdminMapCache.ensure's raw-column capture,
# _combine_bitmask_aware_admin's exp_acc, _fetch_admin_combined_tile's real
# vs. MAX-floor overwrite, and this route's own exposure-prop validation) is
# driven purely by these keys/values, so a new raw-count column is added here
# alone; making it SELECTABLE additionally needs the client-side lists above.
_COMBINED_EXPOSURE_RAW_COL: dict[str, str] = {
    "E_POPULATION": "POPULATION",
    "E_CHILDREN_TOTAL": "CHILDREN_TOTAL",
    "E_INFANT_POPULATION": "INFANT_POPULATION",
    "E_SCHOOL_AGE_POPULATION": "SCHOOL_AGE_POPULATION",
    "E_ADOLESCENT_POPULATION": "ADOLESCENT_POPULATION",
    "E_BUILT_SURFACE_M2": "BUILT_SURFACE_M2",
    "E_NUM_SCHOOLS": "NUM_SCHOOLS",
    "E_NUM_HCS": "NUM_HCS",
    "E_NUM_SHELTERS": "NUM_SHELTERS",
    "E_NUM_WASH": "NUM_WASH",
}

# The aggregate/Probabilistic map's combined probability is a per-tile
# union over the 51-member ensemble: `p(tile) = (count of members where
# ANY active hazard's bit is set at this tile) / 51`: a direct empirical
# fraction, no independence assumption, no copula. It uses the same
# per-member bitmask sources the "Compare Worst Case By" feature uses
# against TRACK_MAT (Wind/Gust: TILE_WIND_BITMASK_MAT/
# TILE_GUST_BITMASK_MAT; River/Rain: the same raw per-member caches,
# _RiverExtentCache/_PrecipRawCache, used for the raw preview layer and
# the per-member endpoint). Gust is treated as a near-deterministic
# function of wind at the same place/time rather than an independent
# event, and river/rain are correlated flood-family members: the union
# avoids double counting these correlated hazards the way an
# independence-assumption formula (`1-prod(1-Pi)`) would.
_BITMASK_ENSEMBLE_SIZE = 51

# river_forecast_date/rain_forecast_date, as populated into
# ms-tile-config-store (map_shell_concept.py's _build_hazard_tile_config)
# via get_latest_river_forecast_time/get_latest_rain_forecast_time, are
# MAT-format ("20260702000000"), the same convention wind_forecast_date
# uses, not RIVER_FORECASTS'/MET_FORECASTS' own raw plain-date/timestamp
# format. /tiles/raster-combined and /geojson/facilities-combined read
# these MAT-format values directly, so they must be converted via
# _mat_date_to_river_date/_mat_date_to_rain_date below before being used
# to query RIVER_FORECASTS/MET_FORECASTS, which are keyed on the raw
# format instead.
def _mat_date_to_river_date(mat_forecast_date: str) -> Optional[str]:
    """'20260702000000' (MAT format, what river_forecast_date/
    ms-tile-config-store's own value genuinely is for the raster/facility
    combined paths) -> '2026-07-02' (RIVER_FORECASTS' own real plain-date
    format, see get_river_extent_forecast_time_for_date's own docstring
    in snowflake_utils.py). River's raw data is keyed by calendar DATE
    only, not a full timestamp. Returns None (not a fabricated fallback)
    on a malformed input. Callers should treat that as "river doesn't
    resolve for this request", same as a real "no data" result.

    `mat_forecast_date=None` doesn't raise ValueError/TypeError from
    pd.to_datetime. It returns pd.NaT, which then raises AttributeError
    on the .strftime() call right after. Guarded explicitly rather than
    widening the except clause, so a genuine parse failure on a
    real-but-malformed string still surfaces as the same None-on-bad-input
    contract, not silently via a different exception class."""
    if not mat_forecast_date:
        return None
    try:
        return pd.to_datetime(mat_forecast_date, format="%Y%m%d%H%M%S").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _mat_date_to_rain_date(mat_forecast_date: str) -> Optional[str]:
    """'20260702000000' (MAT format) -> '2026-07-02 00:00:00' (MET_
    FORECASTS' own real full-timestamp format, see get_precip_forecast_
    time_near's own docstring). Same real conversion
    pages/map_shell_concept.py's own tracks/envelopes callback already
    uses. Returns None on a malformed input, same contract as the river
    version above (including the same real None-input guard)."""
    if not mat_forecast_date:
        return None
    try:
        return pd.to_datetime(mat_forecast_date, format="%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _raw_date_to_mat(raw_date: Optional[str]) -> Optional[str]:
    """Reverse of _mat_date_to_river_date/_mat_date_to_rain_date above:
    River/Rain's own real raw FORECAST_TIME string (whatever
    get_river_extent_forecast_time_for_date/get_precip_forecast_time_near
    actually return, e.g. '2026-07-14' for river's date-only cycles or
    '2026-07-14 06:00:00' for rain's real sub-daily cycle hours -- these
    are NOT the same shape, river has no time-of-day at all while rain
    genuinely does, so this must not assume a fixed '000000' suffix the
    way a naive string-append would) -> mat-format (YYYYMMDDHHMMSS), the
    format TILE_RIVER_BITMASK_MAT/TILE_PRECIP_BITMASK_MAT's own
    FORECAST_TIME column is keyed on (see get_river_tile_bitmask/
    get_rain_tile_bitmask's own docstrings).

    Needed specifically by combined_member_impacts below: unlike
    _combine_bitmask_aware/_combine_bitmask_aware_admin/
    _combine_bitmask_aware_points (which all receive an ALREADY-mat-format
    date via hazard_params/ms-tile-config-store, no conversion needed),
    this endpoint's own river_forecast_time/rain_forecast_time params come
    from a DIFFERENT resolver pair (get_river_extent_forecast_time_for_date/
    get_precip_forecast_time_near) that returns the raw, not mat-format,
    string -- a real, easy-to-miss format difference between otherwise
    near-identical call sites, caught in review before this endpoint's own
    river/rain branches were migrated onto the same MAT-backed getters.

    pd.to_datetime (no fixed `format=`, unlike the mat_date_to_* direction)
    on purpose: correctly parses BOTH real shapes above without needing to
    know in advance which hazard's convention it's seeing. Returns None
    (not a fabricated fallback) on a malformed/missing input, same
    "caller treats as no resolution" contract as the mat->raw direction."""
    if not raw_date:
        return None
    try:
        return pd.to_datetime(raw_date).strftime("%Y%m%d%H%M%S")
    except (ValueError, TypeError):
        return None


def _tile_id_bounds(tile_id: str) -> Optional[tuple[float, float, float, float]]:
    """(west, south, east, north) bounds derived directly from a z14
    quadkey string: no Snowflake/cache lookup needed. A per-tile bitmask
    union can mark a tile as hazard-covered even when that tile has no row
    at all in any hazard's own MAT DataFrame (`merged`, built from
    MERCATOR_TILE_*_MAT): a tile with zero probability from every member
    simply has no row there, the same sparse convention the bitmask
    tables themselves use. Without this, such a tile would be silently
    absent from `merged` and never painted despite being hazard-affected.
    Mirrors the `mercantile.quadkey_to_tile` + `mercantile.bounds` pattern
    already used elsewhere in this file (e.g. the admin-tile decode
    path)."""
    try:
        t = mercantile.quadkey_to_tile(tile_id)
        b = mercantile.bounds(t)
        return b.west, b.south, b.east, b.north
    except Exception:
        return None


def _popcount51(bits_arr: np.ndarray) -> np.ndarray:
    """Vectorized popcount over the 51-member bit range: no per-row
    Python loop. Module-level so every caller that needs a member-fraction
    from a raw uint64 bitmask array (the union itself, a single hazard's
    own bits, or a family combination like `tc_bits & flood_bits`) shares
    one implementation instead of each re-deriving its own bit-counting
    loop; same class of vectorization `_RiverExtentCache`'s own bitmask
    reduction uses elsewhere in this file."""
    pc = np.zeros(len(bits_arr), dtype=np.float64)
    for m in range(_BITMASK_ENSEMBLE_SIZE):
        pc += ((bits_arr >> np.uint64(m)) & np.uint64(1)).astype(np.float64)
    return pc


def _combine_bitmask_aware(merged: pd.DataFrame, used_hazard_names: list[str],
                             country: str, storm: str, hazard_params: dict[str, dict],
                             tile_mask: Optional[Callable[[pd.DataFrame], pd.Series]] = None
                             ) -> tuple[np.ndarray, pd.DataFrame, dict[str, np.ndarray]]:
    """Per-tile union over the 51-member ensemble: see the module-level
    comment above this function for the combination approach.

    Returns `(p_combined, merged, hazard_bits)`:
    - `p_combined` aligned 1:1 with `merged`'s own row order: the union
      across every hazard in `hazard_bits`.
    - `merged` itself may come back with MORE rows than it went in with: a
      tile that only a bitmask (not any hazard's own MAT DataFrame) knows
      about gets appended with bounds derived via _tile_id_bounds and
      PROBABILITY_* columns of 0.0 for every hazard that already had MAT
      rows for other tiles (a genuine 0, not a placeholder: that hazard's
      own MAT data has no signal there; only the bitmask union does).
      Callers must re-derive their own ws/ss/es/ns/bounds_valid arrays from
      the RETURNED `merged`, not the one they passed in.
    - `hazard_bits`: `{hazard_name: per-tile uint64 array}`, aligned to the
      returned `merged` (same possibly-longer length as `p_combined`), for
      every hazard in `used_hazard_names`, including hazards that end up
      contributing nothing (an all-zero array, not a missing key, so
      callers can always safely `hazard_bits.get(name, zeros)` without a
      special case). This lets callers compute per-member family splits
      (TC = wind|gust, Flood = river|rain, `both_frac =
      popcount(tc & flood)/51`, a true joint-occurrence check, not two
      marginal `>0` checks ANDed together) or a per-hazard classification
      hit-count, instead of re-deriving their own bitmask-decoding logic.

    `hazard_params[name]` carries whatever this specific hazard needs to
    resolve its own bitmask, built by the caller (_fetch_combined_
    raster_tile) from the exact same `active` list it already uses for
    ensure_mercator, so there is no separate parameter-resolution path to
    keep in sync.

    `tile_mask`: the caller's own wind/gust/river bitmask DataFrames are
    COUNTRY-WIDE (wind/gust: every z14 tile for this country/storm/
    threshold) or GLOBAL (river's own sparse world-wide extent table),
    never pre-scoped to this one 512x512 display tile the way `merged`
    already is. Filtering by `tile_mask` first keeps "whatever's left in
    `lookup`" below scoped to a genuine coverage gap for THIS tile rather
    than every tile in the whole country/world this hazard covers, which
    would flood `extra_tiles` with irrelevant entries per request (each
    paying a `_tile_id_bounds`/mercantile call) and paint spurious
    off-tile pixels onto this tile's edges (the rasterizer clamps
    out-of-bounds cells into column/row 0 rather than discarding them).
    Pass the same `_tile_mask` closure `_fetch_combined_raster_tile`
    already builds from `_quadkey_like_pattern` for its own hazard-
    DataFrame filtering (`sub = df.loc[_tile_mask(df), ...]`), reusing it
    here (rather than re-deriving a bare prefix string) keeps the
    z==MAT_ZOOM_LEVEL/z>MAT_ZOOM_LEVEL exact-match case correct too, not
    just the z<MAT_ZOOM_LEVEL prefix case. Left optional (default None =
    no filtering) only because `_combine_bitmask_aware_points` doesn't
    share this display-tile concept (facility points aren't scoped to one
    z14 tile at all). No caller of this function should omit it in
    practice.
    """
    from components.data.snowflake_utils import (
        get_wind_tile_bitmask, get_gust_tile_bitmask,
        get_river_tile_bitmask, get_rain_tile_bitmask,
    )

    # `country` can be a "PHL+VNM"-shaped multi-country selection (see
    # _quadkey_like_pattern's own siblings, every other country-scoped
    # query in this file splits on '+' before hitting Snowflake, and
    # _combine_bitmask_aware_points' own `codes` param below does the
    # same). get_wind_tile_bitmask/get_gust_tile_bitmask must be called
    # per country code, not with the raw unsplit string, since
    # `WHERE COUNTRY = 'PHL+VNM'` matches no rows.
    codes = [c.upper() for c in country.split('+') if c.strip()]

    all_tile_ids = merged['TILE_ID'].to_numpy()
    n_tiles = len(all_tile_ids)
    # Tracked per hazard (not one flat `union_bits` array), see this
    # function's own docstring for why. The union itself is just
    # `reduce(or, hazard_bits.values())` at the very end.
    hazard_bits: dict[str, np.ndarray] = {hz: np.zeros(n_tiles, dtype=np.uint64) for hz in used_hazard_names}
    extra_tiles_by_hazard: dict[str, dict[str, np.uint64]] = {hz: {} for hz in used_hazard_names}

    def _apply_bits_df(bits_df: Optional[pd.DataFrame], hz: str):
        """Merges a TILE_ID/BITS DataFrame (wind/gust/river's own shape)
        into `hazard_bits[hz]` for tiles already in `merged`, and into
        `extra_tiles_by_hazard[hz]` for any tile the bitmask knows about
        that `merged` doesn't: same coverage-gap handling this function's
        own docstring describes. Filtered to `tile_mask` first (see this
        function's own docstring) so a country-wide/global bitmask
        DataFrame can't flood extra_tiles with off-tile rows."""
        target_bits = hazard_bits[hz]
        extra_tiles = extra_tiles_by_hazard[hz]
        if bits_df is None or bits_df.empty or 'TILE_ID' not in bits_df.columns:
            return
        if tile_mask is not None:
            bits_df = bits_df[tile_mask(bits_df)]
            if bits_df.empty:
                return
        bits_df = bits_df.drop_duplicates(subset='TILE_ID', keep='first')
        lookup = dict(zip(bits_df['TILE_ID'], pd.to_numeric(bits_df['BITS'], errors='coerce').fillna(0).astype('uint64')))
        for i, tid in enumerate(all_tile_ids):
            b = lookup.pop(tid, None)
            if b is not None and b:
                target_bits[i] |= np.uint64(b)
        # Whatever's left in `lookup` are real tiles this hazard covers
        # (already scoped to this display tile above, when tile_mask was
        # given) that no MAT DataFrame carries a row for at all.
        for tid, b in lookup.items():
            if not b:
                continue
            extra_tiles[tid] = extra_tiles.get(tid, np.uint64(0)) | np.uint64(b)

    for hz in used_hazard_names:
        p = hazard_params.get(hz)
        if p is None:
            continue
        if hz == 'wind':
            for code in codes:
                _apply_bits_df(get_wind_tile_bitmask(code, storm, p['forecast_date'], p['wind_threshold']), hz)
        elif hz == 'gust':
            if p.get('gust_threshold') is not None:
                for code in codes:
                    _apply_bits_df(get_gust_tile_bitmask(code, storm, p['forecast_date'], p['gust_threshold']), hz)
        elif hz == 'river':
            # Real, pre-materialized per-tile-per-member bitmask
            # (TILE_RIVER_BITMASK_MAT), same shape/query pattern as
            # wind/gust above, REPLACES the live _river_extent_cache decode
            # of the raw global GloFAS extent_rp{N}_bymember Parquet (a
            # real, measured multi-minute cold-cache cost, see
            # get_river_tile_bitmask's own docstring for the full "why").
            # `p['forecast_date']` is already mat-format (YYYYMMDDHHMMSS),
            # matching TILE_RIVER_BITMASK_MAT's own FORECAST_TIME column
            # directly -- no _mat_date_to_river_date() conversion needed
            # here (that conversion existed only for _river_extent_cache's
            # own raw-format requirement, which this table doesn't have).
            river_date = p.get('forecast_date')
            if river_date:
                rp_tier = p.get('rp_tier') or _RIVER_EXTENT_DEFAULT_RP_TIER
                step_h = p.get('window_h') or _RIVER_WINDOW_DEFAULT
                for code in codes:
                    _apply_bits_df(get_river_tile_bitmask(code, river_date, rp_tier, step_h), hz)
        elif hz == 'rain':
            # Real, pre-materialized per-tile-per-member bitmask
            # (TILE_PRECIP_BITMASK_MAT), REPLACES the live inline
            # dense-grid-centroid-sampling this branch used to do (the
            # rate_grid/exceeds/member_bits construction that used to live
            # in a SEPARATE pass below, after the extra_tiles merge -- see
            # get_rain_tile_bitmask's own docstring for the full "why").
            # Rain now uses the exact same sparse TILE_ID/BITS shape
            # wind/gust/river already use, so it no longer needs that
            # special second pass or its own extra_tiles carve-out: a real
            # sparse per-tile table can contribute extra_tiles rows the
            # same way the other three hazards already do, handled by the
            # SAME _apply_bits_df call right here, in the SAME loop.
            rain_date = p.get('forecast_date')
            if rain_date:
                window_h = p.get('window_h') or _PRECIP_RATE_DEFAULT_WINDOW_H
                # NOT _PRECIP_PROB_THRESHOLD_MM as a silent fallback here:
                # that constant is a real, meaningful default for the raw
                # continuous-threshold Zarr query paths elsewhere in this
                # file, but TILE_PRECIP_BITMASK_MAT only ever has rows at
                # DATAPIPELINE's own fixed PRECIP_TP_THRESHOLDS_MM set
                # (25/35/45/50/70/75/90/100/103/133/150), which never
                # includes 10.0 -- silently querying WHERE THRESHOLD_MM =
                # 10.0 would return a real, legitimate-looking EMPTY
                # result (not an error), so Rain would render as "no
                # hazard" with no visible failure anywhere. Treat a
                # missing threshold_mm as "Rain not resolvable" instead,
                # same as a missing forecast_date, rather than querying a
                # value that can never match a real row (caught in review).
                if p.get('threshold_mm') is None:
                    continue
                threshold_mm = p['threshold_mm']
                for code in codes:
                    _apply_bits_df(get_rain_tile_bitmask(code, rain_date, threshold_mm, window_h), hz)

    all_extra_tile_ids = set()
    for extra in extra_tiles_by_hazard.values():
        all_extra_tile_ids.update(extra.keys())
    if all_extra_tile_ids:
        new_rows = []
        new_tile_ids = []
        for tid in all_extra_tile_ids:
            b = _tile_id_bounds(tid)
            if b is None:
                continue
            w, s, e, n = b
            row = {'TILE_ID': tid, 'BW': w, 'BS': s, 'BE': e, 'BN': n}
            for hz in used_hazard_names:
                row[f'PROBABILITY_{hz}'] = 0.0
            new_rows.append(row)
            new_tile_ids.append(tid)
        if new_rows:
            extra_df = pd.DataFrame(new_rows)
            merged = pd.concat([merged, extra_df], ignore_index=True, sort=False)
            for hz in used_hazard_names:
                extra_bits_arr = np.array(
                    [extra_tiles_by_hazard[hz].get(tid, np.uint64(0)) for tid in new_tile_ids], dtype=np.uint64)
                hazard_bits[hz] = np.concatenate([hazard_bits[hz], extra_bits_arr])

    # Rain used to be sampled in a separate pass here, after the
    # extra_tiles merge above (a dense-grid-centroid sample couldn't itself
    # discover new tiles the way a sparse TILE_ID table could, so it had to
    # run after other hazards' own gaps were already known). Now that Rain
    # reads from a real sparse TILE_ID/BITS table (TILE_PRECIP_BITMASK_MAT,
    # see the `elif hz == 'rain':` branch in the main loop above), it's
    # merged in that SAME loop/pass as wind/gust/river, so this second pass
    # is no longer needed.

    n_final = len(merged)
    union_bits = np.zeros(n_final, dtype=np.uint64)
    for arr in hazard_bits.values():
        union_bits |= arr

    p_combined = _popcount51(union_bits) / float(_BITMASK_ENSEMBLE_SIZE)
    return p_combined, merged, hazard_bits


# ---------------------------------------------------------------------------
# z14 tile -> admin-region spatial mapping (combined-hazard ADMIN layer only)
# ---------------------------------------------------------------------------
# This module joins the z14 tile grid to admin regions. Wind/Gust's own
# admin-level PROBABILITY comes from a separate, already-Snowflake-
# pre-aggregated column (_ADMIN_*_IMPACT_ONLY_SQL against ADMIN_*_MAT), not
# from a tile-to-admin join.
#
# Every hazard's per-member data exists only at z14-tile granularity:
# Wind/Gust in TILE_WIND_BITMASK_MAT/TILE_GUST_BITMASK_MAT, River/Rain
# resolved on demand from _RiverExtentCache/_PrecipRawCache, and a
# cross-hazard union is only exact where those bits live. So the combined
# ADMIN layer unions per z14 tile and then aggregates down to regions,
# which needs this mapping (tile -> region, plus each tile's raw exposure
# weights and each region's tile count).
#
# A per-REGION bitmask (bit m set iff member m touched the region anywhere)
# is deliberately NOT used here: popcount/51 of such a mask is not the
# admin layer's own probability, which upstream is the AREA-MEAN of z14
# tile probabilities, so the "combined" number could exceed every one of
# its own marginals. See _combine_bitmask_aware_admin's docstring for the
# full derivation.
_TILE_ADMIN_MAP_CACHE_MAX = 8


class _TileAdminMapCache:
    """Real {z14 tile_id -> admin-region ucode} mapping for one
    (country_code, admin_level), plus the parallel numpy arrays a
    vectorized OR-reduction needs.

    Built ENTIRELY from data this process already has resident, no new
    Snowflake query of any kind:
      - z14 tile ids + their bounds come from _DataCache's own
        _ensure_mercator_base_one (BASE_MERCATOR_TILE_MAT + the
        mercantile-derived BW/BS/BE/BN that _precompute_mercator_bounds
        already materialized at base-load time; a tile centroid is just
        the midpoint of those, so re-calling mercantile per tile here
        would recompute a number that is already in memory).
      - admin polygons + their own STRtree come from _DataCache's own
        _ensure_admin_base_one (already parsed once via shapely `shape()`
        and indexed there; re-parsing 10^2-10^4 polygons per country
        would be pure waste).
    A tile is assigned to the admin region whose polygon contains its
    CENTROID (single bulk, C-level `STRtree.query(points, predicate=
    'intersects')`, not a per-tile Python loop).

    Scope note: a tile whose centroid falls EXACTLY on a
    shared admin boundary can legitimately match two neighbouring
    regions; the first match wins (stable, deterministic, see the
    argsort below), so such a tile contributes its members' bits to one
    of the two rather than both. Centroid-in-polygon also means a tile
    straddling a boundary counts wholly toward whichever side its centre
    falls on. Both are real-world negligible at z14 (~2.4km cells against
    admin-1/admin-2 regions spanning tens to hundreds of km) and a full
    polygon-overlap/area-weighted computation would cost orders of
    magnitude more for no decision-relevant difference, documented here
    rather than silently ignored.

    Cache shape mirrors _DataCache's own established pattern exactly:
    per-key lock (never one instance-wide lock), TTL-bounded via the same
    long _BASE_DATA_TTL the two base caches it derives from already use
    (this mapping is threshold/storm/hazard-INDEPENDENT). It only depends
    on country geometry, which changes only when a pipeline re-publishes
    base layers), and a hard entry cap with oldest-first eviction.
    """

    def __init__(self) -> None:
        self._maps: dict[tuple, dict] = {}
        self._loaded_at: dict[tuple, float] = {}
        self._key_locks: dict[tuple, threading.Lock] = {}
        self._key_locks_meta_lock = threading.Lock()

    def _lock_for(self, key: tuple) -> threading.Lock:
        with self._key_locks_meta_lock:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    def _is_fresh(self, key: tuple) -> bool:
        return key in self._loaded_at and (time.time() - self._loaded_at[key]) < _BASE_DATA_TTL

    def _evict_oldest_if_over(self, just_written_key: tuple) -> None:
        if len(self._maps) <= _TILE_ADMIN_MAP_CACHE_MAX:
            return
        candidates = [k for k in self._maps if k in self._loaded_at]
        if not candidates:
            return
        oldest = min(candidates, key=lambda k: self._loaded_at[k])
        if oldest == just_written_key:
            return
        self._maps.pop(oldest, None)
        self._loaded_at.pop(oldest, None)
        with self._key_locks_meta_lock:
            self._key_locks.pop(oldest, None)

    def ensure(self, code: str, admin_level: int) -> dict:
        """Returns the mapping entry for ONE country code:

            {
              'admin_ids':  np.ndarray[object]  # canonical admin-region ucode order
              'tile_ids':   np.ndarray[object]  # every z14 tile that landed in SOME region
              'tile_index': pd.Index            # hash view of 'tile_ids' (built once, not per request)
              'tile_admin': np.ndarray[int64]   # index into 'admin_ids', aligned to 'tile_ids'
              'clat'/'clon': np.ndarray[float64] # that tile's own centroid (aligned too)
              'tile_to_admin': dict[str, str]   # the plain {tile_id: admin ucode} view
              'n_tiles':    np.ndarray[int64]   # z14 tiles per region, aligned to 'admin_ids'
              'raw':        dict[str, np.ndarray[float64]]  # per-tile raw exposure, aligned to 'tile_ids'
            }

        'n_tiles' and 'raw' exist because the admin layer's own
        PROBABILITY is defined upstream as the AREA-MEAN of z14 tile
        probabilities (DATAPIPELINE's create_admin_view_from_envelopes_new
        lists 'probability' in its avg_cols and aggregates it with
        "mean"), and its E_* columns as `sum_over_tiles(raw_tile x
        prob_tile)`. Reproducing either of those for a cross-hazard union
        needs the per-region tile COUNT (the mean's denominator) and the
        per-tile raw counts (the expected-value weights): a per-region
        "does any member touch this polygon" answer cannot produce
        either, and would overstate both by 1-2 orders of magnitude for a
        large region a storm only clips. Both are threshold/storm/
        hazard-independent (pure country geometry + base population), so
        they belong in this cache, computed once, rather than being
        re-derived per request.

        Every array is empty (not None) when the country has no base
        mercator tiles, no admin geometry, or IMPACT_DATA_STORE is not
        SNOWFLAKE (the two base caches return empty placeholders there,
        see their own docstrings). Callers get a real, honest "no
        mapping" instead of an exception.
        """
        key = (code.upper(), admin_level)
        if self._is_fresh(key) and key in self._maps:
            return self._maps[key]
        with self._lock_for(key):
            if self._is_fresh(key) and key in self._maps:
                return self._maps[key]
            entry = self._build(code.upper(), admin_level)
            self._maps[key] = entry
            self._loaded_at[key] = time.time()
            self._evict_oldest_if_over(key)
            log.info("  Cache: %d z=14 tiles mapped to %d admin regions (country=%s L%s)",
                     len(entry['tile_ids']), len(entry['admin_ids']), code, admin_level)
            return entry

    @staticmethod
    def _empty_entry() -> dict:
        return {
            'admin_ids': np.array([], dtype=object),
            'tile_ids': np.array([], dtype=object),
            'tile_index': pd.Index([], dtype=object),
            'tile_admin': np.array([], dtype=np.int64),
            'clat': np.array([], dtype=np.float64),
            'clon': np.array([], dtype=np.float64),
            'tile_to_admin': {},
            'n_tiles': np.array([], dtype=np.int64),
            'raw': {},
        }

    def _build(self, code: str, admin_level: int) -> dict:
        import shapely  # shapely>=2.0 top-level vectorized constructors

        base_df = _cache._ensure_mercator_base_one(code)
        _admin_df, geoms, tree, admin_id_order = _cache._ensure_admin_base_one(code, admin_level)
        if base_df is None or base_df.empty or not geoms or tree is None:
            return self._empty_entry()
        if 'BW' not in base_df.columns:
            return self._empty_entry()

        tile_ids = base_df['TILE_ID'].to_numpy(dtype=object)
        bw = base_df['BW'].to_numpy(dtype=np.float64)
        bs = base_df['BS'].to_numpy(dtype=np.float64)
        be = base_df['BE'].to_numpy(dtype=np.float64)
        bn = base_df['BN'].to_numpy(dtype=np.float64)
        clon = (bw + be) / 2.0
        clat = (bs + bn) / 2.0
        # Rows whose quadkey failed to parse at base-load time carry NaN
        # bounds (_precompute_mercator_bounds' own except branch). Drop
        # them rather than feeding NaN coordinates into the spatial index.
        ok = np.isfinite(clon) & np.isfinite(clat)
        # A duplicate z14 TILE_ID would make the resulting 'tile_index'
        # non-unique, and pd.Index.get_indexer (how the river branch looks
        # tiles up) raises InvalidIndexError on that. Base tables shouldn't
        # contain duplicates, but a defensive keep-first is far cheaper
        # than a 500 on a tile request if one ever appears.
        _seen: set = set()
        uniq = np.fromiter(((t not in _seen) and not _seen.add(t) for t in tile_ids),
                           dtype=bool, count=len(tile_ids))
        ok = ok & uniq
        if not ok.any():
            return self._empty_entry()
        tile_ids, clon, clat = tile_ids[ok], clon[ok], clat[ok]
        # Per-tile raw exposure counts, carried alongside the geometry for
        # the reason this class's own `ensure` docstring gives. Filtered by
        # the SAME `ok` mask so every array below stays index-aligned; a
        # column missing from BASE_MERCATOR_TILE_MAT is simply absent from
        # the dict (callers skip it) rather than being faked as zeros.
        raw_all: dict[str, np.ndarray] = {}
        for raw_col in dict.fromkeys(_COMBINED_EXPOSURE_RAW_COL.values()):
            if raw_col in base_df.columns:
                raw_all[raw_col] = pd.to_numeric(
                    base_df[raw_col], errors='coerce').to_numpy(dtype=np.float64)[ok]

        pts = shapely.points(clon, clat)
        # (2, M): row 0 = index into `pts`, row 1 = index into `geoms`
        # (which lines up positionally with admin_id_order, see
        # _ensure_admin_base_one's own docstring on that invariant).
        hits = tree.query(pts, predicate='intersects')
        if hits.size == 0:
            return self._empty_entry()
        in_idx = np.asarray(hits[0], dtype=np.int64)
        tree_idx = np.asarray(hits[1], dtype=np.int64)
        # First match wins per tile, see the boundary edge case in this
        # class's own docstring. Stable sort makes "first" mean the
        # lowest admin index deterministically, rather than depending on
        # whatever order the index happens to emit pairs in.
        order = np.lexsort((tree_idx, in_idx))
        in_idx, tree_idx = in_idx[order], tree_idx[order]
        first = np.concatenate(([True], in_idx[1:] != in_idx[:-1])) if len(in_idx) > 1 else np.array([True])
        in_idx, tree_idx = in_idx[first], tree_idx[first]

        admin_ids = np.array(admin_id_order, dtype=object)
        matched_tiles = tile_ids[in_idx]
        # Denominator of the region's own AREA-MEAN probability: how many
        # z14 tiles landed in each region. Regions with no tile at all keep
        # a real 0 here (minlength) and are skipped by the consumer rather
        # than dividing by zero.
        n_tiles = np.bincount(tree_idx, minlength=len(admin_ids)).astype(np.int64)
        return {
            'admin_ids': admin_ids,
            'tile_ids': matched_tiles,
            # Built ONCE here, not per request: the river branch below
            # looks a GLOBAL flood-extent table up against this, and
            # rebuilding the hash table on every tile request would pay
            # the country's whole tile count again each time.
            'tile_index': pd.Index(matched_tiles),
            'tile_admin': tree_idx,
            'clat': clat[in_idx],
            'clon': clon[in_idx],
            'tile_to_admin': dict(zip(matched_tiles, admin_ids[tree_idx])),
            'n_tiles': n_tiles,
            'raw': {c: a[in_idx] for c, a in raw_all.items()},
        }


_tile_admin_map_cache = _TileAdminMapCache()


def _combine_bitmask_aware_admin(admin_ids: list[str], used_hazard_names: list[str],
                                   country: str, storm: str, hazard_params: dict[str, dict],
                                   admin_level: int
                                   ) -> tuple[dict[str, float], dict[str, dict[str, float]], list[str]]:
    """Per-ADMIN-REGION cross-hazard combination over the 51-member
    ensemble: the admin-granularity analogue of _combine_bitmask_aware
    (see that function's own module-level comment for the combination
    approach used throughout this codebase).

    Returns `(p_by_admin, expected_by_admin, resolved_hazards)`:
    - `p_by_admin`: {admin ucode -> real combined probability}, defined
      below. Regions with no z14 tile resident are simply absent (the
      caller then keeps that region's own per-hazard values rather than
      stamping a fabricated 0).
    - `expected_by_admin`: {admin ucode -> {E_COL -> expected count}} for
      every `_COMBINED_EXPOSURE_RAW_COL` field, computed the SAME way
      the pipeline computes each hazard's own admin E_* columns.
    - `resolved_hazards`: the subset of `used_hazard_names` whose real
      per-member source actually answered. EMPTY means nothing resolved
      and the two dicts above carry no information: the caller MUST fall
      back rather than render them (see _fetch_admin_combined_tile).

    ── What "the combined probability of a region" actually is ──────────
    A per-region SPATIAL-ANY union (bit `m` set iff member `m` reached any
    point of the region, then popcount/51) is a genuine quantity, but it
    is NOT the quantity the admin layer renders for a single hazard.
    Upstream (DATAPIPELINE create_admin_view_from_envelopes_new) an admin
    region's PROBABILITY is the AREA-MEAN of its z14 tiles' own
    probabilities ('probability' sits in that function's avg_cols and is
    aggregated with "mean"), and its E_POPULATION is
    `sum_over_tiles(pop_tile x prob_tile)`. A spatial-ANY union answers a
    different question and can be 1-2 orders of magnitude larger for a
    big region a storm only clips: the "combined" figure could then read
    HIGHER than every one of its own marginals, with correspondingly
    inflated E_* values.

    So the union is taken where it is exact and cheap (per z14 tile,
    exactly as _combine_bitmask_aware already does for the raster) and
    only THEN aggregated to the region with the same two aggregations the
    pipeline itself uses:

        p_admin(A)  = mean over z14 tiles t in A of  popcount(union_bits_t)/51
        E_col(A)    = sum  over z14 tiles t in A of  raw_col_t x popcount(union_bits_t)/51

    Both are therefore directly comparable with ADMIN_ALL_*_MAT's own
    PROBABILITY/E_* for a single hazard, and both are guaranteed >= every
    active hazard's own marginal (union_bits_t is a superset of each
    hazard's bits_t at every tile), which is what a union must satisfy.

    ── Where each hazard's per-tile bits come from ──────────────────────
    - 'wind'/'gust': TILE_WIND_BITMASK_MAT / TILE_GUST_BITMASK_MAT via
      get_wind_tile_bitmask/get_gust_tile_bitmask: the REAL, DEPLOYED,
      live z14 bitmask tables (the same ones the raster path already
      reads). Deliberately NOT the admin-granularity ADMIN_*_BITMASK_MAT
      draft: those encode the spatial-ANY quantity described above, which
      cannot reproduce an area-mean, so they are not used here (and are no
      longer produced upstream).
    - 'river'/'rain': the SAME on-demand per-member decode
      _combine_bitmask_aware already does against _river_extent_cache /
      _precip_cache, kept at z14 granularity.

    The z14 tile -> region assignment (and each tile's raw population /
    built-surface weights, and each region's tile count) comes from
    _tile_admin_map_cache, which is built purely from data already
    resident in this process.

    GRACEFUL DEGRADATION: a hazard whose source raises (or whose table has
    no rows for this key) contributes an all-zero bit array and is left
    out of `resolved_hazards`. Nothing raises and nothing is fabricated,
    but (unlike before) the caller can now TELL the difference between
    "the union really is 0 here" and "no hazard resolved at all", which is
    the whole point: silently stamping 0.0 over real per-hazard values
    reads as "no hazard here" rather than "this could not be computed".

    `country` may be a multi-country selection ("PHL+VNM"): split on '+'
    before any Snowflake call, same as _combine_bitmask_aware's own
    `codes` line (an unsplit string matches zero COUNTRY rows and
    silently contributes nothing).

    There is no correct per-REGION bitmask to return (that would be the
    spatial-ANY quantity described above, which this function does not
    compute), so only `p_by_admin`/`expected_by_admin`/`resolved_hazards`
    are returned.
    """
    from components.data.snowflake_utils import (
        get_wind_tile_bitmask, get_gust_tile_bitmask,
        get_river_tile_bitmask, get_rain_tile_bitmask,
    )

    codes = [c.upper() for c in country.split('+') if c.strip()]
    # Defensive dedupe: pd.Index.get_indexer (used for every lookup below)
    # raises InvalidIndexError on a non-unique index, and a duplicate ucode
    # is a real possibility for a multi-country selection whose countries
    # share a border region, or from a stale BASE_ADMIN_GEOM_MAT row. Order
    # is preserved so the returned dicts stay stable across calls.
    seen: set = set()
    admin_ids = [a for a in admin_ids if not (a in seen or seen.add(a))]
    n = len(admin_ids)
    if n == 0:
        return {}, {}, []

    admin_index = pd.Index(admin_ids)
    sum_p = np.zeros(n, dtype=np.float64)
    denom = np.zeros(n, dtype=np.float64)
    exp_acc: dict[str, np.ndarray] = {e: np.zeros(n, dtype=np.float64)
                                      for e in _COMBINED_EXPOSURE_RAW_COL}
    resolved: set[str] = set()
    # Which E_* columns actually had a real per-tile raw value, tracked PER
    # REGION. A column BASE_MERCATOR_TILE_MAT does not carry, and a region
    # whose tiles all hold NULL for a column it does carry, which is the
    # normal shape for the facility counts in a country with no shelter or
    # WASH inventory: must be absent from the result, never a computed-
    # looking 0.0 the caller would compare against (and possibly prefer
    # over) the pipeline's own real value. This matches how the pipeline
    # itself aggregates those columns: an all-NULL group stays NULL rather
    # than summing to zero.
    have_exp: dict[str, np.ndarray] = {e: np.zeros(n, dtype=bool)
                                       for e in _COMBINED_EXPOSURE_RAW_COL}
    mapped_any = False

    def _scatter_tile_bits(bits_df: Optional[pd.DataFrame], target: np.ndarray,
                           tile_index: pd.Index) -> None:
        """OR a real z14-granularity TILE_ID/BITS DataFrame into `target`
        (aligned to `tile_index`). Rows for tiles outside this country (
        River's extent table is worldwide) simply have no entry in the
        index and are dropped by the same lookup that does the scatter, so
        no separate bbox pre-filter is needed."""
        if bits_df is None or bits_df.empty or 'TILE_ID' not in bits_df.columns:
            return
        bits_df = bits_df.drop_duplicates(subset='TILE_ID', keep='first')
        pos = tile_index.get_indexer(bits_df['TILE_ID'].to_numpy(dtype=object))
        vals = pd.to_numeric(bits_df['BITS'], errors='coerce').fillna(0).to_numpy(dtype='uint64')
        keep = pos >= 0
        if not keep.any():
            return
        np.bitwise_or.at(target, pos[keep], vals[keep])

    # River/Rain now resolve PER COUNTRY CODE, inside the loop below, the
    # same way Wind/Gust already do (get_river_tile_bitmask/
    # get_rain_tile_bitmask query TILE_RIVER_BITMASK_MAT/
    # TILE_PRECIP_BITMASK_MAT, which are COUNTRY-keyed, unlike the OLD
    # live _river_extent_cache/_precip_cache this replaced, which were
    # genuinely global/country-independent caches worth resolving once
    # outside the loop). `p.get('forecast_date')` is already mat-format
    # (same hazard_params convention every hazard here shares), no
    # _mat_date_to_river_date()/_mat_date_to_rain_date() conversion needed,
    # same reasoning as _combine_bitmask_aware's own river/rain branches.

    for code in codes:
        entry = _tile_admin_map_cache.ensure(code, admin_level)
        n_t = len(entry['tile_ids'])
        if n_t == 0:
            continue
        mapped_any = True
        tile_index = entry['tile_index']
        union_bits = np.zeros(n_t, dtype=np.uint64)

        for hz in used_hazard_names:
            p = hazard_params.get(hz)
            if p is None:
                continue
            hz_bits = np.zeros(n_t, dtype=np.uint64)
            if hz == 'wind':
                if not p.get('forecast_date'):
                    continue
                df = get_wind_tile_bitmask(code, storm, p['forecast_date'], p['wind_threshold'])
                if df is None:
                    # A REAL failure (query raised): distinct from a
                    # legitimately-empty result, see get_wind_tile_bitmask's
                    # own None-vs-empty contract. Not marked resolved.
                    log.warning("admin-combined: wind tile bitmask unavailable for %s/%s", code, storm)
                    continue
                resolved.add('wind')
                _scatter_tile_bits(df, hz_bits, tile_index)
            elif hz == 'gust':
                if not p.get('forecast_date') or p.get('gust_threshold') is None:
                    continue
                df = get_gust_tile_bitmask(code, storm, p['forecast_date'], p['gust_threshold'])
                if df is None:
                    log.warning("admin-combined: gust tile bitmask unavailable for %s/%s", code, storm)
                    continue
                resolved.add('gust')
                _scatter_tile_bits(df, hz_bits, tile_index)
            elif hz == 'river':
                if not p.get('forecast_date'):
                    continue
                rp_tier = p.get('rp_tier') or _RIVER_EXTENT_DEFAULT_RP_TIER
                step_h = p.get('window_h') or _RIVER_WINDOW_DEFAULT
                df = get_river_tile_bitmask(code, p['forecast_date'], rp_tier, step_h)
                if df is None:
                    log.warning("admin-combined: river tile bitmask unavailable for %s", code)
                    continue
                resolved.add('river')
                _scatter_tile_bits(df, hz_bits, tile_index)
            elif hz == 'rain':
                # threshold_mm must be a real one, not a silent
                # _PRECIP_PROB_THRESHOLD_MM fallback: TILE_PRECIP_
                # BITMASK_MAT only ever has rows at DATAPIPELINE's own
                # fixed threshold set, which never includes that
                # constant's value, see get_rain_tile_bitmask's own call
                # site in _combine_bitmask_aware for the full "why".
                if not p.get('forecast_date') or p.get('threshold_mm') is None:
                    continue
                window_h = p.get('window_h') or _PRECIP_RATE_DEFAULT_WINDOW_H
                threshold_mm = p['threshold_mm']
                df = get_rain_tile_bitmask(code, p['forecast_date'], threshold_mm, window_h)
                if df is None:
                    log.warning("admin-combined: rain tile bitmask unavailable for %s", code)
                    continue
                resolved.add('rain')
                _scatter_tile_bits(df, hz_bits, tile_index)
            else:
                continue
            union_bits |= hz_bits

        # --- z14 union -> region, using the pipeline's own two aggregations
        p_tile = _popcount51(union_bits) / float(_BITMASK_ENSEMBLE_SIZE)
        local_to_dest = admin_index.get_indexer(entry['admin_ids'])
        dest_all = local_to_dest[entry['tile_admin']]
        ok = dest_all >= 0
        if not ok.any():
            continue
        dest = dest_all[ok]
        np.add.at(sum_p, dest, p_tile[ok])
        # Denominator of the AREA-MEAN: EVERY z14 tile in the region, not
        # only the ones some member reached: a region half-covered at
        # probability 1.0 must read 0.5, not 1.0.
        reg_ok = local_to_dest >= 0
        np.add.at(denom, local_to_dest[reg_ok], entry['n_tiles'][reg_ok].astype(np.float64))
        for e_col, raw_col in _COMBINED_EXPOSURE_RAW_COL.items():
            arr = entry['raw'].get(raw_col)
            if arr is None:
                continue
            contrib = arr[ok] * p_tile[ok]
            # A NULL raw value means "unknown here", not "zero here": it is
            # left out of the sum, and a region contributes to `have_exp`
            # only where at least one of its tiles carried a real number.
            real = np.isfinite(contrib)
            if not real.any():
                continue
            np.add.at(exp_acc[e_col], dest, np.where(real, contrib, 0.0))
            np.logical_or.at(have_exp[e_col], dest, real)

    if not mapped_any:
        # No z14 tile -> admin mapping is resident for ANY selected country
        # (LOCAL/BLOB mode, or a country with no BASE_MERCATOR_TILE_MAT /
        # BASE_ADMIN_GEOM_MAT rows). Returning zeros here would be a
        # confidently-wrong "no hazard anywhere"; an empty resolved list
        # tells the caller to fall back instead.
        return {}, {}, []

    p_admin = np.divide(sum_p, denom, out=np.zeros_like(sum_p), where=denom > 0)
    has = denom > 0
    p_by_admin = {aid: float(p_admin[i]) for i, aid in enumerate(admin_ids) if has[i]}
    expected_by_admin = {
        aid: {e_col: float(exp_acc[e_col][i]) for e_col in exp_acc if have_exp[e_col][i]}
        for i, aid in enumerate(admin_ids) if has[i]
    }
    return p_by_admin, expected_by_admin, sorted(resolved)


def _hex_to_rgba(h: str) -> tuple[int, int, int, int]:
    h = h.lstrip('#')
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return (r, g, b, 255)


# Pre-parse all palette colors to RGBA tuples at module load time.
_PALETTE_RGBA: dict[str, list[tuple[int, int, int, int]]] = {
    prop: [_hex_to_rgba(c) for c in spec['colors']]
    for prop, spec in _RASTER_PALETTES.items()
}

# Cache for (key, col) min/max so we don't recompute per tile.
# Stored as (min_val, max_val, loaded_at), expires with _TILE_TTL.
_minmax_cache: dict[tuple, tuple[float, float, float]] = {}
_minmax_lock = threading.Lock()

# Log-scale columns with a constant (min_val, max_val): PROBABILITY uses
# "no dynamic scaling" while the RAW (unweighted) population-family
# columns were tried as a fixed scale then reverted. Only PROBABILITY
# stays fixed here now; POPULATION/CHILDREN_TOTAL/INFANT_POPULATION/
# SCHOOL_AGE_POPULATION/ADOLESCENT_POPULATION (and their E_* siblings, see
# _RASTER_PALETTES' own E_POPULATION/etc. entries) are all back to a real,
# data-driven per-country/per-cycle range, the exact same as every other
# never-fixed log column in this file.
_FIXED_SCALE_COLS = {
    'PROBABILITY': (1.0 / 51, 1.0),
}


def _get_minmax(key: tuple, col: str) -> tuple[float, float] | None:
    """Return (min_val, max_val) for a column from the cached DataFrame.

    Respects palette fixed_max (e.g. probability=1.0). For linear palettes with
    a fixed ceiling, min is anchored at 0. For log palettes, min is the smallest
    positive value in the data.
    """
    cache_key = (key, col)
    cached = _minmax_cache.get(cache_key)
    if cached and (time.time() - cached[2]) < _TILE_TTL:
        return (cached[0], cached[1])
    with _minmax_lock:
        cached = _minmax_cache.get(cache_key)
        if cached and (time.time() - cached[2]) < _TILE_TTL:
            return (cached[0], cached[1])
        df = _cache._mercator.get(key)
        if df is None or df.empty or col not in df.columns:
            return None
        spec = _RASTER_PALETTES.get(col, {})
        col_data = pd.to_numeric(df[col], errors='coerce').dropna()
        if col_data.empty:
            return None

        fixed_max = spec.get('fixed_max')
        scale = spec.get('scale')
        # `scale=='log'` takes priority over `fixed_max` for the MIN
        # endpoint: a log-scale palette needs a data-driven positive floor
        # (log(0) is undefined; anchoring at 0 like the linear-only branch
        # below does would break/degenerate the log math entirely).
        # `fixed_max` (e.g. MODERATE_POVERTY_PROB's 1.0 ceiling) still wins
        # for the MAX endpoint when set, so the scale stays an absolute,
        # cross-country-comparable ceiling, just with its LOW end spread
        # out logarithmically instead of linearly compressed into one
        # bucket.
        if scale == 'log':
            pos = col_data[col_data > 0]
            if pos.empty:
                return None
            max_val = float(fixed_max) if fixed_max is not None else float(col_data.max())
            # PROBABILITY and the population-family columns get a
            # constant scale: both min_val AND max_val are
            # hardcoded absolute constants, completely ignoring this
            # country/cycle's own real min/max (see _FIXED_SCALE_COLS'
            # own comment for the full real-world grounding), so the
            # identical real number always maps to the identical real
            # color no matter which country or forecast cycle it comes
            # from.
            #
            # Every OTHER log column (E_NUM_SCHOOLS, BUILT_SURFACE_M2,
            # E_POPULATION, etc.) anchors the floor at the TRUE minimum,
            # not an artificially raised one. A floor-raise (real min(...,
            # max_val * a small fraction)) was tried here to
            # stop a near-zero outlier tile from stretching the whole
            # ramp, but that was specific to PROBABILITY's own
            # distribution shape, which no longer even reaches this
            # branch at all (fully fixed above). Verified against 5
            # real datasets (PHL/JAM/BGD/MEX, both raw
            # population-family and E_* impact columns): the floor-raise
            # was actively HURTING every one of them -- e.g. PHL's real
            # POPULATION column had 97.8% of all nonzero cells collapsed
            # into the single flattest color with the floor raised, vs
            # 0.1% with the true minimum and a full, real 10-color spread.
            # These count-like columns have a genuinely wide, real dynamic
            # range (a single dense-urban z14 cell can be 4-5 orders of
            # magnitude denser than a rural one); the "outlier" the floor
            # was built to suppress is exactly the real signal a log scale
            # exists to show here, not noise.
            if col in _FIXED_SCALE_COLS:
                min_val, max_val = _FIXED_SCALE_COLS[col]
            else:
                min_val = float(pos.min())
                if max_val <= min_val:
                    # `min_val == max_val` (zero-variance distribution) does
                    # not mean "no data": it is a routine case for River
                    # specifically, where a rare RP tier (rp10 = a
                    # 1-in-10-year event) means many tiles only ever have
                    # exactly ONE flooded ensemble member (1/51), producing a
                    # uniform, zero-variance PROBABILITY distribution that is
                    # genuinely present data. Widen geometrically around the
                    # single shared value (÷3 / ×3) rather than returning
                    # None: this places it at the midpoint of the log scale
                    # (neither artificially muted at the bottom nor
                    # overstated at the top), the same treatment regardless
                    # of which log-scale column hits this case.
                    min_val = min_val / 3.0
                    max_val = max_val * 3.0
            result = (min_val, max_val)
        elif fixed_max is not None:
            # Linear scale with known ceiling (e.g. probability 0–1).
            # Anchor min at 0 so the full palette is used correctly.
            result = (0.0, float(fixed_max))
        elif scale == 'rwi':
            result = (-1.0, 1.0)
        elif scale == 'smod':
            result = (10.0, 30.0)
        else:
            pos = col_data[col_data > 0]
            if pos.empty:
                return None
            min_val = float(pos.min())
            max_val = float(col_data.max())
            if max_val <= 0:
                return None
            result = (min_val, max_val)
        _minmax_cache[cache_key] = (result[0], result[1], time.time())
        return result


def _paint_tile_from_color_indices(ws, ss, es, ns, idx, palette_rgba,
                                     tile_w, tile_dw, merc_tile_s, merc_tile_dh):
    """Shared pixel-scatter/WEBP-encode tail for MAT-based raster tiles,
    reused by the combined-hazard endpoint so both share the same
    vectorized projection/scatter code instead of duplicating it.

    `ws`/`ss`/`es`/`ns` (z=14 cell geo-bounds) and `idx` (palette color
    index) must already be validity- AND finite-filtered and row-aligned
    (same length/order) by the caller: this function does no value
    scaling or NaN handling of its own, since single-hazard vs combined/
    classification tiles each compute `idx` via different logic upstream.
    """
    img_arr = np.zeros((512, 512, 4), dtype=np.uint8)
    if len(idx) == 0:
        return None

    # Map z=14 cell bounds to pixel coordinates.
    # X: longitude is linear in Web Mercator. Straightforward.
    # Y: latitude is logarithmic in Web Mercator: must project before scaling
    #    or tiles drift visibly at low zoom levels.
    #
    # Use floor() for both edges then +1 for right/bottom. This guarantees that
    # adjacent display tiles round shared cell boundaries identically (both
    # floor to the same pixel), preventing 1-pixel transparent gaps where the
    # basemap bleeds through. The +1 on px1/py1 ensures minimum 1-pixel coverage
    # without relying on ceil() which can disagree with the neighbouring tile's
    # floor() when the boundary falls exactly on a pixel.
    px0 = np.floor((ws - tile_w) / tile_dw * 512).astype(np.int32)
    px1 = np.floor((es - tile_w) / tile_dw * 512).astype(np.int32) + 1
    merc_ns = np.log(np.tan(np.pi / 4 + np.radians(ns) / 2))
    merc_ss = np.log(np.tan(np.pi / 4 + np.radians(ss) / 2))
    py0 = np.floor((1.0 - (merc_ns - merc_tile_s) / merc_tile_dh) * 512).astype(np.int32)
    py1 = np.floor((1.0 - (merc_ss - merc_tile_s) / merc_tile_dh) * 512).astype(np.int32) + 1

    # Vectorized scatter-paint over the whole tile at once, rather than a
    # per-pixel-rectangle Python loop. Numpy's documented behavior for
    # fancy-index assignment with duplicate indices (`arr[idx] = values`)
    # applies each value in array order, last one wins: the same
    # last-row-wins semantics a per-row loop would produce at
    # cell-boundary overlaps, computed without per-row Python iteration.
    palette_rgba_arr = np.asarray(palette_rgba, dtype=np.uint8)
    x0v = np.maximum(0, px0)
    y0v = np.maximum(0, py0)
    x1v = np.minimum(512, np.maximum(x0v + 1, px1))
    y1v = np.minimum(512, np.maximum(y0v + 1, py1))
    wv = np.maximum(0, x1v - x0v)
    hv = np.maximum(0, y1v - y0v)
    counts = (wv * hv).astype(np.int64)
    total = int(counts.sum())

    if total > 0:
        # cum_start[i] = pixel index where row i's block begins in the flat
        # `total`-length arrays below: rows are laid out in order, so
        # duplicate flat_idx writes at shared-boundary overlaps still
        # resolve last-row-wins.
        cum_start = np.concatenate(([0], np.cumsum(counts)[:-1]))
        pixel_offset = np.arange(total) - np.repeat(cum_start, counts)
        w_per_pixel = np.repeat(wv, counts)
        dy = pixel_offset // w_per_pixel
        dx = pixel_offset % w_per_pixel
        abs_y = np.repeat(y0v, counts) + dy
        abs_x = np.repeat(x0v, counts) + dx
        colors = palette_rgba_arr[np.repeat(idx, counts)]
        flat_idx = abs_y.astype(np.int64) * 512 + abs_x.astype(np.int64)
        img_arr.reshape(-1, 4)[flat_idx] = colors

    if not img_arr.any():
        return None

    img = Image.fromarray(img_arr, 'RGBA')
    buf = io.BytesIO()
    # Lossless WebP eliminates DCT block compression artifacts at tile boundaries.
    # Lossy encoding (quality=80) compresses each tile independently, amplifying
    # any sub-pixel colour difference at edges into a visible seam line.
    img.save(buf, 'WEBP', lossless=True, method=4)
    return buf.getvalue()


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_raster_tile(
    country: str,
    storm: str,
    forecast_date: str,
    wind_threshold: int,
    prop: str,
    z: int,
    x: int,
    y: int,
    hazard: str = "wind",
    gust_threshold: Optional[int] = None,
    rp_tier: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
) -> bytes | None:
    """Render a 512x512 RGBA PNG for the requested tile.

    Always reads from z=14 DataFrame regardless of display zoom `z`.
    Returns None if there are no data rows.
    """
    if prop not in _RASTER_PALETTES:
        return None

    variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
    key = (country, storm, forecast_date) + variant
    _cache.ensure_mercator(country, storm, forecast_date, wind_threshold, hazard,
                           gust_threshold, rp_tier, threshold_mm, window_h)
    df = _cache._mercator.get(key)
    if df is None or df.empty or prop not in df.columns:
        return None

    # Geographic bounds for the requested display tile.
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    tile_dw = tile_e - tile_w  # longitude is linear in Mercator
    # Y-axis uses Web Mercator (log) projection (same as MapLibre) so tiles
    # stay aligned at all zoom levels. Linear lat interpolation drifts visibly
    # at low zoom where a single display tile spans many degrees of latitude.
    _merc_tile_n = math.log(math.tan(math.pi / 4 + math.radians(tile_n) / 2))
    _merc_tile_s = math.log(math.tan(math.pi / 4 + math.radians(tile_s) / 2))
    _merc_tile_dh = _merc_tile_n - _merc_tile_s

    # Filter z=14 rows that fall within this display tile via quadkey prefix.
    like_pat = _quadkey_like_pattern(z, x, y)
    sub = _filter_by_tile_prefix(df, like_pat, _cache._mercator_sorted.get(key))
    if sub.empty:
        return None

    # Min/max for color scaling (computed once, cached).
    minmax = _get_minmax(key, prop)
    if minmax is None:
        return None
    min_val, max_val = minmax

    vals = pd.to_numeric(sub[prop], errors='coerce').to_numpy(dtype=np.float64)
    ws   = sub['BW'].to_numpy(dtype=np.float64)
    ss   = sub['BS'].to_numpy(dtype=np.float64)
    es   = sub['BE'].to_numpy(dtype=np.float64)
    ns   = sub['BN'].to_numpy(dtype=np.float64)

    spec = _RASTER_PALETTES[prop]

    # For SMOD, water/nodata class (value < 11) → transparent.
    # For log scale, zero is undefined (log(0) = -inf) → skip.
    # For RWI, 0 is a valid midpoint (average wealth) → include.
    # For all others (probability, poverty, E_* impact props), 0 = no data → transparent.
    _bounds_valid = np.isfinite(ws) & np.isfinite(ss) & np.isfinite(es) & np.isfinite(ns)
    if spec['scale'] == 'smod':
        valid = np.isfinite(vals) & (vals >= 11) & _bounds_valid
    elif spec['scale'] == 'rwi':
        valid = np.isfinite(vals) & _bounds_valid
    elif spec['scale'] == 'log':
        valid = np.isfinite(vals) & (vals > 0) & _bounds_valid
    else:
        valid = np.isfinite(vals) & (vals != 0) & _bounds_valid
    vals, ws, ss, es, ns = vals[valid], ws[valid], ss[valid], es[valid], ns[valid]

    if len(vals) == 0:
        return None

    # Map values → RGBA using palette (vectorised log/linear).
    palette_rgba = _PALETTE_RGBA[prop]
    n_colors = len(palette_rgba)

    if spec['scale'] == 'log':
        safe = np.where(vals > 0, vals, np.nan)
        log_range = math.log(max_val) - math.log(min_val) if max_val != min_val else 1.0
        t = (np.log(safe) - math.log(min_val)) / log_range
    elif spec['scale'] == 'linear':
        t = (vals - min_val) / (max_val - min_val) if max_val != min_val else np.zeros_like(vals)
    elif spec['scale'] == 'rwi':
        t = (vals - (-1.0)) / 2.0
    else:
        t = (vals - min_val) / (max_val - min_val) if max_val != min_val else np.zeros_like(vals)

    t = np.clip(t, 0.0, 1.0)
    idx = np.floor(t * (n_colors - 1)).astype(np.int32)
    idx = np.clip(idx, 0, n_colors - 1)

    # SMOD: map GHS-SMOD class 10s-digit directly to palette index.
    # 10-19 (rural) → 0, 20-29 (peri-urban) → 1, 30 (urban centre) → 2.
    if spec['scale'] == 'smod':
        tens = np.floor(vals / 10).astype(np.int32)
        idx = np.clip(tens - 1, 0, n_colors - 1)

    # A second, independent validity stage on top of the earlier `valid`
    # mask: `t` (the scaled 0-1 value) can still be non-finite in edge
    # cases the raw-value check didn't catch (e.g. degenerate min==max
    # divisions). Drop those rows now so _paint_tile_from_color_indices
    # can assume every row it receives is already finite/paintable.
    finite = np.isfinite(t)
    ws, ss, es, ns, idx = ws[finite], ss[finite], es[finite], ns[finite], idx[finite]

    return _paint_tile_from_color_indices(
        ws, ss, es, ns, idx, palette_rgba,
        tile_w, tile_dw, _merc_tile_s, _merc_tile_dh,
    )


# ---------------------------------------------------------------------------
# Combined-hazard raster (real multi-hazard Probability/Classification for
# Country Analysis, replacing what would otherwise be N stacked single-
# hazard raw layers, see map_shell_concept.py's own "Hazard Probability"
# radio / ms-hazard-view-mode SegmentedControl for the UI trigger)
# ---------------------------------------------------------------------------
# Deliberately reuses _cache.ensure_mercator/_cache._mercator UNCHANGED:
# each active hazard is warmed via the exact same call the single-hazard
# /tiles/raster/... path already makes, so (a) no new SQL, no new cache-key
# shape is needed, and (b) if the user later switches back to a single
# active hazard, that hazard's data may already be warm from having been
# combined here. Only the small per-TILE_ID subset for the ONE requested
# display tile is ever merged across hazards (never the full per-country
# DataFrames), keeping this cheap even for large countries.

# Mirrors pages/map_shell_concept.py's WIND/GUST/RIVER/RAIN hex constants:
# duplicated on purpose (this FastAPI process never imports the Dash page
# module), same cross-file duplication convention this repo already has for
# _LAYER_TO_PROP/propMap/ePropMap. Keep in sync if those hex values ever
# change.
_CLASSIFICATION_HAZARD_RGBA: dict[str, tuple[int, int, int, int]] = {
    "wind":  (232, 89, 12, 255),   # WIND  #e8590c
    "gust":  (255, 169, 77, 255),  # GUST  #ffa94d
    "river": (24, 100, 171, 255),  # RIVER #1864ab
    "rain":  (77, 171, 247, 255),  # RAIN  #4dabf7
}
# Mirrors HAZARD_BOTH_COLOR/HAZARD_TRIPLE_COLOR (same file/reason as above).
_CLASSIFICATION_BOTH_RGBA = (108, 122, 137, 255)    # #6c7a89
_CLASSIFICATION_TRIPLE_RGBA = (61, 69, 80, 255)     # #3d4550
# Matches _DOUBLE_OVERLAP_PATTERN/_TRIPLE_OVERLAP_PATTERN's own CSS
# (repeating-linear-gradient: 2px rgba(255,255,255,0.3) then 4px transparent,
# 6px period), matching the Hazard Contribution popup's overlap convention,
# reproduced here as per-pixel raster blending since this is a WEBP tile,
# not a DOM element.
_CLASSIFICATION_STRIPE_PERIOD_PX = 6
_CLASSIFICATION_STRIPE_WIDTH_PX = 2
_CLASSIFICATION_STRIPE_BLEND = 0.3


def _unpack_bits51(bits_arr: np.ndarray) -> np.ndarray:
    """Real per-member boolean matrix `(n_tiles, 51)` from a packed uint64
    bitmask array: the inverse of the bit-packing `_combine_bitmask_
    aware`'s own rain branch (and `combined_member_impacts`) use. Needed
    wherever a caller needs to know WHICH members are set, not just how
    many (`_popcount51`), e.g. `_paint_classification_tile`'s own real
    per-member simultaneous-hazard-overlap counting."""
    member_idx = np.arange(_BITMASK_ENSEMBLE_SIZE, dtype=np.uint64)
    return ((bits_arr[:, None] >> member_idx[None, :]) & np.uint64(1)).astype(bool)


def _paint_classification_tile(ws, ss, es, ns, hazard_bits, hazard_names, bounds_valid,
                                 tile_w, tile_dw, merc_tile_s, merc_tile_dh):
    """Per-tile hazard-classification raster: which active hazard(s) hit
    each z=14 cell. `hazard_bits` is `{hazard_name: per-tile uint64 array}`
    (from `_combine_bitmask_aware`'s own return, see that function's
    docstring), `hazard_names` gives the color/lookup order.

    1 hazard hit → that hazard's own solid color (_CLASSIFICATION_HAZARD_
    RGBA). 2 → _CLASSIFICATION_BOTH_RGBA + a single 45-degree white-hatch
    stripe. 3+ → _CLASSIFICATION_TRIPLE_RGBA + the criss-cross (45- AND
    135-degree) variant. Cells hit by zero active hazards are skipped
    entirely (transparent), same "0 = no data" convention every other prop
    in this file already uses.

    `hit_count` is computed per-member: for each of the 51 members at this
    tile, count how many active hazards' bits are set, then take the MAX
    across members: the worst simultaneous-hazard overlap this tile's
    ensemble actually exhibits, matching this app's established "worst
    case" framing elsewhere (Compare Worst Case By, etc.) and its
    ceil-not-round "don't undercount risk" convention. This is not the
    same as checking each hazard's own country-wide marginal PROBABILITY
    independently (`p_matrix > 0`): two hazards could both have nonzero
    marginal probability from entirely different members, with no single
    member ever hit by both, which would overstate "hazards overlap" here.
    The single-hazard color pick below follows the same per-member logic:
    it's the hazard active for the SPECIFIC member that achieves the
    per-tile max, not "any hazard with nonzero marginal probability".

    Does its own small geometry projection (cell bounds → pixel rects)
    rather than reusing _paint_tile_from_color_indices, because striping
    needs each expanded pixel's own absolute (x, y) canvas position: that
    helper only ever assigns one flat color per source row.
    """
    n_tiles = len(ws)
    n_hazards = len(hazard_names)
    unpacked = np.zeros((n_hazards, n_tiles, _BITMASK_ENSEMBLE_SIZE), dtype=bool)
    for j, h in enumerate(hazard_names):
        bits = hazard_bits.get(h)
        if bits is not None and len(bits) == n_tiles:
            unpacked[j] = _unpack_bits51(bits)

    per_member_count = unpacked.sum(axis=0)  # (n_tiles, 51) real simultaneous-hazard count per member
    hit_count = per_member_count.max(axis=1) if n_hazards else np.zeros(n_tiles, dtype=np.int64)
    winning_member = per_member_count.argmax(axis=1) if n_hazards else np.zeros(n_tiles, dtype=np.int64)

    valid = bounds_valid & (hit_count >= 1)
    if not valid.any():
        return None
    ws, ss, es, ns = ws[valid], ss[valid], es[valid], ns[valid]
    hit_count = hit_count[valid]
    winning_member = winning_member[valid]
    unpacked_v = unpacked[:, valid, :]  # (n_hazards, n_valid, 51)

    n = len(hit_count)
    base = np.zeros((n, 4), dtype=np.float64)
    smode = np.zeros(n, dtype=np.int32)  # 0=solid, 1=single hatch, 2=criss-cross

    single = hit_count == 1
    if single.any() and n_hazards:
        hazard_colors = np.array([_CLASSIFICATION_HAZARD_RGBA[h] for h in hazard_names], dtype=np.float64)
        idx_single = np.where(single)[0]
        winners = winning_member[idx_single]
        which_hazard = np.zeros(len(idx_single), dtype=np.int64)
        for j in range(n_hazards):
            matches = unpacked_v[j, idx_single, winners]
            which_hazard[matches] = j
        base[single] = hazard_colors[which_hazard]

    double = hit_count == 2
    base[double] = np.array(_CLASSIFICATION_BOTH_RGBA, dtype=np.float64)
    smode[double] = 1

    triple = hit_count >= 3
    base[triple] = np.array(_CLASSIFICATION_TRIPLE_RGBA, dtype=np.float64)
    smode[triple] = 2

    # z=14 cell bounds -> pixel rects, same projection as
    # _paint_tile_from_color_indices (see that function's own comments for
    # the full "why floor()+1" rationale), duplicated here rather than
    # shared since this function needs per-pixel absolute (x, y), not just a
    # flat per-row color.
    px0 = np.floor((ws - tile_w) / tile_dw * 512).astype(np.int32)
    px1 = np.floor((es - tile_w) / tile_dw * 512).astype(np.int32) + 1
    merc_ns = np.log(np.tan(np.pi / 4 + np.radians(ns) / 2))
    merc_ss = np.log(np.tan(np.pi / 4 + np.radians(ss) / 2))
    py0 = np.floor((1.0 - (merc_ns - merc_tile_s) / merc_tile_dh) * 512).astype(np.int32)
    py1 = np.floor((1.0 - (merc_ss - merc_tile_s) / merc_tile_dh) * 512).astype(np.int32) + 1

    x0v = np.maximum(0, px0)
    y0v = np.maximum(0, py0)
    x1v = np.minimum(512, np.maximum(x0v + 1, px1))
    y1v = np.minimum(512, np.maximum(y0v + 1, py1))
    wv = np.maximum(0, x1v - x0v)
    hv = np.maximum(0, y1v - y0v)
    counts = (wv * hv).astype(np.int64)
    total = int(counts.sum())

    img_arr = np.zeros((512, 512, 4), dtype=np.uint8)
    if total == 0:
        return None

    cum_start = np.concatenate(([0], np.cumsum(counts)[:-1]))
    pixel_offset = np.arange(total) - np.repeat(cum_start, counts)
    w_per_pixel = np.repeat(wv, counts)
    dy = pixel_offset // w_per_pixel
    dx = pixel_offset % w_per_pixel
    abs_y = (np.repeat(y0v, counts) + dy).astype(np.int64)
    abs_x = (np.repeat(x0v, counts) + dx).astype(np.int64)
    row_idx = np.repeat(np.arange(n), counts)
    base_px = base[row_idx]
    smode_px = smode[row_idx]

    period, width, blend = (_CLASSIFICATION_STRIPE_PERIOD_PX, _CLASSIFICATION_STRIPE_WIDTH_PX,
                             _CLASSIFICATION_STRIPE_BLEND)
    pos1 = np.mod(abs_x + abs_y, period)   # 45-degree band
    pos2 = np.mod(abs_x - abs_y, period)   # 135-degree band (criss-cross only)
    hit1 = (pos1 < width) & (smode_px >= 1)
    hit2 = (pos2 < width) & (smode_px >= 2)

    white = np.array([255.0, 255.0, 255.0, 255.0])
    colors = base_px.copy()
    colors[hit1] = colors[hit1] * (1 - blend) + white * blend
    colors[hit2] = colors[hit2] * (1 - blend) + white * blend
    colors[:, 3] = 255.0
    colors = np.clip(colors, 0, 255).astype(np.uint8)

    flat_idx = abs_y * 512 + abs_x
    img_arr.reshape(-1, 4)[flat_idx] = colors

    if not img_arr.any():
        return None

    img = Image.fromarray(img_arr, 'RGBA')
    buf = io.BytesIO()
    img.save(buf, 'WEBP', lossless=True, method=4)
    return buf.getvalue()


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_combined_raster_tile(
    country: str, storm: str, mode: str,
    z: int, x: int, y: int,
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
    prop: Optional[str] = None,
) -> bytes | None:
    """Combined-hazard raster tile: `mode="probability"` blends every
    simultaneously-active hazard into one combined PROBABILITY value per
    cell via a per-tile bitmask union (see _combine_bitmask_aware);
    `mode="classification"` colors each cell by WHICH hazard(s) hit it
    (see _paint_classification_tile); `mode="exposure"` requires `prop` to
    be one of _COMBINED_EXPOSURE_RAW_COL's keys and colors each cell by
    that raw count × the SAME combined probability the "probability"
    branch computes: i.e. expected impact under P(any active hazard hits
    this cell), not N separate per-hazard expected-impact values stacked
    on top of each other.

    `storm` doubles as the placeholder "storm" cache-key component for
    river/rain (they have no storm concept, see _hazardUrlParts's own
    `placeholderStorm` in maplibre_tiles.js, same convention). Each hazard
    family resolves its own forecast_date independently (river/rain are
    not storm-scoped and can lag wind's own cycle, see
    get_tile_impact_totals_by_threshold's own docstring in
    snowflake_utils.py) rather than sharing one path-segment value.

    Query-param shape otherwise mirrors ms-tile-config-store's own
    per-hazard fields 1:1 (pages/map_shell_concept.py's
    _build_hazard_tile_config), so the frontend URL-builder
    (applyCombinedHazardLayer in maplibre_tiles.js) needs zero new
    Dash-side state beyond what already exists.
    """
    # `active` is built via _build_combined_active_hazards, shared with the
    # combined ADMIN tile (_fetch_admin_combined_tile), so the two combined
    # endpoints can never disagree about which hazards are active or what
    # each one's params mean.
    active = _build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not active:
        return None

    def _ensure_one(item: tuple[str, dict]):
        hz, p = item
        _cache.ensure_mercator(country, storm, p["forecast_date"], p["wind_threshold"], hz,
                               p["gust_threshold"], p["rp_tier"], p["threshold_mm"], p["window_h"])
        variant = _hazard_variant(hz, p["wind_threshold"], p["gust_threshold"],
                                    p["rp_tier"], p["threshold_mm"], p["window_h"])
        key = (country, storm, p["forecast_date"]) + variant
        return hz, _cache._mercator.get(key), key

    # Independent per-hazard Snowflake round-trips (same reasoning as
    # ensure_mercator's own multi-country fan-out above): safe to run
    # concurrently via the shared executor rather than N sequential loads.
    if len(active) > 1:
        results = list(_SHARED_EXECUTOR.map(_ensure_one, active))
    else:
        results = [_ensure_one(item) for item in active]
    hazard_dfs = {hz: df for hz, df, _key in results if df is not None and not df.empty}
    hazard_keys = {hz: key for hz, df, key in results if df is not None and not df.empty}
    if not hazard_dfs:
        return None

    like_pat = _quadkey_like_pattern(z, x, y)
    # Per-DataFrame identity, not per-hazard-name: this same closure is also
    # reused by _combine_bitmask_aware on hazard-bitmask frames that were
    # never one of _DataCache's own cached mercator DataFrames (see that
    # function's own docstring), which correctly fall through to the plain
    # scan since they have no entry here.
    _sorted_by_id = {id(df): _cache._mercator_sorted.get(hazard_keys[hz]) for hz, df in hazard_dfs.items()}

    def _tile_mask(df: pd.DataFrame):
        return _tile_prefix_mask(df, like_pat, _sorted_by_id.get(id(df)))

    # Merge ONLY this one display tile's own rows across hazards (never the
    # full per-country DataFrames). Cheap even for large countries. Bounds
    # (BW/BS/BE/BN) are identical for a given TILE_ID across every hazard
    # (all merge onto the SAME cached base df, see ensure_mercator's own
    # _ensure_mercator_base_one call): combine_first backfills bounds from
    # whichever hazard's row has them when an outer-merge leaves a gap.
    merged = None
    used_hazard_names: list[str] = []
    for hz, df in hazard_dfs.items():
        if 'PROBABILITY' not in df.columns:
            continue
        sub = df.loc[_tile_mask(df), ['TILE_ID', 'BW', 'BS', 'BE', 'BN', 'PROBABILITY']].copy()
        if sub.empty:
            continue
        sub = sub.rename(columns={'PROBABILITY': f'PROBABILITY_{hz}'})
        used_hazard_names.append(hz)
        if merged is None:
            merged = sub
            continue
        merged = merged.merge(sub, on='TILE_ID', how='outer', suffixes=('', '_dup'))
        for c in ('BW', 'BS', 'BE', 'BN'):
            dup = c + '_dup'
            if dup in merged.columns:
                merged[c] = merged[c].combine_first(merged[dup])
                merged = merged.drop(columns=[dup])
    if merged is None or merged.empty or not used_hazard_names:
        return None

    # Every consumer here (probability/exposure via p_combined_full,
    # classification via hazard_bits, both from _combine_bitmask_aware
    # below) uses per-member bitmask data, not marginal per-hazard
    # PROBABILITY. See _paint_classification_tile's own docstring for why
    # classification specifically uses bitmask data rather than marginal
    # PROBABILITY.
    raw_col = _COMBINED_EXPOSURE_RAW_COL.get(prop or "")
    if mode == "exposure" and raw_col:
        # The raw base column (POPULATION/CHILDREN_TOTAL/...) is identical
        # for a given TILE_ID across every hazard: each hazard's own
        # per-threshold DataFrame already carries it merged in from the SAME
        # cached country-wide base df (_ensure_mercator_base_one). Pull it
        # from whichever active hazard has it, once. No extra query.
        for hz in used_hazard_names:
            src = hazard_dfs[hz]
            if raw_col not in src.columns:
                continue
            raw_sub = src.loc[_tile_mask(src), ['TILE_ID', raw_col]].drop_duplicates('TILE_ID')
            merged = merged.merge(raw_sub, on='TILE_ID', how='left')
            break

    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    tile_dw = tile_e - tile_w
    merc_tile_n = math.log(math.tan(math.pi / 4 + math.radians(tile_n) / 2))
    merc_tile_s = math.log(math.tan(math.pi / 4 + math.radians(tile_s) / 2))
    merc_tile_dh = merc_tile_n - merc_tile_s

    ws = merged['BW'].to_numpy(dtype=np.float64)
    ss = merged['BS'].to_numpy(dtype=np.float64)
    es = merged['BE'].to_numpy(dtype=np.float64)
    ns = merged['BN'].to_numpy(dtype=np.float64)
    bounds_valid = np.isfinite(ws) & np.isfinite(ss) & np.isfinite(es) & np.isfinite(ns)

    # Per-tile true-union combination, see _combine_bitmask_aware's own
    # module-level comment for the combination approach. Shared by mode==
    # "probability" (color the probability itself), mode=="exposure"
    # (color raw_count × this same combined probability), and mode==
    # "classification" (see _paint_classification_tile's own docstring
    # for why classification uses this same per-member bit data rather
    # than marginal per-hazard PROBABILITY).
    # `hazard_params` reuses the exact same per-hazard dicts `active`
    # already carries for ensure_mercator above: one resolution path, not
    # two to keep in sync.
    hazard_params = dict(active)
    p_combined_full, merged, hazard_bits = _combine_bitmask_aware(
        merged, used_hazard_names, country, storm, hazard_params, tile_mask=_tile_mask)
    # `merged` may have gained rows (real tiles a bitmask covers that no
    # MAT DataFrame had, see _combine_bitmask_aware's own docstring), so
    # every array derived from `merged` before this call must be
    # re-derived from the (possibly longer) one it returned.
    ws = merged['BW'].to_numpy(dtype=np.float64)
    ss = merged['BS'].to_numpy(dtype=np.float64)
    es = merged['BE'].to_numpy(dtype=np.float64)
    ns = merged['BN'].to_numpy(dtype=np.float64)
    bounds_valid = np.isfinite(ws) & np.isfinite(ss) & np.isfinite(es) & np.isfinite(ns)

    if mode == "classification":
        return _paint_classification_tile(ws, ss, es, ns, hazard_bits, used_hazard_names, bounds_valid,
                                            tile_w, tile_dw, merc_tile_s, merc_tile_dh)

    if mode == "exposure" and raw_col and raw_col in merged.columns:
        raw_vals = pd.to_numeric(merged[raw_col], errors='coerce').to_numpy(dtype=np.float64)
        combined_vals = raw_vals * p_combined_full
        # Same "0 = no data, transparent" convention every single-hazard E_*
        # raster already uses (_fetch_raster_tile's own `vals != 0` branch).
        valid = bounds_valid & np.isfinite(combined_vals) & (combined_vals != 0)
        if not valid.any():
            return None
        v_ws, v_ss, v_es, v_ns, vals = ws[valid], ss[valid], es[valid], ns[valid], combined_vals[valid]

        spec = _RASTER_PALETTES[prop]
        palette_rgba = _PALETTE_RGBA[prop]
        n_colors = len(palette_rgba)
        if len(used_hazard_names) == 1:
            # Nothing server-side stops mode="exposure" from being called
            # with only ONE active hazard, client-side gating (see this
            # function's own docstring) is what normally routes a single
            # hazard to the plain /tiles/raster/... endpoint instead. In
            # that case combined_vals is numerically identical to that
            # hazard's own E_* value, but scaling against raw_col's
            # min/max (below) would paint it a visibly different,
            # generally lighter color than /tiles/raster/'s own rendering
            # of the exact same data (raw's own max is always >= E_*'s
            # own max). Use the SAME E_*-column min/max _fetch_raster_tile
            # itself uses for a single hazard, so this endpoint is
            # pixel-consistent with the single-hazard one
            # whenever it's ever hit with just one active hazard.
            minmax = _get_minmax(hazard_keys[used_hazard_names[0]], prop)
        else:
            # Scaling is against the RAW column's own min/max (raw_col,
            # e.g. POPULATION not E_POPULATION), not each hazard's own
            # separately-cached E_* min/max: two different hazards' E_*
            # ranges are not comparable: river's own country-wide
            # E_population max can be far smaller than rain's simply
            # because river's own highest PROBABILITY anywhere is lower,
            # not because the underlying population is smaller, so mixing
            # two independently-scaled ranges can produce a non-monotonic
            # result (a combined cell mapped to a LOWER color bucket than
            # either individual hazard's own rendering of the same tile,
            # despite combined_E being mathematically >= max(E_a, E_b)
            # always, since p_combined >= max(p_a, p_b) for any
            # probabilities in [0,1]). The raw column is hazard-independent
            # (every active hazard's df already carries the exact same
            # country-wide raw base column, see _ensure_mercator_base_one),
            # so this is a single self-consistent range no matter
            # which/how many hazards are combined, and combined_E <= raw
            # always holds, so this range can never under- or over-shoot in
            # a way that breaks monotonicity.
            minmax = None
            for hz in used_hazard_names:
                minmax = _get_minmax(hazard_keys[hz], raw_col)
                if minmax is not None:
                    break
        if minmax is not None:
            min_val, max_val = minmax
        else:
            min_val, max_val = float(np.min(vals)), float(np.max(vals))
        if max_val <= min_val:
            max_val = min_val + 1.0

        # Dispatches on spec['scale'] rather than hardcoding the log-scale
        # formula, mirroring _fetch_raster_tile's own scale dispatch, so
        # this stays correct even if a _COMBINED_EXPOSURE_RAW_COL prop is
        # ever retuned away from 'log'.
        if spec['scale'] == 'log':
            safe = np.where(vals > 0, vals, np.nan)
            log_range = math.log(max_val) - math.log(min_val) if max_val != min_val and min_val > 0 else 1.0
            t = (np.log(safe) - math.log(min_val)) / log_range if min_val > 0 else \
                (vals - min_val) / (max_val - min_val)
        elif spec['scale'] == 'rwi':
            t = (vals - (-1.0)) / 2.0
        else:
            t = (vals - min_val) / (max_val - min_val) if max_val != min_val else np.zeros_like(vals)
        t = np.clip(t, 0.0, 1.0)
        idx = np.clip(np.floor(t * (n_colors - 1)).astype(np.int32), 0, n_colors - 1)
        finite = np.isfinite(t)
        v_ws, v_ss, v_es, v_ns, idx = v_ws[finite], v_ss[finite], v_es[finite], v_ns[finite], idx[finite]
        if len(idx) == 0:
            return None
        return _paint_tile_from_color_indices(v_ws, v_ss, v_es, v_ns, idx, palette_rgba,
                                                tile_w, tile_dw, merc_tile_s, merc_tile_dh)

    # mode == "probability" (default, and exposure's own fallback when
    # `prop` isn't a real _COMBINED_EXPOSURE_RAW_COL key).
    valid = bounds_valid & np.isfinite(p_combined_full) & (p_combined_full > 0)
    if not valid.any():
        return None
    ws, ss, es, ns, p_combined = ws[valid], ss[valid], es[valid], ns[valid], p_combined_full[valid]

    spec = _RASTER_PALETTES['PROBABILITY']
    palette_rgba = _PALETTE_RGBA['PROBABILITY']
    n_colors = len(palette_rgba)
    # A constant 0-100% scale, same real constants as _get_minmax's own
    # PROBABILITY branch (that function's own comment has the full "why":
    # the identical real percentage must always map to the identical real
    # color, regardless of country/cycle/how many hazards are combined).
    # Deliberately NOT the country-specific union-bound range this used to
    # derive from per-hazard _get_minmax calls (min of per-hazard mins,
    # min(1.0, sum of per-hazard maxes)) -- that was itself already
    # dynamic (varying with which hazards happen to be active), the
    # opposite of what's wanted here.
    min_val, max_val = 1.0 / _BITMASK_ENSEMBLE_SIZE, 1.0

    if spec['scale'] == 'log':
        safe = np.where(p_combined > 0, p_combined, np.nan)
        log_range = math.log(max_val) - math.log(min_val) if max_val != min_val and min_val > 0 else 1.0
        t = (np.log(safe) - math.log(min_val)) / log_range if min_val > 0 else \
            (p_combined - min_val) / (max_val - min_val)
    else:
        t = (p_combined - min_val) / (max_val - min_val) if max_val != min_val else np.zeros_like(p_combined)
    t = np.clip(t, 0.0, 1.0)
    idx = np.clip(np.floor(t * (n_colors - 1)).astype(np.int32), 0, n_colors - 1)
    finite = np.isfinite(t)
    ws, ss, es, ns, idx = ws[finite], ss[finite], es[finite], ns[finite], idx[finite]
    if len(idx) == 0:
        return None

    return _paint_tile_from_color_indices(ws, ss, es, ns, idx, palette_rgba,
                                            tile_w, tile_dw, merc_tile_s, merc_tile_dh)


# ---------------------------------------------------------------------------
# Combined-hazard ADMIN (Regions view) vector tiles
# ---------------------------------------------------------------------------
# The Tiles/Regions switch (applyHazardLayer in maplibre_tiles.js) is
# honoured by both the single-hazard path and this combined ADMIN layer:
# when 2+ hazards are active and view_mode is "Regions", the frontend
# renders this server-computed union at admin granularity rather than
# falling back to a client-side MAX across N separate single-hazard admin
# layers.


def _build_combined_active_hazards(
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
) -> list[tuple[str, dict]]:
    """The `[(hazard_name, params_dict), ...]` list every combined-hazard
    endpoint resolves from its own query params: the single definition
    shared by the combined RASTER tile, the combined ADMIN tile, the
    facility markers, the hover tooltip and the country-wide headline
    totals, so none of them can drift apart on which hazards count as
    active or on what each one's params mean.

    Each hazard family carries its own forecast_date (river/rain are not
    storm-scoped and can lag wind's own cycle) rather than sharing one.

    `river_window` stays separate from `window_h` (Rain's own): River and
    Rain can be simultaneously active with different windows, so one
    shared field can never serve both. Leaving river's window unset here
    would make _hazard_variant/ensure_mercator silently fall back to
    _RIVER_WINDOW_DEFAULT (the full 168h horizon) regardless of what the
    UI's accumulation-window control selects.

    Gust requires a `gust_threshold`: every gust source in this file
    (ADMIN_ALL_GUST_MAT / MERCATOR_TILE_GUST_MAT / TILE_GUST_BITMASK_MAT)
    is filtered on an exact threshold value, so a missing one selects zero
    rows. Treating it as active would put an always-empty PROBABILITY_GUST
    key on every feature and a hazard in `used_hazard_names` that can never
    resolve, so it is not active until a threshold arrives.
    """
    active: list[tuple[str, dict]] = []
    if wind_on and wind_forecast_date:
        active.append(("wind", dict(forecast_date=wind_forecast_date, wind_threshold=wind_threshold,
                                     gust_threshold=None, rp_tier=None, threshold_mm=None, window_h=None)))
    if gust_on and wind_forecast_date and gust_threshold is not None:
        active.append(("gust", dict(forecast_date=wind_forecast_date, wind_threshold=wind_threshold,
                                     gust_threshold=gust_threshold, rp_tier=None, threshold_mm=None, window_h=None)))
    if river_on and river_forecast_date:
        active.append(("river", dict(forecast_date=river_forecast_date, wind_threshold=wind_threshold,
                                      gust_threshold=None, rp_tier=rp_tier, threshold_mm=None, window_h=river_window)))
    if rain_on and rain_forecast_date:
        active.append(("rain", dict(forecast_date=rain_forecast_date, wind_threshold=wind_threshold,
                                     gust_threshold=None, rp_tier=None, threshold_mm=threshold_mm, window_h=window_h)))
    return active


def _normalize_combined_hazard_params(
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
) -> tuple:
    """Return the same 13 combined-hazard params, with every value that
    cannot affect the result collapsed to one canonical form.

    Both combined tile endpoints are memoised on their full parameter list,
    so each parameter kept in that list widens the cache key. A hazard that
    _build_combined_active_hazards does not put in `active` contributes
    nothing to the output, so its own params (and `wind_forecast_date`,
    shared by Wind and Gust) are reported as None here, and `*_on` is
    reported as the real active state rather than the raw checkbox. Two
    requests that differ only in a switched-off hazard's threshold, date or
    window therefore land on the SAME cache entry and serve the already
    computed bytes instead of recomputing a result that is identical by
    construction. River's window additionally collapses to
    _RIVER_WINDOW_DEFAULT when unset, which is exactly what _hazard_variant
    and every river query substitute for a missing window, so the two
    spellings of the same window share one entry too.

    Applied at the route, before any cached call, so the whole combined
    chain (raster tile, admin tile, and the country-wide admin combine the
    admin tile forwards its params to) sees one canonical key.
    """
    active = dict(_build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h))
    wind_active = "wind" in active
    gust_active = "gust" in active
    river_active = "river" in active
    rain_active = "rain" in active
    return (
        wind_active,
        wind_forecast_date if (wind_active or gust_active) else None,
        # wind_threshold rides along in every hazard's param dict but only
        # Wind's own sources are filtered by it (see _hazard_variant, and
        # ensure_mercator/ensure_admin's per-hazard SQL branches).
        wind_threshold if wind_active else 0,
        gust_active,
        gust_threshold if gust_active else None,
        river_active,
        river_forecast_date if river_active else None,
        rp_tier if river_active else None,
        (river_window or _RIVER_WINDOW_DEFAULT) if river_active else None,
        rain_active,
        rain_forecast_date if rain_active else None,
        threshold_mm if rain_active else None,
        window_h if rain_active else None,
    )


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=64)
def _combined_admin_probabilities(
    country: str, storm: str, admin_level: int,
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
) -> tuple[dict[str, float], dict[str, dict[str, float]], list[str], list[str]]:
    """Real COUNTRY-WIDE {admin ucode -> combined union probability} for one
    hazard combination, cached per (country, storm, admin_level, every
    hazard param), NOT per display tile, deliberately.

    An admin region routinely spans several display tiles. Computing its
    union from only the part of it inside the current tile would give the
    SAME region a different probability (and therefore a different fill
    colour and a different hover number) depending on which tile happened
    to serve it: a real, user-visible inconsistency. A region's union is
    a property of the whole region, so it is computed whole, once, and
    every display tile that touches the region reads the same value.

    Returns `(probs_by_admin, expected_by_admin, used_hazard_names,
    resolved_hazard_names)`, see _combine_bitmask_aware_admin for what
    each of the first two means and why `resolved_hazard_names` (which may
    be EMPTY, meaning nothing could be computed and the caller must fall
    back) is not the same list as `used_hazard_names`. Callers MUST treat
    the returned dicts as read-only: they are the cached objects
    themselves, not copies (same convention every other _ttl_cache'd
    builder in this file follows).
    """
    active = _build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not active:
        return {}, {}, [], []
    used_hazard_names = [hz for hz, _ in active]

    codes = [c.upper() for c in country.split('+') if c.strip()]
    admin_ids: list[str] = []
    seen: set[str] = set()
    for code in codes:
        _df, _geoms, _tree, id_order = _cache._ensure_admin_base_one(code, admin_level)
        for aid in (id_order or []):
            if aid not in seen:
                seen.add(aid)
                admin_ids.append(aid)
    if not admin_ids:
        # No admin geometry resident (LOCAL/BLOB mode, or a country with
        # no BASE_ADMIN_GEOM_MAT rows at this level): a real, honest "no
        # regions to combine", not an error.
        return {}, {}, used_hazard_names, []

    probs, expected, resolved = _combine_bitmask_aware_admin(
        admin_ids, used_hazard_names, country, storm, dict(active), admin_level)
    return probs, expected, used_hazard_names, resolved


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_admin_combined_tile(
    country: str, storm: str,
    z: int, x: int, y: int, admin_level: int,
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
) -> bytes:
    """Combined-hazard admin-region MVT tile: the Regions-view analogue of
    _fetch_combined_raster_tile, and the vector-tile sibling of
    _fetch_admin_tile (same clip-in-WGS84 -> project-to-Mercator ->
    mapbox_vector_tile.encode path, same "admin" layer name, same
    quantize_bounds, so the frontend can point an identical vector source
    at it and keep its existing `source-layer: 'admin'`).

    What is genuinely different from _fetch_admin_tile:
    - `PROBABILITY` is the REAL per-member union from
      _combine_bitmask_aware_admin (unioned per z14 tile, then AREA-MEANED
      over the region exactly as the pipeline defines every single-hazard
      admin probability), not one hazard's own pre-aggregated marginal and
      not a MAX across hazards.
    - `PROBABILITY_WIND`/`_GUST`/`_RIVER`/`_RAIN` carry each active
      hazard's OWN real marginal probability alongside it, so the hover
      tooltip can still render the per-hazard breakdown rows under the
      combined figure without N extra requests.
    - every `_COMBINED_EXPOSURE_RAW_COL` expected-impact field (the six
      population/built-up columns plus E_NUM_SCHOOLS/E_NUM_HCS/
      E_NUM_SHELTERS/E_NUM_WASH) comes from the same per-z14-tile union as
      `sum_over_tiles(raw_tile x p_union_tile)`: the SAME formula the
      pipeline uses for each hazard's own admin E_* columns, so the
      combined figure is directly comparable with them.

      `region_total_raw x region_probability` is NOT the same quantity and
      is deliberately not used: it spreads the region's whole population
      over the region uniformly and then scales it by a region-wide
      probability, which overstates by roughly the ratio between the
      storm-hit part of the region and the region as a whole (an order of
      magnitude or more for a large admin-1 a cyclone only clips), and it
      disagrees with ADMIN_ALL_*_MAT's own E_POPULATION for the identical
      region.

      A column with no real value for a region (all-NULL raw data, the
      normal shape for facility counts in a country with no such
      inventory) is ABSENT from the union result rather than 0.0, and the
      per-hazard merged value below carries the feature instead.

    DEGRADATION: when NO hazard's per-member source resolves, PROBABILITY
    and E_* are NOT stamped to 0.0 over the real per-hazard values merged
    in above. A union that reads lower than its own marginals is
    impossible, so a zero there would be a confidently-wrong "no hazard
    here" rather than an honest "not computable". The per-hazard MAX merge
    carries the feature instead, and in ALL cases both PROBABILITY and
    every E_* are floored at the largest active marginal: a union is at
    least each of its parts.

    Real, honest remaining limitation: the wind-only E_PEOPLE_IN_NEED/
    E_CHILDREN_IN_NEED vulnerability pair is still merged with a MAX
    across the active hazards' own values, exactly as the client-side
    combine did before this endpoint existed. Unlike the facility counts
    above, this one stays deliberately out of _COMBINED_EXPOSURE_RAW_COL:
    PIN/CHIN are vulnerability-WEIGHTED, not a plain raw-count x
    probability product, and only wind ever populates them at all (a
    deliberate, documented product decision, see _in_need_note), so a
    MAX across hazards here just passes wind's own real value through
    unchanged rather than fabricating a cross-hazard blend. Inventing a
    raw x probability product for a quantity that has no such formula
    upstream would be a new, unreviewed methodology introduced in a
    rendering path rather than in the pipeline that owns those numbers.
    Flagged rather than silently papered over.

    The route's `mode` path segment ("probability"/"exposure") is NOT a
    parameter here: unlike the raster (where the server must decide what
    single quantity to paint into each pixel), this tile carries EVERY
    property and the client's own buildColorExpression picks which one to
    colour by, so both modes produce identical bytes. The segment exists
    purely so the URL shape mirrors the raster endpoint's own {mode} 1:1;
    keeping it out of this function's arguments keeps one cache entry per
    tile instead of one per (tile, mode). "classification" is rejected at
    the route (see admin_combined_tile): it is a per-pixel colour decision
    with no vector-property equivalent.

    COST SHAPE: the tile -> region aggregation itself is cheap (a bitmask
    union plus two vectorised aggregations over the country's z14 tiles).
    What dominates a cold request is loading the raw per-member sources the
    union needs (the worldwide GloFAS extent table and the global tp Zarr
    ), which the single-hazard Regions view never touches at all, since
    those layers read pre-aggregated ADMIN_ALL_*_MAT rows. Those loads are
    shared process-wide caches, so the cost is paid once per source per
    TTL rather than per tile, and every tile of the same viewport after the
    first is served from the memoised result.
    """
    active = _build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not active:
        return b""
    if not country:
        return b""

    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)

    # Per-hazard admin rows for THIS display tile only (the country-wide
    # load itself is already cached inside _DataCache, see ensure_admin).
    # Merged by admin TILE_ID: base/descriptive fields come from whichever
    # hazard has them first (they describe who lives in the region, not a
    # hazard-specific quantity: same reasoning maplibre_tiles.js's own
    # _AOTS_TT_BASE_FIELDS list documents client-side).
    merged: dict[str, dict] = {}
    geom_by_id: dict[str, object] = {}
    for hz, p in active:
        try:
            candidates = _cache.query_admin(
                country, storm, p["forecast_date"], p["wind_threshold"], admin_level,
                tile_w, tile_s, tile_e, tile_n,
                hz, p["gust_threshold"], p["rp_tier"], p["threshold_mm"], p["window_h"])
        except Exception as e:
            # One hazard failing to resolve must never take the whole
            # combined tile down: the union simply loses that hazard's
            # contribution, same fail-open contract
            # _combine_bitmask_aware_admin itself has.
            log.warning("admin-combined: hazard %s failed to load for %s/%s: %s", hz, country, storm, e)
            continue
        for geom, props in candidates:
            tid = props.get("TILE_ID")
            if tid is None:
                continue
            if tid not in geom_by_id:
                # Every hazard's admin rows come from the SAME cached base
                # geometry set (_ensure_admin_base_one), so the polygon is
                # identical whichever hazard supplied it: take the first.
                geom_by_id[tid] = geom
            slot = merged.setdefault(tid, {})
            hz_prob = props.get("PROBABILITY")
            if hz_prob is not None and not (isinstance(hz_prob, float) and pd.isna(hz_prob)):
                slot[f"PROBABILITY_{hz.upper()}"] = _py(hz_prob)
            for k, v in props.items():
                if k == "PROBABILITY":
                    continue  # replaced wholesale by the real union below
                if _safe_prop(v) is None:
                    continue
                if k.startswith("E_"):
                    prev = slot.get(k)
                    if prev is None or v > prev:
                        slot[k] = _py(v)
                elif k not in slot:
                    slot[k] = _py(v)

    if not merged:
        return b""

    probs, expected, used_names, resolved_names = _combined_admin_probabilities(
        country, storm, admin_level,
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not resolved_names:
        # Nothing resolved at all, see this function's own DEGRADATION
        # note. Logged once per tile-cache miss (not per feature) so a real
        # backfill gap is visible in the container log instead of being
        # silently painted as zero hazard.
        log.warning(
            "admin-combined: no per-member source resolved for %s/%s L%s (active: %s), "
            "falling back to the per-hazard MAX merge for this tile",
            country, storm, admin_level, ",".join(used_names) or "-")
    marginal_keys = [f"PROBABILITY_{hz.upper()}" for hz in used_names]

    merc_b = mercantile.xy_bounds(mercantile.Tile(x, y, z))
    tile_box_wgs = box(tile_w, tile_s, tile_e, tile_n)
    tile_box_merc = box(merc_b.left, merc_b.bottom, merc_b.right, merc_b.top)

    features = []
    for tid, props in merged.items():
        geom = geom_by_id.get(tid)
        if geom is None:
            continue
        # A union can never be smaller than any of its own marginals, so
        # the largest active per-hazard value is a hard, real floor. It is
        # what carries the whole feature when nothing resolved
        # (`resolved_names` empty), and it also absorbs the small
        # region-boundary difference between the pipeline's own set of
        # z14 tiles per region and this process's centroid-in-polygon
        # assignment (see _TileAdminMapCache's docstring), without it a
        # region could render a combined probability a hair BELOW the
        # PROBABILITY_WIND printed directly under it in the same tooltip.
        p_floor = 0.0
        for mk in marginal_keys:
            mv = props.get(mk)
            if mv is None:
                continue
            try:
                mvf = float(mv)
            except (TypeError, ValueError):
                continue
            # NaN compares False against everything, so it is skipped here
            # without a separate isnan check.
            if mvf > p_floor:
                p_floor = mvf
        # A region absent from `probs` had no z14 tile resident (nothing
        # to average over): fall back to the floor rather than stamping a
        # fabricated 0.0 over real per-hazard data. The polygon is kept
        # either way: its base population/facility data is still real and
        # still wanted in the hover tooltip.
        p_comb = max(float(probs.get(tid, 0.0)), p_floor) if resolved_names else p_floor
        props["PROBABILITY"] = p_comb
        exp_row = expected.get(tid) if resolved_names else None
        for e_col in _COMBINED_EXPOSURE_RAW_COL:
            # `props[e_col]` currently holds the MAX across the active
            # hazards' own ADMIN_ALL_*_MAT values, itself a valid lower
            # bound for the union's expected count, for the same reason
            # p_floor is one for the probability.
            prev = props.get(e_col)
            try:
                prev_f = float(prev) if prev is not None else None
            except (TypeError, ValueError):
                prev_f = None
            new_f = None if exp_row is None else exp_row.get(e_col)
            if new_f is None and prev_f is None:
                continue
            if new_f is None:
                props[e_col] = prev_f
            elif prev_f is None:
                props[e_col] = float(new_f)
            else:
                props[e_col] = max(float(new_f), prev_f)
        try:
            clipped_wgs = geom.intersection(tile_box_wgs)
            if clipped_wgs.is_empty or clipped_wgs.geom_type == "GeometryCollection":
                continue
            clipped_merc = _to_merc(clipped_wgs).intersection(tile_box_merc)
            if clipped_merc.is_empty:
                continue
            features.append({"geometry": clipped_merc.wkt, "properties": props})
        except Exception as e:
            log.debug("Skip admin-combined clip: %s", e)

    if not features:
        return b""

    pbf = mapbox_vector_tile.encode(
        [{"name": "admin", "features": features}],
        default_options={"quantize_bounds": (merc_b.left, merc_b.bottom, merc_b.right, merc_b.top)},
    )
    raw = bytes(pbf) if not isinstance(pbf, bytes) else pbf
    # gzip: same fix, same reasoning as _fetch_admin_tile above. This tile
    # carries EVEN MORE per-feature properties than the single-hazard admin
    # tile (PROBABILITY_WIND/_GUST/_RIVER/_RAIN plus every combined E_*
    # column, see this function's own docstring), so the relative payload
    # savings from gzip are at least as large here.
    return gzip.compress(raw, compresslevel=6)


# ---------------------------------------------------------------------------
# Global raw precipitation-rate raster (NOT country/storm-scoped)
# ---------------------------------------------------------------------------
# Unlike every other hazard in this file (wind/gust/river/rain are all keyed
# by country+storm or country+forecast_date), the raw tp Zarr on
# AOTS.TC_ECMWF.MET_FORECASTS covers the WHOLE WORLD for a single global
# FORECAST_TIME: there is exactly one file to load per forecast cycle, so
# this gets its own small cache class instead of another _DataCache variant.

# New forecast cycles land every 6-24h in production (see MET_FORECASTS
# ingestion cadence), unlike country-scoped impact data which can change
# within a pipeline run, so a TTL far longer than _TILE_TTL (15 min) is
# appropriate: long enough to avoid re-downloading the ~1.2GB Zarr on every
# request, short enough that a new cycle is picked up same-day without a
# container restart.
_PRECIP_RAW_TTL = 4 * 60 * 60  # 4 hours

# Shorter than _PRECIP_RAW_TTL above on purpose: the per-member rate array
# this backs (see _PrecipRawCache.ensure_member_rate_grid) is ~133MB, far
# larger than the precomputed aggregate grids, and is capped at ONE resident
# entry, so it is held for a session-length window rather than the 4h
# forecast-cycle horizon.
#
# Must be AT LEAST _TILE_TTL: both consumers of this array (the combined
# admin probabilities and the combined admin tile) are themselves memoised
# for _TILE_TTL, and a shorter value here would mean the array expires
# while results derived from it are still being served, forcing a
# re-download+re-decode of the whole tp Zarr on the next miss.
#
# Pinned to _PRECIP_RAW_TTL (not _TILE_TTL) rather than just satisfying that
# minimum: ensure_member_rate_grid re-downloads+re-decodes the SAME
# stage_path ensure_precip_raw already has resident in self._grid (a real
# gap: they don't share the download, see this class's own
# member-cache docstring), so pinning this to _TILE_TTL (15min, 16x shorter
# than _PRECIP_RAW_TTL's 4h) meant that redundant ~1.2GB re-download+decode
# recurred every 15min even while the aggregate cache for the exact same
# file was still fully warm. Matching _PRECIP_RAW_TTL doesn't eliminate the
# redundant download (that needs sharing the actual bytes/zarr store
# between the two caches, a bigger change), but cuts how often it recurs by
# the same 16x.
_PRECIP_MEMBER_TTL = _PRECIP_RAW_TTL

# T+0 -> T+{window}h accumulated-mm window used as a "current rain rate"
# snapshot. tp is stored as a cumulative total from T+0 (see MET_FORECASTS
# ingestion), so any single step value would be an ever-growing blob
# unrelated to "how hard is it raining right now". Differencing T+0 against
# a later step yields a bounded, radar-like rate instead. T+0 is always the
# window start (rather than e.g. T+72h-T+78h) because it's the closest
# available proxy to current conditions at the model's own init time: later
# windows describe a future period, not "now".
#
# Windows supported here are exactly the real windows the app's own
# per-country rain-hazard UI already exposes via ms-rain-window (see
# pages/map_shell_concept.py's _RAIN_MM_BY_WINDOW dict: this list's values
# MUST match that dict's keys, as ints, or the two systems silently
# desync). Kept as a plain list (not imported from pages/map_shell_concept.py)
# because this module must stay import-independent of the Dash page layer.
_PRECIP_RATE_STEP_A = 0
_PRECIP_RATE_WINDOWS_H = [6, 24, 72, 120]

# Backward-compat default window for callers that don't pass window_h at all
# (existing behavior before this was made configurable).
_PRECIP_RATE_DEFAULT_WINDOW_H = 6

# Radar-style color ramp for mm accumulated over the T+0->T+{window}h window.
# Chosen to resemble a standard weather-radar reflectivity ramp (green ->
# yellow -> orange -> red), NOT this app's existing sequential blue/red
# impact-probability palettes. Alpha rises with intensity so heavier rain
# reads as more solid/opaque, matching how radar overlays are usually
# perceived. These 5 breakpoints are for the BASE window (6h) only:
#   < 0.5mm  : transparent  (no perceptible rain)
#   0.5-5mm  : light green  (light rain)
#   5-15mm   : green        (moderate rain)
#   15-30mm  : yellow       (heavy rain)
#   30-60mm  : orange       (very heavy rain)
#   >=60mm   : red          (extreme rain)
# This ramp is scaled per accumulation window (via
# _precip_rate_breaks_for_window() below), not applied unscaled: the
# mean-rate grid holds larger accumulated totals for longer windows (120h/
# 5-day accumulations routinely exceed 150mm, see
# _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM's own 120 entry, the same
# depth-tier classification ms-rain-slider exposes), so a fixed 60mm
# ceiling would render almost the entire map as one saturated "extreme
# rain" red blob for any window longer than 6h, with zero visual
# resolution above 60mm. _precip_rate_breaks_for_window() scales this base
# ramp per window using that same classification data (a single source of
# truth, not an invented second set of numbers).
_PRECIP_RATE_BASE_BREAKS = [0.5, 5.0, 15.0, 30.0, 60.0]
_PRECIP_RATE_COLORS: list[tuple[int, int, int, int]] = [
    (0,   0,   0,   0),
    (168, 230, 145, 140),
    (60,  179, 75,  185),
    (255, 221, 51,  210),
    (255, 140, 0,   225),
    (220, 20,  60,  240),
]

# Fixed ensemble denominator for the PROBABILITY reduction below. Mirrors the
# DATAPIPELINE repo's own precip_utils.exceedance_probability()
# FULL_ENSEMBLE_SIZE constant exactly (same physical constant, same reason:
# hard-coded to 51 rather than read from rate_grid.shape[0], so a Zarr with a
# shrunk member axis for this cycle (e.g. a corrupt/missing perturbed-member
# GRIB upstream) can't silently inflate the probability).
_PRECIP_PROB_ENSEMBLE_SIZE = 51

# "Notable rain" cutoff for the probability variant: the fraction of ensemble
# members whose T+0->T+{window}h accumulated rate exceeds a given mm
# threshold. This is independent of the mean ramp's own 0.5/5/15/30/60mm
# intensity buckets above (those describe magnitude of a single
# ensemble-mean value; this describes ensemble agreement on a threshold).
#
# Backward-compat default threshold for callers that don't pass threshold_mm
# at all (existing behavior before this was made configurable): 10mm/6h
# (~1.7mm/h average) is a widely-used operational threshold for the onset of
# moderate rain: high enough to filter out drizzle/model noise, low enough
# to give useful lead-time signal before conditions turn heavy.
_PRECIP_PROB_THRESHOLD_MM = 10.0

# Real per-window depth-tier thresholds (mm) the app's own ms-rain-slider
# already exposes per ms-rain-window selection, copied verbatim from
# pages/map_shell_concept.py's _RAIN_MM_BY_WINDOW (that dict remains the
# single source of truth; kept here as a plain literal, not imported, for the
# same import-independence reason as _PRECIP_RATE_WINDOWS_H above). Every
# (window_h, threshold_mm) pair reachable from the real UI is precomputed in
# ensure_precip_raw() below, PLUS the legacy default (6, 10.0) pair (not one
# of the real UI tiers, but must keep working for existing callers/bookmarks
# that never pass threshold_mm at all).
_PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM: dict[int, list[float]] = {
    6: [25.0, 50.0, 75.0],
    24: [35.0, 70.0, 103.0],
    72: [45.0, 90.0, 133.0],
    120: [50.0, 100.0, 150.0],
}


def _precip_rate_breaks_for_window(window_h: int) -> list[float]:
    """Scale _PRECIP_RATE_BASE_BREAKS (the 6h radar ramp) up for longer
    accumulation windows, using the ratio of this window's own top real
    depth-tier threshold (_PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM's own highest
    entry: the exact same real classification numbers ms-rain-slider's
    severity tiers use) to the base window's (75mm). See
    _PRECIP_RATE_BASE_BREAKS's own comment for why this scaling exists.

    E.g. 120h's top real tier (150mm) is exactly 2x 6h's (75mm), so its ramp
    breaks are [1.0, 10.0, 30.0, 60.0, 120.0]: "extreme rain" now only
    triggers past 120mm/5-days instead of a flat, physically-too-low 60mm.
    """
    base_top = _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM[_PRECIP_RATE_DEFAULT_WINDOW_H][-1]
    window_top = _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM.get(int(window_h), [base_top])[-1]
    scale = window_top / base_top
    return [round(b * scale, 1) for b in _PRECIP_RATE_BASE_BREAKS]


# Sequential single-hue purple ramp for exceedance PROBABILITY (real
# per-request DYNAMIC min/max, not a fixed 0-100% scale, matching
# river-raw's own conversion, see
# _fetch_river_extent_raster_tile's own comment for the full "why").
# Deliberately NOT the green/yellow/orange/red "intensity" ramp used for
# the mean variant above, so a screenshot alone makes it unambiguous which
# mode is showing: a single hue ramping from pale to saturated purple is
# the conventional way to encode a probability/confidence scale (vs. a
# multi-hue scale, which implies distinct physical categories).
#
# `_PRECIP_PROB_BREAKS` is no longer used to bucket values (kept only for
# any external consumer still reading it, e.g. get_river_raw_stats' own
# API-shape sibling), _colorize_precip_probability now interpolates.
# Bucket 0 is a real, visible tint (never fully transparent, matching
# river's own fix): the smallest real nonzero probability in this
# request's own data is still meaningful signal and must not render
# indistinguishable from "no rain risk at all".
_PRECIP_PROB_BREAKS = [0.10, 0.25, 0.40, 0.60, 0.80]
_PRECIP_PROB_COLORS: list[tuple[int, int, int, int]] = [
    (222, 203, 228, 90),
    (188, 149, 213, 140),
    (152, 96,  198, 180),
    (117, 47,  177, 215),
    (76,  0,   153, 245),
]

_LATEST_PRECIP_RAW_SQL = """
    SELECT FORECAST_TIME, STAGE_PATH
    FROM AOTS.TC_ECMWF.MET_FORECASTS
    WHERE PARAM = 'tp'
    ORDER BY FORECAST_TIME DESC
    LIMIT 1
"""

_PRECIP_RAW_BY_TIME_SQL = """
    SELECT STAGE_PATH
    FROM AOTS.TC_ECMWF.MET_FORECASTS
    WHERE PARAM = 'tp' AND FORECAST_TIME = %s
"""

# Rolling prewarm window (see _prewarm_raw_caches's own comment): the 3 most
# recent DISTINCT real forecast times, not just the single latest one. DISTINCT
# matters here: MET_FORECASTS has many rows per FORECAST_TIME (one per param/
# tile), so a plain ORDER BY ... LIMIT 3 without it could return 3 rows that
# all share the same forecast_time instead of 3 different cycles.
_LATEST_3_PRECIP_RAW_SQL = """
    SELECT DISTINCT FORECAST_TIME, STAGE_PATH
    FROM AOTS.TC_ECMWF.MET_FORECASTS
    WHERE PARAM = 'tp'
    ORDER BY FORECAST_TIME DESC
    LIMIT 3
"""


def _precip_prob_key(window_h: int, threshold_mm: float) -> tuple[int, float]:
    """Normalizes a (window_h, threshold_mm) pair into the exact dict key
    ensure_precip_raw() precomputes prob grids under, rounds threshold_mm to
    1 decimal so float query-string round-tripping (e.g. "103.0" -> 103.0)
    can't silently miss an otherwise-identical precomputed entry."""
    return int(window_h), round(float(threshold_mm), 1)


class _PrecipRawCache:
    """Downloads+opens the latest global tp Zarr ONCE per forecast_time, keeps
    an ensemble-mean precip-RATE grid (mm over T+0->T+{window}h) PLUS
    exceedance-probability grids for every real (window_h, threshold_mm)
    combination the app's own ms-rain-window/ms-rain-slider controls can
    produce (see _PRECIP_RATE_WINDOWS_H/_PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM)
    in memory, all from that SAME single per-forecast_time download.

    Memory/compute tradeoff (explicitly decided, see ensure_precip_raw's own
    docstring for the full reasoning): precompute every (window, threshold)
    grid up front and discard the full per-member array before returning,
    rather than retaining the per-member rate_grid(s) to compute probability
    on demand per request. With 4 windows and ~3 thresholds each (+1 legacy
    default), that's ~17 small (n_lat, n_lon) float32 grids (~2.6MB each ->
    ~45MB per forecast_time) versus retaining 4 full (51, n_lat, n_lon)
    per-member arrays (~133MB EACH -> ~530MB per forecast_time), which would
    multiply across the rolling 3-forecast_time prewarm window into ~1.6GB of
    steady-state extra memory. Precomputing is far cheaper both in steady-
    state memory and in per-request compute (a real request is now a plain
    dict lookup, not a comparison+reduction over a retained (51, H, W) array).

    TTL: _PRECIP_RAW_TTL (4h), not _TILE_TTL (see module comment above).
    Thread-safe via double-checked locking, same pattern as _DataCache.
    """

    def __init__(self) -> None:
        self._grid: dict[str, dict] = {}       # forecast_time -> grid entry
        self._loaded_at: dict[str, float] = {}  # forecast_time -> epoch seconds
        # forecast_time -> epoch seconds of the last ensure_precip_raw() call
        # for it (hit OR fresh load); see _PREWARM_ACCESS_GRACE_SECONDS'
        # own comment for why this exists: without it, the stale-eviction
        # pass below only knows "latest-3 or not", not "in active use".
        self._accessed_at: dict[str, float] = {}
        # Per-forecast_time lock instead of one instance-wide lock: a single
        # process-wide lock would serialize loading EVERY distinct cycle,
        # so an unrelated cold miss (the rolling "latest 3" prewarm loop, a
        # fixed demo-scenario date, and a real user's own hover, each for a
        # DIFFERENT forecast_time) would queue behind whichever download
        # happened to be running, even though they share nothing. Same
        # convention as _DataCache's own _key_locks (see that class).
        self._key_locks: dict[str, threading.Lock] = {}
        self._key_locks_meta_lock = threading.Lock()
        # (forecast_time, stage_path, resolved_at): cheap SQL-only "latest" lookup,
        # cached separately from the (expensive) grid itself.
        self._latest_lock = threading.Lock()
        self._latest: Optional[tuple[str, str, float]] = None
        # A separate, short-TTL, single-entry cache for the full per-member
        # (51, n_lat, n_lon) rate array: deliberately NOT part of
        # self._grid/self._loaded_at above (which stays as memory-conscious
        # as this class's own docstring documents). At most ONE
        # (forecast_time, window_h) entry is ever resident at a time; a new
        # key wholesale-replaces the old one rather than accumulating,
        # see ensure_member_rate_grid's own docstring for the rationale.
        self._member_grid: dict = {}
        self._member_lock = threading.Lock()

    def _lock_for(self, forecast_time: str) -> threading.Lock:
        with self._key_locks_meta_lock:
            lock = self._key_locks.get(forecast_time)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[forecast_time] = lock
            return lock

    def _resolve_latest(self) -> Optional[tuple[str, str]]:
        now = time.time()
        if self._latest and (now - self._latest[2]) < _PRECIP_RAW_TTL:
            return self._latest[0], self._latest[1]
        with self._latest_lock:
            if self._latest and (now - self._latest[2]) < _PRECIP_RAW_TTL:
                return self._latest[0], self._latest[1]
            rows = _run_query(_LATEST_PRECIP_RAW_SQL, [])
            if not rows:
                return None
            forecast_time = str(rows[0]["FORECAST_TIME"])
            stage_path = rows[0]["STAGE_PATH"]
            self._latest = (forecast_time, stage_path, now)
            return forecast_time, stage_path

    def resolve_latest_forecast_time(self) -> Optional[str]:
        """Cheap (SQL-only, no download) lookup of the latest forecast_time."""
        resolved = self._resolve_latest()
        return resolved[0] if resolved else None

    def ensure_precip_raw(self, forecast_time: Optional[str]) -> Optional[str]:
        """Ensure the precip-rate grids for `forecast_time` are loaded in memory.

        `forecast_time` of None/""/"latest" always resolves to the current
        latest cycle. Returns the resolved forecast_time string actually
        loaded, or None if no tp data exists at all (for the requested time,
        or globally).

        Computes+caches a mean-rate grid per real window (_PRECIP_RATE_
        WINDOWS_H) and a probability grid per real (window, threshold)
        combination (_PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM, plus the legacy
        default pair) from the SAME single downloaded per-member array,
        see this class's own docstring for why precomputing up front (rather
        than retaining the per-member array for on-demand computation) was
        chosen.
        """
        latest = self._resolve_latest()
        if latest is None:
            return None
        latest_forecast_time, latest_stage_path = latest

        if forecast_time in (None, "", "latest"):
            forecast_time = latest_forecast_time

        # Marks forecast_time as in-use for the stale-eviction grace period
        # (see _PREWARM_ACCESS_GRACE_SECONDS) regardless of what happens
        # below: a cache hit, a fresh load, and the prewarm loop's own
        # warm-up call all mean the same thing here: "keep this around."
        self._accessed_at[forecast_time] = time.time()

        # stage_path is only needed to actually download something below;
        # the in-memory cache is checked FIRST, before ever resolving
        # stage_path, so the common "already warm" hover-lookup case
        # touches Snowflake zero times instead of running an uncached
        # query for a non-"latest" cycle on every call.
        if forecast_time in self._grid and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _PRECIP_RAW_TTL:
            return forecast_time

        if forecast_time == latest_forecast_time:
            stage_path = latest_stage_path
        else:
            rows = _run_query(_PRECIP_RAW_BY_TIME_SQL, [forecast_time])
            if not rows:
                return None
            stage_path = rows[0]["STAGE_PATH"]

        with self._lock_for(forecast_time):
            if forecast_time in self._grid and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _PRECIP_RAW_TTL:
                return forecast_time
            # forecast_time (and therefore stage_path) identifies an immutable,
            # already-published Zarr file: if we already have IT in memory, the
            # TTL lapsing just means "time to re-check for a NEWER cycle" (handled
            # above via _resolve_latest), not "re-download this same unchanged
            # file". Without this check, the hourly prewarm loop would re-fetch
            # the full multi-hundred-MB-to-1.2GB file every _PRECIP_RAW_TTL even
            # when no new cycle has landed (real cadence for tp can exceed 4h).
            if forecast_time in self._grid:
                self._loaded_at[forecast_time] = time.time()
                return forecast_time
            log.info("PrecipRaw: downloading+opening tp Zarr for %s (%s)…", forecast_time, stage_path)
            # Local import: pulls in the giga-spatial DataStore abstraction, which
            # this otherwise fully self-contained file never needs for anything else.
            from components.data.data_store_utils import get_data_store
            raw_bytes = get_data_store().read_file(stage_path)
            with tempfile.NamedTemporaryFile(suffix=".zarr.zip") as tmp:
                tmp.write(raw_bytes)
                tmp.flush()
                store = zarr.storage.ZipStore(tmp.name, mode="r")
                try:
                    root = zarr.open_group(store=store, mode="r")
                    arr = root["data"]  # shape (51, 25, 481, 1440), float16, mm accumulated from T+0
                    attrs = dict(root.attrs)
                    steps = list(attrs.get("steps", list(range(0, 150, 6))))
                    lat_min, lat_max = float(attrs["lat_min"]), float(attrs["lat_max"])
                    lon_min, lon_max = float(attrs["lon_min"]), float(attrs["lon_max"])

                    ia = steps.index(_PRECIP_RATE_STEP_A)
                    data_a = np.asarray(arr[:, ia, :, :]).astype(np.float32)  # (51, n_lat, n_lon), T+0

                    grids: dict[int, np.ndarray] = {}
                    prob_grids: dict[tuple[int, float], np.ndarray] = {}
                    n_lat = n_lon = None
                    for window_h in _PRECIP_RATE_WINDOWS_H:
                        if window_h not in steps:
                            # Defensive only: every real tp Zarr uses a 6-hourly step
                            # grid (0..144), which covers all 4 real windows exactly.
                            # A cycle with a genuinely truncated step list (e.g. a
                            # partial/degraded ingestion) just skips that window rather
                            # than crashing the whole cache load.
                            log.warning("PrecipRaw: window_h=%d not in this cycle's steps %s, skipping",
                                        window_h, steps)
                            continue
                        ib = steps.index(window_h)
                        data_b = np.asarray(arr[:, ib, :, :]).astype(np.float32)
                        # (51, n_lat, n_lon) mm over [T+0, T+window_h), freed at the end
                        # of this loop iteration (not retained across windows/requests,
                        # see this class's own docstring for why).
                        rate_grid = data_b - data_a
                        del data_b
                        # Ensemble mean across all 51 members -> one representative rate grid,
                        # conceptually mirroring wind/gust's own ensemble-probability coloring
                        # (an aggregate across members, not a single member's raw value).
                        grids[window_h] = np.nanmean(rate_grid, axis=0)  # (n_lat, n_lon)
                        if n_lat is None:
                            n_lat, n_lon = grids[window_h].shape

                        # Exceedance-probability reduction on the SAME per-member rate_grid,
                        # computed here (before it goes out of scope) rather than re-downloading
                        # the Zarr later for the probability variant. Mirrors the DATAPIPELINE
                        # repo's own precip_utils.exceedance_probability() idiom exactly: count
                        # members exceeding the threshold, divide by the fixed ensemble size (not
                        # rate_grid.shape[0]): NaNs compare False against the threshold, so they
                        # fall out as "non-exceeding" automatically, same documented convention.
                        #
                        # Real UI tiers for this window, PLUS the legacy default threshold
                        # (only relevant for the default window, but harmless/cheap to
                        # dedupe via `set` for every window), see
                        # _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM's own docstring.
                        thresholds = set(_PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM.get(window_h, []))
                        if window_h == _PRECIP_RATE_DEFAULT_WINDOW_H:
                            thresholds.add(_PRECIP_PROB_THRESHOLD_MM)
                        for threshold_mm in thresholds:
                            key = _precip_prob_key(window_h, threshold_mm)
                            prob_grids[key] = (
                                (rate_grid > threshold_mm).sum(axis=0)
                                / _PRECIP_PROB_ENSEMBLE_SIZE
                            ).astype(np.float32)  # (n_lat, n_lon), fraction in [0, 1]
                        del rate_grid
                    del data_a
                finally:
                    store.close()
            if not grids:
                log.error("PrecipRaw: no real window could be computed for %s (steps=%s)", forecast_time, steps)
                return None
            self._grid[forecast_time] = {
                "grids": grids,
                "prob_grids": prob_grids,
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max,
                "n_lat": n_lat, "n_lon": n_lon,
                # Grid row 0 = lat_max (rows run N->S), same convention as the
                # DATAPIPELINE repo's own tp Zarr reader (precip_utils.py).
                "lat_step": (lat_max - lat_min) / (n_lat - 1),
                "lon_step": (lon_max - lon_min) / (n_lon - 1),
            }
            self._loaded_at[forecast_time] = time.time()
            default_mean = grids.get(_PRECIP_RATE_DEFAULT_WINDOW_H, next(iter(grids.values())))
            finite = default_mean[np.isfinite(default_mean)]
            log.info("  PrecipRaw: grids ready %s (%dx%d, windows=%s, %d prob combos, default-window mean rate %.2f-%.2fmm)",
                      forecast_time, n_lat, n_lon, sorted(grids.keys()), len(prob_grids),
                      float(finite.min()) if finite.size else 0.0,
                      float(finite.max()) if finite.size else 0.0)
        return forecast_time

    def get_render_entry(self, forecast_time: str, window_h: int, threshold_mm: float) -> Optional[dict]:
        """Returns a flat {grid, prob_grid, lat_min, ...} dict for the given
        (window_h, threshold_mm): the same shape _render_dense_grid_webp/
        _sample_global_grid_tile already expect (and that _RiverExtentCache's
        own get_grid() also returns), so neither of those shared helpers
        needed to change for this per-window/per-threshold cache to exist.

        Returns None if `forecast_time` isn't loaded, or if the mean grid for
        `window_h` was never computed (e.g. a genuinely truncated cycle,
        see ensure_precip_raw's own "not in steps" guard)."""
        entry = self._grid.get(forecast_time)
        if entry is None:
            return None
        mean_grid = entry["grids"].get(int(window_h))
        if mean_grid is None:
            return None
        prob_grid = entry["prob_grids"].get(_precip_prob_key(window_h, threshold_mm))
        return {
            "grid": mean_grid,
            "prob_grid": prob_grid,
            "lat_min": entry["lat_min"], "lat_max": entry["lat_max"],
            "lon_min": entry["lon_min"], "lon_max": entry["lon_max"],
            "n_lat": entry["n_lat"], "n_lon": entry["n_lon"],
            "lat_step": entry["lat_step"], "lon_step": entry["lon_step"],
        }

    def ensure_member_rate_grid(self, forecast_time: Optional[str], window_h: int) -> Optional[str]:
        """Full per-member (51, n_lat, n_lon) rate grid for ONE window_h:
        genuinely ON-DEMAND (downloaded fresh from the same tp Zarr
        ensure_precip_raw uses, not derived from that class's own persistent
        aggregate cache, which deliberately discards rate_grid, see
        ensure_precip_raw's own docstring for why).

        Held for _PRECIP_MEMBER_TTL and capped at ONE resident entry: a
        new (forecast_time, window_h) key wholesale-replaces whatever was
        cached before, bounding worst-case extra memory to ~133MB for the
        length of that window, nowhere near the ~530MB/forecast_time this
        class's own docstring already rejected for persisting per-member
        data across every window/threshold combination. Backs BOTH the raw
        single-member precip tile layer and the rain-side cross-hazard
        worst-member scalar lookup: both only ever need ONE window_h active
        at a time in practice (the current ms-rain-window selection), so
        sharing one slot is sufficient.

        Returns the resolved forecast_time actually loaded, or None if no
        tp data exists for the request.
        """
        latest = self._resolve_latest()
        if latest is None:
            return None
        latest_forecast_time, latest_stage_path = latest
        if forecast_time in (None, "", "latest"):
            forecast_time = latest_forecast_time

        key = (forecast_time, int(window_h))
        now = time.time()
        cached = self._member_grid.get("entry")
        if cached and cached[0] == key and (now - cached[3]) < _PRECIP_MEMBER_TTL:
            return forecast_time

        with self._member_lock:
            cached = self._member_grid.get("entry")
            if cached and cached[0] == key and (now - cached[3]) < _PRECIP_MEMBER_TTL:
                return forecast_time
            if forecast_time == latest_forecast_time:
                stage_path = latest_stage_path
            else:
                rows = _run_query(_PRECIP_RAW_BY_TIME_SQL, [forecast_time])
                if not rows:
                    return None
                stage_path = rows[0]["STAGE_PATH"]

            log.info("PrecipRaw: downloading tp Zarr for per-member window_h=%d, %s (%s)…",
                      window_h, forecast_time, stage_path)
            from components.data.data_store_utils import get_data_store
            raw_bytes = get_data_store().read_file(stage_path)
            with tempfile.NamedTemporaryFile(suffix=".zarr.zip") as tmp:
                tmp.write(raw_bytes)
                tmp.flush()
                store = zarr.storage.ZipStore(tmp.name, mode="r")
                try:
                    root = zarr.open_group(store=store, mode="r")
                    arr = root["data"]
                    attrs = dict(root.attrs)
                    steps = list(attrs.get("steps", list(range(0, 150, 6))))
                    if window_h not in steps:
                        log.warning("PrecipRaw: window_h=%d not in this cycle's steps %s (member grid)",
                                    window_h, steps)
                        return None
                    ia, ib = steps.index(_PRECIP_RATE_STEP_A), steps.index(window_h)
                    data_a = np.asarray(arr[:, ia, :, :]).astype(np.float32)
                    data_b = np.asarray(arr[:, ib, :, :]).astype(np.float32)
                    rate_grid = data_b - data_a  # (51, n_lat, n_lon), kept resident this time
                    del data_a, data_b
                    lat_min, lat_max = float(attrs["lat_min"]), float(attrs["lat_max"])
                    lon_min, lon_max = float(attrs["lon_min"]), float(attrs["lon_max"])
                    n_lat, n_lon = rate_grid.shape[1], rate_grid.shape[2]
                    geo = {
                        "lat_min": lat_min, "lat_max": lat_max,
                        "lon_min": lon_min, "lon_max": lon_max,
                        "n_lat": n_lat, "n_lon": n_lon,
                        "lat_step": (lat_max - lat_min) / (n_lat - 1),
                        "lon_step": (lon_max - lon_min) / (n_lon - 1),
                    }
                finally:
                    store.close()
            self._member_grid = {"entry": (key, rate_grid, geo, now)}  # wholesale replace, at most 1 entry
        return forecast_time

    def get_member_rate_grid(self, forecast_time: str, window_h: int) -> Optional[tuple[np.ndarray, dict]]:
        """Returns (rate_grid (51, n_lat, n_lon), geo dict) if the exact
        (forecast_time, window_h) pair is the currently-resident member
        entry, else None (caller should have just called
        ensure_member_rate_grid, this doesn't itself trigger a load)."""
        cached = self._member_grid.get("entry")
        if cached is None or cached[0] != (forecast_time, int(window_h)):
            return None
        return cached[1], cached[2]

    def compute_member_metric_sums(self, forecast_time: str, window_h: int, threshold_mm: float,
                                     tile_lats: np.ndarray, tile_lons: np.ndarray,
                                     tile_metrics: dict[str, np.ndarray]) -> Optional[dict[str, np.ndarray]]:
        """For a country's own z14 tile centroids (tile_lats/tile_lons),
        each tile carrying several raw per-tile metric values
        (tile_metrics, e.g. population), returns {metric: (51,) ndarray}:
        member m's own sum of tile_metrics[metric][i] over every tile i
        where member m's own precip rate at that tile exceeds
        `threshold_mm` within `window_h`. ONE vectorized boolean-matrix @
        metric-matrix multiply, not 51 separate per-member passes.
        """
        resolved = self.ensure_member_rate_grid(forecast_time, window_h)
        if resolved is None:
            return None
        got = self.get_member_rate_grid(resolved, window_h)
        if got is None:
            return None
        rate_grid, geo = got
        if tile_lats.size == 0:
            return {name: np.zeros(rate_grid.shape[0]) for name in tile_metrics}
        lon_wrapped = ((tile_lons + 180.0) % 360.0) - 180.0
        lon_idx = (np.round((lon_wrapped - geo["lon_min"]) / geo["lon_step"]).astype(np.int64)) % geo["n_lon"]
        # Grid row 0 = lat_max (rows run N->S), same convention as
        # ensure_precip_raw/_sample_global_grid_tile use elsewhere.
        lat_idx = np.clip(np.round((geo["lat_max"] - tile_lats) / geo["lat_step"]).astype(np.int64),
                            0, geo["n_lat"] - 1)
        sampled = rate_grid[:, lat_idx, lon_idx]              # (51, n_tiles), one fancy-index shot
        exceeds = (sampled > threshold_mm).astype(np.float64)  # (51, n_tiles), NaN compares False, same
                                                                  # "non-exceeding" convention as ensure_precip_raw
        return {name: exceeds @ vals for name, vals in tile_metrics.items()}


_precip_cache = _PrecipRawCache()


def _colorize_precip_rate(vals: np.ndarray, breaks: list[float] = _PRECIP_RATE_BASE_BREAKS) -> np.ndarray:
    """Map a (H, W) grid of mm-over-window_h precip rate to an RGBA
    radar-style image.

    `breaks` (default the base 6h ramp for backward compatibility) should
    normally be _precip_rate_breaks_for_window(window_h)'s own per-window
    scaled breaks, see that function's own docstring for why a fixed ramp
    doesn't work across every real accumulation window. Colors always come
    from _PRECIP_RATE_COLORS (only the break POSITIONS scale, not the
    palette itself).
    """
    h, w = vals.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)
    finite = np.isfinite(vals)
    safe = np.where(finite, vals, -1.0)
    idx = np.digitize(safe, breaks)  # 0..len(breaks)
    for i, color in enumerate(_PRECIP_RATE_COLORS):
        mask = finite & (idx == i)
        if mask.any():
            img[mask] = color
    return img


def _colorize_precip_probability(vals: np.ndarray, min_val: float = 0.0, max_val: float = 1.0) -> np.ndarray:
    """Map a (H, W) grid of exceedance-probability fractions (0-1) to an RGBA
    sequential-purple image, DYNAMIC linear scale against `min_val`/`max_val`
    (real per-request range, see _PrecipRawCache's own caching of this pair
    and _fetch_precip_raw_tile's own call site for where they come from;
    matching river-raw's own conversion).

    See _PRECIP_PROB_COLORS above for the exact ramp; _PRECIP_PROB_BREAKS is
    no longer used here.
    """
    h, w = vals.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)
    finite = np.isfinite(vals) & (vals > 0)
    if not finite.any():
        return img
    if max_val <= min_val:
        max_val = min_val + 1e-6
    palette = np.asarray(_PRECIP_PROB_COLORS, dtype=np.uint8)
    n_colors = len(palette)
    t = np.clip((vals - min_val) / (max_val - min_val), 0.0, 1.0)
    idx = np.clip(np.floor(t * (n_colors - 1)).astype(np.int64), 0, n_colors - 1)
    img[finite] = palette[idx[finite]]
    return img


def _sample_global_grid_tile(entry: dict, grid: np.ndarray, z: int, x: int, y: int) -> Optional[np.ndarray]:
    """Sample a global dense lat/lon `grid` (row 0 = lat_max, N->S, the same
    convention every *_RawCache grid entry in this file uses) onto a 512x512
    Web-Mercator tile's pixel centers.

    Shared by precip-raw and river-raw raster tiles (both are "one global
    dense grid, no country/storm scoping" hazards) so they use identical
    Web-Mercator inversion / longitude-wrap / row-latitude math: this is a
    straight extraction of _fetch_precip_raw_tile's original inline version,
    parameterized by `entry`/`grid` instead of hardcoding the precip cache.

    Returns None if the tile is entirely outside the grid's latitude
    coverage, or if every sampled pixel would be non-finite (no data here).
    """
    lat_min, lat_max = entry["lat_min"], entry["lat_max"]
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    # Grid covers -60..60 latitude only (see module docstrings), tiles
    # entirely outside that band (poles) have no data at all; skip sampling.
    if tile_s >= lat_max or tile_n <= lat_min:
        return None

    n_lat, n_lon = entry["n_lat"], entry["n_lon"]
    lon_min = entry["lon_min"]
    lat_step, lon_step = entry["lat_step"], entry["lon_step"]

    # Column (longitude) sample points (linear in Web Mercator x), one per pixel center.
    px = np.arange(512)
    lons = tile_w + (tile_e - tile_w) * (px + 0.5) / 512.0
    lons_wrapped = ((lons + 180.0) % 360.0) - 180.0
    lon_idx = np.round((lons_wrapped - lon_min) / lon_step).astype(np.int64) % n_lon

    # Row (latitude) sample points: Web Mercator y is logarithmic in latitude,
    # so invert per-pixel exactly as _fetch_raster_tile does for z=14 cell bounds.
    merc_n = math.log(math.tan(math.pi / 4 + math.radians(tile_n) / 2))
    merc_s = math.log(math.tan(math.pi / 4 + math.radians(tile_s) / 2))
    py = np.arange(512)
    merc_y = merc_n - (merc_n - merc_s) * (py + 0.5) / 512.0
    lats = np.degrees(2.0 * np.arctan(np.exp(merc_y)) - math.pi / 2.0)
    lat_idx = np.round((lat_max - lats) / lat_step).astype(np.int64)
    valid_row = (lat_idx >= 0) & (lat_idx < n_lat)
    lat_idx_clipped = np.clip(lat_idx, 0, n_lat - 1)

    sampled = grid[np.ix_(lat_idx_clipped, lon_idx)]  # (512, 512): rows=py, cols=px
    sampled = np.where(valid_row[:, None], sampled, np.nan)

    if not np.isfinite(sampled).any():
        return None
    return sampled


def _sample_global_grid_point(entry: dict, grid: Optional[np.ndarray], lon: float, lat: float) -> Optional[float]:
    """Point-lookup counterpart to _sample_global_grid_tile: same grid
    convention (row 0 = lat_max, N->S). Used by the raw-layer hover-tooltip
    endpoints, which need a value at one point rather than rendering/
    sampling a full 512x512 tile."""
    if grid is None:
        return None
    lat_min, lat_max = entry["lat_min"], entry["lat_max"]
    if lat < lat_min or lat > lat_max:
        return None
    n_lat, n_lon = entry["n_lat"], entry["n_lon"]
    lon_min = entry["lon_min"]
    lat_step, lon_step = entry["lat_step"], entry["lon_step"]
    lon_wrapped = ((lon + 180.0) % 360.0) - 180.0
    lon_idx = int(round((lon_wrapped - lon_min) / lon_step)) % n_lon
    lat_idx = int(round((lat_max - lat) / lat_step))
    if lat_idx < 0 or lat_idx >= n_lat:
        return None
    val = grid[lat_idx, lon_idx]
    return None if not np.isfinite(val) else float(val)


def _render_dense_grid_webp(
    entry: Optional[dict], mode: str, mean_colorize, prob_colorize, z: int, x: int, y: int,
) -> Optional[bytes]:
    """Sample+colorize+encode one 512x512 RGBA WebP tile from a cached global
    dense grid entry (shape shared by _PrecipRawCache and _RiverRawCache, see
    _sample_global_grid_tile). `mode="mean"` uses `entry["grid"]` +
    `mean_colorize`; `mode="probability"` uses `entry["prob_grid"]` +
    `prob_colorize`. Both grids/colorizers come from the SAME cached
    per-forecast_time download: selecting "probability" never triggers a
    second download.

    Returns None if `entry` is missing/incomplete, the tile is outside the
    grid's coverage, or every sampled pixel is fully transparent.
    """
    if entry is None:
        return None
    is_probability = mode == "probability"
    grid = entry.get("prob_grid") if is_probability else entry.get("grid")
    if grid is None:
        return None
    colorize = prob_colorize if is_probability else mean_colorize

    sampled = _sample_global_grid_tile(entry, grid, z, x, y)
    if sampled is None:
        return None

    img_arr = colorize(sampled)
    if not img_arr[:, :, 3].any():
        return None

    img = Image.fromarray(img_arr, "RGBA")
    buf = io.BytesIO()
    img.save(buf, "WEBP", lossless=True, method=4)
    return buf.getvalue()


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_precip_raw_tile(
    forecast_time: str, z: int, x: int, y: int, mode: str = "mean",
    window_h: int = _PRECIP_RATE_DEFAULT_WINDOW_H, threshold_mm: float = _PRECIP_PROB_THRESHOLD_MM,
) -> bytes | None:
    """Render a 512x512 RGBA WebP tile from the cached global precip grid.

    `window_h`/`threshold_mm` select WHICH precomputed (window, threshold)
    grid pair to render (see _PrecipRawCache.get_render_entry), defaults
    match the original hardcoded T+0->T+6h/10mm behavior exactly, so existing
    callers that never pass either param are unaffected.

    `mode` selects which reduction of the selected pair is rendered:
    - "mean" (default, backward-compatible): ensemble-mean rate for
      `window_h`, radar-style green/yellow/orange/red ramp (see
      _colorize_precip_rate). `threshold_mm` is unused for this mode.
    - "probability": fraction of ensemble members exceeding `threshold_mm`
      within `window_h`, sequential-purple ramp (see
      _colorize_precip_probability). All grids come from the SAME cached
      per-forecast_time download: selecting any (window, threshold)
      combination never triggers a second Zarr fetch.

    Returns None if the tile is entirely outside the grid's -60..60 latitude
    coverage, if no data exists for `forecast_time`/`window_h`, or if every
    sampled pixel is transparent (no rain in this tile).
    """
    resolved = _precip_cache.ensure_precip_raw(forecast_time)
    if resolved is None:
        return None
    entry = _precip_cache.get_render_entry(resolved, window_h, threshold_mm)
    # mean_colorize applies window_h's own scaled ramp (see
    # _precip_rate_breaks_for_window's own docstring), bound via a closure
    # (not a functools.partial default swap) so _render_dense_grid_webp's
    # generic (vals) -> RGBA colorize signature stays unchanged for
    # river/other callers.
    window_breaks = _precip_rate_breaks_for_window(window_h)
    mean_colorize = lambda vals: _colorize_precip_rate(vals, window_breaks)
    # prob_colorize's own min/max: FIXED floor at 1/_PRECIP_PROB_ENSEMBLE_SIZE
    # (the smallest possible real nonzero exceedance fraction, ~1.96% for a
    # 51-member ensemble: a real 2% value
    # must always render as the lightest color on EVERY cycle, not just
    # cycles whose own true minimum happens to be near 2%, or the same
    # absolute number would look different cycle to cycle, which reads as
    # incoherent), DYNAMIC ceiling (this cycle's own real max, same
    # per-entry-not-per-tile computation as before, still avoids the
    # legend flicker/rescale-as-you-pan problem). Computed once and cached
    # on `entry` itself.
    prob_min_val, prob_max_val = 1.0 / _PRECIP_PROB_ENSEMBLE_SIZE, 1.0
    if entry is not None:
        minmax = entry.get("_prob_minmax")
        if minmax is None:
            prob_grid = entry.get("prob_grid")
            if prob_grid is not None:
                finite = np.isfinite(prob_grid) & (prob_grid > 0)
                nonzero = prob_grid[finite]
                max_val = float(nonzero.max()) if nonzero.size else 1.0
            else:
                max_val = 1.0
            minmax = (1.0 / _PRECIP_PROB_ENSEMBLE_SIZE, max(max_val, 1.0 / _PRECIP_PROB_ENSEMBLE_SIZE))
            entry["_prob_minmax"] = minmax
        prob_min_val, prob_max_val = minmax
    prob_colorize = lambda vals: _colorize_precip_probability(vals, prob_min_val, prob_max_val)
    return _render_dense_grid_webp(entry, mode, mean_colorize, prob_colorize, z, x, y)


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_precip_raw_tile_member(forecast_time: str, z: int, x: int, y: int,
                                    window_h: int, member: int) -> bytes | None:
    """Member-specific counterpart to _fetch_precip_raw_tile: renders ONE
    ensemble member's own real rate (mm over T+0->T+window_h), same
    radar-style ramp as the aggregate "mean" mode. No probability variant
    here (that's an across-member reduction with no single-member meaning).

    Sourced from _PrecipRawCache.ensure_member_rate_grid's own short-TTL,
    single-entry cache: NOT the persistent aggregate cache, which never
    retains per-member data (see that class's own docstring).
    """
    resolved = _precip_cache.ensure_member_rate_grid(forecast_time, window_h)
    if resolved is None:
        return None
    got = _precip_cache.get_member_rate_grid(resolved, window_h)
    if got is None:
        return None
    rate_grid, geo = got
    if member < 1 or member > rate_grid.shape[0]:
        return None
    entry = dict(geo, grid=rate_grid[member - 1])
    window_breaks = _precip_rate_breaks_for_window(window_h)
    mean_colorize = lambda vals: _colorize_precip_rate(vals, window_breaks)
    return _render_dense_grid_webp(entry, "mean", mean_colorize, _colorize_precip_probability, z, x, y)


# ---------------------------------------------------------------------------
# Global raw river FLOOD-EXTENT raster (RP10 per-member), NOT country/storm-
# scoped: THE CURRENT implementation behind /tiles/raster/river-raw/...,
# /stats/river-raw, /preload/river-raw. Replaces the dis24-discharge-based
# section above (now legacy, see the "LEGACY / SUPERSEDED" banner there).
#
# WHY THE SWITCH: raw dis24 discharge (m3/s) alone is not a meaningful "is
# this actually going to flood" signal: a river carrying 500 m3/s could be a
# completely normal Amazon-scale flow, or a genuinely dangerous flood on a
# small stream. RIVER_FORECASTS PARAM='extent_rp10_bymember' is GloFAS
# discharge ALREADY matched against the real JRC historical flood-extent
# raster at the RP10 (10-year return period) tier, upstream, in the
# TC-ECMWF-Forecast-Pipeline repo (glofas_extent_masking.py): each row IS a
# pixel that a specific ensemble member's RP10-exceeding discharge actually
# floods, per real JRC-observed flood geometry, not a synthetic per-cell
# threshold guess. RP10 is used (not RP2/RP5): those two tiers are confirmed
# IS_STANDIN=True placeholder/extrapolated data, whereas RP10 is the only
# tier that is a genuinely computed (IS_STANDIN=False) product right now.
#
# SCHEMA: one row per (pixel_lat, pixel_lon, member, step_h) that IS
# flooded: row existence alone means "member M's RP10 flood extent covers
# this pixel at lead time step_h". below_min_basin is a QC/confidence tag on
# an ALREADY-flooded row (small upstream drainage area -> lower confidence in
# the extent estimate), NOT a separate flood/no-flood indicator: a pixel
# with no row at all for a given member/step is simply "not flooded"; there
# is no explicit "False" row anywhere. This code deliberately does NOT filter
# on below_min_basin: dropping those rows would silently discard real (if
# lower-confidence) flood signal that the task never asked to exclude, and
# there is no separately-confirmed-safe cutoff to draw instead.
#
# STEP_H CHOICE: 24 (T+24h). Same reasoning as the OLD dis24 layer's own
# _RIVER_RAW_STEP_INDEX=0 choice above: extent data shares dis24's daily
# step cadence ([24, 48, 72, 96, 120, 144, 168], no T+0), so T+24h is again
# the earliest/most "current-ish" available lead time. Re-applying an
# already-justified convention rather than deriving a fresh one.
#
# RESOLUTION CHOICE: zoom-14 Web Mercator TILE granularity, the same
# granularity the downstream impact-calculation pipeline
# (Ahead-of-the-Storm-DATAPIPELINE/impact_analysis.py) intersects this
# exact per-member pixel data against for population/tile-level exposure.
# Native pixel spacing here is ~0.0013-0.0015deg (near-JRC 90-150m
# resolution); a z14 tile is ~2.45km at the equator, so each tile still
# collapses a small number of native pixels, but the map's own displayed
# granularity matches the unit the rest of the system reasons about a
# flooded area in. Because flood coverage is sparse (most of the world's
# ~2.68e8 possible z14 tiles have zero signal), the per-cycle result is
# stored as a SPARSE table (one row per distinct non-empty z14 tile,
# global counts are typically tens-of-thousands-to-low-hundreds-of-
# thousands), not a dense (n, n) array, which would be far too much memory
# (~268M cells) for what is overwhelmingly empty space.
#
# AGGREGATION / MEMORY SAFETY: the real per-forecast_time file is ~74M rows
# (28MB compressed) for the whole world, ALL step_h values combined. Loading
# it via pandas.read_parquet() in one shot risks a multi-GB memory spike (an
# estimated 3GB+) for data that, once filtered to step_h==24 and binned,
# collapses to a tiny footprint. Instead this reads the file via
# pyarrow.parquet.ParquetFile.read_row_group() in an explicit per-row-group
# loop (see _RiverExtentCache.ensure_river_extent): each row group is
# converted to numpy arrays, filtered to step_h==24, mapped to a real z14
# tile index per pixel via vectorized Web Mercator math (numpy, not a
# per-row mercantile.tile() call: that scalar function would be far too
# slow at these row counts), reduced to a small per-batch (distinct-tiles-
# in-this-row-group,) uint64 BITMASK via a vectorized groupby (one bit per
# ensemble member, dedup-by-OR, see below), and merged into the running
# global sparse dict before the row-group's raw arrays are discarded; no
# full-file DataFrame or per-pixel/per-tile dense array is ever
# materialized.
#
# DISTINCT-MEMBER COUNTING: one member's flood can span multiple native
# pixels that collapse into the SAME z14 output tile, so counting raw ROWS
# per tile would double/triple/... count a single member. Instead each row
# sets bit (member-1) of a per-tile np.uint64 (idempotent OR: setting the
# same member's bit twice from two colliding pixels is a no-op), so the
# final popcount of each tile's bitmask is a REAL count of DISTINCT members
# (0-51) that flood that tile at step_h=24, regardless of how many raw
# pixels contributed. 51 members fits comfortably in a uint64's 64 bits.
#
# PROBABILITY ONLY, no Mean mode: river only ever has ONE real per-cell
# metric, count-of-flooded-members / 51 (see above), a TRUE RP-tier
# exceedance fraction, no fabricated quantity involved. This is a MORE
# meaningful number than the old dis24 layer's own "probability" (only ever
# a fallback percentile-of-this-cycle's-own-data proxy, never a real
# return-period value). Rendered as a continuous cyan->navy gradient
# (_RIVER_EXTENT_PROB_COLORS).
#
# A Mean/Probability toggle is deliberately NOT offered for river the way
# it is for rain: unlike rain (which has both a real mm intensity AND a
# real exceedance-probability), river has no second independent quantity,
# so a "Mean" option would always describe the identical number under a
# different name. River always renders Probability; the Mean/Probability
# toggle (flood-view-as) is rain-only.
# ---------------------------------------------------------------------------

# rp10/rp20/rp50/rp100 are all real (IS_STANDIN=False), each its own
# distinct Parquet file. Only rp2/rp5 are IS_STANDIN=True: the pipeline's
# own way of flagging "not yet independently computed, this file just
# reuses rp10's own extent as a labelled UPPER-BOUND stand-in" until real
# rp2/rp5 computation exists. See TC-ECMWF-Forecast-Pipeline's own
# glofas_extent_masking.py module comment: flood extent grows
# monotonically with return period, so RP10's (rarer, more extensive)
# extent is a conservative OVERESTIMATE of RP2/RP5's true (smaller, less
# severe) extent, not a lower bound/underestimate.
_RIVER_EXTENT_RP_TIERS = ("rp2", "rp5", "rp10", "rp20", "rp50", "rp100")  # matches ms-river-slider's own _RIVER_RP_TIERS exactly
_RIVER_EXTENT_STANDIN_RP_TIERS = ("rp2", "rp5")  # IS_STANDIN=True, reuses rp10's own extent
_RIVER_EXTENT_DEFAULT_RP_TIER = "rp10"  # matches ms-river-slider's own default index (2 == "rp10")

# Hardcoded (not read from the Parquet's own member-count/array shape), same
# protective rationale as _PRECIP_PROB_ENSEMBLE_SIZE above: guards against a
# corrupt/short file silently inflating the probability fraction.
_RIVER_PROB_ENSEMBLE_SIZE = 51

# Real extent_rp10_bymember cycles land at most ~once/day in production,
# well past 4h, so this is purely "don't hammer the stage on every request"
# , same rationale as precip's own 4h TTL, reused directly.
_RIVER_EXTENT_TTL = _PRECIP_RAW_TTL

# The raw river-extent layer exposes a user-selectable lead-time control
# (ms-river-window, mirroring ms-rain-window's exact SegmentedControl
# pattern) rather than one hardcoded step_h: T+24h alone has zero flood
# signal for onboarded countries (river flooding is slow-onset, unlike
# wind), so a single fixed early lead time cannot represent every
# country/event. 72h is the default: the shortest lead time
# Ahead-of-the-Storm-DATAPIPELINE's own downstream impact pipeline treats
# as meaningful at all (its ingested FILE_PATHs for river never go below
# 72h: 24h/48h are never ingested downstream), and has decent coverage
# for onboarded countries (7,442 PHL / 115,776 BGD z14-pixel rows at 72h,
# vs 0/0 at 24h).
#
# ACCUMULATION SEMANTICS: the "1d/2d/3d.../7d" picker is a cumulative
# window, matching how Rainfall's own window control behaves (real
# T+0->T+window ACCUMULATED mm, monotonically non-decreasing as the
# window grows) rather than a single-day snapshot. River's source rows
# have no built-in cumulative field to lean on the way precip's tp Zarr
# does (tp is already stored cumulative-from-T+0 upstream, see
# _PrecipRawCache's own "shape (51, 25, 481, 1440)... mm accumulated from
# T+0" comment): each row here is a discrete "member M's RP-tier extent
# covers this pixel AT lead time step_h" fact, so accumulation is built
# explicitly: a pixel/member counts as flooded within a selected window
# if it floods at ANY lead time from 24h up through the selected step_h
# (`step_h_col <= step_h`, see the row-group loop in ensure_river_extent
# below): the per-tile bitmask OR-merge implements a set union by
# construction, since OR-ing bits from multiple qualifying days into the
# same running tile_bits dict is a union. Selecting "7d" is therefore the
# full 7-day flood footprint (every member that floods a pixel on ANY of
# the 7 days), monotonically non-decreasing as the window grows,
# matching Rainfall's own accumulation behavior conceptually, even though
# the underlying math differs (boolean set union of per-day member sets
# here, vs summed mm there).
#
# GRANULARITY IS DAILY-ONLY (24h steps), UNLIKE PRECIP'S 6H: this is a real
# data-cadence difference, not an arbitrary omission: GloFAS's own river
# discharge/extent product is emitted at daily lead times only (this list,
# [24, 48, ..., 168], IS GloFAS's real native step cadence, see this
# module's own "STEP_H CHOICE" comment above), whereas ECMWF's precipitation
# forecast (MET_FORECASTS/tp) is emitted 6-hourly, which is what lets
# Rainfall's own window control offer a real 6h option. There is no real 6h
# (or 12h) river-extent data anywhere upstream to accumulate even if this
# control offered it.
#
# SCOPE NOTE: this raw, country/storm-independent GLOBAL preview layer and
# the per-country IMPACT numbers (population/schools/HCs/shelters/WASH,
# MERCATOR_TILE_RIVER_MAT/ADMIN_ALL_RIVER_MAT/the 4 river facility tables,
# see those queries' own "River-flood sibling queries" comment above) both
# filter to the requested cumulative window (STEP_H), so their
# accumulation semantics are consistent with each other and with Rain's
# own windowed raw/impact-number paths.
#
# UI EXPOSES A SUBSET: pages/map_shell_concept.py's own ms-river-window
# control offers a 4-option subset (24/72/120/168h) in that file's own
# _RIVER_EXTENT_STEP_HOURS. This list stays the full 7-value set: it
# documents the underlying data cadence, not just what the UI currently
# exposes, and `step_h` here is unvalidated (any int works), so
# 48/96/144 remain reachable via a direct API call even though no UI
# button requests them.
_RIVER_EXTENT_STEP_HOURS = [24, 48, 72, 96, 120, 144, 168]
_RIVER_EXTENT_DEFAULT_STEP_H = 72

# The 4-value UI subset (matches pages/map_shell_concept.py's own
# ms-river-window _RIVER_EXTENT_STEP_HOURS exactly, per that file's own
# "UI-only subset" comment) is used ONLY by _prewarm_raw_caches'
# background loop below, to proactively warm every window a user could
# actually select, not just this cache's own 72h default. Deliberately a
# subset of the full 7-value _RIVER_EXTENT_STEP_HOURS above (not that full
# list): 48h/96h/144h have no UI button that could ever request them, so
# warming them in the background would be wasted Snowflake/stage I/O for
# data no request will ever need.
_PREWARM_RIVER_WINDOWS_H = [24, 72, 120, 168]

# See module comment above ("RESOLUTION CHOICE"), GloFAS's own native
# 0.05deg domain (-60..60 lat x -180..180 lon), rendered at real zoom-14 Web
# Mercator tile granularity (mirrors _fetch_raster_tile's own z14 quadkey
# rendering pattern) rather than a dense degree grid, see
# _RiverExtentCache.ensure_river_extent for the vectorized tile-index math.
_RIVER_EXTENT_ZOOM = 14  # matches MAT_ZOOM_LEVEL, real impact pipeline's own tile granularity
_RIVER_EXTENT_LAT_MIN = -60.0  # GloFAS's own native domain, informational only (not used for binning)
_RIVER_EXTENT_LAT_MAX = 60.0
_RIVER_EXTENT_LON_MIN = -180.0
_RIVER_EXTENT_LON_MAX = 180.0

_LATEST_RIVER_EXTENT_SQL = """
    SELECT FORECAST_TIME, STAGE_PATH
    FROM AOTS.TC_ECMWF.RIVER_FORECASTS
    WHERE PARAM = %s
    ORDER BY FORECAST_TIME DESC
    LIMIT 1
"""

_RIVER_EXTENT_BY_TIME_SQL = """
    SELECT STAGE_PATH
    FROM AOTS.TC_ECMWF.RIVER_FORECASTS
    WHERE PARAM = %s AND FORECAST_TIME = %s
"""


def _river_extent_param(rp_tier: str) -> str:
    """RIVER_FORECASTS' own PARAM string for a given rp_tier (e.g. 'rp10' ->
    'extent_rp10_bymember'): the naming convention every row in that
    table uses, across all 6 tiers."""
    return f"extent_{rp_tier}_bymember"

# Rolling prewarm window, see _LATEST_3_PRECIP_RAW_SQL's own comment for why
# DISTINCT is required (RIVER_FORECASTS has one row per pixel/member, not one
# per forecast_time). Parameterized by PARAM (rp_tier): the rolling prewarm
# covers all 6 return-period tiers, not just the default rp10, since the
# RP-tier slider is a frequently-used control, without prewarming, switching
# to any other tier would pay a full cold Parquet download+scan every time.
_LATEST_3_RIVER_EXTENT_SQL = """
    SELECT DISTINCT FORECAST_TIME, STAGE_PATH
    FROM AOTS.TC_ECMWF.RIVER_FORECASTS
    WHERE PARAM = %s
    ORDER BY FORECAST_TIME DESC
    LIMIT 3
"""

# Sequential cyan->navy ramp for the PROBABILITY variant (real per-cell
# exceedance fraction: count of the 51 members whose extent covers this
# tile / 51). Deliberately a different hue family from the OLD (legacy)
# river-probability teal ramp and from precip's purple ramp, so all three
# stay visually distinguishable.
#
# Bucket 0 (below 10% member-agreement) is a faint but visible version of
# the lightest hue, not fully transparent: the `probs > 0` filter already
# excludes true zeros upstream in _fetch_river_extent_raster_tile, so this
# bucket only ever receives already-filtered-nonzero probabilities (e.g. a
# member_count=1/51=1.96% cell). A single ensemble member predicting
# flooding here is meaningful, if low-confidence, signal, and rendering it
# fully transparent would make it visually indistinguishable from "no
# flood risk at all".
#
# Alpha values across all buckets are scaled toward full opacity (255) by
# ~30% at each tier relative to a plain linear ramp, meaningfully lifting
# the faint low end while barely touching the already-solid top end,
# preserving the low-to-high visual progression rather than flattening
# every tier to the same near-opaque look, since a flat low alpha (e.g.
# 40) blends to within a few RGB values of pure white against a light
# basemap and is functionally invisible despite carrying real signal.
#
# Buckets 0-2 (below 40% member-agreement) bumped again on top of that
# first pass (real user feedback: still too faint at the low end,
# especially once combined with the river-raw MapLibre layer's own
# 'raster-opacity': 0.75 multiplier in maplibre_tiles.js, which compounds
# against every bucket -- bucket 0's effective on-screen alpha was
# 105/255 x 0.75 ~= 31%, easy to lose against a light basemap). Buckets
# 3-5 left unchanged, already reasonably visible; ordering stays strictly
# increasing so the low-to-high progression is preserved, just steeper at
# the low end than the first pass left it.
_RIVER_EXTENT_PROB_BREAKS = [0.10, 0.25, 0.40, 0.60, 0.80]
_RIVER_EXTENT_PROB_COLORS: list[tuple[int, int, int, int]] = [
    (178, 235, 242, 150),
    (178, 235, 242, 180),
    (77,  208, 225, 195),
    (0,   172, 193, 203),
    (0,   105, 146, 227),
    (1,   50,  96,  248),
]
# Single-member raw-layer color: a specific member's own flood extent is
# a plain boolean (this tile floods for member N, or it doesn't), not a
# "fraction of members agree" continuum, so no gradient applies. Reuses
# the gradient's own darkest/most-saturated step rather than inventing a
# new color, keeping it visually consistent with the aggregate view's
# "high confidence" end.
_RIVER_EXTENT_MEMBER_COLOR: tuple[int, int, int, int] = _RIVER_EXTENT_PROB_COLORS[-1]

class _RiverExtentCache:
    """Downloads+processes the latest global extent_rp10_bymember Parquet
    ONCE per forecast_time via a memory-conscious pyarrow row-group loop (see
    the module-level comment above for the full memory-safety rationale,
    NEVER materializes the full ~74M-row file as one pandas DataFrame),
    producing a SPARSE per-zoom-14-tile DataFrame (one row per distinct
    z14 tile with >=1 flooded member): the SAME shape _fetch_raster_tile
    already expects (TILE_ID quadkey + BW/BS/BE/BN bounds), so tile
    rendering can reuse that function's own vectorized scatter-paint
    pattern (see _fetch_river_extent_raster_tile below) instead of the old
    dense-grid sample/colorize path (_sample_global_grid_tile/
    _render_dense_grid_webp, those remain unchanged and are still used by
    precip-raw, which stays on a dense global grid).

    Per-tile columns: 'TILE_ID' (str quadkey), 'BW'/'BS'/'BE'/'BN' (float
    z14 tile bounds), 'member_count' (int 0-51, real distinct-member
    popcount), 'probability' (float32 member_count/51, real per-cell RP10
    exceedance fraction, the one metric this layer renders, see
    _fetch_river_extent_raster_tile's own docstring below).

    TTL: _RIVER_EXTENT_TTL (4h). Thread-safe via double-checked locking, same
    pattern as _PrecipRawCache/_RiverRawCache above.
    """

    def __init__(self) -> None:
        # step_h joins the cache key alongside (forecast_time, rp_tier),
        # see _RIVER_EXTENT_STEP_HOURS' own comment for why a single
        # hardcoded lead time doesn't work.
        self._grids: dict[tuple[str, str, int], dict] = {}       # (forecast_time, rp_tier, step_h) -> grid entry
        self._loaded_at: dict[tuple[str, str, int], float] = {}   # (forecast_time, rp_tier, step_h) -> epoch seconds
        # (forecast_time, rp_tier) -> epoch seconds of the last
        # ensure_river_extent() call touching that pair (any step_h), same
        # eviction-grace mechanism as _PrecipRawCache._accessed_at, see
        # _PREWARM_ACCESS_GRACE_SECONDS' own comment. Keyed one level
        # coarser than self._grids (no step_h) to match the eviction pass's
        # own per-(forecast_time, rp_tier) granularity below.
        self._accessed_at: dict[tuple[str, str], float] = {}
        # Per-(forecast_time, rp_tier) lock -- that pair is the real download/
        # scan granularity (every step_h window is built from the same single
        # pass, see ensure_river_extent's own docstring), not the finer
        # (forecast_time, rp_tier, step_h) cache-entry key. Same reasoning as
        # _PrecipRawCache's own per-key lock: a single instance-wide lock
        # would serialize loading every distinct (date, tier) against every
        # other one, even across the rolling prewarm loop, a fixed demo date,
        # and a real user's own hover, none of which share anything.
        self._key_locks: dict[tuple[str, str], threading.Lock] = {}
        self._key_locks_meta_lock = threading.Lock()
        self._latest_lock = threading.Lock()
        self._latest: dict[str, tuple[str, str, float]] = {}  # rp_tier -> (forecast_time, stage_path, resolved_at)

    def _lock_for(self, key: tuple[str, str]) -> threading.Lock:
        with self._key_locks_meta_lock:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    def _resolve_latest(self, rp_tier: str) -> Optional[tuple[str, str]]:
        now = time.time()
        cached = self._latest.get(rp_tier)
        if cached and (now - cached[2]) < _RIVER_EXTENT_TTL:
            return cached[0], cached[1]
        with self._latest_lock:
            cached = self._latest.get(rp_tier)
            if cached and (now - cached[2]) < _RIVER_EXTENT_TTL:
                return cached[0], cached[1]
            rows = _run_query(_LATEST_RIVER_EXTENT_SQL, [_river_extent_param(rp_tier)])
            if not rows:
                return None
            # [:10]: RIVER_FORECASTS.FORECAST_TIME is a TIMESTAMP column
            # (always at real midnight for this table, but still a
            # TIMESTAMP type, not a DATE type), so str() of the raw
            # connector value returns "YYYY-MM-DD HH:MM:SS", not the plain
            # "YYYY-MM-DD" this whole class's own docstring promises and
            # every explicit (non-"latest") caller already passes. Without
            # this normalization, a "latest" resolution and an explicit
            # caller requesting the SAME real date land under two
            # DIFFERENT cache keys ("2026-07-02 00:00:00" vs "2026-07-02"),
            # each independently downloading+scanning the identical
            # parquet file, measured as two real ~35s loads for what
            # should have been one. [:10] is a safe no-op on an
            # already-plain date string too, so this stays correct
            # regardless of which form a given caller passes.
            forecast_time = str(rows[0]["FORECAST_TIME"])[:10]
            stage_path = rows[0]["STAGE_PATH"]
            self._latest[rp_tier] = (forecast_time, stage_path, now)
            return forecast_time, stage_path

    def resolve_latest_forecast_time(self, rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER) -> Optional[str]:
        """Cheap (SQL-only, no download) lookup of the latest forecast_time
        for a given rp_tier."""
        resolved = self._resolve_latest(rp_tier)
        return resolved[0] if resolved else None

    def ensure_river_extent(self, forecast_time: Optional[str],
                              rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER,
                              step_h: int = _RIVER_EXTENT_DEFAULT_STEP_H) -> Optional[str]:
        """Ensure the sparse zoom-14-tile flood-extent table for
        (`forecast_time`, `rp_tier`, `step_h`) is loaded in memory.

        `forecast_time` of None/""/"latest" always resolves to the current
        latest cycle FOR THAT TIER (different tiers can, in principle, have
        different latest dates, though in practice they land together).
        `rp_tier` defaults to rp10 (the slider's own default) for backward
        compatibility with every existing caller that doesn't pass it yet.
        `step_h` defaults to _RIVER_EXTENT_DEFAULT_STEP_H (72h), one of
        _RIVER_EXTENT_STEP_HOURS, the CUMULATIVE lead-time window to render:
        a member counts as flooding a pixel if it does so at ANY real lead
        time from 24h up through `step_h` (real union, not a single-day
        snapshot, see that constant's own "ACCUMULATION SEMANTICS" comment
        for the full rationale/history). Returns the resolved forecast_time
        string actually loaded (a
        plain date like "2026-07-14": this data is keyed by DATE, unlike
        dis24's full datetime FORECAST_TIME), or None if no data exists at
        all for this (forecast_time, rp_tier) combination, or globally for
        this tier: a real step_h with zero rows for the requested area
        still resolves (returns the date), just renders an empty tile.

        On a cold cache miss, this downloads the (forecast_time, rp_tier)
        parquet ONCE and builds grids for every UI-exposed window
        (_PREWARM_RIVER_WINDOWS_H), plus `step_h` itself if it falls outside
        that set, in the same pass, not just the single requested `step_h`.
        A caller asking for a different window shortly after (the common
        case: a user dragging the window slider) then hits an in-memory
        cache entry instead of re-downloading and re-scanning the identical
        source file, see this method's own load-loop comments for why this
        is safe (step_h is a cumulative filter, so every window's row
        selection is a subset of any larger window's).
        """
        latest = self._resolve_latest(rp_tier)
        if latest is None:
            return None
        latest_forecast_time, latest_stage_path = latest

        if forecast_time in (None, "", "latest"):
            forecast_time = latest_forecast_time

        # Same eviction-grace marking as ensure_precip_raw's own, see
        # _PREWARM_ACCESS_GRACE_SECONDS' own comment.
        self._accessed_at[(forecast_time, rp_tier)] = time.time()

        # Same pattern as ensure_precip_raw's own: check the in-memory
        # cache BEFORE ever resolving stage_path, since stage_path is only
        # needed to actually download something below. This means a
        # non-"latest" forecast_time that's already loaded and fresh in
        # memory (e.g. a hover over a fixed historical/demo date) never
        # runs a Snowflake query at all.
        key = (forecast_time, rp_tier, step_h)
        if key in self._grids and (time.time() - self._loaded_at.get(key, 0.0)) < _RIVER_EXTENT_TTL:
            return forecast_time

        if forecast_time == latest_forecast_time:
            stage_path = latest_stage_path
        else:
            rows = _run_query(_RIVER_EXTENT_BY_TIME_SQL, [_river_extent_param(rp_tier), forecast_time])
            if not rows:
                return None
            stage_path = rows[0]["STAGE_PATH"]

        with self._lock_for((forecast_time, rp_tier)):
            if key in self._grids and (time.time() - self._loaded_at.get(key, 0.0)) < _RIVER_EXTENT_TTL:
                return forecast_time
            if key in self._grids:
                self._loaded_at[key] = time.time()
                return forecast_time

            # Load every UI-exposed window (_PREWARM_RIVER_WINDOWS_H) in ONE
            # download+row-group pass, plus the actually-requested step_h if
            # it falls outside that set (e.g. a raw _RIVER_EXTENT_STEP_HOURS
            # value with no UI button, see that constant's own comment).
            # step_h is a CUMULATIVE filter (step_h_col <= threshold), so
            # every window's row-selection is a subset of any larger
            # window's. The network download and per-row-group Parquet
            # decompression, the genuinely expensive parts, previously ran
            # once PER window (ensure_river_extent(..., step_h=24) and
            # ensure_river_extent(..., step_h=72) each independently
            # re-downloaded and re-scanned the identical parquet file from
            # scratch); now they run exactly once regardless of how many
            # windows are requested. Only the cheap per-row-group tile
            # aggregation (np.unique + bitwise_or.at) repeats once per
            # window, applied to arrays already decoded in memory.
            step_hours = sorted(set(_PREWARM_RIVER_WINDOWS_H) | {step_h})
            log.info("RiverExtent: downloading+processing %s parquet for %s (%s), windows=%s…",
                      _river_extent_param(rp_tier), forecast_time, stage_path, step_hours)
            t0 = time.perf_counter()
            # Local import: same reasoning as _RiverRawCache/_PrecipRawCache above.
            from components.data.data_store_utils import get_data_store
            raw_bytes = get_data_store().read_file(stage_path)

            n14 = 1 << _RIVER_EXTENT_ZOOM  # 16384 tiles per axis at z=14
            max_step = step_hours[-1]
            # One SPARSE per-z14-tile member bitmask dict per window (NOT a
            # dense (n14, n14) array, see the "DISTINCT-MEMBER COUNTING"
            # module comment for why a sparse dict is exactly equivalent to,
            # and far cheaper than, a dense (tiles, 51) bool array). A
            # handful of these (one per UI window) stay modest even summed,
            # since each is bounded by the real distinct z14 tile count
            # (tens-of-thousands to low-hundreds-of-thousands globally, per
            # this function's own docstring), not the raw row count.
            tile_bits_by_step: dict[int, dict[int, int]] = {s: {} for s in step_hours}
            rows_scanned = 0
            rows_kept_by_step: dict[int, int] = {s: 0 for s in step_hours}
            members_seen_by_step: dict[int, set[int]] = {s: set() for s in step_hours}

            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                tmp.write(raw_bytes)
                tmp.flush()
                pf = pq.ParquetFile(tmp.name)
                n_row_groups = pf.metadata.num_row_groups
                for rg_idx in range(n_row_groups):
                    # Row-group-by-row-group (NOT pandas.read_parquet on the
                    # whole ~74M-row table at once), see module-level
                    # AGGREGATION / MEMORY SAFETY comment.
                    tbl = pf.read_row_group(
                        rg_idx, columns=["pixel_lat", "pixel_lon", "member", "step_h"]
                    )
                    rows_scanned += tbl.num_rows
                    # step_h_col (this row group's own per-row lead-time
                    # values). Uses `<=`, not `==`: this is a CUMULATIVE
                    # window (every lead time from 24h up through the
                    # selected threshold), not a single-day snapshot. See
                    # _RIVER_EXTENT_STEP_HOURS' own "ACCUMULATION SEMANTICS"
                    # comment above for the rationale. No other change is
                    # needed for this to be a set UNION across the
                    # qualifying days: rows from multiple days flow into the
                    # SAME per-batch/per-tile bitmask OR-merge below, and
                    # OR-ing member bits from day 1 and day 3 into the same
                    # tile's bitmask already is "member flooded this tile on
                    # day 1 OR day 3", exactly the desired union.
                    #
                    # First filter to the SUPERSET (largest requested
                    # window): everything downstream in this row group
                    # decodes/transforms that superset's rows exactly once,
                    # then each window below takes its own cheap boolean
                    # sub-mask over the already-decoded arrays.
                    step_h_col = tbl.column("step_h").to_numpy(zero_copy_only=False)
                    superset_mask = step_h_col <= max_step
                    if not superset_mask.any():
                        del tbl, step_h_col, superset_mask
                        continue
                    lat = tbl.column("pixel_lat").to_numpy(zero_copy_only=False)[superset_mask].astype(np.float64)
                    lon = tbl.column("pixel_lon").to_numpy(zero_copy_only=False)[superset_mask].astype(np.float64)
                    member = tbl.column("member").to_numpy(zero_copy_only=False)[superset_mask].astype(np.int64)
                    step_h_sub = step_h_col[superset_mask]
                    del tbl, step_h_col, superset_mask
                    if lat.size == 0:
                        continue

                    # Vectorized zoom-14 Web Mercator tile-index assignment:
                    # numpy math over the whole batch at once (NOT a per-row
                    # mercantile.tile() call, which is a slow scalar function
                    # and would dominate runtime at these row counts). Mirrors
                    # the standard slippy-map tile formula; clipping keeps a
                    # stray near-pole/antimeridian pixel inside the valid
                    # tile-index range instead of raising, same defensive
                    # spirit as _fetch_raster_tile's own clip()s. Computed
                    # ONCE per row group for the superset, then reused for
                    # every window via the sub-mask below.
                    lon_wrapped = ((lon + 180.0) % 360.0) - 180.0
                    x14 = np.floor((lon_wrapped + 180.0) / 360.0 * n14).astype(np.int64)
                    lat_clipped = np.clip(lat, -85.05112878, 85.05112878)
                    lat_rad = np.radians(lat_clipped)
                    y14 = np.floor(
                        (1.0 - np.log(np.tan(lat_rad) + 1.0 / np.cos(lat_rad)) / np.pi) / 2.0 * n14
                    ).astype(np.int64)
                    x14 = np.clip(x14, 0, n14 - 1)
                    y14 = np.clip(y14, 0, n14 - 1)
                    tile_flat = y14 * n14 + x14  # fits easily in int64 (max ~2.68e8)
                    bits = np.uint64(1) << (member - 1).astype(np.uint64)
                    del lat, lon, x14, y14, lat_clipped, lat_rad, lon_wrapped

                    # Per-window sub-mask over the already-decoded superset
                    # arrays, no re-read, no re-decompression, just a cheap
                    # boolean index, then the same per-batch groupby-then-
                    # merge reduction as before, once per window. A single
                    # row group can be millions of rows spanning a much
                    # smaller number of distinct z14 tiles, so duplicates
                    # within this batch are collapsed via a vectorized
                    # groupby (np.unique + bitwise_or.at over the batch's own
                    # small inverse-index array) BEFORE touching the running
                    # global dict for that window.
                    for s in step_hours:
                        thresh_mask = step_h_sub <= s
                        if not thresh_mask.any():
                            continue
                        tf = tile_flat[thresh_mask]
                        bb = bits[thresh_mask]
                        m = member[thresh_mask]
                        uniq_tiles, inverse = np.unique(tf, return_inverse=True)
                        batch_bits = np.zeros(uniq_tiles.size, dtype=np.uint64)
                        np.bitwise_or.at(batch_bits, inverse, bb)
                        td = tile_bits_by_step[s]
                        for tid, b in zip(uniq_tiles.tolist(), batch_bits.tolist()):
                            td[tid] = td.get(tid, 0) | b
                        rows_kept_by_step[s] += int(thresh_mask.sum())
                        members_seen_by_step[s].update(np.unique(m).tolist())
                    del tile_flat, bits, member, step_h_sub

            elapsed = time.perf_counter() - t0
            for s in step_hours:
                s_key = (forecast_time, rp_tier, s)
                tile_bits = tile_bits_by_step[s]
                # Popcount each tile's bitmask -> real distinct-member count
                # (0-51). A plain 51-iteration bit-shift loop over the
                # (n_tiles,) array of accumulated bitmasks is fast and
                # avoids materializing any (tiles, 51) intermediate.
                n_tiles = len(tile_bits)
                if n_tiles:
                    tile_ids_flat = np.fromiter(tile_bits.keys(), dtype=np.int64, count=n_tiles)
                    bits_arr = np.fromiter(tile_bits.values(), dtype=np.uint64, count=n_tiles)
                else:
                    tile_ids_flat = np.empty(0, dtype=np.int64)
                    bits_arr = np.empty(0, dtype=np.uint64)

                member_count = np.zeros(n_tiles, dtype=np.int32)
                for b in range(_RIVER_PROB_ENSEMBLE_SIZE):
                    member_count += ((bits_arr >> np.uint64(b)) & np.uint64(1)).astype(np.int32)
                # bits_arr is kept (not deleted) and stored below as the
                # "BITS" column, used for single-member raw-layer rendering
                # and cross-hazard per-member worst-case impact numbers (see
                # get_member_mask below).

                y14_arr = (tile_ids_flat // n14).astype(np.int64)
                x14_arr = (tile_ids_flat % n14).astype(np.int64)
                probability = member_count.astype(np.float32) / _RIVER_PROB_ENSEMBLE_SIZE

                # quadkey + real z14 tile bounds: mercantile has no
                # vectorized form for these, but this loop is bounded by the
                # real DISTINCT tile count for this window (tens-of-
                # thousands-to-low-hundreds-of-thousands globally per the
                # module comment), not the raw row count, so it is cheap
                # relative to the row-group scan above.
                tile_ids: list[str] = []
                bw: list[float] = []
                bs: list[float] = []
                be: list[float] = []
                bn: list[float] = []
                for xi, yi in zip(x14_arr.tolist(), y14_arr.tolist()):
                    tile_ids.append(mercantile.quadkey(xi, yi, _RIVER_EXTENT_ZOOM))
                    b = mercantile.bounds(xi, yi, _RIVER_EXTENT_ZOOM)
                    bw.append(b.west)
                    bs.append(b.south)
                    be.append(b.east)
                    bn.append(b.north)

                extent_df = pd.DataFrame({
                    "TILE_ID": tile_ids,
                    "BW": bw, "BS": bs, "BE": be, "BN": bn,
                    "member_count": member_count,
                    "probability": probability,
                    # Per-tile uint64 member bitmask (bit m-1 set <=> ensemble
                    # member m floods this tile), see the comment above
                    # bits_arr's own retention for why.
                    "BITS": bits_arr,
                })

                self._grids[s_key] = {
                    "df": extent_df,
                    "rp_tier": rp_tier,
                    "is_standin": rp_tier in _RIVER_EXTENT_STANDIN_RP_TIERS,
                    "rows_scanned": rows_scanned,
                    "rows_kept": rows_kept_by_step[s],
                    "n_members_seen": len(members_seen_by_step[s]),
                    # Shared across every window loaded in this same batch
                    # (the download+decode time, not attributable to any one
                    # window individually since it now serves all of them).
                    "load_seconds": elapsed,
                    # river_raw_tile_value's point lookup uses this dict rather
                    # than a linear `df[df['TILE_ID'] == qk]` scan across every
                    # distinct z14 tile in this table (tens of thousands to low
                    # hundreds of thousands globally, per this function's own
                    # comment above). Built once here, alongside the table
                    # itself (so it's naturally invalidated together whenever
                    # this entry reloads), it's an O(1) dict lookup.
                    "prob_by_tile": dict(zip(tile_ids, probability.tolist())),
                    # Same real fix as _DataCache._mercator_sorted: precomputed
                    # once here so _fetch_river_extent_raster_tile's own
                    # per-display-tile prefix filter is an O(log n) binary
                    # search instead of an O(n) scan across every distinct z14
                    # tile in this table, which is this cache's own single
                    # largest resident structure (see the log line just below).
                    "sorted_index": _build_sorted_tile_index(extent_df["TILE_ID"].to_numpy()),
                }
                self._loaded_at[s_key] = time.time()
                log.info("  RiverExtent: z14 tile table ready %s/%s@%sh (scanned %d rows, kept %d, "
                          "%d members seen, %d distinct z14 tiles with >=1 member flooded, %.1fs shared "
                          "across %d windows)",
                          rp_tier, forecast_time, s, rows_scanned, rows_kept_by_step[s],
                          len(members_seen_by_step[s]), n_tiles, elapsed, len(step_hours))
        return forecast_time

    def get_grid(self, forecast_time: str, rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER,
                  step_h: int = _RIVER_EXTENT_DEFAULT_STEP_H) -> Optional[dict]:
        return self._grids.get((forecast_time, rp_tier, step_h))

    def get_member_mask(self, forecast_time: str, rp_tier: str, step_h: int,
                          member: int) -> Optional[np.ndarray]:
        """Boolean array aligned to entry['df']'s row order: True where
        `member` (1-51) floods that z14 tile. Decoded on-demand from the
        already-resident BITS column: no re-scan of the source parquet,
        no extra download. Returns None only
        when the grid itself isn't loaded/empty (mirrors get_grid's own
        "caller must ensure_river_extent() first" contract)."""
        entry = self.get_grid(forecast_time, rp_tier, step_h)
        if entry is None or entry["df"].empty:
            return None
        bits = pd.to_numeric(entry["df"]["BITS"], errors="coerce").fillna(0).to_numpy(dtype=np.uint64)
        return ((bits >> np.uint64(member - 1)) & np.uint64(1)).astype(bool)


_river_extent_cache = _RiverExtentCache()


# Per-tile facility/population columns (E_population, E_num_schools, ...)
# on the country's own base z14 tile grid, mapped by TILE_ID -> raw count
# column, used by compute_river_member_metric_sums below (and
# rain_member_impacts too (see that endpoint's own docstring) to turn a
# per-member boolean/exceedance tile mask into per-member impact sums
# (population/schools/HCs/shelters/WASH/children/built-up), the same
# metrics the wind-only "Compare Worst Case By" feature tracks per member
# via TRACK_MAT. Shared across hazards (not river-specific despite the
# name's history) since both River and Rain sample the exact same
# get_base_tiles() columns, just via a different per-member boolean mask.
_MEMBER_METRIC_COLS: dict[str, str] = {
    "E_population": "population",
    "E_school_age_population": "school_age_population",
    "E_infant_population": "infant_population",
    "E_adolescent_population": "adolescent_population",
    "E_built_surface_m2": "built_surface_m2",
    "E_num_schools": "num_schools",
    "E_num_hcs": "num_hcs",
    "E_num_shelters": "num_shelters",
    "E_num_wash": "num_wash",
}

# Facility metrics only: real, genuine per-country absences exist for
# these (e.g. Turks and Caicos Islands' all-NULL severity_num_shelters),
# so combined_member_impacts reports a real None rather than a fabricated
# 0 for these specific keys when the raw column has zero real values
# anywhere in a country's own tile set. Population/children/built-up stay
# out of this set deliberately, matching `_v` (always-real, never None)
# vs `_v_or_none` (facility-only) in pages/map_shell_concept.py's own
# _real_member_stats wind-only body.
_MEMBER_METRIC_NULLABLE_COLS = frozenset({"E_num_schools", "E_num_hcs", "E_num_shelters", "E_num_wash"})


def compute_river_member_metric_sums(entry: dict, base_df: pd.DataFrame) -> Optional[dict[str, np.ndarray]]:
    """For EVERY one of the 51 ensemble members at once, the real sum of
    each impact metric (population, schools, HCs, ...) over exactly the
    z14 tiles that member's own bit marks as flooded in `entry` (an
    _RiverExtentCache.get_grid(...) result). Returns
    {"E_population": (51,) ndarray, ...}: ONE vectorized bit-decompose
    ((n_tiles,51) boolean matrix) + matmul against `base_df`'s own raw
    per-tile counts, NOT 51 separate per-member passes: this is the
    "spread across members is nearly free" property the accompanying
    cross-hazard worst-case feature's own plan relies on for staying
    genuinely on-demand.

    `base_df` must carry a 'TILE_ID' column (same z14 quadkey format as
    entry['df']) plus whichever of _MEMBER_METRIC_COLS' raw columns are
    available (missing columns are treated as all-zero, matching this
    codebase's existing "missing facility column -> 0, not fabricated"
    convention elsewhere).
    """
    df = entry.get("df")
    if df is None or df.empty or base_df is None or base_df.empty or "TILE_ID" not in base_df.columns:
        return None
    merged = base_df.merge(df[["TILE_ID", "BITS"]], on="TILE_ID", how="inner")
    if merged.empty:
        return None
    bits = pd.to_numeric(merged["BITS"], errors="coerce").fillna(0).to_numpy(dtype=np.uint64)
    n_members = _RIVER_PROB_ENSEMBLE_SIZE
    # (n_tiles, n_members) boolean membership matrix, one shot, vectorized.
    member_matrix = ((bits[:, None] >> np.arange(n_members, dtype=np.uint64)) & np.uint64(1)).astype(np.float64)
    out: dict[str, np.ndarray] = {}
    for e_col, raw_col in _MEMBER_METRIC_COLS.items():
        vals = merged[raw_col].fillna(0).to_numpy(dtype=np.float64) if raw_col in merged.columns else np.zeros(len(merged))
        out[e_col] = member_matrix.T @ vals  # (n_members,)
    return out


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_river_extent_raster_tile(forecast_time: str, z: int, x: int, y: int,
                                       rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER,
                                       step_h: int = _RIVER_EXTENT_DEFAULT_STEP_H,
                                       member: Optional[int] = None) -> bytes | None:
    """Render a 512x512 RGBA WebP tile from the cached sparse zoom-14-tile
    flood-extent table (see _RiverExtentCache): THE current implementation
    behind /tiles/raster/river-raw/.../*.webp.

    `member` (1-51, or None for the default aggregate view): when set,
    renders ONLY that one ensemble member's own flood extent (decoded from
    the per-tile BITS bitmask via _RiverExtentCache.get_member_mask) as a
    flat single-color mask (_RIVER_EXTENT_MEMBER_COLOR) instead of the
    continuous member-agreement gradient below: a specific member has no
    "fraction agree" concept.

    Mirrors _fetch_raster_tile's own quadkey-filter + vectorized
    scatter-paint pattern almost exactly (same TILE_ID/BW/BS/BE/BN shape,
    same z14 rendering regardless of display zoom `z`) rather than the
    dense-grid sample/colorize path (_sample_global_grid_tile/
    _render_dense_grid_webp, unchanged, still used by precip-raw).

    Renders the continuous per-tile exceedance fraction (cyan->navy
    gradient, more member agreement = darker blue (see
    _RIVER_EXTENT_PROB_BREAKS/_COLORS, applied via np.digitize) by
    default. River has no Mean/Probability toggle: unlike rain, which has
    both a real mm intensity AND a real exceedance-probability, river only
    has this ONE per-cell metric, so a "Mean" mode would only describe the
    identical number under a different name/colour. flood-view-as
    (Mean/Probability) is rain-only. `step_h` (see _RIVER_EXTENT_STEP_HOURS'
    own comment) is the CUMULATIVE lead-time window to render (a member
    counts as flooding a pixel if it does so at ANY day from 24h through
    `step_h`, a set union, not a single-day snapshot), mirroring rain's
    own window selector conceptually.
    """
    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier, step_h)
    if resolved is None:
        return None
    entry = _river_extent_cache.get_grid(resolved, rp_tier, step_h)
    if entry is None:
        return None
    df = entry.get("df")
    if df is None or df.empty:
        return None

    # Geographic bounds for the requested display tile, same Web Mercator
    # setup as _fetch_raster_tile.
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    tile_dw = tile_e - tile_w
    _merc_tile_n = math.log(math.tan(math.pi / 4 + math.radians(tile_n) / 2))
    _merc_tile_s = math.log(math.tan(math.pi / 4 + math.radians(tile_s) / 2))
    _merc_tile_dh = _merc_tile_n - _merc_tile_s

    # Filter z=14 rows that fall within this display tile via quadkey
    # prefix, identical helper/pattern to _fetch_raster_tile.
    like_pat = _quadkey_like_pattern(z, x, y)
    sub = _filter_by_tile_prefix(df, like_pat, entry.get("sorted_index"))
    if sub.empty:
        return None

    img_arr = np.zeros((512, 512, 4), dtype=np.uint8)

    ws = sub['BW'].to_numpy(dtype=np.float64)
    ss = sub['BS'].to_numpy(dtype=np.float64)
    es = sub['BE'].to_numpy(dtype=np.float64)
    en = sub['BN'].to_numpy(dtype=np.float64)
    ns = en  # keep the same short local name the rest of this function already uses below
    _bounds_valid = np.isfinite(ws) & np.isfinite(ss) & np.isfinite(es) & np.isfinite(ns)

    if member is not None:
        # Single-member view: a flat boolean mask decoded from the
        # already-resident BITS column, painted with ONE fixed color (see
        # _RIVER_EXTENT_MEMBER_COLOR's own comment for why no gradient
        # applies here).
        bits = pd.to_numeric(sub['BITS'], errors='coerce').fillna(0).to_numpy(dtype=np.uint64)
        flooded = ((bits >> np.uint64(member - 1)) & np.uint64(1)).astype(bool)
        valid = flooded & _bounds_valid
        ws, ss, es, ns = ws[valid], ss[valid], es[valid], ns[valid]
        if len(ws) == 0:
            return None
        # A single-entry "palette" (one color, index 0): every surviving
        # tile row's idx points at that same entry, reusing the exact same
        # idx/palette_rgba_arr[np.repeat(idx, counts)] scatter-paint pattern
        # below unchanged (idx has one entry PER TILE ROW here, same
        # contract as the gradient branch's np.digitize(...) result).
        palette_rgba_arr = np.asarray([_RIVER_EXTENT_MEMBER_COLOR], dtype=np.uint8)
        idx = np.zeros(len(ws), dtype=np.int64)
    else:
        # The one real per-tile member-agreement fraction (see this
        # function's own docstring): continuous cyan->navy gradient.
        # FIXED floor at 1/_RIVER_PROB_ENSEMBLE_SIZE (the smallest possible
        # real nonzero member-agreement fraction, ~1.96% for 51 members),
        # DYNAMIC ceiling: a real 2% value
        # must always render as the lightest color on EVERY cycle, not
        # only cycles whose own true minimum happens to be near 2%, or the
        # same absolute number looks different cycle to cycle, which reads
        # as incoherent). Replaces the old fixed absolute breaks
        # (_RIVER_EXTENT_PROB_BREAKS, still defined for
        # _RIVER_EXTENT_PROB_COLORS' own shape but no longer used to
        # bucket values here). Ceiling computed once per (forecast_time,
        # rp_tier, step_h) cache entry, from every real nonzero
        # probability in the FULL loaded dataset (not just this one
        # display tile's own rows, which would flicker/rescale as you pan,
        # same reasoning _fetch_combined_raster_tile's own min_val/max_val
        # comment gives), and cached on `entry` itself.
        probs = pd.to_numeric(sub['probability'], errors='coerce').to_numpy(dtype=np.float64)
        valid = np.isfinite(probs) & (probs > 0) & _bounds_valid
        probs, ws, ss, es, ns = probs[valid], ws[valid], ss[valid], es[valid], ns[valid]
        if len(probs) == 0:
            return None
        river_floor = 1.0 / _RIVER_PROB_ENSEMBLE_SIZE
        minmax = entry.get("_prob_minmax")
        if minmax is None:
            all_probs = pd.to_numeric(entry["df"]['probability'], errors='coerce').to_numpy(dtype=np.float64)
            nonzero = all_probs[np.isfinite(all_probs) & (all_probs > 0)]
            max_val = float(nonzero.max()) if len(nonzero) else river_floor
            minmax = (river_floor, max(max_val, river_floor))
            entry["_prob_minmax"] = minmax
        min_val, max_val = minmax
        if max_val <= min_val:
            max_val = min_val + 1e-6
        palette_rgba_arr = np.asarray(_RIVER_EXTENT_PROB_COLORS, dtype=np.uint8)
        n_colors = len(palette_rgba_arr)
        t = np.clip((probs - min_val) / (max_val - min_val), 0.0, 1.0)
        idx = np.clip(np.floor(t * (n_colors - 1)).astype(np.int64), 0, n_colors - 1)

    # Map z=14 tile bounds to pixel coordinates: identical formulas to
    # _fetch_raster_tile (see that function's own comment for the full
    # floor()/+1 boundary-alignment rationale).
    px0 = np.floor((ws - tile_w) / tile_dw * 512).astype(np.int32)
    px1 = np.floor((es - tile_w) / tile_dw * 512).astype(np.int32) + 1
    merc_ns = np.log(np.tan(np.pi / 4 + np.radians(ns) / 2))
    merc_ss = np.log(np.tan(np.pi / 4 + np.radians(ss) / 2))
    py0 = np.floor((1.0 - (merc_ns - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32)
    py1 = np.floor((1.0 - (merc_ss - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32) + 1

    # Vectorized scatter-paint: copied verbatim from _fetch_raster_tile
    # (see that function's own comment for the full "why"; mathematically
    # equivalent to a per-row loop, reused here rather than rederived).
    x0v = np.maximum(0, px0)
    y0v = np.maximum(0, py0)
    x1v = np.minimum(512, np.maximum(x0v + 1, px1))
    y1v = np.minimum(512, np.maximum(y0v + 1, py1))
    # No separate "finite" re-check here: ws/ss/es/ns/idx were already
    # filtered to fully-valid rows above (per mode), unlike
    # _fetch_raster_tile's own version of this block (which paints straight
    # from an unfiltered per-country DataFrame and still needs to skip NaN
    # values inline).
    wv = np.maximum(0, x1v - x0v)
    hv = np.maximum(0, y1v - y0v)
    counts = (wv * hv).astype(np.int64)
    total = int(counts.sum())

    if total > 0:
        cum_start = np.concatenate(([0], np.cumsum(counts)[:-1]))
        pixel_offset = np.arange(total) - np.repeat(cum_start, counts)
        w_per_pixel = np.repeat(wv, counts)
        dy = pixel_offset // w_per_pixel
        dx = pixel_offset % w_per_pixel
        abs_y = np.repeat(y0v, counts) + dy
        abs_x = np.repeat(x0v, counts) + dx
        colors = palette_rgba_arr[np.repeat(idx, counts)]
        flat_idx = abs_y.astype(np.int64) * 512 + abs_x.astype(np.int64)
        img_arr.reshape(-1, 4)[flat_idx] = colors

    if not img_arr.any():
        return None

    img = Image.fromarray(img_arr, 'RGBA')
    buf = io.BytesIO()
    img.save(buf, 'WEBP', lossless=True, method=4)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Proactive pre-warming of the global raw layers (precip-raw, river-raw)
#
# Both _PrecipRawCache/_RiverExtentCache are only ever filled lazily today, on
# whichever real map-tile/preload request happens to hit them first after a
# cold start or TTL expiry -- meaning one real user's request would otherwise
# eat the full ~1.2GB (tp) / real extent_rp10_bymember Parquet download+decode
# latency inline. The loop below runs in a single background daemon thread
# started from the app's lifespan, so the process becomes ready to serve
# other requests immediately (no request ever blocks on it), and re-checks
# well before the 4h TTL (_PRECIP_RAW_TTL/_RIVER_EXTENT_TTL) lapses so that,
# under normal continuous operation, the cache is realistically never cold
# for a real user -- only on a genuinely fresh process (re)start.
# ---------------------------------------------------------------------------

# 1h: comfortably (4x) inside the 4h cache TTL, so a cycle never lapses
# between re-checks, while still being cheap to run indefinitely -- when
# already warm, ensure_precip_raw()/ensure_river_raw() short-circuit via
# their own double-checked locking (a fresh-enough _latest/_grid|_points
# entry is returned via plain dict/attribute lookups, no lock, no SQL, no
# download) before ever reaching the expensive download path.
_PREWARM_INTERVAL_SECONDS = 60 * 60

# Short pause inserted between each individual prewarm sub-task (one
# precip-raw grid, one river-raw (date, tier, window) combination), NOT
# between prewarm cycles. Zarr/Parquet decode is CPU-bound Python work
# that holds the GIL for extended stretches, and this loop is a single
# daemon thread running back-to-back through ~72+ combinations per cold
# cycle: without a pause, a live request's own thread can be starved of
# GIL time for the full duration of that stretch, showing up as visible
# tile/hover latency spikes while a prewarm cycle runs. 50ms per sub-task
# adds well under 4s total to a cold cycle (worst case ~75 sub-tasks),
# trivial against the 1h interval, while giving the scheduler a real
# chance to switch to a waiting live-request thread between tasks.
_PREWARM_YIELD_SECONDS = 0.05

# Grace period during which a precip-raw/river-raw entry is exempt from the
# stale-eviction pass below EVEN IF its forecast_time has fallen out of the
# "latest 3" window, as long as something (a real request OR the prewarm
# loop's own warm-up call) has touched it within this window. Real bug this
# fixes, confirmed live: a user viewing a non-latest forecast_time (e.g. an
# older cycle picked from the date/time selector) had their in-memory grid
# evicted ~23s after it finished loading, on the very next prewarm pass --
# eviction only ever checked "is this in latest-3", never "was this just
# used" -- forcing a full ~60-160s reload (Zarr/Parquet download + GIL-bound
# decode, see _PREWARM_YIELD_SECONDS' own comment) on their next tile
# request, and again on every subsequent prewarm pass for as long as they
# kept looking at it. 20 minutes comfortably covers one real viewing
# session while still bounding steady-state memory for genuinely abandoned
# views (loosely matches _PRECIP_RAW_TTL/_RIVER_EXTENT_TTL's own 4h order of
# magnitude, just short enough to actually free memory across a session).
_PREWARM_ACCESS_GRACE_SECONDS = 20 * 60


def _prewarm_raw_caches() -> None:
    """Background loop (single daemon thread): keeps a ROLLING WINDOW of the
    3 most recent real forecast times for precip-raw and river-raw (now
    extent_rp10_bymember-based, see _RiverExtentCache) always warm, re-checked
    every _PREWARM_INTERVAL_SECONDS. Reuses ensure_precip_raw()/
    ensure_river_extent() directly (same functions the /preload/* endpoints
    and the raw tile endpoints call) -- no HTTP round-trip within the
    process. Each call is a fast no-op when the cache is already warm; see
    _PrecipRawCache/_RiverExtentCache double-checked locking. Errors are
    logged and swallowed so a warm-up failure (e.g. Snowflake hiccup) never
    crashes this thread or blocks the server.

    Keeps the latest 3 distinct forecast times warm rather than only the
    single latest one, so a user looking at yesterday's or the day-before's
    real data (a completely normal thing to do) does not pay a full cold
    ~1.2GB Zarr / real Parquet download. Each cycle re-resolves the real
    latest-3-distinct-times set from Snowflake (_LATEST_3_PRECIP_RAW_SQL/
    _LATEST_3_RIVER_EXTENT_SQL) and evicts any previously-warmed entry that
    has since aged out of that window, so the process doesn't grow
    unbounded: each precip-raw grid alone can be sized in the hundreds of
    MB once decoded.

    River-raw warms all 6 return-period tiers (rp2/rp5/rp10/rp20/rp50/
    rp100), not just the default rp10, since the RP-tier slider is an
    interactive control, without this, switching tiers would pay a cold
    5-20s Parquet download for every user, every time, for any tier
    beyond the one default. ~18 (date, tier) combinations total per cycle
    before the window fan-out below, still a background, non-blocking
    loop.

    Warms all 4 UI-exposed windows (deliberately NOT the full 7-value
    backend set _RIVER_EXTENT_STEP_HOURS documents, 48h/96h/144h have no
    UI button that could ever request them, see that constant's own
    comment, warming them here would be wasted background I/O) per
    (date, tier), bringing the total to ~72 (date, tier, window)
    combinations per cycle. Without this, the other 3 UI window options
    (24/120/168h, see pages/map_shell_concept.py's own ms-river-window)
    would stay cold regardless of how long the server had been running,
    costing a fresh 6-46s GloFAS Parquet re-download+re-scan the first
    time any user selects one of them."""
    while True:
        try:
            rows = _run_query(_LATEST_3_PRECIP_RAW_SQL, [])
            latest_times = {str(r["FORECAST_TIME"]) for r in rows}
        except Exception as e:
            log.error("Prewarm: could not resolve latest-3 precip-raw times: %s", e)
            latest_times = None
        if latest_times is not None:
            if not latest_times:
                log.info("Prewarm: precip-raw, no data currently available to warm")
            else:
                for forecast_time in latest_times:
                    try:
                        resolved = _precip_cache.ensure_precip_raw(forecast_time)
                        if resolved is not None:
                            log.info("Prewarm: precip-raw cache warm (forecast_time=%s)", resolved)
                    except Exception as e:
                        log.error("Prewarm: precip-raw warm-up failed for %s: %s", forecast_time, e)
                    time.sleep(_PREWARM_YIELD_SECONDS)
                # Per-key locks (see _PrecipRawCache.__init__'s own comment)
                # mean there's no single lock left to hold for the whole
                # eviction pass; instead each stale key is popped under its
                # OWN lock, so an eviction can never race an in-progress
                # load for that exact key while still letting unrelated
                # keys load concurrently during eviction. try/except: a
                # concurrent insert could in principle make this dict-view
                # snapshot racy (CPython dict iteration isn't safe against
                # concurrent resize), this is a background daemon thread
                # with no caller to propagate an error to, so failing this
                # one pass (next prewarm cycle retries) beats silently
                # killing the whole loop.
                try:
                    now = time.time()
                    stale = {
                        k for k in _precip_cache._grid.keys()
                        if k not in latest_times
                        and (now - _precip_cache._accessed_at.get(k, 0.0)) >= _PREWARM_ACCESS_GRACE_SECONDS
                    }
                    for forecast_time in stale:
                        with _precip_cache._lock_for(forecast_time):
                            _precip_cache._grid.pop(forecast_time, None)
                            _precip_cache._loaded_at.pop(forecast_time, None)
                            _precip_cache._accessed_at.pop(forecast_time, None)
                except Exception as e:
                    log.error("Prewarm: precip-raw stale-eviction pass failed: %s", e)
                    stale = set()
                if stale:
                    log.info("Prewarm: evicted %d stale precip-raw grid(s) outside the latest-3 window: %s",
                              len(stale), sorted(stale))

        # River-raw: one latest-3 resolution PER rp_tier, each tier is its
        # own distinct Parquet file/forecast_time set (rp2's own file, in
        # particular, has FAR more raw rows than rp10's, not just a smaller
        # copy of it), so each needs its own query and its own eviction
        # pass keyed to (forecast_time, that_tier) only.
        for rp_tier in _RIVER_EXTENT_RP_TIERS:
            try:
                rows = _run_query(_LATEST_3_RIVER_EXTENT_SQL, [_river_extent_param(rp_tier)])
                # [:10]: same TIMESTAMP-vs-DATE normalization as
                # _RiverExtentCache._resolve_latest's own comment, so this
                # set matches the plain "YYYY-MM-DD" keys ensure_river_extent
                # actually caches under (both here and in that other call
                # site), not a distinct "YYYY-MM-DD HH:MM:SS" key that would
                # never match, defeating both the prewarm-hit check and the
                # stale-eviction diff below.
                latest_times = {str(r["FORECAST_TIME"])[:10] for r in rows}
            except Exception as e:
                log.error("Prewarm: could not resolve latest-3 river-raw/%s times: %s", rp_tier, e)
                continue
            if not latest_times:
                log.info("Prewarm: river-raw/%s, no data currently available to warm", rp_tier)
                continue
            # Loop over every UI-exposed window, not just the function's
            # own 72h default, see this function's own docstring for
            # why. _PREWARM_RIVER_WINDOWS_H is this file's own copy of the
            # UI's 4-value set (import-independence convention, same
            # reasoning as _RIVER_WINDOW_DEFAULT's own copy elsewhere in
            # this codebase).
            for forecast_time in latest_times:
                for step_h in _PREWARM_RIVER_WINDOWS_H:
                    try:
                        resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier, step_h)
                        if resolved is not None:
                            log.info("Prewarm: river-raw/%s@%sh cache warm (forecast_time=%s)",
                                      rp_tier, step_h, resolved)
                    except Exception as e:
                        log.error("Prewarm: river-raw/%s@%sh warm-up failed for %s: %s",
                                  rp_tier, step_h, forecast_time, e)
                    time.sleep(_PREWARM_YIELD_SECONDS)
            # Evict anything that has aged out of the rolling window for
            # THIS tier only. Per-key locks (see _RiverExtentCache.__init__'s
            # own comment) mean each stale (forecast_time, rp_tier) pair is
            # popped under its OWN lock instead of one lock for the whole
            # pass, so an eviction can never race an in-progress download
            # for that exact key while unrelated keys still load
            # concurrently during eviction. Keyed on forecast_time alone
            # (k[0]), regardless of window (k[2]): a stale date is stale at
            # every window, not just one. try/except: same reasoning as the
            # precip-raw eviction pass above -- a background daemon thread,
            # failing this one pass beats silently killing the whole loop.
            try:
                now = time.time()
                this_tier_keys = {k for k in _river_extent_cache._grids.keys() if k[1] == rp_tier}
                stale = {
                    k for k in this_tier_keys
                    if k[0] not in latest_times
                    and (now - _river_extent_cache._accessed_at.get((k[0], rp_tier), 0.0)) >= _PREWARM_ACCESS_GRACE_SECONDS
                }
                for key in stale:
                    with _river_extent_cache._lock_for((key[0], rp_tier)):
                        _river_extent_cache._grids.pop(key, None)
                        _river_extent_cache._loaded_at.pop(key, None)
                        _river_extent_cache._accessed_at.pop((key[0], rp_tier), None)
            except Exception as e:
                log.error("Prewarm: river-raw/%s stale-eviction pass failed: %s", rp_tier, e)
                stale = set()
            if stale:
                log.info("Prewarm: evicted %d stale river-raw/%s grid(s) outside the latest-3 window: %s",
                          len(stale), rp_tier, sorted(k[0] for k in stale))
        time.sleep(_PREWARM_INTERVAL_SECONDS)


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI):
    """Starts the pre-warm loop as a fire-and-forget daemon thread on
    startup. Non-blocking: /health (which entrypoint.sh's readiness gate
    polls) returns {"status": "ok"} unconditionally, so it is unaffected
    either way -- the app is ready to serve requests immediately, the
    download happens in the background."""
    log.info("Prewarm: starting background warm-up thread for precip-raw/river-raw (interval=%ss)",
              _PREWARM_INTERVAL_SECONDS)
    threading.Thread(target=_prewarm_raw_caches, daemon=True, name="raw-cache-prewarm").start()
    yield


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AoTS Tile Server",
    description="PBF tile sidecar for the Ahead of the Storm UNICEF dashboard.",
    version="2.0.0",
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/tiles/mercator/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf", response_class=Response)
def mercator_tile(
    country: str, storm: str, forecast_date: str,
    z: int, x: int, y: int,
    wind_threshold: int = Query(...),
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> Response:
    try:
        pbf = _fetch_mercator_tile(country.upper(), storm, forecast_date, wind_threshold, z, x, y,
                                   hazard, gust_threshold, rp_tier, threshold_mm, window_h)
    except Exception as exc:
        log.error("mercator_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not pbf:
        # Browser max-age is aligned to _TILE_TTL (not a separate hardcoded
        # number) since the server's own _fetch_mercator_tile cache
        # (backing this response) expires after _TILE_TTL: a longer
        # browser max-age would let a browser keep serving a stale tile
        # after fresher pipeline output is already available server-side.
        return Response(status_code=204, headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Content-Encoding": "gzip", "Cache-Control": f"public, max-age={_TILE_TTL}"})


@app.get("/tiles/admin/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf", response_class=Response)
def admin_tile(
    country: str, storm: str, forecast_date: str,
    z: int, x: int, y: int,
    wind_threshold: int = Query(...),
    admin_level: int = Query(1),
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> Response:
    try:
        pbf = _fetch_admin_tile(country.upper(), storm, forecast_date, wind_threshold, admin_level, z, x, y,
                                hazard, gust_threshold, rp_tier, threshold_mm, window_h)
    except Exception as exc:
        log.error("admin_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not pbf:
        # Same reasoning as mercator_tile above: aligned to _TILE_TTL
        # rather than a separate hardcoded number.
        return Response(status_code=204, headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Content-Encoding": "gzip", "Cache-Control": f"public, max-age={_TILE_TTL}"})


@app.get("/preload/{country}/{storm}/{forecast_date}")
def preload(country: str, storm: str, forecast_date: str,
            wind_threshold: int = Query(...),
            admin_level: int = Query(1),
            hazard: str = Query("wind"),
            gust_threshold: Optional[int] = Query(None),
            rp_tier: Optional[str] = Query(None),
            threshold_mm: Optional[float] = Query(None),
            window_h: Optional[int] = Query(None)) -> dict:
    """Pre-warm pandas cache. Call this when user selects a storm/forecast.
    Returns immediately; loading happens in a background thread."""
    def _run_and_log(fn, *args) -> None:
        try:
            fn(*args)
        except Exception as e:
            log.error("Preload error in %s: %s", getattr(fn, "__name__", fn), e, exc_info=True)

    def _load():
        # The ensure_* warm-up calls run concurrently, not serially: they
        # are independent (each already has its own per-key lock, see
        # _DataCache), so running them concurrently collapses cold preload
        # to roughly the slowest single load instead of the sum of every
        # load's own cost. Submitted to the dedicated _PRELOAD_EXECUTOR
        # (not _SHARED_EXECUTOR): ensure_mercator/ensure_admin/
        # ensure_facility each internally fan out multi-country requests
        # onto _SHARED_EXECUTOR too, and a _SHARED_EXECUTOR worker blocking
        # on its own pool's tasks would starve it. _PRELOAD_EXECUTOR's
        # workers are long-lived, so they reuse one thread-local Snowflake
        # connection across calls instead of a throwaway thread opening
        # and never closing a fresh one.
        facility_sql = {"gust": _FACILITY_GUST_SQL, "river": _FACILITY_RIVER_SQL,
                        "rain": _FACILITY_PRECIP_SQL}.get(hazard, _FACILITY_IMPACT_SQL)
        futures = [
            _PRELOAD_EXECUTOR.submit(_run_and_log,
                _cache.ensure_mercator, country.upper(), storm, forecast_date, wind_threshold, hazard,
                gust_threshold, rp_tier, threshold_mm, window_h,
            ),
            _PRELOAD_EXECUTOR.submit(_run_and_log,
                _cache.ensure_admin, country.upper(), storm, forecast_date, wind_threshold, admin_level, hazard,
                gust_threshold, rp_tier, threshold_mm, window_h,
            ),
        ]
        for layer_type in facility_sql:
            futures.append(_PRELOAD_EXECUTOR.submit(_run_and_log,
                _cache.ensure_facility, layer_type, country.upper(), storm, forecast_date, wind_threshold, hazard,
                gust_threshold, rp_tier, threshold_mm, window_h,
            ))
        concurrent.futures.wait(futures)
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "country": country, "storm": storm}



# Shared min/max mapping for the "base + gust/river" family of stats queries
# (population/children/etc. from b., PROBABILITY + E_* impact cols from i.),
# gust and river both select this exact same column set (river's own
# BELOW_MIN_BASIN/IS_STANDIN flags are booleans, not ramp-colorable numeric
# stats, so they're intentionally omitted here). Rain has its own much
# smaller mapping (_RAIN_STATS_MAPPING below) since only E_population is
# hazard-conditional for that table (see _MERCATOR_PRECIP_IMPACT_ONLY_SQL's
# own comment).
_GUST_RIVER_STATS_MAPPING = {
    "population":              ("POP_MIN",  "POP_MAX"),
    "children_total":          ("CHI_MIN",  "CHI_MAX"),
    "infant_population":       ("INF_MIN",  "INF_MAX"),
    "school_age_population":   ("SCH_MIN",  "SCH_MAX"),
    "adolescent_population":   ("ADO_MIN",  "ADO_MAX"),
    "built_surface_m2":        ("BLT_MIN",  "BLT_MAX"),
    "moderate_poverty_prob":   ("MOD_MIN",  "MOD_MAX"),
    "severe_poverty_prob":     ("SEV_MIN",  "SEV_MAX"),
    "rwi":                     ("RWI_MIN",  "RWI_MAX"),
    "probability":             ("PROB_MIN", "PROB_MAX"),
    "E_population":            ("E_POP_MIN","E_POP_MAX"),
    "E_children_total":        ("E_CHI_MIN","E_CHI_MAX"),
    "E_infant_population":     ("E_INF_MIN","E_INF_MAX"),
    "E_school_age_population": ("E_SCH_MIN","E_SCH_MAX"),
    "E_adolescent_population": ("E_ADO_MIN","E_ADO_MAX"),
    "E_built_surface_m2":      ("E_BLT_MIN","E_BLT_MAX"),
    "E_num_schools":           ("E_SCL_MIN","E_SCL_MAX"),
    "E_num_hcs":               ("E_HCS_MIN","E_HCS_MAX"),
    "E_num_shelters":          ("E_SHL_MIN","E_SHL_MAX"),
    "E_num_wash":              ("E_WSH_MIN","E_WSH_MAX"),
}

_RAIN_STATS_MAPPING = {
    "population":              ("POP_MIN",  "POP_MAX"),
    "children_total":          ("CHI_MIN",  "CHI_MAX"),
    "infant_population":       ("INF_MIN",  "INF_MAX"),
    "school_age_population":   ("SCH_MIN",  "SCH_MAX"),
    "adolescent_population":   ("ADO_MIN",  "ADO_MAX"),
    "built_surface_m2":        ("BLT_MIN",  "BLT_MAX"),
    "moderate_poverty_prob":   ("MOD_MIN",  "MOD_MAX"),
    "severe_poverty_prob":     ("SEV_MIN",  "SEV_MAX"),
    "rwi":                     ("RWI_MIN",  "RWI_MAX"),
    "probability":             ("PROB_MIN", "PROB_MAX"),
    "E_population":            ("E_POP_MIN","E_POP_MAX"),
}

_GUST_MERCATOR_STATS_SQL = """
    SELECT
        MIN(NULLIF(b.POPULATION, 0))             AS pop_min,   MAX(b.POPULATION)             AS pop_max,
        MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
        MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
        MIN(NULLIF(b.INFANT_POPULATION, 0))      AS inf_min,   MAX(b.INFANT_POPULATION)      AS inf_max,
        MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))  AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)  AS sch_max,
        MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))  AS ado_min,   MAX(b.ADOLESCENT_POPULATION)  AS ado_max,
        MIN(NULLIF(b.BUILT_SURFACE_M2, 0))       AS blt_min,   MAX(b.BUILT_SURFACE_M2)       AS blt_max,
        MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))  AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)  AS mod_max,
        MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))    AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)    AS sev_max,
        MIN(b.RWI)                               AS rwi_min,   MAX(b.RWI)                    AS rwi_max,
        MIN(NULLIF(i.PROBABILITY, 0))            AS prob_min,  MAX(i.PROBABILITY)            AS prob_max,
        MIN(NULLIF(i.E_POPULATION, 0))           AS e_pop_min, MAX(i.E_POPULATION)           AS e_pop_max,
        MIN(NULLIF(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION, 0)) AS e_chi_min,
        MAX(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION)            AS e_chi_max,
        MIN(NULLIF(i.E_INFANT_POPULATION, 0))    AS e_inf_min, MAX(i.E_INFANT_POPULATION)    AS e_inf_max,
        MIN(NULLIF(i.E_SCHOOL_AGE_POPULATION, 0))AS e_sch_min, MAX(i.E_SCHOOL_AGE_POPULATION)AS e_sch_max,
        MIN(NULLIF(i.E_ADOLESCENT_POPULATION, 0))AS e_ado_min, MAX(i.E_ADOLESCENT_POPULATION)AS e_ado_max,
        MIN(NULLIF(i.E_BUILT_SURFACE_M2, 0))     AS e_blt_min, MAX(i.E_BUILT_SURFACE_M2)     AS e_blt_max,
        MIN(NULLIF(i.E_NUM_SCHOOLS, 0))          AS e_scl_min, MAX(i.E_NUM_SCHOOLS)          AS e_scl_max,
        MIN(NULLIF(i.E_NUM_HCS, 0))              AS e_hcs_min, MAX(i.E_NUM_HCS)              AS e_hcs_max,
        MIN(NULLIF(i.E_NUM_SHELTERS, 0))         AS e_shl_min, MAX(i.E_NUM_SHELTERS)         AS e_shl_max,
        MIN(NULLIF(i.E_NUM_WASH, 0))             AS e_wsh_min, MAX(i.E_NUM_WASH)             AS e_wsh_max
    FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
    LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_GUST_MAT i
        ON  b.TILE_ID    = i.ZONE_ID AND b.COUNTRY = i.COUNTRY AND b.ZOOM_LEVEL = i.ZOOM_LEVEL
        AND i.STORM = %s AND i.FORECAST_DATE = %s AND i.GUST_THRESHOLD = %s
    WHERE b.COUNTRY    {country_clause}
      AND b.ZOOM_LEVEL = %s
"""

_RIVER_MERCATOR_STATS_SQL = """
    WITH river_agg AS (
        SELECT ZONE_ID, MAX(PROBABILITY) AS PROBABILITY,
            MAX(E_POPULATION) AS E_POPULATION, MAX(E_INFANT_POPULATION) AS E_INFANT_POPULATION,
            MAX(E_SCHOOL_AGE_POPULATION) AS E_SCHOOL_AGE_POPULATION, MAX(E_ADOLESCENT_POPULATION) AS E_ADOLESCENT_POPULATION,
            MAX(E_BUILT_SURFACE_M2) AS E_BUILT_SURFACE_M2, MAX(E_NUM_SCHOOLS) AS E_NUM_SCHOOLS,
            MAX(E_NUM_HCS) AS E_NUM_HCS, MAX(E_NUM_SHELTERS) AS E_NUM_SHELTERS, MAX(E_NUM_WASH) AS E_NUM_WASH
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT
        WHERE COUNTRY {country_clause} AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
        GROUP BY ZONE_ID
    )
    SELECT
        MIN(NULLIF(b.POPULATION, 0))             AS pop_min,   MAX(b.POPULATION)             AS pop_max,
        MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
        MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
        MIN(NULLIF(b.INFANT_POPULATION, 0))      AS inf_min,   MAX(b.INFANT_POPULATION)      AS inf_max,
        MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))  AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)  AS sch_max,
        MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))  AS ado_min,   MAX(b.ADOLESCENT_POPULATION)  AS ado_max,
        MIN(NULLIF(b.BUILT_SURFACE_M2, 0))       AS blt_min,   MAX(b.BUILT_SURFACE_M2)       AS blt_max,
        MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))  AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)  AS mod_max,
        MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))    AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)    AS sev_max,
        MIN(b.RWI)                               AS rwi_min,   MAX(b.RWI)                    AS rwi_max,
        MIN(NULLIF(i.PROBABILITY, 0))            AS prob_min,  MAX(i.PROBABILITY)            AS prob_max,
        MIN(NULLIF(i.E_POPULATION, 0))           AS e_pop_min, MAX(i.E_POPULATION)           AS e_pop_max,
        MIN(NULLIF(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION, 0)) AS e_chi_min,
        MAX(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION)            AS e_chi_max,
        MIN(NULLIF(i.E_INFANT_POPULATION, 0))    AS e_inf_min, MAX(i.E_INFANT_POPULATION)    AS e_inf_max,
        MIN(NULLIF(i.E_SCHOOL_AGE_POPULATION, 0))AS e_sch_min, MAX(i.E_SCHOOL_AGE_POPULATION)AS e_sch_max,
        MIN(NULLIF(i.E_ADOLESCENT_POPULATION, 0))AS e_ado_min, MAX(i.E_ADOLESCENT_POPULATION)AS e_ado_max,
        MIN(NULLIF(i.E_BUILT_SURFACE_M2, 0))     AS e_blt_min, MAX(i.E_BUILT_SURFACE_M2)     AS e_blt_max,
        MIN(NULLIF(i.E_NUM_SCHOOLS, 0))          AS e_scl_min, MAX(i.E_NUM_SCHOOLS)          AS e_scl_max,
        MIN(NULLIF(i.E_NUM_HCS, 0))              AS e_hcs_min, MAX(i.E_NUM_HCS)              AS e_hcs_max,
        MIN(NULLIF(i.E_NUM_SHELTERS, 0))         AS e_shl_min, MAX(i.E_NUM_SHELTERS)         AS e_shl_max,
        MIN(NULLIF(i.E_NUM_WASH, 0))             AS e_wsh_min, MAX(i.E_NUM_WASH)             AS e_wsh_max
    FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
    LEFT JOIN river_agg i ON b.TILE_ID = i.ZONE_ID
    WHERE b.COUNTRY    {country_clause}
      AND b.ZOOM_LEVEL = %s
"""

_RAIN_MERCATOR_STATS_SQL = """
    SELECT
        MIN(NULLIF(b.POPULATION, 0))             AS pop_min,   MAX(b.POPULATION)             AS pop_max,
        MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
        MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
        MIN(NULLIF(b.INFANT_POPULATION, 0))      AS inf_min,   MAX(b.INFANT_POPULATION)      AS inf_max,
        MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))  AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)  AS sch_max,
        MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))  AS ado_min,   MAX(b.ADOLESCENT_POPULATION)  AS ado_max,
        MIN(NULLIF(b.BUILT_SURFACE_M2, 0))       AS blt_min,   MAX(b.BUILT_SURFACE_M2)       AS blt_max,
        MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))  AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)  AS mod_max,
        MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))    AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)    AS sev_max,
        MIN(b.RWI)                               AS rwi_min,   MAX(b.RWI)                    AS rwi_max,
        MIN(NULLIF(i.PROBABILITY, 0))            AS prob_min,  MAX(i.PROBABILITY)            AS prob_max,
        MIN(NULLIF(i.E_POPULATION, 0))           AS e_pop_min, MAX(i.E_POPULATION)           AS e_pop_max
    FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
    LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT i
        ON  b.TILE_ID = i.ZONE_ID AND b.COUNTRY = i.COUNTRY
        AND i.FORECAST_TIME = %s AND i.THRESHOLD_MM = %s AND i.WINDOW_H = %s
    WHERE b.COUNTRY    {country_clause}
      AND b.ZOOM_LEVEL = %s
"""


def _stats_from_row(r: dict, mapping: dict) -> dict:
    stats = {}
    for prop, (min_k, max_k) in mapping.items():
        mn, mx = r.get(min_k), r.get(max_k)
        if mn is not None and mx is not None:
            stats[prop] = {"min": mn, "max": mx}
    return stats


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_tile_stats(
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = 50,
    zoom_level: int = 14,
    hazard: str = "wind",
    gust_threshold: Optional[int] = None,
    rp_tier: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
) -> dict:
    # Reuses ensure_mercator's own cached/merged DataFrame instead of
    # always re-running a separate full-country SQL aggregate below: the
    # browser's simultaneous tile requests already trigger (or share, via
    # ensure_mercator's own per-key lock) the exact same bulk load for
    # this key, so this avoids duplicating it. Falls through to the
    # standalone SQL aggregate only if this fast path itself fails.
    try:
        _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold, hazard,
                               gust_threshold, rp_tier, threshold_mm, window_h)
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        df = _cache._mercator.get((country.upper(), storm, forecast_date) + variant)
        if df is not None:
            return _stats_from_df(df)
    except Exception as e:
        log.warning("Stats fast-path (ensure_mercator) failed, falling back to SQL aggregate: %s", e)

    try:
        clause, codes = _country_in_clause(country)
        if hazard == "gust":
            rows = _run_query(_GUST_MERCATOR_STATS_SQL.replace("{country_clause}", clause),
                              [storm, forecast_date, gust_threshold, *codes, zoom_level])
            return _stats_from_row(rows[0], _GUST_RIVER_STATS_MAPPING) if rows else {}
        if hazard == "river":
            rows = _run_query(_RIVER_MERCATOR_STATS_SQL.replace("{country_clause}", clause),
                              [*codes, forecast_date, rp_tier, window_h or _RIVER_WINDOW_DEFAULT, *codes, zoom_level])
            return _stats_from_row(rows[0], _GUST_RIVER_STATS_MAPPING) if rows else {}
        if hazard == "rain":
            rows = _run_query(_RAIN_MERCATOR_STATS_SQL.replace("{country_clause}", clause),
                              [forecast_date, threshold_mm, window_h, *codes, zoom_level])
            return _stats_from_row(rows[0], _RAIN_STATS_MAPPING) if rows else {}

        rows = _run_query("""
            SELECT
                MIN(NULLIF(b.POPULATION, 0))             AS pop_min,   MAX(b.POPULATION)             AS pop_max,
                MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
                MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
                MIN(NULLIF(b.INFANT_POPULATION, 0))      AS inf_min,   MAX(b.INFANT_POPULATION)      AS inf_max,
                MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))  AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)  AS sch_max,
                MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))  AS ado_min,   MAX(b.ADOLESCENT_POPULATION)  AS ado_max,
                MIN(NULLIF(b.BUILT_SURFACE_M2, 0))       AS blt_min,   MAX(b.BUILT_SURFACE_M2)       AS blt_max,
                MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))  AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)  AS mod_max,
                MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))    AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)    AS sev_max,
                MIN(b.RWI)                               AS rwi_min,   MAX(b.RWI)                    AS rwi_max,
                MIN(NULLIF(i.PROBABILITY, 0))            AS prob_min,  MAX(i.PROBABILITY)            AS prob_max,
                MIN(NULLIF(i.E_POPULATION, 0))           AS e_pop_min, MAX(i.E_POPULATION)           AS e_pop_max,
                MIN(NULLIF(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION, 0)) AS e_chi_min,
                MAX(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION)            AS e_chi_max,
                MIN(NULLIF(i.E_INFANT_POPULATION, 0))    AS e_inf_min, MAX(i.E_INFANT_POPULATION)    AS e_inf_max,
                MIN(NULLIF(i.E_SCHOOL_AGE_POPULATION, 0))AS e_sch_min, MAX(i.E_SCHOOL_AGE_POPULATION)AS e_sch_max,
                MIN(NULLIF(i.E_ADOLESCENT_POPULATION, 0))AS e_ado_min, MAX(i.E_ADOLESCENT_POPULATION)AS e_ado_max,
                MIN(NULLIF(i.E_BUILT_SURFACE_M2, 0))     AS e_blt_min, MAX(i.E_BUILT_SURFACE_M2)     AS e_blt_max,
                MIN(NULLIF(i.E_NUM_SCHOOLS, 0))          AS e_scl_min, MAX(i.E_NUM_SCHOOLS)          AS e_scl_max,
                MIN(NULLIF(i.E_NUM_HCS, 0))              AS e_hcs_min, MAX(i.E_NUM_HCS)              AS e_hcs_max,
                MIN(NULLIF(i.E_NUM_SHELTERS, 0))         AS e_shl_min, MAX(i.E_NUM_SHELTERS)         AS e_shl_max,
                MIN(NULLIF(i.E_NUM_WASH, 0))             AS e_wsh_min, MAX(i.E_NUM_WASH)             AS e_wsh_max,
                MIN(NULLIF(v.E_PEOPLE_IN_NEED, 0))       AS e_pin_min, MAX(v.E_PEOPLE_IN_NEED)       AS e_pin_max,
                MIN(NULLIF(v.E_CHILDREN_IN_NEED, 0))     AS e_cin_min, MAX(v.E_CHILDREN_IN_NEED)     AS e_cin_max,
                MIN(NULLIF(v.E_INFANT_IN_NEED, 0))       AS e_inn_min, MAX(v.E_INFANT_IN_NEED)       AS e_inn_max,
                MIN(NULLIF(v.E_SCHOOL_AGE_IN_NEED, 0))   AS e_scn_min, MAX(v.E_SCHOOL_AGE_IN_NEED)   AS e_scn_max,
                MIN(NULLIF(v.E_ADOLESCENT_IN_NEED, 0))   AS e_adn_min, MAX(v.E_ADOLESCENT_IN_NEED)   AS e_adn_max,
                MIN(NULLIF(c.CCI_CHILDREN, 0))           AS cci_min,   MAX(c.CCI_CHILDREN)           AS cci_max,
                MIN(NULLIF(c.E_CCI_CHILDREN, 0))         AS e_cci_min, MAX(c.E_CCI_CHILDREN)         AS e_cci_max
            FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
            LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT i
                ON  b.TILE_ID    = i.ZONE_ID
                AND b.COUNTRY    = i.COUNTRY
                AND b.ZOOM_LEVEL = i.ZOOM_LEVEL
                AND i.STORM          = %s
                AND i.FORECAST_DATE  = %s
                AND i.WIND_THRESHOLD = %s
            LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_VULNERABILITY_MAT v
                ON  b.TILE_ID    = v.ZONE_ID
                AND b.COUNTRY    = v.COUNTRY
                AND b.ZOOM_LEVEL = v.ZOOM_LEVEL
                AND v.STORM         = %s
                AND v.FORECAST_DATE = %s
            LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT c
                ON  b.TILE_ID    = c.ZONE_ID
                AND b.COUNTRY    = c.COUNTRY
                AND b.ZOOM_LEVEL = c.ZOOM_LEVEL
                AND c.STORM         = %s
                AND c.FORECAST_DATE = %s
            WHERE b.COUNTRY    = %s
              AND b.ZOOM_LEVEL = %s
        """.replace("b.COUNTRY    = %s", f"b.COUNTRY    {_country_in_clause(country)[0]}"),
            [storm, forecast_date, wind_threshold, storm, forecast_date, storm, forecast_date,
             *_country_in_clause(country)[1], zoom_level])

        if not rows:
            return {}
        r = rows[0]
        mapping = {
            "population":              ("POP_MIN",  "POP_MAX"),
            "children_total":          ("CHI_MIN",  "CHI_MAX"),
            "infant_population":       ("INF_MIN",  "INF_MAX"),
            "school_age_population":   ("SCH_MIN",  "SCH_MAX"),
            "adolescent_population":   ("ADO_MIN",  "ADO_MAX"),
            "built_surface_m2":        ("BLT_MIN",  "BLT_MAX"),
            "moderate_poverty_prob":   ("MOD_MIN",  "MOD_MAX"),
            "severe_poverty_prob":     ("SEV_MIN",  "SEV_MAX"),
            "rwi":                     ("RWI_MIN",  "RWI_MAX"),
            "probability":             ("PROB_MIN", "PROB_MAX"),
            "E_population":            ("E_POP_MIN","E_POP_MAX"),
            "E_children_total":        ("E_CHI_MIN","E_CHI_MAX"),
            "E_infant_population":     ("E_INF_MIN","E_INF_MAX"),
            "E_school_age_population": ("E_SCH_MIN","E_SCH_MAX"),
            "E_adolescent_population": ("E_ADO_MIN","E_ADO_MAX"),
            "E_built_surface_m2":      ("E_BLT_MIN","E_BLT_MAX"),
            "E_num_schools":           ("E_SCL_MIN","E_SCL_MAX"),
            "E_num_hcs":               ("E_HCS_MIN","E_HCS_MAX"),
            "E_num_shelters":          ("E_SHL_MIN","E_SHL_MAX"),
            "E_num_wash":              ("E_WSH_MIN","E_WSH_MAX"),
            "E_people_in_need":        ("E_PIN_MIN","E_PIN_MAX"),
            "E_children_in_need":      ("E_CIN_MIN","E_CIN_MAX"),
            "E_infant_in_need":        ("E_INN_MIN","E_INN_MAX"),
            "E_school_age_in_need":    ("E_SCN_MIN","E_SCN_MAX"),
            "E_adolescent_in_need":    ("E_ADN_MIN","E_ADN_MAX"),
            "cci_children":            ("CCI_MIN",  "CCI_MAX"),
            "E_cci_children":          ("E_CCI_MIN","E_CCI_MAX"),
        }
        stats = {}
        for prop, (min_k, max_k) in mapping.items():
            mn, mx = r.get(min_k), r.get(max_k)
            if mn is not None and mx is not None:
                stats[prop] = {"min": mn, "max": mx}
        return stats

    except Exception as e:
        log.error("Stats error: %s", e, exc_info=True)
        return {}


# Thin route: all the work lives in the cached _fetch_tile_stats above
# (same split used throughout this file, e.g. _fetch_mercator_tile /
# get_mercator_tile), cached via _ttl_cache since this endpoint is driven
# by the hazard-threshold slider debounce settling: the same
# repeated-identical-request shape every other tile-render endpoint in
# this file caches.
@app.get("/stats/{country}/{storm}/{forecast_date}")
def get_tile_stats(
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = 50,
    zoom_level: int = 14,
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> dict:
    return _fetch_tile_stats(country, storm, forecast_date, wind_threshold, zoom_level,
                               hazard, gust_threshold, rp_tier, threshold_mm, window_h)


_GUST_ADMIN_STATS_SQL = """
    SELECT
        MIN(NULLIF(b.POPULATION, 0))              AS pop_min,   MAX(b.POPULATION)              AS pop_max,
        MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
        MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
        MIN(NULLIF(b.INFANT_POPULATION, 0))       AS inf_min,   MAX(b.INFANT_POPULATION)       AS inf_max,
        MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))   AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)   AS sch_max,
        MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))   AS ado_min,   MAX(b.ADOLESCENT_POPULATION)   AS ado_max,
        MIN(NULLIF(b.BUILT_SURFACE_M2, 0))        AS blt_min,   MAX(b.BUILT_SURFACE_M2)        AS blt_max,
        MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))   AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)   AS mod_max,
        MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))     AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)     AS sev_max,
        MIN(b.RWI)                                AS rwi_min,   MAX(b.RWI)                     AS rwi_max,
        MIN(NULLIF(i.PROBABILITY, 0))             AS prob_min,  MAX(i.PROBABILITY)             AS prob_max,
        MIN(NULLIF(i.E_POPULATION, 0))            AS e_pop_min, MAX(i.E_POPULATION)            AS e_pop_max,
        MIN(NULLIF(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION, 0)) AS e_chi_min,
        MAX(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION)            AS e_chi_max,
        MIN(NULLIF(i.E_INFANT_POPULATION, 0))     AS e_inf_min, MAX(i.E_INFANT_POPULATION)     AS e_inf_max,
        MIN(NULLIF(i.E_SCHOOL_AGE_POPULATION, 0)) AS e_sch_min, MAX(i.E_SCHOOL_AGE_POPULATION) AS e_sch_max,
        MIN(NULLIF(i.E_ADOLESCENT_POPULATION, 0)) AS e_ado_min, MAX(i.E_ADOLESCENT_POPULATION) AS e_ado_max,
        MIN(NULLIF(i.E_BUILT_SURFACE_M2, 0))      AS e_blt_min, MAX(i.E_BUILT_SURFACE_M2)      AS e_blt_max,
        MIN(NULLIF(i.E_NUM_SCHOOLS, 0))           AS e_scl_min, MAX(i.E_NUM_SCHOOLS)           AS e_scl_max,
        MIN(NULLIF(i.E_NUM_HCS, 0))               AS e_hcs_min, MAX(i.E_NUM_HCS)               AS e_hcs_max,
        MIN(NULLIF(i.E_NUM_SHELTERS, 0))          AS e_shl_min, MAX(i.E_NUM_SHELTERS)          AS e_shl_max,
        MIN(NULLIF(i.E_NUM_WASH, 0))              AS e_wsh_min, MAX(i.E_NUM_WASH)              AS e_wsh_max
    FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
    LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_GUST_MAT i
        ON  b.TILE_ID     = i.TILE_ID AND b.COUNTRY = i.COUNTRY AND b.ADMIN_LEVEL = i.ADMIN_LEVEL
        AND i.STORM = %s AND i.FORECAST_DATE = %s AND i.GUST_THRESHOLD = %s
    WHERE b.COUNTRY     {country_clause}
      AND b.ADMIN_LEVEL = %s
"""

_RIVER_ADMIN_STATS_SQL = """
    WITH river_agg AS (
        SELECT TILE_ID, ADMIN_LEVEL, MAX(PROBABILITY) AS PROBABILITY,
            MAX(E_POPULATION) AS E_POPULATION, MAX(E_INFANT_POPULATION) AS E_INFANT_POPULATION,
            MAX(E_SCHOOL_AGE_POPULATION) AS E_SCHOOL_AGE_POPULATION, MAX(E_ADOLESCENT_POPULATION) AS E_ADOLESCENT_POPULATION,
            MAX(E_BUILT_SURFACE_M2) AS E_BUILT_SURFACE_M2, MAX(E_NUM_SCHOOLS) AS E_NUM_SCHOOLS,
            MAX(E_NUM_HCS) AS E_NUM_HCS, MAX(E_NUM_SHELTERS) AS E_NUM_SHELTERS, MAX(E_NUM_WASH) AS E_NUM_WASH
        FROM AOTS.TC_ECMWF.ADMIN_ALL_RIVER_MAT
        WHERE COUNTRY {country_clause} AND FORECAST_TIME = %s AND RP_TIER = %s AND STEP_H = %s
        GROUP BY TILE_ID, ADMIN_LEVEL
    )
    SELECT
        MIN(NULLIF(b.POPULATION, 0))              AS pop_min,   MAX(b.POPULATION)              AS pop_max,
        MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
        MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
        MIN(NULLIF(b.INFANT_POPULATION, 0))       AS inf_min,   MAX(b.INFANT_POPULATION)       AS inf_max,
        MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))   AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)   AS sch_max,
        MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))   AS ado_min,   MAX(b.ADOLESCENT_POPULATION)   AS ado_max,
        MIN(NULLIF(b.BUILT_SURFACE_M2, 0))        AS blt_min,   MAX(b.BUILT_SURFACE_M2)        AS blt_max,
        MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))   AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)   AS mod_max,
        MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))     AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)     AS sev_max,
        MIN(b.RWI)                                AS rwi_min,   MAX(b.RWI)                     AS rwi_max,
        MIN(NULLIF(i.PROBABILITY, 0))             AS prob_min,  MAX(i.PROBABILITY)             AS prob_max,
        MIN(NULLIF(i.E_POPULATION, 0))            AS e_pop_min, MAX(i.E_POPULATION)            AS e_pop_max,
        MIN(NULLIF(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION, 0)) AS e_chi_min,
        MAX(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION)            AS e_chi_max,
        MIN(NULLIF(i.E_INFANT_POPULATION, 0))     AS e_inf_min, MAX(i.E_INFANT_POPULATION)     AS e_inf_max,
        MIN(NULLIF(i.E_SCHOOL_AGE_POPULATION, 0)) AS e_sch_min, MAX(i.E_SCHOOL_AGE_POPULATION) AS e_sch_max,
        MIN(NULLIF(i.E_ADOLESCENT_POPULATION, 0)) AS e_ado_min, MAX(i.E_ADOLESCENT_POPULATION) AS e_ado_max,
        MIN(NULLIF(i.E_BUILT_SURFACE_M2, 0))      AS e_blt_min, MAX(i.E_BUILT_SURFACE_M2)      AS e_blt_max,
        MIN(NULLIF(i.E_NUM_SCHOOLS, 0))           AS e_scl_min, MAX(i.E_NUM_SCHOOLS)           AS e_scl_max,
        MIN(NULLIF(i.E_NUM_HCS, 0))               AS e_hcs_min, MAX(i.E_NUM_HCS)               AS e_hcs_max,
        MIN(NULLIF(i.E_NUM_SHELTERS, 0))          AS e_shl_min, MAX(i.E_NUM_SHELTERS)          AS e_shl_max,
        MIN(NULLIF(i.E_NUM_WASH, 0))              AS e_wsh_min, MAX(i.E_NUM_WASH)              AS e_wsh_max
    FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
    LEFT JOIN river_agg i ON b.TILE_ID = i.TILE_ID AND b.ADMIN_LEVEL = i.ADMIN_LEVEL
    WHERE b.COUNTRY     {country_clause}
      AND b.ADMIN_LEVEL = %s
"""

_RAIN_ADMIN_STATS_SQL = """
    SELECT
        MIN(NULLIF(b.POPULATION, 0))              AS pop_min,   MAX(b.POPULATION)              AS pop_max,
        MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
        MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
        MIN(NULLIF(b.INFANT_POPULATION, 0))       AS inf_min,   MAX(b.INFANT_POPULATION)       AS inf_max,
        MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))   AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)   AS sch_max,
        MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))   AS ado_min,   MAX(b.ADOLESCENT_POPULATION)   AS ado_max,
        MIN(NULLIF(b.BUILT_SURFACE_M2, 0))        AS blt_min,   MAX(b.BUILT_SURFACE_M2)        AS blt_max,
        MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))   AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)   AS mod_max,
        MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))     AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)     AS sev_max,
        MIN(b.RWI)                                AS rwi_min,   MAX(b.RWI)                     AS rwi_max,
        MIN(NULLIF(i.PROBABILITY, 0))             AS prob_min,  MAX(i.PROBABILITY)             AS prob_max,
        MIN(NULLIF(i.E_POPULATION, 0))            AS e_pop_min, MAX(i.E_POPULATION)            AS e_pop_max
    FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
    LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_PRECIP_MAT i
        ON  b.TILE_ID = i.TILE_ID AND b.COUNTRY = i.COUNTRY AND b.ADMIN_LEVEL = i.ADMIN_LEVEL
        AND i.FORECAST_TIME = %s AND i.THRESHOLD_MM = %s AND i.WINDOW_H = %s
    WHERE b.COUNTRY     {country_clause}
      AND b.ADMIN_LEVEL = %s
"""


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_admin_stats(
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = 50,
    admin_level: int = 1,
    hazard: str = "wind",
    gust_threshold: Optional[int] = None,
    rp_tier: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
) -> dict:
    # Same approach as _fetch_tile_stats above: reuse ensure_admin's own
    # cached/merged DataFrame instead of always re-running a separate
    # full-country SQL aggregate below.
    try:
        _cache.ensure_admin(country.upper(), storm, forecast_date, wind_threshold, admin_level, hazard,
                            gust_threshold, rp_tier, threshold_mm, window_h)
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        df = _cache._admin.get((country.upper(), storm, forecast_date) + variant + (admin_level,))
        if df is not None:
            return _stats_from_df(df)
    except Exception as e:
        log.warning("Admin stats fast-path (ensure_admin) failed, falling back to SQL aggregate: %s", e)

    try:
        clause, codes = _country_in_clause(country)
        if hazard == "gust":
            rows = _run_query(_GUST_ADMIN_STATS_SQL.replace("{country_clause}", clause),
                              [storm, forecast_date, gust_threshold, *codes, admin_level])
            return _stats_from_row(rows[0], _GUST_RIVER_STATS_MAPPING) if rows else {}
        if hazard == "river":
            rows = _run_query(_RIVER_ADMIN_STATS_SQL.replace("{country_clause}", clause),
                              [*codes, forecast_date, rp_tier, window_h or _RIVER_WINDOW_DEFAULT, *codes, admin_level])
            return _stats_from_row(rows[0], _GUST_RIVER_STATS_MAPPING) if rows else {}
        if hazard == "rain":
            rows = _run_query(_RAIN_ADMIN_STATS_SQL.replace("{country_clause}", clause),
                              [forecast_date, threshold_mm, window_h, *codes, admin_level])
            return _stats_from_row(rows[0], _RAIN_STATS_MAPPING) if rows else {}

        rows = _run_query("""
            SELECT
                MIN(NULLIF(b.POPULATION, 0))              AS pop_min,   MAX(b.POPULATION)              AS pop_max,
                MIN(NULLIF(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION, 0)) AS chi_min,
                MAX(b.INFANT_POPULATION + b.SCHOOL_AGE_POPULATION + b.ADOLESCENT_POPULATION)            AS chi_max,
                MIN(NULLIF(b.INFANT_POPULATION, 0))       AS inf_min,   MAX(b.INFANT_POPULATION)       AS inf_max,
                MIN(NULLIF(b.SCHOOL_AGE_POPULATION, 0))   AS sch_min,   MAX(b.SCHOOL_AGE_POPULATION)   AS sch_max,
                MIN(NULLIF(b.ADOLESCENT_POPULATION, 0))   AS ado_min,   MAX(b.ADOLESCENT_POPULATION)   AS ado_max,
                MIN(NULLIF(b.BUILT_SURFACE_M2, 0))        AS blt_min,   MAX(b.BUILT_SURFACE_M2)        AS blt_max,
                MIN(NULLIF(b.MODERATE_POVERTY_PROB, 0))   AS mod_min,   MAX(b.MODERATE_POVERTY_PROB)   AS mod_max,
                MIN(NULLIF(b.SEVERE_POVERTY_PROB, 0))     AS sev_min,   MAX(b.SEVERE_POVERTY_PROB)     AS sev_max,
                MIN(b.RWI)                                AS rwi_min,   MAX(b.RWI)                     AS rwi_max,
                MIN(NULLIF(i.PROBABILITY, 0))             AS prob_min,  MAX(i.PROBABILITY)             AS prob_max,
                MIN(NULLIF(i.E_POPULATION, 0))            AS e_pop_min, MAX(i.E_POPULATION)            AS e_pop_max,
                MIN(NULLIF(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION, 0)) AS e_chi_min,
                MAX(i.E_INFANT_POPULATION + i.E_SCHOOL_AGE_POPULATION + i.E_ADOLESCENT_POPULATION)            AS e_chi_max,
                MIN(NULLIF(i.E_INFANT_POPULATION, 0))     AS e_inf_min, MAX(i.E_INFANT_POPULATION)     AS e_inf_max,
                MIN(NULLIF(i.E_SCHOOL_AGE_POPULATION, 0)) AS e_sch_min, MAX(i.E_SCHOOL_AGE_POPULATION) AS e_sch_max,
                MIN(NULLIF(i.E_ADOLESCENT_POPULATION, 0)) AS e_ado_min, MAX(i.E_ADOLESCENT_POPULATION) AS e_ado_max,
                MIN(NULLIF(i.E_BUILT_SURFACE_M2, 0))      AS e_blt_min, MAX(i.E_BUILT_SURFACE_M2)      AS e_blt_max,
                MIN(NULLIF(i.E_NUM_SCHOOLS, 0))           AS e_scl_min, MAX(i.E_NUM_SCHOOLS)           AS e_scl_max,
                MIN(NULLIF(i.E_NUM_HCS, 0))               AS e_hcs_min, MAX(i.E_NUM_HCS)               AS e_hcs_max,
                MIN(NULLIF(i.E_NUM_SHELTERS, 0))          AS e_shl_min, MAX(i.E_NUM_SHELTERS)          AS e_shl_max,
                MIN(NULLIF(i.E_NUM_WASH, 0))              AS e_wsh_min, MAX(i.E_NUM_WASH)              AS e_wsh_max,
                MIN(NULLIF(v.E_PEOPLE_IN_NEED, 0))        AS e_pin_min, MAX(v.E_PEOPLE_IN_NEED)        AS e_pin_max,
                MIN(NULLIF(v.E_CHILDREN_IN_NEED, 0))      AS e_cin_min, MAX(v.E_CHILDREN_IN_NEED)      AS e_cin_max,
                MIN(NULLIF(v.E_INFANT_IN_NEED, 0))        AS e_inn_min, MAX(v.E_INFANT_IN_NEED)        AS e_inn_max,
                MIN(NULLIF(v.E_SCHOOL_AGE_IN_NEED, 0))    AS e_scn_min, MAX(v.E_SCHOOL_AGE_IN_NEED)    AS e_scn_max,
                MIN(NULLIF(v.E_ADOLESCENT_IN_NEED, 0))    AS e_adn_min, MAX(v.E_ADOLESCENT_IN_NEED)    AS e_adn_max,
                MIN(NULLIF(c.CCI_CHILDREN, 0))            AS cci_min,   MAX(c.CCI_CHILDREN)            AS cci_max,
                MIN(NULLIF(c.E_CCI_CHILDREN, 0))          AS e_cci_min, MAX(c.E_CCI_CHILDREN)          AS e_cci_max
            FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
            LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT i
                ON  b.TILE_ID     = i.TILE_ID
                AND b.COUNTRY     = i.COUNTRY
                AND b.ADMIN_LEVEL = i.ADMIN_LEVEL
                AND i.STORM          = %s
                AND i.FORECAST_DATE  = %s
                AND i.WIND_THRESHOLD = %s
            LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_VULNERABILITY_MAT v
                ON  b.TILE_ID     = v.TILE_ID
                AND b.COUNTRY     = v.COUNTRY
                AND b.ADMIN_LEVEL = v.ADMIN_LEVEL
                AND v.STORM         = %s
                AND v.FORECAST_DATE = %s
            LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT c
                ON  b.TILE_ID     = c.TILE_ID
                AND b.COUNTRY     = c.COUNTRY
                AND b.ADMIN_LEVEL = c.ADMIN_LEVEL
                AND c.STORM          = %s
                AND c.FORECAST_DATE  = %s
            WHERE b.COUNTRY     = %s
              AND b.ADMIN_LEVEL = %s
        """.replace("b.COUNTRY     = %s", f"b.COUNTRY     {_country_in_clause(country)[0]}"),
            [storm, forecast_date, wind_threshold, storm, forecast_date, storm, forecast_date,
             *_country_in_clause(country)[1], admin_level])

        if not rows:
            return {}
        r = rows[0]
        mapping = {
            "population":              ("POP_MIN",  "POP_MAX"),
            "children_total":          ("CHI_MIN",  "CHI_MAX"),
            "infant_population":       ("INF_MIN",  "INF_MAX"),
            "school_age_population":   ("SCH_MIN",  "SCH_MAX"),
            "adolescent_population":   ("ADO_MIN",  "ADO_MAX"),
            "built_surface_m2":        ("BLT_MIN",  "BLT_MAX"),
            "moderate_poverty_prob":   ("MOD_MIN",  "MOD_MAX"),
            "severe_poverty_prob":     ("SEV_MIN",  "SEV_MAX"),
            "rwi":                     ("RWI_MIN",  "RWI_MAX"),
            "probability":             ("PROB_MIN", "PROB_MAX"),
            "E_population":            ("E_POP_MIN","E_POP_MAX"),
            "E_children_total":        ("E_CHI_MIN","E_CHI_MAX"),
            "E_infant_population":     ("E_INF_MIN","E_INF_MAX"),
            "E_school_age_population": ("E_SCH_MIN","E_SCH_MAX"),
            "E_adolescent_population": ("E_ADO_MIN","E_ADO_MAX"),
            "E_built_surface_m2":      ("E_BLT_MIN","E_BLT_MAX"),
            "E_num_schools":           ("E_SCL_MIN","E_SCL_MAX"),
            "E_num_hcs":               ("E_HCS_MIN","E_HCS_MAX"),
            "E_num_shelters":          ("E_SHL_MIN","E_SHL_MAX"),
            "E_num_wash":              ("E_WSH_MIN","E_WSH_MAX"),
            "E_people_in_need":        ("E_PIN_MIN","E_PIN_MAX"),
            "E_children_in_need":      ("E_CIN_MIN","E_CIN_MAX"),
            "E_infant_in_need":        ("E_INN_MIN","E_INN_MAX"),
            "E_school_age_in_need":    ("E_SCN_MIN","E_SCN_MAX"),
            "E_adolescent_in_need":    ("E_ADN_MIN","E_ADN_MAX"),
            "cci_children":            ("CCI_MIN",  "CCI_MAX"),
            "E_cci_children":          ("E_CCI_MIN","E_CCI_MAX"),
        }
        stats = {}
        for prop, (min_k, max_k) in mapping.items():
            mn, mx = r.get(min_k), r.get(max_k)
            if mn is not None and mx is not None:
                stats[prop] = {"min": mn, "max": mx}
        return stats

    except Exception as e:
        log.error("Admin stats error: %s", e, exc_info=True)
        return {}


# Same real perf fix as get_tile_stats above: thin route, real work now
# lives in the cached _fetch_admin_stats.
@app.get("/admin-stats/{country}/{storm}/{forecast_date}")
def get_admin_stats(
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = 50,
    admin_level: int = 1,
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> dict:
    return _fetch_admin_stats(country, storm, forecast_date, wind_threshold, admin_level,
                                hazard, gust_threshold, rp_tier, threshold_mm, window_h)


@app.get("/tiles/raster/{country}/{storm}/{forecast_date}/{prop}/{z}/{x}/{y}.webp",
         response_class=Response)
def raster_tile(
    country: str, storm: str, forecast_date: str,
    prop: str,
    z: int, x: int, y: int,
    wind_threshold: int = Query(...),
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> Response:
    """Return a 512×512 RGBA WebP raster tile colored by `prop`."""
    try:
        webp_bytes = _fetch_raster_tile(
            country.upper(), storm, forecast_date, wind_threshold,
            prop.upper(), z, x, y,
            hazard, gust_threshold, rp_tier, threshold_mm, window_h,
        )
    except Exception as exc:
        log.error("raster_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not webp_bytes:
        # Return a transparent 1×1 WebP rather than 204 No Content.
        # MapLibre falls back to parent (z−1) tiles for non-200 raster responses,
        # which causes row-level column-boundary misalignment when some tiles have
        # data and others don't. A transparent image keeps MapLibre in the z-tile
        # grid and renders as fully transparent (no visible effect).
        # Aligned to _TILE_TTL rather than a separate hardcoded number,
        # see mercator_tile's own comment for why.
        return Response(content=_TRANSPARENT_WEBP, media_type="image/webp",
                        headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(
        content=webp_bytes,
        media_type="image/webp",
        headers={"Cache-Control": f"public, max-age={_TILE_TTL}"},
    )


@app.get("/tiles/raster-combined/{country}/{storm}/{mode}/{z}/{x}/{y}.webp",
         response_class=Response)
def raster_combined_tile(
    country: str, storm: str,
    mode: str,
    z: int, x: int, y: int,
    wind_on: bool = Query(False),
    wind_forecast_date: Optional[str] = Query(None),
    wind_threshold: int = Query(50),
    gust_on: bool = Query(False),
    gust_threshold: Optional[int] = Query(None),
    river_on: bool = Query(False),
    river_forecast_date: Optional[str] = Query(None),
    rp_tier: Optional[str] = Query(None),
    river_window: Optional[int] = Query(None),
    rain_on: bool = Query(False),
    rain_forecast_date: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
    prop: Optional[str] = Query(None),
) -> Response:
    """Return a 512x512 RGBA WebP raster tile combining every active hazard
    (`mode="probability"`: one real per-tile bitmask-union value, see
    _combine_bitmask_aware; `mode="classification"`: which hazard(s) hit
    each cell; `mode="exposure"`:
    real expected impact for `prop`: required, must be one of
    _COMBINED_EXPOSURE_RAW_COL's keys, under the SAME combined probability)
    . See _fetch_combined_raster_tile's own docstring, including why each
    hazard family carries its OWN forecast_date query param instead of one
    shared path segment. Only invoked client-side when 2+ hazards are
    simultaneously active in Probability mode, or whenever Classification
    mode is selected at all; a single active hazard keeps using the
    existing /tiles/raster/... route regardless of `prop`.

    `river_window` is kept separate from `window_h` (Rain's own) for the
    same reason _fetch_combined_facility_rows' own river_window/window_h
    split exists: River and Rain can be simultaneously active here with
    different windows.
    """
    if mode not in ("probability", "classification", "exposure"):
        raise HTTPException(status_code=400, detail=f"invalid mode: {mode}")
    prop_upper = prop.upper() if prop else None
    if mode == "exposure" and prop_upper not in _COMBINED_EXPOSURE_RAW_COL:
        raise HTTPException(status_code=400, detail=f"invalid/unsupported exposure prop: {prop}")
    (wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
     river_on, river_forecast_date, rp_tier, river_window,
     rain_on, rain_forecast_date, threshold_mm, window_h) = _normalize_combined_hazard_params(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    try:
        webp_bytes = _fetch_combined_raster_tile(
            country.upper(), storm, mode, z, x, y,
            wind_on, wind_forecast_date, wind_threshold,
            gust_on, gust_threshold,
            river_on, river_forecast_date, rp_tier, river_window,
            rain_on, rain_forecast_date, threshold_mm, window_h,
            prop_upper,
        )
    except Exception as exc:
        log.error("raster_combined_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not webp_bytes:
        # Same "transparent tile, not 204" reasoning as raster_tile above.
        return Response(content=_TRANSPARENT_WEBP, media_type="image/webp",
                        headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(
        content=webp_bytes,
        media_type="image/webp",
        headers={"Cache-Control": f"public, max-age={_TILE_TTL}"},
    )


@app.get("/tiles/admin-combined/{country}/{storm}/{mode}/{z}/{x}/{y}.pbf",
         response_class=Response)
def admin_combined_tile(
    country: str, storm: str,
    mode: str,
    z: int, x: int, y: int,
    admin_level: int = Query(1),
    wind_on: bool = Query(False),
    wind_forecast_date: Optional[str] = Query(None),
    wind_threshold: int = Query(50),
    gust_on: bool = Query(False),
    gust_threshold: Optional[int] = Query(None),
    river_on: bool = Query(False),
    river_forecast_date: Optional[str] = Query(None),
    rp_tier: Optional[str] = Query(None),
    river_window: Optional[int] = Query(None),
    rain_on: bool = Query(False),
    rain_forecast_date: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> Response:
    """Combined-hazard ADMIN-region MVT tile: the Regions-view counterpart
    of /tiles/raster-combined/..., serving the Tiles/Regions switch when
    2+ hazards are simultaneously active (see _fetch_admin_combined_tile's
    own docstring for what each property means and what is still merged
    the old MAX way).

    Query-param shape is deliberately IDENTICAL to raster_combined_tile's
    (plus `admin_level`, which the raster has no concept of), so the
    frontend's own combined URL builder can emit both from one place:
    same reasoning /tile-value-combined already follows for the hover path.

    `mode="classification"` is rejected: the raster's classification view
    is a per-pixel colour decision (which hazard(s) hit THIS cell) with no
    meaningful single-value vector-property equivalent for a whole admin
    region: a region containing both a wind-only tile and a flood-only
    tile has no honest single classification. Returning 400 rather than
    silently serving a probability tile under a classification URL.
    """
    if mode not in ("probability", "exposure"):
        raise HTTPException(status_code=400, detail=f"invalid/unsupported admin-combined mode: {mode}")
    (wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
     river_on, river_forecast_date, rp_tier, river_window,
     rain_on, rain_forecast_date, threshold_mm, window_h) = _normalize_combined_hazard_params(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    try:
        # `mode` is validated above but deliberately not forwarded: this
        # tile carries every property in both modes (see
        # _fetch_admin_combined_tile's own docstring), so the two modes
        # produce identical bytes and share one cache entry.
        pbf = _fetch_admin_combined_tile(
            country.upper(), storm, z, x, y, admin_level,
            wind_on, wind_forecast_date, wind_threshold,
            gust_on, gust_threshold,
            river_on, river_forecast_date, rp_tier, river_window,
            rain_on, rain_forecast_date, threshold_mm, window_h,
        )
    except Exception as exc:
        log.error("admin_combined_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not pbf:
        # 204 (not a transparent placeholder), same convention admin_tile
        # above uses for an empty VECTOR tile, and aligned to _TILE_TTL
        # rather than a separate hardcoded max-age.
        return Response(status_code=204, headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Content-Encoding": "gzip", "Cache-Control": f"public, max-age={_TILE_TTL}"})


@app.get("/geojson/facilities/{layer_type}/{country}/{storm}/{forecast_date}",
         response_class=Response)
def facility_geojson(
    layer_type: str,
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = Query(...),
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
    combine: bool = Query(True),
) -> Response:
    """Return a styled GeoJSON FeatureCollection for the requested facility layer.

    Properties include `_color`, `_radius`, `_opacity`, `_weight`, `_fillOpacity` for
    Leaflet's pointToLayer, plus lowercase field names for tooltip functions.
    Response is gzip-compressed so the browser receives it efficiently via fetch().

    `combine` (default True): whether to color/size each facility by its
    per-hazard PROBABILITY at `wind_threshold`/etc, or render every
    facility as a plain, uncolored location instead. The client
    (pages/map_shell_concept.py's _register_ms_facility_layer) always
    fetches with a wind_threshold (the query itself doesn't stop being
    hazard-conditional just because no hazard checkbox happens to be on),
    so this flag exists to keep facility points from staying tinted by
    whatever threshold was last selected when every hazard is turned off
    (or hidden via the eye icon). When False, `prob` is forced to 0 for
    every row below (same code path a confirmed-zero PROBABILITY already
    takes, the base/neutral color), and the `probability` property is
    dropped from the response so a tooltip can't show a stale percentage
    for a marker that's deliberately not being colored by it.
    """
    valid_layers = {"gust": _FACILITY_GUST_SQL, "river": _FACILITY_RIVER_SQL,
                    "rain": _FACILITY_PRECIP_SQL}.get(hazard, _FACILITY_IMPACT_SQL)
    if layer_type not in valid_layers:
        raise HTTPException(status_code=404, detail=f"Unknown layer type: {layer_type}")
    try:
        body = _fetch_facility_geojson_body(layer_type, country, storm, forecast_date, wind_threshold,
                                              hazard, gust_threshold, rp_tier, threshold_mm, window_h, combine)
        return Response(
            content=body,
            media_type="application/geo+json",
            headers={"Content-Encoding": "gzip", "Cache-Control": "public, max-age=300"},
        )
    except Exception as exc:
        log.error("facility_geojson error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# The full response (iterrows + json.dumps + gzip) is cached rather than
# rebuilt on every call, matching the browser's own Cache-Control here,
# which already assumes a stable 5-minute response. Same _ttl_cache
# pattern used throughout this file; the route above wraps the cached
# gzip bytes in a fresh Response object each time (cheap) rather than
# caching the Response itself.
def _facility_probability_minmax(probs) -> tuple[float, float]:
    """A constant 0-100% range for a facility layer's own PROBABILITY
    coloring: this is a percentage, so the range is always 0-100%, and the
    same applies to the facilities' own impact probabilities. Both
    endpoints are hardcoded absolute constants (1/_BITMASK_ENSEMBLE_SIZE
    ~1.96% .. 1.0), completely ignoring `probs` (this response's own real
    facility values) -- same real constants and reasoning as
    _get_minmax's own PROBABILITY branch (see that function's own comment
    for the full "why": the identical real percentage must always map to
    the identical real color, not just within one response's own facility
    set but everywhere).

    An earlier version of this function derived (min_val, max_val) from
    THIS response's own real nonzero values, which was a bug:
    a lone facility with a genuinely low real probability (e.g. 3.9%)
    could still land near the "max" of a low-variance facility set and
    render as a dark, high-severity-looking color, exactly contradicting
    what a true percentage scale should show. `probs` is kept as a
    parameter for call-site compatibility (every caller still computes
    and passes it) but is now unused.
    """
    return 1.0 / _BITMASK_ENSEMBLE_SIZE, 1.0


def _facility_color_for_prob(base_color: str, prob: float, min_val: float = 1.0 / _BITMASK_ENSEMBLE_SIZE, max_val: float = 1.0) -> tuple[str, int]:
    """Color/radius by the SAME log-scale-with-raised-floor convention the
    raster PROBABILITY layer uses, replacing the old fixed absolute
    breakpoints; reuses _RASTER_PALETTES['PROBABILITY']'s own 10-color ramp
    directly, not a separate facility-only palette, so a marker's color
    and the tile color at that same point mean the same real number.
    Shared by the single-hazard and combined-hazard facility GeoJSON
    builders so the two code paths can never visually diverge for the
    same underlying number and the same (min_val, max_val) range.

    `min_val`/`max_val` come from _facility_probability_minmax (real
    per-response range, not a hardcoded default: the defaults here only
    matter for prob<=0's early return, which ignores them entirely).

    Radius still scales in discrete visual steps (2-10px, a Leaflet dot's
    own legibility concern, distinct from the raster's continuous color
    interpolation) but now driven by the same log-scale `t` (0-1 position
    on the ramp) instead of its own independent absolute breakpoints.
    """
    if prob <= 0:
        return base_color, 2
    colors = _RASTER_PALETTES['PROBABILITY']['colors']
    n = len(colors)
    if max_val <= min_val:
        max_val = min_val + 1e-6
    safe = prob if prob > 0 else min_val
    if min_val > 0:
        log_range = math.log(max_val) - math.log(min_val)
        t = (math.log(safe) - math.log(min_val)) / log_range if log_range else 1.0
    else:
        t = (safe - min_val) / (max_val - min_val)
    t = max(0.0, min(1.0, t))
    idx = min(int(t * (n - 1)), n - 1)
    radius = 4 + round(idx * 6 / (n - 1))
    return colors[idx], radius


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_facility_geojson_body(layer_type, country, storm, forecast_date, wind_threshold,
                                    hazard, gust_threshold, rp_tier, threshold_mm, window_h, combine):
    base_color = _FACILITY_BASE_COLORS[layer_type]
    df = _cache.get_facility_df(layer_type, country.upper(), storm, forecast_date, wind_threshold,
                                hazard, gust_threshold, rp_tier, threshold_mm, window_h)
    features = []
    # Real min/max across this response's own facilities (see
    # _facility_probability_minmax's own docstring), computed once before
    # the per-row loop rather than per-marker.
    min_val, max_val = _facility_probability_minmax(
        pd.to_numeric(df.get("PROBABILITY"), errors='coerce').tolist() if combine and "PROBABILITY" in df.columns else []
    )
    # to_dict("records") instead of iterrows(): iterrows() boxes every row
    # into a fresh pandas Series (dtype-upcasting the whole row to a common
    # type, plus real per-row construction overhead), while to_dict
    # converts the DataFrame to a list of plain dicts in one call and
    # leaves each column's own dtype alone. row.get(...)/row.items() below
    # behave identically on a dict as they did on a Series, so this is a
    # drop-in swap, not a behavior change.
    for row in df.to_dict("records"):
        lat = row.get("LATITUDE")
        lon = row.get("LONGITUDE")
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            continue
        prob = float(row.get("PROBABILITY") or 0) if combine else 0.0
        color, radius = _facility_color_for_prob(base_color, prob, min_val, max_val)
        props = {k.lower(): _safe_prop(v)
                 for k, v in row.items()
                 if k not in ("LATITUDE", "LONGITUDE")
                 and (combine or k != "PROBABILITY")}
        # `_strokeColor` is the facility's own FIXED per-type color
        # (_FACILITY_BASE_COLORS): without this, schools were visually
        # indistinguishable from health centers/shelters/WASH whenever
        # they happened to land on the same probability-derived color,
        # since `_color` (fill AND stroke) was the same
        # single probability-driven value for every facility type. The
        # FILL still varies by real probability (`_color`, unchanged); the
        # OUTLINE now always identifies the real facility TYPE regardless
        # of its current probability, so two co-located facilities of
        # different types stay visually distinguishable at a glance no
        # matter what color their probability-driven fill happens to be.
        props.update({
            "_color": color, "_strokeColor": base_color, "_radius": radius,
            "_opacity": 0.8, "_weight": 2, "_fillOpacity": 0.7,
        })
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "properties": props,
        })
    return gzip.compress(
        json.dumps({"type": "FeatureCollection", "features": features}).encode(),
        compresslevel=6,
    )


# ---------------------------------------------------------------------------
# Combined-hazard facility markers: merges each active hazard's own
# facility table by ZONE_ID (selected for schools/shelters/wash too, not
# just health (see the SQL dicts above), then combines PROBABILITY via
# the same per-tile bitmask union _combine_bitmask_aware uses for the
# raster (see _combine_bitmask_aware_points below).
# ---------------------------------------------------------------------------

def _fetch_one_hazard_facility_rows(layer_type: str, code: str, hazard: str, storm: str,
                                      forecast_date: str, wind_threshold: int,
                                      gust_threshold: Optional[int], rp_tier: Optional[str],
                                      threshold_mm: Optional[float], window_h: Optional[int]) -> list[dict]:
    """Per-hazard facility row fetch for combined-hazard merging: mirrors
    _DataCache.ensure_facility's own inner _load_one, but (a) keeps
    ZONE_ID on every row (never popped) so the caller can merge across
    hazards by it, and (b) skips the single-hazard "no rows -> base layer"
    fallback: a hazard contributing nothing to a combined fetch should
    just be absent from the merge, not silently swapped in as an unrelated
    uncolored base layer for every OTHER hazard's own data too.
    """
    if hazard == "gust":
        rows = _run_query(_FACILITY_GUST_SQL[layer_type], [code, storm, forecast_date, gust_threshold])
    elif hazard == "river":
        rows = _run_query(_FACILITY_RIVER_SQL[layer_type], [code, forecast_date, rp_tier, window_h or _RIVER_WINDOW_DEFAULT])
    elif hazard == "rain":
        rows = _run_query(_FACILITY_PRECIP_SQL[layer_type], [code, forecast_date, threshold_mm, window_h])
    else:
        rows = _run_query(_FACILITY_IMPACT_SQL[layer_type], [code, storm, forecast_date, wind_threshold])
    if layer_type == "health" and hazard in ("wind", "gust", "rain") and rows:
        # Same ZONE_ID -> coords enrichment ensure_facility's own _load_one
        # does (health's lean wind/gust/rain queries carry no LATITUDE/
        # LONGITUDE of their own), river's own health query already
        # resolves them directly via ST_CENTROID, untouched here.
        coords = _cache._ensure_hc_coords_one(hazard, code, storm, forecast_date)
        enriched = []
        for row in rows:
            lat_lon = coords.get(row.get("ZONE_ID"))
            if lat_lon is None:
                continue
            row["LATITUDE"], row["LONGITUDE"] = lat_lon
            enriched.append(row)
        rows = enriched
    return rows


def _combine_bitmask_aware_points(zone_ids: list[str], lats: np.ndarray, lons: np.ndarray,
                                     used_hazard_names: list[str], codes: list[str], storm: str,
                                     hazard_params: dict[str, dict]) -> dict[str, float]:
    """Per-FACILITY true-union combination: same per-tile ensemble-
    popcount methodology _combine_bitmask_aware uses for the raster (see
    that function's own module-level comment), adapted to point locations
    instead of a display tile's own TILE_ID column: each facility is
    snapped onto the same z14 tile grid the raster renders
    (`mercantile.tile(lon, lat, 14)` -> quadkey), so a facility's own
    combined probability is always consistent with whichever raster tile
    it visually sits on top of. Rain is sampled at each facility's own
    exact lat/lon (more precise than the raster path's tile-centroid
    approximation: a facility's coordinates are already known here, no
    approximation needed).

    `codes` (multi-country support): z14 quadkeys are globally unique (a
    given tile_id can only ever belong to ONE place on Earth), so
    Wind/Gust bitmask rows from EVERY country in `codes` are safely
    unioned into one flat TILE_ID lookup with no risk of collision, rather
    than needing to track which country each facility itself belongs to.

    Returns {zone_id: combined_probability}; a zone_id absent from the
    input (should not happen: every entry in `by_zone` has a real
    lat/lon by construction) or with NaN lat/lon gets 0.0 (real, not
    fabricated: no known location means no known hazard exposure).

    Rain is now snapped onto the SAME z14 tile grid as Wind/Gust/River
    (get_rain_tile_bitmask reads TILE_PRECIP_BITMASK_MAT, tile-granularity,
    like every other hazard's own bitmask table), no longer sampled at
    each facility's own exact lat/lon against a dense grid: that used to
    be MORE precise than the raster path's own tile-centroid sampling, a
    real methodological difference between what colors the raster tile
    and what colors the facility marker drawn on top of it. Snapping both
    to the identical tile grid removes that inconsistency (a facility
    always agrees with whichever raster tile it visually sits on top of,
    for every hazard now, not just Wind/Gust/River), at the cost of the
    same tile-vs-exact-point precision loss River/Wind/Gust already accept.
    """
    from components.data.snowflake_utils import (
        get_wind_tile_bitmask, get_gust_tile_bitmask,
        get_river_tile_bitmask, get_rain_tile_bitmask,
    )

    n = len(zone_ids)
    valid_latlon = np.isfinite(lats) & np.isfinite(lons)
    tile_ids = np.full(n, "", dtype=object)
    for i in range(n):
        if not valid_latlon[i]:
            continue
        try:
            t = mercantile.tile(float(lons[i]), float(lats[i]), 14)
            tile_ids[i] = mercantile.quadkey(t)
        except Exception:
            continue

    union_bits = np.zeros(n, dtype=np.uint64)

    def _apply_bits_df(bits_df: Optional[pd.DataFrame]):
        if bits_df is None or bits_df.empty or 'TILE_ID' not in bits_df.columns:
            return
        bits_df = bits_df.drop_duplicates(subset='TILE_ID', keep='first')
        lookup = dict(zip(bits_df['TILE_ID'], pd.to_numeric(bits_df['BITS'], errors='coerce').fillna(0).astype('uint64')))
        for i in range(n):
            if not tile_ids[i]:
                continue
            b = lookup.get(tile_ids[i])
            if b is not None and b:
                union_bits[i] |= np.uint64(b)

    for hz in used_hazard_names:
        p = hazard_params.get(hz)
        if p is None:
            continue
        if hz == 'wind' and p.get('forecast_date'):
            for code in codes:
                _apply_bits_df(get_wind_tile_bitmask(code, storm, p['forecast_date'], p['wind_threshold']))
        elif hz == 'gust' and p.get('forecast_date') and p.get('gust_threshold') is not None:
            for code in codes:
                _apply_bits_df(get_gust_tile_bitmask(code, storm, p['forecast_date'], p['gust_threshold']))
        elif hz == 'river' and p.get('forecast_date'):
            # p['forecast_date'] is already mat-format (same hazard_params
            # convention every hazard here shares), no
            # _mat_date_to_river_date() conversion needed, same reasoning
            # as _combine_bitmask_aware's own river branch.
            rp_tier = p.get('rp_tier') or _RIVER_EXTENT_DEFAULT_RP_TIER
            step_h = p.get('window_h') or _RIVER_WINDOW_DEFAULT
            for code in codes:
                _apply_bits_df(get_river_tile_bitmask(code, p['forecast_date'], rp_tier, step_h))
        elif hz == 'rain' and p.get('forecast_date') and p.get('threshold_mm') is not None:
            # Same mat-format reasoning as the river branch above, no
            # _mat_date_to_rain_date() conversion needed. threshold_mm
            # must be a real one, not a silent _PRECIP_PROB_THRESHOLD_MM
            # fallback, see get_rain_tile_bitmask's own call site in
            # _combine_bitmask_aware for the full "why".
            window_h = p.get('window_h') or _PRECIP_RATE_DEFAULT_WINDOW_H
            threshold_mm = p['threshold_mm']
            for code in codes:
                _apply_bits_df(get_rain_tile_bitmask(code, p['forecast_date'], threshold_mm, window_h))

    popcount = np.zeros(n, dtype=np.float64)
    for m in range(_BITMASK_ENSEMBLE_SIZE):
        popcount += ((union_bits >> np.uint64(m)) & np.uint64(1)).astype(np.float64)
    p_combined = np.where(valid_latlon, popcount / float(_BITMASK_ENSEMBLE_SIZE), 0.0)
    return dict(zip(zone_ids, p_combined))


def _fetch_combined_facility_rows(
    layer_type: str, country: str, storm: str,
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
) -> list[dict]:
    """Merge every active hazard's own facility rows by ZONE_ID, combining
    PROBABILITY via the same per-tile bitmask union _combine_bitmask_aware
    uses for the raster (see _combine_bitmask_aware_points). Base/
    descriptive fields (NAME, TYPE, LATITUDE, LONGITUDE, etc) are filled
    in from whichever hazard's own row has them first: mirrors
    maplibre_tiles.js's own _combineHazardTileProps convention for the
    hover-tooltip path (base fields are hazard-independent, "fill in from
    whichever response has them" is correct, not a MAX/SUM decision).

    `river_window`/`window_h` are kept as two separate params: River and
    Rain can both be simultaneously active within the same combined
    fetch, and their own window option sets differ (river:
    24/72/120/168h, rain: 6/24/72/120h), a single shared `window_h` param
    here would force them onto the same numeric value whenever both are
    checked at once, silently corrupting whichever one didn't match the
    shared value. The single-hazard endpoints (_DataCache.ensure_mercator
    etc.) don't have this problem: they only ever resolve ONE hazard per
    request, so reusing the one generic `window_h` slot there is safe.
    """
    # Same active-hazard definition every other combined path uses, so a
    # hazard that counts as active for the map raster/admin tiles counts as
    # active for the facility markers drawn on top of them too.
    active_params = _build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not active_params:
        return []
    active: list[tuple[str, str]] = [(hz, p["forecast_date"]) for hz, p in active_params]

    codes = [c.upper() for c in country.split('+') if c.strip()]

    def _fetch(item: tuple[str, str]) -> tuple[str, list[dict]]:
        hz, fdate = item
        # river_window and window_h(rain) are deliberately picked apart
        # here, not both passed through unconditionally, see this
        # function's own docstring for why sharing one slot would be wrong
        # whenever river+rain are simultaneously active.
        w = river_window if hz == "river" else (window_h if hz == "rain" else None)
        all_rows: list[dict] = []
        for code in codes:
            all_rows.extend(_fetch_one_hazard_facility_rows(
                layer_type, code, hz, storm, fdate,
                wind_threshold, gust_threshold, rp_tier, threshold_mm, w))
        return hz, all_rows

    if len(active) > 1:
        results = list(_SHARED_EXECUTOR.map(_fetch, active))
    else:
        results = [_fetch(item) for item in active]

    # PROBABILITY is not read off each hazard's own row here: every
    # facility's combined probability comes from the bitmask union below
    # (_combine_bitmask_aware_points), keyed by the facility's own
    # location, not from these per-hazard SQL rows' PROBABILITY column
    # (which stays queried for other purposes upstream). Only the
    # base/descriptive fields (NAME, TYPE, LAT/LON, ...) are merged here.
    by_zone: dict[str, dict] = {}
    for hz, rows in results:
        for row in rows:
            zid = row.get("ZONE_ID")
            if zid is None:
                continue
            entry = by_zone.setdefault(zid, {})
            for k, v in row.items():
                if k in ("ZONE_ID", "PROBABILITY") or v is None:
                    continue
                if entry.get(k) is None:
                    entry[k] = v

    if not by_zone:
        # Mirrors single-hazard mode's own fallback (_DataCache.
        # ensure_facility falls back to _FACILITY_BASE_SQL when a hazard's
        # own query returns zero rows), so facility points stay visible
        # (uncolored) rather than rendering zero markers when every active
        # hazard genuinely returns nothing (e.g. a small country with no
        # schools inside any active hazard's footprint).
        base_rows: list[dict] = []
        for code in codes:
            base_rows.extend(_run_query(_FACILITY_BASE_SQL[layer_type], [code]))
        return base_rows

    # Per-tile-per-member bitmask union, same as _combine_bitmask_aware,
    # but keyed by each facility's own containing z14 tile
    # (mercantile.tile(lon, lat, 14) -> quadkey) instead of a
    # display-tile's TILE_ID column: a facility is a point, not a tile,
    # so this maps it onto the same z14 grid the raster itself renders,
    # keeping a facility's own color/probability consistent with the
    # raster tile it sits on top of. Rain is sampled at the facility's
    # own exact lat/lon (more precise than the raster path's own
    # tile-centroid approximation, since a facility's coordinates are
    # already known here: no approximation needed).
    # The per-hazard param dicts built by _build_combined_active_hazards:
    # each hazard's own forecast_date, threshold and window, including the
    # river_window/window_h split this function's docstring describes.
    hazard_params: dict[str, dict] = dict(active_params)
    zids = list(by_zone.keys())
    lats = np.array([by_zone[z].get("LATITUDE") for z in zids], dtype=np.float64)
    lons = np.array([by_zone[z].get("LONGITUDE") for z in zids], dtype=np.float64)
    # `used_hazard_names` is `active`'s own first elements (`active` is
    # always non-empty by this point, checked above).
    used_hazard_names = [hz for hz, _ in active]
    combined = _combine_bitmask_aware_points(zids, lats, lons, used_hazard_names, codes, storm, hazard_params)

    out = []
    for zid, entry in by_zone.items():
        entry["ZONE_ID"] = zid
        entry["PROBABILITY"] = combined.get(zid, 0.0)
        out.append(entry)
    return out


@app.get("/geojson/facilities-combined/{layer_type}/{country}/{storm}",
         response_class=Response)
def facility_geojson_combined(
    layer_type: str,
    country: str, storm: str,
    wind_on: bool = Query(False),
    wind_forecast_date: Optional[str] = Query(None),
    wind_threshold: int = Query(50),
    gust_on: bool = Query(False),
    gust_threshold: Optional[int] = Query(None),
    river_on: bool = Query(False),
    river_forecast_date: Optional[str] = Query(None),
    rp_tier: Optional[str] = Query(None),
    river_window: Optional[int] = Query(None),
    rain_on: bool = Query(False),
    rain_forecast_date: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> Response:
    """Combined-hazard facility GeoJSON: colors/sizes each facility by the
    per-tile bitmask-union combined PROBABILITY across every simultaneously-
    active hazard (see _fetch_combined_facility_rows / _combine_bitmask_
    aware_points), instead of picking one hazard's own facility table.
    Only invoked client-side when 2+ hazards are simultaneously active
    (see _register_ms_facility_layer in pages/map_shell_concept.py); a
    single active hazard keeps using the existing /geojson/facilities/...
    route.

    `river_window` (kept separate from `window_h`) is River's own
    cumulative lead-time window, see _fetch_combined_facility_rows' own
    docstring for why this can't safely share `window_h` (Rain's own
    window) when both hazards are active at once.
    """
    if layer_type not in _FACILITY_BASE_COLORS:
        raise HTTPException(status_code=404, detail=f"Unknown layer type: {layer_type}")
    try:
        body = _fetch_combined_facility_geojson_body(
            layer_type, country, storm,
            wind_on, wind_forecast_date, wind_threshold,
            gust_on, gust_threshold,
            river_on, river_forecast_date, rp_tier, river_window,
            rain_on, rain_forecast_date, threshold_mm, window_h,
        )
        return Response(
            content=body,
            media_type="application/geo+json",
            headers={"Content-Encoding": "gzip", "Cache-Control": "public, max-age=300"},
        )
    except Exception as exc:
        log.error("facility_geojson_combined error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_combined_facility_geojson_body(
    layer_type, country, storm,
    wind_on, wind_forecast_date, wind_threshold,
    gust_on, gust_threshold,
    river_on, river_forecast_date, rp_tier, river_window,
    rain_on, rain_forecast_date, threshold_mm, window_h,
):
    base_color = _FACILITY_BASE_COLORS[layer_type]
    rows = _fetch_combined_facility_rows(
        layer_type, country.upper(), storm,
        wind_on, wind_forecast_date, wind_threshold,
        gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h,
    )
    features = []
    min_val, max_val = _facility_probability_minmax([row.get("PROBABILITY") for row in rows])
    for row in rows:
        lat, lon = row.get("LATITUDE"), row.get("LONGITUDE")
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            continue
        prob = float(row.get("PROBABILITY") or 0)
        color, radius = _facility_color_for_prob(base_color, prob, min_val, max_val)
        props = {k.lower(): _safe_prop(v) for k, v in row.items() if k not in ("LATITUDE", "LONGITUDE")}
        # `_strokeColor`: see _fetch_facility_geojson_body's own comment
        # for the full "why" (real facility TYPE must stay visually
        # identifiable via a fixed outline color, independent of the
        # probability-driven fill).
        props.update({
            "_color": color, "_strokeColor": base_color, "_radius": radius,
            "_opacity": 0.8, "_weight": 2, "_fillOpacity": 0.7,
        })
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "properties": props,
        })
    return gzip.compress(
        json.dumps({"type": "FeatureCollection", "features": features}).encode(),
        compresslevel=6,
    )


def _get_tile_value_for_hazard(country: str, storm: str, forecast_date: str,
                                 lon: float, lat: float, wind_threshold: int, hazard: str = "wind",
                                 gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                                 threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> dict:
    """Per-hazard property lookup for the z=14 Mercator tile at (lon, lat)
    , the shared core both `tile_value()` (single-hazard hover) and
    `tile_value_combined()` (multi-hazard hover, see that endpoint's own
    docstring) build on. Lets the combined endpoint look up N hazards' own
    PROBABILITY/base values server-side in one request, instead of the
    client doing N separate `/tile-value/` fetches and combining them
    itself client-side."""
    tile = mercantile.tile(lon, lat, 14)
    qk = mercantile.quadkey(tile)
    variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
    key = (country.upper(), storm, forecast_date) + variant
    _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold, hazard,
                          gust_threshold, rp_tier, threshold_mm, window_h)
    df = _cache._mercator.get(key)
    if df is None or df.empty:
        return {}
    row = _filter_by_tile_prefix(df, qk, _cache._mercator_sorted.get(key))
    if row.empty:
        return {}
    result = row.iloc[0].drop(labels=['BW', 'BS', 'BE', 'BN'], errors='ignore').to_dict()
    return {k: (None if pd.isna(v) else v) for k, v in result.items()}


@app.get("/tile-value/{country}/{storm}/{forecast_date}")
def tile_value(
    country: str, storm: str, forecast_date: str,
    lon: float = Query(...),
    lat: float = Query(...),
    wind_threshold: int = Query(...),
    hazard: str = Query("wind"),
    gust_threshold: Optional[int] = Query(None),
    rp_tier: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> dict:
    """Return all property values for the z=14 Mercator tile at the given lon/lat.

    Used for hover tooltips on the SINGLE-hazard raster layer (mode=raw
    single-hazard fallback / any caller that only ever cares about one
    hazard at a time). The real multi-hazard union case is
    `tile_value_combined()` below: this endpoint's own contract/shape is
    otherwise unchanged.
    """
    return _get_tile_value_for_hazard(country, storm, forecast_date, lon, lat, wind_threshold, hazard,
                                        gust_threshold, rp_tier, threshold_mm, window_h)


@app.get("/tile-value-combined/{country}/{storm}")
def tile_value_combined(
    country: str, storm: str,
    lon: float = Query(...),
    lat: float = Query(...),
    wind_on: bool = Query(False),
    wind_forecast_date: Optional[str] = Query(None),
    wind_threshold: int = Query(50),
    gust_on: bool = Query(False),
    gust_threshold: Optional[int] = Query(None),
    river_on: bool = Query(False),
    river_forecast_date: Optional[str] = Query(None),
    rp_tier: Optional[str] = Query(None),
    river_window: Optional[int] = Query(None),
    rain_on: bool = Query(False),
    rain_forecast_date: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> dict:
    """Multi-hazard combined tile-value lookup for hover/click tooltips:
    server-side counterpart to `_combineHazardTileProps` in
    maplibre_tiles.js. The headline "Combined Impact Probability" is
    computed here via the per-tile bitmask union (_combine_bitmask_aware_
    points), matching the same methodology the combined raster underneath
    the tooltip uses (_combine_bitmask_aware), rather than a client-side
    MAX(active hazards' own PROBABILITY) approximation.

    One server round trip replaces N parallel `/tile-value/` fetches plus
    a client-side combine: same rationale `_fetch_combined_facility_rows`
    already established for the facility-marker case.

    Returns `{"combinedProps": {...same shape tile_value() returns per
    hazard, merged: first non-null value per key wins, PROBABILITY
    replaced by the union value}, "perHazardProbs": [{hazard, prob}, ...]}`
    , `perHazardProbs` keeps each hazard's own INDIVIDUAL PROBABILITY
    (read directly off that hazard's own MAT row via
    `_get_tile_value_for_hazard`) for the breakdown sub-rows
    `_buildTileTooltip` renders under the headline figure; only the
    headline "Combined" number comes from the per-member union.
    """
    # Same active-hazard definition (and same per-hazard param dicts) the
    # combined raster/admin tiles under this tooltip resolve, so the
    # headline number can never describe a different hazard set than the
    # pixels it is printed over.
    active = _build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not active:
        return {"combinedProps": {}, "perHazardProbs": []}

    combined_props: dict = {}
    per_hazard_probs: list = []
    for hz, p in active:
        props = _get_tile_value_for_hazard(
            country, storm, p["forecast_date"], lon, lat, p["wind_threshold"], hz,
            p["gust_threshold"], p["rp_tier"], p["threshold_mm"], p["window_h"],
        )
        if not props:
            continue
        prob = props.get("PROBABILITY")
        if prob is not None:
            per_hazard_probs.append({"hazard": hz, "prob": prob})
        for k, v in props.items():
            if v is not None and combined_props.get(k) is None:
                combined_props[k] = v

    if not combined_props and not per_hazard_probs:
        return {"combinedProps": {}, "perHazardProbs": []}

    # Per-tile bitmask union for the headline figure: same methodology
    # _combine_bitmask_aware_points uses for facility markers (river/rain
    # forecast_date arrive here in MAT format, same as the raster/facility
    # paths, that function's own header comment covers the conversion).
    hazard_params: dict[str, dict] = dict(active)
    used_hazard_names = [hz for hz, _ in active]
    codes = [c.upper() for c in country.split('+') if c.strip()]
    union = _combine_bitmask_aware_points(
        ["hover"], np.array([lat], dtype=np.float64), np.array([lon], dtype=np.float64),
        used_hazard_names, codes, storm, hazard_params,
    )
    union_prob = union.get("hover")
    if union_prob is not None:
        combined_props["PROBABILITY"] = float(union_prob)

    return {
        "combinedProps": {k: (None if (isinstance(v, float) and pd.isna(v)) else v) for k, v in combined_props.items()},
        "perHazardProbs": per_hazard_probs,
    }


# ---------------------------------------------------------------------------
# Global raw precipitation-rate endpoints (NOT country/storm-scoped, see
# _PrecipRawCache above)
# ---------------------------------------------------------------------------

@app.get("/tiles/raster/precip-raw/{forecast_time}/{z}/{x}/{y}.webp", response_class=Response)
def precip_raw_tile(
    forecast_time: str, z: int, x: int, y: int,
    mode: str = Query("mean", pattern="^(mean|probability)$"),
    window_h: int = Query(_PRECIP_RATE_DEFAULT_WINDOW_H),
    threshold_mm: float = Query(_PRECIP_PROB_THRESHOLD_MM),
    member: Optional[int] = Query(None, ge=1, le=51),
) -> Response:
    """Global precip raster tile (mm over T+0->T+{window_h}h).

    `forecast_time` may be the literal string "latest" to always track the
    most recent tp forecast cycle without the caller needing to look it up.

    `window_h` (default 6, backward-compatible): accumulation window in
    hours, must be one of _PRECIP_RATE_WINDOWS_H (the same real windows
    ms-rain-window exposes) or the tile renders empty (no precomputed grid
    for it). `threshold_mm` (default 10.0, backward-compatible): only used
    when `mode=probability`, must be one of _PRECIP_PROB_THRESHOLDS_BY_
    WINDOW_MM[window_h] (the same real depth tiers ms-rain-slider exposes for
    that window) or, again, the tile renders empty.

    `mode=mean` (default, backward-compatible): ensemble-mean rate for
    `window_h`, radar-style ramp. `mode=probability`: fraction of ensemble
    members exceeding `threshold_mm` within `window_h`, sequential-purple
    ramp. All are derived from the same cached per-forecast_time download.

    `member` (1-51, optional): when set, ignores `mode`/`threshold_mm`
    entirely and renders ONLY that one ensemble member's own rate: a
    specific member has no Mean/Probability distinction (both are
    across-member reductions with no single-member meaning). Sourced from
    a separate, short-TTL, on-demand fetch (see _PrecipRawCache.
    ensure_member_rate_grid): NOT the persistent aggregate cache this
    endpoint otherwise uses.
    """
    try:
        if member is not None:
            webp_bytes = _fetch_precip_raw_tile_member(forecast_time, z, x, y, window_h, member)
        else:
            webp_bytes = _fetch_precip_raw_tile(forecast_time, z, x, y, mode, window_h, threshold_mm)
    except Exception as exc:
        log.error("precip_raw_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not webp_bytes:
        # Same convention as raster_tile(): a transparent tile (not 204) keeps
        # MapLibre in the z-tile grid instead of falling back to a parent tile.
        return Response(content=_TRANSPARENT_WEBP, media_type="image/webp",
                        headers={"Cache-Control": "public, max-age=3600"})
    return Response(
        content=webp_bytes,
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/tile-value/precip-raw/{forecast_time}")
def precip_raw_tile_value(
    forecast_time: str,
    lon: float = Query(...),
    lat: float = Query(...),
    mode: str = Query("mean", pattern="^(mean|probability)$"),
    window_h: int = Query(_PRECIP_RATE_DEFAULT_WINDOW_H),
    threshold_mm: float = Query(_PRECIP_PROB_THRESHOLD_MM),
    member: Optional[int] = Query(None, ge=1, le=51),
) -> dict:
    """Point lookup for the raw precip-rate raster's hover tooltip. This
    GLOBAL, country-independent layer has its own hover mechanism here
    (assets/maplibre_tiles.js's hover handler otherwise only queries the
    per-country hazard system's own /tile-value/{country}/... endpoint,
    which bails out entirely whenever no country is selected: this
    layer's own normal Global-mode state). Reuses the same cached dense
    grid _fetch_precip_raw_tile already renders 512x512 tiles from
    (_PrecipRawCache.get_render_entry): a single grid-index lookup, no
    fresh Zarr read.

    `mode` mirrors _fetch_precip_raw_tile's own param: only the ONE
    metric matching the active display mode is computed/returned, same as
    the raster itself (the raster only ever renders ONE ramp per `mode`,
    see _render_dense_grid_webp). Computing both mean_mm and probability
    regardless of `mode` would risk the tooltip surfacing a number
    disconnected from what's visually painted: e.g. a spot with an
    unremarkable mean rate (15mm/120h) could clear the mean-intensity
    ramp's own low first break while the map is actually painting
    Probability-mode and showing nothing there at all.

    `member` (1-51, optional): when set, ignores `mode`/`threshold_mm` and
    returns that one member's own rate (from _PrecipRawCache's short-TTL
    member-grid cache, the same one precip_raw_tile's own member branch
    uses) instead of the aggregate mean/probability."""
    if member is not None:
        resolved_m = _precip_cache.ensure_member_rate_grid(forecast_time, window_h)
        if resolved_m is None:
            return {}
        got = _precip_cache.get_member_rate_grid(resolved_m, window_h)
        if got is None:
            return {}
        rate_grid, geo = got
        if member < 1 or member > rate_grid.shape[0]:
            return {}
        entry_m = dict(geo, grid=rate_grid[member - 1])
        val = _sample_global_grid_point(entry_m, entry_m["grid"], lon, lat)
        window_break = _precip_rate_breaks_for_window(window_h)[0]
        if val is not None and val >= window_break:
            return {"member_mm": val, "member": member}
        return {}
    resolved = _precip_cache.ensure_precip_raw(forecast_time)
    if resolved is None:
        return {}
    entry = _precip_cache.get_render_entry(resolved, window_h, threshold_mm)
    if entry is None:
        return {}
    result: dict = {}
    if mode == "mean":
        # This dense grid is finite (non-NaN) almost everywhere, not just
        # where it's visually raining: most of that is a near-zero rate
        # the color ramp itself already treats as invisible (below its
        # own first break, see _precip_rate_breaks_for_window's own
        # docstring). Same threshold the map's own paint uses to decide
        # what counts as visible rain, so the tooltip doesn't report rain
        # where none is painted.
        mean_val = _sample_global_grid_point(entry, entry["grid"], lon, lat)
        mean_break = _precip_rate_breaks_for_window(window_h)[0]
        if mean_val is not None and mean_val >= mean_break:
            result["mean_mm"] = mean_val
    else:
        prob_val = _sample_global_grid_point(entry, entry.get("prob_grid"), lon, lat)
        if prob_val is not None and prob_val > 0:
            result["probability"] = prob_val
    return result


# ---------------------------------------------------------------------------
# Per-member scalar impact endpoints for River
# and Rain, backing the cross-hazard "Compare Worst Case By" feature in
# pages/map_shell_concept.py (_member_river_impacts/_member_rain_impacts).
# Server-to-server ONLY (called from the Dash app process via
# config.TILE_SERVER_URL, never fetched by the browser): returns real
# per-member E_* impact sums for a country's own base tiles, computed
# on-demand from data already resident/fetchable via the raw-layer caches
# above (_river_extent_cache's BITS column, _precip_cache's short-TTL
# member-grid cache), NOT a new persistent per-member cache of their own.
# ---------------------------------------------------------------------------

@app.get("/impact/river-member/{country}/{forecast_time}")
def river_member_impacts(
    country: str, forecast_time: str,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
    step_h: int = Query(_RIVER_EXTENT_DEFAULT_STEP_H),
) -> dict:
    """Real per-member (1-51) river impact sums for `country` at the given
    (forecast_time, rp_tier, step_h): {"members": {"1": {"E_population":
    ..., "E_num_schools": ..., ...}, ..., "51": {...}}, "n_members": 51,
    "resolved_forecast_time": ...}.

    Reuses whatever _river_extent_cache already has resident for the
    aggregate raw layer/combined impact system (no extra parquet download
    if that's already warm), see compute_river_member_metric_sums's own
    docstring for the vectorized bit-decompose + matmul this does instead
    of 51 separate per-member passes.
    """
    empty = {"members": {}, "n_members": 0, "resolved_forecast_time": None}
    try:
        resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier, step_h)
        if resolved is None:
            return empty
        entry = _river_extent_cache.get_grid(resolved, rp_tier, step_h)
        if entry is None or entry["df"].empty:
            return {**empty, "resolved_forecast_time": resolved}
        from components.data.snowflake_utils import get_base_tiles
        base = get_base_tiles(country, zoom_level=_RIVER_EXTENT_ZOOM)
        if base.empty:
            return {**empty, "resolved_forecast_time": resolved}
        base = base.rename(columns={"tile_id": "TILE_ID"})
        sums = compute_river_member_metric_sums(entry, base)
        if sums is None:
            return {**empty, "resolved_forecast_time": resolved}
        members = {str(m): {k: float(v[m - 1]) for k, v in sums.items()} for m in range(1, _RIVER_PROB_ENSEMBLE_SIZE + 1)}
        return {"members": members, "n_members": _RIVER_PROB_ENSEMBLE_SIZE, "resolved_forecast_time": resolved}
    except Exception as exc:
        log.error("river_member_impacts error: %s", exc, exc_info=True)
        return empty


@app.get("/impact/rain-member/{country}/{forecast_time}")
def rain_member_impacts(
    country: str, forecast_time: str,
    window_h: int = Query(_PRECIP_RATE_DEFAULT_WINDOW_H),
    threshold_mm: float = Query(_PRECIP_PROB_THRESHOLD_MM),
) -> dict:
    """Per-member (1-51) rain impact sums for `country`: same shape and
    same full metric set as river_member_impacts.

    Samples the full _MEMBER_METRIC_COLS set (not just E_population)
    against Rain's own per-member exceedance mask: this endpoint reads
    get_base_tiles() directly, which has the exact same age/facility
    columns River's own endpoint uses, unlike MERCATOR_TILE_PRECIP_MAT
    (that pre-aggregated table has no facility columns, see
    _fetch_real_combined_tile_totals_uncached's own comment, accurate for
    that code path). Computing only E_population here would make a
    worst-case member picked for a huge Rain-driven population number
    show a misleading "0" for Children/Schools/HCs/Shelters/WASH at the
    same time: those metrics only ever come from River, which can have
    near-zero signal for whichever specific member Rain's population
    happens to dominate.
    """
    empty = {"members": {}, "n_members": 0, "resolved_forecast_time": None}
    try:
        from components.data.snowflake_utils import get_base_tiles
        base = get_base_tiles(country, zoom_level=14)
        if base.empty:
            return empty
        lats, lons = _get_base_tile_centroids(country, 14, base)
        tile_metrics = {e_col: base[raw_col].fillna(0).to_numpy(dtype=np.float64)
                          for e_col, raw_col in _MEMBER_METRIC_COLS.items() if raw_col in base.columns}
        sums = _precip_cache.compute_member_metric_sums(forecast_time, window_h, threshold_mm, lats, lons, tile_metrics)
        if sums is None:
            return empty
        resolved = forecast_time if forecast_time not in (None, "", "latest") else _precip_cache.resolve_latest_forecast_time()
        members = {str(m): {e_col: float(vals[m - 1]) for e_col, vals in sums.items()} for m in range(1, _PRECIP_PROB_ENSEMBLE_SIZE + 1)}
        return {"members": members, "n_members": _PRECIP_PROB_ENSEMBLE_SIZE, "resolved_forecast_time": resolved}
    except Exception as exc:
        log.error("rain_member_impacts error: %s", exc, exc_info=True)
        return empty


@app.get("/impact/combined-member/{country}")
def combined_member_impacts(
    country: str,
    storm: Optional[str] = Query(None),
    forecast_date: Optional[str] = Query(None),
    wind_threshold: Optional[int] = Query(None, ge=1),
    gust_threshold: Optional[int] = Query(None, ge=1),
    river_forecast_time: Optional[str] = Query(None),
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
    step_h: int = Query(_RIVER_EXTENT_DEFAULT_STEP_H),
    rain_forecast_time: Optional[str] = Query(None),
    window_h: int = Query(_PRECIP_RATE_DEFAULT_WINDOW_H),
    threshold_mm: float = Query(_PRECIP_PROB_THRESHOLD_MM),
) -> dict:
    """Per-member (1-51) COMBINED Wind+Gust+River+Rain impact sums for
    `country`: a TILE-LEVEL UNION across every active hazard (a bit OR'd
    per z14 tile per member), computed BEFORE summing population/facility
    counts, not approximated by combining independently-summed per-hazard
    totals afterward. `country` may cover any subset of these hazards
    being active; a country-aggregate max() across hazards would wrongly
    assume the smaller footprint's exposed population is a SUBSET of the
    larger one's, since different hazards can affect different locations
    within the same country.

    Wind/Gust bitmasks come from TILE_WIND_BITMASK_MAT/TILE_GUST_BITMASK_MAT
    (get_wind_tile_bitmask/get_gust_tile_bitmask in
    components/data/snowflake_utils.py): per-tile, per-member envelope
    coverage. `storm`/`forecast_date` are shared between Wind and Gust
    (same key shape TRACK_MAT/TRACK_GUST_MAT use), only the threshold
    param differs per hazard, and each is independently optional: passing
    only `wind_threshold` (not `gust_threshold`) activates Wind alone,
    and vice versa. Gust is passed whenever the dashboard's own Gust
    toggle is active alongside a flood hazard (see pages/map_shell_concept.py's
    own _member_combined_impacts_impl), so this endpoint activates Wind,
    Gust, both, or neither, independently of the flood-side params.

    River flooding traces river channels/floodplains, a narrow footprint;
    Rain exceedance can be broad and diffuse with no reason to overlap
    those channels: the tile-level union avoids assuming one hazard's
    exposed population is a subset of the other's.

    At least one of {wind_threshold, gust_threshold, river_forecast_time,
    rain_forecast_time} must resolve to something active (all absent
    returns empty): any subset works correctly, degenerating to just
    those hazards' own union (no artificial "union with nothing").
    """
    empty = {"members": {}, "n_members": 0,
              "wind_active": False, "gust_active": False,
              "river_resolved_forecast_time": None, "rain_resolved_forecast_time": None}
    wind_active = wind_threshold is not None and storm is not None and forecast_date is not None
    gust_active = gust_threshold is not None and storm is not None and forecast_date is not None
    if not wind_active and not gust_active and river_forecast_time is None and rain_forecast_time is None:
        return empty
    try:
        from components.data.snowflake_utils import (
            get_base_tiles, get_wind_tile_bitmask, get_gust_tile_bitmask,
            get_river_tile_bitmask, get_rain_tile_bitmask,
        )
        base_raw = get_base_tiles(country, zoom_level=14)
        if base_raw.empty:
            return empty
        # _get_base_tile_centroids' own identity cache below is keyed
        # against base_raw (get_base_tiles()'s own cached object), not the
        # renamed/reset copy: rename()/reset_index(drop=True) both return
        # a NEW DataFrame object every call even when the underlying data
        # is identical, which would defeat the identity check every time.
        # Row order is preserved by both operations, so lats/lons computed
        # from base_raw still align 1:1 with `base` below.
        base = base_raw.rename(columns={"tile_id": "TILE_ID"}).reset_index(drop=True)
        n_tiles = len(base)
        n_members = _RIVER_PROB_ENSEMBLE_SIZE  # == _PRECIP_PROB_ENSEMBLE_SIZE, both real 51-member ensembles

        def _decode_bitmask_matrix(bits_df: Optional[pd.DataFrame], label: str) -> np.ndarray:
            """bits_df: a TILE_ID/BITS DataFrame (get_wind_tile_bitmask's
            own return shape: Snowflake normalizes column aliases to
            UPPERCASE regardless of SQL spelling, see that function's own
            docstring) -> (n_tiles, n_members) boolean matrix LEFT-merged
            against `base`'s own tile order, same pattern river_matrix
            below uses. A tile absent from bits_df (no member's envelope
            reaches it) gets BITS=0.

            `bits_df is None` (the getter's own exception sentinel,
            distinct from a legitimate empty-but-successful query, see
            get_wind_tile_bitmask's own docstring) is logged distinctly
            from a "queried fine, zero rows" result, so a Snowflake
            connection drop / permissions issue / renamed table doesn't
            silently masquerade as "this hazard covers zero tiles" with
            no trace anywhere. The contribution to the union is the same
            all-zero matrix either way (fail-open), only the
            observability differs.

            Also de-duplicates on TILE_ID before merging: the left merge
            below assumes at most one row per tile; a duplicate would
            inflate the merged row count and make the `|` union below
            raise a shape-mismatch error, which the outer try/except
            would turn into a total loss of this endpoint (River and Rain
            too, not just this one hazard). `drop_duplicates` is a no-op
            for well-formed data and a safety net against a future
            duplicate."""
            if bits_df is None:
                log.warning("combined_member_impacts: %s bitmask query failed (see prior error log), "
                            "treating as zero contribution to the union, not zero coverage", label)
                return np.zeros((n_tiles, n_members), dtype=bool)
            if bits_df.empty or "TILE_ID" not in bits_df.columns:
                return np.zeros((n_tiles, n_members), dtype=bool)
            bits_df = bits_df.drop_duplicates(subset="TILE_ID", keep="first")
            merged = base[["TILE_ID"]].merge(bits_df[["TILE_ID", "BITS"]], on="TILE_ID", how="left")
            bits = pd.to_numeric(merged["BITS"], errors="coerce").fillna(0).to_numpy(dtype=np.uint64)
            return ((bits[:, None] >> np.arange(n_members, dtype=np.uint64)) & np.uint64(1)).astype(bool)

        wind_matrix = np.zeros((n_tiles, n_members), dtype=bool)
        if wind_active:
            wind_matrix = _decode_bitmask_matrix(get_wind_tile_bitmask(country, storm, forecast_date, wind_threshold), "wind")

        gust_matrix = np.zeros((n_tiles, n_members), dtype=bool)
        if gust_active:
            gust_matrix = _decode_bitmask_matrix(get_gust_tile_bitmask(country, storm, forecast_date, gust_threshold), "gust")

        # River/Rain now read TILE_RIVER_BITMASK_MAT/TILE_PRECIP_BITMASK_MAT
        # via get_river_tile_bitmask/get_rain_tile_bitmask, the same fast
        # MAT-backed getters Wind/Gust already use just above (via
        # _decode_bitmask_matrix, reused here unchanged) -- REPLACES the
        # live _river_extent_cache/_precip_cache decode of raw global
        # GloFAS/precip source files (a real, measured multi-minute
        # cold-cache cost, see get_river_tile_bitmask's own docstring).
        #
        # river_forecast_time/rain_forecast_time arrive here in each
        # hazard's own RAW format (get_river_extent_forecast_time_for_date/
        # get_precip_forecast_time_near's own real return shape, e.g.
        # '2026-07-14' for river, '2026-07-14 06:00:00' for rain) -- a
        # DIFFERENT convention from _combine_bitmask_aware's own hazard_
        # params, which already arrive mat-format. _raw_date_to_mat()
        # converts to the format TILE_RIVER_BITMASK_MAT/TILE_PRECIP_
        # BITMASK_MAT are actually keyed on, a real, easy-to-miss
        # difference between otherwise near-identical call sites, caught
        # in review before this endpoint's own migration.
        river_matrix = np.zeros((n_tiles, n_members), dtype=bool)
        river_resolved = None
        river_mat_date = _raw_date_to_mat(river_forecast_time)
        if river_mat_date is not None:
            for code in [c.upper() for c in country.split('+') if c.strip()]:
                river_matrix |= _decode_bitmask_matrix(
                    get_river_tile_bitmask(code, river_mat_date, rp_tier, step_h), "river")
            river_resolved = river_forecast_time

        rain_matrix = np.zeros((n_tiles, n_members), dtype=bool)
        rain_resolved = None
        rain_mat_date = _raw_date_to_mat(rain_forecast_time)
        # NOTE: `threshold_mm` here is this endpoint's own top-level param
        # (default _PRECIP_PROB_THRESHOLD_MM=10.0, a real value for the raw
        # continuous-threshold query paths, NOT a value TILE_PRECIP_
        # BITMASK_MAT ever has rows for -- see get_rain_tile_bitmask's own
        # call site in _combine_bitmask_aware for the full "why"). A caller
        # passing rain_forecast_time but omitting threshold_mm would still
        # hit that same silent-empty-result gap; not resolvable at this
        # narrow syntax level since a plain `float` param has no
        # "not provided" sentinel distinct from "explicitly 10.0" the way
        # `Optional[...] = None` params elsewhere in this file do. Flagged
        # in review as a real, currently-unreachable-via-the-live-UI risk
        # (pages/map_shell_concept.py always resolves a real threshold_mm
        # before calling this endpoint's own caller), not fixed here since
        # doing so properly needs an endpoint signature change with wider
        # blast radius than this bugfix pass.
        if rain_mat_date is not None:
            for code in [c.upper() for c in country.split('+') if c.strip()]:
                rain_matrix |= _decode_bitmask_matrix(
                    get_rain_tile_bitmask(code, rain_mat_date, threshold_mm, window_h), "rain")
            rain_resolved = rain_forecast_time

        # OR every active hazard's own matrix BEFORE summing, rather than
        # combining independently-summed totals (via max() or an
        # independence-assumption formula), since this per-tile-per-member
        # data is available for the union.
        union_matrix = (wind_matrix | gust_matrix | river_matrix | rain_matrix).astype(np.float64)
        out: dict[str, np.ndarray] = {}
        # Facility metrics (schools/HCs/shelters/WASH) are genuinely
        # absent for some countries (e.g. Turks and Caicos Islands' own
        # all-NULL severity_num_shelters: same case _real_member_stats's
        # own _v_or_none handles for the wind-only path). Tracked per
        # metric via `unavailable` below and reported as None (not a
        # fabricated 0) for every member when the raw column exists but
        # has zero non-NaN values anywhere in this country's own tile
        # set, rather than reporting a confident "0 at risk" for a metric
        # this country has no data for at all. Population/children/
        # built-up stay in the always-real, fillna(0) group: matching
        # `_v` (not `_v_or_none`) in _real_member_stats's own wind-only
        # body.
        unavailable: set[str] = set()
        for e_col, raw_col in _MEMBER_METRIC_COLS.items():
            col_present = raw_col in base.columns and base[raw_col].notna().any()
            if e_col in _MEMBER_METRIC_NULLABLE_COLS and not col_present:
                unavailable.add(e_col)
                out[e_col] = np.zeros(n_members)  # never read, members dict emits None for this key below
                continue
            vals = base[raw_col].fillna(0).to_numpy(dtype=np.float64) if raw_col in base.columns else np.zeros(n_tiles)
            out[e_col] = union_matrix.T @ vals  # (51,)
        members = {
            str(m): {k: (None if k in unavailable else float(v[m - 1])) for k, v in out.items()}
            for m in range(1, n_members + 1)
        }
        return {"members": members, "n_members": n_members,
                "wind_active": wind_active, "gust_active": gust_active,
                "river_resolved_forecast_time": river_resolved, "rain_resolved_forecast_time": rain_resolved}
    except Exception as exc:
        log.error("combined_member_impacts error: %s", exc, exc_info=True)
        return empty


# Columns _ensure_mercator_base_one's own cached base carries for every
# country: the RAW (not E_*/probability-weighted) values
# combined_country_totals below multiplies by the per-tile bitmask union
# to get an expected-value sum, instead of summing an already
# hazard-specific E_* column.
_COUNTRY_TOTALS_RAW_COLS = [
    'POPULATION', 'INFANT_POPULATION', 'SCHOOL_AGE_POPULATION', 'ADOLESCENT_POPULATION',
    'BUILT_SURFACE_M2', 'NUM_SCHOOLS', 'NUM_HCS', 'NUM_SHELTERS', 'NUM_WASH',
]


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _combined_bitmask_fracs(
    country: str, storm: str,
    wind_on: bool, wind_forecast_date: Optional[str], wind_threshold: int,
    gust_on: bool, gust_threshold: Optional[int],
    river_on: bool, river_forecast_date: Optional[str], rp_tier: Optional[str], river_window: Optional[int],
    rain_on: bool, rain_forecast_date: Optional[str], threshold_mm: Optional[float], window_h: Optional[int],
):
    """Shared core of the real per-tile bitmask union methodology, factored
    out of _combined_country_totals_cached (see that function's own
    docstring for the full real methodology writeup) so
    _combined_admin_totals_cached below can reuse the EXACT same real
    per-tile fractions, merged DataFrame included (carries ADMIN_ID, see
    _MERCATOR_BASE_SQL's own comment), just grouped differently at the
    final summation step instead of duplicating this whole computation a
    second time. Returns None under the exact same "nothing real to
    compute" conditions _combined_country_totals_cached's own early
    returns already covered (no active hazard, no real hazard_dfs, no
    real PROBABILITY column merged), or a tuple:
    (merged, p_combined, tc_only_frac, flood_only_frac, both_frac,
    river_only_frac, rain_only_frac, river_rain_both_frac, has_flood_split)

    @_ttl_cache'd here directly (same decorator/TTL/maxsize its two callers
    already use), not just at the country/admin caller layer: the Full
    Impact Breakdown modal calls BOTH _combined_country_totals_cached (for
    the main table) and _combined_admin_totals_cached (for the Admin Level
    1 table) with IDENTICAL params for the same render, and this function's
    own merge/outer-join/per-tile bitmask decode+popcount is the genuinely
    CPU-bound part of the whole computation (the real Snowflake I/O
    underneath, ensure_mercator/get_wind_tile_bitmask/etc., is already
    @ttl_cache'd separately either way). Without this decorator, opening the
    Admin Level 1 section recomputed that whole merge a second time from
    already-cached raw inputs, real avoidable latency on every modal
    render, not just a cold-cache miss. Safe to cache by reference (not a
    copy): neither caller mutates the returned `merged` DataFrame or any
    frac array in place, both only read from them via pd.to_numeric(...)
    .to_numpy() or by building new Series, matching this file's established
    "cache the objects themselves, callers must not mutate" convention.
    """
    # Same active-hazard definition the combined map layers use, so the
    # headline totals always describe exactly the hazard set the map is
    # painting.
    active = _build_combined_active_hazards(
        wind_on, wind_forecast_date, wind_threshold, gust_on, gust_threshold,
        river_on, river_forecast_date, rp_tier, river_window,
        rain_on, rain_forecast_date, threshold_mm, window_h)
    if not active:
        return None

    def _ensure_one(item: tuple[str, dict]):
        hz, p = item
        _cache.ensure_mercator(country, storm, p["forecast_date"], p["wind_threshold"], hz,
                               p["gust_threshold"], p["rp_tier"], p["threshold_mm"], p["window_h"])
        variant = _hazard_variant(hz, p["wind_threshold"], p["gust_threshold"],
                                    p["rp_tier"], p["threshold_mm"], p["window_h"])
        key = (country, storm, p["forecast_date"]) + variant
        return hz, _cache._mercator.get(key)

    results = (list(_SHARED_EXECUTOR.map(_ensure_one, active)) if len(active) > 1
               else [_ensure_one(item) for item in active])
    hazard_dfs = {hz: df for hz, df in results if df is not None and not df.empty}
    if not hazard_dfs:
        return None

    # Whole-country merge: same shape _fetch_combined_raster_tile builds
    # per display tile, just covering every real tile at once (no
    # _tile_mask filtering). RAW population/facility columns (and ADMIN_ID)
    # are IDENTICAL across every hazard's own DataFrame (all merge onto the
    # SAME cached base df, see ensure_mercator's own _ensure_mercator_
    # base_one call), so they're taken once from whichever hazard has them
    # first, rather than re-merged/combine_first'd per hazard the way
    # PROBABILITY_{hz} (genuinely different per hazard) needs to be.
    merged = None
    used_hazard_names: list[str] = []
    for hz, df in hazard_dfs.items():
        if 'PROBABILITY' not in df.columns:
            continue
        used_hazard_names.append(hz)
        prob_sub = df[['TILE_ID', 'PROBABILITY']].rename(columns={'PROBABILITY': f'PROBABILITY_{hz}'})
        if merged is None:
            base_cols = ['TILE_ID', 'ADMIN_ID', 'BW', 'BS', 'BE', 'BN'] + [c for c in _COUNTRY_TOTALS_RAW_COLS if c in df.columns]
            merged = df[base_cols].copy()
            merged = merged.merge(prob_sub, on='TILE_ID', how='outer')
        else:
            merged = merged.merge(prob_sub, on='TILE_ID', how='outer')
            for c in ['ADMIN_ID', 'BW', 'BS', 'BE', 'BN'] + _COUNTRY_TOTALS_RAW_COLS:
                if c in df.columns and c not in merged.columns:
                    merged = merged.merge(df[['TILE_ID', c]], on='TILE_ID', how='left')
    if merged is None or merged.empty or not used_hazard_names:
        return None

    # A single active hazard has nothing to combine, so it never touches
    # the bitmask union at all: it uses that hazard's own real marginal
    # PROBABILITY_{hz} column directly, the exact same real per-tile MAT
    # data the single-hazard raster/admin layers already render from. The
    # bitmask union (TILE_WIND_BITMASK_MAT/TILE_GUST_BITMASK_MAT plus the
    # on-demand river/rain per-member decode) exists to answer a genuinely
    # different question, "of the 51 members, how many does AT LEAST ONE
    # of several simultaneously-active hazards hit," which only has
    # meaning once 2+ hazards are active together. Routing a single-hazard
    # request through it anyway makes the total silently depend on a data
    # source (the per-member bitmask table) the map's own single-hazard
    # rendering never needs, so a country/date/threshold combination whose
    # marginal PROBABILITY is real but whose bitmask table has no rows for
    # (an older or otherwise unbackfilled run) would compute a confidently
    # wrong 0 here while the map itself renders the real hazard correctly.
    if len(used_hazard_names) == 1:
        only_hz = used_hazard_names[0]
        p_combined = pd.to_numeric(merged[f'PROBABILITY_{only_hz}'], errors='coerce').fillna(0.0).to_numpy(dtype=np.float64)
        n_final = len(merged)
        zeros = np.zeros(n_final, dtype=np.float64)
        if only_hz in ('wind', 'gust'):
            tc_only_frac, flood_only_frac, both_frac = p_combined, zeros, zeros
        else:
            tc_only_frac, flood_only_frac, both_frac = zeros, p_combined, zeros
        river_only_frac = rain_only_frac = river_rain_both_frac = zeros
        has_flood_split = False
    else:
        hazard_params = dict(active)
        p_combined, merged, hazard_bits = _combine_bitmask_aware(merged, used_hazard_names, country, storm, hazard_params,
                                                                   tile_mask=None)

        # TC (Wind|Gust) vs Flood (River|Rain) family split, computed from the
        # same per-tile-per-member bits the union itself uses. `both_frac` is
        # a per-member JOINT check (`tc_bits & flood_bits`, not two marginal
        # `>0` checks ANDed together): the same per-member correctness
        # `_paint_classification_tile` relies on for its own hit-count.
        n_final = len(merged)
        tc_bits = np.zeros(n_final, dtype=np.uint64)
        for hz in ('wind', 'gust'):
            tc_bits |= hazard_bits.get(hz, np.zeros(n_final, dtype=np.uint64))
        flood_bits = np.zeros(n_final, dtype=np.uint64)
        for hz in ('river', 'rain'):
            flood_bits |= hazard_bits.get(hz, np.zeros(n_final, dtype=np.uint64))
        both_bits = tc_bits & flood_bits
        tc_only_bits = tc_bits & ~flood_bits
        flood_only_bits = flood_bits & ~tc_bits
        both_frac = _popcount51(both_bits) / float(_BITMASK_ENSEMBLE_SIZE)
        tc_only_frac = _popcount51(tc_only_bits) / float(_BITMASK_ENSEMBLE_SIZE)
        flood_only_frac = _popcount51(flood_only_bits) / float(_BITMASK_ENSEMBLE_SIZE)

        # Within-Flood split (River Flooding vs Rainfall vs both at once),
        # same real per-member bitmask methodology as the TC-vs-Flood split
        # just above, one level deeper. river_bits/rain_bits already exist
        # in hazard_bits (they're exactly what flood_bits itself was OR'd
        # from a few lines up), so this needs no new data source or extra
        # Snowflake round trip, just the same popcount-of-AND/AND-NOT
        # pattern applied to the two flood members instead of the two
        # families. Only meaningful when River Flooding AND Rainfall are
        # BOTH simultaneously active ('river' in used_hazard_names implies
        # a key in hazard_bits, see the .get(hz, zeros) fallback above); a
        # single active flood member (or Flood not active at all) has
        # nothing to overlap with, so `has_flood_split` stays False and the
        # popup falls back to the old illustrative split for that case,
        # same convention the family split above already uses.
        has_flood_split = 'river' in hazard_bits and 'rain' in hazard_bits
        if has_flood_split:
            river_bits = hazard_bits['river']
            rain_bits = hazard_bits['rain']
            river_rain_both_bits = river_bits & rain_bits
            river_only_bits = river_bits & ~rain_bits
            rain_only_bits = rain_bits & ~river_bits
            river_rain_both_frac = _popcount51(river_rain_both_bits) / float(_BITMASK_ENSEMBLE_SIZE)
            river_only_frac = _popcount51(river_only_bits) / float(_BITMASK_ENSEMBLE_SIZE)
            rain_only_frac = _popcount51(rain_only_bits) / float(_BITMASK_ENSEMBLE_SIZE)
        else:
            zeros_final = np.zeros(n_final, dtype=np.float64)
            river_only_frac = rain_only_frac = river_rain_both_frac = zeros_final

    return (merged, p_combined, tc_only_frac, flood_only_frac, both_frac,
            river_only_frac, rain_only_frac, river_rain_both_frac, has_flood_split)


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _combined_country_totals_cached(
    country: str, storm: str,
    wind_on: bool = False,
    wind_forecast_date: Optional[str] = None,
    wind_threshold: int = 50,
    gust_on: bool = False,
    gust_threshold: Optional[int] = None,
    river_on: bool = False,
    river_forecast_date: Optional[str] = None,
    rp_tier: Optional[str] = None,
    river_window: Optional[int] = None,
    rain_on: bool = False,
    rain_forecast_date: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
) -> dict:
    """Country-wide "at risk" totals via the per-tile bitmask union: the
    same combination methodology as `pages/map_shell_concept.py`'s own
    `_fetch_real_combined_tile_totals_uncached` for the Impact Summary
    panel's headline Children/People/Schools/Health-Centers/Shelters/
    WASH-at-Risk numbers, rather than a row-wise MAX of each hazard's own
    already-probability-weighted E_* column per tile. A row-wise MAX
    avoids double-counting a cell two hazards both threaten, but is not
    the same "count of members where ANY active hazard hits this tile"
    fraction the raster/facility/tooltip paths use.

    Same core computation as `_combine_bitmask_aware` (this is that exact
    function, called with `tile_mask=None`, i.e. every z14 tile in the
    country, not one display tile), except the RAW (not E_*)
    population/facility columns are multiplied by the resulting
    `p_combined` and summed, giving a true expected-value total under
    "the real fraction of the 51-member ensemble where AT LEAST ONE
    active hazard hits this tile", no independence assumption, no MAX
    approximation, the same real methodology the map itself paints with.

    People/Children In Need (PIN/CHIN) is DELIBERATELY NOT computed here
    ; stays wind-only via the existing TRACK_MAT-based path in
    map_shell_concept.py, unchanged (no vulnerability/CCI pipeline exists
    for gust/river/rain to combine into a real per-hazard in-need number,
    same documented limitation the old function already had).

    Returns real weighted sums (population/infant_population/
    school_age_population/adolescent_population/built_surface_m2/
    num_schools/num_hcs/num_shelters/num_wash), or {} when no active
    hazard resolves to any real data for this country/date.
    """
    fracs = _combined_bitmask_fracs(country, storm, wind_on, wind_forecast_date, wind_threshold,
                                       gust_on, gust_threshold, river_on, river_forecast_date, rp_tier, river_window,
                                       rain_on, rain_forecast_date, threshold_mm, window_h)
    if fracs is None:
        return {}
    (merged, p_combined, tc_only_frac, flood_only_frac, both_frac,
     river_only_frac, rain_only_frac, river_rain_both_frac, has_flood_split) = fracs

    def _wsum(col: str, frac: np.ndarray = p_combined) -> Optional[float]:
        if col not in merged.columns or merged[col].isna().all():
            return None
        vals = pd.to_numeric(merged[col], errors='coerce').fillna(0.0).to_numpy(dtype=np.float64)
        return float(np.sum(vals * frac))

    _RAW_COLS = ('POPULATION', 'INFANT_POPULATION', 'SCHOOL_AGE_POPULATION', 'ADOLESCENT_POPULATION',
                 'BUILT_SURFACE_M2', 'NUM_SCHOOLS', 'NUM_HCS', 'NUM_SHELTERS', 'NUM_WASH')
    _RAW_TO_KEY = {'POPULATION': 'population', 'INFANT_POPULATION': 'infant_population',
                   'SCHOOL_AGE_POPULATION': 'school_age_population', 'ADOLESCENT_POPULATION': 'adolescent_population',
                   'BUILT_SURFACE_M2': 'built_surface_m2', 'NUM_SCHOOLS': 'num_schools',
                   'NUM_HCS': 'num_hcs', 'NUM_SHELTERS': 'num_shelters', 'NUM_WASH': 'num_wash'}
    family_split = {
        "tc_only": {_RAW_TO_KEY[c]: _wsum(c, tc_only_frac) for c in _RAW_COLS},
        "flood_only": {_RAW_TO_KEY[c]: _wsum(c, flood_only_frac) for c in _RAW_COLS},
        "both": {_RAW_TO_KEY[c]: _wsum(c, both_frac) for c in _RAW_COLS},
    }
    # Real within-Flood split (see has_flood_split's own comment above),
    # None (not a zeroed-out dict) whenever River Flooding and Rainfall
    # aren't BOTH simultaneously active, so callers can tell "genuinely
    # nothing to split" apart from "real split, and it happens to be 0
    # people in some bucket."
    flood_split = {
        "river_only": {_RAW_TO_KEY[c]: _wsum(c, river_only_frac) for c in _RAW_COLS},
        "rain_only": {_RAW_TO_KEY[c]: _wsum(c, rain_only_frac) for c in _RAW_COLS},
        "both": {_RAW_TO_KEY[c]: _wsum(c, river_rain_both_frac) for c in _RAW_COLS},
    } if has_flood_split else None

    return {
        "population": _wsum('POPULATION'),
        "infant_population": _wsum('INFANT_POPULATION'),
        "school_age_population": _wsum('SCHOOL_AGE_POPULATION'),
        "adolescent_population": _wsum('ADOLESCENT_POPULATION'),
        "built_surface_m2": _wsum('BUILT_SURFACE_M2'),
        "num_schools": _wsum('NUM_SCHOOLS'),
        "num_hcs": _wsum('NUM_HCS'),
        "num_shelters": _wsum('NUM_SHELTERS'),
        "num_wash": _wsum('NUM_WASH'),
        "family_split": family_split,
        "flood_split": flood_split,
    }


@app.get("/impact/combined-totals/{country}/{storm}")
def combined_country_totals(
    country: str, storm: str,
    wind_on: bool = Query(False),
    wind_forecast_date: Optional[str] = Query(None),
    wind_threshold: int = Query(50),
    gust_on: bool = Query(False),
    gust_threshold: Optional[int] = Query(None),
    river_on: bool = Query(False),
    river_forecast_date: Optional[str] = Query(None),
    rp_tier: Optional[str] = Query(None),
    river_window: Optional[int] = Query(None),
    rain_on: bool = Query(False),
    rain_forecast_date: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
) -> dict:
    """Route wrapper around `_combined_country_totals_cached` (see its own
    docstring for the real methodology). The route layer stays thin so the
    actual computation can be `@_ttl_cache`d: repeat requests for the same
    active-hazard parameter tuple (the Impact Summary panel refires this
    on several unrelated UI changes) are served from cache instead of
    redoing the full whole-country bitmask union and family-split popcount
    every time.
    """
    return _combined_country_totals_cached(
        country=country, storm=storm,
        wind_on=wind_on, wind_forecast_date=wind_forecast_date, wind_threshold=wind_threshold,
        gust_on=gust_on, gust_threshold=gust_threshold,
        river_on=river_on, river_forecast_date=river_forecast_date, rp_tier=rp_tier, river_window=river_window,
        rain_on=rain_on, rain_forecast_date=rain_forecast_date, threshold_mm=threshold_mm, window_h=window_h,
    )


@_ttl_cache(ttl_seconds=1800, maxsize=256)
def _base_admin_names_cached(country: str) -> dict:
    """ADMIN_ID -> real NAME lookup for `country`, from BASE_ADMIN_MAT (a
    storm/hazard-independent geography table, unlike ADMIN_ALL_IMPACT_MAT/
    ADMIN_ALL_RIVER_MAT/ADMIN_ALL_PRECIP_MAT which all require a real
    storm/date/threshold match to have any rows at all; this lookup must
    still resolve real names even when NONE of those do, e.g. a flood-only
    scenario with no active storm). Long TTL (30 min): country admin
    boundaries never change within a session. {} on any error/empty
    result, callers fall back to the ADMIN_ID itself as a display name."""
    try:
        rows = _run_query(
            "SELECT TILE_ID, NAME FROM AOTS.TC_ECMWF.BASE_ADMIN_MAT WHERE COUNTRY = %s",
            params=[country],
        )
        return {r["TILE_ID"]: r["NAME"] for r in rows}
    except Exception as exc:
        logger.warning("Could not load BASE_ADMIN_MAT names for %s: %s", country, exc)
        return {}


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _combined_admin_totals_cached(
    country: str, storm: str,
    wind_on: bool = False,
    wind_forecast_date: Optional[str] = None,
    wind_threshold: int = 50,
    gust_on: bool = False,
    gust_threshold: Optional[int] = None,
    river_on: bool = False,
    river_forecast_date: Optional[str] = None,
    rp_tier: Optional[str] = None,
    river_window: Optional[int] = None,
    rain_on: bool = False,
    rain_forecast_date: Optional[str] = None,
    threshold_mm: Optional[float] = None,
    window_h: Optional[int] = None,
    admin_level: int = 1,
) -> list[dict]:
    """Real per-admin-1-region "at risk" totals via the SAME per-tile
    bitmask union _combined_country_totals_cached uses (see its own
    docstring for the full real methodology writeup and
    _combined_bitmask_fracs' own comment for why this reuses that exact
    computation rather than a second copy of it), just GROUPED by ADMIN_ID
    at the final summation step instead of summed to one whole-country
    number. Gives a real per-region combined total AND a real per-region
    TC-only/Both/Flood-only family split (and, when applicable, a real
    within-Flood River-only/Rain-only/Both split too), the exact same
    real methodology the main country-level table above already shows,
    not a fabricated illustrative split under real region names, or one
    separate table per hazard.

    Returns a list of {"admin_id", "name", "population", ...,
    "family_split", "flood_split"} dicts, one per real admin region that
    has at least one real tile in the merged bitmask result (a region with
    zero real tiles for the country's own admin boundary set, which shouldn't
    happen in practice, ADMIN_ID coverage is expected to be exhaustive,
    is simply absent, not fabricated as an all-zero row). Empty list under
    the exact same "nothing real to compute" conditions
    _combined_country_totals_cached itself returns {} for.

    `admin_level` is accepted (kept in the cache key, matching the route's
    own signature) but NOT currently plumbed anywhere below: BASE_MERCATOR_
    TILE_MAT.ADMIN_ID (the grouping key, via _combined_bitmask_fracs) and
    BASE_ADMIN_MAT (the name lookup, via _base_admin_names_cached) are both
    hard-fixed to real admin-1 boundaries only, unlike this codebase's
    OTHER, older admin-level plumbing (_ADMIN_BASE_SQL/_ensure_admin_base_
    one) which genuinely does take and use a real admin_level. Silently
    returning admin-1 data mislabeled as whatever level was requested would
    be exactly the "confident wrong answer with no error" failure mode this
    project's own conventions exist to prevent (see this repo's own
    "fail loudly on a hard structural gap" convention), so any level other
    than the one real level this actually computes fails loudly here
    instead. The only current caller (pages/map_shell_concept.py's
    _fetch_real_combined_admin_totals) never requests anything else, so
    this is dormant today, not a live bug, only a foot-gun for a future
    caller.
    """
    if admin_level != 1:
        raise HTTPException(status_code=400, detail="Only admin_level=1 has real per-region data (BASE_MERCATOR_TILE_MAT/"
                                                        "BASE_ADMIN_MAT are both admin-1-only); no admin_level=2+ backend exists yet.")
    fracs = _combined_bitmask_fracs(country, storm, wind_on, wind_forecast_date, wind_threshold,
                                       gust_on, gust_threshold, river_on, river_forecast_date, rp_tier, river_window,
                                       rain_on, rain_forecast_date, threshold_mm, window_h)
    if fracs is None:
        return []
    (merged, p_combined, tc_only_frac, flood_only_frac, both_frac,
     river_only_frac, rain_only_frac, river_rain_both_frac, has_flood_split) = fracs

    if 'ADMIN_ID' not in merged.columns:
        return []
    admin_ids = merged['ADMIN_ID'].to_numpy()

    # Grouped weighted sum: min_count=1 makes an all-NaN group's own sum
    # come back NaN (-> None below), the SAME real "genuinely no data for
    # THIS region's own facility column" distinction _wsum's own
    # `.isna().all()` check makes at the whole-country level, just applied
    # per-group instead of once across every tile (a region-wide real gap
    # (e.g. a facility column entirely NULL for one specific admin region
    # but real elsewhere in the same country) must stay None there, not a
    # fabricated 0 folded into that region's own total.
    def _gsum_by_admin(col: str, frac: np.ndarray) -> dict:
        if col not in merged.columns:
            return {}
        vals = pd.to_numeric(merged[col], errors='coerce')
        weighted = pd.Series(vals.to_numpy(dtype=np.float64) * frac, index=merged.index)
        weighted[vals.isna()] = np.nan
        grouped = pd.DataFrame({'ADMIN_ID': admin_ids, 'val': weighted}).groupby('ADMIN_ID')['val'].sum(min_count=1)
        return grouped.to_dict()

    _RAW_COLS_ADMIN = ('POPULATION', 'INFANT_POPULATION', 'SCHOOL_AGE_POPULATION', 'ADOLESCENT_POPULATION',
                         'BUILT_SURFACE_M2', 'NUM_SCHOOLS', 'NUM_HCS', 'NUM_SHELTERS', 'NUM_WASH')
    _RAW_TO_KEY_ADMIN = {'POPULATION': 'population', 'INFANT_POPULATION': 'infant_population',
                           'SCHOOL_AGE_POPULATION': 'school_age_population', 'ADOLESCENT_POPULATION': 'adolescent_population',
                           'BUILT_SURFACE_M2': 'built_surface_m2', 'NUM_SCHOOLS': 'num_schools',
                           'NUM_HCS': 'num_hcs', 'NUM_SHELTERS': 'num_shelters', 'NUM_WASH': 'num_wash'}

    totals_by_admin = {col: _gsum_by_admin(col, p_combined) for col in _RAW_COLS_ADMIN}
    tc_only_by_admin = {col: _gsum_by_admin(col, tc_only_frac) for col in _RAW_COLS_ADMIN}
    flood_only_by_admin = {col: _gsum_by_admin(col, flood_only_frac) for col in _RAW_COLS_ADMIN}
    both_by_admin = {col: _gsum_by_admin(col, both_frac) for col in _RAW_COLS_ADMIN}
    if has_flood_split:
        river_only_by_admin = {col: _gsum_by_admin(col, river_only_frac) for col in _RAW_COLS_ADMIN}
        rain_only_by_admin = {col: _gsum_by_admin(col, rain_only_frac) for col in _RAW_COLS_ADMIN}
        river_rain_both_by_admin = {col: _gsum_by_admin(col, river_rain_both_frac) for col in _RAW_COLS_ADMIN}

    names = _base_admin_names_cached(country)

    def _val(d: dict, admin_id) -> Optional[float]:
        v = d.get(admin_id)
        return None if v is None or (isinstance(v, float) and np.isnan(v)) else float(v)

    # Filter out missing ADMIN_ID BEFORE sorting, not inside the loop after
    # sorted() has already run: a handful of z14 tiles can genuinely fall
    # outside every admin boundary (e.g. open water just inside a
    # country's own bounding box), leaving a real None/NaN mixed in among
    # the real string IDs, and sorted() on a set mixing str and float NaN
    # raises TypeError, not something to discover only once such a tile
    # happens to be present.
    real_admin_ids = sorted({
        a for a in set(admin_ids)
        if a is not None and not (isinstance(a, float) and np.isnan(a))
    })
    regions = []
    for admin_id in real_admin_ids:
        family_split = {
            "tc_only": {_RAW_TO_KEY_ADMIN[c]: _val(tc_only_by_admin[c], admin_id) for c in _RAW_COLS_ADMIN},
            "flood_only": {_RAW_TO_KEY_ADMIN[c]: _val(flood_only_by_admin[c], admin_id) for c in _RAW_COLS_ADMIN},
            "both": {_RAW_TO_KEY_ADMIN[c]: _val(both_by_admin[c], admin_id) for c in _RAW_COLS_ADMIN},
        }
        flood_split = None
        if has_flood_split:
            flood_split = {
                "river_only": {_RAW_TO_KEY_ADMIN[c]: _val(river_only_by_admin[c], admin_id) for c in _RAW_COLS_ADMIN},
                "rain_only": {_RAW_TO_KEY_ADMIN[c]: _val(rain_only_by_admin[c], admin_id) for c in _RAW_COLS_ADMIN},
                "both": {_RAW_TO_KEY_ADMIN[c]: _val(river_rain_both_by_admin[c], admin_id) for c in _RAW_COLS_ADMIN},
            }
        regions.append({
            "admin_id": admin_id,
            "name": names.get(admin_id, admin_id),
            "population": _val(totals_by_admin['POPULATION'], admin_id),
            "infant_population": _val(totals_by_admin['INFANT_POPULATION'], admin_id),
            "school_age_population": _val(totals_by_admin['SCHOOL_AGE_POPULATION'], admin_id),
            "adolescent_population": _val(totals_by_admin['ADOLESCENT_POPULATION'], admin_id),
            "built_surface_m2": _val(totals_by_admin['BUILT_SURFACE_M2'], admin_id),
            "num_schools": _val(totals_by_admin['NUM_SCHOOLS'], admin_id),
            "num_hcs": _val(totals_by_admin['NUM_HCS'], admin_id),
            "num_shelters": _val(totals_by_admin['NUM_SHELTERS'], admin_id),
            "num_wash": _val(totals_by_admin['NUM_WASH'], admin_id),
            "family_split": family_split,
            "flood_split": flood_split,
        })
    regions.sort(key=lambda r: r["name"])
    return regions


@app.get("/impact/combined-admin-totals/{country}/{storm}")
def combined_admin_totals(
    country: str, storm: str,
    wind_on: bool = Query(False),
    wind_forecast_date: Optional[str] = Query(None),
    wind_threshold: int = Query(50),
    gust_on: bool = Query(False),
    gust_threshold: Optional[int] = Query(None),
    river_on: bool = Query(False),
    river_forecast_date: Optional[str] = Query(None),
    rp_tier: Optional[str] = Query(None),
    river_window: Optional[int] = Query(None),
    rain_on: bool = Query(False),
    rain_forecast_date: Optional[str] = Query(None),
    threshold_mm: Optional[float] = Query(None),
    window_h: Optional[int] = Query(None),
    admin_level: int = Query(1),
) -> list[dict]:
    """Route wrapper around `_combined_admin_totals_cached` (see its own
    docstring for the real methodology), the admin-region-grouped sibling
    of `combined_country_totals` above. Same thin-route-layer-plus-
    `@_ttl_cache`d-computation split, same reasoning.
    """
    return _combined_admin_totals_cached(
        country=country, storm=storm,
        wind_on=wind_on, wind_forecast_date=wind_forecast_date, wind_threshold=wind_threshold,
        gust_on=gust_on, gust_threshold=gust_threshold,
        river_on=river_on, river_forecast_date=river_forecast_date, rp_tier=rp_tier, river_window=river_window,
        rain_on=rain_on, rain_forecast_date=rain_forecast_date, threshold_mm=threshold_mm, window_h=window_h,
        admin_level=admin_level,
    )


@app.get("/impact/river-curve-excl-rain/{country}/{storm}")
def river_curve_excl_rain(
    country: str, storm: str,
    river_forecast_date: str = Query(...),
    river_window: Optional[int] = Query(None),
    rain_forecast_date: str = Query(...),
    threshold_mm: float = Query(...),
    window_h: int = Query(...),
) -> dict:
    """Batched sibling of combined_country_totals for the Hazard
    Contribution popup's real River Flooding curve (see
    _river_curve_totals_excl_rain's own docstring in map_shell_concept.py
    for the full "why" this needs a real per-tier joint decomposition, not
    the marginal one).

    Sweeps all 6 River RP tiers in ONE request instead of the caller
    making 6 separate HTTP round trips: each tier still goes through the
    exact same `_combined_country_totals_cached` (`@_ttl_cache`d,
    identical correctness to combined_country_totals above), this
    endpoint only removes 5 of those 6 round trips' network overhead and
    this single-process server's own request-serialization cost, letting
    tiers 2-6 reuse whatever this request's own first tier call already
    warmed (the rain side stays fixed across all 6 tiers, so its own
    Zarr/bitmask decode only needs to happen once per request instead of
    racing across 6 concurrent external requests, see the caller's own
    comment about the duplicate-Zarr-download symptom this fixes).

    Returns {"tiers": [...6 RP tier strings...], "flood_splits": [...6
    flood_split dicts, one per tier, same shape combined_country_totals'
    own "flood_split" key returns...]}.
    """
    results = []
    for tier in _RIVER_EXTENT_RP_TIERS:
        totals = _combined_country_totals_cached(
            country=country, storm=storm,
            river_on=True, river_forecast_date=river_forecast_date, rp_tier=tier, river_window=river_window,
            rain_on=True, rain_forecast_date=rain_forecast_date, threshold_mm=threshold_mm, window_h=window_h,
        )
        results.append(totals.get("flood_split"))
    return {"tiers": list(_RIVER_EXTENT_RP_TIERS), "flood_splits": results}


@app.get("/impact/rain-grid-excl-river/{country}/{storm}")
def rain_grid_excl_river(
    country: str, storm: str,
    river_forecast_date: str = Query(...),
    rp_tier: str = Query(...),
    river_window: Optional[int] = Query(None),
    rain_forecast_date: str = Query(...),
    cells: str = Query(..., description='JSON list of [window_h_str, threshold_mm] pairs to sweep'),
) -> dict:
    """Batched sibling of combined_country_totals for the Hazard
    Contribution popup's real Rainfall window x depth-tier grid, same
    real-per-cell-round-trip-elimination fix as river_curve_excl_rain
    above, applied to the OTHER member (12 cells instead of 6 tiers).

    `cells` carries the caller's own _RAIN_MM_BY_WINDOW-derived (window,
    mm) pairs as a JSON string (this module stays import-independent of
    pages/map_shell_concept.py, see _PRECIP_RATE_WINDOWS_H's own comment
    for why that table isn't duplicated here), rather than this endpoint
    hardcoding a second copy of that mapping that could silently drift
    out of sync with the real one.

    Returns {"cells": [...echoed input pairs...], "flood_splits": [...N
    flood_split dicts, aligned 1:1 with `cells`...]}.
    """
    cell_list = json.loads(cells)
    results = []
    for window_h_s, mm in cell_list:
        totals = _combined_country_totals_cached(
            country=country, storm=storm,
            river_on=True, river_forecast_date=river_forecast_date, rp_tier=rp_tier, river_window=river_window,
            rain_on=True, rain_forecast_date=rain_forecast_date, threshold_mm=mm, window_h=int(window_h_s),
        )
        results.append(totals.get("flood_split"))
    return {"cells": cell_list, "flood_splits": results}


@app.get("/stats/precip-raw/{forecast_time}")
def get_precip_raw_stats(
    forecast_time: str,
    mode: str = Query("mean", pattern="^(mean|probability)$"),
    window_h: int = Query(_PRECIP_RATE_DEFAULT_WINDOW_H),
    threshold_mm: float = Query(_PRECIP_PROB_THRESHOLD_MM),
) -> dict:
    """Legend range for the precip palette.

    This is a fixed scale (not a per-country relative scale like the existing
    /stats/{country}/...), returned without forcing a ~1.2GB grid download
    just to answer a legend request.

    `mode=mean`: physical mm-over-window_h scale, breaks scaled per
    `window_h` (see _precip_rate_breaks_for_window). `mode=probability`:
    fixed [0, 1] fraction scale, no need to compute min/max from data
    since exceedance-probability is always in that range by construction;
    echoes back the requested window_h/threshold_mm so the legend can
    show the actual selected threshold rather than a fixed default.
    """
    resolved = (
        _precip_cache.resolve_latest_forecast_time()
        if forecast_time in (None, "", "latest")
        else forecast_time
    )
    if mode == "probability":
        return {
            "min": 0.0,
            "max": 1.0,
            "breaks": _PRECIP_PROB_BREAKS,
            "threshold_mm": threshold_mm,
            "window_h": window_h,
            "forecast_time": resolved,
            "mode": mode,
        }
    window_breaks = _precip_rate_breaks_for_window(window_h)
    return {
        "min": 0.0,
        "max": window_breaks[-1],
        "breaks": window_breaks,
        "window_h": window_h,
        "forecast_time": resolved,
        "mode": mode,
    }


@app.get("/preload/precip-raw/{forecast_time}")
def preload_precip_raw(forecast_time: str) -> dict:
    """Pre-warm the precip-raw grid cache. Returns immediately; the ~1.2GB
    download + Zarr open happens in a background thread (same fire-and-forget
    style as /preload/{country}/{storm}/{forecast_date}).

    Warms EVERY real (window_h, threshold_mm) combination in one download:
    all are computed together in ensure_precip_raw() from the same per-member
    rate_grid(s), so there is no separate "window"/"mode" to pass here.

    Submitted to _PRELOAD_EXECUTOR rather than a raw threading.Thread: a
    fresh OS thread opens a fresh thread-local Snowflake connection
    (get_snowflake_connection caches per-thread) and never explicitly
    closes it before the daemon thread exits, so a preload burst leaks one
    open Snowflake session per call. The pool's long-lived workers reuse
    the same handful of connections across calls instead."""
    def _load():
        try:
            _precip_cache.ensure_precip_raw(forecast_time)
        except Exception as e:
            log.error("Preload precip-raw error: %s", e, exc_info=True)
    _PRELOAD_EXECUTOR.submit(_load)
    return {"status": "loading", "forecast_time": forecast_time}


# ---------------------------------------------------------------------------
# Global raw river endpoints (NOT country/storm-scoped). Backed by
# _RiverExtentCache (extent_rp10_bymember), see that section's module
# comment for the rationale behind this data source. The external URL
# shape (/tiles/raster/river-raw/{forecast_time}/{z}/{x}/{y}.webp,
# /stats/river-raw/{forecast_time}, /preload/river-raw/{forecast_time})
# takes `forecast_time` as a plain date (e.g. "2026-07-14"), since
# extent_rp10_bymember is keyed by DATE rather than a full datetime.
# ---------------------------------------------------------------------------

@app.get("/tiles/raster/river-raw/{forecast_time}/{z}/{x}/{y}.webp", response_class=Response)
def river_raw_raster_tile(
    forecast_time: str, z: int, x: int, y: int,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
    step_h: int = Query(_RIVER_EXTENT_DEFAULT_STEP_H),
    member: Optional[int] = Query(None, ge=1, le=51),
) -> Response:
    """Global river FLOOD-EXTENT (per-member, return-period tier) RASTER
    tile: the endpoint the frontend uses. See the _RiverExtentCache
    module comment above for the rationale (step_h/resolution/
    aggregation choices, and why there is no Mean/Probability mode split).

    `forecast_time` may be the literal string "latest" to always track the
    most recent cycle for the requested `rp_tier` without the caller needing
    to look it up. Unlike the old dis24 layer, this is a plain DATE string
    (e.g. "2026-07-14"), not a full datetime.

    `rp_tier` (default rp10, matching ms-river-slider's own default): one
    of rp2/rp5/rp10/rp20/rp50/rp100. rp2/rp5 are IS_STANDIN=True upstream
    (see _RIVER_EXTENT_STANDIN_RP_TIERS): they reuse rp10's own extent as
    a labelled UPPER-BOUND stand-in (flood extent grows monotonically
    with return period, so RP10's extent conservatively overestimates
    RP2/RP5's true, smaller extent, see TC-ECMWF-Forecast-Pipeline's own
    glofas_extent_masking.py), not an independently-computed rp2/rp5
    result; see /stats/river-raw's own `is_standin` field for surfacing
    this to the frontend.

    Always renders the per-z14-tile fraction of members whose extent
    covers that tile at the requested `rp_tier`, cyan->navy gradient, no
    `mode` query param: river has no second independent quantity the way
    rain has both mm and exceedance-probability, so a Mean/Probability
    toggle here would only describe the identical number under a
    different name/colour; see _fetch_river_extent_raster_tile's own
    docstring.

    `step_h` is a CUMULATIVE window (member-flood union across every day
    from 24h through `step_h`, not a single-day snapshot, see
    _RIVER_EXTENT_STEP_HOURS' own "ACCUMULATION SEMANTICS" comment).

    `member` (1-51, optional): renders ONLY that one ensemble member's own
    flood extent (a flat single-color mask) instead of the aggregate
    member-agreement gradient, see _fetch_river_extent_raster_tile's own
    docstring for the rationale.
    """
    try:
        webp_bytes = _fetch_river_extent_raster_tile(forecast_time, z, x, y, rp_tier, step_h, member)
    except Exception as exc:
        log.error("river_raw_raster_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not webp_bytes:
        # Same convention as precip_raw_tile/raster_tile(): a transparent
        # tile (not 204) keeps MapLibre in the z-tile grid.
        return Response(content=_TRANSPARENT_WEBP, media_type="image/webp",
                        headers={"Cache-Control": "public, max-age=3600"})
    return Response(
        content=webp_bytes,
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/tile-value/river-raw/{forecast_time}")
def river_raw_tile_value(
    forecast_time: str,
    lon: float = Query(...),
    lat: float = Query(...),
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER),
    step_h: int = Query(_RIVER_EXTENT_DEFAULT_STEP_H),
    member: Optional[int] = Query(None, ge=1, le=51),
) -> dict:
    """Point lookup for the raw river-extent raster's hover tooltip: same
    quadkey-lookup pattern as tile_value() (the per-country endpoint),
    against the sparse z14-tile _RiverExtentCache table instead of the
    per-country mercator cache: no fresh parquet read, reuses whatever
    _fetch_river_extent_raster_tile already cached for this
    forecast_time/rp_tier.

    Uses the entry's own "prob_by_tile" dict (built once, see
    _RiverExtentCache's own comment on it) instead of a linear
    `df[df['TILE_ID'] == qk]` scan across every distinct z14 tile in the
    table on every single hover.

    `step_h` is a CUMULATIVE window (see river_raw_raster_tile's own
    docstring): the returned probability is the fraction of members
    flooding this pixel at ANY real day up through `step_h`, not just on
    `step_h` itself.

    `member` (1-51, optional), when set, returns a boolean `flooded` for
    that one member (decoded from the same per-tile BITS bitmask
    river_raw_raster_tile uses) instead of the aggregate `probability`
    fraction."""
    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier, step_h)
    if resolved is None:
        return {}
    entry = _river_extent_cache.get_grid(resolved, rp_tier, step_h)
    if entry is None:
        return {}
    tile = mercantile.tile(lon, lat, 14)
    qk = mercantile.quadkey(tile)
    if member is not None:
        df = entry.get("df")
        if df is None or df.empty:
            return {}
        row = df[df["TILE_ID"] == qk]
        if row.empty:
            return {"flooded": False, "rp_tier": rp_tier, "step_h": step_h, "member": member}
        bits = int(pd.to_numeric(row.iloc[0]["BITS"], errors="coerce") or 0)
        flooded = bool((bits >> (member - 1)) & 1)
        return {"flooded": flooded, "rp_tier": rp_tier, "step_h": step_h, "member": member}
    prob_by_tile = entry.get("prob_by_tile")
    if not prob_by_tile:
        return {}
    prob = prob_by_tile.get(qk)
    if prob is None or (isinstance(prob, float) and math.isnan(prob)):
        return {}
    return {"probability": float(prob), "rp_tier": rp_tier, "step_h": step_h}


@app.get("/stats/river-raw/{forecast_time}")
def get_river_raw_stats(
    forecast_time: str,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
    step_h: int = Query(_RIVER_EXTENT_DEFAULT_STEP_H),
) -> dict:
    """Legend range for the river flood-extent raster (see _RiverExtentCache
    above). Not currently called by the frontend (the Dash app builds its
    own legend text directly, see pages/map_shell_concept.py's
    _legend_raw_flood_info); kept for API-shape parity with precip's own
    real stats endpoint.

    Always returns the continuous [0, 1] probability-fraction shape
    (`breaks`, `ensemble_size`): river has only this one real metric, no
    Mean/Probability mode split.

    `is_standin` is True for rp2/rp5: the frontend uses this to show a
    real, data-driven "not natively computed, RP10 used as an upper-bound
    estimate" note rather than a silently-wrong-looking layer.

    `step_h` is a CUMULATIVE window (see river_raw_raster_tile's own
    docstring), echoed back verbatim: this endpoint's own [0,1] breaks/
    ensemble_size shape is unaffected by the window itself.
    """
    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier, step_h)
    is_standin = rp_tier in _RIVER_EXTENT_STANDIN_RP_TIERS
    if resolved is None:
        return {"min": None, "max": None, "forecast_time": None,
                 "rp_tier": rp_tier, "step_h": step_h, "is_standin": is_standin}
    return {
        "min": 0.0,
        "max": 1.0,
        "breaks": _RIVER_EXTENT_PROB_BREAKS,
        "ensemble_size": _RIVER_PROB_ENSEMBLE_SIZE,
        "forecast_time": resolved,
        "rp_tier": rp_tier,
        "step_h": step_h,
        "is_standin": is_standin,
    }


@app.get("/preload/river-raw/{forecast_time}")
def preload_river_raw(
    forecast_time: str,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
    step_h: int = Query(_RIVER_EXTENT_DEFAULT_STEP_H),
) -> dict:
    """Pre-warm the river flood-extent cache (the sparse zoom-14-tile
    probability table, computed once in ensure_river_extent()) for one real
    (forecast_time, rp_tier, step_h) combination: `step_h` a CUMULATIVE
    window, see river_raw_raster_tile's own docstring. Returns immediately;
    the ~28-56MB parquet download + row-group processing happens in a
    background thread (same fire-and-forget style as
    /preload/precip-raw/{forecast_time}).

    Submitted to _PRELOAD_EXECUTOR rather than a raw threading.Thread,
    same rationale as preload_precip_raw above: reuses the pool's
    long-lived per-worker Snowflake connections instead of leaking one
    fresh connection per call."""
    def _load():
        try:
            _river_extent_cache.ensure_river_extent(forecast_time, rp_tier, step_h)
        except Exception as e:
            log.error("Preload river-raw error: %s", e, exc_info=True)
    _PRELOAD_EXECUTOR.submit(_load)
    return {"status": "loading", "forecast_time": forecast_time}
