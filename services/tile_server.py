"""
tile_server.py — FastAPI PBF tile sidecar for Ahead of the Storm

Mercator tiles: BASE_MERCATOR_TILE_MAT has no GEOMETRY column.
  TILE_ID is a quadkey string. Geometry reconstructed via mercantile.
  PBF encoded via mapbox_vector_tile Python library.

Admin tiles: BASE_ADMIN_GEOM_MAT has a GEOGRAPHY GEOMETRY column.
  ST_INTERSECTS filter in SQL; geometry parsed from GeoJSON returned
  by Snowflake connector; clipped to tile bbox via shapely.

Performance: on first tile request for a (country, storm, forecast_date,
  wind_threshold) combo, ONE bulk Snowflake query loads ALL rows into an
  in-memory pandas DataFrame. Subsequent tiles use vectorised str.startswith
  filtering (~0.5ms for 100k rows, fully thread-safe).

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
from typing import Optional

# Tile data and rendered tiles expire after this many seconds so new pipeline
# output is served without a container restart (matches snowflake_utils TTL).
_TILE_TTL = 15 * 60  # 15 minutes

# _DataCache size caps (see its own _evict_oldest_if_over) — generous enough
# to hold several countries/storms/dates at once (this app's real usage
# pattern) without ever growing unbounded across a long-running session.
# Facility entries (points, not a full tile grid) are cheaper per-key than
# mercator/admin, hence the higher cap.
_MERCATOR_CACHE_MAX = 24
_ADMIN_CACHE_MAX = 24
_FACILITY_CACHE_MAX = 48


def _ttl_cache(ttl_seconds: int, maxsize: int = 128):
    """LRU cache with a sliding PER-ENTRY TTL, thread-safe, single-flight.

    The previous implementation bucketed on time.time() // ttl_seconds,
    which meant every entry in the cache — every tile, across every
    country/storm/property combination — expired at the exact same instant
    every ttl_seconds, a cache stampede under any real concurrent traffic at
    that moment (found in the 2026-08 performance audit). Each entry now
    expires ttl_seconds after IT was individually cached, so misses spread
    out over time instead of synchronizing.

    Single-flight (2026-08 perf audit, finding #3): a miss used to be
    computed outside the lock with no coordination between callers, so N
    concurrent requests for the SAME cold key each redid the full (often
    Snowflake-backed) work, measured as two identical concurrent cold calls
    both taking the full ~2.75s with zero sharing. A `pending` dict now
    tracks an in-flight Future per key; the first caller for a cold key
    computes it and resolves the Future for everyone else waiting on that
    same key, while callers for a DIFFERENT key are still never blocked by
    it (the actual computation still runs outside the lock).
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
            # Computed outside the lock — a slow Snowflake-backed miss on one
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
# entrypoint.sh — never imported into the Dash/gunicorn process), so
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

# SPCS OAuth — when running inside Snowflake Container Services the connector
# reads a short-lived OAuth token from a file mounted by the SPCS runtime.
# Locally, fall back to USER + PASSWORD env vars.
SPCS_RUN        = os.getenv("SPCS_RUN", "false").lower() == "true"
SPCS_TOKEN_PATH = os.getenv("SPCS_TOKEN_PATH", "/snowflake/session/token")
SNOWFLAKE_HOST  = os.getenv("SNOWFLAKE_HOST", "")
SNOWFLAKE_PORT  = int(os.getenv("SNOWFLAKE_PORT") or "443")

# Data source mode — controls where tile/admin/facility data is loaded from.
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
        # Snowflake credentials optional in LOCAL/BLOB mode — stats fall back to DataFrame.
        SNOWFLAKE_USER     = os.getenv("SNOWFLAKE_USER", "")
        SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")

MAT_ZOOM_LEVEL: int = 14

# Thread-local connections — one persistent Snowflake connection per FastAPI
# worker thread, mirroring components/data/snowflake_utils.py's own pattern.
# Previously a SINGLE module-global connection was shared by every thread,
# guarded by a global _query_lock that forced every query in the whole
# process to run one at a time — exactly when concurrent cold-loads (several
# countries/users hitting an empty cache at once) most needed parallelism.
# Each thread now owns its own connection, so concurrent requests execute
# their queries in parallel with no shared-cursor risk.
_thread_local = threading.local()
_CONN_HEALTH_CHECK_INTERVAL = 300  # seconds — matches snowflake_utils.py


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
    return snowflake.connector.connect(**kwargs)


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
    # 512×512 matches the size of data tiles — a 1×1 image may cause MapLibre
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

    # CCI and vulnerability files have NO wind threshold in their filename — they aggregate all thresholds.
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

    # CCI and vulnerability files have NO wind threshold — they aggregate all thresholds.
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
    # Drop raw geometry bytes unconditionally — they're not JSON-serializable.
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
# Stats helper — compute min/max from a cached DataFrame (LOCAL/BLOB mode)
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
# Mercator tiles — reconstruct geometry from quadkey TILE_ID
# ---------------------------------------------------------------------------
#
# Split base / impact (2026-08 perf audit, findings #5+#6): the 15 base
# columns below are byte-identical across every threshold/hazard for a given
# country, only the small impact/vulnerability/CCI columns actually vary
# per (storm, forecast_date, threshold). _MERCATOR_BASE_SQL is queried once
# per (country, zoom_level) and cached with a long TTL (see
# _ensure_mercator_base_one); every hazard/threshold variant below queries
# ONLY its own small ZONE_ID-keyed impact columns and merges them onto the
# cached base DataFrame in pandas (_merge_no_collision) instead of re-running
# a whole-country 3-way LEFT JOIN per threshold. Bonus: the base query's
# bind params are now identical across every threshold, so Snowflake's own
# 24h result cache can serve repeat base loads even across container
# restarts.

_MERCATOR_BASE_SQL = """
SELECT
    b.TILE_ID,
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
# Admin tiles — GEOMETRY column exists; use shapely for clipping
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
# Gust sibling queries — MERCATOR_TILE_GUST_MAT/ADMIN_ALL_GUST_MAT, keyed by
# STORM + FORECAST_DATE + GUST_THRESHOLD (same shape as wind, just a
# different threshold column). Deliberately OMIT the
# MERCATOR_TILE_VULNERABILITY_MAT/MERCATOR_TILE_CCI_MAT joins present in the
# wind query above: both E_PEOPLE_IN_NEED/E_CHILDREN_IN_NEED and
# CCI_CHILDREN/E_CCI_CHILDREN are computed from WIND ensemble envelope data
# specifically (wind-speed-band-weighted) — not hazard-agnostic values.
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
# River-flood sibling queries — MERCATOR_TILE_RIVER_MAT/ADMIN_ALL_RIVER_MAT.
# NOT storm-scoped at all: keyed by COUNTRY + FORECAST_TIME + RP_TIER (a
# return-period tier string 'rp2'/'rp5'/'rp10'/'rp20'/'rp50'/'rp100' — the
# STORM/FORECAST_DATE path segments are ignored for this hazard, see
# tile_server.py's own endpoint functions and map_shell_concept.py's config
# assembly for how the (unused) storm segment is filled with a placeholder).
#
# Real schema surprise (confirmed via a live query, not assumed): both
# MERCATOR_TILE_RIVER_MAT and every *_RIVER_MAT facility table carry MULTIPLE
# STEP_H rows per (COUNTRY, FORECAST_TIME, RP_TIER, tile/facility) — e.g. rp10
# has STEP_H in {48, 72, 96, 168} for the same tile, each a different lead
# time within the same forecast run. Unlike wind/gust (whose WIND_THRESHOLD
# envelope is already a single "ever crossed X kt during the whole track"
# aggregate), river has no such pre-collapsed row. Every river query below
# therefore aggregates across STEP_H with MAX(...) (peak probability/exposure
# at ANY point in the forecast horizon for that RP tier) — the same "at least
# this severity, at some point" semantics the other hazards already have.
# BOOLOR_AGG folds the two boolean flag columns (true if true in ANY step).
#
# E_CHILDREN_TOTAL (2026-08 fix, real bug found+fixed): this table has no
# stored E_CHILDREN_TOTAL column, so it's computed here the same way wind's
# own _MERCATOR_IMPACT_ONLY_SQL does — as a summed expression, not selected
# raw. Uses MAX(E_INFANT_POPULATION + E_SCHOOL_AGE_POPULATION +
# E_ADOLESCENT_POPULATION) — the row-level sum's own peak across STEP_H —
# rather than MAX(E_INFANT)+MAX(E_SCHOOL_AGE)+MAX(E_ADOLESCENT) (summing
# three INDEPENDENTLY-peaking steps), matching the "peak at any single point
# in the forecast horizon" semantics every other column in this query
# already uses, and matching how _RIVER_MERCATOR_STATS_SQL's own e_chi_min/
# e_chi_max already compute this same expression for color-scale
# normalization — that stats query already accounted for "Children (total)"
# correctly; this tile query just never selected the matching value, so the
# color scale was ready but the raster painted nothing for it.
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
WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
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
WHERE COUNTRY = %s AND ADMIN_LEVEL = %s AND FORECAST_TIME = %s AND RP_TIER = %s
GROUP BY TILE_ID
"""

# ---------------------------------------------------------------------------
# Rainfall sibling queries — MERCATOR_TILE_PRECIP_MAT/ADMIN_ALL_PRECIP_MAT.
# Also NOT storm-scoped: keyed by COUNTRY + FORECAST_TIME + THRESHOLD_MM +
# WINDOW_H (uses PRECIP_MAT, not the ratio-based PRECIPRATIO_MAT sibling —
# the app's ms-rain-slider/ms-rain-window controls are already threshold-mm
# based via _RAIN_MM_BY_WINDOW, not ratio based).
#
# Real schema asymmetry (confirmed live, unlike river/wind/gust): only
# E_POPULATION is E_-prefixed on this table — every other exposure column
# (SCHOOL_AGE_POPULATION, NUM_SCHOOLS, etc.) on MERCATOR_TILE_PRECIP_MAT/
# ADMIN_ALL_PRECIP_MAT is a bare, hazard-UNCONDITIONAL duplicate of the base
# layer's own column (same values), not a rain-specific exposed count — no
# "expected number of schools affected by rain" data exists at all. So only
# E_POPULATION is selected below; every other exposure figure comes from the
# base join (merged in pandas) same as always, and the browser's existing
# fmtE()-based tooltip fallback (base_count × probability) naturally supplies
# an estimate for those — no special-case tooltip code needed for this gap.
#
# Confirmed live: ZONE_ID on this table IS a valid mercantile z14 quadkey
# (decodes to real in-country coordinates, e.g. PHL tiles), despite the
# presence of NATIVE_CELL_ROW/NATIVE_CELL_COL columns (that pair references
# the underlying meteorological native grid cell — a separate concept from
# the ZONE_ID display quadkey). _fetch_mercator_tile's bounds-reconstruction
# logic is reused unmodified.
#
# Confirmed live: no STEP_H-style duplication for precip (one row per
# ZONE_ID per (COUNTRY, FORECAST_TIME, THRESHOLD_MM, WINDOW_H) combo) — a
# plain filter suffices, unlike river's MAX(...) aggregation above.
# ---------------------------------------------------------------------------

_MERCATOR_PRECIP_IMPACT_ONLY_SQL = """
SELECT
    ZONE_ID AS TILE_ID,
    PROBABILITY,
    E_POPULATION
FROM AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT
WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
"""

_ADMIN_PRECIP_IMPACT_ONLY_SQL = """
SELECT
    TILE_ID,
    PROBABILITY,
    E_POPULATION
FROM AOTS.TC_ECMWF.ADMIN_ALL_PRECIP_MAT
WHERE COUNTRY = %s AND ADMIN_LEVEL = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
"""



# ---------------------------------------------------------------------------
# Pandas bulk cache — single z=14 DataFrame
# ---------------------------------------------------------------------------
# One bulk Snowflake query loads ALL z=14 tiles for a country at first request.
# Subsequent tiles use vectorised str.startswith filtering (~0.5ms for 100k rows).
# Quadkey prefix hierarchy guarantees every matched tile is fully contained in
# the requested map tile → intersection(tile_box) is always a no-op, skipped.
# All per-tile mercantile calls are hoisted to load time via numpy arrays.
# Thread-safe: pandas DataFrame reads are GIL-protected; writes use
# double-checked locking.

def _precompute_mercator_bounds(tile_ids: "pd.Series") -> pd.DataFrame:
    """Vectorised bounds computation — runs once at bulk-load time."""
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
# e.g. a region selection like "AIA+ATG+..."). Real perf fix (2026-08 audit,
# finding #8's own fix applied to this file's mirror of the same bug):
# these call sites used to open a THROWAWAY `with ThreadPoolExecutor(...)`
# per call, so every worker thread paid a fresh Snowflake connect() handshake
# even on a warm cache, and the connection attached to that thread was never
# closed when the executor exited (leaked until GC/server-side timeout).
# Long-lived worker threads reuse their thread-local connection (see
# get_connection()) after the first call. Sized well above the largest
# per-call fan-out (`min(8, len(codes))`) so a handful of concurrent
# multi-country requests never queue behind each other's leaf queries.
_SHARED_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=16, thread_name_prefix="aots-tile-fanout",
)

# TTL for the base (threshold/storm/hazard-independent) mercator/admin
# caches, see _ensure_mercator_base_one/_ensure_admin_base_one. Population,
# infrastructure counts, and admin-region geometry only change when the
# pipeline re-runs for a country (not per-forecast), so this is deliberately
# much longer-lived than _TILE_TTL (the per-threshold impact data).
_BASE_DATA_TTL = _TILE_TTL * 4  # 60 minutes
_MERCATOR_BASE_CACHE_MAX = 16
_ADMIN_BASE_CACHE_MAX = 16


def _hazard_variant(hazard: str, wind_threshold: int, gust_threshold: Optional[int],
                     rp_tier: Optional[str], threshold_mm: Optional[float],
                     window_h: Optional[int]) -> tuple:
    """Cache-key suffix uniquely identifying a hazard + its own threshold(s).

    Every _DataCache dict is keyed by (country, storm, forecast_date) + this
    variant tuple — so e.g. Wind@50kt, Gust@50kt, River@rp10, and Rain@25mm/6h
    for the exact same country/storm/forecast_date path segments are four
    completely separate cache entries, never collide, and can all be loaded
    and served simultaneously (independently toggleable hazard layers).
    """
    if hazard == "gust":
        return ("gust", gust_threshold)
    if hazard == "river":
        return ("river", rp_tier)
    if hazard == "rain":
        return ("rain", threshold_mm, window_h)
    return ("wind", wind_threshold)


class _DataCache:
    """Bulk-loads from Snowflake; serves from pandas DataFrames.

    Load: ~2s Snowflake + ~0.5s bounds pre-computation (one-time per country/storm).
    Serve: ~0.5ms vectorised str.startswith at all zoom levels.
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

    Base/impact split (2026-08 perf audit, findings #5+#6, SNOWFLAKE mode
    only): ensure_mercator/ensure_admin no longer re-run a whole-country
    3-way LEFT JOIN per threshold. Each first queries/caches its own
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
        self._admin: dict[tuple, pd.DataFrame] = {}
        self._admin_geoms: dict[tuple, tuple] = {}  # key → (geom_list, strtree, props_list)
        self._facility: dict[tuple, pd.DataFrame] = {}
        # Threshold/storm/hazard-independent base caches (findings #5+#6) , 
        # own long TTL via _BASE_DATA_TTL, see _ensure_mercator_base_one /
        # _ensure_admin_base_one. Key shapes ("mercator_base"/"admin_base"
        # prefixed) never collide with the per-variant caches above, sharing
        # the same _loaded_at/_key_locks infra (see _evict_oldest_if_over's
        # own comment on key-shape separation).
        self._mercator_base: dict[tuple, pd.DataFrame] = {}
        self._admin_base: dict[tuple, tuple] = {}  # key → (df, geoms, tree, tile_id_order)
        # ZONE_ID -> (lat, lon) health-centre coordinate lookup (finding #15),
        # key → dict[str, tuple[float, float]], same long _BASE_DATA_TTL as
        # the base caches above — see _ensure_hc_coords_one.
        self._hc_coords: dict[tuple, dict] = {}
        # Per-cache-key lock instead of one instance-wide lock — real perf
        # bug found+fixed here (2026-08, user-reported: a 4-country storm
        # selection loading "way too long"): a single self._load_lock used
        # to serialize EVERY ensure_mercator/ensure_admin/ensure_facility
        # call across the whole process, so an unrelated cache miss (a
        # different hazard, admin level, or facility layer — even from a
        # different browser tab) queued behind whichever load happened to
        # be running, regardless of key. _key_locks_meta_lock only guards
        # the tiny dict-of-locks itself, not the actual loads.
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
        key space, e.g. _admin + _admin_geoms) to `max_size` entries —
        evicts the single oldest-loaded entry (by self._loaded_at) whenever
        a fresh write pushes the primary dict over the cap.

        Real fix (2026-08, multi-agent audit): _is_fresh only ever
        refreshes a STALE key in place — nothing ever REMOVED an old one,
        so a long-running multi-country/multi-storm/multi-date session
        (this app's real usage pattern) grew these dicts unbounded; a
        single mercator entry alone can be 300k-470k rows plus a parsed
        admin geometry list + STRtree per key. Mirrors the "keep latest-N"
        eviction _PrecipRawCache/_RiverExtentCache already do for their own
        caches, called under the same per-key lock every write already
        holds, so this never races with a concurrent load.
        """
        primary = cache_dicts[0]
        if len(primary) <= max_size:
            return
        # Oldest by _loaded_at among keys actually present in the primary
        # dict — _loaded_at is shared across every cache in this class (the
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
                            code, forecast_date, rp_tier,
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
                        vuln_rows = _run_query(_MERCATOR_VULN_ONLY_SQL, [
                            code, MAT_ZOOM_LEVEL, storm, forecast_date,
                        ])
                        cci_rows = _run_query(_MERCATOR_CCI_ONLY_SQL, [
                            code, MAT_ZOOM_LEVEL, storm, forecast_date,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                        if vuln_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(vuln_rows), on="TILE_ID")
                        if cci_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(cci_rows), on="TILE_ID")
                    return merged
                elif hazard == "wind":
                    rows = _load_mercator_from_files(code, storm, forecast_date, wind_threshold)
                    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["TILE_ID"])
                else:
                    log.warning("%s STAGE (file-based) mercator loading not implemented — returning empty for %s",
                                hazard, code)
                    return pd.DataFrame(columns=["TILE_ID"])

            # Independent per-country Snowflake round-trips — safe to run
            # concurrently (pure network I/O). Real perf fix (2026-08,
            # user-reported: a multi-country storm selection, e.g. MELISSA
            # across Turks and Caicos/Jamaica/Cuba/Nicaragua, loading "way
            # too long" when not already prewarmed), this loop used to pay
            # ~2s/country fully serially under the old single global lock.
            # Uses the shared long-lived _SHARED_EXECUTOR (see its own
            # comment) instead of an ephemeral per-call executor.
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
            self._loaded_at[key] = time.time()
            self._evict_oldest_if_over(key, [self._mercator], _MERCATOR_CACHE_MAX)

    def query_mercator(self, country: str, storm: str, forecast_date: str,
                       wind_threshold: int, like_pat: str, z: int, hazard: str = "wind",
                       gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                       threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> list[dict]:
        self.ensure_mercator(country, storm, forecast_date, wind_threshold, hazard,
                             gust_threshold, rp_tier, threshold_mm, window_h)
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        df = self._mercator.get((country, storm, forecast_date) + variant)
        if df is None or df.empty:
            return []
        if like_pat.endswith("%"):
            mask = df["TILE_ID"].str.startswith(like_pat[:-1], na=False)
        else:
            mask = df["TILE_ID"] == like_pat
        return df[mask].to_dict("records")

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
                            code, admin_level, forecast_date, rp_tier,
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
                        vuln_rows = _run_query(_ADMIN_VULN_ONLY_SQL, [
                            code, admin_level, storm, forecast_date,
                        ])
                        cci_rows = _run_query(_ADMIN_CCI_ONLY_SQL, [
                            code, admin_level, storm, forecast_date,
                        ])
                        if impact_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(impact_rows), on="TILE_ID")
                        if vuln_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(vuln_rows), on="TILE_ID")
                        if cci_rows:
                            merged = _merge_no_collision(merged, pd.DataFrame(cci_rows), on="TILE_ID")
                    return merged, geoms, tree, tile_id_order
                elif hazard == "wind":
                    rows = _load_admin_from_files(code, storm, forecast_date, wind_threshold, admin_level)
                    merged = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["TILE_ID"])
                    return merged, None, None, None
                else:
                    log.warning("%s STAGE (file-based) admin loading not implemented — returning empty for %s",
                                hazard, code)
                    return pd.DataFrame(columns=["TILE_ID"]), None, None, None

            # See ensure_mercator's own comment — same real perf fix, same
            # safe-to-parallelize reasoning, same shared executor.
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
                        # Real perf win (finding #6): nothing dropped, single
                        # country, the cached base STRtree's own geometry
                        # set/order is unchanged, so reuse it directly
                        # instead of rebuilding it from scratch every
                        # threshold change (only PROPS actually vary).
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
        """ZONE_ID -> (lat, lon) lookup for health-centre facilities (finding
        #15), cached per (hazard, code, storm, forecast_date) with the same
        long _BASE_DATA_TTL as the mercator/admin base caches — ZONE_ID
        coordinates are threshold-independent within a forecast cycle,
        verified live: the same ZONE_ID resolves to an identical lat/lon
        across every threshold and even across the wind/gust sibling
        tables. Only called for hazard in (wind, gust, rain) — river's
        HC_RIVER_MAT already does its own self-contained ZONE_ID GROUP BY.
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
                                          [code, forecast_date, rp_tier])
                    elif hazard == "rain":
                        rows = _run_query(_FACILITY_PRECIP_SQL[layer_type],
                                          [code, forecast_date, threshold_mm, window_h])
                    else:
                        rows = _run_query(_FACILITY_IMPACT_SQL[layer_type],
                                          [code, storm, forecast_date, wind_threshold])
                    if not rows:
                        log.info("  No impact data for %s %s — using base layer", layer_type, code)
                        rows = _run_query(_FACILITY_BASE_SQL[layer_type], [code])
                    elif layer_type == "health" and hazard in ("wind", "gust", "rain"):
                        # Finding #15 fix: the lean queries above no longer
                        # carry LATITUDE/LONGITUDE (dropped the per-row
                        # ST_CENTROID) — resolve via the cached ZONE_ID
                        # lookup instead. Rows with no coord match (should
                        # never happen given both queries share the same
                        # geometry-not-null filter) are dropped rather than
                        # rendered with a missing/wrong location.
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
                    log.warning("%s STAGE (file-based) facility loading not implemented — returning empty for %s",
                                hazard, code)
                    return []

            all_rows: list[dict] = []
            # See ensure_mercator's own comment — same real perf fix, same
            # safe-to-parallelize reasoning, same shared executor.
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
# Facility SQL — impact tables (with PROBABILITY) and base fallbacks
#
# Health-centre coordinates: the *_HC_MAT impact tables carry no plain
# LATITUDE/LONGITUDE columns (unlike the school/shelter/WASH siblings), only
# a raw ALL_DATA:geometry blob. A NAME-keyed join onto BASE_HC_MAT (which has
# plain LATITUDE/LONGITUDE) was tried as a perf optimization but reverted:
# NAME is not a reliable key — a large share of health-centre NAMEs (roughly
# a fifth to two-fifths of rows, country-dependent, e.g. JPN) are shared by
# multiple facilities at genuinely different coordinates, so a NAME-only join
# silently collapses distinct facilities onto one arbitrary shared location.
#
# Real fix (2026-08-05, finding #15): ZONE_ID — already present on every
# *_HC_MAT table (HC_IMPACT_MAT/HC_GUST_MAT/HC_PRECIP_MAT) — IS a stable,
# collision-free per-facility identifier, verified live against real PHL
# data: comparing derived (ROUND(lat,6), ROUND(lon,6)) per ZONE_ID (not raw
# geometry strings, which have benign encoding variance) across every real
# storm/forecast_date/threshold combination and even across the separate
# wind/gust tables showed 0/2059 zones unstable. So instead of every
# threshold-scoped query recomputing ST_Y/ST_X(ST_CENTROID(...)) per row
# (below, now dropped from these three), coordinates are resolved once per
# (hazard, country, storm, forecast_date) via _ensure_hc_coords_one's own
# ZONE_ID-keyed GROUP BY query (see _HC_COORDS_SQL) and cached with the same
# long _BASE_DATA_TTL as the mercator/admin base caches, then merged onto
# each lean per-threshold row in ensure_facility's _load_one. River's own
# HC_RIVER_MAT (_FACILITY_RIVER_SQL below) already does its own self-
# contained ZONE_ID GROUP BY per query (task from an earlier fix) and is
# untouched by this change — it doesn't need cross-query caching since it
# has no separate base/impact split.
# ---------------------------------------------------------------------------

_FACILITY_IMPACT_SQL: dict[str, str] = {
    "schools": """
        SELECT SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
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
        SELECT NAME, SHELTER_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT NAME, WASH_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

_FACILITY_GUST_SQL: dict[str, str] = {
    "schools": """
        SELECT SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
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
        SELECT NAME, SHELTER_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND GUST_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT NAME, WASH_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_GUST_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND GUST_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

# River facility queries aggregate across STEP_H (same real multi-step-per-tier
# duplication documented for _MERCATOR_RIVER_IMPACT_ONLY_SQL, which applies
# identically to every *_RIVER_MAT facility table — confirmed live:
# SCHOOL_RIVER_MAT alone had 159,652 rows for only 39,913 distinct schools at
# rp10, a 4x duplication exactly matching rp10's 4 real STEP_H values).
#
# GROUP BY ZONE_ID, not by name/type columns — real bug found+fixed here: these
# queries used to group by descriptive columns (SCHOOL_NAME, NAME+TYPE+...),
# which collapses multiple genuinely distinct facilities that share identical
# descriptive metadata (confirmed live against PHL/rp2: NAME-grouping silently
# dropped ~11% of real health centres, ~21% of schools, ~16% of shelters, and
# ~87% of WASH facilities down to one arbitrary shared row each). ZONE_ID is a
# real per-facility identifier already present in every one of these tables
# and confirmed live to never map to more than one distinct name within a
# single (country, forecast_time, rp_tier) slice for any of the four facility
# types — the correct, collision-free key to aggregate the STEP_H duplication
# away without merging distinct real facilities.
_FACILITY_RIVER_SQL: dict[str, str] = {
    "schools": """
        SELECT MAX(SCHOOL_NAME) AS SCHOOL_NAME, MAX(EDUCATION_LEVEL) AS EDUCATION_LEVEL,
               MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY ZONE_ID
    """,
    "health": """
        SELECT NAME, FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY,
               ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(GEOM_STR, 'HEX')))) AS LATITUDE,
               ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(GEOM_STR, 'HEX')))) AS LONGITUDE
        FROM (
            SELECT MAX(NAME) AS NAME, MAX(HEALTH_AMENITY_TYPE) AS FACILITY_TYPE,
                   MAX(OPERATOR_TYPE) AS OPERATOR_TYPE,
                   MAX(PROBABILITY) AS PROBABILITY,
                   MAX(ALL_DATA:geometry::STRING) AS GEOM_STR
            FROM AOTS.TC_ECMWF.HC_RIVER_MAT
            WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
              AND ALL_DATA:geometry::STRING IS NOT NULL
            GROUP BY ZONE_ID
        )
    """,
    "shelters": """
        SELECT MAX(NAME) AS NAME, MAX(SHELTER_TYPE) AS SHELTER_TYPE, MAX(CATEGORY) AS CATEGORY,
               MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY ZONE_ID
    """,
    "wash": """
        SELECT MAX(NAME) AS NAME, MAX(WASH_TYPE) AS WASH_TYPE, MAX(CATEGORY) AS CATEGORY,
               MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY ZONE_ID
    """,
}

# Rain facility queries — no STEP_H duplication (confirmed live), plain SELECT.
_FACILITY_PRECIP_SQL: dict[str, str] = {
    "schools": """
        SELECT SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
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
        SELECT NAME, SHELTER_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "wash": """
        SELECT NAME, WASH_TYPE, CATEGORY, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_PRECIP_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND THRESHOLD_MM = %s AND WINDOW_H = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
}

# ZONE_ID -> (lat, lon) health-centre coordinate lookup, one query per
# (hazard, country, storm, forecast_date) regardless of how many thresholds
# get browsed — see the finding #15 comment above _FACILITY_IMPACT_SQL and
# _DataCache._ensure_hc_coords_one. No threshold filter: pulls every ZONE_ID
# for the whole forecast cycle in one pass, GROUP BY collapses the (verified
# harmless — see comment above) per-threshold-row duplication.
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
        # v == v is False for float NaN — filters out Snowflake NULLs that
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
    return bytes(pbf) if not isinstance(pbf, bytes) else pbf


# ---------------------------------------------------------------------------
# Raster palette definitions + color helpers
# ---------------------------------------------------------------------------

_RASTER_PALETTES: dict[str, dict] = {
    'PROBABILITY':             {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'linear', 'fixed_max': 1.0},
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
# Stored as (min_val, max_val, loaded_at) — expires with _TILE_TTL.
_minmax_cache: dict[tuple, tuple[float, float, float]] = {}
_minmax_lock = threading.Lock()


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
        if fixed_max is not None:
            # Linear scale with known ceiling (e.g. probability 0–1).
            # Anchor min at 0 so the full palette is used correctly.
            result = (0.0, float(fixed_max))
        elif spec.get('scale') == 'rwi':
            result = (-1.0, 1.0)
        elif spec.get('scale') == 'smod':
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
    # Y-axis uses Web Mercator (log) projection — same as MapLibre — so tiles
    # stay aligned at all zoom levels. Linear lat interpolation drifts visibly
    # at low zoom where a single display tile spans many degrees of latitude.
    _merc_tile_n = math.log(math.tan(math.pi / 4 + math.radians(tile_n) / 2))
    _merc_tile_s = math.log(math.tan(math.pi / 4 + math.radians(tile_s) / 2))
    _merc_tile_dh = _merc_tile_n - _merc_tile_s

    # Filter z=14 rows that fall within this display tile via quadkey prefix.
    like_pat = _quadkey_like_pattern(z, x, y)
    if like_pat.endswith('%'):
        prefix = like_pat[:-1]
        mask = df['TILE_ID'].str.startswith(prefix, na=False)
    else:
        mask = df['TILE_ID'] == like_pat
    sub = df[mask]
    if sub.empty:
        return None

    # Min/max for color scaling (computed once, cached).
    minmax = _get_minmax(key, prop)
    if minmax is None:
        return None
    min_val, max_val = minmax

    # Build RGBA canvas — vectorised numpy using exact tile bounds (no seams).
    img_arr = np.zeros((512, 512, 4), dtype=np.uint8)

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

    # Map z=14 cell bounds to pixel coordinates.
    # X: longitude is linear in Web Mercator — straightforward.
    # Y: latitude is logarithmic in Web Mercator — must project before scaling
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
    py0 = np.floor((1.0 - (merc_ns - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32)
    py1 = np.floor((1.0 - (merc_ss - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32) + 1

    # Vectorized scatter-paint — replaces a Python-level `for i in
    # range(len(vals))` loop that re-executed per pixel-rectangle on every
    # cache-miss tile (found to be the single biggest per-request CPU cost
    # in the 2026-08 performance audit). Numpy's documented behavior for
    # fancy-index assignment with duplicate indices (`arr[idx] = values`)
    # applies each value in array order, last one wins — exactly the same
    # last-row-wins semantics the original loop had at cell-boundary
    # overlaps, just computed without a per-row Python iteration. Verified
    # byte-identical to the original loop across 400+ randomized trials
    # (including boundary-clipped and heavily-overlapping cases) before
    # being wired in here — see .perf_scratch_test/ for that check.
    palette_rgba_arr = np.asarray(palette_rgba, dtype=np.uint8)
    x0v = np.maximum(0, px0)
    y0v = np.maximum(0, py0)
    x1v = np.minimum(512, np.maximum(x0v + 1, px1))
    y1v = np.minimum(512, np.maximum(y0v + 1, py1))
    finite = np.isfinite(t)
    wv = np.where(finite, np.maximum(0, x1v - x0v), 0)
    hv = np.where(finite, np.maximum(0, y1v - y0v), 0)
    counts = (wv * hv).astype(np.int64)
    total = int(counts.sum())

    if total > 0:
        # cum_start[i] = pixel index where row i's block begins in the flat
        # `total`-length arrays below — rows are laid out in order, so
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


# ---------------------------------------------------------------------------
# Global raw precipitation-rate raster (NOT country/storm-scoped)
# ---------------------------------------------------------------------------
# Unlike every other hazard in this file (wind/gust/river/rain are all keyed
# by country+storm or country+forecast_date), the raw tp Zarr on
# AOTS.TC_ECMWF.MET_FORECASTS covers the WHOLE WORLD for a single global
# FORECAST_TIME — there is exactly one file to load per forecast cycle, so
# this gets its own small cache class instead of another _DataCache variant.

# New forecast cycles land every 6-24h in production (see MET_FORECASTS
# ingestion cadence), unlike country-scoped impact data which can change
# within a pipeline run — so a TTL far longer than _TILE_TTL (15 min) is
# appropriate: long enough to avoid re-downloading the ~1.2GB Zarr on every
# request, short enough that a new cycle is picked up same-day without a
# container restart.
_PRECIP_RAW_TTL = 4 * 60 * 60  # 4 hours

# T+0 -> T+{window}h accumulated-mm window used as a "current rain rate"
# snapshot. tp is stored as a cumulative total from T+0 (see MET_FORECASTS
# ingestion), so any single step value would be an ever-growing blob
# unrelated to "how hard is it raining right now". Differencing T+0 against
# a later step yields a bounded, radar-like rate instead. T+0 is always the
# window start (rather than e.g. T+72h-T+78h) because it's the closest
# available proxy to current conditions at the model's own init time — later
# windows describe a future period, not "now".
#
# Windows supported here are exactly the real windows the app's own
# per-country rain-hazard UI already exposes via ms-rain-window (see
# pages/map_shell_concept.py's _RAIN_MM_BY_WINDOW dict — this list's values
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
# Real bug found+fixed here: this ramp used to be applied UNSCALED to every
# window's mean-rate grid, even though the grid itself genuinely does hold
# larger accumulated totals for longer windows (real 120h/5-day accumulations
# routinely exceed 150mm — see _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM's own
# 120 entry, the same real depth-tier classification ms-rain-slider exposes).
# A fixed 60mm ceiling meant almost the entire map rendered as one saturated
# "extreme rain" red blob for any window longer than 6h, with zero visual
# resolution above 60mm. _precip_rate_breaks_for_window() below scales this
# base ramp per window using that same real classification data (a single
# source of truth, not an invented second set of numbers).
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
# shrunk member axis for this cycle — e.g. a corrupt/missing perturbed-member
# GRIB upstream — can't silently inflate the probability).
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
# moderate rain — high enough to filter out drizzle/model noise, low enough
# to give useful lead-time signal before conditions turn heavy.
_PRECIP_PROB_THRESHOLD_MM = 10.0

# Real per-window depth-tier thresholds (mm) the app's own ms-rain-slider
# already exposes per ms-rain-window selection — copied verbatim from
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
    entry — the exact same real classification numbers ms-rain-slider's
    severity tiers use) to the base window's (75mm). Real bug fixed here —
    see _PRECIP_RATE_BASE_BREAKS's own comment for the full "why".

    E.g. 120h's top real tier (150mm) is exactly 2x 6h's (75mm), so its ramp
    breaks are [1.0, 10.0, 30.0, 60.0, 120.0] — "extreme rain" now only
    triggers past 120mm/5-days instead of a flat, physically-too-low 60mm.
    """
    base_top = _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM[_PRECIP_RATE_DEFAULT_WINDOW_H][-1]
    window_top = _PRECIP_PROB_THRESHOLDS_BY_WINDOW_MM.get(int(window_h), [base_top])[-1]
    scale = window_top / base_top
    return [round(b * scale, 1) for b in _PRECIP_RATE_BASE_BREAKS]


# Sequential single-hue purple ramp for exceedance PROBABILITY (0-100% of
# members exceeding _PRECIP_PROB_THRESHOLD_MM). Deliberately NOT the
# green/yellow/orange/red "intensity" ramp used for the mean variant above,
# so a screenshot alone makes it unambiguous which mode is showing — a
# single hue ramping from pale to saturated purple is the conventional way
# to encode a 0-1 probability/confidence scale (vs. a multi-hue scale, which
# implies distinct physical categories).
_PRECIP_PROB_BREAKS = [0.10, 0.25, 0.40, 0.60, 0.80]
_PRECIP_PROB_COLORS: list[tuple[int, int, int, int]] = [
    (0,   0,   0,   0),
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

# Rolling prewarm window (see _prewarm_raw_caches's own comment) — the 3 most
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
    ensure_precip_raw() precomputes prob grids under — rounds threshold_mm to
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

    TTL: _PRECIP_RAW_TTL (4h), not _TILE_TTL — see module comment above.
    Thread-safe via double-checked locking, same pattern as _DataCache.
    """

    def __init__(self) -> None:
        self._grid: dict[str, dict] = {}       # forecast_time -> grid entry
        self._loaded_at: dict[str, float] = {}  # forecast_time -> epoch seconds
        self._load_lock = threading.Lock()
        # (forecast_time, stage_path, resolved_at) — cheap SQL-only "latest" lookup,
        # cached separately from the (expensive) grid itself.
        self._latest_lock = threading.Lock()
        self._latest: Optional[tuple[str, str, float]] = None

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
        default pair) from the SAME single downloaded per-member array —
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

        # Real perf fix (2026-08, user-reported: map hover tooltips are
        # "still quite slow") — stage_path is ONLY needed to actually
        # download something below; resolving it for a non-"latest" cycle
        # used to run a real, wholly uncached Snowflake query on EVERY
        # single call (including every single hover request), even when
        # the grid for that exact forecast_time was already fully loaded
        # and fresh in memory. Checking the in-memory cache FIRST — before
        # ever resolving stage_path — means the common "already warm"
        # hover-lookup case now touches Snowflake zero times.
        if forecast_time in self._grid and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _PRECIP_RAW_TTL:
            return forecast_time

        if forecast_time == latest_forecast_time:
            stage_path = latest_stage_path
        else:
            rows = _run_query(_PRECIP_RAW_BY_TIME_SQL, [forecast_time])
            if not rows:
                return None
            stage_path = rows[0]["STAGE_PATH"]

        with self._load_lock:
            if forecast_time in self._grid and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _PRECIP_RAW_TTL:
                return forecast_time
            # forecast_time (and therefore stage_path) identifies an immutable,
            # already-published Zarr file — if we already have IT in memory, the
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
                            # Defensive only — every real tp Zarr uses a 6-hourly step
                            # grid (0..144), which covers all 4 real windows exactly.
                            # A cycle with a genuinely truncated step list (e.g. a
                            # partial/degraded ingestion) just skips that window rather
                            # than crashing the whole cache load.
                            log.warning("PrecipRaw: window_h=%d not in this cycle's steps %s, skipping",
                                        window_h, steps)
                            continue
                        ib = steps.index(window_h)
                        data_b = np.asarray(arr[:, ib, :, :]).astype(np.float32)
                        # (51, n_lat, n_lon) mm over [T+0, T+window_h) — freed at the end
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
                        # rate_grid.shape[0]) — NaNs compare False against the threshold, so they
                        # fall out as "non-exceeding" automatically, same documented convention.
                        #
                        # Real UI tiers for this window, PLUS the legacy default threshold
                        # (only relevant for the default window, but harmless/cheap to
                        # dedupe via `set` for every window) — see
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
                # Grid row 0 = lat_max (rows run N->S) — same convention as the
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
        (window_h, threshold_mm) — the same shape _render_dense_grid_webp/
        _sample_global_grid_tile already expect (and that _RiverExtentCache's
        own get_grid() also returns), so neither of those shared helpers
        needed to change for this per-window/per-threshold cache to exist.

        Returns None if `forecast_time` isn't loaded, or if the mean grid for
        `window_h` was never computed (e.g. a genuinely truncated cycle —
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


_precip_cache = _PrecipRawCache()


def _colorize_precip_rate(vals: np.ndarray, breaks: list[float] = _PRECIP_RATE_BASE_BREAKS) -> np.ndarray:
    """Map a (H, W) grid of mm-over-window_h precip rate to an RGBA
    radar-style image.

    `breaks` (default the base 6h ramp for backward compatibility) should
    normally be _precip_rate_breaks_for_window(window_h)'s own per-window
    scaled breaks — see that function's own docstring for why a fixed ramp
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


def _colorize_precip_probability(vals: np.ndarray) -> np.ndarray:
    """Map a (H, W) grid of exceedance-probability fractions (0-1) to an RGBA
    sequential-purple image.

    See _PRECIP_PROB_BREAKS/_PRECIP_PROB_COLORS above for the exact ramp.
    """
    h, w = vals.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)
    finite = np.isfinite(vals)
    safe = np.where(finite, vals, -1.0)
    idx = np.digitize(safe, _PRECIP_PROB_BREAKS)  # 0..len(_PRECIP_PROB_BREAKS)
    for i, color in enumerate(_PRECIP_PROB_COLORS):
        mask = finite & (idx == i)
        if mask.any():
            img[mask] = color
    return img


def _sample_global_grid_tile(entry: dict, grid: np.ndarray, z: int, x: int, y: int) -> Optional[np.ndarray]:
    """Sample a global dense lat/lon `grid` (row 0 = lat_max, N->S — the same
    convention every *_RawCache grid entry in this file uses) onto a 512x512
    Web-Mercator tile's pixel centers.

    Shared by precip-raw and river-raw raster tiles (both are "one global
    dense grid, no country/storm scoping" hazards) so they use identical
    Web-Mercator inversion / longitude-wrap / row-latitude math — this is a
    straight extraction of _fetch_precip_raw_tile's original inline version,
    parameterized by `entry`/`grid` instead of hardcoding the precip cache.

    Returns None if the tile is entirely outside the grid's latitude
    coverage, or if every sampled pixel would be non-finite (no data here).
    """
    lat_min, lat_max = entry["lat_min"], entry["lat_max"]
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    # Grid covers -60..60 latitude only (see module docstrings) — tiles
    # entirely outside that band (poles) have no data at all; skip sampling.
    if tile_s >= lat_max or tile_n <= lat_min:
        return None

    n_lat, n_lon = entry["n_lat"], entry["n_lon"]
    lon_min = entry["lon_min"]
    lat_step, lon_step = entry["lat_step"], entry["lon_step"]

    # Column (longitude) sample points — linear in Web Mercator x — one per pixel center.
    px = np.arange(512)
    lons = tile_w + (tile_e - tile_w) * (px + 0.5) / 512.0
    lons_wrapped = ((lons + 180.0) % 360.0) - 180.0
    lon_idx = np.round((lons_wrapped - lon_min) / lon_step).astype(np.int64) % n_lon

    # Row (latitude) sample points — Web Mercator y is logarithmic in latitude,
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
    """Point-lookup counterpart to _sample_global_grid_tile — same grid
    convention (row 0 = lat_max, N->S). Used by the raw-layer hover-tooltip
    endpoints (real bug found+fixed 2026-08, user-reported: "there are no
    tooltips for the raw layers... on the map directly, like we had already
    for the storms" — these two global raster layers had NO hover mechanism
    at all, unlike tracks/envelopes (Leaflet tooltips) and the per-country
    hazard tiles (/tile-value/{country}/...)) instead of rendering/sampling
    a full 512x512 tile for a single point."""
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
    dense grid entry (shape shared by _PrecipRawCache and _RiverRawCache — see
    _sample_global_grid_tile). `mode="mean"` uses `entry["grid"]` +
    `mean_colorize`; `mode="probability"` uses `entry["prob_grid"]` +
    `prob_colorize`. Both grids/colorizers come from the SAME cached
    per-forecast_time download — selecting "probability" never triggers a
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
    grid pair to render (see _PrecipRawCache.get_render_entry) — defaults
    match the original hardcoded T+0->T+6h/10mm behavior exactly, so existing
    callers that never pass either param are unaffected.

    `mode` selects which reduction of the selected pair is rendered:
    - "mean" (default, backward-compatible): ensemble-mean rate for
      `window_h`, radar-style green/yellow/orange/red ramp (see
      _colorize_precip_rate). `threshold_mm` is unused for this mode.
    - "probability": fraction of ensemble members exceeding `threshold_mm`
      within `window_h`, sequential-purple ramp (see
      _colorize_precip_probability). All grids come from the SAME cached
      per-forecast_time download — selecting any (window, threshold)
      combination never triggers a second Zarr fetch.

    Returns None if the tile is entirely outside the grid's -60..60 latitude
    coverage, if no data exists for `forecast_time`/`window_h`, or if every
    sampled pixel is transparent (no rain in this tile).
    """
    resolved = _precip_cache.ensure_precip_raw(forecast_time)
    if resolved is None:
        return None
    entry = _precip_cache.get_render_entry(resolved, window_h, threshold_mm)
    # Real bug fixed here: mean_colorize used to always apply the base 6h
    # ramp regardless of window_h — see _precip_rate_breaks_for_window's own
    # docstring. Bound via a closure (not a functools.partial default swap)
    # so _render_dense_grid_webp's generic (vals) -> RGBA colorize signature
    # stays unchanged for river/other callers.
    window_breaks = _precip_rate_breaks_for_window(window_h)
    mean_colorize = lambda vals: _colorize_precip_rate(vals, window_breaks)
    return _render_dense_grid_webp(entry, mode, mean_colorize, _colorize_precip_probability, z, x, y)


# ---------------------------------------------------------------------------
# Global raw river FLOOD-EXTENT raster (RP10 per-member), NOT country/storm-
# scoped — THE CURRENT implementation behind /tiles/raster/river-raw/...,
# /stats/river-raw, /preload/river-raw. Replaces the dis24-discharge-based
# section above (now legacy — see the "LEGACY / SUPERSEDED" banner there).
#
# WHY THE SWITCH: raw dis24 discharge (m3/s) alone is not a meaningful "is
# this actually going to flood" signal — a river carrying 500 m3/s could be a
# completely normal Amazon-scale flow, or a genuinely dangerous flood on a
# small stream. RIVER_FORECASTS PARAM='extent_rp10_bymember' is GloFAS
# discharge ALREADY matched against the real JRC historical flood-extent
# raster at the RP10 (10-year return period) tier, upstream, in the
# TC-ECMWF-Forecast-Pipeline repo (glofas_extent_masking.py) — each row IS a
# pixel that a specific ensemble member's RP10-exceeding discharge actually
# floods, per real JRC-observed flood geometry, not a synthetic per-cell
# threshold guess. RP10 is used (not RP2/RP5): those two tiers are confirmed
# IS_STANDIN=True placeholder/extrapolated data, whereas RP10 is the only
# tier that is a genuinely computed (IS_STANDIN=False) product right now.
#
# SCHEMA: one row per (pixel_lat, pixel_lon, member, step_h) that IS
# flooded — row existence alone means "member M's RP10 flood extent covers
# this pixel at lead time step_h". below_min_basin is a QC/confidence tag on
# an ALREADY-flooded row (small upstream drainage area -> lower confidence in
# the extent estimate), NOT a separate flood/no-flood indicator — a pixel
# with no row at all for a given member/step is simply "not flooded"; there
# is no explicit "False" row anywhere. This code deliberately does NOT filter
# on below_min_basin: dropping those rows would silently discard real (if
# lower-confidence) flood signal that the task never asked to exclude, and
# there is no separately-confirmed-safe cutoff to draw instead.
#
# STEP_H CHOICE: 24 (T+24h). Same reasoning as the OLD dis24 layer's own
# _RIVER_RAW_STEP_INDEX=0 choice above — extent data shares dis24's daily
# step cadence ([24, 48, 72, 96, 120, 144, 168], no T+0), so T+24h is again
# the earliest/most "current-ish" available lead time. Re-applying an
# already-justified convention rather than deriving a fresh one.
#
# RESOLUTION CHOICE (2026-08 revision): zoom-14 Web Mercator TILE granularity
# — the SAME granularity the real downstream impact-calculation pipeline
# (Ahead-of-the-Storm-DATAPIPELINE/impact_analysis.py) already intersects
# this exact per-member pixel data against for population/tile-level
# exposure. Previously this rendered onto a dense 0.1deg lat/lon grid
# (_RIVER_EXTENT_RES_DEG et al, since removed) — that binning was GloFAS's
# own convenient round-number grid, not tied to anything the impact math
# actually uses, and threw away real resolution for no reason tied to how
# the data is consumed elsewhere. Native pixel spacing here is
# ~0.0013-0.0015deg (near-JRC 90-150m resolution); a z14 tile is ~2.45km at
# the equator, so each tile still collapses a small number of native pixels
# — but now the map's own displayed granularity matches the real unit the
# rest of the system reasons about a flooded area in, instead of an
# arbitrary coarser round-number grid. Because flood coverage is sparse
# (most of the world's ~2.68e8 possible z14 tiles have zero signal), the
# per-cycle result is stored as a SPARSE table (one row per distinct
# non-empty z14 tile — real global counts are expected in the tens-of-
# thousands-to-low-hundreds-of-thousands range), not a dense (n, n) array,
# which would be far too much memory (~268M cells) for what is overwhelmingly
# empty space.
#
# AGGREGATION / MEMORY SAFETY: the real per-forecast_time file is ~74M rows
# (28MB compressed) for the whole world, ALL step_h values combined. Loading
# it via pandas.read_parquet() in one shot risks a multi-GB memory spike (an
# estimated 3GB+) for data that, once filtered to step_h==24 and binned,
# collapses to a tiny footprint. Instead this reads the file via
# pyarrow.parquet.ParquetFile.read_row_group() in an explicit per-row-group
# loop (see _RiverExtentCache.ensure_river_extent) — each row group is
# converted to numpy arrays, filtered to step_h==24, mapped to a real z14
# tile index per pixel via vectorized Web Mercator math (numpy, not a
# per-row mercantile.tile() call — that scalar function would be far too
# slow at these row counts), reduced to a small per-batch (distinct-tiles-
# in-this-row-group,) uint64 BITMASK via a vectorized groupby (one bit per
# ensemble member, dedup-by-OR — see below), and merged into the running
# global sparse dict before the row-group's raw arrays are discarded; no
# full-file DataFrame or per-pixel/per-tile dense array is ever
# materialized.
#
# DISTINCT-MEMBER COUNTING: one member's flood can span multiple native
# pixels that collapse into the SAME z14 output tile, so counting raw ROWS
# per tile would double/triple/... count a single member. Instead each row
# sets bit (member-1) of a per-tile np.uint64 (idempotent OR — setting the
# same member's bit twice from two colliding pixels is a no-op), so the
# final popcount of each tile's bitmask is a REAL count of DISTINCT members
# (0-51) that flood that tile at step_h=24, regardless of how many raw
# pixels contributed. 51 members fits comfortably in a uint64's 64 bits.
#
# PROBABILITY ONLY — no Mean mode: river only ever has ONE real per-cell
# metric, count-of-flooded-members / 51 (see above), a TRUE RP-tier
# exceedance fraction, no fabricated quantity involved. This is a MORE
# meaningful number than the old dis24 layer's own "probability" (only ever
# a fallback percentile-of-this-cycle's-own-data proxy, never a real
# return-period value). Rendered as a continuous cyan->navy gradient
# (_RIVER_EXTENT_PROB_COLORS).
#
# A prior revision of this code gave river its own Mean/Probability toggle
# for UI symmetry with precip/wind (first as a flat >50%-consensus mask,
# then as a second continuous gradient over the exact same fraction with a
# different hue) — removed per explicit user request: unlike rain (which
# has both a real mm intensity AND a real exceedance-probability), river has
# no second independent quantity, so a "Mean" option was always describing
# the identical number under a different name. River now always renders
# Probability; the Mean/Probability toggle (flood-view-as) is rain-only.
# ---------------------------------------------------------------------------

# Real bug found+fixed here (2026-08): the module comment above used to claim
# rp10 was "the only genuinely computed (IS_STANDIN=False) tier available" —
# a live query against AOTS.TC_ECMWF.RIVER_FORECASTS shows this was stale:
# rp10/rp20/rp50/rp100 are ALL real (IS_STANDIN=False), each its own distinct
# Parquet file. Only rp2/rp5 are IS_STANDIN=True — the pipeline's own way of
# flagging "not yet independently computed, this file just reuses rp10's own
# extent as a labelled UPPER-BOUND stand-in" until real rp2/rp5 computation
# exists. Confirmed via TC-ECMWF-Forecast-Pipeline's own
# glofas_extent_masking.py module comment: flood extent grows monotonically
# with return period, so RP10's (rarer, more extensive) extent is a
# conservative OVERESTIMATE of RP2/RP5's true (smaller, less severe) extent
# — not a lower bound/underestimate.
_RIVER_EXTENT_RP_TIERS = ("rp2", "rp5", "rp10", "rp20", "rp50", "rp100")  # matches ms-river-slider's own _RIVER_RP_TIERS exactly
_RIVER_EXTENT_STANDIN_RP_TIERS = ("rp2", "rp5")  # IS_STANDIN=True — confirmed live, reuses rp10's own extent
_RIVER_EXTENT_DEFAULT_RP_TIER = "rp10"  # matches ms-river-slider's own default index (2 == "rp10")

# Hardcoded (not read from the Parquet's own member-count/array shape), same
# protective rationale as _PRECIP_PROB_ENSEMBLE_SIZE above: guards against a
# corrupt/short file silently inflating the probability fraction.
_RIVER_PROB_ENSEMBLE_SIZE = 51

# Real extent_rp10_bymember cycles land at most ~once/day in production,
# well past 4h, so this is purely "don't hammer the stage on every request"
# — same rationale as precip's own 4h TTL, reused directly.
_RIVER_EXTENT_TTL = _PRECIP_RAW_TTL

# See module comment above ("STEP_H CHOICE").
_RIVER_EXTENT_STEP_H = 24

# See module comment above ("RESOLUTION CHOICE") — GloFAS's own native
# 0.05deg domain (-60..60 lat x -180..180 lon), rendered at real zoom-14 Web
# Mercator tile granularity (mirrors _fetch_raster_tile's own z14 quadkey
# rendering pattern) rather than a dense degree grid — see
# _RiverExtentCache.ensure_river_extent for the vectorized tile-index math.
_RIVER_EXTENT_ZOOM = 14  # matches MAT_ZOOM_LEVEL — real impact pipeline's own tile granularity
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
    'extent_rp10_bymember') — the exact naming convention every real row in
    that table uses, confirmed live against all 6 real tiers."""
    return f"extent_{rp_tier}_bymember"

# Rolling prewarm window — see _LATEST_3_PRECIP_RAW_SQL's own comment for why
# DISTINCT is required (RIVER_FORECASTS has one row per pixel/member, not one
# per forecast_time). Parameterized by PARAM (rp_tier) — real feature added
# here: the rolling prewarm now covers all 6 real return-period tiers, not
# just the default rp10, since the RP-tier slider is a genuine, real,
# frequently-used control now (previously switching to any other tier paid
# a full 5-20s cold Parquet download+scan every time, for every user, since
# nothing else ever warmed it).
_LATEST_3_RIVER_EXTENT_SQL = """
    SELECT DISTINCT FORECAST_TIME, STAGE_PATH
    FROM AOTS.TC_ECMWF.RIVER_FORECASTS
    WHERE PARAM = %s
    ORDER BY FORECAST_TIME DESC
    LIMIT 3
"""

# Sequential cyan->navy ramp for the PROBABILITY variant (real per-cell
# exceedance fraction — count of the 51 members whose extent covers this
# tile / 51). Deliberately a different hue family from the OLD (legacy)
# river-probability teal ramp and from precip's purple ramp, so all three
# stay visually distinguishable.
_RIVER_EXTENT_PROB_BREAKS = [0.10, 0.25, 0.40, 0.60, 0.80]
_RIVER_EXTENT_PROB_COLORS: list[tuple[int, int, int, int]] = [
    (0,   0,   0,   0),
    (178, 235, 242, 90),
    (77,  208, 225, 140),
    (0,   172, 193, 180),
    (0,   105, 146, 215),
    (1,   50,  96,  245),
]

class _RiverExtentCache:
    """Downloads+processes the latest global extent_rp10_bymember Parquet
    ONCE per forecast_time via a memory-conscious pyarrow row-group loop (see
    the module-level comment above for the full memory-safety rationale —
    NEVER materializes the full ~74M-row file as one pandas DataFrame),
    producing a SPARSE per-zoom-14-tile DataFrame (one row per distinct
    z14 tile with >=1 flooded member) — the SAME shape _fetch_raster_tile
    already expects (TILE_ID quadkey + BW/BS/BE/BN bounds), so tile
    rendering can reuse that function's own vectorized scatter-paint
    pattern (see _fetch_river_extent_raster_tile below) instead of the old
    dense-grid sample/colorize path (_sample_global_grid_tile/
    _render_dense_grid_webp — those remain unchanged and are still used by
    precip-raw, which stays on a dense global grid).

    Per-tile columns: 'TILE_ID' (str quadkey), 'BW'/'BS'/'BE'/'BN' (float
    z14 tile bounds), 'member_count' (int 0-51, real distinct-member
    popcount), 'probability' (float32 member_count/51 — real per-cell RP10
    exceedance fraction, the one metric this layer renders — see
    _fetch_river_extent_raster_tile's own docstring below).

    TTL: _RIVER_EXTENT_TTL (4h). Thread-safe via double-checked locking, same
    pattern as _PrecipRawCache/_RiverRawCache above.
    """

    def __init__(self) -> None:
        self._grids: dict[tuple[str, str], dict] = {}       # (forecast_time, rp_tier) -> grid entry
        self._loaded_at: dict[tuple[str, str], float] = {}   # (forecast_time, rp_tier) -> epoch seconds
        self._load_lock = threading.Lock()
        self._latest_lock = threading.Lock()
        self._latest: dict[str, tuple[str, str, float]] = {}  # rp_tier -> (forecast_time, stage_path, resolved_at)

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
            forecast_time = str(rows[0]["FORECAST_TIME"])
            stage_path = rows[0]["STAGE_PATH"]
            self._latest[rp_tier] = (forecast_time, stage_path, now)
            return forecast_time, stage_path

    def resolve_latest_forecast_time(self, rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER) -> Optional[str]:
        """Cheap (SQL-only, no download) lookup of the latest forecast_time
        for a given rp_tier."""
        resolved = self._resolve_latest(rp_tier)
        return resolved[0] if resolved else None

    def ensure_river_extent(self, forecast_time: Optional[str],
                              rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER) -> Optional[str]:
        """Ensure the sparse zoom-14-tile flood-extent table for
        (`forecast_time`, `rp_tier`) is loaded in memory.

        `forecast_time` of None/""/"latest" always resolves to the current
        latest cycle FOR THAT TIER (different tiers can, in principle, have
        different latest dates, though in practice they land together).
        `rp_tier` defaults to rp10 (the slider's own default) for backward
        compatibility with every existing caller that doesn't pass it yet.
        Returns the resolved forecast_time string actually loaded (a plain
        date like "2026-07-14" — this data is keyed by DATE, unlike dis24's
        full datetime FORECAST_TIME), or None if no data exists at all for
        this (forecast_time, rp_tier) combination, or globally for this tier.
        """
        latest = self._resolve_latest(rp_tier)
        if latest is None:
            return None
        latest_forecast_time, latest_stage_path = latest

        if forecast_time in (None, "", "latest"):
            forecast_time = latest_forecast_time

        # Real perf fix (2026-08, user-reported: map hover tooltips are
        # "still quite slow") — same fix as ensure_precip_raw's own: check
        # the in-memory cache BEFORE ever resolving stage_path, since
        # stage_path is only needed to actually download something below.
        # The old order ran a real, wholly uncached Snowflake query on
        # EVERY single call for a non-"latest" forecast_time (e.g. every
        # hover over a fixed historical/demo date), even when the table
        # was already fully loaded and fresh in memory.
        key = (forecast_time, rp_tier)
        if key in self._grids and (time.time() - self._loaded_at.get(key, 0.0)) < _RIVER_EXTENT_TTL:
            return forecast_time

        if forecast_time == latest_forecast_time:
            stage_path = latest_stage_path
        else:
            rows = _run_query(_RIVER_EXTENT_BY_TIME_SQL, [_river_extent_param(rp_tier), forecast_time])
            if not rows:
                return None
            stage_path = rows[0]["STAGE_PATH"]

        with self._load_lock:
            if key in self._grids and (time.time() - self._loaded_at.get(key, 0.0)) < _RIVER_EXTENT_TTL:
                return forecast_time
            if key in self._grids:
                self._loaded_at[key] = time.time()
                return forecast_time
            log.info("RiverExtent: downloading+processing %s parquet for %s (%s)…",
                      _river_extent_param(rp_tier), forecast_time, stage_path)
            t0 = time.perf_counter()
            # Local import: same reasoning as _RiverRawCache/_PrecipRawCache above.
            from components.data.data_store_utils import get_data_store
            raw_bytes = get_data_store().read_file(stage_path)

            n14 = 1 << _RIVER_EXTENT_ZOOM  # 16384 tiles per axis at z=14
            # Running SPARSE per-z14-tile member bitmask — a Python dict
            # (flat tile id -> uint64 bitmask), NOT a dense (n14, n14) array
            # (~268M cells — far too much memory for what is overwhelmingly
            # empty ocean/land-without-flood-signal space). One bit per
            # ensemble member (51 fits comfortably in 64) — see the
            # module-level "DISTINCT-MEMBER COUNTING" comment for why this is
            # exactly equivalent to (and far cheaper than) a (tiles, 51) bool
            # array, and idempotent under colliding native pixels.
            tile_bits: dict[int, int] = {}
            rows_scanned = 0
            rows_kept = 0
            members_seen: set[int] = set()

            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                tmp.write(raw_bytes)
                tmp.flush()
                pf = pq.ParquetFile(tmp.name)
                n_row_groups = pf.metadata.num_row_groups
                for rg_idx in range(n_row_groups):
                    # Row-group-by-row-group (NOT pandas.read_parquet on the
                    # whole ~74M-row table at once) — see module-level
                    # AGGREGATION / MEMORY SAFETY comment.
                    tbl = pf.read_row_group(
                        rg_idx, columns=["pixel_lat", "pixel_lon", "member", "step_h"]
                    )
                    rows_scanned += tbl.num_rows
                    step_h = tbl.column("step_h").to_numpy(zero_copy_only=False)
                    step_mask = step_h == _RIVER_EXTENT_STEP_H
                    if not step_mask.any():
                        del tbl
                        continue
                    lat = tbl.column("pixel_lat").to_numpy(zero_copy_only=False)[step_mask].astype(np.float64)
                    lon = tbl.column("pixel_lon").to_numpy(zero_copy_only=False)[step_mask].astype(np.float64)
                    member = tbl.column("member").to_numpy(zero_copy_only=False)[step_mask].astype(np.int64)
                    del tbl, step_h, step_mask
                    rows_kept += lat.size
                    if lat.size == 0:
                        continue
                    members_seen.update(np.unique(member).tolist())

                    # Vectorized zoom-14 Web Mercator tile-index assignment —
                    # numpy math over the whole batch at once (NOT a per-row
                    # mercantile.tile() call, which is a slow scalar function
                    # and would dominate runtime at these row counts). Mirrors
                    # the standard slippy-map tile formula; clipping keeps a
                    # stray near-pole/antimeridian pixel inside the valid
                    # tile-index range instead of raising, same defensive
                    # spirit as _fetch_raster_tile's own clip()s.
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

                    # Per-BATCH reduction first — a single row group can be
                    # millions of rows spanning a much smaller number of
                    # distinct z14 tiles, so collapse duplicates within this
                    # batch via a vectorized groupby (np.unique + bitwise_or.at
                    # over the batch's own small inverse-index array) BEFORE
                    # touching the running global dict, then merge the small
                    # per-batch result in with a bitwise OR — idempotent, same
                    # "distinct member seen" semantics as a dense
                    # np.bitwise_or.at would give, just keyed by a real z14
                    # tile id instead of a dense degree-grid cell.
                    uniq_tiles, inverse = np.unique(tile_flat, return_inverse=True)
                    batch_bits = np.zeros(uniq_tiles.size, dtype=np.uint64)
                    np.bitwise_or.at(batch_bits, inverse, bits)
                    for tid, b in zip(uniq_tiles.tolist(), batch_bits.tolist()):
                        tile_bits[tid] = tile_bits.get(tid, 0) | b
                    del lat, lon, member, tile_flat, bits, uniq_tiles, inverse, batch_bits

            # Popcount each tile's bitmask -> real distinct-member count (0-51).
            # A plain 51-iteration bit-shift loop over the (n_tiles,) array of
            # accumulated bitmasks is fast and avoids materializing any
            # (tiles, 51) intermediate.
            n_tiles = len(tile_bits)
            if n_tiles:
                tile_ids_flat = np.fromiter(tile_bits.keys(), dtype=np.int64, count=n_tiles)
                bits_arr = np.fromiter(tile_bits.values(), dtype=np.uint64, count=n_tiles)
            else:
                tile_ids_flat = np.empty(0, dtype=np.int64)
                bits_arr = np.empty(0, dtype=np.uint64)
            del tile_bits

            member_count = np.zeros(n_tiles, dtype=np.int32)
            for b in range(_RIVER_PROB_ENSEMBLE_SIZE):
                member_count += ((bits_arr >> np.uint64(b)) & np.uint64(1)).astype(np.int32)
            del bits_arr

            y14_arr = (tile_ids_flat // n14).astype(np.int64)
            x14_arr = (tile_ids_flat % n14).astype(np.int64)
            probability = member_count.astype(np.float32) / _RIVER_PROB_ENSEMBLE_SIZE

            # quadkey + real z14 tile bounds — mercantile has no vectorized
            # form for these, but this loop is bounded by the real DISTINCT
            # tile count (tens-of-thousands-to-low-hundreds-of-thousands
            # globally per the module comment), not the ~195M raw row count,
            # so it is cheap relative to the row-group scan above.
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
            })

            elapsed = time.perf_counter() - t0
            self._grids[key] = {
                "df": extent_df,
                "rp_tier": rp_tier,
                "is_standin": rp_tier in _RIVER_EXTENT_STANDIN_RP_TIERS,
                "rows_scanned": rows_scanned,
                "rows_kept": rows_kept,
                "n_members_seen": len(members_seen),
                "load_seconds": elapsed,
                # Real perf fix (2026-08, user-reported: the raw-layer hover
                # tooltip is "quite slow") — river_raw_tile_value's point
                # lookup used to do `df[df['TILE_ID'] == qk]`, a full linear
                # scan across every distinct z14 tile in this table (tens of
                # thousands to low hundreds of thousands globally, per this
                # function's own comment above) on EVERY single hover
                # request. Built once here, alongside the table itself (so
                # it's naturally invalidated together whenever this entry
                # reloads), it turns that into an O(1) dict lookup instead.
                "prob_by_tile": dict(zip(tile_ids, probability.tolist())),
            }
            self._loaded_at[key] = time.time()
            log.info("  RiverExtent: z14 tile table ready %s/%s (scanned %d rows, kept %d @step_h=%d, "
                      "%d members seen, %d distinct z14 tiles with >=1 member flooded, %.1fs)",
                      rp_tier, forecast_time, rows_scanned, rows_kept, _RIVER_EXTENT_STEP_H,
                      len(members_seen), n_tiles, elapsed)
        return forecast_time

    def get_grid(self, forecast_time: str, rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER) -> Optional[dict]:
        return self._grids.get((forecast_time, rp_tier))


_river_extent_cache = _RiverExtentCache()


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_river_extent_raster_tile(forecast_time: str, z: int, x: int, y: int,
                                       rp_tier: str = _RIVER_EXTENT_DEFAULT_RP_TIER) -> bytes | None:
    """Render a 512x512 RGBA WebP tile from the cached sparse zoom-14-tile
    flood-extent table (see _RiverExtentCache) — THE current implementation
    behind /tiles/raster/river-raw/.../*.webp.

    As of the 2026-08 zoom-14-granularity revision, this mirrors
    _fetch_raster_tile's OWN quadkey-filter + vectorized scatter-paint
    pattern almost exactly (same TILE_ID/BW/BS/BE/BN shape, same z14
    rendering regardless of display zoom `z`) instead of the old
    dense-grid sample/colorize path (_sample_global_grid_tile/
    _render_dense_grid_webp — unchanged, still used by precip-raw).

    Always renders the continuous per-tile exceedance fraction (cyan->navy
    gradient, more member agreement = darker blue — see
    _RIVER_EXTENT_PROB_BREAKS/_COLORS, applied via np.digitize). River has
    no Mean/Probability toggle (removed per explicit user request — unlike
    rain, which has both a real mm intensity AND a real exceedance-
    probability, river only ever has this ONE real per-cell metric, so a
    second "Mean" mode was always just describing the identical number
    under a different name/colour). flood-view-as (Mean/Probability) is now
    rain-only.
    """
    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier)
    if resolved is None:
        return None
    entry = _river_extent_cache.get_grid(resolved, rp_tier)
    if entry is None:
        return None
    df = entry.get("df")
    if df is None or df.empty:
        return None

    # Geographic bounds for the requested display tile — same Web Mercator
    # setup as _fetch_raster_tile.
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    tile_dw = tile_e - tile_w
    _merc_tile_n = math.log(math.tan(math.pi / 4 + math.radians(tile_n) / 2))
    _merc_tile_s = math.log(math.tan(math.pi / 4 + math.radians(tile_s) / 2))
    _merc_tile_dh = _merc_tile_n - _merc_tile_s

    # Filter z=14 rows that fall within this display tile via quadkey prefix
    # — identical helper/pattern to _fetch_raster_tile.
    like_pat = _quadkey_like_pattern(z, x, y)
    if like_pat.endswith('%'):
        prefix = like_pat[:-1]
        mask = df['TILE_ID'].str.startswith(prefix, na=False)
    else:
        mask = df['TILE_ID'] == like_pat
    sub = df[mask]
    if sub.empty:
        return None

    img_arr = np.zeros((512, 512, 4), dtype=np.uint8)

    ws = sub['BW'].to_numpy(dtype=np.float64)
    ss = sub['BS'].to_numpy(dtype=np.float64)
    es = sub['BE'].to_numpy(dtype=np.float64)
    en = sub['BN'].to_numpy(dtype=np.float64)
    ns = en  # keep the same short local name the rest of this function already uses below
    _bounds_valid = np.isfinite(ws) & np.isfinite(ss) & np.isfinite(es) & np.isfinite(ns)

    # The one real per-tile member-agreement fraction (see this function's
    # own docstring) — continuous cyan->navy gradient via np.digitize.
    probs = pd.to_numeric(sub['probability'], errors='coerce').to_numpy(dtype=np.float64)
    valid = np.isfinite(probs) & (probs > 0) & _bounds_valid
    probs, ws, ss, es, ns = probs[valid], ws[valid], ss[valid], es[valid], ns[valid]
    if len(probs) == 0:
        return None
    palette_rgba_arr = np.asarray(_RIVER_EXTENT_PROB_COLORS, dtype=np.uint8)
    idx = np.digitize(probs, _RIVER_EXTENT_PROB_BREAKS)
    idx = np.clip(idx, 0, len(palette_rgba_arr) - 1)

    # Map z=14 tile bounds to pixel coordinates — identical formulas to
    # _fetch_raster_tile (see that function's own comment for the full
    # floor()/+1 boundary-alignment rationale).
    px0 = np.floor((ws - tile_w) / tile_dw * 512).astype(np.int32)
    px1 = np.floor((es - tile_w) / tile_dw * 512).astype(np.int32) + 1
    merc_ns = np.log(np.tan(np.pi / 4 + np.radians(ns) / 2))
    merc_ss = np.log(np.tan(np.pi / 4 + np.radians(ss) / 2))
    py0 = np.floor((1.0 - (merc_ns - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32)
    py1 = np.floor((1.0 - (merc_ss - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32) + 1

    # Vectorized scatter-paint — copied verbatim from _fetch_raster_tile
    # (see that function's own comment for the full "why" — verified
    # byte-identical to a per-row Python loop across 400+ randomized trials
    # there; reused here rather than rederived).
    x0v = np.maximum(0, px0)
    y0v = np.maximum(0, py0)
    x1v = np.minimum(512, np.maximum(x0v + 1, px1))
    y1v = np.minimum(512, np.maximum(y0v + 1, py1))
    # No separate "finite" re-check here — ws/ss/es/ns/idx were already
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

    Added per explicit user request ("make sure the latest 3 dates are
    always pre-warmed, drop the oldest when a new day comes"): previously
    this only ever warmed the SINGLE latest forecast time, so a user looking
    at yesterday's or the day-before's real data (still a completely normal,
    common thing to do — nothing here is Demo-Scenario-specific) always paid
    a full cold ~1.2GB Zarr / real Parquet download. Each cycle re-resolves
    the real latest-3-distinct-times set from Snowflake (_LATEST_3_PRECIP_
    RAW_SQL/_LATEST_3_RIVER_EXTENT_SQL) and evicts any previously-warmed
    entry that has since aged out of that window, so the process doesn't
    just grow unbounded — each precip-raw grid alone can be sized in the
    hundreds of MB once decoded.

    River-raw ALSO now warms all 6 real return-period tiers (rp2/rp5/rp10/
    rp20/rp50/rp100), not just the default rp10 — real feature added per
    explicit user request, since the RP-tier slider was made genuinely
    interactive and switching tiers previously always paid a real 5-20s
    cold Parquet download (confirmed live) for every user, every time,
    for any tier beyond the one default. ~18 (date, tier) combinations
    total per cycle — still a background, non-blocking loop."""
    while True:
        try:
            rows = _run_query(_LATEST_3_PRECIP_RAW_SQL, [])
            latest_times = {str(r["FORECAST_TIME"]) for r in rows}
        except Exception as e:
            log.error("Prewarm: could not resolve latest-3 precip-raw times: %s", e)
            latest_times = None
        if latest_times is not None:
            if not latest_times:
                log.info("Prewarm: precip-raw — no data currently available to warm")
            else:
                for forecast_time in latest_times:
                    try:
                        resolved = _precip_cache.ensure_precip_raw(forecast_time)
                        if resolved is not None:
                            log.info("Prewarm: precip-raw cache warm (forecast_time=%s)", resolved)
                    except Exception as e:
                        log.error("Prewarm: precip-raw warm-up failed for %s: %s", forecast_time, e)
                with _precip_cache._load_lock:
                    stale = set(_precip_cache._grid.keys()) - latest_times
                    for forecast_time in stale:
                        _precip_cache._grid.pop(forecast_time, None)
                        _precip_cache._loaded_at.pop(forecast_time, None)
                if stale:
                    log.info("Prewarm: evicted %d stale precip-raw grid(s) outside the latest-3 window: %s",
                              len(stale), sorted(stale))

        # River-raw: one latest-3 resolution PER real rp_tier — each tier is
        # its own distinct Parquet file/forecast_time set (confirmed live —
        # rp2's own file, in particular, has FAR more raw rows than rp10's,
        # not just a smaller copy of it), so each needs its own query and its
        # own eviction pass keyed to (forecast_time, that_tier) only.
        for rp_tier in _RIVER_EXTENT_RP_TIERS:
            try:
                rows = _run_query(_LATEST_3_RIVER_EXTENT_SQL, [_river_extent_param(rp_tier)])
                latest_times = {str(r["FORECAST_TIME"]) for r in rows}
            except Exception as e:
                log.error("Prewarm: could not resolve latest-3 river-raw/%s times: %s", rp_tier, e)
                continue
            if not latest_times:
                log.info("Prewarm: river-raw/%s — no data currently available to warm", rp_tier)
                continue
            for forecast_time in latest_times:
                try:
                    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier)
                    if resolved is not None:
                        log.info("Prewarm: river-raw/%s cache warm (forecast_time=%s)", rp_tier, resolved)
                except Exception as e:
                    log.error("Prewarm: river-raw/%s warm-up failed for %s: %s", rp_tier, forecast_time, e)
            # Evict anything that has aged out of the rolling window for
            # THIS tier only — under the SAME lock loads use, so an eviction
            # can never race an in-progress download for that exact key.
            with _river_extent_cache._load_lock:
                this_tier_keys = {k for k in _river_extent_cache._grids.keys() if k[1] == rp_tier}
                stale = {k for k in this_tier_keys if k[0] not in latest_times}
                for key in stale:
                    _river_extent_cache._grids.pop(key, None)
                    _river_extent_cache._loaded_at.pop(key, None)
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
        # Real perf/correctness fix (2026-08, multi-agent audit): browser
        # max-age used to be a hardcoded 3600s (1h) while the server's own
        # _fetch_mercator_tile cache (backing this response) expires after
        # _TILE_TTL (900s/15min) — meaning a browser could keep serving a
        # stale tile for up to 45 real minutes after fresher pipeline output
        # was already available server-side. Aligned to _TILE_TTL directly
        # (not a second hardcoded number) so the two can never drift again.
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
        # Same real perf/correctness fix as mercator_tile above — aligned
        # to _TILE_TTL instead of a hardcoded 3600s that outlived the
        # server's own 900s cache.
        return Response(status_code=204, headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})


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
            log.error("Preload error in %s: %s", getattr(fn, "__name__", fn), e)

    def _load():
        # Real perf fix (2026-08 audit, finding #17): the 6 ensure_* warm-up
        # calls used to run strictly serially in this one background thread
        # (sum of every load's own cost); they're independent (each already
        # has its own per-key lock, see _DataCache), so running them
        # concurrently collapses cold preload to roughly the slowest single
        # load instead. Plain threading.Thread (not _SHARED_EXECUTOR)
        # deliberately here, ensure_mercator/ensure_admin/ensure_facility
        # each internally fan out multi-country requests onto
        # _SHARED_EXECUTOR too, and a pool worker blocking on its OWN pool's
        # tasks risks starving it; these top-level supervisor threads are
        # uncounted OS threads, so they never compete with the pool's own
        # worker budget.
        facility_sql = {"gust": _FACILITY_GUST_SQL, "river": _FACILITY_RIVER_SQL,
                        "rain": _FACILITY_PRECIP_SQL}.get(hazard, _FACILITY_IMPACT_SQL)
        tasks = [
            threading.Thread(target=_run_and_log, args=(
                _cache.ensure_mercator, country.upper(), storm, forecast_date, wind_threshold, hazard,
                gust_threshold, rp_tier, threshold_mm, window_h,
            )),
            threading.Thread(target=_run_and_log, args=(
                _cache.ensure_admin, country.upper(), storm, forecast_date, wind_threshold, admin_level, hazard,
                gust_threshold, rp_tier, threshold_mm, window_h,
            )),
        ]
        for layer_type in facility_sql:
            tasks.append(threading.Thread(target=_run_and_log, args=(
                _cache.ensure_facility, layer_type, country.upper(), storm, forecast_date, wind_threshold, hazard,
                gust_threshold, rp_tier, threshold_mm, window_h,
            )))
        for t in tasks:
            t.start()
        for t in tasks:
            t.join()
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "country": country, "storm": storm}



# Shared min/max mapping for the "base + gust/river" family of stats queries
# (population/children/etc. from b., PROBABILITY + E_* impact cols from i.) —
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
        WHERE COUNTRY {country_clause} AND FORECAST_TIME = %s AND RP_TIER = %s
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
    # Real perf fix (2026-08 audit, finding #13): reuse ensure_mercator's own
    # cached/merged DataFrame instead of always re-running a separate
    # full-country SQL aggregate below, the browser's simultaneous tile
    # requests already trigger (or share, via ensure_mercator's own per-key
    # lock) the exact same bulk load for this key, so this avoids
    # duplicating it in SNOWFLAKE mode too (previously only LOCAL/BLOB mode
    # did this). Falls through to the original standalone SQL aggregate only
    # if this fast path itself fails.
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
                              [*codes, forecast_date, rp_tier, *codes, zoom_level])
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


# Real perf fix (2026-08, multi-agent audit): this endpoint used to run the
# full Snowflake query above on EVERY call, even though it's driven by the
# hazard-threshold slider debounce settling — the exact same
# repeated-identical-request shape every other tile-render endpoint in this
# file already caches via _ttl_cache. Thin route, all real work now lives in
# the cached _fetch_tile_stats above (same split already used throughout
# this file, e.g. _fetch_mercator_tile / get_mercator_tile).
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
        WHERE COUNTRY {country_clause} AND FORECAST_TIME = %s AND RP_TIER = %s
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
    # Same real perf fix as _fetch_tile_stats above (finding #13), reuse
    # ensure_admin's own cached/merged DataFrame instead of always
    # re-running a separate full-country SQL aggregate below.
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
                              [*codes, forecast_date, rp_tier, *codes, admin_level])
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


# Same real perf fix as get_tile_stats above — thin route, real work now
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
        # Real perf/correctness fix (2026-08, multi-agent audit): aligned to
        # _TILE_TTL instead of a hardcoded 3600s that outlived the server's
        # own 900s cache — see mercator_tile's own comment for the full "why".
        return Response(content=_TRANSPARENT_WEBP, media_type="image/webp",
                        headers={"Cache-Control": f"public, max-age={_TILE_TTL}"})
    return Response(
        content=webp_bytes,
        media_type="image/webp",
        headers={"Cache-Control": f"public, max-age={_TILE_TTL}"},
    )


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

    `combine` (default True): whether to color/size each facility by its real
    per-hazard PROBABILITY at `wind_threshold`/etc, or render every facility
    as a plain, uncolored location instead. Real feature added here per
    explicit user request: the client (pages/map_shell_concept.py's
    _register_ms_facility_layer) still ALWAYS fetches with a real
    wind_threshold — the query itself doesn't stop being hazard-conditional
    just because no hazard checkbox happens to be on — so without this flag,
    turning every hazard off (or hiding them via the eye icon) left facility
    points still tinted by whatever threshold was last selected. When False,
    `prob` is forced to 0 for every row below (same code path a real,
    confirmed-zero PROBABILITY already takes — the base/neutral color), and
    the real `probability` property is dropped from the response so a
    tooltip can't show a stale percentage for a marker that's deliberately
    NOT being colored by it.
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


# Real perf fix (2026-08, multi-agent audit): this used to rebuild the full
# response (iterrows + json.dumps + gzip) on EVERY call, even though the
# browser's own Cache-Control here already assumes a stable 5-minute
# response — measured 0.727s/0.727s/0.736s on three identical back-to-back
# requests (i.e. genuinely uncached server-side). Same _ttl_cache pattern
# already used throughout this file; the route above wraps the cached gzip
# bytes in a fresh Response object each time (cheap) rather than caching
# the Response itself.
@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=2048)
def _fetch_facility_geojson_body(layer_type, country, storm, forecast_date, wind_threshold,
                                    hazard, gust_threshold, rp_tier, threshold_mm, window_h, combine):
    base_color = _FACILITY_BASE_COLORS[layer_type]
    df = _cache.get_facility_df(layer_type, country.upper(), storm, forecast_date, wind_threshold,
                                hazard, gust_threshold, rp_tier, threshold_mm, window_h)
    features = []
    for _, row in df.iterrows():
        lat = row.get("LATITUDE")
        lon = row.get("LONGITUDE")
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            continue
        prob = float(row.get("PROBABILITY") or 0) if combine else 0.0
        if prob <= 0:
            color, radius = base_color, 4
        elif prob <= 0.15:
            color, radius = "#FFFF00", 10
        elif prob <= 0.30:
            color, radius = "#FFD700", 12
        elif prob <= 0.45:
            color, radius = "#FFA500", 15
        elif prob <= 0.60:
            color, radius = "#FF8C00", 18
        elif prob <= 0.75:
            color, radius = "#FF4500", 20
        elif prob <= 0.90:
            color, radius = "#DC143C", 22
        else:
            color, radius = "#8B0000", 25
        props = {k.lower(): _safe_prop(v)
                 for k, v in row.items()
                 if k not in ("LATITUDE", "LONGITUDE")
                 and (combine or k != "PROBABILITY")}
        props.update({
            "_color": color, "_radius": radius,
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

    Used for hover tooltips on the raster layer.
    """
    tile = mercantile.tile(lon, lat, 14)
    qk = mercantile.quadkey(tile)
    variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
    key = (country.upper(), storm, forecast_date) + variant
    _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold, hazard,
                          gust_threshold, rp_tier, threshold_mm, window_h)
    df = _cache._mercator.get(key)
    if df is None or df.empty:
        return {}
    row = df[df['TILE_ID'] == qk]
    if row.empty:
        return {}
    result = row.iloc[0].drop(labels=['BW', 'BS', 'BE', 'BN'], errors='ignore').to_dict()
    return {k: (None if pd.isna(v) else v) for k, v in result.items()}


# ---------------------------------------------------------------------------
# Global raw precipitation-rate endpoints (NOT country/storm-scoped — see
# _PrecipRawCache above)
# ---------------------------------------------------------------------------

@app.get("/tiles/raster/precip-raw/{forecast_time}/{z}/{x}/{y}.webp", response_class=Response)
def precip_raw_tile(
    forecast_time: str, z: int, x: int, y: int,
    mode: str = Query("mean", pattern="^(mean|probability)$"),
    window_h: int = Query(_PRECIP_RATE_DEFAULT_WINDOW_H),
    threshold_mm: float = Query(_PRECIP_PROB_THRESHOLD_MM),
) -> Response:
    """Global precip raster tile (mm over T+0->T+{window_h}h).

    `forecast_time` may be the literal string "latest" to always track the
    most recent tp forecast cycle without the caller needing to look it up.

    `window_h` (default 6, backward-compatible): accumulation window in
    hours — must be one of _PRECIP_RATE_WINDOWS_H (the same real windows
    ms-rain-window exposes) or the tile renders empty (no precomputed grid
    for it). `threshold_mm` (default 10.0, backward-compatible): only used
    when `mode=probability` — must be one of _PRECIP_PROB_THRESHOLDS_BY_
    WINDOW_MM[window_h] (the same real depth tiers ms-rain-slider exposes for
    that window) or, again, the tile renders empty.

    `mode=mean` (default, backward-compatible): ensemble-mean rate for
    `window_h`, radar-style ramp. `mode=probability`: fraction of ensemble
    members exceeding `threshold_mm` within `window_h`, sequential-purple
    ramp. All are derived from the same cached per-forecast_time download.
    """
    try:
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
) -> dict:
    """Point lookup for the raw precip-rate raster's hover tooltip — real
    bug found+fixed here (2026-08, user-reported: "there are no tooltips
    for the raw layers still... on the map directly, like we had already
    for the storms"): this GLOBAL, country-independent layer had NO hover
    mechanism at all — assets/maplibre_tiles.js's hover handler only ever
    queries the per-country hazard system's own /tile-value/{country}/...
    endpoint, and bails out entirely whenever no country is selected (this
    layer's own normal Global-mode state). Reuses the SAME cached dense
    grid _fetch_precip_raw_tile already renders 512x512 tiles from
    (_PrecipRawCache.get_render_entry) — a single grid-index lookup, no
    fresh Zarr read.

    `mode` mirrors _fetch_precip_raw_tile's own param — real bug found+
    fixed here (2026-08, user-reported: the tooltip "still showing
    precipitation everywhere if there is nothing"): this used to always
    compute+return BOTH mean_mm and probability regardless of which ONE is
    actually being painted right now (the raster only ever renders ONE
    ramp per `mode` — see _render_dense_grid_webp). A spot with a real but
    unremarkable mean rate (e.g. 15mm/120h) could clear the mean-intensity
    ramp's own low first break while the map is actually painting
    Probability-mode (e.g. "% of members over 100mm") and showing nothing
    there at all — so the tooltip surfaced a real number completely
    disconnected from what the cursor was visually hovering over. Now only
    computes/returns the ONE metric matching the active display mode, same
    as the raster itself."""
    resolved = _precip_cache.ensure_precip_raw(forecast_time)
    if resolved is None:
        return {}
    entry = _precip_cache.get_render_entry(resolved, window_h, threshold_mm)
    if entry is None:
        return {}
    result: dict = {}
    if mode == "mean":
        # Real UX bug found+fixed here (2026-08, user-reported: the
        # tooltip "shows the precipitation everywhere, but with 0%"):
        # this dense grid is finite (real, non-NaN) almost everywhere, not
        # just where it's visually raining — most of that is a near-zero
        # rate the color ramp itself already treats as invisible (below
        # its own first real break — see _precip_rate_breaks_for_window's
        # own docstring). Same real threshold the map's own paint already
        # uses to decide what counts as visible rain.
        mean_val = _sample_global_grid_point(entry, entry["grid"], lon, lat)
        mean_break = _precip_rate_breaks_for_window(window_h)[0]
        if mean_val is not None and mean_val >= mean_break:
            result["mean_mm"] = mean_val
    else:
        prob_val = _sample_global_grid_point(entry, entry.get("prob_grid"), lon, lat)
        if prob_val is not None and prob_val > 0:
            result["probability"] = prob_val
    return result


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
    `window_h` (see _precip_rate_breaks_for_window — real bug fixed here,
    this used to always echo back the fixed base-6h breaks regardless of
    window_h). `mode=probability`: fixed [0, 1] fraction scale — no need to
    compute real min/max from data since exceedance-probability is always
    in that range by construction; echoes back the requested
    window_h/threshold_mm rather than the old fixed _PRECIP_PROB_THRESHOLD_MM
    so the legend can show the ACTUAL selected threshold, not always "10mm".
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

    Warms EVERY real (window_h, threshold_mm) combination in one download —
    all are computed together in ensure_precip_raw() from the same per-member
    rate_grid(s), so there is no separate "window"/"mode" to pass here."""
    def _load():
        try:
            _precip_cache.ensure_precip_raw(forecast_time)
        except Exception as e:
            log.error("Preload precip-raw error: %s", e)
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "forecast_time": forecast_time}


# ---------------------------------------------------------------------------
# Global raw river endpoints (NOT country/storm-scoped). Backed by
# _RiverExtentCache (extent_rp10_bymember) as of 2026-07-31 — see that
# section's module comment for the full "why the switch" rationale. The
# external URL shape (/tiles/raster/river-raw/{forecast_time}/{z}/{x}/{y}.webp,
# /stats/river-raw/{forecast_time}, /preload/river-raw/{forecast_time}) is
# UNCHANGED from the old dis24-based version — only `forecast_time`'s real
# meaning shifts subtly (now a plain date like "2026-07-14", since
# extent_rp10_bymember is keyed by DATE rather than dis24's full datetime).
# ---------------------------------------------------------------------------

@app.get("/tiles/raster/river-raw/{forecast_time}/{z}/{x}/{y}.webp", response_class=Response)
def river_raw_raster_tile(
    forecast_time: str, z: int, x: int, y: int,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
) -> Response:
    """Global river FLOOD-EXTENT (per-member, real return-period tier)
    RASTER tile — THE endpoint the frontend should use. See the
    _RiverExtentCache module comment above for the full rationale (why
    extent_rpN_bymember replaces raw dis24 discharge, step_h/resolution/
    aggregation choices, and why there is no Mean/Probability mode split).

    `forecast_time` may be the literal string "latest" to always track the
    most recent cycle for the requested `rp_tier` without the caller needing
    to look it up. Unlike the old dis24 layer, this is a plain DATE string
    (e.g. "2026-07-14"), not a full datetime.

    `rp_tier` (default rp10, matching ms-river-slider's own default): one of
    rp2/rp5/rp10/rp20/rp50/rp100 — real bug fixed here, this used to be
    hardcoded to rp10 regardless of what the slider was set to. rp2/rp5 are
    IS_STANDIN=True upstream (see _RIVER_EXTENT_STANDIN_RP_TIERS) — they
    reuse rp10's own real extent as a labelled UPPER-BOUND stand-in (flood
    extent grows monotonically with return period, so RP10's extent
    conservatively overestimates RP2/RP5's true, smaller extent — see
    TC-ECMWF-Forecast-Pipeline's own glofas_extent_masking.py), not an
    independently-computed rp2/rp5 result; see /stats/river-raw's own
    `is_standin` field for surfacing this to the frontend.

    Always renders the real per-z14-tile fraction of members whose extent
    covers that tile at the requested `rp_tier`, cyan->navy gradient — no
    `mode` query param anymore (removed per explicit user request: river has
    no second independent quantity the way rain has both real mm and a real
    exceedance-probability, so an earlier Mean/Probability toggle here was
    always describing the identical number under a different name/colour;
    see _fetch_river_extent_raster_tile's own docstring).
    """
    try:
        webp_bytes = _fetch_river_extent_raster_tile(forecast_time, z, x, y, rp_tier)
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
) -> dict:
    """Point lookup for the raw river-extent raster's hover tooltip — same
    real gap/fix as precip_raw_tile_value above. Same quadkey-lookup
    pattern as tile_value() (the per-country endpoint), against the sparse
    z14-tile _RiverExtentCache table instead of the per-country mercator
    cache — no fresh parquet read, reuses whatever _fetch_river_extent_
    raster_tile already cached for this forecast_time/rp_tier.

    Real perf fix (2026-08, user-reported: "it's still quite slow") — uses
    the entry's own "prob_by_tile" dict (built once, see _RiverExtentCache's
    own comment on it) instead of a linear `df[df['TILE_ID'] == qk]` scan
    across every distinct z14 tile in the table on every single hover."""
    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier)
    if resolved is None:
        return {}
    entry = _river_extent_cache.get_grid(resolved, rp_tier)
    if entry is None:
        return {}
    prob_by_tile = entry.get("prob_by_tile")
    if not prob_by_tile:
        return {}
    tile = mercantile.tile(lon, lat, 14)
    qk = mercantile.quadkey(tile)
    prob = prob_by_tile.get(qk)
    if prob is None or (isinstance(prob, float) and math.isnan(prob)):
        return {}
    return {"probability": float(prob), "rp_tier": rp_tier}


@app.get("/stats/river-raw/{forecast_time}")
def get_river_raw_stats(
    forecast_time: str,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
) -> dict:
    """Legend range for the river flood-extent raster (see _RiverExtentCache
    above). Not currently called by the frontend (the Dash app builds its
    own legend text directly — see pages/map_shell_concept.py's
    _legend_raw_flood_info); kept for API-shape parity with precip's own
    real stats endpoint.

    Always returns the continuous [0, 1] probability-fraction shape
    (`breaks`, `ensemble_size`) — river has only ever this one real metric,
    no Mean/Probability mode split (removed per explicit user request).

    `is_standin` is True for rp2/rp5: the frontend uses this to show a
    real, data-driven "not natively computed, RP10 used as an upper-bound
    estimate" note rather than a silently-wrong-looking layer.
    """
    resolved = _river_extent_cache.ensure_river_extent(forecast_time, rp_tier)
    is_standin = rp_tier in _RIVER_EXTENT_STANDIN_RP_TIERS
    if resolved is None:
        return {"min": None, "max": None, "forecast_time": None,
                 "rp_tier": rp_tier, "is_standin": is_standin}
    return {
        "min": 0.0,
        "max": 1.0,
        "breaks": _RIVER_EXTENT_PROB_BREAKS,
        "ensemble_size": _RIVER_PROB_ENSEMBLE_SIZE,
        "forecast_time": resolved,
        "rp_tier": rp_tier,
        "is_standin": is_standin,
    }


@app.get("/preload/river-raw/{forecast_time}")
def preload_river_raw(
    forecast_time: str,
    rp_tier: str = Query(_RIVER_EXTENT_DEFAULT_RP_TIER, pattern="^(rp2|rp5|rp10|rp20|rp50|rp100)$"),
) -> dict:
    """Pre-warm the river flood-extent cache (the sparse zoom-14-tile
    probability table, computed once in ensure_river_extent() — no separate
    "mode" to pass here). Returns immediately; the ~28MB parquet download +
    row-group processing happens in a background thread (same
    fire-and-forget style as /preload/precip-raw/{forecast_time})."""
    def _load():
        try:
            _river_extent_cache.ensure_river_extent(forecast_time, rp_tier)
        except Exception as e:
            log.error("Preload river-raw error: %s", e)
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "forecast_time": forecast_time}
