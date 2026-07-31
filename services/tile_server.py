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
from functools import lru_cache
from typing import Optional

# Tile data and rendered tiles expire after this many seconds so new pipeline
# output is served without a container restart (matches snowflake_utils TTL).
_TILE_TTL = 15 * 60  # 15 minutes


def _ttl_cache(ttl_seconds: int, maxsize: int = 128):
    """lru_cache with automatic TTL expiry (bucket-based, thread-safe)."""
    def decorator(func):
        @functools.lru_cache(maxsize=maxsize)
        def cached(_bucket, args, kwargs):
            return func(*args, **dict(kwargs))

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            bucket = int(time.time() // ttl_seconds)
            return cached(bucket, args, tuple(sorted(kwargs.items())))

        wrapper.cache_clear = cached.cache_clear
        wrapper.cache_info  = cached.cache_info
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

_conn: Optional[snowflake.connector.SnowflakeConnection] = None
_conn_lock = threading.Lock()
_query_lock = threading.Lock()


def get_connection() -> snowflake.connector.SnowflakeConnection:
    global _conn
    with _conn_lock:
        try:
            if _conn is not None and not _conn.is_closed():
                return _conn
        except Exception as exc:
            log.debug("Stale connection check failed, reconnecting: %s", exc)
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
        _conn = snowflake.connector.connect(**kwargs)
        return _conn


def _run_query(sql: str, params: list) -> list[dict]:
    global _conn
    for attempt in range(2):
        conn = get_connection()
        try:
            with _query_lock:
                cur = conn.cursor()
                try:
                    cur.execute(sql, params)
                    cols = [d[0].upper() for d in cur.description]
                    return [dict(zip(cols, row)) for row in cur.fetchall()]
                finally:
                    cur.close()
        except snowflake.connector.errors.ProgrammingError as exc:
            if exc.errno == 390114 and attempt == 0:
                log.info("SPCS token expired — reconnecting with fresh token…")
                with _conn_lock:
                    if _conn is conn:
                        _conn = None
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
        keep = [c for c in ["TILE_ID", "E_PEOPLE_IN_NEED", "E_CHILDREN_IN_NEED"] if c in vuln.columns]
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
        keep = [c for c in ["TILE_ID", "E_PEOPLE_IN_NEED", "E_CHILDREN_IN_NEED"] if c in vuln.columns]
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

# Bulk variant — loads ALL rows for a country; no TILE_ID LIKE filter.
_MERCATOR_FULL_SQL = """
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
    b.NUM_WASH,
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
    i.E_NUM_WASH,
    v.E_PEOPLE_IN_NEED,
    v.E_CHILDREN_IN_NEED,
    c.CCI_CHILDREN,
    c.E_CCI_CHILDREN
FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT i
    ON  b.TILE_ID        = i.ZONE_ID
    AND b.COUNTRY        = i.COUNTRY
    AND b.ZOOM_LEVEL     = i.ZOOM_LEVEL
    AND i.STORM          = %s
    AND i.FORECAST_DATE  = %s
    AND i.WIND_THRESHOLD = %s
LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_VULNERABILITY_MAT v
    ON  b.TILE_ID        = v.ZONE_ID
    AND b.COUNTRY        = v.COUNTRY
    AND b.ZOOM_LEVEL     = v.ZOOM_LEVEL
    AND v.STORM          = %s
    AND v.FORECAST_DATE  = %s
LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT c
    ON  b.TILE_ID        = c.ZONE_ID
    AND b.COUNTRY        = c.COUNTRY
    AND b.ZOOM_LEVEL     = c.ZOOM_LEVEL
    AND c.STORM          = %s
    AND c.FORECAST_DATE  = %s
WHERE b.COUNTRY    = %s
  AND b.ZOOM_LEVEL = %s
"""

# ---------------------------------------------------------------------------
# Admin tiles — GEOMETRY column exists; use shapely for clipping
# ---------------------------------------------------------------------------

# Bulk variant — loads ALL rows for a country/admin_level; no ST_INTERSECTS filter.
_ADMIN_FULL_SQL = """
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
    ST_ASGEOJSON(b.GEOMETRY) AS GEOJSON,
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
    i.E_NUM_WASH,
    v.E_PEOPLE_IN_NEED,
    v.E_CHILDREN_IN_NEED,
    c.CCI_CHILDREN,
    c.E_CCI_CHILDREN
FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT i
    ON  b.TILE_ID        = i.TILE_ID
    AND b.COUNTRY        = i.COUNTRY
    AND b.ADMIN_LEVEL    = i.ADMIN_LEVEL
    AND i.STORM          = %s
    AND i.FORECAST_DATE  = %s
    AND i.WIND_THRESHOLD = %s
LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_VULNERABILITY_MAT v
    ON  b.TILE_ID        = v.TILE_ID
    AND b.COUNTRY        = v.COUNTRY
    AND b.ADMIN_LEVEL    = v.ADMIN_LEVEL
    AND v.STORM          = %s
    AND v.FORECAST_DATE  = %s
LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT c
    ON  b.TILE_ID        = c.TILE_ID
    AND b.COUNTRY        = c.COUNTRY
    AND b.ADMIN_LEVEL    = c.ADMIN_LEVEL
    AND c.STORM          = %s
    AND c.FORECAST_DATE  = %s
WHERE b.COUNTRY     = %s
  AND b.ADMIN_LEVEL = %s
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

_MERCATOR_GUST_SQL = """
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
    b.NUM_WASH,
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
FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_GUST_MAT i
    ON  b.TILE_ID        = i.ZONE_ID
    AND b.COUNTRY        = i.COUNTRY
    AND b.ZOOM_LEVEL     = i.ZOOM_LEVEL
    AND i.STORM          = %s
    AND i.FORECAST_DATE  = %s
    AND i.GUST_THRESHOLD = %s
WHERE b.COUNTRY    = %s
  AND b.ZOOM_LEVEL = %s
"""

_ADMIN_GUST_SQL = """
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
    ST_ASGEOJSON(b.GEOMETRY) AS GEOJSON,
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
FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_GUST_MAT i
    ON  b.TILE_ID        = i.TILE_ID
    AND b.COUNTRY        = i.COUNTRY
    AND b.ADMIN_LEVEL    = i.ADMIN_LEVEL
    AND i.STORM          = %s
    AND i.FORECAST_DATE  = %s
    AND i.GUST_THRESHOLD = %s
WHERE b.COUNTRY     = %s
  AND b.ADMIN_LEVEL = %s
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
# ---------------------------------------------------------------------------

_MERCATOR_RIVER_SQL = """
WITH river_agg AS (
    SELECT
        ZONE_ID,
        MAX(PROBABILITY)              AS PROBABILITY,
        BOOLOR_AGG(BELOW_MIN_BASIN)   AS BELOW_MIN_BASIN,
        BOOLOR_AGG(IS_STANDIN)        AS IS_STANDIN,
        MAX(E_POPULATION)             AS E_POPULATION,
        MAX(E_INFANT_POPULATION)      AS E_INFANT_POPULATION,
        MAX(E_SCHOOL_AGE_POPULATION)  AS E_SCHOOL_AGE_POPULATION,
        MAX(E_ADOLESCENT_POPULATION)  AS E_ADOLESCENT_POPULATION,
        MAX(E_BUILT_SURFACE_M2)       AS E_BUILT_SURFACE_M2,
        MAX(E_NUM_SCHOOLS)            AS E_NUM_SCHOOLS,
        MAX(E_NUM_HCS)                AS E_NUM_HCS,
        MAX(E_NUM_SHELTERS)           AS E_NUM_SHELTERS,
        MAX(E_NUM_WASH)               AS E_NUM_WASH
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_RIVER_MAT
    WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
    GROUP BY ZONE_ID
)
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
    b.NUM_WASH,
    i.PROBABILITY,
    i.BELOW_MIN_BASIN,
    i.IS_STANDIN,
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
FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
LEFT JOIN river_agg i ON b.TILE_ID = i.ZONE_ID
WHERE b.COUNTRY    = %s
  AND b.ZOOM_LEVEL = %s
"""

_ADMIN_RIVER_SQL = """
WITH river_agg AS (
    SELECT
        TILE_ID,
        ADMIN_LEVEL,
        MAX(PROBABILITY)              AS PROBABILITY,
        BOOLOR_AGG(BELOW_MIN_BASIN)   AS BELOW_MIN_BASIN,
        BOOLOR_AGG(IS_STANDIN)        AS IS_STANDIN,
        MAX(E_POPULATION)             AS E_POPULATION,
        MAX(E_INFANT_POPULATION)      AS E_INFANT_POPULATION,
        MAX(E_SCHOOL_AGE_POPULATION)  AS E_SCHOOL_AGE_POPULATION,
        MAX(E_ADOLESCENT_POPULATION)  AS E_ADOLESCENT_POPULATION,
        MAX(E_BUILT_SURFACE_M2)       AS E_BUILT_SURFACE_M2,
        MAX(E_NUM_SCHOOLS)            AS E_NUM_SCHOOLS,
        MAX(E_NUM_HCS)                AS E_NUM_HCS,
        MAX(E_NUM_SHELTERS)           AS E_NUM_SHELTERS,
        MAX(E_NUM_WASH)               AS E_NUM_WASH
    FROM AOTS.TC_ECMWF.ADMIN_ALL_RIVER_MAT
    WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
    GROUP BY TILE_ID, ADMIN_LEVEL
)
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
    ST_ASGEOJSON(b.GEOMETRY) AS GEOJSON,
    i.PROBABILITY,
    i.BELOW_MIN_BASIN,
    i.IS_STANDIN,
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
FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
LEFT JOIN river_agg i ON b.TILE_ID = i.TILE_ID AND b.ADMIN_LEVEL = i.ADMIN_LEVEL
WHERE b.COUNTRY     = %s
  AND b.ADMIN_LEVEL = %s
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
# E_POPULATION is selected from `i` below; every other exposure figure comes
# from the base join (`b.`) same as always, and the browser's existing
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
# plain LEFT JOIN suffices, unlike river's MAX(...) aggregation above.
# ---------------------------------------------------------------------------

_MERCATOR_PRECIP_SQL = """
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
    b.NUM_WASH,
    i.PROBABILITY,
    i.E_POPULATION
FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT b
LEFT JOIN AOTS.TC_ECMWF.MERCATOR_TILE_PRECIP_MAT i
    ON  b.TILE_ID       = i.ZONE_ID
    AND b.COUNTRY       = i.COUNTRY
    AND i.FORECAST_TIME = %s
    AND i.THRESHOLD_MM  = %s
    AND i.WINDOW_H      = %s
WHERE b.COUNTRY    = %s
  AND b.ZOOM_LEVEL = %s
"""

_ADMIN_PRECIP_SQL = """
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
    ST_ASGEOJSON(b.GEOMETRY) AS GEOJSON,
    i.PROBABILITY,
    i.E_POPULATION
FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT b
LEFT JOIN AOTS.TC_ECMWF.ADMIN_ALL_PRECIP_MAT i
    ON  b.TILE_ID       = i.TILE_ID
    AND b.COUNTRY       = i.COUNTRY
    AND b.ADMIN_LEVEL   = i.ADMIN_LEVEL
    AND i.FORECAST_TIME = %s
    AND i.THRESHOLD_MM  = %s
    AND i.WINDOW_H      = %s
WHERE b.COUNTRY     = %s
  AND b.ADMIN_LEVEL = %s
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
    their own sibling SQL (see _MERCATOR_GUST_SQL etc. above) and their own
    threshold param (gust_threshold/rp_tier/threshold_mm+window_h respectively)
    instead of wind_threshold — wind_threshold itself is still accepted (and
    harmlessly ignored) for non-wind hazards so callers never need to omit it.
    """

    def __init__(self) -> None:
        self._mercator: dict[tuple, pd.DataFrame] = {}
        self._admin: dict[tuple, pd.DataFrame] = {}
        self._admin_geoms: dict[tuple, tuple] = {}  # key → (geom_list, strtree, props_list)
        self._facility: dict[tuple, pd.DataFrame] = {}
        self._load_lock = threading.Lock()
        self._loaded_at: dict[tuple, float] = {}  # key → epoch seconds when loaded

    def _is_fresh(self, key: tuple) -> bool:
        return key in self._loaded_at and (time.time() - self._loaded_at[key]) < _TILE_TTL

    # --- mercator --------------------------------------------------------

    def ensure_mercator(self, country: str, storm: str, forecast_date: str,
                        wind_threshold: int, hazard: str = "wind",
                        gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                        threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> None:
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (country, storm, forecast_date) + variant
        if self._is_fresh(key) and key in self._mercator:
            return
        with self._load_lock:
            if self._is_fresh(key) and key in self._mercator:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]
            all_rows: list[dict] = []
            for code in codes:
                log.info("Cache: bulk-loading mercator %s/%s/%s hazard=%s variant=%s [%s]…",
                         code, storm, forecast_date, hazard, variant, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    if hazard == "gust":
                        all_rows.extend(_run_query(_MERCATOR_GUST_SQL, [
                            storm, forecast_date, gust_threshold, code, MAT_ZOOM_LEVEL,
                        ]))
                    elif hazard == "river":
                        all_rows.extend(_run_query(_MERCATOR_RIVER_SQL, [
                            code, forecast_date, rp_tier, code, MAT_ZOOM_LEVEL,
                        ]))
                    elif hazard == "rain":
                        all_rows.extend(_run_query(_MERCATOR_PRECIP_SQL, [
                            forecast_date, threshold_mm, window_h, code, MAT_ZOOM_LEVEL,
                        ]))
                    else:
                        all_rows.extend(_run_query(_MERCATOR_FULL_SQL, [
                            storm, forecast_date, wind_threshold,
                            storm, forecast_date,
                            storm, forecast_date,
                            code, MAT_ZOOM_LEVEL,
                        ]))
                elif hazard == "wind":
                    all_rows.extend(_load_mercator_from_files(code, storm, forecast_date, wind_threshold))
                else:
                    log.warning("%s STAGE (file-based) mercator loading not implemented — returning empty for %s",
                                hazard, code)
            if all_rows:
                df = pd.DataFrame(all_rows)
                df = pd.concat([df, _precompute_mercator_bounds(df["TILE_ID"])], axis=1)
            else:
                df = pd.DataFrame(columns=["TILE_ID", "BW", "BS", "BE", "BN"])
            log.info("  Cache: %d z=14 tiles ready (country=%s, hazard=%s)", len(df), country, hazard)
            self._mercator[key] = df
            self._loaded_at[key] = time.time()

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
        from shapely.strtree import STRtree
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (country, storm, forecast_date) + variant + (admin_level,)
        if self._is_fresh(key) and key in self._admin_geoms:
            return
        with self._load_lock:
            if self._is_fresh(key) and key in self._admin_geoms:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]
            all_rows_admin: list[dict] = []
            for code in codes:
                log.info("Cache: bulk-loading admin %s/%s/%s hazard=%s variant=%s L%s [%s]…",
                         code, storm, forecast_date, hazard, variant, admin_level, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    if hazard == "gust":
                        all_rows_admin.extend(_run_query(_ADMIN_GUST_SQL, [
                            storm, forecast_date, gust_threshold, code, admin_level,
                        ]))
                    elif hazard == "river":
                        all_rows_admin.extend(_run_query(_ADMIN_RIVER_SQL, [
                            code, forecast_date, rp_tier, code, admin_level,
                        ]))
                    elif hazard == "rain":
                        all_rows_admin.extend(_run_query(_ADMIN_PRECIP_SQL, [
                            forecast_date, threshold_mm, window_h, code, admin_level,
                        ]))
                    else:
                        all_rows_admin.extend(_run_query(_ADMIN_FULL_SQL, [
                            storm, forecast_date, wind_threshold,
                            storm, forecast_date,
                            storm, forecast_date,
                            code, admin_level,
                        ]))
                elif hazard == "wind":
                    all_rows_admin.extend(_load_admin_from_files(code, storm, forecast_date, wind_threshold, admin_level))
                else:
                    log.warning("%s STAGE (file-based) admin loading not implemented — returning empty for %s",
                                hazard, code)
            df = pd.DataFrame(all_rows_admin) if all_rows_admin else pd.DataFrame(columns=["TILE_ID"])
            self._admin[key] = df
            # Pre-parse geometries and build spatial index for instant tile filtering.
            geoms, props_list = [], []
            for _, row in df.iterrows():
                geojson_str = row.get("GEOJSON")
                if not geojson_str:
                    continue
                try:
                    geojson_data = json.loads(geojson_str) if isinstance(geojson_str, str) else geojson_str
                    geom = shape(geojson_data)
                    props = {k: _py(v) for k, v in row.items()
                             if k != "GEOJSON" and _safe_prop(v) is not None}
                    geoms.append(geom)
                    props_list.append(props)
                except Exception as e:
                    log.debug("Skip admin geom: %s", e)
            tree = STRtree(geoms)
            self._admin_geoms[key] = (geoms, tree, props_list)
            self._loaded_at[key] = time.time()
            log.info("  Cache: %d admin regions parsed + indexed", len(geoms))

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

    def ensure_facility(self, layer_type: str, country: str, storm: str,
                        forecast_date: str, wind_threshold: int, hazard: str = "wind",
                        gust_threshold: Optional[int] = None, rp_tier: Optional[str] = None,
                        threshold_mm: Optional[float] = None, window_h: Optional[int] = None) -> None:
        variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
        key = (layer_type, country, storm, forecast_date) + variant
        if self._is_fresh(key) and key in self._facility:
            return
        with self._load_lock:
            if self._is_fresh(key) and key in self._facility:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]
            all_rows: list[dict] = []
            for code in codes:
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
                elif hazard == "wind":
                    rows = _load_facility_from_files(layer_type, code, storm, forecast_date, wind_threshold)
                else:
                    log.warning("%s STAGE (file-based) facility loading not implemented — returning empty for %s",
                                hazard, code)
                    rows = []
                all_rows.extend(rows)
            df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
                columns=["NAME", "PROBABILITY", "LATITUDE", "LONGITUDE"])
            if "PROBABILITY" in df.columns:
                df["PROBABILITY"] = pd.to_numeric(df["PROBABILITY"], errors="coerce").fillna(0.0)
            log.info("  Cache: %d %s points (country=%s, hazard=%s)", len(df), layer_type, country, hazard)
            self._facility[key] = df
            self._loaded_at[key] = time.time()

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
# ---------------------------------------------------------------------------

_FACILITY_IMPACT_SQL: dict[str, str] = {
    "schools": """
        SELECT SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY, LATITUDE, LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
        WHERE COUNTRY = %s AND STORM = %s AND FORECAST_DATE = %s AND WIND_THRESHOLD = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """,
    "health": """
        SELECT NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY,
               ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LATITUDE,
               ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LONGITUDE
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
        SELECT NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY,
               ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LATITUDE,
               ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LONGITUDE
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

# River facility queries aggregate across STEP_H with MAX(PROBABILITY) — same
# real multi-step-per-tier duplication documented above _MERCATOR_RIVER_SQL
# applies identically to every *_RIVER_MAT facility table (confirmed live:
# SCHOOL_RIVER_MAT alone had 159,652 rows for only 39,913 distinct schools at
# rp10 — a 4x duplication exactly matching rp10's 4 real STEP_H values).
_FACILITY_RIVER_SQL: dict[str, str] = {
    "schools": """
        SELECT SCHOOL_NAME, EDUCATION_LEVEL, MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.SCHOOL_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY SCHOOL_NAME, EDUCATION_LEVEL
    """,
    "health": """
        SELECT NAME, FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY,
               ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(GEOM_STR, 'HEX')))) AS LATITUDE,
               ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(GEOM_STR, 'HEX')))) AS LONGITUDE
        FROM (
            SELECT NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE,
                   MAX(PROBABILITY) AS PROBABILITY,
                   MAX(ALL_DATA:geometry::STRING) AS GEOM_STR
            FROM AOTS.TC_ECMWF.HC_RIVER_MAT
            WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
              AND ALL_DATA:geometry::STRING IS NOT NULL
            GROUP BY NAME, HEALTH_AMENITY_TYPE, OPERATOR_TYPE
        )
    """,
    "shelters": """
        SELECT NAME, SHELTER_TYPE, CATEGORY, MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.SHELTER_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY NAME, SHELTER_TYPE, CATEGORY
    """,
    "wash": """
        SELECT NAME, WASH_TYPE, CATEGORY, MAX(PROBABILITY) AS PROBABILITY,
               MAX(LATITUDE) AS LATITUDE, MAX(LONGITUDE) AS LONGITUDE
        FROM AOTS.TC_ECMWF.WASH_RIVER_MAT
        WHERE COUNTRY = %s AND FORECAST_TIME = %s AND RP_TIER = %s
          AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
        GROUP BY NAME, WASH_TYPE, CATEGORY
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
        SELECT NAME, HEALTH_AMENITY_TYPE AS FACILITY_TYPE, OPERATOR_TYPE, PROBABILITY,
               ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LATITUDE,
               ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(ALL_DATA:geometry::STRING, 'HEX')))) AS LONGITUDE
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

    for i in range(len(vals)):
        if not np.isfinite(t[i]):
            continue
        r, g, b, a = palette_rgba[idx[i]]
        x0 = max(0, px0[i])
        y0 = max(0, py0[i])
        x1 = min(512, max(x0 + 1, px1[i]))
        y1 = min(512, max(y0 + 1, py1[i]))
        img_arr[y0:y1, x0:x1] = (r, g, b, a)

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

# T+0 -> T+6h accumulated-mm window used as a "current rain rate" snapshot.
# tp is stored as a cumulative total from T+0 (see MET_FORECASTS ingestion),
# so any single step value would be an ever-growing blob unrelated to "how
# hard is it raining right now". Differencing two adjacent steps yields a
# bounded, radar-like rate instead. The very first window (T+0 to T+6h) is
# chosen over a later one (e.g. T+72h-T+78h) because it's the closest
# available proxy to current conditions at the model's own init time — later
# windows describe a future 6h period, not "now".
_PRECIP_RATE_STEP_A = 0
_PRECIP_RATE_STEP_B = 6

# Radar-style color ramp for mm accumulated over the T+0->T+6h window.
# Chosen to resemble a standard weather-radar reflectivity ramp (green ->
# yellow -> orange -> red), NOT this app's existing sequential blue/red
# impact-probability palettes. Alpha rises with intensity so heavier rain
# reads as more solid/opaque, matching how radar overlays are usually
# perceived.
#   < 0.5mm  : transparent  (no perceptible rain)
#   0.5-5mm  : light green  (light rain)
#   5-15mm   : green        (moderate rain)
#   15-30mm  : yellow       (heavy rain)
#   30-60mm  : orange       (very heavy rain)
#   >=60mm   : red          (extreme rain)
_PRECIP_RATE_BREAKS = [0.5, 5.0, 15.0, 30.0, 60.0]
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
# members whose T+0->T+6h accumulated rate exceeds this many mm. 10mm/6h
# (~1.7mm/h average) is a widely-used operational threshold for the onset of
# moderate rain — high enough to filter out drizzle/model noise, low enough
# to give useful lead-time signal before conditions turn heavy. This is a
# single fixed cut point, chosen independently of the mean ramp's own
# 0.5/5/15/30/60mm intensity buckets above (those describe magnitude of a
# single ensemble-mean value; this describes ensemble agreement on a single
# threshold).
_PRECIP_PROB_THRESHOLD_MM = 10.0

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


class _PrecipRawCache:
    """Downloads+opens the latest global tp Zarr ONCE per forecast_time, keeps
    an ensemble-mean precip-RATE grid (mm over the T+0->T+6h window) in memory.

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
        """Ensure the precip-rate grid for `forecast_time` is loaded in memory.

        `forecast_time` of None/""/"latest" always resolves to the current
        latest cycle. Returns the resolved forecast_time string actually
        loaded, or None if no tp data exists at all (for the requested time,
        or globally).
        """
        latest = self._resolve_latest()
        if latest is None:
            return None
        latest_forecast_time, latest_stage_path = latest

        if forecast_time in (None, "", "latest"):
            forecast_time = latest_forecast_time
            stage_path = latest_stage_path
        elif forecast_time == latest_forecast_time:
            stage_path = latest_stage_path
        else:
            rows = _run_query(_PRECIP_RAW_BY_TIME_SQL, [forecast_time])
            if not rows:
                return None
            stage_path = rows[0]["STAGE_PATH"]

        if forecast_time in self._grid and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _PRECIP_RAW_TTL:
            return forecast_time
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
                    ia, ib = steps.index(_PRECIP_RATE_STEP_A), steps.index(_PRECIP_RATE_STEP_B)
                    data_a = np.asarray(arr[:, ia, :, :]).astype(np.float32)
                    data_b = np.asarray(arr[:, ib, :, :]).astype(np.float32)
                    rate_grid = data_b - data_a  # (51, n_lat, n_lon) mm over [ia, ib)
                    # Ensemble mean across all 51 members -> one representative rate grid,
                    # conceptually mirroring wind/gust's own ensemble-probability coloring
                    # (an aggregate across members, not a single member's raw value).
                    mean_rate = np.nanmean(rate_grid, axis=0)  # (n_lat, n_lon)
                    # Exceedance-probability reduction on the SAME per-member rate_grid,
                    # computed here (before it goes out of scope) rather than re-downloading
                    # the Zarr later for the probability variant. Mirrors the DATAPIPELINE
                    # repo's own precip_utils.exceedance_probability() idiom exactly: count
                    # members exceeding the threshold, divide by the fixed ensemble size (not
                    # rate_grid.shape[0]) — NaNs compare False against the threshold, so they
                    # fall out as "non-exceeding" automatically, same documented convention.
                    prob_rate = (
                        (rate_grid > _PRECIP_PROB_THRESHOLD_MM).sum(axis=0)
                        / _PRECIP_PROB_ENSEMBLE_SIZE
                    ).astype(np.float32)  # (n_lat, n_lon), fraction in [0, 1]
                    lat_min, lat_max = float(attrs["lat_min"]), float(attrs["lat_max"])
                    lon_min, lon_max = float(attrs["lon_min"]), float(attrs["lon_max"])
                finally:
                    store.close()
            n_lat, n_lon = mean_rate.shape
            self._grid[forecast_time] = {
                "grid": mean_rate,
                "prob_grid": prob_rate,
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max,
                "n_lat": n_lat, "n_lon": n_lon,
                # Grid row 0 = lat_max (rows run N->S) — same convention as the
                # DATAPIPELINE repo's own tp Zarr reader (precip_utils.py).
                "lat_step": (lat_max - lat_min) / (n_lat - 1),
                "lon_step": (lon_max - lon_min) / (n_lon - 1),
            }
            self._loaded_at[forecast_time] = time.time()
            finite = mean_rate[np.isfinite(mean_rate)]
            log.info("  PrecipRaw: grid ready %s (%dx%d, mean rate %.2f-%.2fmm)",
                      forecast_time, n_lat, n_lon,
                      float(finite.min()) if finite.size else 0.0,
                      float(finite.max()) if finite.size else 0.0)
        return forecast_time

    def get_grid(self, forecast_time: str) -> Optional[dict]:
        return self._grid.get(forecast_time)


_precip_cache = _PrecipRawCache()


def _colorize_precip_rate(vals: np.ndarray) -> np.ndarray:
    """Map a (H, W) grid of mm-over-6h precip rate to an RGBA radar-style image.

    See _PRECIP_RATE_BREAKS/_PRECIP_RATE_COLORS above for the exact ramp.
    """
    h, w = vals.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)
    finite = np.isfinite(vals)
    safe = np.where(finite, vals, -1.0)
    idx = np.digitize(safe, _PRECIP_RATE_BREAKS)  # 0..len(_PRECIP_RATE_BREAKS)
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
def _fetch_precip_raw_tile(forecast_time: str, z: int, x: int, y: int, mode: str = "mean") -> bytes | None:
    """Render a 512x512 RGBA WebP tile from the cached global precip grid.

    `mode` selects which reduction of the cached data is rendered:
    - "mean" (default, backward-compatible): ensemble-mean rate, radar-style
      green/yellow/orange/red ramp (see _colorize_precip_rate).
    - "probability": fraction of ensemble members exceeding
      _PRECIP_PROB_THRESHOLD_MM, sequential-purple ramp (see
      _colorize_precip_probability). Both grids come from the SAME cached
      per-forecast_time download — selecting "probability" never triggers a
      second Zarr fetch.

    Returns None if the tile is entirely outside the grid's -60..60 latitude
    coverage, if no data exists for `forecast_time`, or if every sampled
    pixel is transparent (no rain in this tile).
    """
    resolved = _precip_cache.ensure_precip_raw(forecast_time)
    if resolved is None:
        return None
    entry = _precip_cache.get_grid(resolved)
    return _render_dense_grid_webp(entry, mode, _colorize_precip_rate, _colorize_precip_probability, z, x, y)


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
# RESOLUTION CHOICE: 0.1deg output grid — the SAME resolution/domain as the
# old dis24 rasterization above (_RIVER_RASTER_RES_DEG/_RIVER_RASTER_LAT/LON_*),
# reused here for an analogous but distinct reason: native pixel spacing here
# is ~0.0013-0.0015deg (near-JRC 90-150m resolution) — roughly 65-115x finer
# per degree than 0.1deg in each dimension. Rendering at anywhere close to
# native resolution would balloon the output grid to many millions-of-times
# more cells than dis24's own 0.1deg grid (native data here is dense across
# the whole flood-relevant domain, unlike dis24's sparse ~457K points) for no
# visual benefit at the country-analysis zoom levels this map actually
# renders at, while a much coarser grid would blur distinct river
# reaches/floodplains into indistinguishable blobs. 0.1deg keeps memory
# bounded (~4.3M cells, tens of MB per grid) and stays visually consistent
# with precip/the old river layer's own resolution, without over-investing
# compute into a needlessly fine grid.
#
# AGGREGATION / MEMORY SAFETY: the real per-forecast_time file is ~74M rows
# (28MB compressed) for the whole world, ALL step_h values combined. Loading
# it via pandas.read_parquet() in one shot risks a multi-GB memory spike (an
# estimated 3GB+) for data that, once filtered to step_h==24 and binned,
# collapses to a tiny footprint. Instead this reads the file via
# pyarrow.parquet.ParquetFile.read_row_group() in an explicit per-row-group
# loop (see _RiverExtentCache.ensure_river_extent) — each row group is
# converted to numpy arrays, filtered to step_h==24, and immediately reduced
# into a compact (n_cells,) uint64 BITMASK (one bit per ensemble member,
# dedup-by-OR — see below) before being discarded; no full-file DataFrame or
# (cells x 51 members) dense array is ever materialized.
#
# DISTINCT-MEMBER COUNTING: one member's flood can span multiple native
# pixels that collapse into the SAME 0.1deg output cell, so counting raw ROWS
# per cell would double/triple/... count a single member. Instead each row
# sets bit (member-1) of a per-cell np.uint64 via np.bitwise_or.at (an
# idempotent OR — setting the same member's bit twice from two colliding
# pixels is a no-op), so the final popcount of each cell's bitmask is a REAL
# count of DISTINCT members (0-51) that flood that cell at step_h=24,
# regardless of how many raw pixels contributed. 51 members fits comfortably
# in a uint64's 64 bits, and a single (n_cells,) uint64 array (~34.6MB for
# the whole 0.1deg grid) is far cheaper than a (n_cells, 51) boolean array
# (~220MB) while being exactly equivalent.
#
# MEAN vs PROBABILITY JUDGMENT CALL: this data is inherently a binary
# per-member flooded/not-flooded fact at a single fixed return period (RP10)
# — unlike precip's mm or the old dis24 layer's m3/s, there is NO natural
# continuous "mean intensity" to compute here, and fabricating one (e.g.
# averaging the member count into some invented "severity score") would
# misrepresent the data as something it is not. So:
#   - "probability" mode is the one real, non-fabricated continuous metric:
#     count-of-flooded-members / 51 (see above) — a TRUE per-cell RP10
#     exceedance fraction, rendered as a continuous cyan->navy gradient
#     (_colorize_river_extent_probability). This is a MORE meaningful number
#     than the old dis24 layer's own "probability" (only ever a fallback
#     percentile-of-this-cycle's-own-data proxy, never a real return-period
#     value) — deliberately given its own distinct hue family so it doesn't
#     look like a continuation of that older, less meaningful metric.
#   - "mean" mode renders a MAJORITY-VOTE binary consensus mask instead
#     (>50% of the 51 members agree this cell floods at RP10 => a single
#     flat solid color; anything else => transparent), rather than reusing
#     probability's gradient under a different name. This keeps the two
#     modes visually AND semantically distinct (a discrete "where does the
#     ensemble majority agree flooding happens" layer vs. a continuous "how
#     confident is that" layer) while still being an honest reduction of the
#     real per-member data, not an invented continuous quantity.
# ---------------------------------------------------------------------------

_RIVER_EXTENT_RP_TIER = "rp10"  # only genuinely computed (IS_STANDIN=False) tier available; see module comment

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
# 0.05deg domain (-60..60 lat x -180..180 lon) rendered at a 0.1deg OUTPUT
# grid (2x the native cell width, ~4.3M cells: 1200 x 3600), thickening each
# river reach to a visible width across zoom levels while still collapsing
# at most a handful of native cells per output cell.
_RIVER_EXTENT_RES_DEG = 0.1
_RIVER_EXTENT_LAT_MIN = -60.0
_RIVER_EXTENT_LAT_MAX = 60.0
_RIVER_EXTENT_LON_MIN = -180.0
_RIVER_EXTENT_LON_MAX = 180.0
_RIVER_EXTENT_N_LAT = 1200
_RIVER_EXTENT_N_LON = 3600

_LATEST_RIVER_EXTENT_SQL = """
    SELECT FORECAST_TIME, STAGE_PATH
    FROM AOTS.TC_ECMWF.RIVER_FORECASTS
    WHERE PARAM = 'extent_rp10_bymember'
    ORDER BY FORECAST_TIME DESC
    LIMIT 1
"""

_RIVER_EXTENT_BY_TIME_SQL = """
    SELECT STAGE_PATH
    FROM AOTS.TC_ECMWF.RIVER_FORECASTS
    WHERE PARAM = 'extent_rp10_bymember' AND FORECAST_TIME = %s
"""

# Flat, solid "consensus flood" color for the MEAN/majority-vote mask (see
# module comment) — deliberately an orange/red, NOT a blue/teal, so it reads
# as visually distinct at a glance from BOTH the probability ramp immediately
# below AND the old (now-legacy) dis24 blue/purple + teal ramps.
_RIVER_EXTENT_MASK_COLOR: tuple[int, int, int, int] = (255, 87, 34, 190)

# Sequential cyan->navy ramp for the PROBABILITY variant (real RP10
# exceedance fraction — see module comment). Deliberately a different hue
# family from the OLD (legacy) river-probability teal ramp above and from
# precip's purple ramp, so all three stay visually distinguishable.
_RIVER_EXTENT_PROB_BREAKS = [0.10, 0.25, 0.40, 0.60, 0.80]
_RIVER_EXTENT_PROB_COLORS: list[tuple[int, int, int, int]] = [
    (0,   0,   0,   0),
    (178, 235, 242, 90),
    (77,  208, 225, 140),
    (0,   172, 193, 180),
    (0,   105, 146, 215),
    (1,   50,  96,  245),
]


def _colorize_river_extent_probability(vals: np.ndarray) -> np.ndarray:
    """Map a (H, W) grid of REAL per-cell RP10 flood-exceedance fractions
    (0-1 = count of the 51 members whose extent covers this cell / 51, see
    _RiverExtentCache) to an RGBA sequential cyan->navy image. See
    _RIVER_EXTENT_PROB_BREAKS/_COLORS above for the exact ramp and the
    module-level comment for why this differs from the old dis24 layer's own
    (fallback-percentile, not a true return period) probability ramp."""
    h, w = vals.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)
    finite = np.isfinite(vals)
    safe = np.where(finite, vals, -1.0)
    idx = np.digitize(safe, _RIVER_EXTENT_PROB_BREAKS)
    for i, color in enumerate(_RIVER_EXTENT_PROB_COLORS):
        mask = finite & (idx == i)
        if mask.any():
            img[mask] = color
    return img


def _colorize_river_extent_mean(vals: np.ndarray) -> np.ndarray:
    """"Mean" mode for flood-extent data — see the long MEAN vs PROBABILITY
    module comment above for the full judgment call. `vals` here is already a
    MAJORITY-VOTE mask computed by _RiverExtentCache (1.0 = more than half of
    the 51 members agree this cell floods at RP10, NaN = no data or a
    minority) — rendered as a single FLAT solid color (no gradient/breaks),
    deliberately not reusing the probability ramp, so this reads as a
    discrete "does the ensemble majority agree" layer rather than a
    continuous intensity that does not actually exist for this data."""
    h, w = vals.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)
    mask = np.isfinite(vals) & (vals > 0.5)
    img[mask] = _RIVER_EXTENT_MASK_COLOR
    return img


class _RiverExtentCache:
    """Downloads+processes the latest global extent_rp10_bymember Parquet
    ONCE per forecast_time via a memory-conscious pyarrow row-group loop (see
    the module-level comment above for the full memory-safety rationale —
    NEVER materializes the full ~74M-row file as one pandas DataFrame),
    producing the SAME grid-entry shape as _PrecipRawCache/_RiverRawCache
    (grid/prob_grid/lat_min/.../lat_step/lon_step) so it can reuse
    _sample_global_grid_tile/_render_dense_grid_webp unchanged.

    `grid` = majority-vote mask (1.0/NaN, see _colorize_river_extent_mean).
    `prob_grid` = real 0-1 fraction of members flooding each cell (see
    _colorize_river_extent_probability). Both derived from the SAME per-cell
    member-bitmask computed in one pass — no second file read for the second
    mode.

    TTL: _RIVER_EXTENT_TTL (4h). Thread-safe via double-checked locking, same
    pattern as _PrecipRawCache/_RiverRawCache above.
    """

    def __init__(self) -> None:
        self._grids: dict[str, dict] = {}       # forecast_time -> grid entry
        self._loaded_at: dict[str, float] = {}   # forecast_time -> epoch seconds
        self._load_lock = threading.Lock()
        self._latest_lock = threading.Lock()
        self._latest: Optional[tuple[str, str, float]] = None

    def _resolve_latest(self) -> Optional[tuple[str, str]]:
        now = time.time()
        if self._latest and (now - self._latest[2]) < _RIVER_EXTENT_TTL:
            return self._latest[0], self._latest[1]
        with self._latest_lock:
            if self._latest and (now - self._latest[2]) < _RIVER_EXTENT_TTL:
                return self._latest[0], self._latest[1]
            rows = _run_query(_LATEST_RIVER_EXTENT_SQL, [])
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

    def ensure_river_extent(self, forecast_time: Optional[str]) -> Optional[str]:
        """Ensure the flood-extent grid pair for `forecast_time` is loaded in
        memory.

        `forecast_time` of None/""/"latest" always resolves to the current
        latest cycle. Returns the resolved forecast_time string actually
        loaded (a plain date like "2026-07-14" — this data is keyed by DATE,
        unlike dis24's full datetime FORECAST_TIME), or None if no
        extent_rp10_bymember data exists at all (for the requested time, or
        globally).
        """
        latest = self._resolve_latest()
        if latest is None:
            return None
        latest_forecast_time, latest_stage_path = latest

        if forecast_time in (None, "", "latest"):
            forecast_time = latest_forecast_time
            stage_path = latest_stage_path
        elif forecast_time == latest_forecast_time:
            stage_path = latest_stage_path
        else:
            rows = _run_query(_RIVER_EXTENT_BY_TIME_SQL, [forecast_time])
            if not rows:
                return None
            stage_path = rows[0]["STAGE_PATH"]

        if forecast_time in self._grids and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _RIVER_EXTENT_TTL:
            return forecast_time
        with self._load_lock:
            if forecast_time in self._grids and (time.time() - self._loaded_at.get(forecast_time, 0.0)) < _RIVER_EXTENT_TTL:
                return forecast_time
            if forecast_time in self._grids:
                self._loaded_at[forecast_time] = time.time()
                return forecast_time
            log.info("RiverExtent: downloading+processing extent_rp10_bymember parquet for %s (%s)…",
                      forecast_time, stage_path)
            t0 = time.perf_counter()
            # Local import: same reasoning as _RiverRawCache/_PrecipRawCache above.
            from components.data.data_store_utils import get_data_store
            raw_bytes = get_data_store().read_file(stage_path)

            n_lat, n_lon = _RIVER_EXTENT_N_LAT, _RIVER_EXTENT_N_LON
            n_cells_flat = n_lat * n_lon
            # One bit per ensemble member (51 fits comfortably in 64) — see the
            # module-level "DISTINCT-MEMBER COUNTING" comment for why this is
            # exactly equivalent to (and far cheaper than) a (cells, 51) bool
            # array, and idempotent under colliding native pixels.
            membership_bits = np.zeros(n_cells_flat, dtype=np.uint64)
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

                    # Nearest-cell assignment onto the 0.1deg output grid — same
                    # method/convention as the old dis24 _rasterize_river_points
                    # (row 0 = lat_max, N->S).
                    row_idx = np.round((_RIVER_EXTENT_LAT_MAX - lat) / _RIVER_EXTENT_RES_DEG).astype(np.int64)
                    lon_wrapped = ((lon + 180.0) % 360.0) - 180.0
                    col_idx = np.round((lon_wrapped - _RIVER_EXTENT_LON_MIN) / _RIVER_EXTENT_RES_DEG).astype(np.int64)
                    in_bounds = (row_idx >= 0) & (row_idx < n_lat) & (col_idx >= 0) & (col_idx < n_lon)
                    if not in_bounds.all():
                        row_idx = row_idx[in_bounds]
                        col_idx = col_idx[in_bounds]
                        member = member[in_bounds]
                    if row_idx.size == 0:
                        continue
                    flat_idx = row_idx * n_lon + col_idx
                    bits = np.uint64(1) << (member - 1).astype(np.uint64)
                    np.bitwise_or.at(membership_bits, flat_idx, bits)

            # Popcount each cell's bitmask -> real distinct-member count (0-51).
            # A plain 51-iteration bit-shift loop over a single (n_cells,) array
            # is fast and avoids materializing any (cells, 51) intermediate.
            member_count = np.zeros(n_cells_flat, dtype=np.int32)
            for b in range(_RIVER_PROB_ENSEMBLE_SIZE):
                member_count += ((membership_bits >> np.uint64(b)) & np.uint64(1)).astype(np.int32)
            del membership_bits

            has_data = member_count > 0
            prob_flat = member_count.astype(np.float32) / _RIVER_PROB_ENSEMBLE_SIZE
            prob_flat[~has_data] = np.nan
            prob_grid = prob_flat.reshape(n_lat, n_lon)

            # MAJORITY-VOTE mask for "mean" mode — see module-level MEAN vs
            # PROBABILITY comment. > (not >=) half of 51 -> >25.5 -> >=26.
            majority_flat = np.where(member_count > (_RIVER_PROB_ENSEMBLE_SIZE / 2.0), 1.0, np.nan).astype(np.float32)
            mean_grid = majority_flat.reshape(n_lat, n_lon)

            elapsed = time.perf_counter() - t0
            self._grids[forecast_time] = {
                "grid": mean_grid,
                "prob_grid": prob_grid,
                "member_count_grid": member_count.reshape(n_lat, n_lon),
                "lat_min": _RIVER_EXTENT_LAT_MIN, "lat_max": _RIVER_EXTENT_LAT_MAX,
                "lon_min": _RIVER_EXTENT_LON_MIN, "lon_max": _RIVER_EXTENT_LON_MAX,
                "n_lat": n_lat, "n_lon": n_lon,
                "lat_step": _RIVER_EXTENT_RES_DEG, "lon_step": _RIVER_EXTENT_RES_DEG,
                "rp_tier": _RIVER_EXTENT_RP_TIER,
                "rows_scanned": rows_scanned,
                "rows_kept": rows_kept,
                "n_members_seen": len(members_seen),
                "load_seconds": elapsed,
            }
            self._loaded_at[forecast_time] = time.time()
            n_nonzero_cells = int(has_data.sum())
            log.info("  RiverExtent: grid ready %s (scanned %d rows, kept %d @step_h=%d, "
                      "%d members seen, %d/%d cells with >=1 member flooded, %.1fs)",
                      forecast_time, rows_scanned, rows_kept, _RIVER_EXTENT_STEP_H,
                      len(members_seen), n_nonzero_cells, n_cells_flat, elapsed)
        return forecast_time

    def get_grid(self, forecast_time: str) -> Optional[dict]:
        return self._grids.get(forecast_time)


_river_extent_cache = _RiverExtentCache()


@_ttl_cache(ttl_seconds=_TILE_TTL, maxsize=8192)
def _fetch_river_extent_raster_tile(forecast_time: str, z: int, x: int, y: int, mode: str = "mean") -> bytes | None:
    """Render a 512x512 RGBA WebP tile from the cached global RP10
    flood-extent grid (see _RiverExtentCache) — THE current implementation
    behind /tiles/raster/river-raw/.../*.webp (see the module-level comment
    above for the full rationale for replacing the old dis24-based
    raw-discharge implementation, since removed).

    ALWAYS renders the continuous per-cell RP10 exceedance fraction
    (cyan->navy gradient, more member agreement = darker blue — see
    _colorize_river_extent_probability), regardless of the `mode` param —
    explicit product decision: unlike rain (a genuine continuous quantity
    that can be meaningfully averaged), river's "Mean" was a binary
    majority-vote consensus mask with no real gradient at all, which read as
    inconsistent with rain's own true continuous Mean under the same shared
    Mean/Probability toggle. `mode` is kept as a parameter only so the
    shared flood-view-as-driven call site (which also drives rain) doesn't
    need a river-specific code path; the majority-vote mask colorizer
    (_colorize_river_extent_mean) is no longer called from here at all.
    """
    resolved = _river_extent_cache.ensure_river_extent(forecast_time)
    if resolved is None:
        return None
    entry = _river_extent_cache.get_grid(resolved)
    return _render_dense_grid_webp(entry, "probability", _colorize_river_extent_mean, _colorize_river_extent_probability, z, x, y)


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
    """Background loop (single daemon thread): warms the precip-raw and
    river-raw (now extent_rp10_bymember-based, see _RiverExtentCache) caches
    immediately on startup, then re-warms every _PREWARM_INTERVAL_SECONDS.
    Reuses ensure_precip_raw()/ensure_river_extent() directly (same functions
    the /preload/* endpoints and the raw tile endpoints call) -- no HTTP
    round-trip within the process. Each call is a fast no-op when the cache
    is already warm; see _PrecipRawCache/_RiverExtentCache double-checked
    locking. Errors are logged and swallowed so a warm-up failure (e.g.
    Snowflake hiccup) never crashes this thread or blocks the server."""
    while True:
        for label, ensure_fn in (
            ("precip-raw", lambda: _precip_cache.ensure_precip_raw(None)),
            ("river-raw", lambda: _river_extent_cache.ensure_river_extent(None)),
        ):
            try:
                log.info("Prewarm: checking %s cache…", label)
                resolved = ensure_fn()
                if resolved is None:
                    log.info("Prewarm: %s cache — no data currently available to warm", label)
                else:
                    log.info("Prewarm: %s cache warm (forecast_time=%s)", label, resolved)
            except Exception as e:
                log.error("Prewarm: %s cache warm-up failed: %s", label, e)
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
        return Response(status_code=204, headers={"Cache-Control": "public, max-age=3600"})
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Content-Encoding": "gzip", "Cache-Control": "public, max-age=3600"})


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
        return Response(status_code=204, headers={"Cache-Control": "public, max-age=3600"})
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Cache-Control": "public, max-age=3600"})


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
    def _load():
        try:
            _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold, hazard,
                                   gust_threshold, rp_tier, threshold_mm, window_h)
            _cache.ensure_admin(country.upper(), storm, forecast_date, wind_threshold, admin_level, hazard,
                                gust_threshold, rp_tier, threshold_mm, window_h)
            facility_sql = {"gust": _FACILITY_GUST_SQL, "river": _FACILITY_RIVER_SQL,
                            "rain": _FACILITY_PRECIP_SQL}.get(hazard, _FACILITY_IMPACT_SQL)
            for layer_type in facility_sql:
                _cache.ensure_facility(layer_type, country.upper(), storm, forecast_date, wind_threshold, hazard,
                                       gust_threshold, rp_tier, threshold_mm, window_h)
        except Exception as e:
            log.error("Preload error: %s", e)
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "country": country, "storm": storm}



# Shared min/max mapping for the "base + gust/river" family of stats queries
# (population/children/etc. from b., PROBABILITY + E_* impact cols from i.) —
# gust and river both select this exact same column set (river's own
# BELOW_MIN_BASIN/IS_STANDIN flags are booleans, not ramp-colorable numeric
# stats, so they're intentionally omitted here). Rain has its own much
# smaller mapping (_RAIN_STATS_MAPPING below) since only E_population is
# hazard-conditional for that table (see _MERCATOR_PRECIP_SQL's own comment).
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
    try:
        if IMPACT_DATA_STORE != "SNOWFLAKE":
            _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold, hazard,
                                   gust_threshold, rp_tier, threshold_mm, window_h)
            variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
            df = _cache._mercator.get((country.upper(), storm, forecast_date) + variant)
            return _stats_from_df(df) if df is not None else {}

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
    try:
        if IMPACT_DATA_STORE != "SNOWFLAKE":
            _cache.ensure_admin(country.upper(), storm, forecast_date, wind_threshold, admin_level, hazard,
                                gust_threshold, rp_tier, threshold_mm, window_h)
            variant = _hazard_variant(hazard, wind_threshold, gust_threshold, rp_tier, threshold_mm, window_h)
            df = _cache._admin.get((country.upper(), storm, forecast_date) + variant + (admin_level,))
            return _stats_from_df(df) if df is not None else {}

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
        return Response(content=_TRANSPARENT_WEBP, media_type="image/webp",
                        headers={"Cache-Control": "public, max-age=3600"})
    return Response(
        content=webp_bytes,
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=3600"},
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
) -> Response:
    """Return a styled GeoJSON FeatureCollection for the requested facility layer.

    Properties include `_color`, `_radius`, `_opacity`, `_weight`, `_fillOpacity` for
    Leaflet's pointToLayer, plus lowercase field names for tooltip functions.
    Response is gzip-compressed so the browser receives it efficiently via fetch().
    """
    valid_layers = {"gust": _FACILITY_GUST_SQL, "river": _FACILITY_RIVER_SQL,
                    "rain": _FACILITY_PRECIP_SQL}.get(hazard, _FACILITY_IMPACT_SQL)
    if layer_type not in valid_layers:
        raise HTTPException(status_code=404, detail=f"Unknown layer type: {layer_type}")
    base_color = _FACILITY_BASE_COLORS[layer_type]
    try:
        df = _cache.get_facility_df(layer_type, country.upper(), storm, forecast_date, wind_threshold,
                                    hazard, gust_threshold, rp_tier, threshold_mm, window_h)
        features = []
        for _, row in df.iterrows():
            lat = row.get("LATITUDE")
            lon = row.get("LONGITUDE")
            if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
                continue
            prob = float(row.get("PROBABILITY") or 0)
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
                     if k not in ("LATITUDE", "LONGITUDE")}
            props.update({
                "_color": color, "_radius": radius,
                "_opacity": 0.8, "_weight": 2, "_fillOpacity": 0.7,
            })
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                "properties": props,
            })
        body = gzip.compress(
            json.dumps({"type": "FeatureCollection", "features": features}).encode(),
            compresslevel=6,
        )
        return Response(
            content=body,
            media_type="application/geo+json",
            headers={"Content-Encoding": "gzip", "Cache-Control": "public, max-age=300"},
        )
    except Exception as exc:
        log.error("facility_geojson error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
) -> Response:
    """Global precip raster tile (mm over the T+0->T+6h window).

    `forecast_time` may be the literal string "latest" to always track the
    most recent tp forecast cycle without the caller needing to look it up.

    `mode=mean` (default, backward-compatible): ensemble-mean rate, radar-style
    ramp. `mode=probability`: fraction of ensemble members exceeding
    _PRECIP_PROB_THRESHOLD_MM, sequential-purple ramp. Both are derived from
    the same cached per-forecast_time download.
    """
    try:
        webp_bytes = _fetch_precip_raw_tile(forecast_time, z, x, y, mode)
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


@app.get("/stats/precip-raw/{forecast_time}")
def get_precip_raw_stats(
    forecast_time: str,
    mode: str = Query("mean", pattern="^(mean|probability)$"),
) -> dict:
    """Legend range for the precip palette.

    This is a fixed scale (not a per-country relative scale like the existing
    /stats/{country}/...), returned without forcing a ~1.2GB grid download
    just to answer a legend request.

    `mode=mean`: physical mm-over-6h scale (unchanged from before). `mode=
    probability`: fixed [0, 1] fraction scale — no need to compute real
    min/max from data since exceedance-probability is always in that range
    by construction.
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
            "threshold_mm": _PRECIP_PROB_THRESHOLD_MM,
            "forecast_time": resolved,
            "mode": mode,
        }
    return {
        "min": 0.0,
        "max": _PRECIP_RATE_BREAKS[-1],
        "breaks": _PRECIP_RATE_BREAKS,
        "forecast_time": resolved,
        "mode": mode,
    }


@app.get("/preload/precip-raw/{forecast_time}")
def preload_precip_raw(forecast_time: str) -> dict:
    """Pre-warm the precip-raw grid cache. Returns immediately; the ~1.2GB
    download + Zarr open happens in a background thread (same fire-and-forget
    style as /preload/{country}/{storm}/{forecast_date}).

    Warms both the mean and probability grids in one download — both are
    computed together in ensure_precip_raw() from the same per-member
    rate_grid, so there is no separate "mode" to pass here."""
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
    mode: str = Query("mean", pattern="^(mean|probability)$"),
) -> Response:
    """Global river FLOOD-EXTENT (RP10, per-member) RASTER tile — THE
    endpoint the frontend should use. See the _RiverExtentCache module
    comment above for the full rationale (why extent_rp10_bymember replaces
    raw dis24 discharge, step_h/resolution/aggregation choices, and the
    Mean-vs-Probability judgment call).

    `forecast_time` may be the literal string "latest" to always track the
    most recent extent_rp10_bymember forecast cycle without the caller
    needing to look it up. Unlike the old dis24 layer, this is a plain DATE
    string (e.g. "2026-07-14"), not a full datetime.

    `mode=mean` (default): majority-vote consensus flood mask — a single
    flat solid color wherever >50% of the 51 members agree this cell floods
    at RP10 (see _colorize_river_extent_mean). `mode=probability`: the REAL
    per-cell fraction of members whose RP10 flood extent covers this cell,
    cyan->navy gradient (see _colorize_river_extent_probability). Both modes
    are derived from the same cached per-forecast_time download.
    """
    try:
        webp_bytes = _fetch_river_extent_raster_tile(forecast_time, z, x, y, mode)
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


@app.get("/stats/river-raw/{forecast_time}")
def get_river_raw_stats(
    forecast_time: str,
    mode: str = Query("mean", pattern="^(mean|probability)$"),
) -> dict:
    """Legend range for the river flood-extent raster (extent_rp10_bymember —
    see _RiverExtentCache above).

    ALWAYS returns the continuous per-cell RP10 exceedance fraction shape
    (fixed [0, 1] scale, `breaks`, `ensemble_size`) regardless of `mode` —
    matches _fetch_river_extent_raster_tile's own always-probability
    rendering (see that function's own docstring for why river dropped the
    separate majority-vote "mean" mode entirely). `mode` is echoed back only
    for API-shape consistency with rain's own real Mean/Probability stats.
    """
    resolved = _river_extent_cache.ensure_river_extent(forecast_time)
    if resolved is None:
        return {"min": None, "max": None, "forecast_time": None, "mode": mode, "rp_tier": _RIVER_EXTENT_RP_TIER}
    return {
        "min": 0.0,
        "max": 1.0,
        "breaks": _RIVER_EXTENT_PROB_BREAKS,
        "ensemble_size": _RIVER_PROB_ENSEMBLE_SIZE,
        "forecast_time": resolved,
        "mode": mode,
        "rp_tier": _RIVER_EXTENT_RP_TIER,
    }


@app.get("/preload/river-raw/{forecast_time}")
def preload_river_raw(forecast_time: str) -> dict:
    """Pre-warm the river flood-extent cache (both mean/probability grids,
    computed together in ensure_river_extent() — no separate "mode" to pass
    here). Returns immediately; the ~28MB parquet download + row-group
    processing happens in a background thread (same fire-and-forget style as
    /preload/precip-raw/{forecast_time})."""
    def _load():
        try:
            _river_extent_cache.ensure_river_extent(forecast_time)
        except Exception as e:
            log.error("Preload river-raw error: %s", e)
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "forecast_time": forecast_time}
