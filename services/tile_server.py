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

import gzip
import io
import json
import logging
import math
import os
import threading
from functools import lru_cache
from typing import Optional

from PIL import Image

import mapbox_vector_tile
import numpy as np
import mercantile
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from shapely.geometry import box, shape
from shapely.ops import transform as _shapely_transform

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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
    conn = get_connection()
    with _query_lock:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            cols = [d[0].upper() for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()


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
        if v != v:  # NaN scalar check
            return None
    except (ValueError, TypeError):
        return None  # array/complex types can't be NaN-tested
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


class _DataCache:
    """Bulk-loads once from Snowflake; serves from pandas DataFrames.

    Load: ~2s Snowflake + ~0.5s bounds pre-computation (one-time per country/storm).
    Serve: ~0.5ms vectorised str.startswith at all zoom levels.
    Thread-safe: reads are GIL-protected dict/DataFrame ops; writes use
    double-checked locking.
    """

    def __init__(self) -> None:
        self._mercator: dict[tuple, pd.DataFrame] = {}
        self._admin: dict[tuple, pd.DataFrame] = {}
        self._admin_geoms: dict[tuple, tuple] = {}  # key → (geom_list, strtree, props_list)
        self._facility: dict[tuple, pd.DataFrame] = {}
        self._load_lock = threading.Lock()

    # --- mercator --------------------------------------------------------

    def ensure_mercator(self, country: str, storm: str, forecast_date: str,
                        wind_threshold: int) -> None:
        key = (country, storm, forecast_date, wind_threshold)
        if key in self._mercator:
            return
        with self._load_lock:
            if key in self._mercator:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]
            all_rows: list[dict] = []
            for code in codes:
                log.info("Cache: bulk-loading mercator %s/%s/%s/%skt [%s]…",
                         code, storm, forecast_date, wind_threshold, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    all_rows.extend(_run_query(_MERCATOR_FULL_SQL, [
                        storm, forecast_date, wind_threshold,
                        storm, forecast_date,
                        storm, forecast_date,
                        code, MAT_ZOOM_LEVEL,
                    ]))
                else:
                    all_rows.extend(_load_mercator_from_files(code, storm, forecast_date, wind_threshold))
            if all_rows:
                df = pd.DataFrame(all_rows)
                df = pd.concat([df, _precompute_mercator_bounds(df["TILE_ID"])], axis=1)
            else:
                df = pd.DataFrame(columns=["TILE_ID", "BW", "BS", "BE", "BN"])
            log.info("  Cache: %d z=14 tiles ready (country=%s)", len(df), country)
            self._mercator[key] = df

    def query_mercator(self, country: str, storm: str, forecast_date: str,
                       wind_threshold: int, like_pat: str, z: int) -> list[dict]:
        self.ensure_mercator(country, storm, forecast_date, wind_threshold)
        df = self._mercator.get((country, storm, forecast_date, wind_threshold))
        if df is None or df.empty:
            return []
        if like_pat.endswith("%"):
            mask = df["TILE_ID"].str.startswith(like_pat[:-1], na=False)
        else:
            mask = df["TILE_ID"] == like_pat
        return df[mask].to_dict("records")

    # --- admin -----------------------------------------------------------

    def ensure_admin(self, country: str, storm: str, forecast_date: str,
                     wind_threshold: int, admin_level: int) -> None:
        from shapely.strtree import STRtree
        key = (country, storm, forecast_date, wind_threshold, admin_level)
        if key in self._admin_geoms:
            return
        with self._load_lock:
            if key in self._admin_geoms:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]
            all_rows_admin: list[dict] = []
            for code in codes:
                log.info("Cache: bulk-loading admin %s/%s/%s/%skt L%s [%s]…",
                         code, storm, forecast_date, wind_threshold, admin_level, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    all_rows_admin.extend(_run_query(_ADMIN_FULL_SQL, [
                        storm, forecast_date, wind_threshold,
                        storm, forecast_date,
                        storm, forecast_date,
                        code, admin_level,
                    ]))
                else:
                    all_rows_admin.extend(_load_admin_from_files(code, storm, forecast_date, wind_threshold, admin_level))
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
            log.info("  Cache: %d admin regions parsed + indexed", len(geoms))

    def query_admin(self, country: str, storm: str, forecast_date: str,
                    wind_threshold: int, admin_level: int,
                    tile_w: float, tile_s: float, tile_e: float, tile_n: float) -> list[tuple]:
        """Returns list of (geom, props) for features that intersect the tile bbox."""
        self.ensure_admin(country, storm, forecast_date, wind_threshold, admin_level)
        entry = self._admin_geoms.get((country, storm, forecast_date, wind_threshold, admin_level))
        if not entry:
            return []
        geoms, tree, props_list = entry
        tile_box = box(tile_w, tile_s, tile_e, tile_n)
        candidate_idxs = tree.query(tile_box, predicate='intersects')
        return [(geoms[i], props_list[i]) for i in candidate_idxs]

    # --- facility points -------------------------------------------------

    def ensure_facility(self, layer_type: str, country: str, storm: str,
                        forecast_date: str, wind_threshold: int) -> None:
        key = (layer_type, country, storm, forecast_date, wind_threshold)
        if key in self._facility:
            return
        with self._load_lock:
            if key in self._facility:
                return
            codes = [c.upper() for c in country.split('+') if c.strip()]
            all_rows: list[dict] = []
            for code in codes:
                log.info("Cache: bulk-loading facility %s %s/%s/%s/%skt [%s]…",
                         layer_type, code, storm, forecast_date, wind_threshold, IMPACT_DATA_STORE)
                if IMPACT_DATA_STORE == "SNOWFLAKE":
                    rows = _run_query(_FACILITY_IMPACT_SQL[layer_type],
                                      [code, storm, forecast_date, wind_threshold])
                    if not rows:
                        log.info("  No impact data for %s %s — using base layer", layer_type, code)
                        rows = _run_query(_FACILITY_BASE_SQL[layer_type], [code])
                else:
                    rows = _load_facility_from_files(layer_type, code, storm, forecast_date, wind_threshold)
                all_rows.extend(rows)
            df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
                columns=["NAME", "PROBABILITY", "LATITUDE", "LONGITUDE"])
            if "PROBABILITY" in df.columns:
                df["PROBABILITY"] = pd.to_numeric(df["PROBABILITY"], errors="coerce").fillna(0.0)
            log.info("  Cache: %d %s points (country=%s)", len(df), layer_type, country)
            self._facility[key] = df

    def get_facility_df(self, layer_type: str, country: str, storm: str,
                        forecast_date: str, wind_threshold: int) -> "pd.DataFrame":
        self.ensure_facility(layer_type, country, storm, forecast_date, wind_threshold)
        return self._facility.get((layer_type, country, storm, forecast_date, wind_threshold),
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


@lru_cache(maxsize=8192)
def _fetch_mercator_tile(
    country: str,
    storm: str,
    forecast_date: str,
    wind_threshold: int,
    z: int,
    x: int,
    y: int,
) -> bytes:
    like_pat = _quadkey_like_pattern(z, x, y)
    rows = _cache.query_mercator(country, storm, forecast_date, wind_threshold, like_pat, z)
    if not rows:
        return b""

    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)

    features = []
    for row in rows:
        w, s, e, n = row.get("BW"), row.get("BS"), row.get("BE"), row.get("BN")
        # Skip rows with invalid pre-computed bounds (NaN from failed quadkey parse)
        if w is None or w != w:
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


@lru_cache(maxsize=2048)
def _fetch_admin_tile(
    country: str,
    storm: str,
    forecast_date: str,
    wind_threshold: int,
    admin_level: int,
    z: int,
    x: int,
    y: int,
) -> bytes:
    tile_w, tile_s, tile_e, tile_n = _tile_bounds(z, x, y)
    candidates = _cache.query_admin(country, storm, forecast_date, wind_threshold, admin_level,
                                   tile_w, tile_s, tile_e, tile_n)
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
    'E_PEOPLE_IN_NEED':        {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
    'E_CHILDREN_IN_NEED':      {'colors': ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#f03b20','#e31a1c','#bd0026','#800026'], 'scale': 'log'},
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
_minmax_cache: dict[tuple, tuple[float, float]] = {}
_minmax_lock = threading.Lock()


def _get_minmax(key: tuple, col: str) -> tuple[float, float] | None:
    """Return (min_val, max_val) for a column from the cached DataFrame.

    Respects palette fixed_max (e.g. probability=1.0). For linear palettes with
    a fixed ceiling, min is anchored at 0. For log palettes, min is the smallest
    positive value in the data.
    """
    cache_key = (key, col)
    if cache_key in _minmax_cache:
        return _minmax_cache[cache_key]
    with _minmax_lock:
        if cache_key in _minmax_cache:
            return _minmax_cache[cache_key]
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
        _minmax_cache[cache_key] = result
        return result


@lru_cache(maxsize=8192)
def _fetch_raster_tile(
    country: str,
    storm: str,
    forecast_date: str,
    wind_threshold: int,
    prop: str,
    z: int,
    x: int,
    y: int,
) -> bytes | None:
    """Render a 512x512 RGBA PNG for the requested tile.

    Always reads from z=14 DataFrame regardless of display zoom `z`.
    Returns None if there are no data rows.
    """
    if prop not in _RASTER_PALETTES:
        return None

    key = (country, storm, forecast_date, wind_threshold)
    _cache.ensure_mercator(*key)
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
    if spec['scale'] == 'smod':
        valid = np.isfinite(vals) & (vals >= 11) & np.isfinite(ws) & np.isfinite(ns)
    elif spec['scale'] == 'rwi':
        valid = np.isfinite(vals) & np.isfinite(ws) & np.isfinite(ns)
    elif spec['scale'] == 'log':
        valid = np.isfinite(vals) & (vals > 0) & np.isfinite(ws) & np.isfinite(ns)
    else:
        valid = np.isfinite(vals) & (vals != 0) & np.isfinite(ws) & np.isfinite(ns)
    vals, ws, ss, es, ns = vals[valid], ws[valid], ss[valid], es[valid], ns[valid]

    if len(vals) == 0:
        return None

    # Map values → RGBA using palette (vectorised log/linear).
    palette_rgba = _PALETTE_RGBA[prop]
    n_colors = len(palette_rgba)

    if spec['scale'] == 'log':
        safe = np.where(vals > 0, vals, np.nan)
        t = (np.log(safe) - math.log(min_val)) / (math.log(max_val) - math.log(min_val))
    elif spec['scale'] == 'linear':
        t = (vals - min_val) / (max_val - min_val)
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
    px0 = np.floor((ws - tile_w) / tile_dw * 512).astype(np.int32)
    px1 = np.ceil ((es - tile_w) / tile_dw * 512).astype(np.int32)
    merc_ns = np.log(np.tan(np.pi / 4 + np.radians(ns) / 2))
    merc_ss = np.log(np.tan(np.pi / 4 + np.radians(ss) / 2))
    py0 = np.floor((1.0 - (merc_ns - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32)
    py1 = np.ceil ((1.0 - (merc_ss - _merc_tile_s) / _merc_tile_dh) * 512).astype(np.int32)

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
    img.save(buf, 'WEBP', quality=80, method=4)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AoTS Tile Server",
    description="PBF tile sidecar for the Ahead of the Storm UNICEF dashboard.",
    version="2.0.0",
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
) -> Response:
    try:
        pbf = _fetch_mercator_tile(country.upper(), storm, forecast_date, wind_threshold, z, x, y)
    except Exception as exc:
        log.error("mercator_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not pbf:
        return Response(status_code=204)
    return Response(content=pbf, media_type="application/x-protobuf",
                    headers={"Content-Encoding": "gzip"})


@app.get("/tiles/admin/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf", response_class=Response)
def admin_tile(
    country: str, storm: str, forecast_date: str,
    z: int, x: int, y: int,
    wind_threshold: int = Query(...),
    admin_level: int = Query(1),
) -> Response:
    try:
        pbf = _fetch_admin_tile(country.upper(), storm, forecast_date, wind_threshold, admin_level, z, x, y)
    except Exception as exc:
        log.error("admin_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not pbf:
        return Response(status_code=204)
    return Response(content=pbf, media_type="application/x-protobuf")


@app.get("/preload/{country}/{storm}/{forecast_date}")
def preload(country: str, storm: str, forecast_date: str,
            wind_threshold: int = Query(...),
            admin_level: int = Query(1)) -> dict:
    """Pre-warm pandas cache. Call this when user selects a storm/forecast.
    Returns immediately; loading happens in a background thread."""
    def _load():
        try:
            _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold)
            _cache.ensure_admin(country.upper(), storm, forecast_date, wind_threshold, admin_level)
            for layer_type in _FACILITY_IMPACT_SQL:
                _cache.ensure_facility(layer_type, country.upper(), storm, forecast_date, wind_threshold)
        except Exception as e:
            log.error("Preload error: %s", e)
    threading.Thread(target=_load, daemon=True).start()
    return {"status": "loading", "country": country, "storm": storm}



@app.get("/stats/{country}/{storm}/{forecast_date}")
def get_tile_stats(
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = 50,
    zoom_level: int = 14,
) -> dict:
    try:
        if IMPACT_DATA_STORE != "SNOWFLAKE":
            _cache.ensure_mercator(country.upper(), storm, forecast_date, wind_threshold)
            df = _cache._mercator.get((country.upper(), storm, forecast_date, wind_threshold))
            return _stats_from_df(df) if df is not None else {}
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


@app.get("/admin-stats/{country}/{storm}/{forecast_date}")
def get_admin_stats(
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = 50,
    admin_level: int = 1,
) -> dict:
    try:
        if IMPACT_DATA_STORE != "SNOWFLAKE":
            _cache.ensure_admin(country.upper(), storm, forecast_date, wind_threshold, admin_level)
            df = _cache._admin.get((country.upper(), storm, forecast_date, wind_threshold, admin_level))
            return _stats_from_df(df) if df is not None else {}
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
) -> Response:
    """Return a 512×512 RGBA WebP raster tile colored by `prop`."""
    try:
        webp_bytes = _fetch_raster_tile(
            country.upper(), storm, forecast_date, wind_threshold,
            prop.upper(), z, x, y,
        )
    except Exception as exc:
        log.error("raster_tile error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not webp_bytes:
        return Response(status_code=204)
    return Response(
        content=webp_bytes,
        media_type="image/webp",
    )


@app.get("/geojson/facilities/{layer_type}/{country}/{storm}/{forecast_date}",
         response_class=Response)
def facility_geojson(
    layer_type: str,
    country: str, storm: str, forecast_date: str,
    wind_threshold: int = Query(...),
) -> Response:
    """Return a styled GeoJSON FeatureCollection for the requested facility layer.

    Properties include `_color`, `_radius`, `_opacity`, `_weight`, `_fillOpacity` for
    Leaflet's pointToLayer, plus lowercase field names for tooltip functions.
    Response is gzip-compressed so the browser receives it efficiently via fetch().
    """
    if layer_type not in _FACILITY_IMPACT_SQL:
        raise HTTPException(status_code=404, detail=f"Unknown layer type: {layer_type}")
    base_color = _FACILITY_BASE_COLORS[layer_type]
    try:
        df = _cache.get_facility_df(layer_type, country.upper(), storm, forecast_date, wind_threshold)
        features = []
        for _, row in df.iterrows():
            lat = row.get("LATITUDE")
            lon = row.get("LONGITUDE")
            if lat is None or lon is None or lat != lat or lon != lon:
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
) -> dict:
    """Return all property values for the z=14 Mercator tile at the given lon/lat.

    Used for hover tooltips on the raster layer.
    """
    tile = mercantile.tile(lon, lat, 14)
    qk = mercantile.quadkey(tile)
    key = (country.upper(), storm, forecast_date, wind_threshold)
    _cache.ensure_mercator(*key)
    df = _cache._mercator.get(key)
    if df is None or df.empty:
        return {}
    row = df[df['TILE_ID'] == qk]
    if row.empty:
        return {}
    result = row.iloc[0].drop(labels=['BW', 'BS', 'BE', 'BN'], errors='ignore').to_dict()
    return {k: (None if (v != v) else v) for k, v in result.items()}
