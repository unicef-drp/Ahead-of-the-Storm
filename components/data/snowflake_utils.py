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
from functools import lru_cache
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
# Replaces lru_cache with time-bounded expiry so the Dash app automatically
# serves fresh Snowflake data without a container restart.
#
# Mechanism: bucket = int(time.time() // ttl_seconds) is injected as the
# first argument of the inner lru_cache. The bucket integer increments every
# ttl_seconds seconds (at fixed wall-clock boundaries), which forces a cache
# miss and a fresh Snowflake query at most ttl_seconds after data changes.
# Thread-safe: inherits lru_cache's internal lock.

_META_TTL    = 15 * 60   # 15 min — storm list, forecast times (new storms appear promptly)
_IMPACT_TTL  = 15 * 60   # 15 min — impact queries (new pipeline output picked up within 15 min)
_BASE_TTL    = 60 * 60   # 60 min — base layers (schools/HCs/tiles — change only on re-init)


def ttl_cache(ttl_seconds: int, maxsize: int = 128):
    """lru_cache with automatic TTL-based expiry."""
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

# Per-thread connection storage — each Gunicorn worker thread gets its own connection
_thread_local = threading.local()
_HEALTH_CHECK_INTERVAL = 300  # seconds — recheck liveness at most once every 5 min

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

def get_envelope_data_snowflake(track_id, forecast_time):
    """Get envelope data directly from Snowflake"""
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
    """
    query = """
        WITH latest AS (
            SELECT COUNTRY, MAX(FORECAST_DATE) AS latest_forecast
            FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
            GROUP BY COUNTRY
            HAVING DATEDIFF('hour',
                TO_TIMESTAMP(MAX(FORECAST_DATE), 'YYYYMMDDHH24MISS'),
                CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
            ) <= 12
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
def _get_country_totals_cached(country: str) -> dict:
    try:
        query = """
        SELECT
            SUM(POPULATION)                                                          AS total_population,
            SUM(COALESCE(INFANT_POPULATION, 0)
              + COALESCE(SCHOOL_AGE_POPULATION, 0)
              + COALESCE(ADOLESCENT_POPULATION, 0))                                  AS total_children
        FROM AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT
        WHERE COUNTRY = %s
        """
        df = _run_query(query, params=[country])
        if df.empty or df.iloc[0]["TOTAL_POPULATION"] is None:
            return {"total_population": None, "total_children": None}
        row = df.iloc[0]
        return {
            "total_population": int(row["TOTAL_POPULATION"]) if pd.notna(row["TOTAL_POPULATION"]) else None,
            "total_children":   int(row["TOTAL_CHILDREN"])   if pd.notna(row["TOTAL_CHILDREN"])   else None,
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