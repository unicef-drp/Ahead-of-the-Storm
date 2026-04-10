#!/usr/bin/env python3
"""
Snowflake Utilities Module

This module provides utility functions for Snowflake operations and data retrieval.
It serves as a focused toolkit for connecting to Snowflake and retrieving hurricane data.

Key Components:
- Snowflake connection management
- Hurricane track data retrieval from TC_TRACKS table
- Hurricane envelope data retrieval from TC_ENVELOPES_COMBINED table
- Data format conversion utilities

Usage:
    from snowflake_utils import get_envelopes_from_snowflake, get_hurricane_data_from_snowflake
    envelopes = get_envelopes_from_snowflake('JERRY', '2025-10-10 00:00:00')
    tracks = get_hurricane_data_from_snowflake('JERRY', '2025-10-10 00:00:00')
"""

import os
from functools import lru_cache
import pandas as pd
import numpy as np
import geopandas as gpd
import snowflake.connector
from shapely import wkt as shapely_wkt
import warnings

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config

# Connection caching
_connection_cache = None
_connection_created = False
_connection_verbose = True  # Set to False to suppress connection messages

def _is_connection_alive(conn):
    """Check if a Snowflake connection is still alive"""
    try:
        # Try a simple query to check if connection is alive
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return True
    except:
        return False

def get_snowflake_connection():
    """
    Get or create a Snowflake connection from centralized configuration.
    
    Reuses a cached connection if it's still alive, otherwise creates a new one.
    This improves efficiency by avoiding unnecessary reconnections.
    
    Supports two authentication methods:
    1. SPCS OAuth (for Snowflake Container Services):
       - Set SPCS_RUN=true
       - Token read from SPCS_TOKEN_PATH (default: /snowflake/session/token)
       - Requires SNOWFLAKE_HOST and SNOWFLAKE_PORT for internal network
       
    2. Password Authentication (traditional):
       - Requires SNOWFLAKE_USER and SNOWFLAKE_PASSWORD
    """
    global _connection_cache, _connection_created, _connection_verbose
    config.validate_snowflake_config()
    
    # Check if we have a cached connection that's still alive
    if _connection_cache is not None:
        if _is_connection_alive(_connection_cache):
            return _connection_cache
        else:
            # Connection is dead, clear cache
            try:
                _connection_cache.close()
            except:
                pass
            _connection_cache = None
    
    # Need to create a new connection
    should_print = _connection_verbose and not _connection_created
    
    if config.SPCS_RUN:
        # SPCS OAuth authentication using session token
        if should_print:
            print("Connecting to Snowflake with SPCS OAuth authentication...")
        
        try:
            with open(config.SPCS_TOKEN_PATH, 'r') as f:
                token = f.read().strip()
            
            # SPCS requires specific connection parameters for internal network
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
            if should_print:
                print(f"✓ Loaded OAuth token from {config.SPCS_TOKEN_PATH}")
                print(f"✓ Using SPCS internal network: {conn_params['host']}:{conn_params['port']}")
        except Exception as e:
            raise ValueError(f"Failed to load OAuth token from {config.SPCS_TOKEN_PATH}: {str(e)}")
    else:
        # Non-SPCS mode: use standard connection parameters with password
        if should_print:
            print("Connecting to Snowflake with password authentication...")
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
        if should_print:
            print("✓ Connected to Snowflake (connection will be reused)")
        # Cache the connection for reuse
        _connection_cache = conn
        _connection_created = True
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to Snowflake: {str(e)}")
        raise

def get_hurricane_data_from_snowflake(track_id, forecast_time):
    """
    Get hurricane track data from Snowflake TC_TRACKS table
    
    Args:
        track_id: Storm identifier (e.g., 'JERRY')
        forecast_time: Forecast time (e.g., '2025-10-10 00:00:00')
    
    Returns:
        pandas.DataFrame: Hurricane track data with wind field polygons
    """
    conn = get_snowflake_connection()
    
    query = """
    SELECT 
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME,
        LATITUDE,
        LONGITUDE,
        PRESSURE_HPA,
        WIND_SPEED_KNOTS,
        RADIUS_OF_MAXIMUM_WINDS_KM,
        RADIUS_34_KNOT_WINDS_NE_KM,
        RADIUS_34_KNOT_WINDS_SE_KM,
        RADIUS_34_KNOT_WINDS_SW_KM,
        RADIUS_34_KNOT_WINDS_NW_KM,
        RADIUS_50_KNOT_WINDS_NE_KM,
        RADIUS_50_KNOT_WINDS_SE_KM,
        RADIUS_50_KNOT_WINDS_SW_KM,
        RADIUS_50_KNOT_WINDS_NW_KM,
        RADIUS_64_KNOT_WINDS_NE_KM,
        RADIUS_64_KNOT_WINDS_SE_KM,
        RADIUS_64_KNOT_WINDS_SW_KM,
        RADIUS_64_KNOT_WINDS_NW_KM,
        ST_ASWKT(WIND_FIELD_POLYGON_34KT) AS WIND_FIELD_POLYGON_34KT,
        ST_ASWKT(WIND_FIELD_POLYGON_50KT) AS WIND_FIELD_POLYGON_50KT,
        ST_ASWKT(WIND_FIELD_POLYGON_64KT) AS WIND_FIELD_POLYGON_64KT
    FROM TC_TRACKS
    WHERE TRACK_ID = %s AND FORECAST_TIME = %s
    ORDER BY ENSEMBLE_MEMBER, LEAD_TIME
    """
    
    df = pd.read_sql(query, conn, params=[track_id, forecast_time])
    # Don't close connection - it's cached and will be reused
    
    return df

def get_envelopes_from_snowflake(track_id, forecast_time):
    """
    Get envelope data from Snowflake TC_ENVELOPES_COMBINED table
    
    Args:
        track_id: Storm identifier (e.g., 'JERRY')
        forecast_time: Forecast time (e.g., '2025-10-10 00:00:00')
    
    Returns:
        pandas.DataFrame: Envelope data with geography polygons
    """
    conn = get_snowflake_connection()
    
    query = """
    SELECT 
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME_RANGE,
        WIND_THRESHOLD,
        ST_ASWKT(ENVELOPE_REGION) AS ENVELOPE_REGION
    FROM TC_ENVELOPES_COMBINED
    WHERE TRACK_ID = %s AND FORECAST_TIME = %s
    ORDER BY ENSEMBLE_MEMBER, WIND_THRESHOLD
    """
    
    df = pd.read_sql(query, conn, params=[track_id, forecast_time])
    # Don't close connection - it's cached and will be reused
    
    return df

def convert_envelopes_to_geodataframe(envelopes_df):
    """
    Convert envelope DataFrame to GeoDataFrame for processing
    
    Args:
        envelopes_df: DataFrame with envelope data from Snowflake
    
    Returns:
        geopandas.GeoDataFrame: Envelopes as GeoDataFrame
    """
    if envelopes_df.empty:
        return gpd.GeoDataFrame()
    
    # Parse WKT polygons
    geometries = []
    for wkt_str in envelopes_df['ENVELOPE_REGION']:
        if pd.notna(wkt_str) and wkt_str:
            try:
                geom = shapely_wkt.loads(wkt_str)
                geometries.append(geom)
            except:
                geometries.append(None)
        else:
            geometries.append(None)
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(envelopes_df, geometry=geometries, crs='EPSG:4326')
    
    # Rename columns to lowercase for consistency with processing functions
    column_mapping = {
        'ENSEMBLE_MEMBER': 'ensemble_member',
        'WIND_THRESHOLD': 'wind_threshold',
        'ENVELOPE_REGION': 'envelope_region'
    }
    gdf = gdf.rename(columns=column_mapping)
    
    # Remove rows with invalid geometries
    gdf = gdf[gdf.geometry.notna()]
    
    return gdf

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
        conn = get_snowflake_connection()
        
        # Query to get distinct wind thresholds for the specific storm and forecast time
        query = """
        SELECT DISTINCT WIND_THRESHOLD 
        FROM TC_ENVELOPES_COMBINED 
        WHERE TRACK_ID = %s 
        AND FORECAST_TIME = %s
        ORDER BY WIND_THRESHOLD
        """
        
        df = pd.read_sql(query, conn, params=[storm, forecast_time])
        # Don't close connection - it's cached and will be reused
        
        if not df.empty:
            # Convert to list of strings and sort
            thresholds = [str(int(th)) for th in df['WIND_THRESHOLD'].tolist()]
            thresholds.sort(key=int)  # Sort numerically
            print(f"Found {len(thresholds)} wind thresholds for {storm} at {forecast_time}: {thresholds}")
            return thresholds
        else:
            # Return empty list if no data found - don't use defaults
            print(f"No wind thresholds found for {storm} at {forecast_time}")
            return []
            
    except Exception as e:
        print(f"Error getting wind thresholds from Snowflake: {str(e)}")
        # Return empty list on error - don't use defaults
        return []

@lru_cache(maxsize=1)
def get_latest_forecast_time_overall():
    """
    Get the latest forecast issue time from Snowflake across all storms
    
    Returns:
        datetime: Latest forecast issue time (when the most recent forecast was issued), or None if no data found
    """
    try:
        conn = get_snowflake_connection()
        
        # Query to get the most recent forecast time across all storms
        query = """
        SELECT MAX(FORECAST_TIME) as MAX_FORECAST_TIME
        FROM TC_TRACKS
        """
        
        df = pd.read_sql(query, conn)
        # Don't close connection - it's cached and will be reused
        
        if not df.empty and pd.notna(df['MAX_FORECAST_TIME'].iloc[0]):
            latest_time = df['MAX_FORECAST_TIME'].iloc[0]
            return latest_time
        else:
            return None
            
    except Exception as e:
        print(f"Error getting latest forecast time from Snowflake: {str(e)}")
        return None

def get_envelope_data_snowflake(track_id, forecast_time):
    """Get envelope data directly from Snowflake"""
    try:
        conn = get_snowflake_connection()
        
        # Get envelope data from TC_ENVELOPES_COMBINED
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
        
        df = pd.read_sql(query, conn, params=[track_id, str(forecast_time)])
        # Don't close connection - reuse it for better performance
        # conn.close()
        
        if not df.empty:
            # Debug: Check what format we're getting
            if len(df) > 0 and 'ENVELOPE_REGION' in df.columns:
                sample_geom = df['ENVELOPE_REGION'].iloc[0]
                if isinstance(sample_geom, str):
                    if sample_geom.strip().startswith('{'):
                        print(f"⚠ Warning: Snowflake returned GeoJSON format instead of WKT for envelopes")
                    elif sample_geom.strip().startswith('POLYGON') or sample_geom.strip().startswith('MULTIPOLYGON'):
                        print(f"✓ Snowflake returned WKT format for envelopes")
                    else:
                        print(f"⚠ Unknown geometry format from Snowflake: starts with '{sample_geom[:20] if len(sample_geom) > 20 else sample_geom}'")
            
            # Rename columns to match expected format
            df = df.rename(columns={'ENVELOPE_REGION': 'geometry', 'WIND_THRESHOLD': 'wind_threshold'})
            return df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        print(f"Error getting envelope data from Snowflake: {str(e)}")
        return pd.DataFrame()
    

def get_active_countries():
    """
    Get active countries from PIPELINE_COUNTRIES table in Snowflake
    
    Returns:
        pandas.DataFrame: DataFrame with columns COUNTRY_CODE, COUNTRY_NAME, CENTER_LAT, CENTER_LON, VIEW_ZOOM, ZOOM_LEVEL
        Returns empty DataFrame on error
    """
    try:
        conn = get_snowflake_connection()
        
        # Get active countries from PIPELINE_COUNTRIES table
        query = '''
        SELECT 
            COUNTRY_CODE,
            COUNTRY_NAME,
            CENTER_LAT,
            CENTER_LON,
            VIEW_ZOOM,
            ZOOM_LEVEL
        FROM PIPELINE_COUNTRIES
        WHERE ACTIVE = TRUE
        ORDER BY COUNTRY_CODE
        '''
        
        df = pd.read_sql(query, conn)
        # Don't close connection - it's cached and will be reused
        
        if not df.empty:
            print(f"✓ Loaded {len(df)} active countries from PIPELINE_COUNTRIES")
        else:
            print("⚠ No active countries found in PIPELINE_COUNTRIES table")
        
        return df
        
    except Exception as e:
        print(f"Error getting active countries from Snowflake: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=['COUNTRY_CODE', 'COUNTRY_NAME', 'CENTER_LAT', 'CENTER_LON', 'VIEW_ZOOM', 'ZOOM_LEVEL'])

def get_lat_lons(row):
    """
    Get latitude and longitude for a hurricane track from Snowflake
    
    Args:
        row: pandas Series or dict with 'TRACK_ID' and 'FORECAST_TIME' keys
    
    Returns:
        pandas.Series: Series with 'latitude' and 'longitude' values
    """
    try:
        conn = get_snowflake_connection()
        
        # Get any available track data at lead time 0
        query = '''
        SELECT LATITUDE, LONGITUDE
        FROM TC_TRACKS
        WHERE TRACK_ID = %s AND FORECAST_TIME = %s 
        AND LEAD_TIME = 0
        LIMIT 1
        '''
        
        df_latlon = pd.read_sql(query, conn, params=[row['TRACK_ID'], str(row['FORECAST_TIME'])])
        # Don't close connection - it's cached and will be reused
        
        if len(df_latlon) > 0:
            lat = df_latlon.iloc[0]['LATITUDE']
            lon = df_latlon.iloc[0]['LONGITUDE']
        else:
            lat = np.nan
            lon = np.nan
            
        return pd.Series([lat, lon], index=["latitude", "longitude"])
            
    except Exception as e:
        print(f"Error getting lat/lon from Snowflake: {str(e)}")
        return pd.Series([np.nan, np.nan], index=["latitude", "longitude"])

def get_snowflake_data():
    """Get hurricane metadata directly from Snowflake"""
    try:
        conn = get_snowflake_connection()
        
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
        
        df = pd.read_sql(query, conn)
        # Don't close connection - it's cached and will be reused
        
        return df
        
    except Exception as e:
        print(f"Error getting Snowflake data: {str(e)}")
        return pd.DataFrame({'TRACK_ID': [], 'FORECAST_TIME': [], 'ENSEMBLE_COUNT': []})


# ---------------------------------------------------------------------------
# Impact data queries — *_MAT tables
# ---------------------------------------------------------------------------

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
        conn = get_snowflake_connection()
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
        df = pd.read_sql(query, conn, params=[country, storm, forecast_date, wind_threshold])
        print(f"✓ Loaded {len(df)} school impact rows from SQL ({country}/{storm}/{forecast_date}/{wind_threshold}kt)")
        return df
    except Exception as e:
        print(f"Error querying SCHOOL_IMPACT_MAT: {str(e)}")
        return pd.DataFrame()


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
        conn = get_snowflake_connection()
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
        df = pd.read_sql(query, conn, params=[country, storm, forecast_date, wind_threshold])
        print(f"✓ Loaded {len(df)} health centre impact rows from SQL ({country}/{storm}/{forecast_date}/{wind_threshold}kt)")
        return df
    except Exception as e:
        print(f"Error querying HC_IMPACT_MAT: {str(e)}")
        return pd.DataFrame()


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
        conn = get_snowflake_connection()
        query = """
        SELECT
            ZONE_ID,
            ADMIN_ID,
            PROBABILITY,
            E_POPULATION,
            E_BUILT_SURFACE_M2,
            E_NUM_SCHOOLS,
            E_SCHOOL_AGE_POPULATION,
            E_INFANT_POPULATION,
            E_NUM_HCS,
            E_RWI,
            E_SMOD_CLASS
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
          AND ZOOM_LEVEL = %s
        """
        df = pd.read_sql(query, conn, params=[country, storm, forecast_date, wind_threshold, zoom_level])
        print(f"✓ Loaded {len(df)} tile impact rows from SQL ({country}/{storm}/{forecast_date}/{wind_threshold}kt zoom={zoom_level})")
        return df
    except Exception as e:
        print(f"Error querying MERCATOR_TILE_IMPACT_MAT: {str(e)}")
        return pd.DataFrame()


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
        conn = get_snowflake_connection()
        query = """
        SELECT
            TILE_ID,
            NAME,
            ADMIN_LEVEL,
            PROBABILITY,
            E_POPULATION,
            E_SCHOOL_AGE_POPULATION,
            E_INFANT_POPULATION,
            E_BUILT_SURFACE_M2,
            E_NUM_SCHOOLS,
            E_NUM_HCS,
            E_SMOD_CLASS,
            E_RWI
        FROM AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
          AND ADMIN_LEVEL = %s
        """
        df = pd.read_sql(query, conn, params=[country, storm, forecast_date, wind_threshold, admin_level])
        print(f"✓ Loaded {len(df)} admin impact rows from SQL ({country}/{storm}/{forecast_date}/{wind_threshold}kt admin_level={admin_level})")
        return df
    except Exception as e:
        print(f"Error querying ADMIN_ALL_IMPACT_MAT: {str(e)}")
        return pd.DataFrame()


def get_admin_cci(country: str, storm: str, forecast_date: str, admin_level: int = 1) -> pd.DataFrame:
    """
    Query ADMIN_ALL_CCI_MAT for Child Cyclone Index data at administrative level.

    No wind_threshold parameter — CCI data is threshold-independent.

    Args:
        country: Country code (e.g. 'JAM')
        storm: Storm identifier (e.g. 'BERYL')
        forecast_date: Forecast date string matching the table (e.g. '2024-07-01 06:00:00')
        admin_level: Administrative level to query (default 1)

    Returns:
        pandas.DataFrame with all CCI columns plus ZONE_ID
    """
    try:
        conn = get_snowflake_connection()
        query = """
        SELECT *
        FROM AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND ADMIN_LEVEL = %s
        """
        df = pd.read_sql(query, conn, params=[country, storm, forecast_date, admin_level])
        print(f"✓ Loaded {len(df)} admin CCI rows from SQL ({country}/{storm}/{forecast_date} admin_level={admin_level})")
        return df
    except Exception as e:
        print(f"Error querying ADMIN_ALL_CCI_MAT: {str(e)}")
        return pd.DataFrame()


def get_available_admin_levels(country: str) -> list:
    """
    Return the admin levels available in ADMIN_ALL_IMPACT_MAT for a given country.

    Args:
        country: Country code (e.g. 'JAM')

    Returns:
        Sorted list of integer admin levels, e.g. [1, 2, 3]
    """
    try:
        conn = get_snowflake_connection()
        query = """
        SELECT DISTINCT ADMIN_LEVEL
        FROM AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT
        WHERE COUNTRY = %s
        ORDER BY 1
        """
        df = pd.read_sql(query, conn, params=[country])
        return df['ADMIN_LEVEL'].tolist()
    except Exception as e:
        print(f"Error querying available admin levels: {str(e)}")


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
        conn = get_snowflake_connection()
        query = """
        SELECT
            ZONE_ID                        AS zone_id,
            WIND_THRESHOLD                 AS wind_threshold,
            SEVERITY_POPULATION            AS severity_population,
            SEVERITY_SCHOOL_AGE_POPULATION AS severity_school_age_population,
            SEVERITY_INFANT_POPULATION     AS severity_infant_population,
            SEVERITY_SCHOOLS               AS severity_schools,
            SEVERITY_HCS                   AS severity_hcs,
            SEVERITY_BUILT_SURFACE_M2      AS severity_built_surface_m2,
            GEOMETRY
        FROM AOTS.TC_ECMWF.TRACK_MAT
        WHERE COUNTRY = %s
          AND STORM = %s
          AND FORECAST_DATE = %s
          AND WIND_THRESHOLD = %s
        ORDER BY ZONE_ID
        """
        df = pd.read_sql(query, conn, params=[country, storm, forecast_date, wind_threshold])

        def _parse_wkb(g):
            if g is None:
                return None
            # Snowflake returns VARIANT binary as a JSON-quoted hex string
            hex_str = g.strip('"') if isinstance(g, str) else g.hex()
            return shapely_wkb.loads(bytes.fromhex(hex_str))

        df['geometry'] = df['GEOMETRY'].apply(_parse_wkb)
        df = df.drop(columns=['GEOMETRY'])
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
        print(f"✓ Loaded {len(gdf)} track rows from SQL ({country}/{storm}/{forecast_date}/{wind_threshold}kt)")
        return gdf
    except Exception as e:
        print(f"Error querying TRACK_MAT: {str(e)}")
        return gpd.GeoDataFrame()
        return []