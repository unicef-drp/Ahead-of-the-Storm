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
import geopandas as gpd
import snowflake.connector
from shapely import wkt as shapely_wkt
import warnings

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Import centralized configuration
from components.config import config

def get_snowflake_connection():
    """Create Snowflake connection from centralized configuration."""
    config.validate_snowflake_config()
    
    conn = snowflake.connector.connect(
        account=config.SNOWFLAKE_ACCOUNT,
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA
    )
    return conn

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
    conn.close()
    
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
    conn.close()
    
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
        conn.close()
        
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
        conn.close()
        
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
        query = '''
        SELECT 
            ENSEMBLE_MEMBER,
            WIND_THRESHOLD,
            ENVELOPE_REGION
        FROM TC_ENVELOPES_COMBINED
        WHERE TRACK_ID = %s AND FORECAST_TIME = %s
        ORDER BY ENSEMBLE_MEMBER, WIND_THRESHOLD
        '''
        
        df = pd.read_sql(query, conn, params=[track_id, str(forecast_time)])
        conn.close()
        
        if not df.empty:
            # Rename columns to match expected format
            df = df.rename(columns={'ENVELOPE_REGION': 'geometry', 'WIND_THRESHOLD': 'wind_threshold'})
            return df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        print(f"Error getting envelope data from Snowflake: {str(e)}")
        return pd.DataFrame()
    

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
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"Error getting Snowflake data: {str(e)}")
        return pd.DataFrame({'TRACK_ID': [], 'FORECAST_TIME': [], 'ENSEMBLE_COUNT': []})