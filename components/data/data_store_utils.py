#!/usr/bin/env python3
"""
Data Store Utilities Module

This module provides centralized data store management utilities for the Ahead of the Storm application.
It consolidates data store initialization logic and provides a single source of truth for data store configuration.

Key Components:
- Centralized data store initialization based on environment variables
- Consistent data store configuration across the application
- Support for LocalDataStore, ADLSDataStore, and SnowflakeDataStore

Usage:
    from data_store_utils import get_data_store
    data_store = get_data_store()
"""

# Import GigaSpatial components
from gigaspatial.core.io.adls_data_store import ADLSDataStore
from gigaspatial.core.io.local_data_store import LocalDataStore
from gigaspatial.core.io.snowflake_data_store import SnowflakeDataStore
from gigaspatial.core.io.readers import read_dataset

# Import centralized configuration
from components.config import config as app_config

def get_data_store():
    """
    Get the appropriate data store based on centralized configuration.
    
    This controls where pre-processed impact views are stored:
    - LOCAL: Local filesystem (default)
    - BLOB: Azure Blob Storage (read-only, this app only reads data)
    - SNOWFLAKE: Snowflake internal stage (read-only, this app only reads data)
    
    Note: Snowflake can be used for BOTH raw hurricane forecast data (tables) AND impact views (stages).
    
    Returns:
        DataStore: Configured data store instance
    """
    impact_data_store = app_config.IMPACT_DATA_STORE
    
    if impact_data_store == 'BLOB':
        return ADLSDataStore()
    elif impact_data_store == 'SNOWFLAKE':
        if SnowflakeDataStore is None:
            raise ImportError(
                "SnowflakeDataStore not available. Please ensure giga-spatial>=0.7.0 is installed "
                "and includes the SnowflakeDataStore class."
            )
        
        # Note: SnowflakeDataStore uses standard password authentication
        # SPCS OAuth is not currently supported for Snowflake stages
        # Use password authentication (SPCS_RUN=false) for Snowflake stage access
        return SnowflakeDataStore(
            account=app_config.SNOWFLAKE_ACCOUNT,
            user=app_config.SNOWFLAKE_USER,
            password=app_config.SNOWFLAKE_PASSWORD,
            warehouse=app_config.SNOWFLAKE_WAREHOUSE,
            database=app_config.SNOWFLAKE_DATABASE,
            schema=app_config.SNOWFLAKE_SCHEMA,
            stage_name=app_config.SNOWFLAKE_STAGE_NAME
        )
    else:
        # Default to local storage
        return LocalDataStore()


def get_impact_data(data_type: str, giga_store, filepath: str, **sql_params):
    """
    Load impact data via SQL (MAT tables) or file download (stage), controlled by
    the IMPACT_DATA_SOURCE env var.

    IMPORTANT: IMPACT_DATA_SOURCE=SQL connects directly to Snowflake MAT tables and
    bypasses the file store (giga_store) entirely. It works regardless of IMPACT_DATA_STORE
    (LOCAL/BLOB/SNOWFLAKE), as long as Snowflake credentials are configured — which the app
    always requires for TC_TRACKS and PIPELINE_COUNTRIES anyway.

    Args:
        data_type: One of 'school', 'hc', 'shelter', 'wash', 'tile', 'admin_impact', 'admin_cci', 'tile_cci', 'track'.
        giga_store: Configured data store instance (used for STAGE path only).
        filepath: Path to the file on the data store (used for STAGE path only).
        **sql_params: Keyword args passed to the SQL function when IMPACT_DATA_SOURCE='SQL'.
                      Expected keys: country, storm, forecast_date, wind_threshold,
                      zoom_level (tile only), admin_level (admin only).

    Returns:
        pandas.DataFrame (or GeoDataFrame for data_type='track')
    """
    import time
    _t0 = time.perf_counter()

    def _norm(col):
        """Normalize column names: keep E_ prefix uppercase, lowercase everything else."""
        if col.upper().startswith('E_'):
            return 'E_' + col[2:].lower()
        return col.lower()

    if app_config.IMPACT_DATA_SOURCE == 'SQL':
        from components.data.snowflake_utils import (
            get_school_impacts,
            get_hc_impacts,
            get_shelter_impacts,
            get_wash_impacts,
            get_tile_impacts,
            get_admin_impacts,
            get_admin_cci,
            get_tile_cci,
            get_track_impacts,
        )

        country = sql_params['country']
        storm = sql_params['storm']
        forecast_date = sql_params['forecast_date']

        if data_type == 'school':
            result = get_school_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'hc':
            result = get_hc_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'shelter':
            result = get_shelter_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'wash':
            result = get_wash_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'tile':
            result = get_tile_impacts(country, storm, forecast_date, sql_params['wind_threshold'],
                                      sql_params.get('zoom_level', 14))
        elif data_type == 'admin_impact':
            result = get_admin_impacts(country, storm, forecast_date, sql_params['wind_threshold'],
                                       sql_params.get('admin_level', 1))
        elif data_type == 'admin_cci':
            result = get_admin_cci(country, storm, forecast_date, sql_params.get('admin_level', 1))
        elif data_type == 'tile_cci':
            result = get_tile_cci(country, storm, forecast_date, sql_params.get('zoom_level', 14))
        elif data_type == 'track':
            result = get_track_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        else:
            raise ValueError(f"Unknown data_type '{data_type}' for SQL path")
        if not result.empty:
            # Normalize: keep E_ prefix uppercase, lowercase everything else
            # Matches the convention in STAGE Parquet/CSV files (e.g. E_population, zone_id)
            result.columns = [_norm(c) for c in result.columns]
        source_label = f"SQL/{data_type}"
    else:
        # STAGE path — original behaviour
        result = read_dataset(giga_store, filepath)
        source_label = f"STAGE/{filepath}"
        # CCI stage files use mixed-case column names (CCI_children, E_CCI_children).
        # Normalize them to match the SQL path convention (cci_children, E_cci_children).
        if data_type in ('tile_cci', 'admin_cci') and not result.empty:
            result.columns = [_norm(c) for c in result.columns]

    _elapsed = time.perf_counter() - _t0
    print(f"[perf] {source_label} → {len(result)} rows in {_elapsed:.2f}s")
    return result