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