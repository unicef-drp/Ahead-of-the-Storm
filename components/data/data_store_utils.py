#!/usr/bin/env python3
"""
Data Store Utilities Module

This module provides centralized data store management utilities for the Ahead of the Storm application.
It consolidates data store initialization logic and provides a single source of truth for data store configuration.

Key Components:
- Centralized data store initialization based on environment variables
- Consistent data store configuration across the application
- Support for LocalDataStore (local filesystem) and ADLSDataStore (Azure Blob Storage)

Usage:
    from data_store_utils import get_data_store
    data_store = get_data_store()
"""

# Import GigaSpatial components
from gigaspatial.core.io.adls_data_store import ADLSDataStore
from gigaspatial.core.io.local_data_store import LocalDataStore

# Import centralized configuration
from components.config import config as app_config

def get_data_store():
    """
    Get the appropriate data store based on centralized configuration.
    
    This controls where pre-processed impact views are stored:
    - LOCAL: Local filesystem (default)
    - BLOB: Azure Blob Storage (read-only, this app only reads data)
    
    Note: Snowflake is a separate data source for raw hurricane forecast data.
    
    Returns:
        DataStore: Configured data store instance
    """
    impact_data_store = app_config.IMPACT_DATA_STORE
    
    if impact_data_store == 'BLOB':
        return ADLSDataStore()
    else:
        # Default to local storage
        return LocalDataStore()
