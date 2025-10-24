#!/usr/bin/env python3
"""
Data Store Utilities Module

This module provides centralized data store management utilities for the Ahead of the Storm application.
It consolidates data store initialization logic and provides a single source of truth for data store configuration.

Key Components:
- ADLSDataStoreRO class for read-only Azure Blob Storage access
- Centralized data store initialization based on environment variables
- Consistent data store configuration across the application

Usage:
    from data_store_utils import get_data_store
    data_store = get_data_store()
"""

import os
from azure.storage.blob import BlobServiceClient

# Import GigaSpatial components
from gigaspatial.core.io.adls_data_store import ADLSDataStore
from gigaspatial.core.io.local_data_store import LocalDataStore
from gigaspatial.config import config

# Import centralized configuration
from components.config import config as app_config

class ADLSDataStoreRO(ADLSDataStore):
    """
    Read-only Azure Data Lake Storage Data Store
    
    This class provides read-only access to Azure Blob Storage using SAS tokens.
    It's used when you need read-only access to data without write permissions.
    """
    
    def __init__(self, account_url: str, sas_token: str, container: str = config.ADLS_CONTAINER_NAME):
        """
        Initialize read-only ADLS data store
        
        Args:
            account_url: Azure Storage account URL
            sas_token: SAS token for authentication
            container: Container name (defaults to config.ADLS_CONTAINER_NAME)
        """
        self.blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
        self.container_client = self.blob_service_client.get_container_client(container=container)
        self.container = container

def get_data_store():
    """
    Get the appropriate data store based on centralized configuration
    
    Returns:
        DataStore: Configured data store instance
    """
    data_pipeline_db = app_config.DATA_PIPELINE_DB
    
    if data_pipeline_db == 'BLOB':
        return ADLSDataStore()
    elif data_pipeline_db == "RO_BLOB":
        app_config.validate_azure_config()
        return ADLSDataStoreRO(app_config.ACCOUNT_URL, app_config.SAS_TOKEN)
    elif data_pipeline_db == 'LOCAL':
        return LocalDataStore()
    else:
        # Default to local storage
        return LocalDataStore()
