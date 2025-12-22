#!/usr/bin/env python3
"""
Configuration Module

This module provides centralized configuration management for the Ahead of the Storm application.
It handles environment variable loading and provides a single source of truth for configuration.

Key Components:
- Centralized environment variable loading
- Configuration validation
- Default value management
- Environment-specific settings

Usage:
    from config import config
    snowflake_account = config.SNOWFLAKE_ACCOUNT
"""

import os
import math
from dotenv import load_dotenv

# Load environment variables from the project root
# This assumes the .env file is in the project root directory
load_dotenv()

class Config:
    """Centralized configuration class"""
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
    
    # SPCS (Snowflake Container Services) OAuth Configuration
    SPCS_RUN = os.getenv('SPCS_RUN', 'false').lower() == 'true'
    SPCS_TOKEN_PATH = os.getenv('SPCS_TOKEN_PATH', '/snowflake/session/token')
    SNOWFLAKE_HOST = os.getenv('SNOWFLAKE_HOST')
    SNOWFLAKE_PORT = os.getenv('SNOWFLAKE_PORT')
    
    # Impact Data Storage Configuration
    # Controls where pre-processed impact views are stored (LOCAL or BLOB)
    # Note: Snowflake is a separate data source for raw hurricane forecast data
    # ADLS variables are read directly by giga-spatial's ADLSDataStore
    ADLS_ACCOUNT_URL = os.getenv('ADLS_ACCOUNT_URL')
    ADLS_SAS_TOKEN = os.getenv('ADLS_SAS_TOKEN')
    ADLS_CONTAINER_NAME = os.getenv('ADLS_CONTAINER_NAME')
    IMPACT_DATA_STORE = os.getenv('IMPACT_DATA_STORE', 'LOCAL')
    
    # Application Configuration
    RESULTS_DIR = os.getenv('RESULTS_DIR')
    BBOX_FILE = os.getenv('BBOX_FILE')
    STORMS_FILE = os.getenv('STORMS_FILE')
    VIEWS_DIR = os.getenv('VIEWS_DIR')
    ROOT_DATA_DIR = os.getenv('ROOT_DATA_DIR')
    
    # Mapbox Configuration
    MAPBOX_ACCESS_TOKEN = os.getenv('MAPBOX_ACCESS_TOKEN')

    CCI_COL = 'CCI_children' 
    E_CCI_COL = 'E_CCI_children' 
    
    CHANGES_POP = [(100,500),(500,1000),(1000,math.inf)]
    CHANGES_FACILITIES = [(3,10),(10,20),(20,math.inf)]
    
    @classmethod
    def validate_snowflake_config(cls):
        """Validate that all required Snowflake configuration is present"""
        # Check for SPCS mode
        if cls.SPCS_RUN:
            # SPCS OAuth mode: validate token file exists
            from pathlib import Path
            token_file = Path(cls.SPCS_TOKEN_PATH)
            if not token_file.exists():
                raise ValueError(f"SPCS token file not found: {cls.SPCS_TOKEN_PATH}")
            if not token_file.is_file():
                raise ValueError(f"SPCS token path is not a file: {cls.SPCS_TOKEN_PATH}")
            
            # SPCS mode requires: ACCOUNT, WAREHOUSE, DATABASE, SCHEMA, HOST, PORT
            required_vars = [
                'SNOWFLAKE_ACCOUNT',
                'SNOWFLAKE_WAREHOUSE',
                'SNOWFLAKE_DATABASE',
                'SNOWFLAKE_SCHEMA',
                'SNOWFLAKE_HOST',
                'SNOWFLAKE_PORT'
            ]
        else:
            # Non-SPCS mode requires: ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA
            required_vars = [
                'SNOWFLAKE_ACCOUNT',
                'SNOWFLAKE_USER', 
                'SNOWFLAKE_PASSWORD',
                'SNOWFLAKE_WAREHOUSE',
                'SNOWFLAKE_DATABASE',
                'SNOWFLAKE_SCHEMA'
            ]
        
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_azure_config(cls):
        """Validate that all required Azure configuration is present"""
        if cls.IMPACT_DATA_STORE == 'BLOB':
            required_vars = ['ADLS_ACCOUNT_URL', 'ADLS_SAS_TOKEN', 'ADLS_CONTAINER_NAME']
            missing = [var for var in required_vars if not getattr(cls, var)]
            if missing:
                raise ValueError(f"Missing Azure environment variables: {', '.join(missing)}")

# Create a global config instance
config = Config()
