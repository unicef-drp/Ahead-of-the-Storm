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
from dotenv import load_dotenv

# Load environment variables from the project root
# This assumes the .env file is in the project root directory
load_dotenv()

class Config:
    """Centralized configuration class"""
    
    # Snowflake Configuration
    # SNOWFLAKE_USE_RO=true swaps in SNOWFLAKE_RO_USER/SNOWFLAKE_RO_PASSWORD
    # (a read-only service account, e.g. AOTS_AZURE_SERVICE) in place of the
    # normal SNOWFLAKE_USER/SNOWFLAKE_PASSWORD, used for locally sanity-checking
    # a read-only credential before it's used in a real deployment, without
    # touching the primary credentials. Unset/false: only
    # SNOWFLAKE_USER/SNOWFLAKE_PASSWORD are read.
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    _USE_RO = os.getenv('SNOWFLAKE_USE_RO', 'false').lower() == 'true'
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_RO_USER') if _USE_RO else os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_RO_PASSWORD') if _USE_RO else os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
    
    # SPCS (Snowflake Container Services) OAuth Configuration
    SPCS_RUN = os.getenv('SPCS_RUN', 'false').lower() == 'true'
    SPCS_TOKEN_PATH = os.getenv('SPCS_TOKEN_PATH', '/snowflake/session/token')
    SNOWFLAKE_HOST = os.getenv('SNOWFLAKE_HOST')
    SNOWFLAKE_PORT = os.getenv('SNOWFLAKE_PORT')
    
    # Impact Data Storage Configuration
    # IMPACT_DATA_STORE selects how the tile server loads impact data: SNOWFLAKE
    # queries the *_MAT tables, LOCAL and BLOB read the *_views/ files from the
    # local filesystem or Azure Blob. Raw global river and precip files are read
    # from the Snowflake stage or Blob.
    # ADLS variables are read directly by giga-spatial's ADLSDataStore
    ADLS_ACCOUNT_URL = os.getenv('ADLS_ACCOUNT_URL')
    ADLS_SAS_TOKEN = os.getenv('ADLS_SAS_TOKEN')
    ADLS_CONTAINER_NAME = os.getenv('ADLS_CONTAINER_NAME')
    # Snowflake stage name holding the raw river/precip files (only needed if IMPACT_DATA_STORE='SNOWFLAKE')
    SNOWFLAKE_STAGE_NAME = os.getenv('SNOWFLAKE_STAGE_NAME')
    IMPACT_DATA_STORE = os.getenv('IMPACT_DATA_STORE', 'LOCAL')
    
    # Application Configuration
    VIEWS_DIR = os.getenv('VIEWS_DIR')
    ROOT_DATA_DIR = os.getenv('ROOT_DATA_DIR')
    
    # Tile sidecar URL (FastAPI server serving raster/vector tiles, stats, and preload endpoints)
    TILE_SERVER_URL = os.getenv('TILE_SERVER_URL', 'http://localhost:8001')

    # Whether nginx is fronting this process (entrypoint.sh sets this: true for
    # BOTH SPCS and Azure Web App for Containers, since both run the exact same
    # Docker image/entrypoint.sh with nginx proxying Dash+tile-server on one
    # public port; false for local `python app.py` dev, which has no nginx and
    # talks to the tile server directly on TILE_SERVER_URL). Deliberately NOT
    # the same flag as SPCS_RUN, which only selects Snowflake auth mode; Azure
    # has SPCS_RUN=false but IS behind nginx just like SPCS, so a value keyed
    # on SPCS_RUN alone cannot tell local dev and Azure apart correctly for
    # either setting. Browser-facing tile_server_url fields must key on THIS,
    # not on SPCS_RUN (see map_shell_concept.py).
    BEHIND_REVERSE_PROXY = os.getenv('BEHIND_REVERSE_PROXY', 'false').lower() == 'true'

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

# Create a global config instance
config = Config()
