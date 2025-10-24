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
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
    
    # Azure Blob Storage Configuration
    ACCOUNT_URL = os.getenv('ACCOUNT_URL')
    SAS_TOKEN = os.getenv('SAS_TOKEN')
    DATA_PIPELINE_DB = os.getenv('DATA_PIPELINE_DB', 'LOCAL')
    
    # Application Configuration
    RESULTS_DIR = os.getenv('RESULTS_DIR')
    BBOX_FILE = os.getenv('BBOX_FILE')
    STORMS_FILE = os.getenv('STORMS_FILE')
    VIEWS_DIR = os.getenv('VIEWS_DIR')
    ROOT_DATA_DIR = os.getenv('ROOT_DATA_DIR')
    
    # Mapbox Configuration
    MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')
    
    @classmethod
    def validate_snowflake_config(cls):
        """Validate that all required Snowflake configuration is present"""
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
        if cls.DATA_PIPELINE_DB in ['BLOB', 'RO_BLOB']:
            required_vars = ['ACCOUNT_URL', 'SAS_TOKEN']
            missing = [var for var in required_vars if not getattr(cls, var)]
            if missing:
                raise ValueError(f"Missing Azure environment variables: {', '.join(missing)}")

# Create a global config instance
config = Config()
