#!/bin/bash
# Azure App Service startup script for Dash application
# This runs gunicorn to serve the Dash app in production

gunicorn --bind 0.0.0.0:8000 --workers 4 --timeout 120 app:server

