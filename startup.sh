#!/bin/bash
# Azure App Service startup script for Dash application
# This runs gunicorn to serve the Dash app in production

# Use configuration file if it exists, otherwise use command-line args
if [ -f "gunicorn.conf.py" ]; then
    echo "Using gunicorn.conf.py for configuration"
    gunicorn --config gunicorn.conf.py app:server
else
    # Fallback: command-line configuration
    # Optimize for Azure App Service:
    # - Fewer workers (2) = faster startup, less memory
    # - Longer timeout for data operations
    # - Threads for I/O-bound operations (Snowflake queries)
    WORKERS=${WEBAPP_WORKERS:-2}
    
    echo "Starting with $WORKERS workers"
    gunicorn --bind 0.0.0.0:8000 \
             --workers $WORKERS \
             --threads 2 \
             --timeout 300 \
             --keep-alive 65 \
             --worker-class sync \
             --worker-connections 1000 \
             --max-requests 1000 \
             --max-requests-jitter 50 \
             --preload \
             --access-logfile - \
             --error-logfile - \
             app:server
fi

