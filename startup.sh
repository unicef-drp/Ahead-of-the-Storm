#!/bin/bash
# Azure App Service startup script for Dash application
# This runs gunicorn to serve the Dash app in production

# Calculate optimal worker count
# For Basic B3: 4 vCPU, 7GB RAM
# Formula: (2 * CPU) + 1 is a good starting point, but with 7GB RAM we can be more conservative
# With 3 instances, we want ~2-3 workers per instance to avoid memory pressure
CPU_COUNT=${WEB_CONCURRENCY:-$(python -c "import os; print(os.cpu_count() or 4)")}
# For Basic B3 (4 vCPU): use 4-6 workers to balance CPU and memory
# Each worker can use ~500MB-1GB memory, so 4-6 workers fits within 7GB nicely
WORKERS=$((CPU_COUNT + 2))  # 4 vCPU + 2 = 6 workers
# Cap at 6 for Basic B3 to ensure enough memory per worker (~1GB each)
WORKERS=$((WORKERS > 6 ? 6 : WORKERS))
# Minimum of 2 workers
WORKERS=$((WORKERS < 2 ? 2 : WORKERS))

# Use dynamic port if provided by Azure (PORT env var), otherwise default to 8000
PORT=${PORT:-8000}

echo "Starting Gunicorn with $WORKERS workers on port $PORT"

gunicorn \
    --bind 0.0.0.0:$PORT \
    --workers $WORKERS \
    --worker-class sync \
    --timeout 120 \
    --keep-alive 5 \
    --max-requests 1000 \
    --max-requests-jitter 100 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    --preload \
    app:server

