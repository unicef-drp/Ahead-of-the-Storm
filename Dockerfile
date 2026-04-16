# Ahead-of-the-Storm — SPCS (Snowflake Container Services) deployment image
#
# Build (run from project root):
#   docker build -t unicef-dash-app:latest . --platform=linux/amd64
#
# Tag & push to Snowflake registry (see snowflake_spcs/03_build_and_push.sh):
#   docker tag unicef-dash-app:latest <registry>/<repo>/unicef-dash-app:latest
#   docker push <registry>/<repo>/unicef-dash-app:latest

FROM python:3.11-slim

LABEL maintainer="UNICEF"
LABEL description="Ahead-of-the-Storm – Hurricane Impacts"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Runtime system deps only.
# Geospatial wheels (geopandas, shapely, pyproj) bundle their own GEOS/PROJ/GDAL —
# no build-essential or *-dev headers needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (separate layer — only invalidated when
# requirements change, not on every code change).
COPY requirements.spcs.txt .
RUN pip install --upgrade pip && pip install -r requirements.spcs.txt

# Copy application code
COPY . .

# Snowflake internal stage is mounted here by the SPCS service spec
RUN mkdir -p /DataStore && chmod 755 /DataStore
VOLUME ["/DataStore"]

EXPOSE 8000

# ── SPCS defaults ─────────────────────────────────────────────────────────────
# All variables below can be overridden in the SPCS service spec `env:` block.
# SPCS_RUN=true enables OAuth token auth via /snowflake/session/token.
# IMPACT_DATA_SOURCE=SQL queries MAT tables directly — no stage file downloads.
ENV PORT=8000 \
    WEB_CONCURRENCY=2 \
    SPCS_RUN=true \
    SPCS_TOKEN_PATH=/snowflake/session/token \
    SNOWFLAKE_ACCOUNT="" \
    SNOWFLAKE_HOST="" \
    SNOWFLAKE_PORT="" \
    SNOWFLAKE_WAREHOUSE="" \
    SNOWFLAKE_DATABASE="" \
    SNOWFLAKE_SCHEMA="" \
    IMPACT_DATA_SOURCE=SQL \
    IMPACT_DATA_STORE=SNOWFLAKE \
    SNOWFLAKE_STAGE_NAME="" \
    ROOT_DATA_DIR=geodb \
    RESULTS_DIR=results \
    VIEWS_DIR=aos_views \
    MAPBOX_ACCESS_TOKEN=""

# Allow extra time for Snowflake warehouse resume on first request after idle period
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD curl -f http://localhost:${PORT}/ || exit 1

# ── Gunicorn (SPCS-tuned) ─────────────────────────────────────────────────────
# CPU_X64_XS: 2 vCPU, 8 GB RAM
# Worker class: gthread — Snowflake queries are I/O-bound; threads allow other
#   requests to proceed while one thread waits on a query response.
#   2 workers × 4 threads = 8 concurrent requests on 2 vCPU.
# Memory: ~1.4 GB per worker (libraries + lru_cache) × 2 = ~2.8 GB used,
#   leaving ~5 GB headroom on 8 GB.
# max-requests 5000: workers restart periodically to prevent memory creep, but
#   infrequently enough that the 3-query startup cost amortises well.
# Override WEB_CONCURRENCY in the service spec env block if needed.
CMD gunicorn \
    --bind 0.0.0.0:${PORT:-8000} \
    --workers ${WEB_CONCURRENCY:-2} \
    --worker-class gthread \
    --threads 4 \
    --timeout 300 \
    --keep-alive 5 \
    --max-requests 5000 \
    --max-requests-jitter 500 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:server
