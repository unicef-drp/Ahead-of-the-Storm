# Ahead-of-the-Storm — SPCS (Snowflake Container Services) deployment image
#
# Build (run from project root):
#   docker build -t unicef-dash-app:latest . --platform=linux/amd64
#
# Tag & push to Snowflake image registry:
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
    libgl1 \
    nginx \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (separate layer — only invalidated when
# requirements change, not on every code change).
COPY requirements.spcs.txt .
RUN pip install --upgrade pip && pip install -r requirements.spcs.txt

# Pre-build matplotlib's fontManager cache into the image at build time
# instead of paying it on every fresh container's first PDF/impact-report
# request (pages/report.py's own _generate_map_image, the only real
# matplotlib call site — measured live at ~8-8.6s of one-time cost per
# process, real user-facing latency, not just a startup-log line). MPLCONFIGDIR
# fixed to a real path (rather than relying on $HOME, which this root-only
# image never sets explicitly) so the cache built here is guaranteed to be
# the SAME directory matplotlib looks in at runtime.
ENV MPLCONFIGDIR=/app/.matplotlib
RUN mkdir -p /app/.matplotlib && \
    python -c "import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot"

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
    TILE_PORT=8001 \
    TILE_WORKERS=1 \
    WEB_CONCURRENCY=1 \
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

# ── Startup ───────────────────────────────────────────────────────────────────
# entrypoint.sh starts three processes in the same container:
#   1. nginx on 0.0.0.0:PORT (public — reverse proxy + tile cache)
#   2. uvicorn tile_server on 127.0.0.1:TILE_PORT (loopback only)
#   3. gunicorn Dash app on 127.0.0.1:8050 (loopback only)
#
# Both app processes share the same SPCS OAuth token file and env vars.
# Single gunicorn worker (1 process, 8 threads):
#   - Avoids fork-safety issues with snowflake-connector-python native extensions
#   - Eliminates Dash callback-map race condition on first request
# Single uvicorn worker for tile server (TILE_WORKERS=1 default, overridable in service spec):
#   NOTE: tile_server.py has no __main__ block — always start via uvicorn, never python3 directly.
RUN chmod +x /app/entrypoint.sh
CMD ["/app/entrypoint.sh"]
