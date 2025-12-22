# Dockerfile for UNICEF Ahead-of-the-Storm DASK Application
# Supports both standard password authentication and SPCS OAuth token authentication

FROM python:3.11-slim

LABEL maintainer="UNICEF"
LABEL description="Ahead-of-the-Storm Hurricane Impact Visualization Dashboard"
LABEL version="1.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Create datastore mount point for persistent data
# This will hold geodb, project_results, and other persistent data
RUN mkdir -p /datastore && \
    chmod 755 /datastore

# Define volume mount point
VOLUME ["/datastore"]

# Copy requirements first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application code
COPY . .

# Copy startup script and make it executable
COPY startup.sh .
RUN chmod +x startup.sh

# Create directories for runtime data
RUN mkdir -p /app/assets /app/components /app/pages && \
    chmod -R 755 /app

# Expose port for web application
EXPOSE 8000

# Environment variables with defaults
# Application Configuration
ENV PORT=8000 \
    WEB_CONCURRENCY=4

# Snowflake Configuration (override at runtime)
# Standard authentication (password-based)
ENV SNOWFLAKE_ACCOUNT="" \
    SNOWFLAKE_USER="" \
    SNOWFLAKE_PASSWORD="" \
    SNOWFLAKE_WAREHOUSE="" \
    SNOWFLAKE_DATABASE="" \
    SNOWFLAKE_SCHEMA=""

# SPCS OAuth Configuration (optional, for Snowflake Container Services)
# Set SPCS_RUN=true to enable SPCS OAuth authentication
ENV SPCS_RUN="false" \
    SPCS_TOKEN_PATH="/snowflake/session/token" \
    SNOWFLAKE_HOST="" \
    SNOWFLAKE_PORT=""

# Data Storage Configuration
# Point to datastore mount point
ENV ROOT_DATA_DIR="/datastore/geodb" \
    RESULTS_DIR="/datastore/project_results/climate/lacro_project" \
    VIEWS_DIR="aos_views" \
    BBOX_FILE="bbox.parquet" \
    STORMS_FILE="storms.json"

# Impact Data Storage Configuration (optional)
# Controls where pre-processed impact views are stored (LOCAL, BLOB, or SNOWFLAKE)
# Note: Snowflake can be used for BOTH raw hurricane forecast data (tables) AND impact views (stages)
ENV IMPACT_DATA_STORE="LOCAL" \
    ADLS_ACCOUNT_URL="" \
    ADLS_SAS_TOKEN="" \
    ADLS_CONTAINER_NAME="giga" \
    SNOWFLAKE_STAGE_NAME=""

# Mapbox Configuration (optional)
ENV MAPBOX_ACCESS_TOKEN=""

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/ || exit 1

# Default command: run startup.sh which starts gunicorn
CMD ["./startup.sh"]

# Alternative direct command (uncomment to use instead of startup.sh):
# CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "4", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "app:server"]

