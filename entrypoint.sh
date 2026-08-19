#!/bin/bash
# entrypoint.sh — orchestrates three processes inside the SPCS container:
#
#   nginx        0.0.0.0:8000  (public — proxy + tile cache)
#     └─► Dash   127.0.0.1:8050  (gunicorn, 1 worker, 8 threads)
#     └─► Tiles  127.0.0.1:8001  (uvicorn, 1 async worker)
#
# Horizontal scaling is handled by the SPCS compute pool (more container
# instances), not more workers per container. One process per service avoids
# snowflake-connector fork-safety issues and lets threads share the LRU cache.
#
# nginx provides:
#   - Tile response caching (5-min TTL, cross-request, survives Dash restarts)
#   - Single public port (8000) for both Dash and tile endpoints
#   - proxy_cache_lock: one upstream render per cache miss, no thundering herd

set -e

PORT=${PORT:-8000}
DASH_PORT=${DASH_PORT:-8050}
TILE_PORT=${TILE_PORT:-8001}

# ── 1. nginx ──────────────────────────────────────────────────────────────────
mkdir -p /var/cache/nginx/aots_tiles /var/log/nginx /run/nginx
# Inject runtime ports into the nginx config
sed -e "s/__NGINX_PORT__/${PORT}/g" \
    -e "s/__DASH_PORT__/${DASH_PORT}/g" \
    -e "s/__TILE_PORT__/${TILE_PORT}/g" \
    /app/nginx.spcs.conf > /tmp/nginx.conf

nginx -c /tmp/nginx.conf
echo "[entrypoint] nginx started on port ${PORT}"

# ── 2. Tile server ────────────────────────────────────────────────────────────
echo "[entrypoint] Starting tile server on 127.0.0.1:${TILE_PORT}..."
uvicorn services.tile_server:app \
    --host 127.0.0.1 \
    --port "${TILE_PORT}" \
    --workers "${TILE_WORKERS:-1}" \
    --log-level info \
    &
TILE_PID=$!
echo "[entrypoint] Tile server PID: ${TILE_PID}"

# Wait for tile server — exit hard if it never becomes healthy
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${TILE_PORT}/health" > /dev/null 2>&1; then
        echo "[entrypoint] Tile server ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "[entrypoint] ERROR: Tile server did not become healthy after 30s — aborting." >&2
        exit 1
    fi
    sleep 1
done

# ── 3. Dash app (foreground — container exits when this exits) ────────────────
echo "[entrypoint] Starting Dash app on 127.0.0.1:${DASH_PORT}..."
# nginx is always in front of us here (step 1, both under SPCS and Azure Web
# App for Containers — same image, same entrypoint.sh) — tell the app so it
# serves browser-facing tile URLs relative/same-origin instead of an absolute
# http://localhost:8001 that would resolve against the viewer's own machine.
# See components/config.py's BEHIND_REVERSE_PROXY for the full rationale.
export BEHIND_REVERSE_PROXY=true
exec gunicorn \
    --bind "127.0.0.1:${DASH_PORT}" \
    --workers 1 \
    --worker-class gthread \
    --threads 8 \
    --timeout 300 \
    --keep-alive 5 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:server
