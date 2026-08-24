# AoTS Tile Server

FastAPI sidecar that serves Mapbox Vector Tiles (MVT/PBF) from Snowflake for
the Ahead of the Storm dashboard.

## Start

```bash
pip install -r services/requirements.txt
uvicorn services.tile_server:app --host 0.0.0.0 --port 8001 --reload
```

Credentials are read from the project `.env` (same file used by the Dash app).

## Endpoints

28 routes total (see `services/tile_server.py`'s own `@app.get` decorators for the exact/current list);
grouped by purpose:

```
GET /health
GET /tiles/raster/{country}/{storm}/{forecast_date}/{prop}/{z}/{x}/{y}.webp   -- WebP raster tiles
GET /tiles/mercator/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf         -- MVT vector, mercator grid
GET /tiles/admin/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf           -- MVT vector, admin regions
GET /tiles/raster-combined/{country}/{storm}/{mode}/{z}/{x}/{y}.webp         -- combined-hazard raster
GET /tiles/admin-combined/{country}/{storm}/{mode}/{z}/{x}/{y}.pbf          -- combined-hazard admin MVT
GET /tiles/raster/river-raw/... and /tiles/raster/precip-raw/...            -- global raw hazard rasters
GET /geojson/facilities/{layer_type}/{country}/{storm}/{forecast_date}      -- schools/health/shelters/WASH
GET /geojson/facilities-combined/{layer_type}/{country}/{storm}
GET /tile-value/{country}/{storm}/{forecast_date}                            -- hover tooltip data
GET /tile-value-combined/{country}/{storm}
GET /stats/{country}/{storm}/{forecast_date} and /admin-stats/...           -- min/max for colour scale
GET /preload/{country}/{storm}/{forecast_date}                               -- warms the in-memory cache
GET /impact/river-member/... , /impact/rain-member/... , /impact/combined-*  -- ensemble-member impact
```

Example mercator tile URL:
`http://localhost:8001/tiles/mercator/PHL/2024309N13282/2024-11-05/7/108/62.pbf?wind_threshold=50`

## How it works

On the first request for a country/storm/date combination the server bulk-loads
all rows for that key from Snowflake into an in-process pandas DataFrame (~2 s).
Mercator bounds for each quadkey are pre-computed once at load time.  Subsequent
tile requests are served entirely from memory via a vectorised `str.startswith`
filter (~0.5 ms per tile).  Cache freshness uses double-checked locking on each
access (`_DataCache._is_fresh()`), a lazy per-entry TTL check (`_TILE_TTL` = 15
min for tile data), not a periodic background sweep -- a stale entry is only
actually reloaded the next time it's requested. A separate background thread
(`raw-cache-prewarm`) runs hourly, but only prewarms the unrelated global
precip-raw/river-raw caches, not this per-country/storm/date cache.

> **Note:** The separate `requirements.txt` in this directory exists because the
> tile server is independently deployable as an SPCS service or Docker container
> and needs `fastapi`, `uvicorn`, and `mapbox-vector-tile` that the main Dash
> app does not use.

## Wiring into the Dash app

The tile server is wired into the dashboard via `components/map/maplibre_tiles.js`.  MapLibre
sources are configured by `applyTileConfig()`, called reactively as the user changes hazard/layer/date
selections on the current root page (`pages/map_shell_concept.py`) -- there is no separate "Load
Layers" button on that page. The legacy `/legacy` page (`pages/dashboard.py`) does have a "Load Layers"
button as part of its own, different UI flow.
