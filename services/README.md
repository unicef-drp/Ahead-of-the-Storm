# AoTS Tile Server

FastAPI sidecar that serves Mapbox Vector Tiles (MVT/PBF) from Snowflake for
the Ahead of the Storm dashboard.

## Start

```bash
pip install -r requirements.txt -r services/requirements.txt
uvicorn services.tile_server:app --host 0.0.0.0 --port 8001 --reload
```

Credentials are read from the project `.env` (same file used by the Dash app).

## Endpoints

See `services/tile_server.py`'s own `@app.get` decorators for the exact, current list of routes;
grouped by purpose:

```
GET /health
GET /debug/cache-stats                                                       : in-process cache sizes (ops tooling)
GET /tiles/raster/{country}/{storm}/{forecast_date}/{prop}/{z}/{x}/{y}.webp  : WebP raster tiles
GET /tiles/mercator/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf        : MVT vector, mercator grid
GET /tiles/admin/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf           : MVT vector, admin regions
GET /tiles/raster-combined/{country}/{storm}/{mode}/{z}/{x}/{y}.webp         : combined-hazard raster
GET /tiles/admin-combined/{country}/{storm}/{mode}/{z}/{x}/{y}.pbf           : combined-hazard admin MVT
GET /tiles/raster/river-raw/{forecast_time}/{z}/{x}/{y}.webp                 : global raw river flood-extent raster
GET /tiles/raster/precip-raw/{forecast_time}/{z}/{x}/{y}.webp                : global raw precipitation raster
GET /geojson/facilities/{layer_type}/{country}/{storm}/{forecast_date}       : schools/health/shelters/WASH
GET /geojson/facilities-combined/{layer_type}/{country}/{storm}
GET /tile-value/{country}/{storm}/{forecast_date}                            : hover tooltip data
GET /tile-value-combined/{country}/{storm}
GET /tile-value/river-raw/{forecast_time} and /tile-value/precip-raw/{forecast_time}
GET /stats/{country}/{storm}/{forecast_date} and /admin-stats/...            : min/max for colour scale
GET /stats/river-raw/{forecast_time} and /stats/precip-raw/{forecast_time}
GET /preload/{country}/{storm}/{forecast_date}                               : warms the in-memory cache
GET /preload/river-raw/{forecast_time} and /preload/precip-raw/{forecast_time}
GET /impact/river-member/... , /impact/rain-member/... , /impact/combined-member/...   : ensemble-member impact
GET /impact/combined-totals/... , /impact/combined-admin-totals/...          : combined-hazard totals
GET /impact/rain-grid-excl-river/... , /impact/river-grid-excl-rain/...      : exclusive-of-the-other-flood-hazard grids
```

Example mercator tile URL:
`http://localhost:8001/tiles/mercator/PHL/2024309N13282/20241105000000/7/108/62.pbf?wind_threshold=50`

## How it works

On the first request for a country/storm/date combination the server bulk-loads
all rows for that key from Snowflake into an in-process pandas DataFrame (~2 s).
Mercator bounds for each quadkey are pre-computed once at load time.  Subsequent
tile requests are served entirely from memory by binary search over a precomputed sorted
`TILE_ID` index (~0.5 ms per tile).  Cache freshness uses double-checked locking on each
access (`_DataCache._is_fresh()`), a lazy per-entry TTL check (`_TILE_TTL` = 4 h
for tile data): a stale entry is reloaded the next time it is requested. A
separate background thread (`raw-cache-prewarm`, started from the FastAPI lifespan)
runs hourly and prewarms the global precip-raw and river-raw caches, including the
river extent grids, not this per-country/storm/date cache.

### River extent source

The global river-raw layer reads the GloFAS flood-extent Parquet for the requested
date and return-period tier in one of two layouts: the by-member file
(`extent_rpN_bymember`, one row per pixel, member and lead-time step) or the compact
bymask file (`extent_bymask_rpN`, one row per pixel with a uint64 member bitmask per
lead-time step, roughly 5-10x smaller). The optional `RIVER_EXTENT_SOURCE` environment
variable selects the layout: `auto` (default) reads the compact file when it exists in
Blob and otherwise the by-member file, `bymask` requires the compact file, `bymember`
never reads it. Both layouts produce the same per-tile result.

> **Note:** The tile server imports `components.*` and needs the repo requirements
> files (`requirements.txt` locally, `requirements.docker.txt` in the Docker image)
> for `pyarrow`, `zarr` and `giga-spatial`. The `requirements.txt` in this directory
> adds the packages the main Dash app does not use (`fastapi`, `uvicorn`,
> `mapbox-vector-tile`) and does not cover the tile server on its own.

## Wiring into the Dash app

The tile server is wired into the dashboard via `components/map/maplibre_tiles.js`.  MapLibre
sources are configured by `applyTileConfig()`, called reactively as the user changes hazard/layer/date
selections on the app's main page (`pages/map_shell_concept.py`).
