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

```
GET /health
GET /tiles/mercator/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf?wind_threshold=50
GET /tiles/admin/{country}/{storm}/{forecast_date}/{z}/{x}/{y}.pbf?wind_threshold=50&admin_level=1
```

Example mercator tile URL:
`http://localhost:8001/tiles/mercator/PHL/2024309N13282/2024-11-05/7/108/62.pbf?wind_threshold=50`

## How it works

On the first request for a country/storm/date combination the server bulk-loads
all rows for that key from Snowflake into an in-process pandas DataFrame (~2 s).
Mercator bounds for each quadkey are pre-computed once at load time.  Subsequent
tile requests are served entirely from memory via a vectorised `str.startswith`
filter (~0.5 ms per tile).  Cache invalidation uses double-checked locking; a
background thread refreshes stale entries every 5 minutes.

> **Note:** The separate `requirements.txt` in this directory exists because the
> tile server is independently deployable as an SPCS service or Docker container
> and needs `fastapi`, `uvicorn`, and `mapbox-vector-tile` that the main Dash
> app does not use.

## Wiring into the Dash app

The tile server is wired into the dashboard via `maplibre_tiles.js`.  MapLibre
sources (`aots-mercator` raster, `aots-admin` vector) are configured by
`applyTileConfig()` after the user clicks "Load Layers".
