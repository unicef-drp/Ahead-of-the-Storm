# Ahead of the Storm – Setup Guide

## Prerequisites

1. **Python 3.11+** installed
2. **Virtual environment** activated (`.venv`)
3. **Environment variables** configured in `.env` file

## Step 1: Set Up Bounding Box (Required Only Once)

The bounding box defines the geographic region of interest for hurricane impact analysis.

Using here countries in the Caribbean + Central America: ['ATG','BLZ','NIC','DOM','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB']

**Command:**
```bash
python -c "import sys; sys.path.append('components/data'); from impact_analysis import save_bounding_box; countries = ['ATG','BLZ','NIC','DOM','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB']; save_bounding_box(countries); print('Bounding box created!')"
```

**What it does:**
- Fetches administrative boundaries for all specified countries from UNICEF GeoRepo
- Creates a padded bounding box (1000km padding) that encompasses all countries
- Saves the bounding box to `project_results/climate/lacro_project/bbox.parquet`


## Step 2: Initialize Base Data (Required Only Once or After Adding Countries)

This step creates the mercator tile views with demographic and infrastructure data for each country.

**Command:**
```bash
python components/data/main_pipeline.py --type initialize
```

**What it does:**
- Creates mercator tiles for each country at zoom level 14
- Downloads and aggregates demographic data (WorldPop population)
- Downloads and aggregates infrastructure data (GHSL built surface, SMOD settlement)
- Fetches school locations (via GIGA API)
- Fetches health center locations (via HealthSites API)
- Saves base views to `geodb/aos_views/mercator_views/`

**Note:** This process can take 30-60 minutes and downloads several GB of data. It only needs to be run once, or when you add new countries.


## Step 3: Process Storm Data

Once initialized, you can run the pipeline to process hurricane data from Snowflake.

**Command:**
```bash
python components/data/main_pipeline.py
```

**What it does:**
- Connects to Snowflake and retrieves available storm data
- For each storm/forecast combination that intersects the bounding box:
  - Loads hurricane envelope data
  - Creates impact views for schools, health centers, tiles, and tracks
  - Saves views to `geodb/aos_views/hc_views/`, `geodb/aos_views/school_views/`, etc.
  - Records processed storms in `storms.json`

**Process Flow:**
1. Reads `storms.json` to track which storms have been processed
2. Queries Snowflake for new storms
3. For each storm not yet processed:
   - Loads envelope data (wind impact areas at different thresholds)
   - Checks if envelopes intersect with region of interest
   - Creates impact views for each country in the region
   - Marks storm as processed in `storms.json`

**Output Example:**
```
======================================================================
HURRICANE IMPACT ANALYSIS PIPELINE
======================================================================
Storm: LORENZO
Forecast Time: 20251015120000
Countries: ATG, BLZ, NIC, DOM, DMA, GRD, MSR, KNA, LCA, VCT, AIA, VGB
Skip Analysis: False
======================================================================
STEP 1: Impact Analysis
--------------------------------------------------
Running impact analysis for LORENZO at 20251015120000
Countries: ATG, BLZ, NIC, DOM, DMA, GRD, MSR, KNA, LCA, VCT, AIA, VGB
Loading envelope data from Snowflake...
Loaded 23 envelope records
Envelopes already converted to GeoDataFrame
Getting bounding box...
Envelopes intersect with region
Creating impact views...
  Processing ATG...
    Processing schools...
    Created 3 school views
    Processing health centers...
    Created 3 health center views
    Processing tiles...
    Created 3 tile views
    Processing tracks...
    Created 3 track views
Impact analysis completed successfully
======================================================================
```

## Step 4: Run the Dash Application

After processing storm data, you can visualize the results in the web application.

**Command:**
```bash
python app.py
```

**What it does:**
- Starts a Dash web server (typically on http://127.0.0.1:8050)
- Loads processed storm data and impact views
- Displays interactive maps showing hurricane tracks and impact zones
- Shows schools, health centers, and population at risk



## Troubleshooting

### "Bounding box not found" error
- Run **Step 1** to create the bounding box

### "No envelope data found" error
- This is normal for storms that don't affect the region
- Check that envelopes intersect with the bounding box

### "401 Unauthorized" for school data
- Verify `GIGA_SCHOOL_LOCATION_API_KEY` is set correctly in `.env`

### "SSL Certificate" errors
- Install: `pip install pip-system-certs`

### Pipeline fails on specific countries
- Some smaller Caribbean islands may have issues
- You can skip problematic countries or run with only working ones:
  ```bash
  python components/data/main_pipeline.py --countries ATG BLZ DOM
  ```

## Quick Start Summary

```bash
# 1. Create bounding box (one-time setup)
python -c "import sys; sys.path.append('components/data'); from impact_analysis import save_bounding_box; save_bounding_box(['ATG','BLZ','NIC','DOM','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB'])"

# 2. Initialize base data (one-time setup, takes 30-60 min)
python components/data/main_pipeline.py --type initialize

# 3. Process storms (run regularly to update with new storm data)
python components/data/main_pipeline.py

# 4. Launch the application
python app.py
```

## Data Storage

- **Bounding box:** `project_results/climate/lacro_project/bbox.parquet`
- **Base views:** `geodb/aos_views/mercator_views/`
- **Impact views:** `geodb/aos_views/hc_views/`, `geodb/aos_views/school_views/`, etc.
- **Processed storms:** `project_results/climate/lacro_project/storms.json`
- **Raw data:** `geodb/bronze/` (downloaded automatically)

