# Snowflake Materialized Tables

SQL scripts for setting up and maintaining the Snowflake materialized tables that power the Ahead of the Storm Dash app and the Snowflake Intelligence agent.

## Overview

Raw ECMWF ensemble forecast data arrives as CSV/Parquet files in Snowflake stage `@AOTS.TC_ECMWF.AOTS_ANALYSIS`. These scripts load that data into real Snowflake tables (`*_MAT`) clustered for fast filtered queries (<1 s vs 17–50 s on raw stage). The tables are the single data source for:

- The Dash app when `IMPACT_DATA_SOURCE=SQL`
- All stored procedures in `../intelligence/`

## Tables

| Table | Source files | Used for |
|---|---|---|
| `MERCATOR_TILE_IMPACT_MAT` | `mercator_views/*.csv` | Expected tile impact, threshold analysis |
| `ADMIN_ALL_IMPACT_MAT` | `admin_views/*_admin*.csv` | Admin breakdown, trend comparison |
| `TRACK_MAT` | `track_views/*.parquet` | Worst-case scenario, scenario distribution |
| `SCHOOL_IMPACT_MAT` | `school_views/*.parquet` | Named schools at risk |
| `HC_IMPACT_MAT` | `hc_views/*.parquet` | Named health facilities at risk |
| `SHELTER_IMPACT_MAT` | `shelter_views/*.parquet` | Named shelters at risk |
| `WASH_IMPACT_MAT` | `wash_views/*.parquet` | Named WASH facilities at risk |
| `BASE_ADMIN_MAT` | `admin_views/*_admin1.parquet` | Admin name lookups |
| `MERCATOR_TILE_CCI_MAT` | `mercator_views/*_cci*.csv` | Tile-level Child Climate Index |
| `ADMIN_ALL_CCI_MAT` | `admin_views/*_cci*.csv` | Admin-level Child Climate Index |

## Prerequisites

- Snowflake account with SYSADMIN role
- Impact analysis CSV/Parquet files in stage `AOTS.TC_ECMWF.AOTS_ANALYSIS`
- Database `AOTS`, schema `TC_ECMWF` must exist

## Setup Instructions

Run scripts in this order:

### Step 1: Create Materialized Tables (`01_setup_materialized_tables.sql`)

Creates all `*_MAT` tables, loads data from stage, creates `REFRESH_MATERIALIZED_VIEWS()` stored procedure, and creates a scheduled refresh task.

Paste into a Snowflake worksheet or run with the Snowflake CLI. The `CREATE TABLE AS SELECT` statements do the initial data load immediately.

**Resume the task once the setup is verified:**
```sql
ALTER TASK AOTS.TC_ECMWF.REFRESH_MATERIALIZED_VIEWS_TASK RESUME;
```

**Two-format CSV compatibility:**

The pipeline ships two CSV formats with different column counts:

| Format | Countries | Discriminator |
|---|---|---|
| Old 12-col | JAM, VNM (pre-2026) | `$13 IS NULL` |
| New 16-col | PNG, SLB (April 2026+) | `$13 IS NOT NULL` |

Both formats are handled in the same `SELECT` using `IFF($13 IS NULL, old_pos, new_pos)` per column. Old-format files return NULL for `E_ADOLESCENT_POPULATION`, `E_NUM_SHELTERS`, `E_NUM_WASH`, `E_SMOD_CLASS_L1`.

**Adding new columns when the pipeline changes:** see `MAT_TABLE_FIX.md`.

### Step 2: Set Up Regional Groups (`01b_setup_regional_groups.sql`)

Creates `REFRESH_REGIONAL_GROUPS()` — the procedure that derives regional rows in every MAT table from member-country rows. Run once after Step 1.

`REFRESH_REGIONAL_GROUPS()` is called automatically at the end of `REFRESH_MATERIALIZED_VIEWS()` — no separate task or manual call needed after this setup.

**How regional groups work:**

A region is a row in `PIPELINE_COUNTRIES` with `IS_REGION = TRUE` and a `MEMBER_CODES` array of ISO3 codes. The app queries `WHERE country = 'ECA'` identically to any individual country.

| Table type | How rows are derived |
|---|---|
| Tile / admin / facility / CCI | Union of all member-country rows, re-tagged with the region code |
| `TRACK_MAT` | SUM of severities per `zone_id` (ensemble member) across member countries |

**Pipeline exclusion:** Regions are excluded from data pipeline processing via `IS_REGION = TRUE`. The `DATAPIPELINE` repo filters `WHERE IS_REGION IS NULL OR IS_REGION = FALSE` in all country selection queries (`country_utils.py`). `COUNTRY_BOUNDARY` is intentionally left NULL for regions — the pipeline's spatial storm filter already skips NULL rows.

### Step 3: Register a Region (`01c_add_regional_group.sql`)

Template for registering a new multi-country region (e.g. ECA — East Caribbean Area). Run once per region; no other files need to change. ECA is included as a commented-out reference example.

Fill in the values at the bottom of the script and run it. The next `REFRESH_MATERIALIZED_VIEWS()` call (or the hourly task) will populate the regional rows automatically.

### Step 4: Verify (`05_test_materialized_tables.sql`)

Test queries to verify all tables are correctly populated — row counts, aggregates, worst-case member selection, expected vs worst-case comparison.

## Key Notes

- **Admin level filtering**: `ADMIN_ALL_IMPACT_MAT` contains rows for all admin levels. Always filter `AND admin_level = 1` to avoid double-counting.
- **Expected impact**: Aggregated from `MERCATOR_TILE_IMPACT_MAT` with `zoom_level = 14`.
- **Worst-case scenario**: From `TRACK_MAT`, group by `zone_id`, select member with highest `severity_population`.
- **Refreshing**: Call `REFRESH_MATERIALIZED_VIEWS()` after new storm data arrives, or wait for the hourly task.
