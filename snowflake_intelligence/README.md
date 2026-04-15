# Snowflake Intelligence Setup for Hurricane Impact Analysis

This directory contains SQL scripts to set up Snowflake Intelligence for hurricane impact analysis. The system queries pre-computed materialized tables built from ECMWF ensemble forecast data and generates actionable intelligence reports for emergency response specialists.

## Overview

The Snowflake Intelligence system provides AI-powered situation intelligence for hurricane impact analysis. It uses raw forecast data stored in Snowflake stages and generates comprehensive reports including:

- **Executive Summary**: Quick overview of threat level and key concerns
- **Expected Impact**: Probabilistic impact values with administrative area breakdowns
- **Scenario Analysis**: Distribution analysis across ensemble members and worst-case likelihood assessment
- **Trend Analysis**: Comparison between current and previous forecast runs
- **Key Takeaways**: Concise summary of critical findings

## Architecture

- **Materialized Tables (`*_MAT`)**: Real Snowflake tables loaded from stage files, clustered for fast filtered queries (<1s vs 17–50s on raw stage). The sole data source for all stored procedures and for the Dash app when `IMPACT_DATA_SOURCE=SQL`.
- **Stored Procedures**: 17 governed SQL queries executed as AI agent tools — all query `*_MAT` tables directly.
- **AI Agent (`HURRICANE_INTELLIGENCE`)**: Orchestrates tool calls and generates intelligence reports in five sections.

## Prerequisites

- Snowflake account with ACCOUNTADMIN role (or CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT privilege)
- Impact analysis CSV/Parquet files in Snowflake stage: `AOTS.TC_ECMWF.AOTS_ANALYSIS`
- Base admin Parquet files in stage
- Warehouse: `SF_AI_WH` (must exist - used for agent queries to enable separate cost monitoring)

## Configuration

- **Database**: `AOTS`
- **Schema**: `TC_ECMWF`
- **Stage**: `AOTS.TC_ECMWF.AOTS_ANALYSIS`
- **Warehouse**: `SF_AI_WH` (for agent queries - separate from setup warehouse for cost monitoring)

## Setup Instructions

Execute the SQL scripts in the following order:

### Step 0: Set Up Materialized Tables (`01_setup_materialized_tables.sql`)

Creates real Snowflake tables loaded from stage CSV/Parquet files. These tables are the primary data source for both the Dash app (`IMPACT_DATA_SOURCE=SQL`) and the stored procedures.

**What it does:**
- Creates file formats (`CSV_ADMIN_VIEWS_FORMAT` with `ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE`)
- Creates and populates materialized tables from stage files:
  - `MERCATOR_TILE_IMPACT_MAT` — tile-level probabilistic impact data, clustered by `(COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD, ZOOM_LEVEL)`
  - `ADMIN_ALL_IMPACT_MAT` — admin-region impact with probability and admin level, clustered by `(COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD, ADMIN_LEVEL)`
  - `MERCATOR_TILE_CCI_MAT` — tile-level Child Climate Index data
  - `ADMIN_ALL_CCI_MAT` — admin-level Child Climate Index data
  - `TRACK_MAT` — per-ensemble-member severity and wind-envelope geometry
  - `SCHOOL_IMPACT_MAT` — point-level school impact data (one row per school per storm/threshold)
  - `HC_IMPACT_MAT` — point-level health centre impact data
  - `SHELTER_IMPACT_MAT` — point-level shelter impact data
  - `WASH_IMPACT_MAT` — point-level WASH facility impact data
- Creates `REFRESH_MATERIALIZED_VIEWS()` stored procedure to reload all tables
- Creates a scheduled task (`REFRESH_MATERIALIZED_VIEWS_TASK`) that calls the refresh hourly

**Run:**
```bash
snow sql -f snowflake_intelligence/01_setup_materialized_tables.sql
```
Or paste into a Snowflake worksheet. The `CREATE TABLE AS SELECT` statements do the initial data load immediately.

**Two-format CSV compatibility:**

The pipeline ships two CSV formats with different column counts and ordering:

| Format | Countries | Columns | Discriminator |
|--------|-----------|---------|---------------|
| Old 12-col | JAM, VNM (pre-2026) | `zone_id, id, prob, E_pop, E_built, E_schools, E_school_age, E_infant, E_hcs, E_rwi, E_smod` | `$13 IS NULL` |
| New 16-col | PNG, SLB (April 2026+) | `zone_id, id, prob, E_pop, E_school_age, E_infant, E_adolescent, E_built, E_smod, E_smod_l1, E_rwi, E_schools, E_hcs, E_shelters, E_wash` | `$13 IS NOT NULL` |

Both formats are handled in the same SELECT using `IFF($13 IS NULL, old_pos, new_pos)` per column. Old-format files return NULL for the 4 new-only columns (`E_ADOLESCENT_POPULATION`, `E_NUM_SHELTERS`, `E_NUM_WASH`, `E_SMOD_CLASS_L1`). Admin CSVs use `$15` as the discriminator.

**Adding new columns when the pipeline changes:** See `MAT_TABLE_FIX.md` for a step-by-step guide.

**Refreshing data:** Call `REFRESH_MATERIALIZED_VIEWS()` after new storm data arrives, or wait for the hourly scheduled task. The task is created in SUSPENDED state — resume it once the setup is verified:
```sql
ALTER TASK AOTS.TC_ECMWF.REFRESH_MATERIALIZED_VIEWS_TASK RESUME;
```

### Step 1: Set Up Snowflake Intelligence (`02_setup_snowflake_intelligence.sql`)


Creates the Snowflake Intelligence object and grants necessary privileges.

**What it does:**
- Creates account-level Snowflake Intelligence object: `SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT`
- Grants USAGE privilege to SYSADMIN role
- Verifies setup with SHOW commands

**Note:** After agents are created, they must be added to this object using:
```sql
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT 
ADD AGENT <agent_name>;
```

### Step 2: Create Stored Procedures (`04_create_stored_procedures.sql`)

Creates the 17 stored procedures that serve as tools for the AI agent. All procedures query `*_MAT` tables directly.

**Stored Procedures:**

1. **GET_EXPECTED_IMPACT_VALUES**: Expected (probabilistic) impact for a country/storm/date/threshold — from `MERCATOR_TILE_IMPACT_MAT` zoom=14
2. **GET_SINGLE_METRIC**: Returns one named metric efficiently (e.g. `expected_population`, `worst_case_population`)
3. **GET_WORST_CASE_SCENARIO**: Worst-case ensemble member impact — from `TRACK_MAT`
4. **GET_SCENARIO_DISTRIBUTION**: Distribution statistics across ensemble members, including embedded risk classification (LOW/MODERATE/HIGH) — from `TRACK_MAT`
5. **GET_ALL_WIND_THRESHOLDS_ANALYSIS**: Expected impact for all wind thresholds in one call — from `MERCATOR_TILE_IMPACT_MAT`
6. **GET_THRESHOLD_PROBABILITIES**: Average impact probability per wind threshold — from `ADMIN_ALL_IMPACT_MAT`
7. **GET_ADMIN_LEVEL_BREAKDOWN**: Admin-area impact breakdown — from `ADMIN_ALL_IMPACT_MAT` (admin_level=1)
8. **GET_ADMIN_LEVEL_TREND_COMPARISON**: Change between two forecast dates at admin level — from `ADMIN_ALL_IMPACT_MAT`
9. **GET_PREVIOUS_FORECAST_DATE**: Previous forecast date for single-step trend — from `ADMIN_ALL_IMPACT_MAT`
10. **GET_FORECAST_DATE_HISTORY**: Last N forecast dates for multi-run trend — from `ADMIN_ALL_IMPACT_MAT`
11. **GET_HIGH_RISK_SCHOOLS**: Named schools above a probability threshold — from `SCHOOL_IMPACT_MAT`
12. **GET_HIGH_RISK_HEALTH_CENTERS**: Named health facilities above a probability threshold — from `HC_IMPACT_MAT`
13. **VALIDATE_ADMIN_TOTALS**: Cross-checks admin totals vs tile totals (1% tolerance) — called after `GET_ADMIN_LEVEL_BREAKDOWN`
14. **DISCOVER_AVAILABLE_STORMS**: Available storms for a country/date — from `ADMIN_ALL_IMPACT_MAT`
15. **GET_LATEST_FORECAST_DATE**: Latest forecast date for a country (optionally filtered by storm)
16. **GET_LATEST_DATA_OVERALL**: Latest data across all countries/storms
17. **GET_COUNTRY_ISO3_CODE**: Resolves country name to ISO3 code

**All procedures:**
- Return JSON (VARIANT) objects
- Use `EXECUTE AS OWNER` for security
- Include proper error handling
- Grant USAGE to SYSADMIN role

### Step 3: Create Agent (`03_create_agent.sql`)

Creates the AI agent that generates intelligence reports.

**Agent Name:** `HURRICANE_INTELLIGENCE`

**What it does:**
- Provides comprehensive situation updates for any country during a storm
- Uses stored procedures as tools to query data
- Generates reports in 5 sections:
  1. Executive Summary
  2. Expected Impact
  3. Scenario Analysis
  4. Trend Analysis
  5. Key Takeaways

**Key features:**
- **Multi-language support**: Detects user's language and responds in that language
- **Wind threshold handling**: Supports different wind thresholds (34kt, 50kt, 64kt, etc.)
- **Incomplete query handling**: Intelligently handles missing parameters (country, storm, date)
- **Data validation**: Verifies calculations and flags potential data mismatches
- **Admin-level breakdowns**: Shows detailed administrative area breakdowns
- **Risk classification**: Classifies worst-case scenarios as SPECIAL CASE, PLAUSIBLE, or REAL THREAT

**Agent Configuration:**
- Orchestration budget: 120 seconds, 25,000 tokens
- Uses 17 stored procedure tools
- Executes on `SF_AI_WH` warehouse (separate warehouse for cost monitoring)
- Requires SYSADMIN role for usage

**Granted Privileges:**
- USAGE on agent to SYSADMIN
- USAGE on warehouse to SYSADMIN
- SELECT on all materialized tables to SYSADMIN
- USAGE on all stored procedures to SYSADMIN

### Step 4: Test (`05_test_materialized_tables.sql`)

Contains test queries to verify that all materialized tables are correctly populated.

**Test queries:**
1. Test ADMIN_ALL_IMPACT_MAT row counts and aggregates
2. Test MERCATOR_TILE_IMPACT_MAT row counts and aggregates
3. Test TRACK_MAT including worst-case member selection
4. Test SCHOOL_IMPACT_MAT and HC_IMPACT_MAT row counts
5. Test BASE_ADMIN_MAT
6. Compare Expected vs Worst-Case (Agent Logic Test)

## Usage

### Querying the Agent

Once set up, users can query the agent using Snowflake's natural language interface:

```sql
-- Example: Get situation update for Jamaica during storm MELISSA
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE',
    'What is the situation for Jamaica during storm MELISSA?'
);
```

### Required Role

Users must use the `SYSADMIN` role (or a role with procedure USAGE) when querying the agent.

### Supported Queries

The agent supports various query formats:

- **Full specification**: "What is the situation for Jamaica during storm MELISSA on October 28, 2025, 00Z?"
- **Missing storm**: "What is the situation for Jamaica on October 28, 2025, 00Z" (agent will discover available storms)
- **Missing date**: "What is the latest situation for Jamaica?" (agent will find latest forecast)
- **Missing country**: "What is the latest situation?" (agent will find latest data overall)
- **Wind threshold**: "What is the situation at 34kt wind threshold?" (agent will use specified threshold)
- **Country name**: "What is the situation for Philippines?" (agent will resolve to ISO3 code)


## Data Sources

### Materialized Tables

All data is served from materialized tables in `AOTS.TC_ECMWF`, loaded from stage `@AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/` by `01_setup_materialized_tables.sql`.

| Table | Source | Used for |
|-------|--------|----------|
| `MERCATOR_TILE_IMPACT_MAT` | `mercator_views/*.csv` | Expected impact (zoom=14), threshold analysis |
| `ADMIN_ALL_IMPACT_MAT` | `admin_views/*_admin*.csv` | Admin breakdown, trend comparison |
| `TRACK_MAT` | `track_views/*.parquet` | Worst-case scenario, scenario distribution |
| `SCHOOL_IMPACT_MAT` | `school_views/*.parquet` | Named schools at risk |
| `HC_IMPACT_MAT` | `hc_views/*.parquet` | Named health facilities at risk |
| `SHELTER_IMPACT_MAT` | `shelter_views/*.parquet` | Named shelters at risk |
| `WASH_IMPACT_MAT` | `wash_views/*.parquet` | Named WASH facilities at risk |
| `BASE_ADMIN_MAT` | `admin_views/*_admin1.parquet` | Admin name lookups |

### Expected Impact Calculation

Expected impact values use `MERCATOR_TILE_IMPACT_MAT` with `zoom_level = 14`:
- Aggregates tile-level expected values (E_population, E_num_schools, etc.)
- Uses probability-weighted calculations
- Matches dashboard application logic

### Worst-Case Scenario Calculation

Worst-case scenarios use `TRACK_MAT`:
- Groups by `zone_id` (ensemble member)
- Sums severity metrics per member
- Selects member with highest `severity_population`

### Admin-Level Breakdown

Admin-level breakdowns use `ADMIN_ALL_IMPACT_MAT` with `admin_level = 1`:
- Contains rows for all admin levels — always filter `AND admin_level = 1` to avoid double-counting
- Names are stored directly in the table (no separate join required)


**Execution order:** `01_setup_materialized_tables.sql` → `02_setup_snowflake_intelligence.sql` → `04_create_stored_procedures.sql` → `03_create_agent.sql`