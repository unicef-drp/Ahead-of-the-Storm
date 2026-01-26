# Snowflake Intelligence Setup for Hurricane Impact Analysis

This directory contains SQL scripts to set up Snowflake Intelligence for hurricane impact analysis. The system transforms probabilistic forecast data into actionable intelligence reports for emergency response specialists.

## Overview

The Snowflake Intelligence system provides AI-powered situation intelligence for hurricane impact analysis. It uses raw forecast data stored in Snowflake stages and generates comprehensive reports including:

- **Executive Summary**: Quick overview of threat level and key concerns
- **Expected Impact**: Probabilistic impact values with administrative area breakdowns
- **Scenario Analysis**: Distribution analysis across ensemble members and worst-case likelihood assessment
- **Trend Analysis**: Comparison between current and previous forecast runs
- **Key Takeaways**: Concise summary of critical findings

## Architecture

The system follows a simplified architecture where the AI agent computes all intelligence from raw data:

- **Raw Data Views**: Direct access to stage files (CSV and Parquet)
- **Stored Procedures**: Governed SQL queries executed as agent tools
- **AI Agent**: Processes data and generates intelligence reports
- **No Pre-computed Views**: All calculations are done dynamically by the agent

## Prerequisites

- Snowflake account with ACCOUNTADMIN role (or CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT privilege)
- Impact analysis CSV/Parquet files in Snowflake stage: `AOTS.TC_ECMWF.AOTS_ANALYSIS`
- Base admin Parquet files in stage
- Warehouse: `AOTS_WH` (must exist)

## Configuration

- **Database**: `AOTS`
- **Schema**: `TC_ECMWF`
- **Stage**: `AOTS.TC_ECMWF.AOTS_ANALYSIS`
- **Warehouse**: `AOTS_WH`

## Setup Instructions

Execute the SQL scripts in the following order:

### Step 1: Create Views (`01_setup_views.sql`)

Creates file formats and raw data views that read directly from Snowflake stages.

**What it does:**
- Creates file formats for CSV and Parquet files
- Creates raw data views that extract data from stage files:
  - `ADMIN_IMPACT_VIEWS_RAW`: Admin-level impact data from CSV files
  - `SCHOOL_IMPACT_VIEWS_RAW`: School impact data from Parquet files
  - `HEALTH_CENTER_IMPACT_VIEWS_RAW`: Health center impact data from Parquet files
  - `TRACK_VIEWS_RAW`: Ensemble track impact data from Parquet files
  - `MERCATOR_TILE_IMPACT_VIEWS_RAW`: Tile-level impact data from CSV files
  - `BASE_ADMIN_VIEWS_RAW`: Admin ID to name mappings from Parquet files

**File naming conventions:**
- CSV files: `{country}_{storm}_{date}_{wind_threshold}_admin1.csv`
- Parquet files: `{country}_{storm}_{date}_{wind_threshold}.parquet`
- Admin mapping files: `{country}_admin1.parquet`

**Key features:**
- Extracts metadata (country, storm, forecast_date, wind_threshold) from filenames
- Handles both CSV and Parquet formats
- Parses VARIANT columns from Parquet files
- Includes verification queries at the end

### Step 2: Set Up Snowflake Intelligence (`02_setup_snowflake_intelligence.sql`)

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

### Step 3: Create Stored Procedures (`04_create_stored_procedures.sql`)

Creates all stored procedures that serve as tools for the AI agent.

**Stored Procedures:**

1. **GET_EXPECTED_IMPACT_VALUES**: Returns expected (probabilistic) impact values for a country, storm, forecast date, and wind threshold
2. **DISCOVER_AVAILABLE_STORMS**: Finds available storms for a given country and date
3. **GET_LATEST_FORECAST_DATE**: Gets the latest forecast date for a country (optionally with storm)
4. **GET_LATEST_DATA_OVERALL**: Gets latest data across all countries/storms
5. **GET_WORST_CASE_SCENARIO**: Returns worst-case impact from ensemble members
6. **GET_SCENARIO_DISTRIBUTION**: Returns distribution statistics (percentiles, mean, stddev) across ensemble members
7. **GET_PREVIOUS_FORECAST_DATE**: Gets previous forecast date for trend analysis
8. **GET_ALL_WIND_THRESHOLDS_ANALYSIS**: Returns expected impact values for all available wind thresholds
9. **GET_ADMIN_LEVEL_BREAKDOWN**: Returns admin-level impact breakdown by administrative areas
10. **GET_ADMIN_LEVEL_TREND_COMPARISON**: Returns admin-level comparison between current and previous forecasts
11. **GET_RISK_CLASSIFICATION**: Classifies worst-case scenario risk level (SPECIAL CASE, PLAUSIBLE, or REAL THREAT)
12. **GET_THRESHOLD_PROBABILITIES**: Returns probability values by wind threshold
13. **GET_COUNTRY_ISO3_CODE**: Resolves country name to ISO3 code

**All procedures:**
- Return JSON (VARIANT) objects
- Use `EXECUTE AS OWNER` for security
- Include proper error handling
- Grant USAGE to SYSADMIN role

### Step 4: Create Agent (`03_create_agent.sql`)

Creates the AI agent that generates intelligence reports.

**Agent Name:** `HURRICANE_SITUATION_INTELLIGENCE`

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
- Orchestration budget: 120 seconds, 20,000 tokens
- Uses 13 stored procedure tools
- Executes on `AOTS_WH` warehouse
- Requires SYSADMIN role for usage

**Granted Privileges:**
- USAGE on agent to SYSADMIN
- USAGE on warehouse to SYSADMIN
- SELECT on all raw data views to SYSADMIN
- USAGE on all stored procedures to SYSADMIN

### Step 5: Test Views (`05_test_views.sql`)

Contains test queries to verify that views work correctly.

**Test queries:**
1. Test CSV view (ADMIN_IMPACT_VIEWS_RAW)
2. Test CSV view (MERCATOR_TILE_IMPACT_VIEWS_RAW)
3. Test Parquet view (TRACK_VIEWS_RAW) including worst-case member selection
4. Test Parquet views (SCHOOL and HC)
5. Compare Expected vs Worst-Case (Agent Logic Test)

## Usage

### Querying the Agent

Once set up, users can query the agent using Snowflake's natural language interface:

```sql
-- Example: Get situation update for Jamaica during storm MELISSA
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'AOTS.TC_ECMWF.HURRICANE_SITUATION_INTELLIGENCE',
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

### Raw Data Views

All views read from the stage: `@AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/`

**View Structure:**

| View Name | Data Source | Format | Location |
|-----------|-------------|--------|----------|
| `ADMIN_IMPACT_VIEWS_RAW` | Admin-level impact | CSV | `admin_views/` |
| `SCHOOL_IMPACT_VIEWS_RAW` | School impact | Parquet | `school_views/` |
| `HEALTH_CENTER_IMPACT_VIEWS_RAW` | Health center impact | Parquet | `hc_views/` |
| `TRACK_VIEWS_RAW` | Ensemble tracks | Parquet | `track_views/` |
| `MERCATOR_TILE_IMPACT_VIEWS_RAW` | Tile-level impact | CSV | `mercator_views/` |
| `BASE_ADMIN_VIEWS_RAW` | Admin name mappings | Parquet | `admin_views/` |

### Expected Impact Calculation

Expected impact values use `MERCATOR_TILE_IMPACT_VIEWS_RAW` with `zoom_level = 14`:
- Aggregates tile-level expected values (E_population, E_num_schools, etc.)
- Uses probability-weighted calculations
- Matches dashboard application logic

### Worst-Case Scenario Calculation

Worst-case scenarios use `TRACK_VIEWS_RAW`:
- Groups by `zone_id` (ensemble member)
- Sums severity metrics per member
- Selects member with highest `severity_population`

### Admin-Level Breakdown

Admin-level breakdowns use `ADMIN_IMPACT_VIEWS_RAW`:
- Aggregates by admin name (from `name` column)
- Joins with `BASE_ADMIN_VIEWS_RAW` for human-readable names
- Falls back to admin IDs if names not available


## File Structure

```
snowflake_intelligence/
├── 01_setup_views.sql              # Create file formats and raw data views
├── 02_setup_snowflake_intelligence.sql  # Set up Snowflake Intelligence object
├── 03_create_agent.sql             # Create AI agent with tools and instructions
├── 04_create_stored_procedures.sql # Create stored procedures used by agent
├── 05_test_views.sql               # Test queries to verify setup
└── README.md                       # This file
```