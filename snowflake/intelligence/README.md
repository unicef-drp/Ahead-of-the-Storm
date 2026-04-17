# Snowflake Intelligence — Hurricane Situation Analysis

SQL scripts to set up the Snowflake Cortex AI agent (`HURRICANE_INTELLIGENCE`) that generates actionable intelligence reports from the materialized tables.

## Overview

The agent queries the `*_MAT` tables (set up in `../mat_tables/`) using 17 stored procedures as tools, and generates five-section situation reports for emergency response specialists:

1. **Executive Summary** — threat level and key concerns
2. **Expected Impact** — probabilistic impact values with admin breakdowns
3. **Scenario Analysis** — ensemble distribution and worst-case likelihood
4. **Trend Analysis** — comparison between current and previous forecast runs
5. **Key Takeaways** — critical findings

## Prerequisites

- MAT tables set up and populated — run `../mat_tables/` scripts first
- Snowflake account with ACCOUNTADMIN role (or `CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT` privilege)
- Warehouse `SF_AI_WH` must exist (used for agent queries; kept separate for cost monitoring)

## Setup Instructions

Run scripts in this order:

### Step 1: Set Up Snowflake Intelligence (`02_setup_snowflake_intelligence.sql`)

Creates the account-level Snowflake Intelligence object `SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT` and grants privileges. After agents are created, add them to this object:

```sql
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT 
ADD AGENT HURRICANE_INTELLIGENCE;
```

### Step 2: Create Role (`08_create_aots_agent_role.sql`)

Creates the `AOTS_AGENT` role with appropriate privileges. Run before creating stored procedures.

### Step 3: Create Stored Procedures (`04_create_stored_procedures.sql`)

Creates 17 stored procedures that serve as agent tools. All query `*_MAT` tables directly, return JSON (`VARIANT`), use `EXECUTE AS OWNER`, and grant USAGE to SYSADMIN.

| # | Procedure | Description |
|---|---|---|
| 1 | `GET_EXPECTED_IMPACT_VALUES` | Expected (probabilistic) impact — population, schools, HCs, shelters, WASH |
| 2 | `GET_SINGLE_METRIC` | One named metric efficiently (13 supported metrics) |
| 3 | `GET_WORST_CASE_SCENARIO` | Worst-case ensemble member from `TRACK_MAT` |
| 4 | `GET_SCENARIO_DISTRIBUTION` | Distribution statistics + risk classification across ensemble members |
| 5 | `GET_ALL_WIND_THRESHOLDS_ANALYSIS` | Expected impact for all wind thresholds in one call |
| 6 | `GET_THRESHOLD_PROBABILITIES` | Average impact probability per wind threshold |
| 7 | `GET_ADMIN_LEVEL_BREAKDOWN` | Admin-area impact breakdown (admin_level=1) |
| 8 | `GET_ADMIN_LEVEL_TREND_COMPARISON` | Change between two forecast dates at admin level |
| 9 | `GET_PREVIOUS_FORECAST_DATE` | Previous forecast date for single-step trend |
| 10 | `GET_FORECAST_DATE_HISTORY` | Last N forecast dates |
| 11 | `GET_HIGH_RISK_SCHOOLS` | Named schools above a probability threshold |
| 12 | `GET_HIGH_RISK_HEALTH_CENTERS` | Named health facilities above a probability threshold |
| 13 | `VALIDATE_ADMIN_TOTALS` | Cross-checks admin vs tile totals (1% tolerance) |
| 14 | `DISCOVER_AVAILABLE_STORMS` | Available storms for a country/date |
| 15 | `GET_LATEST_FORECAST_DATE` | Latest forecast date for a country |
| 16 | `GET_LATEST_DATA_OVERALL` | Latest data across all countries/storms |
| 17 | `GET_COUNTRY_ISO3_CODE` | Resolves country name to ISO3 code |

**Supported `GET_SINGLE_METRIC` metrics:** `expected_population`, `expected_children` (0–19), `expected_school_age` (5–14), `expected_infants` (0–4), `expected_adolescents` (15–19), `expected_schools`, `expected_health_centers`, `expected_shelters`, `expected_wash`, `worst_case_population`, `worst_case_children`, `worst_to_expected_ratio`, `ensemble_count`.

### Step 4: Create Agent (`03_create_agent.sql`)

Creates `HURRICANE_INTELLIGENCE` — the Cortex AI agent. Configuration:

- Orchestration budget: 120 s, 25,000 tokens
- Warehouse: `SF_AI_WH`
- 17 stored procedure tools
- Multi-language support (responds in the user's language)
- Risk classification: LOW / MODERATE / HIGH for worst-case scenarios

### Step 5: Cost Tracking (`06_track_agent_costs.sql`)

Optional. Sets up views/queries to monitor agent query costs by warehouse.

## Usage

```sql
-- Requires SYSADMIN role (or a role with procedure USAGE)
USE ROLE SYSADMIN;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE',
    'What is the situation for Jamaica during storm MELISSA on October 28, 2025?'
);
```

The agent handles missing parameters gracefully:

| Query | Behaviour |
|---|---|
| Full spec (country + storm + date) | Generates full report |
| Missing storm | Calls `DISCOVER_AVAILABLE_STORMS` first |
| Missing date | Calls `GET_LATEST_FORECAST_DATE` first |
| Missing country | Calls `GET_LATEST_DATA_OVERALL` first |
| Country name instead of ISO3 | Calls `GET_COUNTRY_ISO3_CODE` first |

## Evaluations

Agent quality is measured with Snowflake Native Cortex Agent Evaluations. Dataset and views live in `07_create_eval_dataset.sql` and `07b_create_eval_views.sql`.

### Datasets

| Evalset name | View | Cases | Purpose |
|---|---|---|---|
| `HURRICANE_INTELLIGENCE_EVALSET` | `EVAL_ALL` | 72 | Full baseline — run after any agent change |
| `HURRICANE_INTELLIGENCE_MIXED_EVALSET` | `EVAL_MIXED` | 10 | Quick cross-category regression check |
| `HURRICANE_INTELLIGENCE_SINGLE_METRICS_EVALSET` | `EVAL_SINGLE_METRICS` | 14 | Single-metric precision (all supported metrics) |
| `HURRICANE_INTELLIGENCE_FULL_REPORTS_EVALSET` | `EVAL_FULL_REPORTS` | 5 | Five-section report quality |
| `HURRICANE_INTELLIGENCE_NAMED_FACILITIES_EVALSET` | `EVAL_NAMED_FACILITIES` | 5 | Schools and health center name lookups |

### Running evaluations

```sql
PUT file://snowflake/intelligence/eval_config.yaml @AOTS.TC_ECMWF.AGENT_STAGE AUTO_COMPRESS=FALSE;
CALL EXECUTE_AI_EVALUATION(
    'START',
    OBJECT_CONSTRUCT('run_name', 'v3.1_new_metrics'),
    '@AOTS.TC_ECMWF.AGENT_STAGE/eval_config.yaml'
);
```

Set `dataset_name` in `eval_config.yaml` to choose which evalset to run.
