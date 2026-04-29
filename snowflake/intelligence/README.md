# Snowflake Intelligence — Hurricane Situation Analysis

SQL scripts to set up the Snowflake Cortex AI agent (`HURRICANE_INTELLIGENCE`) that generates actionable intelligence reports from the materialized tables.

## Overview

The agent queries the `*_MAT` tables (set up in `../mat_tables/`) using 18 stored procedures as tools, and generates five-section situation reports for emergency response specialists:

1. **Executive Summary** — threat level and key concerns
2. **Expected Impact** — probabilistic impact values with admin breakdowns
3. **Scenario Analysis** — ensemble distribution and worst-case likelihood
4. **Trend Analysis** — comparison between current and previous forecast runs
5. **Key Takeaways** — critical findings

---

## Prerequisites

- MAT tables set up and populated — run `../mat_tables/` scripts first
- Snowflake account with ACCOUNTADMIN role (or `CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT` privilege)
- Warehouse `SF_AI_WH` must exist (used for agent queries; kept separate from `AOTS_WH` for cost monitoring)

---

## Setup Instructions

Run scripts in this order:

### Step 1: Set Up Snowflake Intelligence (`01_setup_snowflake_intelligence.sql`)

Creates the account-level Snowflake Intelligence object and grants privileges. After agents are created, register them:

```sql
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT 
ADD AGENT HURRICANE_INTELLIGENCE;
```

### Step 2: Create Agent (`02_create_agent.sql`)

Creates `HURRICANE_INTELLIGENCE` — the Cortex AI agent.

### Step 3: Create Stored Procedures (`03_create_stored_procedures.sql`)

Creates **18 stored procedures** that serve as agent tools. All query `*_MAT` tables directly, return JSON (`VARIANT`), use `EXECUTE AS OWNER`, and grant USAGE to `SYSADMIN` and `AOTS_ROLE`.

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
| 11 | `GET_HIGH_RISK_SCHOOLS` | Named schools above a probability threshold (up to 20) |
| 12 | `GET_HIGH_RISK_HEALTH_CENTERS` | Named health facilities above a probability threshold (up to 20) |
| 13 | `VALIDATE_ADMIN_TOTALS` | Cross-checks admin vs tile totals (1% tolerance) |
| 14 | `DISCOVER_AVAILABLE_STORMS` | Available storms for a country/date |
| 15 | `GET_LATEST_FORECAST_DATE` | Latest forecast date for a country |
| 16 | `GET_LATEST_DATA_OVERALL` | Latest data across all countries/storms |
| 17 | `GET_COUNTRY_ISO3_CODE` | Resolves country name to ISO3 code |
| 18 | `GET_CENTROID_SHIFT` | Shift in children-at-risk-weighted centroid since prior forecast |

**Supported `GET_SINGLE_METRIC` metrics:** `expected_population`, `expected_children` (0–19), `expected_school_age` (5–14), `expected_infants` (0–4), `expected_adolescents` (15–19), `expected_schools`, `expected_health_centers`, `expected_shelters`, `expected_wash`, `worst_case_population`, `worst_case_children`, `worst_to_expected_ratio`, `ensemble_count`.

#### GET_CENTROID_SHIFT

Returns the shift in the **children-at-risk-weighted geographic centroid** between the current and the immediately prior forecast run at a given wind threshold:

```json
{
  "has_previous": true,
  "dist_km": 13,
  "direction": "W",
  "top_gainer": { "name": "Saint James", "delta": 4074 },
  "top_loser":  { "name": "Saint Catherine", "delta": -3604 },
  "previous_forecast_date": "20251027120000"
}
```

This is **not** the movement of a single storm track — it is the change in where the expected impact (averaged across all 50 ensemble members) is concentrated geographically.

**Important implementation note:** `GET_PREVIOUS_FORECAST_DATE` is a stored procedure and must be called with `CALL` syntax inside `snowflake.createStatement`, not as a UDF in a `SELECT`. The result is a JSON string that needs `JSON.parse()` before use.

#### Date Normalisation (all procedures)

All date parameters are normalised on entry:
```sql
RPAD(REGEXP_REPLACE(?, '[^0-9]', ''), 14, '0')
```
This accepts 8-digit (`20251028`), 14-digit (`20251028180000`), and ISO (`2025-10-28 18:00:00`) formats.

### Step 4: Grant Agent Privileges (`06_grant_aots_agent_privileges.sql`)

Grants stored procedure USAGE, table SELECT, and agent USAGE to the `AOTS_AGENT` role. Requires the `AOTS_AGENT` role to already exist.

### Step 5: Cost Tracking (`04_track_agent_costs.sql`)

Optional. Sets up views/queries to monitor agent query costs by warehouse.

---

## Usage

```sql
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

---

## Key Agent Behaviour Rules

- **Date normalisation**: all procedures use `RPAD(REGEXP_REPLACE(?, '[^0-9]', ''), 14, '0')` on every date WHERE clause — handles 8-digit, 14-digit, and ISO formats.
- **Calendar-day date resolution**: when user gives a date with no time (e.g. "28 October 2025"), agent must call `get_forecast_date_history` with N=6 and pick the LATEST entry on that calendar day — never assume 00Z.
- **Named facility tables**: `GET_HIGH_RISK_SCHOOLS` and `GET_HIGH_RISK_HEALTH_CENTERS` return up to 20 results. Agent must render the full Markdown table — do not describe the table, render it.
- **Context reuse prevention**: agent re-runs tools fresh every query even if date/storm appears in conversation history.
- **Full report requires storm + date**: if either is missing from a full_report query, agent must ask before calling any tools.
- **Provenance labels**: `data` / `inferred` — no brackets, after the value. Every table requires a table-level attribution line immediately below it.
- **Refusal must be clean and final**: one sentence stating what is out of scope, then stop. No alternatives or workarounds.

---

## Evaluations

Agent quality is measured with Snowflake Native Cortex Agent Evaluations. Dataset and views live in `05_create_eval_dataset.sql` and `05b_create_eval_views.sql`.

### Datasets

| Evalset name | View | Cases | Purpose |
|---|---|---|---|
| `HURRICANE_INTELLIGENCE_EVALSET` | `EVAL_ALL` | 75+ | Full baseline — run after any agent change |
| `HURRICANE_INTELLIGENCE_MIXED_EVALSET` | `EVAL_MIXED` | 11 | Quick cross-category regression check (1 per category) |
| `HURRICANE_INTELLIGENCE_SINGLE_METRICS_EVALSET` | `EVAL_SINGLE_METRICS` | 14 | Single-metric precision (all supported metrics) |
| `HURRICANE_INTELLIGENCE_FULL_REPORTS_EVALSET` | `EVAL_FULL_REPORTS` | 5 | Five-section report quality |
| `HURRICANE_INTELLIGENCE_NAMED_FACILITIES_EVALSET` | `EVAL_NAMED_FACILITIES` | 5 | Schools and health center name lookups |
| `HURRICANE_INTELLIGENCE_CENTROID_SHIFT_EVALSET` | `EVAL_CENTROID_SHIFT` | 3 | Geographic impact footprint shift queries |


---

## Data Layer — 6 Views in TC_ECMWF

All `*_RAW` views point to materialized `*_MAT` tables.

| View | Content | Procedures |
|---|---|---|
| `ADMIN_IMPACT_VIEWS_RAW` | Expected impact counts per admin area | GET_ADMIN_LEVEL_BREAKDOWN, GET_ADMIN_LEVEL_TREND_COMPARISON, GET_CENTROID_SHIFT |
| `MERCATOR_TILE_IMPACT_VIEWS_RAW` | Tile-level expected impact | GET_EXPECTED_IMPACT_VALUES, GET_SINGLE_METRIC, GET_WORST_CASE_SCENARIO, GET_SCENARIO_DISTRIBUTION, GET_ALL_WIND_THRESHOLDS_ANALYSIS, GET_PREVIOUS_FORECAST_DATE, GET_FORECAST_DATE_HISTORY |
| `TRACK_VIEWS_RAW` | Per-ensemble-member severity scores | GET_WORST_CASE_SCENARIO, GET_SCENARIO_DISTRIBUTION |
| `BASE_ADMIN_VIEWS_RAW` | Admin ID → name lookup | Joined by admin-level procedures |
| `SCHOOL_IMPACT_VIEWS_RAW` | Individual named schools with probability | GET_HIGH_RISK_SCHOOLS |
| `HEALTH_CENTER_IMPACT_VIEWS_RAW` | Individual named health facilities with probability | GET_HIGH_RISK_HEALTH_CENTERS |

`GET_CENTROID_SHIFT` additionally queries `BASE_ADMIN_GEOM_MAT` for centroid coordinates and `ADMIN_ALL_IMPACT_MAT` for per-admin impact values.
