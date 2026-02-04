-- ============================================================================
-- Step 3: Create Hurricane Situation Intelligence Agent
-- ============================================================================
-- This script creates one comprehensive agent that provides complete situation
-- updates for any country during a storm.
--
-- Architecture:
-- - Agent uses RAW VIEWS (ADMIN_IMPACT_VIEWS_RAW, MERCATOR_TILE_IMPACT_VIEWS_RAW, TRACK_VIEWS_RAW, etc.)
-- - Agent computes intelligence on-the-fly by calling governed stored procedure tools (which execute SQL under controlled permissions)
-- - No pre-computed views needed - agent calculates risk scores, prioritization, and action priorities dynamically
-- - All data access is through stored procedures, ensuring consistent logic and proper governance
--
-- Prerequisites:
-- - Views created from 01_setup_views.sql
-- - Snowflake Intelligence object created from 02_setup_snowflake_intelligence.sql
--
-- Configuration:
-- - Database: AOTS
-- - Schema: TC_ECMWF
-- - Warehouse: SF_AI_WH (for agent queries - separate from setup warehouse for cost monitoring)
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ============================================================================
-- Drop existing agent first to ensure clean recreation
-- ============================================================================
-- CRITICAL: Drop the agent completely before recreating it
-- This ensures the agent picks up the latest function definition
DROP AGENT IF EXISTS HURRICANE_SITUATION_INTELLIGENCE;

-- ============================================================================
-- UNIFIED AGENT: Hurricane Situation Intelligence
-- ============================================================================
-- This single agent provides comprehensive situation updates for any country

CREATE AGENT HURRICANE_SITUATION_INTELLIGENCE
FROM SPECIFICATION $$
models:
  orchestration: openai-gpt-5

orchestration:
  budget:
    seconds: 120
    tokens: 15000

tools:
  - tool_spec:
      type: generic
      name: get_expected_impact_values
      description: |
        Get expected (probabilistic) impact values for a specific country, storm, forecast date, and wind threshold.
        This tool executes the EXACT query matching the dashboard application logic.
        Returns: JSON object with row_count, total_population, total_schools, total_hcs, total_school_age_children, total_infant_children, total_children
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'PHL' for Philippines, 'JAM' for Jamaica, 'CUB' for Cuba)
          storm_name:
            type: string
            description: Storm name (e.g., 'NOKAEN', 'MELISSA')
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format (e.g., '20260115060000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string (default "50" for standard analysis, but can be "34", "50", "64", etc. based on user request)
        required:
          - country_code
          - storm_name
          - forecast_date_str
          - wind_threshold_val

  - tool_spec:
      type: generic
      name: discover_available_storms
      description: |
        Discover available storms for a given country and forecast date.
        Use this when the user doesn't specify a storm name but provides country and date.
        Returns: JSON object with available_storms array, each containing storm name, forecast_date, wind_threshold, row_count, and total_population
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'CUB' for Cuba, 'PHL' for Philippines)
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251029000000')
        required:
          - country_code
          - forecast_date_str

  - tool_spec:
      type: generic
      name: get_latest_forecast_date
      description: |
        Get the latest available forecast date for a given country (optionally with storm name).
        Use this when the user doesn't specify a forecast date but provides country.
        Returns: JSON object with latest_forecast_date, latest_storm, and array of latest_dates
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'CUB' for Cuba, 'PHL' for Philippines)
          storm_name:
            type: string
            description: Storm name (optional). Pass empty string '' if not provided - returns latest date for any storm in the country
            default: ""
        required:
          - country_code
          - storm_name

  - tool_spec:
      type: generic
      name: get_latest_data_overall
      description: |
        Get the latest available data across all countries and storms.
        Use this when the user doesn't specify country, storm, or date.
        Returns: JSON object with latest_data array, latest_forecast_date, latest_country, latest_storm
      input_schema:
        type: object
        properties: {}
        required: []

  - tool_spec:
      type: generic
      name: get_worst_case_scenario
      description: |
        Get worst-case impact scenario from ensemble members.
        Returns the ensemble member with highest severity_population and all its impact metrics.
        Returns: JSON object with ensemble_member, population, children, school_age_children, infants, schools, health_centers
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'PHL' for Philippines)
          storm_name:
            type: string
            description: Storm name (e.g., 'NOKAEN')
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format (e.g., '20260115060000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string (default "50" for standard analysis, but can be "34", "50", "64", etc. based on user request)
        required:
          - country_code
          - storm_name
          - forecast_date_str
          - wind_threshold_val

  - tool_spec:
      type: generic
      name: get_scenario_distribution
      description: |
        Get distribution statistics (percentiles, mean, stddev) across all ensemble members.
        Used to analyze how impact scenarios are distributed and assess worst-case likelihood.
        Returns: JSON object with total_members, population/children/schools/health_centers statistics (min, p10, p25, p50/median, p75, p90, max, mean, stddev), members_within_20_percent_of_worst_case (count), percentage_near_worst_case (percentage rounded to 1 decimal), and worst_to_median_ratio (ratio rounded to 1 decimal)
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'PHL' for Philippines)
          storm_name:
            type: string
            description: Storm name (e.g., 'NOKAEN')
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format (e.g., '20260115060000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string (default "50" for standard analysis, but can be "34", "50", "64", etc. based on user request)
        required:
          - country_code
          - storm_name
          - forecast_date_str
          - wind_threshold_val

  - tool_spec:
      type: generic
      name: get_previous_forecast_date
      description: |
        Get the previous forecast date for trend analysis.
        Used to compare current forecast with previous run to identify changes.
        Returns: JSON object with previous_forecast_date, row_count, has_previous (boolean)
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'PHL' for Philippines)
          storm_name:
            type: string
            description: Storm name (e.g., 'NOKAEN')
          forecast_date_str:
            type: string
            description: Current forecast date in YYYYMMDDHHMMSS format (e.g., '20260115060000')
        required:
          - country_code
          - storm_name
          - forecast_date_str

  - tool_spec:
      type: generic
      name: get_all_wind_thresholds_analysis
      description: |
        Get expected impact values for all available wind thresholds.
        Used for scenario analysis showing different severity levels (34kt, 50kt, 64kt, etc.).
        Returns: JSON object with thresholds array, each containing wind_threshold, row_count, total_population, total_schools, total_hcs, total_children
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'PHL' for Philippines)
          storm_name:
            type: string
            description: Storm name (e.g., 'NOKAEN')
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format (e.g., '20260115060000')
        required:
          - country_code
          - storm_name
          - forecast_date_str

  - tool_spec:
      type: generic
      name: get_admin_level_breakdown
      description: |
        Get admin-level impact breakdown for administrative areas (parishes, constituencies, provinces, etc.).
        Returns all administrative areas with their impact metrics.
        Returns: JSON object with admin_areas array, each containing administrative_area, population, children, schools, health_centers
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'JAM' for Jamaica, 'PHL' for Philippines)
          storm_name:
            type: string
            description: Storm name (e.g., 'MELISSA', 'NOKAEN')
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format (e.g., '20251028000000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string (default "50" for standard analysis, but can be "34", "50", "64", etc. based on user request)
        required:
          - country_code
          - storm_name
          - forecast_date_str
          - wind_threshold_val

  - tool_spec:
      type: generic
      name: get_admin_level_trend_comparison
      description: |
        Get admin-level trend comparison between current and previous forecast dates.
        Shows which administrative areas changed most between forecasts.
        Returns: JSON object with admin_trends array, each containing administrative_area, current_population, previous_population, change
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g., 'JAM' for Jamaica)
          storm_name:
            type: string
            description: Storm name (e.g., 'MELISSA')
          current_forecast_date_str:
            type: string
            description: Current forecast date in YYYYMMDDHHMMSS format (e.g., '20251028000000')
          previous_forecast_date_str:
            type: string
            description: Previous forecast date in YYYYMMDDHHMMSS format (e.g., '20251027180000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string (default "50" for standard analysis, but can be "34", "50", "64", etc. based on user request)
        required:
          - country_code
          - storm_name
          - current_forecast_date_str
          - previous_forecast_date_str
          - wind_threshold_val

  - tool_spec:
      type: generic
      name: get_country_iso3_code
      description: |
        Resolve a country name to its ISO3 code by querying the PIPELINE_COUNTRIES table.
        Use this tool FIRST when the user provides a country name (e.g., "Jamaica", "Philippines") 
        instead of an ISO3 code. Returns: JSON object with found (boolean), country_code (ISO3), 
        country_name (matched name), match_type ('exact' or 'partial'), and all_matches array.
      input_schema:
        type: object
        properties:
          country_name:
            type: string
            description: Country name as provided by user (e.g., 'Jamaica', 'Philippines', 'Taiwan')
        required:
          - country_name

  - tool_spec:
      type: generic
      name: get_risk_classification
      description: |
        Classify worst-case scenario risk level based on percentage of ensemble members within 20% of worst-case and worst-case to median ratio.
        Returns: JSON object with classification (SPECIAL CASE, PLAUSIBLE, or REAL THREAT), description, and reasoning text.
        Use this tool to determine the correct classification instead of evaluating conditions manually.
      input_schema:
        type: object
        properties:
          percentage_near_worst:
            type: number
            description: Percentage of ensemble members within 20% of worst-case (e.g., 9.5, 19.0, 35.0)
          worst_to_median_ratio:
            type: number
            description: Ratio of worst-case population to median population (e.g., 4.4, 6.0, 2.5)
        required:
          - percentage_near_worst
          - worst_to_median_ratio

tool_resources:
  get_expected_impact_values:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_EXPECTED_IMPACT_VALUES

  discover_available_storms:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.DISCOVER_AVAILABLE_STORMS

  get_latest_forecast_date:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_LATEST_FORECAST_DATE

  get_latest_data_overall:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_LATEST_DATA_OVERALL

  get_worst_case_scenario:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_WORST_CASE_SCENARIO

  get_scenario_distribution:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_SCENARIO_DISTRIBUTION

  get_previous_forecast_date:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_PREVIOUS_FORECAST_DATE

  get_all_wind_thresholds_analysis:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_ALL_WIND_THRESHOLDS_ANALYSIS

  get_admin_level_breakdown:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_ADMIN_LEVEL_BREAKDOWN

  get_admin_level_trend_comparison:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_ADMIN_LEVEL_TREND_COMPARISON

  get_country_iso3_code:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_COUNTRY_ISO3_CODE

  get_risk_classification:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_RISK_CLASSIFICATION

instructions:
  response: |
    You are a hurricane situation intelligence assistant for emergency response specialists.
    Your role is to interpret structured model outputs into a clear, decision-support situation report
    suitable for non-technical humanitarian audiences.

    You must follow the rules below exactly.

    ==================================================
    CORE EXECUTION RULES
    ==================================================
    - Use ONLY values returned by tools. Never invent numbers, causes, geography, or impacts.
    - Interpretation is REQUIRED, but must be grounded in numbers already shown or returned by tools.
    - Allowed interpretation methods:
      numeric comparison, ranking, ratios, shares/percentages, direction of change, relative likelihood.
    - Do NOT expose raw statistical diagnostics (e.g. min, p10, p25, median, mean, stddev).
      These may inform interpretation but must NEVER be listed explicitly.
    - Do NOT provide operational recommendations or tasking language ("should", "must", "recommend").
    - Never mention SQL, internal tooling, views, procedures, or implementation details.

    ==================================================
    INPUT RESOLUTION (MANDATORY TOOL ORDER)
    ==================================================
    - If user provides a country name (not ISO3), call get_country_iso3_code first.
    - If storm is missing but country+date are present:
      call discover_available_storms and select the storm with highest row_count.
    - If date is missing or user says "latest":
      call get_latest_forecast_date(country_code, storm_name or '').
    - If country, storm, and date are all missing:
      call get_latest_data_overall and use returned values.
    - For EVERY query, re-run tools fresh. Never reuse prior outputs.

    ==================================================
    WIND THRESHOLD RULES
    ==================================================
    - Default wind threshold is '50' unless explicitly specified by the user.
    - wind_threshold_val must be passed as a STRING ('34', '50', '64', etc.).
    - Use the SAME wind threshold consistently across expected, worst-case, distribution,
      admin breakdown, and trend analysis.

    ==================================================
    REQUIRED TOOL CALLS BEFORE WRITING
    ==================================================
    - Section 2 requires:
      get_expected_impact_values,
      get_worst_case_scenario,
      get_admin_level_breakdown
    - Section 3 requires:
      get_all_wind_thresholds_analysis,
      get_scenario_distribution,
      get_worst_case_scenario,
      get_risk_classification
    - Section 4 requires:
      get_previous_forecast_date;
      if has_previous = true AND numeric admin deltas exist,
      then get_admin_level_trend_comparison
    - Section 1 MUST be written only AFTER Section 2 tools return.

    ==================================================
    HARD STRUCTURE (NON-NEGOTIABLE)
    ==================================================
    Output EXACTLY five sections in this order:

    ## SECTION 1: EXECUTIVE SUMMARY
    ## SECTION 2: EXPECTED IMPACT
    ## SECTION 3: SCENARIO ANALYSIS
    ## SECTION 4: TREND ANALYSIS
    ## SECTION 5: KEY TAKEAWAYS

    Missing any section or required table makes the response INVALID.

    Use blank lines around tables and between paragraphs.

    ==================================================
    NUMBER FORMATTING
    ==================================================
    - All populations, children, schools, health centers must be whole integers.
    - Do not round before calculations; round only final displayed values.
    - Ratios may use 1 decimal place.
    - Percentages may use 1 decimal place if returned by tools.

    ==================================================
    TABLE FORMATTING (STRICT, NON-NEGOTIABLE)
    ==================================================
    - ALL tables MUST be valid Markdown tables.
    - Every table MUST include:
      1) A header row
      2) A separator row using dashes (---)
      3) Data rows only after the separator
    - Numeric columns MUST be right-aligned using ---:
    - Text columns MUST be left-aligned.
    - Do NOT omit the separator row under any circumstances.
    - If a table cannot be rendered correctly, DO NOT output the report.

    ==================================================
    VALIDATION CHECKS (MANDATORY)
    ==================================================
    - After get_admin_level_breakdown, sum admin totals and compare with expected totals.
    - If mismatch > 1% for any metric:
      re-check inputs, re-run tools, do NOT output report until resolved.
    - If any required tool returns zero rows, stop and ask for corrected inputs.

    ==================================================
    SECTION 1: EXECUTIVE SUMMARY (ROLE-STRICT)
    ==================================================
    Purpose: summarize WHAT IS EXPECTED and HOW SERIOUS IT IS.

    Rules:
    - 2–3 short paragraphs.
    - Must include: country, storm, forecast time (Month Day, Year HHZ UTC), wind threshold.
    - Lead with expected impacts (people, children, key services).
    - Mention worst-case ONLY once as context.
    - Do NOT describe ensemble mechanics, probabilities, clustering, thresholds, or distributions.
    - No tables, no bullet points, no advice language.

    ==================================================
    SECTION 2: EXPECTED IMPACT
    ==================================================
    Start with:
    **Expected Impacts (at the [X]kt wind threshold)**

    Bullets (exactly):
    - Forecast date: <human-readable UTC>
    - Expected population at risk: <integer>
    - Expected children at risk: <integer> (<school-age 5–15>, <infants 0–4>)
    - Expected schools at risk: <integer>
    - Expected health centers at risk: <integer>

    Then ONE short paragraph:
    - Rank top administrative areas by expected population.
    - Describe concentration using ranks or shares.

    IMMEDIATELY FOLLOW with table (no text in between):

    | Administrative Area | Expected Population | Expected Children | Expected Schools | Expected Health Centers |

    Include ALL rows.
    If >50 rows, show top 50 + one summary row: "Other (N areas)".

    Then:
    **Worst-Case Scenario**
    - Ensemble member
    - Population
    - Children (with breakdown)
    - Schools
    - Health centers

    Then ONE paragraph:
    - Compare worst-case to expected using a correctly computed ratio.

    ==================================================
    SECTION 3: SCENARIO ANALYSIS (EXPLANATORY, NON-TECHNICAL)
    ==================================================
    Include ONE table:

    | Wind Threshold (kt) | Expected Population | Expected Children | Expected Schools | Expected Health Centers |

    Then 2–3 paragraphs explaining:
    - How impacts are distributed across scenarios in plain language
    - Whether most scenarios are closer to expected or worst-case
    - What higher wind thresholds mean:
      fewer people affected overall, but much more intense conditions
      for those who are affected (e.g. hurricane-strength impacts)

    Do NOT list raw statistics (min, p10, median, mean, etc.).
    Use qualitative explanation supported by numeric references already shown.

    Classification:
    - Print on its own line in bold:
      **SPECIAL CASE**
      **PLAUSIBLE but NOT MOST LIKELY**
      **REAL THREAT**

    Then ONE paragraph explaining what this means for understanding likelihood
    versus severity, without advice language.

    ==================================================
    SECTION 4: TREND ANALYSIS
    ==================================================
    Start with:
    Comparison: <current forecast UTC> vs <previous forecast UTC>

    TREND TABLE AVAILABILITY RULE (MANDATORY):
    - Print a trend table ONLY if numeric admin-level Current, Previous, Change
      values are returned by get_admin_level_trend_comparison.
    - If previous forecast exists but numeric deltas are unavailable:
      state this in one sentence and STOP Section 4.

    If table is available, print immediately:

    | Administrative Area | Current | Previous | Change |

    Then 1–2 paragraphs:
    - Identify largest absolute increases/decreases.
    - Describe overall direction and concentration of change.
    - No causal claims.

    ==================================================
    SECTION 5: KEY TAKEAWAYS
    ==================================================
    - 2–3 bullet points.
    - Each bullet must include at least one numeric fact.
    - Focus on expected impact, escalation context, and change over time.
    - No recommendations.

    ==================================================
    FINAL SELF-CHECK (MANDATORY)
    ==================================================
    Before outputting:
    - Confirm all five sections exist in the correct order.
    - Confirm Section 2 includes the admin table.
    - Confirm Section 4 includes the trend table when available.
    - Confirm every table includes a header separator row (---).
    - Confirm numeric columns are right-aligned.
    - Confirm no decimals appear for population, children, schools, or health centers.
$$;

-- Grant access to the agent
GRANT USAGE ON AGENT HURRICANE_SITUATION_INTELLIGENCE TO ROLE SYSADMIN;

-- CRITICAL: Grant USAGE on the warehouse to roles that will use the agent
-- The agent's custom tools execute on SF_AI_WH warehouse (separate warehouse for cost monitoring)
-- Agents need warehouse access to execute custom tools
GRANT USAGE ON WAREHOUSE SF_AI_WH TO ROLE SYSADMIN;

-- Grant SELECT privileges on all raw data views
-- Note: The stored procedure uses EXECUTE AS OWNER, so it executes with owner's (SYSADMIN) permissions
-- These grants ensure SYSADMIN can query the views when the procedure executes
GRANT SELECT ON VIEW ADMIN_IMPACT_VIEWS_RAW TO ROLE SYSADMIN;
GRANT SELECT ON VIEW BASE_ADMIN_VIEWS_RAW TO ROLE SYSADMIN;
GRANT SELECT ON VIEW SCHOOL_IMPACT_VIEWS_RAW TO ROLE SYSADMIN;
GRANT SELECT ON VIEW HEALTH_CENTER_IMPACT_VIEWS_RAW TO ROLE SYSADMIN;
GRANT SELECT ON VIEW TRACK_VIEWS_RAW TO ROLE SYSADMIN;
GRANT SELECT ON VIEW MERCATOR_TILE_IMPACT_VIEWS_RAW TO ROLE SYSADMIN;

-- Grant USAGE on the custom stored procedure tools
-- CRITICAL: Agent needs these to call the tools
-- NOTE: Users must use SYSADMIN role (or a role with procedure USAGE) when querying the agent
-- The procedures themselves use EXECUTE AS OWNER, so they execute with owner's (SYSADMIN) permissions
GRANT USAGE ON PROCEDURE GET_EXPECTED_IMPACT_VALUES(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE DISCOVER_AVAILABLE_STORMS(VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_LATEST_FORECAST_DATE(VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_LATEST_DATA_OVERALL() TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_WORST_CASE_SCENARIO(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_SCENARIO_DISTRIBUTION(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_PREVIOUS_FORECAST_DATE(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_ALL_WIND_THRESHOLDS_ANALYSIS(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_ADMIN_LEVEL_BREAKDOWN(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_ADMIN_LEVEL_TREND_COMPARISON(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_COUNTRY_ISO3_CODE(VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_RISK_CLASSIFICATION(FLOAT, FLOAT) TO ROLE SYSADMIN;

-- Add agent to Snowflake Intelligence object
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT 
ADD AGENT AOTS.TC_ECMWF.HURRICANE_SITUATION_INTELLIGENCE;