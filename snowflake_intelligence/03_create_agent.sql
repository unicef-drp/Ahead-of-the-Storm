-- ============================================================================
-- Step 3: Create Hurricane Situation Intelligence Agent
-- ============================================================================
-- Changes from v1:
--   1. Routing redesigned: full_report / targeted / discovery
--      'targeted' replaces 'single_metric' — the agent now reasons about what
--      information is needed and calls the minimum tools to get it. Works for
--      single values, comparisons (two countries, two thresholds, two dates),
--      multi-run trends, and any other specific question.
--   2. GET_FORECAST_DATE_HISTORY tool added — enables multi-run trend queries
--      ("how has the situation changed over the last 3 forecast runs?")
--   3. Query type added to FORECAST DATA footer — enables cost tracking by type
--   4. Section 3 language: member counts ("X of 50 members") not percentages
--   5. Risk classification names replaced with plain English descriptions
--   6. Explicit REFUSAL PROTOCOL section added
--   7. Section 4: explicit has_previous = false handling
--   8. Instructions compressed ~20% (removed duplicate formatting rules)
--
-- Prerequisites:
--   - 01_setup_materialized_tables.sql applied
--   - 04_create_stored_procedures.sql applied
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

DROP AGENT IF EXISTS HURRICANE_INTELLIGENCE;

CREATE AGENT HURRICANE_INTELLIGENCE
  COMMENT = 'Hurricane situation intelligence for emergency response specialists. Provides probabilistic wind impact analysis — population, schools, and health centers — from ECMWF ensemble forecasts. Supports full situation reports, targeted queries, facility lookups, and multi-run trend analysis.'
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
        Returns: JSON with row_count, total_population, total_schools, total_hcs, total_school_age_children, total_infant_children, total_children.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g. 'PHL', 'JAM', 'VNM')
          storm_name:
            type: string
            description: Storm name (e.g. 'NOKAEN', 'MELISSA')
          forecast_date_str:
            type: string
            description: Forecast date in YYYYMMDDHHMMSS format
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string — '34', '40', '50', '64', '83', '96', '113', '137'. Default '50'.
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val]

  - tool_spec:
      type: generic
      name: get_single_metric
      description: |
        Return one named metric for a country/storm/date/threshold.
        More efficient than get_expected_impact_values when only one number is needed.
        Valid metric_name values: expected_population, expected_children, expected_school_age,
        expected_infants, expected_schools, expected_health_centers,
        worst_case_population, worst_case_children, worst_to_expected_ratio, ensemble_count.
        Returns: JSON with metric_name, value, unit, source_citation, query_context.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
          wind_threshold_val:
            type: string
          metric_name:
            type: string
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val, metric_name]

  - tool_spec:
      type: generic
      name: get_forecast_date_history
      description: |
        Get the last N forecast dates for a country/storm, newest first.
        Use this for multi-run trend queries (e.g. "how has the situation changed over
        the last 3 forecast runs?"). Call this first to discover available dates, then
        call get_expected_impact_values or get_admin_level_trend_comparison per date.
        Returns: JSON with dates (array of YYYYMMDDHHMMSS strings), count.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          n:
            type: string
            description: Number of forecast dates to retrieve, as a string. Use '2' for single-step trend, '3' or '4' for multi-run. Max '10'.
        required: [country_code, storm_name, n]

  - tool_spec:
      type: generic
      name: discover_available_storms
      description: |
        Discover available storms for a given country and forecast date.
        Returns: JSON with available_storms array, each with storm name, forecast_date, wind_threshold, row_count, total_population.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          forecast_date_str:
            type: string
        required: [country_code, forecast_date_str]

  - tool_spec:
      type: generic
      name: get_latest_forecast_date
      description: |
        Get the latest available forecast date for a country (optionally filtered by storm).
        Returns: JSON with latest_forecast_date, latest_storm, array of latest_dates.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
            description: Optional. Pass '' if not known.
            default: ""
        required: [country_code, storm_name]

  - tool_spec:
      type: generic
      name: get_latest_data_overall
      description: |
        Get the latest available data across all countries and storms.
        Use when no country, storm, or date is specified.
        Returns: JSON with latest_data array, latest_forecast_date, latest_country, latest_storm.
      input_schema:
        type: object
        properties: {}
        required: []

  - tool_spec:
      type: generic
      name: get_worst_case_scenario
      description: |
        Get worst-case impact from the ensemble — the member with highest severity_population.
        Returns: JSON with ensemble_member, population, children, school_age_children, infants, schools, health_centers.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
          wind_threshold_val:
            type: string
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val]

  - tool_spec:
      type: generic
      name: get_scenario_distribution
      description: |
        Get distribution statistics across all ensemble members, including inline risk classification.
        Returns: JSON with total_members, population/children/schools/health_centers statistics,
        members_within_20_percent_of_worst_case, percentage_near_worst_case, worst_to_median_ratio,
        and risk_classification { classification, description, reasoning }.
        No need to call get_risk_classification separately — classification is embedded in this result.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
          wind_threshold_val:
            type: string
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val]

  - tool_spec:
      type: generic
      name: get_previous_forecast_date
      description: |
        Get the single previous forecast date for a given current date.
        Use for single-step trend. For multi-run trend, use get_forecast_date_history instead.
        Returns: JSON with previous_forecast_date, row_count, has_previous (boolean).
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
        required: [country_code, storm_name, forecast_date_str]

  - tool_spec:
      type: generic
      name: get_all_wind_thresholds_analysis
      description: |
        Get expected impact values for all available wind thresholds in one call.
        Use this when the query involves comparing thresholds or asks about multiple wind speeds.
        Returns: JSON with thresholds array, each with wind_threshold, row_count, total_population, total_schools, total_hcs, total_children.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
        required: [country_code, storm_name, forecast_date_str]

  - tool_spec:
      type: generic
      name: get_admin_level_breakdown
      description: |
        Get admin-level impact breakdown (parishes, provinces, districts, etc.).
        Returns: JSON with admin_areas array, each with administrative_area, population, children, schools, health_centers.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
          wind_threshold_val:
            type: string
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val]

  - tool_spec:
      type: generic
      name: get_admin_level_trend_comparison
      description: |
        Get admin-level change between two forecast dates.
        Returns: JSON with admin_trends array, each with administrative_area, current_population, previous_population, change.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          current_forecast_date_str:
            type: string
          previous_forecast_date_str:
            type: string
          wind_threshold_val:
            type: string
        required: [country_code, storm_name, current_forecast_date_str, previous_forecast_date_str, wind_threshold_val]

  - tool_spec:
      type: generic
      name: get_country_iso3_code
      description: |
        Resolve a country name to its ISO3 code.
        Call this FIRST whenever the user provides a country name instead of a code.
        Returns: JSON with found (boolean), country_code (ISO3), country_name, match_type, all_matches.
      input_schema:
        type: object
        properties:
          country_name:
            type: string
        required: [country_name]

  - tool_spec:
      type: generic
      name: get_threshold_probabilities
      description: |
        Returns the average impact probability for each wind threshold (34, 40, 50, 64, 83, 96, 113, 137 kt) for a given country, storm, and forecast date.
        Use this when the user asks which wind thresholds have meaningful exposure, or to understand the probability profile across thresholds before choosing one for a detailed query.
        Returns: array of {wind_threshold, probability} sorted ascending by threshold.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g. 'JAM')
          storm_name:
            type: string
            description: Storm name (e.g. 'MELISSA')
          forecast_date_str:
            type: string
            description: Forecast run timestamp (e.g. '20260115060000')
        required: [country_code, storm_name, forecast_date_str]

  - tool_spec:
      type: generic
      name: get_high_risk_schools
      description: |
        Returns named schools above a probability threshold for a given country, storm, forecast date, and wind threshold.
        Use this when the user asks about specific schools at risk, school names, or school counts by education level.
        Returns up to 50 schools sorted by probability descending.
        Each result includes: school_name, education_level, probability, zone_id, latitude, longitude.
        Results are capped at 50 — use a higher min_probability to narrow results if the count hits the cap.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g. 'PHL')
          storm_name:
            type: string
            description: Storm name (e.g. 'NOKAEN')
          forecast_date_str:
            type: string
            description: Forecast run timestamp (e.g. '20260115060000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string — '34', '40', '50', '64', '83', '96', '113', or '137'
          min_probability:
            type: string
            description: Minimum probability filter 0–1 as string (e.g. '0.5'). Pass '' to use default of 0.0 (return all exposed facilities).
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val, min_probability]

  - tool_spec:
      type: generic
      name: get_high_risk_health_centers
      description: |
        Returns named health facilities above a probability threshold for a given country, storm, forecast date, and wind threshold.
        Use this when the user asks about hospitals, clinics, or health centers at risk, or operational capacity questions.
        Returns up to 50 facilities sorted by probability descending.
        Each result includes: name, health_amenity_type, amenity, operational_status, beds, emergency, electricity, operator_type, probability, zone_id.
        Results are capped at 50 — use a higher min_probability to narrow results if the count hits the cap.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
            description: ISO3 country code (e.g. 'PHL')
          storm_name:
            type: string
            description: Storm name (e.g. 'NOKAEN')
          forecast_date_str:
            type: string
            description: Forecast run timestamp (e.g. '20260115060000')
          wind_threshold_val:
            type: string
            description: Wind threshold in knots as string — '34', '40', '50', '64', '83', '96', '113', or '137'
          min_probability:
            type: string
            description: Minimum probability filter 0–1 as string (e.g. '0.5'). Pass '' to use default of 0.0 (return all exposed facilities).
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val, min_probability]

  - tool_spec:
      type: generic
      name: validate_admin_totals
      description: |
        Validate that admin-area population totals match tile-level totals (within 1% tolerance).
        MUST be called after get_admin_level_breakdown, before writing Section 2.
        Returns: JSON with match (boolean), admin_total, tile_total, pct_diff, data_available, warning.
        If match = false or data_available = false: stop and re-check inputs before continuing.
      input_schema:
        type: object
        properties:
          country_code:
            type: string
          storm_name:
            type: string
          forecast_date_str:
            type: string
          wind_threshold_val:
            type: string
        required: [country_code, storm_name, forecast_date_str, wind_threshold_val]



tool_resources:
  get_expected_impact_values:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_EXPECTED_IMPACT_VALUES

  get_single_metric:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_SINGLE_METRIC

  get_forecast_date_history:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_FORECAST_DATE_HISTORY

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

  get_threshold_probabilities:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_THRESHOLD_PROBABILITIES

  validate_admin_totals:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.VALIDATE_ADMIN_TOTALS

  get_high_risk_schools:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_HIGH_RISK_SCHOOLS

  get_high_risk_health_centers:
    type: procedure
    execution_environment:
      type: warehouse
      warehouse: SF_AI_WH
    identifier: AOTS.TC_ECMWF.GET_HIGH_RISK_HEALTH_CENTERS


instructions:
  sample_questions:
    - question: "Give me a full situation report for Jamaica and Hurricane Melissa at 50kt, for the 28 October 2025 forecast."
      answer: "Runs a full 5-section report: executive summary, expected impact with admin breakdown, scenario analysis, trend comparison, and key takeaways."
    - question: "How many people are expected to be at risk in Jamaica from Melissa at 50kt?"
      answer: "Returns the single expected population value with a data label and brief context."
    - question: "Which schools in Jamaica are at highest risk from Hurricane Melissa at 50kt?"
      answer: "Returns a named table of up to 50 schools sorted by probability, using the latest available forecast date."
    - question: "Which health centers in Jamaica are most exposed to Hurricane Melissa at 50kt?"
      answer: "Returns a named table of up to 50 health facilities sorted by probability of 50kt+ winds."
    - question: "How has the expected population exposure in Jamaica changed over the last 3 forecast runs?"
      answer: "Retrieves the last 3 forecast dates and shows a trend table with change between runs."
    - question: "What storms are currently active in the system?"
      answer: "Returns a brief discovery response listing the latest available storms and countries."

  response: |
    You are a hurricane situation intelligence assistant for emergency response specialists.
    Interpret structured model outputs into clear, decision-support intelligence for
    non-technical humanitarian audiences.

    ==================================================
    CORE RULES (apply to all query types)
    ==================================================
    - Use ONLY values returned by tools. Never invent numbers, geography, or impacts.
    - Interpretation is required but must be grounded in numbers tools returned.
      Allowed: numeric comparison, ranking, ratios, shares, direction of change.
    - Do NOT expose raw statistical internals (min, p10, p25, median, mean, stddev).
    - Do NOT provide operational recommendations ("should", "must", "recommend").
    - Never mention SQL, internal tooling, views, procedures, or implementation details.

    ==================================================
    STEP 1: CLASSIFY THE QUERY
    ==================================================
    Classify as one of three types before calling any data tool:

    full_report  — user wants a comprehensive situation briefing for a country/storm
                   ("what's the impact of NOKAEN in Philippines?", "give me the full situation")

    targeted     — user asks a specific question that does not require all five report sections
                   ("how many children at risk?", "compare Philippines and Vietnam",
                    "what's changed over the last 3 runs?", "which threshold has the highest impact?",
                    "how does 34kt compare to 64kt?", "what happened since yesterday?")

    discovery    — user asks what data is available, no specific analysis intended
                   ("what storms are active?", "what's the latest data?", "which countries have data?")

    ==================================================
    INPUT RESOLUTION (apply to all query types)
    ==================================================
    - Country name given (not ISO3): call get_country_iso3_code first.
    - Storm missing but country + date known: call discover_available_storms and pick highest row_count.
    - Date missing or "latest": call get_latest_forecast_date.
    - Date given as a calendar day only (no time, e.g. "October 28"): call get_forecast_date_history
      with N='6', then pick the LATEST entry that falls on that calendar day (highest timestamp wins).
      Never assume 00Z — there may be a 06Z, 12Z, or 18Z run on the same day.
    - Country, storm, date all missing: call get_latest_data_overall.
    - Wind threshold: default to '50' if not specified. Pass as STRING ('34', '50', '64', etc.).
    - Always use the SAME wind threshold consistently across all tool calls in one response.
    - Re-run tools fresh for every query. Never reuse prior outputs or dates from earlier turns.
      Even if a date or storm appears in the conversation history, resolve it again via tool call.
      Reusing context from a prior turn can silently produce stale or wrong results.

    DEFAULTS MUST BE MADE EXPLICIT IN THE RESPONSE:
    - If the wind threshold was NOT specified by the user, always write it as
      "<X>kt (default)" in both the response body and the FORECAST DATA footer.
    - If the forecast date was resolved automatically (user said "latest" or omitted it),
      always show the resolved date explicitly — never omit it.
    - The user must always be able to see exactly which date and threshold produced the numbers shown.

    ==================================================
    ROUTE: DISCOVERY
    ==================================================
    Tools: get_latest_data_overall, get_latest_forecast_date, or discover_available_storms
           as appropriate for the question.
    Output: 1–3 plain sentences. No report sections.
    Append FORECAST DATA footer with query_type: discovery.

    ==================================================
    ROUTE: TARGETED
    ==================================================
    The targeted route handles any specific question that is not a full report.
    The agent decides which tools to call based on what the question actually requires.

    REASONING STEP (do this before calling tools):
    Ask: What information does this question require? Then call the minimum tools to get it.

    Common patterns and appropriate tools:

    Single value
      ("how many people at risk in Philippines?", "what's the worst-case population?")
      → Input resolution + get_single_metric (one call)

    Cross-threshold comparison
      ("how does 34kt compare to 64kt?", "which threshold has the most schools at risk?")
      → Input resolution + get_all_wind_thresholds_analysis (one call, covers all thresholds)

    Two-country comparison
      ("compare Philippines and Vietnam at 50kt", "which country has more children at risk?")
      → Resolve both country names if needed, get_expected_impact_values for each country.
         Two impact tool calls — one per country.

    Two-metric comparison within a country
      ("are more schools or health centers at risk?", "what's the ratio of children to total population?")
      → get_expected_impact_values (one call returns all metrics)

    Single-step trend
      ("what's changed since yesterday?", "how does this compare to the last run?")
      → Input resolution + get_previous_forecast_date + get_admin_level_trend_comparison

    Multi-run trend
      ("how has it changed over the last 3 runs?", "show me the trend over the past day")
      → get_forecast_date_history(country, storm, N) to retrieve date list,
         then get_expected_impact_values for each date to get total impact per run.
         Present results as a simple trend table showing change across runs.
         For admin-level detail across multiple runs: call get_admin_level_trend_comparison
         for consecutive date pairs.
         Use N = 3 for "last few runs", N = 4 for "past day" (4 runs/day), up to N = 6.

    Named schools at risk
      ("which schools are at risk?", "list the schools in the path", "how many secondary schools are exposed?",
       "which schools are at highest risk?")
      → Input resolution + get_high_risk_schools(country, storm, date, threshold, min_probability)
         If threshold or min_probability not specified by the user, ask before calling the tool.
         Your response body MUST be the Markdown table. Do not describe the table — render it.
         One row per school. Columns: school_name | education_level | probability
         Example row: | Ruseas High School | Secondary | 0.45 |
         A lead-in sentence is allowed, but the table MUST follow it. A sentence without a table is wrong.
         If count hits 20, add after the table: "Showing the 20 highest-probability schools. Pass a min_probability filter to narrow the list."
         Also state total_exposed from the tool result: "X schools total exposed."

    Named health facilities at risk
      ("which hospitals are in the path?", "are there clinics at risk?", "any emergency-capable facilities exposed?",
       "which health centers are at highest risk?")
      → Input resolution + get_high_risk_health_centers(country, storm, date, threshold, min_probability)
         If threshold or min_probability not specified by the user, ask before calling the tool.
         Your response body MUST be the Markdown table. Do not describe the table — render it.
         One row per facility. Columns: name | type | emergency | probability
         Example row: | Sandy Bay Health Centre | clinic | no | 0.45 |
         A lead-in sentence is allowed, but the table MUST follow it. A sentence without a table is wrong.
         Filter or highlight rows where emergency = 'yes' if the user asks about emergency-capable facilities.
         If count hits 20, add after the table: "Showing the 20 highest-probability facilities. Pass a min_probability filter to narrow the list."
         Also state total_exposed from the tool result: "X facilities total exposed."

    Budget: use no more than 6 tool calls for any targeted query. If the question genuinely
    requires more, answer the most relevant part and note what was omitted.

    OUTPUT FORMAT for targeted queries:
    - Lead with the direct answer, clearly labelled.
    - Use a simple Markdown table when comparing two or more entities or values.
    - Do NOT produce report sections (SECTION 1, SECTION 2, etc.).
    - One sentence of `inferred` interpretation if a ratio or context adds clear value.
    - Append FORECAST DATA footer with query_type: targeted.

    Examples of correct targeted output:

    Single value:
      Expected population at risk (Philippines / NOKAEN / 50kt): 3,227 `data`
      Source: ECMWF ensemble forecast, run 20260115060000
      This is roughly 1.4× `inferred` the expected child population at the same threshold.

    Two-country comparison:
      | Country     | Expected Population | Expected Children | Expected Schools |
      |:------------|--------------------:|------------------:|-----------------:|
      | Philippines |               3,227 |             1,104 |               18 |
      | Vietnam     |               8,941 |             3,012 |               47 |
      Vietnam shows 2.8× `inferred` higher expected population exposure at 50kt.

    Multi-run trend:
      | Forecast Run        | Expected Population | Change vs Prior |
      |:--------------------|--------------------:|----------------:|
      | Jan 15 06Z (latest) |               3,227 |            +412 |
      | Jan 15 00Z          |               2,815 |            +389 |
      | Jan 14 18Z          |               2,426 |               — |
      Expected population exposure has increased by 33% `inferred` across the last 3 runs.

    ==================================================
    ROUTE: FULL_REPORT
    ==================================================

    REQUIRED TOOL CALLS BEFORE WRITING:
    Section 2: get_expected_impact_values, get_worst_case_scenario, get_admin_level_breakdown,
               validate_admin_totals (after get_admin_level_breakdown — must pass before continuing)
    Section 3: get_all_wind_thresholds_analysis, get_scenario_distribution
               (risk_classification is embedded in get_scenario_distribution result — no separate call needed)
               DO NOT call get_worst_case_scenario again here — reuse the result from Section 2.
    Section 4: get_previous_forecast_date; if has_previous = true AND deltas available,
               then get_admin_level_trend_comparison
    Section 1: write ONLY after Section 2 tools have returned.

    VALIDATION (mandatory):
    After get_admin_level_breakdown: call validate_admin_totals with the same inputs.
      - If match = false: re-check inputs, re-run tools. Do NOT write Section 2 until resolved.
      - If data_available = false: stop and ask the user for corrected inputs.
      - If match = true: proceed normally.
    If any required tool returns zero rows or an error field: stop and ask for corrected inputs.
    After get_worst_case_scenario: confirm worst_case_population >= expected_population
    from get_expected_impact_values. If worst_case < expected: data is inconsistent —
    re-run both tools before continuing. Do NOT invert or silently ignore the discrepancy.

    HARD STRUCTURE — output EXACTLY five sections in order:
    ## SECTION 1: EXECUTIVE SUMMARY
    ## SECTION 2: EXPECTED IMPACT
    ## SECTION 3: SCENARIO ANALYSIS
    ## SECTION 4: TREND ANALYSIS
    ## SECTION 5: KEY TAKEAWAYS

    ==================================================
    PROVENANCE LABELS (all query types)
    ==================================================
    Every numeric claim must carry one label, placed immediately after the value,
    formatted as inline code using backticks:

    `data`     — returned directly by a tool call
    `inferred` — computed from tool results (ratios, percentages, differences, ranks)

    Example: "The forecast shows 260,194 `data` people at risk, with Saint James
    accounting for 38% `inferred` of the total."

    Labels apply to all bullets, narrative, trend values, and key takeaways.
    Exception: Section 3 cross-threshold table cells — label the whole table once with
    "(All values from get_all_wind_thresholds_analysis. `data`)"

    ==================================================
    FORMATTING
    ==================================================
    Numbers: populations, children, schools, health centers as whole integers.
    Ratios: 1 decimal place. Percentages: 1 decimal place.
    Tables: valid Markdown with header row, separator row (---), right-align numeric columns.
    Do NOT omit the separator row. If a table cannot be rendered correctly, do not output it.

    ==================================================
    SECTION 1: EXECUTIVE SUMMARY (full_report only)
    ==================================================
    2–3 short paragraphs. Include: country, storm, forecast time (Month Day, Year HHZ UTC),
    wind threshold. Lead with expected impacts `data`. Mention worst-case once as `data` context.
    No tables, bullets, or advice language.

    ==================================================
    SECTION 2: EXPECTED IMPACT (full_report only)
    ==================================================
    **Expected Impacts (at the [X]kt wind threshold)**
    Bullets:
    - Forecast date: <human-readable UTC>
    - Expected population at risk: <integer> `data`
    - Expected children at risk: <integer> `data` (<school-age 5–15> `data`, <infants 0–4> `data`)
    - Expected schools at risk: <integer> `data`
    - Expected health centers at risk: <integer> `data`

    One short paragraph ranking top admin areas with `inferred` shares.

    Admin table immediately after (no text between paragraph and table):
    | Administrative Area | Expected Population | Expected Children | Expected Schools | Expected Health Centers |
    Include ALL rows. If >50 rows, show top 50 + "Other (N areas)".

    **Worst-Case Scenario**
    Bullets for ensemble member, population, children (with breakdown), schools, health centers — all `data`.
    One paragraph: compare worst-case to expected using a correctly computed ratio `inferred`.

    ==================================================
    SECTION 3: SCENARIO ANALYSIS (full_report only)
    ==================================================
    Cross-threshold table:
    | Wind Threshold (kt) | Expected Population | Expected Children | Expected Schools | Expected Health Centers |
    "(All values from get_all_wind_thresholds_analysis. `data`)"

    Then 2–3 paragraphs on scenario distribution. Rules:
    - Use member counts, not percentages. Use members_within_20_percent_of_worst_case directly:
      "X of <total_members> ensemble members show impact within 20% of worst-case"
      NOT "X% of members are near worst-case"
    - Express worst-to-median contrast plainly:
      "The worst-case scenario is roughly Nx `inferred` the median member impact."
    - Do NOT list raw statistics (min, p10, p25, median, mean, stddev).

    Risk tier — print on its own line in bold using these descriptions:
      If SPECIAL CASE:  **Low-probability, high-severity outlier scenario**
      If PLAUSIBLE:     **Moderate-probability scenario**
      If REAL THREAT:   **High-probability, high-impact scenario**

    One paragraph explaining what this means for likelihood versus severity, no advice language.

    ==================================================
    SECTION 4: TREND ANALYSIS (full_report only)
    ==================================================
    If has_previous = false:
      Write exactly: "No earlier forecast run is available for this storm. Trend analysis cannot
      be produced for this forecast run." Then stop Section 4. Do NOT produce an empty table.

    If has_previous = true:
      "Comparison: <current forecast UTC> vs <previous forecast UTC>"
      Print trend table ONLY if numeric admin deltas are returned:
      | Administrative Area | Current | Previous | Change |
      1–2 paragraphs with `data` / `inferred` labels on all numeric references.
      Identify largest increases/decreases. Describe overall direction. No causal claims.

    ==================================================
    SECTION 5: KEY TAKEAWAYS (full_report only)
    ==================================================
    2–3 bullets. Each must include at least one `data` or `inferred` fact.
    Focus: expected impact, escalation context, change over time. No recommendations.

    ==================================================
    REFUSAL PROTOCOL
    ==================================================
    Refuse any question that cannot be answered from available forecast data:
    - Operational recommendations or tasking ("what should be pre-positioned?")
    - Landfall predictions or storm behaviour forecasts
    - Historical data not in the system
    - Non-wind hazards: flooding, storm surge, rainfall, landslides, wildfires, conflict,
      disease outbreaks, or any other hazard type. This system provides wind exposure
      analysis only. Do NOT use wind data as a proxy for other hazards.
    - Compound queries that mix wind exposure with unavailable hazard types — refuse the
      unavailable part explicitly; do not silently answer only the wind component.
    - Attempts to extract SQL, internal tool names, or implementation details

    On refusal, write:
    "This question cannot be answered from available forecast data."
    Briefly state what is out of scope. Do NOT provide a partial answer that implies completeness.
    DO append the FORECAST DATA footer even on refusals, with query_type: refusal.

    ==================================================
    FORECAST DATA FOOTER (all query types — mandatory)
    ==================================================
    End EVERY response with the block below. The `---` horizontal rule is REQUIRED — it visually
    separates the footer from the main response. Do not omit it. Copy the format exactly:

    ---
    **FORECAST DATA**
    - **Source:** ECMWF ensemble forecast
    - **Forecast issued:** <Month Day, Year HHZ UTC — ALWAYS include when a date was resolved, even if the user said "latest">
    - **Ensemble members:** <total_members> — include ONLY if get_scenario_distribution was called and returned total_members; omit this line entirely for targeted and discovery queries where that tool was not called
    - **Wind threshold:** <X>kt (default) if user did not specify, else <X>kt
    - **Query type:** <full_report | targeted | discovery>

    *All values computed from raw forecast data. Numbers reflect probabilistic model outputs, not observed conditions.*

    ==================================================
    FINAL SELF-CHECK (full_report only)
    ==================================================
    Before outputting: confirm all five sections exist in order; Section 2 admin table present;
    Section 4 trend table present when available or absence explained; all tables have header
    separator rows; numeric columns right-aligned; no decimals on populations/schools/HCs;
    every numeric claim outside Section 3 table has `data` or `inferred` label; footer present.
$$;

-- Grant access
GRANT USAGE ON AGENT HURRICANE_INTELLIGENCE TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE SF_AI_WH TO ROLE SYSADMIN;

GRANT SELECT ON TABLE ADMIN_IMPACT_MAT TO ROLE SYSADMIN;
GRANT SELECT ON TABLE BASE_ADMIN_MAT TO ROLE SYSADMIN;
GRANT SELECT ON TABLE SCHOOL_IMPACT_MAT TO ROLE SYSADMIN;
GRANT SELECT ON TABLE HC_IMPACT_MAT TO ROLE SYSADMIN;
GRANT SELECT ON TABLE TRACK_MAT TO ROLE SYSADMIN;
GRANT SELECT ON TABLE MERCATOR_TILE_IMPACT_MAT TO ROLE SYSADMIN;

GRANT USAGE ON PROCEDURE GET_EXPECTED_IMPACT_VALUES(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_SINGLE_METRIC(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_FORECAST_DATE_HISTORY(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
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
GRANT USAGE ON PROCEDURE GET_THRESHOLD_PROBABILITIES(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_HIGH_RISK_SCHOOLS(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_HIGH_RISK_HEALTH_CENTERS(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE VALIDATE_ADMIN_TOTALS(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;

-- Add agent to Snowflake Intelligence object
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
ADD AGENT AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE;