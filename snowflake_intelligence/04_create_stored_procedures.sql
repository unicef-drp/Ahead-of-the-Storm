-- ============================================================================
-- Create Stored Procedures for Agent to Call
-- ============================================================================
-- These stored procedures execute the EXACT queries needed, ensuring
-- Cortex Analyst doesn't generate incorrect SQL.
--
-- Prerequisites:
-- - Materialized tables created from 01_setup_materialized_tables.sql
--
-- Configuration:
-- - Database: AOTS
-- - Schema: TC_ECMWF
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ============================================================================
-- Stored Procedure: Get Expected Impact Values
-- ============================================================================
-- Returns probabilistic expected impact totals for a given country, storm,
-- forecast date, and wind threshold. Reads from MERCATOR_TILE_IMPACT_MAT
-- at zoom level 14. These are the headline figures used in Section 2.
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'JAM')
--   storm_name         — Storm name (e.g. 'MELISSA')
--   forecast_date_str  — Forecast run timestamp (e.g. '20251028000000')
--   wind_threshold_val — Wind threshold in knots: '34', '50', '64', '96', or '137'
--
-- Returns: JSON object with
--   row_count                 — number of tiles matched
--   total_population          — expected population at risk
--   total_schools             — expected schools at risk (float; fractional counts are normal)
--   total_hcs                 — expected health centers at risk
--   total_school_age_children — expected school-age children (5–15) at risk
--   total_infant_children     — expected infant children (0–4) at risk
--   total_children            — total expected children at risk (school-age + infants)
--
-- Example:
--   GET_EXPECTED_IMPACT_VALUES('JAM', 'MELISSA', '20251028000000', '50')
--   → { total_population: 260194, total_schools: 142.3, total_hcs: 18.7, total_children: 52400, ... }

CREATE OR REPLACE PROCEDURE GET_EXPECTED_IMPACT_VALUES(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  // Convert wind_threshold_val from VARCHAR to number for SQL query
  var wind_threshold_num = parseInt(WIND_THRESHOLD_VAL);
  
  var sql_command = `
    SELECT 
        COUNT(*)::INT AS row_count,
        SUM(COALESCE(E_population, 0)) AS total_population,
        SUM(COALESCE(E_num_schools, 0)) AS total_schools,
        SUM(COALESCE(E_num_hcs, 0)) AS total_hcs,
        SUM(COALESCE(E_school_age_population, 0)) AS total_school_age_children,
        SUM(COALESCE(E_infant_population, 0)) AS total_infant_children,
        SUM(COALESCE(E_school_age_population, 0) + COALESCE(E_infant_population, 0)) AS total_children
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
    WHERE country = ?
      AND UPPER(storm) = UPPER(?)
      AND forecast_date = ?
      AND wind_threshold = ?
      AND zoom_level = 14
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
  });
  
  var result = stmt.execute();
  result.next();
  
  // Return as JSON object
  return {
    row_count: result.getColumnValue(1),
    total_population: result.getColumnValue(2),
    total_schools: result.getColumnValue(3),
    total_hcs: result.getColumnValue(4),
    total_school_age_children: result.getColumnValue(5),
    total_infant_children: result.getColumnValue(6),
    total_children: result.getColumnValue(7)
  };
$$;


-- ============================================================================
-- Helper Procedure: Discover Available Storms for Country/Date
-- ============================================================================
-- Returns available storms for a given country and forecast date.
-- Used when the user does not specify a storm name.
--
-- Parameters:
--   country_code      — ISO3 country code (e.g. 'JAM')
--   forecast_date_str — Forecast run timestamp (e.g. '20251028000000').
--                       Pass '' or NULL to return storms across all dates.
--
-- Returns: JSON object with
--   country            — echoed country_code
--   forecast_date      — echoed forecast_date_str
--   available_storms[] — array of { storm, forecast_date, wind_threshold, row_count, total_population }
--   count              — number of storm/threshold combinations returned
--
-- Example:
--   DISCOVER_AVAILABLE_STORMS('JAM', '20251028000000')
--   → { available_storms: [{ storm: 'MELISSA', wind_threshold: 34, total_population: 520000 }, ...], count: 4 }

CREATE OR REPLACE PROCEDURE DISCOVER_AVAILABLE_STORMS(
    country_code VARCHAR,
    forecast_date_str VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var sql_command = `
    SELECT 
        storm,
        forecast_date,
        wind_threshold,
        COUNT(*)::INT AS row_count,
        SUM(COALESCE(E_population, 0)) AS total_population
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
    WHERE country = ?
      AND forecast_date = ?
      AND zoom_level = 14
    GROUP BY storm, forecast_date, wind_threshold
    ORDER BY row_count DESC, storm
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, FORECAST_DATE_STR]
  });
  
  var result = stmt.execute();
  var storms = [];
  
  while (result.next()) {
    storms.push({
      storm: result.getColumnValue(1),
      forecast_date: result.getColumnValue(2),
      wind_threshold: result.getColumnValue(3),
      row_count: result.getColumnValue(4),
      total_population: result.getColumnValue(5)
    });
  }
  
  return {
    country: COUNTRY_CODE,
    forecast_date: FORECAST_DATE_STR,
    available_storms: storms,
    count: storms.length
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Latest Forecast Date for Country
-- ============================================================================
-- Returns the most recent available forecast dates for a given country,
-- optionally filtered to a specific storm. Tries 50kt data first; falls back
-- to any available threshold if no 50kt data exists.
--
-- Parameters:
--   country_code — ISO3 country code (e.g. 'PHL')
--   storm_name   — Storm name (e.g. 'NOKAEN'). Pass '' or NULL to return
--                  dates across all storms for the country.
--
-- Returns: JSON object with
--   country              — echoed country_code
--   storm                — echoed storm_name, or 'ANY' if not specified
--   latest_dates[]       — up to 10 most recent { forecast_date, storm, row_count }
--   latest_forecast_date — date string of the most recent entry, or null
--   latest_storm         — storm name of the most recent entry, or null
--   fallback_applied     — true if 50kt had no data and any-threshold fallback was used
--   warning              — null, or explanation if fallback was applied
--
-- Example:
--   GET_LATEST_FORECAST_DATE('PHL', 'NOKAEN')
--   → { latest_forecast_date: '20260115060000', latest_storm: 'NOKAEN', fallback_applied: false, ... }

CREATE OR REPLACE PROCEDURE GET_LATEST_FORECAST_DATE(
    country_code VARCHAR,
    storm_name VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var stormValue = (typeof STORM_NAME !== 'undefined' && STORM_NAME !== null) ? STORM_NAME : '';
  if (stormValue === '' || stormValue === 'null' || stormValue === 'NULL') stormValue = '';

  var dates = [];
  var fallback_applied = false;

  // ---- ATTEMPT 1: wind_threshold = 50 (matches agent default) ----
  var sql_50, binds_50;
  if (stormValue === '') {
    sql_50 = `SELECT forecast_date, storm, COUNT(*)::INT AS row_count
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE country = ? AND zoom_level = 14 AND wind_threshold = 50
              GROUP BY forecast_date, storm ORDER BY forecast_date DESC, row_count DESC LIMIT 10`;
    binds_50 = [COUNTRY_CODE];
  } else {
    sql_50 = `SELECT forecast_date, storm, COUNT(*)::INT AS row_count
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE country = ? AND UPPER(storm) = UPPER(?) AND zoom_level = 14 AND wind_threshold = 50
              GROUP BY forecast_date, storm ORDER BY forecast_date DESC, row_count DESC LIMIT 10`;
    binds_50 = [COUNTRY_CODE, stormValue];
  }
  var r50 = snowflake.createStatement({ sqlText: sql_50, binds: binds_50 }).execute();
  while (r50.next()) {
    dates.push({ forecast_date: r50.getColumnValue(1), storm: r50.getColumnValue(2), row_count: r50.getColumnValue(3) });
  }

  // ---- ATTEMPT 2: fallback — any available threshold (if no 50kt data) ----
  if (dates.length === 0) {
    fallback_applied = true;
    var sql_any, binds_any;
    if (stormValue === '') {
      sql_any = `SELECT forecast_date, storm, COUNT(*)::INT AS row_count
                 FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
                 WHERE country = ? AND zoom_level = 14
                 GROUP BY forecast_date, storm ORDER BY forecast_date DESC, row_count DESC LIMIT 10`;
      binds_any = [COUNTRY_CODE];
    } else {
      sql_any = `SELECT forecast_date, storm, COUNT(*)::INT AS row_count
                 FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
                 WHERE country = ? AND UPPER(storm) = UPPER(?) AND zoom_level = 14
                 GROUP BY forecast_date, storm ORDER BY forecast_date DESC, row_count DESC LIMIT 10`;
      binds_any = [COUNTRY_CODE, stormValue];
    }
    var rAny = snowflake.createStatement({ sqlText: sql_any, binds: binds_any }).execute();
    while (rAny.next()) {
      dates.push({ forecast_date: rAny.getColumnValue(1), storm: rAny.getColumnValue(2), row_count: rAny.getColumnValue(3) });
    }
  }

  return {
    country: COUNTRY_CODE,
    storm: STORM_NAME || 'ANY',
    latest_dates: dates,
    latest_forecast_date: dates.length > 0 ? dates[0].forecast_date : null,
    latest_storm: dates.length > 0 ? dates[0].storm : null,
    fallback_applied: fallback_applied,
    warning: fallback_applied ? 'No 50kt data found for this country/storm. Results based on all available thresholds.' : null
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Latest Data Overall
-- ============================================================================
-- Returns the most recent available forecast data across all countries and
-- storms. Used when the user does not specify a country or date. Tries 50kt
-- data first; falls back to any available threshold if needed.
--
-- Parameters: none
--
-- Returns: JSON object with
--   latest_data[]        — up to 20 entries: { country, storm, forecast_date, row_count, total_population }
--   latest_forecast_date — date string of the most recent entry globally, or null
--   latest_country       — country code of the most recent entry, or null
--   latest_storm         — storm name of the most recent entry, or null
--   fallback_applied     — true if 50kt had no data and any-threshold fallback was used
--   warning              — null, or explanation if fallback was applied
--
-- Example:
--   GET_LATEST_DATA_OVERALL()
--   → { latest_country: 'PHL', latest_storm: 'NOKAEN', latest_forecast_date: '20260115060000', ... }

CREATE OR REPLACE PROCEDURE GET_LATEST_DATA_OVERALL()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var data = [];
  var fallback_applied = false;

  // ---- ATTEMPT 1: wind_threshold = 50 (matches agent default) ----
  var r50 = snowflake.createStatement({
    sqlText: `SELECT country, storm, forecast_date, COUNT(*)::INT AS row_count,
                     SUM(COALESCE(E_population, 0)) AS total_population
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE zoom_level = 14 AND wind_threshold = 50
              GROUP BY country, storm, forecast_date
              ORDER BY forecast_date DESC, row_count DESC LIMIT 20`
  }).execute();
  while (r50.next()) {
    data.push({ country: r50.getColumnValue(1), storm: r50.getColumnValue(2),
                forecast_date: r50.getColumnValue(3), row_count: r50.getColumnValue(4),
                total_population: r50.getColumnValue(5) });
  }

  // ---- ATTEMPT 2: fallback — any threshold ----
  if (data.length === 0) {
    fallback_applied = true;
    var rAny = snowflake.createStatement({
      sqlText: `SELECT country, storm, forecast_date, COUNT(*)::INT AS row_count,
                       SUM(COALESCE(E_population, 0)) AS total_population
                FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
                WHERE zoom_level = 14
                GROUP BY country, storm, forecast_date
                ORDER BY forecast_date DESC, row_count DESC LIMIT 20`
    }).execute();
    while (rAny.next()) {
      data.push({ country: rAny.getColumnValue(1), storm: rAny.getColumnValue(2),
                  forecast_date: rAny.getColumnValue(3), row_count: rAny.getColumnValue(4),
                  total_population: rAny.getColumnValue(5) });
    }
  }

  return {
    latest_data: data,
    latest_forecast_date: data.length > 0 ? data[0].forecast_date : null,
    latest_country: data.length > 0 ? data[0].country : null,
    latest_storm: data.length > 0 ? data[0].storm : null,
    fallback_applied: fallback_applied,
    warning: fallback_applied ? 'No 50kt data found globally. Results based on all available thresholds.' : null
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Worst-Case Scenario
-- ============================================================================
-- Returns impact values for the worst-case ensemble member: the single track
-- (zone_id) with the highest total severity_population summed across all grid
-- zones. Reads from TRACK_MAT. Used alongside expected values to frame the
-- risk range in Section 2.
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'JAM')
--   storm_name         — Storm name (e.g. 'MELISSA')
--   forecast_date_str  — Forecast run timestamp (e.g. '20251028000000')
--   wind_threshold_val — Wind threshold in knots: '34', '50', '64', '96', or '137'
--
-- Returns: JSON object with
--   ensemble_member     — zone_id of the worst-case ensemble member
--   population          — worst-case total population at risk
--   school_age_children — worst-case school-age children (5–15)
--   infants             — worst-case infant children (0–4)
--   children            — worst-case total children (school-age + infants)
--   schools             — worst-case schools at risk
--   health_centers      — worst-case health centers at risk
--   (all numeric fields are 0 and ensemble_member is null if no data found)
--
-- Example:
--   GET_WORST_CASE_SCENARIO('JAM', 'MELISSA', '20251028000000', '50')
--   → { ensemble_member: 42, population: 485000, schools: 210, health_centers: 31, ... }

CREATE OR REPLACE PROCEDURE GET_WORST_CASE_SCENARIO(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var wind_threshold_num = parseInt(WIND_THRESHOLD_VAL);
  
  var sql_command = `
    WITH member_impacts AS (
      SELECT 
        zone_id AS ensemble_member,
        SUM(COALESCE(severity_population, 0)) AS member_population,
        SUM(COALESCE(severity_school_age_population, 0)) AS member_school_age_children,
        SUM(COALESCE(severity_infant_population, 0)) AS member_infants,
        SUM(COALESCE(severity_school_age_population, 0) + COALESCE(severity_infant_population, 0)) AS member_children,
        SUM(COALESCE(severity_schools, 0)) AS member_schools,
        SUM(COALESCE(severity_hcs, 0)) AS member_hcs
      FROM AOTS.TC_ECMWF.TRACK_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
      GROUP BY zone_id
      HAVING SUM(COALESCE(severity_population, 0)) > 0
    ),
    worst_case_member AS (
      SELECT *
      FROM member_impacts
      ORDER BY member_population DESC
      LIMIT 1
    )
    SELECT 
      ensemble_member,
      member_population,
      member_school_age_children,
      member_infants,
      member_children,
      member_schools,
      member_hcs
    FROM worst_case_member
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
  });
  
  var result = stmt.execute();
  
  if (result.next()) {
    return {
      ensemble_member: result.getColumnValue(1),
      population: result.getColumnValue(2),
      school_age_children: result.getColumnValue(3),
      infants: result.getColumnValue(4),
      children: result.getColumnValue(5),
      schools: result.getColumnValue(6),
      health_centers: result.getColumnValue(7)
    };
  } else {
    return {
      ensemble_member: null,
      population: 0,
      school_age_children: 0,
      infants: 0,
      children: 0,
      schools: 0,
      health_centers: 0
    };
  }
$$;


-- ============================================================================
-- Helper Procedure: Get Scenario Distribution Analysis
-- ============================================================================
-- Returns distribution statistics (percentiles, mean, stddev) across all
-- ensemble members, plus an inline risk classification. Reads from TRACK_MAT.
-- Used for Section 3 scenario analysis.
--
-- Risk classification rules (computed inline):
--   SPECIAL CASE — <10% of members near worst-case AND worst/median ratio >5×
--   PLAUSIBLE    — 10–30% of members near worst-case OR ratio 3–5×
--   REAL THREAT  — >30% of members near worst-case OR ratio <3×
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'JAM')
--   storm_name         — Storm name (e.g. 'MELISSA')
--   forecast_date_str  — Forecast run timestamp (e.g. '20251028000000')
--   wind_threshold_val — Wind threshold in knots: '34', '50', '64', '96', or '137'
--
-- Returns: JSON object with
--   total_members                         — count of ensemble members with population > 0
--   population { min, p10, p25, p50, p75, p90, max, mean, stddev }
--   children   { min, p50, max, mean }
--   schools    { min, p50, max, mean }
--   health_centers { min, p50, max, mean }
--   members_within_20_percent_of_worst_case — count of members within 80% of max population
--   percentage_near_worst_case            — percentage of members within 80% of max
--   worst_to_median_ratio                 — max population / p50 population
--   risk_classification { classification, description, reasoning }
--
-- Example:
--   GET_SCENARIO_DISTRIBUTION('JAM', 'MELISSA', '20251028000000', '50')
--   → { total_members: 51, population: { p50: 180000, max: 485000 },
--       risk_classification: { classification: 'PLAUSIBLE', ... } }

CREATE OR REPLACE PROCEDURE GET_SCENARIO_DISTRIBUTION(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var wind_threshold_num = parseInt(WIND_THRESHOLD_VAL);
  
  var sql_command = `
    WITH member_impacts AS (
      SELECT 
        zone_id AS ensemble_member,
        SUM(COALESCE(severity_population, 0)) AS member_population,
        SUM(COALESCE(severity_school_age_population, 0) + COALESCE(severity_infant_population, 0)) AS member_children,
        SUM(COALESCE(severity_schools, 0)) AS member_schools,
        SUM(COALESCE(severity_hcs, 0)) AS member_hcs
      FROM AOTS.TC_ECMWF.TRACK_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
      GROUP BY zone_id
      HAVING SUM(COALESCE(severity_population, 0)) > 0
    ),
    worst_case_value AS (
      SELECT MAX(member_population) AS worst_case_population
      FROM member_impacts
    ),
    members_within_20_percent AS (
      SELECT COUNT(*)::INT AS count_within_20_percent
      FROM member_impacts, worst_case_value
      WHERE member_population >= (worst_case_value.worst_case_population * 0.8)
    )
    SELECT 
      COUNT(*)::INT AS total_members,
      MIN(member_population) AS min_population,
      APPROX_PERCENTILE(member_population, 0.10) AS p10_population,
      APPROX_PERCENTILE(member_population, 0.25) AS p25_population,
      APPROX_PERCENTILE(member_population, 0.50) AS p50_population,
      APPROX_PERCENTILE(member_population, 0.75) AS p75_population,
      APPROX_PERCENTILE(member_population, 0.90) AS p90_population,
      MAX(member_population) AS max_population,
      AVG(member_population) AS mean_population,
      STDDEV(member_population) AS stddev_population,
      MIN(member_children) AS min_children,
      APPROX_PERCENTILE(member_children, 0.50) AS p50_children,
      MAX(member_children) AS max_children,
      AVG(member_children) AS mean_children,
      MIN(member_schools) AS min_schools,
      APPROX_PERCENTILE(member_schools, 0.50) AS p50_schools,
      MAX(member_schools) AS max_schools,
      AVG(member_schools) AS mean_schools,
      MIN(member_hcs) AS min_hcs,
      APPROX_PERCENTILE(member_hcs, 0.50) AS p50_hcs,
      MAX(member_hcs) AS max_hcs,
      AVG(member_hcs) AS mean_hcs,
      (SELECT count_within_20_percent FROM members_within_20_percent) AS members_within_20_percent_of_worst_case
    FROM member_impacts
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
  });
  
  var result = stmt.execute();
  result.next();
  
  var total_members = result.getColumnValue(1);
  var members_within_20_percent = result.getColumnValue(23);
  var median_population = result.getColumnValue(5);
  var max_population = result.getColumnValue(8);
  
  // Calculate percentage near worst-case
  var percentage_near_worst = total_members > 0 ? (members_within_20_percent / total_members) * 100 : 0;
  var worst_to_median_ratio = median_population > 0 ? max_population / median_population : 0;
  var pct   = Math.round(percentage_near_worst * 10) / 10;
  var ratio = Math.round(worst_to_median_ratio * 10) / 10;

  // Inline risk classification — replaces the separate GET_RISK_CLASSIFICATION call
  var classification, description, reasoning;
  if (pct < 10.0 && ratio > 5.0) {
    classification = 'SPECIAL CASE';
    description    = 'a highly unlikely outlier';
    reasoning      = 'Only ' + pct.toFixed(1) + '% of forecast scenarios cluster near this severity level, and worst-case impacts are ' + ratio.toFixed(1) + 'x the median. Worst-case is a LOW-PROBABILITY TAIL RISK.';
  } else if ((pct >= 10.0 && pct <= 30.0) || (ratio >= 3.0 && ratio <= 5.0)) {
    classification = 'PLAUSIBLE';
    description    = 'but NOT MOST LIKELY';
    reasoning      = pct.toFixed(1) + '% of members project impacts within 20% of worst-case; worst-case is ' + ratio.toFixed(1) + 'x the median. A meaningful minority project severe outcomes — CREDIBLE ESCALATION RISK.';
  } else if (pct > 30.0 || ratio < 3.0) {
    classification = 'REAL THREAT';
    description    = '';
    reasoning      = pct.toFixed(1) + '% of members cluster near worst-case; worst-case is ' + ratio.toFixed(1) + 'x the median. High concentration near severe outcomes.';
  } else {
    classification = 'PLAUSIBLE';
    description    = 'but NOT MOST LIKELY';
    reasoning      = 'Risk assessment indicates moderate escalation potential.';
  }

  return {
    total_members: total_members,
    population: {
      min: result.getColumnValue(2),
      p10: result.getColumnValue(3),
      p25: result.getColumnValue(4),
      p50: median_population,
      p75: result.getColumnValue(6),
      p90: result.getColumnValue(7),
      max: max_population,
      mean: result.getColumnValue(9),
      stddev: result.getColumnValue(10)
    },
    children: {
      min: result.getColumnValue(11),
      p50: result.getColumnValue(12),
      max: result.getColumnValue(13),
      mean: result.getColumnValue(14)
    },
    schools: {
      min: result.getColumnValue(15),
      p50: result.getColumnValue(16),
      max: result.getColumnValue(17),
      mean: result.getColumnValue(18)
    },
    health_centers: {
      min: result.getColumnValue(19),
      p50: result.getColumnValue(20),
      max: result.getColumnValue(21),
      mean: result.getColumnValue(22)
    },
    members_within_20_percent_of_worst_case: members_within_20_percent,
    percentage_near_worst_case: pct,
    worst_to_median_ratio: ratio,
    risk_classification: {
      classification: classification,
      description:    description,
      reasoning:      reasoning
    }
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Previous Forecast Date
-- ============================================================================
-- Returns the most recent forecast date strictly before the given date for
-- the same country and storm. Used to resolve the comparison date for trend
-- analysis before calling GET_ADMIN_LEVEL_TREND_COMPARISON. Tries 50kt data
-- first; falls back to any available threshold if needed.
--
-- Parameters:
--   country_code      — ISO3 country code (e.g. 'PHL')
--   storm_name        — Storm name (e.g. 'NOKAEN')
--   forecast_date_str — Current forecast date (e.g. '20260115060000');
--                       returns the date immediately before this one
--
-- Returns: JSON object with
--   previous_forecast_date — date string of the previous run, or null
--   row_count              — tile count for that date (at zoom 14)
--   has_previous           — false if no earlier date exists in the data
--   fallback_applied       — true if 50kt had no data and any-threshold fallback was used
--   warning                — null, or explanation if fallback was applied
--
-- Example:
--   GET_PREVIOUS_FORECAST_DATE('PHL', 'NOKAEN', '20260115060000')
--   → { previous_forecast_date: '20260114180000', has_previous: true, fallback_applied: false }

CREATE OR REPLACE PROCEDURE GET_PREVIOUS_FORECAST_DATE(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var binds = [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR];

  // ---- ATTEMPT 1: wind_threshold = 50 ----
  var r50 = snowflake.createStatement({
    sqlText: `SELECT forecast_date, COUNT(*)::INT AS row_count
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE country = ? AND UPPER(storm) = UPPER(?) AND forecast_date < ?
                AND zoom_level = 14 AND wind_threshold = 50
              GROUP BY forecast_date ORDER BY forecast_date DESC LIMIT 1`,
    binds: binds
  }).execute();

  if (r50.next()) {
    return { previous_forecast_date: r50.getColumnValue(1), row_count: r50.getColumnValue(2), has_previous: true, fallback_applied: false };
  }

  // ---- ATTEMPT 2: fallback — any threshold ----
  var rAny = snowflake.createStatement({
    sqlText: `SELECT forecast_date, COUNT(*)::INT AS row_count
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE country = ? AND UPPER(storm) = UPPER(?) AND forecast_date < ?
                AND zoom_level = 14
              GROUP BY forecast_date ORDER BY forecast_date DESC LIMIT 1`,
    binds: binds
  }).execute();

  if (rAny.next()) {
    return { previous_forecast_date: rAny.getColumnValue(1), row_count: rAny.getColumnValue(2), has_previous: true, fallback_applied: true,
             warning: 'No 50kt data found; previous date resolved from all available thresholds.' };
  }

  return { previous_forecast_date: null, row_count: 0, has_previous: false, fallback_applied: false };
$$;


-- ============================================================================
-- Helper Procedure: Get All Wind Thresholds Analysis
-- ============================================================================
-- Returns expected impact values for every available wind threshold for a
-- given country, storm, and forecast date. Calculates the percentage reduction
-- from 34kt for each higher threshold. Used for Section 3 cross-threshold
-- scenario table.
--
-- Parameters:
--   country_code      — ISO3 country code (e.g. 'JAM')
--   storm_name        — Storm name (e.g. 'MELISSA')
--   forecast_date_str — Forecast run timestamp (e.g. '20251028000000')
--
-- Returns: JSON object with
--   thresholds[] — array sorted by wind_threshold ASC; each entry:
--     { wind_threshold, row_count, total_population, total_schools,
--       total_hcs, total_children, percentage_reduction_from_34kt }
--     (percentage_reduction_from_34kt is null if no 34kt data exists)
--   count        — number of thresholds returned
--
-- Example:
--   GET_ALL_WIND_THRESHOLDS_ANALYSIS('JAM', 'MELISSA', '20251028000000')
--   → { thresholds: [{ wind_threshold: 34, total_population: 520000, percentage_reduction_from_34kt: 0 },
--                    { wind_threshold: 50, total_population: 260000, percentage_reduction_from_34kt: 50.0 }, ...] }

CREATE OR REPLACE PROCEDURE GET_ALL_WIND_THRESHOLDS_ANALYSIS(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var sql_command = `
    SELECT 
      wind_threshold,
      COUNT(*)::INT AS row_count,
      SUM(COALESCE(E_population, 0)) AS total_population,
      SUM(COALESCE(E_num_schools, 0)) AS total_schools,
      SUM(COALESCE(E_num_hcs, 0)) AS total_hcs,
      SUM(COALESCE(E_school_age_population, 0) + COALESCE(E_infant_population, 0)) AS total_children
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
    WHERE country = ?
      AND UPPER(storm) = UPPER(?)
      AND forecast_date = ?
      AND zoom_level = 14
    GROUP BY wind_threshold
    ORDER BY wind_threshold ASC
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR]
  });
  
  var result = stmt.execute();
  var thresholds = [];
  var population_34kt = null;
  
  // First pass: collect all thresholds and find 34kt population
  while (result.next()) {
    var threshold = result.getColumnValue(1);
    var population = result.getColumnValue(3);
    
    if (threshold === 34) {
      population_34kt = population;
    }
    
    thresholds.push({
      wind_threshold: threshold,
      row_count: result.getColumnValue(2),
      total_population: population,
      total_schools: result.getColumnValue(4),
      total_hcs: result.getColumnValue(5),
      total_children: result.getColumnValue(6)
    });
  }
  
  // Calculate percentage reduction from 34kt for each threshold
  if (population_34kt && population_34kt > 0) {
    for (var i = 0; i < thresholds.length; i++) {
      var threshold = thresholds[i];
      if (threshold.wind_threshold !== 34) {
        var reduction = ((population_34kt - threshold.total_population) / population_34kt) * 100;
        threshold.percentage_reduction_from_34kt = Math.round(reduction * 10) / 10; // Round to 1 decimal place
      } else {
        threshold.percentage_reduction_from_34kt = 0;
      }
    }
  } else {
    // If no 34kt threshold found, set percentage_reduction_from_34kt to null for all
    for (var i = 0; i < thresholds.length; i++) {
      thresholds[i].percentage_reduction_from_34kt = null;
    }
  }
  
  return {
    thresholds: thresholds,
    count: thresholds.length
  };
$$;


-- ============================================================================
-- Stored Procedure: Get Admin-Level Breakdown
-- ============================================================================
-- Returns expected impact by administrative area (admin-1 level), joining
-- ADMIN_IMPACT_MAT with BASE_ADMIN_MAT to resolve human-readable names.
-- Sorted by population descending. Used in all report sections.
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'JAM')
--   storm_name         — Storm name (e.g. 'MELISSA')
--   forecast_date_str  — Forecast run timestamp (e.g. '20251028000000')
--   wind_threshold_val — Wind threshold in knots: '34', '50', '64', '96', or '137'
--
-- Returns: JSON object with
--   admin_areas[] — array sorted by population DESC; each entry:
--     { administrative_area, population, children, schools, health_centers }
--   count         — number of admin areas returned
--
-- Example:
--   GET_ADMIN_LEVEL_BREAKDOWN('JAM', 'MELISSA', '20251028000000', '50')
--   → { admin_areas: [{ administrative_area: 'Saint Andrew', population: 82000, children: 16400, schools: 38.2, health_centers: 5.1 }, ...], count: 14 }

CREATE OR REPLACE PROCEDURE GET_ADMIN_LEVEL_BREAKDOWN(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var wind_threshold_num = parseInt(WIND_THRESHOLD_VAL);
  
  var sql_command = `
    WITH admin_impacts_aggregated AS (
      SELECT 
        a.name AS admin_id,
        SUM(COALESCE(a.E_population, 0)) AS population,
        SUM(COALESCE(a.E_school_age_population, 0) + COALESCE(a.E_infant_population, 0)) AS children,
        SUM(COALESCE(a.E_num_schools, 0)) AS schools,
        SUM(COALESCE(a.E_num_hcs, 0)) AS health_centers
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT a
      WHERE a.country = ?
        AND UPPER(a.storm) = UPPER(?)
        AND a.forecast_date = ?
        AND a.wind_threshold = ?
      GROUP BY a.name
    ),
    admin_name_mapping AS (
      SELECT DISTINCT
        a.name AS admin_id,
        COALESCE(b.admin_name, b.name) AS admin_name
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT a
      LEFT JOIN AOTS.TC_ECMWF.BASE_ADMIN_MAT b 
        ON a.name = b.tile_id 
        AND b.country = ?
      WHERE a.country = ?
        AND UPPER(a.storm) = UPPER(?)
        AND a.forecast_date = ?
        AND a.wind_threshold = ?
    ),
    admin_with_names AS (
      SELECT 
        agg.admin_id,
        COALESCE(mapping.admin_name, agg.admin_id) AS administrative_area,
        agg.population,
        agg.children,
        agg.schools,
        agg.health_centers
      FROM admin_impacts_aggregated agg
      LEFT JOIN admin_name_mapping mapping ON agg.admin_id = mapping.admin_id
    )
    SELECT 
      administrative_area,
      population,
      children,
      schools,
      health_centers
    FROM admin_with_names
    ORDER BY population DESC
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num,
            COUNTRY_CODE, COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
  });
  
  var result = stmt.execute();
  var admin_areas = [];
  
  while (result.next()) {
    admin_areas.push({
      administrative_area: result.getColumnValue(1),
      population: result.getColumnValue(2),
      children: result.getColumnValue(3),
      schools: result.getColumnValue(4),
      health_centers: result.getColumnValue(5)
    });
  }
  
  return {
    admin_areas: admin_areas,
    count: admin_areas.length
  };
$$;


-- ============================================================================
-- Stored Procedure: Get Admin-Level Trend Comparison
-- ============================================================================
-- Returns admin-area population delta between two forecast runs, sorted by
-- absolute change descending. Joins ADMIN_IMPACT_MAT with BASE_ADMIN_MAT for
-- human-readable names. Used in Section 4 trend analysis.
--
-- Parameters:
--   country_code               — ISO3 country code (e.g. 'JAM')
--   storm_name                 — Storm name (e.g. 'MELISSA')
--   current_forecast_date_str  — Most recent forecast date (e.g. '20251028000000')
--   previous_forecast_date_str — Earlier forecast date (e.g. '20251027180000')
--   wind_threshold_val         — Wind threshold in knots: '34', '50', '64', '96', or '137'
--
-- Returns: JSON object with
--   admin_trends[] — array sorted by |change| DESC; each entry:
--     { administrative_area, current_population, previous_population,
--       change, percentage_change }
--     (percentage_change is null if previous_population was 0)
--   count          — number of admin areas returned
--
-- Example:
--   GET_ADMIN_LEVEL_TREND_COMPARISON('JAM', 'MELISSA', '20251028000000', '20251027180000', '50')
--   → { admin_trends: [{ administrative_area: 'Saint Andrew', current_population: 82000,
--                        previous_population: 70000, change: 12000, percentage_change: 17.1 }], count: 14 }

CREATE OR REPLACE PROCEDURE GET_ADMIN_LEVEL_TREND_COMPARISON(
    country_code VARCHAR,
    storm_name VARCHAR,
    current_forecast_date_str VARCHAR,
    previous_forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var wind_threshold_num = parseInt(WIND_THRESHOLD_VAL);
  
  var sql_command = `
    WITH current_aggregated AS (
      SELECT 
        a.name AS admin_id,
        SUM(COALESCE(a.E_population, 0)) AS pop
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT a
      WHERE a.country = ?
        AND UPPER(a.storm) = UPPER(?)
        AND a.forecast_date = ?
        AND a.wind_threshold = ?
      GROUP BY a.name
    ),
    current_name_mapping AS (
      SELECT DISTINCT
        a.name AS admin_id,
        COALESCE(b.admin_name, b.name) AS admin_name
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT a
      LEFT JOIN AOTS.TC_ECMWF.BASE_ADMIN_MAT b 
        ON a.name = b.tile_id 
        AND b.country = ?
      WHERE a.country = ?
        AND UPPER(a.storm) = UPPER(?)
        AND a.forecast_date = ?
        AND a.wind_threshold = ?
    ),
    current_with_names AS (
      SELECT 
        COALESCE(mapping.admin_name, curr.admin_id) AS administrative_area,
        curr.pop AS current_population
      FROM current_aggregated curr
      LEFT JOIN current_name_mapping mapping ON curr.admin_id = mapping.admin_id
    ),
    previous_aggregated AS (
      SELECT 
        a.name AS admin_id,
        SUM(COALESCE(a.E_population, 0)) AS pop
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT a
      WHERE a.country = ?
        AND UPPER(a.storm) = UPPER(?)
        AND a.forecast_date = ?
        AND a.wind_threshold = ?
      GROUP BY a.name
    ),
    previous_name_mapping AS (
      SELECT DISTINCT
        a.name AS admin_id,
        COALESCE(b.admin_name, b.name) AS admin_name
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT a
      LEFT JOIN AOTS.TC_ECMWF.BASE_ADMIN_MAT b 
        ON a.name = b.tile_id 
        AND b.country = ?
      WHERE a.country = ?
        AND UPPER(a.storm) = UPPER(?)
        AND a.forecast_date = ?
        AND a.wind_threshold = ?
    ),
    previous_with_names AS (
      SELECT 
        COALESCE(mapping.admin_name, prev.admin_id) AS administrative_area,
        prev.pop AS previous_population
      FROM previous_aggregated prev
      LEFT JOIN previous_name_mapping mapping ON prev.admin_id = mapping.admin_id
    ),
    combined_data AS (
      SELECT 
        COALESCE(c.administrative_area, p.administrative_area) AS administrative_area,
        COALESCE(c.current_population, 0) AS current_population,
        COALESCE(p.previous_population, 0) AS previous_population,
        COALESCE(c.current_population, 0) - COALESCE(p.previous_population, 0) AS change
      FROM current_with_names c
      FULL OUTER JOIN previous_with_names p ON c.administrative_area = p.administrative_area
    )
    SELECT 
      administrative_area,
      current_population,
      previous_population,
      change
    FROM combined_data
    ORDER BY ABS(change) DESC
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, CURRENT_FORECAST_DATE_STR, wind_threshold_num,
            COUNTRY_CODE, COUNTRY_CODE, STORM_NAME, CURRENT_FORECAST_DATE_STR, wind_threshold_num,
            COUNTRY_CODE, STORM_NAME, PREVIOUS_FORECAST_DATE_STR, wind_threshold_num,
            COUNTRY_CODE, COUNTRY_CODE, STORM_NAME, PREVIOUS_FORECAST_DATE_STR, wind_threshold_num]
  });
  
  var result = stmt.execute();
  var admin_trends = [];
  
  while (result.next()) {
    var current_pop = result.getColumnValue(2);
    var previous_pop = result.getColumnValue(3);
    var change = result.getColumnValue(4);
    
    // Calculate percentage change
    var percentage_change = null;
    if (previous_pop > 0) {
      percentage_change = (change / previous_pop) * 100;
      percentage_change = Math.round(percentage_change * 10) / 10; // Round to 1 decimal place
    } else if (change > 0) {
      percentage_change = null; // Cannot calculate percentage change from zero
    } else {
      percentage_change = 0; // No change
    }
    
    admin_trends.push({
      administrative_area: result.getColumnValue(1),
      current_population: current_pop,
      previous_population: previous_pop,
      change: change,
      percentage_change: percentage_change
    });
  }
  
  return {
    admin_trends: admin_trends,
    count: admin_trends.length
  };
$$;


-- ============================================================================
-- Stored Procedure: Get Threshold Probabilities
-- ============================================================================
-- Returns probability values by wind threshold for a specific country, storm,
-- and forecast date. Used for populating the Probability column in Section 3
-- scenario analysis table.
--
-- Parameters:
--   country_code      — ISO3 country code (e.g. 'JAM')
--   storm_name        — Storm name (e.g. 'MELISSA')
--   forecast_date_str — Forecast run timestamp (e.g. '20251028000000')
--
-- Returns: JSON object with
--   probabilities[] — array sorted by wind_threshold ASC; each entry:
--     { wind_threshold, probability }
--     (probability is averaged across admin areas for that threshold)
--   count           — number of threshold entries returned
--   has_data        — false if no probability data found for these parameters
--
-- Example:
--   GET_THRESHOLD_PROBABILITIES('JAM', 'MELISSA', '20251028000000')
--   → { probabilities: [{ wind_threshold: 34, probability: 0.87 }, { wind_threshold: 50, probability: 0.62 }], count: 4, has_data: true }

CREATE OR REPLACE PROCEDURE GET_THRESHOLD_PROBABILITIES(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var sql_command = `
    SELECT DISTINCT
        wind_threshold,
        AVG(probability) AS probability
    FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT
    WHERE country = ?
      AND UPPER(storm) = UPPER(?)
      AND forecast_date = ?
      AND probability IS NOT NULL
    GROUP BY wind_threshold
    ORDER BY wind_threshold ASC
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR]
  });
  
  var result = stmt.execute();
  var probabilities = [];
  
  while (result.next()) {
    probabilities.push({
      wind_threshold: result.getColumnValue(1),
      probability: result.getColumnValue(2)
    });
  }
  
  return {
    probabilities: probabilities,
    count: probabilities.length,
    has_data: probabilities.length > 0
  };
$$;


-- ============================================================================
-- Stored Procedure: Get Country ISO3 Code from Country Name
-- ============================================================================
-- Resolves a country name to its ISO3 code by querying PIPELINE_COUNTRIES.
-- Supports partial matching and returns the best match ranked by exactness.
-- Call this first when the user provides a country name rather than a code.
--
-- Parameters:
--   country_name — Free-text country name (e.g. 'Jamaica', 'Philippines',
--                  'Philipp' — partial match is supported)
--
-- Returns: JSON object with (when found):
--   found        — true
--   country_code — ISO3 code (e.g. 'JAM')
--   country_name — full name as stored in PIPELINE_COUNTRIES
--   match_type   — 'exact' or 'partial'
--   all_matches  — up to 10 matches: [{ country_code, country_name }]
--
--   When not found:
--   found        — false
--   error        — 'Country not found in PIPELINE_COUNTRIES table'
--   suggestions  — [] (empty array)
--
-- Example:
--   GET_COUNTRY_ISO3_CODE('Jamaica')
--   → { found: true, country_code: 'JAM', country_name: 'Jamaica', match_type: 'exact' }

CREATE OR REPLACE PROCEDURE GET_COUNTRY_ISO3_CODE(
    country_name VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var sql_command = `
    SELECT 
        COUNTRY_CODE,
        COUNTRY_NAME
    FROM AOTS.TC_ECMWF.PIPELINE_COUNTRIES
    WHERE UPPER(COUNTRY_NAME) LIKE UPPER('%' || ? || '%')
    ORDER BY 
        CASE 
            WHEN UPPER(COUNTRY_NAME) = UPPER(?) THEN 1
            WHEN UPPER(COUNTRY_NAME) LIKE UPPER(? || '%') THEN 2
            ELSE 3
        END,
        COUNTRY_NAME
    LIMIT 10
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_NAME, COUNTRY_NAME, COUNTRY_NAME]
  });
  
  var result = stmt.execute();
  var countries = [];
  
  while (result.next()) {
    countries.push({
      country_code: result.getColumnValue(1),
      country_name: result.getColumnValue(2)
    });
  }
  
  if (countries.length === 0) {
    return {
      found: false,
      error: 'Country not found in PIPELINE_COUNTRIES table',
      country_name: COUNTRY_NAME,
      suggestions: []
    };
  }
  
  // If exact match, return it
  var exactMatch = countries.find(c => c.country_name.toUpperCase() === COUNTRY_NAME.toUpperCase());
  if (exactMatch) {
    return {
      found: true,
      country_code: exactMatch.country_code,
      country_name: exactMatch.country_name,
      match_type: 'exact',
      all_matches: countries
    };
  }
  
  // Return best match (first in sorted list)
  return {
    found: true,
    country_code: countries[0].country_code,
    country_name: countries[0].country_name,
    match_type: 'partial',
    all_matches: countries
  };
$$;



-- ============================================================================
-- Stored Procedure: Get Single Metric
-- ============================================================================
-- Returns a single named metric for a given query context, plus a provenance
-- citation string. Powers targeted single-metric queries without running the
-- full 5-section report pipeline (3–6× cheaper than a full report).
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'PHL')
--   storm_name         — Storm name (e.g. 'NOKAEN')
--   forecast_date_str  — Forecast run timestamp (e.g. '20260115060000')
--   wind_threshold_val — Wind threshold in knots: '34', '50', '64', '96', or '137'
--   metric_name        — One of (case-insensitive):
--                          expected_population     — total expected population at risk
--                          expected_children       — expected children at risk (school-age + infants)
--                          expected_school_age     — expected school-age children (5–15)
--                          expected_infants        — expected infant children (0–4)
--                          expected_schools        — expected schools at risk
--                          expected_health_centers — expected health centers at risk
--                          worst_case_population   — worst-case ensemble member population
--                          worst_case_children     — worst-case ensemble member children
--                          worst_to_expected_ratio — ratio of worst-case to expected population
--                          ensemble_count          — number of ensemble members in the dataset
--
-- Returns: JSON object with
--   metric_name     — echoed metric_name
--   value           — numeric result (rounded)
--   unit            — 'people', 'facilities', 'ensemble members', or 'x (ratio)'
--   source_citation — provenance string for the agent to include in its response
--   query_context   — { country_code, storm_name, forecast_date, wind_threshold_kt }
--   error           — present only if metric_name is unrecognised or no data found
--
-- Example:
--   GET_SINGLE_METRIC('PHL', 'NOKAEN', '20260115060000', '50', 'expected_population')
--   → { metric_name: 'expected_population', value: 1240000, unit: 'people', source_citation: '...', ... }

CREATE OR REPLACE PROCEDURE GET_SINGLE_METRIC(
    country_code VARCHAR,
    storm_name VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR,
    metric_name VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var wind_threshold_num = parseInt(WIND_THRESHOLD_VAL);
  var metric = METRIC_NAME.toLowerCase().trim();

  // ---- Expected impact metrics (from MERCATOR_TILE_IMPACT_MAT) ----
  var expected_metrics = [
    'expected_population', 'expected_children', 'expected_school_age',
    'expected_infants', 'expected_schools', 'expected_health_centers'
  ];

  // ---- Worst-case metrics (from TRACK_MAT) ----
  var worst_case_metrics = [
    'worst_case_population', 'worst_case_children'
  ];

  // ---- Derived metrics ----
  var derived_metrics = ['worst_to_expected_ratio', 'ensemble_count'];

  var value = null;
  var unit = '';
  var error = null;

  if (expected_metrics.indexOf(metric) !== -1) {
    var col_map = {
      'expected_population':    'SUM(COALESCE(E_population, 0))',
      'expected_children':      'SUM(COALESCE(E_school_age_population, 0) + COALESCE(E_infant_population, 0))',
      'expected_school_age':    'SUM(COALESCE(E_school_age_population, 0))',
      'expected_infants':       'SUM(COALESCE(E_infant_population, 0))',
      'expected_schools':       'SUM(COALESCE(E_num_schools, 0))',
      'expected_health_centers':'SUM(COALESCE(E_num_hcs, 0))'
    };
    var sql = `
      SELECT ${col_map[metric]}
      FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
        AND zoom_level = 14
    `;
    var stmt = snowflake.createStatement({
      sqlText: sql,
      binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
    });
    var result = stmt.execute();
    result.next();
    value = Math.round(result.getColumnValue(1));
    unit = (metric.indexOf('school') !== -1 || metric.indexOf('health') !== -1) ? 'facilities' : 'people';

  } else if (worst_case_metrics.indexOf(metric) !== -1) {
    var wc_col = (metric === 'worst_case_population')
      ? 'severity_population'
      : '(severity_school_age_population + severity_infant_population)';
    var sql = `
      SELECT ${wc_col}
      FROM AOTS.TC_ECMWF.TRACK_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
      ORDER BY severity_population DESC
      LIMIT 1
    `;
    var stmt = snowflake.createStatement({
      sqlText: sql,
      binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
    });
    var result = stmt.execute();
    if (result.next()) {
      value = Math.round(result.getColumnValue(1));
      unit = 'people';
    } else {
      error = 'No ensemble data found for the given parameters.';
    }

  } else if (metric === 'ensemble_count') {
    var sql = `
      SELECT COUNT(DISTINCT zone_id)
      FROM AOTS.TC_ECMWF.TRACK_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
    `;
    var stmt = snowflake.createStatement({
      sqlText: sql,
      binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
    });
    var result = stmt.execute();
    result.next();
    value = result.getColumnValue(1);
    unit = 'ensemble members';

  } else if (metric === 'worst_to_expected_ratio') {
    // Fetch both in parallel via two queries
    var sql_exp = `
      SELECT SUM(COALESCE(E_population, 0))
      FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
      WHERE country = ? AND UPPER(storm) = UPPER(?)
        AND forecast_date = ? AND wind_threshold = ? AND zoom_level = 14
    `;
    var sql_wc = `
      SELECT severity_population
      FROM AOTS.TC_ECMWF.TRACK_MAT
      WHERE country = ? AND UPPER(storm) = UPPER(?)
        AND forecast_date = ? AND wind_threshold = ?
      ORDER BY severity_population DESC LIMIT 1
    `;
    var binds = [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num];
    var r_exp = snowflake.createStatement({sqlText: sql_exp, binds: binds}).execute();
    var r_wc  = snowflake.createStatement({sqlText: sql_wc,  binds: binds}).execute();
    r_exp.next(); r_wc.next();
    var expected_pop = r_exp.getColumnValue(1);
    var wc_pop = r_wc.getColumnValue(1);
    value = (expected_pop > 0) ? Math.round((wc_pop / expected_pop) * 10) / 10 : null;
    unit = 'x (ratio)';

  } else {
    error = 'Unknown metric_name: ' + METRIC_NAME +
            '. Valid values: expected_population, expected_children, expected_school_age, ' +
            'expected_infants, expected_schools, expected_health_centers, ' +
            'worst_case_population, worst_case_children, worst_to_expected_ratio, ensemble_count.';
  }

  var source_citation = '[DATA: GET_SINGLE_METRIC | ' + COUNTRY_CODE + ' / ' + STORM_NAME +
                        ' / ' + FORECAST_DATE_STR + ' / ' + wind_threshold_num + 'kt]';

  if (error) {
    return { error: error, metric_name: METRIC_NAME, source_citation: source_citation };
  }

  return {
    metric_name: METRIC_NAME,
    value: value,
    unit: unit,
    source_citation: source_citation,
    query_context: {
      country_code: COUNTRY_CODE,
      storm_name: STORM_NAME,
      forecast_date: FORECAST_DATE_STR,
      wind_threshold_kt: wind_threshold_num
    }
  };
$$;


-- Grant execute permission to roles that will use the agent
-- MUST grant to the role(s) that users will use when querying the agent
--
-- Grant to SYSADMIN (users should switch to SYSADMIN role when using the agent)
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
GRANT USAGE ON PROCEDURE GET_THRESHOLD_PROBABILITIES(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_SINGLE_METRIC(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;

-- ============================================================================
-- Stored Procedure: Get Forecast Date History
-- ============================================================================
-- Returns the last N available forecast dates for a country/storm,
-- most recent first. The agent calls this to discover which dates exist,
-- then calls GET_EXPECTED_IMPACT_VALUES or GET_ADMIN_LEVEL_TREND_COMPARISON
-- for each date to build a multi-run trend picture.
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'PHL')
--   storm_name         — Storm name (e.g. 'NOKAEN')
--   n                  — How many forecast dates to return (max 10)
--
-- Returns:
--   dates              — array of forecast date strings, newest first
--   count              — number of dates returned
--   fallback_applied   — true if 50kt had no data and any-threshold fallback was used
--   warning            — null or explanation if fallback was applied
--
-- Example: GET_FORECAST_DATE_HISTORY('PHL', 'NOKAEN', 4)
-- Returns the 4 most recent forecast runs for NOKAEN/Philippines.

CREATE OR REPLACE PROCEDURE GET_FORECAST_DATE_HISTORY(
    country_code VARCHAR,
    storm_name VARCHAR,
    n VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  // Cap n at 10 to prevent excessive tool calls
  var limit = Math.min(parseInt(N) || 4, 10);

  var dates = [];
  var fallback_applied = false;

  // ---- ATTEMPT 1: wind_threshold = 50 (matches agent default) ----
  // LIMIT must be a literal — cannot use bind parameter for LIMIT in Snowflake JS
  var r50 = snowflake.createStatement({
    sqlText: `SELECT DISTINCT forecast_date
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE country = ? AND UPPER(storm) = UPPER(?) AND zoom_level = 14 AND wind_threshold = 50
              ORDER BY forecast_date DESC LIMIT ${limit}`,
    binds: [COUNTRY_CODE, STORM_NAME]
  }).execute();
  while (r50.next()) dates.push(r50.getColumnValue(1));

  // ---- ATTEMPT 2: fallback — any threshold ----
  if (dates.length === 0) {
    fallback_applied = true;
    var rAny = snowflake.createStatement({
      sqlText: `SELECT DISTINCT forecast_date
                FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
                WHERE country = ? AND UPPER(storm) = UPPER(?) AND zoom_level = 14
                ORDER BY forecast_date DESC LIMIT ${limit}`,
      binds: [COUNTRY_CODE, STORM_NAME]
    }).execute();
    while (rAny.next()) dates.push(rAny.getColumnValue(1));
  }

  return {
    dates: dates,
    count: dates.length,
    fallback_applied: fallback_applied,
    warning: fallback_applied ? 'No 50kt data found for this storm. Dates resolved from all available thresholds.' : null
  };
$$;

-- ============================================================================
-- Stored Procedure: Get High-Risk Schools
-- ============================================================================
-- Returns named schools above a probability threshold for a given
-- country / storm / forecast date / wind threshold.
--
-- Parameters:
--   country_code    — ISO3 country code (e.g. 'PHL')
--   storm_name      — Storm name (e.g. 'NOKAEN')
--   forecast_date   — Forecast run timestamp (e.g. '20260115060000')
--   wind_threshold  — Wind threshold in knots: '34', '50', '64', '96', or '137'
--   min_probability — Minimum probability to include (0–1). Pass '' to use default 0.0.
--
-- Returns:
--   facilities[]      — array of school objects, sorted by probability DESC
--   count             — number of facilities returned (max 50)
--   total_exposed     — all schools with any exposure (probability > 0)
--   threshold_applied — the min_probability value used
--
-- Each facility object:
--   school_name, education_level, probability, zone_id, latitude, longitude
--
-- Example:
--   GET_HIGH_RISK_SCHOOLS('PHL', 'NOKAEN', '20260115060000', '34', '0.5')
--   → Schools in Philippines with ≥50% probability of 34kt+ winds

CREATE OR REPLACE PROCEDURE GET_HIGH_RISK_SCHOOLS(
    country_code      VARCHAR,
    storm_name        VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR,
    min_probability   VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  // Default to 0.0 — always return top-50 schools by probability.
  // Users/agents may pass a higher threshold (e.g. '0.5') to filter down.
  var threshold = (MIN_PROBABILITY !== '' && MIN_PROBABILITY !== null && MIN_PROBABILITY !== undefined)
    ? parseFloat(MIN_PROBABILITY) : 0.0;
  if (isNaN(threshold) || threshold < 0) threshold = 0.0;
  if (threshold > 1) threshold = 1.0;

  var wt = parseInt(WIND_THRESHOLD_VAL) || 34;

  // Single query: count all exposed schools (probability > 0) + top-50 above threshold
  var sql = `
    WITH all_exposed_cte AS (
      SELECT COUNT(*) AS total_exposed
      FROM AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
        AND all_data:probability::FLOAT > 0
    )
    SELECT
        s.all_data:school_name::VARCHAR     AS school_name,
        s.all_data:education_level::VARCHAR AS education_level,
        s.all_data:probability::FLOAT       AS probability,
        s.all_data:zone_id::VARCHAR         AS zone_id,
        s.all_data:latitude::FLOAT          AS latitude,
        s.all_data:longitude::FLOAT         AS longitude,
        c.total_exposed
    FROM (
      SELECT all_data, all_data:probability::FLOAT AS prob
      FROM AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
        AND all_data:probability::FLOAT > 0
        AND all_data:probability::FLOAT >= ?
      ORDER BY all_data:probability::FLOAT DESC
      LIMIT 50
    ) s, all_exposed_cte c
  `;

  var stmt = snowflake.createStatement({
    sqlText: sql,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wt,
            COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wt, threshold]
  });

  var result = stmt.execute();
  var facilities = [];
  var total_exposed = 0;

  while (result.next()) {
    var name = result.getColumnValue('SCHOOL_NAME');
    if (!name) name = '(unnamed school)';
    if (facilities.length === 0) total_exposed = result.getColumnValue('TOTAL_EXPOSED');
    facilities.push({
      school_name:     name,
      education_level: result.getColumnValue('EDUCATION_LEVEL') || 'Unknown',
      probability:     Math.round(result.getColumnValue('PROBABILITY') * 1000) / 1000,
      zone_id:         result.getColumnValue('ZONE_ID'),
      latitude:        result.getColumnValue('LATITUDE'),
      longitude:       result.getColumnValue('LONGITUDE')
    });
  }

  return {
    facilities:          facilities,
    count:               facilities.length,
    total_exposed:       total_exposed,
    threshold_applied:   threshold,
    wind_threshold_kt:   wt,
    note: facilities.length === 50
      ? 'Showing top 50 by probability. total_exposed gives the full count of schools with any exposure.'
      : null
  };
$$;

-- ============================================================================
-- Stored Procedure: Get High-Risk Health Centers
-- ============================================================================
-- Returns named health facilities above a probability threshold for a given
-- country / storm / forecast date / wind threshold.
--
-- Parameters:
--   country_code    — ISO3 country code (e.g. 'PHL')
--   storm_name      — Storm name (e.g. 'NOKAEN')
--   forecast_date   — Forecast run timestamp (e.g. '20260115060000')
--   wind_threshold  — Wind threshold in knots: '34', '50', '64', '96', or '137'
--   min_probability — Minimum probability to include (0–1). Pass '' to use default 0.0.
--
-- Returns:
--   facilities[]      — array of health center objects, sorted by probability DESC
--   count             — number of facilities returned (max 50)
--   total_exposed     — all facilities with any exposure (probability > 0)
--   threshold_applied — the min_probability value used
--
-- Each facility object:
--   name, health_amenity_type, amenity, operational_status, beds, emergency,
--   electricity, operator_type, probability, zone_id
--   (fields may be null if not populated in source data)
--
-- Example:
--   GET_HIGH_RISK_HEALTH_CENTERS('PHL', 'NOKAEN', '20260115060000', '50', '0.5')
--   → Health facilities in Philippines with ≥50% probability of 50kt+ winds

CREATE OR REPLACE PROCEDURE GET_HIGH_RISK_HEALTH_CENTERS(
    country_code       VARCHAR,
    storm_name         VARCHAR,
    forecast_date_str  VARCHAR,
    wind_threshold_val VARCHAR,
    min_probability    VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  // Default to 0.0 — always return top-50 facilities by probability.
  var threshold = (MIN_PROBABILITY !== '' && MIN_PROBABILITY !== null && MIN_PROBABILITY !== undefined)
    ? parseFloat(MIN_PROBABILITY) : 0.0;
  if (isNaN(threshold) || threshold < 0) threshold = 0.0;
  if (threshold > 1) threshold = 1.0;

  var wt = parseInt(WIND_THRESHOLD_VAL) || 34;

  // Single query: count all exposed facilities (probability > 0) + top-50 above threshold
  var sql = `
    WITH all_exposed_cte AS (
      SELECT COUNT(*) AS total_exposed
      FROM AOTS.TC_ECMWF.HC_IMPACT_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
        AND all_data:probability::FLOAT > 0
    )
    SELECT
        h.all_data:name::VARCHAR                AS name,
        h.all_data:health_amenity_type::VARCHAR AS health_amenity_type,
        h.all_data:amenity::VARCHAR             AS amenity,
        h.all_data:operational_status::VARCHAR  AS operational_status,
        h.all_data:beds::VARCHAR                AS beds,
        h.all_data:emergency::VARCHAR           AS emergency,
        h.all_data:electricity::VARCHAR         AS electricity,
        h.all_data:operator_type::VARCHAR       AS operator_type,
        h.all_data:probability::FLOAT           AS probability,
        h.all_data:zone_id::VARCHAR             AS zone_id,
        c.total_exposed
    FROM (
      SELECT all_data, all_data:probability::FLOAT AS prob
      FROM AOTS.TC_ECMWF.HC_IMPACT_MAT
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
        AND all_data:probability::FLOAT > 0
        AND all_data:probability::FLOAT >= ?
      ORDER BY all_data:probability::FLOAT DESC
      LIMIT 50
    ) h, all_exposed_cte c
  `;

  var stmt = snowflake.createStatement({
    sqlText: sql,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wt,
            COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wt, threshold]
  });

  var result = stmt.execute();
  var facilities = [];
  var total_exposed = 0;

  while (result.next()) {
    var name = result.getColumnValue('NAME');
    if (!name) name = '(unnamed facility)';
    if (facilities.length === 0) total_exposed = result.getColumnValue('TOTAL_EXPOSED');
    facilities.push({
      name:                result.getColumnValue('NAME') || '(unnamed facility)',
      health_amenity_type: result.getColumnValue('HEALTH_AMENITY_TYPE') || null,
      amenity:             result.getColumnValue('AMENITY') || null,
      operational_status:  result.getColumnValue('OPERATIONAL_STATUS') || null,
      beds:                result.getColumnValue('BEDS') || null,
      emergency:           result.getColumnValue('EMERGENCY') || null,
      electricity:         result.getColumnValue('ELECTRICITY') || null,
      operator_type:       result.getColumnValue('OPERATOR_TYPE') || null,
      probability:         Math.round(result.getColumnValue('PROBABILITY') * 1000) / 1000,
      zone_id:             result.getColumnValue('ZONE_ID')
    });
  }

  return {
    facilities:        facilities,
    count:             facilities.length,
    total_exposed:     total_exposed,
    threshold_applied: threshold,
    wind_threshold_kt: wt,
    note: facilities.length === 50
      ? 'Showing top 50 by probability. total_exposed gives the full count of facilities with any exposure.'
      : null
  };
$$;

-- ============================================================================
-- Stored Procedure: Validate Admin Totals
-- ============================================================================
-- Checks that the sum of admin-level expected population matches the
-- tile-level expected population (within a 1% tolerance).
--
-- This converts the LLM-instructed validation check into a hard procedural
-- check. The agent MUST call this after get_admin_level_breakdown and before
-- writing Section 2. If match = false, it must re-check inputs and re-run tools.
--
-- Parameters:
--   country_code       — ISO3 country code (e.g. 'JAM')
--   storm_name         — Storm name (e.g. 'MELISSA')
--   forecast_date_str  — Forecast date (e.g. '20251028000000')
--   wind_threshold_val — Wind threshold as string: '34', '50', '64', etc.
--
-- Returns:
--   match          — true if difference is within 1% tolerance
--   admin_total    — sum of E_population from ADMIN_IMPACT_MAT
--   tile_total     — sum of E_population from MERCATOR_TILE_IMPACT_MAT
--   pct_diff       — absolute percentage difference between the two totals
--   tolerance_pct  — tolerance threshold used (1.0)
--   data_available — false if either query returns zero rows (bad inputs)
--   warning        — null if match, explanation string if mismatch
--
-- Example:
--   CALL VALIDATE_ADMIN_TOTALS('JAM', 'MELISSA', '20251028000000', '50')
--   → { match: true, admin_total: 260050, tile_total: 260194, pct_diff: 0.06 }

CREATE OR REPLACE PROCEDURE VALIDATE_ADMIN_TOTALS(
    country_code      VARCHAR,
    storm_name        VARCHAR,
    forecast_date_str VARCHAR,
    wind_threshold_val VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var wt = parseInt(WIND_THRESHOLD_VAL);
  if (isNaN(wt)) {
    return { match: false, data_available: false,
             error: 'Invalid wind_threshold_val: ' + WIND_THRESHOLD_VAL };
  }

  // --- Admin total from ADMIN_IMPACT_MAT ---
  var r_admin = snowflake.createStatement({
    sqlText: `SELECT SUM(COALESCE(E_population, 0)) AS admin_total
              FROM AOTS.TC_ECMWF.ADMIN_IMPACT_MAT
              WHERE country = ?
                AND UPPER(storm) = UPPER(?)
                AND forecast_date = ?
                AND wind_threshold = ?`,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wt]
  }).execute();
  r_admin.next();
  var admin_total = r_admin.getColumnValue('ADMIN_TOTAL') || 0;

  // --- Tile total from MERCATOR_TILE_IMPACT_MAT ---
  var r_tile = snowflake.createStatement({
    sqlText: `SELECT SUM(COALESCE(E_population, 0)) AS tile_total
              FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
              WHERE country = ?
                AND UPPER(storm) = UPPER(?)
                AND forecast_date = ?
                AND wind_threshold = ?
                AND zoom_level = 14`,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wt]
  }).execute();
  r_tile.next();
  var tile_total = r_tile.getColumnValue('TILE_TOTAL') || 0;

  // --- No data at all ---
  if (tile_total === 0 && admin_total === 0) {
    return {
      match: false,
      data_available: false,
      admin_total: 0,
      tile_total: 0,
      pct_diff: null,
      tolerance_pct: 1.0,
      warning: 'Both admin and tile queries returned zero. Check country_code, storm_name, forecast_date_str, and wind_threshold_val.'
    };
  }

  // --- Compute difference ---
  var reference = tile_total > 0 ? tile_total : admin_total;
  var diff = Math.abs(admin_total - tile_total);
  var pct_diff = Math.round((diff / reference) * 1000) / 10;  // 1 decimal place
  var tolerance = 1.0;
  var match = pct_diff <= tolerance;

  return {
    match: match,
    data_available: true,
    admin_total: Math.round(admin_total),
    tile_total: Math.round(tile_total),
    pct_diff: pct_diff,
    tolerance_pct: tolerance,
    warning: match ? null
      : 'Admin total (' + Math.round(admin_total) + ') differs from tile total (' + Math.round(tile_total) + ') by ' + pct_diff + '%. Exceeds 1% tolerance — re-check inputs before writing Section 2.'
  };
$$;

GRANT USAGE ON PROCEDURE GET_FORECAST_DATE_HISTORY(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_HIGH_RISK_SCHOOLS(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE GET_HIGH_RISK_HEALTH_CENTERS(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE VALIDATE_ADMIN_TOTALS(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;

GRANT USAGE ON SCHEMA TC_ECMWF TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE AOTS TO ROLE SYSADMIN;