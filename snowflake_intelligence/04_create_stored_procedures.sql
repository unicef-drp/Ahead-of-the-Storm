-- ============================================================================
-- Create Stored Procedures for Agent to Call
-- ============================================================================
-- These stored procedures execute the EXACT queries needed, ensuring
-- Cortex Analyst doesn't generate incorrect SQL.
--
-- Prerequisites:
-- - Views created from 01_setup_views.sql
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
-- Using JavaScript stored procedure that returns a JSON object
-- This format works better with Cortex Agents than table-returning functions
-- According to Snowflake docs, stored procedures are preferred for agent tools

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
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
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
-- Returns available storms for a given country and forecast date
-- Used when user doesn't specify storm name

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
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
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
-- Returns the latest available forecast date for a given country (optionally with storm)
-- Used when user doesn't specify forecast date

CREATE OR REPLACE PROCEDURE GET_LATEST_FORECAST_DATE(
    country_code VARCHAR,
    storm_name VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var sql_command;
  var binds;
  
  // Handle NULL, undefined, or empty string as "no storm specified"
  // When agent doesn't provide storm_name, it may be undefined/null/empty
  var stormValue = (typeof STORM_NAME !== 'undefined' && STORM_NAME !== null) ? STORM_NAME : '';
  if (stormValue === '' || stormValue === 'null' || stormValue === 'NULL') {
    // No storm specified - get latest date for country
    // Optimized: Add wind_threshold filter to reduce scan size (use 34kt as most common)
    sql_command = `
      SELECT 
          forecast_date,
          storm,
          COUNT(*)::INT AS row_count
      FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
      WHERE country = ?
        AND zoom_level = 14
        AND wind_threshold = 34
      GROUP BY forecast_date, storm
      ORDER BY forecast_date DESC, row_count DESC
      LIMIT 10
    `;
    binds = [COUNTRY_CODE];
  } else {
    // Storm specified - get latest date for country + storm
    sql_command = `
      SELECT 
          forecast_date,
          storm,
          COUNT(*)::INT AS row_count
      FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND zoom_level = 14
      GROUP BY forecast_date, storm
      ORDER BY forecast_date DESC, row_count DESC
      LIMIT 10
    `;
    binds = [COUNTRY_CODE, stormValue];
  }
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: binds
  });
  
  var result = stmt.execute();
  var dates = [];
  
  while (result.next()) {
    dates.push({
      forecast_date: result.getColumnValue(1),
      storm: result.getColumnValue(2),
      row_count: result.getColumnValue(3)
    });
  }
  
  return {
    country: COUNTRY_CODE,
    storm: STORM_NAME || 'ANY',
    latest_dates: dates,
    latest_forecast_date: dates.length > 0 ? dates[0].forecast_date : null,
    latest_storm: dates.length > 0 ? dates[0].storm : null
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Latest Data Overall
-- ============================================================================
-- Returns the latest available data across all countries/storms
-- Used when user doesn't specify country or date

CREATE OR REPLACE PROCEDURE GET_LATEST_DATA_OVERALL()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var sql_command = `
    SELECT 
        country,
        storm,
        forecast_date,
        COUNT(*)::INT AS row_count,
        SUM(COALESCE(E_population, 0)) AS total_population
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
    WHERE zoom_level = 14
      AND wind_threshold = 34
    GROUP BY country, storm, forecast_date
    ORDER BY forecast_date DESC, row_count DESC
    LIMIT 20
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command
  });
  
  var result = stmt.execute();
  var data = [];
  
  while (result.next()) {
    data.push({
      country: result.getColumnValue(1),
      storm: result.getColumnValue(2),
      forecast_date: result.getColumnValue(3),
      row_count: result.getColumnValue(4),
      total_population: result.getColumnValue(5)
    });
  }
  
  return {
    latest_data: data,
    latest_forecast_date: data.length > 0 ? data[0].forecast_date : null,
    latest_country: data.length > 0 ? data[0].country : null,
    latest_storm: data.length > 0 ? data[0].storm : null
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Worst-Case Scenario
-- ============================================================================
-- Returns worst-case impact values from TRACK_VIEWS_RAW
-- Worst-case is the ensemble member with highest severity_population

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
      FROM AOTS.TC_ECMWF.TRACK_VIEWS_RAW
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
-- Returns distribution statistics (percentiles, mean, etc.) across ensemble members
-- Similar to forecast_analysis.py percentile calculations

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
      FROM AOTS.TC_ECMWF.TRACK_VIEWS_RAW
      WHERE country = ?
        AND UPPER(storm) = UPPER(?)
        AND forecast_date = ?
        AND wind_threshold = ?
      GROUP BY zone_id
      HAVING SUM(COALESCE(severity_population, 0)) > 0
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
      AVG(member_hcs) AS mean_hcs
    FROM member_impacts
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR, wind_threshold_num]
  });
  
  var result = stmt.execute();
  result.next();
  
  return {
    total_members: result.getColumnValue(1),
    population: {
      min: result.getColumnValue(2),
      p10: result.getColumnValue(3),
      p25: result.getColumnValue(4),
      p50: result.getColumnValue(5),
      p75: result.getColumnValue(6),
      p90: result.getColumnValue(7),
      max: result.getColumnValue(8),
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
    }
  };
$$;


-- ============================================================================
-- Helper Procedure: Get Previous Forecast Date
-- ============================================================================
-- Returns the previous forecast date for trend analysis
-- Used to compare current forecast with previous run

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
  var sql_command = `
    SELECT 
      forecast_date,
      COUNT(*)::INT AS row_count
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
    WHERE country = ?
      AND UPPER(storm) = UPPER(?)
      AND forecast_date < ?
      AND zoom_level = 14
      AND wind_threshold = 34
    GROUP BY forecast_date
    ORDER BY forecast_date DESC
    LIMIT 1
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_command,
    binds: [COUNTRY_CODE, STORM_NAME, FORECAST_DATE_STR]
  });
  
  var result = stmt.execute();
  
  if (result.next()) {
    return {
      previous_forecast_date: result.getColumnValue(1),
      row_count: result.getColumnValue(2),
      has_previous: true
    };
  } else {
    return {
      previous_forecast_date: null,
      row_count: 0,
      has_previous: false
    };
  }
$$;


-- ============================================================================
-- Helper Procedure: Get All Wind Thresholds Analysis
-- ============================================================================
-- Returns expected impact values for all available wind thresholds
-- Used for scenario analysis showing different severity levels

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
    FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_VIEWS_RAW
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
  
  while (result.next()) {
    thresholds.push({
      wind_threshold: result.getColumnValue(1),
      row_count: result.getColumnValue(2),
      total_population: result.getColumnValue(3),
      total_schools: result.getColumnValue(4),
      total_hcs: result.getColumnValue(5),
      total_children: result.getColumnValue(6)
    });
  }
  
  return {
    thresholds: thresholds,
    count: thresholds.length
  };
$$;


-- ============================================================================
-- Stored Procedure: Get Admin-Level Breakdown
-- ============================================================================
-- Returns admin-level impact breakdown for a specific country, storm, forecast date, and wind threshold.
-- Used for displaying administrative area breakdowns in all sections.
-- Returns: JSON object with admin_areas array, each containing name, population, children, schools, health_centers

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
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_VIEWS_RAW a
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
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_VIEWS_RAW a
      LEFT JOIN AOTS.TC_ECMWF.BASE_ADMIN_VIEWS_RAW b 
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
-- Returns admin-level comparison between current and previous forecast dates.
-- Used for trend analysis showing which administrative areas changed most.
-- Returns: JSON object with admin_trends array, each containing name, current_population, previous_population, change

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
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_VIEWS_RAW a
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
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_VIEWS_RAW a
      LEFT JOIN AOTS.TC_ECMWF.BASE_ADMIN_VIEWS_RAW b 
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
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_VIEWS_RAW a
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
      FROM AOTS.TC_ECMWF.ADMIN_IMPACT_VIEWS_RAW a
      LEFT JOIN AOTS.TC_ECMWF.BASE_ADMIN_VIEWS_RAW b 
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
    admin_trends.push({
      administrative_area: result.getColumnValue(1),
      current_population: result.getColumnValue(2),
      previous_population: result.getColumnValue(3),
      change: result.getColumnValue(4)
    });
  }
  
  return {
    admin_trends: admin_trends,
    count: admin_trends.length
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

GRANT USAGE ON SCHEMA TC_ECMWF TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE AOTS TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE AOTS_WH TO ROLE SYSADMIN;