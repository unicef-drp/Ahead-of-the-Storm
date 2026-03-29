-- ============================================================================
-- Step 1: Setup – Materialized Tables for Hurricane Impact Analysis
-- ============================================================================
-- Creates the data layer that stored procedures query.
--
-- Architecture: stage files -> materialized tables
--   1. File formats        — CSV and Parquet format specs for stage reads
--   2. Materialized tables — real Snowflake tables loaded from stage; clustered
--                            for fast filtered queries (<1s vs 17–50s on stage)
--   3. Refresh procedure   — CALL REFRESH_MATERIALIZED_VIEWS() to reload all tables
--   4. Scheduled task      — runs refresh every hour; new data available within ~60 min
--
-- Cluster keys match the WHERE clauses used by stored procedures:
--   (country, storm, forecast_date, wind_threshold)
--
-- Configuration:
--   Database: AOTS
--   Schema:   TC_ECMWF
--   Stage:    AOTS.TC_ECMWF.AOTS_ANALYSIS
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ============================================================================
-- File Formats
-- ============================================================================

CREATE OR REPLACE FILE FORMAT CSV_ADMIN_VIEWS_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT PARQUET_ADMIN_FORMAT
    TYPE = PARQUET
    BINARY_AS_TEXT = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE;


-- ============================================================================
-- Materialized Tables (initial load from stage)
-- ============================================================================

-- MERCATOR_TILE_IMPACT_MAT
-- CSV column order:
--   $1 = (pandas index — empty), $2 = zone_id, $3 = admin_id, $4 = probability,
--   $5 = E_population, $6 = E_built_surface_m2, $7 = E_num_schools,
--   $8 = E_school_age_population, $9 = E_infant_population, $10 = E_num_hcs,
--   $11 = E_rwi, $12 = E_smod_class
-- CRITICAL: Use FLOAT for schools/hcs — CSV contains decimal values (e.g. 0.583333)
--           that get truncated if cast to NUMBER.
CREATE OR REPLACE TABLE MERCATOR_TILE_IMPACT_MAT
CLUSTER BY (country, storm, forecast_date, wind_threshold, zoom_level)
AS
SELECT
    METADATA$FILENAME AS file_path,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT) AS wind_threshold,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 5), '.csv', '') AS INT) AS zoom_level,
    TRY_CAST($2 AS VARCHAR) AS zone_id,
    TRY_CAST($3 AS VARCHAR) AS admin_id,
    TRY_CAST($4 AS FLOAT) AS probability,
    TRY_CAST($5 AS FLOAT) AS E_population,
    TRY_CAST($6 AS FLOAT) AS E_built_surface_m2,
    TRY_CAST($7 AS FLOAT) AS E_num_schools,
    TRY_CAST($8 AS FLOAT) AS E_school_age_population,
    TRY_CAST($9 AS FLOAT) AS E_infant_population,
    TRY_CAST($10 AS FLOAT) AS E_num_hcs,
    TRY_CAST($11 AS FLOAT) AS E_rwi,
    TRY_CAST($12 AS FLOAT) AS E_smod_class
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_[0-9]+\\.csv');

-- ADMIN_IMPACT_MAT
-- CSV column order:
--   $1 = tile_id, $2 = name, $3–10 = E_ columns, $11 = probability
CREATE OR REPLACE TABLE ADMIN_IMPACT_MAT
CLUSTER BY (country, storm, forecast_date, wind_threshold)
AS
SELECT
    METADATA$FILENAME AS file_path,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4), '_admin1.csv', '') AS INT) AS wind_threshold,
    TRY_CAST($1 AS VARCHAR) AS tile_id,
    $2 AS name,
    TRY_CAST($3 AS NUMBER) AS E_school_age_population,
    TRY_CAST($4 AS NUMBER) AS E_infant_population,
    TRY_CAST($5 AS NUMBER) AS E_built_surface_m2,
    TRY_CAST($6 AS NUMBER) AS E_population,
    TRY_CAST($7 AS NUMBER) AS E_num_schools,
    TRY_CAST($8 AS NUMBER) AS E_num_hcs,
    TRY_CAST($9 AS NUMBER) AS E_smod_class,
    TRY_CAST($10 AS NUMBER) AS E_rwi,
    TRY_CAST($11 AS NUMBER) AS probability
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_admin1\\.csv');

-- TRACK_MAT
-- Per-ensemble-member severity metrics.
CREATE OR REPLACE TABLE TRACK_MAT
CLUSTER BY (country, storm, forecast_date, wind_threshold)
AS
WITH parquet_data AS (
    SELECT
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/track_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet')
)
SELECT
    file_path,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    parquet_variant:zone_id::INT AS zone_id,
    parquet_variant:severity_population::NUMBER AS severity_population,
    parquet_variant:severity_school_age_population::NUMBER AS severity_school_age_population,
    parquet_variant:severity_infant_population::NUMBER AS severity_infant_population,
    parquet_variant:severity_schools::NUMBER AS severity_schools,
    parquet_variant:severity_hcs::NUMBER AS severity_hcs,
    parquet_variant:severity_built_surface_m2::NUMBER AS severity_built_surface_m2,
    parquet_variant:geometry AS geometry
FROM parquet_data;

-- SCHOOL_IMPACT_MAT
CREATE OR REPLACE TABLE SCHOOL_IMPACT_MAT
CLUSTER BY (country, storm, forecast_date, wind_threshold)
AS
WITH parquet_data AS (
    SELECT
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/school_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet')
)
SELECT
    file_path,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    parquet_variant:school_name::VARCHAR     AS school_name,
    parquet_variant:education_level::VARCHAR AS education_level,
    parquet_variant:probability::FLOAT       AS probability,
    parquet_variant:zone_id::VARCHAR         AS zone_id,
    parquet_variant:latitude::FLOAT          AS latitude,
    parquet_variant:longitude::FLOAT         AS longitude,
    parquet_variant:country_iso3_code::VARCHAR AS country_iso3_code,
    parquet_variant                          AS all_data
FROM parquet_data;

-- HC_IMPACT_MAT
CREATE OR REPLACE TABLE HC_IMPACT_MAT
CLUSTER BY (country, storm, forecast_date, wind_threshold)
AS
WITH parquet_data AS (
    SELECT
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/hc_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet')
)
SELECT
    file_path,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    parquet_variant:name::VARCHAR                AS name,
    parquet_variant:health_amenity_type::VARCHAR AS health_amenity_type,
    parquet_variant:amenity::VARCHAR             AS amenity,
    parquet_variant:operational_status::VARCHAR  AS operational_status,
    parquet_variant:beds::VARCHAR                AS beds,
    parquet_variant:emergency::VARCHAR           AS emergency,
    parquet_variant:electricity::VARCHAR         AS electricity,
    parquet_variant:operator_type::VARCHAR       AS operator_type,
    parquet_variant:probability::FLOAT           AS probability,
    parquet_variant:zone_id::VARCHAR             AS zone_id,
    parquet_variant                              AS all_data
FROM parquet_data;

-- BASE_ADMIN_MAT
-- Admin ID -> human-readable name lookup
CREATE OR REPLACE TABLE BASE_ADMIN_MAT
AS
WITH parquet_data AS (
    SELECT
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_admin1\\.parquet')
),
flattened_data AS (
    SELECT
        SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
        parquet_variant:zone_id::VARCHAR AS zone_id_direct,
        parquet_variant:tile_id::VARCHAR AS tile_id_direct,
        parquet_variant:name::VARCHAR AS name_direct,
        parquet_variant:NAME::VARCHAR AS name_upper_direct,
        parquet_variant:properties:zone_id::VARCHAR AS zone_id_props,
        parquet_variant:properties:tile_id::VARCHAR AS tile_id_props,
        parquet_variant:properties:name::VARCHAR AS name_props,
        parquet_variant:properties:NAME::VARCHAR AS name_upper_props
    FROM parquet_data
)
SELECT DISTINCT
    country,
    COALESCE(zone_id_direct, tile_id_direct, zone_id_props, tile_id_props) AS tile_id,
    COALESCE(name_direct, name_upper_direct, name_props, name_upper_props) AS admin_name,
    COALESCE(name_direct, name_upper_direct, name_props, name_upper_props) AS name
FROM flattened_data
WHERE COALESCE(zone_id_direct, tile_id_direct, zone_id_props, tile_id_props) IS NOT NULL
  AND COALESCE(name_direct, name_upper_direct, name_props, name_upper_props) IS NOT NULL;



-- ============================================================================
-- Refresh Stored Procedure
-- ============================================================================
-- Truncates and reloads all tables from stage.
-- Runtime: ~1–3 minutes. Call manually or let the Task below run it.

CREATE OR REPLACE PROCEDURE REFRESH_MATERIALIZED_VIEWS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  function run(sql) {
    return snowflake.execute({ sqlText: sql });
  }

  var tables = [
    {
      name: 'MERCATOR_TILE_IMPACT_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
        SELECT
            METADATA$FILENAME AS file_path,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1) AS country,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2) AS storm,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3) AS forecast_date,
            TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT) AS wind_threshold,
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 5), '.csv', '') AS INT) AS zoom_level,
            TRY_CAST($2 AS VARCHAR) AS zone_id,
            TRY_CAST($3 AS VARCHAR) AS admin_id,
            TRY_CAST($4 AS FLOAT) AS probability,
            TRY_CAST($5 AS FLOAT) AS E_population,
            TRY_CAST($6 AS FLOAT) AS E_built_surface_m2,
            TRY_CAST($7 AS FLOAT) AS E_num_schools,
            TRY_CAST($8 AS FLOAT) AS E_school_age_population,
            TRY_CAST($9 AS FLOAT) AS E_infant_population,
            TRY_CAST($10 AS FLOAT) AS E_num_hcs,
            TRY_CAST($11 AS FLOAT) AS E_rwi,
            TRY_CAST($12 AS FLOAT) AS E_smod_class
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_[0-9]+\\\\.csv')
      `
    },
    {
      name: 'ADMIN_IMPACT_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.ADMIN_IMPACT_MAT
        SELECT
            METADATA$FILENAME AS file_path,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1) AS country,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2) AS storm,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3) AS forecast_date,
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4), '_admin1.csv', '') AS INT) AS wind_threshold,
            TRY_CAST($1 AS VARCHAR) AS tile_id,
            $2 AS name,
            TRY_CAST($3 AS NUMBER) AS E_school_age_population,
            TRY_CAST($4 AS NUMBER) AS E_infant_population,
            TRY_CAST($5 AS NUMBER) AS E_built_surface_m2,
            TRY_CAST($6 AS NUMBER) AS E_population,
            TRY_CAST($7 AS NUMBER) AS E_num_schools,
            TRY_CAST($8 AS NUMBER) AS E_num_hcs,
            TRY_CAST($9 AS NUMBER) AS E_smod_class,
            TRY_CAST($10 AS NUMBER) AS E_rwi,
            TRY_CAST($11 AS NUMBER) AS probability
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_admin1\\\\.csv')
      `
    },
    {
      name: 'TRACK_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.TRACK_MAT
        WITH parquet_data AS (
            SELECT METADATA$FILENAME AS file_path, $1 AS parquet_variant
            FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/track_views/
                (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\\\.parquet')
        )
        SELECT
            file_path,
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3),
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT),
            parquet_variant:zone_id::INT,
            parquet_variant:severity_population::NUMBER,
            parquet_variant:severity_school_age_population::NUMBER,
            parquet_variant:severity_infant_population::NUMBER,
            parquet_variant:severity_schools::NUMBER,
            parquet_variant:severity_hcs::NUMBER,
            parquet_variant:severity_built_surface_m2::NUMBER,
            parquet_variant:geometry
        FROM parquet_data
      `
    },
    {
      name: 'SCHOOL_IMPACT_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
        WITH parquet_data AS (
            SELECT METADATA$FILENAME AS file_path, $1 AS parquet_variant
            FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/school_views/
                (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\\\.parquet')
        )
        SELECT
            file_path,
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3),
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT),
            parquet_variant:school_name::VARCHAR,
            parquet_variant:education_level::VARCHAR,
            parquet_variant:probability::FLOAT,
            parquet_variant:zone_id::VARCHAR,
            parquet_variant:latitude::FLOAT,
            parquet_variant:longitude::FLOAT,
            parquet_variant:country_iso3_code::VARCHAR,
            parquet_variant
        FROM parquet_data
      `
    },
    {
      name: 'HC_IMPACT_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.HC_IMPACT_MAT
        WITH parquet_data AS (
            SELECT METADATA$FILENAME AS file_path, $1 AS parquet_variant
            FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/hc_views/
                (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\\\.parquet')
        )
        SELECT
            file_path,
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3),
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT),
            parquet_variant:name::VARCHAR,
            parquet_variant:health_amenity_type::VARCHAR,
            parquet_variant:amenity::VARCHAR,
            parquet_variant:operational_status::VARCHAR,
            parquet_variant:beds::VARCHAR,
            parquet_variant:emergency::VARCHAR,
            parquet_variant:electricity::VARCHAR,
            parquet_variant:operator_type::VARCHAR,
            parquet_variant:probability::FLOAT,
            parquet_variant:zone_id::VARCHAR,
            parquet_variant
        FROM parquet_data
      `
    },
    {
      name: 'BASE_ADMIN_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_ADMIN_MAT
        WITH parquet_data AS (
            SELECT METADATA$FILENAME AS file_path, $1 AS parquet_variant
            FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
                (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_admin1\\\\.parquet')
        ),
        flattened_data AS (
            SELECT
                SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
                parquet_variant:zone_id::VARCHAR AS zone_id_direct,
                parquet_variant:tile_id::VARCHAR AS tile_id_direct,
                parquet_variant:name::VARCHAR AS name_direct,
                parquet_variant:NAME::VARCHAR AS name_upper_direct,
                parquet_variant:properties:zone_id::VARCHAR AS zone_id_props,
                parquet_variant:properties:tile_id::VARCHAR AS tile_id_props,
                parquet_variant:properties:name::VARCHAR AS name_props,
                parquet_variant:properties:NAME::VARCHAR AS name_upper_props
            FROM parquet_data
        )
        SELECT DISTINCT
            country,
            COALESCE(zone_id_direct, tile_id_direct, zone_id_props, tile_id_props),
            COALESCE(name_direct, name_upper_direct, name_props, name_upper_props),
            COALESCE(name_direct, name_upper_direct, name_props, name_upper_props)
        FROM flattened_data
        WHERE COALESCE(zone_id_direct, tile_id_direct, zone_id_props, tile_id_props) IS NOT NULL
          AND COALESCE(name_direct, name_upper_direct, name_props, name_upper_props) IS NOT NULL
      `
    }
  ];

  var refreshed = [];
  var errors = [];

  for (var i = 0; i < tables.length; i++) {
    var t = tables[i];
    try {
      run(t.sql);
      refreshed.push(t.name);
    } catch (e) {
      errors.push(t.name + ': ' + e.message);
    }
  }

  if (errors.length > 0) {
    return 'PARTIAL: refreshed [' + refreshed.join(', ') + '], errors: ' + errors.join(' | ');
  }
  return 'OK: refreshed [' + refreshed.join(', ') + '] at ' + new Date().toISOString();
$$;

GRANT USAGE ON PROCEDURE REFRESH_MATERIALIZED_VIEWS() TO ROLE SYSADMIN;


-- ============================================================================
-- Scheduled Task — refresh every hour
-- ============================================================================
-- ECMWF publishes 4 runs/day (~00Z, 06Z, 12Z, 18Z). Hourly refresh means
-- new data is available within ~60 minutes of the pipeline writing files.
--
-- To change schedule:  ALTER TASK REFRESH_MATERIALIZED_VIEWS_TASK
--                      MODIFY SCHEDULE = 'USING CRON 0 */6 * * * UTC';
-- To run immediately:  EXECUTE TASK REFRESH_MATERIALIZED_VIEWS_TASK;
-- To run manually:     CALL REFRESH_MATERIALIZED_VIEWS();
-- To pause:            ALTER TASK REFRESH_MATERIALIZED_VIEWS_TASK SUSPEND;

CREATE OR REPLACE TASK REFRESH_MATERIALIZED_VIEWS_TASK
  WAREHOUSE = SF_AI_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  CALL AOTS.TC_ECMWF.REFRESH_MATERIALIZED_VIEWS();

-- Tasks are created suspended — resume to activate
ALTER TASK REFRESH_MATERIALIZED_VIEWS_TASK SUSPEND;

GRANT MONITOR ON TASK REFRESH_MATERIALIZED_VIEWS_TASK TO ROLE SYSADMIN;


-- ============================================================================
-- Test — confirm tables are loaded from stage (<1s expected)
-- ============================================================================
SELECT COUNT(*) AS mercator_rows,
       COUNT(DISTINCT forecast_date) AS forecast_dates,
       COUNT(DISTINCT country) AS countries
FROM MERCATOR_TILE_IMPACT_MAT
WHERE zoom_level = 14;

SELECT COUNT(*) AS admin_rows      FROM ADMIN_IMPACT_MAT;
SELECT COUNT(*) AS track_rows      FROM TRACK_MAT;
SELECT COUNT(*) AS school_rows     FROM SCHOOL_IMPACT_MAT;
SELECT COUNT(*) AS hc_rows         FROM HC_IMPACT_MAT;
SELECT COUNT(*) AS admin_name_rows FROM BASE_ADMIN_MAT;