-- ============================================================================
-- Create AI-Ready Views for Hurricane Impact Analysis
-- ============================================================================
-- This script creates structured views that transform probabilistic forecast
-- data into actionable insights for emergency response specialists.
--
-- Architecture: Simplified approach - agent computes all intelligence from raw data
-- - Raw data views: Direct access to stage files
-- - Agent handles: Risk scoring, prioritization, trend analysis, aggregation
--
-- Prerequisites:
-- - Impact analysis CSV/Parquet files in Snowflake stage
-- - Base admin Parquet files in stage
--
-- Configuration:
-- - Database: AOTS
-- - Schema: TC_ECMWF
-- - Stage: AOTS.TC_ECMWF.AOTS_ANALYSIS
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
-- Base Views: Raw Data Extraction
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ADMIN_IMPACT_VIEWS_RAW View
-- ----------------------------------------------------------------------------
-- Reads ALL impact data from CSV files in the stage.
-- Extracts metadata from filenames: country, storm, forecast_date, wind_threshold
-- Parses CSV columns: tile_id, name, E_ columns, probability
--
-- CSV column order (VERIFIED from actual CSV files):
-- $1 = tile_id, $2 = name, $3-10 = E_ columns, $11 = probability

CREATE OR REPLACE VIEW ADMIN_IMPACT_VIEWS_RAW AS
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

-- ----------------------------------------------------------------------------
-- SCHOOL_IMPACT_VIEWS_RAW View
-- ----------------------------------------------------------------------------
-- Reads school impact data from Parquet files.
-- File pattern: {country}_{storm}_{date}_{wind_threshold}.parquet
-- Location: school_views/

CREATE OR REPLACE VIEW SCHOOL_IMPACT_VIEWS_RAW AS
WITH parquet_data AS (
    SELECT 
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant  -- Parquet files read as single VARIANT column
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/school_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet')
)
SELECT 
    file_path,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    -- Extract fields from VARIANT
    -- School views typically contain: geometry, properties with school data
    parquet_variant:geometry AS geometry,
    parquet_variant:properties AS properties,
    parquet_variant AS all_data  -- Include full VARIANT for flexibility
FROM parquet_data;

-- ----------------------------------------------------------------------------
-- HEALTH_CENTER_IMPACT_VIEWS_RAW View
-- ----------------------------------------------------------------------------
-- Reads health center impact data from Parquet files.
-- File pattern: {country}_{storm}_{date}_{wind_threshold}.parquet
-- Location: hc_views/

CREATE OR REPLACE VIEW HEALTH_CENTER_IMPACT_VIEWS_RAW AS
WITH parquet_data AS (
    SELECT 
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant  -- Parquet files read as single VARIANT column
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/hc_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet')
)
SELECT 
    file_path,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    -- Extract fields from VARIANT
    -- Health center views typically contain: geometry, properties with HC data
    parquet_variant:geometry AS geometry,
    parquet_variant:properties AS properties,
    parquet_variant AS all_data  -- Include full VARIANT for flexibility
FROM parquet_data;

-- ----------------------------------------------------------------------------
-- TRACK_VIEWS_RAW View
-- ----------------------------------------------------------------------------
-- Reads ensemble track impact data from Parquet files.
-- File pattern: {country}_{storm}_{date}_{wind_threshold}.parquet
-- Location: track_views/
-- Contains: severity metrics per ensemble member track

CREATE OR REPLACE VIEW TRACK_VIEWS_RAW AS
WITH parquet_data AS (
    SELECT 
        METADATA$FILENAME AS file_path,
        $1 AS parquet_variant  -- Parquet files read as single VARIANT column
    FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/track_views/
        (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet')
)
SELECT 
    file_path,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    -- Extract fields from VARIANT using :: casting syntax
    -- Field names verified: zone_id, severity_population, severity_school_age_population, etc.
    parquet_variant:zone_id::INT AS zone_id,
    parquet_variant:severity_population::NUMBER AS severity_population,
    parquet_variant:severity_school_age_population::NUMBER AS severity_school_age_population,
    parquet_variant:severity_infant_population::NUMBER AS severity_infant_population,
    parquet_variant:severity_schools::NUMBER AS severity_schools,
    parquet_variant:severity_hcs::NUMBER AS severity_hcs,
    parquet_variant:severity_built_surface_m2::NUMBER AS severity_built_surface_m2,
    parquet_variant:geometry AS geometry
FROM parquet_data;

-- ----------------------------------------------------------------------------
-- MERCATOR_TILE_IMPACT_VIEWS_RAW View
-- ----------------------------------------------------------------------------
-- Reads mercator tile-level impact data from CSV files.
-- File pattern: {country}_{storm}_{date}_{wind_threshold}_{zoom_level}.csv
-- Location: mercator_views/
-- Content: Tile-level expected impact values (more granular than admin-level)
-- Columns: zone_id (tile_id), E_population, E_school_age_population, E_infant_population,
--          E_built_surface_m2, E_num_schools, E_num_hcs, E_rwi, E_smod_class, probability

CREATE OR REPLACE VIEW MERCATOR_TILE_IMPACT_VIEWS_RAW AS
SELECT 
    METADATA$FILENAME AS file_path,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1) AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2) AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3) AS forecast_date,
    TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT) AS wind_threshold,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 5), '.csv', '') AS INT) AS zoom_level,
    -- CSV column order:
    -- CSV header: None, zone_id, id, probability, E_population, E_built_surface_m2, 
    --              E_num_schools, E_school_age_population, E_infant_population, 
    --              E_num_hcs, E_rwi, E_smod_class
    -- With SKIP_HEADER=1, data rows start at $1:
    -- $1 = (empty/index column - pandas writes this but it's empty in header)
    -- $2 = zone_id (Tile ID)
    -- $3 = id (Admin identifier, e.g., "JAM_0003_V2")
    -- $4 = probability
    -- $5 = E_population
    -- $6 = E_built_surface_m2
    -- $7 = E_num_schools
    -- $8 = E_school_age_population
    -- $9 = E_infant_population
    -- $10 = E_num_hcs
    -- $11 = E_rwi
    -- $12 = E_smod_class
    TRY_CAST($2 AS VARCHAR) AS zone_id,  -- Tile ID
    TRY_CAST($3 AS VARCHAR) AS admin_id,  -- Admin identifier (e.g., "JAM_0003_V2")
    TRY_CAST($4 AS FLOAT) AS probability,
    TRY_CAST($5 AS FLOAT) AS E_population,
    TRY_CAST($6 AS FLOAT) AS E_built_surface_m2,
    TRY_CAST($7 AS FLOAT) AS E_num_schools,  -- CRITICAL: Use FLOAT not NUMBER - CSV contains decimal values (e.g., 0.583333) that get truncated with NUMBER
    TRY_CAST($8 AS FLOAT) AS E_school_age_population,
    TRY_CAST($9 AS FLOAT) AS E_infant_population,
    TRY_CAST($10 AS FLOAT) AS E_num_hcs,  -- CRITICAL: Use FLOAT not NUMBER - CSV contains decimal values that get truncated with NUMBER
    TRY_CAST($11 AS FLOAT) AS E_rwi,
    TRY_CAST($12 AS FLOAT) AS E_smod_class
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_[0-9]+\\.csv');


-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Check that all views exist
SHOW VIEWS LIKE '%_RAW' IN SCHEMA AOTS.TC_ECMWF;

-- Verify views have data
SELECT 
    'ADMIN_IMPACT_VIEWS_RAW' AS view_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT country) AS countries,
    COUNT(DISTINCT storm) AS storms,
    CASE WHEN COUNT(*) > 0 THEN 'Data found' ELSE 'No data' END AS status
FROM ADMIN_IMPACT_VIEWS_RAW
UNION ALL
SELECT 
    'SCHOOL_IMPACT_VIEWS_RAW',
    COUNT(*),
    COUNT(DISTINCT country),
    COUNT(DISTINCT storm),
    CASE WHEN COUNT(*) > 0 THEN 'Data found' ELSE 'No data' END
FROM SCHOOL_IMPACT_VIEWS_RAW
UNION ALL
SELECT 
    'HEALTH_CENTER_IMPACT_VIEWS_RAW',
    COUNT(*),
    COUNT(DISTINCT country),
    COUNT(DISTINCT storm),
    CASE WHEN COUNT(*) > 0 THEN 'Data found' ELSE 'No data' END
FROM HEALTH_CENTER_IMPACT_VIEWS_RAW
UNION ALL
SELECT 
    'TRACK_VIEWS_RAW',
    COUNT(*),
    COUNT(DISTINCT country),
    COUNT(DISTINCT storm),
    CASE WHEN COUNT(*) > 0 THEN 'Data found' ELSE 'No data' END
FROM TRACK_VIEWS_RAW
UNION ALL
SELECT 
    'MERCATOR_TILE_IMPACT_VIEWS_RAW',
    COUNT(*),
    COUNT(DISTINCT country),
    COUNT(DISTINCT storm),
    CASE WHEN COUNT(*) > 0 THEN 'Data found' ELSE 'No data' END
FROM MERCATOR_TILE_IMPACT_VIEWS_RAW;