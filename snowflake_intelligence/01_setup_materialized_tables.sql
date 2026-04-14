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
-- TWO-FORMAT COMPATIBILITY:
-- The pipeline ships two CSV formats that differ in column count AND order:
--
--   Old 12-col (JAM/VNM, pre-2026):
--     $1=idx $2=zone_id $3=id $4=prob $5=E_pop $6=E_built $7=E_schools
--     $8=E_school_age $9=E_infant $10=E_hcs $11=E_rwi $12=E_smod_class
--
--   New 16-col (PNG/SLB, April 2026+):
--     $1=idx $2=zone_id $3=id $4=prob $5=E_pop $6=E_school_age $7=E_infant
--     $8=E_adolescent $9=E_built $10=E_smod_class $11=E_smod_class_l1
--     $12=E_rwi $13=E_schools $14=E_hcs $15=E_shelters $16=E_wash
--
-- Format detection uses IFF($13 IS NULL, old_pos, new_pos) per column:
--   • Old files: $13 doesn't exist → NULL (ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
--   • New files: $13 = E_num_schools ≥ 0.0 (never None from the pipeline)
--
-- Admin CSV uses $15 as discriminator (NULL=old, probability float=new).
-- CCI CSV format is stable — simple positional refs used.
--
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

-- Standard CSV format with header skip — used for all CSV stage reads.
-- ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE allows accessing $N beyond the row width
-- (returns NULL), which is how we detect old vs new format.
CREATE OR REPLACE FILE FORMAT CSV_ADMIN_VIEWS_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Legacy alias kept for reference — identical to CSV_ADMIN_VIEWS_FORMAT.
-- PARSE_HEADER=TRUE is only useful with COPY INTO MATCH_BY_COLUMN_NAME,
-- which cannot be combined with INCLUDE_METADATA. Not used for data loads.
CREATE OR REPLACE FILE FORMAT CSV_NAMED_COLS_FORMAT
    TYPE = CSV
    PARSE_HEADER = TRUE
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

-- ----------------------------------------------------------------------------
-- MERCATOR_TILE_IMPACT_MAT
-- ----------------------------------------------------------------------------
-- Source: mercator_views/{country}_{storm}_{date}_{wind}_{zoom}.csv
--
-- Format discriminator: $13 IS NULL → old 12-col; $13 IS NOT NULL → new 16-col
-- ($13 = E_num_schools in new format, always ≥ 0.0, never None)
--
-- CRITICAL: Use FLOAT for schools/hcs — CSV contains decimals (e.g. 0.583333)
--           that get truncated if cast to NUMBER.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE MERCATOR_TILE_IMPACT_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD, ZOOM_LEVEL)
AS
SELECT
    METADATA$FILENAME                                                                             AS file_path,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                   AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2)                                   AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3)                                   AS forecast_date,
    TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT)                  AS wind_threshold,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 5), '.csv', '') AS INT) AS zoom_level,
    TRY_CAST($2 AS VARCHAR)                                                                       AS zone_id,
    TRY_CAST($3 AS VARCHAR)                                                                       AS admin_id,
    TRY_CAST($4 AS FLOAT)                                                                         AS probability,
    TRY_CAST($5 AS FLOAT)                                                                         AS E_population,
    -- Columns that differ between formats (IFF: old pos, new pos)
    IFF($13 IS NULL, TRY_CAST($6  AS FLOAT), TRY_CAST($9  AS FLOAT))                             AS E_built_surface_m2,
    IFF($13 IS NULL, TRY_CAST($7  AS FLOAT), TRY_CAST($13 AS FLOAT))                             AS E_num_schools,
    IFF($13 IS NULL, TRY_CAST($8  AS FLOAT), TRY_CAST($6  AS FLOAT))                             AS E_school_age_population,
    IFF($13 IS NULL, TRY_CAST($9  AS FLOAT), TRY_CAST($7  AS FLOAT))                             AS E_infant_population,
    IFF($13 IS NULL, TRY_CAST($10 AS FLOAT), TRY_CAST($14 AS FLOAT))                             AS E_num_hcs,
    IFF($13 IS NULL, TRY_CAST($11 AS FLOAT), TRY_CAST($12 AS FLOAT))                             AS E_rwi,
    IFF($13 IS NULL, TRY_CAST($12 AS FLOAT), TRY_CAST($10 AS FLOAT))                             AS E_smod_class,
    -- New columns only in 16-col format (NULL for old files)
    IFF($13 IS NULL, NULL,                   TRY_CAST($8  AS FLOAT))                             AS E_adolescent_population,
    IFF($13 IS NULL, NULL,                   TRY_CAST($15 AS FLOAT))                             AS E_num_shelters,
    IFF($13 IS NULL, NULL,                   TRY_CAST($16 AS FLOAT))                             AS E_num_wash,
    IFF($13 IS NULL, NULL,                   TRY_CAST($11 AS FLOAT))                             AS E_smod_class_l1
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_[0-9]+\\.csv');

-- ----------------------------------------------------------------------------
-- ADMIN_ALL_IMPACT_MAT
-- ----------------------------------------------------------------------------
-- Source: admin_views/{country}_{storm}_{date}_{wind}_admin{N}.csv
--
-- Old 12-col format:
--   $2=tile_id $3=E_school_age $4=E_infant $5=E_built $6=E_pop
--   $7=E_schools $8=E_hcs $9=E_smod $10=E_rwi $11=prob $12=name
--
-- New 16-col format:
--   $2=tile_id $3=E_pop $4=E_school_age $5=E_infant $6=E_adolescent
--   $7=E_built $8=E_schools $9=E_hcs $10=E_shelters $11=E_wash
--   $12=E_smod $13=E_smod_l1 $14=E_rwi $15=prob $16=name
--
-- Format discriminator: $15 IS NULL → old 12-col; $15 IS NOT NULL → new 16-col
-- ($15 = probability in new format, always a float, never None)
--
-- Admin level parsed from filename (admin1 → 1, admin2 → 2).
-- Pattern matches _34_admin1.csv but NOT _admin1_cci.csv (CCI excluded).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ADMIN_ALL_IMPACT_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD, ADMIN_LEVEL)
AS
SELECT
    METADATA$FILENAME                                                                             AS file_path,
    TRY_CAST(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1),
        'admin([0-9]+)\\.csv$', 1, 1, 'e', 1) AS INT)                                           AS admin_level,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                   AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2)                                   AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3)                                   AS forecast_date,
    TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT)                  AS wind_threshold,
    TRY_CAST($2 AS VARCHAR)                                                                       AS tile_id,
    IFF($15 IS NULL, TRY_CAST($6  AS FLOAT), TRY_CAST($3  AS FLOAT))                             AS E_population,
    IFF($15 IS NULL, TRY_CAST($3  AS FLOAT), TRY_CAST($4  AS FLOAT))                             AS E_school_age_population,
    IFF($15 IS NULL, TRY_CAST($4  AS FLOAT), TRY_CAST($5  AS FLOAT))                             AS E_infant_population,
    IFF($15 IS NULL, NULL,                   TRY_CAST($6  AS FLOAT))                             AS E_adolescent_population,
    IFF($15 IS NULL, TRY_CAST($5  AS FLOAT), TRY_CAST($7  AS FLOAT))                             AS E_built_surface_m2,
    IFF($15 IS NULL, TRY_CAST($7  AS FLOAT), TRY_CAST($8  AS FLOAT))                             AS E_num_schools,
    IFF($15 IS NULL, TRY_CAST($8  AS FLOAT), TRY_CAST($9  AS FLOAT))                             AS E_num_hcs,
    IFF($15 IS NULL, NULL,                   TRY_CAST($10 AS FLOAT))                             AS E_num_shelters,
    IFF($15 IS NULL, NULL,                   TRY_CAST($11 AS FLOAT))                             AS E_num_wash,
    IFF($15 IS NULL, TRY_CAST($9  AS FLOAT), TRY_CAST($12 AS FLOAT))                             AS E_smod_class,
    IFF($15 IS NULL, NULL,                   TRY_CAST($13 AS FLOAT))                             AS E_smod_class_l1,
    IFF($15 IS NULL, TRY_CAST($10 AS FLOAT), TRY_CAST($14 AS FLOAT))                             AS E_rwi,
    IFF($15 IS NULL, TRY_CAST($11 AS FLOAT), TRY_CAST($15 AS FLOAT))                             AS probability,
    IFF($15 IS NULL, $12,                    $16)                                                 AS name
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_admin[0-9]+\\.csv');

-- ----------------------------------------------------------------------------
-- MERCATOR_TILE_CCI_MAT
-- ----------------------------------------------------------------------------
-- Source: mercator_views/{country}_{storm}_{date}_{zoom}_cci.csv
-- CCI format is stable — positional refs used directly.
--   $1=idx $2=zone_id $3=CCI_children $4=E_CCI_children $5=CCI_school_age
--   $6=E_CCI_school_age $7=CCI_infants $8=E_CCI_infants $9=CCI_pop
--   $10=E_CCI_pop $11=id (admin_id)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE MERCATOR_TILE_CCI_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, ZOOM_LEVEL)
AS
SELECT
    METADATA$FILENAME                                                                             AS file_path,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                   AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2)                                   AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3)                                   AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4), '_cci.csv', '') AS INT) AS zoom_level,
    TRY_CAST($2  AS VARCHAR)                                                                      AS zone_id,
    TRY_CAST($11 AS VARCHAR)                                                                      AS admin_id,
    TRY_CAST($3  AS FLOAT)                                                                        AS CCI_children,
    TRY_CAST($4  AS FLOAT)                                                                        AS E_CCI_children,
    TRY_CAST($5  AS FLOAT)                                                                        AS CCI_school_age,
    TRY_CAST($6  AS FLOAT)                                                                        AS E_CCI_school_age,
    TRY_CAST($7  AS FLOAT)                                                                        AS CCI_infants,
    TRY_CAST($8  AS FLOAT)                                                                        AS E_CCI_infants,
    TRY_CAST($9  AS FLOAT)                                                                        AS CCI_pop,
    TRY_CAST($10 AS FLOAT)                                                                        AS E_CCI_pop,
    NULL::FLOAT                                                                                   AS CCI_adolescents,
    NULL::FLOAT                                                                                   AS E_CCI_adolescents
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_cci\\.csv');

-- ----------------------------------------------------------------------------
-- ADMIN_ALL_CCI_MAT
-- ----------------------------------------------------------------------------
-- Source: admin_views/{country}_{storm}_{date}_admin{N}_cci.csv
-- CCI format is stable — positional refs used directly.
--   $1=idx $2=tile_id $3=CCI_children $4=E_CCI_children $5=CCI_school_age
--   $6=E_CCI_school_age $7=CCI_infants $8=E_CCI_infants $9=CCI_pop $10=E_CCI_pop
-- Admin level parsed from filename. Pattern matches _admin1_cci.csv only.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ADMIN_ALL_CCI_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, ADMIN_LEVEL)
AS
SELECT
    METADATA$FILENAME                                                                             AS file_path,
    TRY_CAST(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1),
        'admin([0-9]+)_cci\\.csv$', 1, 1, 'e', 1) AS INT)                                       AS admin_level,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                   AS country,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2)                                   AS storm,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3)                                   AS forecast_date,
    TRY_CAST($2  AS VARCHAR)                                                                      AS tile_id,
    TRY_CAST($3  AS FLOAT)                                                                        AS CCI_children,
    TRY_CAST($4  AS FLOAT)                                                                        AS E_CCI_children,
    TRY_CAST($5  AS FLOAT)                                                                        AS CCI_school_age,
    TRY_CAST($6  AS FLOAT)                                                                        AS E_CCI_school_age,
    TRY_CAST($7  AS FLOAT)                                                                        AS CCI_infants,
    TRY_CAST($8  AS FLOAT)                                                                        AS E_CCI_infants,
    TRY_CAST($9  AS FLOAT)                                                                        AS CCI_pop,
    TRY_CAST($10 AS FLOAT)                                                                        AS E_CCI_pop,
    NULL::FLOAT                                                                                   AS CCI_adolescents,
    NULL::FLOAT                                                                                   AS E_CCI_adolescents
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
    (FILE_FORMAT => CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_admin[0-9]+_cci\\.csv');

-- ----------------------------------------------------------------------------
-- TRACK_MAT
-- Per-ensemble-member severity metrics.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE TRACK_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD)
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
    parquet_variant:zone_id::INT                           AS zone_id,
    parquet_variant:severity_population::NUMBER            AS severity_population,
    parquet_variant:severity_school_age_population::NUMBER AS severity_school_age_population,
    parquet_variant:severity_infant_population::NUMBER     AS severity_infant_population,
    parquet_variant:severity_schools::NUMBER               AS severity_schools,
    parquet_variant:severity_hcs::NUMBER                   AS severity_hcs,
    parquet_variant:severity_built_surface_m2::NUMBER      AS severity_built_surface_m2,
    parquet_variant:geometry                               AS geometry
FROM parquet_data;

-- ----------------------------------------------------------------------------
-- SCHOOL_IMPACT_MAT
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SCHOOL_IMPACT_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD)
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
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1)                        AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2)                        AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3)                        AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    parquet_variant:school_name::VARCHAR       AS school_name,
    parquet_variant:education_level::VARCHAR   AS education_level,
    parquet_variant:probability::FLOAT         AS probability,
    parquet_variant:zone_id::VARCHAR           AS zone_id,
    parquet_variant:latitude::FLOAT            AS latitude,
    parquet_variant:longitude::FLOAT           AS longitude,
    parquet_variant:country_iso3_code::VARCHAR AS country_iso3_code,
    parquet_variant                            AS all_data
FROM parquet_data;

-- ----------------------------------------------------------------------------
-- HC_IMPACT_MAT
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HC_IMPACT_MAT
CLUSTER BY (COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD)
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
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1)                        AS country,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 2)                        AS storm,
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 3)                        AS forecast_date,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 4), '.parquet', '') AS INT) AS wind_threshold,
    parquet_variant:name::VARCHAR                  AS name,
    parquet_variant:health_amenity_type::VARCHAR   AS health_amenity_type,
    parquet_variant:amenity::VARCHAR               AS amenity,
    parquet_variant:operational_status::VARCHAR    AS operational_status,
    parquet_variant:beds::VARCHAR                  AS beds,
    parquet_variant:emergency::VARCHAR             AS emergency,
    parquet_variant:electricity::VARCHAR           AS electricity,
    parquet_variant:operator_type::VARCHAR         AS operator_type,
    parquet_variant:probability::FLOAT             AS probability,
    parquet_variant:zone_id::VARCHAR               AS zone_id,
    parquet_variant                                AS all_data
FROM parquet_data;

-- ----------------------------------------------------------------------------
-- BASE_ADMIN_MAT
-- Admin ID -> human-readable name lookup
-- ----------------------------------------------------------------------------
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
        parquet_variant:zone_id::VARCHAR     AS zone_id_direct,
        parquet_variant:tile_id::VARCHAR     AS tile_id_direct,
        parquet_variant:name::VARCHAR        AS name_direct,
        parquet_variant:NAME::VARCHAR        AS name_upper_direct,
        parquet_variant:properties:zone_id::VARCHAR  AS zone_id_props,
        parquet_variant:properties:tile_id::VARCHAR  AS tile_id_props,
        parquet_variant:properties:name::VARCHAR     AS name_props,
        parquet_variant:properties:NAME::VARCHAR     AS name_upper_props
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
-- Runtime: ~2–5 minutes. Call manually or let the Task below run it.
--
-- Uses same IFF-based format detection as CREATE TABLE above:
--   IFF($13 IS NULL, old_pos, new_pos)  — mercator tile impact
--   IFF($15 IS NULL, old_pos, new_pos)  — admin impact

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
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD, ZOOM_LEVEL,
             ZONE_ID, ADMIN_ID, PROBABILITY,
             E_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS,
             E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION, E_NUM_HCS,
             E_RWI, E_SMOD_CLASS,
             E_ADOLESCENT_POPULATION, E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS_L1)
        SELECT
            METADATA$FILENAME,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3),
            TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT),
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 5), '.csv', '') AS INT),
            TRY_CAST($2 AS VARCHAR),
            TRY_CAST($3 AS VARCHAR),
            TRY_CAST($4 AS FLOAT),
            TRY_CAST($5 AS FLOAT),
            IFF($13 IS NULL, TRY_CAST($6  AS FLOAT), TRY_CAST($9  AS FLOAT)),
            IFF($13 IS NULL, TRY_CAST($7  AS FLOAT), TRY_CAST($13 AS FLOAT)),
            IFF($13 IS NULL, TRY_CAST($8  AS FLOAT), TRY_CAST($6  AS FLOAT)),
            IFF($13 IS NULL, TRY_CAST($9  AS FLOAT), TRY_CAST($7  AS FLOAT)),
            IFF($13 IS NULL, TRY_CAST($10 AS FLOAT), TRY_CAST($14 AS FLOAT)),
            IFF($13 IS NULL, TRY_CAST($11 AS FLOAT), TRY_CAST($12 AS FLOAT)),
            IFF($13 IS NULL, TRY_CAST($12 AS FLOAT), TRY_CAST($10 AS FLOAT)),
            IFF($13 IS NULL, NULL,                   TRY_CAST($8  AS FLOAT)),
            IFF($13 IS NULL, NULL,                   TRY_CAST($15 AS FLOAT)),
            IFF($13 IS NULL, NULL,                   TRY_CAST($16 AS FLOAT)),
            IFF($13 IS NULL, NULL,                   TRY_CAST($11 AS FLOAT))
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_[0-9]+\\\\.csv')
      `
    },
    {
      name: 'ADMIN_ALL_IMPACT_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT
            (FILE_PATH, ADMIN_LEVEL, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             TILE_ID,
             E_POPULATION, E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION,
             E_ADOLESCENT_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS, E_NUM_HCS,
             E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS, E_SMOD_CLASS_L1, E_RWI,
             PROBABILITY, NAME)
        SELECT
            METADATA$FILENAME,
            TRY_CAST(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1),
                'admin([0-9]+)\\\\.csv$', 1, 1, 'e', 1) AS INT),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3),
            TRY_CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4) AS INT),
            TRY_CAST($2 AS VARCHAR),
            IFF($15 IS NULL, TRY_CAST($6  AS FLOAT), TRY_CAST($3  AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($3  AS FLOAT), TRY_CAST($4  AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($4  AS FLOAT), TRY_CAST($5  AS FLOAT)),
            IFF($15 IS NULL, NULL,                   TRY_CAST($6  AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($5  AS FLOAT), TRY_CAST($7  AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($7  AS FLOAT), TRY_CAST($8  AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($8  AS FLOAT), TRY_CAST($9  AS FLOAT)),
            IFF($15 IS NULL, NULL,                   TRY_CAST($10 AS FLOAT)),
            IFF($15 IS NULL, NULL,                   TRY_CAST($11 AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($9  AS FLOAT), TRY_CAST($12 AS FLOAT)),
            IFF($15 IS NULL, NULL,                   TRY_CAST($13 AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($10 AS FLOAT), TRY_CAST($14 AS FLOAT)),
            IFF($15 IS NULL, TRY_CAST($11 AS FLOAT), TRY_CAST($15 AS FLOAT)),
            IFF($15 IS NULL, $12,                    $16)
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_admin[0-9]+\\\\.csv')
      `
    },
    {
      name: 'MERCATOR_TILE_CCI_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, ZOOM_LEVEL,
             ZONE_ID, ADMIN_ID,
             CCI_CHILDREN, E_CCI_CHILDREN,
             CCI_SCHOOL_AGE, E_CCI_SCHOOL_AGE,
             CCI_INFANTS, E_CCI_INFANTS,
             CCI_POP, E_CCI_POP)
        SELECT
            METADATA$FILENAME,
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3),
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 4), '_cci.csv', '') AS INT),
            TRY_CAST($2  AS VARCHAR),
            TRY_CAST($11 AS VARCHAR),
            TRY_CAST($3  AS FLOAT),
            TRY_CAST($4  AS FLOAT),
            TRY_CAST($5  AS FLOAT),
            TRY_CAST($6  AS FLOAT),
            TRY_CAST($7  AS FLOAT),
            TRY_CAST($8  AS FLOAT),
            TRY_CAST($9  AS FLOAT),
            TRY_CAST($10 AS FLOAT)
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_[0-9]+_cci\\\\.csv')
      `
    },
    {
      name: 'ADMIN_ALL_CCI_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT
            (FILE_PATH, ADMIN_LEVEL, COUNTRY, STORM, FORECAST_DATE,
             TILE_ID,
             CCI_CHILDREN, E_CCI_CHILDREN,
             CCI_SCHOOL_AGE, E_CCI_SCHOOL_AGE,
             CCI_INFANTS, E_CCI_INFANTS,
             CCI_POP, E_CCI_POP)
        SELECT
            METADATA$FILENAME,
            TRY_CAST(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1),
                'admin([0-9]+)_cci\\\\.csv$', 1, 1, 'e', 1) AS INT),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2),
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 3),
            TRY_CAST($2  AS VARCHAR),
            TRY_CAST($3  AS FLOAT),
            TRY_CAST($4  AS FLOAT),
            TRY_CAST($5  AS FLOAT),
            TRY_CAST($6  AS FLOAT),
            TRY_CAST($7  AS FLOAT),
            TRY_CAST($8  AS FLOAT),
            TRY_CAST($9  AS FLOAT),
            TRY_CAST($10 AS FLOAT)
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.CSV_ADMIN_VIEWS_FORMAT, PATTERN => '.*_admin[0-9]+_cci\\\\.csv')
      `
    },
    {
      name: 'TRACK_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.TRACK_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             ZONE_ID, SEVERITY_POPULATION, SEVERITY_SCHOOL_AGE_POPULATION,
             SEVERITY_INFANT_POPULATION, SEVERITY_SCHOOLS, SEVERITY_HCS,
             SEVERITY_BUILT_SURFACE_M2, GEOMETRY)
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
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY,
             ZONE_ID, LATITUDE, LONGITUDE, COUNTRY_ISO3_CODE, ALL_DATA)
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
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             NAME, HEALTH_AMENITY_TYPE, AMENITY, OPERATIONAL_STATUS,
             BEDS, EMERGENCY, ELECTRICITY, OPERATOR_TYPE,
             PROBABILITY, ZONE_ID, ALL_DATA)
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
            (COUNTRY, TILE_ID, ADMIN_NAME, NAME)
        WITH parquet_data AS (
            SELECT METADATA$FILENAME AS file_path, $1 AS parquet_variant
            FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
                (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_admin1\\\\.parquet')
        ),
        flattened_data AS (
            SELECT
                SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS country,
                parquet_variant:zone_id::VARCHAR     AS zone_id_direct,
                parquet_variant:tile_id::VARCHAR     AS tile_id_direct,
                parquet_variant:name::VARCHAR        AS name_direct,
                parquet_variant:NAME::VARCHAR        AS name_upper_direct,
                parquet_variant:properties:zone_id::VARCHAR  AS zone_id_props,
                parquet_variant:properties:tile_id::VARCHAR  AS tile_id_props,
                parquet_variant:properties:name::VARCHAR     AS name_props,
                parquet_variant:properties:NAME::VARCHAR     AS name_upper_props
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
CREATE OR REPLACE TASK REFRESH_MATERIALIZED_VIEWS_TASK
  WAREHOUSE = SF_AI_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  CALL AOTS.TC_ECMWF.REFRESH_MATERIALIZED_VIEWS();

ALTER TASK REFRESH_MATERIALIZED_VIEWS_TASK SUSPEND;

GRANT MONITOR ON TASK REFRESH_MATERIALIZED_VIEWS_TASK TO ROLE SYSADMIN;