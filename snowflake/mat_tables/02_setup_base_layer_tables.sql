-- ============================================================================
-- Step 2: Base Layer MAT Tables
-- ============================================================================
-- Country-static tables that do NOT require a storm/forecast/threshold.
-- They serve two roles in SQL/Snowflake mode (IMPACT_DATA_SOURCE=SQL):
--
--   1. GEOMETRY SOURCE for impact views: when a storm IS selected and "Load
--      Layers" is clicked, the app queries the impact MAT tables for statistics
--      then LEFT JOINs onto these tables to get tile/admin geometry.
--      (tile_id is a mercator quadkey; polygon is reconstructed via mercantile)
--
--   2. FALLBACK when no impact data exists: if any layer comes back empty after
--      the impact query, the app falls back to these tables to show base context
--      data (SMOD, RWI, population, facility counts, admin boundaries). A yellow
--      "Base Layers Only — No Impact Data Available" alert is shown, hurricane
--      track/envelope controls and CCI/probability layer options are disabled,
--      and the Report page displays a "No Impact Data" warning instead of the
--      report template.
--
-- Source files are written by the pipeline --type initialize phase:
--   mercator_views/{COUNTRY}_{ZOOM}.parquet   -> BASE_MERCATOR_TILE_MAT
--   school_views/{COUNTRY}_schools.parquet    -> BASE_SCHOOL_MAT
--   hc_views/{COUNTRY}_health_centers.parquet -> BASE_HC_MAT
--   shelter_views/{COUNTRY}_shelters.parquet  -> BASE_SHELTER_MAT
--   wash_views/{COUNTRY}_wash.parquet         -> BASE_WASH_MAT
--   admin_views/{COUNTRY}_admin{N}.parquet    -> BASE_ADMIN_GEOM_MAT
--
-- Custom data (geodb/custom/) is merged into these files by the pipeline
-- before upload — the app sees one authoritative file per country.
--
-- Only applies when IMPACT_DATA_SOURCE=SQL. LOCAL and BLOB deployments read
-- parquet files directly and are unaffected.
--
-- Geometry notes:
--   Tiles:  NO geometry stored. Tile IDs are mercator quadkeys; polygon geometry
--           is reconstructed in Python using mercantile.quadkey_to_tile() +
--           mercantile.bounds() + shapely.geometry.box(). Avoids 100-400 bytes
--           of WKB per row.
--   HCs:    No explicit lat/lon in source parquet — extracted at table creation
--           via ST_Y/ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY(...)))) matching
--           the pattern used by get_hc_impacts() in snowflake_utils.py.
--   Admin:  Full polygon geometry stored as native GEOGRAPHY type. Queried back
--           with ST_ASGEOJSON(GEOMETRY) and reconstructed in Python via
--           shapely.geometry.shape(json.loads(...)). ADMIN_ALL_IMPACT_MAT also
--           carries a NAME column; the app drops it before merging to preserve
--           the authoritative NAME from this table in the tooltip.
--
-- Python query functions (all @lru_cache in snowflake_utils.py):
--   get_base_tiles(country, zoom_level=14)  -> GeoDataFrame (geometry from quadkeys)
--   get_base_schools(country)               -> DataFrame
--   get_base_hcs(country)                   -> DataFrame (lat/lon pre-computed)
--   get_base_shelters(country)              -> DataFrame
--   get_base_wash(country)                  -> DataFrame
--   get_base_admin(country, admin_level=1)  -> GeoDataFrame (geometry from ST_ASGEOJSON)
--
-- Refresh: CALL REFRESH_BASE_LAYER_TABLES()
-- Called automatically at end of REFRESH_MATERIALIZED_VIEWS().
-- Can also be called standalone after pipeline --type initialize runs.
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;


-- ----------------------------------------------------------------------------
-- BASE_MERCATOR_TILE_MAT
-- ----------------------------------------------------------------------------
-- Source: mercator_views/{COUNTRY}_{ZOOM}.parquet
-- Pattern '.*_[0-9]+\.parquet' matches only base tiles (impact files are CSV).
--
-- Two parquet column sets exist depending on pipeline version:
--   Old (ATG, JAM, SLB, ...): tile_id, built_surface_m2, smod_class, id,
--     num_schools, num_hcs, num_shelters, num_wash
--   New (PNG, ...): above + population, school_age_population,
--     infant_population, adolescent_population
-- Variant column access returns NULL for absent columns — no discrimination needed.
-- NO geometry stored — reconstruct from quadkey using mercantile in Python.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BASE_MERCATOR_TILE_MAT
CLUSTER BY (COUNTRY, ZOOM_LEVEL)
AS
SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                      AS country,
    TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', -1), '.parquet', '') AS INT) AS zoom_level,
    $1:tile_id::VARCHAR                                                                              AS tile_id,
    $1:id::VARCHAR                                                                                   AS admin_id,
    $1:population::FLOAT                                                                             AS population,
    $1:school_age_population::FLOAT                                                                  AS school_age_population,
    $1:infant_population::FLOAT                                                                      AS infant_population,
    $1:adolescent_population::FLOAT                                                                  AS adolescent_population,
    $1:built_surface_m2::FLOAT                                                                       AS built_surface_m2,
    $1:smod_class::FLOAT                                                                             AS smod_class,
    $1:smod_class_l1::FLOAT                                                                          AS smod_class_l1,
    $1:rwi::FLOAT                                                                                    AS rwi,
    $1:num_schools::INT                                                                              AS num_schools,
    $1:num_hcs::INT                                                                                  AS num_hcs,
    $1:num_shelters::INT                                                                             AS num_shelters,
    $1:num_wash::INT                                                                                 AS num_wash
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
    (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\.parquet');


-- ----------------------------------------------------------------------------
-- BASE_SCHOOL_MAT
-- ----------------------------------------------------------------------------
-- Source: school_views/{COUNTRY}_schools.parquet
-- Columns from GIGA API (or custom override): school_id_giga, school_name,
-- education_level, latitude, longitude, country_iso3_code
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BASE_SCHOOL_MAT
CLUSTER BY (COUNTRY)
AS
SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)  AS country,
    $1:school_id_giga::VARCHAR                                   AS school_id_giga,
    $1:school_name::VARCHAR                                      AS school_name,
    $1:education_level::VARCHAR                                  AS education_level,
    $1:latitude::FLOAT                                           AS latitude,
    $1:longitude::FLOAT                                          AS longitude,
    $1:country_iso3_code::VARCHAR                                AS country_iso3_code
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/school_views/
    (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_schools\\.parquet');


-- ----------------------------------------------------------------------------
-- BASE_HC_MAT
-- ----------------------------------------------------------------------------
-- Source: hc_views/{COUNTRY}_health_centers.parquet
-- Columns from HealthSites.io (or custom override). No explicit lat/lon —
-- coordinates extracted from WKB geometry using ST_Y/ST_X, matching the
-- pattern used by get_hc_impacts() in snowflake_utils.py.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BASE_HC_MAT
CLUSTER BY (COUNTRY)
AS
SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                      AS country,
    $1:name::VARCHAR                                                                                 AS name,
    $1:health_amenity_type::VARCHAR                                                                  AS health_amenity_type,
    $1:amenity::VARCHAR                                                                              AS amenity,
    $1:operational_status::VARCHAR                                                                   AS operational_status,
    $1:beds::VARCHAR                                                                                 AS beds,
    $1:emergency::VARCHAR                                                                            AS emergency,
    $1:electricity::VARCHAR                                                                          AS electricity,
    $1:operator_type::VARCHAR                                                                        AS operator_type,
    ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY($1:geometry::STRING, 'HEX'))))                      AS latitude,
    ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY($1:geometry::STRING, 'HEX'))))                      AS longitude
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/hc_views/
    (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_health_centers\\.parquet');


-- ----------------------------------------------------------------------------
-- BASE_SHELTER_MAT
-- ----------------------------------------------------------------------------
-- Source: shelter_views/{COUNTRY}_shelters.parquet
-- Columns from OSM Overpass (or custom override).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BASE_SHELTER_MAT
CLUSTER BY (COUNTRY)
AS
SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)  AS country,
    $1:name::VARCHAR                                             AS name,
    $1:name_en::VARCHAR                                          AS name_en,
    $1:shelter_type::VARCHAR                                     AS shelter_type,
    $1:category::VARCHAR                                         AS category,
    $1:latitude::FLOAT                                           AS latitude,
    $1:longitude::FLOAT                                          AS longitude
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/shelter_views/
    (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_shelters\\.parquet');


-- ----------------------------------------------------------------------------
-- BASE_WASH_MAT
-- ----------------------------------------------------------------------------
-- Source: wash_views/{COUNTRY}_wash.parquet
-- Columns from OSM Overpass (or custom override).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BASE_WASH_MAT
CLUSTER BY (COUNTRY)
AS
SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)  AS country,
    $1:name::VARCHAR                                             AS name,
    $1:name_en::VARCHAR                                          AS name_en,
    $1:wash_type::VARCHAR                                        AS wash_type,
    $1:category::VARCHAR                                         AS category,
    $1:latitude::FLOAT                                           AS latitude,
    $1:longitude::FLOAT                                          AS longitude
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/wash_views/
    (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_wash\\.parquet');


-- ----------------------------------------------------------------------------
-- BASE_ADMIN_GEOM_MAT
-- ----------------------------------------------------------------------------
-- Source: admin_views/{COUNTRY}_admin{N}.parquet
-- Admin boundary polygons with demographic data — no storm required.
-- Pattern '.*_admin[0-9]+\.parquet' excludes CCI files (*_admin1_cci.parquet).
--
-- Geometry stored as native GEOGRAPHY type (WKB hex → TO_GEOGRAPHY).
-- Query with ST_ASGEOJSON(GEOMETRY) to get GeoJSON for map rendering.
-- Clustered by (COUNTRY, ADMIN_LEVEL) to match typical WHERE clause.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BASE_ADMIN_GEOM_MAT
CLUSTER BY (COUNTRY, ADMIN_LEVEL)
AS
SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1)                                      AS country,
    TRY_CAST(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1),
        'admin([0-9]+)\\.parquet$', 1, 1, 'e', 1) AS INT)                                           AS admin_level,
    $1:tile_id::VARCHAR                                                                              AS tile_id,
    $1:name::VARCHAR                                                                                 AS name,
    $1:population::FLOAT                                                                             AS population,
    $1:school_age_population::FLOAT                                                                  AS school_age_population,
    $1:infant_population::FLOAT                                                                      AS infant_population,
    $1:adolescent_population::FLOAT                                                                  AS adolescent_population,
    $1:built_surface_m2::FLOAT                                                                       AS built_surface_m2,
    $1:smod_class::FLOAT                                                                             AS smod_class,
    $1:smod_class_l1::FLOAT                                                                          AS smod_class_l1,
    $1:rwi::FLOAT                                                                                    AS rwi,
    $1:num_schools::INT                                                                              AS num_schools,
    $1:num_hcs::INT                                                                                  AS num_hcs,
    $1:num_shelters::INT                                                                             AS num_shelters,
    $1:num_wash::INT                                                                                 AS num_wash,
    TO_GEOGRAPHY(TRY_TO_BINARY($1:geometry::STRING, 'HEX'))                                         AS geometry
FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
    (FILE_FORMAT => PARQUET_ADMIN_FORMAT, PATTERN => '.*_admin[0-9]+\\.parquet');


-- ============================================================================
-- Refresh Stored Procedure
-- ============================================================================
-- Truncates and reloads all 6 base layer tables from stage.
-- Called automatically at end of REFRESH_MATERIALIZED_VIEWS().
-- Can also be called standalone after pipeline --type initialize runs.
-- ============================================================================
CREATE OR REPLACE PROCEDURE REFRESH_BASE_LAYER_TABLES()
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
      name: 'BASE_MERCATOR_TILE_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_MERCATOR_TILE_MAT
            (COUNTRY, ZOOM_LEVEL, TILE_ID, ADMIN_ID,
             POPULATION, SCHOOL_AGE_POPULATION, INFANT_POPULATION, ADOLESCENT_POPULATION,
             BUILT_SURFACE_M2, SMOD_CLASS, SMOD_CLASS_L1, RWI,
             NUM_SCHOOLS, NUM_HCS, NUM_SHELTERS, NUM_WASH)
        SELECT
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            TRY_CAST(REPLACE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', -1), '.parquet', '') AS INT),
            $1:tile_id::VARCHAR,
            $1:id::VARCHAR,
            $1:population::FLOAT,
            $1:school_age_population::FLOAT,
            $1:infant_population::FLOAT,
            $1:adolescent_population::FLOAT,
            $1:built_surface_m2::FLOAT,
            $1:smod_class::FLOAT,
            $1:smod_class_l1::FLOAT,
            $1:rwi::FLOAT,
            $1:num_schools::INT,
            $1:num_hcs::INT,
            $1:num_shelters::INT,
            $1:num_wash::INT
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/mercator_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_[0-9]+\\\\.parquet')
      `
    },
    {
      name: 'BASE_SCHOOL_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_SCHOOL_MAT
            (COUNTRY, SCHOOL_ID_GIGA, SCHOOL_NAME, EDUCATION_LEVEL,
             LATITUDE, LONGITUDE, COUNTRY_ISO3_CODE)
        SELECT
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            $1:school_id_giga::VARCHAR,
            $1:school_name::VARCHAR,
            $1:education_level::VARCHAR,
            $1:latitude::FLOAT,
            $1:longitude::FLOAT,
            $1:country_iso3_code::VARCHAR
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/school_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_schools\\\\.parquet')
      `
    },
    {
      name: 'BASE_HC_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_HC_MAT
            (COUNTRY, NAME, HEALTH_AMENITY_TYPE, AMENITY, OPERATIONAL_STATUS,
             BEDS, EMERGENCY, ELECTRICITY, OPERATOR_TYPE, LATITUDE, LONGITUDE)
        SELECT
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            $1:name::VARCHAR,
            $1:health_amenity_type::VARCHAR,
            $1:amenity::VARCHAR,
            $1:operational_status::VARCHAR,
            $1:beds::VARCHAR,
            $1:emergency::VARCHAR,
            $1:electricity::VARCHAR,
            $1:operator_type::VARCHAR,
            ST_Y(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY($1:geometry::STRING, 'HEX')))),
            ST_X(ST_CENTROID(TO_GEOGRAPHY(TRY_TO_BINARY($1:geometry::STRING, 'HEX'))))
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/hc_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_health_centers\\\\.parquet')
      `
    },
    {
      name: 'BASE_SHELTER_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_SHELTER_MAT
            (COUNTRY, NAME, NAME_EN, SHELTER_TYPE, CATEGORY, LATITUDE, LONGITUDE)
        SELECT
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            $1:name::VARCHAR,
            $1:name_en::VARCHAR,
            $1:shelter_type::VARCHAR,
            $1:category::VARCHAR,
            $1:latitude::FLOAT,
            $1:longitude::FLOAT
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/shelter_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_shelters\\\\.parquet')
      `
    },
    {
      name: 'BASE_WASH_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_WASH_MAT
            (COUNTRY, NAME, NAME_EN, WASH_TYPE, CATEGORY, LATITUDE, LONGITUDE)
        SELECT
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            $1:name::VARCHAR,
            $1:name_en::VARCHAR,
            $1:wash_type::VARCHAR,
            $1:category::VARCHAR,
            $1:latitude::FLOAT,
            $1:longitude::FLOAT
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/wash_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_wash\\\\.parquet')
      `
    },
    {
      name: 'BASE_ADMIN_GEOM_MAT',
      sql: `
        INSERT OVERWRITE INTO AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT
            (COUNTRY, ADMIN_LEVEL, TILE_ID, NAME,
             POPULATION, SCHOOL_AGE_POPULATION, INFANT_POPULATION, ADOLESCENT_POPULATION,
             BUILT_SURFACE_M2, SMOD_CLASS, SMOD_CLASS_L1, RWI,
             NUM_SCHOOLS, NUM_HCS, NUM_SHELTERS, NUM_WASH, GEOMETRY)
        SELECT
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 1),
            TRY_CAST(REGEXP_SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1),
                'admin([0-9]+)\\\\.parquet$', 1, 1, 'e', 1) AS INT),
            $1:tile_id::VARCHAR,
            $1:name::VARCHAR,
            $1:population::FLOAT,
            $1:school_age_population::FLOAT,
            $1:infant_population::FLOAT,
            $1:adolescent_population::FLOAT,
            $1:built_surface_m2::FLOAT,
            $1:smod_class::FLOAT,
            $1:smod_class_l1::FLOAT,
            $1:rwi::FLOAT,
            $1:num_schools::INT,
            $1:num_hcs::INT,
            $1:num_shelters::INT,
            $1:num_wash::INT,
            TO_GEOGRAPHY(TRY_TO_BINARY($1:geometry::STRING, 'HEX'))
        FROM @AOTS.TC_ECMWF.AOTS_ANALYSIS/geodb/aos_views/admin_views/
            (FILE_FORMAT => AOTS.TC_ECMWF.PARQUET_ADMIN_FORMAT, PATTERN => '.*_admin[0-9]+\\\\.parquet')
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

GRANT USAGE ON PROCEDURE REFRESH_BASE_LAYER_TABLES() TO ROLE SYSADMIN;
