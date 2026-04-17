-- ============================================================================
-- Add a new regional group
-- ============================================================================
-- Template for registering a new multi-country office region.
-- Run this script once per new region; no other files need to change.
--
-- How regional groups work:
--   PIPELINE_COUNTRIES holds a row per region (IS_REGION=TRUE, MEMBER_CODES=[...]).
--   REFRESH_REGIONAL_GROUPS() (defined in 01b_setup_regional_groups.sql) reads
--   those rows and re-derives regional rows in every MAT table by unioning /
--   aggregating the member-country rows already loaded from stage files.
--   REFRESH_MATERIALIZED_VIEWS() calls REFRESH_REGIONAL_GROUPS() automatically
--   at the end of each refresh, so regional rows stay in sync with no extra steps.
--   The app queries regional rows identically to country rows (WHERE country='ECA').
--
-- Prerequisites:
--   - 01b_setup_regional_groups.sql has been run (REFRESH_REGIONAL_GROUPS procedure exists)
--   - All MEMBER_CODES must already exist as rows in PIPELINE_COUNTRIES
--   - COUNTRY_BOUNDARY is intentionally left NULL for regions — the pipeline
--     uses it for spatial storm filtering but skips rows where it is NULL.
--     Regions are excluded from pipeline processing via IS_REGION=TRUE
--     (see country_utils.py in the DATAPIPELINE repo).
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- Extend PIPELINE_COUNTRIES if not already done (idempotent)
ALTER TABLE PIPELINE_COUNTRIES ADD COLUMN IF NOT EXISTS IS_REGION    BOOLEAN DEFAULT FALSE;
ALTER TABLE PIPELINE_COUNTRIES ADD COLUMN IF NOT EXISTS MEMBER_CODES ARRAY;
UPDATE PIPELINE_COUNTRIES SET IS_REGION = FALSE WHERE IS_REGION IS NULL;

-- ============================================================================
-- ECA (East Caribbean Area) — registered 2026-04-17, example for reference
-- ============================================================================
/*
MERGE INTO PIPELINE_COUNTRIES AS target
USING (
    SELECT
        'ECA'                                                             AS COUNTRY_CODE,
        'East Caribbean Area'                                             AS COUNTRY_NAME,
        13.5                                                              AS CENTER_LAT,
        -61.0                                                             AS CENTER_LON,
        6                                                                 AS VIEW_ZOOM,
        14                                                                AS ZOOM_LEVEL,
        TRUE                                                              AS ACTIVE,
        TRUE                                                              AS IS_REGION,
        ARRAY_CONSTRUCT('AIA','ATG','BRB','VGB','DMA','GRD','MSR','KNA','LCA','VCT','TTO','TCA') AS MEMBER_CODES
) AS source ON target.COUNTRY_CODE = source.COUNTRY_CODE
WHEN MATCHED THEN UPDATE SET
    COUNTRY_NAME  = source.COUNTRY_NAME,
    CENTER_LAT    = source.CENTER_LAT,
    CENTER_LON    = source.CENTER_LON,
    VIEW_ZOOM     = source.VIEW_ZOOM,
    ZOOM_LEVEL    = source.ZOOM_LEVEL,
    ACTIVE        = source.ACTIVE,
    IS_REGION     = source.IS_REGION,
    MEMBER_CODES  = source.MEMBER_CODES
WHEN NOT MATCHED THEN INSERT
    (COUNTRY_CODE, COUNTRY_NAME, CENTER_LAT, CENTER_LON, VIEW_ZOOM, ZOOM_LEVEL,
     ACTIVE, IS_REGION, MEMBER_CODES)
VALUES
    (source.COUNTRY_CODE, source.COUNTRY_NAME, source.CENTER_LAT, source.CENTER_LON,
     source.VIEW_ZOOM, source.ZOOM_LEVEL, source.ACTIVE, source.IS_REGION, source.MEMBER_CODES);
*/

-- ============================================================================
-- New region template
-- ============================================================================

MERGE INTO PIPELINE_COUNTRIES AS target
USING (
    SELECT
        '<REGION_CODE>'                    AS COUNTRY_CODE,   -- e.g. 'WCAR'
        '<Region Display Name>'            AS COUNTRY_NAME,   -- e.g. 'West & Central Africa Region'
        <CENTER_LAT>                       AS CENTER_LAT,     -- map center latitude
        <CENTER_LON>                       AS CENTER_LON,     -- map center longitude
        <VIEW_ZOOM>                        AS VIEW_ZOOM,      -- initial map zoom (typically 4–7)
        14                                 AS ZOOM_LEVEL,     -- tile zoom level (keep 14)
        TRUE                               AS ACTIVE,
        TRUE                               AS IS_REGION,
        ARRAY_CONSTRUCT('<ISO1>', '<ISO2>') AS MEMBER_CODES   -- ISO3 codes of member countries
) AS source ON target.COUNTRY_CODE = source.COUNTRY_CODE
WHEN MATCHED THEN UPDATE SET
    COUNTRY_NAME  = source.COUNTRY_NAME,
    CENTER_LAT    = source.CENTER_LAT,
    CENTER_LON    = source.CENTER_LON,
    VIEW_ZOOM     = source.VIEW_ZOOM,
    ZOOM_LEVEL    = source.ZOOM_LEVEL,
    ACTIVE        = source.ACTIVE,
    IS_REGION     = source.IS_REGION,
    MEMBER_CODES  = source.MEMBER_CODES
WHEN NOT MATCHED THEN INSERT
    (COUNTRY_CODE, COUNTRY_NAME, CENTER_LAT, CENTER_LON, VIEW_ZOOM, ZOOM_LEVEL,
     ACTIVE, IS_REGION, MEMBER_CODES)
VALUES
    (source.COUNTRY_CODE, source.COUNTRY_NAME, source.CENTER_LAT, source.CENTER_LON,
     source.VIEW_ZOOM, source.ZOOM_LEVEL, source.ACTIVE, source.IS_REGION, source.MEMBER_CODES);
