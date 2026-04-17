-- ============================================================================
-- Step 1b: Regional Groups — one-time procedure setup
-- ============================================================================
-- Run once after 01_setup_materialized_tables.sql.
--
-- What this does:
--   Creates REFRESH_REGIONAL_GROUPS() — the procedure that derives regional
--   rows in every MAT table from member-country rows already loaded from stage.
--
-- REFRESH_REGIONAL_GROUPS() is called automatically at the end of
-- REFRESH_MATERIALIZED_VIEWS() (01_setup_materialized_tables.sql) — no
-- separate task or manual call needed after this setup.
--
--
-- Architecture:
--   Tile / admin / facility / CCI tables → UNION  (geographic rows, no overlap)
--   TRACK_MAT                            → SUM per zone_id (zone_id = ensemble
--                                          member; summing gives region-wide
--                                          severity per track scenario)
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ============================================================================
-- REFRESH_REGIONAL_GROUPS procedure
-- ============================================================================
-- Reads all active regions from PIPELINE_COUNTRIES at call time, so adding a
-- new region (via 01c_add_regional_group.sql) requires no changes here.

CREATE OR REPLACE PROCEDURE REFRESH_REGIONAL_GROUPS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  function run(sql) { return snowflake.execute({ sqlText: sql }); }

  var regionRes = run(`
    SELECT COUNTRY_CODE, MEMBER_CODES
    FROM AOTS.TC_ECMWF.PIPELINE_COUNTRIES
    WHERE IS_REGION = TRUE AND ACTIVE = TRUE
  `);

  var regions = [];
  while (regionRes.next()) {
    var code   = regionRes.getColumnValue('COUNTRY_CODE');
    var members = regionRes.getColumnValue('MEMBER_CODES');
    var inList  = members.map(function(m) { return "'" + m + "'"; }).join(',');
    regions.push({ code: code, inList: inList });
  }

  if (regions.length === 0) {
    return 'OK: no active regions in PIPELINE_COUNTRIES';
  }

  var ALL_TABLES = [
    'MERCATOR_TILE_IMPACT_MAT', 'ADMIN_ALL_IMPACT_MAT', 'TRACK_MAT',
    'SCHOOL_IMPACT_MAT', 'HC_IMPACT_MAT', 'SHELTER_IMPACT_MAT', 'WASH_IMPACT_MAT',
    'BASE_ADMIN_MAT', 'MERCATOR_TILE_CCI_MAT', 'ADMIN_ALL_CCI_MAT'
  ];

  var refreshed = [];
  var errors    = [];

  for (var r = 0; r < regions.length; r++) {
    var code   = regions[r].code;
    var inList = regions[r].inList;

    // Delete stale regional rows
    for (var d = 0; d < ALL_TABLES.length; d++) {
      try {
        run("DELETE FROM AOTS.TC_ECMWF." + ALL_TABLES[d] + " WHERE COUNTRY = '" + code + "'");
      } catch(e) {
        errors.push('DELETE ' + ALL_TABLES[d] + ': ' + e.message);
      }
    }

    var inserts = [

      // MERCATOR_TILE_IMPACT_MAT — union (H3 zone_ids are globally unique)
      { name: code + ':MERCATOR_TILE_IMPACT_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD, ZOOM_LEVEL,
             ZONE_ID, ADMIN_ID, PROBABILITY,
             E_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS,
             E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION, E_NUM_HCS,
             E_RWI, E_SMOD_CLASS,
             E_ADOLESCENT_POPULATION, E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS_L1)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD, ZOOM_LEVEL,
            ZONE_ID, ADMIN_ID, PROBABILITY,
            E_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS,
            E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION, E_NUM_HCS,
            E_RWI, E_SMOD_CLASS,
            E_ADOLESCENT_POPULATION, E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS_L1
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      // ADMIN_ALL_IMPACT_MAT — union (per-island admin regions all retained)
      { name: code + ':ADMIN_ALL_IMPACT_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT
            (FILE_PATH, ADMIN_LEVEL, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             TILE_ID, E_POPULATION, E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION,
             E_ADOLESCENT_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS, E_NUM_HCS,
             E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS, E_SMOD_CLASS_L1, E_RWI,
             PROBABILITY, NAME)
        SELECT
            'REGIONAL/` + code + `', ADMIN_LEVEL, '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD,
            TILE_ID, E_POPULATION, E_SCHOOL_AGE_POPULATION, E_INFANT_POPULATION,
            E_ADOLESCENT_POPULATION, E_BUILT_SURFACE_M2, E_NUM_SCHOOLS, E_NUM_HCS,
            E_NUM_SHELTERS, E_NUM_WASH, E_SMOD_CLASS, E_SMOD_CLASS_L1, E_RWI,
            PROBABILITY, NAME
        FROM AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      // TRACK_MAT — SUM per ensemble member (zone_id = track number)
      // Same ensemble members run over all islands; summing gives region-wide
      // severity per scenario. GEOMETRY omitted (not meaningful for aggregate).
      { name: code + ':TRACK_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.TRACK_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             ZONE_ID,
             SEVERITY_POPULATION, SEVERITY_SCHOOL_AGE_POPULATION,
             SEVERITY_INFANT_POPULATION, SEVERITY_ADOLESCENT_POPULATION,
             SEVERITY_SCHOOLS, SEVERITY_HCS,
             SEVERITY_NUM_SHELTERS, SEVERITY_NUM_WASH, SEVERITY_BUILT_SURFACE_M2)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD, ZONE_ID,
            SUM(SEVERITY_POPULATION),
            SUM(SEVERITY_SCHOOL_AGE_POPULATION),
            SUM(SEVERITY_INFANT_POPULATION),
            SUM(SEVERITY_ADOLESCENT_POPULATION),
            SUM(SEVERITY_SCHOOLS),
            SUM(SEVERITY_HCS),
            SUM(SEVERITY_NUM_SHELTERS),
            SUM(SEVERITY_NUM_WASH),
            SUM(SEVERITY_BUILT_SURFACE_M2)
        FROM AOTS.TC_ECMWF.TRACK_MAT
        WHERE COUNTRY IN (` + inList + `)
        GROUP BY STORM, FORECAST_DATE, WIND_THRESHOLD, ZONE_ID
      `},

      { name: code + ':SCHOOL_IMPACT_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY,
             ZONE_ID, LATITUDE, LONGITUDE, COUNTRY_ISO3_CODE, ALL_DATA)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD,
            SCHOOL_NAME, EDUCATION_LEVEL, PROBABILITY,
            ZONE_ID, LATITUDE, LONGITUDE, COUNTRY_ISO3_CODE, ALL_DATA
        FROM AOTS.TC_ECMWF.SCHOOL_IMPACT_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      { name: code + ':HC_IMPACT_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.HC_IMPACT_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             NAME, HEALTH_AMENITY_TYPE, AMENITY, OPERATIONAL_STATUS,
             BEDS, EMERGENCY, ELECTRICITY, OPERATOR_TYPE,
             PROBABILITY, ZONE_ID, ALL_DATA)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD,
            NAME, HEALTH_AMENITY_TYPE, AMENITY, OPERATIONAL_STATUS,
            BEDS, EMERGENCY, ELECTRICITY, OPERATOR_TYPE,
            PROBABILITY, ZONE_ID, ALL_DATA
        FROM AOTS.TC_ECMWF.HC_IMPACT_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      { name: code + ':SHELTER_IMPACT_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.SHELTER_IMPACT_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             NAME, SHELTER_TYPE, CATEGORY, PROBABILITY,
             ZONE_ID, LATITUDE, LONGITUDE, ALL_DATA)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD,
            NAME, SHELTER_TYPE, CATEGORY, PROBABILITY,
            ZONE_ID, LATITUDE, LONGITUDE, ALL_DATA
        FROM AOTS.TC_ECMWF.SHELTER_IMPACT_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      { name: code + ':WASH_IMPACT_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.WASH_IMPACT_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, WIND_THRESHOLD,
             NAME, WASH_TYPE, CATEGORY, PROBABILITY,
             ZONE_ID, LATITUDE, LONGITUDE, ALL_DATA)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, WIND_THRESHOLD,
            NAME, WASH_TYPE, CATEGORY, PROBABILITY,
            ZONE_ID, LATITUDE, LONGITUDE, ALL_DATA
        FROM AOTS.TC_ECMWF.WASH_IMPACT_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      { name: code + ':BASE_ADMIN_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.BASE_ADMIN_MAT (COUNTRY, TILE_ID, ADMIN_NAME, NAME)
        SELECT DISTINCT '` + code + `', TILE_ID, ADMIN_NAME, NAME
        FROM AOTS.TC_ECMWF.BASE_ADMIN_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      { name: code + ':MERCATOR_TILE_CCI_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT
            (FILE_PATH, COUNTRY, STORM, FORECAST_DATE, ZOOM_LEVEL,
             ZONE_ID, ADMIN_ID,
             CCI_CHILDREN, E_CCI_CHILDREN, CCI_SCHOOL_AGE, E_CCI_SCHOOL_AGE,
             CCI_INFANTS, E_CCI_INFANTS, CCI_POP, E_CCI_POP,
             CCI_ADOLESCENTS, E_CCI_ADOLESCENTS)
        SELECT
            'REGIONAL/` + code + `', '` + code + `',
            STORM, FORECAST_DATE, ZOOM_LEVEL,
            ZONE_ID, ADMIN_ID,
            CCI_CHILDREN, E_CCI_CHILDREN, CCI_SCHOOL_AGE, E_CCI_SCHOOL_AGE,
            CCI_INFANTS, E_CCI_INFANTS, CCI_POP, E_CCI_POP,
            CCI_ADOLESCENTS, E_CCI_ADOLESCENTS
        FROM AOTS.TC_ECMWF.MERCATOR_TILE_CCI_MAT
        WHERE COUNTRY IN (` + inList + `)
      `},

      { name: code + ':ADMIN_ALL_CCI_MAT', sql: `
        INSERT INTO AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT
            (FILE_PATH, ADMIN_LEVEL, COUNTRY, STORM, FORECAST_DATE,
             TILE_ID,
             CCI_CHILDREN, E_CCI_CHILDREN, CCI_SCHOOL_AGE, E_CCI_SCHOOL_AGE,
             CCI_INFANTS, E_CCI_INFANTS, CCI_POP, E_CCI_POP,
             CCI_ADOLESCENTS, E_CCI_ADOLESCENTS)
        SELECT
            'REGIONAL/` + code + `', ADMIN_LEVEL, '` + code + `',
            STORM, FORECAST_DATE,
            TILE_ID,
            CCI_CHILDREN, E_CCI_CHILDREN, CCI_SCHOOL_AGE, E_CCI_SCHOOL_AGE,
            CCI_INFANTS, E_CCI_INFANTS, CCI_POP, E_CCI_POP,
            CCI_ADOLESCENTS, E_CCI_ADOLESCENTS
        FROM AOTS.TC_ECMWF.ADMIN_ALL_CCI_MAT
        WHERE COUNTRY IN (` + inList + `)
      `}
    ];

    for (var i = 0; i < inserts.length; i++) {
      var t = inserts[i];
      try {
        run(t.sql);
        refreshed.push(t.name);
      } catch(e) {
        errors.push(t.name + ': ' + e.message);
      }
    }
  }

  if (errors.length > 0) {
    return 'PARTIAL: refreshed [' + refreshed.join(', ') + '], errors: ' + errors.join(' | ');
  }
  return 'OK: refreshed [' + regions.map(function(r){ return r.code; }).join(', ') + '] at ' + new Date().toISOString();
$$;

GRANT USAGE ON PROCEDURE REFRESH_REGIONAL_GROUPS() TO ROLE SYSADMIN;
