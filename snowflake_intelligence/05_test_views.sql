-- ============================================================================
-- Test Script: Verify Views Can Read from Snowflake Stage
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;


-- ============================================================================
-- Test 1: Test CSV View (ADMIN_IMPACT_VIEWS_RAW)
-- ============================================================================

SELECT 
    country,
    storm,
    forecast_date,
    wind_threshold,
    COUNT(*) AS row_count,
    SUM(E_population) AS total_population,
    SUM(E_num_schools) AS total_schools,
    SUM(E_num_hcs) AS total_hcs
FROM ADMIN_IMPACT_VIEWS_RAW
WHERE country = 'JAM' AND wind_threshold = 34
GROUP BY country, storm, forecast_date, wind_threshold
ORDER BY forecast_date DESC
LIMIT 5;


-- ============================================================================
-- Test 2: Test CSV View (MERCATOR_TILE_IMPACT_VIEWS_RAW)
-- ============================================================================

SELECT 
    country,
    storm,
    forecast_date,
    wind_threshold,
    zoom_level,
    COUNT(*) AS row_count,
    SUM(E_population) AS total_population,
    SUM(E_num_schools) AS total_schools,
    SUM(E_num_hcs) AS total_hcs
FROM MERCATOR_TILE_IMPACT_VIEWS_RAW
WHERE country = 'JAM' AND wind_threshold = 34 AND zoom_level = 14
GROUP BY country, storm, forecast_date, wind_threshold, zoom_level
ORDER BY forecast_date DESC
LIMIT 5;


-- ============================================================================
-- Test 3: Test Parquet View (TRACK_VIEWS_RAW)
-- ============================================================================

-- First check if view can read data
SELECT 
    country,
    storm,
    forecast_date,
    wind_threshold,
    COUNT(*) AS row_count,
    COUNT(DISTINCT zone_id) AS distinct_members,
    SUM(severity_population) AS total_population,
    SUM(severity_schools) AS total_schools,
    SUM(severity_hcs) AS total_hcs
FROM TRACK_VIEWS_RAW
WHERE country = 'JAM' AND wind_threshold = 34
GROUP BY country, storm, forecast_date, wind_threshold
ORDER BY forecast_date DESC
LIMIT 5;

-- Test worst-case member selection (as agent would do)
WITH member_impacts AS (
    SELECT 
        zone_id,
        SUM(COALESCE(severity_population, 0)) AS member_population,
        SUM(COALESCE(severity_schools, 0)) AS member_schools,
        SUM(COALESCE(severity_hcs, 0)) AS member_hcs
    FROM TRACK_VIEWS_RAW
    WHERE country = 'JAM' 
      AND storm = 'MELISSA'
      AND forecast_date = '20251028000000'
      AND wind_threshold = 34
    GROUP BY zone_id
)
SELECT 
    zone_id AS worst_case_member,
    member_population,
    member_schools,
    member_hcs
FROM member_impacts
ORDER BY member_population DESC
LIMIT 1;


-- ============================================================================
-- Test 4: Test Parquet Views (SCHOOL and HC)
-- ============================================================================

SELECT 
    country,
    storm,
    forecast_date,
    wind_threshold,
    COUNT(*) AS row_count
FROM SCHOOL_IMPACT_VIEWS_RAW
WHERE country = 'JAM' AND wind_threshold = 34
GROUP BY country, storm, forecast_date, wind_threshold
ORDER BY forecast_date DESC
LIMIT 5;

SELECT 
    country,
    storm,
    forecast_date,
    wind_threshold,
    COUNT(*) AS row_count
FROM HEALTH_CENTER_IMPACT_VIEWS_RAW
WHERE country = 'JAM' AND wind_threshold = 34
GROUP BY country, storm, forecast_date, wind_threshold
ORDER BY forecast_date DESC
LIMIT 5;


-- ============================================================================
-- Test 5: Compare Expected vs Worst-Case (Agent Logic Test)
-- ============================================================================

-- Expected values (from MERCATOR_TILE_IMPACT_VIEWS_RAW)
WITH expected_values AS (
    SELECT 
        SUM(COALESCE(E_population, 0)) AS expected_population,
        SUM(COALESCE(E_num_schools, 0)) AS expected_schools,
        SUM(COALESCE(E_num_hcs, 0)) AS expected_hcs
    FROM MERCATOR_TILE_IMPACT_VIEWS_RAW
    WHERE country = 'JAM'
      AND storm = 'MELISSA'
      AND forecast_date = '20251028000000'
      AND wind_threshold = 34
      AND zoom_level = 14
),
worst_case_values AS (
    WITH member_impacts AS (
        SELECT 
            zone_id,
            SUM(COALESCE(severity_population, 0)) AS member_population,
            SUM(COALESCE(severity_schools, 0)) AS member_schools,
            SUM(COALESCE(severity_hcs, 0)) AS member_hcs
        FROM TRACK_VIEWS_RAW
        WHERE country = 'JAM'
          AND storm = 'MELISSA'
          AND forecast_date = '20251028000000'
          AND wind_threshold = 34
        GROUP BY zone_id
    )
    SELECT 
        member_population AS worst_case_population,
        member_schools AS worst_case_schools,
        member_hcs AS worst_case_hcs
    FROM member_impacts
    ORDER BY member_population DESC
    LIMIT 1
)
SELECT 
    ev.expected_population,
    wc.worst_case_population,
    ROUND(ev.expected_population / NULLIF(wc.worst_case_population, 0), 2) AS ratio,
    ev.expected_schools,
    wc.worst_case_schools,
    ev.expected_hcs,
    wc.worst_case_hcs,
    CASE 
        WHEN ev.expected_population / NULLIF(wc.worst_case_population, 0) >= 1.0 THEN 'ERROR: Expected >= Worst-case (aggregating multiple storms?)'
        WHEN ev.expected_population / NULLIF(wc.worst_case_population, 0) < 0.01 THEN 'ERROR: Expected << Worst-case (wrong view?)'
        WHEN ev.expected_population / NULLIF(wc.worst_case_population, 0) BETWEEN 0.3 AND 0.7 THEN 'OK: Ratio is reasonable'
        ELSE 'WARNING: Ratio outside expected range'
    END AS validation_status
FROM expected_values ev
CROSS JOIN worst_case_values wc;