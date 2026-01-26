-- ============================================================================
-- AGENT COST TRACKING
-- ============================================================================
-- 1. Gets TOTAL costs from METERING_HISTORY (AI Services + Warehouse)
-- 2. Counts SUCCESSFUL agent calls from QUERY_HISTORY  
-- 3. Divides total cost by successful calls = accurate cost per call
--
-- USAGE:
-- 1. Adjust DAYS_TO_ANALYZE
-- 2. Run to see accurate cost breakdown
--
-- REQUIRES: ACCOUNTADMIN or access to SNOWFLAKE.ACCOUNT_USAGE
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

SET DAYS_TO_ANALYZE = 1;

WITH
-- ============================================================================
-- STEP 1: Get TOTAL costs for the period (all services)
-- ============================================================================
total_costs_by_day AS (
    SELECT 
        DATE(mh.start_time) AS date,
        SUM(CASE WHEN mh.service_type = 'AI_SERVICES' THEN mh.credits_used ELSE 0 END) AS ai_credits,
        SUM(CASE WHEN mh.service_type = 'WAREHOUSE_METERING' AND mh.name = 'AOTS_WH' THEN mh.credits_used ELSE 0 END) AS warehouse_credits,
        SUM(CASE 
            WHEN mh.service_type = 'AI_SERVICES' THEN mh.credits_used
            WHEN mh.service_type = 'WAREHOUSE_METERING' AND mh.name = 'AOTS_WH' THEN mh.credits_used
            ELSE 0
        END) AS total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY mh
    WHERE mh.start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_TIMESTAMP())
        AND (
            mh.service_type = 'AI_SERVICES'
            OR (mh.service_type = 'WAREHOUSE_METERING' AND mh.name = 'AOTS_WH')
        )
    GROUP BY DATE(mh.start_time)
),

-- ============================================================================
-- STEP 2: Count SUCCESSFUL agent calls (real calls only)
-- ============================================================================
successful_calls_by_day AS (
    SELECT 
        DATE(qh.START_TIME) AS date,
        COUNT(*) AS call_count,
        ROUND(AVG(qh.TOTAL_ELAPSED_TIME / 1000), 1) AS avg_execution_seconds,
        ROUND(MIN(qh.TOTAL_ELAPSED_TIME / 1000), 1) AS min_execution_seconds,
        ROUND(MAX(qh.TOTAL_ELAPSED_TIME / 1000), 1) AS max_execution_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    WHERE qh.QUERY_TEXT ILIKE '%SELECT%HURRICANE_SITUATION_INTELLIGENCE%'
        AND qh.EXECUTION_STATUS = 'SUCCESS'
        AND qh.TOTAL_ELAPSED_TIME > 5000  -- More than 5 seconds (filters out quick checks)
        AND qh.START_TIME >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_TIMESTAMP())
    GROUP BY DATE(qh.START_TIME)
),

-- ============================================================================
-- STEP 3: Join costs with call counts and calculate cost per call
-- ============================================================================
daily_summary AS (
    SELECT 
        COALESCE(tc.date, sc.date) AS date,
        COALESCE(sc.call_count, 0) AS successful_calls,
        sc.avg_execution_seconds,
        sc.min_execution_seconds,
        sc.max_execution_seconds,
        COALESCE(tc.ai_credits, 0) AS ai_credits,
        COALESCE(tc.warehouse_credits, 0) AS warehouse_credits,
        COALESCE(tc.total_credits, 0) AS total_credits,
        COALESCE(tc.ai_credits, 0) * 3.0 AS ai_cost_usd,
        COALESCE(tc.warehouse_credits, 0) * 3.0 AS warehouse_cost_usd,
        COALESCE(tc.total_credits, 0) * 3.0 AS total_cost_usd,
        CASE 
            WHEN COALESCE(sc.call_count, 0) > 0 
            THEN COALESCE(tc.total_credits, 0) * 3.0 / sc.call_count
            ELSE 0
        END AS cost_per_call_usd,
        CASE 
            WHEN COALESCE(tc.total_credits, 0) > 0
            THEN ROUND(COALESCE(tc.ai_credits, 0) / tc.total_credits * 100, 1)
            ELSE 0
        END AS ai_pct,
        CASE 
            WHEN COALESCE(tc.total_credits, 0) > 0
            THEN ROUND(COALESCE(tc.warehouse_credits, 0) / tc.total_credits * 100, 1)
            ELSE 0
        END AS warehouse_pct
    FROM total_costs_by_day tc
    FULL OUTER JOIN successful_calls_by_day sc
        ON tc.date = sc.date
    WHERE COALESCE(tc.total_credits, 0) > 0 OR COALESCE(sc.call_count, 0) > 0
),

-- ============================================================================
-- STEP 4: Calculate period totals
-- ============================================================================
period_totals AS (
    SELECT 
        SUM(successful_calls) AS total_calls,
        ROUND(AVG(avg_execution_seconds), 1) AS avg_execution_seconds,
        MIN(min_execution_seconds) AS min_execution_seconds,
        MAX(max_execution_seconds) AS max_execution_seconds,
        SUM(ai_credits) AS total_ai_credits,
        SUM(warehouse_credits) AS total_warehouse_credits,
        SUM(total_credits) AS total_credits,
        SUM(ai_cost_usd) AS total_ai_cost_usd,
        SUM(warehouse_cost_usd) AS total_warehouse_cost_usd,
        SUM(total_cost_usd) AS total_cost_usd,
        CASE 
            WHEN SUM(successful_calls) > 0 
            THEN SUM(total_cost_usd) / SUM(successful_calls)
            ELSE 0
        END AS avg_cost_per_call_usd,
        CASE 
            WHEN SUM(total_cost_usd) > 0
            THEN ROUND(SUM(ai_cost_usd) / SUM(total_cost_usd) * 100, 1)
            ELSE 0
        END AS ai_pct,
        CASE 
            WHEN SUM(total_cost_usd) > 0
            THEN ROUND(SUM(warehouse_cost_usd) / SUM(total_cost_usd) * 100, 1)
            ELSE 0
        END AS warehouse_pct
    FROM daily_summary
)

-- ============================================================================
-- OUTPUT: Daily breakdown + Period summary only
-- ============================================================================

-- SECTION 1: Daily breakdown
SELECT 
    'DAILY' AS section,
    TO_VARCHAR(date) AS date,
    successful_calls AS calls,
    avg_execution_seconds AS avg_seconds,
    min_execution_seconds AS min_seconds,
    max_execution_seconds AS max_seconds,
    ROUND(ai_credits, 4) AS ai_credits,
    ROUND(warehouse_credits, 4) AS warehouse_credits,
    ROUND(total_credits, 4) AS total_credits,
    ROUND(ai_cost_usd, 2) AS ai_cost_usd,
    ROUND(warehouse_cost_usd, 2) AS warehouse_cost_usd,
    ROUND(total_cost_usd, 2) AS total_cost_usd,
    ROUND(cost_per_call_usd, 2) AS cost_per_call_usd,
    TO_VARCHAR(ai_pct) || '%' AS ai_pct,
    TO_VARCHAR(warehouse_pct) || '%' AS warehouse_pct
FROM daily_summary

UNION ALL

-- SECTION 2: Period summary
SELECT 
    'SUMMARY',
    'TOTAL (' || $DAYS_TO_ANALYZE || ' days)',
    total_calls,
    avg_execution_seconds,
    min_execution_seconds,
    max_execution_seconds,
    ROUND(total_ai_credits, 4),
    ROUND(total_warehouse_credits, 4),
    ROUND(total_credits, 4),
    ROUND(total_ai_cost_usd, 2),
    ROUND(total_warehouse_cost_usd, 2),
    ROUND(total_cost_usd, 2),
    ROUND(avg_cost_per_call_usd, 2),
    TO_VARCHAR(ai_pct) || '%',
    TO_VARCHAR(warehouse_pct) || '%'
FROM period_totals

ORDER BY 
    CASE section
        WHEN 'DAILY' THEN 1
        WHEN 'SUMMARY' THEN 2
    END,
    date DESC;