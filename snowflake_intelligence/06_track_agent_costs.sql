-- ============================================================================
-- AGENT COST TRACKING (Warehouse-Based Approach)
-- ============================================================================
-- Uses warehouse-based filtering for accurate cost tracking
-- Works regardless of whether Cortex Analyst tables are available
-- Combines AI costs and warehouse costs from SF_AI_WH warehouse
-- Calculates cost per agent call for monitoring
--
-- APPROACH:
-- 1. Get AI costs from METERING_HISTORY filtered by SF_AI_WH warehouse
--    (This captures all AI service costs on the agent warehouse)
-- 2. Get warehouse costs from WAREHOUSE_METERING_HISTORY (SF_AI_WH only)
-- 3. Count successful agent calls from QUERY_HISTORY
-- 4. Calculate cost per call (total cost / call count)
--
-- REQUIRES: ACCOUNTADMIN or access to SNOWFLAKE.ACCOUNT_USAGE
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

SET DAYS_TO_ANALYZE = 1;

WITH
-- ============================================================================
-- STEP 1: Get AI costs from Metering History
-- ============================================================================
-- Get AI service costs from METERING_HISTORY
-- NOTE: AI_SERVICES costs may not be directly filterable by warehouse in METERING_HISTORY
-- If SF_AI_WH is dedicated ONLY to agent queries, we can use all AI_SERVICES costs
-- OR filter by time period and assume they're agent-related if warehouse is dedicated
--
-- Alternative: If CORTEX_ANALYST_USAGE_HISTORY exists and has data, use that instead
-- (uncomment the alternative CTE below if Cortex Analyst tables are available)
ai_costs_by_day AS (
    SELECT 
        DATE(start_time) AS date,
        SUM(credits_used) AS ai_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
    WHERE start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_DATE())
        AND service_type = 'AI_SERVICES'
        -- NOTE: If AI_SERVICES can't be filtered by warehouse, this includes ALL AI costs
        -- Only accurate if SF_AI_WH is the ONLY source of AI service usage in your account
        -- OR if you can filter by warehouse (check your METERING_HISTORY structure)
    GROUP BY DATE(start_time)
),
-- ALTERNATIVE: Use Cortex Analyst Usage History if available (uncomment if needed)
-- ai_costs_by_day AS (
--     SELECT 
--         DATE_TRUNC('day', start_time) AS date,
--         SUM(credits) AS ai_credits
--     FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
--     WHERE start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_DATE())
--     GROUP BY DATE_TRUNC('day', start_time)
-- ),

-- ============================================================================
-- STEP 2: Get warehouse costs from Warehouse Metering History
-- ============================================================================
-- Filter by SF_AI_WH warehouse to get only agent-related warehouse costs
warehouse_costs_by_day AS (
    SELECT 
        DATE_TRUNC('day', start_time) AS date,
        SUM(credits_used) AS warehouse_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_DATE())
        AND warehouse_name = 'SF_AI_WH'
    GROUP BY DATE_TRUNC('day', start_time)
),

-- ============================================================================
-- STEP 3: Count successful agent calls from Query History
-- ============================================================================
-- Count actual agent invocations by searching for:
-- 1. SNOWFLAKE.CORTEX.COMPLETE function calls (the function used to call agents)
-- 2. Agent identifier: AOTS.TC_ECMWF.HURRICANE_SITUATION_INTELLIGENCE
-- 3. Filter by execution time > 5 seconds to exclude quick checks/metadata queries
-- 4. Filter by warehouse = SF_AI_WH to ensure we only count agent-related queries
--
-- NOTE: This approach counts queries that call the agent, but each agent call
-- may generate multiple internal queries (tool calls). For cost per "user call",
-- this is the correct count. For cost per "tool invocation", you'd need to
-- look at the stored procedure calls separately.
successful_calls_by_day AS (
    SELECT 
        DATE(start_time) AS date,
        COUNT(*) AS call_count,
        ROUND(AVG(total_elapsed_time / 1000), 1) AS avg_execution_seconds,
        ROUND(MIN(total_elapsed_time / 1000), 1) AS min_execution_seconds,
        ROUND(MAX(total_elapsed_time / 1000), 1) AS max_execution_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE (
        -- Look for SNOWFLAKE.CORTEX.COMPLETE function calls with agent name
        (query_text ILIKE '%SNOWFLAKE.CORTEX.COMPLETE%' 
         AND query_text ILIKE '%HURRICANE_SITUATION_INTELLIGENCE%')
        -- OR look for the full agent identifier
        OR query_text ILIKE '%AOTS.TC_ECMWF.HURRICANE_SITUATION_INTELLIGENCE%'
    )
        AND execution_status = 'SUCCESS'
        AND total_elapsed_time > 5000  -- More than 5 seconds (filters out quick checks)
        AND warehouse_name = 'SF_AI_WH'  -- Only count queries on agent warehouse
        AND start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_DATE())
    GROUP BY DATE(start_time)
),

-- ============================================================================
-- STEP 4: Combine costs and calculate daily totals
-- ============================================================================
daily_costs AS (
    SELECT 
        COALESCE(ac.date, wc.date) AS date,
        COALESCE(ac.ai_credits, 0) AS ai_credits,
        COALESCE(wc.warehouse_credits, 0) AS warehouse_credits,
        COALESCE(ac.ai_credits, 0) + COALESCE(wc.warehouse_credits, 0) AS total_credits,
        COALESCE(ac.ai_credits, 0) * 3.0 AS ai_cost_usd,
        COALESCE(wc.warehouse_credits, 0) * 3.0 AS warehouse_cost_usd,
        (COALESCE(ac.ai_credits, 0) + COALESCE(wc.warehouse_credits, 0)) * 3.0 AS total_cost_usd
    FROM ai_costs_by_day ac
    FULL OUTER JOIN warehouse_costs_by_day wc
        ON ac.date = wc.date
),

-- ============================================================================
-- STEP 5: Join costs with call counts and calculate cost per call
-- ============================================================================
daily_summary AS (
    SELECT 
        COALESCE(dc.date, sc.date) AS date,
        COALESCE(sc.call_count, 0) AS successful_calls,
        sc.avg_execution_seconds,
        sc.min_execution_seconds,
        sc.max_execution_seconds,
        COALESCE(dc.ai_credits, 0) AS ai_credits,
        COALESCE(dc.warehouse_credits, 0) AS warehouse_credits,
        COALESCE(dc.total_credits, 0) AS total_credits,
        COALESCE(dc.ai_cost_usd, 0) AS ai_cost_usd,
        COALESCE(dc.warehouse_cost_usd, 0) AS warehouse_cost_usd,
        COALESCE(dc.total_cost_usd, 0) AS total_cost_usd,
        CASE 
            WHEN COALESCE(sc.call_count, 0) > 0 
            THEN COALESCE(dc.total_cost_usd, 0) / sc.call_count
            ELSE 0
        END AS cost_per_call_usd,
        CASE 
            WHEN COALESCE(dc.total_credits, 0) > 0
            THEN ROUND(COALESCE(dc.ai_credits, 0) / dc.total_credits * 100, 1)
            ELSE 0
        END AS ai_pct,
        CASE 
            WHEN COALESCE(dc.total_credits, 0) > 0
            THEN ROUND(COALESCE(dc.warehouse_credits, 0) / dc.total_credits * 100, 1)
            ELSE 0
        END AS warehouse_pct
    FROM daily_costs dc
    FULL OUTER JOIN successful_calls_by_day sc
        ON dc.date = sc.date
    WHERE COALESCE(dc.total_credits, 0) > 0 OR COALESCE(sc.call_count, 0) > 0
),

-- ============================================================================
-- STEP 6: Calculate period totals
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
-- OUTPUT: Daily breakdown + Period summary
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
    'SUMMARY' AS section,
    'TOTAL (' || $DAYS_TO_ANALYZE || ' days)' AS date,
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