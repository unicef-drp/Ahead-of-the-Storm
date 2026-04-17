-- ============================================================================
-- AGENT COST TRACKING — Per-Query Breakdown (Token + Warehouse)
-- ============================================================================
-- Primary source: SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
-- Provides exact per-query token and credit breakdown with ~45-min latency.
--
-- Warehouse cost (est.): WAREHOUSE_METERING_HISTORY for SF_AI_WH.
--   Uses actual credits_used (per-second billing, 60-second minimum).
--   Per-query cost is prorated: actual hourly warehouse cost / queries in that hour.
--   Labeled "(est.)" throughout — it is an approximation.
--
-- Output:
--   SECTION 1 — Individual query log (most recent first)
--   SECTION 2 — Daily summary
--   SECTION 3 — Period summary
--
-- Token columns (from tokens_granular JSON, per Snowflake docs):
--   cache_read_input  — tokens served from prompt cache (lowest cost)
--   cache_write_input — tokens written to prompt cache (one-time cache creation cost)
--   input             — uncached input tokens
--   output            — output / completion tokens
--
-- Warehouse size reference (Gen1, credits/hour — source: Snowflake docs):
--   XS=1  S=2  M=4  L=8  XL=16  2XL=32  3XL=64  4XL=128
--   SF_AI_WH is XS → 1 credit/hr
--   The script uses actual credits_used from WAREHOUSE_METERING_HISTORY, so
--   the size table above is for reference only — no formula change needed.
--
-- REQUIRES: ACCOUNTADMIN or access to SNOWFLAKE.ACCOUNT_USAGE
-- ============================================================================

SET DAYS_TO_ANALYZE  = 7;
SET CREDIT_PRICE_USD = 3.0;   -- ($/credit)
SET WH_NAME          = 'SF_AI_WH';

-- ============================================================================
-- Step 1: Flatten tokens_granular JSON to get per-query token counts
-- Structure: array → {query_id} → cortex_agents → {model} → {cache_read_input, input, output}
-- ============================================================================
WITH flattened AS (
    SELECT
        h.start_time,
        h.end_time,
        h.agent_name,
        h.token_credits,
        SUM(model_tok.value:cache_read_input::INT)  AS tokens_cache_read,
        SUM(model_tok.value:cache_write_input::INT) AS tokens_cache_write,
        SUM(model_tok.value:input::INT)             AS tokens_input,
        SUM(model_tok.value:output::INT)            AS tokens_output
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY h,
         LATERAL FLATTEN(input => h.tokens_granular)           arr_tok,
         LATERAL FLATTEN(input => arr_tok.value)               qid_tok,
         LATERAL FLATTEN(input => qid_tok.value:cortex_agents) model_tok
    WHERE h.start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_TIMESTAMP())
      AND h.agent_name ILIKE '%HURRICANE%'
      AND qid_tok.key != 'start_time'
    GROUP BY h.start_time, h.end_time, h.agent_name, h.token_credits
),

-- ============================================================================
-- Step 2: Count queries per calendar hour (for warehouse cost proration)
-- ============================================================================
queries_per_hour AS (
    SELECT
        DATE_TRUNC('hour', start_time) AS hour_bucket,
        COUNT(*)                       AS query_count
    FROM flattened
    GROUP BY DATE_TRUNC('hour', start_time)
),

-- ============================================================================
-- Step 3: Get hourly warehouse credits for SF_AI_WH
-- Uses actual credits_used from WAREHOUSE_METERING_HISTORY multiplied by
-- $CREDIT_PRICE_USD. Reflects real per-second uptime — a warm but idle XS
-- warehouse may show < 1 credit/hr.
-- ============================================================================
wh_hourly AS (
    SELECT
        DATE_TRUNC('hour', start_time)    AS hour_bucket,
        SUM(credits_used)                 AS wh_credits_used,
        SUM(credits_used) * $CREDIT_PRICE_USD AS wh_cost_usd
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name = $WH_NAME
      AND start_time >= DATEADD('days', -$DAYS_TO_ANALYZE, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('hour', start_time)
),

-- ============================================================================
-- Step 4: Build raw per-query rows with prorated warehouse cost
-- ============================================================================
raw AS (
    SELECT
        f.start_time,
        f.end_time,
        DATEDIFF('second', f.start_time, f.end_time)           AS duration_seconds,
        f.agent_name,
        -- LLM token cost (exact)
        f.token_credits                                         AS token_credits,
        f.token_credits * $CREDIT_PRICE_USD                    AS token_cost_usd,
        -- Token counts
        COALESCE(f.tokens_cache_read, 0)                       AS tokens_cache_read,
        COALESCE(f.tokens_cache_write, 0)                      AS tokens_cache_write,
        COALESCE(f.tokens_input, 0)                            AS tokens_input,
        COALESCE(f.tokens_output, 0)                           AS tokens_output,
        COALESCE(f.tokens_cache_read, 0)
            + COALESCE(f.tokens_cache_write, 0)
            + COALESCE(f.tokens_input, 0)
            + COALESCE(f.tokens_output, 0)                     AS tokens_total,
        -- Warehouse cost (estimated, prorated by queries in same hour)
        COALESCE(w.wh_cost_usd, 0) / NULLIF(q.query_count, 0) AS wh_cost_est_usd,
        -- Total cost = token + warehouse (est.)
        f.token_credits * $CREDIT_PRICE_USD
            + COALESCE(w.wh_cost_usd, 0) / NULLIF(q.query_count, 0) AS total_cost_usd
    FROM flattened f
    LEFT JOIN queries_per_hour q
        ON DATE_TRUNC('hour', f.start_time) = q.hour_bucket
    LEFT JOIN wh_hourly w
        ON DATE_TRUNC('hour', f.start_time) = w.hour_bucket
),

-- ============================================================================
-- Step 5: Daily summary
-- ============================================================================
daily AS (
    SELECT
        DATE(start_time)                   AS date,
        COUNT(*)                           AS calls,
        ROUND(AVG(duration_seconds), 1)    AS avg_seconds,
        ROUND(MIN(duration_seconds), 1)    AS min_seconds,
        ROUND(MAX(duration_seconds), 1)    AS max_seconds,
        SUM(tokens_total)                  AS tokens_total,
        SUM(tokens_cache_read)             AS tokens_cache_read,
        SUM(tokens_cache_write)            AS tokens_cache_write,
        SUM(tokens_input)                  AS tokens_input,
        SUM(tokens_output)                 AS tokens_output,
        ROUND(SUM(token_cost_usd), 4)      AS token_cost_usd,
        ROUND(SUM(wh_cost_est_usd), 4)     AS wh_cost_est_usd,
        ROUND(SUM(total_cost_usd), 4)      AS total_cost_usd,
        ROUND(AVG(total_cost_usd), 4)      AS avg_cost_per_call_usd,
        ROUND(MIN(total_cost_usd), 4)      AS min_cost_usd,
        ROUND(MAX(total_cost_usd), 4)      AS max_cost_usd
    FROM raw
    GROUP BY DATE(start_time)
),

-- ============================================================================
-- Step 6: Period totals
-- ============================================================================
period AS (
    SELECT
        COUNT(*)                           AS calls,
        ROUND(AVG(duration_seconds), 1)    AS avg_seconds,
        SUM(tokens_total)                  AS tokens_total,
        SUM(tokens_cache_read)             AS tokens_cache_read,
        SUM(tokens_cache_write)            AS tokens_cache_write,
        SUM(tokens_input)                  AS tokens_input,
        SUM(tokens_output)                 AS tokens_output,
        ROUND(SUM(token_cost_usd), 4)      AS token_cost_usd,
        ROUND(SUM(wh_cost_est_usd), 4)     AS wh_cost_est_usd,
        ROUND(SUM(total_cost_usd), 4)      AS total_cost_usd,
        ROUND(AVG(total_cost_usd), 4)      AS avg_cost_per_call_usd,
        ROUND(MIN(total_cost_usd), 4)      AS min_cost_usd,
        ROUND(MAX(total_cost_usd), 4)      AS max_cost_usd
    FROM raw
)

-- ============================================================================
-- SECTION 1: Per-query log
-- ============================================================================
SELECT
    'QUERY'                                          AS section,
    TO_VARCHAR(start_time, 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
    agent_name,
    duration_seconds                                 AS seconds,
    tokens_total,
    tokens_cache_read,
    tokens_cache_write,
    tokens_input,
    tokens_output,
    ROUND(token_cost_usd, 4)                         AS token_cost_usd,
    ROUND(wh_cost_est_usd, 4)                        AS wh_cost_est_usd,
    ROUND(total_cost_usd, 4)                         AS total_cost_usd,
    NULL::VARCHAR                                    AS note
FROM raw

UNION ALL

-- ============================================================================
-- SECTION 2: Daily summary rows
-- ============================================================================
SELECT
    'DAY'                  AS section,
    TO_VARCHAR(date)       AS timestamp,
    NULL                   AS agent_name,
    avg_seconds            AS seconds,
    tokens_total,
    tokens_cache_read,
    tokens_cache_write,
    tokens_input,
    tokens_output,
    token_cost_usd,
    wh_cost_est_usd,
    total_cost_usd,
    calls || ' calls | avg $' || avg_cost_per_call_usd
        || ' | range $' || min_cost_usd || '–$' || max_cost_usd AS note
FROM daily

UNION ALL

-- ============================================================================
-- SECTION 3: Period summary
-- ============================================================================
SELECT
    'TOTAL'                                     AS section,
    'LAST ' || $DAYS_TO_ANALYZE || ' DAYS'      AS timestamp,
    NULL                                        AS agent_name,
    avg_seconds                                 AS seconds,
    tokens_total,
    tokens_cache_read,
    tokens_cache_write,
    tokens_input,
    tokens_output,
    token_cost_usd,
    wh_cost_est_usd,
    total_cost_usd,
    calls || ' calls | avg $' || avg_cost_per_call_usd
        || ' | range $' || min_cost_usd || '–$' || max_cost_usd AS note
FROM period

ORDER BY
    CASE section
        WHEN 'QUERY' THEN 1
        WHEN 'DAY'   THEN 2
        WHEN 'TOTAL' THEN 3
    END,
    timestamp DESC;