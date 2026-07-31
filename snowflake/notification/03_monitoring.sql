-- ==============================================================================
-- 07b_alert_agent/03_monitoring.sql — Alert Cost & Delivery Monitoring
-- ==============================================================================
-- Run any section independently in Snowsight to inspect alert activity and cost.
--
-- Sections:
--   1. Delivery log         — what was sent, to how many recipients, when
--   2. Cortex LLM usage     — token consumption per alert window
--   3. Warehouse compute    — credits consumed by SEND_ALERT()
--   4. Combined cost view   — LLM + compute joined to alert log by timestamp
--   5. Rolling totals       — spend by day / week / month
--
-- Note on latency: SNOWFLAKE.ACCOUNT_USAGE views have up to 45-minute lag.
-- For immediate post-run checks use the INFORMATION_SCHEMA table functions
-- (Section 3b) which reflect data within seconds but only cover 7 days.
-- ==============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ─── CONFIG ───────────────────────────────────────────────────────────────────
-- Set Snowflake credit price in USD before running cost queries.
SET credit_price_usd = 3.00;


-- ==============================================================================
-- SECTION 1: Alert delivery log
-- What was sent, to whom, and when.
-- ==============================================================================

-- 1a. All alerts — most recent first
SELECT
    SENT_AT,
    TRACK_ID,
    TO_CHAR(FORECAST_TIME, 'YYYY-MM-DD HH24:MI') AS FORECAST_RUN,
    COUNTRY_CODE,
    RECIPIENT_COUNT,
    LENGTH(EMAIL_BODY)                            AS EMAIL_BODY_CHARS,
    EMAIL_SUBJECT
FROM AOTS.TC_ECMWF.ALERT_SENT_LOG
ORDER BY SENT_AT DESC;


-- 1b. Summary — alerts per storm
SELECT
    TRACK_ID,
    COUNT(DISTINCT COUNTRY_CODE)     AS countries_alerted,
    SUM(RECIPIENT_COUNT)             AS total_emails_sent,
    MIN(SENT_AT)                     AS first_alert,
    MAX(SENT_AT)                     AS last_alert
FROM AOTS.TC_ECMWF.ALERT_SENT_LOG
GROUP BY TRACK_ID
ORDER BY last_alert DESC;


-- 1c. Alerts in the last 30 days
SELECT
    DATE_TRUNC('day', SENT_AT)       AS alert_day,
    COUNT(*)                         AS alert_count,
    SUM(RECIPIENT_COUNT)             AS emails_sent
FROM AOTS.TC_ECMWF.ALERT_SENT_LOG
WHERE SENT_AT >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- ==============================================================================
-- SECTION 2: Cortex LLM usage
-- Token consumption from CORTEX.COMPLETE calls made by SEND_ALERT().
-- ACCOUNT_USAGE lag: up to 45 minutes.
-- ==============================================================================

-- 2a. LLM usage over the last 30 days, by model
SELECT
    DATE_TRUNC('day', START_TIME)    AS usage_day,
    MODEL_NAME,
    SUM(INPUT_TOKENS)                AS input_tokens,
    SUM(OUTPUT_TOKENS)               AS output_tokens,
    SUM(INPUT_TOKENS + OUTPUT_TOKENS) AS total_tokens,
    SUM(TOKEN_CREDITS)               AS credits_used,
    ROUND(SUM(TOKEN_CREDITS) * $credit_price_usd, 4) AS estimated_usd,
    COUNT(*)                         AS calls
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, credits_used DESC;


-- 2b. Per-call detail — last 7 days (useful for spotting outlier prompt sizes)
SELECT
    START_TIME,
    MODEL_NAME,
    FUNCTION_NAME,
    INPUT_TOKENS,
    OUTPUT_TOKENS,
    TOKEN_CREDITS,
    ROUND(TOKEN_CREDITS * $credit_price_usd, 5) AS estimated_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;


-- 2c. Average tokens per CORTEX.COMPLETE call (baseline for cost estimation)
SELECT
    MODEL_NAME,
    COUNT(*)                                      AS total_calls,
    ROUND(AVG(INPUT_TOKENS))                      AS avg_input_tokens,
    ROUND(AVG(OUTPUT_TOKENS))                     AS avg_output_tokens,
    ROUND(AVG(TOKEN_CREDITS), 6)                  AS avg_credits_per_call,
    ROUND(AVG(TOKEN_CREDITS) * $credit_price_usd, 5) AS avg_usd_per_call
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1;


-- ==============================================================================
-- SECTION 3a: Warehouse compute — ACCOUNT_USAGE (45-min lag, 365-day history)
-- Finds queries belonging to SEND_ALERT() by procedure name + warehouse.
-- ==============================================================================

SELECT
    DATE_TRUNC('day', START_TIME)                 AS query_day,
    WAREHOUSE_NAME,
    COUNT(*)                                      AS query_count,
    ROUND(SUM(EXECUTION_TIME) / 1000.0, 1)        AS total_execution_sec,
    ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 6)    AS cloud_svc_credits,
    ROUND(SUM(CREDITS_USED_CLOUD_SERVICES) * $credit_price_usd, 4) AS cloud_svc_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND (
      QUERY_TEXT ILIKE '%SEND_ALERT%'
      OR QUERY_TEXT ILIKE '%ALERT_SENT_LOG%'
      OR QUERY_TEXT ILIKE '%AOTS_EMAIL_INTEGRATION%'
  )
GROUP BY 1, 2
ORDER BY 1 DESC;


-- ==============================================================================
-- SECTION 3b: Warehouse compute — INFORMATION_SCHEMA (near real-time, 7 days)
-- Use this immediately after a test run for instant feedback.
-- ==============================================================================

SELECT
    START_TIME,
    WAREHOUSE_NAME,
    QUERY_TYPE,
    ROUND(EXECUTION_TIME / 1000.0, 2)             AS execution_sec,
    CREDITS_USED_CLOUD_SERVICES,
    ROUND(CREDITS_USED_CLOUD_SERVICES * $credit_price_usd, 5) AS estimated_usd,
    LEFT(QUERY_TEXT, 120)                         AS query_preview
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
    RESULT_LIMIT     => 200
))
WHERE WAREHOUSE_NAME = 'AOTS_WH'
  AND (
      QUERY_TEXT ILIKE '%SEND_ALERT%'
      OR QUERY_TEXT ILIKE '%ALERT_SENT_LOG%'
      OR QUERY_TEXT ILIKE '%AOTS_EMAIL_INTEGRATION%'
  )
ORDER BY START_TIME DESC;


-- ==============================================================================
-- SECTION 4: Combined cost view
-- Joins Cortex LLM spend to alert log entries by 5-minute timestamp windows.
-- Gives a per-alert-run cost estimate.
-- ==============================================================================

WITH alert_windows AS (
    -- Give each alert a ±5-minute window to match Cortex calls by time
    SELECT
        SENT_AT,
        DATEADD('minute', -5, SENT_AT) AS window_start,
        DATEADD('minute',  5, SENT_AT) AS window_end,
        TRACK_ID,
        COUNTRY_CODE,
        RECIPIENT_COUNT
    FROM AOTS.TC_ECMWF.ALERT_SENT_LOG
    WHERE SENT_AT >= DATEADD('day', -30, CURRENT_TIMESTAMP())
),
cortex_by_window AS (
    SELECT
        c.START_TIME,
        c.TOKEN_CREDITS,
        c.INPUT_TOKENS,
        c.OUTPUT_TOKENS,
        a.TRACK_ID,
        a.COUNTRY_CODE,
        a.SENT_AT,
        a.RECIPIENT_COUNT
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY c
    JOIN alert_windows a
      ON c.START_TIME BETWEEN a.window_start AND a.window_end
    WHERE c.START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT
    SENT_AT,
    TRACK_ID,
    COUNTRY_CODE,
    RECIPIENT_COUNT,
    SUM(INPUT_TOKENS)                                       AS llm_input_tokens,
    SUM(OUTPUT_TOKENS)                                      AS llm_output_tokens,
    ROUND(SUM(TOKEN_CREDITS), 6)                            AS llm_credits,
    ROUND(SUM(TOKEN_CREDITS) * $credit_price_usd, 5)        AS llm_usd,
    ROUND(SUM(TOKEN_CREDITS) * $credit_price_usd, 5)        AS approx_total_usd
FROM cortex_by_window
GROUP BY SENT_AT, TRACK_ID, COUNTRY_CODE, RECIPIENT_COUNT
ORDER BY SENT_AT DESC;


-- ==============================================================================
-- SECTION 5: Rolling totals — spend by period
-- ==============================================================================

-- 5a. Daily spend (LLM only — warehouse metering requires separate join)
SELECT
    DATE_TRUNC('day', START_TIME)                          AS day,
    SUM(INPUT_TOKENS)                                      AS input_tokens,
    SUM(OUTPUT_TOKENS)                                     AS output_tokens,
    ROUND(SUM(TOKEN_CREDITS), 6)                           AS credits,
    ROUND(SUM(TOKEN_CREDITS) * $credit_price_usd, 4)       AS usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- 5b. Month-to-date and last-month summary
SELECT
    DATE_TRUNC('month', START_TIME)                        AS month,
    COUNT(*)                                               AS llm_calls,
    SUM(INPUT_TOKENS)                                      AS input_tokens,
    SUM(OUTPUT_TOKENS)                                     AS output_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4)                           AS credits,
    ROUND(SUM(TOKEN_CREDITS) * $credit_price_usd, 2)       AS usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- 5c. Cost per recipient (amortised — LLM cost divided across all emails sent)
WITH llm_total AS (
    SELECT ROUND(SUM(TOKEN_CREDITS) * $credit_price_usd, 4) AS total_llm_usd
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
    WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
),
email_total AS (
    SELECT SUM(RECIPIENT_COUNT) AS total_recipients
    FROM AOTS.TC_ECMWF.ALERT_SENT_LOG
    WHERE SENT_AT >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT
    l.total_llm_usd,
    e.total_recipients,
    CASE WHEN e.total_recipients > 0
         THEN ROUND(l.total_llm_usd / e.total_recipients, 5)
         ELSE NULL
    END AS llm_usd_per_recipient
FROM llm_total l, email_total e;
