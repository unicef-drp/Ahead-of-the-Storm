-- ============================================================================
-- Evaluation views and dataset registration for HURRICANE_INTELLIGENCE
-- Native Cortex Agent Evaluations
--
-- Run 05_create_eval_dataset.sql first to create the base table and test cases.
--
-- Ground truth output note:
--   Snowflake requires ground_truth_output to be embedded inside the
--   GROUND_TRUTH_DATA JSON (key: 'ground_truth_output'), not as a separate
--   column mapping. All views below merge it in via OBJECT_INSERT when set.
--
-- Views available after running this script:
--
--   Category views:
--     EVAL_FULL_REPORTS         FR cases
--     EVAL_SINGLE_METRICS       SM cases
--     EVAL_MISSING_INPUT        MR cases
--     EVAL_TREND                TR cases
--     EVAL_ADMIN_BREAKDOWN      AB cases
--     EVAL_REFUSALS             RF + HZ cases
--     EVAL_NAMED_FACILITIES     SC + HC cases
--     EVAL_CENTROID_SHIFT       CS cases
--
--   Collection views (curated cross-category sets):
--     EVAL_MIXED                1 case per category (11 total) — quick regression check
--
-- To run an evaluation, set dataset_name in eval_config.yaml to the registered
-- dataset name (see below) and upload + run:
--   PUT file://eval_config.yaml @AOTS.TC_ECMWF.AGENT_STAGE AUTO_COMPRESS=FALSE;
--   CALL EXECUTE_AI_EVALUATION(
--       'START',
--       OBJECT_CONSTRUCT('run_name', 'v3.0_baseline'),
--       '@AOTS.TC_ECMWF.AGENT_STAGE/eval_config.yaml'
--   );
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ============================================================================
-- Helper: merged GROUND_TRUTH_DATA
--   When GROUND_TRUTH_OUTPUT is set, embeds it as 'ground_truth_output' inside
--   the GROUND_TRUTH_DATA JSON so Snowflake's answer_correctness metric sees it.
-- ============================================================================

-- ============================================================================
-- Category views — one per category for focused evaluation after a specific change
-- ============================================================================

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_FULL_REPORTS AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'full_report';

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_SINGLE_METRICS AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'single_metric';

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_MISSING_INPUT AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'missing_input';

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_TREND AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'trend';

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_ADMIN_BREAKDOWN AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'admin_breakdown';

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_REFUSALS AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY IN ('refusal', 'wrong_hazard');

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_NAMED_FACILITIES AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'named_facilities';

CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_CENTROID_SHIFT AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE CATEGORY = 'centroid_shift';


-- ============================================================================
-- Collection views — curated cross-category sets
-- ============================================================================

-- Mixed: 1 representative case per category (11 total).
-- A good default for a quick check across all capabilities after any agent change.
CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_MIXED AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET
    WHERE ID IN (
        'FR-02',  -- full report, explicit date + threshold
        'SM-01',  -- single metric, latest date resolution
        'MR-01',  -- missing input — country only
        'TR-02',  -- trend — better or worse
        'AB-02',  -- admin breakdown — targeted parish question
        'RF-01',  -- refusal — operational recommendation
        'HZ-01',  -- wrong hazard — flooding
        'ML-02',  -- multi-language (French)
        'DQ-01',  -- discovery — active storms
        'SC-01',  -- named facilities — calendar-day date (regression case)
        'CS-01'   -- centroid shift — geographic impact footprint shift
    );


-- ============================================================================
-- Register evaluation datasets
-- ============================================================================
-- Each registered dataset maps a view (or the full table) to a named evalset
-- that can be referenced in eval_config.yaml.
--
-- NOTE: ground_truth_output is embedded inside GROUND_TRUTH_DATA by the views
-- above. Do NOT add 'expected_output' mapping here — Snowflake ignores it
-- silently and answer_correctness will always score 0.

-- Full 72-case baseline (uses a view to get merged GROUND_TRUTH_DATA)
CREATE OR REPLACE VIEW AOTS.TC_ECMWF.EVAL_ALL AS
    SELECT
        ID, CATEGORY, INPUT_QUERY,
        IFF(GROUND_TRUTH_OUTPUT IS NOT NULL,
            OBJECT_INSERT(GROUND_TRUTH_DATA::OBJECT, 'ground_truth_output', TO_VARIANT(GROUND_TRUTH_OUTPUT)),
            GROUND_TRUTH_DATA
        ) AS GROUND_TRUTH_DATA,
        SHOULD_REFUSE, EXPECTED_QUERY_CLASSIFICATION
    FROM AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVAL_DATASET;

CALL SYSTEM$CREATE_EVALUATION_DATASET(
    'CORTEX AGENT',
    'AOTS.TC_ECMWF.EVAL_ALL',
    'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_EVALSET',
    {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'}
);

-- Mixed 11-case set
CALL SYSTEM$CREATE_EVALUATION_DATASET(
    'CORTEX AGENT',
    'AOTS.TC_ECMWF.EVAL_MIXED',
    'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_MIXED_EVALSET',
    {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'}
);

-- Category datasets — for focused evaluation after a change to a specific capability
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_FULL_REPORTS',     'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_FULL_REPORTS_EVALSET',     {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_SINGLE_METRICS',   'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_SINGLE_METRICS_EVALSET',   {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_MISSING_INPUT',    'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_MISSING_INPUT_EVALSET',    {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_TREND',            'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_TREND_EVALSET',            {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_ADMIN_BREAKDOWN',  'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_ADMIN_BREAKDOWN_EVALSET',  {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_REFUSALS',         'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_REFUSALS_EVALSET',         {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_NAMED_FACILITIES',  'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_NAMED_FACILITIES_EVALSET',  {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
CALL SYSTEM$CREATE_EVALUATION_DATASET('CORTEX AGENT', 'AOTS.TC_ECMWF.EVAL_CENTROID_SHIFT',   'AOTS.TC_ECMWF.HURRICANE_INTELLIGENCE_CENTROID_SHIFT_EVALSET',   {'query_text': 'INPUT_QUERY', 'expected_tools': 'GROUND_TRUTH_DATA'});
