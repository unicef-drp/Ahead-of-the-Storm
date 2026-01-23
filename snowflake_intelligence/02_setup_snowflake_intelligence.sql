-- ============================================================================
-- Step 2: Set Up Snowflake Intelligence
-- ============================================================================
-- This script creates the Snowflake Intelligence object and configures it
-- for use with hurricane impact analysis agents.
--
-- Prerequisites:
-- - ACCOUNTADMIN role (or CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT privilege)
-- - Views created from 01_setup_views.sql
--
-- Configuration:
-- - Database: AOTS
-- - Schema: TC_ECMWF
-- ============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

-- ============================================================================
-- STEP 1: Create Snowflake Intelligence Object
-- ============================================================================
-- This is an account-level object that manages all agents for Snowflake Intelligence

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- ============================================================================
-- STEP 2: Grant Privileges
-- ============================================================================

-- Grant USAGE
-- Allows users to see and use agents in Snowflake Intelligence
GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE SYSADMIN;

-- Grant MODIFY to roles that should be able to add/remove agents
--GRANT MODIFY ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE SYSADMIN;

-- ============================================================================
-- STEP 3: Verify Setup
-- ============================================================================

-- Check if Snowflake Intelligence object exists
SHOW SNOWFLAKE INTELLIGENCES;

-- Check privileges
SHOW GRANTS ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- ============================================================================
-- NOTES:
-- ============================================================================
-- After agents are created, they need to be added to this object using:
-- ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT <agent_name>;