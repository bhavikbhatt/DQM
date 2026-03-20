-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Tag Driven Data Quality Management on Databricks
-- MAGIC
-- MAGIC This notebook sets up an example of how to configure centralized
-- MAGIC Data Quality management utilizing tag -> rule mapping and recording evaluation
-- MAGIC results.
-- MAGIC
-- MAGIC For this example, a dq rule configuration is also created that monitors
-- MAGIC all columns tagged with `uhg_dq: firstName` or `uhg_dq: lastName` that validates format aligns
-- MAGIC with specified name format expectations.  (Not null, first letter capitalized.)
-- MAGIC

-- COMMAND ----------

USE CATALOG uhg_demos;
USE SCHEMA cdo_data_quality;


-- COMMAND ----------

-- =============================================================
-- 1. CENTRAL RULE CONFIGURATION TABLE
-- This table maps UC Tag Names to the SQL logic they represent.
-- =============================================================
CREATE OR REPLACE TABLE cdo_dq_rule_mapping (
    tag_name        STRING,      -- e.g., 'uhg_dq'
    tag_value       STRING,      -- e.g., 'firstName'
    rule_name       STRING,      -- Friendly name for reporting
    sql_expression  STRING,      -- The actual logic (parameterized with {col})
    criticality     STRING,      
    created_at      TIMESTAMP
) 
USING DELTA;



-- COMMAND ----------

-- =============================================================
-- CENTRAL DATA QUALITY RESULTS TABLE (Aggregated)
-- This table stores summary counts per rule execution.
-- =============================================================
CREATE OR REPLACE TABLE uhg_demos.cdo_data_quality.cdo_dq_results (
    table_name      STRING,      -- Full path: catalog.schema.table
    column_name     STRING,      -- The column that was scanned
    rule_name       STRING,      -- Rule applied
    total_rows      LONG,        -- Total records scanned
    rows_error      LONG,        -- Count of rows where _errors was not null
    rows_warn       LONG,        -- Count of rows where _warnings was not null
    scan_date       TIMESTAMP,   -- When the scan occurred
    run_id          STRING       -- Unique ID for the job run
) 
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Untitled
-- =============================================================
-- 3. SEED THE MAPPING DATA
-- =============================================================

--- 
-- FIRST AND LAST NAME CONFORMANCE
--
INSERT INTO cdo_dq_rule_mapping (tag_name, tag_value, rule_name, sql_expression, criticality)
VALUES 
    ('uhg_dq',
     'firstName', 
     'First Name Conformance', 
     "{col} IS NOT NULL AND LENGTH(TRIM({col})) >= 1 AND {col} RLIKE '^[A-Za-z][A-Za-z'' -]*$'",
     'error'),
    ('uhg_dq',
     'lastName', 
     'Last Name Conformance', 
     "{col} IS NOT NULL AND LENGTH(TRIM({col})) >= 1 AND {col} RLIKE '^[A-Za-z][A-Za-z'' -]*$'",
     'error');

