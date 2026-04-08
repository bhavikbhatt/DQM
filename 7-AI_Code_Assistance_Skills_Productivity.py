# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks AI Code Assistance, Skills & Productivity
# MAGIC
# MAGIC This notebook walks through the AI-powered productivity features available in Databricks,
# MAGIC with practical examples applied to the Data Quality use case from Notebooks 1–6.
# MAGIC
# MAGIC ### Sections
# MAGIC
# MAGIC | Section | Capability | What You'll See |
# MAGIC |---------|-----------|----------------|
# MAGIC | **1** | Genie Code — AI assistant in notebooks | Autocomplete, `/explain`, `/fix`, `/doc`, `@` references |
# MAGIC | **2** | Genie Code Skills — packaging domain knowledge | Create a custom DQ skill that Genie Code loads automatically |
# MAGIC | **3** | Genie Code Agent Mode — autonomous workflows | Let the AI build an entire analysis notebook |
# MAGIC | **4** | AI/BI Genie Spaces — trusted NL-to-SQL | UC functions as trusted assets for conversational analytics |
# MAGIC | **5** | Best practices for accuracy & productivity | Tips to get the most out of AI assistance |
# MAGIC
# MAGIC ### Documentation
# MAGIC | Topic | Link |
# MAGIC |-------|------|
# MAGIC | Genie Code Overview | [docs.databricks.com/.../genie-code](https://docs.databricks.com/aws/en/genie-code/) |
# MAGIC | Use Genie Code | [docs.databricks.com/.../use-genie-code](https://docs.databricks.com/aws/en/genie-code/use-genie-code) |
# MAGIC | Genie Code Skills | [docs.databricks.com/.../skills](https://docs.databricks.com/aws/en/genie-code/skills) |
# MAGIC | Tips for Better Responses | [docs.databricks.com/.../tips](https://docs.databricks.com/aws/en/genie-code/tips) |
# MAGIC | Data Science Agent | [docs.databricks.com/.../ds-agent](https://docs.databricks.com/aws/en/notebooks/ds-agent) |
# MAGIC | AI/BI Genie Spaces | [docs.databricks.com/.../genie](https://docs.databricks.com/aws/en/genie/set-up) |
# MAGIC | Trusted Assets | [docs.databricks.com/.../trusted-assets](https://docs.databricks.com/aws/en/genie/trusted-assets) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Genie Code — AI Assistant in Notebooks
# MAGIC
# MAGIC **Genie Code** (formerly "Databricks Assistant") is the AI coding assistant built into
# MAGIC every Databricks notebook. It is Unity Catalog-aware — it knows your tables, columns,
# MAGIC and metadata.
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC | Feature | How to Use | What It Does |
# MAGIC |---------|-----------|--------------|
# MAGIC | **Autocomplete** | Start typing — or press `Option+Shift+Space` (Mac) / `Ctrl+Shift+Space` (Win) | Context-aware code suggestions using your UC metadata |
# MAGIC | **Chat** | Click the Genie Code icon in the sidebar (or `Cmd+I`) | Conversational AI — ask questions, generate code, iterate |
# MAGIC | **`/explain`** | Select code → type `/explain` in chat | Explains what the selected code does |
# MAGIC | **`/fix`** | Select code with an error → type `/fix` in chat | Diagnoses and fixes the error |
# MAGIC | **`/doc`** | Select a function → type `/doc` in chat | Generates docstrings and inline comments |
# MAGIC | **`@table_name`** | Type `@` followed by a table/notebook/file name | Brings that resource into the conversation as context |
# MAGIC | **Quick Fix** | Click the lightbulb on a single-line error | Auto-fix triggered inline without opening chat |
# MAGIC
# MAGIC ### Live Demo: Try These in This Notebook
# MAGIC
# MAGIC The cells below are designed for **live demos** of each feature.

# COMMAND ----------

# DBTITLE 1,1a. Autocomplete Demo — Start Typing and Watch
# --- AUTOCOMPLETE DEMO ---
# Instructions: Place your cursor at the end of each line below and press
# Option+Shift+Space (Mac) or Ctrl+Shift+Space (Win) to trigger autocomplete.
#
# Genie Code knows about your Unity Catalog tables. Try:

# Type: spark.read.table("uhg_demos.cdo_data_quality.
# → Autocomplete suggests table names: member_info, employee_record, customer_contact, etc.

# Type: df = spark.table("uhg_demos.cdo_data_quality.member_info").select("
# → Autocomplete suggests column names: member_id, first_name, last_name, etc.

# Try it below (uncomment and start editing):
# df = spark.table("uhg_demos.cdo_data_quality.member_info")

# COMMAND ----------

# DBTITLE 1,1b. Generate Code from a Comment
# --- CODE GENERATION FROM COMMENTS ---
# Instructions: Write a comment describing what you want, then press
# Option+Shift+Space — Genie Code generates the code.
#
# Try these (uncomment one at a time and trigger autocomplete after the comment):

# Read the member_info table and show the count of records with invalid email addresses

# Find all columns tagged with 'uhg_dq' in the cdo_data_quality schema using information_schema

# Calculate the percentage of null values per column in the employee_record table

# COMMAND ----------

# DBTITLE 1,1c. /explain Demo
# --- /EXPLAIN DEMO ---
# Instructions:
#   1. Select the code block below
#   2. Open Genie Code chat (Cmd+I or sidebar icon)
#   3. Type: /explain
#   4. Genie Code will explain what this query does in plain English

# Select this code, then use /explain:
from pyspark.sql import functions as F

summary = (
    spark.table("uhg_demos.cdo_data_quality.cdo_dq_results")
    .groupBy("table_name", "rule_name")
    .agg(
        F.sum("total_rows").alias("total_scanned"),
        F.sum("rows_error").alias("total_errors"),
        F.round(
            F.sum("rows_error") * 100.0 / F.sum("total_rows"), 2
        ).alias("error_rate_pct")
    )
    .orderBy(F.desc("error_rate_pct"))
)

display(summary)

# COMMAND ----------

# DBTITLE 1,1d. /fix Demo — Intentional Errors
# --- /FIX DEMO ---
# Instructions:
#   1. Run this cell — it will fail with an error
#   2. Select the code
#   3. Open Genie Code chat and type: /fix
#   4. Genie Code diagnoses the error and suggests the fix

# This has intentional errors for demo purposes:
from pyspark.sql import functions as F

# Error 1: Wrong table name (typo)
# Error 2: Wrong column name
df = spark.table("uhg_demos.cdo_data_quality.memberr_info")  # typo: memberr
result = df.groupBy("fist_name").count()  # typo: fist_name → first_name
display(result)

# COMMAND ----------

# DBTITLE 1,1e. /doc Demo
# --- /DOC DEMO ---
# Instructions:
#   1. Select the function below
#   2. Open Genie Code chat and type: /doc
#   3. Genie Code generates a complete docstring

def calculate_dq_score(table_name, results_table="uhg_demos.cdo_data_quality.cdo_dq_results"):
    from pyspark.sql import functions as F

    results = spark.table(results_table).filter(F.col("table_name") == table_name)
    total = results.agg(F.sum("total_rows")).collect()[0][0] or 0
    errors = results.agg(F.sum("rows_error")).collect()[0][0] or 0

    if total == 0:
        return {"table": table_name, "score": None, "status": "NO DATA"}

    score = round((1 - errors / total) * 100, 2)
    status = "PASS" if score >= 95 else "WARN" if score >= 80 else "FAIL"
    return {"table": table_name, "score": score, "status": status}

# COMMAND ----------

# DBTITLE 1,1f. @ References — Pull Context Into Chat
# MAGIC %md
# MAGIC ### Using `@` References
# MAGIC
# MAGIC In the Genie Code chat, type `@` to reference a specific resource. Genie Code
# MAGIC loads that resource as context for better, more accurate responses.
# MAGIC
# MAGIC **Examples to try in chat:**
# MAGIC
# MAGIC | What to type | What happens |
# MAGIC |-------------|-------------|
# MAGIC | `@uhg_demos.cdo_data_quality.member_info` | Genie Code sees the full schema (columns, types, tags) |
# MAGIC | `@uhg_demos.cdo_data_quality.cdo_dq_results` | References the DQ results table for query generation |
# MAGIC | `@3-DQ_Runner` | Pulls the DQ Runner notebook code into context |
# MAGIC
# MAGIC **Try this prompt in chat:**
# MAGIC > Using @uhg_demos.cdo_data_quality.cdo_dq_results, write a query that shows the
# MAGIC > top 5 tables with the highest error rates from the most recent scan run.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Genie Code Skills — Packaging Domain Knowledge
# MAGIC
# MAGIC **Skills** are reusable packages of domain knowledge that Genie Code loads automatically
# MAGIC when relevant. They follow the open [agentskills.io](https://agentskills.io/) standard.
# MAGIC
# MAGIC ### Why Skills Matter
# MAGIC
# MAGIC Without a skill, Genie Code gives generic answers. With a skill, it gives answers
# MAGIC tailored to **your** organization's conventions, naming standards, and best practices.
# MAGIC
# MAGIC | Without Skill | With DQ Skill |
# MAGIC |--------------|---------------|
# MAGIC | "Here's a generic null check query" | "Here's a query using your `cdo_dq_rule_mapping` table and UC tags" |
# MAGIC | Generic column validation | Knows about `uhg_dq` tag conventions and DQX engine |
# MAGIC | No awareness of your DQ architecture | References your results table, rule mapping, and chargeback setup |
# MAGIC
# MAGIC ### Skill Structure
# MAGIC
# MAGIC ```
# MAGIC Workspace/.assistant/skills/
# MAGIC └── data-quality-ops/
# MAGIC     ├── SKILL.md            ← Main skill definition (YAML frontmatter + instructions)
# MAGIC     ├── dq_patterns.sql     ← Optional: SQL templates referenced by the skill
# MAGIC     └── examples/
# MAGIC         └── chargeback.py   ← Optional: example code for the AI to reference
# MAGIC ```
# MAGIC
# MAGIC Two placement options:
# MAGIC - **Workspace-wide:** `/Workspace/.assistant/skills/<name>/` — available to everyone
# MAGIC - **Personal:** `/Workspace/Users/{you}/.assistant/skills/<name>/` — only for you

# COMMAND ----------

# DBTITLE 1,2a. Create a Data Quality Skill — SKILL.md
# This cell creates the skill definition file.
# In practice, you'd create this file in the workspace UI or via the workspace API.

skill_md_content = """---
name: data-quality-ops
description: >
  Use this skill when the user asks about data quality monitoring, DQ rules,
  tag-based quality checks, DQX scanning, chargeback for DQ operations,
  or anomaly detection on Unity Catalog tables. Covers the CDO Data Quality
  architecture including rule mapping, DQX engine, DQM anomaly detection,
  and multi-tenant billing attribution.
---

# Data Quality Operations — CDO Team Conventions

## Architecture Overview

Our DQ architecture uses **Unity Catalog tags as the universal join key**:
- Column tags (`uhg_dq = firstName`, `uhg_dq = lastName`) drive DQX rule mapping
- Schema tags (`dqm_monitor = enabled`) drive anomaly detection enrollment
- Schema tags (`billing_code`, `askid`) drive chargeback attribution

## Key Tables

| Table | Purpose |
|-------|---------|
| `uhg_demos.cdo_data_quality.cdo_dq_rule_mapping` | Maps UC tag names to SQL validation rules |
| `uhg_demos.cdo_data_quality.cdo_dq_results` | Aggregated DQ scan results (errors, warnings per column) |
| `uhg_demos.cdo_data_quality.dqx_client_registry` | Approved clients with billing codes |
| `uhg_demos.cdo_data_quality.dqx_scan_audit_log` | Audit trail of all DQ scan requests |
| `system.data_quality_monitoring.table_results` | DQM anomaly detection results (freshness, completeness) |
| `system.billing.usage` | Billing data — filter with `element_at(custom_tags, 'billing_code')` |

## DQX Engine Usage

We use the `databricks-labs-dqx` library. The pattern is:
1. Discover columns via `system.information_schema.column_tags`
2. Look up rules from `cdo_dq_rule_mapping` based on tag values
3. Apply checks via `DQEngine.apply_checks_by_metadata(df, rules)`
4. Write summary results to `cdo_dq_results`

## Chargeback Queries

For billing attribution, always use:
```sql
element_at(custom_tags, 'billing_code') AS billing_code
```
on `system.billing.usage` and filter with:
```sql
WHERE element_at(custom_tags, 'cost_center') = 'DQ-Scanning'
```

## Naming Conventions

- Tag names: lowercase with underscores (`uhg_dq`, `dqm_monitor`, `billing_code`)
- Rule names: Title Case with spaces ("First Name Conformance")
- Billing codes: UPPERCASE-NNN format ("ACME-001")
"""

print("=== SKILL.md Content ===")
print(skill_md_content)

# COMMAND ----------

# DBTITLE 1,2b. Deploy the Skill to the Workspace
# Deploy the skill files to the workspace
import json

SKILL_PATH = "/Workspace/.assistant/skills/data-quality-ops"

# Create the skill directory and file
# Method 1: Via the Workspace API
w = WorkspaceClient()

# Create the directory
try:
    w.workspace.mkdirs(SKILL_PATH)
    print(f"Created directory: {SKILL_PATH}")
except Exception as e:
    print(f"Directory may already exist: {e}")

# Upload SKILL.md
import base64
skill_bytes = skill_md_content.encode("utf-8")

try:
    w.workspace.import_(
        path=f"{SKILL_PATH}/SKILL.md",
        content=base64.b64encode(skill_bytes).decode("utf-8"),
        format="AUTO",
        overwrite=True
    )
    print(f"Deployed: {SKILL_PATH}/SKILL.md")
except Exception as e:
    print(f"Upload result: {e}")

print(f"\nSkill deployed to: {SKILL_PATH}")
print("Genie Code will auto-load this skill when users ask about data quality.")

# COMMAND ----------

# DBTITLE 1,2c. Add a SQL Template to the Skill (Optional)
# Optional: add a SQL template file that the skill can reference
# This teaches Genie Code your team's exact query patterns

dq_patterns_sql = """-- =============================================================
-- DQ Patterns — Standard Queries for the CDO Data Quality Team
-- =============================================================

-- Pattern 1: Error rate by table and rule (most recent scan)
SELECT
    table_name,
    rule_name,
    total_rows,
    rows_error,
    ROUND(rows_error * 100.0 / total_rows, 2) AS error_rate_pct
FROM uhg_demos.cdo_data_quality.cdo_dq_results
WHERE run_id = (SELECT MAX(run_id) FROM uhg_demos.cdo_data_quality.cdo_dq_results)
ORDER BY error_rate_pct DESC;

-- Pattern 2: DQ trend over time
SELECT
    DATE(scan_date) AS scan_day,
    table_name,
    rule_name,
    ROUND(AVG(rows_error * 100.0 / total_rows), 2) AS avg_error_rate
FROM uhg_demos.cdo_data_quality.cdo_dq_results
GROUP BY DATE(scan_date), table_name, rule_name
ORDER BY scan_day DESC, avg_error_rate DESC;

-- Pattern 3: Chargeback summary
SELECT
    element_at(custom_tags, 'billing_code') AS billing_code,
    element_at(custom_tags, 'requesting_team') AS team,
    DATE_TRUNC('month', usage_date) AS month,
    ROUND(SUM(usage_quantity), 4) AS dbus
FROM system.billing.usage
WHERE element_at(custom_tags, 'cost_center') = 'DQ-Scanning'
GROUP BY ALL
ORDER BY month DESC, dbus DESC;
"""

try:
    sql_bytes = dq_patterns_sql.encode("utf-8")
    w.workspace.import_(
        path=f"{SKILL_PATH}/dq_patterns.sql",
        content=base64.b64encode(sql_bytes).decode("utf-8"),
        format="AUTO",
        overwrite=True
    )
    print(f"Deployed: {SKILL_PATH}/dq_patterns.sql")
except Exception as e:
    print(f"SQL template upload: {e}")

# COMMAND ----------

# DBTITLE 1,2d. Verify the Skill
# List the skill files to confirm deployment
try:
    items = w.workspace.list(SKILL_PATH)
    print(f"Skill contents at {SKILL_PATH}:")
    for item in items:
        print(f"  {item.path}")
except Exception as e:
    print(f"Could not list: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing the Skill
# MAGIC
# MAGIC Now that the skill is deployed, try these prompts in **Genie Code chat**:
# MAGIC
# MAGIC 1. **"Show me the error rate for each table from the latest DQ scan"**
# MAGIC    → Genie Code uses the skill's table knowledge to write a precise query against `cdo_dq_results`
# MAGIC
# MAGIC 2. **"How much DQX compute did ACME-001 use last month?"**
# MAGIC    → Genie Code knows to query `system.billing.usage` with `element_at(custom_tags, 'billing_code')`
# MAGIC
# MAGIC 3. **"Add a new DQ rule for email format validation using the DQX engine"**
# MAGIC    → Genie Code references the skill's DQX pattern and rule mapping conventions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Genie Code Agent Mode — Autonomous Workflows
# MAGIC
# MAGIC **Agent Mode** lets Genie Code plan and execute multi-step tasks autonomously.
# MAGIC Instead of generating one cell at a time, it creates an entire notebook — planning,
# MAGIC writing code, running it, debugging errors, and iterating.
# MAGIC
# MAGIC ### How to Activate
# MAGIC 1. Open Genie Code chat (`Cmd+I` or sidebar icon)
# MAGIC 2. Toggle **Agent Mode** ON (switch at the top of the chat panel)
# MAGIC 3. Give it a high-level goal
# MAGIC
# MAGIC ### Demo Prompts to Try
# MAGIC
# MAGIC | Prompt | What Agent Mode Does |
# MAGIC |--------|---------------------|
# MAGIC | "Build a DQ scorecard notebook that reads from `@uhg_demos.cdo_data_quality.cdo_dq_results` and creates a summary with pass/fail status per table" | Plans → creates cells → runs them → fixes any errors |
# MAGIC | "Analyze the DQM anomaly detection results from `@system.data_quality_monitoring.table_results` and create visualizations showing freshness and completeness trends" | Multi-cell analysis with charts |
# MAGIC | "Create a parameterized notebook that accepts a billing_code and generates a chargeback report from system.billing.usage" | Builds a reusable report notebook |
# MAGIC
# MAGIC ### Data Science Agent
# MAGIC
# MAGIC For ML/analytics use cases, Databricks also offers the **Data Science Agent** — a
# MAGIC specialized agent that builds complete EDA, feature engineering, and model training
# MAGIC notebooks from scratch. Activate it from the notebook menu:
# MAGIC **New Notebook → Data Science Agent**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. AI/BI Genie Spaces — Trusted NL-to-SQL for Analytics
# MAGIC
# MAGIC **Genie Spaces** are conversational analytics interfaces where business users ask
# MAGIC questions in natural language and get SQL-backed answers. For the DQ use case, this
# MAGIC means stakeholders can ask "What's our data quality score?" without writing SQL.
# MAGIC
# MAGIC ### Trusted Assets = Accurate Answers
# MAGIC
# MAGIC **Trusted assets** are Unity Catalog functions and example queries that Genie uses
# MAGIC to produce verified, accurate results. When Genie uses a trusted asset, the response
# MAGIC is labeled **"Trusted"** — giving users confidence in the answer.
# MAGIC
# MAGIC ### The Pattern
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │  Step 1: Create UC Functions (SQL)                        │
# MAGIC │  → Encapsulate complex business logic                     │
# MAGIC │  → Add thorough COMMENT strings                           │
# MAGIC │                                                           │
# MAGIC │  Step 2: Register in Genie Space                          │
# MAGIC │  → Instructions tab → SQL Queries → Add UC function       │
# MAGIC │                                                           │
# MAGIC │  Step 3: Users Ask Questions                              │
# MAGIC │  → "What's the DQ score for member_info?"                 │
# MAGIC │  → Genie calls the UC function → "Trusted" response       │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4a. Create UC Function: DQ Score by Table
# MAGIC %sql
# MAGIC -- This UC function encapsulates the DQ score calculation.
# MAGIC -- When registered as a trusted asset in a Genie Space, users can ask
# MAGIC -- "What's the DQ score for member_info?" and get a Trusted response.
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION uhg_demos.cdo_data_quality.fn_dq_score_by_table(
# MAGIC     target_table STRING COMMENT 'The table name to check, e.g., member_info or uhg_demos.cdo_data_quality.member_info'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC     table_name STRING,
# MAGIC     rule_name STRING,
# MAGIC     total_rows BIGINT,
# MAGIC     rows_error BIGINT,
# MAGIC     error_rate_pct DOUBLE,
# MAGIC     status STRING,
# MAGIC     scan_date TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Returns the data quality score for a specific table from the most recent DQ scan. Shows error rates per rule and a PASS/WARN/FAIL status. Use this when someone asks about data quality scores, error rates, or DQ status for a table.'
# MAGIC RETURN
# MAGIC     SELECT
# MAGIC         r.table_name,
# MAGIC         r.rule_name,
# MAGIC         r.total_rows,
# MAGIC         r.rows_error,
# MAGIC         ROUND(r.rows_error * 100.0 / r.total_rows, 2) AS error_rate_pct,
# MAGIC         CASE
# MAGIC             WHEN r.rows_error * 100.0 / r.total_rows <= 5  THEN 'PASS'
# MAGIC             WHEN r.rows_error * 100.0 / r.total_rows <= 20 THEN 'WARN'
# MAGIC             ELSE 'FAIL'
# MAGIC         END AS status,
# MAGIC         r.scan_date
# MAGIC     FROM uhg_demos.cdo_data_quality.cdo_dq_results r
# MAGIC     WHERE r.table_name LIKE CONCAT('%', target_table, '%')
# MAGIC       AND r.run_id = (SELECT MAX(run_id) FROM uhg_demos.cdo_data_quality.cdo_dq_results)
# MAGIC     ORDER BY error_rate_pct DESC;

# COMMAND ----------

# DBTITLE 1,4b. Create UC Function: DQ Trend Over Time
# MAGIC %sql
# MAGIC -- Trend function: shows how DQ scores change over time for a given table
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION uhg_demos.cdo_data_quality.fn_dq_trend(
# MAGIC     target_table STRING COMMENT 'The table name to analyze trends for',
# MAGIC     days_back INT DEFAULT 30 COMMENT 'Number of days to look back, default 30'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC     scan_day DATE,
# MAGIC     table_name STRING,
# MAGIC     avg_error_rate DOUBLE,
# MAGIC     total_scans BIGINT
# MAGIC )
# MAGIC COMMENT 'Shows the data quality error rate trend over time for a table. Use this when someone asks about DQ trends, improvements, or regressions. Returns daily averages for the specified lookback period.'
# MAGIC RETURN
# MAGIC     SELECT
# MAGIC         DATE(scan_date) AS scan_day,
# MAGIC         table_name,
# MAGIC         ROUND(AVG(rows_error * 100.0 / total_rows), 2) AS avg_error_rate,
# MAGIC         COUNT(*) AS total_scans
# MAGIC     FROM uhg_demos.cdo_data_quality.cdo_dq_results
# MAGIC     WHERE table_name LIKE CONCAT('%', target_table, '%')
# MAGIC       AND scan_date >= DATEADD(DAY, -days_back, CURRENT_TIMESTAMP())
# MAGIC     GROUP BY DATE(scan_date), table_name
# MAGIC     ORDER BY scan_day DESC;

# COMMAND ----------

# DBTITLE 1,4c. Create UC Function: Client Chargeback Summary
# MAGIC %sql
# MAGIC -- Chargeback function: shows DQ scanning costs per client billing code
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION uhg_demos.cdo_data_quality.fn_dq_chargeback(
# MAGIC     target_billing_code STRING DEFAULT NULL COMMENT 'Optional: filter to a specific billing code like ACME-001. NULL returns all clients.',
# MAGIC     months_back INT DEFAULT 3 COMMENT 'Number of months to look back, default 3'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC     billing_code STRING,
# MAGIC     requesting_team STRING,
# MAGIC     usage_month DATE,
# MAGIC     total_dbus DOUBLE,
# MAGIC     scan_runs BIGINT
# MAGIC )
# MAGIC COMMENT 'Returns the DQ scanning chargeback summary per client billing code. Shows DBUs consumed and number of scan runs. Use this when someone asks about DQ costs, chargeback, billing, or client usage.'
# MAGIC RETURN
# MAGIC     SELECT
# MAGIC         element_at(custom_tags, 'billing_code') AS billing_code,
# MAGIC         element_at(custom_tags, 'requesting_team') AS requesting_team,
# MAGIC         DATE_TRUNC('month', usage_date) AS usage_month,
# MAGIC         ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC         COUNT(DISTINCT usage_metadata.job_run_id) AS scan_runs
# MAGIC     FROM system.billing.usage
# MAGIC     WHERE element_at(custom_tags, 'cost_center') = 'DQ-Scanning'
# MAGIC       AND element_at(custom_tags, 'billing_code') IS NOT NULL
# MAGIC       AND usage_date >= DATEADD(MONTH, -months_back, CURRENT_DATE())
# MAGIC       AND (target_billing_code IS NULL
# MAGIC            OR element_at(custom_tags, 'billing_code') = target_billing_code)
# MAGIC     GROUP BY
# MAGIC         element_at(custom_tags, 'billing_code'),
# MAGIC         element_at(custom_tags, 'requesting_team'),
# MAGIC         DATE_TRUNC('month', usage_date)
# MAGIC     ORDER BY usage_month DESC, total_dbus DESC;

# COMMAND ----------

# DBTITLE 1,4d. Test the UC Functions
# MAGIC %sql
# MAGIC -- Test: DQ score for member_info
# MAGIC SELECT * FROM uhg_demos.cdo_data_quality.fn_dq_score_by_table('member_info');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test: DQ trend for member_info over last 30 days
# MAGIC SELECT * FROM uhg_demos.cdo_data_quality.fn_dq_trend('member_info', 30);

# COMMAND ----------

# DBTITLE 1,4e. Set Up a Genie Space with These Functions
# MAGIC %md
# MAGIC ### Creating the Genie Space
# MAGIC
# MAGIC 1. Go to **AI/BI Genie** in the left sidebar → **New Genie Space**
# MAGIC 2. Name it: `CDO Data Quality Insights`
# MAGIC 3. Add tables:
# MAGIC    - `uhg_demos.cdo_data_quality.cdo_dq_results`
# MAGIC    - `uhg_demos.cdo_data_quality.cdo_dq_rule_mapping`
# MAGIC    - `uhg_demos.cdo_data_quality.dqx_client_registry`
# MAGIC 4. Go to **Instructions** tab → **SQL Queries** → Add:
# MAGIC    - `uhg_demos.cdo_data_quality.fn_dq_score_by_table`
# MAGIC    - `uhg_demos.cdo_data_quality.fn_dq_trend`
# MAGIC    - `uhg_demos.cdo_data_quality.fn_dq_chargeback`
# MAGIC 5. Add general instructions:
# MAGIC    ```
# MAGIC    This space provides insights on data quality for the CDO team.
# MAGIC    - Use fn_dq_score_by_table when asked about DQ scores or error rates
# MAGIC    - Use fn_dq_trend when asked about quality trends over time
# MAGIC    - Use fn_dq_chargeback when asked about costs or billing per client
# MAGIC    - Error rate thresholds: ≤5% = PASS, ≤20% = WARN, >20% = FAIL
# MAGIC    ```
# MAGIC
# MAGIC ### Questions Users Can Now Ask (Trusted Responses)
# MAGIC
# MAGIC | User Question | Genie Uses |
# MAGIC |--------------|-----------|
# MAGIC | "What's the DQ score for member_info?" | `fn_dq_score_by_table('member_info')` |
# MAGIC | "Show me DQ trends for employee_record over the last 60 days" | `fn_dq_trend('employee_record', 60)` |
# MAGIC | "How much did ACME-001 spend on DQ scans?" | `fn_dq_chargeback('ACME-001')` |
# MAGIC | "Which tables are failing data quality checks?" | `fn_dq_score_by_table` filtered by FAIL status |
# MAGIC | "Compare DQ costs across all clients this quarter" | `fn_dq_chargeback(NULL, 3)` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Best Practices — Maximizing AI Accuracy & Productivity
# MAGIC
# MAGIC ### Genie Code Best Practices
# MAGIC
# MAGIC | Practice | Why It Matters |
# MAGIC |---------|---------------|
# MAGIC | **Be specific — name tables and columns** | "Query the `cdo_dq_results` table for error rates" → much better than "show me DQ data" |
# MAGIC | **Use `@` references** | `@uhg_demos.cdo_data_quality.member_info` gives Genie Code the full schema context |
# MAGIC | **Iterate in conversation** | Build on previous responses — "now filter that to just the FAIL rows" |
# MAGIC | **Use `/explain` before modifying** | Understand existing code before changing it — prevents drift |
# MAGIC | **Deploy Skills for your team** | Skills encode institutional knowledge that persists across sessions |
# MAGIC | **Leverage Agent Mode for greenfield work** | "Build a scorecard notebook" → Agent plans, codes, and debugs |
# MAGIC
# MAGIC ### Genie Spaces Best Practices
# MAGIC
# MAGIC | Practice | Why It Matters |
# MAGIC |---------|---------------|
# MAGIC | **Write thorough `COMMENT` strings on UC functions** | Genie reads comments to decide when to invoke functions — vague comments = wrong function choices |
# MAGIC | **Prioritize UC functions over plain text instructions** | Functions produce **Trusted** responses; text instructions produce best-effort |
# MAGIC | **Use parameterized functions** | `fn_dq_score_by_table('member_info')` is reusable; a hardcoded query is not |
# MAGIC | **Add example queries** | Genie learns your team's query patterns from examples |
# MAGIC | **Grant `EXECUTE` on functions** | Users need function-level permissions, not just table SELECT |
# MAGIC | **Test with real user questions** | Ask the questions your stakeholders actually ask — tune instructions based on results |
# MAGIC
# MAGIC ### Productivity Multipliers
# MAGIC
# MAGIC | Scenario | Without AI | With AI |
# MAGIC |----------|-----------|---------|
# MAGIC | Write a DQ scoring query | Lookup table schema, write SQL from scratch | Type intent → autocomplete generates it |
# MAGIC | Debug a failing DQ pipeline | Read error, search docs, try fixes | Select code → `/fix` → apply suggestion |
# MAGIC | Onboard new team member | Read all notebooks, ask colleagues | `/explain` on each notebook, `@` reference tables |
# MAGIC | Ad-hoc stakeholder question | "Let me write a query and get back to you" | Stakeholder asks Genie Space directly → Trusted answer |
# MAGIC | Build a new analysis | Start from blank notebook | Agent Mode builds it end-to-end |