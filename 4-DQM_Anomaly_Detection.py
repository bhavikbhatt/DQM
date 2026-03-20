# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Monitoring (DQM) — Anomaly Detection
# MAGIC
# MAGIC This notebook is a comprehensive reference for enabling, managing, and querying
# MAGIC **Databricks Data Quality Monitoring (Anomaly Detection)** at scale using the SDK, REST API, SQL, and CLI.
# MAGIC
# MAGIC ### What DQM Anomaly Detection monitors (automatically):
# MAGIC | Dimension | What it does |
# MAGIC |-----------|-------------|
# MAGIC | **Freshness** | Analyzes commit history to predict the next update — flags tables as stale if commits are unusually late |
# MAGIC | **Completeness** | Predicts expected row volume over 24h — flags tables with fewer rows than predicted minimum |
# MAGIC | **Percent Null** *(Beta)* | Monitors null-value percentage per column over 24h periods |
# MAGIC
# MAGIC ### Sections
# MAGIC 1. **Enable DQM on a schema** (SDK, REST API, CLI) — including table exclusion
# MAGIC 2. **Permissions reference** — minimum grants needed
# MAGIC 3. **Mass-enable DQM** on all schemas with a governed tag
# MAGIC 4. **Query DQM results** from system tables
# MAGIC 5. **Programmatically add a governed tag** to a schema
# MAGIC 6. **Query which schemas have DQM enabled**
# MAGIC
# MAGIC ### Documentation Links
# MAGIC | Topic | Link |
# MAGIC |-------|------|
# MAGIC | DQM Overview | [docs.databricks.com/data-quality-monitoring](https://docs.databricks.com/aws/en/data-quality-monitoring) |
# MAGIC | Anomaly Detection | [docs.databricks.com/.../anomaly-detection](https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection/) |
# MAGIC | DQM System Tables | [docs.databricks.com/.../system-tables](https://docs.databricks.com/aws/en/admin/system-tables/data-quality-monitoring) |
# MAGIC | Create Monitor API | [docs.databricks.com/.../create-monitor-api](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/create-monitor-api) |
# MAGIC | Governed Tags | [docs.databricks.com/.../governed-tags](https://docs.databricks.com/aws/en/admin/governed-tags/) |
# MAGIC | UC Privileges Reference | [docs.databricks.com/.../manage-privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges.html) |
# MAGIC | REST API (Quality Monitors) | [docs.databricks.com/api/.../qualitymonitors](https://docs.databricks.com/api/workspace/qualitymonitors) |

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.68.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Target catalog/schema for single-schema demos
CATALOG = "uhg_demos"
SCHEMA  = "cdo_data_quality"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Enable DQM (Anomaly Detection) on a Schema
# MAGIC
# MAGIC DQM is enabled at the **schema level**. Once enabled, Databricks automatically monitors
# MAGIC every table in that schema for freshness and completeness anomalies.
# MAGIC
# MAGIC ### Minimum Permissions
# MAGIC | Action | Required Privilege |
# MAGIC |--------|-------------------|
# MAGIC | Enable DQM on a schema | `MANAGE` on the schema — **or** be the schema owner |
# MAGIC | View health indicators on tables | `SELECT` or `BROWSE` on the table |
# MAGIC | Access DQM system tables | Account Admin (must explicitly grant to others) |

# COMMAND ----------

# DBTITLE 1,1a. Enable DQM via Python SDK
from databricks.sdk.service.dataquality import Monitor, AnomalyDetectionConfig

# Look up the schema to get its ID
schema_info = w.schemas.get(full_name=f"{CATALOG}.{SCHEMA}")
print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Schema ID: {schema_info.schema_id}")

# Clean up any existing monitor so this cell is re-runnable
try:
    w.data_quality.delete_monitor(object_type="schema", object_id=schema_info.schema_id)
    print("Deleted existing monitor.")
except Exception:
    pass  # No existing monitor — nothing to delete

# Enable anomaly detection on the schema
monitor = Monitor(
    object_type="schema",
    object_id=schema_info.schema_id,
    anomaly_detection_config=AnomalyDetectionConfig()
)

result = w.data_quality.create_monitor(monitor=monitor)
print(f"\nDQM Enabled. Monitor: {result}")

# COMMAND ----------

# DBTITLE 1,1b. Enable DQM via REST API (curl equivalent)
# MAGIC %md
# MAGIC ### REST API — Enable DQM on a schema
# MAGIC
# MAGIC ```bash
# MAGIC # Step 1: Get the schema_id
# MAGIC SCHEMA_ID=$(databricks schemas get uhg_demos.cdo_data_quality | jq -r '.schema_id')
# MAGIC
# MAGIC # Step 2: Create the monitor
# MAGIC curl -X POST "https://${DATABRICKS_HOST}/api/2.0/data-quality/monitors" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d "{
# MAGIC     \"monitor\": {
# MAGIC       \"object_type\": \"schema\",
# MAGIC       \"object_id\": \"${SCHEMA_ID}\",
# MAGIC       \"anomaly_detection_config\": {}
# MAGIC     }
# MAGIC   }"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,1c. Enable DQM via Databricks CLI
# MAGIC %md
# MAGIC ### CLI — Enable DQM on a schema
# MAGIC
# MAGIC ```bash
# MAGIC # Get the schema ID
# MAGIC databricks schemas get uhg_demos.cdo_data_quality | jq -r '.schema_id'
# MAGIC
# MAGIC # Create a config file
# MAGIC cat > dq_config.json << 'EOF'
# MAGIC {
# MAGIC   "monitor": {
# MAGIC     "object_type": "schema",
# MAGIC     "object_id": "<SCHEMA_ID_FROM_ABOVE>",
# MAGIC     "anomaly_detection_config": {}
# MAGIC   }
# MAGIC }
# MAGIC EOF
# MAGIC
# MAGIC # Enable DQM
# MAGIC databricks data-quality create-monitor --json @dq_config.json
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Permissions Reference
# MAGIC
# MAGIC | Scope | Required Privilege | Who Typically Has It |
# MAGIC |-------|-------------------|---------------------|
# MAGIC | Enable DQM on **one schema** | `MANAGE` on that schema, or be schema owner | Data engineers, schema owners |
# MAGIC | Enable DQM on **all schemas in a catalog** | `MANAGE` on the catalog | Catalog admins |
# MAGIC | Enable DQM **across catalogs** (mass enable) | `MANAGE` on each catalog — **or** metastore admin | Platform admins, metastore admins |
# MAGIC | Enable DQM **system tables** | Account admin or metastore admin | Workspace admins |
# MAGIC | **Read** DQM system table results | Account admin must grant `SELECT` on `system.data_quality_monitoring` | Granted per-user or per-group |
# MAGIC | View health indicators in Catalog Explorer | `SELECT` or `BROWSE` on the table | Any data consumer |
# MAGIC
# MAGIC ### Granting permissions
# MAGIC ```sql
# MAGIC -- Grant ability to enable DQM on a single schema
# MAGIC GRANT MANAGE ON SCHEMA uhg_demos.cdo_data_quality TO `data_quality_team`;
# MAGIC
# MAGIC -- Grant ability to enable DQM on all schemas in a catalog
# MAGIC GRANT MANAGE ON CATALOG uhg_demos TO `platform_admins`;
# MAGIC
# MAGIC -- Grant read access to DQM system tables (run as account admin)
# MAGIC GRANT SELECT ON SCHEMA system.data_quality_monitoring TO `data_quality_team`;
# MAGIC ```
# MAGIC
# MAGIC ### Does mass-enable require metastore admin?
# MAGIC **Not necessarily.** If the user has `MANAGE` on each target catalog, they can enable DQM on all schemas
# MAGIC within those catalogs without metastore admin. Metastore admin is only required if:
# MAGIC - You need to enable DQM across catalogs you don't own or have `MANAGE` on
# MAGIC - You need to enable the `system.data_quality_monitoring` system schema itself

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Mass-Enable DQM on All Schemas with a Governed Tag
# MAGIC
# MAGIC **Pattern:** Scan Unity Catalog for all schemas tagged with a governed tag (e.g., `dqm_monitor = enabled`),
# MAGIC then programmatically enable DQM on each one.
# MAGIC
# MAGIC This also demonstrates **table exclusion** — tables tagged `dqm_exclude = true` are reported but
# MAGIC skipped by custom DQ scan jobs (see Section 1d).
# MAGIC
# MAGIC This can be scheduled as a **Databricks Job** to continuously pick up newly tagged schemas.

# COMMAND ----------

# DBTITLE 1,3a. Mass-Enable DQM — Python SDK
from databricks.sdk.service.dataquality import Monitor, AnomalyDetectionConfig

TAG_NAME  = "dqm_monitor"
TAG_VALUE = "enabled"

# Step 1: Discover all schemas tagged for DQM monitoring
tagged_schemas = spark.sql(f"""
    SELECT DISTINCT catalog_name, schema_name
    FROM system.information_schema.schema_tags
    WHERE tag_name  = '{TAG_NAME}'
      AND tag_value = '{TAG_VALUE}'
""").collect()

print(f"Found {len(tagged_schemas)} schema(s) tagged with '{TAG_NAME}={TAG_VALUE}':\n")

# Step 2: Enable DQM on each tagged schema
results = []
for row in tagged_schemas:
    full_name = f"{row.catalog_name}.{row.schema_name}"
    try:
        schema_info = w.schemas.get(full_name=full_name)

        monitor = Monitor(
            object_type="schema",
            object_id=schema_info.schema_id,
            anomaly_detection_config=AnomalyDetectionConfig()
        )
        w.data_quality.create_monitor(monitor=monitor)
        results.append((full_name, "ENABLED"))
        print(f"  [OK] {full_name}")

    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "already enabled" in error_msg.lower():
            results.append((full_name, "ALREADY ENABLED"))
            print(f"  [SKIP] {full_name} — already enabled")
        else:
            results.append((full_name, f"ERROR: {error_msg[:100]}"))
            print(f"  [ERROR] {full_name} — {error_msg[:100]}")

# Step 3: Summary
print(f"\n--- Summary ---")
for schema_name, status in results:
    print(f"  {schema_name}: {status}")

# COMMAND ----------

# DBTITLE 1,3b. Mass-Enable — Discovery Query (SQL only)
# MAGIC %sql
# MAGIC -- Preview: which schemas would be enabled?
# MAGIC -- Use this query to audit before running the mass-enable script above.
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     t.catalog_name,
# MAGIC     t.schema_name,
# MAGIC     t.tag_name,
# MAGIC     t.tag_value
# MAGIC FROM system.information_schema.schema_tags t
# MAGIC WHERE t.tag_name  = 'dqm_monitor'
# MAGIC   AND t.tag_value = 'enabled'
# MAGIC ORDER BY t.catalog_name, t.schema_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mass-Enable via REST API (Bash loop)
# MAGIC
# MAGIC ```bash
# MAGIC #!/bin/bash
# MAGIC # Mass-enable DQM on all schemas tagged with dqm_monitor=enabled
# MAGIC
# MAGIC HOST="https://${DATABRICKS_HOST}"
# MAGIC TOKEN="${DATABRICKS_TOKEN}"
# MAGIC
# MAGIC # Query for tagged schemas using the SQL Statement API
# MAGIC SCHEMAS=$(databricks sql execute \
# MAGIC   --statement "SELECT DISTINCT catalog_name, schema_name
# MAGIC                FROM system.information_schema.schema_tags
# MAGIC                WHERE tag_name = 'dqm_monitor' AND tag_value = 'enabled'" \
# MAGIC   --format JSON | jq -r '.[] | "\(.catalog_name).\(.schema_name)"')
# MAGIC
# MAGIC for FULL_SCHEMA in $SCHEMAS; do
# MAGIC     SCHEMA_ID=$(databricks schemas get "$FULL_SCHEMA" | jq -r '.schema_id')
# MAGIC     echo "Enabling DQM on $FULL_SCHEMA (ID: $SCHEMA_ID)..."
# MAGIC
# MAGIC     curl -s -X POST "${HOST}/api/2.0/data-quality/monitors" \
# MAGIC       -H "Authorization: Bearer ${TOKEN}" \
# MAGIC       -H "Content-Type: application/json" \
# MAGIC       -d "{\"monitor\":{\"object_type\":\"schema\",\"object_id\":\"${SCHEMA_ID}\",\"anomaly_detection_config\":{}}}"
# MAGIC
# MAGIC     echo " -> Done"
# MAGIC done
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Query DQM Results from System Tables
# MAGIC
# MAGIC DQM results are stored in **`system.data_quality_monitoring.table_results`**.
# MAGIC
# MAGIC > **Prerequisite:** The `system.data_quality_monitoring` system schema must be enabled by an account admin or metastore admin.
# MAGIC
# MAGIC **Doc Reference:** [DQM System Tables](https://docs.databricks.com/aws/en/admin/system-tables/data-quality-monitoring)
# MAGIC
# MAGIC ### System Table Schema
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `event_time` | TIMESTAMP | When the result row was generated |
# MAGIC | `catalog_name` | STRING | Catalog of the monitored table |
# MAGIC | `schema_name` | STRING | Schema of the monitored table |
# MAGIC | `table_name` | STRING | Table name |
# MAGIC | `status` | STRING | Consolidated health status at the table level |
# MAGIC | `freshness` | STRUCT | Contains `status` and `commit_freshness` (status, error_code, last_value, predicted_value) |
# MAGIC | `completeness` | STRUCT | Contains `status`, `total_row_count`, `daily_row_count` (each with status, error_code, last/predicted values) |
# MAGIC | `downstream_impact` | STRUCT | Contains `impact_level`, `num_downstream_tables`, `num_queries_on_affected_tables` |
# MAGIC | `root_cause_analysis` | STRUCT | Upstream jobs metadata |

# COMMAND ----------

# DBTITLE 1,4a. View All DQM Results
# MAGIC %sql
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     table_name,
# MAGIC     status,
# MAGIC     freshness.status AS freshness_status,
# MAGIC     completeness.status AS completeness_status,
# MAGIC     downstream_impact.impact_level
# MAGIC FROM system.data_quality_monitoring.table_results
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,4b. Find Tables with Anomalies
# MAGIC %sql
# MAGIC -- Tables flagged with freshness or completeness issues
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     catalog_name || '.' || schema_name || '.' || table_name AS full_table_name,
# MAGIC     status,
# MAGIC     freshness.status AS freshness_status,
# MAGIC     freshness.commit_freshness.last_value AS last_commit,
# MAGIC     freshness.commit_freshness.predicted_value AS predicted_commit,
# MAGIC     completeness.total_row_count.last_value AS actual_rows,
# MAGIC     completeness.total_row_count.min_predicted_value AS min_expected_rows,
# MAGIC     downstream_impact.impact_level AS impact_level,
# MAGIC     downstream_impact.num_downstream_tables AS downstream_tables,
# MAGIC     downstream_impact.num_queries_on_affected_tables AS queries_affected
# MAGIC FROM system.data_quality_monitoring.table_results
# MAGIC WHERE status != 'HEALTHY'
# MAGIC ORDER BY event_time DESC;

# COMMAND ----------

# DBTITLE 1,4c. Latest Scan per Schema
# MAGIC %sql
# MAGIC -- When was each schema last scanned by DQM?
# MAGIC SELECT
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     MAX(event_time) AS last_scan_time,
# MAGIC     COUNT(DISTINCT table_name) AS tables_monitored
# MAGIC FROM system.data_quality_monitoring.table_results
# MAGIC GROUP BY catalog_name, schema_name
# MAGIC ORDER BY last_scan_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Programmatically Add a Governed Tag to a Schema
# MAGIC
# MAGIC Governed tags are account-level tags with enforced policies (allowed values, assignment permissions).
# MAGIC They are in **Public Preview**.
# MAGIC
# MAGIC **Doc Reference:** [Governed Tags](https://docs.databricks.com/aws/en/admin/governed-tags/)

# COMMAND ----------

# DBTITLE 1,5a. Add Tag via SQL
# MAGIC %sql
# MAGIC -- Tag a schema for DQM monitoring
# MAGIC ALTER SCHEMA uhg_demos.cdo_data_quality SET TAGS ('dqm_monitor' = 'enabled');

# COMMAND ----------

# DBTITLE 1,5b. Add Tag via Python SDK
# Tag a schema programmatically via spark.sql (the SDK does not have a direct set-tags method)
spark.sql(f"ALTER SCHEMA {CATALOG}.{SCHEMA} SET TAGS ('dqm_monitor' = 'enabled')")
print(f"Tag 'dqm_monitor=enabled' applied to {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,5c. Add Tag via REST API
# MAGIC %md
# MAGIC ### REST API — Set a tag on a schema
# MAGIC
# MAGIC ```bash
# MAGIC # Get the schema ID first
# MAGIC SCHEMA_ID=$(databricks schemas get uhg_demos.cdo_data_quality | jq -r '.schema_id')
# MAGIC
# MAGIC # Set the governed tag
# MAGIC curl -X PATCH "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/schemas/${CATALOG}.${SCHEMA}" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "properties": {
# MAGIC       "tags": {
# MAGIC         "dqm_monitor": "enabled"
# MAGIC       }
# MAGIC     }
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC **Or via SQL Statement API:**
# MAGIC ```bash
# MAGIC databricks sql execute \
# MAGIC   --statement "ALTER SCHEMA uhg_demos.cdo_data_quality SET TAGS ('dqm_monitor' = 'enabled')"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,5d. Verify Tags on a Schema
# MAGIC %sql
# MAGIC -- Show all tags on schemas in uhg_demos
# MAGIC SELECT
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.schema_tags
# MAGIC WHERE catalog_name = 'uhg_demos'
# MAGIC ORDER BY schema_name, tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Query Which Schemas/Catalogs Have DQM Enabled
# MAGIC
# MAGIC There are two approaches:
# MAGIC 1. **Tag-based** — query `information_schema.schema_tags` for your DQM tag (this is what drives the mass-enable workflow)
# MAGIC 2. **System table-based** — use DQM results as a proxy for enablement (if a schema has results, DQM is running)

# COMMAND ----------

# DBTITLE 1,6a. Tag-Based — Which Schemas Are Tagged for DQM?
# MAGIC %sql
# MAGIC -- Schemas explicitly tagged for DQM monitoring
# MAGIC SELECT
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     tag_value AS dqm_tag_value
# MAGIC FROM system.information_schema.schema_tags
# MAGIC WHERE tag_name = 'dqm_monitor'
# MAGIC ORDER BY catalog_name, schema_name;

# COMMAND ----------

# DBTITLE 1,6b. System Table — Which Schemas Have Active DQM Scans?
# MAGIC %sql
# MAGIC -- Cross-reference all schemas with DQM scan history
# MAGIC WITH latest_scans AS (
# MAGIC     SELECT
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         MAX(event_time) AS last_scan_time,
# MAGIC         COUNT(DISTINCT table_name) AS tables_monitored
# MAGIC     FROM system.data_quality_monitoring.table_results
# MAGIC     GROUP BY catalog_name, schema_name
# MAGIC ),
# MAGIC all_schemas AS (
# MAGIC     SELECT catalog_name, schema_name
# MAGIC     FROM system.information_schema.schemata
# MAGIC     WHERE catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
# MAGIC       AND catalog_name NOT LIKE '__databricks_internal%'
# MAGIC )
# MAGIC SELECT
# MAGIC     s.catalog_name,
# MAGIC     s.schema_name,
# MAGIC     CASE
# MAGIC         WHEN l.last_scan_time IS NOT NULL THEN 'DQM ENABLED'
# MAGIC         ELSE 'NOT ENABLED'
# MAGIC     END AS dqm_status,
# MAGIC     l.last_scan_time,
# MAGIC     l.tables_monitored
# MAGIC FROM all_schemas s
# MAGIC LEFT JOIN latest_scans l
# MAGIC     ON s.catalog_name = l.catalog_name
# MAGIC     AND s.schema_name = l.schema_name
# MAGIC ORDER BY dqm_status, s.catalog_name, s.schema_name;

# COMMAND ----------

# DBTITLE 1,6c. Combined View — Tags + Scan Status
# MAGIC %sql
# MAGIC -- Full picture: tagged schemas + their actual DQM scan status
# MAGIC WITH tagged AS (
# MAGIC     SELECT catalog_name, schema_name, tag_value
# MAGIC     FROM system.information_schema.schema_tags
# MAGIC     WHERE tag_name = 'dqm_monitor'
# MAGIC ),
# MAGIC scanned AS (
# MAGIC     SELECT
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         MAX(event_time) AS last_scan_time,
# MAGIC         COUNT(DISTINCT table_name) AS tables_monitored
# MAGIC     FROM system.data_quality_monitoring.table_results
# MAGIC     GROUP BY catalog_name, schema_name
# MAGIC )
# MAGIC SELECT
# MAGIC     COALESCE(t.catalog_name, s.catalog_name) AS catalog_name,
# MAGIC     COALESCE(t.schema_name, s.schema_name) AS schema_name,
# MAGIC     CASE WHEN t.tag_value IS NOT NULL THEN 'YES' ELSE 'NO' END AS tagged_for_dqm,
# MAGIC     CASE WHEN s.last_scan_time IS NOT NULL THEN 'YES' ELSE 'NO' END AS has_scan_results,
# MAGIC     s.last_scan_time,
# MAGIC     s.tables_monitored
# MAGIC FROM tagged t
# MAGIC FULL OUTER JOIN scanned s
# MAGIC     ON t.catalog_name = s.catalog_name
# MAGIC     AND t.schema_name = s.schema_name
# MAGIC ORDER BY catalog_name, schema_name;