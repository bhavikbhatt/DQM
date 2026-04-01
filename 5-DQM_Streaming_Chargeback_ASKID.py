# Databricks notebook source
# MAGIC %md
# MAGIC # DQM Event Streaming, Tenant Chargeback & ASKID Usage Tracking
# MAGIC
# MAGIC This notebook covers three capabilities built on top of the Data Quality Monitoring
# MAGIC infrastructure established in Notebooks 1–4:
# MAGIC
# MAGIC | Section | Capability | Pattern |
# MAGIC |---------|-----------|---------|
# MAGIC | **1** | Stream incremental DQM changes to Kafka | Structured Streaming + Lakeflow Declarative Pipeline (DLT) Sink |
# MAGIC | **2** | Tenant chargeback for DQM usage | Schema tags + `system.billing.usage` attribution |
# MAGIC | **3** | ASKID-based usage tracking | Tag schemas with ASKID → query system tables for per-ASKID consumption |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Notebooks 1–4 have been run (schema `uhg_demos.cdo_data_quality` exists with DQM enabled)
# MAGIC - Access to `system.billing.usage`, `system.data_quality_monitoring.table_results`, and `system.query.history`
# MAGIC - For Kafka sections: a Kafka-compatible broker endpoint (examples use placeholder values)
# MAGIC
# MAGIC ### Documentation
# MAGIC | Topic | Link |
# MAGIC |-------|------|
# MAGIC | DQM System Tables | [docs.databricks.com/.../data-quality-monitoring](https://docs.databricks.com/aws/en/admin/system-tables/data-quality-monitoring) |
# MAGIC | Delta Streaming Reads | [docs.databricks.com/.../delta-lake](https://docs.databricks.com/aws/en/structured-streaming/delta-lake) |
# MAGIC | Kafka Connector | [docs.databricks.com/.../kafka](https://docs.databricks.com/aws/en/connect/streaming/kafka/) |
# MAGIC | DLT Sink API | [docs.databricks.com/.../ldp-sinks](https://docs.databricks.com/aws/en/ldp/ldp-sinks) |
# MAGIC | Billing System Tables | [docs.databricks.com/.../billing](https://docs.databricks.com/aws/en/admin/system-tables/billing) |
# MAGIC | Query History System Table | [docs.databricks.com/.../query-history](https://docs.databricks.com/aws/en/admin/system-tables/query-history) |
# MAGIC | View DQM Expenses | [docs.databricks.com/.../expense](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/expense) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Stream Incremental DQM Changes to Kafka
# MAGIC
# MAGIC `system.data_quality_monitoring.table_results` is a Delta table — it supports
# MAGIC **Structured Streaming** reads out of the box. As DQM scans complete and new rows are
# MAGIC appended, a streaming query picks them up incrementally using Delta's change tracking.
# MAGIC
# MAGIC We show two approaches:
# MAGIC - **1a.** Structured Streaming (direct — runs on any cluster or job)
# MAGIC - **1b.** Lakeflow Declarative Pipeline with the Sink API (managed — recommended for production)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1a. Structured Streaming: System Table → Kafka
# MAGIC
# MAGIC This approach runs on any compute. It reads incrementally from the DQM system table
# MAGIC and writes JSON events to a Kafka topic.
# MAGIC
# MAGIC > **Key option:** `skipChangeCommits = true` — system tables may receive internal
# MAGIC > UPDATE/DELETE operations. This option ensures the stream only processes appended rows
# MAGIC > and doesn't fail on non-append commits.

# COMMAND ----------

from pyspark.sql import functions as F

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "your-broker:9092"          # Replace with your Kafka broker(s)
KAFKA_TOPIC             = "dqm-anomaly-events"        # Target Kafka topic
CHECKPOINT_PATH         = "/Volumes/uhg_demos/cdo_data_quality/checkpoints/dqm_to_kafka"

# --- Read incrementally from the DQM system table ---
dqm_stream = (
    spark.readStream
      .option("skipChangeCommits", "true")       # Required: skip non-append commits
      .option("maxFilesPerTrigger", "100")        # Rate limit per micro-batch
      .table("system.data_quality_monitoring.table_results")
)

# --- Transform: flatten key fields and serialize to JSON ---
kafka_payload = (
    dqm_stream.select(
        # Use table_id as the Kafka message key for partitioning
        F.col("table_id").cast("string").alias("key"),
        # Serialize the full event as a JSON value
        F.to_json(
            F.struct(
                F.col("event_time"),
                F.col("catalog_name"),
                F.col("schema_name"),
                F.col("table_name"),
                F.col("table_id"),
                F.col("status"),
                F.col("freshness.status").alias("freshness_status"),
                F.col("freshness.commit_freshness.last_value").alias("freshness_last_commit"),
                F.col("freshness.commit_freshness.predicted_value").alias("freshness_predicted_commit"),
                F.col("completeness.status").alias("completeness_status"),
                F.col("completeness.total_row_count.last_value").alias("completeness_actual_rows"),
                F.col("completeness.total_row_count.min_predicted_value").alias("completeness_min_expected"),
                F.col("downstream_impact.impact_level").alias("impact_level"),
                F.col("downstream_impact.num_downstream_tables").alias("downstream_tables"),
            )
        ).alias("value")
    )
)

# --- Write to Kafka ---
# Uncomment to run (requires a live Kafka broker):
#
# query = (
#     kafka_payload.writeStream
#       .format("kafka")
#       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
#       .option("topic", KAFKA_TOPIC)
#       .option("checkpointLocation", CHECKPOINT_PATH)
#       .trigger(availableNow=True)   # Process all new data, then stop
#       .start()
# )
# query.awaitTermination()

# --- Preview what would be sent ---
display(
    spark.read
      .option("skipChangeCommits", "true")
      .table("system.data_quality_monitoring.table_results")
      .select(
          F.col("table_id").cast("string").alias("key"),
          F.to_json(
              F.struct(
                  "event_time", "catalog_name", "schema_name", "table_name",
                  "status",
                  F.col("freshness.status").alias("freshness_status"),
                  F.col("completeness.status").alias("completeness_status"),
                  F.col("downstream_impact.impact_level").alias("impact_level"),
              )
          ).alias("value")
      )
      .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Modes
# MAGIC
# MAGIC | Mode | Use Case |
# MAGIC |------|----------|
# MAGIC | `trigger(availableNow=True)` | Batch-style: process all new data since last checkpoint, then stop. Schedule as a Databricks Job. |
# MAGIC | `trigger(processingTime="5 minutes")` | Continuous: check for new data every 5 minutes on a long-running cluster. |
# MAGIC | No trigger (default) | Process as fast as possible — use for backfill. |
# MAGIC
# MAGIC For most DQM use cases, `availableNow=True` scheduled as a job (e.g., hourly) is the
# MAGIC right balance of cost and latency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Lakeflow Declarative Pipeline (DLT) with Kafka Sink
# MAGIC
# MAGIC For production workloads, **Lakeflow Declarative Pipelines** (formerly DLT) provide
# MAGIC managed checkpointing, lineage tracking, retry handling, and monitoring — all declarative.
# MAGIC
# MAGIC The **Sink API** (Public Preview) allows a DLT pipeline to write directly to Kafka.
# MAGIC
# MAGIC > **To deploy:** Create a new DLT pipeline, point it at this cell's code (or a separate
# MAGIC > notebook), and configure it as a triggered or continuous pipeline.
# MAGIC
# MAGIC ```python
# MAGIC # --- This code runs inside a Lakeflow Declarative Pipeline ---
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Step 1: Define the Kafka sink
# MAGIC dlt.create_sink(
# MAGIC     name="dqm_kafka_sink",
# MAGIC     format="kafka",
# MAGIC     options={
# MAGIC         "kafka.bootstrap.servers": "your-broker:9092",
# MAGIC         "topic": "dqm-anomaly-events",
# MAGIC     }
# MAGIC )
# MAGIC
# MAGIC # Step 2: Stream from the DQM system table → Kafka
# MAGIC @dlt.append_flow(name="dqm_events_to_kafka", target="dqm_kafka_sink")
# MAGIC def stream_dqm_to_kafka():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC           .option("skipChangeCommits", "true")
# MAGIC           .table("system.data_quality_monitoring.table_results")
# MAGIC           .select(
# MAGIC               F.col("table_id").cast("string").alias("key"),
# MAGIC               F.to_json(F.struct(
# MAGIC                   "event_time", "catalog_name", "schema_name",
# MAGIC                   "table_name", "status",
# MAGIC                   F.col("freshness.status").alias("freshness_status"),
# MAGIC                   F.col("completeness.status").alias("completeness_status"),
# MAGIC                   F.col("downstream_impact.impact_level").alias("impact_level"),
# MAGIC               )).alias("value")
# MAGIC           )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### Why DLT for this?
# MAGIC | Benefit | Detail |
# MAGIC |---------|--------|
# MAGIC | **Managed checkpointing** | No need to manage checkpoint paths — DLT handles it |
# MAGIC | **Built-in retry** | Transient Kafka failures are retried automatically |
# MAGIC | **Lineage** | The system table → Kafka flow appears in Unity Catalog lineage |
# MAGIC | **Scheduling** | Triggered mode = cost-efficient; continuous mode = low latency |
# MAGIC | **Observability** | Pipeline health visible in the DLT UI and system tables |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1c. Filter: Only Stream Anomalies (Not Healthy Results)
# MAGIC
# MAGIC In most cases, downstream consumers only care about **unhealthy** events. Add a filter
# MAGIC to reduce Kafka volume and focus alerting:

# COMMAND ----------

# Only stream anomalies — skip healthy results
dqm_anomalies_only = (
    spark.readStream
      .option("skipChangeCommits", "true")
      .table("system.data_quality_monitoring.table_results")
      .filter("status != 'Healthy'")
      .select(
          F.col("table_id").cast("string").alias("key"),
          F.to_json(
              F.struct(
                  "event_time", "catalog_name", "schema_name", "table_name",
                  "status",
                  F.col("freshness.status").alias("freshness_status"),
                  F.col("freshness.commit_freshness.last_value").alias("last_commit"),
                  F.col("completeness.status").alias("completeness_status"),
                  F.col("completeness.total_row_count.last_value").alias("actual_rows"),
                  F.col("completeness.total_row_count.min_predicted_value").alias("min_expected_rows"),
                  F.col("downstream_impact.impact_level").alias("impact_level"),
                  F.col("downstream_impact.num_downstream_tables").alias("downstream_tables"),
                  F.col("downstream_impact.num_queries_on_affected_tables").alias("queries_affected"),
              )
          ).alias("value")
      )
)

# Preview anomalies that would be streamed
display(
    spark.read
      .table("system.data_quality_monitoring.table_results")
      .filter("status != 'Healthy'")
      .select(
          "event_time", "catalog_name", "schema_name", "table_name", "status",
          F.col("freshness.status").alias("freshness"),
          F.col("completeness.status").alias("completeness"),
          F.col("downstream_impact.impact_level").alias("impact"),
      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Tenant Chargeback for DQM Usage
# MAGIC
# MAGIC **Goal:** Determine how much each tenant (schema owner) is consuming for Data Quality
# MAGIC Monitoring so that costs can be charged back to the appropriate team.
# MAGIC
# MAGIC **Pattern:**
# MAGIC 1. Tag each schema with its owning tenant (we already tag schemas — extend with a tenant tag)
# MAGIC 2. Join `system.billing.usage` (filtered to DQM) with schema metadata to attribute costs
# MAGIC 3. Cross-reference with `system.billing.list_prices` for dollar amounts
# MAGIC
# MAGIC ### How Databricks Bills for DQM
# MAGIC DQM usage appears in `system.billing.usage` with:
# MAGIC - `billing_origin_product = 'DATA_QUALITY_MONITORING'`
# MAGIC - `usage_metadata.schema_id` — the schema being monitored (for anomaly detection)
# MAGIC - `usage_metadata.table_id` — the specific table (for data profiling)

# COMMAND ----------

# DBTITLE 1,2a. Tag Schemas with Tenant Ownership
# MAGIC %sql
# MAGIC -- Tag schemas with the owning tenant/team for chargeback attribution
# MAGIC -- In practice, this would be done at schema creation time or via automation
# MAGIC
# MAGIC ALTER SCHEMA uhg_demos.cdo_data_quality SET TAGS ('tenant' = 'CDO_Data_Quality_Team');
# MAGIC
# MAGIC -- Example: if other schemas existed for other tenants
# MAGIC -- ALTER SCHEMA uhg_demos.claims_analytics SET TAGS ('tenant' = 'Claims_Analytics_Team');
# MAGIC -- ALTER SCHEMA uhg_demos.pharmacy_data    SET TAGS ('tenant' = 'Pharmacy_Operations');

# COMMAND ----------

# DBTITLE 1,2b. DQM Billing Usage by Schema
# MAGIC %sql
# MAGIC -- View raw DQM billing events with schema-level detail
# MAGIC SELECT
# MAGIC     usage_date,
# MAGIC     workspace_id,
# MAGIC     sku_name,
# MAGIC     usage_quantity,
# MAGIC     usage_metadata.schema_id,
# MAGIC     usage_metadata.table_id,
# MAGIC     billing_origin_product
# MAGIC FROM system.billing.usage
# MAGIC WHERE billing_origin_product = 'DATA_QUALITY_MONITORING' -- Databricks auto-tag 
# MAGIC ORDER BY usage_date DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,2c. Chargeback: DQM Cost per Tenant (DBU)
# MAGIC %sql
# MAGIC -- Join billing usage with schema tags to attribute DQM costs per tenant
# MAGIC -- This gives DBU consumption per tenant for DQM
# MAGIC
# MAGIC WITH dqm_usage AS (
# MAGIC     SELECT
# MAGIC         u.usage_date,
# MAGIC         u.sku_name,
# MAGIC         u.usage_quantity,
# MAGIC         u.usage_metadata.schema_id AS schema_id
# MAGIC     FROM system.billing.usage u
# MAGIC     WHERE u.billing_origin_product = 'DATA_QUALITY_MONITORING'
# MAGIC       AND u.usage_metadata.schema_id IS NOT NULL
# MAGIC ),
# MAGIC tenant_tags AS (
# MAGIC     SELECT
# MAGIC         s.catalog_name,
# MAGIC         s.schema_name,
# MAGIC         s.schema_id,
# MAGIC         t.tag_value AS tenant
# MAGIC     FROM system.information_schema.schemata s
# MAGIC     JOIN system.information_schema.schema_tags t
# MAGIC       ON s.catalog_name = t.catalog_name
# MAGIC       AND s.schema_name = t.schema_name
# MAGIC     WHERE t.tag_name = 'ASK35453'
# MAGIC )
# MAGIC SELECT
# MAGIC     tt.tenant,
# MAGIC     tt.catalog_name,
# MAGIC     tt.schema_name,
# MAGIC     DATE_TRUNC('month', du.usage_date) AS usage_month,
# MAGIC     ROUND(SUM(du.usage_quantity), 4) AS total_dbus,
# MAGIC     COUNT(*) AS billing_events
# MAGIC FROM dqm_usage du
# MAGIC JOIN tenant_tags tt ON du.schema_id = tt.schema_id
# MAGIC GROUP BY tt.tenant, tt.catalog_name, tt.schema_name, DATE_TRUNC('month', du.usage_date)
# MAGIC ORDER BY usage_month DESC, total_dbus DESC;

# COMMAND ----------

# DBTITLE 1,2d. Chargeback: DQM Cost per Tenant (Dollars)
# MAGIC %sql
# MAGIC -- Full dollar-cost chargeback by joining with list prices
# MAGIC
# MAGIC WITH dqm_usage AS (
# MAGIC     SELECT
# MAGIC         u.usage_date,
# MAGIC         u.usage_start_time,
# MAGIC         u.sku_name,
# MAGIC         u.usage_quantity,
# MAGIC         u.usage_metadata.schema_id AS schema_id
# MAGIC     FROM system.billing.usage u
# MAGIC     WHERE u.billing_origin_product = 'DATA_QUALITY_MONITORING'
# MAGIC       AND u.usage_metadata.schema_id IS NOT NULL
# MAGIC ),
# MAGIC tenant_tags AS (
# MAGIC     SELECT
# MAGIC         s.schema_id,
# MAGIC         t.tag_value AS tenant
# MAGIC     FROM system.information_schema.schemata s
# MAGIC     JOIN system.information_schema.schema_tags t
# MAGIC       ON s.catalog_name = t.catalog_name
# MAGIC       AND s.schema_name = t.schema_name
# MAGIC     WHERE t.tag_name = 'tenant'
# MAGIC ),
# MAGIC priced AS (
# MAGIC     SELECT
# MAGIC         tt.tenant,
# MAGIC         du.usage_date,
# MAGIC         du.usage_quantity,
# MAGIC         du.usage_quantity * p.pricing.effective_list.default AS estimated_cost_usd
# MAGIC     FROM dqm_usage du
# MAGIC     JOIN tenant_tags tt ON du.schema_id = tt.schema_id
# MAGIC     JOIN system.billing.list_prices p
# MAGIC       ON du.sku_name = p.sku_name
# MAGIC       AND du.usage_start_time >= p.price_start_time
# MAGIC       AND du.usage_start_time < COALESCE(p.price_end_time, TIMESTAMP '2099-12-31')
# MAGIC )
# MAGIC SELECT
# MAGIC     tenant,
# MAGIC     DATE_TRUNC('month', usage_date) AS usage_month,
# MAGIC     ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC     ROUND(SUM(estimated_cost_usd), 2) AS estimated_cost_usd
# MAGIC FROM priced
# MAGIC GROUP BY tenant, DATE_TRUNC('month', usage_date)
# MAGIC ORDER BY usage_month DESC, estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,2e. DQM Cost Breakdown by Table (Detail View)
# MAGIC %sql
# MAGIC -- Drill down: which tables within each tenant are driving DQM cost?
# MAGIC
# MAGIC WITH dqm_usage AS (
# MAGIC     SELECT
# MAGIC         u.usage_date,
# MAGIC         u.usage_quantity,
# MAGIC         u.usage_metadata.schema_id AS schema_id,
# MAGIC         u.usage_metadata.table_id AS table_id
# MAGIC     FROM system.billing.usage u
# MAGIC     WHERE u.billing_origin_product = 'DATA_QUALITY_MONITORING'
# MAGIC ),
# MAGIC tenant_tags AS (
# MAGIC     SELECT s.schema_id, s.catalog_name, s.schema_name, t.tag_value AS tenant
# MAGIC     FROM system.information_schema.schemata s
# MAGIC     JOIN system.information_schema.schema_tags t
# MAGIC       ON s.catalog_name = t.catalog_name AND s.schema_name = t.schema_name
# MAGIC     WHERE t.tag_name = 'tenant'
# MAGIC ),
# MAGIC table_names AS (
# MAGIC     SELECT table_id, catalog_name, schema_name, table_name
# MAGIC     FROM system.information_schema.tables
# MAGIC )
# MAGIC SELECT
# MAGIC     tt.tenant,
# MAGIC     COALESCE(tn.catalog_name || '.' || tn.schema_name || '.' || tn.table_name,
# MAGIC              du.table_id) AS table_path,
# MAGIC     DATE_TRUNC('month', du.usage_date) AS usage_month,
# MAGIC     ROUND(SUM(du.usage_quantity), 4) AS total_dbus,
# MAGIC     COUNT(*) AS scan_count
# MAGIC FROM dqm_usage du
# MAGIC LEFT JOIN tenant_tags tt ON du.schema_id = tt.schema_id
# MAGIC LEFT JOIN table_names tn ON du.table_id = tn.table_id
# MAGIC GROUP BY tt.tenant, table_path, DATE_TRUNC('month', du.usage_date)
# MAGIC ORDER BY total_dbus DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. ASKID-Based Usage Tracking
# MAGIC
# MAGIC **ASKID** is the internal tenant/project identifier. The pattern:
# MAGIC
# MAGIC 1. **Tag schemas** with `askid = <value>` — this is the single source of truth linking
# MAGIC    a Unity Catalog schema to an organizational project
# MAGIC 2. **Query system tables** joined against those tags to get per-ASKID usage for:
# MAGIC    - DQM billing consumption
# MAGIC    - DQM anomaly detection results
# MAGIC    - Query history / compute usage
# MAGIC
# MAGIC This leverages the same governed-tag infrastructure from Notebook 4 — tags are the
# MAGIC universal key that ties platform telemetry back to business ownership.

# COMMAND ----------

# DBTITLE 1,3a. Tag Schemas with ASKID
# MAGIC %sql
# MAGIC -- Assign ASKIDs to schemas. Each schema belongs to one project/tenant.
# MAGIC -- In production, this would be automated at provisioning time.
# MAGIC
# MAGIC ALTER SCHEMA uhg_demos.cdo_data_quality SET TAGS ('askid' = 'ASK-10042');
# MAGIC
# MAGIC -- Examples for other schemas:
# MAGIC -- ALTER SCHEMA uhg_demos.claims_analytics SET TAGS ('askid' = 'ASK-20087');
# MAGIC -- ALTER SCHEMA uhg_demos.pharmacy_data    SET TAGS ('askid' = 'ASK-30155');
# MAGIC -- ALTER SCHEMA uhg_demos.member_services  SET TAGS ('askid' = 'ASK-10042');  -- Same ASKID can own multiple schemas

# COMMAND ----------

# DBTITLE 1,3b. View All ASKID Assignments
# MAGIC %sql
# MAGIC -- Inventory: which schemas are assigned to which ASKIDs?
# MAGIC SELECT
# MAGIC     tag_value AS askid,
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     tag_name
# MAGIC FROM system.information_schema.schema_tags
# MAGIC WHERE tag_name = 'askid'
# MAGIC ORDER BY tag_value, catalog_name, schema_name;

# COMMAND ----------

# DBTITLE 1,3c. DQM Anomaly Results by ASKID
# MAGIC %sql
# MAGIC -- Which ASKIDs have data quality issues? Join DQM results with ASKID tags.
# MAGIC
# MAGIC WITH askid_schemas AS (
# MAGIC     SELECT
# MAGIC         t.tag_value AS askid,
# MAGIC         s.schema_id,
# MAGIC         s.catalog_name,
# MAGIC         s.schema_name
# MAGIC     FROM system.information_schema.schema_tags t
# MAGIC     JOIN system.information_schema.schemata s
# MAGIC       ON t.catalog_name = s.catalog_name AND t.schema_name = s.schema_name
# MAGIC     WHERE t.tag_name = 'askid'
# MAGIC )
# MAGIC SELECT
# MAGIC     a.askid,
# MAGIC     r.catalog_name,
# MAGIC     r.schema_name,
# MAGIC     r.table_name,
# MAGIC     r.event_time,
# MAGIC     r.status,
# MAGIC     r.freshness.status AS freshness_status,
# MAGIC     r.completeness.status AS completeness_status,
# MAGIC     r.downstream_impact.impact_level AS impact_level
# MAGIC FROM system.data_quality_monitoring.table_results r
# MAGIC JOIN askid_schemas a
# MAGIC   ON r.schema_id = a.schema_id
# MAGIC ORDER BY a.askid, r.event_time DESC;

# COMMAND ----------

# DBTITLE 1,3d. DQM Health Summary by ASKID
# MAGIC %sql
# MAGIC -- Executive view: ASKID-level health rollup
# MAGIC
# MAGIC WITH askid_schemas AS (
# MAGIC     SELECT t.tag_value AS askid, s.schema_id
# MAGIC     FROM system.information_schema.schema_tags t
# MAGIC     JOIN system.information_schema.schemata s
# MAGIC       ON t.catalog_name = s.catalog_name AND t.schema_name = s.schema_name
# MAGIC     WHERE t.tag_name = 'askid'
# MAGIC ),
# MAGIC latest_results AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY catalog_name, schema_name, table_name
# MAGIC             ORDER BY event_time DESC
# MAGIC         ) AS rn
# MAGIC     FROM system.data_quality_monitoring.table_results
# MAGIC )
# MAGIC SELECT
# MAGIC     a.askid,
# MAGIC     COUNT(DISTINCT r.table_name) AS tables_monitored,
# MAGIC     SUM(CASE WHEN r.status = 'Healthy' THEN 1 ELSE 0 END) AS healthy_tables,
# MAGIC     SUM(CASE WHEN r.status != 'Healthy' THEN 1 ELSE 0 END) AS unhealthy_tables,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN r.status = 'Healthy' THEN 1 ELSE 0 END) * 100.0
# MAGIC         / NULLIF(COUNT(*), 0), 1
# MAGIC     ) AS health_pct
# MAGIC FROM latest_results r
# MAGIC JOIN askid_schemas a ON r.schema_id = a.schema_id
# MAGIC WHERE r.rn = 1
# MAGIC GROUP BY a.askid
# MAGIC ORDER BY unhealthy_tables DESC;

# COMMAND ----------

# DBTITLE 1,3e. DQM Billing Consumption by ASKID (DBU + Cost)
# MAGIC %sql
# MAGIC -- Chargeback: how much DQM compute is each ASKID consuming?
# MAGIC
# MAGIC WITH askid_schemas AS (
# MAGIC     SELECT t.tag_value AS askid, s.schema_id
# MAGIC     FROM system.information_schema.schema_tags t
# MAGIC     JOIN system.information_schema.schemata s
# MAGIC       ON t.catalog_name = s.catalog_name AND t.schema_name = s.schema_name
# MAGIC     WHERE t.tag_name = 'askid'
# MAGIC ),
# MAGIC dqm_usage AS (
# MAGIC     SELECT
# MAGIC         usage_date,
# MAGIC         usage_start_time,
# MAGIC         sku_name,
# MAGIC         usage_quantity,
# MAGIC         usage_metadata.schema_id AS schema_id
# MAGIC     FROM system.billing.usage
# MAGIC     WHERE billing_origin_product = 'DATA_QUALITY_MONITORING'
# MAGIC       AND usage_metadata.schema_id IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     a.askid,
# MAGIC     DATE_TRUNC('month', du.usage_date) AS usage_month,
# MAGIC     ROUND(SUM(du.usage_quantity), 4) AS total_dbus,
# MAGIC     ROUND(SUM(du.usage_quantity * p.pricing.effective_list.default), 2) AS estimated_cost_usd
# MAGIC FROM dqm_usage du
# MAGIC JOIN askid_schemas a ON du.schema_id = a.schema_id
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON du.sku_name = p.sku_name
# MAGIC   AND du.usage_start_time >= p.price_start_time
# MAGIC   AND du.usage_start_time < COALESCE(p.price_end_time, TIMESTAMP '2099-12-31')
# MAGIC GROUP BY a.askid, DATE_TRUNC('month', du.usage_date)
# MAGIC ORDER BY usage_month DESC, estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,3f. Query Usage by ASKID (Warehouse Compute)
# MAGIC %sql
# MAGIC -- Track query volume and compute time for schemas owned by each ASKID.
# MAGIC -- Uses system.query.history joined with schema tags via catalog/schema references.
# MAGIC --
# MAGIC -- NOTE: system.query.history does not have direct catalog/schema columns.
# MAGIC -- We use statement_text pattern matching as a pragmatic approach.
# MAGIC -- For more precise attribution, use system.access.audit logs.
# MAGIC
# MAGIC WITH askid_schemas AS (
# MAGIC     SELECT
# MAGIC         t.tag_value AS askid,
# MAGIC         s.catalog_name,
# MAGIC         s.schema_name
# MAGIC     FROM system.information_schema.schema_tags t
# MAGIC     JOIN system.information_schema.schemata s
# MAGIC       ON t.catalog_name = s.catalog_name AND t.schema_name = s.schema_name
# MAGIC     WHERE t.tag_name = 'askid'
# MAGIC )
# MAGIC SELECT
# MAGIC     a.askid,
# MAGIC     DATE_TRUNC('month', q.start_time) AS query_month,
# MAGIC     COUNT(*) AS total_queries,
# MAGIC     SUM(CASE WHEN q.execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS successful_queries,
# MAGIC     SUM(CASE WHEN q.execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
# MAGIC     ROUND(SUM(q.total_duration_ms) / 1000.0 / 3600.0, 2) AS total_compute_hours,
# MAGIC     ROUND(AVG(q.total_duration_ms) / 1000.0, 1) AS avg_query_duration_sec,
# MAGIC     SUM(q.read_rows) AS total_rows_read,
# MAGIC     ROUND(SUM(q.read_bytes) / (1024*1024*1024.0), 2) AS total_gb_read
# MAGIC FROM system.query.history q
# MAGIC JOIN askid_schemas a
# MAGIC   ON UPPER(q.statement_text) LIKE CONCAT('%', UPPER(a.catalog_name), '.', UPPER(a.schema_name), '%')
# MAGIC WHERE q.start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
# MAGIC   AND q.execution_status IN ('FINISHED', 'FAILED')
# MAGIC GROUP BY a.askid, DATE_TRUNC('month', q.start_time)
# MAGIC ORDER BY query_month DESC, total_queries DESC;

# COMMAND ----------

# DBTITLE 1,3g. Full ASKID Chargeback Report (All Dimensions)
# MAGIC %sql
# MAGIC -- Combined chargeback: DQM cost + query volume + data health per ASKID
# MAGIC -- This is the "one view" a finance or platform team would use for showback/chargeback
# MAGIC
# MAGIC WITH askid_schemas AS (
# MAGIC     SELECT t.tag_value AS askid, s.schema_id, s.catalog_name, s.schema_name
# MAGIC     FROM system.information_schema.schema_tags t
# MAGIC     JOIN system.information_schema.schemata s
# MAGIC       ON t.catalog_name = s.catalog_name AND t.schema_name = s.schema_name
# MAGIC     WHERE t.tag_name = 'askid'
# MAGIC ),
# MAGIC -- DQM billing
# MAGIC dqm_cost AS (
# MAGIC     SELECT
# MAGIC         a.askid,
# MAGIC         ROUND(SUM(u.usage_quantity), 4) AS dqm_dbus_30d
# MAGIC     FROM system.billing.usage u
# MAGIC     JOIN askid_schemas a ON u.usage_metadata.schema_id = a.schema_id
# MAGIC     WHERE u.billing_origin_product = 'DATA_QUALITY_MONITORING'
# MAGIC       AND u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC     GROUP BY a.askid
# MAGIC ),
# MAGIC -- DQM health
# MAGIC dqm_health AS (
# MAGIC     SELECT
# MAGIC         a.askid,
# MAGIC         COUNT(DISTINCT r.table_name) AS tables_monitored,
# MAGIC         SUM(CASE WHEN r.status != 'Healthy' THEN 1 ELSE 0 END) AS anomalies_30d
# MAGIC     FROM system.data_quality_monitoring.table_results r
# MAGIC     JOIN askid_schemas a ON r.schema_id = a.schema_id
# MAGIC     WHERE r.event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
# MAGIC     GROUP BY a.askid
# MAGIC ),
# MAGIC -- Query volume
# MAGIC query_vol AS (
# MAGIC     SELECT
# MAGIC         a.askid,
# MAGIC         COUNT(*) AS queries_30d,
# MAGIC         ROUND(SUM(q.total_duration_ms) / 1000.0 / 3600.0, 2) AS compute_hours_30d
# MAGIC     FROM system.query.history q
# MAGIC     JOIN askid_schemas a
# MAGIC       ON UPPER(q.statement_text) LIKE CONCAT('%', UPPER(a.catalog_name), '.', UPPER(a.schema_name), '%')
# MAGIC     WHERE q.start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
# MAGIC     GROUP BY a.askid
# MAGIC )
# MAGIC SELECT
# MAGIC     a.askid,
# MAGIC     COLLECT_SET(a.catalog_name || '.' || a.schema_name) AS schemas,
# MAGIC     COALESCE(c.dqm_dbus_30d, 0) AS dqm_dbus_30d,
# MAGIC     COALESCE(h.tables_monitored, 0) AS tables_monitored,
# MAGIC     COALESCE(h.anomalies_30d, 0) AS anomalies_30d,
# MAGIC     COALESCE(v.queries_30d, 0) AS queries_30d,
# MAGIC     COALESCE(v.compute_hours_30d, 0) AS compute_hours_30d
# MAGIC FROM (SELECT DISTINCT askid FROM askid_schemas) a
# MAGIC LEFT JOIN dqm_cost c ON a.askid = c.askid
# MAGIC LEFT JOIN dqm_health h ON a.askid = h.askid
# MAGIC LEFT JOIN query_vol v ON a.askid = v.askid
# MAGIC ORDER BY dqm_dbus_30d DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary: Tag-Driven Platform Governance
# MAGIC
# MAGIC All three capabilities in this notebook build on the same foundational pattern:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    Unity Catalog Tags                          │
# MAGIC │                                                                │
# MAGIC │   Schema Tags:  askid = 'ASK-10042'                           │
# MAGIC │                 tenant = 'CDO_Data_Quality_Team'               │
# MAGIC │                 dqm_monitor = 'enabled'                        │
# MAGIC │                                                                │
# MAGIC │   Column Tags:  uhg_dq = 'firstName'  (from Notebook 2)       │
# MAGIC └────────────┬────────────────────┬───────────────┬──────────────┘
# MAGIC              │                    │               │
# MAGIC              ▼                    ▼               ▼
# MAGIC   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
# MAGIC   │  DQM Streaming   │ │   Chargeback     │ │  ASKID Tracking  │
# MAGIC   │                  │ │                  │ │                  │
# MAGIC   │  system.dqm.*    │ │  system.billing  │ │  system.billing  │
# MAGIC   │       ↓          │ │  .usage joined   │ │  system.dqm.*   │
# MAGIC   │  Structured      │ │  with schema     │ │  system.query   │
# MAGIC   │  Streaming /     │ │  tags → per-     │ │  .history joined │
# MAGIC   │  DLT Sink        │ │  tenant DBU +    │ │  with askid tags │
# MAGIC   │       ↓          │ │  dollar costs    │ │  → per-project   │
# MAGIC   │  Kafka / Event   │ │                  │ │  usage report    │
# MAGIC   │  Hub / webhook   │ │                  │ │                  │
# MAGIC   └──────────────────┘ └──────────────────┘ └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC | Principle | Implementation |
# MAGIC |-----------|---------------|
# MAGIC | **Tags are the universal join key** | Every system table query joins back to `information_schema.schema_tags` |
# MAGIC | **Tag once, query everywhere** | One `askid` tag on a schema unlocks DQM results, billing, and query attribution |
# MAGIC | **Incremental by default** | Structured Streaming + `skipChangeCommits` for efficient CDC from any system table |
# MAGIC | **Cost attribution is native** | `billing_origin_product = 'DATA_QUALITY_MONITORING'` + `usage_metadata.schema_id` — no custom instrumentation needed |