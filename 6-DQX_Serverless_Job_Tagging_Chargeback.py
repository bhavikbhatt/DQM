# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Serverless Job Tagging & Multi-Tenant Chargeback
# MAGIC
# MAGIC **Scenario:** A central Data Quality team runs DQX scanning jobs on **serverless compute**
# MAGIC on behalf of many clients/tenants. Each client has its own billing code. When a client
# MAGIC requests an ad-hoc DQ scan, the job must be tagged with the client's billing code so the
# MAGIC central team can:
# MAGIC
# MAGIC 1. Know **who** requested which scans
# MAGIC 2. Query `system.billing.usage` for **per-client chargeback**
# MAGIC
# MAGIC ### Two Tagging Mechanisms
# MAGIC
# MAGIC | Mechanism | Compute Type | How Tags Flow to Billing | Ad-Hoc Capable? |
# MAGIC |-----------|-------------|--------------------------|-----------------|
# MAGIC | **Serverless Budget Policies** | Serverless | Policy tags → `custom_tags` in `system.billing.usage` | Yes — assign per-client policies |
# MAGIC | **Job-Level Tags** (Jobs API) | Classic / Jobs Compute | Job tags → cluster tags → `custom_tags` | Yes — set tags per job definition or per-trigger |
# MAGIC
# MAGIC ### How This Notebook Is Organized
# MAGIC
# MAGIC | Section | What It Covers |
# MAGIC |---------|---------------|
# MAGIC | **1** | Budget policies for serverless chargeback (recommended) |
# MAGIC | **2** | Creating per-client DQX job templates via the Jobs API |
# MAGIC | **3** | Ad-hoc triggering: tag a run with the requesting client's billing code |
# MAGIC | **4** | Querying `system.billing.usage` for per-client DQX chargeback |
# MAGIC | **5** | End-to-end automation: parameterized wrapper for client-triggered scans |
# MAGIC
# MAGIC ### Documentation
# MAGIC | Topic | Link |
# MAGIC |-------|------|
# MAGIC | Budget Policies | [docs.databricks.com/.../budget-policies](https://docs.databricks.com/aws/en/admin/usage/budget-policies) |
# MAGIC | Usage Detail Tags | [docs.databricks.com/.../usage-detail-tags](https://docs.databricks.com/aws/en/admin/account-settings/usage-detail-tags) |
# MAGIC | Billing System Tables | [docs.databricks.com/.../billing](https://docs.databricks.com/aws/en/admin/system-tables/billing) |
# MAGIC | Jobs API Reference | [docs.databricks.com/api/.../jobs](https://docs.databricks.com/api/workspace/jobs) |
# MAGIC | Monitor Serverless Cost | [docs.databricks.com/.../serverless-billing](https://docs.databricks.com/aws/en/admin/system-tables/serverless-billing) |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Notebooks 1–3 have been run (`uhg_demos.cdo_data_quality` exists with DQX configured)
# MAGIC - Access to `system.billing.usage` and `system.billing.list_prices`
# MAGIC - Workspace admin access for budget policy creation (Section 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Serverless Budget Policies — The Recommended Approach
# MAGIC
# MAGIC For **serverless compute**, custom tags in `system.billing.usage` are populated
# MAGIC **exclusively via budget policies** — not job tags or spark config.
# MAGIC
# MAGIC **Pattern:** Create one budget policy per client/billing code. When the central DQ team
# MAGIC triggers a scan on behalf of a client, they select that client's budget policy. The
# MAGIC policy's tags automatically flow to `system.billing.usage.custom_tags`.
# MAGIC
# MAGIC ### How Budget Policies Work
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │  Admin creates budget policies per client:               │
# MAGIC │                                                          │
# MAGIC │  ┌──────────────────┐  ┌──────────────────┐             │
# MAGIC │  │ Policy: Client_A │  │ Policy: Client_B │  ...        │
# MAGIC │  │ billing_code:    │  │ billing_code:    │             │
# MAGIC │  │   "ACME-001"     │  │   "GLOBEX-002"   │             │
# MAGIC │  └────────┬─────────┘  └────────┬─────────┘             │
# MAGIC │           │                     │                        │
# MAGIC │           ▼                     ▼                        │
# MAGIC │  DQ team selects policy when triggering serverless job   │
# MAGIC │           │                     │                        │
# MAGIC │           ▼                     ▼                        │
# MAGIC │  system.billing.usage.custom_tags:                       │
# MAGIC │    {"billing_code": "ACME-001"}                          │
# MAGIC │    {"billing_code": "GLOBEX-002"}                        │
# MAGIC └──────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,1a. Create Budget Policies via REST API (Admin)
# MAGIC %md
# MAGIC ### Create a Budget Policy per Client
# MAGIC
# MAGIC Budget policies are managed via the **Account API** (admin only). Each policy
# MAGIC includes custom tags that flow to billing system tables.
# MAGIC
# MAGIC ```bash
# MAGIC # Create a budget policy for Client A
# MAGIC curl -X POST "https://${DATABRICKS_HOST}/api/2.0/budget-policies" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "policy_name": "DQX - Client A (ACME-001)",
# MAGIC     "custom_tags": [
# MAGIC       {"key": "billing_code", "value": "ACME-001"},
# MAGIC       {"key": "requesting_team", "value": "ACME Corp"},
# MAGIC       {"key": "cost_center", "value": "DQ-Scanning"}
# MAGIC     ]
# MAGIC   }'
# MAGIC
# MAGIC # Create a budget policy for Client B
# MAGIC curl -X POST "https://${DATABRICKS_HOST}/api/2.0/budget-policies" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "policy_name": "DQX - Client B (GLOBEX-002)",
# MAGIC     "custom_tags": [
# MAGIC       {"key": "billing_code", "value": "GLOBEX-002"},
# MAGIC       {"key": "requesting_team", "value": "Globex Inc"},
# MAGIC       {"key": "cost_center", "value": "DQ-Scanning"}
# MAGIC     ]
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC > **Key:** The `custom_tags` on the policy automatically populate
# MAGIC > `system.billing.usage.custom_tags` for every serverless workload that uses this policy.

# COMMAND ----------

# DBTITLE 1,1b. Create Budget Policies via Python SDK (Admin)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Define client billing codes
clients = [
    {"policy_name": "DQX - Client A (ACME-001)",   "billing_code": "ACME-001",   "team": "ACME Corp"},
    {"policy_name": "DQX - Client B (GLOBEX-002)",  "billing_code": "GLOBEX-002", "team": "Globex Inc"},
    {"policy_name": "DQX - Client C (INITECH-003)", "billing_code": "INITECH-003","team": "Initech LLC"},
]

# Create budget policies (requires admin privileges)
# Uncomment to run:
#
# for client in clients:
#     policy = w.budget_policies.create(
#         policy_name=client["policy_name"],
#         custom_tags=[
#             {"key": "billing_code",     "value": client["billing_code"]},
#             {"key": "requesting_team",  "value": client["team"]},
#             {"key": "cost_center",      "value": "DQ-Scanning"},
#         ]
#     )
#     print(f"Created policy: {policy.policy_name} (ID: {policy.policy_id})")

print("Budget policy definitions ready. Uncomment and run with admin privileges to create.")

# COMMAND ----------

# DBTITLE 1,1c. List Existing Budget Policies
# MAGIC %md
# MAGIC ### Verify Budget Policies
# MAGIC
# MAGIC ```bash
# MAGIC # List all budget policies
# MAGIC curl -s "https://${DATABRICKS_HOST}/api/2.0/budget-policies" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" | jq '.policies[] | {policy_name, policy_id, custom_tags}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Job-Level Tags — Per-Client DQX Job Templates
# MAGIC
# MAGIC For **classic compute** or as a **complementary approach** alongside budget policies,
# MAGIC you can set tags directly on the Job definition. These tags flow to the job cluster's
# MAGIC `custom_tags` and then to `system.billing.usage`.
# MAGIC
# MAGIC **Pattern:** Create one DQX job per client with the client's billing code in the job tags.
# MAGIC The central DQ team triggers the appropriate job when a client requests a scan.

# COMMAND ----------

# DBTITLE 1,2a. Create Tagged DQX Job per Client (Jobs API)
import json

# The DQX runner notebook path (Notebook 3 from this series)
DQX_NOTEBOOK_PATH = "/Workspace/Shared/CDO Data Quality/3-DQ_Runner"

# Define client-specific job configurations
client_jobs = [
    {
        "name": "DQX Scan - ACME Corp (ACME-001)",
        "billing_code": "ACME-001",
        "requesting_team": "ACME Corp",
        "schemas_to_scan": "uhg_demos.cdo_data_quality"
    },
    {
        "name": "DQX Scan - Globex Inc (GLOBEX-002)",
        "billing_code": "GLOBEX-002",
        "requesting_team": "Globex Inc",
        "schemas_to_scan": "uhg_demos.cdo_data_quality"
    },
]

def create_tagged_dqx_job(client_config: dict) -> dict:
    """
    Build a job spec that tags the run with the client's billing code.
    Tags on the job propagate to system.billing.usage.custom_tags (classic compute)
    and are queryable in system.lakeflow.job_run_timeline.
    """
    job_spec = {
        "name": client_config["name"],
        "tags": {
            "billing_code":    client_config["billing_code"],
            "requesting_team": client_config["requesting_team"],
            "cost_center":     "DQ-Scanning",
            "scan_type":       "ad-hoc"
        },
        "tasks": [
            {
                "task_key": "dqx_scan",
                "notebook_task": {
                    "notebook_path": DQX_NOTEBOOK_PATH,
                    "base_parameters": {
                        "schemas_to_scan": client_config["schemas_to_scan"],
                        "billing_code":    client_config["billing_code"]
                    }
                },
                "environment_key": "default"
            }
        ],
        "environments": [
            {
                "environment_key": "default",
                "spec": {
                    "client": "1",
                    "dependencies": ["databricks-labs-dqx"]
                }
            }
        ],
        "queue": {"enabled": True}
    }
    return job_spec

# Preview the job specs
for client in client_jobs:
    spec = create_tagged_dqx_job(client)
    print(f"\n--- {client['name']} ---")
    print(json.dumps(spec["tags"], indent=2))

# COMMAND ----------

# DBTITLE 1,2b. Create the Jobs via SDK
# Create jobs via the Databricks SDK
# Uncomment to run:

# for client in client_jobs:
#     spec = create_tagged_dqx_job(client)
#     job = w.jobs.create(**spec)
#     print(f"Created job: {spec['name']} (Job ID: {job.job_id})")

print("Job definitions ready. Uncomment to create in the workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Ad-Hoc Triggering — Tag a Run with the Requesting Client
# MAGIC
# MAGIC When a client requests a scan, the central DQ team triggers the job. The run
# MAGIC inherits the job's tags (billing_code, requesting_team, etc.).
# MAGIC
# MAGIC ### Option A: Trigger via API
# MAGIC ```bash
# MAGIC # Trigger the ACME Corp DQX scan job
# MAGIC curl -X POST "https://${DATABRICKS_HOST}/api/2.1/jobs/run-now" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "job_id": 123456,
# MAGIC     "notebook_params": {
# MAGIC       "schemas_to_scan": "uhg_demos.cdo_data_quality",
# MAGIC       "billing_code": "ACME-001"
# MAGIC     }
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC ### Option B: Submit a One-Time Run with Custom Tags
# MAGIC For truly ad-hoc runs where no pre-created job exists, use `runs/submit` and
# MAGIC pass the billing code as a notebook parameter + rely on the budget policy for
# MAGIC serverless billing attribution.

# COMMAND ----------

# DBTITLE 1,3a. Trigger Ad-Hoc via SDK
def trigger_client_dqx_scan(job_id: int, billing_code: str, schemas: str = "uhg_demos.cdo_data_quality"):
    """Trigger a DQX scan for a specific client. The job's tags handle billing attribution."""
    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params={
            "schemas_to_scan": schemas,
            "billing_code": billing_code,
            "triggered_by": "central_dq_team",
        }
    )
    print(f"Triggered run {run.run_id} for billing_code={billing_code}")
    return run

# Example usage (uncomment with real job_id):
# trigger_client_dqx_scan(job_id=123456, billing_code="ACME-001")

# COMMAND ----------

# DBTITLE 1,3b. Submit One-Time Ad-Hoc Run (No Pre-Created Job)
def submit_adhoc_dqx_scan(billing_code: str, requesting_team: str, schemas: str = "uhg_demos.cdo_data_quality"):
    """
    Submit a one-time DQX run tagged with the client's billing code.
    Uses runs/submit — no pre-existing job required.
    For serverless billing attribution, the user should also select the
    client's budget policy when applicable.
    """
    run = w.jobs.submit(
        run_name=f"DQX Ad-Hoc Scan - {requesting_team} ({billing_code})",
        tasks=[
            {
                "task_key": "dqx_adhoc_scan",
                "notebook_task": {
                    "notebook_path": "/Workspace/Shared/CDO Data Quality/3-DQ_Runner",
                    "base_parameters": {
                        "schemas_to_scan": schemas,
                        "billing_code": billing_code,
                        "requesting_team": requesting_team,
                    }
                },
                "environment_key": "default"
            }
        ],
        environments=[
            {
                "environment_key": "default",
                "spec": {
                    "client": "1",
                    "dependencies": ["databricks-labs-dqx"]
                }
            }
        ],
        tags={
            "billing_code": billing_code,
            "requesting_team": requesting_team,
            "cost_center": "DQ-Scanning",
            "scan_type": "ad-hoc"
        }
    )
    print(f"Submitted ad-hoc run {run.run_id} | billing_code={billing_code} | team={requesting_team}")
    return run

# Example:
# submit_adhoc_dqx_scan("ACME-001", "ACME Corp")
# submit_adhoc_dqx_scan("GLOBEX-002", "Globex Inc", schemas="uhg_demos.claims_analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Querying `system.billing.usage` for Per-Client DQX Chargeback
# MAGIC
# MAGIC Once jobs run with billing code tags (via budget policies or job-level tags),
# MAGIC all usage rows in `system.billing.usage` carry the `custom_tags` map.
# MAGIC
# MAGIC ### Key Columns in `system.billing.usage`
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `custom_tags` | MAP<STRING, STRING> — includes budget policy tags and job/cluster tags |
# MAGIC | `billing_origin_product` | Product that generated the usage (e.g., `JOBS`, `SERVERLESS_COMPUTE`) |
# MAGIC | `usage_quantity` | DBUs consumed |
# MAGIC | `sku_name` | SKU for price lookup |
# MAGIC | `usage_metadata` | Struct with `job_id`, `job_run_id`, `notebook_id`, etc. |

# COMMAND ----------

# DBTITLE 1,4a. All DQX-Tagged Usage by Billing Code
# MAGIC %sql
# MAGIC -- View all serverless/jobs usage tagged with a billing_code
# MAGIC -- custom_tags is a MAP<STRING, STRING> — use element_at() to extract values
# MAGIC
# MAGIC SELECT
# MAGIC     usage_date,
# MAGIC     element_at(custom_tags, 'billing_code') AS billing_code,
# MAGIC     element_at(custom_tags, 'requesting_team') AS requesting_team,
# MAGIC     element_at(custom_tags, 'cost_center') AS cost_center,
# MAGIC     sku_name,
# MAGIC     billing_origin_product,
# MAGIC     usage_quantity AS dbus,
# MAGIC     usage_metadata.job_id,
# MAGIC     usage_metadata.job_run_id
# MAGIC FROM system.billing.usage
# MAGIC WHERE element_at(custom_tags, 'billing_code') IS NOT NULL
# MAGIC   AND element_at(custom_tags, 'cost_center') = 'DQ-Scanning'
# MAGIC ORDER BY usage_date DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,4b. Monthly DQX Chargeback by Client (DBU)
# MAGIC %sql
# MAGIC -- Chargeback report: total DBUs consumed per billing_code per month
# MAGIC
# MAGIC SELECT
# MAGIC     element_at(custom_tags, 'billing_code') AS billing_code,
# MAGIC     element_at(custom_tags, 'requesting_team') AS requesting_team,
# MAGIC     DATE_TRUNC('month', usage_date) AS usage_month,
# MAGIC     ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC     COUNT(DISTINCT usage_metadata.job_run_id) AS scan_runs,
# MAGIC     COUNT(*) AS billing_events
# MAGIC FROM system.billing.usage
# MAGIC WHERE element_at(custom_tags, 'cost_center') = 'DQ-Scanning'
# MAGIC   AND element_at(custom_tags, 'billing_code') IS NOT NULL
# MAGIC GROUP BY
# MAGIC     element_at(custom_tags, 'billing_code'),
# MAGIC     element_at(custom_tags, 'requesting_team'),
# MAGIC     DATE_TRUNC('month', usage_date)
# MAGIC ORDER BY usage_month DESC, total_dbus DESC;

# COMMAND ----------

# DBTITLE 1,4c. Monthly DQX Chargeback by Client (Dollars)
# MAGIC %sql
# MAGIC -- Full dollar-cost chargeback per client
# MAGIC
# MAGIC WITH tagged_usage AS (
# MAGIC     SELECT
# MAGIC         u.usage_date,
# MAGIC         u.usage_start_time,
# MAGIC         u.sku_name,
# MAGIC         u.usage_quantity,
# MAGIC         element_at(u.custom_tags, 'billing_code') AS billing_code,
# MAGIC         element_at(u.custom_tags, 'requesting_team') AS requesting_team
# MAGIC     FROM system.billing.usage u
# MAGIC     WHERE element_at(u.custom_tags, 'cost_center') = 'DQ-Scanning'
# MAGIC       AND element_at(u.custom_tags, 'billing_code') IS NOT NULL
# MAGIC ),
# MAGIC priced AS (
# MAGIC     SELECT
# MAGIC         t.billing_code,
# MAGIC         t.requesting_team,
# MAGIC         t.usage_date,
# MAGIC         t.usage_quantity,
# MAGIC         t.usage_quantity * p.pricing.effective_list.default AS estimated_cost_usd
# MAGIC     FROM tagged_usage t
# MAGIC     JOIN system.billing.list_prices p
# MAGIC       ON t.sku_name = p.sku_name
# MAGIC       AND t.usage_start_time >= p.price_start_time
# MAGIC       AND t.usage_start_time < COALESCE(p.price_end_time, TIMESTAMP '2099-12-31')
# MAGIC )
# MAGIC SELECT
# MAGIC     billing_code,
# MAGIC     requesting_team,
# MAGIC     DATE_TRUNC('month', usage_date) AS usage_month,
# MAGIC     ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC     ROUND(SUM(estimated_cost_usd), 2) AS estimated_cost_usd
# MAGIC FROM priced
# MAGIC GROUP BY billing_code, requesting_team, DATE_TRUNC('month', usage_date)
# MAGIC ORDER BY usage_month DESC, estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,4d. Drill Down: Per-Run Cost Detail
# MAGIC %sql
# MAGIC -- Detail view: cost breakdown per individual scan run
# MAGIC
# MAGIC SELECT
# MAGIC     element_at(custom_tags, 'billing_code') AS billing_code,
# MAGIC     element_at(custom_tags, 'requesting_team') AS requesting_team,
# MAGIC     usage_date,
# MAGIC     usage_metadata.job_id AS job_id,
# MAGIC     usage_metadata.job_run_id AS run_id,
# MAGIC     sku_name,
# MAGIC     ROUND(usage_quantity, 6) AS dbus,
# MAGIC     element_at(custom_tags, 'scan_type') AS scan_type
# MAGIC FROM system.billing.usage
# MAGIC WHERE element_at(custom_tags, 'cost_center') = 'DQ-Scanning'
# MAGIC   AND element_at(custom_tags, 'billing_code') IS NOT NULL
# MAGIC ORDER BY usage_date DESC, billing_code
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,4e. Cross-Reference: Job Run Timeline + Tags
# MAGIC %sql
# MAGIC -- Join job run timeline with billing to see run details alongside cost
# MAGIC -- system.lakeflow.job_run_timeline has run_name, result_state, etc.
# MAGIC
# MAGIC SELECT
# MAGIC     j.run_name,
# MAGIC     j.result_state,
# MAGIC     j.start_time,
# MAGIC     j.end_time,
# MAGIC     TIMESTAMPDIFF(MINUTE, j.start_time, j.end_time) AS duration_minutes,
# MAGIC     element_at(u.custom_tags, 'billing_code') AS billing_code,
# MAGIC     element_at(u.custom_tags, 'requesting_team') AS requesting_team,
# MAGIC     ROUND(SUM(u.usage_quantity), 4) AS total_dbus
# MAGIC FROM system.lakeflow.job_run_timeline j
# MAGIC JOIN system.billing.usage u
# MAGIC   ON u.usage_metadata.job_run_id = CAST(j.run_id AS STRING)
# MAGIC WHERE element_at(u.custom_tags, 'cost_center') = 'DQ-Scanning'
# MAGIC GROUP BY j.run_name, j.result_state, j.start_time, j.end_time,
# MAGIC          element_at(u.custom_tags, 'billing_code'),
# MAGIC          element_at(u.custom_tags, 'requesting_team')
# MAGIC ORDER BY j.start_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. End-to-End: Parameterized DQX Scan Wrapper
# MAGIC
# MAGIC This section shows a complete automation pattern: a single function that the
# MAGIC central DQ team calls when a client requests a scan. It:
# MAGIC
# MAGIC 1. Validates the client's billing code
# MAGIC 2. Submits a tagged DQX job run
# MAGIC 3. Logs the request for audit
# MAGIC 4. Returns tracking info

# COMMAND ----------

# DBTITLE 1,5a. Client Registry Table
# MAGIC %sql
# MAGIC -- Central registry of approved clients and their billing codes
# MAGIC -- This is the source of truth for who can request DQ scans
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS uhg_demos.cdo_data_quality.dqx_client_registry (
# MAGIC     billing_code    STRING NOT NULL,
# MAGIC     client_name     STRING NOT NULL,
# MAGIC     contact_email   STRING,
# MAGIC     schemas_allowed STRING,     -- Comma-separated list of schemas this client can scan
# MAGIC     is_active       BOOLEAN DEFAULT TRUE,
# MAGIC     created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC );
# MAGIC
# MAGIC -- Seed with sample clients
# MAGIC MERGE INTO uhg_demos.cdo_data_quality.dqx_client_registry AS target
# MAGIC USING (
# MAGIC     SELECT * FROM VALUES
# MAGIC         ('ACME-001',    'ACME Corp',    'dq@acme.com',    'uhg_demos.cdo_data_quality', TRUE),
# MAGIC         ('GLOBEX-002',  'Globex Inc',   'dq@globex.com',  'uhg_demos.cdo_data_quality', TRUE),
# MAGIC         ('INITECH-003', 'Initech LLC',  'dq@initech.com', 'uhg_demos.cdo_data_quality', TRUE)
# MAGIC     AS src(billing_code, client_name, contact_email, schemas_allowed, is_active)
# MAGIC ) AS source
# MAGIC ON target.billing_code = source.billing_code
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT * FROM uhg_demos.cdo_data_quality.dqx_client_registry;

# COMMAND ----------

# DBTITLE 1,5b. Scan Request Audit Log
# MAGIC %sql
# MAGIC -- Audit log: every DQX scan request is recorded here
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS uhg_demos.cdo_data_quality.dqx_scan_audit_log (
# MAGIC     request_id      STRING,
# MAGIC     billing_code    STRING,
# MAGIC     client_name     STRING,
# MAGIC     schemas_scanned STRING,
# MAGIC     job_run_id      STRING,
# MAGIC     requested_by    STRING,
# MAGIC     request_time    TIMESTAMP,
# MAGIC     status          STRING      -- SUBMITTED, COMPLETED, FAILED
# MAGIC );

# COMMAND ----------

# DBTITLE 1,5c. Parameterized Scan Dispatcher
import uuid
from datetime import datetime

DQX_NOTEBOOK_PATH = "/Workspace/Shared/CDO Data Quality/3-DQ_Runner"

def request_dqx_scan(billing_code: str, schemas: str = None, requested_by: str = "central_dq_team"):
    """
    End-to-end: validate client, submit tagged DQX run, log the request.

    Parameters:
        billing_code:  The client's billing code (must exist in dqx_client_registry)
        schemas:       Comma-separated schemas to scan (defaults to client's allowed schemas)
        requested_by:  Who on the DQ team is triggering this
    """
    # 1. Validate client
    client = spark.sql(f"""
        SELECT * FROM uhg_demos.cdo_data_quality.dqx_client_registry
        WHERE billing_code = '{billing_code}' AND is_active = TRUE
    """).collect()

    if not client:
        raise ValueError(f"Unknown or inactive billing code: {billing_code}")

    client = client[0]
    target_schemas = schemas or client.schemas_allowed
    request_id = str(uuid.uuid4())[:8]

    print(f"=== DQX Scan Request ===")
    print(f"Request ID:    {request_id}")
    print(f"Client:        {client.client_name} ({billing_code})")
    print(f"Schemas:       {target_schemas}")
    print(f"Requested by:  {requested_by}")

    # 2. Submit tagged job run
    # The tags on this job flow to system.billing.usage.custom_tags
    run = w.jobs.submit(
        run_name=f"DQX-{billing_code}-{request_id}",
        tasks=[{
            "task_key": "dqx_scan",
            "notebook_task": {
                "notebook_path": DQX_NOTEBOOK_PATH,
                "base_parameters": {
                    "schemas_to_scan": target_schemas,
                    "billing_code":    billing_code,
                    "request_id":      request_id,
                }
            },
            "environment_key": "default"
        }],
        environments=[{
            "environment_key": "default",
            "spec": {"client": "1", "dependencies": ["databricks-labs-dqx"]}
        }],
        tags={
            "billing_code":    billing_code,
            "requesting_team": client.client_name,
            "cost_center":     "DQ-Scanning",
            "scan_type":       "ad-hoc",
            "request_id":      request_id,
        }
    )

    # 3. Log the request
    spark.sql(f"""
        INSERT INTO uhg_demos.cdo_data_quality.dqx_scan_audit_log
        VALUES ('{request_id}', '{billing_code}', '{client.client_name}',
                '{target_schemas}', '{run.run_id}', '{requested_by}',
                CURRENT_TIMESTAMP(), 'SUBMITTED')
    """)

    print(f"Run ID:        {run.run_id}")
    print(f"Status:        SUBMITTED")
    print(f"Audit logged:  dqx_scan_audit_log")

    return {"request_id": request_id, "run_id": run.run_id, "billing_code": billing_code}

# COMMAND ----------

# DBTITLE 1,5d. Example: Trigger a Client Scan
# --- Trigger a scan for ACME Corp ---
# Uncomment to execute:
# result = request_dqx_scan("ACME-001")

# --- Trigger a scan for Globex Inc ---
# result = request_dqx_scan("GLOBEX-002")

print("Uncomment above to trigger ad-hoc scans. Each run is tagged and audit-logged.")

# COMMAND ----------

# DBTITLE 1,5e. View Audit Log
# MAGIC %sql
# MAGIC SELECT * FROM uhg_demos.cdo_data_quality.dqx_scan_audit_log
# MAGIC ORDER BY request_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary: Multi-Tenant DQX Chargeback Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │  Client Request                                                     │
# MAGIC │  "Please scan our schemas for data quality"                         │
# MAGIC │                                                                     │
# MAGIC │  billing_code = "ACME-001"                                          │
# MAGIC └──────────────────────┬──────────────────────────────────────────────┘
# MAGIC                        │
# MAGIC                        ▼
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │  Central DQ Team — Scan Dispatcher                          │
# MAGIC │                                                              │
# MAGIC │  1. Validate billing_code in client registry                 │
# MAGIC │  2. Submit DQX job with tags:                                │
# MAGIC │     • billing_code = "ACME-001"                              │
# MAGIC │     • requesting_team = "ACME Corp"                          │
# MAGIC │     • cost_center = "DQ-Scanning"                            │
# MAGIC │  3. Log request in audit table                               │
# MAGIC └──────────────────────┬───────────────────────────────────────┘
# MAGIC                        │
# MAGIC              ┌─────────┴─────────┐
# MAGIC              ▼                   ▼
# MAGIC   ┌─────────────────┐  ┌──────────────────┐
# MAGIC   │ Serverless Job  │  │  Budget Policy   │
# MAGIC   │ Tags (job API)  │  │  Tags (admin)    │
# MAGIC   └────────┬────────┘  └────────┬─────────┘
# MAGIC            │                    │
# MAGIC            └────────┬───────────┘
# MAGIC                     ▼
# MAGIC          ┌──────────────────────┐
# MAGIC          │ system.billing.usage │
# MAGIC          │   custom_tags:       │
# MAGIC          │     billing_code     │
# MAGIC          │     requesting_team  │
# MAGIC          │     cost_center      │
# MAGIC          └──────────┬───────────┘
# MAGIC                     │
# MAGIC                     ▼
# MAGIC          ┌──────────────────────┐
# MAGIC          │ Chargeback Queries   │
# MAGIC          │  Per-client DBUs     │
# MAGIC          │  Per-client $$$      │
# MAGIC          │  Per-run detail      │
# MAGIC          └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC | Principle | Implementation |
# MAGIC |-----------|---------------|
# MAGIC | **Serverless billing attribution** | Budget policies are the primary mechanism — admin assigns per-client policies with `custom_tags` |
# MAGIC | **Job-level tags** | Complement budget policies — set `billing_code` on the job definition for classic compute attribution |
# MAGIC | **Ad-hoc tagging** | The scan dispatcher validates the client, submits a tagged job, and logs the request |
# MAGIC | **Chargeback queries** | `element_at(custom_tags, 'billing_code')` on `system.billing.usage` gives per-client cost |
# MAGIC | **Audit trail** | Every scan request is logged with billing code, schemas, run ID, and timestamp |