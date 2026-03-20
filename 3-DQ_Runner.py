# Databricks notebook source
# MAGIC %pip install --upgrade --force-reinstall databricks-labs-dqx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
import uuid
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import OutputConfig, InputConfig
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

# --- Configuration ---
CATALOG       = "uhg_demos"
SCHEMA        = "cdo_data_quality"
MAPPING_TABLE = f"{CATALOG}.{SCHEMA}.cdo_dq_rule_mapping"
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.cdo_dq_results"

# --- Initialization ---
# Setup the Native DQX Engine (Serverless compatible mode)
engine = DQEngine(WorkspaceClient())
job_run_id = str(uuid.uuid4()) # Generate one ID for the entire notebook run

def run_universal_dq_scan():
    # 1. Discovery (remains the same)
    discovery_query = f"""
        SELECT 
            tag.catalog_name, tag.schema_name, tag.table_name, tag.column_name,
            map.rule_name, map.sql_expression, map.criticality
        FROM system.information_schema.column_tags AS tag
        JOIN {MAPPING_TABLE} AS map 
          ON tag.tag_name = map.tag_name AND tag.tag_value = map.tag_value
    """
    work_items = spark.sql(discovery_query).collect()
    
    if not work_items:
        print("No columns found for DQ Monitoring.")
        return

    # 2. Execution Loop
    for item in work_items:
        full_table_path = f"{item.catalog_name}.{item.schema_name}.{item.table_name}"
        col_name = item.column_name
        
        # We'll use a clean name for the column to avoid resolution issues
        clean_rule_name = f"dq_check_{item.rule_name.replace(' ', '_')}"
        
        formatted_sql = item.sql_expression.replace("{col}", col_name)
        df = spark.read.table(full_table_path)
        
        dqx_rules = [{
            "name": clean_rule_name, # Use a clean name with no spaces
            "check": {"function": "sql_expression", "arguments": {"expression": formatted_sql}},
            "criticality": item.criticality
        }]
        
        checked_df = engine.apply_checks_by_metadata(df, dqx_rules)
        
        summary_df = checked_df.select(
            F.lit(full_table_path).alias("table_name"),
            F.lit(col_name).alias("column_name"),
            F.lit(item.rule_name).alias("rule_name"),
            
            F.when(F.col("_errors").isNotNull() & (F.size(F.col("_errors")) > 0), 1)
             .otherwise(0).alias("has_error"),
            
            F.when(F.col("_warnings").isNotNull() & (F.size(F.col("_warnings")) > 0), 1)
             .otherwise(0).alias("has_warn")
        ).groupBy("table_name", "column_name", "rule_name").agg(
            F.count("*").alias("total_rows"),
            F.sum("has_error").alias("rows_error"),
            F.sum("has_warn").alias("rows_warn")
        ).withColumn("scan_date", F.current_timestamp()) \
         .withColumn("run_id", F.lit(job_run_id))

        summary_df.write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)

if __name__ == "__main__":
    run_universal_dq_scan()