# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from place_id import *

# COMMAND ----------

source_catalog = "d_dnst_020_silver"
source_schema = "silver_03"
source_table = "google_api_mapping_table"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_03"
target_table = "02_C4S_with_place_id"

# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")
df_01_c4s = spark.table(f"{source_catalog}.silver_01.c4c_account")

# COMMAND ----------

df_joined = (
    df_01_c4s.alias("c")
    .join(
        df_source.alias("m"),
        on = F.expr("c.payload_string = m.payload and c.api_name = m.api_name"),
        how = "left"
    )
    .selectExpr(
        "c.*",
        "m.browser_url",
        "m.place_id",
    )
    .drop("c.place_id")
)

# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

# COMMAND ----------

df_flowbase = df_joined
flowbase = {
            "catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}",
               f"{source_catalog}.silver_01.01_c4s_dump"
            ],
            "deploy_strategy": "column",
            "operation_hooks": [
                {
                    "id": "set_columnMapping_true",
                    "code_language": "python",
                    "trigger_timing": "pre",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": f"spark.sql(\"\"\"{enable_columnmapping_sql_code}\"\"\")"
                }
            ]
}
