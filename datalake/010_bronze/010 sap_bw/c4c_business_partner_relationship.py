# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from commenting_module import *

# COMMAND ----------

source_catalog = "d_dp_000_landing"
source_schema = "volumes"
source_path = "sap_bw/c4c_business_partner_relationship"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sap_bw"
target_table = "c4c_business_partner_relationship"

# COMMAND ----------

df = spark.read.format('parquet').load(full_path)

# COMMAND ----------

df_prepped = (
    df
    .withColumn(
        "c4cu_businesspartner_id", 
        F.regexp_replace(F.col("c4cu_businesspartner_id"), "^0+", "")
    )
    .withColumn(
        "c4cu_businesspartner2_id", 
        F.regexp_replace(F.col("c4cu_businesspartner2_id"), "^0+", "")
    )
    .withColumn(
        "c4cu_numberatwholesaler_id", 
        F.regexp_replace(F.col("c4cu_numberatwholesaler_id"), "^0+", "")
    )
)

# COMMAND ----------

column_comments = [
    {
        "col": "c4cu_businesspartner_id",
        "type": "STRING",
        "comment": "test"
    }
]

# COMMAND ----------

add_column_hook_code = get_add_comment_hook_code(
    target_catalog,
    target_schema,
    target_table,
    column_metadata=column_comments
)

# COMMAND ----------

table_description = "test"

# COMMAND ----------

add_table_description_hook_code = get_add_table_description_hook_code(
    target_catalog,
    target_schema,
    target_table,
    table_description = table_description
)

# COMMAND ----------

df_flowbase = df_prepped
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            # "dependencies": []
            "operation_hooks": [
                {
                    "id": "add_column_comments",
                    "code_language": "python",
                    "trigger_timing": "post",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": add_column_hook_code
                },
                {
                    "id": "add_table_description",
                    "code_language": "python",
                    "trigger_timing": "post",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": add_table_description_hook_code
                }
            ]
            }
