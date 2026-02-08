# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from commenting_module import *

# COMMAND ----------

source_catalog = "d_dnst_000_landing"
source_schema = "volumes"
source_path = "sharepoint_lists/uom_assign_internal_to_language_dependent_unit_t006a.csv"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sharepoint_list"
target_table = "uom_assign_internal_to_language_dependent_unit_t006a"

# COMMAND ----------

df = spark.read.csv(full_path, header=True, inferSchema=True)

# COMMAND ----------

df_prepped = (
    df
    .selectExpr(
    "converted_uom AS converted_uom",
    "language AS language",
    "muom_unit_id AS muom_unit_id",
    "uom_desc AS uom_desc"
    )
)

# COMMAND ----------

column_comments = [
    {
        "col": "converted_uom",
        "type": "STRING",
        "comment": "converted UOM"
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
            "dependencies": ["d_dnst_000_landing.metadata.staging_uom_assign_internal_to_language_dependent_unit_t006a"],
            "deploy_strategy": "recreate",
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
