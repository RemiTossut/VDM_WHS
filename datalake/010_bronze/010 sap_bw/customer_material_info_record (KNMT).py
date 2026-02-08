# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from commenting_module import *

# COMMAND ----------

source_catalog = "d_dp_000_landing"
source_schema = "volumes"
source_path = "sap_bw/customer_material_info_record"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sap_bw"
target_table = "customer_material_info_record"

# COMMAND ----------

column_comments = [
    {
        "col": "cumi_customermaterialnumber_id",
        "type": "STRING",
        "comment": "Material Number Used by Customer"
    },
    {
        "col": "cumi_customernumber_id",
        "type": "STRING",
        "comment": "Customer number"
    },
    {
        "col": "cumi_material_id",
        "type": "STRING",
        "comment": "Material Number"
    },
    {
        "col": "cumi_salesorganization_id",
        "type": "STRING",
        "comment": "Sales Organization"
    },
    {
        "col": "cumi_distributionchannel_id",
        "type": "STRING",
        "comment": "Distribution Channel"
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

table_description = """SAP ECC Lineage
- Table description in SAP ECC: **customer_material_info_record**
- Technical name in SAP ECC: **KNMT**

This is a mapping table between our customers product references and Vandemoortele s material references."""

# COMMAND ----------

add_table_description_hook_code = get_add_table_description_hook_code(
    target_catalog,
    target_schema,
    target_table,
    table_description = table_description
)

# COMMAND ----------

df = spark.read.format('parquet').load(full_path)

# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

# COMMAND ----------

df_flowbase = df
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            # "dependencies": [],
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
                },
                {
                    "id": "set columnMapping true",
                    "code_language": "sql",
                    "trigger_timing": "pre",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": enable_columnmapping_sql_code
                }
            ]
            }
