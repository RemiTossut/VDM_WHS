# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from commenting_module import *

# COMMAND ----------

source_catalog = "d_dp_000_landing"
source_schema = "volumes"
source_path = "sap_bw/c4c_account"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sap_bw"
target_table = "c4c_account"

# COMMAND ----------

df = spark.read.format('parquet').load(full_path)

# COMMAND ----------

column_comments = [
    {
        "col": "c4cu_account_desc",
        "type": "STRING",
        "comment": "This is the customer's name as stored in SAP C4C."
    },
    {
        "col": "c4cu_customer_id",
        "type": "STRING",
        "comment": "This is the customer reference in SAP ECC R3, stored in the 'General Data in Customer Master' -table (KNA1) in th field 'customer' (KUNNR)"
    }

]

# COMMAND ----------

add_column_comment_hook_code = get_add_comment_hook_code(
    target_catalog,
    target_schema,
    target_table,
    column_metadata=column_comments
)

# COMMAND ----------

table_description = """The table contains information related to accounts in SAP C4C.SAP C4C is Vandemoortele's CRM (Customer Relation Management system), this system is often referred to as "C4S".The table includes details such as account IDs, customer IDs, and geographical information like country and city. This data can be used for customer segmentation, analyzing account distributions across regions, and understanding customer demographics."""

# COMMAND ----------

add_table_description_hook_code = get_add_table_description_hook_code(
    target_catalog,
    target_schema,
    target_table,
    table_description = table_description
)

# COMMAND ----------

df_flowbase = df
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "deploy_strategy": "recreate",
            "operation_hooks": [
                {
                    "id": "add_column_comments",
                    "code_language": "python",
                    "trigger_timing": "post",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": add_column_comment_hook_code
                },
                {
                    "id": "add_table_description",
                    "code_language": "python",
                    "trigger_timing": "post",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": add_table_description_hook_code
                }
            ]
            # "dependencies": []
            }
