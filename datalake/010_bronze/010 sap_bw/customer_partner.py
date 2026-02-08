# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *

# COMMAND ----------

source_catalog = "d_dp_000_landing"
source_schema = "volumes"
source_path = "sap_bw/customer_partners"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sap_bw"
target_table = "customer_partner"

# COMMAND ----------

df = spark.read.format('parquet').load(full_path)

# COMMAND ----------

# (
#     df
#     .filter(F.col("c4cu_name1_id") != "")
#     # .filter(F.col("c4cu_countrykey_id") == "BE")
#     .display()
# )

# COMMAND ----------

df_flowbase = df
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "deploy_strategy": "recreate"
            # "dependencies": []
            }
