# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *

# COMMAND ----------

source_catalog = "d_dnst_000_landing"
source_schema = "volumes"
source_path = "sharepoint_lists/wholesaler_uom_conversion_table.csv"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sharepoint_list"
target_table = "wholesaler_uom_conversion_table"

# COMMAND ----------

df = spark.read.csv(full_path, header=True, inferSchema=True)

# COMMAND ----------

df_prepped = (
    df
    .selectExpr(
        "Title as title",
        "UoM as uom",
        "ConvertedUoM as converted_uom",
        "Wholesaler as wholesaler",
        "Wholesaler_Name as wholesaler_name"
    )
)

# COMMAND ----------

df_flowbase = df_prepped
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "dependencies": ["d_dnst_000_landing.metadata.staging_wholesaler_uom_conversion_table_sharepoint_list_metadata"],
            "deploy_strategy": "recreate"
            }
