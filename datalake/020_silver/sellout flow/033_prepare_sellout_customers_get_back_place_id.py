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
target_table = "03_sellout_with_place_id"

# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")
df_03_sellout_customers = spark.table(f"{source_catalog}.{source_schema}.03_sellout_customers")

# COMMAND ----------

df_joined = (
    df_03_sellout_customers.alias("c")
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

df_flowbase = df_joined
flowbase = {
            "catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}"
            ]
}
