# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from place_id import *

# COMMAND ----------

API_KEY = dbutils.secrets.get("keyvault-dp", "google-maps-api-key")

# COMMAND ----------

source_catalog = "d_dnst_020_silver"
source_schema = "silver_02"
source_table = "02_sellout_consolidated"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_03"
target_table = "03_sellout_customers"


# COMMAND ----------

# %sql 
# DROP TABLE d_dnst_020_silver.silver_03.`03_sellout_customers`

# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

# COMMAND ----------

df_relevant_columns = (
    df_source
    .select(
        "customer_number",
        "customer_address",
        "customer_city",
        "customer_postal_code",
        "customer_country",
        "customer_name",
        "owner_name",
        "wholesaler_name",
        "vat_number",
        "wholesaler_id"
    )
    .withColumn("customer_name", F.coalesce(F.col("customer_name"), F.col("owner_name")))
    .distinct()
)

# COMMAND ----------

all_columns_for_hash = df_relevant_columns.columns

# Convert column names to Column objects for F.concat_ws
cols_for_hash = [F.col(c) for c in all_columns_for_hash]

df_relevant_columns = df_relevant_columns.withColumn(
    "unique_id",
    F.abs(F.hash(F.concat_ws("_", *cols_for_hash)))
)

# COMMAND ----------

df_with_addressvalidation_payload = add_payload_to_df(
    df_relevant_columns, "addressvalidation", 
    api_key = API_KEY, 
    payload_type = 'sellout'
)

# COMMAND ----------

df_flowbase = df_with_addressvalidation_payload
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}"
               ]
            }
