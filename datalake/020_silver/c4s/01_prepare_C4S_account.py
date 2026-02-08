# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from place_id import *

# COMMAND ----------

source_catalog = "d_dnst_010_bronze"
source_schema = "sap_bw"
source_table = "c4c_account"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_01"
target_table = "c4c_account"

# COMMAND ----------

API_KEY = dbutils.secrets.get("keyvault-dp", "google-maps-api-key")

# COMMAND ----------

spark.table("d_dnst_010_bronze.sap_bw.c4c_account")

# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

# COMMAND ----------

df_prepped = (
    df_source
    .filter(F.col("c4cu_country_desc") == 'Belgium')
    .selectExpr(
        "NULLIF(TRIM(c4cu_account_id), '') AS c4cu_account_id",
        "NULLIF(TRIM(c4cu_account_desc), '') AS c4cu_account_desc",
        "NULLIF(TRIM(c4cu_country_id), '') AS c4cu_country_id",
        "NULLIF(TRIM(c4cu_country_desc), '') AS c4cu_country_desc",
        "NULLIF(TRIM(c4cu_streetaddress_id), '') AS c4cu_streetaddress_id",
        "NULLIF(TRIM(c4cu_housenumber_id), '') AS c4cu_housenumber_id",
        "NULLIF(TRIM(c4cu_city_id), '') AS c4cu_city_id",
        "NULLIF(TRIM(c4cu_postalcode_id), '') AS c4cu_postalcode_id",
        "NULLIF(TRIM(c4cu_vatregistration_id), '') AS c4cu_vatregistration_id",
        "NULLIF(TRIM(c4cu_taxnumber4_id), '') AS c4cu_taxnumber4_id"
    )
    .withColumn(
        "addressLines", 
            F.concat_ws(
                " ",  # Use a comma-space separator for clarity in the combined string
                F.col("c4cu_account_desc"),
                F.col("c4cu_streetaddress_id"), 
                F.col("c4cu_housenumber_id"),
                F.col("c4cu_city_id"), 
                F.col("c4cu_postalcode_id")
            )
    )
    .withColumn(
    "c4cu_street_house_number", 
    F.concat_ws(" ", F.col("c4cu_streetaddress_id"), F.col("c4cu_housenumber_id"))
    )
)

# COMMAND ----------

df_with_addressvalidation_payload = add_payload_to_df(df_prepped, "addressvalidation", api_key = API_KEY, payload_type = "c4s")


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
