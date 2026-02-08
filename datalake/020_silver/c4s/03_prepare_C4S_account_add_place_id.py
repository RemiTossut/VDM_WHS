# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from place_id import *

# COMMAND ----------

API_KEY = dbutils.secrets.get("keyvault-dp", "google-maps-api-key")
gmaps = googlemaps.Client(key=API_KEY)

# COMMAND ----------

source_catalog = "d_dnst_020_silver"
source_schema = "silver_02"
source_table = "02_c4s_filtered_place_id"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_03"
target_table = "google_api_mapping_table"

# COMMAND ----------

merge_statement = f"""
MERGE INTO <<<target>>> AS target
USING (
  SELECT
    -- Business partner columns
    business_partner_name,
    business_partner_country,
    business_partner_postal_code,
    business_partner_address,
    business_partner_city,

    -- Keys and payload
    api_name,
    payload_string AS payload,

    -- Values
    api_url,
    browser_url,
    place_id,
    google_api_raw_json,
    api_execution_timestamp
  FROM <<<source>>>
) AS source
ON  source.api_name = target.api_name
AND source.payload  = target.payload

WHEN MATCHED THEN UPDATE SET
  -- keep the BP identity as-is; only update the dynamic fields
  target.place_id                 = source.place_id,
  target.browser_url              = source.browser_url,
  target.google_api_raw_json      = source.google_api_raw_json,
  target.api_url_masked           = source.api_url,
  target.api_execution_timestamp  = source.api_execution_timestamp

WHEN NOT MATCHED THEN INSERT (
  business_partner_name,
  business_partner_country,
  business_partner_postal_code,
  business_partner_address,
  business_partner_city,
  payload,
  api_name,
  api_url_masked,
  browser_url,
  place_id,
  google_api_raw_json,
  api_execution_timestamp
) VALUES (
  source.business_partner_name,
  source.business_partner_country,
  source.business_partner_postal_code,
  source.business_partner_address,
  source.business_partner_city,
  source.payload,
  source.api_name,
  source.api_url,
  source.browser_url,
  source.place_id,
  source.google_api_raw_json,
  source.api_execution_timestamp
)
"""

# COMMAND ----------

def get_fn_flowbase():
    src = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

    df_prepped = (
        src.selectExpr(
            "c4cu_account_desc          as business_partner_name",
            "c4cu_country_desc       as business_partner_country",
            "c4cu_postalcode_id   as business_partner_postal_code",
            "c4cu_streetaddress_id       as business_partner_address",
            "c4cu_city_id          as business_partner_city",
            "payload_string",
            "payload",
            "api_name",
            "api_url",
            "browser_url",
            "place_id",
            # "CAST(NULL AS STRING)     as google_api_raw_json",
            # "CAST(NULL AS TIMESTAMP)  as api_execution_timestamp"
        )
        .withColumn("api_url_masked", F.regexp_replace("api_url", r"(?<=key=)[^&]+", "***"))
        # .drop("api_url")
    )
    df_with_api = apply_google_api(df_prepped, "addressvalidation")
    return df_with_api

# COMMAND ----------

fn_flowbase = get_fn_flowbase
flowbase = {
            "catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "merge_custom",
            "merge_statement": merge_statement,
            "table_structure": (
                  "business_partner_name STRING, "
                  "business_partner_country STRING, "
                  "business_partner_postal_code STRING, "
                  "business_partner_address STRING, "
                  "business_partner_city STRING, "
                  "payload STRING, "
                  "api_name STRING, "
                  "api_url_masked STRING, "
                  "browser_url STRING, "
                  "place_id STRING, "
                  "google_api_raw_json STRING, "
                  "api_execution_timestamp TIMESTAMP"
               ),""
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}"
            ],
            "deploy_strategy": "column",
}
