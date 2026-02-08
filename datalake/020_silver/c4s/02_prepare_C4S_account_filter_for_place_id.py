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
source_schema = "silver_01"
source_table = "c4c_account"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_02"
target_table = "02_c4s_filtered_place_id"


# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

# COMMAND ----------

# create google maps place id holder table if it does not exist yet
google_api_mapping_table_path = "d_dnst_020_silver.silver_03.google_api_mapping_table"
mapping_table_schema = T.StructType([
    T.StructField("business_partner_name",        T.StringType(), True),
    T.StructField("business_partner_country",     T.StringType(), True),
    T.StructField("business_partner_postal_code", T.StringType(), True),
    T.StructField("business_partner_address",     T.StringType(), True),
    T.StructField("business_partner_city",        T.StringType(), True),     
    T.StructField("payload",              T.StringType(), True), 
    T.StructField("api_name",             T.StringType(), True),
    T.StructField("api_url_masked",              T.StringType(), True),
    T.StructField("browser_url",          T.StringType(), True),   
    T.StructField("place_id",             T.StringType(), True),
    T.StructField("google_api_raw_json",  T.StringType(), True),
    T.StructField("api_execution_timestamp",  T.TimestampType(), True),
])

# Create empty DataFrame with schema
empty_df = spark.createDataFrame([], schema=mapping_table_schema)

# Save as Delta table if not exists
empty_df.write.format("delta").mode("ignore").saveAsTable(google_api_mapping_table_path)
# empty_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(google_api_mapping_table_path)

# COMMAND ----------

df_google_maps_placeholder = spark.table(google_api_mapping_table_path)

# COMMAND ----------

df_with_existing_adressvalidation_mapping = fetch_existing_mappings(df_source,df_google_maps_placeholder,"addressvalidation")

# COMMAND ----------

api_calls_limit = 5
from datetime import datetime

current_month = datetime.today().replace(day=1).strftime('%Y-%m-%d')

number_of_api_calls_this_month = spark.sql(
    f"SELECT COUNT(*) FROM {google_api_mapping_table_path} WHERE api_execution_timestamp >= '{current_month}'"
).collect()[0][0]

# print("number_of_api_calls_this_month: ", number_of_api_calls_this_month)
api_calls_left = api_calls_limit - number_of_api_calls_this_month
# print("api_calls_left: ", api_calls_left)

# COMMAND ----------

df_filtered = (
    df_with_existing_adressvalidation_mapping
    .filter(F.col("place_id").isNull())
    .limit(max(api_calls_left, 0))
)

# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

# COMMAND ----------

df_flowbase = df_filtered
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}"
               ],
            "deploy_strategy": "column",
            # "operation_hooks": [
            #     {
            #         "id": "set columnMapping true",
            #         "code_language": "python",
            #         "trigger_timing": "pre",
            #         "trigger_functions": ["execute", "execute_full"], 
            #         "code": spark.sql(enable_columnmapping_sql_code)
            #     }
            # ]
            }
