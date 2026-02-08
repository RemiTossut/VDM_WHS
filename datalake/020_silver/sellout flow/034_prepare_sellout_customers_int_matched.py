# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

# import imports
from imports import *
from matching_functions import *
from commenting_module import get_add_comment_hook_code

# COMMAND ----------

source_catalog = "d_dnst_020_silver"
source_schema = "silver_03"
source_table = "03_sellout_with_place_id"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_03"
target_table = "031_sellout_customers_int_matched"

# COMMAND ----------

# %sql
# DROP TABLE d_dnst_020_silver.silver_03.`031_sellout_customers_int_matched`

# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

# COMMAND ----------

def get_matched_table():
    Matched_table = apply_matching(
    source=df_source,
    target=df_source,
    target_id_column="unique_id",
    source_id_column= "unique_id",
    multiple_match_handler="best",
    id_matching_allowed = False,
    match_rules=[
        {
            "match_type": "exact",
            "result_column": "int_customer_number_match",
            "columns": [
                {
                    "source_column": "customer_number",
                    "target_column": "customer_number",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True 
                }
            ]
        },
        {
            "match_type": "exact",
            "result_column": "int_vat_number_match",
            "columns": [
                {
                    "source_column": "vat_number",
                    "target_column": "vat_number",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True
                }
            ]
        },
        {
            "match_type": "exact",
            "result_column": "int_place_id_match",
            "columns": [
                {
                    "source_column": "place_id",
                    "target_column": "place_id",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True
                }
            ]
        },
        {
            "match_type": "levenstein",
            "result_column": "int_levenstein_name_address",
            "score_column": "levenstein_score",
            "comparison_details_column": "match_details", # New: column for transparency map
            "min_score": 0.75, 
            "trim": True,
            "case_insensitive": True,
            "columns": [
                {
                    "source_column": "customer_name",
                    "target_column": "customer_name",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True,
                    "weight": 0.5,
                    "match": "fuzzy"
                },
                {
                    "source_column": "customer_address",
                    "target_column": "customer_address",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True,
                    "weight": 0.5,
                    "match": "fuzzy"
                },
                {
                    "source_column": "customer_postal_code",
                    "target_column": "customer_postal_code",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True,
                    "match": "exact"
                }
            ]
        },
    ]
    )
    
    return Matched_table

# COMMAND ----------

def get_fn_flowbase():

    df_final = assign_unified_id_pyspark(
    df=get_matched_table(),
    match_columns=["int_customer_number_match", "int_vat_number_match", "int_place_id_match","int_levenstein_name_address"],
    id_column="unique_id",
    label_column="dim_sellout_cust_id",
    max_iterations = 6
    )
    df_final = (
        df_final
        # .withColumn("match_details", F.to_json(F.col("match_details")))
        # .filter(F.col("unique_id").isin('1162832285'))
        .withColumn("unique_id", F.col("unique_id").cast("integer"))
    )



    return df_final

# COMMAND ----------

# window_spec = Window.partitionBy("unique_id")
# (
#     get_matched_table()
#     .withColumn(
#     "match_count", 
#     F.count(source_id_column).over(window_spec)
#     )
#     .filter(
#     F.col("match_count") > 2
#     )
#     .orderBy("unique_id")
#     .display()
# )

# COMMAND ----------

# get_fn_flowbase()

# COMMAND ----------

fn_flowbase = get_fn_flowbase
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "table_structure": """
                  customer_number STRING, 
                  customer_address STRING, 
                  customer_city STRING, 
                  customer_postal_code STRING, 
                  customer_country STRING, 
                  customer_name STRING, 
                  owner_name STRING, 
                  wholesaler_name STRING, 
                  vat_number STRING, 
                  wholesaler_id STRING, 
                  unique_id INTEGER NOT NULL, 
                  payload STRUCT<
                        address: STRUCT<
                        regionCode: STRING, 
                        addressLines: ARRAY<STRING>, 
                        locality: STRING, 
                        postalCode: STRING
                        >
                  >, 
                  api_url STRING, 
                  payload_string STRING, 
                  api_name STRING, 
                  run_id LONG, 
                  browser_url STRING, 
                  place_id STRING, 
                  int_customer_number_match STRING, 
                  int_vat_number_match STRING, 
                  int_place_id_match STRING, 
                  int_levenstein_name_address STRING, 
                  levenstein_score DOUBLE,
                  value_dict STRING,
                  match_details STRING,
                  dim_sellout_cust_id STRING
            """, # <-- This ensures it's a single string
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}"
               ],
            "write_options": {"mergeSchema":"true"}
            }
