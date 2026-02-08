# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from matching_functions import *

# COMMAND ----------

C4S_source_catalog = "d_dnst_020_silver"
C4S_source_schema = "silver_03"
C4S_source_table = "02_C4S_with_place_id"

c4c_business_partner_relationship_catalog = "d_dnst_010_bronze"
c4c_business_partner_relationship_schema = "sap_bw"
c4c_business_partner_relationship_table = "c4c_business_partner_relationship"

sellout_cust_source_catalog = "d_dnst_020_silver"
sellout_cust_source_schema = "silver_03"
sellout_cust_source_table = "031_sellout_customers_int_matched"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_03"
target_table = "032_sellout_customers_ext_matched"

# COMMAND ----------

# %sql
# DROP TABLE d_dnst_020_silver.silver_03.`032_sellout_customers_ext_matched`

# COMMAND ----------

df_C4S = spark.table(f"{C4S_source_catalog}.{C4S_source_schema}.{C4S_source_table}")
df_selout_cust = spark.table(f"{sellout_cust_source_catalog}.{sellout_cust_source_schema}.{sellout_cust_source_table}").drop("value_dict")
# df_BPR = spark.table(f"{C4S_BPR_source_catalog}.{C4S_BPR_source_schema}.{C4S_BPR_source_table}")
df_c4c_business_partner_relationship = spark.table(f"{c4c_business_partner_relationship_catalog}.{c4c_business_partner_relationship_schema}.{c4c_business_partner_relationship_table}")

# COMMAND ----------

def get_matched_c4s_table():
    Matched_with_c4s = apply_matching(
    source=df_selout_cust,
    target=df_C4S,
    target_id_column="c4cu_account_id",
    source_id_column= "unique_id",
    id_matching_allowed = False,
    multiple_match_handler="multiple",
    match_rules=[
        # 1. Exact match on 'vat_number' (source) vs 'Vat_Reg_No' (target)
        {
            "match_type": "exact",
            "result_column": "ext_Tax_Number_4_match",
            "columns": [
                {
                    "source_column": "vat_number",
                    "target_column": "c4cu_vatregistration_id",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True 
                }
            ]
        },
        # 2. Exact match on 'vat_number' (source) vs 'Tax_Number_4' (target)
        {
            "match_type": "exact",
            "result_column": "ext_vat_number_match",
            "columns": [
                {
                    "source_column": "vat_number",
                    "target_column": "c4cu_taxnumber4_id",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True 
                }
            ]
        },
        # 3. Exact match on 'place_id'
        {
            "match_type": "exact",
            "result_column": "ext_place_id_match",
            "columns": [
                {
                    "source_column": "place_id",
                    "target_column": "place_id",
                }
            ]
        },
        # 4. Levenstein (fuzzy) match (already in the list format)
        {
            "match_type": "levenstein",
            "result_column": "ext_levenstein_name_address",
            "score_column": "ext_levenstein_score",
            "comparison_details_column": "ext_match_details",
            "min_score": 0.7, 
            "trim": True,
            "case_insensitive": True,
            "columns": [
                {
                    "source_column": "customer_name",
                    "target_column": "c4cu_account_desc",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True,
                    "weight": 0.5,
                    "match": "fuzzy"
                },
                {
                    "source_column": "customer_address",
                    "target_column": "c4cu_street_house_number",
                    # "target_column": "c4cu_streetaddress_id",
                    "make_case_insensitive": True,
                    "trim_spaces_before_and_after": True,
                    "weight": 0.5,
                    "match": "fuzzy"
                },
                {
                    "source_column": "customer_postal_code",
                    "target_column": "c4cu_postalcode_id",
                    "trim_spaces_before_and_after": True,
                    "match": "exact"
                }
            ]
        },
    ]
    ) 
    return Matched_with_c4s

# COMMAND ----------


def get_matched_bpr_table():
    Matched_with_BPR = (
    apply_matching(
    source = df_selout_cust,
    target = df_c4c_business_partner_relationship,
    target_id_column="c4cu_businesspartner_id",
    source_id_column= "unique_id",
    id_matching_allowed = False,
    multiple_match_handler="multiple",
    match_rules=[
        {
            "match_type": "exact",
            "result_column": "ext_BPR_match",
            "trim": True,
            "case_insensitive": True,
            "columns": [
                {"source_column": "customer_number", "target_column": "c4cu_numberatwholesaler_id"},
                {"source_column": "wholesaler_id", "target_column": "c4cu_businesspartner2_id"}
            ]
        }
    ]
    )
    )
    return Matched_with_BPR

# COMMAND ----------

window_spec = Window.partitionBy("dim_sellout_cust_id")

# COMMAND ----------

def get_fn_flowbase():
    df_joined = (
        get_matched_c4s_table().alias("C4S")
        .join(
            get_matched_bpr_table().alias("BPR"),
            on = F.expr("C4S.unique_id = BPR.unique_id"),
            how = "left"
        )
        .selectExpr(
            "C4S.*",
            "BPR.ext_BPR_match"
        )
        .withColumn("C4S_account_id",
            F.coalesce(
                F.col("ext_BPR_match"),
                F.col("ext_Tax_Number_4_match"),
                F.col("ext_vat_number_match"),
                F.col("ext_place_id_match"),
                F.col("ext_levenstein_name_address")
            )    
        )
        .withColumn("direct_c4S_account_id", F.col("C4S_account_id"))
        .withColumn("C4S_account_id", F.first("direct_c4S_account_id", ignorenulls=True).over(window_spec))
        .withColumn("unique_id", F.col("unique_id").cast("integer"))

    )

    distinct_counts = (
        df_joined.groupBy("dim_sellout_cust_id")
        .agg(F.countDistinct("C4S_account_id", "direct_c4S_account_id").alias("customer_number_Occurrence").cast("INT"))
    )

    df_with_occurrences = df_joined.join(distinct_counts, on="dim_sellout_cust_id", how="left")
             

    return df_with_occurrences

# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

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
               dim_sellout_cust_id STRING,
               ext_Tax_Number_4_match STRING,
               ext_vat_number_match STRING,
               ext_place_id_match STRING,
               ext_levenstein_name_address STRING,
               ext_BPR_match STRING,
               value_dict STRING,
               match_details STRING,
               ext_levenstein_score DOUBLE,
               ext_match_details STRING,
               direct_c4S_account_id STRING,
               C4S_account_id STRING,
               customer_number_Occurrence INT

            """,

            "dependencies": [
               f"{C4S_source_catalog}.{C4S_source_schema}.{C4S_source_table}",
               f"{sellout_cust_source_catalog}.{sellout_cust_source_schema}.{sellout_cust_source_table}",
               f"{c4c_business_partner_relationship_catalog}.{c4c_business_partner_relationship_schema}.{c4c_business_partner_relationship_table}",
               ],
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
