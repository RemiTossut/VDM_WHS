# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5525668 - Spuntini Biebuyck - V0", 
        "column_mapping": {
            "wholesaler_name": F.lit("Spuntini_Biebuyck_5525668"),
            "wholesaler_id": F.lit("5525668"),
            "customer_address": "klstraat", 
            "customer_city": "klstad", 
            "customer_postal_code": ["klpostnr","klpost"],
            "customer_country": None,
            "customer_name": "klnaam",
            "owner_name": "klnaam_1",
            "customer_number": "klant",
            "product_number": ["kodelev","kodelev_vdm"],
            "article_name": "omschr",
            "EAN": None,
            "total_amount": "aantal",
            "UOM": None,
            "vat_number": "klantbtw",
            "date": "datum",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Spuntini_Biebuyck_5525668_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5525668___Spuntini_Biebuyck___V0/"

# COMMAND ----------

#dynamic wholesaler config
target_table = wholesaler_name.lower()
dependency_table_path = f"d_dnst_000_landing.metadata.{target_table}_metadata"

# COMMAND ----------

# general config:
base_volume_path= "/Volumes/d_dnst_000_landing/volumes/sellout_data"
bronze_base_table_path = "/Volumes/d_dnst_010_bronze"
target_catalog = "d_dnst_010_bronze"
target_schema = "sharepoint"

# COMMAND ----------

def get_fn_flowbase():
    return process_wholesaler_csv_files(spark, wholesaler_name, config, base_volume_path, bronze_base_table_path, golden_template_schema)

# COMMAND ----------

(
    get_fn_flowbase()
    .select("customer_city", "full_file_path", "customer_postal_code", "customer_address", "customer_name")
    .filter(F.col("full_file_path").contains("2023"))
    .filter(F.col("customer_city").isNull())
    .display()
)

# COMMAND ----------

fn_flowbase = get_fn_flowbase
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name": target_table,
            "operation_type": "overwrite",
            "dependencies": [
               dependency_table_path
               ],
            "table_structure":  create_spark_schema_from_dict(golden_template_schema)
            }
