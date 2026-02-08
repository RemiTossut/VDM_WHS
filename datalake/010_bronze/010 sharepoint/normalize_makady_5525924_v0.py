# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5525924 - Makady - V0", 
        "column_mapping": {
            "wholesaler_name": F.lit("Makady_5525924"),
            "wholesaler_id": F.lit("5525924"),
            "customer_address": "addressstreet_1", 
            "customer_city": "addresslongcity", 
            "customer_postal_code": "addresszipcode",
            "customer_country": None,
            "customer_name": "custname",
            "owner_name": "unnamed_3",
            "customer_number": "customer",
            "product_number": "supplierreference",
            "article_name": ["descriptionnl","descriptionfr"],
            "EAN": "unnamed_21",
            "total_amount": "moduleqty",
            "UOM": "packagingmodulenl",
            # "vat_number": "vatnumber",
            # "vat_number": F.format_string("%.0f", F.col("vatnumber")),
            "vat_number": F.format_string("%.0f", F.col("vatnumber").cast("double")), # Cast to numeric for the formatter
            "date": "saledate",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Makady_5525924_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5525924___Makady___V0/"

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
