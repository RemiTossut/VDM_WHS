# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from commenting_module import *

# COMMAND ----------

source_catalog = "d_dp_000_landing"
source_schema = "volumes"
source_path = "sap_bw/material_unit_of_measure_conversion"
full_path = f"/Volumes/{source_catalog}/{source_schema}/{source_path}"
# "/Volumes/d_dnst_000_landing/volumes/manual_dump/C4S/5_08_2025/accounts/"

target_catalog = "d_dnst_010_bronze"
target_schema = "sap_bw"
target_table = "material_unit_of_measure_conversion"

# COMMAND ----------

df = spark.read.format('parquet').load(full_path)

# COMMAND ----------

column_comments = [
    {
        "col": "muom_material_id",
        "type": "STRING",
        "comment": """The material number in SAP ECC R3
MARM.MATNR in SAP ECC R3"""
    },
    {
        "col": "muom_unit_id",
        "type": "STRING",
        "comment": """Alternative Unit Of Measure,
MARM.MEINH in SAP ECC R3"""
    },
    {
        "col": "muom_denominator_qty",
        "type": "DECIMAL(38,18)",
        "comment": """This is the denominator of the conversion factor which can be used to convert quantities expressed in the 'muom_unit_id' into the 'Base Unit of Measure'.
This is the MARM.UMREN 
(In Dutch "de noemer")"""
    },
    {
        "col": "muom_numerator_qty",
        "type": "DECIMAL(38,18)",
        "comment": """This is the numerator of the conversion factor which can be used to convert quantities expressed in the 'muom_unit_id' into the 'Base Unit of Measure'.
This is the MARM.UMREZ 
(In Dutch "de teller")"""
    },
    {
        "col": "muom_eanupc_id",
        "type": "STRING",
        "comment": """This field indicates the EAN/GTIN code per level of article (CU/Carton/Pallet).
MARM.EAN11

It is expected to be 13 characters long, with no leading zeros

For Vandemoortele & Croustico products, the barcodes are determined by PMD. The generation of the new barcodes is done in Excel files, managed by PMD, ensuring that the correct structure is maintained.
For Private Label products, customers can provide their own barcodes. These are then communicated to PMD by our commercial colleagues via SAP CRM.

PMD stands for our 'Product Master Data'-department"""
    },
]

# COMMAND ----------

add_column_hook_code = get_add_comment_hook_code(
    target_catalog,
    target_schema,
    target_table,
    column_metadata=column_comments
)

# COMMAND ----------

table_description = """The table contains data related to the conversion of material units of measure.It originates from SAP ECC R3, the 'Unit of Measure for Material'-table (MARM).It allows to convert alternative Unit of Measures to the so-called Base Unit of Measure in SAP ECC. The Base Unit of Measure is the unit in which the material is defined in SAP ECC R3.The table includes information on material IDs, unit IDs, and the quantities for both the numerator and denominator in the conversion process. This data can be used for analyzing and managing inventory, ensuring accurate measurements in supply chain operations, and facilitating reporting on material usage across different units."""

# COMMAND ----------

add_table_description_hook_code = get_add_table_description_hook_code(
    target_catalog,
    target_schema,
    target_table,
    table_description = table_description
)

# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

# COMMAND ----------

df_flowbase = df
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            # "dependencies": [],
            "deploy_strategy": "column",
            "operation_hooks": [
                {
                    "id": "add_column_comments",
                    "code_language": "python",
                    "trigger_timing": "post",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": add_column_hook_code
                },
                {
                    "id": "add_table_description",
                    "code_language": "python",
                    "trigger_timing": "post",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": add_table_description_hook_code
                },
                {
                    "id": "set columnMapping true",
                    "code_language": "sql",
                    "trigger_timing": "pre",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": enable_columnmapping_sql_code
                }
            ]
            }
