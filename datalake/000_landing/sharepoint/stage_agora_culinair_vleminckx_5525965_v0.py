# Databricks notebook source
# %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from landing_sharepoint import *
from landing_functions import *

# COMMAND ----------

#where metadata will be written
target_catalog = "d_dnst_000_landing"
target_schema = "metadata"
target_table ="Agora_Culinair_Vleminckx_5525965_V0_metadata"

# --- CONFIG ---
landing_root = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5525965___Agora_Culinair_Vleminckx___V0/"
sharepoint_folder = "Collection sell-out data/5525965 - Agora Culinair Vleminckx - V0"

config = {
    "file_pattern": "*.xlsx",           # Only Excel files
    "folder_name_skip": ['Old'],        # Skip these folders
    "sheet_name": None,                 # If set, only this sheet is processed
    "sheet_name_skip": [],      # Skip these sheets when processing all sheets
}

# COMMAND ----------

# --- SECRETS ---
client_id     = dbutils.secrets.get("keyvault-dp", "sharepoint--client-id")
client_secret = dbutils.secrets.get("keyvault-dp", "sharepoint--client-secret")
tenant_id     = dbutils.secrets.get("keyvault-dp", "sharepoint--tenant-id")

# COMMAND ----------

table_structure = StructType([
    StructField("source_path", StringType(), True),
    StructField("sheet_name", StringType(), True),
    StructField("output_path", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("column_count", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("runstarttime", TimestampType(), True)
])

# COMMAND ----------

def get_token():
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

token = get_token()
headers = {"Authorization": f"Bearer {token}"}

# COMMAND ----------

# ============================================================
# 2) GET SITE & DRIVE IDs
# ============================================================
site_url = "https://graph.microsoft.com/v1.0/sites/vandemoortele.sharepoint.com:/sites/combeluxprofsalesmgt"
site_id = requests.get(site_url, headers=headers).json()["id"]

drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
drive_id = [d["id"] for d in requests.get(drive_url, headers=headers).json()["value"] if d["name"] == "Business Intelligence"][0]

# COMMAND ----------

# ============================================================
# 3) LIST FILES RECURSIVELY (WITH FOLDER SKIP)
# ============================================================
def list_files(folder_path):
    enc = urllib.parse.quote(folder_path, safe="")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc}:/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    items = r.json()["value"]
    files = []
    for it in items:
        name = it.get("name")
        path = f"{folder_path}/{name}"
        if "file" in it and fnmatch.fnmatch(name, config["file_pattern"]):
            files.append(path)
        elif "folder" in it:
            # Skip folders in config
            if any(skip.lower() in name.lower() for skip in config["folder_name_skip"]):
                print(f"⏩ Skipping folder: {name}")
                continue
            files.extend(list_files(path))
    return files




# COMMAND ----------

# ============================================================
# 4) DOWNLOAD, CONVERT TO CSV, WRITE TO LANDING
# ============================================================

# def download_and_stage_excel_files(files_to_download, landing_root):
def download_and_stage_excel_files():
    files_to_download = list_files(sharepoint_folder)
    print(f"Found {len(files_to_download)} Excel files")
    metadata_rows = []
    run_start_time = time.strftime("%Y-%m-%d %H:%M:%S")  # Common timestamp for all rows

    for sp_path in files_to_download:
        try:
            # --- Download file from SharePoint ---
            enc = urllib.parse.quote(sp_path, safe="")
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc}:/content"
            r = requests.get(url, headers=headers, stream=True)
            r.raise_for_status()

            buf = BytesIO(r.content)
            xls = pd.ExcelFile(buf)
            filename_base = os.path.splitext(os.path.basename(sp_path))[0].replace(" ", "_").replace("-", "_")

            # --- Determine sheets to process ---
            if config["sheet_name"]:
                sheets_to_process = [config["sheet_name"]] if config["sheet_name"] in xls.sheet_names else []
            else:
                sheets_to_process = [s for s in xls.sheet_names if s not in (config["sheet_name_skip"] or [])]

            for sheet in sheets_to_process:
                df = pd.read_excel(xls, sheet_name=sheet)
                out_filename = f"{filename_base}_{sheet}.csv"
                # out_path = f"{landing_root}/{out_filename}"  # Logical path for Unity Catalog
                # Compute relative path (subfolders)
                relative_path = os.path.dirname(sp_path.replace(sharepoint_folder, "").lstrip("/"))  # e.g., '2023'
                out_dir = f"{landing_root}/{relative_path}"
                dbutils.fs.mkdirs(out_dir)  # Ensure nested folders exist
                out_path = f"{out_dir}/{out_filename}"

                # ✅ Write CSV using dbutils.fs.put (no /dbfs prefix!)
                csv_data = df.to_csv(index=False)
                dbutils.fs.put(out_path, csv_data, overwrite=True)

                # --- Collect metadata ---
                metadata_rows.append(Row(
                    source_path=sp_path,
                    sheet_name=sheet,
                    output_path=out_path,
                    row_count=len(df),
                    column_count=len(df.columns),
                    timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
                    runstarttime=run_start_time  # Add common run
                ))

                print(f"✅ Saved {out_path}")

        except Exception as e:
            print(f"❌ Error processing {sp_path}: {e}")

    df_metadata = spark.createDataFrame(metadata_rows)

    # Cast to match table_structure
    df_metadata = (
        df_metadata
        .withColumn("row_count", F.col("row_count").cast(IntegerType()))
        .withColumn("column_count", F.col("column_count").cast(IntegerType()))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("runstarttime", F.to_timestamp("runstarttime"))
        .withColumn(
            "output_path", 
            F.regexp_replace(F.regexp_replace("output_path", "//", "/"), " ", "_")
        )
    )
    return df_metadata

# COMMAND ----------

fn_flowbase = download_and_stage_excel_files
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name": target_table,
            "operation_type": "append",
            "table_structure": table_structure
            }

# COMMAND ----------

# download_and_stage_excel_files()
