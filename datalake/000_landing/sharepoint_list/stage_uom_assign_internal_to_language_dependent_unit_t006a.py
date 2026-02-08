# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from api_handler import get_request, post_request
from imports import *

# COMMAND ----------

target_catalog = "d_dnst_000_landing"
target_schema = "metadata"
target_table ="staging_uom_assign_internal_to_language_dependent_unit_t006a"

# --- CONFIG ---
landing_path = "/Volumes/d_dnst_000_landing/volumes/sharepoint_lists/uom_assign_internal_to_language_dependent_unit_t006a.csv"

# COMMAND ----------

# --- SECRETS ---
client_id     = dbutils.secrets.get("keyvault-dp", "sharepoint--client-id")
client_secret = dbutils.secrets.get("keyvault-dp", "sharepoint--client-secret")
tenant_id     = dbutils.secrets.get("keyvault-dp", "sharepoint--tenant-id")

# COMMAND ----------

site_domain = 'vandemoortele.sharepoint.com'
# site_name = 'combeluxprofsalesmgt'
site_name = "PR-DigitalTransformation"
list_name = 'uom_assign_internal_to_language_dependent_unit_t006a'

# COMMAND ----------

token_url = "https://graph.microsoft.com/.default"
sites_url = f"https://graph.microsoft.com/v1.0/sites/{site_domain}:/sites/{site_name}"
lists_url = "https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
list_items_url = "https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=fields"
list_columns_url = "https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"

# COMMAND ----------

table_structure = StructType([StructField('url', StringType(), True),
                             StructField('payload', StringType(), True),
                             StructField('status_code', IntegerType(), True), 
                             StructField('request_time', TimestampType(), True),
                             StructField('request_duration', FloatType(), True),
                             StructField('error', StringType(), True)])

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

def stage_sharepoint():
    # create response queue
    response_queue = queue.Queue()
    try:
        # get token
        # token = get_token()
        access_token = get_token()
        token_header = {"Authorization": f"Bearer {access_token}"}

        # get site id
        response = get_request(url=sites_url, headers=token_header)
        site_id = json.loads(response["payload"])["id"]

        # get list id
        response = get_request(url=lists_url.format(site_id=site_id), headers=token_header)
        response_queue.put(response)
        pdf_lists = pd.DataFrame(json.loads(response["payload"])["value"])
        list_id = pdf_lists[pdf_lists["name"]==list_name]["id"].values[0]
        print(f"list id: {list_id}")

        # get column names from list
        response = get_request(url=list_columns_url.format(site_id=site_id,list_id=list_id), headers=token_header)
        response_queue.put(response)
        columns = json.loads(response["payload"])["value"]
        column_mapping = {column['displayName']: column['name'] for column in columns}
        column_mapping_sorted = dict(sorted(column_mapping.items()))
        print(column_mapping_sorted)

        # get items
        response = get_request(url=list_items_url.format(site_id=site_id,list_id=list_id), headers=token_header)
        response_queue.put(response)
        items = [item["fields"] for item in json.loads(response["payload"])["value"]]

        next_link = json.loads(response["payload"]).get("@odata.nextLink", None)
        
        while next_link is not None:
            print(next_link)
            response = get_request(url=next_link, headers=token_header)
            response_queue.put(response)
            items.extend([item["fields"] for item in json.loads(response["payload"])["value"]])
            next_link = json.loads(response["payload"]).get("@odata.nextLink", None)

        # normalize fields: ensure all expected keys are present
        normalized_items = []
        
        for item in items:
            normalized_item = {key: item.get(value, None) for key, value in column_mapping_sorted.items()}
            normalized_items.append(normalized_item)

        # download file
        pd.DataFrame(normalized_items).to_csv(landing_path, index=False)

    except Exception as e:
        response =  {
            "url": None, 
            "payload": None, 
            "status_code": -1,
            "request_time": None,
            "request_duration": None,
            "error": str(e)
            }
        response_queue.put(response)
        print(e)

    # collect requests
    df_requests = spark.createDataFrame(list(response_queue.queue), schema = table_structure)
    return df_requests

# COMMAND ----------

fn_flowbase = stage_sharepoint
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name": target_table,
            "operation_type": "append",
            "table_structure": table_structure
            }
