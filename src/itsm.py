import os
import requests
import logging
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

token = os.getenv("ITSM_BEARER_TOKEN")
if not token:
    raise ValueError("❌ ITSM_BEARER_TOKEN not set in .env")

# check for ASCII-only token
try:
    token.encode("ascii")
except UnicodeEncodeError as e:
    raise ValueError(f"Token contains non-ASCII characters: {e}")

headers = {
    "Authorization": f"Bearer {token.strip()}",
    "Accept": "application/json",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def flatten_record(rec):
    """Flatten any dicts (with 'value') into plain strings."""
    for k, v in list(rec.items()):
        if isinstance(v, dict) and "value" in v:
            rec[k] = v["value"]
    return rec

def fetch_records(table, query, fields, limit=1000, max_records=None):
    """
    Generic function to fetch specific columns from ServiceNow with pagination.
    """
    url = f"https://dimensiondataservices.service-now.com/api/now/table/{table}"
    all_records = []
    offset = 0

    logging.info("Fetching from %s with query: %s", table, query)
    logging.debug("Fields requested: %s", fields)

    while True:
        params = {
            "sysparm_query": query,
            "sysparm_fields": fields,
            "sysparm_limit": limit,
            "sysparm_offset": offset,
        }
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("result", [])
        # 🔹 flatten dicts once here
        data = [flatten_record(r) for r in data]

        logging.info("Fetched %d records (offset %d) from %s", len(data), offset, table)

        if not data:
            break

        all_records.extend(data)

        if max_records and len(all_records) >= max_records:
            all_records = all_records[:max_records]
            break

        offset += limit
        time.sleep(0.2)  # polite pause between requests

    logging.info("Completed fetch from %s. Total records: %d", table, len(all_records))
    return all_records


def fetch_cmdb_ci(query, limit=1000, max_records=None):
    """Fetch cmdb_ci records with specific query (no default)."""
    fields = (
        "sys_id,name,u_customer_device_name,u_system_name,u_customer_integration_id,"
        "location,location.u_site_id,model_id,manufacturer,serial_number,u_category,"
        "category,sys_class_name,ip_address,u_status,operational_status,"
        "u_ci_external_system_id,mac_address,u_priority,company,sys_created_on,"
        "sys_updated_by,sys_updated_on,u_license_key,u_line_of_business,"
        "u_os_serial,u_rim_configuration,u_scope,u_service_properties"
    )
    return fetch_records("cmdb_ci", query, fields, limit, max_records)


def fetch_contract_rel_ci(query, limit=1000, max_records=None):
    """Fetch contract_rel_ci records with specific query (no default)."""
    fields = (
        "sys_id,ci_id,"
        "contract.name,contract.u_type,contract.u_service,contract.u_service_contract_number,"
        "u_start_date,u_end_date,u_sales_organization,u_contract_item_code,u_status,"
        "sys_updated_on,sys_updated_by,u_integration_action,sys_created_on,sys_created_by"
    )
    return fetch_records("contract_rel_ci", query, fields, limit, max_records)
