import os
import pyodbc
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
conn_str = os.getenv("SQL_CONNECTION_STRING")
if not conn_str:
    raise ValueError("SQL_CONNECTION_STRING not set in .env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def execute_batch_insert(records, table, columns, value_getter, batch_size=1000):
    rows = [value_getter(r) for r in records]

    if not rows:
        logging.warning("No records to insert into %s", table)
        return

    logging.info("Starting insert into %s. Total records: %d", table, len(rows))

    placeholders = ", ".join("?" for _ in columns)
    col_list = ", ".join(columns + ["load_time"])

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders}, GETDATE())
    """

    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                cursor.executemany(sql, batch)
                conn.commit()
                logging.info(
                    "Inserted %d rows into %s (progress: %d/%d)",
                    len(batch), table, i + len(batch), len(rows)
                )

    logging.info("Completed insert into %s", table)


def insert_cmdb_ci(records, batch_size=1000):
    return execute_batch_insert(
        records=records,
        table="dbo.cmdb_ci_staging",
        columns=[
            "sys_id", "name", "u_customer_device_name", "u_system_name",
            "u_customer_integration_id", "location", "location_u_site_id",
            "model_id", "manufacturer", "serial_number", "u_category", "category",
            "sys_class_name", "ip_address", "u_status", "operational_status",
            "u_ci_external_system_id", "mac_address", "u_priority", "company",
            "sys_created_on", "sys_updated_by", "sys_updated_on", "u_license_key",
            "u_line_of_business", "u_os_serial", "u_rim_configuration", "u_scope",
            "u_service_properties",
        ],
        value_getter=lambda r: (
            r.get("sys_id"), r.get("name"), r.get("u_customer_device_name"),
            r.get("u_system_name"), r.get("u_customer_integration_id"),
            r.get("location"), r.get("location.u_site_id"), r.get("model_id"),
            r.get("manufacturer"), r.get("serial_number"), r.get("u_category"),
            r.get("category"), r.get("sys_class_name"), r.get("ip_address"),
            r.get("u_status"), r.get("operational_status"),
            r.get("u_ci_external_system_id"), r.get("mac_address"),
            r.get("u_priority"), r.get("company"), r.get("sys_created_on"),
            r.get("sys_updated_by"), r.get("sys_updated_on"),
            r.get("u_license_key"), r.get("u_line_of_business"),
            r.get("u_os_serial"), r.get("u_rim_configuration"), r.get("u_scope"),
            r.get("u_service_properties"),
        ),
        batch_size=batch_size,
    )


def insert_contract_rel_ci(records, batch_size=1000):
    return execute_batch_insert(
        records=records,
        table="dbo.contract_rel_ci_staging",
        columns=[
            "sys_id", "ci_id", "contract_name", "contract_u_type",
            "contract_u_service", "contract_u_service_contract_number",
            "u_start_date", "u_end_date", "u_sales_organization",
            "u_contract_item_code", "u_status", "sys_updated_on",
            "sys_updated_by", "u_integration_action", "sys_created_on",
            "sys_created_by",
        ],
        value_getter=lambda r: (
            r.get("sys_id"), r.get("ci_id"), r.get("contract.name"),
            r.get("contract.u_type"), r.get("contract.u_service"),
            r.get("contract.u_service_contract_number"), r.get("u_start_date"),
            r.get("u_end_date"), r.get("u_sales_organization"),
            r.get("u_contract_item_code"), r.get("u_status"),
            r.get("sys_updated_on"), r.get("sys_updated_by"),
            r.get("u_integration_action"), r.get("sys_created_on"),
            r.get("sys_created_by"),
        ),
        batch_size=batch_size,
    )
