import os
import logging
from itsm import fetch_cmdb_ci, fetch_contract_rel_ci
from db import insert_cmdb_ci, insert_contract_rel_ci

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def main():
    mode = os.getenv("LOAD_MODE", "monthly").lower()
    logging.info("Starting ETL process in %s mode...", mode)

    if mode == "monthly":
        cmdb_query = os.getenv("CMDB_QUERY_MONTHLY")
        contract_query = os.getenv("CONTRACT_QUERY_MONTHLY")
    elif mode == "weekly":
        cmdb_query = os.getenv("CMDB_QUERY_WEEKLY")
        contract_query = os.getenv("CONTRACT_QUERY_WEEKLY")
    else:
        raise ValueError("Invalid LOAD_MODE. Use 'monthly' or 'weekly'.")

    try:
        logging.info("Fetching cmdb_ci with query: %s", cmdb_query)
        cmdb_records = fetch_cmdb_ci(query=cmdb_query, limit=1000)
        logging.info("Fetched %d cmdb_ci records", len(cmdb_records))
        insert_cmdb_ci(cmdb_records)
    except Exception as e:
        logging.error("Error in cmdb_ci pipeline: %s", e, exc_info=True)

    # try:
    #     logging.info("Fetching contract_rel_ci with query: %s", contract_query)
    #     contract_records = fetch_contract_rel_ci(query=contract_query, limit=1000)
    #     logging.info("Fetched %d contract_rel_ci records", len(contract_records))
    #     insert_contract_rel_ci(contract_records)
    # except Exception as e:
    #     logging.error("Error in contract_rel_ci pipeline: %s", e, exc_info=True)

    logging.info("ETL process completed.")


if __name__ == "__main__":
    main()
