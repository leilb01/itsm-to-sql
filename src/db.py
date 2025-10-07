import os
import logging
from dotenv import load_dotenv
from sqlmodel import SQLModel, Session, create_engine
from models import CMDB_CI, ContractRelCI

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

# Create database engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}", echo=False, fast_executemany=True)

# Create tables if they don't exist
SQLModel.metadata.create_all(engine)

#Insert CMDB records 
def load_cmdb(records):
    with Session(engine) as session:
        objs = [CMDB_CI(**r) for r in records]
        session.add_all(objs)
        session.commit()
        logging.info(f"Inserted {len(objs)} records into cmdb_ci_staging.")

# Insert Contract records
def load_contract_rel_ci(records):
    with Session(engine) as session:
        objs = [ContractRelCI(**r) for r in records]
        session.add_all(objs)
        session.commit()
        logging.info(f"Inserted {len(objs)} records into contract_rel_ci_staging.")