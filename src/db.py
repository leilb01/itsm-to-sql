import os
import logging
from dotenv import load_dotenv
from sqlmodel import SQLModel, Session, create_engine
from models import CMDB_CI, ContractRelCI
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()
conn_str = os.getenv("SQL_CONNECTION_STRING")
# encode for SQLAlchemy URL
encoded_conn_str = quote_plus(conn_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}", pool_pre_ping=True, fast_executemany=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

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