from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

# CMDB_CI staging model
class CMDB_CI(SQLModel, table=True):
    __tablename__ = "cmdb_ci_staging"

    sys_id: str = Field(primary_key=True)
    name: Optional[str] = None
    u_customer_device_name: Optional[str] = None
    u_system_name: Optional[str] = None
    u_customer_integration_id: Optional[str] = None
    location_u_site_id: Optional[str] = None
    model_id: Optional[str] = None
    manufacturer: Optional[str] = None
    serial_number: Optional[str] = None
    u_category: Optional[str] = None
    category: Optional[str] = None
    sys_class_name: Optional[str] = None
    ip_address: Optional[str] = None
    u_status: Optional[str] = None
    operational_status: Optional[str] = None
    u_ci_external_system_id: Optional[str] = None
    mac_address: Optional[str] = None
    u_priority: Optional[str] = None
    company: Optional[str] = None
    sys_created_on: Optional[str] = None
    sys_updated_by: Optional[str] = None
    sys_updated_on: Optional[str] = None
    u_license_key: Optional[str] = None
    u_line_of_business: Optional[str] = None
    u_os_serial: Optional[str] = None
    u_rim_configuration: Optional[str] = None
    u_service_properties: Optional[str] = None
    u_line_of_business: Optional[str] = None
    u_service_properties: Optional[str] = None

# Contract_REL_CI staging model
class ContractRelCI(SQLModel, table=True):
    __tablename__ = "contract_rel_ci_staging"

    sys_id: str = Field(primary_key=True)
    ci_id: Optional[str] = None
    contract_name: Optional[str] = None
    contract_u_type: Optional[str] = None
    contract_u_service: Optional[str] = None
    contract_u_service_contract_number: Optional[str] = None
    u_start_date: Optional[str] = None
    u_end_date: Optional[str] = None
    u_sales_organization: Optional[str] = None
    u_contract_item_code: Optional[str] = None
    u_status: Optional[str] = None
    sys_updated_on: Optional[str] = None
    sys_updated_by: Optional[str] = None
    u_integration_action: Optional[str] = None
    sys_created_on: Optional[str] = None
    sys_created_by: Optional[str] = None