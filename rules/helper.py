import re
import os
from datetime import datetime
from sqlalchemy import text
import pandas as pd
from .logger import setup_logger

logger = setup_logger("helper")


def get_rule_from_master(dq_rules_master, rule_id):
    # dq_rules_master is now a single consolidated DataFrame
    if 'RuleID' not in dq_rules_master.columns:
        logger.warning("⚠️ 'RuleID' column not found in consolidated master")
        return None
    
    # Find the rule
    matching_rules = dq_rules_master[dq_rules_master['RuleID'] == rule_id]
    
    if matching_rules.empty:
        logger.warning(f"⚠️ Rule ID '{rule_id}' not found in consolidated master")
        return None
    
    # Return the first matching rule (there should only be one)
    return matching_rules.iloc[0]


def get_metadata_id(metadata_type, metadata_value, engine):
    # Handle empty or NA values
    if not metadata_value or str(metadata_value).upper() in ['NA', 'NAN', 'NONE', '']:
        return None
    
    query = text("""
        SELECT metadata_id 
        FROM healthfirst_configdb.validation_rule_metadata
        WHERE UPPER(metadata_set) = UPPER(:metadata_type) 
        AND UPPER(metadata_value) = UPPER(:metadata_value)
    """)
    
    try:
        with engine.connect() as conn:
            result = pd.read_sql(query, conn, params={
                "metadata_type": metadata_type,
                "metadata_value": metadata_value
            })
        
        if not result.empty:
            return int(result.iloc[0, 0])
        else:
            logger.warning(f"⚠️ Metadata not found: {metadata_type} = {metadata_value}")
            return None
    except Exception as e:
        logger.warning(f"⚠️ Error getting metadata for {metadata_type}={metadata_value}: {e}")
        return None


def get_max_rule_id(engine, schema="healthfirst_configdb"):
    query = f"SELECT COALESCE(MAX(rule_id), 0) AS max_rule_id FROM {schema}.validation_rules"
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    
    return int(result["max_rule_id"].iloc[0])


def get_existing_rule_id(engine, business_rule_id, schema="healthfirst_configdb"):
    query = text(f"""
        SELECT rule_id 
        FROM {schema}.validation_rules 
        WHERE business_rule_id = :rule_id
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={"rule_id": business_rule_id})
    
    if not result.empty:
        return int(result["rule_id"].iloc[0])
    return None


def check_duplicate_rule(engine, business_rule_id, schema="healthfirst_configdb"):
    query = text(f"""
        SELECT rule_id, business_rule_id, rule_category_id, rule_category_desc,
               rule_name, rule_desc, rule_type_id, entity_type_id, range_type_id,
               min, max, regex_pattern, sql_query, batch_error_message,
               ui_error_message_summary, ui_field_error_message, endorsement_date,
               enabled, user_name, sub_entity_type_id, ingest_or_ui_id,
               enforcement_level_id, error_warning_type_id, dq_wkflw_ticket_ind
        FROM {schema}.validation_rules
        WHERE business_rule_id = :business_rule_id
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={"business_rule_id": business_rule_id})
    
    return result


def compare_rule_data(existing_row, new_row):
    fields_to_compare = [
        "business_rule_id", "rule_category_id", "rule_category_desc",
        "rule_name", "rule_desc", "rule_type_id", "entity_type_id",
        "range_type_id", "min", "max", "regex_pattern", "sql_query",
        "batch_error_message", "ui_error_message_summary", "ui_field_error_message",
        "endorsement_date", "enabled", "user_name", "sub_entity_type_id",
        "ingest_or_ui_id", "enforcement_level_id", "error_warning_type_id",
        "dq_wkflw_ticket_ind"
    ]
    
    for field in fields_to_compare:
        existing_value = existing_row.get(field)
        new_value = new_row.get(field)
        
        # Handle None/NaN comparisons
        if pd.isna(existing_value) and pd.isna(new_value):
            continue
        elif pd.isna(existing_value) or pd.isna(new_value):
            return False
        elif str(existing_value).strip().upper() != str(new_value).strip().upper():
            return False
    
    return True


def get_hrpdm_table_id(engine, table_name, zone, tenant):
    query = text(f"""
        SELECT table_id 
        FROM {tenant}_configdb.des_zone_table_list 
        WHERE UPPER(table_name) = :table_name 
        AND process_zone = :zone
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={
            "table_name": table_name.upper(),
            "zone": zone
        })
    
    if not result.empty:
        return int(result.iloc[0, 0])
    else:
        logger.warning(f"⚠️ Table {table_name} not found in {zone} zone for tenant {tenant}")
        return None


def get_entity_info(engine, entity_type, tenant):
    query = text(f"""
        SELECT pdm_entity_id, entity_key_field_name  
        FROM {tenant}_configdb.pdm_entity_master 
        WHERE UPPER(entity_name) = :entity_type
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={"entity_type": entity_type.upper()})
    
    if not result.empty:
        return int(result.iloc[0, 0]), result.iloc[0, 1]
    else:
        logger.warning(f"⚠️ Entity {entity_type} not found for tenant {tenant}")
        return None, None


def get_max_rule_extn_id(engine, tenant, source_owner=None):
    if source_owner:
        query = f"""
            SELECT COALESCE(MAX(rule_extn_id), 0) AS max_rule_id 
            FROM {tenant}_configdb.des_validation_rules_extn 
            WHERE UPPER(source_owner_name) = '{source_owner.upper()}'
        """
    else:
        query = f"""
            SELECT COALESCE(MAX(rule_extn_id), 0) AS max_rule_id 
            FROM {tenant}_configdb.des_validation_rules_extn
        """
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    
    return int(result["max_rule_id"].iloc[0])


def get_existing_rule_extn_id(engine, rule_id, source_owner, tenant):
    query = text(f"""
        SELECT rule_extn_id 
        FROM {tenant}_configdb.des_validation_rules_extn 
        WHERE rule_id = :rule_id 
        AND UPPER(source_owner_name) = :source_owner_name
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={
            "rule_id": rule_id,
            "source_owner_name": source_owner.upper()
        })
    
    if not result.empty:
        return int(result.iloc[0, 0])
    return None


def check_duplicate_config(engine, rule_id, source_owner, tenant):
    query = text(f"""
        SELECT rule_extn_id, rule_id, task_id, rule_applied_zone, hrpdm_table_id, 
               hrpdm_column_names, source_table_id, source_column_names, sql_query,
               active_flag, implmnt_type, implmnt_order, reference_codeset_id,
               entity_key, pdm_entity_id, source_owner_name
        FROM {tenant}_configdb.des_validation_rules_extn 
        WHERE rule_id = :rule_id 
        AND UPPER(source_owner_name) = :source_owner_name
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={
            "rule_id": rule_id,
            "source_owner_name": source_owner.upper()
        })
    
    return result


def compare_config_data(existing_row, new_row):
    fields_to_compare = [
        "rule_id", "task_id", "rule_applied_zone", "hrpdm_table_id",
        "hrpdm_column_names", "source_table_id", "source_column_names", 
        "sql_query", "active_flag", "implmnt_type", "implmnt_order",
        "reference_codeset_id", "entity_key", "pdm_entity_id", "source_owner_name"
    ]
    
    for field in fields_to_compare:
        existing_value = existing_row.get(field)
        new_value = new_row.get(field)
        
        # Handle None/NaN comparisons
        if pd.isna(existing_value) and pd.isna(new_value):
            continue
        elif pd.isna(existing_value) or pd.isna(new_value):
            return False
        elif str(existing_value).strip().upper() != str(new_value).strip().upper():
            return False
    
    return True


def get_source_table_id(engine, source_owner, tenant):
    query = text(f"""
        SELECT DISTINCT source_table_id 
        FROM {tenant}_configdb.des_validation_rules_extn 
        WHERE source_owner_name = :source_owner_name
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={"source_owner_name": source_owner.upper()})
    
    if not result.empty:
        return ','.join(result.iloc[:, 0].astype(str))
    return None


def extract_column_value(row, *possible_keys):
    for key in possible_keys:
        if key in row.index and pd.notna(row.get(key)):
            return str(row.get(key)).strip()
    return ""


def standardize_entity_type(entity_type):
    entity_type = str(entity_type).strip().upper().replace(" ", "_")
    
    entity_type_map = {
        "LOCATION_ORGANIZATION": "ORGANIZATION_LOCATION"
    }
    
    return entity_type_map.get(entity_type, entity_type)


def standardize_sub_entity(sub_entity):
    sub_entity = str(sub_entity).strip().upper()
    
    sub_entity_map = {
        "LOCATION ORGANIZATION": "ORGANIZATION AND LOCATION ORGANIZATION"
    }
    
    return sub_entity_map.get(sub_entity, sub_entity)


def get_version_info(base_dir: str):
    year_digit = 2 if datetime.now().year < 2026 else 3
    month_digit = datetime.now().month
    version_pattern = re.compile(r"load_validation_rules_data_ver_(\d+)_(\d+)_(\d+)\.(csv|xml)$")

    versions = []
    for f in os.listdir(base_dir):
        m = version_pattern.match(f)
        if m:
            year, month, ver = map(int, m.groups()[:3])
            versions.append((year, month, ver))

    if not versions:
        return f"{year_digit}_{month_digit}_1"

    latest = max(versions)
    next_seq = latest[2] + 1 if (latest[0] == year_digit and latest[1] == month_digit) else 1
    return f"{year_digit}_{month_digit}_{next_seq}"


def get_version_info_extn(base_dir: str):
    year_digit = 2 if datetime.now().year < 2026 else 3
    month_digit = datetime.now().month
    version_pattern = re.compile(r"load_des_validation_rules_extn_data_ver_(\d+)_(\d+)_(\d+)\.(csv|xml)$")

    versions = []
    for f in os.listdir(base_dir):
        m = version_pattern.match(f)
        if m:
            year, month, ver = map(int, m.groups()[:3])
            versions.append((year, month, ver))

    if not versions:
        return f"{year_digit}_{month_digit}_1"

    latest = max(versions)
    next_seq = latest[2] + 1 if (latest[0] == year_digit and latest[1] == month_digit) else 1
    return f"{year_digit}_{month_digit}_{next_seq}"


def get_hrp_source_table_id(engine, tenant, entity_type, hepdm_table):
    query = f"""select source_table_id from
        {tenant}_configdb.cdi_mapping_table cmt
        join
       {tenant}_configdb.cdi_definition cd
        on cmt.ingestion_id= cd.ingestion_id
        where upper(hrpdm_table_name) like '%{hepdm_table}%'
        and cd.source_name ='HRP'and  upper(entity_name) = '{entity_type}'
        and q"""
    return pd.read_sql(query, engine)

