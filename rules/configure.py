import pandas as pd
from sqlalchemy import text
from .helper import (
    get_rule_from_master,
    get_max_rule_extn_id,
    get_existing_rule_extn_id,
    get_hrpdm_table_id,
    get_entity_info,
    get_source_table_id,
    check_duplicate_config,
    compare_config_data,
    extract_column_value
)
from .logger import setup_logger, log_separator

logger = setup_logger("configure")


def prepare_configure_rules(dq_rules_master, config_df, tenant, engine):
    logger.info("")
    log_separator(logger, "=", 60)
    logger.info(f"PREPARING CONFIGURE RULES FOR TENANT: {tenant.upper()}")
    log_separator(logger, "=", 60)
    
    # Get initial max IDs from database
    max_hrp_id = get_max_rule_extn_id(engine, tenant, source_owner="HRP")
    max_overall_id = get_max_rule_extn_id(engine, tenant)
    
    logger.info(f"📊 Max HRP rule_extn_id: {max_hrp_id}")
    logger.info(f"📊 Max overall rule_extn_id: {max_overall_id}")
    
    rows = []
    
    for idx, config in config_df.iterrows():
        rule_id = config["ruleid"]
        source_owner = config.get("sourceownername", "").upper()
        zone = config.get("zoneapplied", "").upper()
        
        logger.info(f"\n🔄 Processing Configuration #{idx + 1}: {rule_id}")
        logger.info(f"   Tenant: {tenant} | Zone: {zone} | Source: {source_owner}")
        logger.info("-" * 60)
        
        # Step 1: Get rule_id from validation_rules table
        db_rule_id = _get_rule_id_from_db(engine, rule_id, tenant)
        if db_rule_id is None:
            logger.warning(f"⚠️  Rule '{rule_id}' not found in {tenant} validation_rules. Skipping.")
            continue
        
        # Step 2: Get the rule from consolidated master
        master_row = get_rule_from_master(dq_rules_master, rule_id)
        
        if master_row is None:
            logger.warning(f"⚠️  Rule ID '{rule_id}' not found in consolidated master. Skipping.")
            continue
        
        # Log the source sheet for reference
        source_sheet = master_row.get('SourceSheet', 'Unknown')
        logger.info(f"✓ Found in sheet: '{source_sheet}'")
        
        # Step 4: Determine rule_extn_id (new or existing)
        existing_extn_id = get_existing_rule_extn_id(engine, db_rule_id, source_owner, tenant)
        
        if existing_extn_id:
            rule_extn_id = existing_extn_id
            logger.info(f"✓ Using existing rule_extn_id: {rule_extn_id}")
        else:
            # Assign new ID based on source owner
            if source_owner == 'HRP':
                max_hrp_id += 1
                rule_extn_id = max_hrp_id
                max_overall_id = max(max_overall_id, rule_extn_id)
                logger.info(f"✓ Assigned new HRP rule_extn_id: {rule_extn_id}")
            else:
                max_overall_id += 1
                rule_extn_id = max_overall_id
                logger.info(f"✓ Assigned new rule_extn_id: {rule_extn_id}")
        
        # Step 5: Extract configuration data
        config_data = _extract_config_data(
            master_row, 
            config, 
            db_rule_id, 
            rule_extn_id,
            zone,
            source_owner,
            tenant, 
            engine
        )
        
        if config_data is None:
            logger.warning(f"⚠️  Could not extract configuration data. Skipping.")
            continue
        
        # Step 6: Check for duplicates
        if _is_duplicate(engine, db_rule_id, source_owner, tenant, config_data):
            logger.warning(f"⚠️  Configuration already exists with identical data. Skipping.")
            continue
        
        logger.info(f"✓ Configuration data prepared for processing")
        rows.append(config_data)
    
    logger.info("")
    log_separator(logger, "=", 60)
    logger.info(f"✅ Prepared {len(rows)} configuration(s) for tenant {tenant}")
    log_separator(logger, "=", 60)
    logger.info("")
    
    return pd.DataFrame(rows)


def _get_rule_id_from_db(engine, business_rule_id, tenant):
    query = text(f"""
        SELECT rule_id 
        FROM {tenant}_configdb.validation_rules 
        WHERE business_rule_id = :rule_id
    """)
    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={"rule_id": business_rule_id})
    
    if not result.empty:
        return int(result.iloc[0, 0])
    return None


def _extract_config_data(master_row, config, rule_id, rule_extn_id, zone, source_owner, tenant, engine):
    # Get table information - use standardized column names
    target_table = extract_column_value(master_row, "TableName", "Table Name").upper()
    hrpdm_table_id = get_hrpdm_table_id(engine, target_table, zone, tenant)
    
    if hrpdm_table_id is None:
        return None
    
    # Get entity information
    entity_type = extract_column_value(master_row, "Entity").upper()
    pdm_entity_id, entity_key = get_entity_info(engine, entity_type, tenant)
    
    if pdm_entity_id is None or entity_key is None:
        return None
    
    # Get source table ID (only for non-HRP)
    if source_owner == "HRP":
        source_table_id = None
    else:
        source_table_id = get_source_table_id(engine, source_owner, tenant)
    
    # Get column names - use standardized column names
    hrpdm_column_names = extract_column_value(
        master_row,
        "ColumnName",
        "Column Name"
    ).upper()
    
    # Build complete configuration data
    return {
        "rule_extn_id": rule_extn_id,
        "rule_id": rule_id,
        "task_id": None,
        "rule_applied_zone": zone,
        "hrpdm_table_id": hrpdm_table_id,
        "hrpdm_column_names": hrpdm_column_names,
        "source_table_id": source_table_id,
        "source_column_names": None,
        "sql_query": None,
        "active_flag": "Y",
        "implmnt_type": None,
        "implmnt_order": None,
        "reference_codeset_id": None,
        "entity_key": entity_key,
        "pdm_entity_id": pdm_entity_id,
        "source_owner_name": source_owner
    }


def _is_duplicate(engine, rule_id, source_owner, tenant, new_config_data):
    existing = check_duplicate_config(engine, rule_id, source_owner, tenant)
    
    if existing.empty:
        return False
    
    existing_row = existing.iloc[0]
    return compare_config_data(existing_row, new_config_data)

