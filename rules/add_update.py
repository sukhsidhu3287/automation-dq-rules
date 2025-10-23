import pandas as pd
from .helper import (
    get_rule_from_master,
    get_metadata_id,
    get_max_rule_id,
    get_existing_rule_id,
    check_duplicate_rule,
    compare_rule_data,
    extract_column_value,
    standardize_entity_type,
    standardize_sub_entity
)
from .logger import setup_logger, log_separator

logger = setup_logger("add_update")


def prepare_add_update_rules(dq_rules_master, rules_df, engine):
    logger.info("")
    log_separator(logger, "=", 60)
    logger.info("PREPARING ADD/UPDATE RULES")
    log_separator(logger, "=", 60)
    
    # Get current max rule_id from database
    max_rule_id = get_max_rule_id(engine)
    logger.info(f"📊 Current max rule_id in database: {max_rule_id}")
    
    rows = []
    
    for idx, rule in rules_df.iterrows():
        rule_id = rule["ruleid"]
        logger.info(f"\n🔄 Processing Rule #{idx + 1}: {rule_id}")
        logger.info("-" * 60)
        
        # Step 1: Get the rule from consolidated master
        master_row = get_rule_from_master(dq_rules_master, rule_id)
        
        if master_row is None:
            logger.warning(f"⚠️  Rule ID '{rule_id}' not found in consolidated master. Skipping.")
            continue
        
        # Log the source sheet for reference
        source_sheet = master_row.get('SourceSheet', 'Unknown')
        logger.info(f"✓ Found in sheet: '{source_sheet}'")
        
        # Step 3: Extract and transform rule data
        rule_data = _extract_rule_data(master_row, rule, engine)
        
        # Step 4: Determine rule_id (new or existing)
        existing_rule_id = get_existing_rule_id(engine, rule_id)
        
        if existing_rule_id:
            rule_data["rule_id"] = existing_rule_id
            logger.info(f"✓ Using existing rule_id: {existing_rule_id}")
        else:
            max_rule_id += 1
            rule_data["rule_id"] = max_rule_id
            logger.info(f"✓ Assigned new rule_id: {max_rule_id}")
        
        # Step 5: Check for duplicates
        if _is_duplicate(engine, rule_id, rule_data):
            logger.warning(f"⚠️  Rule already exists with identical data. Skipping.")
            continue
        
        logger.info(f"✓ Rule data prepared for processing")
        rows.append(rule_data)
    
    logger.info("")
    log_separator(logger, "=", 60)
    logger.info(f"✅ Prepared {len(rows)} rule(s) for add/update")
    log_separator(logger, "=", 60)
    logger.info("")
    
    return pd.DataFrame(rows)


def _extract_rule_data(master_row, rule, engine):
    # Rule Category - now standardized to RuleType
    rule_category_raw = extract_column_value(master_row, "RuleType", "Rule Type", "Rule Category")
    rule_category = rule_category_raw.upper().replace(" ", "_")
    rule_category_id = get_metadata_id("Rule Category", rule_category, engine)
    
    # Rule Type (from UI)
    rule_type = str(rule.get("ruletype", "")).upper().strip()
    rule_type_id = get_metadata_id("Rule Type", rule_type, engine)
    
    # Entity Type
    entity_type_raw = extract_column_value(master_row, "Entity")
    entity_type = standardize_entity_type(entity_type_raw)
    entity_type_id = get_metadata_id("Entity Type", entity_type, engine)
    
    # Sub Entity - now standardized to SubEntity
    sub_entity_raw = extract_column_value(master_row, "SubEntity", "Sub Entity", "Sub-Entity")
    sub_entity = standardize_sub_entity(sub_entity_raw)
    sub_entity_id = get_metadata_id("Sub_Entity_Type", sub_entity, engine)
    
    # Ingest/UI - now standardized to IngestUIOnly
    ingest_or_ui = extract_column_value(
        master_row,
        "IngestUIOnly",
        "Ingest+UI/UI Only",
        "Ingest+UI/Ui Only"
    ).upper()
    ingest_or_ui_id = get_metadata_id("Ingest_Or_UI", ingest_or_ui, engine)
    
    # Enforcement Level
    enforcement_level = extract_column_value(master_row, "Enforcement Level").upper()
    enforcement_level_id = None
    error_warning_type_id = None
    
    if enforcement_level and enforcement_level != "NA":
        enforcement_level_id = get_metadata_id("Enforcement_Level", enforcement_level, engine)
        if enforcement_level_id == 28:
            error_warning_type_id = 31
        else:
            error_warning_type_id = 32
    
    # Build complete rule data - use standardized column names
    return {
        "rule_id": None,  # Will be set later
        "business_rule_id": rule["ruleid"],
        "rule_category_id": rule_category_id,
        "rule_category_desc": rule_category,
        "rule_name": master_row.get("RuleName") or master_row.get("Rule Name", ""),
        "rule_desc": master_row.get("RuleDescription") or master_row.get("Rule Description", ""),
        "rule_type_id": rule_type_id,
        "entity_type_id": entity_type_id,
        "range_type_id": "",
        "min": "",
        "max": "",
        "regex_pattern": "",
        "sql_query": "",
        "batch_error_message": master_row.get("Interface( Batch, API) ingestion Error Message", ""),
        "ui_error_message_summary": master_row.get("UI Error Message Summary", ""),
        "ui_field_error_message": master_row.get("UI Error Message Under Field", ""),
        "endorsement_date": master_row.get("Date Updated", ""),
        "enabled": "Y",
        "user_name": "SYSTEM",
        "sub_entity_type_id": sub_entity_id,
        "ingest_or_ui_id": ingest_or_ui_id,
        "enforcement_level_id": enforcement_level_id,
        "error_warning_type_id": error_warning_type_id,
        "dq_wkflw_ticket_ind": "TRUE"
    }


def _is_duplicate(engine, business_rule_id, new_rule_data):
    existing = check_duplicate_rule(engine, business_rule_id)
    
    if existing.empty:
        return False
    
    existing_row = existing.iloc[0]
    return compare_rule_data(existing_row, new_rule_data)

