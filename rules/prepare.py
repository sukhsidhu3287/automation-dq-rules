import re
import pandas as pd
from .metadata import get_metadata_id

def prepare_rules(dq_rules_master, add_rules_df, engine, sheet_map):
    max_id_query = f"SELECT COALESCE(MAX(rule_id), 0) AS max_rule_id FROM healthfirst_configdb.validation_rules"
    max_id_df = pd.read_sql(max_id_query, engine)
    next_rule_id = int(max_id_df["max_rule_id"].iloc[0]) + 1

    rows = []
    for _, rule in add_rules_df.iterrows():
        sheet_name = sheet_map.get(rule["entity"], None)
        if sheet_name not in dq_rules_master:
            print(f"⚠️ Sheet '{sheet_name}' not found. Skipping {rule.get('ruleid')}.")
            continue

        sheet_df = dq_rules_master[sheet_name]
        rule_row = sheet_df[sheet_df["RuleID"] == rule["ruleid"]]
        if rule_row.empty:
            print(f"⚠️ RuleID {rule['ruleid']} not found in '{sheet_name}'. Skipping.")
            continue

        rule_row = rule_row.iloc[0]

        # Rule Category
        rule_category = str(rule_row.get("Rule Category") or rule_row.get("Rule Type", "")) \
                        .upper().strip().replace(" ", "_")
        rule_category_id = get_metadata_id("Rule Category", rule_category, engine)

        # Entity Type
        entity_type = str(rule_row.get("Entity", "")).strip().upper()
        entity_type_id = get_metadata_id("Entity Type", entity_type, engine)

        # Sub Entity
        sub_entity = str(rule_row.get("Sub Entity", "")).strip().upper()
        sub_entity_id = get_metadata_id("Sub_Entity_Type", sub_entity, engine)

        # Ingest/UI
        ingest_map = {"Ingest+UI": "BOTH", "UI Only": "UI", "Ingest only": "INGEST"}
        ingest_val = str(rule_row.get("Ingest+UI/UI Only ", "")).strip()
        ingest_or_ui = ingest_map.get(ingest_val, ingest_val.upper())
        ingest_or_ui_id = get_metadata_id("Ingest_Or_UI", ingest_or_ui, engine)

        # Enforcement
        enforcement_level = str(rule_row.get("Enforcement Level", "")).upper()
        enforcement_level_id = None if enforcement_level == "NA" else \
            get_metadata_id("Enforcement_Level", enforcement_level, engine)

        enabled_flag = "Y" # yes for now for all

        rows.append({
            "rule_id": next_rule_id,
            "business_rule_id": rule_row.get("RuleID"),
            "rule_category_id": rule_category_id,
            "rule_category_desc": rule_row.get("Rule Type", ""),
            "rule_name": rule_row.get("Rule Name", ""),
            "rule_desc": rule_row.get("Rule Description", ""),
            "rule_type_id": None,
            "entity_type_id": entity_type_id,
            "range_type_id": "",
            "min": "",
            "max": "",
            "regex_pattern": "",
            "sql_query": "",
            "batch_error_message": rule_row.get("Interface( Batch, API) ingestion Error Message", ""),
            "ui_error_message_summary": rule_row.get("UI Error Message Summary", ""),
            "ui_field_error_message": rule_row.get("UI Error Message Under Field", ""),
            "endorsement_date": rule_row.get("Date Updated", ""),
            "enabled": enabled_flag,
            "user_name": "SYSTEM",
            "sub_entity_type_id": sub_entity_id,
            "ingest_or_ui_id": ingest_or_ui_id,
            "enforcement_level_id": enforcement_level_id,
            "error_warning_type_id": "",
            "dq_wkflw_ticket_ind": "TRUE"
        })

        next_rule_id += 1

    return pd.DataFrame(rows)

def prepare_rules_extn(dq_rules_master, add_rules_df, tenant, engine, sheet_map):
    # This function can be implemented similarly if needed
    pass