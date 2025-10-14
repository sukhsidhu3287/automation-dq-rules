from sqlalchemy import text
import re
import pandas as pd
from .metadata import get_metadata_id, get_hrp_source_table_id

def prepare_rules(dq_rules_master, add_rules_df, engine, sheet_map):
    max_id_query = f"SELECT COALESCE(MAX(rule_id), 0) AS max_rule_id FROM healthfirst_configdb.validation_rules"
    with engine.connect() as conn:
        max_id_df =  pd.read_sql(max_id_query, conn)
    max_rule_id = int(max_id_df["max_rule_id"].iloc[0])

    rows = []
    for _, rule in add_rules_df.iterrows():
        sheet_name = sheet_map.get(rule["entity"], None)
        if sheet_name not in dq_rules_master:
            print(f"⚠️ Sheet '{sheet_name}' not found. Skipping {rule.get('ruleid')}.")
            continue

        sheet_df = dq_rules_master[sheet_name]
        master_mapping_row = sheet_df[sheet_df["RuleID"] == rule["ruleid"]]
        if master_mapping_row.empty:
            print(f"⚠️ RuleID {rule['ruleid']} not found in '{sheet_name}'. Skipping.")
            continue

        master_mapping_row = master_mapping_row.iloc[0]

        # Rule Category
        rule_category = str(master_mapping_row.get("Rule Category") or master_mapping_row.get("Rule Type", "")) \
                        .upper().strip().replace(" ", "_")
        rule_category_id = get_metadata_id("Rule Category", rule_category, engine)

        #rule type
        rule_type = str(rule.get("ruletype")).upper().strip()
        rule_type_id = get_metadata_id("Rule Type", rule_type, engine)

        # Entity Type
        entity_type = str(master_mapping_row.get("Entity", "")).strip().upper()
        entity_type_id = get_metadata_id("Entity Type", entity_type, engine)

        # Sub Entity
        sub_entity = str(master_mapping_row.get("Sub Entity", "")).strip().upper()
        sub_entity_id = get_metadata_id("Sub_Entity_Type", sub_entity, engine)

        # Ingest/UI
        ingest_map = {"Ingest+UI": "BOTH", "UI Only": "UI", "Ingest only": "INGEST"}
        ingest_val = str(master_mapping_row.get("Ingest+UI/UI Only ", "")).strip()
        ingest_or_ui = ingest_map.get(ingest_val, ingest_val.upper())
        ingest_or_ui_id = get_metadata_id("Ingest_Or_UI", ingest_or_ui, engine)

        # Enforcement
        enforcement_level = str(master_mapping_row.get("Enforcement Level", "")).upper()
        enforcement_level_id = None if enforcement_level == "NA" else \
            get_metadata_id("Enforcement_Level", enforcement_level, engine)
        
        if enforcement_level_id == 28:
            error_warning_type_id = 31
        else:
            error_warning_type_id = 32

        enabled_flag = "Y" # yes for now for all
        
        rule_id_query = text("""
            SELECT rule_id 
            FROM healthfirst_configdb.validation_rules 
            WHERE business_rule_id = :rule_id
        """)

        with engine.connect() as conn:
            existing_rule_id = pd.read_sql(rule_id_query, conn, params={"rule_id": master_mapping_row.get("RuleID")})

        if existing_rule_id.empty:
            rule_id = max_rule_id + 1
            max_rule_id += 1
        else:
            rule_id = int(existing_rule_id["rule_id"].iloc[0])

        row_to_append = {
            "rule_id": rule_id,
            "business_rule_id": master_mapping_row.get("RuleID"),
            "rule_category_id": rule_category_id,
            "rule_category_desc": master_mapping_row.get("Rule Type", ""),
            "rule_name": master_mapping_row.get("Rule Name", ""),
            "rule_desc": master_mapping_row.get("Rule Description", ""),
            "rule_type_id": rule_type_id,
            "entity_type_id": entity_type_id,
            "range_type_id": "",
            "min": "",
            "max": "",
            "regex_pattern": "",
            "sql_query": "",
            "batch_error_message": master_mapping_row.get("Interface( Batch, API) ingestion Error Message", ""),
            "ui_error_message_summary": master_mapping_row.get("UI Error Message Summary", ""),
            "ui_field_error_message": master_mapping_row.get("UI Error Message Under Field", ""),
            "endorsement_date": master_mapping_row.get("Date Updated", ""),
            "enabled": enabled_flag,
            "user_name": "SYSTEM",
            "sub_entity_type_id": sub_entity_id,
            "ingest_or_ui_id": ingest_or_ui_id,
            "enforcement_level_id": enforcement_level_id,
            "error_warning_type_id": error_warning_type_id,
            "dq_wkflw_ticket_ind": "TRUE"
        }
        
        # Check if rule already exists with the same data
        check_existing_data = text("""
            SELECT rule_id, business_rule_id, rule_category_id, rule_category_desc,
                   rule_name, rule_desc, rule_type_id, entity_type_id, range_type_id,
                   min, max, regex_pattern, sql_query, batch_error_message,
                   ui_error_message_summary, ui_field_error_message, endorsement_date,
                   enabled, user_name, sub_entity_type_id, ingest_or_ui_id,
                   enforcement_level_id, error_warning_type_id, dq_wkflw_ticket_ind
            FROM healthfirst_configdb.validation_rules
            WHERE business_rule_id = :business_rule_id
        """)
        with engine.connect() as conn:
            exists_df = pd.read_sql(check_existing_data, conn, params={"business_rule_id": master_mapping_row.get("RuleID")})
        
        append = True
        if not exists_df.empty:
            # Get the existing row data
            existing_row = exists_df.iloc[0]
            
            # Compare each field (excluding rule_id as it's auto-generated for new records)
            fields_to_compare = [
                "business_rule_id", "rule_category_id", "rule_category_desc",
                "rule_name", "rule_desc", "rule_type_id", "entity_type_id",
                "range_type_id", "min", "max", "regex_pattern", "sql_query",
                "batch_error_message", "ui_error_message_summary", "ui_field_error_message",
                "endorsement_date", "enabled", "user_name", "sub_entity_type_id",
                "ingest_or_ui_id", "enforcement_level_id", "error_warning_type_id",
                "dq_wkflw_ticket_ind"
            ]
            
            is_same = True
            for field in fields_to_compare:
                existing_value = existing_row.get(field)
                new_value = row_to_append.get(field)
                
                # Handle None/NaN comparisons
                if pd.isna(existing_value) and pd.isna(new_value):
                    continue
                elif pd.isna(existing_value) or pd.isna(new_value):
                    is_same = False
                    break
                elif str(existing_value).strip().upper() != str(new_value).strip().upper():
                    is_same = False
                    break
            
            if is_same:
                print(f"⚠️ Rule {rule['ruleid']} already exists with identical data. Skipping.")
                append = False
            else:
                print(f"⚠️ Rule {rule['ruleid']} exists but with different data. Will update.")
                append = True
        
        # Only append the row if append flag is True
        if append:
            rows.append(row_to_append)


    return pd.DataFrame(rows)

def prepare_rules_extn(dq_rules_master, configure_rules_df, tenant, engine, sheet_map, dq_rules_csv):
    max_id_query = f"SELECT COALESCE(MAX(rule_id), 0) AS max_rule_id FROM {tenant}_configdb.des_validation_rules_extn where upper(source_owner_name) = '{configure_rules_df.iloc[0]['sourceownername'].upper()}'"
    with engine.connect() as conn:
        max_id_df =  pd.read_sql(max_id_query, conn)
        max_rule_extn_id = int(max_id_df["max_rule_id"].iloc[0])
        id_exists = text(f"Select rule_extn_id from {tenant}_configdb.des_validation_rules_extn where rule_extn_id = {max_rule_extn_id +1}")
        if id_exists is not None:
            max_id_query = f"SELECT COALESCE(MAX(rule_extn_id), 0) AS max_rule_id FROM {tenant}_configdb.des_validation_rules_extn"
            max_rule_extn_id = int(pd.read_sql(max_id_query, conn).iloc[0,0])
    rows = []
    for _, rule in configure_rules_df.iterrows():
        DQ_table_rule_id_query = text(f"Select rule_id from {tenant}_configdb.validation_rules where business_rule_id = :rule_id")
        with engine.connect() as conn:
            DQ_table_rule_id = pd.read_sql(DQ_table_rule_id_query, conn, params={"rule_id": rule["ruleid"]})
        DQ_table_rule_id = int(DQ_table_rule_id.iloc[0,0])

        already_configured = text(f"""
            SELECT 1 FROM {tenant}_configdb.des_validation_rules_extn WHERE rule_id = :rule_id
        """)
        with engine.connect() as conn:
            exists_df = pd.read_sql(already_configured, conn, params={"rule_id": DQ_table_rule_id})
        
        if exists_df.empty:
            rule_extn_id = max_rule_extn_id + 1
            max_rule_extn_id += 1
        else:
            rule_extn_id = int(exists_df.iloc[0,0])

        sheet_name = sheet_map.get(rule["entity"], None)
        if sheet_name not in dq_rules_master:
            print(f"⚠️ Sheet '{sheet_name}' not found. Skipping {rule.get('ruleid')}.")
            continue

        sheet_df = dq_rules_master[sheet_name]
        master_mapping_row = sheet_df[sheet_df["RuleID"] == rule["ruleid"]]
        if master_mapping_row.empty:
            print(f"⚠️ RuleID {rule['ruleid']} not found in '{sheet_name}'. Skipping.")
            continue

        master_mapping_row = master_mapping_row.iloc[0]

        rule_id_query = text(f"Select rule_id from {tenant}_configdb.validation_rules where business_rule_id = :rule_id")

        with engine.connect() as conn:
            existing_rule_id = pd.read_sql(rule_id_query, conn, params={"rule_id": master_mapping_row.get("RuleID")})

        if existing_rule_id.empty:
            # rule is not present in validation rules table, so add rule_id from the csv we created
            rule_row_in_csv = dq_rules_csv[dq_rules_csv["business_rule_id"] == master_mapping_row.get("RuleID")]
            if rule_row_in_csv.empty:
                print(f"⚠️ RuleID {master_mapping_row.get('RuleID')} not found DB and in CSV. Skipping.")
                continue
            else:
                rule_id = int(rule_row_in_csv["rule_id"].iloc[0])
        else:
            rule_id = int(existing_rule_id["rule_id"].iloc[0])
        
        rule_appied_zone = rule.get("zoneapplied", "").upper()
        target_table = master_mapping_row.get("Table Name", "").strip().upper()
        hrpdm_table_id_query = text(f"select table_id from {tenant}_configdb.des_zone_table_list dztl where upper(dztl.table_name) = :target_table")
        hrpdm_table_id = pd.read_sql(hrpdm_table_id_query, engine, params={"target_table": target_table})

        entity_type = str(master_mapping_row.get("Entity", "")).strip().upper()
        entity_key_query = text(f"select pdm_entity_id, entity_key_field_name  from {tenant}_configdb.pdm_entity_master where upper(entity_name) = :entity_type")
        entity_key_query_result = pd.read_sql(entity_key_query, engine, params={"entity_type": entity_type})
        entity_key = entity_key_query_result.iloc[0,1]
        pdm_entity_id = entity_key_query_result.iloc[0,0]

        if rule.get("sourceownername", "").strip().lower() == "hrp":
            # source_table_id = get_hrp_source_table_id(engine, tenant, entity_type, target_table)
            source_table_id = None
        else:
            source_table_id_query = text(f"select distinct source_table_id from {tenant}_configdb.des_validation_rules_extn where source_owner_name = :source_owner_name")
            source_table_id = pd.read_sql(source_table_id_query, engine, params={"source_owner_name": rule.get("sourceownername", "").upper()})
            source_table_id = ','.join(source_table_id.iloc[:,0].astype(str)) if not source_table_id.empty else None
        
        row_to_append = {
            "rule_extn_id": rule_extn_id,
            "rule_id": rule_id,
            "task_id": None,
            "rule_applied_zone": rule_appied_zone,
            "hrpdm_table_id": hrpdm_table_id.iloc[0,0] if not hrpdm_table_id.empty else None,
            "hrpdm_column_names": master_mapping_row.get("Column Name", "").strip().upper(),
            "source_table_id": source_table_id,
            "source_column_names": None,
            "sql_query":None,
            "active_flag": "Y",
            "implmnt_type": None,
            "implmnt_order": None,
            "reference_codeset_id": None,
            "entity_key": entity_key,
            "pdm_entity_id": pdm_entity_id,
            "source_owner_name": rule.get("sourceownername", "").upper()
            }
        
        already_configured = text(f"""
            SELECT rule_extn_id, rule_id, task_id, rule_applied_zone, hrpdm_table_id, 
                   hrpdm_column_names, source_table_id, source_column_names, sql_query,
                   active_flag, implmnt_type, implmnt_order, reference_codeset_id,
                   entity_key, pdm_entity_id, source_owner_name
            FROM {tenant}_configdb.des_validation_rules_extn WHERE rule_id = :rule_id
        """)
        with engine.connect() as conn:
            exists_df = pd.read_sql(already_configured, conn, params={"rule_id": rule_id})
        append = True
        if not exists_df.empty:
            # Get the existing row data
            existing_row = exists_df.iloc[0]
            
            # Compare each field (excluding rule_extn_id as it's auto-generated)
            fields_to_compare = [
                "rule_id", "task_id", "rule_applied_zone", "hrpdm_table_id",
                "hrpdm_column_names", "source_table_id", "source_column_names", 
                "sql_query", "active_flag", "implmnt_type", "implmnt_order",
                "reference_codeset_id", "entity_key", "pdm_entity_id", "source_owner_name"
            ]
            
            is_same = True
            for field in fields_to_compare:
                existing_value = existing_row.get(field)
                new_value = row_to_append.get(field)
                
                # Handle None/NaN comparisons
                if pd.isna(existing_value) and pd.isna(new_value):
                    continue
                elif pd.isna(existing_value) or pd.isna(new_value):
                    is_same = False
                    break
                elif str(existing_value).strip().upper() != str(new_value).strip().upper():
                    is_same = False
                    break
            
            if is_same:
                print(f"⚠️ Rule {rule['ruleid']} already exists with identical data. Skipping.")
                append = False
            else:
                print(f"⚠️ Rule {rule['ruleid']} exists but with different data. Will update.")
                append = True
        
        # Only append the row if append flag is True
        if append:
            rows.append(row_to_append)
        
    return pd.DataFrame(rows)