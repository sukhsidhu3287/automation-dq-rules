import pandas as pd

from config import TENANT_DATA_FOLDER_PATHS, TENANT_DEV_FILE_PATHS, ENGINE
from rules.helper import get_version_info, get_version_info_extn
from rules.add_update import prepare_add_update_rules
from rules.configure import prepare_configure_rules
from rules.writers import write_csv, write_xml, update_dev_file, write_csv_extn, write_xml_extn
from rules.logger import setup_logger, log_section_start, log_subsection, log_file_operation

engine = ENGINE
logger = setup_logger("main")

def consolidate_dq_master_sheets(dq_file_path):
    sheets_to_consolidate = [
        'Provider Network',
        'Practitioner', 
        'Organization',
        'Address',
        'Network'
    ]
    
    logger.info(f"📚 Consolidating {len(sheets_to_consolidate)} sheets into single DataFrame...")
    
    # Read all specified sheets
    all_sheets = pd.read_excel(dq_file_path, sheet_name=sheets_to_consolidate)
    
    # Column name mappings to standardize
    column_mappings = {
        'Rule ID': 'RuleID',
        'RuleID': 'RuleID',
        'rule id': 'RuleID',
        'ruleid': 'RuleID',
        'Rule Type': 'RuleType',
        'Rule type': 'RuleType',
        'Rule Category': 'RuleType',
        'rule type': 'RuleType',
        'rule category': 'RuleType',
        'Sub Entity': 'SubEntity',
        'Sub-Entity': 'SubEntity',
        'sub entity': 'SubEntity',
        'sub-entity': 'SubEntity',
        'Ingest+UI/UI Only': 'IngestUIOnly',
        'Ingest+UI/Ui Only': 'IngestUIOnly',
        'Ingest+UI/UI only': 'IngestUIOnly',
        'ingest+ui/ui only': 'IngestUIOnly',
        'Column Name': 'ColumnName',
        'column name': 'ColumnName',
        'Table Name': 'TableName',
        'table name': 'TableName',
        'Rule Name': 'RuleName',
        'rule name': 'RuleName',
        'Rule Description': 'RuleDescription',
        'rule description': 'RuleDescription'
    }
    
    consolidated_dfs = []
    
    for sheet_name, df in all_sheets.items():
        # Create a copy to avoid modifying original
        df_copy = df.copy()
        
        # Strip whitespace from column names first
        df_copy.columns = [str(c).strip() for c in df_copy.columns]
        
        # Apply column name mappings (case-insensitive)
        new_columns = []
        for col in df_copy.columns:
            # Check if column needs mapping
            mapped_col = column_mappings.get(col, col)
            new_columns.append(mapped_col)
        
        df_copy.columns = new_columns
        
        # Add source sheet as a column for reference
        df_copy['SourceSheet'] = sheet_name
        
        consolidated_dfs.append(df_copy)
        logger.info(f"  ✓ {sheet_name}: {len(df_copy)} rules")
    
    # Concatenate all dataframes
    consolidated_df = pd.concat(consolidated_dfs, ignore_index=True, sort=False)
    
    logger.info(f"✅ Consolidated {len(consolidated_df)} total rules from {len(sheets_to_consolidate)} sheets")
    
    return consolidated_df

def main_ui_workflow(dq_file_path, rules_df, workflow_type):
    log_section_start(logger, f"Workflow: {workflow_type.upper()}")
    
    # Load and consolidate DQ rules master
    logger.info("📂 Loading DQ Rules Master file...")
    dq_rules_master = consolidate_dq_master_sheets(dq_file_path)
    
    # Standardize input DataFrame column names
    rules_df.columns = [c.strip().lower() for c in rules_df.columns]
    
    generated_files = []
    
    # Route to appropriate workflow
    if workflow_type == "add_update":
        generated_files = _process_add_update_workflow(dq_rules_master, rules_df, engine)
    elif workflow_type == "configure":
        generated_files = _process_configure_workflow(dq_rules_master, rules_df, engine)
    else:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    
    logger.info(f"📊 Total files generated: {len(generated_files)}")
    
    return generated_files


def _process_add_update_workflow(dq_rules_master, rules_df, engine):
    log_subsection(logger, "📝 Processing ADD/UPDATE workflow")
    
    # Get paths and version
    path = TENANT_DATA_FOLDER_PATHS["common"]
    dev_path = TENANT_DEV_FILE_PATHS["common"]
    version = get_version_info(path)
    ticket = rules_df.iloc[0]["ticket"]
    
    logger.info(f"📍 Target: Common repository")
    logger.info(f"📌 Version: {version}")
    logger.info(f"🎫 Ticket: {ticket}")
    
    # Prepare rules data using new modular function
    dq_rules_df = prepare_add_update_rules(dq_rules_master, rules_df, engine)
    
    if dq_rules_df.empty:
        logger.warning("⚠️  No rules to process")
        return []
    
    # Generate output files
    generated_files = []
    
    csv_file = write_csv(dq_rules_df, path, version)
    log_file_operation(logger, "Generated CSV", csv_file)
    generated_files.append(csv_file)
    
    xml_file = write_xml(path, version, ticket)
    log_file_operation(logger, "Generated XML", xml_file)
    generated_files.append(xml_file)
    
    dev_file = update_dev_file(dev_path, version, ticket)
    log_file_operation(logger, "Updated Dev File", dev_file)
    generated_files.append(dev_file)
    
    return generated_files


def _process_configure_workflow(dq_rules_master, rules_df, engine):
    log_subsection(logger, "⚙️  Processing CONFIGURE workflow")
    
    # Get unique tenants and ticket
    unique_tenants = rules_df["tenant"].unique()
    ticket = rules_df.iloc[0]["ticket"]
    
    logger.info(f"🎫 Ticket: {ticket}")
    logger.info(f"🏢 Tenants: {', '.join(unique_tenants)}")
    
    generated_files = []
    
    # Process each tenant separately
    for tenant_name in unique_tenants:
        logger.info(f"\n{'─'*60}")
        logger.info(f"Processing tenant: {tenant_name.upper()}")
        logger.info(f"{'─'*60}")
        
        
        # Get paths and version
        path = TENANT_DATA_FOLDER_PATHS[tenant_name]
        dev_path = TENANT_DEV_FILE_PATHS[tenant_name]
        version = get_version_info_extn(path)
        
        logger.info(f"📌 Version: {version}")
        
        # Filter configurations for this tenant
        tenant_configs = rules_df[rules_df["tenant"] == tenant_name]
        logger.info(f"📊 Configurations: {len(tenant_configs)}")
        
        # Prepare configuration data using new modular function
        dq_rules_extn_df = prepare_configure_rules(
            dq_rules_master, 
            tenant_configs, 
            tenant_name, 
            engine
        )
        
        if dq_rules_extn_df.empty:
            logger.warning(f"⚠️  No configurations to process for {tenant_name}")
            continue
        
        # Generate output files
        extn_csv_file = write_csv_extn(dq_rules_extn_df, path, version)
        log_file_operation(logger, "Generated CSV", extn_csv_file)
        generated_files.append(extn_csv_file)
        
        extn_xml_file = write_xml_extn(path, version, ticket)
        log_file_operation(logger, "Generated XML", extn_xml_file)
        generated_files.append(extn_xml_file)
        
        dev_file = update_dev_file(dev_path, version, ticket)
        log_file_operation(logger, "Updated Dev File", dev_file)
        generated_files.append(dev_file)
    
    return generated_files
