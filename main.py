import pandas as pd
from sqlalchemy import create_engine

from config import TENANT_NAMES, TENANT_DATA_FOLDER_PATHS, TENANT_DEV_FILE_PATHS, SHEET_NAME, ENGINE
from rules.metadata import get_version_info, get_version_info_extn
from rules.prepare import prepare_rules, prepare_rules_extn
from rules.writers import write_csv, write_xml, update_dev_file, write_csv_extn, write_xml_extn

engine = ENGINE
def main(dq_file_path, jira_file_path):
    # Load all sheets in the DQ rules excel into a dict of DataFrames
    dq_rules_master = pd.read_excel(dq_file_path, sheet_name=None)
    # Load Jira excel into a single DataFrame
    jira_desc = pd.read_excel(jira_file_path)
    # Standardize column names: lowercase + strip whitespace
    jira_desc.columns = [c.strip().lower() for c in jira_desc.columns]

    add_rules_df = jira_desc[
        (jira_desc["action"].str.lower() == "add") | 
        (jira_desc["action"].str.lower() == "update")
    ]

    path = TENANT_DATA_FOLDER_PATHS["c"]
    dev_path = TENANT_DEV_FILE_PATHS["c"]
    version = get_version_info(path)

    dq_rules_csv = prepare_rules(dq_rules_master, add_rules_df, engine, SHEET_NAME)
    csv_file = write_csv(dq_rules_csv, path, version)
    print(f"Generated CSV: {csv_file}")

    xml_file = write_xml(path, version, add_rules_df.iloc[0]["ticket"])
    print(f"Generated XML: {xml_file}")

    dev_file = update_dev_file(dev_path, version, add_rules_df.iloc[0]["ticket"])
    print(f"Updated Dev File: {dev_file}")

    configure_rules_df = jira_desc[jira_desc["action"].str.lower() == "configure"]
    for tenant in TENANT_NAMES:
        path = TENANT_DATA_FOLDER_PATHS[tenant]
        dev_path = TENANT_DEV_FILE_PATHS[tenant]
        version = get_version_info_extn(path)
        tenant_name = TENANT_NAMES[tenant]
        configure_rules_df_tenant_specific = configure_rules_df[configure_rules_df["tenant"].str.lower() == tenant.lower()]

         # Write CSV for tenant
        if not configure_rules_df_tenant_specific.empty:
            print(f"Processing configuration for tenant '{tenant_name}'...")
            dq_rules_extn = prepare_rules_extn(dq_rules_master, configure_rules_df_tenant_specific, tenant_name, engine, SHEET_NAME, csv_file)
            if not dq_rules_extn.empty:
                extn_csv_file = write_csv(dq_rules_extn, path, version)
                print(f"Generated Extension CSV for tenant '{tenant_name}': {extn_csv_file}")
                extn_xml_file = write_xml_extn(path, version, add_rules_df.iloc[0]["ticket"])
                print(f"Generated Extension XML for tenant '{tenant_name}': {extn_xml_file}")
            else:
                print(f"No configuration changes for tenant '{tenant_name}'.")

    print("All done!")
    return
