import pandas as pd
from sqlalchemy import create_engine

from config import TENANT_NAMES, TENANT_DATA_FOLDER_PATHS, TENANT_DEV_FILE_PATHS, SHEET_NAME
from rules.metadata import get_version_info
from rules.prepare import prepare_rules, prepare_rules_extn
from rules.writers import write_csv, write_xml, update_dev_file

engine = create_engine("postgresql+psycopg2://pdm:postgres@localhost:5432/pdm_db")

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

    print("All done!")
    return csv_file, xml_file
