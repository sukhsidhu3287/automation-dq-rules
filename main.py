import pandas as pd
from sqlalchemy import create_engine

from config import TENANT_NAMES, TENANT_DATA_FOLDER_PATHS, TENANT_DEV_FILE_PATHS, SHEET_NAME
from rules.metadata import get_version_info
from rules.prepare import prepare_rules, prepare_rules_extn
from rules.writers import write_csv, write_xml, update_dev_file

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5434/postgres")

def main():
    dq_rules_master = pd.read_excel(input("Enter path to DQ rules master Excel: ").strip(), sheet_name=None)
    jira_desc = pd.read_excel(input("Enter path to JIRA description Excel: ").strip())
    jira_desc.columns = [c.strip().lower() for c in jira_desc.columns]

    add_rules_df = jira_desc[jira_desc["action"].str.lower() == "add"]

    path = TENANT_DATA_FOLDER_PATHS["c"]
    dev_path = TENANT_DEV_FILE_PATHS["c"]
    version = get_version_info(path)

    dq_rules_csv = prepare_rules(dq_rules_master, add_rules_df, engine, SHEET_NAME)
    csv_file = write_csv(dq_rules_csv, path, version)
    print(f"Generated CSV: {csv_file}")

    xml_file = write_xml(path, version, add_rules_df.iloc[0]["ticket"])
    print(f"Generated XML: {xml_file}")

    dev_file = update_dev_file(dev_path, version)
    print(f"Updated Dev File: {dev_file}")

    configure_rules_all = jira_desc[jira_desc["action"].str.lower() == "configure"]

    # for tenant in TENANT_NAMES.keys():
        # add_rules_df = configure_rules_all[configure_rules_all["tenant"] == tenant]
        # if add_rules_df.empty:
        #     continue

        # path = TENANT_DATA_FOLDER_PATHS[tenant]
        # dev_path = TENANT_DEV_FILE_PATHS[tenant]
        # version = get_version_info(path)

        # dq_rules_csv = prepare_rules_extn(dq_rules_master, add_rules_df, TENANT_NAMES[tenant], engine, SHEET_NAME)
        # csv_file = write_csv(dq_rules_csv, path, version)
        # print(f"Generated CSV: {csv_file}")

        # xml_file = write_xml(path, version, add_rules_df.iloc[0]["ticket"])
        # print(f"Generated XML: {xml_file}")

        # dev_file = update_dev_file(dev_path, version)
        # print(f"Updated Dev File: {dev_file}")

if __name__ == "__main__":
    main()
