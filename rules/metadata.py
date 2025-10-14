import re
import os
import pandas as pd
from datetime import datetime

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



def get_metadata_id(metadata_set, metadata_value, engine):
    query = """
        SELECT metadata_id
        FROM healthfirst_configdb.validation_rule_metadata
        WHERE metadata_set = %s AND metadata_value = %s
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=(metadata_set, metadata_value))
    return df.iloc[0,0] if not df.empty else None

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