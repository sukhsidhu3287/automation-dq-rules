import os, csv, re
from datetime import datetime

def write_csv(df, path, version):
    os.makedirs(path, exist_ok=True)
    file = os.path.join(path, f"load_validation_rules_data_ver_{version}.csv")
    df.to_csv(file, index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
    return file

def write_xml(path, version, ticket_number):
    xml_file = os.path.join(path, f"load_validation_rules_data_ver_{version}.xml")
    with open(xml_file, "w", encoding="utf-8") as f:
        f.write(f"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<databaseChangeLog xmlns="http://www.liquibase.org/xml/ns/dbchangelog" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:pro="http://www.liquibase.org/xml/ns/pro"
                   xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog 
                                       http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd
                                       http://www.liquibase.org/xml/ns/pro 
                                       http://www.liquibase.org/xml/ns/pro/liquibase-pro-latest.xsd">

    <changeSet author="${{author}}" id="load_validation_rules_data_{version}__{ticket_number}" >
        <loadUpdateData tableName="validation_rules"
                        file="load_validation_rules_data_ver_{version}.csv"
                        encoding="UTF-8"
                        separator=","
                        quotchar='"'
                        primaryKey="rule_id"
                        relativeToChangelogFile="true">
            <column index="0" name="rule_id" type="numeric"/>
            <column index="1" name="business_rule_id" type="string"/>
            <column index="2" name="rule_category_id" type="numeric"/>
            <column index="3" name="rule_category_desc" type="string"/>
            <column index="4" name="rule_name" type="string"/>
            <column index="5" name="rule_desc" type="string"/>
            <column index="6" name="rule_type_id" type="numeric"/>
            <column index="7" name="entity_type_id" type="numeric"/>
            <column index="8" name="range_type_id" type="numeric"/>
            <column index="9" name="min" type="string"/>
            <column index="10" name="max" type="string"/>
            <column index="11" name="regex_pattern" type="string"/>
            <column index="12" name="sql_query" type="string"/>
            <column index="13" name="batch_error_message" type="string"/>
            <column index="14" name="ui_error_message_summary" type="string"/>
            <column index="15" name="ui_field_error_message" type="string"/>
            <column index="16" name="endorsement_date" type="date"/>
            <column index="17" name="enabled" type="boolean"/>
            <column index="18" name="user_name" type="string"/>
            <column index="19" name="sub_entity_type_id" type="numeric"/>
            <column index="20" name="ingest_or_ui_id" type="numeric"/>
            <column index="21" name="enforcement_level_id" type="numeric"/>
            <column index="22" name="error_warning_type_id" type="numeric"/>
            <column index="23" name="dq_wkflw_ticket_ind" type="boolean"/>
        </loadUpdateData>
    </changeSet>

</databaseChangeLog>
""")
    return xml_file

def update_dev_file(path: str, version: str):
    now = datetime.now()
    year_digit = 2 if now.year < 2026 else 3
    dev_file = os.path.join(path, f"dev-{year_digit}.{now.month}.0.xml")

    include_line = f'    <include file="data/load_validation_rules_data_ver_{version}.xml" relativeToChangelogFile="true"/>'

    if not os.path.exists(dev_file):
        with open(dev_file, "w", encoding="utf-8") as f:
            f.write(f"<databaseChangeLog> ... {include_line} ... </databaseChangeLog>")
    else:
        with open(dev_file, "r", encoding="utf-8") as f:
            content = f.read()
        if include_line not in content:
            pattern = r'(<include [^>]+/>\s*)'
            matches = list(re.finditer(pattern, content))
            last = matches[-1]
            insert_pos = last.end()
            content = content[:insert_pos] + "\n" + include_line + "\n" + content[insert_pos:]
            with open(dev_file, "w", encoding="utf-8") as f:
                f.write(content)
    return dev_file

