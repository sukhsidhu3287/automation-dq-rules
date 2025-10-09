import os, csv, re
from datetime import datetime

def write_csv(df, path, version):
    try:
        # Validate path components
        if not path:
            raise ValueError(f"Invalid path: path is empty")
        
        # Create directory with better error handling
        os.makedirs(path, exist_ok=True)
        
        # Validate filename components
        if not version or not str(version).strip():
            raise ValueError(f"Invalid version: '{version}'")
        
        filename = f"load_validation_rules_data_ver_{version}.csv"
        file = os.path.join(path, filename)
        
        print(f"Writing CSV to: {file}")
        df.to_csv(file, index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
        return file
    except OSError as e:
        raise OSError(f"Failed to write CSV file. Path: '{path}', Version: '{version}'. Error: {str(e)}") from e

def write_csv_extn(df, path, version):
    try:
        os.makedirs(path, exist_ok=True)
        filename = f"load_des_validation_rules_extn_data_ver_{version}.csv"
        file = os.path.join(path, filename)
        print(f"Writing CSV extension to: {file}")
        df.to_csv(file, index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
        return file
    except OSError as e:
        raise OSError(f"Failed to write CSV extension file. Path: '{path}', Version: '{version}'. Error: {str(e)}") from e

def write_xml(path, version, ticket_number):
    try:
        os.makedirs(path, exist_ok=True)
        filename = f"load_validation_rules_data_ver_{version}.xml"
        xml_file = os.path.join(path, filename)
        print(f"Writing XML to: {xml_file}")
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
    except OSError as e:
        raise OSError(f"Failed to write XML file. Path: '{path}', Version: '{version}'. Error: {str(e)}") from e

def write_xml_extn(path, version, ticket_number):
    try:
        os.makedirs(path, exist_ok=True)
        filename = f"load_des_validation_rules_extn_data_ver_{version}.xml"
        xml_file = os.path.join(path, filename)
        print(f"Writing XML extension to: {xml_file}")
        with open(xml_file, "w", encoding="utf-8") as f:
            f.write(f"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<databaseChangeLog  xmlns="http://www.liquibase.org/xml/ns/dbchangelog" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:pro="http://www.liquibase.org/xml/ns/pro"
                    xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog 
                                    http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd
                                    http://www.liquibase.org/xml/ns/pro 
                                    http://www.liquibase.org/xml/ns/pro/liquibase-pro-latest.xsd">

    <changeSet author="${{author}}" id="load_des_validation_rules_extn_data_ver_{version}__{ticket_number}">
        <loadUpdateData tableName="des_validation_rules_extn"
                  file="load_des_validation_rules_extn_data_ver_2_9_5.csv"
                  encoding="UTF-8"
                  separator=","
                  primaryKey="rule_extn_id"
                  relativeToChangelogFile="true">                  
            <column index="0" name="rule_extn_id" type="numeric"/>
            <column index="1" name="rule_id" type="numeric"/>
			<column index="2" name="task_id" type="numeric"/>
			<column index="3" name="rule_applied_zone" type="string"/>
			<column index="4" name="hrpdm_table_id" type="string"/>
			<column index="5" name="hrpdm_column_names" type="string"/>
			<column index="6" name="source_table_id" type="string"/>
			<column index="7" name="source_column_names" type="string"/>
			<column index="8" name="sql_query" type="string"/>
			<column index="9" name="active_flag" type="string"/>
			<column index="10" name="implmnt_type" type="string"/>
			<column index="11" name="implmnt_order" type="numeric"/>
			<column index="12" name="reference_codeset_id" type="numeric"/>
			<column index="13" name="entity_key" type="string"/>
			<column index="14" name="pdm_entity_id" type="numeric"/>	
			<column index="15" name="source_owner_name" type="string"/>	
        </loadUpdateData>
    </changeSet>


</databaseChangeLog>""")
        return xml_file
    except OSError as e:
        raise OSError(f"Failed to write XML extension file. Path: '{path}', Version: '{version}'. Error: {str(e)}") from e

def update_dev_file(path, version, ticket_number):
    try:
        # Validate that the path exists or can be created
        if not path:
            raise ValueError(f"Invalid path: path is empty")
        
        # Ensure the directory exists
        os.makedirs(path, exist_ok=True)
        
        now = datetime.now()
        year_digit = 2 if now.year < 2026 else 3
        filename = f"dev-{year_digit}.{now.month}.0.xml"
        dev_file = os.path.join(path, filename)

        include_line = f'\n<!-- below are for {ticket_number} ADD DQ Rules-->\n<include file="data/load_validation_rules_data_ver_{version}.xml" relativeToChangelogFile="true"/>\n'

        print(f"Updating Dev File: {dev_file}")

        if not os.path.exists(dev_file):
            print(f"Dev file does not exist. Creating new one: {dev_file}")
            with open(dev_file, "w", encoding="utf-8") as f:
                f.write(f"<databaseChangeLog> ... {include_line} ... </databaseChangeLog>")
        else:
            with open(dev_file, "r", encoding="utf-8") as f:
                content = f.read()
            if include_line not in content:
                # Look for all <include .../> tags
                include_pattern = r'(<include [^>]+/>\s*)'
                matches = list(re.finditer(include_pattern, content))

                if matches:
                    # Insert after last include
                    last = matches[-1]
                    insert_pos = last.end()
                    content = content[:insert_pos] + "\n" + include_line + "\n" + content[insert_pos:]
                else:
                    # No <include>, insert before <changeSet author="${author}" id="configdb_ver_2_10_0">
                    change_set_pattern = r'(<changeSet\s+author="\$\{author\}"\s+id="configdb_ver_2_10_0"\s*>)'
                    match = re.search(change_set_pattern, content)
                    if match:
                        insert_pos = match.start()
                        content = content[:insert_pos] + include_line + "\n" + content[insert_pos:]
                    else:
                        raise ValueError("Could not find target <changeSet> block for insertion.")
                
                with open(dev_file, "w", encoding="utf-8") as f:
                    f.write(content)

        return dev_file
    except OSError as e:
        raise OSError(f"Failed to update dev file. Path: '{path}', Filename: '{filename if 'filename' in locals() else 'N/A'}'. Error: {str(e)}") from e
    except Exception as e:
        raise Exception(f"Error updating dev file. Path: '{path}'. Error: {str(e)}") from e