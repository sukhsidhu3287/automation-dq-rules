from flask import Flask, render_template, request, redirect, url_for
import os
import pandas as pd
from main import main_ui_workflow
from rules.logger import setup_logger, log_separator, log_file_operation, log_error, log_section_start, reset_log_file
import traceback

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Set up logger
logger = setup_logger("app")

# Store the DQ master file path in session
DQ_MASTER_FILE = None

@app.route("/", methods=["GET"])
def landing():
    return render_template("upload_master.html")

@app.route("/upload-master", methods=["POST"])
def upload_master():
    try:
        log_section_start(logger, "DQ Rules Master Upload")
        
        dq_file = request.files.get("dq_excel")
        if not dq_file:
            log_error(logger, "No file uploaded")
            return render_template("result.html", 
                                 success=False, 
                                 message="No file uploaded",
                                 error_details="Please select a DQ Rules Master file")
        
        # Save the master file
        global DQ_MASTER_FILE
        DQ_MASTER_FILE = os.path.join(UPLOAD_FOLDER, "dq_rules_master.xlsx")
        dq_file.save(DQ_MASTER_FILE)
        
        log_file_operation(logger, "Uploaded DQ Rules Master", DQ_MASTER_FILE)
        
        logger.info("✅ DQ Rules Master file uploaded successfully")
        log_separator(logger, "=", 70)
        
        # Redirect to choose operation page
        return redirect(url_for('choose_operation'))
        
    except Exception as e:
        error_details = traceback.format_exc()
        log_error(logger, f"Error uploading file: {str(e)}", e)
        return render_template("result.html",
                             success=False,
                             message="Error uploading file",
                             error_details=error_details)

@app.route("/choose-operation", methods=["GET"])
def choose_operation():
    if not DQ_MASTER_FILE or not os.path.exists(DQ_MASTER_FILE):
        return redirect(url_for('landing'))
    
    filename = 'dq_rules_master.xlsx'
    return render_template("choose_operation.html", filename=filename)

@app.route("/add-update-rule", methods=["GET", "POST"])
def add_update_rule():
    if request.method == "GET":
        # Check if master file is uploaded
        if not DQ_MASTER_FILE or not os.path.exists(DQ_MASTER_FILE):
            return redirect(url_for('landing'))
        return render_template("add_update_table.html")
    
    try:
        # Reset log file for this action (delete old, create new)
        reset_log_file()
        
        # Check if master file is uploaded
        if not DQ_MASTER_FILE or not os.path.exists(DQ_MASTER_FILE):
            log_error(logger, "DQ Rules Master file not found")
            return render_template("result.html",
                                 success=False,
                                 message="DQ Rules Master file not found",
                                 error_details="Please upload the DQ Rules Master file first from the home page")
        
        # Extract form data
        ticket = request.form.get("ticket")
        log_section_start(logger, f"Add/Update Rules - Ticket: {ticket}")
        
        # Parse rules from table form (arrays)
        rule_ids = request.form.getlist("rule_id[]")
        descriptions = request.form.getlist("description[]")
        rule_types = request.form.getlist("rule_type[]")
        
        # Build rules list
        rules = []
        for i in range(len(rule_ids)):
            # Skip empty rows
            if not rule_ids[i].strip():
                continue
                
            rule = {
                "ticket": ticket,
                "tenant": "common",  # Always common for add/update
                "ruleid": rule_ids[i].strip(),
                "description": descriptions[i].strip(),
                "action": "add",  # Backend will determine if add or update automatically
                "ruletype": rule_types[i],
                "entity": None,  # Not needed anymore
                "zoneapplied": None,
                "sourceownername": None
            }
            rules.append(rule)
            logger.info(f"📝 Rule {i + 1}: {rule_ids[i]} - {rule_types[i]}")
        
        if not rules:
            log_error(logger, "No rules found in the form")
            return render_template("result.html",
                                 success=False,
                                 message="No rules found in the form",
                                 error_details="Please add at least one rule")
        
        logger.info(f"ℹ️  Total rules to process: {len(rules)}")
        
        # Create a DataFrame from the rules
        rules_df = pd.DataFrame(rules)
        
        # Call the main workflow
        generated_files = main_ui_workflow(DQ_MASTER_FILE, rules_df, "add_update")
        
        logger.info(f"✅ Successfully processed {len(rules)} rule(s)")
        log_separator(logger, "=", 70)
        
        return render_template("result.html",
                             success=True,
                             message=f"Successfully processed {len(rules)} rule(s)",
                             files=generated_files)
    
    except Exception as e:
        error_details = traceback.format_exc()
        log_error(logger, f"Error in add_update_rule: {str(e)}", e)
        return render_template("result.html",
                             success=False,
                             message=f"Error processing rules: {str(e)}",
                             error_details=error_details)

@app.route("/configure-rule", methods=["GET", "POST"])
def configure_rule():
    if request.method == "GET":
        # Check if master file is uploaded
        if not DQ_MASTER_FILE or not os.path.exists(DQ_MASTER_FILE):
            return redirect(url_for('landing'))
        return render_template("configure_table.html")
    
    try:
        # Reset log file for this action (delete old, create new)
        reset_log_file()
        
        # Check if master file is uploaded
        if not DQ_MASTER_FILE or not os.path.exists(DQ_MASTER_FILE):
            log_error(logger, "DQ Rules Master file not found")
            return render_template("result.html",
                                 success=False,
                                 message="DQ Rules Master file not found",
                                 error_details="Please upload the DQ Rules Master file first from the home page")
        
        # Extract form data
        ticket = request.form.get("ticket")
        log_section_start(logger, f"Configure Rules - Ticket: {ticket}")
        
        # Parse configurations from table form (arrays)
        rule_ids = request.form.getlist("rule_id[]")
        tenants = request.form.getlist("tenant[]")
        zones = request.form.getlist("zone[]")
        source_owners = request.form.getlist("source_owner[]")
        
        # Build configurations list
        configs = []
        for i in range(len(rule_ids)):
            # Skip empty rows
            if not rule_ids[i].strip():
                continue
                
            config = {
                "ticket": ticket,
                "tenant": tenants[i],
                "ruleid": rule_ids[i].strip(),
                "description": None,  # Not needed for configure
                "action": "configure",
                "ruletype": None,  # Not needed for configure
                "entity": None,  # Not needed anymore
                "zoneapplied": zones[i],
                "sourceownername": source_owners[i]
            }
            configs.append(config)
            logger.info(f"⚙️  Config {i + 1}: {rule_ids[i]} - {tenants[i]} ({zones[i]})")
        
        if not configs:
            log_error(logger, "No configurations found in the form")
            return render_template("result.html",
                                 success=False,
                                 message="No configurations found in the form",
                                 error_details="Please add at least one configuration")
        
        logger.info(f"ℹ️  Total configurations to process: {len(configs)}")
        
        # Create a DataFrame from the configs
        configs_df = pd.DataFrame(configs)
        
        # Call the main workflow
        generated_files = main_ui_workflow(DQ_MASTER_FILE, configs_df, "configure")
        
        logger.info(f"✅ Successfully processed {len(configs)} configuration(s)")
        log_separator(logger, "=", 70)
        
        return render_template("result.html",
                             success=True,
                             message=f"Successfully processed {len(configs)} configuration(s)",
                             files=generated_files)
    
    except Exception as e:
        error_details = traceback.format_exc()
        log_error(logger, f"Error in configure_rule: {str(e)}", e)
        return render_template("result.html",
                             success=False,
                             message=f"Error processing configurations: {str(e)}",
                             error_details=error_details)


if __name__ == "__main__":
    app.run(debug=True)

