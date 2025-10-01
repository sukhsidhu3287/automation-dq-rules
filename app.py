from flask import Flask, render_template, request
import os
from main import main   

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Save uploaded files
        dq_file = request.files["dq_excel"]
        jira_file = request.files["jira_excel"]

        dq_rules_master_path = os.path.join(UPLOAD_FOLDER, "dq_rules_master.xlsx")
        jira_description_path = os.path.join(UPLOAD_FOLDER, "jira_description.xlsx")

        dq_file.save(dq_rules_master_path)
        jira_file.save(jira_description_path)

        try:
            csv_file_path, xml_file_path = main(dq_rules_master_path, jira_description_path)
            return render_template("result.html", csv_file=csv_file_path, xml_file=xml_file_path)
        except Exception as e:
            return f"Error generating files: {e}", 400

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
