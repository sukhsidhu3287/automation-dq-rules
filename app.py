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
            main(dq_rules_master_path, jira_description_path)
            return render_template("passed.html")
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"Error occurred: {error_details}")
            return render_template("failed.html", error=str(e) + "\n\n" + error_details)

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
