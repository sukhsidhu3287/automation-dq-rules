# HE DQ Rules Automation

A Python automation project to generate **Data Quality (DQ) rules CSV and XML files** from Excel inputs with a simple **browser-based frontend**.  

This tool integrates **Excel-based rule definitions** and **Jira descriptions** into structured outputs for automated validation and downstream use.

---

## ✨ Features

- 📂 Reads **DQ Rules Excel** (multi-sheet) and **Jira Description Excel**  
- ⚙️ Generates **CSV and XML outputs** for DQ rules  
- 🌐 Browser-based UI built with **Flask**  
- 🖥️ Upload files, set version, and generate outputs with one click  
- 🔧 Easily extendable for new rule categories or formats  

---

## 📦 Dependencies

The project requires **Python 3.10+** and the following libraries:

- `flask` → Web frontend  
- `pandas` → Excel/CSV handling  
- `sqlalchemy` → Database integration  
- `psycopg2-binary` → PostgreSQL driver (if DB is used)  
- `openpyxl` → Excel reader/writer  

Install dependencies with:

```bash
pip install -r requirements.txt
