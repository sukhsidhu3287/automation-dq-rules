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

## 📋 Input Files

### Jira Description Excel (`jira_description.xlsx`)

This file defines which rules to process and how. It should contain the following columns:

| Column Name               | Description                              | Example Values                                                                        |
| ------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------- |
| **Action**          | Type of action to perform                | `add`, `update`, `configure`                                                    |
| **Ticket**          | Jira ticket number                       | `PDM-1234`                                                                          |
| **RuleID**          | Rule ID from DQ rules master file        | `DQ1201`, `DQ1202`                                                                |
| **Entity**          | Entity type                              | `org`, `pract`                                                                    |
| **RuleType**        | Type of rule                             | `MANDATORY`, `query`                                                              |
| **Tenant**          | Tenant name (only for configure actions) | `c - common`, `hf - healthfirst`, `p -php`, `s - sutter`, `hs - healthsync` |
| **ZoneApplied**     | Zone where rule is applied               | `STAGING`, `CORE`                                                                 |
| **SourceOwnerName** | Source owner name                        | `HRP`, `ROATER`                                                                   |

#### Action Types:

- **`add`** - Add a new rule to the system
- **`update`** - Update an existing rule
- **`configure`** - Configure tenant-specific for rule extn table

---

## ⚙️ Configuration

### How to Change Configuration According to Your Needs

All configuration settings are centralized in the `config.py` file. Update this file to match your local environment and project structure.

### 1. **Database Configuration**

Update the database connection parameters:

```python
# DB ENVIRONMENT VARIABLES
USER = "postgres"          # Your database username
PASSWORD = "postgres"      # Your database password
HOST = "localhost"         # Database host
PORT = "5434"             # Database port
DB = "postgres"           # Database name
```

The `ENGINE` variable is automatically created from these parameters using SQLAlchemy.

### 2. **Project Path Configuration**

Update the `COMMON_PATH` to point to your local project directory:

```python
COMMON_PATH = path to your "hrp.pdm.schema.service\src\main\resources\db\changelog\configdb\changelogs" Folder
```

**Important:**

- Replace `YOUR_USERNAME` with your actual username
- Ensure this directory exists on your system
- Use raw strings (prefix with `r`) for Windows paths to handle backslashes correctly

### 3. **Tenant Configuration**

The application supports multiple tenants. Tenant paths are automatically generated from `COMMON_PATH`:

```python
TENANT_DEV_FILE_PATHS = {
    "c": COMMON_PATH,                                    # Common
    "hf": os.path.join(COMMON_PATH, "tenants", "healthfirst"),  # HealthFirst
    "p": os.path.join(COMMON_PATH, "tenants", "pehp"),          # PEHP
    "s": os.path.join(COMMON_PATH, "tenants", "sutter"),        # Sutter
    "hs": os.path.join(COMMON_PATH, "tenants", "healthsync"),   # HealthSync
}
```

**Data folder paths** are auto-generated with `/data` appended to each tenant path.

### 4. **Tenant Names Mapping**

The `TENANT_NAMES` dictionary maps short codes to full tenant names:

```python
TENANT_NAMES = {
    "hf": "healthfirst",
    "p": "pehp",
    "s": "sutter",
    "hs": "healthsync",
}
```

### 5. **Sheet Name Mapping**

The `SHEET_NAME` dictionary maps entity codes to Excel sheet names:

```python
SHEET_NAME = {
    "org": "Organization",
    "prv": "Provider Network",
    "pract": "Practitioner",
}
```

If your DQ Rules Excel file has different sheet names, update this mapping accordingly.

### 6. **Verifying Configuration**

When you run the application, it will:

- ✅ Check if `COMMON_PATH` exists
- ⚠️ Display a warning if the path is not found
- 📝 Suggest updating the path in `config.py`

**Note:** The `config.py` file is in `.gitignore` to prevent sensitive credentials from being committed to version control.

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
```

---
