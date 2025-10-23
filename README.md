# HE DQ Rules Automation

A Python automation project to generate **Data Quality (DQ) rules CSV and XML files** with a **modern browser-based UI**.

This tool provides an intuitive UI to add, update, and configure DQ rules without needing manual Excel files for rule definitions.

---

## ✨ Features

- 🚀 **Modern UI-Driven Workflow** - No need for Jira Description Excel files
- 📂 Reads **DQ Rules Master Excel** (all sheets automatically loaded)
- 🔄 **Two Operation Modes:**
  - **Add/Update DQ Rules** - Create or modify validation rules
  - **Configure DQ Rules** - Set up tenant-specific configurations
- ⚙️ Generates **CSV and XML outputs** for DQ rules
- 🌐 Beautiful browser-based UI built with **Flask**
- 📝 Form-based data entry with validation
- 🎯 **Auto-detection** of sheet names based on Rule IDs
- 🔧 Easily extendable for new rule categories or formats

---

## 🎯 How It Works

### New UI-Driven Workflow

1. **Upload DQ Rules Master File** - Upload your master Excel file containing all rule sheets
2. **Choose Operation:**
   - **Add/Update DQ Rule** - For creating or updating rules in the common repository
   - **Configure DQ Rule** - For tenant-specific rule configurations
3. **Fill in Form** - Enter rule details directly in the browser
4. **Submit** - Process and generate CSV/XML files automatically

### What's Changed from Old Version

✅ **No more Jira Description Excel needed** - All data entered via UI
✅ **No Entity field required** - Sheet names auto-detected from Rule IDs
✅ **No Tenant field for Add/Update** - Always goes to common
✅ **Standardized column names** - Automatic handling of column variations
✅ **Multi-rule support** - Add multiple rules in one submission

---

## 📋 Operation Modes

### 1. Add/Update DQ Rule

Use this mode to create new rules or update existing ones in the **common** repository.

**Required Fields:**

- **Ticket Number** - Jira ticket (e.g., `PDM-12345`)
- **Rule ID** - Unique rule identifier (e.g., `DQ1442`)
- **Description** - Rule description
- **Action** - Select from dropdown: `Add` or `Update`
- **Rule Type** - Select from dropdown

**Notes:**

- No tenant required (always common)
- No entity field needed (auto-detected from Rule ID)
- Can add multiple rules in one go

### 2. Configure DQ Rule

Use this mode to configure rules for specific tenants and zones.

**Required Fields:**

- **Ticket Number** - Jira ticket (e.g., `PDM-12345`)
- **Rule ID** - Rule to configure (e.g., `DQ1442`)
- **Tenant** - Select from dropdown
- **Zone Applied** - Select from dropdown
- **Source Owner** - Select from dropdown

**Notes:**

- Can configure multiple rules for different tenants at once
- Sheet names auto-detected from Rule IDs

---

## 📂 Input Files

### DQ Rules Master Excel

This is the **only** Excel file you need to upload. It should contain:

- **Multiple sheets** with rule definitions
- **Standardized columns** (variations automatically handled):
  - `RuleID` or `Rule ID`
  - `Rule Name`
  - `Rule Description`
  - `Rule Category` or `Rule Type`
  - `Entity`
  - `Sub Entity` or `Sub-Entity`
  - `Table Name`
  - `Column Name`
  - `Enforcement Level`
  - And other rule-specific fields

**All sheets are automatically loaded** - no need to specify sheet names manually!

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

### 4. **Sheet Name Auto-Detection**

**No manual sheet mapping required!** The application automatically:

- Loads all sheets from the DQ Rules Master Excel
- Searches through all sheets to find Rule IDs
- Detects the correct sheet for each rule automatically

### 5. **Verifying Configuration**

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

## 🚀 How to Run

### 1. Configure the Application

Before running, update `config.py` with your database credentials and project paths (see Configuration section above).

### 2. Start the Flask Server

```bash
python app.py
```

The server will start on `http://localhost:5000` by default.

### 3. Access the Web Interface

Open your browser and navigate to:

```
http://localhost:5000
```

### 4. Upload Master File

1. On the landing page, scroll to the **Upload DQ Rules Master Excel** section
2. Select your DQ Rules Master Excel file
3. Click **Upload Master File**
4. Wait for confirmation

### 5. Choose Operation

Click one of the two options:

#### Option A: Add/Update DQ Rule

1. Click **Add/Update DQ Rule** button
2. Enter the **Ticket Number**
3. Fill in rule details:
   - Rule ID
   - Description
   - Action (Add/Update from dropdown)
   - Rule Type (from dropdown)
4. Click **+ Add Another Rule** to add more rules (optional)
5. Click **Submit All Rules**

#### Option B: Configure DQ Rule

1. Click **Configure DQ Rule** button
2. Enter the **Ticket Number**
3. Fill in configuration details:
   - Rule ID
   - Tenant (from dropdown)
   - Zone Applied (from dropdown)
   - Source Owner (from dropdown)
4. Click **+ Add Another Configuration** to add more (optional)
5. Click **Submit All Configurations**

### 6. View Results

After submission:

- ✅ **Success** - View list of generated CSV/XML files
- ❌ **Error** - View error details and try again

---
