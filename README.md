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
