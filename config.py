import os
from sqlalchemy import create_engine

# NOTE: Update this path to match your local project structure
COMMON_PATH = r"c:\Users\sukhd\Projects\HE_PDM_code\hrp.pdm.schema.service\src\main\resources\db\changelog\configdb\changelogs"

# Validate that the base path exists - if not, provide helpful error message
if not os.path.exists(COMMON_PATH):
    print(f"WARNING: COMMON_PATH does not exist: {COMMON_PATH}")
    print(f"Please update the COMMON_PATH in config.py to match your local project structure.")
    print(f"The application will attempt to create the necessary directories, but file operations may fail.")
HF_PATH = os.path.join(COMMON_PATH, "tenants", "healthfirst")
PEHP_PATH = os.path.join(COMMON_PATH, "tenants", "pehp")
SUTTER_PATH = os.path.join(COMMON_PATH, "tenants", "sutter")
HSYNC_PATH = os.path.join(COMMON_PATH, "tenants", "healthsync")

# DB ENVIRONMENT VARIABLES
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = "5434"
DB = "postgres"

ENGINE = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

TENANT_DEV_FILE_PATHS = {
    "c": COMMON_PATH,
    "hf": HF_PATH,
    "p": PEHP_PATH,
    "s": SUTTER_PATH,
    "hs": HSYNC_PATH,
}

TENANT_DATA_FOLDER_PATHS = {
    k: os.path.join(v, "data") for k, v in TENANT_DEV_FILE_PATHS.items()
}

TENANT_NAMES = {
    "hf": "healthfirst",
    "p": "pehp",
    "s": "sutter",
    "hs": "healthsync",
}

SHEET_NAME = {
    "org": "Organization",
    "prv": "Provider Network",
    "pract": "Practitioner",
}