import os
from sqlalchemy import create_engine

# Update this path to match your local project structure
COMMON_PATH = r"c:\Users\sukhd\Projects\HE_PDM_code\hrp.pdm.schema.service\src\main\resources\db\changelog\configdb\changelogs"

# Validate that the base path exists - if not, provide helpful error message
if not os.path.exists(COMMON_PATH):
    print(f"WARNING: COMMON_PATH does not exist: {COMMON_PATH}")
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
    "common": COMMON_PATH,
    "healthfirst": HF_PATH,
    "pehp": PEHP_PATH,
    "sutter": SUTTER_PATH,
    "healthsync": HSYNC_PATH,
}

TENANT_DATA_FOLDER_PATHS = {
    k: os.path.join(v, "data") for k, v in TENANT_DEV_FILE_PATHS.items()
}

# Note: Sheet names are now auto-detected based on Rule IDs in the master file
# No need for manual SHEET_NAME mapping anymore