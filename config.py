import os

COMMON_PATH = r"C:\Users\VarunMandloi\Downloads\HE_PDM\HE_PDM\hrp.pdm.schema.service\src\main\resources\db\changelog\configdb\changelogs"
HF_PATH = os.path.join(COMMON_PATH, "tenants", "healthfirst")
PEHP_PATH = os.path.join(COMMON_PATH, "tenants", "pehp")
SUTTER_PATH = os.path.join(COMMON_PATH, "tenants", "sutter")
HSYNC_PATH = os.path.join(COMMON_PATH, "tenants", "healthsync")

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
