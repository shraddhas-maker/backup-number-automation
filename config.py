import os
from datetime import timedelta

# ---------- Database ----------
DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "your_user"),
    "password": os.getenv("DB_PASS", "your_pass"),
    "database": os.getenv("DB_NAME", "your_db"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "connect_timeout": 10,
}

# ---------- Scheduler ----------
RUN_LOOKBACK_MINUTES = 5  # minutes to look back from "now"

# ---------- Sheets / Input ----------
# Use Google Sheets? If false, script will use CSV fallback.
SHEETS_USE_GSHEETS = os.getenv("SHEETS_USE_GSHEETS", "false").lower() == "true"

GSHEETS = {
    "credentials_json": os.getenv("GS_CREDS_JSON", "path/to/gs_creds.json"),

    # Accounts Google Sheet
    "accounts": {
        "spreadsheet_id": os.getenv("GS_ACCOUNTS_SHEET_ID", ""),
        "sheet_name": os.getenv("GS_ACCOUNTS_SHEET_NAME", "Accounts"),
    },

    # Tenant Exceptions Sheet
    "tenant_exceptions": {
        "spreadsheet_id": os.getenv("GS_TENANT_EXCEPTIONS_ID", ""),
        "sheet_name": os.getenv("GS_TENANT_EXCEPTIONS_SHEET_NAME", "TenantExceptions"),
    },

    # Region Preferences Sheet
    "region_preferences": {
        "spreadsheet_id": os.getenv("GS_REGION_PREFERENCES_ID", ""),
        "sheet_name": os.getenv("GS_REGION_PREFERENCES_SHEET_NAME", "RegionPreferences"),
    },
}

# ---------- CSV fallback ----------
CSV_FILES = {
    "accounts": os.getenv("CSV_ACCOUNTS", "data/accounts.csv"),
    "tenant_exceptions": os.getenv("CSV_TENANT_EXCEPTIONS", "data/tenant_exceptions.csv"),
    "region_preferences": os.getenv("CSV_REGION_PREFERENCES", "data/region_preferences.csv"),
}

# ---------- External API ----------
ADD_PN_API = {
    "url": os.getenv("ADD_PN_URL", "https://example.com/addpn.json"),
    "timeout": 15,
    "headers": {"Content-Type": "application/json"},
}

# ---------- Email ----------
SMTP = {
    "host": os.getenv("SMTP_HOST", "smtp.example.com"),
    "port": int(os.getenv("SMTP_PORT", 587)),
    "user": os.getenv("SMTP_USER", "user@example.com"),
    "password": os.getenv("SMTP_PASS", "smtp_password"),
    "from_addr": os.getenv("SMTP_FROM", "no-reply@example.com"),
    "use_tls": True,
}

EMAIL = {
    "admin_to": os.getenv("ADMIN_EMAIL", "ops@example.com"),
    "subject_prefix": "[BackupPNs]",
}

# ---------- Logging ----------
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_ROTATE_MB = 10

# ---------- Business rules ----------
PN_PER_PILOT_PER_VN = int(os.getenv("PN_PER_PILOT_PER_VN", 1))

# ---------- Database Table Names ----------
TABLES = {
    "purchased_numbers": os.getenv("TBL_PURCHASED", "IncomingPhoneNumber"),
    "pri": os.getenv("TBL_PRI", "Pri"),
    "available_pns": os.getenv("TBL_AVAILABLE_PNS", "AvailablePhoneNumber"),
    "outgoing_calls": os.getenv("TBL_OUTGOING", "OutgoingCallerIds"),
    "pvm": os.getenv("TBL_PVM", "PhysicalVirtualMap"),
}
