# config.py
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
# The script expects to be run by cron. If you want internal scheduling, implement here.
RUN_LOOKBACK_MINUTES = 5  # EndTime = now - this many minutes

# ---------- Sheets / Input ----------
# You can either use Google Sheets (gspread) or CSV files.
SHEETS_USE_GSHEETS = False
# If using CSV, place files here or use absolute path:
CSV_FILES = {
    "accounts": "data/accounts.csv",             # Must have columns: tenant, account_id, status
    "tenant_exceptions": "data/tenant_exceptions.csv",  # tenant, allowed_operators (comma separated)
    "region_preferences": "data/region_preferences.csv" # region, pilots (comma separated)
}

# If using Google Sheets, provide credentials JSON path & sheet names
GSHEETS = {
    "credentials_json": os.getenv("GS_CREDS_JSON", "path/to/gs_creds.json"),
    "accounts_sheet": os.getenv("GS_ACCOUNTS_SHEET", "Accounts"),
    "tenant_exceptions_sheet": os.getenv("GS_TENANT_EXCEPTIONS", "TenantExceptions"),
    "region_preferences_sheet": os.getenv("GS_REGION_PREFERENCES", "RegionPreferences"),
    "spreadsheet_id": os.getenv("GS_SPREADSHEET_ID", "")  # id of spreadsheet
}

# ---------- API ----------
ADD_PN_API = {
    "url": os.getenv("ADD_PN_URL", "https://example.com/addpn.json"),
    "timeout": 15,
    "headers": {"Content-Type": "application/json"},
    # If auth required, set authorization header or other fields here
    # "headers": {"Authorization": "Bearer <token>", ...}
}

# ---------- Email ----------
SMTP = {
    "host": os.getenv("SMTP_HOST", "smtp.example.com"),
    "port": int(os.getenv("SMTP_PORT", 587)),
    "user": os.getenv("SMTP_USER", "user@example.com"),
    "password": os.getenv("SMTP_PASS", "smtp_password"),
    "from_addr": os.getenv("SMTP_FROM", "no-reply@example.com"),
    "use_tls": True
}
EMAIL = {
    "admin_to": os.getenv("ADMIN_EMAIL", "ops@example.com"),
    "subject_prefix": "[BackupPNs]"
}

# ---------- Logging ----------
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_ROTATE_MB = 10

# ---------- Business rules ----------
# number of PN per pilot per VN (example rule)
PN_PER_PILOT_PER_VN = int(os.getenv("PN_PER_PILOT_PER_VN", 1))

# DBS table names (change if your schema uses different names)
TABLES = {
    "purchased_numbers": os.getenv("TBL_PURCHASED", "IncomingPhoneNumber"),  # VNs purchased
    "pri": os.getenv("TBL_PRI", "Pri"),                                   # pri table with pilot status
    "available_pns": os.getenv("TBL_AVAILABLE_PNS", "AvailablePhoneNumber"), # PNs pool
    "outgoing_calls": os.getenv("TBL_OUTGOING", "OutgoingCallerIds")      # outgoing caller IDs,
}
