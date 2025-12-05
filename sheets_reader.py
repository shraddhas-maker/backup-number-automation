# sheets_reader.py
import pandas as pd
from config import SHEETS_USE_GSHEETS, CSV_FILES, GSHEETS
import logging

log = logging.getLogger(__name__)

if SHEETS_USE_GSHEETS:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials


def read_csv_or_sheet(key):
    """
    key: one of 'accounts', 'tenant_exceptions', 'region_preferences'
    Reads from Google Sheets (gspread) OR CSV files.
    """

    if SHEETS_USE_GSHEETS:
        creds_path = GSHEETS["credentials_json"]

        # Get the config block inside GSHEETS (accounts / tenant_exceptions / region_preferences)
        cfg = GSHEETS.get(key)
        if not cfg:
            raise KeyError(f"Missing Google Sheet config for key '{key}'")

        spreadsheet_id = cfg.get("spreadsheet_id")
        sheet_name = cfg.get("sheet_name")

        if not spreadsheet_id:
            raise ValueError(f"Spreadsheet ID not configured for key '{key}'")

        # Authenticate with service account
        gc = gspread.service_account(filename=creds_path)

        # Open individual spreadsheet
        sh = gc.open_by_key(spreadsheet_id)

        # Open worksheet by name
        worksheet = sh.worksheet(sheet_name)

        # Fetch all rows
        records = worksheet.get_all_records()
        return pd.DataFrame(records)

    else:
        # Fallback: CSV mode
        path = CSV_FILES.get(key)
        if not path:
            raise FileNotFoundError(f"No CSV path configured for '{key}'")
        return pd.read_csv(path)


def load_accounts():
    df = read_csv_or_sheet("accounts")
    # expected columns: tenant, account_id, status, email (optional)
    df = df.fillna("")
    df = df[df['status'].str.strip().str.lower() == 'yes']
    return df.to_dict(orient='records')


def load_tenant_exceptions():
    df = read_csv_or_sheet("tenant_exceptions")
    # expected columns: tenant, allowed_operators (comma separated)
    ex_map = {}
    for _, r in df.iterrows():
        tenant = str(r.get('tenant')).strip()
        pilots = str(r.get('allowed_operators', '')).strip()
        if tenant:
            ex_map[tenant] = [p.strip() for p in pilots.split(',') if p.strip()]
    return ex_map


def load_region_preferences():
    df = read_csv_or_sheet("region_preferences")
    # expected: region, pilots (comma separated)
    pref = {}
    for _, r in df.iterrows():
        region = str(r.get('region')).strip()
        pilots = str(r.get('pilots', '')).strip()
        if region:
            pref[region] = [p.strip() for p in pilots.split(',') if p.strip()]
    return pref
