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
    """
    if SHEETS_USE_GSHEETS:
        creds = GSHEETS["credentials_json"]
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        gc = gspread.service_account(filename=creds)
        sh = gc.open_by_key(GSHEETS["spreadsheet_id"])
        sheet_name = GSHEETS.get(f"{key}_sheet", key)
        worksheet = sh.worksheet(sheet_name)
        records = worksheet.get_all_records()
        return pd.DataFrame(records)
    else:
        path = CSV_FILES.get(key)
        if not path:
            raise FileNotFoundError(f"No CSV path configured for {key}")
        df = pd.read_csv(path)
        return df

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
