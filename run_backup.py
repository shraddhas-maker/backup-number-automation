#!/usr/bin/env python3
"""
Backup PN Automation - Combined Single File
Automatically adds backup phone numbers to purchased virtual numbers
"""

import os
import sys
import json
import logging
import requests
import smtplib
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from mysql.connector import pooling

# ============================================================
# CONFIGURATION
# ============================================================

class Config:
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

    # ---------- Google Sheets ----------
    SHEETS_USE_GSHEETS = os.getenv("SHEETS_USE_GSHEETS", "true").lower() == "true"
    
    GSHEETS = {
        "credentials_json": os.getenv("GS_CREDS_JSON", "path/to/gs_creds.json"),
        
        # Sheet configurations for different categories
        "mum_move_ahead": {
            "spreadsheet_id": os.getenv("GS_MUM_MOVE_AHEAD_ID", ""),
            "sheet_name": os.getenv("GS_MUM_MOVE_AHEAD_SHEET", "MUM_MoveAhead"),
        },
        "sig_move_ahead": {
            "spreadsheet_id": os.getenv("GS_SIG_MOVE_AHEAD_ID", ""),
            "sheet_name": os.getenv("GS_SIG_MOVE_AHEAD_SHEET", "SIG_MoveAhead"),
        },
        "mum_whitelist": {
            "spreadsheet_id": os.getenv("GS_MUM_WHITELIST_ID", ""),
            "sheet_name": os.getenv("GS_MUM_WHITELIST_SHEET", "MUM_Whitelist"),
        },
        "sig_whitelist": {
            "spreadsheet_id": os.getenv("GS_SIG_WHITELIST_ID", ""),
            "sheet_name": os.getenv("GS_SIG_WHITELIST_SHEET", "SIG_Whitelist"),
        },
        "mum_sp": {
            "spreadsheet_id": os.getenv("GS_MUM_SP_ID", ""),
            "sheet_name": os.getenv("GS_MUM_SP_SHEET", "MUM_SP"),
        },
        "sig_sp": {
            "spreadsheet_id": os.getenv("GS_SIG_SP_ID", ""),
            "sheet_name": os.getenv("GS_SIG_SP_SHEET", "SIG_SP"),
        },
        "region_pilots": {
            "spreadsheet_id": os.getenv("GS_REGION_PILOTS_ID", ""),
            "sheet_name": os.getenv("GS_REGION_PILOTS_SHEET", "RegionPilots"),
        },
        "tenant_exceptions": {
            "spreadsheet_id": os.getenv("GS_TENANT_EXCEPTIONS_ID", ""),
            "sheet_name": os.getenv("GS_TENANT_EXCEPTIONS_SHEET", "TenantExceptions"),
        },
    }

    # ---------- CSV fallback ----------
    CSV_FILES = {
        "mum_move_ahead": os.getenv("CSV_MUM_MOVE_AHEAD", "data/mum_move_ahead.csv"),
        "sig_move_ahead": os.getenv("CSV_SIG_MOVE_AHEAD", "data/sig_move_ahead.csv"),
        "mum_whitelist": os.getenv("CSV_MUM_WHITELIST", "data/mum_whitelist.csv"),
        "sig_whitelist": os.getenv("CSV_SIG_WHITELIST", "data/sig_whitelist.csv"),
        "mum_sp": os.getenv("CSV_MUM_SP", "data/mum_sp.csv"),
        "sig_sp": os.getenv("CSV_SIG_SP", "data/sig_sp.csv"),
        "region_pilots": os.getenv("CSV_REGION_PILOTS", "data/region_pilots.csv"),
        "tenant_exceptions": os.getenv("CSV_TENANT_EXCEPTIONS", "data/tenant_exceptions.csv"),
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
        "pvm": os.getenv("TBL_PVM", "PhysicalVirtualMap"),
    }


# ============================================================
# LOGGING SETUP
# ============================================================

def setup_logging():
    os.makedirs(Config.LOG_DIR, exist_ok=True)
    log_file = os.path.join(Config.LOG_DIR, "backup_run.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # File handler with rotation
    fh = RotatingFileHandler(
        log_file, 
        maxBytes=Config.LOG_ROTATE_MB * 1024 * 1024, 
        backupCount=5
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


# ============================================================
# DATABASE CONNECTION POOL
# ============================================================

class DatabasePool:
    pool = None
    
    @classmethod
    def init_pool(cls, pool_size=5):
        if cls.pool is None:
            cls.pool = pooling.MySQLConnectionPool(
                pool_name="backup_pn_pool",
                pool_size=pool_size,
                **Config.DB
            )
    
    @classmethod
    def get_conn(cls):
        if cls.pool is None:
            cls.init_pool()
        return cls.pool.get_connection()
    
    @classmethod
    def fetchall(cls, query, params=()):
        query = query.format(**Config.TABLES)
        cnx = cls.get_conn()
        cur = cnx.cursor(dictionary=True)
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        cnx.close()
        return rows
    
    @classmethod
    def fetchone(cls, query, params=()):
        query = query.format(**Config.TABLES)
        cnx = cls.get_conn()
        cur = cnx.cursor(dictionary=True)
        cur.execute(query, params)
        row = cur.fetchone()
        cur.close()
        cnx.close()
        return row
    
    @classmethod
    def execute(cls, query, params=(), commit=True):
        query = query.format(**Config.TABLES)
        cnx = cls.get_conn()
        cur = cnx.cursor()
        cur.execute(query, params)
        if commit:
            cnx.commit()
        lastrow = cur.lastrowid
        cur.close()
        cnx.close()
        return lastrow


# ============================================================
# GOOGLE SHEETS READER
# ============================================================

class SheetsReader:
    
    @staticmethod
    def read_csv_or_sheet(key):
        """
        Read data from Google Sheets or CSV fallback
        key: one of the sheet types (mum_move_ahead, sig_move_ahead, etc.)
        """
        log = logging.getLogger(__name__)
        
        if Config.SHEETS_USE_GSHEETS:
            try:
                import gspread
                
                creds_path = Config.GSHEETS["credentials_json"]
                cfg = Config.GSHEETS.get(key)
                
                if not cfg:
                    raise KeyError(f"Missing Google Sheet config for key '{key}'")
                
                spreadsheet_id = cfg.get("spreadsheet_id")
                sheet_name = cfg.get("sheet_name")
                
                if not spreadsheet_id:
                    log.warning(f"Spreadsheet ID not configured for '{key}', returning empty DataFrame")
                    return pd.DataFrame()
                
                # Authenticate with service account
                gc = gspread.service_account(filename=creds_path)
                sh = gc.open_by_key(spreadsheet_id)
                worksheet = sh.worksheet(sheet_name)
                records = worksheet.get_all_records()
                
                return pd.DataFrame(records)
                
            except Exception as e:
                log.error(f"Failed to read Google Sheet '{key}': {e}")
                return pd.DataFrame()
        else:
            # Fallback: CSV mode
            path = Config.CSV_FILES.get(key)
            if not path or not os.path.exists(path):
                log.warning(f"CSV file not found for '{key}': {path}")
                return pd.DataFrame()
            return pd.read_csv(path)
    
    @staticmethod
    def load_active_accounts():
        """Load accounts from move_ahead sheets (active customers)"""
        log = logging.getLogger(__name__)
        
        # Load from move_ahead sheets (primary action sheets)
        mum_df = SheetsReader.read_csv_or_sheet("mum_move_ahead")
        sig_df = SheetsReader.read_csv_or_sheet("sig_move_ahead")
        
        # Combine both dataframes
        all_accounts = []
        
        for df, source in [(mum_df, "MUM"), (sig_df, "SIG")]:
            if df.empty:
                continue
            
            df = df.fillna("")
            # Filter active accounts (status = 'yes' or similar)
            if 'status' in df.columns:
                df = df[df['status'].str.strip().str.lower().isin(['yes', 'active', 'enabled'])]
            
            for _, row in df.iterrows():
                account_sid = str(row.get('AccountSid', row.get('account_sid', ''))).strip()
                if account_sid:
                    all_accounts.append({
                        'AccountSid': account_sid,
                        'source': source,
                        'email': str(row.get('email', '')).strip()
                    })
        
        log.info(f"Loaded {len(all_accounts)} active accounts from move_ahead sheets")
        return all_accounts
    
    @staticmethod
    def load_region_pilots():
        """Load region-wise pilot mappings"""
        log = logging.getLogger(__name__)
        df = SheetsReader.read_csv_or_sheet("region_pilots")
        
        if df.empty:
            log.warning("No region-pilot mappings found")
            return {}
        
        region_map = {}
        for _, row in df.iterrows():
            region = str(row.get('region', row.get('Region', ''))).strip()
            pilots = str(row.get('pilots', row.get('Pilots', ''))).strip()
            
            if region and pilots:
                # Split comma-separated pilots
                pilot_list = [p.strip() for p in pilots.split(',') if p.strip()]
                region_map[region] = pilot_list
        
        log.info(f"Loaded {len(region_map)} region-pilot mappings")
        return region_map
    
    @staticmethod
    def load_tenant_exceptions():
        """Load tenant-specific operator exceptions"""
        log = logging.getLogger(__name__)
        df = SheetsReader.read_csv_or_sheet("tenant_exceptions")
        
        if df.empty:
            log.warning("No tenant exceptions found")
            return {}
        
        exception_map = {}
        for _, row in df.iterrows():
            tenant = str(row.get('tenant', row.get('AccountSid', ''))).strip()
            allowed_ops = str(row.get('allowed_operators', row.get('pilots', ''))).strip()
            
            if tenant and allowed_ops:
                # Split comma-separated operators/pilots
                ops_list = [o.strip() for o in allowed_ops.split(',') if o.strip()]
                exception_map[tenant] = ops_list
        
        log.info(f"Loaded {len(exception_map)} tenant exceptions")
        return exception_map


# ============================================================
# EMAIL SENDER
# ============================================================

class EmailSender:
    
    @staticmethod
    def send_email(to_addrs, subject, body):
        log = logging.getLogger(__name__)
        
        msg = MIMEText(body, "plain")
        msg['Subject'] = f"{Config.EMAIL['subject_prefix']} {subject}"
        msg['From'] = Config.SMTP['from_addr']
        msg['To'] = ", ".join(to_addrs if isinstance(to_addrs, (list, tuple)) else [to_addrs])
        
        try:
            server = smtplib.SMTP(Config.SMTP['host'], Config.SMTP['port'], timeout=20)
            if Config.SMTP.get('use_tls'):
                server.starttls()
            if Config.SMTP.get('user'):
                server.login(Config.SMTP['user'], Config.SMTP['password'])
            
            server.sendmail(
                Config.SMTP['from_addr'],
                to_addrs if isinstance(to_addrs, list) else [to_addrs],
                msg.as_string()
            )
            server.quit()
            log.info(f"Email sent to {to_addrs}")
            return True
        except Exception as e:
            log.exception(f"Failed to send email: {e}")
            return False


# ============================================================
# UTILITIES
# ============================================================

def compute_time_window(lookback_minutes):
    """
    Compute time window for querying purchased numbers
    Start: Beginning of current day (00:00 UTC)
    End: Current time minus lookback_minutes
    """
    now = datetime.utcnow()
    end_time = now - timedelta(minutes=lookback_minutes)
    start_of_day = datetime(now.year, now.month, now.day)
    return start_of_day, end_time


# ============================================================
# CORE BUSINESS LOGIC
# ============================================================

class BackupPNProcessor:
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
    
    def get_purchased_vns_for_tenant(self, tenant_id, start_time, end_time):
        """Fetch purchased virtual numbers for a tenant within time window"""
        query = """
        SELECT 
            a.sid,
            a.PhoneNumber AS vn_number,
            b.Region
        FROM {purchased_numbers} a
        JOIN {available_pns} b
            ON a.PhoneNumber = b.PhoneNumber
        WHERE 
            a.AccountSid = %s
            AND a.DateCreated >= %s
            AND a.DateCreated <= %s
        """
        
        rows = DatabasePool.fetchall(query, (tenant_id, start_time, end_time))
        self.log.info(f"Found {len(rows)} purchased VNs for tenant {tenant_id}")
        return rows
    
    def is_pilot_active(self, pilot):
        """Check if pilot is active in PRI table"""
        query = """
        SELECT pilot, state
        FROM {pri}
        WHERE pilot = %s
        """
        
        row = DatabasePool.fetchone(query, (pilot,))
        if not row:
            self.log.warning(f"Pilot {pilot} not found in PRI table")
            return False
        
        state = str(row.get("state", "")).strip().lower()
        active_states = ("up", "active", "online")
        is_active = state in active_states
        
        self.log.info(f"Pilot {pilot} state: {state} (active: {is_active})")
        return is_active
    
    def fetch_available_pns_for_pilot(self, pilot, region, needed):
        """Fetch available phone numbers from a specific pilot and region"""
        query = """
        SELECT 
            a.sid,
            a.PhoneNumber AS pn,
            a.Region,
            a._Pri
        FROM {available_pns} a
        WHERE 
            a.AccountSid IS NULL
            AND a._state = 'pn'
            AND a.Rental < 0
            AND a.Region = %s
            AND a.PhoneNumber NOT IN (
                SELECT PhysicalNumber FROM {pvm}
            )
            AND a._Pri IN (
                SELECT id FROM {pri}
                WHERE pilot = %s
                AND state = 'active'
            )
        LIMIT %s
        """
        
        rows = DatabasePool.fetchall(query, (region, pilot, needed))
        self.log.info(f"Found {len(rows)} available PNs from pilot {pilot} in region {region}")
        return rows
    
    def call_addpn_api(self, vn, pn, tenant_id):
        """Call the ADD PN API to attach physical number to virtual number"""
        payload = {
            "vn": vn,
            "pn": pn,
            "tenant_id": tenant_id
        }
        
        try:
            response = requests.post(
                Config.ADD_PN_API["url"],
                json=payload,
                headers=Config.ADD_PN_API.get("headers", {}),
                timeout=Config.ADD_PN_API.get("timeout", 15)
            )
            
            if response.status_code in (200, 201):
                self.log.info(f"Successfully added PN {pn} to VN {vn}")
                return True, (response.json() if response.content else {})
            else:
                self.log.error(f"API failed for PN {pn} to VN {vn}: {response.status_code}")
                return False, {"status_code": response.status_code, "text": response.text}
        
        except Exception as e:
            self.log.exception(f"Exception calling ADD PN API: {e}")
            return False, {"error": str(e)}
    
    def process_tenant(self, tenant_record, tenant_exceptions, region_pilots, start_time, end_time):
        """Main processing logic for a single tenant"""
        tenant_id = tenant_record.get("AccountSid")
        self.log.info(f"=" * 60)
        self.log.info(f"Processing tenant: {tenant_id}")
        
        # Step 1: Get purchased VNs
        purchased_vns = self.get_purchased_vns_for_tenant(tenant_id, start_time, end_time)
        
        if not purchased_vns:
            self.log.info(f"No VNs purchased for tenant {tenant_id} in the window")
            return None
        
        results = []
        
        # Step 2: Process each purchased VN
        for vn in purchased_vns:
            vn_number = vn["vn_number"]
            region = vn.get("Region")
            
            vn_result = {
                "vn": vn_number,
                "region": region,
                "assigned": [],
                "warnings": []
            }
            
            self.log.info(f"Processing VN: {vn_number} (Region: {region})")
            
            # Step 3: Determine pilots to use
            # Check if tenant has operator exceptions
            if tenant_id in tenant_exceptions:
                pilots = tenant_exceptions[tenant_id]
                self.log.info(f"Using tenant exception pilots: {pilots}")
            else:
                # Use region-based pilots
                pilots = region_pilots.get(region, [])
                self.log.info(f"Using region pilots for {region}: {pilots}")
            
            if not pilots:
                vn_result["warnings"].append(f"No pilots configured for region {region}")
                results.append(vn_result)
                continue
            
            # Step 4: Add backup PNs from each pilot
            needed_per_pilot = Config.PN_PER_PILOT_PER_VN
            
            for pilot in pilots:
                # Check if pilot is active
                if not self.is_pilot_active(pilot):
                    vn_result["warnings"].append(f"Pilot {pilot} is inactive/dead")
                    continue
                
                # Fetch available PNs from this pilot
                available_pns = self.fetch_available_pns_for_pilot(pilot, region, needed_per_pilot)
                
                if not available_pns:
                    vn_result["warnings"].append(f"No available PNs from pilot {pilot}")
                    continue
                
                # Add each PN via API
                for pn_row in available_pns:
                    pn = pn_row["pn"]
                    
                    success, api_response = self.call_addpn_api(vn_number, pn, tenant_id)
                    
                    if success:
                        vn_result["assigned"].append({
                            "pn": pn,
                            "pilot": pilot,
                            "api_response": api_response
                        })
                        self.log.info(f"✓ Added PN {pn} from pilot {pilot} to VN {vn_number}")
                    else:
                        vn_result["warnings"].append(
                            f"Failed to add PN {pn} from pilot {pilot}: {api_response}"
                        )
            
            results.append(vn_result)
        
        return results


# ============================================================
# EMAIL REPORT BUILDER
# ============================================================

def build_email_report(tenant, results):
    """Build detailed email report for tenant"""
    if not results:
        return f"No purchased VNs for tenant {tenant.get('AccountSid')}"
    
    lines = [
        "=" * 70,
        f"Backup PN Assignment Report",
        f"Tenant: {tenant.get('AccountSid')}",
        f"Source: {tenant.get('source')}",
        f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        "=" * 70,
        ""
    ]
    
    total_vns = len(results)
    total_assigned = sum(len(r["assigned"]) for r in results)
    total_warnings = sum(len(r["warnings"]) for r in results)
    
    lines.append(f"Summary:")
    lines.append(f"  - Virtual Numbers Processed: {total_vns}")
    lines.append(f"  - Backup PNs Added: {total_assigned}")
    lines.append(f"  - Warnings: {total_warnings}")
    lines.append("")
    
    for idx, r in enumerate(results, 1):
        lines.append(f"{idx}. Virtual Number: {r['vn']}")
        lines.append(f"   Region: {r['region']}")
        
        if r["assigned"]:
            lines.append(f"   Assigned Backup PNs ({len(r['assigned'])}):")
            for a in r["assigned"]:
                lines.append(f"      • {a['pn']} (Pilot: {a['pilot']})")
        else:
            lines.append(f"   ⚠ No backup PNs assigned")
        
        if r["warnings"]:
            lines.append(f"   Warnings:")
            for w in r["warnings"]:
                lines.append(f"      ⚠ {w}")
        
        lines.append("")
    
    return "\n".join(lines)


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    setup_logging()
    log = logging.getLogger("main")
    
    log.info("=" * 70)
    log.info("Starting Backup PN Automation")
    log.info("=" * 70)
    
    # Initialize database pool
    DatabasePool.init_pool(pool_size=5)
    
    # Compute time window
    start_time, end_time = compute_time_window(Config.RUN_LOOKBACK_MINUTES)
    log.info(f"Time Window: {start_time} to {end_time}")
    
    # Load configuration data
    log.info("Loading configuration data from sheets...")
    accounts = SheetsReader.load_active_accounts()
    region_pilots = SheetsReader.load_region_pilots()
    tenant_exceptions = SheetsReader.load_tenant_exceptions()
    
    if not accounts:
        log.warning("No active accounts found. Exiting.")
        return
    
    # Initialize processor
    processor = BackupPNProcessor()
    
    # Process each tenant
    success_count = 0
    error_count = 0
    
    for account in accounts:
        try:
            log.info("")
            results = processor.process_tenant(
                account,
                tenant_exceptions,
                region_pilots,
                start_time,
                end_time
            )
            
            # Build and send email report
            email_body = build_email_report(account, results)
            
            to_addr = account.get("email") or Config.EMAIL["admin_to"]
            EmailSender.send_email(
                [to_addr],
                f"Backup PN Report - {account.get('AccountSid')}",
                email_body
            )
            
            success_count += 1
            
        except Exception as e:
            error_count += 1
            log.exception(f"Error processing tenant {account.get('AccountSid')}: {e}")
            EmailSender.send_email(
                [Config.EMAIL["admin_to"]],
                f"ERROR: Backup PN Job Failed - {account.get('AccountSid')}",
                f"Error processing tenant {account.get('AccountSid')}:\n\n{str(e)}"
            )
    
    # Final summary
    log.info("")
    log.info("=" * 70)
    log.info("Backup PN Automation Completed")
    log.info(f"Success: {success_count} | Errors: {error_count}")
    log.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error in main execution: {e}")
        sys.exit(1)