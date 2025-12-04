# run_backup.py
import json, requests, logging
from config import RUN_LOOKBACK_MINUTES, PN_PER_PILOT_PER_VN, ADD_PN_API
from config import TABLES
from utils import setup_logging, compute_time_window, safe_list
from sheets_reader import load_accounts, load_tenant_exceptions, load_region_preferences
import db, sql_queries as sqlq
from emailer import send_email

setup_logging()
log = logging.getLogger("run_backup")
db.init_pool(pool_size=5)

def get_purchased_vns_for_tenant(tenant_id, start, end):
    rows = db.fetchall(sqlq.Q_PURCHASED_VNS, (tenant_id, start, end))
    return rows

def is_pilot_active(pilot):
    row = db.fetchone(sqlq.Q_PRI_STATUS, (pilot,))
    if not row:
        return False
    status = str(row.get('status', '')).strip().lower()
    return status in ('up', 'active', 'online')

def fetch_available_pns_for_pilot_region(pilot, region, needed):
    # fetch PNs, then try to reserve them
    rows = db.fetchall(sqlq.Q_FETCH_PNS, (pilot, region, needed))
    reserved = []
    for r in rows:
        pn_id = r['id']
        # reserve to avoid race (using a reserved_by id, e.g., "auto-backup")
        ok = db.reserve_pn(pn_id, "auto-backup")
        if ok:
            reserved.append(r)
        else:
            log.info("PN id %s couldn't be reserved (race)", pn_id)
    return reserved

def call_addpn_api(vn, pn, tenant_id):
    payload = {
        "vn": vn,
        "pn": pn,
        "tenant_id": tenant_id
    }
    try:
        r = requests.post(ADD_PN_API['url'], json=payload, headers=ADD_PN_API.get('headers', {}), timeout=ADD_PN_API.get('timeout', 15))
        if r.status_code in (200,201):
            return True, r.json() if r.content else {}
        else:
            return False, {"status_code": r.status_code, "text": r.text}
    except Exception as e:
        return False, {"error": str(e)}

def process_tenant(tenant_record, tenant_ex_map, region_prefs, start_time, end_time):
    tenant_id = tenant_record.get('account_id') or tenant_record.get('tenant') or tenant_record.get('tenant_id')
    tenant_name = tenant_record.get('tenant') or tenant_record.get('tenant_name') or tenant_id
    log.info("Processing tenant: %s (%s)", tenant_name, tenant_id)
    purchased = get_purchased_vns_for_tenant(tenant_id, start_time, end_time)
    if not purchased:
        log.info("No purchased VNs for tenant %s in window", tenant_id)
        return None

    email_lines = []
    for vn in purchased:
        vn_id = vn['id']
        vn_number = vn['vn_number']
        region = vn.get('region')
        log.info("Processing VN %s (region=%s)", vn_number, region)

        # Determine allowed pilots
        if tenant_name in tenant_ex_map:
            allowed_pilots = tenant_ex_map[tenant_name]
            log.info("Tenant exception pilots for %s: %s", tenant_name, allowed_pilots)
        else:
            allowed_pilots = region_prefs.get(region, [])
            log.info("Region pilots for %s: %s", region, allowed_pilots)

        # compute needed PNs for this VN (example: 1 PN per pilot per VN)
        needed_per_pilot = PN_PER_PILOT_PER_VN
        # results per VN
        vn_results = {"vn": vn_number, "region": region, "assigned": [], "warnings": []}

        for pilot in allowed_pilots:
            # Check pilot status from PRI
            if not is_pilot_active(pilot):
                log.warning("Pilot %s is not active. Skipping.", pilot)
                vn_results['warnings'].append(f"Pilot {pilot} inactive/DEAD - skipped")
                continue

            # Fetch and reserve available PNs for this pilot & region
            pns = fetch_available_pns_for_pilot_region(pilot, region, needed_per_pilot)
            if not pns:
                log.warning("No available PN for pilot %s region %s", pilot, region)
                vn_results['warnings'].append(f"No backup PN available from pilot {pilot}")
                continue

            # For each PN, call addpn.json
            for pn_row in pns:
                pn = pn_row['pn']
                ok, resp = call_addpn_api(vn_number, pn, tenant_id)
                if ok:
                    log.info("Assigned PN %s to VN %s", pn, vn_number)
                    vn_results['assigned'].append({"pn": pn, "pilot": pilot, "api_resp": resp})
                    # Optionally record assignment in DB (if you have table)
                    try:
                        db.execute(sqlq.Q_INSERT_ASSIGN, (vn_id, pn_row['id'], pilot, tenant_id))
                    except Exception as e:
                        log.exception("Failed to insert assignment record: %s", e)
                else:
                    log.error("API addpn failed for pn %s -> %s", pn, resp)
                    vn_results['warnings'].append(f"Failed to add PN {pn} via API: {resp}")

        # append per-vn to email
        email_lines.append(vn_results)

    return email_lines

def build_email_body(tenant, results):
    if not results:
        return f"No VNs processed for tenant {tenant.get('tenant') or tenant.get('account_id')}"
    lines = []
    lines.append(f"Tenant: {tenant.get('tenant') or tenant.get('account_id')}")
    for r in results:
        lines.append(f"\nVN: {r['vn']} (region: {r['region']})")
        if r['assigned']:
            lines.append(" Assigned PNs:")
            for a in r['assigned']:
                lines.append(f"  - {a['pn']} (pilot: {a['pilot']})")
        if r['warnings']:
            lines.append(" Warnings:")
            for w in r['warnings']:
                lines.append(f"  - {w}")
    return "\n".join(lines)

def main():
    start_time, end_time = compute_time_window(RUN_LOOKBACK_MINUTES)
    log.info("Window Start: %s  End: %s", start_time, end_time)

    accounts = load_accounts()
    tenant_ex_map = load_tenant_exceptions()
    region_prefs = load_region_preferences()

    for acc in accounts:
        try:
            results = process_tenant(acc, tenant_ex_map, region_prefs, start_time, end_time)
            body = build_email_body(acc, results)
            to_addr = acc.get('email') or acc.get('contact_email') or None
            if not to_addr:
                # fallback: send to admin
                to_addr = [EMAIL['admin_to']]
            else:
                to_addr = [to_addr, EMAIL['admin_to']]
            # send tenant email
            send_email(to_addr, f"Backup PNs report for tenant {acc.get('tenant') or acc.get('account_id')}", body)
        except Exception as e:
            log.exception("Failure while processing tenant %s: %s", acc, e)
            send_email([EMAIL['admin_to']], f"Backup PN job error for tenant {acc}", f"Error: {e}")

if __name__ == "__main__":
    main()
