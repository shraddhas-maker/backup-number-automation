# run_backup.py

import json, requests, logging
from config import RUN_LOOKBACK_MINUTES, PN_PER_PILOT_PER_VN, ADD_PN_API, EMAIL
from config import TABLES
from utils import setup_logging, compute_time_window
from sheets_reader import load_accounts, load_tenant_exceptions, load_region_preferences
import db, sql_queries as sqlq
from emailer import send_email

setup_logging()
log = logging.getLogger("run_backup")
db.init_pool(pool_size=5)


# ------------------------------------------------------------
# FETCH PURCHASED VNs (IncomingPhoneNumber)
# ------------------------------------------------------------
def get_purchased_vns_for_tenant(tenant_id, start, end):
    rows = db.fetchall(sqlq.Q_PURCHASED_VNS, (tenant_id, start, end))
    return rows


# ------------------------------------------------------------
# CHECK PILOT STATUS (PRI table)
# ------------------------------------------------------------
def is_pilot_active(pilot):
    row = db.fetchone(sqlq.Q_PRI_STATUS, (pilot,))
    if not row:
        return False

    state = str(row.get("state", "")).strip().lower()
    # Mark DOWN/DEAD/INACTIVE as unusable
    active_states = ("up", "active", "online")
    return state in active_states


# ------------------------------------------------------------
# FETCH & RESERVE PNs FROM AvailablePhoneNumber
# ------------------------------------------------------------
def fetch_available_pns_for_pilot_region(pilot, region, needed):
    rows = db.fetchall(sqlq.Q_FETCH_PNS, (pilot, region, needed))
    reserved = []

    for r in rows:
        sid = r["sid"]  # unique PN ID
        ok = db.reserve_pn(sid, "auto-backup")

        if ok:
            reserved.append(r)
        else:
            log.warning("PN SID %s could not be reserved (race)", sid)

    return reserved


# ------------------------------------------------------------
# CALL ATTACH API
# ------------------------------------------------------------
def call_addpn_api(vn, pn, tenant_id):
    payload = {
        "vn": vn,
        "pn": pn,
        "tenant_id": tenant_id
    }

    try:
        r = requests.post(
            ADD_PN_API["url"],
            json=payload,
            headers=ADD_PN_API.get("headers", {}),
            timeout=ADD_PN_API.get("timeout", 15)
        )

        if r.status_code in (200, 201):
            return True, (r.json() if r.content else {})
        else:
            return False, {"status_code": r.status_code, "text": r.text}

    except Exception as e:
        return False, {"error": str(e)}


# ------------------------------------------------------------
# MAIN PER-TENANT PROCESSING
# ------------------------------------------------------------
def process_tenant(tenant_record, tenant_ex_map, region_prefs, start_time, end_time):

    tenant_id = tenant_record.get("AccountSid")
    log.info("Processing tenant: %s", tenant_id)

    purchased = get_purchased_vns_for_tenant(tenant_id, start_time, end_time)

    if not purchased:
        log.info("No VNs purchased in the window")
        return None

    email_data = []

    for vn in purchased:
        vn_id = vn["sid"]
        vn_number = vn["PhoneNumber"]
        region = vn.get("Region")

        vn_result = {
            "vn": vn_number,
            "region": region,
            "assigned": [],
            "warnings": []
        }

        log.info("Processing VN %s (Region=%s)", vn_number, region)

        # Load pilots
        if tenant_id in tenant_ex_map:
            pilots = tenant_ex_map[tenant_id]
        else:
            pilots = region_prefs.get(region, [])

        needed = PN_PER_PILOT_PER_VN

        for pilot in pilots:

            # Check PRI table state
            if not is_pilot_active(pilot):
                vn_result["warnings"].append(f"Pilot {pilot} inactive/dead")
                continue

            # Fetch PNs
            pns = fetch_available_pns_for_pilot_region(pilot, region, needed)

            if not pns:
                vn_result["warnings"].append(f"No PN available for {pilot}")
                continue

            # Call API
            for pn_row in pns:
                pn = pn_row["PhoneNumber"]

                ok, resp = call_addpn_api(vn_number, pn, tenant_id)

                if ok:
                    vn_result["assigned"].append({
                        "pn": pn,
                        "pilot": pilot,
                        "api_resp": resp
                    })

                    # Record DB assignment if required
                    try:
                        db.execute(sqlq.Q_INSERT_ASSIGN, (
                            vn_id,
                            pn_row["sid"],
                            pilot,
                            tenant_id
                        ))
                    except Exception as e:
                        log.exception("Insert assignment failed: %s", e)
                else:
                    vn_result["warnings"].append(f"Failed to assign PN {pn}: {resp}")

        email_data.append(vn_result)

    return email_data


# ------------------------------------------------------------
# EMAIL BODY BUILDER
# ------------------------------------------------------------
def build_email_body(tenant, results):
    if not results:
        return f"No purchased VNs for tenant {tenant.get('AccountSid')}"

    lines = [f"Tenant: {tenant.get('AccountSid')}"]

    for r in results:
        lines.append(f"\nVN: {r['vn']} (Region: {r['region']})")

        if r["assigned"]:
            lines.append(" Assigned PNs:")
            for a in r["assigned"]:
                lines.append(f"  - {a['pn']} (Pilot: {a['pilot']})")

        if r["warnings"]:
            lines.append(" Warnings:")
            for w in r["warnings"]:
                lines.append(f"  - {w}")

    return "\n".join(lines)


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    start_time, end_time = compute_time_window(RUN_LOOKBACK_MINUTES)
    log.info("Window Start: %s | End: %s", start_time, end_time)

    accounts = load_accounts()
    tenant_ex_map = load_tenant_exceptions()
    region_prefs = load_region_preferences()

    for acc in accounts:
        try:
            results = process_tenant(acc, tenant_ex_map, region_prefs, start_time, end_time)
            email_body = build_email_body(acc, results)

            to_addr = acc.get("email") or EMAIL["admin_to"]
            send_email([to_addr], "Backup PN Report", email_body)

        except Exception as e:
            log.exception("Tenant processing error: %s", e)
            send_email([EMAIL["admin_to"]], "Backup PN Job Error", f"Error: {e}")


if __name__ == "__main__":
    main()
