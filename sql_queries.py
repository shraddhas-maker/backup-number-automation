# sql_queries.py

# Fetch purchased VNs for tenant in time window
Q_PURCHASED_VNS = """
SELECT id, tenant_id, tenant_name, vn_number, region, date_created
FROM {purchased_numbers}
WHERE tenant_id = %s
  AND date_created BETWEEN %s AND %s
"""

# Check pilot status in PRI table
Q_PRI_STATUS = """
SELECT pilot, status
FROM {pri}
WHERE pilot = %s
LIMIT 1
"""

# Fetch available PNs for pilot & region (and mark tentative to avoid race)
Q_FETCH_PNS = """
SELECT id, pn, pilot, region
FROM {available_pns}
WHERE pilot = %s
  AND region = %s
  AND status = 'Available'
LIMIT %s
"""

# An example update to mark a PN reserved (use transaction)
Q_RESERVE_PN = """
UPDATE {available_pns}
SET status = 'Reserved', reserved_at = NOW(), reserved_by = %s
WHERE id = %s AND status = 'Available'
"""

# Optional: insert mapping record into assigned table (if you maintain an assignments table)
Q_INSERT_ASSIGN = """
INSERT INTO assigned_pn_to_vn (vn_id, pn_id, pilot, assigned_at, tenant_id)
VALUES (%s, %s, %s, NOW(), %s)
"""
