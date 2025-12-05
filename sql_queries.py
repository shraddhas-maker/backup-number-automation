from config import TABLES

######################################################################
# 1. QUERY: Fetch Purchased Virtual Numbers (VNs) within time window
######################################################################
Q_PURCHASED_VNS = f"""
SELECT 
    a.sid,
    a.PhoneNumber AS vn_number,
    b.Region
FROM {TABLES['purchased_numbers']} a
JOIN {TABLES['available_pns']} b
    ON a.PhoneNumber = b.PhoneNumber
WHERE 
    a.AccountSid = %s
    AND a.DateCreated >= %s
    AND a.DateCreated <= %s;
"""


######################################################################
# 2. QUERY: PRI Table — Check Pilot Status (active / dead / inactive)
######################################################################
Q_PRI_STATUS = f"""
SELECT 
    pilot,
    state
FROM {TABLES['pri']}
WHERE pilot = %s;
"""


######################################################################
# 3. QUERY: Count Available PN Pool (for debugging)
######################################################################
Q_COUNT_AVAILABLE_PNS = f"""
SELECT COUNT(*) AS total, _state
FROM {TABLES['available_pns']}
WHERE 
    AccountSid IS NULL
    AND _state = 'pn'
    AND Rental < 0
    AND PhoneNumber NOT IN (
        SELECT PhysicalNumber FROM {TABLES['pvm']}
    )
GROUP BY _state;
"""


######################################################################
# 4. QUERY: Insert Assignment Record (PN added to VN)
######################################################################
Q_INSERT_ASSIGN = f"""
INSERT INTO backup_pn_assignments
(vn_sid, pn_sid, pilot, tenant_id, created_at)
VALUES (%s, %s, %s, %s, NOW());
"""


######################################################################
# 5. QUERY: Fetch Available PN for Region + Pilot (MAIN QUERY)
######################################################################
Q_FETCH_PNS = f"""
SELECT 
    a.sid,
    a.PhoneNumber AS pn,
    a.Region,
    a._Pri
FROM {TABLES['available_pns']} a
WHERE 
    a.AccountSid IS NULL
    AND a._state = 'pn'
    AND a.Rental < 0
    AND a.Region = %s   -- region
    AND a.PhoneNumber NOT IN (
        SELECT PhysicalNumber FROM {TABLES['pvm']}
    )
    AND a._Pri IN (
        SELECT id FROM {TABLES['pri']}
        WHERE pilot = %s           -- pilot
        AND state = 'active'
    )
LIMIT %s; 
"""
