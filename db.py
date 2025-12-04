# db.py
import mysql.connector
from mysql.connector import pooling
from config import DB, TABLES
import sql_queries as sqlq

# Simple connection pool
pool = None

def init_pool(pool_size=5):
    global pool
    if pool is None:
        pool = pooling.MySQLConnectionPool(pool_name="mypool",
                                           pool_size=pool_size,
                                           **DB)

def get_conn():
    if pool is None:
        init_pool()
    return pool.get_connection()

# convenience fetch
def fetchall(query, params=(), format_tables=True):
    if format_tables:
        query = query.format(**TABLES)
    cnx = get_conn()
    cur = cnx.cursor(dictionary=True)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows

def fetchone(query, params=(), format_tables=True):
    if format_tables:
        query = query.format(**TABLES)
    cnx = get_conn()
    cur = cnx.cursor(dictionary=True)
    cur.execute(query, params)
    row = cur.fetchone()
    cur.close()
    cnx.close()
    return row

def execute(query, params=(), commit=True, format_tables=True):
    if format_tables:
        query = query.format(**TABLES)
    cnx = get_conn()
    cur = cnx.cursor()
    cur.execute(query, params)
    if commit:
        cnx.commit()
    lastrow = cur.lastrowid
    cur.close()
    cnx.close()
    return lastrow

def reserve_pn(pn_id, reserved_by):
    # atomic update, return True if reserved
    q = sqlq.Q_RESERVE_PN.format(**TABLES)
    cnx = get_conn()
    cur = cnx.cursor()
    cur.execute(q, (reserved_by, pn_id))
    cnx.commit()
    changed = cur.rowcount
    cur.close()
    cnx.close()
    return changed == 1
