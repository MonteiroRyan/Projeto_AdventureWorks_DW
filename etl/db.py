import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# Configure duas conexões: OLTP (AdventureWorks) e DW (mesmo cluster ou não)
OLTP_DSN = os.getenv("postgresql://postgres:8486@localhost:5432/oltp_adventureworks")  # e.g. postgresql://user:pass@localhost:5432/adventureworks
DW_DSN = os.getenv("postgresql://postgres:8486@localhost:5432/warehouse")      # e.g. postgresql://user:pass@localhost:5432/warehouse

def get_conn_oltp():
    return psycopg2.connect(OLTP_DSN, cursor_factory=RealDictCursor)

def get_conn_dw():
    return psycopg2.connect(DW_DSN, cursor_factory=RealDictCursor)

def fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()

def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

def execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
    conn.commit()

def executemany(conn, sql, seq_of_params):
    with conn.cursor() as cur:
        cur.executemany(sql, seq_of_params)
    conn.commit()