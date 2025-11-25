from datetime import date
from .db import get_conn_oltp, get_conn_dw, fetch_all, fetch_one, execute

def yyyymmdd(d): return d.year*10000 + d.month*100 + d.day

def get_key(conn, sql, params):
    r = fetch_one(conn, sql, params)
    return r["id"] if r else None

def load_inventory_snapshot(snapshot_date: date):
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        date_key = yyyymmdd(snapshot_date)
        # Snapshot: usar production.productinventory (quantidade por product + location)
        rows = fetch_all(oltp, """
          SELECT productid, locationid, quantity
          FROM production.productinventory
        """)
        for r in rows:
            product_key = get_key(dw, "SELECT product_key AS id FROM dw.dim_product WHERE product_nk = %s AND is_current", (r["productid"],))
            location_key = get_key(dw, "SELECT location_key AS id FROM dw.dim_location WHERE location_nk = %s", (r["locationid"],))
            execute(dw, """
              INSERT INTO dw.fact_inventory_snapshot(snapshot_date_key, product_key, location_key, quantity_on_hand)
              VALUES (%s, %s, %s, %s)
            """, (date_key, product_key, location_key, r["quantity"]))
    finally:
        oltp.close(); dw.close()

if __name__ == "__main__":
    load_inventory_snapshot(date.today())