from .db import get_conn_oltp, get_conn_dw, fetch_all, fetch_one, execute
from datetime import date

def yyyymmdd(d): return d.year*10000 + d.month*100 + d.day

def get_key(conn, sql, params):
    r = fetch_one(conn, sql, params)
    return r["id"] if r else None

def load_fact_purchases(truncate=False):
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        if truncate:
            execute(dw, "TRUNCATE TABLE dw.fact_purchases RESTART IDENTITY")

        rows = fetch_all(oltp, """
          SELECT
            d.purchaseorderdetailid,
            h.purchaseorderid,
            h.orderdate,
            h.vendorid,
            d.productid,
            d.orderqty,
            d.unitprice,
            (d.orderqty * d.unitprice)::numeric(18,4) AS line_total,
            pi.locationid
          FROM purchasing.purchaseorderdetail d
          JOIN purchasing.purchaseorderheader h ON h.purchaseorderid = d.purchaseorderid
          LEFT JOIN production.productinventory pi ON pi.productid = d.productid
        """)

        for r in rows:
            order_date_key = yyyymmdd(r["orderdate"]) if r["orderdate"] else None
            product_key = get_key(dw, "SELECT product_key AS id FROM dw.dim_product WHERE product_nk = %s AND is_current", (r["productid"],))
            vendor_key = get_key(dw, "SELECT vendor_key AS id FROM dw.dim_vendor WHERE vendor_nk = %s", (r["vendorid"],))
            location_key = get_key(dw, "SELECT location_key AS id FROM dw.dim_location WHERE location_nk = %s", (r["locationid"],)) if r["locationid"] else None

            execute(dw, """
              INSERT INTO dw.fact_purchases(
                order_date_key, vendor_key, product_key, location_key,
                purchase_order_number, purchase_order_line_id,
                order_qty, unit_price, line_total
              ) VALUES (
                %(order_date_key)s, %(vendor_key)s, %(product_key)s, %(location_key)s,
                %(purchase_order_number)s, %(purchase_order_line_id)s,
                %(order_qty)s, %(unit_price)s, %(line_total)s
              )
            """, {
                "order_date_key": order_date_key,
                "vendor_key": vendor_key,
                "product_key": product_key,
                "location_key": location_key,
                "purchase_order_number": str(r["purchaseorderid"]),
                "purchase_order_line_id": r["purchaseorderdetailid"],
                "order_qty": r["orderqty"],
                "unit_price": r["unitprice"],
                "line_total": r["line_total"],
            })
    finally:
        oltp.close(); dw.close()

if __name__ == "__main__":
    load_fact_purchases(truncate=True)