from datetime import date
from .db import get_conn_oltp, get_conn_dw, fetch_all, fetch_one, execute

def yyyymmdd(d): return d.year*10000 + d.month*100 + d.day

def get_key(conn, sql, params):
    r = fetch_one(conn, sql, params)
    return r["id"] if r else None

def ensure_dim_keys(dw, row):
    # Datas
    order_date_key = yyyymmdd(row["orderdate"]) if row["orderdate"] else None
    due_date_key = yyyymmdd(row["duedate"]) if row["duedate"] else None
    ship_date_key = yyyymmdd(row["shipdate"]) if row["shipdate"] else None

    # Product (SCD2: pega current por NK)
    product_key = get_key(dw, "SELECT product_key AS id FROM dw.dim_product WHERE product_nk = %s AND is_current", (row["productid"],))

    # Customer (SCD2)
    customer_key = get_key(dw, "SELECT customer_key AS id FROM dw.dim_customer WHERE customer_nk = %s AND is_current", (row["customerid"],))

    territory_key = get_key(dw, "SELECT territory_key AS id FROM dw.dim_territory WHERE territory_nk = %s", (row["territoryid"],))
    employee_key = get_key(dw, "SELECT employee_key AS id FROM dw.dim_employee WHERE employee_nk = %s", (row["salespersonid"],)) if row["salespersonid"] else None
    store_key = get_key(dw, "SELECT store_key AS id FROM dw.dim_store WHERE store_nk = %s", (row["storeid"],)) if row["storeid"] else None
    ship_method_key = get_key(dw, "SELECT ship_method_key AS id FROM dw.dim_shipmethod WHERE ship_method_nk = %s", (row["shipmethodid"],)) if row["shipmethodid"] else None
    promotion_key = get_key(dw, "SELECT promotion_key AS id FROM dw.dim_promotion WHERE promotion_nk = %s", (row["specialofferid"],)) if row["specialofferid"] else None
    credit_card_key = get_key(dw, "SELECT credit_card_key AS id FROM dw.dim_creditcard WHERE credit_card_nk = %s", (row["creditcardid"],)) if row["creditcardid"] else None

    return {
        "order_date_key": order_date_key,
        "due_date_key": due_date_key,
        "ship_date_key": ship_date_key,
        "product_key": product_key,
        "customer_key": customer_key,
        "territory_key": territory_key,
        "employee_key": employee_key,
        "store_key": store_key,
        "ship_method_key": ship_method_key,
        "promotion_key": promotion_key,
        "credit_card_key": credit_card_key,
    }

def load_fact_sales(incremental=True):
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        # Watermark por SalesOrderDetailID
        last_id = None
        if incremental:
            rc = fetch_one(dw, "SELECT last_watermark_value FROM dw.etl_run_control WHERE pipeline_name = 'fact_sales'")
            if rc and rc["last_watermark_value"]:
                last_id = int(rc["last_watermark_value"])

        rows = fetch_all(oltp, f"""
          WITH base AS (
            SELECT
              d.salesorderdetailid,
              d.salesorderid,
              h.salesordernumber,
              h.orderdate, h.duedate, h.shipdate,
              h.taxamt, h.freight, h.subtotal AS order_subtotal,
              h.creditcardid, h.territoryid, h.shipmethodid,
              h.salespersonid,
              c.customerid,
              c.storeid,
              d.productid,
              d.orderqty,
              d.unitprice,
              d.unitpricediscount,
              d.linetotal,
              so.specialofferid
            FROM sales.salesorderdetail d
            JOIN sales.salesorderheader h ON h.salesorderid = d.salesorderid
            JOIN sales.customer c ON c.customerid = h.customerid
            LEFT JOIN sales.specialofferproduct sop ON sop.productid = d.productid
            LEFT JOIN sales.specialoffer so ON so.specialofferid = sop.specialofferid
            WHERE (%(last_id)s IS NULL OR d.salesorderdetailid > %(last_id)s)
          ),
          lines AS (
            SELECT
              b.*,
              -- Subtotal por linha calculado via unitprice e desconto (garante consistência)
              (b.orderqty * b.unitprice * (1 - b.unitpricediscount))::numeric(18,4) AS line_subtotal_calc
            FROM base b
          ),
          alloc AS (
            SELECT
              l.*,
              CASE WHEN NULLIF(l.order_subtotal, 0) IS NULL THEN 0
                   ELSE (l.line_subtotal_calc / l.order_subtotal) * l.taxamt END AS tax_alloc,
              CASE WHEN NULLIF(l.order_subtotal, 0) IS NULL THEN 0
                   ELSE (l.line_subtotal_calc / l.order_subtotal) * l.freight END AS freight_alloc
            FROM lines l
          )
          SELECT * FROM alloc
          ORDER BY salesorderdetailid
        """, {"last_id": last_id})

        # Inserção
        for r in rows:
            keys = ensure_dim_keys(dw, r)
            if not keys["order_date_key"]:
                continue

            # custo padrão do produto vigente
            stdc = fetch_one(dw, """
              SELECT standard_cost FROM dw.dim_product
              WHERE product_key = %s
            """, (keys["product_key"],))
            standard_cost = stdc["standard_cost"] if stdc else 0
            standard_cost_amount = float(standard_cost or 0) * float(r["orderqty"] or 0)

            line_subtotal = float(r["line_subtotal_calc"] or 0)
            tax_alloc = float(r["tax_alloc"] or 0)
            freight_alloc = float(r["freight_alloc"] or 0)
            total_due_line = line_subtotal + tax_alloc + freight_alloc
            gross_margin_amount = line_subtotal - standard_cost_amount

            shipping_days = None
            if r["orderdate"] and r["shipdate"]:
                shipping_days = (r["shipdate"] - r["orderdate"]).days
            on_time_delivery = None
            if r["duedate"] and r["shipdate"]:
                on_time_delivery = r["shipdate"] <= r["duedate"]

            execute(dw, """
              INSERT INTO dw.fact_sales(
                order_date_key, due_date_key, ship_date_key,
                customer_key, product_key, territory_key, employee_key, store_key, ship_method_key,
                promotion_key, credit_card_key,
                sales_order_number, sales_order_line_id,
                order_qty, unit_price, unit_price_discount,
                line_subtotal, tax_amount_alloc, freight_amount_alloc, total_due_line,
                standard_cost_amount, gross_margin_amount, shipping_days, on_time_delivery
              ) VALUES (
                %(order_date_key)s, %(due_date_key)s, %(ship_date_key)s,
                %(customer_key)s, %(product_key)s, %(territory_key)s, %(employee_key)s, %(store_key)s, %(ship_method_key)s,
                %(promotion_key)s, %(credit_card_key)s,
                %(sales_order_number)s, %(salesorderdetailid)s,
                %(orderqty)s, %(unitprice)s, %(unitpricediscount)s,
                %(line_subtotal)s, %(tax_alloc)s, %(freight_alloc)s, %(total_due_line)s,
                %(standard_cost_amount)s, %(gross_margin_amount)s, %(shipping_days)s, %(on_time_delivery)s
              )
            """, {
                **keys,
                "sales_order_number": r["salesordernumber"],
                "salesorderdetailid": r["salesorderdetailid"],
                "orderqty": r["orderqty"],
                "unitprice": r["unitprice"],
                "unitpricediscount": r["unitpricediscount"],
                "line_subtotal": line_subtotal,
                "tax_alloc": tax_alloc,
                "freight_alloc": freight_alloc,
                "total_due_line": total_due_line,
                "standard_cost_amount": standard_cost_amount,
                "gross_margin_amount": gross_margin_amount,
                "shipping_days": shipping_days,
                "on_time_delivery": on_time_delivery,
            })

        # Atualiza watermark
        if rows:
            max_id = max(r["salesorderdetailid"] for r in rows)
            execute(dw, """
              INSERT INTO dw.etl_run_control(pipeline_name, last_watermark_value)
              VALUES ('fact_sales', %s)
              ON CONFLICT (pipeline_name) DO UPDATE
                SET last_watermark_value = EXCLUDED.last_watermark_value, updated_at = now()
            """, (str(max_id),))
    finally:
        oltp.close(); dw.close()

if __name__ == "__main__":
    load_fact_sales()