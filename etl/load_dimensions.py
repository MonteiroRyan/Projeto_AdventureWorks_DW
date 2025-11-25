from datetime import date
from .db import get_conn_oltp, get_conn_dw, fetch_all, execute
from .scd import upsert_scd2_product, upsert_scd2_customer

def load_dim_product():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, """
            SELECT
              p.productid AS product_nk,
              p.name AS product_name,
              p.productnumber AS product_number,
              p.color,
              p.size,
              p.style,
              sc.name AS subcategory,
              c.name AS category,
              p.standardcost AS standard_cost,
              p.listprice AS list_price
            FROM production.product p
            LEFT JOIN production.productsubcategory sc ON sc.productsubcategoryid = p.productsubcategoryid
            LEFT JOIN production.productcategory c ON c.productcategoryid = sc.productcategoryid
            WHERE p.discontinueddate IS NULL OR p.discontinueddate IS NOT NULL
        """)
        for r in rows:
            r["valid_from"] = date.today()
            upsert_scd2_product(dw, r)
    finally:
        oltp.close(); dw.close()

def load_dim_customer():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        # Individual customers
        rows_individual = fetch_all(oltp, """
          SELECT
            c.customerid AS customer_nk,
            'Individual'::text AS customer_type,
            c.personid AS person_nk,
            NULL::int AS store_nk,
            COALESCE(pp.firstname || ' ' || pp.lastname, 'N/A') AS customer_name,
            ea.emailaddress,
            ph.phonenumber AS phone,
            c.territoryid AS territory_nk
          FROM sales.customer c
          JOIN person.person pp ON pp.businessentityid = c.personid
          LEFT JOIN person.emailaddress ea ON ea.businessentityid = c.personid
          LEFT JOIN person.personphone ph ON ph.businessentityid = c.personid
          WHERE c.personid IS NOT NULL
        """)
        # Store customers
        rows_store = fetch_all(oltp, """
          SELECT
            c.customerid AS customer_nk,
            'Store'::text AS customer_type,
            NULL::int AS person_nk,
            s.businessentityid AS store_nk,
            s.name AS customer_name,
            NULL::text AS emailaddress,
            NULL::text AS phone,
            c.territoryid AS territory_nk
          FROM sales.customer c
          JOIN sales.store s ON s.businessentityid = c.storeid
          WHERE c.storeid IS NOT NULL
        """)

        dw_conn = dw
        for r in rows_individual + rows_store:
            r["email_address"] = r.pop("emailaddress", None)
            r["valid_from"] = date.today()
            upsert_scd2_customer(dw_conn, r)

    finally:
        oltp.close(); dw.close()

def load_dim_territory():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, """
          SELECT territoryid AS territory_nk, name, countryregioncode, "group"
          FROM sales.salesterritory
        """)
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_territory(territory_nk, name, country_region_code, "group")
              VALUES (%(territory_nk)s, %(name)s, %(countryregioncode)s, %(group)s)
              ON CONFLICT (territory_nk) DO UPDATE
                SET name = EXCLUDED.name,
                    country_region_code = EXCLUDED.country_region_code,
                    "group" = EXCLUDED."group"
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_employee():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, """
          SELECT sp.businessentityid AS employee_nk, COALESCE(p.firstname || ' ' || p.lastname, 'N/A') AS employee_name
          FROM sales.salesperson sp
          JOIN person.person p ON p.businessentityid = sp.businessentityid
        """)
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_employee(employee_nk, employee_name)
              VALUES (%(employee_nk)s, %(employee_name)s)
              ON CONFLICT (employee_nk) DO UPDATE SET employee_name = EXCLUDED.employee_name
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_store():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, """
          SELECT businessentityid AS store_nk, name AS store_name
          FROM sales.store
        """)
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_store(store_nk, store_name)
              VALUES (%(store_nk)s, %(store_name)s)
              ON CONFLICT (store_nk) DO UPDATE SET store_name = EXCLUDED.store_name
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_shipmethod():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, "SELECT shipmethodid AS ship_method_nk, name FROM purchasing.shipmethod")
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_shipmethod(ship_method_nk, name)
              VALUES (%(ship_method_nk)s, %(name)s)
              ON CONFLICT (ship_method_nk) DO UPDATE SET name = EXCLUDED.name
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_promotion():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, """
          SELECT specialofferid AS promotion_nk, description, discountpct AS discount_pct, type, category
          FROM sales.specialoffer
        """)
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_promotion(promotion_nk, description, discount_pct, "type", category)
              VALUES (%(promotion_nk)s, %(description)s, %(discount_pct)s, %(type)s, %(category)s)
              ON CONFLICT (promotion_nk) DO UPDATE
                SET description = EXCLUDED.description,
                    discount_pct = EXCLUDED.discount_pct,
                    "type" = EXCLUDED."type",
                    category = EXCLUDED.category
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_vendor():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, "SELECT businessentityid AS vendor_nk, name AS vendor_name FROM purchasing.vendor")
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_vendor(vendor_nk, vendor_name)
              VALUES (%(vendor_nk)s, %(vendor_name)s)
              ON CONFLICT (vendor_nk) DO UPDATE SET vendor_name = EXCLUDED.vendor_name
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_creditcard():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, "SELECT creditcardid AS credit_card_nk, cardtype AS card_type FROM sales.creditcard")
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_creditcard(credit_card_nk, card_type)
              VALUES (%(credit_card_nk)s, %(card_type)s)
              ON CONFLICT (credit_card_nk) DO UPDATE SET card_type = EXCLUDED.card_type
            """, r)
    finally:
        oltp.close(); dw.close()

def load_dim_location():
    oltp = get_conn_oltp(); dw = get_conn_dw()
    try:
        rows = fetch_all(oltp, "SELECT locationid AS location_nk, name AS location_name FROM production.location")
        for r in rows:
            execute(dw, """
              INSERT INTO dw.dim_location(location_nk, location_name)
              VALUES (%(location_nk)s, %(location_name)s)
              ON CONFLICT (location_nk) DO UPDATE SET location_name = EXCLUDED.location_name
            """, r)
    finally:
        oltp.close(); dw.close()

def load_all_dimensions():
    load_dim_product()
    load_dim_customer()
    load_dim_territory()
    load_dim_employee()
    load_dim_store()
    load_dim_shipmethod()
    load_dim_promotion()
    load_dim_vendor()
    load_dim_creditcard()
    load_dim_location()