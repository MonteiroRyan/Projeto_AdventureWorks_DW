from datetime import date
from typing import Dict, Any, Tuple
from .db import fetch_one, execute

# Funções utilitárias SCD2 para produto e cliente (simples e performático o suficiente para cargas de exemplo).
# Estratégia:
# - Checa linha atual (is_current = true) por natural key.
# - Se não existe, insere nova (valid_from = hoje).
# - Se existe e mudou conteúdo relevante, encerra linha atual (valid_to = ontem, is_current = false) e insere nova.

def _different(a: Any, b: Any) -> bool:
    return (a or None) != (b or None)

def upsert_scd2_product(dw_conn, row: Dict[str, Any]) -> int:
    """
    row esperada:
      product_nk, product_name, product_number, color, size, style, subcategory, category,
      standard_cost, list_price, valid_from (date)
    Retorna product_key.
    """
    current = fetch_one(dw_conn, """
        SELECT * FROM dw.dim_product
        WHERE product_nk = %s AND is_current = true
    """, (row["product_nk"],))
    today = row.get("valid_from") or date.today()

    if not current:
        # insert new current
        execute(dw_conn, """
            INSERT INTO dw.dim_product
              (product_nk, product_name, product_number, color, size, style, subcategory, category,
               standard_cost, list_price, valid_from, valid_to, is_current)
            VALUES
              (%(product_nk)s, %(product_name)s, %(product_number)s, %(color)s, %(size)s, %(style)s, %(subcategory)s, %(category)s,
               %(standard_cost)s, %(list_price)s, %(valid_from)s, NULL, true)
        """, row)
        new_key = fetch_one(dw_conn, "SELECT currval(pg_get_serial_sequence('dw.dim_product','product_key')) AS id")["id"]
        return new_key

    changed = any([
        _different(current["product_name"], row["product_name"]),
        _different(current["product_number"], row["product_number"]),
        _different(current["color"], row["color"]),
        _different(current["size"], row["size"]),
        _different(current["style"], row["style"]),
        _different(current["subcategory"], row["subcategory"]),
        _different(current["category"], row["category"]),
        _different(str(current["standard_cost"]), str(row["standard_cost"])),
        _different(str(current["list_price"]), str(row["list_price"])),
    ])
    if not changed:
        return current["product_key"]

    # close current
    execute(dw_conn, """
        UPDATE dw.dim_product
           SET valid_to = %(yesterday)s, is_current = false
         WHERE product_key = %(product_key)s
    """, {"yesterday": today, "product_key": current["product_key"]})

    # insert new
    execute(dw_conn, """
        INSERT INTO dw.dim_product
          (product_nk, product_name, product_number, color, size, style, subcategory, category,
           standard_cost, list_price, valid_from, valid_to, is_current)
        VALUES
          (%(product_nk)s, %(product_name)s, %(product_number)s, %(color)s, %(size)s, %(style)s, %(subcategory)s, %(category)s,
           %(standard_cost)s, %(list_price)s, %(valid_from)s, NULL, true)
    """, row)
    new_key = fetch_one(dw_conn, "SELECT currval(pg_get_serial_sequence('dw.dim_product','product_key')) AS id")["id"]
    return new_key

def upsert_scd2_customer(dw_conn, row: Dict[str, Any]) -> int:
    """
    row esperada:
      customer_nk, customer_type, person_nk, store_nk, customer_name, email_address, phone, territory_nk, valid_from
    Retorna customer_key.
    """
    current = fetch_one(dw_conn, """
        SELECT * FROM dw.dim_customer
        WHERE customer_nk = %s AND is_current = true
    """, (row["customer_nk"],))
    today = row.get("valid_from") or date.today()

    if not current:
        execute(dw_conn, """
            INSERT INTO dw.dim_customer
              (customer_nk, customer_type, person_nk, store_nk, customer_name, email_address, phone, territory_nk,
               valid_from, valid_to, is_current)
            VALUES
              (%(customer_nk)s, %(customer_type)s, %(person_nk)s, %(store_nk)s, %(customer_name)s, %(email_address)s, %(phone)s, %(territory_nk)s,
               %(valid_from)s, NULL, true)
        """, row)
        new_key = fetch_one(dw_conn, "SELECT currval(pg_get_serial_sequence('dw.dim_customer','customer_key')) AS id")["id"]
        return new_key

    changed = any([
        _different(current["customer_type"], row["customer_type"]),
        _different(current["person_nk"], row["person_nk"]),
        _different(current["store_nk"], row["store_nk"]),
        _different(current["customer_name"], row["customer_name"]),
        _different(current["email_address"], row["email_address"]),
        _different(current["phone"], row["phone"]),
        _different(current["territory_nk"], row["territory_nk"]),
    ])
    if not changed:
        return current["customer_key"]

    execute(dw_conn, """
        UPDATE dw.dim_customer
           SET valid_to = %(yesterday)s, is_current = false
         WHERE customer_key = %(customer_key)s
    """, {"yesterday": today, "customer_key": current["customer_key"]})

    execute(dw_conn, """
        INSERT INTO dw.dim_customer
          (customer_nk, customer_type, person_nk, store_nk, customer_name, email_address, phone, territory_nk,
           valid_from, valid_to, is_current)
        VALUES
          (%(customer_nk)s, %(customer_type)s, %(person_nk)s, %(store_nk)s, %(customer_name)s, %(email_address)s, %(phone)s, %(territory_nk)s,
           %(valid_from)s, NULL, true)
    """, row)
    new_key = fetch_one(dw_conn, "SELECT currval(pg_get_serial_sequence('dw.dim_customer','customer_key')) AS id")["id"]
    return new_key