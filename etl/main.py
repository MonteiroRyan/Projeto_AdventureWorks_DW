from datetime import date
from .load_dim_date import load_dim_date
from .load_dimensions import load_all_dimensions
from .load_fact_sales import load_fact_sales
from .load_fact_purchases import load_fact_purchases
from .load_fact_inventory_snapshot import load_inventory_snapshot

def full_load():
    # Datas (ajuste range conforme seu OLTP)
    load_dim_date()
    # Dimensões
    load_all_dimensions()
    # Fatos
    load_fact_sales(incremental=False)   # primeira carga pode ser full
    load_fact_purchases(truncate=True)
    # Snapshot de inventário da data corrente (ou fim do mês)
    load_inventory_snapshot(date.today())

def daily_incremental():
    load_all_dimensions()            # mantém dims atualizadas (SCD2 em produto/cliente)
    load_fact_sales(incremental=True)
    # compras e inventário podem ser agendados conforme necessidade
    # load_fact_purchases(truncate=False)  # Ex.: implementar watermark semelhante ao sales
    # load_inventory_snapshot(date.today())

if __name__ == "__main__":
    full_load()