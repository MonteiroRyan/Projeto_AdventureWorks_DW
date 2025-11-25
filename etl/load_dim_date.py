from datetime import date, timedelta
from .db import get_conn_dw, execute

def daterange(d1, d2):
    for n in range((d2 - d1).days + 1):
        yield d1 + timedelta(days=n)

def yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day

def load_dim_date(start=date(2000,1,1), end=date(2015,12,31)):
    dw = get_conn_dw()
    try:
        for d in daterange(start, end):
            execute(dw, """
                INSERT INTO dw.dim_date(date_key, full_date, year, quarter, month, day, week, day_of_week, is_weekend)
                VALUES (%s, %s, %s, EXTRACT(QUARTER FROM %s)::int, %s, %s, EXTRACT(WEEK FROM %s)::int,
                        EXTRACT(ISODOW FROM %s)::int, CASE WHEN EXTRACT(ISODOW FROM %s) IN (6,7) THEN true ELSE false END)
                ON CONFLICT (date_key) DO NOTHING
            """, (yyyymmdd(d), d, d.year, d, d.month, d.day, d, d, d))
    finally:
        dw.close()

if __name__ == "__main__":
    load_dim_date()