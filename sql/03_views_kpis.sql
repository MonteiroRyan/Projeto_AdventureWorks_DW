-- Views/consultas auxiliares para KPIs

-- 1) Receita total e 2) Receita líquida e 3) Margem
CREATE OR REPLACE VIEW dw.v_kpi_sales_revenue AS
SELECT
  d.year,
  d.month,
  SUM(fs.total_due_line) AS total_revenue,
  SUM(fs.line_subtotal) AS net_sales,
  SUM(fs.gross_margin_amount) AS gross_margin,
  CASE WHEN SUM(fs.line_subtotal) > 0
       THEN SUM(fs.gross_margin_amount) / SUM(fs.line_subtotal)
       ELSE 0 END AS gross_margin_pct
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year, d.month;

-- 4) Quantidade vendida
CREATE OR REPLACE VIEW dw.v_kpi_units_sold AS
SELECT d.year, d.month, SUM(order_qty) AS units_sold
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year, d.month;

-- 5) Ticket médio
CREATE OR REPLACE VIEW dw.v_kpi_aov AS
SELECT
  d.year,
  d.month,
  SUM(fs.total_due_line) / NULLIF(COUNT(DISTINCT fs.sales_order_number), 0)::numeric AS avg_order_value
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year, d.month;

-- 6) Desconto médio ponderado
CREATE OR REPLACE VIEW dw.v_kpi_avg_discount AS
SELECT
  d.year, d.month,
  CASE WHEN SUM(fs.line_subtotal) > 0
       THEN SUM(fs.unit_price_discount * fs.line_subtotal) / SUM(fs.line_subtotal)
       ELSE 0 END AS avg_discount_pct
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year, d.month;

-- 7) Tempo médio de entrega
CREATE OR REPLACE VIEW dw.v_kpi_shipping_days AS
SELECT d.year, d.month, AVG(fs.shipping_days) AS avg_shipping_days
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year, d.month;

-- 8) Entrega no prazo (%)
CREATE OR REPLACE VIEW dw.v_kpi_on_time_delivery AS
SELECT d.year, d.month, AVG(CASE WHEN fs.on_time_delivery THEN 1 ELSE 0 END)::numeric AS on_time_rate
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year, d.month;

-- 9) Receita por território
CREATE OR REPLACE VIEW dw.v_kpi_revenue_by_territory AS
SELECT
  d.year, d.month,
  t.name AS territory,
  SUM(fs.total_due_line) AS revenue
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.order_date_key
LEFT JOIN dw.dim_territory t ON t.territory_key = fs.territory_key
GROUP BY d.year, d.month, t.name;

-- 10) Custo total de compras
CREATE OR REPLACE VIEW dw.v_kpi_total_purchases AS
SELECT d.year, d.month, SUM(fp.line_total) AS total_purchases
FROM dw.fact_purchases fp
JOIN dw.dim_date d ON d.date_key = fp.order_date_key
GROUP BY d.year, d.month;