-- Dimensões

-- dim_date (chave substituta natural YYYYMMDD)
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key int PRIMARY KEY,   -- yyyymmdd
  full_date date NOT NULL UNIQUE,
  year int NOT NULL,
  quarter int NOT NULL,
  month int NOT NULL,
  day int NOT NULL,
  week int NOT NULL,
  day_of_week int NOT NULL, -- 1..7
  is_weekend boolean NOT NULL
);

-- dim_product (SCD2)
CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_key bigserial PRIMARY KEY,
  product_nk int NOT NULL, -- Production.Product.ProductID
  product_name text,
  product_number text,
  color text,
  size text,
  style text,
  subcategory text,
  category text,
  standard_cost numeric(18,4),
  list_price numeric(18,4),
  valid_from date NOT NULL,
  valid_to date,
  is_current boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_product_current ON dw.dim_product(product_nk) WHERE is_current;

-- dim_customer (SCD2)
CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_key bigserial PRIMARY KEY,
  customer_nk int NOT NULL, -- Sales.Customer.CustomerID
  customer_type text,       -- 'Individual' ou 'Store'
  person_nk int,            -- Person.BusinessEntityID
  store_nk int,             -- Store.BusinessEntityID
  customer_name text,
  email_address text,
  phone text,
  territory_nk int,
  valid_from date NOT NULL,
  valid_to date,
  is_current boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current ON dw.dim_customer(customer_nk) WHERE is_current;

-- dim_territory (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_territory (
  territory_key bigserial PRIMARY KEY,
  territory_nk int NOT NULL UNIQUE,
  name text,
  country_region_code text,
  "group" text
);

-- dim_employee (SCD1 - vendedor)
CREATE TABLE IF NOT EXISTS dw.dim_employee (
  employee_key bigserial PRIMARY KEY,
  employee_nk int NOT NULL UNIQUE, -- HumanResources.Employee.BusinessEntityID
  employee_name text
);

-- dim_store (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_store (
  store_key bigserial PRIMARY KEY,
  store_nk int NOT NULL UNIQUE, -- Sales.Store.BusinessEntityID
  store_name text
);

-- dim_shipmethod (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_shipmethod (
  ship_method_key bigserial PRIMARY KEY,
  ship_method_nk int NOT NULL UNIQUE, -- Purchasing.ShipMethod.ShipMethodID
  name text
);

-- dim_promotion (SCD1) -> Sales.SpecialOffer
CREATE TABLE IF NOT EXISTS dw.dim_promotion (
  promotion_key bigserial PRIMARY KEY,
  promotion_nk int NOT NULL UNIQUE, -- SpecialOfferID
  description text,
  discount_pct numeric(9,6),
  "type" text,
  category text
);

-- dim_vendor (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_vendor (
  vendor_key bigserial PRIMARY KEY,
  vendor_nk int NOT NULL UNIQUE, -- Purchasing.Vendor.BusinessEntityID
  vendor_name text
);

-- dim_creditcard (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_creditcard (
  credit_card_key bigserial PRIMARY KEY,
  credit_card_nk int NOT NULL UNIQUE, -- CreditCardID
  card_type text
);

-- dim_location (SCD1) -> Production.Location
CREATE TABLE IF NOT EXISTS dw.dim_location (
  location_key bigserial PRIMARY KEY,
  location_nk int NOT NULL UNIQUE, -- LocationID
  location_name text
);

-- Fatos

-- fact_sales
CREATE TABLE IF NOT EXISTS dw.fact_sales (
  fact_sales_id bigserial PRIMARY KEY,
  order_date_key int NOT NULL REFERENCES dw.dim_date(date_key),
  due_date_key int REFERENCES dw.dim_date(date_key),
  ship_date_key int REFERENCES dw.dim_date(date_key),

  customer_key bigint REFERENCES dw.dim_customer(customer_key),
  product_key bigint REFERENCES dw.dim_product(product_key),
  territory_key bigint REFERENCES dw.dim_territory(territory_key),
  employee_key bigint REFERENCES dw.dim_employee(employee_key),
  store_key bigint REFERENCES dw.dim_store(store_key),
  ship_method_key bigint REFERENCES dw.dim_shipmethod(ship_method_key),
  promotion_key bigint REFERENCES dw.dim_promotion(promotion_key),
  credit_card_key bigint REFERENCES dw.dim_creditcard(credit_card_key),

  sales_order_number text,  -- degenerate
  sales_order_line_id int,  -- SalesOrderDetailID
  order_qty int NOT NULL,
  unit_price numeric(18,4) NOT NULL,
  unit_price_discount numeric(18,6) NOT NULL,

  line_subtotal numeric(18,4) NOT NULL, -- qty * unit_price * (1 - discount)
  tax_amount_alloc numeric(18,4) NOT NULL DEFAULT 0,
  freight_amount_alloc numeric(18,4) NOT NULL DEFAULT 0,
  total_due_line numeric(18,4) NOT NULL, -- subtotal + tax_alloc + freight_alloc

  standard_cost_amount numeric(18,4) NOT NULL DEFAULT 0,
  gross_margin_amount numeric(18,4) NOT NULL DEFAULT 0,

  shipping_days int,
  on_time_delivery boolean,

  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_dates ON dw.fact_sales(order_date_key, ship_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product ON dw.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer ON dw.fact_sales(customer_key);

-- fact_purchases
CREATE TABLE IF NOT EXISTS dw.fact_purchases (
  fact_purchases_id bigserial PRIMARY KEY,
  order_date_key int NOT NULL REFERENCES dw.dim_date(date_key),
  vendor_key bigint REFERENCES dw.dim_vendor(vendor_key),
  product_key bigint REFERENCES dw.dim_product(product_key),
  location_key bigint REFERENCES dw.dim_location(location_key),

  purchase_order_number text,
  purchase_order_line_id int,
  order_qty int NOT NULL,
  unit_price numeric(18,4) NOT NULL,
  line_total numeric(18,4) NOT NULL,

  created_at timestamptz NOT NULL DEFAULT now()
);

-- fact_inventory_snapshot (p.ex. mês a mês)
CREATE TABLE IF NOT EXISTS dw.fact_inventory_snapshot (
  fact_inventory_snapshot_id bigserial PRIMARY KEY,
  snapshot_date_key int NOT NULL REFERENCES dw.dim_date(date_key),
  product_key bigint REFERENCES dw.dim_product(product_key),
  location_key bigint REFERENCES dw.dim_location(location_key),
  quantity_on_hand int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);