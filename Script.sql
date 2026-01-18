-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Bronze Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sales Table

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_bronze
AS SELECT *,current_timestamp() as ingestion_date FROM cloud_files("/Volumes/project/dlt_ecomm/sales", "csv",map("header", "true", "cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Customers Table

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers_bronze
AS SELECT *, current_timestamp() as ingestion_date FROM cloud_files("/Volumes/project/dlt_ecomm/customers", "csv",map("header", "true", "cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Product Table

-- COMMAND ----------

CREATE STREAMING LIVE TABLE products_bronze
AS SELECT *, current_timestamp() as ingestion_date FROM cloud_files("/Volumes/project/dlt_ecomm/products", "csv",map("header", "true", "cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sales Table

-- COMMAND ----------

-- Applied data quality constraints
-- Dropped duplicate records
CREATE STREAMING LIVE TABLE sales_silver
(
 CONSTRAINT valid_ids EXPECT (order_id is not null AND customer_id is not null AND product_id is not null) on violation drop row,
 CONSTRAINT valid_transaction_id EXPECT (transaction_id is not null),
 CONSTRAINT positive_quantity EXPECT (quantity > 0),
 CONSTRAINT valid_amount EXPECT (total_amount > 0),
 CONSTRAINT reasonable_discount EXPECT (discount_amount >= 0 AND discount_amount < total_amount),
 CONSTRAINT recent_date EXPECT (order_date <= current_date())
)
as
select distinct * from Stream(LIVE.sales_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Customer Table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver (
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email EXPECT (customer_email LIKE '%@%.%'),
  CONSTRAINT valid_city EXPECT (customer_city IS NOT NULL)
)
as
select distinct * from Stream(LIVE.customers_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Product Table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE products_silver (
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT positive_price EXPECT (product_price > 0),
  CONSTRAINT mandatory_category EXPECT (product_category IS NOT NULL)
)
as
select distinct * from Stream(LIVE.products_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sales Table

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_gold
as
select * from Stream(LIVE.sales_silver);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Customer Table

-- COMMAND ----------

-- Applied SCD Type 2 on customers table
CREATE OR REFRESH STREAMING TABLE customers_gold;

APPLY CHANGES INTO live.customers_gold
FROM stream(live.customers_silver)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY sequenceNum
COLUMNS * EXCEPT (operation, sequenceNum, _rescued_data, ingestion_date)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- Active customers
create streaming table active_customers_gold as 
select customer_id,customer_name,customer_email,customer_city,customer_state from STREAM(live.customers_gold) where `__END_AT` is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Product Table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE products_gold;

-- Applied SCD Type 1 on products table
APPLY CHANGES INTO live.products_gold
FROM stream(live.products_silver)
KEYS (product_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY seqNum
COLUMNS * EXCEPT (operation,seqNum ,_rescued_data,ingestion_date)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inner Join

-- COMMAND ----------

-- DBTITLE 1,Untitled
CREATE LIVE TABLE sales_summary_gold
COMMENT "Final joined table for BI: Connects Sales, Customers, and Products."
AS
SELECT
  -- Sales Info
  s.order_id,
  s.transaction_id,
  s.order_date,
  s.quantity,
  s.total_amount,
  s.discount_amount,
  
  -- Customer Info
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  
  -- Product Info
  p.product_id,
  p.product_name,
  p.product_category,
  p.product_price
FROM LIVE.sales_gold s
INNER JOIN LIVE.active_customers_gold c ON s.customer_id = c.customer_id
INNER JOIN LIVE.products_gold p ON s.product_id = p.product_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Left Join

-- COMMAND ----------

CREATE LIVE TABLE left_join_sales
COMMENT "Final joined table for BI: Connects Sales, Customers, and Products."
AS
SELECT
  -- Sales Info
  s.order_id,
  s.transaction_id,
  s.order_date,
  s.quantity,
  s.total_amount,
  s.discount_amount,
  
  -- Customer Info
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  
  -- Product Info
  p.product_id,
  p.product_name,
  p.product_category,
  p.product_price
FROM LIVE.sales_gold s
LEFT JOIN LIVE.active_customers_gold c ON s.customer_id = c.customer_id
LEFT JOIN LIVE.products_gold p ON s.product_id = p.product_id;
