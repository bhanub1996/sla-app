# Databricks notebook source
%sql
-- Clean and deduplicate Customers
CREATE OR REPLACE TEMPORARY VIEW clean_customers_v AS
SELECT *
FROM dev_uhccd.customer_360.bronze_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ingest_ts DESC) = 1;

-- Merge Customers into Silver
MERGE INTO dev_uhccd.customer_360.silver_customers AS target
USING clean_customers_v AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- Clean and deduplicate Orders
CREATE OR REPLACE TEMP VIEW clean_orders_v AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY ingest_ts DESC
           ) AS rn
    FROM dev_uhccd.customer_360.bronze_orders
)
WHERE rn = 1;

-- Merge Orders into Silver
MERGE INTO dev_uhccd.customer_360.silver_orders AS target
USING clean_orders_v AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- COMMAND ----------

-- Clean and deduplicate Events
CREATE OR REPLACE TEMP VIEW clean_events_v AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id
               ORDER BY ingest_ts DESC
           ) AS rn
    FROM dev_uhccd.customer_360.bronze_events
)
WHERE rn = 1;

-- Merge Events into Silver
MERGE INTO dev_uhccd.customer_360.silver_events AS target
USING clean_events_v AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;
