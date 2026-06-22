# Databricks notebook source
%sql
CREATE OR REPLACE TABLE dev_uhccd.customer_360.gold_customer_360 AS
WITH orders_agg AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        SUM(order_amount) AS total_revenue,
        AVG(order_amount) AS avg_order_value,
        MIN(order_timestamp) AS first_order_date,
        MAX(order_timestamp) AS last_order_date
    FROM dev_uhccd.customer_360.silver_orders
    GROUP BY customer_id
),
events_agg AS (
    SELECT
        customer_id,
        COUNT(*) AS views_last_30_days
    FROM dev_uhccd.customer_360.silver_events
    WHERE event_type = 'view_product'
      AND event_time >= current_date() - INTERVAL 30 DAYS
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.city,
    c.acquisition_channel,
    
    o.first_order_date,
    o.last_order_date,
    
    DATEDIFF(current_date(), o.last_order_date) AS days_since_last_order,
    
    o.total_orders,
    o.total_revenue,
    o.avg_order_value,
    
    COALESCE(e.views_last_30_days, 0) AS views_last_30_days

FROM dev_uhccd.customer_360.silver_customers c
LEFT JOIN orders_agg o
  ON c.customer_id = o.customer_id
LEFT JOIN events_agg e
  ON c.customer_id = e.customer_id;
