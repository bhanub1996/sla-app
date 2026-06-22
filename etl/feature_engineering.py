# Databricks notebook source
%sql
CREATE OR REPLACE TABLE dev_uhccd.customer_360.features_churn_clv AS
WITH customer_base AS (
    SELECT 
        customer_id,
        city,
        acquisition_channel,
        total_orders,
        total_revenue,
        avg_order_value,
        days_since_last_order,
        views_last_30_days
    FROM dev_uhccd.customer_360.gold_customer_360
),
behavioral_metrics AS (
    SELECT 
        customer_id,
        -- Churn Risk Proxy Calculation: Inactive for > 30 days with low recent app session hits
        CASE 
            WHEN days_since_last_order > 30 AND views_last_30_days < 2 THEN 1 
            ELSE 0 
        END AS churn_label,
        
        -- Customer Lifetime Value Vector Strategy Calculation
        CASE 
            WHEN total_revenue > 500 AND total_orders > 5 THEN 'High Value'
            WHEN total_revenue BETWEEN 100 AND 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS clv_segment
    FROM dev_uhccd.customer_360.gold_customer_360
)
SELECT 
    c.customer_id,
    c.city,
    c.acquisition_channel,
    c.total_orders,
    c.total_revenue,
    c.avg_order_value,
    c.days_since_last_order,
    c.views_last_30_days,
    b.churn_label,
    b.clv_segment,
    current_timestamp() AS feature_generation_ts
FROM customer_base c
JOIN behavioral_metrics b 
  ON c.customer_id = b.customer_id;

SELECT * FROM dev_uhccd.customer_360.features_churn_clv LIMIT 10;
