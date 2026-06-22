# Databricks notebook source
%sql
UPDATE dev_uhccd.customer_360.pipeline_config
SET refresh_interval = 
    CASE 
        WHEN (SELECT COUNT(*) FROM dev_uhccd.customer_360.bronze_orders WHERE ingest_ts >= current_timestamp() - INTERVAL 1 HOUR) > 10000 THEN '5 minutes'
        WHEN (SELECT COUNT(*) FROM dev_uhccd.customer_360.bronze_orders WHERE ingest_ts >= current_timestamp() - INTERVAL 1 HOUR) > 10000 THEN '15 minutes'
        ELSE '1 hour'
    END
WHERE entity = 'orders';
