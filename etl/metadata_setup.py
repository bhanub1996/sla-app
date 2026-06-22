# Databricks notebook source
%sql
CREATE TABLE IF NOT EXISTS dev_uhccd.customer_360.pipeline_config (
    entity STRING,
    primary_key STRING,
    watermark_column STRING,
    refresh_interval STRING,
    sla_minutes INT,
    is_active BOOLEAN,
    last_run TIMESTAMP
);

INSERT INTO dev_uhccd.customer_360.pipeline_config VALUES 
('customers', 'customer_id', 'signup_date', '1 hour', 60, true, NULL),
('orders', 'order_id', 'order_timestamp', '15 minutes', 15, true, NULL),
('events', 'event_id', 'event_time', '5 minutes', 5, true, NULL);

%sql
CREATE TABLE IF NOT EXISTS dev_uhccd.customer_360.schema_registry (
    table_name STRING,
    schema_hash STRING,
    recorded_at TIMESTAMP
);

%sql
CREATE TABLE IF NOT EXISTS dev_uhccd.customer_360.policy_rules (
    rule_id STRING,
    entity STRING,
    rule_type STRING,
    rule_value STRING
);

INSERT INTO dev_uhccd.customer_360.policy_rules VALUES 
('R1', 'orders', 'max_latency_minutes', '30'),
('R2', 'customers', 'mandatory_column', 'customer_id'),
('R3', 'events', 'retention_days', '90');
