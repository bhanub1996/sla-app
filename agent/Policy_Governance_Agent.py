# Databricks notebook source
import pyspark.sql.functions as F

def enforce_governance_rules():
    print("Initializing Enterprise Policy Governance Agent scan...")
    
    # 1. Fetch Active Governance Guardrails
    rules_df = spark.table("dev_uhccd.customer_360.policy_rules")
    rules = rules_df.collect()
    
    for rule in rules:
        entity = rule["entity"]
        rule_type = rule["rule_type"]
        rule_value = rule["rule_value"]
        
        table_path = f"dev_uhccd.customer_360.silver_{entity}"
        
        # Check if table exists prior to applying data scrubbing logic
        if spark.catalog.tableExists(table_path):
            
            # Rule Type: Column Presence Validation
            if rule_type == "mandatory_column":
                columns = spark.table(table_path).columns
                if rule_value not in columns:
                    raise ValueError(f"🚨 CRITICAL GOVERNANCE FAILURE: Table {table_path} misses required identifier: {rule_value}")
                print(f"✅ Governance Pass: {table_path} contains required primary structural key '{rule_value}'.")
            
            # Rule Type: Retention Management / Data Pruning
            elif rule_type == "retention_days":
                print(f"Enforcing lifecycle retention policy: Pruning {table_path} elements older than {rule_value} days.")
                # Execute point-in-time state purge using Delta Lake syntax
                spark.sql(f"""
                    DELETE FROM {table_path}
                    WHERE ingest_ts < current_date() - INTERVAL {rule_value} DAYS
                """)
                
            # Rule Type: Dynamic SLA Latency Enforcement
            elif rule_type == "max_latency_minutes":
                max_ts = spark.sql(f"SELECT MAX(ingest_ts) as last_ingest FROM {table_path}").collect()[0]["last_ingest"]
                if max_ts:
                    latency = spark.sql(f"SELECT DATEDIFF(minute, '{max_ts}', current_timestamp()) as diff").collect()[0]["diff"]
                    if latency > int(rule_value):
                        print(f"⚠️ SLA BREACH: {table_path} pipeline latency exceeds limit. Latency: {latency} mins. Limit: {rule_value} mins.")
                    else:
                        print(f"✅ SLA Compliance verified for {table_path}. Latency within threshold.")

enforce_governance_rules()
