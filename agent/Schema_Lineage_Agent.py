# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
import hashlib

def calculate_schema_hash(schema_str):
    return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()

def track_and_enforce_lineage(table_name):
    print(f"Analyzing schema lineage for: {table_name}")
    try:
        df = spark.table(table_name)
    except Exception as e:
        print(f"Table {table_name} not found. Skipping lineage checks.")
        return
        
    # Generate unified string representation of the schema string
    schema_str = ",".join([f"{c}:{t}" for c, t in sorted(df.dtypes)])
    current_hash = calculate_schema_hash(schema_str)
    
    # Fetch last known state from schema registry
    registry_table = "dev_uhccd.customer_360.schema_registry"
    
    last_registered = spark.sql(f"""
        SELECT schema_hash FROM {registry_table} 
        WHERE table_name = '{table_name}' 
        ORDER BY recorded_at DESC LIMIT 1
    """).collect()
    
    if not last_registered:
        print(f"First-time registration for {table_name}. Initializing baseline lineage.")
        spark.sql(f"""
            INSERT INTO {registry_table} VALUES 
            ('{table_name}', '{current_hash}', current_timestamp())
        """)
    elif last_registered[0]["schema_hash"] != current_hash:
        print(f"⚠️ SCHEMA DRIFT DETECTED FOR {table_name}!")
        print(f"Previous Hash: {last_registered[0]['schema_hash']}")
        print(f"New Hash: {current_hash}")
        
        # Log evolution entry into registry
        spark.sql(f"""
            INSERT INTO {registry_table} VALUES 
            ('{table_name}', '{current_hash}', current_timestamp())
        """)
        # In a full orchestration pipeline, flag an alert or trigger downstream updates here
    else:
        print(f"Schema lineage for {table_name} matches stable baseline.")

# Execute schema checks across target silver layers
entities = ["silver_customers", "silver_orders", "silver_events"]
for entity in entities:
    track_and_enforce_lineage(f"dev_uhccd.customer_360.{entity}")
