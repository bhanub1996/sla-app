# Databricks notebook source
config_df = spark.table("dev_uhccd.customer_360.pipeline_config")

for row in config_df.collect():
    entity = row["entity"]
    active = row["is_active"]
    
    if active:
        print(f"Running pipeline for {entity}")
        
        # Bronze -> already streaming
        
        # Silver MERGE
        spark.sql(f"""
        MERGE INTO dev_uhccd.customer_360.silver_{entity} t
        USING (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {row['primary_key']} ORDER BY ingest_ts DESC) rn
                FROM dev_uhccd.customer_360.bronze_{entity}
            ) WHERE rn = 1
        ) s
        ON t.{row['primary_key']} = s.{row['primary_key']}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
