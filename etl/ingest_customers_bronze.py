# Databricks notebook source

entity="customers"
raw_path=f"/Volumes/dev_uhccd/customer_360/raw_landing_zone/{entity}"
checkpoint_path=f"/Volumes/dev_uhccd/customer_360/checkpoints/{entity}"
schema_location=f"/Volumes/dev_uhccd/customer_360/raw_landing_zone/_schemas/{entity}"

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.maxFilesPerTrigger", 1) \
    .option("cloudFiles.schemaLocation", schema_location) \
    .option("multiLine", "true") \
    .load(raw_path)

display(df)

from pyspark.sql.functions import current_timestamp, input_file_name, col
enriched_df=df.withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

display(enriched_df)

enriched_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .trigger(once=True) \
    .toTable(f"dev_uhccd.customer_360.bronze_{entity}")


entity="orders"
raw_path=f"/Volumes/dev_uhccd/customer_360/raw_landing_zone/{entity}"
checkpoint_path=f"/Volumes/dev_uhccd/customer_360/checkpoints/{entity}"
schema_location=f"/Volumes/dev_uhccd/customer_360/raw_landing_zone/_schemas/{entity}"

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.maxFilesPerTrigger", 1) \
    .option("cloudFiles.schemaLocation", schema_location) \
    .option("multiLine", "true") \
    .load(raw_path)

display(df)

from pyspark.sql.functions import current_timestamp, input_file_name, col
enriched_df=df.withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

display(enriched_df)

enriched_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .trigger(once=True) \
    .toTable(f"dev_uhccd.customer_360.bronze_{entity}")


entity="events"
raw_path=f"/Volumes/dev_uhccd/customer_360/raw_landing_zone/{entity}"
checkpoint_path=f"/Volumes/dev_uhccd/customer_360/checkpoints/{entity}"
schema_location=f"/Volumes/dev_uhccd/customer_360/raw_landing_zone/_schemas/{entity}"

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.maxFilesPerTrigger", 1) \
    .option("cloudFiles.schemaLocation", schema_location) \
    .option("multiLine", "true") \
    .load(raw_path)

display(df)

from pyspark.sql.functions import current_timestamp, input_file_name, col
enriched_df=df.withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

display(enriched_df)

enriched_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .trigger(once=True) \
    .toTable(f"dev_uhccd.customer_360.bronze_{entity}")
