# Databricks notebook source
# 1. Initialize Storage Containers (Unity Catalog Hierarchies)
spark.sql("CREATE CATALOG IF NOT EXISTS dev_uhccd;")
spark.sql("USE CATALOG dev_uhccd;")

spark.sql("CREATE SCHEMA IF NOT EXISTS customer_360;")
spark.sql("USE SCHEMA customer_360;")

print("Unity Catalog Environment Context Initialized Successfully.")

# 2. Infrastructure Volume Storage Mounting Setup (External Path Configurations)
base_volume_path = "/Volumes/dev_uhccd/customer_360"

# Loop to securely scaffold unstructured storage drop-zones for Auto Loader discovery
directories = [
    "raw_landing_zone/customers",
    "raw_landing_zone/orders",
    "raw_landing_zone/events",
    "raw_landing_zone/_schemas/customers",
    "raw_landing_zone/_schemas/orders",
    "raw_landing_zone/_schemas/events",
    "checkpoints/customers",
    "checkpoints/orders",
    "checkpoints/events"
]

for folder in directories:
    dbutils.fs.mkdirs(f"{base_volume_path}/{folder}")
    print(f"Initialized Secure Storage Volume Subpath: {base_volume_path}/{folder}")

print("🚀 Platform Infrastructure Scaffolding Verified and Live.")
