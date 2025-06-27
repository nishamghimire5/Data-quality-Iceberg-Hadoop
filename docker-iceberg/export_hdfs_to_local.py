#!/usr/bin/env python3
"""
Export Iceberg daily tables from HDFS to local /warehouse for DQOps
"""

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

# List of daily tables to export
TABLES = [
    "application_train_daily",
    "application_test_daily",
    "bureau_daily",
    "bureau_balance_daily",
    "credit_card_balance_daily",
    "installments_payments_daily",
    "POS_CASH_balance_daily",
    "previous_application_daily"
]

for table in TABLES:
    hdfs_path = f"hdfs://namenode:9000/warehouse/{table}"
    local_path = f"/warehouse/{table}"
    print(f"\n➡️  Exporting {hdfs_path} to {local_path} (Parquet)...")
    try:
        df = spark.read.parquet(hdfs_path)
        df.write.mode("overwrite").parquet(local_path)
        print(f"   ✅ Exported {table} to {local_path}")
    except Exception as e:
        print(f"   ❌ Failed to export {table}: {str(e)}")

print("\n📁 All daily tables exported to /warehouse!") 