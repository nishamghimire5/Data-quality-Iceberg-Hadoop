#!/usr/bin/env python3
"""Simple test to identify the Column->int issue"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, rand
import random

# Initialize Spark
spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "/home/iceberg/warehouse") \
    .getOrCreate()

print("Reading CSV...")
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/data/home-credit-default-risk-dataset/application_train.csv")

print("Original count:", df.count())

# Test sampling
simulation_date = "2025-06-15"
sample_fraction = random.uniform(0.005, 0.02)
seed_value = abs(hash(str(simulation_date))) % 2147483647

print(f"Sample fraction: {sample_fraction}")
print(f"Seed value: {seed_value}")

# Try the sampling
daily_sample = df.sample(fraction=sample_fraction, seed=seed_value)
print("Sample count:", daily_sample.count())

print("Success!")
