#!/usr/bin/env python3
"""
Simple test to debug the Column/int() error
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, rand
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_hdfs_connection():
    """Test basic HDFS connection and file reading"""
    try:
        # Initialize Spark
        spark = SparkSession.builder \
            .appName("DebugTest") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        logger.info("✅ Spark session created successfully")
        
        # Test HDFS connection
        spark.sql("CREATE DATABASE IF NOT EXISTS dq_warehouse").show()
        logger.info("✅ HDFS warehouse connection successful")
        
        # Test reading a file from HDFS
        logger.info("📁 Testing file read from HDFS...")
        df = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/application_train.csv")
        
        logger.info(f"📊 File read successful! Rows: {df.count()}, Columns: {len(df.columns)}")
        
        # Test sampling
        logger.info("🎲 Testing sampling...")
        simulation_date = "2025-06-16"
        sample_fraction = 0.01  # 1%
        
        # The problematic line - let's debug this
        logger.info(f"🔍 Debug: simulation_date = {simulation_date}, type = {type(simulation_date)}")
        seed_value = abs(hash(str(simulation_date))) % 2147483647
        logger.info(f"🔍 Debug: seed_value = {seed_value}, type = {type(seed_value)}")
        
        sampled_df = df.sample(fraction=sample_fraction, seed=seed_value)
        logger.info(f"✅ Sampling successful! Sample rows: {sampled_df.count()}")
        
        # Test adding metadata
        logger.info("📋 Testing metadata addition...")
        with_meta = sampled_df \
            .withColumn("ingestion_date", lit(simulation_date)) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("data_source", lit("debug_test")) \
            .withColumn("batch_id", lit(f"debug_batch_{simulation_date}")) \
            .withColumn("simulation_id", monotonically_increasing_id())
        
        logger.info(f"✅ Metadata addition successful! Final columns: {len(with_meta.columns)}")
        
        # Show schema
        logger.info("📋 Final schema:")
        with_meta.printSchema()
        
        spark.stop()
        logger.info("✅ Test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_hdfs_connection()
