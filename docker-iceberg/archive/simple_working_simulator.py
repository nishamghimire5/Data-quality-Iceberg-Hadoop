#!/usr/bin/env python3
"""
Simplified Daily Data Simulator - Working Version
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleDailySimulator:
    def __init__(self):
        self.spark = self.init_spark()
        self.simulation_date = datetime.now().strftime("%Y-%m-%d")
    
    def init_spark(self):
        """Initialize Spark session"""
        spark = SparkSession.builder \
            .appName("SimpleDailySimulator") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        # Test connection
        spark.sql("CREATE DATABASE IF NOT EXISTS dq_warehouse").show()
        logger.info("✅ Successfully connected to HDFS warehouse")
        return spark
    
    def simulate_table(self, csv_path, table_name):
        """Simulate data for a single table"""
        try:
            # Read data
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            logger.info(f"📁 Read {df.count()} rows from {csv_path}")
            
            # Sample 1% of data (fixed seed for reproducibility)
            sampled_df = df.sample(fraction=0.01, seed=42)
            
            # Add metadata
            final_df = sampled_df \
                .withColumn("ingestion_date", lit(self.simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"{table_name}_batch_{self.simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            record_count = final_df.count()
            
            # Write to HDFS (using parquet for now)
            output_path = f"hdfs://namenode:9000/warehouse/{table_name}_daily"
            final_df.write.mode("overwrite").parquet(output_path)
            
            logger.info(f"✅ {table_name}: {record_count} records written to {output_path}")
            return {"status": "success", "records": record_count, "table": table_name}
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate {table_name}: {str(e)}")
            return {"status": "error", "message": str(e), "table": table_name}
    
    def run_simulation(self):
        """Run simulation for all tables"""
        logger.info(f"🚀 Starting daily simulation for {self.simulation_date}")
        
        tables = [
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/application_train.csv", "application_train"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/application_test.csv", "application_test"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/bureau.csv", "bureau"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/bureau_balance.csv", "bureau_balance"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/credit_card_balance.csv", "credit_card_balance"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/installments_payments.csv", "installments_payments"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/POS_CASH_balance.csv", "POS_CASH_balance"),
            ("hdfs://namenode:9000/data/home-credit-default-risk-dataset/previous_application.csv", "previous_application")
        ]
        
        results = []
        successful = 0
        total_records = 0
        
        for csv_path, table_name in tables:
            result = self.simulate_table(csv_path, table_name)
            results.append(result)
            if result["status"] == "success":
                successful += 1
                total_records += result["records"]
        
        # Print summary
        print("\n" + "="*60)
        print("📊 SIMPLE DAILY SIMULATION REPORT")
        print("="*60)
        print(f"Date: {self.simulation_date}")
        print("-"*60)
        
        for result in results:
            if result["status"] == "success":
                print(f"✅ {result['table']}: {result['records']} records")
            else:
                print(f"❌ {result['table']}: {result['message']}")
        
        print("-"*60)
        print(f"📈 Summary:")
        print(f"   Total Records: {total_records}")
        print(f"   Successful Tables: {successful}/{len(tables)}")
        print(f"   Success Rate: {(successful/len(tables)*100):.1f}%")
        print("="*60)
        
        return results

if __name__ == "__main__":
    simulator = SimpleDailySimulator()
    results = simulator.run_simulation()
    print("\n🎉 Simple daily simulation completed!")
    print("📁 Check HDFS warehouse for generated data")
