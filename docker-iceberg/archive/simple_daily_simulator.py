#!/usr/bin/env python3
"""
Simplified Daily Data Simulator - Fixed Version
Focuses on working simulation without Column/int() issues
"""

import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, rand
from pathlib import Path
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleDailyDataSimulator:
    """Simplified Daily Data Simulator for all 8 Home Credit tables"""
    
    def __init__(self):
        """Initialize the simulator"""
        self.spark = self.init_spark()
        self.simulation_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Simple Daily Data Simulator initialized for {self.simulation_date}")

    def init_spark(self):
        """Initialize Spark session with Iceberg support"""
        spark = SparkSession.builder \
            .appName("SimpleDailyDataSimulator") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "/home/iceberg/warehouse") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
        return spark

    def simulate_table(self, table_name, csv_path, sample_rate=0.01):
        """Generic table simulation method"""
        try:
            logger.info(f"Simulating {table_name} data for {self.simulation_date}")
            
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            original_count = original_df.count()
            logger.info(f"Original {table_name} has {original_count:,} records")
            
            # Sample data with fixed seed
            seed_val = int(abs(hash(self.simulation_date + table_name)) % 2147483647)
            daily_sample = original_df.sample(fraction=sample_rate, seed=seed_val)
            sample_count = daily_sample.count()
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(self.simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"{table_name}_batch_{self.simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to warehouse (using Parquet for simplicity)
            output_path = f"/home/iceberg/warehouse/{table_name}_daily"
            daily_sample_with_meta.write.mode("overwrite").parquet(output_path)
            
            logger.info(f"✅ {table_name}: {sample_count:,} records simulated and saved")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": sample_count,
                "original_count": original_count,
                "sample_rate": sample_rate,
                "output_path": output_path
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate {table_name}: {str(e)}")
            return {"status": "error", "table": table_name, "message": str(e)}

    def run_all_simulations(self):
        """Run simulation for all 8 Home Credit tables"""
        logger.info("🚀 Running simulation for all 8 Home Credit tables")
        
        # Define all 8 tables with their paths and sample rates
        tables = {
            "application_train": {
                "path": "/data/home-credit-default-risk-dataset/application_train.csv",
                "sample_rate": 0.01  # 1%
            },
            "application_test": {
                "path": "/data/home-credit-default-risk-dataset/application_test.csv", 
                "sample_rate": 0.02  # 2%
            },
            "bureau": {
                "path": "/data/home-credit-default-risk-dataset/bureau.csv",
                "sample_rate": 0.005  # 0.5%
            },
            "bureau_balance": {
                "path": "/data/home-credit-default-risk-dataset/bureau_balance.csv",
                "sample_rate": 0.001  # 0.1% (very large table)
            },
            "credit_card_balance": {
                "path": "/data/home-credit-default-risk-dataset/credit_card_balance.csv",
                "sample_rate": 0.002  # 0.2%
            },
            "installments_payments": {
                "path": "/data/home-credit-default-risk-dataset/installments_payments.csv",
                "sample_rate": 0.003  # 0.3%
            },
            "POS_CASH_balance": {
                "path": "/data/home-credit-default-risk-dataset/POS_CASH_balance.csv",
                "sample_rate": 0.002  # 0.2%
            },
            "previous_application": {
                "path": "/data/home-credit-default-risk-dataset/previous_application.csv",
                "sample_rate": 0.01   # 1%
            }
        }
        
        results = {
            "simulation_date": self.simulation_date,
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }
        
        # Process each table
        for table_name, config in tables.items():
            result = self.simulate_table(table_name, config["path"], config["sample_rate"])
            results["tables"][table_name] = result
        
        # Generate summary report
        self.generate_summary_report(results)
        
        return results

    def generate_summary_report(self, results):
        """Generate a summary report"""
        print("\n" + "="*70)
        print("📊 DAILY DATA SIMULATION REPORT - ALL 8 TABLES")
        print("="*70)
        print(f"📅 Date: {results['simulation_date']}")
        print(f"⏰ Timestamp: {results['timestamp']}")
        print("-"*70)
        
        total_records = 0
        successful_tables = 0
        
        for table_name, table_result in results['tables'].items():
            if table_result.get('status') == 'success':
                record_count = table_result.get('record_count', 0)
                original_count = table_result.get('original_count', 0)
                sample_rate = table_result.get('sample_rate', 0) * 100
                
                print(f"✅ {table_name:20} | {record_count:>8,} records | {sample_rate:>5.1f}% | {original_count:>10,} orig")
                total_records += record_count
                successful_tables += 1
            else:
                print(f"❌ {table_name:20} | ERROR: {table_result.get('message', 'Unknown')}")
        
        print("-"*70)
        print(f"📈 Summary:")
        print(f"   ✅ Successful Tables: {successful_tables}/8")
        print(f"   📊 Total Records Simulated: {total_records:,}")
        print(f"   📁 Output Location: /home/iceberg/warehouse/[table]_daily/")
        print("="*70)

def main():
    """Main entry point"""
    print("🚀 Simple Daily Data Simulator for All 8 Home Credit Tables")
    print("="*70)
    
    # Initialize and run simulator
    simulator = SimpleDailyDataSimulator()
    results = simulator.run_all_simulations()
    
    print("\n🎉 Simulation completed!")
    print("📁 Data saved to /home/iceberg/warehouse/")
    print("🔄 Ready for DQ analysis!")

if __name__ == "__main__":
    main()
