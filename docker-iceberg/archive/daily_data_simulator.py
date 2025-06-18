#!/usr/bin/env python3
"""
Daily Data Simulator for Home Credit Dataset
Implements Item 2: Prepare script that simulate data added everyday

This script simulates daily incremental data ingestion to Iceberg tables on HDFS,
providing realistic data volume and patterns for DQ monitoring.
"""

import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, rand
from pathlib import Path
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DailyDataSimulator:
    """Simulates daily data ingestion for Home Credit dataset"""
    
    def __init__(self, config_path="config/dq-config.json"):
        """Initialize the simulator"""
        self.config = self.load_config(config_path)
        self.spark = self.init_spark()
        self.simulation_date = datetime.now().strftime("%Y-%m-%d")
        self.storage_path = "/tmp/warehouse"  # Default, updated in init_spark
        
        logger.info(f"Daily Data Simulator initialized for {self.simulation_date}")

    def load_config(self, config_path):
        """Load configuration"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Config file {config_path} not found")
            return {}

    def init_spark(self):
        """Initialize Spark session"""
        try:
            spark = SparkSession.builder \
                .appName("DailyDataSimulator") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg.type", "hadoop") \
                .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
              # Test HDFS connectivity
            try:
                # Try to access HDFS first
                spark.sql("CREATE DATABASE IF NOT EXISTS dq_warehouse").show()
                logger.info("✅ Successfully connected to HDFS warehouse")
                self.storage_path = "hdfs://namenode:9000/warehouse"
            except Exception as e:
                logger.warning(f"⚠️  HDFS connection failed: {e}")
                logger.info("🔄 Using local storage instead...")
                # Update warehouse path to local
                spark.stop()
                spark = SparkSession.builder \
                    .appName("DailyDataSimulator_Local") \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.iceberg.type", "hadoop") \
                    .config("spark.sql.catalog.iceberg.warehouse", "/tmp/warehouse") \
                    .getOrCreate()
                self.storage_path = "/tmp/warehouse"
                
            return spark
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark session: {e}")
            raise

    def simulate_application_train_daily(self, simulation_date=None):
        """Simulate daily application_train data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating application_train data for {simulation_date}")
        
        try:
            # Read original data from HDFS
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/application_train.csv")
            
            # Sample data for daily simulation (0.5% to 2% of original)
            sample_fraction = random.uniform(0.005, 0.02)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "application", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"app_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.application_train_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate application_train data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_bureau_daily(self, simulation_date=None):
        """Simulate daily bureau data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating bureau data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/bureau.csv")
              # Sample data for daily simulation (1% to 3% of original)
            sample_fraction = random.uniform(0.01, 0.03)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "bureau", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"bureau_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.bureau_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate bureau data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_credit_card_balance_daily(self, simulation_date=None):
        """Simulate daily credit card balance data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating credit_card_balance data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/credit_card_balance.csv")
            
            # Sample data for daily simulation (0.2% to 1% of original - credit card data is large)
            sample_fraction = random.uniform(0.002, 0.01)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "credit_card", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"cc_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.credit_card_balance_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate credit_card_balance data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_application_test_daily(self, simulation_date=None):
        """Simulate daily application_test data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating application_test data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/application_test.csv")
            
            # Sample data for daily simulation (5-15% of original)
            sample_fraction = random.uniform(0.05, 0.15)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "application", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"app_test_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.application_test_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate application_test data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_bureau_balance_daily(self, simulation_date=None):
        """Simulate daily bureau_balance data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating bureau_balance data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/bureau_balance.csv")
            
            # Sample data for daily simulation (0.1-0.5% of original - bureau_balance is very large)
            sample_fraction = random.uniform(0.001, 0.005)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "bureau", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"bureau_bal_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.bureau_balance_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate bureau_balance data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_installments_payments_daily(self, simulation_date=None):
        """Simulate daily installments_payments data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating installments_payments data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/installments_payments.csv")
            
            # Sample data for daily simulation (0.2-1% of original - installments is large)
            sample_fraction = random.uniform(0.002, 0.01)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "installments", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"installments_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.installments_payments_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate installments_payments data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_pos_cash_balance_daily(self, simulation_date=None):
        """Simulate daily POS_CASH_balance data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating POS_CASH_balance data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/POS_CASH_balance.csv")
            
            # Sample data for daily simulation (0.2-1% of original - POS_CASH is large)
            sample_fraction = random.uniform(0.002, 0.01)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "pos_cash", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"pos_cash_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.POS_CASH_balance_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate POS_CASH_balance data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def simulate_previous_application_daily(self, simulation_date=None):
        """Simulate daily previous_application data"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"Simulating previous_application data for {simulation_date}")
        
        try:
            # Read original data
            original_df = self.spark.read.option("header", "true").option("inferSchema", "true") \
                .csv("hdfs://namenode:9000/data/home-credit-default-risk-dataset/previous_application.csv")
            
            # Sample data for daily simulation (1-5% of original)
            sample_fraction = random.uniform(0.01, 0.05)
            daily_sample = original_df.sample(fraction=sample_fraction, seed=abs(hash(str(simulation_date))) % 2147483647)
            
            # Add realistic daily variations
            daily_sample = self.add_daily_variations(daily_sample, "application", simulation_date)
            
            # Add metadata
            daily_sample_with_meta = daily_sample \
                .withColumn("ingestion_date", lit(simulation_date)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("data_source", lit("daily_simulation")) \
                .withColumn("batch_id", lit(f"prev_app_batch_{simulation_date}")) \
                .withColumn("simulation_id", monotonically_increasing_id())
            
            # Write to Iceberg table
            table_name = "iceberg.dq_warehouse.previous_application_daily"
            record_count = daily_sample_with_meta.count()
            
            if self.table_exists(table_name):
                daily_sample_with_meta.writeTo(table_name).append()
                logger.info(f"✅ Appended {record_count} records to {table_name}")
            else:
                daily_sample_with_meta.writeTo(table_name).create()
                logger.info(f"✅ Created {table_name} with {record_count} records")
            
            return {
                "status": "success",
                "table": table_name,
                "record_count": record_count,
                "simulation_date": simulation_date,
                "sample_fraction": sample_fraction
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to simulate previous_application data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def add_daily_variations(self, df, data_type, simulation_date):
        """Add realistic daily variations to the data"""
        try:
            # Add some noise to numeric columns to simulate daily changes
            if data_type == "application":
                # Slightly vary income amounts (±5%)
                if "AMT_INCOME_TOTAL" in df.columns:
                    df = df.withColumn("AMT_INCOME_TOTAL", 
                        col("AMT_INCOME_TOTAL") * (1 + (rand() - 0.5) * 0.1))
                
                # Vary credit amounts (±3%)
                if "AMT_CREDIT" in df.columns:
                    df = df.withColumn("AMT_CREDIT", 
                        col("AMT_CREDIT") * (1 + (rand() - 0.5) * 0.06))
            
            elif data_type == "bureau":
                # Vary credit amounts in bureau data
                if "AMT_CREDIT_SUM" in df.columns:
                    df = df.withColumn("AMT_CREDIT_SUM", 
                        col("AMT_CREDIT_SUM") * (1 + (rand() - 0.5) * 0.08))
            
            elif data_type == "credit_card":
                # Vary balance amounts
                if "AMT_BALANCE" in df.columns:
                    df = df.withColumn("AMT_BALANCE", 
                        col("AMT_BALANCE") * (1 + (rand() - 0.5) * 0.15))
            
            elif data_type == "installments":
                # Vary payment amounts
                if "AMT_PAYMENT" in df.columns:
                    df = df.withColumn("AMT_PAYMENT", 
                        col("AMT_PAYMENT") * (1 + (rand() - 0.5) * 0.12))
                if "AMT_INSTALMENT" in df.columns:
                    df = df.withColumn("AMT_INSTALMENT", 
                        col("AMT_INSTALMENT") * (1 + (rand() - 0.5) * 0.08))
            
            elif data_type == "pos_cash":
                # Vary balance amounts in POS CASH
                if "CNT_INSTALMENT" in df.columns:
                    df = df.withColumn("CNT_INSTALMENT", 
                        col("CNT_INSTALMENT") * (1 + (rand() - 0.5) * 0.05))
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to add variations: {str(e)}")
            return df

    def table_exists(self, table_name):
        """Check if Iceberg table exists"""
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except:
            return False

    def simulate_historical_data(self, days_back=7):
        """Simulate historical data for multiple days"""
        logger.info(f"Simulating historical data for {days_back} days")
        
        results = []
        base_date = datetime.now()
        
        for i in range(days_back, 0, -1):
            sim_date = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
              # Simulate all 8 tables for this date
            app_train_result = self.simulate_application_train_daily(sim_date)
            app_test_result = self.simulate_application_test_daily(sim_date)
            bureau_result = self.simulate_bureau_daily(sim_date)
            bureau_balance_result = self.simulate_bureau_balance_daily(sim_date)
            cc_result = self.simulate_credit_card_balance_daily(sim_date)
            installments_result = self.simulate_installments_payments_daily(sim_date)
            pos_cash_result = self.simulate_pos_cash_balance_daily(sim_date)
            prev_app_result = self.simulate_previous_application_daily(sim_date)
            
            day_results = {
                "simulation_date": sim_date,
                "application_train": app_train_result,
                "application_test": app_test_result,
                "bureau": bureau_result,
                "bureau_balance": bureau_balance_result,
                "credit_card_balance": cc_result,
                "installments_payments": installments_result,
                "POS_CASH_balance": pos_cash_result,
                "previous_application": prev_app_result
            }
            
            results.append(day_results)
            
            logger.info(f"✅ Completed simulation for {sim_date}")
        
        return results

    def run_daily_simulation(self, simulation_date=None):
        """Run complete daily simulation for all tables"""
        if simulation_date is None:
            simulation_date = self.simulation_date
            
        logger.info(f"🚀 Running daily simulation for {simulation_date}")
        
        results = {
            "simulation_date": simulation_date,
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }
          # Simulate all 8 Home Credit tables
        tables = [
            "application_train", "application_test", "bureau", "bureau_balance",
            "credit_card_balance", "installments_payments", "POS_CASH_balance", 
            "previous_application"
        ]
        
        for table in tables:
            try:
                if table == "application_train":
                    result = self.simulate_application_train_daily(simulation_date)
                elif table == "bureau":
                    result = self.simulate_bureau_daily(simulation_date)
                elif table == "credit_card_balance":
                    result = self.simulate_credit_card_balance_daily(simulation_date)
                elif table == "application_test":
                    result = self.simulate_application_test_daily(simulation_date)
                elif table == "bureau_balance":
                    result = self.simulate_bureau_balance_daily(simulation_date)
                elif table == "installments_payments":
                    result = self.simulate_installments_payments_daily(simulation_date)
                elif table == "POS_CASH_balance":
                    result = self.simulate_pos_cash_balance_daily(simulation_date)
                elif table == "previous_application":
                    result = self.simulate_previous_application_daily(simulation_date)
                
                results["tables"][table] = result
                
            except Exception as e:
                logger.error(f"❌ Failed to simulate {table}: {str(e)}")
                results["tables"][table] = {"status": "error", "message": str(e)}
        
        # Save simulation results
        results_dir = Path("dq-results")
        results_dir.mkdir(exist_ok=True)
        
        results_file = results_dir / f"daily_simulation_{simulation_date.replace('-', '')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"📊 Simulation results saved to {results_file}")
        
        return results

    def generate_simulation_report(self, results):
        """Generate a detailed simulation report"""
        print("\n" + "="*60)
        print(f"📊 DAILY DATA SIMULATION REPORT")
        print("="*60)
        print(f"Date: {results['simulation_date']}")
        print(f"Timestamp: {results['timestamp']}")
        print("-"*60)
        
        total_records = 0
        successful_tables = 0
        
        for table_name, table_result in results['tables'].items():
            status = table_result.get('status', 'unknown')
            record_count = table_result.get('record_count', 0)
            
            if status == 'success':
                print(f"✅ {table_name}: {record_count:,} records")
                total_records += record_count
                successful_tables += 1
            else:
                print(f"❌ {table_name}: {table_result.get('message', 'Unknown error')}")
        
        print("-"*60)
        print(f"📈 Summary:")
        print(f"   Total Records Simulated: {total_records:,}")
        print(f"   Successful Tables: {successful_tables}/{len(results['tables'])}")
        print(f"   Success Rate: {(successful_tables/len(results['tables'])*100):.1f}%")
        print("="*60)

def main():
    """Main entry point"""
    print("🚀 Daily Data Simulator for Home Credit Dataset")
    print("Implementing Item 2: Daily data simulation script")
    print("="*60)
    
    # Initialize simulator
    simulator = DailyDataSimulator()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--historical":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            print(f"📅 Running historical simulation for {days} days...")
            results = simulator.simulate_historical_data(days)
            print(f"✅ Historical simulation completed for {len(results)} days")
        elif sys.argv[1] == "--date":
            sim_date = sys.argv[2] if len(sys.argv) > 2 else None
            results = simulator.run_daily_simulation(sim_date)
            simulator.generate_simulation_report(results)
        else:
            print(f"❌ Unknown argument: {sys.argv[1]}")
            print("Usage: python daily_data_simulator.py [--historical [days]] [--date YYYY-MM-DD]")
    else:
        # Run today's simulation
        results = simulator.run_daily_simulation()
        simulator.generate_simulation_report(results)
    
    print("\n🎉 Daily data simulation completed!")
    print("📁 Check dq-results/ directory for simulation reports")
    print("🗄️  Data ingested into Iceberg tables on HDFS warehouse")

if __name__ == "__main__":
    main()
