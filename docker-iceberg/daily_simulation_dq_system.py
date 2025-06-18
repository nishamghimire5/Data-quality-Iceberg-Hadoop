#!/usr/bin/env python3
"""
Complete Daily DQ System - With True Daily Simulation
====================================================
This system performs:
1. Fresh daily simulation from HDFS CSV files
2. Creates new Iceberg tables with current timestamp
3. Analyzes the fresh simulated data
4. Shows different results each run

This demonstrates REAL daily data changes!
"""

import os
import json
import logging
from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg, count as spark_count
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DailySimulationDQSystem:
    """Complete DQ System with True Daily Simulation"""
    
    def __init__(self):
        """Initialize the system"""
        self.spark = self.init_spark()
        self.results_dir = "dq-results"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Generate unique simulation ID for this run
        self.simulation_date = datetime.now().strftime("%Y-%m-%d")
        self.simulation_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.random_seed = random.randint(1, 1000)  # Different seed each run!
        
        # Original CSV files in HDFS
        self.hdfs_csv_path = "hdfs://namenode:9000/data/home-credit-default-risk-dataset"
        self.warehouse_path = "hdfs://namenode:9000/warehouse"
        
        # CSV file mapping
        self.csv_files = {
            "application_train": f"{self.hdfs_csv_path}/application_train.csv",
            "application_test": f"{self.hdfs_csv_path}/application_test.csv", 
            "bureau": f"{self.hdfs_csv_path}/bureau.csv",
            "bureau_balance": f"{self.hdfs_csv_path}/bureau_balance.csv",
            "credit_card_balance": f"{self.hdfs_csv_path}/credit_card_balance.csv",
            "installments_payments": f"{self.hdfs_csv_path}/installments_payments.csv",
            "POS_CASH_balance": f"{self.hdfs_csv_path}/POS_CASH_balance.csv",
            "previous_application": f"{self.hdfs_csv_path}/previous_application.csv"
        }
        
        logger.info(f"🚀 Daily Simulation DQ System initialized")
        logger.info(f"📅 Simulation Date: {self.simulation_date}")
        logger.info(f"🔢 Simulation ID: {self.simulation_id}")
        logger.info(f"🎲 Random Seed: {self.random_seed}")

    def init_spark(self):
        """Initialize Spark session"""
        spark = SparkSession.builder \
            .appName("DailySimulationDQSystem") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session initialized")
        return spark

    def simulate_daily_data(self):
        """
        Step 1: Simulate fresh daily data from HDFS CSV files
        This creates NEW data each time with different sampling!
        """
        logger.info("📅 Starting FRESH daily data simulation...")
        
        # Use different sample fractions for variety
        base_fraction = 0.01  # Base 1%
        fraction_variance = random.uniform(0.005, 0.015)  # 0.5% to 1.5% variance
        sample_fraction = base_fraction + fraction_variance
        
        logger.info(f"🎲 Using sample fraction: {sample_fraction:.3f} (seed: {self.random_seed})")
        
        simulated_tables = {}
        
        for table_name, csv_path in self.csv_files.items():
            try:
                logger.info(f"📁 Processing {table_name} from HDFS CSV...")
                
                # Read original CSV from HDFS
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_path)
                
                original_count = df.count()
                logger.info(f"   📊 Original rows: {original_count:,}")
                
                # Apply FRESH sampling with different seed each time
                sampled_df = df.sample(fraction=sample_fraction, seed=self.random_seed)
                sampled_count = sampled_df.count()
                logger.info(f"   📊 Sampled rows: {sampled_count:,} ({sample_fraction*100:.1f}%)")
                
                # Add unique daily simulation metadata
                daily_df = sampled_df \
                    .withColumn("ingestion_date", lit(self.simulation_date)) \
                    .withColumn("ingestion_timestamp", current_timestamp()) \
                    .withColumn("simulation_id", lit(self.simulation_id)) \
                    .withColumn("data_source", lit("hdfs_csv_fresh")) \
                    .withColumn("batch_id", monotonically_increasing_id()) \
                    .withColumn("random_seed", lit(self.random_seed))
                
                # Store with unique table name for this simulation
                daily_table_name = f"{table_name}_daily_{self.simulation_id}"
                simulated_tables[table_name] = daily_table_name
                
                # Save as temporary table for analysis
                daily_df.createOrReplaceTempView(daily_table_name)
                
                logger.info(f"   ✅ Created fresh table: {daily_table_name}")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to simulate {table_name}: {str(e)}")
        
        logger.info("✅ Fresh daily simulation completed")
        return simulated_tables

    def analyze_fresh_data(self, simulated_tables):
        """
        Step 2: Analyze the FRESH simulated data
        """
        logger.info("🔍 Starting analysis of FRESH simulated data...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        all_results = []
        
        for original_name, temp_table_name in simulated_tables.items():
            logger.info(f"📊 Analyzing fresh data: {original_name}")
            
            try:
                # Read from temporary table (fresh data)
                df = self.spark.table(temp_table_name)
                
                # Basic metrics
                total_rows = df.count()
                total_columns = len(df.columns)
                metadata_cols = ['ingestion_date', 'ingestion_timestamp', 'simulation_id', 
                               'data_source', 'batch_id', 'random_seed']
                business_columns = [col for col in df.columns if col not in metadata_cols]
                
                table_result = {
                    'table_name': f"{original_name}_daily",
                    'simulation_id': self.simulation_id,
                    'random_seed': self.random_seed,
                    'total_rows': total_rows,
                    'total_columns': total_columns,
                    'business_columns': len(business_columns),
                    'column_analysis': [],
                    'analysis_timestamp': datetime.now().isoformat()
                }
                
                # Analyze each business column
                for col_name in business_columns[:10]:  # Limit to first 10 for speed
                    col_analysis = self.analyze_column_simple(df, col_name, total_rows)
                    table_result['column_analysis'].append(col_analysis)
                
                all_results.append(table_result)
                logger.info(f"   ✅ Analyzed: {total_rows:,} rows, {len(business_columns)} columns")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to analyze {original_name}: {str(e)}")
                all_results.append({
                    'table_name': f"{original_name}_daily",
                    'error': str(e),
                    'simulation_id': self.simulation_id,
                    'analysis_timestamp': datetime.now().isoformat()
                })
        
        # Generate reports
        self.generate_fresh_reports(all_results, timestamp)
        logger.info("✅ Fresh data analysis completed")
        
        return all_results

    def analyze_column_simple(self, df, col_name, total_rows):
        """Simple column analysis"""
        try:
            # Null analysis
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            col_type = str(df.schema[col_name].dataType)
            
            analysis = {
                'column_name': col_name,
                'data_type': col_type,
                'total_rows': total_rows,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'non_null_count': total_rows - null_count
            }
            
            # Simple quality score
            quality_score = 100 - min(null_percentage, 50)
            analysis['data_quality_score'] = round(quality_score, 1)
            
            return analysis
            
        except Exception as e:
            return {
                'column_name': col_name,
                'error': str(e),
                'data_quality_score': 0
            }

    def generate_fresh_reports(self, results, timestamp):
        """Generate reports for fresh simulated data"""
        
        # JSON report with simulation details
        json_file = f"{self.results_dir}/fresh_daily_analysis_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'simulation_id': self.simulation_id,
                'random_seed': self.random_seed,
                'simulation_date': self.simulation_date,
                'summary': {
                    'total_tables': len(results),
                    'total_rows': sum(r.get('total_rows', 0) for r in results),
                    'total_columns': sum(r.get('business_columns', 0) for r in results)
                },
                'table_results': results
            }, f, indent=2, default=str)
        
        # Summary with simulation info
        summary_file = f"{self.results_dir}/fresh_daily_summary_{timestamp}.txt"
        with open(summary_file, 'w') as f:
            f.write("FRESH DAILY DATA QUALITY ANALYSIS\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Simulation ID: {self.simulation_id}\n")
            f.write(f"Random Seed: {self.random_seed}\n")
            f.write(f"Simulation Date: {self.simulation_date}\n\n")
            f.write(f"Total Tables: {len(results)}\n")
            f.write(f"Total Rows: {sum(r.get('total_rows', 0) for r in results):,}\n\n")
            
            for result in results:
                if 'error' in result:
                    f.write(f"❌ {result['table_name']}: ERROR\n")
                else:
                    f.write(f"✅ {result['table_name']}: {result['total_rows']:,} rows (seed: {result['random_seed']})\n")
        
        logger.info(f"📊 Fresh simulation reports generated:")
        logger.info(f"   📋 JSON: {json_file}")
        logger.info(f"   📝 Summary: {summary_file}")

def main():
    """Main execution - TRUE daily simulation"""
    print("📅 FRESH DAILY DATA QUALITY SIMULATION")
    print("=" * 60)
    print("This run will generate DIFFERENT results each time!")
    print("✅ Fresh CSV sampling from HDFS")
    print("✅ Different random seed each run")
    print("✅ Unique simulation ID")
    print("✅ Fresh data analysis")
    print("=" * 60)
    
    # Initialize system
    dq_system = DailySimulationDQSystem()
    
    # Step 1: Simulate fresh daily data
    simulated_tables = dq_system.simulate_daily_data()
    
    # Step 2: Analyze fresh data
    results = dq_system.analyze_fresh_data(simulated_tables)
    
    print("\n🎉 FRESH DAILY SIMULATION COMPLETED!")
    print("📁 Check dq-results/ for reports with simulation details")
    print("🔄 Run again to see DIFFERENT results!")

if __name__ == "__main__":
    main()
