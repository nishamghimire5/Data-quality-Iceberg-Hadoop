#!/usr/bin/env python3
"""
Simple Data Quality System for All 8 Home Credit Tables
Fixed version that works with existing Iceberg tables
"""

import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, max, current_timestamp
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleDQSystem:
    """Simple Data Quality System for All 8 Home Credit Tables"""
    
    def __init__(self):
        """Initialize the DQ System"""
        self.spark = self.init_spark()
        self.results_dir = Path("dq-results")
        self.results_dir.mkdir(exist_ok=True)
        
        # The 8 main Home Credit tables (excluding metadata tables)
        self.tables = [
            "application_train_daily",
            "application_test_daily", 
            "bureau_daily",
            "bureau_balance_daily",
            "credit_card_balance_daily",
            "installments_payments_daily",
            "POS_CASH_balance_daily",
            "previous_application_daily"
        ]
        
    def init_spark(self):
        """Initialize Spark session with Iceberg support"""
        spark = SparkSession.builder \
            .appName("SimpleDQSystem") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "/home/iceberg/warehouse") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
        return spark
    
    def check_table_exists(self, table_name):
        """Check if table exists in the warehouse"""
        try:
            warehouse_path = f"/home/iceberg/warehouse/{table_name}"
            df = self.spark.read.parquet(warehouse_path)
            return True, df.count()
        except Exception as e:
            logger.warning(f"Table {table_name} not found: {str(e)}")
            return False, 0
    
    def run_volume_checks(self, table_name, df):
        """Run volume (record count) checks"""
        try:
            record_count = df.count()
            
            # Define thresholds based on table type
            volume_thresholds = {
                "application_train_daily": {"min": 1000, "max": 50000},
                "application_test_daily": {"min": 500, "max": 25000},
                "bureau_daily": {"min": 2000, "max": 100000},
                "bureau_balance_daily": {"min": 10000, "max": 500000},
                "credit_card_balance_daily": {"min": 2000, "max": 100000},
                "installments_payments_daily": {"min": 10000, "max": 200000},
                "POS_CASH_balance_daily": {"min": 5000, "max": 150000},
                "previous_application_daily": {"min": 5000, "max": 100000}
            }
            
            threshold = volume_thresholds.get(table_name, {"min": 100, "max": 1000000})
            
            if record_count < threshold["min"]:
                status = "FAIL"
                message = f"Record count {record_count:,} below minimum {threshold['min']:,}"
            elif record_count > threshold["max"]:
                status = "FAIL" 
                message = f"Record count {record_count:,} above maximum {threshold['max']:,}"
            else:
                status = "PASS"
                message = f"Record count {record_count:,} within acceptable range"
                
            return {
                "check_type": "volume",
                "table": table_name,
                "record_count": record_count,
                "min_threshold": threshold["min"],
                "max_threshold": threshold["max"],
                "status": status,
                "message": message
            }
            
        except Exception as e:
            logger.error(f"Volume check failed for {table_name}: {str(e)}")
            return {
                "check_type": "volume",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }
    
    def run_null_checks(self, table_name, df):
        """Run NULL value checks on critical columns"""
        try:
            results = []
            
            # Get all columns
            columns = df.columns
            
            # Define critical columns per table
            critical_columns = {
                "application_train_daily": ["SK_ID_CURR", "TARGET"],
                "application_test_daily": ["SK_ID_CURR"], 
                "bureau_daily": ["SK_ID_CURR", "SK_ID_BUREAU"],
                "bureau_balance_daily": ["SK_ID_BUREAU"],
                "credit_card_balance_daily": ["SK_ID_CURR", "SK_ID_PREV"],
                "installments_payments_daily": ["SK_ID_CURR", "SK_ID_PREV"],
                "POS_CASH_balance_daily": ["SK_ID_CURR", "SK_ID_PREV"],
                "previous_application_daily": ["SK_ID_CURR", "SK_ID_PREV"]
            }
            
            critical_cols = critical_columns.get(table_name, [])
            
            # Add some numeric columns to check if they exist
            numeric_cols = ["AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE"]
            check_cols = list(set(critical_cols + [col for col in numeric_cols if col in columns]))
            
            total_rows = df.count()
            
            for column in check_cols:
                if column in columns:
                    null_count = df.filter(col(column).isNull()).count()
                    null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                    
                    # 5% threshold for critical columns, 20% for others
                    threshold = 5.0 if column in critical_cols else 20.0
                    
                    status = "PASS" if null_percentage <= threshold else "FAIL"
                    
                    results.append({
                        "table": table_name,
                        "column": column,
                        "null_count": null_count,
                        "total_rows": total_rows,
                        "null_percentage": round(null_percentage, 2),
                        "threshold": threshold,
                        "status": status,
                        "is_critical": column in critical_cols
                    })
            
            return {
                "check_type": "null_checks",
                "table": table_name,
                "column_results": results,
                "overall_status": "PASS" if all(r["status"] == "PASS" for r in results) else "FAIL"
            }
            
        except Exception as e:
            logger.error(f"NULL check failed for {table_name}: {str(e)}")
            return {
                "check_type": "null_checks",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }
    
    def run_range_checks(self, table_name, df):
        """Run range validation checks on numeric columns"""
        try:
            results = []
            
            # Define numeric ranges for common columns
            numeric_ranges = {
                "AMT_INCOME_TOTAL": {"min": 0, "max": 10000000},
                "AMT_CREDIT": {"min": 0, "max": 5000000},
                "AMT_ANNUITY": {"min": 0, "max": 1000000},
                "AMT_GOODS_PRICE": {"min": 0, "max": 5000000},
                "DAYS_BIRTH": {"min": -25000, "max": -6000},
                "DAYS_EMPLOYED": {"min": -20000, "max": 1},
                "CNT_CHILDREN": {"min": 0, "max": 20}
            }
            
            columns = df.columns
            total_rows = df.count()
            
            for column, range_def in numeric_ranges.items():
                if column in columns:
                    # Count violations
                    violations = df.filter(
                        (col(column) < range_def["min"]) | 
                        (col(column) > range_def["max"])
                    ).count()
                    
                    violation_percentage = (violations / total_rows) * 100 if total_rows > 0 else 0
                    status = "PASS" if violation_percentage <= 5.0 else "FAIL"  # 5% threshold
                    
                    results.append({
                        "table": table_name,
                        "column": column,
                        "violations": violations,
                        "total_rows": total_rows,
                        "violation_percentage": round(violation_percentage, 2),
                        "min_allowed": range_def["min"],
                        "max_allowed": range_def["max"],
                        "status": status
                    })
            
            return {
                "check_type": "range_checks",
                "table": table_name,
                "column_results": results,
                "overall_status": "PASS" if all(r["status"] == "PASS" for r in results) else "FAIL"
            }
            
        except Exception as e:
            logger.error(f"Range check failed for {table_name}: {str(e)}")
            return {
                "check_type": "range_checks",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }
    
    def run_freshness_checks(self, table_name, df):
        """Run data freshness checks"""
        try:
            # Check if ingestion_timestamp column exists
            if "ingestion_timestamp" in df.columns:
                latest_timestamp = df.agg(max("ingestion_timestamp")).collect()[0][0]
                
                if latest_timestamp:
                    from datetime import datetime
                    current_time = datetime.now()
                    
                    # Convert to datetime if it's a string
                    if isinstance(latest_timestamp, str):
                        latest_timestamp = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
                    
                    hours_old = (current_time - latest_timestamp.replace(tzinfo=None)).total_seconds() / 3600
                    
                    # 24 hour threshold
                    threshold_hours = 24
                    status = "PASS" if hours_old <= threshold_hours else "FAIL"
                    
                    return {
                        "check_type": "freshness",
                        "table": table_name,
                        "latest_timestamp": latest_timestamp.isoformat(),
                        "hours_old": round(hours_old, 2),
                        "threshold_hours": threshold_hours,
                        "status": status,
                        "message": f"Data is {hours_old:.1f} hours old"
                    }
            
            return {
                "check_type": "freshness", 
                "table": table_name,
                "status": "SKIP",
                "message": "No timestamp column found"
            }
            
        except Exception as e:
            logger.error(f"Freshness check failed for {table_name}: {str(e)}")
            return {
                "check_type": "freshness",
                "table": table_name,
                "status": "ERROR", 
                "message": str(e)
            }
    
    def run_all_checks(self):
        """Run all DQ checks on all 8 tables"""
        logger.info("🚀 Starting Data Quality checks for all 8 Home Credit tables")
        
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "tables_processed": {},
            "summary": {
                "total_tables": len(self.tables),
                "successful_checks": 0,
                "failed_checks": 0,
                "error_checks": 0
            }
        }
        
        for table_name in self.tables:
            logger.info(f"🔍 Checking table: {table_name}")
            
            # Check if table exists
            exists, row_count = self.check_table_exists(table_name)
            
            if not exists:
                logger.warning(f"⚠️  Table {table_name} not found, skipping...")
                all_results["tables_processed"][table_name] = {
                    "status": "SKIPPED",
                    "message": "Table not found"
                }
                continue
            
            try:
                # Load table data
                warehouse_path = f"/home/iceberg/warehouse/{table_name}"
                df = self.spark.read.parquet(warehouse_path)
                
                # Run all checks
                volume_result = self.run_volume_checks(table_name, df)
                null_result = self.run_null_checks(table_name, df)
                range_result = self.run_range_checks(table_name, df)
                freshness_result = self.run_freshness_checks(table_name, df)
                
                # Combine results
                table_results = {
                    "record_count": row_count,
                    "checks": {
                        "volume": volume_result,
                        "null_checks": null_result,
                        "range_checks": range_result,
                        "freshness": freshness_result
                    },
                    "overall_status": "PASS"  # Will be updated based on individual checks
                }
                
                # Determine overall status
                check_statuses = []
                for check_type, check_result in table_results["checks"].items():
                    if isinstance(check_result, dict):
                        if "overall_status" in check_result:
                            check_statuses.append(check_result["overall_status"])
                        elif "status" in check_result:
                            check_statuses.append(check_result["status"])
                
                if "FAIL" in check_statuses:
                    table_results["overall_status"] = "FAIL"
                    all_results["summary"]["failed_checks"] += 1
                elif "ERROR" in check_statuses:
                    table_results["overall_status"] = "ERROR"
                    all_results["summary"]["error_checks"] += 1
                else:
                    all_results["summary"]["successful_checks"] += 1
                
                all_results["tables_processed"][table_name] = table_results
                
                logger.info(f"✅ Completed checks for {table_name}: {table_results['overall_status']}")
                
            except Exception as e:
                logger.error(f"❌ Failed to check {table_name}: {str(e)}")
                all_results["tables_processed"][table_name] = {
                    "status": "ERROR",
                    "message": str(e)
                }
                all_results["summary"]["error_checks"] += 1
        
        # Save results
        self.save_results(all_results)
        self.generate_reports(all_results)
        
        return all_results
    
    def save_results(self, results):
        """Save DQ results to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON results
        json_file = self.results_dir / f"dq_results_all_tables_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"📁 Results saved to {json_file}")
    
    def generate_reports(self, results):
        """Generate detailed DQ reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate summary report
        summary_file = self.results_dir / f"dq_summary_all_tables_{timestamp}.txt"
        
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DATA QUALITY REPORT - ALL 8 HOME CREDIT TABLES\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {results['timestamp']}\n")
            f.write(f"Tables Processed: {results['summary']['total_tables']}\n")
            f.write(f"Successful: {results['summary']['successful_checks']}\n")
            f.write(f"Failed: {results['summary']['failed_checks']}\n")
            f.write(f"Errors: {results['summary']['error_checks']}\n")
            f.write("=" * 80 + "\n\n")
            
            # Table-by-table results
            for table_name, table_results in results["tables_processed"].items():
                f.write(f"📊 TABLE: {table_name}\n")
                f.write("-" * 60 + "\n")
                
                if "record_count" in table_results:
                    f.write(f"   Records: {table_results['record_count']:,}\n")
                    f.write(f"   Overall Status: {table_results['overall_status']}\n")
                    
                    # Volume check
                    if "volume" in table_results["checks"]:
                        vol = table_results["checks"]["volume"]
                        f.write(f"   📈 Volume: {vol['status']} - {vol['message']}\n")
                    
                    # NULL checks
                    if "null_checks" in table_results["checks"]:
                        null_check = table_results["checks"]["null_checks"]
                        if "column_results" in null_check:
                            f.write(f"   🔍 NULL Checks: {null_check['overall_status']}\n")
                            for col_result in null_check["column_results"]:
                                f.write(f"      - {col_result['column']}: {col_result['null_percentage']}% null ({col_result['status']})\n")
                    
                    # Range checks  
                    if "range_checks" in table_results["checks"]:
                        range_check = table_results["checks"]["range_checks"]
                        if "column_results" in range_check:
                            f.write(f"   📏 Range Checks: {range_check['overall_status']}\n")
                            for col_result in range_check["column_results"]:
                                f.write(f"      - {col_result['column']}: {col_result['violation_percentage']}% violations ({col_result['status']})\n")
                    
                    # Freshness check
                    if "freshness" in table_results["checks"]:
                        fresh = table_results["checks"]["freshness"]
                        f.write(f"   ⏰ Freshness: {fresh['status']} - {fresh['message']}\n")
                
                else:
                    f.write(f"   Status: {table_results.get('status', 'UNKNOWN')}\n")
                    f.write(f"   Message: {table_results.get('message', 'No details')}\n")
                
                f.write("\n")
        
        logger.info(f"📋 Summary report saved to {summary_file}")
        
        # Generate CSV reports for each check type
        self.generate_csv_reports(results, timestamp)
    
    def generate_csv_reports(self, results, timestamp):
        """Generate CSV reports for each check type"""
        
        # Volume checks CSV
        volume_data = []
        null_data = []
        range_data = []
        freshness_data = []
        
        for table_name, table_results in results["tables_processed"].items():
            if "checks" in table_results:
                checks = table_results["checks"]
                
                # Volume data
                if "volume" in checks and "record_count" in checks["volume"]:
                    vol = checks["volume"]
                    volume_data.append({
                        "table_name": table_name,
                        "record_count": vol.get("record_count", 0),
                        "min_threshold": vol.get("min_threshold", 0),
                        "max_threshold": vol.get("max_threshold", 0),
                        "status": vol.get("status", "UNKNOWN"),
                        "message": vol.get("message", "")
                    })
                
                # NULL data
                if "null_checks" in checks and "column_results" in checks["null_checks"]:
                    for col_result in checks["null_checks"]["column_results"]:
                        null_data.append({
                            "table_name": table_name,
                            "column_name": col_result["column"],
                            "null_count": col_result["null_count"],
                            "total_rows": col_result["total_rows"],
                            "null_percentage": col_result["null_percentage"],
                            "threshold": col_result["threshold"],
                            "status": col_result["status"],
                            "is_critical": col_result["is_critical"]
                        })
                
                # Range data
                if "range_checks" in checks and "column_results" in checks["range_checks"]:
                    for col_result in checks["range_checks"]["column_results"]:
                        range_data.append({
                            "table_name": table_name,
                            "column_name": col_result["column"],
                            "violations": col_result["violations"],
                            "total_rows": col_result["total_rows"],
                            "violation_percentage": col_result["violation_percentage"],
                            "min_allowed": col_result["min_allowed"],
                            "max_allowed": col_result["max_allowed"],
                            "status": col_result["status"]
                        })
                
                # Freshness data
                if "freshness" in checks:
                    fresh = checks["freshness"]
                    freshness_data.append({
                        "table_name": table_name,
                        "latest_timestamp": fresh.get("latest_timestamp", ""),
                        "hours_old": fresh.get("hours_old", 0),
                        "threshold_hours": fresh.get("threshold_hours", 24),
                        "status": fresh.get("status", "UNKNOWN"),
                        "message": fresh.get("message", "")
                    })
        
        # Save CSV files
        import pandas as pd
        
        if volume_data:
            pd.DataFrame(volume_data).to_csv(
                self.results_dir / f"dq_volume_checks_{timestamp}.csv", index=False
            )
        
        if null_data:
            pd.DataFrame(null_data).to_csv(
                self.results_dir / f"dq_null_checks_{timestamp}.csv", index=False
            )
        
        if range_data:
            pd.DataFrame(range_data).to_csv(
                self.results_dir / f"dq_range_checks_{timestamp}.csv", index=False
            )
        
        if freshness_data:
            pd.DataFrame(freshness_data).to_csv(
                self.results_dir / f"dq_freshness_checks_{timestamp}.csv", index=False
            )
        
        logger.info(f"📊 CSV reports generated with timestamp {timestamp}")

def main():
    """Main entry point"""
    print("🚀 Simple Data Quality System for All 8 Home Credit Tables")
    print("=" * 70)
    
    # Initialize DQ system
    dq_system = SimpleDQSystem()
    
    # Run all checks
    results = dq_system.run_all_checks()
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 DATA QUALITY SUMMARY")
    print("=" * 70)
    print(f"📅 Timestamp: {results['timestamp']}")
    print(f"📋 Tables Processed: {results['summary']['total_tables']}")
    print(f"✅ Successful: {results['summary']['successful_checks']}")
    print(f"❌ Failed: {results['summary']['failed_checks']}")
    print(f"🚨 Errors: {results['summary']['error_checks']}")
    print("=" * 70)
    
    # Table-by-table summary
    for table_name, table_results in results["tables_processed"].items():
        status_emoji = "✅" if table_results.get("overall_status") == "PASS" else "❌" if table_results.get("overall_status") == "FAIL" else "⚠️"
        if "record_count" in table_results:
            print(f"{status_emoji} {table_name}: {table_results['record_count']:,} records - {table_results['overall_status']}")
        else:
            print(f"{status_emoji} {table_name}: {table_results.get('status', 'UNKNOWN')}")
    
    print("\n📁 Check dq-results/ directory for detailed reports")
    print("🔗 Access DQOps at: http://localhost:8082")
    print("🔗 Access OpenRefine at: http://localhost:3333")

if __name__ == "__main__":
    main()
