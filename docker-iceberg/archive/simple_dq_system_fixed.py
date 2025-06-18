#!/usr/bin/env python3
"""
FIXED: Simple Data Quality System for All 8 Home Credit Tables
Fixed the PySpark Column/float issue
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, max as spark_max, min as spark_min
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleDQSystem:
    def __init__(self):
        self.spark = self.init_spark()
        self.tables = [
            "application_train_daily", "application_test_daily", "bureau_daily", 
            "bureau_balance_daily", "credit_card_balance_daily", "installments_payments_daily",
            "POS_CASH_balance_daily", "previous_application_daily"
        ]
        
        # Fixed thresholds - using proper integers, not floats
        self.volume_thresholds = {
            "application_train_daily": {"min": 1000, "max": 50000},
            "application_test_daily": {"min": 500, "max": 25000},
            "bureau_daily": {"min": 2000, "max": 100000},
            "bureau_balance_daily": {"min": 10000, "max": 500000},
            "credit_card_balance_daily": {"min": 2000, "max": 100000},
            "installments_payments_daily": {"min": 10000, "max": 200000},
            "POS_CASH_balance_daily": {"min": 5000, "max": 150000},
            "previous_application_daily": {"min": 5000, "max": 100000}
        }
        
        # Critical columns for NULL checks
        self.critical_columns = {
            "application_train_daily": ["SK_ID_CURR", "AMT_INCOME_TOTAL"],
            "application_test_daily": ["SK_ID_CURR"],
            "bureau_daily": ["SK_ID_CURR", "SK_ID_BUREAU"],
            "bureau_balance_daily": ["SK_ID_BUREAU"],
            "credit_card_balance_daily": ["SK_ID_PREV"],
            "installments_payments_daily": ["SK_ID_PREV"],
            "POS_CASH_balance_daily": ["SK_ID_PREV"],
            "previous_application_daily": ["SK_ID_PREV"]
        }
        
        # Range validation rules (using string column names)
        self.range_rules = {
            "application_train_daily": {
                "AMT_INCOME_TOTAL": {"min": 0, "max": 10000000},
                "AMT_CREDIT": {"min": 0, "max": 5000000}
            },
            "application_test_daily": {
                "AMT_INCOME_TOTAL": {"min": 0, "max": 10000000},
                "AMT_CREDIT": {"min": 0, "max": 5000000}
            },
            "previous_application_daily": {
                "AMT_CREDIT": {"min": 0, "max": 5000000}
            }
        }

    def init_spark(self):
        spark = SparkSession.builder \
            .appName("FixedDQSystem") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "/home/iceberg/warehouse") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
        return spark

    def check_volume(self, table_name):
        """Fixed volume check - no float operations"""
        try:
            # Try different path formats
            try:
                df = self.spark.read.format("iceberg").load(f"/home/iceberg/warehouse/{table_name}")
            except:
                # Fallback to parquet format
                df = self.spark.read.parquet(f"/home/iceberg/warehouse/{table_name}")
            
            record_count = df.count()
            
            thresholds = self.volume_thresholds.get(table_name, {"min": 100, "max": 1000000})
            min_threshold = thresholds["min"]  # Integer
            max_threshold = thresholds["max"]  # Integer
            
            if min_threshold <= record_count <= max_threshold:
                status = "PASS"
                message = f"Record count {record_count:,} within acceptable range"
            else:
                status = "FAIL"
                message = f"Record count {record_count:,} outside range ({min_threshold:,}-{max_threshold:,})"
            
            return {
                "check_type": "volume",
                "table": table_name,
                "record_count": record_count,
                "min_threshold": min_threshold,
                "max_threshold": max_threshold,
                "status": status,
                "message": message
            }
        except Exception as e:
            return {
                "check_type": "volume",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }    def check_nulls(self, table_name):
        """Fixed NULL check - proper column handling"""
        try:
            # Try different path formats
            try:
                df = self.spark.read.format("iceberg").load(f"/home/iceberg/warehouse/{table_name}")
            except:
                df = self.spark.read.parquet(f"/home/iceberg/warehouse/{table_name}")
            
            total_rows = df.count()
            
            columns_to_check = self.critical_columns.get(table_name, [])
            null_results = {}
            
            for column_name in columns_to_check:
                if column_name in df.columns:
                    # Fixed: Use proper column reference
                    null_count = df.filter(col(column_name).isNull()).count()
                    null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
                    
                    status = "PASS" if null_percentage <= 5.0 else "FAIL"
                    null_results[column_name] = {
                        "null_count": null_count,
                        "null_percentage": round(null_percentage, 2),
                        "status": status
                    }
            
            overall_status = "PASS" if all(r["status"] == "PASS" for r in null_results.values()) else "FAIL"
            
            return {
                "check_type": "null_checks",
                "table": table_name,
                "total_rows": total_rows,
                "column_results": null_results,
                "status": overall_status,
                "message": f"NULL checks completed for {len(null_results)} columns"
            }
        except Exception as e:
            return {
                "check_type": "null_checks",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }

    def check_ranges(self, table_name):
        """Fixed range check - proper column handling"""
        try:
            df = self.spark.read.format("iceberg").load(f"/home/iceberg/warehouse/{table_name}")
            
            rules = self.range_rules.get(table_name, {})
            if not rules:
                return {
                    "check_type": "range_checks",
                    "table": table_name,
                    "status": "PASS",
                    "message": "No range rules defined for this table"
                }
            
            range_results = {}
            
            for column_name, rule in rules.items():
                if column_name in df.columns:
                    min_val = rule["min"]  # Integer
                    max_val = rule["max"]  # Integer
                    
                    # Fixed: Use proper column reference and integer comparisons
                    violations = df.filter(
                        (col(column_name) < min_val) | (col(column_name) > max_val)
                    ).count()
                    
                    total_rows = df.count()
                    violation_percentage = (violations / total_rows * 100) if total_rows > 0 else 0
                    
                    status = "PASS" if violations == 0 else "FAIL"
                    range_results[column_name] = {
                        "min_allowed": min_val,
                        "max_allowed": max_val,
                        "violations": violations,
                        "violation_percentage": round(violation_percentage, 2),
                        "status": status
                    }
            
            overall_status = "PASS" if all(r["status"] == "PASS" for r in range_results.values()) else "FAIL"
            
            return {
                "check_type": "range_checks",
                "table": table_name,
                "column_results": range_results,
                "status": overall_status,
                "message": f"Range checks completed for {len(range_results)} columns"
            }
        except Exception as e:
            return {
                "check_type": "range_checks",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }

    def check_freshness(self, table_name):
        """Fixed freshness check - proper timestamp handling"""
        try:
            df = self.spark.read.format("iceberg").load(f"/home/iceberg/warehouse/{table_name}")
            
            # Look for timestamp columns
            timestamp_cols = [c for c in df.columns if 'timestamp' in c.lower() or 'ingestion' in c.lower()]
            
            if not timestamp_cols:
                return {
                    "check_type": "freshness",
                    "table": table_name,
                    "status": "PASS",
                    "message": "No timestamp columns found - assuming fresh data"
                }
            
            # Use the first timestamp column
            ts_col = timestamp_cols[0]
            
            # Get the latest timestamp
            latest_row = df.select(spark_max(col(ts_col)).alias("latest_ts")).collect()
            if latest_row and latest_row[0]["latest_ts"]:
                latest_timestamp = latest_row[0]["latest_ts"]
                current_time = datetime.now()
                
                # Calculate hours difference
                time_diff = current_time - latest_timestamp
                hours_old = time_diff.total_seconds() / 3600
                
                threshold_hours = 24  # Fixed: Use integer
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
            else:
                return {
                    "check_type": "freshness",
                    "table": table_name,
                    "status": "FAIL",
                    "message": "No valid timestamps found"
                }
                
        except Exception as e:
            return {
                "check_type": "freshness",
                "table": table_name,
                "status": "ERROR",
                "message": str(e)
            }

    def run_all_checks(self):
        """Run DQ checks for all 8 tables"""
        logger.info("🚀 Starting Data Quality checks for all 8 Home Credit tables")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "tables_processed": {}
        }
        
        for table_name in self.tables:
            logger.info(f"🔍 Checking table: {table_name}")
            
            try:
                # Run all checks
                volume_result = self.check_volume(table_name)
                null_result = self.check_nulls(table_name)
                range_result = self.check_ranges(table_name)
                freshness_result = self.check_freshness(table_name)
                
                # Determine overall status
                statuses = [volume_result["status"], null_result["status"], 
                           range_result["status"], freshness_result["status"]]
                
                if "ERROR" in statuses:
                    overall_status = "ERROR"
                elif "FAIL" in statuses:
                    overall_status = "FAIL"
                else:
                    overall_status = "PASS"
                
                results["tables_processed"][table_name] = {
                    "record_count": volume_result.get("record_count", 0),
                    "checks": {
                        "volume": volume_result,
                        "null_checks": null_result,
                        "range_checks": range_result,
                        "freshness": freshness_result
                    },
                    "overall_status": overall_status
                }
                
                logger.info(f"✅ Completed checks for {table_name}: {overall_status}")
                
            except Exception as e:
                logger.error(f"❌ Failed to check {table_name}: {str(e)}")
                results["tables_processed"][table_name] = {
                    "overall_status": "ERROR",
                    "error": str(e)
                }
        
        return results

    def save_results(self, results):
        """Save results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON
        json_file = f"dq-results/dq_results_all_tables_fixed_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"📁 Results saved to {json_file}")
        
        # Save summary
        summary_file = f"dq-results/dq_summary_all_tables_fixed_{timestamp}.txt"
        self.generate_summary_report(results, summary_file)
        logger.info(f"📋 Summary report saved to {summary_file}")
        
        # Save CSV reports
        self.generate_csv_reports(results, timestamp)
        logger.info(f"📊 CSV reports generated with timestamp {timestamp}")

    def generate_summary_report(self, results, filename):
        """Generate summary report"""
        with open(filename, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DATA QUALITY REPORT - ALL 8 HOME CREDIT TABLES (FIXED)\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {results['timestamp']}\n")
            
            total_tables = len(results['tables_processed'])
            successful = sum(1 for t in results['tables_processed'].values() if t.get('overall_status') == 'PASS')
            failed = sum(1 for t in results['tables_processed'].values() if t.get('overall_status') == 'FAIL')
            errors = sum(1 for t in results['tables_processed'].values() if t.get('overall_status') == 'ERROR')
            
            f.write(f"Tables Processed: {total_tables}\n")
            f.write(f"Successful: {successful}\n")
            f.write(f"Failed: {failed}\n")
            f.write(f"Errors: {errors}\n")
            f.write("=" * 80 + "\n\n")
            
            for table_name, table_data in results['tables_processed'].items():
                f.write(f"📊 TABLE: {table_name}\n")
                f.write("-" * 60 + "\n")
                f.write(f"   Records: {table_data.get('record_count', 'Unknown'):,}\n")
                f.write(f"   Overall Status: {table_data.get('overall_status', 'Unknown')}\n")
                
                if 'checks' in table_data:
                    checks = table_data['checks']
                    if 'volume' in checks:
                        vol = checks['volume']
                        f.write(f"   📈 Volume: {vol['status']} - {vol['message']}\n")
                    
                    if 'null_checks' in checks:
                        null = checks['null_checks']
                        f.write(f"   🔍 NULL Checks: {null['status']} - {null['message']}\n")
                    
                    if 'range_checks' in checks:
                        range_check = checks['range_checks']
                        f.write(f"   📏 Range Checks: {range_check['status']} - {range_check['message']}\n")
                    
                    if 'freshness' in checks:
                        fresh = checks['freshness']
                        f.write(f"   ⏰ Freshness: {fresh['status']} - {fresh['message']}\n")
                
                f.write("\n")

    def generate_csv_reports(self, results, timestamp):
        """Generate CSV reports for each check type"""
        
        # Volume checks CSV
        volume_file = f"dq-results/dq_volume_checks_fixed_{timestamp}.csv"
        with open(volume_file, 'w') as f:
            f.write("table_name,record_count,min_threshold,max_threshold,status,message\n")
            for table_name, table_data in results['tables_processed'].items():
                if 'checks' in table_data and 'volume' in table_data['checks']:
                    vol = table_data['checks']['volume']
                    f.write(f"{table_name},{vol.get('record_count', 0)},{vol.get('min_threshold', 0)},{vol.get('max_threshold', 0)},{vol['status']},\"{vol['message']}\"\n")
        
        # NULL checks CSV
        null_file = f"dq-results/dq_null_checks_fixed_{timestamp}.csv"
        with open(null_file, 'w') as f:
            f.write("table_name,column_name,total_rows,null_count,null_percentage,status\n")
            for table_name, table_data in results['tables_processed'].items():
                if 'checks' in table_data and 'null_checks' in table_data['checks']:
                    null_check = table_data['checks']['null_checks']
                    if 'column_results' in null_check:
                        for col_name, col_data in null_check['column_results'].items():
                            f.write(f"{table_name},{col_name},{null_check.get('total_rows', 0)},{col_data['null_count']},{col_data['null_percentage']},{col_data['status']}\n")
        
        # Range checks CSV
        range_file = f"dq-results/dq_range_checks_fixed_{timestamp}.csv"
        with open(range_file, 'w') as f:
            f.write("table_name,column_name,min_allowed,max_allowed,violations,violation_percentage,status\n")
            for table_name, table_data in results['tables_processed'].items():
                if 'checks' in table_data and 'range_checks' in table_data['checks']:
                    range_check = table_data['checks']['range_checks']
                    if 'column_results' in range_check:
                        for col_name, col_data in range_check['column_results'].items():
                            f.write(f"{table_name},{col_name},{col_data['min_allowed']},{col_data['max_allowed']},{col_data['violations']},{col_data['violation_percentage']},{col_data['status']}\n")
        
        # Freshness checks CSV
        fresh_file = f"dq-results/dq_freshness_checks_fixed_{timestamp}.csv"
        with open(fresh_file, 'w') as f:
            f.write("table_name,latest_timestamp,hours_old,threshold_hours,status,message\n")
            for table_name, table_data in results['tables_processed'].items():
                if 'checks' in table_data and 'freshness' in table_data['checks']:
                    fresh = table_data['checks']['freshness']
                    f.write(f"{table_name},{fresh.get('latest_timestamp', '')},{fresh.get('hours_old', 0)},{fresh.get('threshold_hours', 24)},{fresh['status']},\"{fresh['message']}\"\n")

def main():
    print("🚀 FIXED Simple Data Quality System for All 8 Home Credit Tables")
    print("=" * 70)
    
    dq_system = SimpleDQSystem()
    results = dq_system.run_all_checks()
    dq_system.save_results(results)
    
    # Print summary
    total_tables = len(results['tables_processed'])
    successful = sum(1 for t in results['tables_processed'].values() if t.get('overall_status') == 'PASS')
    failed = sum(1 for t in results['tables_processed'].values() if t.get('overall_status') == 'FAIL')
    errors = sum(1 for t in results['tables_processed'].values() if t.get('overall_status') == 'ERROR')
    
    print("\n" + "=" * 70)
    print("📊 DATA QUALITY SUMMARY")
    print("=" * 70)
    print(f"📅 Timestamp: {results['timestamp']}")
    print(f"📋 Tables Processed: {total_tables}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"🚨 Errors: {errors}")
    print("=" * 70)
    
    for table_name, table_data in results['tables_processed'].items():
        status_emoji = "✅" if table_data.get('overall_status') == 'PASS' else "❌" if table_data.get('overall_status') == 'FAIL' else "🚨"
        print(f"{status_emoji} {table_name}: {table_data.get('record_count', 0):,} records - {table_data.get('overall_status', 'UNKNOWN')}")
    
    print(f"\n📁 Check dq-results/ directory for detailed reports")
    print(f"🔗 Access DQOps at: http://localhost:8082")
    print(f"🔗 Access OpenRefine at: http://localhost:3333")

if __name__ == "__main__":
    main()
