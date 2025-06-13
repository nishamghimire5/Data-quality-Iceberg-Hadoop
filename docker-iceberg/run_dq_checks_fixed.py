from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, lit, expr, max as max_
import json
import sys
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality Checks") \
    .getOrCreate()

# Read configuration file
try:
    with open('/home/iceberg/notebooks/dq-config.json', 'r') as config_file:
        config = json.load(config_file)
except Exception as e:
    print(f"Error reading configuration file: {str(e)}")
    sys.exit(1)

# Function to run freshness checks
def run_freshness_checks(table_config):
    table_name = table_config["name"]
    checks = [check for check in table_config["checks"] if check["type"] == "freshness"]
    
    if not checks:
        return None
    
    results = []
    for check in checks:
        max_age_days = check["maxAgeDays"]
        alert_level = check["alertLevel"]
        
        try:
            # Query to check data freshness
            df = spark.sql(f"""
                SELECT 
                  '{table_name}' as table_name,
                  CURRENT_TIMESTAMP() as check_time,
                  MAX(LOAD_DATE) as last_updated,
                  CASE 
                    WHEN MAX(LOAD_DATE) < current_timestamp() - INTERVAL {max_age_days} DAY THEN 'ALERT: Stale data (older than {max_age_days} days)'
                    ELSE 'OK: Data is fresh'
                  END as freshness_status,
                  '{alert_level}' as alert_level
                FROM home_credit.{table_name}
                GROUP BY 1, 5
            """)
            
            results.append(df)
        except Exception as e:
            print(f"Error running freshness check for {table_name}: {str(e)}")
    
    return results

# Function to run null checks
def run_null_checks(table_config):
    table_name = table_config["name"]
    checks = [check for check in table_config["checks"] if check["type"] == "nullCheck"]
    
    if not checks:
        return None
    
    results = []
    for check in checks:
        columns = check["columns"]
        max_null_pct = check["maxNullPercentage"]
        alert_level = check["alertLevel"]
        
        try:
            # Create separate records for each column to standardize schema
            column_results = []
            for col in columns:
                query = f"""
                    SELECT
                      '{table_name}' as table_name,
                      '{col}' as column_name,
                      COUNT(*) as total_rows,
                      SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count,
                      ROUND(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage,
                      CASE
                        WHEN (SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) > {max_null_pct}
                        THEN 'ALERT: Too many NULL values (> {max_null_pct}%)'
                        ELSE 'OK: NULL values within acceptable range'
                      END as null_check_status,
                      '{alert_level}' as alert_level
                    FROM home_credit.{table_name}
                """
                
                df = spark.sql(query)
                column_results.append(df)
            
            # Union all column results for this table
            if column_results:
                table_result = column_results[0]
                for i in range(1, len(column_results)):
                    table_result = table_result.union(column_results[i])
                results.append(table_result)
                
        except Exception as e:
            print(f"Error running null check for {table_name}: {str(e)}")
    
    return results

# Function to run value range checks
def run_value_range_checks(table_config):
    table_name = table_config["name"]
    checks = [check for check in table_config["checks"] if check["type"] == "valueRange"]
    
    if not checks:
        return None
    
    results = []
    for check in checks:
        column = check["column"]
        min_val = check["min"]
        max_val = check["max"]
        alert_level = check["alertLevel"]
        
        try:
            query = f"""
                SELECT
                  '{table_name}' as table_name,
                  '{column}' as column_name,
                  MIN({column}) as min_value,
                  MAX({column}) as max_value,
                  AVG({column}) as avg_value,
                  COUNT(*) as total_rows,
                  SUM(CASE WHEN {column} < {min_val} OR {column} > {max_val} THEN 1 ELSE 0 END) as out_of_range_count,
                  ROUND(SUM(CASE WHEN {column} < {min_val} OR {column} > {max_val} THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as out_of_range_percentage,
                  CASE
                    WHEN SUM(CASE WHEN {column} < {min_val} OR {column} > {max_val} THEN 1 ELSE 0 END) > 0
                    THEN 'ALERT: Values outside allowed range ({min_val} to {max_val})'
                    ELSE 'OK: All values within range'
                  END as range_check_status,
                  '{alert_level}' as alert_level
                FROM home_credit.{table_name}
                WHERE {column} IS NOT NULL
                GROUP BY 1, 2, 10
            """
            
            df = spark.sql(query)
            results.append(df)
        except Exception as e:
            print(f"Error running value range check for {table_name}.{column}: {str(e)}")
    
    return results

# Function to run volume checks
def run_volume_checks(table_config):
    table_name = table_config["name"]
    checks = [check for check in table_config["checks"] if check["type"] == "volumeCheck"]
    
    if not checks:
        return None
    
    results = []
    for check in checks:
        min_row_count = check["minRowCount"]
        alert_level = check["alertLevel"]
        
        try:
            query = f"""
                SELECT
                  '{table_name}' as table_name,
                  COUNT(*) as row_count,
                  CASE
                    WHEN COUNT(*) < {min_row_count} THEN 'ALERT: Row count below minimum ({min_row_count})'
                    ELSE 'OK: Row count meets minimum threshold'
                  END as volume_check_status,
                  '{alert_level}' as alert_level
                FROM home_credit.{table_name}
                GROUP BY 1, 4
            """
            
            df = spark.sql(query)
            results.append(df)
        except Exception as e:
            print(f"Error running volume check for {table_name}: {str(e)}")
    
    return results

# Function to generate a clean, readable text report
def generate_text_report(all_results, output_path, alert_count):
    """Generate a clean, vertical-format text report"""
    
    report_lines = []
    
    # Header
    report_lines.append("=" * 80)
    report_lines.append("DATA QUALITY CHECK REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Total Alerts Found: {alert_count}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Separate results by type
    freshness_results = []
    null_results = []
    range_results = []
    volume_results = []
    
    for df in all_results:
        columns = df.columns
        if "freshness_status" in columns:
            freshness_results.append(df)
        elif "null_check_status" in columns:
            null_results.append(df)
        elif "range_check_status" in columns:
            range_results.append(df)
        elif "volume_check_status" in columns:
            volume_results.append(df)
    
    # Process each check type
    def add_section(title, results, status_col):
        if not results:
            return
            
        report_lines.append(f"\n{title}")
        report_lines.append("-" * 50)
        
        for df in results:
            rows = df.collect()
            for row in rows:
                report_lines.append("")
                
                # Table name
                report_lines.append(f"📊 Table: {row['table_name']}")
                
                # Check-specific details
                if status_col == "freshness_status":
                    report_lines.append(f"   Last Updated: {row['last_updated']}")
                    report_lines.append(f"   Check Time: {row['check_time']}")
                    status = row['freshness_status']
                elif status_col == "null_check_status":
                    report_lines.append(f"   Column: {row['column_name']}")
                    report_lines.append(f"   Total Rows: {row['total_rows']:,}")
                    report_lines.append(f"   Null Count: {row['null_count']:,}")
                    report_lines.append(f"   Null Percentage: {row['null_percentage']}%")
                    status = row['null_check_status']
                elif status_col == "range_check_status":
                    report_lines.append(f"   Column: {row['column_name']}")
                    report_lines.append(f"   Min Value: {row['min_value']:,.2f}")
                    report_lines.append(f"   Max Value: {row['max_value']:,.2f}")
                    report_lines.append(f"   Average: {row['avg_value']:,.2f}")
                    report_lines.append(f"   Total Rows: {row['total_rows']:,}")
                    report_lines.append(f"   Out of Range: {row['out_of_range_count']:,} ({row['out_of_range_percentage']}%)")
                    status = row['range_check_status']
                elif status_col == "volume_check_status":
                    report_lines.append(f"   Row Count: {row['row_count']:,}")
                    status = row['volume_check_status']
                
                # Status with emoji indicators
                if "ALERT" in status:
                    report_lines.append(f"   🚨 Status: {status}")
                    report_lines.append(f"   Alert Level: {row['alert_level'].upper()}")
                else:
                    report_lines.append(f"   ✅ Status: {status}")
                    report_lines.append(f"   Alert Level: {row['alert_level']}")
    
    # Add all sections
    add_section("FRESHNESS CHECKS", freshness_results, "freshness_status")
    add_section("NULL VALUE CHECKS", null_results, "null_check_status")
    add_section("RANGE CHECKS", range_results, "range_check_status")
    add_section("VOLUME CHECKS", volume_results, "volume_check_status")
    
    # Summary section
    report_lines.append("\n" + "=" * 80)
    report_lines.append("SUMMARY")
    report_lines.append("=" * 80)
    
    # Count by check type
    total_freshness = sum(df.count() for df in freshness_results)
    total_null = sum(df.count() for df in null_results)
    total_range = sum(df.count() for df in range_results)
    total_volume = sum(df.count() for df in volume_results)
    
    report_lines.append(f"Freshness Checks: {total_freshness}")
    report_lines.append(f"Null Value Checks: {total_null}")
    report_lines.append(f"Range Checks: {total_range}")
    report_lines.append(f"Volume Checks: {total_volume}")
    report_lines.append(f"Total Checks: {total_freshness + total_null + total_range + total_volume}")
    
    # Alert summary
    if alert_count > 0:
        report_lines.append(f"\n🚨 ATTENTION: {alert_count} alerts require review")
        
        # List specific alerts
        report_lines.append("\nAlert Details:")
        for df in all_results:
            rows = df.collect()
            for row in rows:
                status = None
                
                if "freshness_status" in df.columns:
                    status = row['freshness_status']
                elif "null_check_status" in df.columns:
                    status = row['null_check_status']
                elif "range_check_status" in df.columns:
                    status = row['range_check_status']
                elif "volume_check_status" in df.columns:
                    status = row['volume_check_status']
                
                if status and "ALERT" in status:
                    table = row['table_name']
                    level = row['alert_level']
                    if "column_name" in row.asDict():
                        column = row['column_name']
                        report_lines.append(f"  • {table}.{column}: {status} [{level}]")
                    else:
                        report_lines.append(f"  • {table}: {status} [{level}]")
    else:
        report_lines.append(f"\n✅ No alerts - all data quality checks passed!")
    
    report_lines.append("\n" + "=" * 80)
    
    # Write the report
    text_report_path = f"{output_path}.txt"
    with open(text_report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    return text_report_path

# Run all checks
all_results = []
alert_count = 0

print("\n" + "="*80)
print("RUNNING DATA QUALITY CHECKS")
print("="*80)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"/home/iceberg/notebooks/dq-results/dq_report_{timestamp}"

# Process each table in the configuration
for table in config["tables"]:
    table_name = table["name"]
    print(f"\nChecking table: {table_name}")
    
    # Run all types of checks
    freshness_results = run_freshness_checks(table)
    null_results = run_null_checks(table)
    range_results = run_value_range_checks(table)
    volume_results = run_volume_checks(table)
    
    # Combine results
    table_results = []
    for result_set in [freshness_results, null_results, range_results, volume_results]:
        if result_set:
            table_results.extend(result_set)
    
    # Save table results
    if table_results:
        all_results.extend(table_results)
        
        # Count alerts and display them
        for df in table_results:
            try:
                # Check for alerts
                alert_rows = None
                columns = df.columns
                
                if "freshness_status" in columns:
                    alert_rows = df.filter(col("freshness_status").like("ALERT:%"))
                elif "null_check_status" in columns:
                    alert_rows = df.filter(col("null_check_status").like("ALERT:%"))
                elif "range_check_status" in columns:
                    alert_rows = df.filter(col("range_check_status").like("ALERT:%"))
                elif "volume_check_status" in columns:
                    alert_rows = df.filter(col("volume_check_status").like("ALERT:%"))
                
                if alert_rows and alert_rows.count() > 0:
                    alert_count += alert_rows.count()
                    print(f"\nALERTS FOUND for {table_name}:")
                    alert_rows.show(truncate=False)
                
                # Also show all results for inspection
                print(f"\nAll results for {table_name}:")
                df.show(truncate=False)
                
            except Exception as e:
                print(f"Error processing results for {table_name}: {str(e)}")

# Save results
if all_results:
    print("\n" + "="*80)
    print(f"DATA QUALITY CHECK SUMMARY: {alert_count} alerts found")
    print("="*80)
    
    try:
        # Save each check type separately
        freshness_dfs = []
        null_dfs = []
        range_dfs = []
        volume_dfs = []
        
        for df in all_results:
            columns = df.columns
            if "freshness_status" in columns:
                freshness_dfs.append(df)
            elif "null_check_status" in columns:
                null_dfs.append(df)
            elif "range_check_status" in columns:
                range_dfs.append(df)
            elif "volume_check_status" in columns:
                volume_dfs.append(df)
        
        # Save each type separately
        if freshness_dfs:
            combined = freshness_dfs[0]
            for i in range(1, len(freshness_dfs)):
                combined = combined.union(freshness_dfs[i])
            combined.write.mode("overwrite").parquet(f"{output_path}_freshness.parquet")
            combined.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}_freshness.csv")
            print(f"- Freshness results: {output_path}_freshness.csv")
            
        if null_dfs:
            combined = null_dfs[0]
            for i in range(1, len(null_dfs)):
                combined = combined.union(null_dfs[i])
            combined.write.mode("overwrite").parquet(f"{output_path}_null.parquet")
            combined.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}_null.csv")
            print(f"- Null check results: {output_path}_null.csv")
            
        if range_dfs:
            combined = range_dfs[0]
            for i in range(1, len(range_dfs)):
                combined = combined.union(range_dfs[i])
            combined.write.mode("overwrite").parquet(f"{output_path}_range.parquet")
            combined.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}_range.csv")
            print(f"- Range check results: {output_path}_range.csv")
            
        if volume_dfs:
            combined = volume_dfs[0]
            for i in range(1, len(volume_dfs)):
                combined = combined.union(volume_dfs[i])
            combined.write.mode("overwrite").parquet(f"{output_path}_volume.parquet")
            combined.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}_volume.csv")
            print(f"- Volume check results: {output_path}_volume.csv")
            
        # Generate clean text report
        text_report_path = generate_text_report(all_results, output_path, alert_count)
        print(f"- Readable text report: {text_report_path}")
            
    except Exception as e:
        print(f"Error saving results: {str(e)}")
else:
    print("No data quality check results were generated.")

# Close the Spark session
spark.stop()
