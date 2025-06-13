from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime
import os
import sys

# Get date parameter if provided, default to today
data_date = sys.argv[1] if len(sys.argv) > 1 else "20250612"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Daily Data") \
    .getOrCreate()

# Table definitions with their corresponding file names
tables = [
    {"table_name": "application_train", "file_name": f"application_train_{data_date}.csv"},
    {"table_name": "application_test", "file_name": f"application_test_{data_date}.csv"},
    {"table_name": "bureau", "file_name": f"bureau_{data_date}.csv"},
    {"table_name": "bureau_balance", "file_name": f"bureau_balance_{data_date}.csv"},
    {"table_name": "credit_card_balance", "file_name": f"credit_card_balance_{data_date}.csv"},
    {"table_name": "installments_payments", "file_name": f"installments_payments_{data_date}.csv"},
    {"table_name": "pos_cash_balance", "file_name": f"POS_CASH_balance_{data_date}.csv"},
    {"table_name": "previous_application", "file_name": f"previous_application_{data_date}.csv"}
]

# Base directory for data files
base_dir = f"/data/home-credit-default-risk-dataset/daily/{data_date}"

# Process each table
results = []
for table_info in tables:
    table_name = table_info["table_name"]
    file_name = table_info["file_name"]
    file_path = f"{base_dir}/{file_name}"
    
    print(f"\n{'='*50}")
    print(f"Processing {table_name} from {file_path}")
    print(f"{'='*50}")
    
    # Check if file exists
    try:
        # Load the CSV file
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        
        # Add timestamp columns
        df = df.withColumn("DATA_TIMESTAMP", lit(data_date)) \
            .withColumn("LOAD_DATE", current_timestamp())
        
        # Show sample and count
        print(f"Sample data for {table_name}:")
        df.show(3)
        row_count = df.count()
        print(f"Total rows for {table_name}: {row_count}")
          # Insert into Iceberg table
        print(f"Loading data into Iceberg table home_credit.{table_name}...")
          # Delete existing data for this date before loading (to prevent duplicates)
        try:
            # Execute DELETE and make sure it completes successfully
            delete_result = spark.sql(f"DELETE FROM home_credit.{table_name} WHERE DATA_TIMESTAMP = '{data_date}'")
            # Force execution of the DELETE operation by collecting results
            delete_result.collect()
            # Refresh table metadata to ensure clean state
            spark.sql(f"REFRESH TABLE home_credit.{table_name}")
            print(f"Successfully deleted existing data for {table_name} with DATA_TIMESTAMP = '{data_date}'")
        except Exception as e:
            print(f"Warning: Could not delete existing data: {str(e)}")
            
        # Use saveAsTable instead of save to ensure table consistency
        df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(f"home_credit.{table_name}")
        
        # Verify data was loaded
        verification_count = spark.sql(f"SELECT COUNT(*) FROM home_credit.{table_name} WHERE DATA_TIMESTAMP = '{data_date}'").collect()[0][0]
        print(f"Verified rows in home_credit.{table_name}: {verification_count}")
        
        # Save result
        results.append({
            "table": table_name,
            "file": file_name,
            "rows_processed": row_count,
            "rows_loaded": verification_count,
            "status": "SUCCESS" if verification_count == row_count else "PARTIAL"
        })
        
    except Exception as e:
        print(f"ERROR processing {table_name}: {str(e)}")
        results.append({
            "table": table_name,
            "file": file_name,
            "status": "ERROR",
            "error": str(e)
        })

# Print summary
print("\n\n" + "="*80)
print("LOADING SUMMARY")
print("="*80)
print(f"{'Table':<25} {'Status':<10} {'Rows Processed':<15} {'Rows Loaded':<15}")
print("-"*80)
for result in results:
    status = result["status"]
    rows_processed = result.get("rows_processed", "N/A")
    rows_loaded = result.get("rows_loaded", "N/A")
    print(f"{result['table']:<25} {status:<10} {str(rows_processed):<15} {str(rows_loaded):<15}")

# Close the Spark session
spark.stop()
