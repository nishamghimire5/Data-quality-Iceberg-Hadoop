#!/bin/pwsh

# Script to create a PySpark script for loading data

# Parameters
param (
    [string]$date = "20250612",
    [string]$dataDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily\20250612"
)

# Create a Python script for loading data
$pythonScript = @"
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Data") \
    .getOrCreate()

# Path to CSV file - using direct path inside container
file_path = "/data/home-credit-default-risk-dataset/daily/20250612/application_train_20250612.csv"

# Load the CSV file
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Add timestamp columns
df = df.withColumn("DATA_TIMESTAMP", lit("20250612")) \
    .withColumn("LOAD_DATE", lit(datetime.now()))

# Show a sample of the data
print("Sample data:")
df.show(5)

# Count rows
print(f"Total rows: {df.count()}")

# Insert into Iceberg table
print("Loading data into Iceberg table...")
df.write \
    .format("iceberg") \
    .mode("append") \
    .save("home_credit.application_train")

print("Data loaded successfully")

# Count rows in the destination table
count = spark.sql("SELECT COUNT(*) FROM home_credit.application_train WHERE DATA_TIMESTAMP = '20250612'").collect()[0][0]
print(f"Rows in home_credit.application_train: {count}")

# Close the Spark session
spark.stop()
"@

# Save the Python script
$pythonScriptPath = "n:\Projects\task2\docker-iceberg\load_data.py"
$pythonScript | Out-File -FilePath $pythonScriptPath -Encoding utf8

Write-Host "Created PySpark script at: $pythonScriptPath" -ForegroundColor Cyan

# Execute the Python script in the Spark container
Write-Host "Running PySpark script..." -ForegroundColor Cyan
docker exec -i spark-iceberg spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1 /home/iceberg/notebooks/load_data.py

Write-Host "Completed PySpark execution" -ForegroundColor Green
