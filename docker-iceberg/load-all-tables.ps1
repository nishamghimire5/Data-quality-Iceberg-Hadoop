#!/bin/pwsh

# Script to load all daily data into Iceberg tables

# Parameters
param (
    [string]$date = "20250612",
    [string]$dataDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily\$date",
    [switch]$verbose = $false
)

# Check if the data directory exists
if (-not (Test-Path $dataDir)) {
    Write-Host "Error: Data directory does not exist: $dataDir" -ForegroundColor Red
    exit 1
}

# Copy the Python script to the container
Write-Host "Copying load_all_tables.py to Spark container..." -ForegroundColor Cyan
docker cp "n:\Projects\task2\docker-iceberg\load_all_tables.py" spark-iceberg:/home/iceberg/notebooks/

# Execute the Python script in the Spark container
Write-Host "Running PySpark script to load all tables..." -ForegroundColor Cyan
docker exec -i spark-iceberg spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1 /home/iceberg/notebooks/load_all_tables.py $date

Write-Host "Completed data loading process" -ForegroundColor Green

# Optionally run data quality checks if requested
if ($PSBoundParameters.ContainsKey('runDqChecks') -and $runDqChecks) {
    Write-Host "Running data quality checks..." -ForegroundColor Cyan
    & "n:\Projects\task2\docker-iceberg\run-dq-checks-pyspark.ps1" -verbose:$verbose
}
