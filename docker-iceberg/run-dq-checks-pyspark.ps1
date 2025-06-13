#!/bin/pwsh

# Script to run data quality checks using PySpark

# Parameters
param (
    [string]$configPath = "n:\Projects\task2\docker-iceberg\dq-config.json",
    [string]$outputPath = "n:\Projects\task2\docker-iceberg\dq-results",
    [switch]$verbose = $false
)

# Ensure output directory exists
if (-not (Test-Path $outputPath)) {
    New-Item -Path $outputPath -ItemType Directory -Force | Out-Null
}

# Copy configuration file to container
Write-Host "Copying configuration file to Spark container..." -ForegroundColor Cyan
docker cp $configPath spark-iceberg:/home/iceberg/notebooks/dq-config.json

# Create output directory in container
docker exec spark-iceberg mkdir -p /home/iceberg/notebooks/dq-results

# Copy the Python script to the container
Write-Host "Copying run_dq_checks_fixed.py to Spark container..." -ForegroundColor Cyan
docker cp "n:\Projects\task2\docker-iceberg\run_dq_checks_fixed.py" spark-iceberg:/home/iceberg/notebooks/

# Execute the Python script in the Spark container
Write-Host "Running PySpark data quality checks..." -ForegroundColor Cyan
docker exec -i spark-iceberg spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1 /home/iceberg/notebooks/run_dq_checks_fixed.py

# Copy results back from container
Write-Host "Copying results back from container..." -ForegroundColor Cyan
docker cp spark-iceberg:/home/iceberg/notebooks/dq-results/. $outputPath

# Generate clean text report automatically
Write-Host "Generating clean text report..." -ForegroundColor Cyan
python "n:\Projects\task2\docker-iceberg\generate_clean_report.py" $outputPath

# Display results location
Write-Host "Data quality check results saved to: $outputPath" -ForegroundColor Green

# If verbose, list the result files
if ($verbose) {
    Write-Host "Result files:" -ForegroundColor Cyan
    Get-ChildItem -Path $outputPath | Sort-Object LastWriteTime -Descending | Select-Object -First 5 | Format-Table Name, LastWriteTime, Length -AutoSize
}
