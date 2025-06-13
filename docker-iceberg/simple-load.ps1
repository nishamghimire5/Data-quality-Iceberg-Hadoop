#!/bin/pwsh

# Script to load data using a different syntax
param (
    [string]$date = "20250612",
    [string]$dataDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily\20250612"
)

# Test a single file with a different syntax
$filePath = Join-Path $dataDir "application_train_$date.csv"

if (-not (Test-Path $filePath)) {
    Write-Host "File not found: $filePath" -ForegroundColor Red
    exit 1
}

# Convert Windows path to Docker-compatible path
$absolutePath = (Resolve-Path $filePath).Path
$projectRoot = (Resolve-Path "n:\Projects\task2").Path
$relativePath = $absolutePath.Replace($projectRoot, "").TrimStart("\")
$dockerPath = "/data/$($relativePath.Replace("\", "/"))"

Write-Host "Loading file: $filePath" -ForegroundColor Cyan
Write-Host "Docker path: $dockerPath" -ForegroundColor Cyan

# Use a simplified syntax for loading
$loadCommand = @"
-- Use a simplified syntax
DROP TABLE IF EXISTS temp_table;

CREATE TABLE temp_table
USING CSV
OPTIONS (
  path '$dockerPath',
  header 'true',
  inferSchema 'true',
  delimiter ','
);

-- See what we loaded
SELECT * FROM temp_table LIMIT 5;

-- Insert data into our iceberg table
INSERT INTO home_credit.application_train
SELECT *, '$date' as DATA_TIMESTAMP, TIMESTAMP('$date 00:00:00') as LOAD_DATE
FROM temp_table;

-- Verify the count
SELECT COUNT(*) FROM home_credit.application_train WHERE DATA_TIMESTAMP = '$date';
"@

# Execute the load command
$loadCommand | docker exec -i spark-iceberg spark-sql
