#!/bin/pwsh

# Script to load CSV data directly into Iceberg tables
param (
    [string]$date = "20250612",
    [string]$dataDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily\20250612"
)

# Test a simple direct load with spark-sql
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

# Direct load command with simpler syntax
$loadCommand = @"
-- First, check if we can directly read the CSV file
CREATE OR REPLACE TEMPORARY VIEW test_view AS
SELECT * FROM csv
OPTIONS (
  path '$dockerPath',
  header 'true',
  inferSchema 'true',
  mode 'PERMISSIVE'
);

-- Check what we can read
SELECT * FROM test_view LIMIT 5;

-- Insert into the actual table
INSERT INTO home_credit.application_train
SELECT *, '$date' as DATA_TIMESTAMP, TIMESTAMP('2025-06-12 16:58:27') as LOAD_DATE
FROM test_view;

-- Check the count
SELECT COUNT(*) FROM home_credit.application_train WHERE DATA_TIMESTAMP = '$date';
"@

# Execute the load command
$loadCommand | docker exec -i spark-iceberg spark-sql
