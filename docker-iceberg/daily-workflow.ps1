#!/bin/pwsh

# Main workflow script for daily data processing with Iceberg

# Parameters
param (
    [string]$date = (Get-Date -Format "yyyyMMdd"),
    [string]$dataDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily",
    [switch]$skipDataPrep = $false,
    [switch]$skipDataLoad = $false,
    [switch]$skipDQChecks = $false,
    [switch]$verbose = $false
)

# Function to log with timestamp
function Log-Message {
    param (
        [string]$message,
        [string]$color = "White"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $message" -ForegroundColor $color
}

# Set up logging
$logFile = "n:\Projects\task2\docker-iceberg\logs\workflow_$date.log"
$logDir = Split-Path $logFile -Parent
if (-not (Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
}

# Start workflow
$startTime = Get-Date
Log-Message "Starting daily data workflow for date: $date" "Cyan" | Tee-Object -FilePath $logFile -Append

# Step 1: Check if Hadoop and Iceberg services are running
Log-Message "Checking if services are running..." "Cyan" | Tee-Object -FilePath $logFile -Append
$hadoopRunning = docker ps --format "{{.Names}}" | Select-String -Pattern "namenode"
$icebergRunning = docker ps --format "{{.Names}}" | Select-String -Pattern "spark-iceberg"

if (-not $hadoopRunning) {
    Log-Message "Starting Hadoop cluster..." "Yellow" | Tee-Object -FilePath $logFile -Append
    Set-Location "n:\Projects\task2\docker-hadoop"
    docker-compose up -d
    Start-Sleep -Seconds 30  # Give Hadoop time to start
}

if (-not $icebergRunning) {
    Log-Message "Starting Iceberg services..." "Yellow" | Tee-Object -FilePath $logFile -Append
    Set-Location "n:\Projects\task2\docker-iceberg"
    docker-compose up -d
    Start-Sleep -Seconds 20  # Give Iceberg services time to start
}

# Step 2: Prepare daily data (skip if requested)
if (-not $skipDataPrep) {
    $dailyDataDir = Join-Path $dataDir $date
    if (-not (Test-Path $dailyDataDir)) {
        Log-Message "Preparing daily data for $date..." "Cyan" | Tee-Object -FilePath $logFile -Append
        & "n:\Projects\task2\docker-iceberg\prepare-daily-data.ps1" -date $date -verbose:$verbose
    } else {
        Log-Message "Daily data directory already exists: $dailyDataDir" "Green" | Tee-Object -FilePath $logFile -Append
    }
} else {
    Log-Message "Skipping data preparation step" "Yellow" | Tee-Object -FilePath $logFile -Append
}

# Step 3: Load data into Iceberg tables
if (-not $skipDataLoad) {
    Log-Message "Loading data into Iceberg tables..." "Cyan" | Tee-Object -FilePath $logFile -Append
    $loadOutput = & "n:\Projects\task2\docker-iceberg\load-all-tables.ps1" -date $date -verbose:$verbose
    $loadOutput | Tee-Object -FilePath $logFile -Append
} else {
    Log-Message "Skipping data loading step" "Yellow" | Tee-Object -FilePath $logFile -Append
}

# Step 4: Run data quality checks
if (-not $skipDQChecks) {
    Log-Message "Running data quality checks..." "Cyan" | Tee-Object -FilePath $logFile -Append
    $dqOutput = & "n:\Projects\task2\docker-iceberg\run-dq-checks-pyspark.ps1" -verbose:$verbose
    $dqOutput | Tee-Object -FilePath $logFile -Append
    
    # Check for alert messages in DQ output
    $alertCount = ($dqOutput | Select-String -Pattern "ALERT:" -AllMatches).Matches.Count
    if ($alertCount -gt 0) {
        Log-Message "Found $alertCount alerts in data quality checks!" "Yellow" | Tee-Object -FilePath $logFile -Append
    } else {
        Log-Message "All data quality checks passed successfully" "Green" | Tee-Object -FilePath $logFile -Append
    }
} else {
    Log-Message "Skipping data quality check step" "Yellow" | Tee-Object -FilePath $logFile -Append
}

# Calculate total runtime
$endTime = Get-Date
$runtime = $endTime - $startTime
Log-Message "Workflow completed in $($runtime.TotalMinutes.ToString('0.00')) minutes" "Green" | Tee-Object -FilePath $logFile -Append
Log-Message "Log file saved to: $logFile" "Green"
