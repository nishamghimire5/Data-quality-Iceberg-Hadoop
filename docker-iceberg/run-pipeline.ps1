#!/bin/pwsh

# Master script to run the entire data quality process

# Parameters
param (
    [switch]$skipDataPrep = $false,
    [switch]$skipDataLoad = $false,
    [switch]$skipDqChecks = $false,
    [switch]$verbose = $false,
    [int]$samplePercentage = 1,      # Reduced sample percentage for faster processing
    [switch]$useFastMethod = $true,  # Use optimized method for data processing
    [switch]$optimizeLoading = $true # Use optimized loading strategy
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$date = Get-Date -Format "yyyyMMdd"
$overallStartTime = Get-Date

# Function to format time difference
function Format-TimeSpan {
    param (
        [TimeSpan]$timeSpan
    )
    
    if ($timeSpan.TotalHours -ge 1) {
        return "{0:D2}h:{1:D2}m:{2:D2}s" -f $timeSpan.Hours, $timeSpan.Minutes, $timeSpan.Seconds
    } elseif ($timeSpan.TotalMinutes -ge 1) {
        return "{0:D2}m:{1:D2}s" -f $timeSpan.Minutes, $timeSpan.Seconds
    } else {
        return "{0:D2}s" -f $timeSpan.Seconds
    }
}

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "             ICEBERG ON HADOOP - DATA QUALITY PIPELINE                " -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "Date: $date" -ForegroundColor Cyan
Write-Host "Working directory: $scriptDir" -ForegroundColor Cyan
Write-Host "Sample percentage: $samplePercentage%" -ForegroundColor Cyan
Write-Host "Using fast processing method: $useFastMethod" -ForegroundColor Cyan
Write-Host "Using optimized loading: $optimizeLoading" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan

# Step 1: Check if services are running
Write-Progress -Activity "Data Quality Pipeline" -Status "Checking services" -PercentComplete 5
Write-Host "`n[1/4] Checking if services are running..." -ForegroundColor Cyan

$hadoopRunning = docker ps --format "{{.Names}}" | Select-String -Pattern "namenode"
$icebergRunning = docker ps --format "{{.Names}}" | Select-String -Pattern "spark-iceberg"

if (-not $hadoopRunning) {
    Write-Host "Hadoop is not running. Starting Hadoop..." -ForegroundColor Yellow
    Set-Location "n:\Projects\task2\docker-hadoop"
    docker-compose up -d
    Start-Sleep -Seconds 20  # Give Hadoop time to start
}

if (-not $icebergRunning) {
    Write-Host "Iceberg services are not running. Starting Iceberg..." -ForegroundColor Yellow
    & "$scriptDir\setup.ps1"
    Start-Sleep -Seconds 20  # Give Iceberg time to start
}

Write-Host "All services are running." -ForegroundColor Green

# Step 2: Prepare daily data
$stepTimes = @{}

if (-not $skipDataPrep) {
    Write-Progress -Activity "Data Quality Pipeline" -Status "Preparing daily data" -PercentComplete 20
    Write-Host "`n[2/4] Preparing daily data..." -ForegroundColor Cyan
    
    $startTime = Get-Date
    & "$scriptDir\prepare-daily-data.ps1" -date $date -samplePercentage $samplePercentage -useFastMethod:$useFastMethod
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error preparing daily data. Exiting." -ForegroundColor Red
        exit 1
    }
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    $stepTimes["DataPrep"] = $duration
    
    Write-Host "Daily data preparation completed in $(Format-TimeSpan -timeSpan $duration)." -ForegroundColor Green
} else {
    Write-Host "`n[2/4] Skipping daily data preparation." -ForegroundColor Yellow
}

# Step 3: Load data into Iceberg tables
if (-not $skipDataLoad) {
    Write-Progress -Activity "Data Quality Pipeline" -Status "Loading data into Iceberg" -PercentComplete 50
    Write-Host "`n[3/4] Loading data into Iceberg tables..." -ForegroundColor Cyan
    
    $startTime = Get-Date
    # Removed -createTables switch as table creation is no longer handled here.
    & "$scriptDir\\load-daily-data.ps1" -date $date -optimizeLoading:$optimizeLoading 
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error loading daily data. Exiting." -ForegroundColor Red
        exit 1
    }
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    $stepTimes["DataLoad"] = $duration
    
    Write-Host "Data loading completed in $(Format-TimeSpan -timeSpan $duration)." -ForegroundColor Green
} else {
    Write-Host "`n[3/4] Skipping data loading." -ForegroundColor Yellow
}

# Step 4: Run data quality checks
if (-not $skipDqChecks) {    Write-Progress -Activity "Data Quality Pipeline" -Status "Running data quality checks" -PercentComplete 80
    Write-Host "`n[4/4] Running data quality checks..." -ForegroundColor Cyan
    
    $startTime = Get-Date
    & "$scriptDir\run-dq-checks-pyspark.ps1" -verbose:$verbose
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Data quality checks failed." -ForegroundColor Red
    } else {
        $endTime = Get-Date
        $duration = $endTime - $startTime
        $stepTimes["DqChecks"] = $duration
        
        Write-Host "Data quality checks completed in $(Format-TimeSpan -timeSpan $duration)." -ForegroundColor Green
    }
} else {
    Write-Host "`n[4/4] Skipping data quality checks." -ForegroundColor Yellow
}

# Complete the progress bar
Write-Progress -Activity "Data Quality Pipeline" -Status "Completed" -PercentComplete 100
Start-Sleep -Seconds 1
Write-Progress -Activity "Data Quality Pipeline" -Completed

# Calculate overall duration
$overallEndTime = Get-Date
$overallDuration = $overallEndTime - $overallStartTime

Write-Host "`n=====================================================================" -ForegroundColor Cyan
Write-Host "                DATA QUALITY PIPELINE COMPLETED                       " -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "Summary:"
Write-Host "- Date: $date"
Write-Host "- Sample percentage: $samplePercentage%"
Write-Host "- Fast processing: $useFastMethod"
Write-Host "- Optimized loading: $optimizeLoading"
Write-Host "- Total runtime: $(Format-TimeSpan -timeSpan $overallDuration)"
Write-Host "- Data preparation: $(if ($skipDataPrep) { 'Skipped' } else { 'Completed in ' + (Format-TimeSpan -timeSpan $stepTimes["DataPrep"]) })"
Write-Host "- Data loading: $(if ($skipDataLoad) { 'Skipped' } else { 'Completed in ' + (Format-TimeSpan -timeSpan $stepTimes["DataLoad"]) })"
Write-Host "- Data quality checks: $(if ($skipDqChecks) { 'Skipped' } else { 'Completed in ' + (Format-TimeSpan -timeSpan $stepTimes["DqChecks"]) })"
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "Services:"
Write-Host "- Jupyter Notebook: http://localhost:8888"
Write-Host "- Spark UI: http://localhost:8080"
Write-Host "- MinIO Console: http://localhost:9001 (user: admin, password: password)"
Write-Host "- Hadoop NameNode: http://localhost:9870"
Write-Host "=====================================================================" -ForegroundColor Cyan
