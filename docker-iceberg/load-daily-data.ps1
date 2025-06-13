#!/bin/pwsh

# Script to simulate daily data loads into Iceberg tables

# Parameters
param (
    [string]$date = (Get-Date -Format "yyyyMMdd"),
    [string]$dailyDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily",
    [switch]$createTables = $false,   # Whether to create tables if they don't exist
    [switch]$appendOnly = $false,     # If true, only append data; if false, will replace data for the date
    [switch]$optimizeLoading = $true   # Use optimized loading strategy
)

# Function to load data for a specific date
function Load-DailyData {
    param (
        [string]$date,
        [string]$dailyDir,
        [switch]$createTables,
        [switch]$appendOnly,
        [switch]$optimizeLoading
    )
    
    Write-Host "Loading data for $date..." -ForegroundColor Cyan
    Write-Progress -Activity "Loading Daily Data" -Status "Initializing" -PercentComplete 0
    
    # Check if the daily directory exists
    $dateSpecificDir = Join-Path $dailyDir $date
    if (-not (Test-Path $dateSpecificDir)) {
        Write-Host "Directory not found: $dateSpecificDir" -ForegroundColor Red
        return $false
    }
    
    # Check if files exist
    $requiredFiles = @(
        "application_train_$date.csv",
        "application_test_$date.csv",
        "bureau_$date.csv",
        "bureau_balance_$date.csv",
        "credit_card_balance_$date.csv",
        "installments_payments_$date.csv",
        "POS_CASH_balance_$date.csv",
        "previous_application_$date.csv"
    )
    
    $missingFiles = @()
    foreach ($file in $requiredFiles) {
        $filePath = Join-Path $dateSpecificDir $file
        if (-not (Test-Path $filePath)) {
            $missingFiles += $file
        }
    }
    
    if ($missingFiles.Count -gt 0) {
        Write-Host "Missing files for $($date):" -ForegroundColor Yellow
        foreach ($file in $missingFiles) {
            Write-Host "  - $file" -ForegroundColor Yellow
        }
        Write-Host "Will proceed with available files only." -ForegroundColor Yellow
    }

    # Table creation logic removed as per revised scope.
    # It's assumed tables are created once separately or exist.
    # if ($createTables) {
    #     Write-Host "Creating database and tables (if they don't exist)..." -ForegroundColor Cyan
    #     Write-Progress -Activity "Loading Daily Data" -Status "Creating tables" -PercentComplete 15
        
    #     $createTablesSQL = @"
    # -- Create database
    # CREATE DATABASE IF NOT EXISTS home_credit;
    # USE home_credit;

    # -- Create application_train table
    # CREATE TABLE IF NOT EXISTS home_credit.application_train (
    #   SK_ID_CURR INT,
    #   TARGET INT,
    #   NAME_CONTRACT_TYPE STRING,
    #   CODE_GENDER STRING,
    #   FLAG_OWN_CAR STRING,
    #   FLAG_OWN_REALTY STRING,
    #   CNT_CHILDREN INT,
    #   AMT_INCOME_TOTAL DOUBLE,
    #   AMT_CREDIT DOUBLE,
    #   AMT_ANNUITY DOUBLE,
    #   AMT_GOODS_PRICE DOUBLE,
    #   NAME_TYPE_SUITE STRING,
    #   NAME_INCOME_TYPE STRING,
    #   NAME_EDUCATION_TYPE STRING,
    #   NAME_FAMILY_STATUS STRING,
    #   NAME_HOUSING_TYPE STRING,
    #   REGION_POPULATION_RELATIVE DOUBLE,
    #   DAYS_BIRTH INT,
    #   DAYS_EMPLOYED INT,
    #   DAYS_REGISTRATION DOUBLE,
    #   DAYS_ID_PUBLISH INT,
    #   OWN_CAR_AGE DOUBLE,
    #   FLAG_MOBIL INT,
    #   FLAG_EMP_PHONE INT,
    #   FLAG_WORK_PHONE INT,
    #   FLAG_CONT_MOBILE INT,
    #   FLAG_PHONE INT,
    #   FLAG_EMAIL INT,
    #   OCCUPATION_TYPE STRING,
    #   CNT_FAM_MEMBERS DOUBLE,
    #   REGION_RATING_CLIENT INT,
    #   REGION_RATING_CLIENT_W_CITY INT,
    #   WEEKDAY_APPR_PROCESS_START STRING,
    #   HOUR_APPR_PROCESS_START INT,
    #   REG_REGION_NOT_LIVE_REGION INT,
    #   REG_REGION_NOT_WORK_REGION INT,
    #   LIVE_REGION_NOT_WORK_REGION INT,
    #   REG_CITY_NOT_LIVE_CITY INT,
    #   REG_CITY_NOT_WORK_CITY INT,
    #   LIVE_CITY_NOT_WORK_CITY INT,
    #   ORGANIZATION_TYPE STRING,
    #   EXT_SOURCE_1 DOUBLE,
    #   EXT_SOURCE_2 DOUBLE,
    #   EXT_SOURCE_3 DOUBLE,
    #   APARTMENTS_AVG DOUBLE,
    #   BASEMENTAREA_AVG DOUBLE,
    #   YEARS_BEGINEXPLUATATION_AVG DOUBLE,
    #   YEARS_BUILD_AVG DOUBLE,
    #   COMMONAREA_AVG DOUBLE,
    #   ELEVATORS_AVG DOUBLE,
    #   ENTRANCES_AVG DOUBLE,
    #   FLOORSMAX_AVG DOUBLE,
    #   FLOORSMIN_AVG DOUBLE,
    #   LANDAREA_AVG DOUBLE,
    #   LIVINGAPARTMENTS_AVG DOUBLE,
    #   LIVINGAREA_AVG DOUBLE,
    #   NONLIVINGAPARTMENTS_AVG DOUBLE,
    #   NONLIVINGAREA_AVG DOUBLE,
    #   APARTMENTS_MODE DOUBLE,
    #   BASEMENTAREA_MODE DOUBLE,
    #   YEARS_BEGINEXPLUATATION_MODE DOUBLE,
    #   YEARS_BUILD_MODE DOUBLE,
    #   COMMONAREA_MODE DOUBLE,
    #   ELEVATORS_MODE DOUBLE,
    #   ENTRANCES_MODE DOUBLE,
    #   FLOORSMAX_MODE DOUBLE,
    #   FLOORSMIN_MODE DOUBLE,
    #   LANDAREA_MODE DOUBLE,
    #   LIVINGAPARTMENTS_MODE DOUBLE,
    #   LIVINGAREA_MODE DOUBLE,
    #   NONLIVINGAPARTMENTS_MODE DOUBLE,
    #   NONLIVINGAREA_MODE DOUBLE,
    #   APARTMENTS_MEDI DOUBLE,
    #   BASEMENTAREA_MEDI DOUBLE,
    #   YEARS_BEGINEXPLUATATION_MEDI DOUBLE,
    #   YEARS_BUILD_MEDI DOUBLE,
    #   COMMONAREA_MEDI DOUBLE,
    #   ELEVATORS_MEDI DOUBLE,
    #   ENTRANCES_MEDI DOUBLE,
    #   FLOORSMAX_MEDI DOUBLE,
    #   FLOORSMIN_MEDI DOUBLE,
    #   LANDAREA_MEDI DOUBLE,
    #   LIVINGAPARTMENTS_MEDI DOUBLE,
    #   LIVINGAREA_MEDI DOUBLE,
    #   NONLIVINGAPARTMENTS_MEDI DOUBLE,
    #   NONLIVINGAREA_MEDI DOUBLE,
    #   FONDKAPREMONT_MODE STRING,
    #   HOUSETYPE_MODE STRING,
    #   TOTALAREA_MODE DOUBLE,
    #   WALLSMATERIAL_MODE STRING,
    #   EMERGENCYSTATE_MODE STRING,
    #   OBS_30_CNT_SOCIAL_CIRCLE DOUBLE,
    #   DEF_30_CNT_SOCIAL_CIRCLE DOUBLE,
    #   OBS_60_CNT_SOCIAL_CIRCLE DOUBLE,
    #   DEF_60_CNT_SOCIAL_CIRCLE DOUBLE,
    #   DAYS_LAST_PHONE_CHANGE DOUBLE,
    #   FLAG_DOCUMENT_2 INT,
    #   FLAG_DOCUMENT_3 INT,
    #   FLAG_DOCUMENT_4 INT,
    #   FLAG_DOCUMENT_5 INT,
    #   FLAG_DOCUMENT_6 INT,
    #   FLAG_DOCUMENT_7 INT,
    #   FLAG_DOCUMENT_8 INT,
    #   FLAG_DOCUMENT_9 INT,
    #   FLAG_DOCUMENT_10 INT,
    #   FLAG_DOCUMENT_11 INT,
    #   FLAG_DOCUMENT_12 INT,
    #   FLAG_DOCUMENT_13 INT,
    #   FLAG_DOCUMENT_14 INT,
    #   FLAG_DOCUMENT_15 INT,
    #   FLAG_DOCUMENT_16 INT,
    #   FLAG_DOCUMENT_17 INT,
    #   FLAG_DOCUMENT_18 INT,
    #   FLAG_DOCUMENT_19 INT,
    #   FLAG_DOCUMENT_20 INT,
    #   FLAG_DOCUMENT_21 INT,
    #   AMT_REQ_CREDIT_BUREAU_HOUR DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_DAY DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_WEEK DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_MON DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_QRT DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_YEAR DOUBLE,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create application_test table
    # CREATE TABLE IF NOT EXISTS home_credit.application_test (
    #   SK_ID_CURR INT,
    #   NAME_CONTRACT_TYPE STRING,
    #   CODE_GENDER STRING,
    #   FLAG_OWN_CAR STRING,
    #   FLAG_OWN_REALTY STRING,
    #   CNT_CHILDREN INT,
    #   AMT_INCOME_TOTAL DOUBLE,
    #   AMT_CREDIT DOUBLE,
    #   AMT_ANNUITY DOUBLE,
    #   AMT_GOODS_PRICE DOUBLE,
    #   NAME_TYPE_SUITE STRING,
    #   NAME_INCOME_TYPE STRING,
    #   NAME_EDUCATION_TYPE STRING,
    #   NAME_FAMILY_STATUS STRING,
    #   NAME_HOUSING_TYPE STRING,
    #   REGION_POPULATION_RELATIVE DOUBLE,
    #   DAYS_BIRTH INT,
    #   DAYS_EMPLOYED INT,
    #   DAYS_REGISTRATION DOUBLE,
    #   DAYS_ID_PUBLISH INT,
    #   OWN_CAR_AGE DOUBLE,
    #   FLAG_MOBIL INT,
    #   FLAG_EMP_PHONE INT,
    #   FLAG_WORK_PHONE INT,
    #   FLAG_CONT_MOBILE INT,
    #   FLAG_PHONE INT,
    #   FLAG_EMAIL INT,
    #   OCCUPATION_TYPE STRING,
    #   CNT_FAM_MEMBERS DOUBLE,
    #   REGION_RATING_CLIENT INT,
    #   REGION_RATING_CLIENT_W_CITY INT,
    #   WEEKDAY_APPR_PROCESS_START STRING,
    #   HOUR_APPR_PROCESS_START INT,
    #   REG_REGION_NOT_LIVE_REGION INT,
    #   REG_REGION_NOT_WORK_REGION INT,
    #   LIVE_REGION_NOT_WORK_REGION INT,
    #   REG_CITY_NOT_LIVE_CITY INT,
    #   REG_CITY_NOT_WORK_CITY INT,
    #   LIVE_CITY_NOT_WORK_CITY INT,
    #   ORGANIZATION_TYPE STRING,
    #   EXT_SOURCE_1 DOUBLE,
    #   EXT_SOURCE_2 DOUBLE,
    #   EXT_SOURCE_3 DOUBLE,
    #   APARTMENTS_AVG DOUBLE,
    #   BASEMENTAREA_AVG DOUBLE,
    #   YEARS_BEGINEXPLUATATION_AVG DOUBLE,
    #   YEARS_BUILD_AVG DOUBLE,
    #   COMMONAREA_AVG DOUBLE,
    #   ELEVATORS_AVG DOUBLE,
    #   ENTRANCES_AVG DOUBLE,
    #   FLOORSMAX_AVG DOUBLE,
    #   FLOORSMIN_AVG DOUBLE,
    #   LANDAREA_AVG DOUBLE,
    #   LIVINGAPARTMENTS_AVG DOUBLE,
    #   LIVINGAREA_AVG DOUBLE,
    #   NONLIVINGAPARTMENTS_AVG DOUBLE,
    #   NONLIVINGAREA_AVG DOUBLE,
    #   APARTMENTS_MODE DOUBLE,
    #   BASEMENTAREA_MODE DOUBLE,
    #   YEARS_BEGINEXPLUATATION_MODE DOUBLE,
    #   YEARS_BUILD_MODE DOUBLE,
    #   COMMONAREA_MODE DOUBLE,
    #   ELEVATORS_MODE DOUBLE,
    #   ENTRANCES_MODE DOUBLE,
    #   FLOORSMAX_MODE DOUBLE,
    #   FLOORSMIN_MODE DOUBLE,
    #   LANDAREA_MODE DOUBLE,
    #   LIVINGAPARTMENTS_MODE DOUBLE,
    #   LIVINGAREA_MODE DOUBLE,
    #   NONLIVINGAPARTMENTS_MODE DOUBLE,
    #   NONLIVINGAREA_MODE DOUBLE,
    #   APARTMENTS_MEDI DOUBLE,
    #   BASEMENTAREA_MEDI DOUBLE,
    #   YEARS_BEGINEXPLUATATION_MEDI DOUBLE,
    #   YEARS_BUILD_MEDI DOUBLE,
    #   COMMONAREA_MEDI DOUBLE,
    #   ELEVATORS_MEDI DOUBLE,
    #   ENTRANCES_MEDI DOUBLE,
    #   FLOORSMAX_MEDI DOUBLE,
    #   FLOORSMIN_MEDI DOUBLE,
    #   LANDAREA_MEDI DOUBLE,
    #   LIVINGAPARTMENTS_MEDI DOUBLE,
    #   LIVINGAREA_MEDI DOUBLE,
    #   NONLIVINGAPARTMENTS_MEDI DOUBLE,
    #   NONLIVINGAREA_MEDI DOUBLE,
    #   FONDKAPREMONT_MODE STRING,
    #   HOUSETYPE_MODE STRING,
    #   TOTALAREA_MODE DOUBLE,
    #   WALLSMATERIAL_MODE STRING,
    #   EMERGENCYSTATE_MODE STRING,
    #   OBS_30_CNT_SOCIAL_CIRCLE DOUBLE,
    #   DEF_30_CNT_SOCIAL_CIRCLE DOUBLE,
    #   OBS_60_CNT_SOCIAL_CIRCLE DOUBLE,
    #   DEF_60_CNT_SOCIAL_CIRCLE DOUBLE,
    #   DAYS_LAST_PHONE_CHANGE DOUBLE,
    #   FLAG_DOCUMENT_2 INT,
    #   FLAG_DOCUMENT_3 INT,
    #   FLAG_DOCUMENT_4 INT,
    #   FLAG_DOCUMENT_5 INT,
    #   FLAG_DOCUMENT_6 INT,
    #   FLAG_DOCUMENT_7 INT,
    #   FLAG_DOCUMENT_8 INT,
    #   FLAG_DOCUMENT_9 INT,
    #   FLAG_DOCUMENT_10 INT,
    #   FLAG_DOCUMENT_11 INT,
    #   FLAG_DOCUMENT_12 INT,
    #   FLAG_DOCUMENT_13 INT,
    #   FLAG_DOCUMENT_14 INT,
    #   FLAG_DOCUMENT_15 INT,
    #   FLAG_DOCUMENT_16 INT,
    #   FLAG_DOCUMENT_17 INT,
    #   FLAG_DOCUMENT_18 INT,
    #   FLAG_DOCUMENT_19 INT,
    #   FLAG_DOCUMENT_20 INT,
    #   FLAG_DOCUMENT_21 INT,
    #   AMT_REQ_CREDIT_BUREAU_HOUR DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_DAY DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_WEEK DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_MON DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_QRT DOUBLE,
    #   AMT_REQ_CREDIT_BUREAU_YEAR DOUBLE,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create bureau table
    # CREATE TABLE IF NOT EXISTS home_credit.bureau (
    #   SK_ID_CURR INT,
    #   SK_ID_BUREAU INT,
    #   CREDIT_ACTIVE STRING,
    #   CREDIT_CURRENCY STRING,
    #   DAYS_CREDIT INT,
    #   CREDIT_DAY_OVERDUE INT,
    #   DAYS_CREDIT_ENDDATE DOUBLE,
    #   DAYS_ENDDATE_FACT DOUBLE,
    #   AMT_CREDIT_MAX_OVERDUE DOUBLE,
    #   CNT_CREDIT_PROLONG INT,
    #   AMT_CREDIT_SUM DOUBLE,
    #   AMT_CREDIT_SUM_DEBT DOUBLE,
    #   AMT_CREDIT_SUM_LIMIT DOUBLE,
    #   AMT_CREDIT_SUM_OVERDUE DOUBLE,
    #   CREDIT_TYPE STRING,
    #   DAYS_CREDIT_UPDATE INT,
    #   AMT_ANNUITY DOUBLE,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create bureau_balance table
    # CREATE TABLE IF NOT EXISTS home_credit.bureau_balance (
    #   SK_ID_BUREAU INT,
    #   MONTHS_BALANCE INT,
    #   STATUS STRING,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create credit_card_balance table
    # CREATE TABLE IF NOT EXISTS home_credit.credit_card_balance (
    #   SK_ID_PREV INT,
    #   SK_ID_CURR INT,
    #   MONTHS_BALANCE INT,
    #   AMT_BALANCE DOUBLE,
    #   AMT_CREDIT_LIMIT_ACTUAL DOUBLE,
    #   AMT_DRAWINGS_ATM_CURRENT DOUBLE,
    #   AMT_DRAWINGS_CURRENT DOUBLE,
    #   AMT_DRAWINGS_OTHER_CURRENT DOUBLE,
    #   AMT_DRAWINGS_POS_CURRENT DOUBLE,
    #   AMT_INST_MIN_REGULARITY DOUBLE,
    #   AMT_PAYMENT_CURRENT DOUBLE,
    #   AMT_PAYMENT_TOTAL_CURRENT DOUBLE,
    #   AMT_RECEIVABLE_PRINCIPAL DOUBLE,
    #   AMT_RECIVABLE DOUBLE,
    #   AMT_TOTAL_RECEIVABLE DOUBLE,
    #   CNT_DRAWINGS_ATM_CURRENT INT,
    #   CNT_DRAWINGS_CURRENT INT,
    #   CNT_DRAWINGS_OTHER_CURRENT INT,
    #   CNT_DRAWINGS_POS_CURRENT INT,
    #   CNT_INSTALMENT_MATURE_CUM INT,
    #   NAME_CONTRACT_STATUS STRING,
    #   SK_DPD INT,
    #   SK_DPD_DEF INT,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create installments_payments table
    # CREATE TABLE IF NOT EXISTS home_credit.installments_payments (
    #   SK_ID_PREV INT,
    #   SK_ID_CURR INT,
    #   NUM_INSTALMENT_VERSION DOUBLE,
    #   NUM_INSTALMENT_NUMBER INT,
    #   DAYS_INSTALMENT INT,
    #   DAYS_ENTRY_PAYMENT INT,
    #   AMT_INSTALMENT DOUBLE,
    #   AMT_PAYMENT DOUBLE,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create POS_CASH_balance table
    # CREATE TABLE IF NOT EXISTS home_credit.pos_cash_balance (
    #   SK_ID_PREV INT,
    #   SK_ID_CURR INT,
    #   MONTHS_BALANCE INT,
    #   CNT_INSTALMENT DOUBLE,
    #   CNT_INSTALMENT_FUTURE DOUBLE,
    #   NAME_CONTRACT_STATUS STRING,
    #   SK_DPD INT,
    #   SK_DPD_DEF INT,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));

    # -- Create previous_application table
    # CREATE TABLE IF NOT EXISTS home_credit.previous_application (
    #   SK_ID_PREV INT,
    #   SK_ID_CURR INT,
    #   NAME_CONTRACT_TYPE STRING,
    #   AMT_ANNUITY DOUBLE,
    #   AMT_APPLICATION DOUBLE,
    #   AMT_CREDIT DOUBLE,
    #   AMT_DOWN_PAYMENT DOUBLE,
    #   AMT_GOODS_PRICE DOUBLE,
    #   WEEKDAY_APPR_PROCESS_START STRING,
    #   HOUR_APPR_PROCESS_START INT,
    #   FLAG_LAST_APPL_PER_CONTRACT STRING,
    #   NFLAG_LAST_APPL_IN_DAY INT,
    #   RATE_DOWN_PAYMENT DOUBLE,
    #   RATE_INTEREST_PRIMARY DOUBLE,
    #   RATE_INTEREST_PRIVILEGED DOUBLE,
    #   NAME_CASH_LOAN_PURPOSE STRING,
    #   NAME_CONTRACT_STATUS STRING,
    #   DAYS_DECISION INT,
    #   NAME_PAYMENT_TYPE STRING,
    #   CODE_REJECT_REASON STRING,
    #   NAME_TYPE_SUITE STRING,
    #   NAME_CLIENT_TYPE STRING,
    #   NAME_GOODS_CATEGORY STRING,
    #   NAME_PORTFOLIO STRING,
    #   NAME_PRODUCT_TYPE STRING,
    #   CHANNEL_TYPE STRING,
    #   SELLERPLACE_AREA INT,
    #   NAME_SELLER_INDUSTRY STRING,
    #   CNT_PAYMENT DOUBLE,
    #   NAME_YIELD_GROUP STRING,
    #   PRODUCT_COMBINATION STRING,
    #   DAYS_FIRST_DRAWING DOUBLE,
    #   DAYS_FIRST_DUE DOUBLE,
    #   DAYS_LAST_DUE_1ST_VERSION DOUBLE,
    #   DAYS_LAST_DUE DOUBLE,
    #   DAYS_TERMINATION DOUBLE,
    #   NFLAG_INSURED_ON_APPROVAL DOUBLE,
    #   DATA_TIMESTAMP STRING,
    #   LOAD_DATE TIMESTAMP
    # ) USING iceberg
    # PARTITIONED BY (days(LOAD_DATE));
    # "@

    #     $createTablesSQL | docker exec -i spark-iceberg spark-sql
        
    #     # Check if tables were created successfully
    #     $checkTablesSQL = "SHOW TABLES IN home_credit;"
    #     $tablesResult = $checkTablesSQL | docker exec -i spark-iceberg spark-sql # Renamed to avoid conflict
    #     Write-Host "Tables in home_credit database:" -ForegroundColor Green
    #     Write-Host $tablesResult -ForegroundColor Green # Use the new variable name
    # }
    
    # Load data for each table
    $tableMap = @{
        "application_train_$date.csv" = "application_train"
        "application_test_$date.csv" = "application_test"
        "bureau_$date.csv" = "bureau"
        "bureau_balance_$date.csv" = "bureau_balance"
        "credit_card_balance_$date.csv" = "credit_card_balance"
        "installments_payments_$date.csv" = "installments_payments"
        "POS_CASH_balance_$date.csv" = "pos_cash_balance"
        "previous_application_$date.csv" = "previous_application"
    }
    
    $currentTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $tableCount = $tableMap.Count
    $tableCounter = 0
    
    foreach ($file in $tableMap.Keys) {
        $tableCounter++
        $progressPercent = [Math]::Min(25 + [Math]::Round(($tableCounter / $tableCount) * 70), 95)
        Write-Progress -Activity "Loading Daily Data" -Status "Loading table $tableCounter of $tableCount" -PercentComplete $progressPercent -CurrentOperation $tableMap[$file]
        $filePath = Join-Path $dateSpecificDir $file
        $tableName = $tableMap[$file]
        
        if (Test-Path $filePath) {
            Write-Host "Loading data from $file into $tableName table..." -ForegroundColor Cyan
            
            # Prepare SQL for loading data
            if (-not $appendOnly) {
                # Delete existing data for this date (if any)
                $deleteSQL = "DELETE FROM home_credit.$tableName WHERE DATA_TIMESTAMP = '$date';"
                $deleteSQL | docker exec -i spark-iceberg spark-sql
            }            # Prepare path with proper formatting for Linux
            # Use relative path from project root
            $absolutePath = (Resolve-Path $filePath).Path
            $projectRoot = (Resolve-Path "n:\Projects\task2").Path
            $relativePath = $absolutePath.Replace($projectRoot, "").TrimStart("\")
              # Format for Docker - assuming project is mounted at /data
            $dockerPath = "/data/$($relativePath.Replace("\", "/"))"
            
            # Log paths for debugging
            Write-Host "Windows path: $filePath" -ForegroundColor Gray
            Write-Host "Docker path: $dockerPath" -ForegroundColor Gray
            
            # Load data into table
            if ($optimizeLoading) {
                # Use optimized loading with partitioning and caching
                # Fix backtick syntax for file path
                $loadSQL = @"
-- Set Spark configurations for optimized loading
SET spark.sql.shuffle.partitions=8;
SET spark.sql.adaptive.enabled=true;
SET spark.sql.adaptive.coalescePartitions.enabled=true;
SET spark.sql.adaptive.skewJoin.enabled=true;
SET spark.sql.inMemoryColumnarStorage.compressed=true;
SET spark.sql.files.maxPartitionBytes=134217728;

-- Create a temporary view with the CSV data
CREATE OR REPLACE TEMPORARY VIEW temp_data AS
SELECT *, '$date' as DATA_TIMESTAMP, TIMESTAMP('$currentTimestamp') as LOAD_DATE
FROM csv.`$dockerPath`
OPTIONS ('header'='true', 'inferSchema'='true', 'mode'='PERMISSIVE', 'nullValue'='');

-- Cache the temporary table for better performance
CACHE TABLE temp_data_cached AS
SELECT * FROM temp_data;

-- Insert the data from the cached table
INSERT INTO home_credit.$tableName 
SELECT * FROM temp_data_cached;

-- Uncache the temporary tables
UNCACHE TABLE IF EXISTS temp_data_cached;
UNCACHE TABLE IF EXISTS temp_data;
"@            } else {
                # Standard loading approach
                $loadSQL = @"
-- Add DATA_TIMESTAMP column if reading from CSV
INSERT INTO home_credit.$tableName 
SELECT *, '$date' as DATA_TIMESTAMP, TIMESTAMP('$currentTimestamp') as LOAD_DATE
FROM csv.`$dockerPath`
OPTIONS ('header'='true', 'inferSchema'='true', 'mode'='PERMISSIVE', 'nullValue'='');
"@
            }
            
            Write-Host "Loading data from $file into $tableName table..." -ForegroundColor Cyan
            $loadSQL | docker exec -i spark-iceberg spark-sql
            
            # Check row count
            $countSQL = "SELECT COUNT(*) FROM home_credit.$tableName WHERE DATA_TIMESTAMP = '$date';"
            $rowCount = $countSQL | docker exec -i spark-iceberg spark-sql
            Write-Host "Loaded $rowCount rows into $tableName for date $date" -ForegroundColor Green
        } else {
            Write-Host "File not found: $filePath - Skipping load for $tableName" -ForegroundColor Yellow
        }
    }
      Write-Host "Data load completed for $date" -ForegroundColor Cyan
    Write-Progress -Activity "Loading Daily Data" -Status "Completed" -PercentComplete 100
    return $true
}

# Main script execution
$startTime = Get-Date
Write-Host "Starting daily data load for date: $date" -ForegroundColor Cyan
Write-Host "Using optimized loading: $optimizeLoading" -ForegroundColor Cyan

# Load data
$success = Load-DailyData -date $date -dailyDir $dailyDir -createTables:$createTables -appendOnly:$appendOnly -optimizeLoading:$optimizeLoading

# Log completion and duration
$endTime = Get-Date
$duration = $endTime - $startTime
Write-Host "Daily data load $($success ? 'completed successfully' : 'failed') for $date" -ForegroundColor ($success ? 'Green' : 'Red')
Write-Host "Duration: $($duration.Minutes) minutes $($duration.Seconds) seconds (total $($duration.TotalSeconds) seconds)" -ForegroundColor Cyan
