# Data Quality Assurance System with Apache Iceberg on Hadoop

This project implements a comprehensive data quality assurance system using Apache Iceberg on top of Hadoop infrastructure. The system processes financial data from the Home Credit Default Risk dataset and performs automated data quality checks to ensure data integrity, freshness, and compliance.

## 🎯 Project Overview

**Task Scope (as per supervisor requirements):**
1. **Data source**: ICEberg on top of HDFS  
2. **Daily data simulation**: Script that simulates data added everyday
3. **SQL-native DQ checks**: Configuration-driven quality checks with alerting
4. **Data validation**: Volume monitoring, NULL value checks, min/max range validation
5. **Automated monitoring**: Comprehensive DQ monitoring system

## 📊 Dataset Information

**Source**: Home Credit Default Risk Dataset (Kaggle)
- **Main Tables**: 8 financial data tables with relationships
- **Records**: ~300K+ loan applications with historical credit data
- **Key Tables**:
  - `application_train/test` - Main loan applications (30K+ records)
  - `bureau` - External credit history (172K+ records) 
  - `bureau_balance` - Monthly credit balances (2.7M+ records)
  - `credit_card_balance` - Credit card history (384K+ records)
  - `installments_payments` - Payment history (1.3M+ records)
  - `pos_cash_balance` - POS loan balances (999K+ records)
  - `previous_application` - Previous applications (167K+ records)

## 🏗️ Architecture

```
┌─────────────────┬─────────────────┬─────────────────┐
│   Data Source   │   Processing    │   Storage       │
├─────────────────┼─────────────────┼─────────────────┤
│ CSV Files       │ Apache Spark    │ Apache Iceberg  │
│ (Daily Batches) │ PySpark Jobs    │ on Hadoop HDFS  │
└─────────────────┴─────────────────┴─────────────────┘
                            │
                   ┌─────────────────┐
                   │  Data Quality   │
                   │     System      │
                   │  (DQ Checks)    │
                   └─────────────────┘
```

**Infrastructure Components:**
- **Hadoop Cluster**: HDFS storage with NameNode, DataNode, ResourceManager
- **Apache Iceberg**: Modern table format with ACID transactions and schema evolution
- **Apache Spark**: Distributed processing engine for data loading and DQ checks
- **MinIO**: S3-compatible object storage for Iceberg metadata
- **REST Catalog**: Iceberg catalog service for table management

## 🔄 Data Processing Workflow

### 1. **Data Preparation** (`prepare-daily-data.ps1`)
- **Purpose**: Simulates daily data ingestion by sampling from source dataset
- **Process**: 
  - Takes 1% sample from original CSV files (configurable)
  - Adds `DATA_TIMESTAMP` column for temporal tracking
  - Creates date-specific directories (`daily/YYYYMMDD/`)
  - Generates 8 daily files with naming pattern: `table_YYYYMMDD.csv`
- **Performance**: Fast processing using .NET StreamReader (processes 2.7M records in ~30 seconds)

### 2. **Data Loading** (`load-daily-data.ps1`)
- **Purpose**: Loads daily CSV files into Iceberg tables
- **Process**:
  - Validates file existence and directory structure
  - Uses PySpark with optimized configurations (adaptive query execution, partitioning)
  - Adds `LOAD_DATE` timestamp for audit trail
  - Supports append-only or replace modes
  - Uses proper SQL syntax with backticks for file paths
- **Features**: 
  - Docker path conversion (Windows → Linux)
  - Row count validation
  - Progress tracking and logging

### 3. **Master Pipeline** (`run-pipeline.ps1`)
- **Purpose**: Orchestrates the complete data quality workflow
- **Steps**:
  1. Service health checks (Hadoop/Iceberg containers)
  2. Data preparation (if not skipped)
  3. Data loading into Iceberg tables  
  4. Data quality checks execution
- **Configuration**: Supports skip flags, sample percentages, optimization toggles

## 🔍 Data Quality Checks System

### Configuration-Driven Approach (`dq-config.json`)
The DQ system uses JSON configuration to define checks per table:

```json
{
  "tables": [
    {
      "name": "application_train",
      "checks": [
        {"type": "freshness", "maxAgeDays": 1, "alertLevel": "high"},
        {"type": "nullCheck", "columns": ["SK_ID_CURR"], "alertLevel": "high"},
        {"type": "valueRange", "column": "AMT_INCOME_TOTAL", "min": 0, "max": 10000000},
        {"type": "volumeCheck", "minRowCount": 100, "alertLevel": "medium"}
      ]
    }
  ]
}
```

### Check Types Implemented:

1. **Freshness Checks**
   - Validates data age against `LOAD_DATE` column
   - Alerts if data older than specified threshold (24 hours)
   - Critical for ensuring timely data updates

2. **NULL Value Validation**
   - Checks null percentages in critical columns
   - Configurable threshold per column
   - Essential for data completeness validation

3. **Range Validation**
   - Validates numeric values within business rules
   - Min/max boundaries for financial amounts
   - Detects outliers and data entry errors

4. **Volume Monitoring**
   - Ensures minimum row counts per table
   - Detects data loading failures
   - Monitors data volume trends

### DQ Execution Process (`run-dq-checks-pyspark.ps1`)
1. **Setup**: Copies configuration and Python scripts to Spark container
2. **Execution**: Runs PySpark job with Iceberg packages
3. **Processing**: Executes all configured checks using SQL queries
4. **Results**: Generates CSV and Parquet outputs for each check type
5. **Reporting**: Creates clean text report with status summary

### Report Generation (`generate_clean_report.py`)
- **Input**: CSV files from DQ checks (freshness, null, range, volume)
- **Output**: Human-readable text report with:
  - Table-wise check results
  - Status indicators (✅ OK, 🚨 ALERT)
  - Alert summary with details
  - Executive summary with total check counts

## 📁 Directory Structure

```
docker-iceberg/
├── run-pipeline.ps1              # Master orchestration script
├── prepare-daily-data.ps1        # Daily data simulation
├── load-daily-data.ps1          # Data loading into Iceberg
├── run-dq-checks-pyspark.ps1    # DQ checks execution
├── run_dq_checks_fixed.py       # Python DQ implementation
├── generate_clean_report.py     # Report generation
├── dq-config.json              # DQ checks configuration
├── dq-results/                 # DQ check outputs
├── warehouse/                  # Iceberg warehouse
└── docker-compose.yml          # Infrastructure setup
```

## 🚀 Quick Start Guide

### Prerequisites
- Docker and Docker Compose installed
- PowerShell Core (pwsh)
- Python 3.x with required packages

### 1. Setup Infrastructure
```powershell
# Start Hadoop cluster
cd docker-hadoop
docker-compose up -d

# Start Iceberg services  
cd docker-iceberg
docker-compose up -d

# Initialize tables and directories
.\setup.ps1
```

### 2. Run Complete Pipeline
```powershell
# Execute full workflow (data prep → loading → DQ checks)
.\run-pipeline.ps1 -verbose

# Or run individual steps
.\prepare-daily-data.ps1 -date 20250613    # Prepare daily data
.\load-daily-data.ps1 -date 20250613       # Load into Iceberg
.\run-dq-checks-pyspark.ps1 -verbose       # Run DQ checks
```

### 3. View Results
- **DQ Reports**: `dq-results/dq_report_TIMESTAMP.txt`
- **Raw Data**: `dq-results/*.csv` and `dq-results/*.parquet`
- **Iceberg Tables**: Accessible via Spark SQL at `home_credit.*`

## 📈 Sample Results

**Recent DQ Check Summary:**
- **Total Checks**: 54 (8 Freshness + 25 NULL + 13 Range + 8 Volume)
- **Status**: 49 ✅ PASSED, 5 🚨 ALERTS
- **Key Findings**:
  - All data fresh (loaded 2025-06-13)
  - Zero null values in critical columns
  - 5 range alerts for high-value financial transactions
  - All volume checks passed

## 🛠️ Configuration Options

### Data Processing
- `$samplePercentage`: Control data volume (1% = ~30K records)
- `$useFastMethod`: Enable optimized processing
- `$optimizeLoading`: Use Spark optimizations

### DQ Checks
- `maxAgeDays`: Freshness threshold
- `maxNullPercentage`: NULL tolerance
- `min/max`: Range boundaries
- `minRowCount`: Volume thresholds

## 🎯 Business Value

1. **Data Quality Assurance**: Automated validation ensures reliable financial data
2. **Early Problem Detection**: Real-time alerts for data issues
3. **Compliance**: Auditable data lineage and quality metrics
4. **Scalability**: Handles millions of records efficiently
5. **Monitoring**: Continuous quality tracking with historical trends

## 📋 Technical Notes

- **Performance**: Optimized for large datasets (2.7M+ records processed in minutes)
- **Reliability**: ACID transactions via Iceberg, fault-tolerant Spark processing
- **Flexibility**: JSON-driven configuration, modular PowerShell scripts
- **Monitoring**: Comprehensive logging and progress tracking
- **Compatibility**: Windows-based development with Linux container execution

## 🔧 Command Reference

### Core Commands
```powershell
# Full pipeline execution
.\run-pipeline.ps1 -verbose

# Individual components
.\prepare-daily-data.ps1 -date 20250613 -samplePercentage 2
.\load-daily-data.ps1 -date 20250613 -optimizeLoading
.\run-dq-checks-pyspark.ps1 -verbose

# Report generation
python generate_clean_report.py
```

### Docker Operations
```powershell
# Check service status
docker ps

# View logs
docker logs spark-iceberg
docker logs namenode

# Access Spark SQL
docker exec -it spark-iceberg spark-sql
```

### Web Interfaces
- **Spark UI**: http://localhost:8080
- **Hadoop NameNode**: http://localhost:9870
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Iceberg REST**: http://localhost:8181

---

*This system successfully implements requirements 1-5 of the Data Quality Assurance task, providing a robust foundation for financial data processing and quality monitoring.*
