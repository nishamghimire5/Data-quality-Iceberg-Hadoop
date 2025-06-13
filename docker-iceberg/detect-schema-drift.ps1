#!/bin/pwsh

# Script to detect schema drift in Iceberg tables
# This script compares current schema with historical schema and logs changes

# Parameters
param (
    [string]$configFile = "n:\Projects\task2\docker-iceberg\dq-config.json",
    [string]$schemaHistoryDir = "n:\Projects\task2\docker-iceberg\schema_history"
)

# Ensure schema history directory exists
if (-not (Test-Path $schemaHistoryDir)) {
    New-Item -Path $schemaHistoryDir -ItemType Directory -Force
    Write-Host "Created schema history directory: $schemaHistoryDir"
}

# Read configuration
$config = Get-Content $configFile -Raw | ConvertFrom-Json
$tables = $config.tables
$schemaDriftConfig = $config.schemaDrift

# Function to get current schema for a table
function Get-TableSchema {
    param (
        [string]$tableName
    )
    
    $schemaQuery = "DESCRIBE TABLE home_credit.$tableName"
    $schemaOutput = $schemaQuery | docker exec -i spark-iceberg spark-sql
    
    # Parse the output to get column details
    $columns = @()
    $schemaLines = $schemaOutput -split "`n" | Select-Object -Skip 1  # Skip header
    
    foreach ($line in $schemaLines) {
        if ($line -match '^\|?\s*(\S+)\s*\|?\s*(\S+)') {
            $columnName = $Matches[1].Trim()
            $dataType = $Matches[2].Trim()
            
            # Skip if we've reached the schema footer (e.g., # Partitioning)
            if ($columnName -match '^#') {
                break
            }
            
            if ($columnName -and $dataType) {
                $columns += [PSCustomObject]@{
                    Name = $columnName
                    Type = $dataType
                }
            }
        }
    }
    
    return $columns
}

# Function to save schema to history
function Save-SchemaHistory {
    param (
        [string]$tableName,
        [array]$schema
    )
    
    $date = Get-Date -Format "yyyyMMdd"
    $schemaFile = Join-Path $schemaHistoryDir "$tableName-$date.json"
    
    $schema | ConvertTo-Json | Out-File $schemaFile -Encoding utf8
    Write-Host "Saved schema history for $tableName to $schemaFile"
}

# Function to get last saved schema
function Get-LastSchema {
    param (
        [string]$tableName
    )
    
    $schemaFiles = Get-ChildItem -Path $schemaHistoryDir -Filter "$tableName-*.json" | Sort-Object Name -Descending
    
    if ($schemaFiles.Count -gt 0) {
        $lastSchemaFile = $schemaFiles[0].FullName
        $lastSchema = Get-Content $lastSchemaFile -Raw | ConvertFrom-Json
        return $lastSchema
    }
    
    return $null
}

# Function to compare schemas and detect drift
function Compare-Schemas {
    param (
        [string]$tableName,
        [array]$currentSchema,
        [array]$previousSchema
    )
    
    $changes = @()
    
    # Check for added columns
    if ($schemaDriftConfig.trackAddedColumns) {
        $currentColumns = $currentSchema | ForEach-Object { $_.Name }
        $previousColumns = $previousSchema | ForEach-Object { $_.Name }
        
        $addedColumns = $currentColumns | Where-Object { $_ -notin $previousColumns }
        foreach ($column in $addedColumns) {
            $columnDef = $currentSchema | Where-Object { $_.Name -eq $column }
            $changes += [PSCustomObject]@{
                Type = "ADDED_COLUMN"
                Table = $tableName
                Column = $column
                DataType = $columnDef.Type
                Message = "Column '$column' of type '$($columnDef.Type)' was added to table '$tableName'"
            }
        }
    }
    
    # Check for dropped columns
    if ($schemaDriftConfig.trackDroppedColumns) {
        $currentColumns = $currentSchema | ForEach-Object { $_.Name }
        $previousColumns = $previousSchema | ForEach-Object { $_.Name }
        
        $droppedColumns = $previousColumns | Where-Object { $_ -notin $currentColumns }
        foreach ($column in $droppedColumns) {
            $columnDef = $previousSchema | Where-Object { $_.Name -eq $column }
            $changes += [PSCustomObject]@{
                Type = "DROPPED_COLUMN"
                Table = $tableName
                Column = $column
                DataType = $columnDef.Type
                Message = "Column '$column' of type '$($columnDef.Type)' was dropped from table '$tableName'"
            }
        }
    }
    
    # Check for type changes
    if ($schemaDriftConfig.trackTypeChanges) {
        $commonColumns = $currentSchema | Where-Object { $previousSchema.Name -contains $_.Name }
        
        foreach ($column in $commonColumns) {
            $previousDef = $previousSchema | Where-Object { $_.Name -eq $column.Name }
            
            if ($column.Type -ne $previousDef.Type) {
                $changes += [PSCustomObject]@{
                    Type = "TYPE_CHANGE"
                    Table = $tableName
                    Column = $column.Name
                    OldType = $previousDef.Type
                    NewType = $column.Type
                    Message = "Column '$($column.Name)' in table '$tableName' changed type from '$($previousDef.Type)' to '$($column.Type)'"
                }
            }
        }
    }
    
    return $changes
}

# Function to log changes to file and optionally create GitHub issues
function Log-Changes {
    param (
        [array]$changes
    )
    
    if ($changes.Count -eq 0) {
        Write-Host "No schema changes detected." -ForegroundColor Green
        return
    }
    
    $logFile = Join-Path $schemaHistoryDir "schema_changes_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    
    "Schema Changes Detected at $(Get-Date)" | Out-File $logFile -Encoding utf8
    "===================================" | Out-File $logFile -Encoding utf8 -Append
    
    foreach ($change in $changes) {
        $change.Message | Out-File $logFile -Encoding utf8 -Append
    }
    
    Write-Host "Logged $($changes.Count) schema changes to $logFile" -ForegroundColor Yellow
    
    # TODO: Implement GitHub issue creation if enabled in config
    if ($config.notifications.github.enabled) {
        Write-Host "GitHub issue creation is enabled, but not implemented in this script yet." -ForegroundColor Yellow
        Write-Host "Changes would be posted to: $($config.notifications.github.repository)" -ForegroundColor Yellow
    }
    
    # TODO: Implement email notification if enabled in config
    if ($config.notifications.email.enabled) {
        Write-Host "Email notification is enabled, but not implemented in this script yet." -ForegroundColor Yellow
        Write-Host "Notifications would be sent to: $($config.notifications.email.recipients -join ', ')" -ForegroundColor Yellow
    }
}

# Main script logic
Write-Host "Starting schema drift detection..." -ForegroundColor Cyan

$allChanges = @()

foreach ($table in $tables) {
    $tableName = $table.name
    Write-Host "Processing table: $tableName" -ForegroundColor Cyan
    
    # Get current schema
    $currentSchema = Get-TableSchema -tableName $tableName
    
    # Save current schema to history
    Save-SchemaHistory -tableName $tableName -schema $currentSchema
    
    # Get previous schema (if exists)
    $previousSchema = Get-LastSchema -tableName $tableName
    
    if ($previousSchema) {
        # Compare schemas to detect drift
        $changes = Compare-Schemas -tableName $tableName -currentSchema $currentSchema -previousSchema $previousSchema
        $allChanges += $changes
        
        if ($changes.Count -gt 0) {
            Write-Host "Detected $($changes.Count) schema changes for table $tableName" -ForegroundColor Yellow
        } else {
            Write-Host "No schema changes detected for table $tableName" -ForegroundColor Green
        }
    } else {
        Write-Host "No previous schema found for table $tableName. This is the first run." -ForegroundColor Yellow
    }
}

# Log all changes
Log-Changes -changes $allChanges

Write-Host "Schema drift detection completed." -ForegroundColor Cyan
