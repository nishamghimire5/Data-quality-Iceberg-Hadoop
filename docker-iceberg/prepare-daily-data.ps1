#!/bin/pwsh

# This script prepares and simulates daily data loads into the system
# It takes data from the main dataset files and creates daily snapshots with significantly improved performance

# Parameters
param (
    [string]$date = (Get-Date -Format "yyyyMMdd"),
    [string]$sourceDir = "n:\Projects\task2\home-credit-default-risk-dataset",
    [string]$dailyDir = "n:\Projects\task2\home-credit-default-risk-dataset\daily",
    [int]$samplePercentage = 1, # Take 1% of records by default for faster processing
    [switch]$addTimestamp = $true, # Add timestamp column to data
    [switch]$useFastMethod = $true # Use the fast method for processing
)

# Ensure daily directory exists
$dateSpecificDir = Join-Path $dailyDir $date
if (-not (Test-Path $dateSpecificDir)) {
    New-Item -Path $dateSpecificDir -ItemType Directory -Force
    Write-Host "Created directory: $dateSpecificDir"
}

# Function to process a sample of records from source files using the fast method
function Process-DataSample-Fast {
    param (
        [string]$sourceFile,
        [string]$targetFile,
        [int]$samplePercentage = 1,  # Take 1% of records by default
        [switch]$addNoise = $false,   # Whether to add noise to numeric columns
        [switch]$addTimestamp = $true # Add timestamp column
    )
    
    Write-Host "Processing $sourceFile -> $targetFile using fast method"
    
    # Check if source file exists
    if (-not (Test-Path $sourceFile)) {
        Write-Host "Source file not found: $sourceFile" -ForegroundColor Red
        return
    }
    
    # Use .NET StreamReader for fast file processing
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Progress -Activity "Preparing Daily Data" -Status "Processing $sourceFile" -PercentComplete 5
    
    # Get the header first
    $reader = New-Object System.IO.StreamReader $sourceFile
    $header = $reader.ReadLine()
    if ($addTimestamp) {
        $header = "$header,DATA_TIMESTAMP"
    }
    
    # Set up writer for target file
    $writer = New-Object System.IO.StreamWriter $targetFile
    $writer.WriteLine($header)
    
    # Process the rest of the file with sampling
    $random = New-Object System.Random
    $lineCount = 0
    $includedCount = 0
    
    while (($line = $reader.ReadLine()) -ne $null) {
        $lineCount++
        
        # Show progress every 50,000 lines
        if ($lineCount % 50000 -eq 0) {
            $percentComplete = [Math]::Min(5 + [Math]::Round(($lineCount / 1000000) * 90), 95)
            Write-Progress -Activity "Preparing Daily Data" -Status "Processing $sourceFile" -PercentComplete $percentComplete -CurrentOperation "Processed $lineCount lines, included $includedCount"
        }
        
        # Determine if we should include this line based on sample percentage
        if ($random.Next(100) -lt $samplePercentage) {
            $processedLine = $line
            
            if ($addNoise) {
                # Add some noise to numeric values to simulate changing data (optimized)
                $fields = $line -split ','
                for ($i = 0; $i -lt $fields.Length; $i++) {
                    $field = $fields[$i]
                    if ([double]::TryParse($field, [ref]$null)) {
                        # It's a number, add some noise (±5%)
                        $number = [double]$field
                        $noise = $random.NextDouble() * 0.1 - 0.05  # -5% to +5%
                        $newValue = $number * (1 + $noise)
                        $fields[$i] = [string]$newValue
                    }
                }
                $processedLine = $fields -join ','
            }
            
            if ($addTimestamp) {
                # Add timestamp to the end of the line
                $processedLine = "$processedLine,$date"
            }
            
            $writer.WriteLine($processedLine)
            $includedCount++
        }
    }
    
    # Close the reader and writer
    $reader.Close()
    $writer.Close()
    
    $stopwatch.Stop()
    Write-Progress -Activity "Preparing Daily Data" -Status "Completed processing $sourceFile" -PercentComplete 100
    Write-Host "Created $targetFile with $includedCount data rows in $($stopwatch.Elapsed.TotalSeconds) seconds" -ForegroundColor Green
}

# Legacy function kept for compatibility
function Process-DataSample {
    param (
        [string]$sourceFile,
        [string]$targetFile,
        [int]$samplePercentage = 10,  # Take 10% of records by default
        [switch]$addNoise = $false,   # Whether to add noise to numeric columns
        [switch]$addTimestamp = $true # Add timestamp column
    )
    
    Write-Host "Processing $sourceFile -> $targetFile using standard method"
    
    # Check if source file exists
    if (-not (Test-Path $sourceFile)) {
        Write-Host "Source file not found: $sourceFile" -ForegroundColor Red
        return
    }
    
    # Read header and determine total number of lines
    Write-Progress -Activity "Preparing Daily Data" -Status "Analyzing $sourceFile" -PercentComplete 10
    $header = Get-Content $sourceFile -TotalCount 1
    
    # Instead of reading the whole file to count lines, use a more efficient approach
    $lineCount = 0
    $reader = New-Object System.IO.StreamReader($sourceFile)
    while ($reader.ReadLine() -ne $null) { $lineCount++ }
    $reader.Close()
    
    Write-Host "File has $lineCount lines, sampling at $samplePercentage%"
    
    # Select random lines more efficiently - don't create a huge array
    $random = New-Object System.Random
    
    # If adding timestamp, modify the header
    if ($addTimestamp) {
        $header = "$header,DATA_TIMESTAMP"
    }
    
    # Process the file line by line with a more efficient approach
    $content = [System.Collections.ArrayList]::new()
    [void]$content.Add($header)
    
    $reader = New-Object System.IO.StreamReader($sourceFile)
    $reader.ReadLine() # Skip header as we've already processed it
    
    $processedLines = 1 # Count the header
    for ($i = 1; $i -lt $lineCount; $i++) {
        $line = $reader.ReadLine()
        if ($line -eq $null) { break }
        
        if ($i % 10000 -eq 0) {
            $percentComplete = [Math]::Min(10 + [Math]::Round(($i / $lineCount) * 80), 90)
            Write-Progress -Activity "Preparing Daily Data" -Status "Processing $sourceFile" -PercentComplete $percentComplete -CurrentOperation "Line $i of $lineCount"
        }
        
        # Use random sampling rather than building a huge array of line numbers to skip
        if ($random.Next(100) -lt $samplePercentage) {
            $processedLine = $line
            
            if ($addNoise) {
                # Add some noise to numeric values to simulate changing data
                $fields = $line -split ','
                $newFields = @()
                foreach ($field in $fields) {
                    if ([double]::TryParse($field, [ref]$null)) {
                        # It's a number, add some noise (±5%)
                        $number = [double]$field
                        $noise = $random.NextDouble() * 0.1 - 0.05  # -5% to +5%
                        $newValue = $number * (1 + $noise)
                        $newFields += [string]$newValue
                    } else {
                        $newFields += $field
                    }
                }
                $processedLine = $newFields -join ','
            }
            
            if ($addTimestamp) {
                # Add timestamp to the end of the line
                $processedLine = "$processedLine,$date"
            }
            
            [void]$content.Add($processedLine)
            $processedLines++
        }
    }
    
    $reader.Close()
    
    # Write to target file - Use .NET methods for better performance
    $writer = New-Object System.IO.StreamWriter($targetFile)
    foreach ($line in $content) {
        $writer.WriteLine($line)
    }
    $writer.Close()
    
    Write-Progress -Activity "Preparing Daily Data" -Status "Completed processing $sourceFile" -PercentComplete 100
    Write-Host "Created $targetFile with $($processedLines - 1) data rows" -ForegroundColor Green
}

# Process all files in the dataset
$filesToProcess = @(
    @{Source = "application_train.csv"; Target = "application_train_$date.csv"; AddNoise = $true},
    @{Source = "application_test.csv"; Target = "application_test_$date.csv"; AddNoise = $true},
    @{Source = "bureau.csv"; Target = "bureau_$date.csv"; AddNoise = $true},
    @{Source = "bureau_balance.csv"; Target = "bureau_balance_$date.csv"; AddNoise = $true},
    @{Source = "credit_card_balance.csv"; Target = "credit_card_balance_$date.csv"; AddNoise = $true},
    @{Source = "installments_payments.csv"; Target = "installments_payments_$date.csv"; AddNoise = $true},
    @{Source = "POS_CASH_balance.csv"; Target = "POS_CASH_balance_$date.csv"; AddNoise = $true},
    @{Source = "previous_application.csv"; Target = "previous_application_$date.csv"; AddNoise = $true}
    # HomeCredit_columns_description.csv and sample_submission.csv are reference files, not data files
)

# Log start time for performance tracking
$startTime = Get-Date
Write-Host "Starting data preparation for date: $date" -ForegroundColor Cyan
Write-Host "Taking $samplePercentage% sample from each file" -ForegroundColor Cyan
Write-Host "Using $(if ($useFastMethod) { 'fast' } else { 'standard' }) processing method" -ForegroundColor Cyan

# Process each file
$totalFiles = $filesToProcess.Count
$fileCounter = 0
foreach ($file in $filesToProcess) {    $fileCounter++
    $progressPercent = [Math]::Round(($fileCounter / $totalFiles) * 100)
    Write-Progress -Activity "Processing Data Files" -Status "File $fileCounter of $totalFiles" -PercentComplete $progressPercent -Id 1 -CurrentOperation $file.Source
    
    $sourcePath = Join-Path $sourceDir $file.Source
    $targetPath = Join-Path $dateSpecificDir $file.Target
    
    if ($useFastMethod) {
        Process-DataSample-Fast -sourceFile $sourcePath -targetFile $targetPath -samplePercentage $samplePercentage -addNoise:$file.AddNoise -addTimestamp:$addTimestamp
    } else {
        Process-DataSample -sourceFile $sourcePath -targetFile $targetPath -samplePercentage $samplePercentage -addNoise:$file.AddNoise -addTimestamp:$addTimestamp
    }
}

# Complete progress
Write-Progress -Activity "Processing Data Files" -Completed -Id 1

# Log completion and duration
$endTime = Get-Date
$duration = $endTime - $startTime
Write-Host "Daily data preparation complete for $date" -ForegroundColor Cyan
Write-Host "Duration: $($duration.Minutes) minutes $($duration.Seconds) seconds (total $($duration.TotalSeconds) seconds)" -ForegroundColor Cyan
Write-Host "Files generated: $($filesToProcess.Count)" -ForegroundColor Cyan
Write-Host "Output directory: $dateSpecificDir" -ForegroundColor Cyan
