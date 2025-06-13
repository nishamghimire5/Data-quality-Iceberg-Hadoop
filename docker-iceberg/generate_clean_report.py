#!/usr/bin/env python3
"""
Generate a clean, readable text report from DQ check CSV results
"""

import pandas as pd
import glob
import os
from datetime import datetime
import sys

def generate_clean_text_report(results_dir, output_file=None):
    """Generate a clean, vertical-format text report from CSV files"""
    
    # Find the latest set of CSV files
    csv_files = glob.glob(os.path.join(results_dir, "*_*.csv"))
    
    if not csv_files:
        print("No CSV files found in the results directory.")
        return None
    
    # Group files by timestamp
    file_groups = {}
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        # Extract timestamp from filename (e.g., dq_report_20250613_062034_freshness.csv)
        parts = filename.split('_')
        if len(parts) >= 4:
            timestamp = f"{parts[2]}_{parts[3]}"
            check_type = parts[4].replace('.csv', '')
            
            if timestamp not in file_groups:
                file_groups[timestamp] = {}
            file_groups[timestamp][check_type] = file_path
    
    # Use the latest timestamp
    latest_timestamp = max(file_groups.keys())
    latest_files = file_groups[latest_timestamp]
    
    print(f"Generating report for timestamp: {latest_timestamp}")
    
    # Start building the report
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("DATA QUALITY CHECK REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Report Timestamp: {latest_timestamp}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    total_alerts = 0
    alert_details = []
    
    # Process each check type
    check_types = [
        ('freshness', 'FRESHNESS CHECKS', 'freshness_status'),
        ('null', 'NULL VALUE CHECKS', 'null_check_status'),
        ('range', 'RANGE CHECKS', 'range_check_status'),
        ('volume', 'VOLUME CHECKS', 'volume_check_status')
    ]
    
    for check_key, title, status_col in check_types:
        if check_key in latest_files:
            # Find the actual CSV file inside the directory
            csv_dir = latest_files[check_key]
            csv_files_in_dir = glob.glob(os.path.join(csv_dir, "*.csv"))
            
            if csv_files_in_dir:
                csv_file = csv_files_in_dir[0]  # Take the first (and usually only) CSV file
                
                try:
                    df = pd.read_csv(csv_file)
                    
                    report_lines.append(f"\n{title}")
                    report_lines.append("-" * 50)
                    
                    for _, row in df.iterrows():
                        report_lines.append("")
                        report_lines.append(f"📊 Table: {row['table_name']}")
                        
                        if check_key == 'freshness':
                            report_lines.append(f"   Last Updated: {row['last_updated']}")
                            report_lines.append(f"   Check Time: {row['check_time']}")
                            status = row[status_col]
                        elif check_key == 'null':
                            report_lines.append(f"   Column: {row['column_name']}")
                            report_lines.append(f"   Total Rows: {row['total_rows']:,}")
                            report_lines.append(f"   Null Count: {row['null_count']:,}")
                            report_lines.append(f"   Null Percentage: {row['null_percentage']}%")
                            status = row[status_col]
                        elif check_key == 'range':
                            report_lines.append(f"   Column: {row['column_name']}")
                            report_lines.append(f"   Min Value: {row['min_value']:,.2f}")
                            report_lines.append(f"   Max Value: {row['max_value']:,.2f}")
                            report_lines.append(f"   Average: {row['avg_value']:,.2f}")
                            report_lines.append(f"   Total Rows: {row['total_rows']:,}")
                            report_lines.append(f"   Out of Range: {row['out_of_range_count']:,} ({row['out_of_range_percentage']}%)")
                            status = row[status_col]
                        elif check_key == 'volume':
                            report_lines.append(f"   Row Count: {row['row_count']:,}")
                            status = row[status_col]
                        
                        # Status with emoji indicators
                        if "ALERT" in status:
                            report_lines.append(f"   🚨 Status: {status}")
                            report_lines.append(f"   Alert Level: {row['alert_level'].upper()}")
                            total_alerts += 1
                            
                            # Add to alert details
                            table = row['table_name']
                            level = row['alert_level']
                            if 'column_name' in row and pd.notna(row['column_name']):
                                column = row['column_name']
                                alert_details.append(f"  • {table}.{column}: {status} [{level}]")
                            else:
                                alert_details.append(f"  • {table}: {status} [{level}]")
                        else:
                            report_lines.append(f"   ✅ Status: {status}")
                            report_lines.append(f"   Alert Level: {row['alert_level']}")
                    
                except Exception as e:
                    report_lines.append(f"\nError reading {check_key} results: {str(e)}")
    
    # Summary section
    report_lines.append("\n" + "=" * 80)
    report_lines.append("SUMMARY")
    report_lines.append("=" * 80)
    
    # Count checks by type
    check_counts = {}
    for check_key, title, status_col in check_types:
        if check_key in latest_files:
            csv_dir = latest_files[check_key]
            csv_files_in_dir = glob.glob(os.path.join(csv_dir, "*.csv"))
            if csv_files_in_dir:
                try:
                    df = pd.read_csv(csv_files_in_dir[0])
                    check_counts[title.split()[0]] = len(df)
                except:
                    check_counts[title.split()[0]] = 0
    
    for check_type, count in check_counts.items():
        report_lines.append(f"{check_type} Checks: {count}")
    
    total_checks = sum(check_counts.values())
    report_lines.append(f"Total Checks: {total_checks}")
    
    # Alert summary
    if total_alerts > 0:
        report_lines.append(f"\n🚨 ATTENTION: {total_alerts} alerts require review")
        report_lines.append("\nAlert Details:")
        report_lines.extend(alert_details)
    else:
        report_lines.append(f"\n✅ No alerts - all data quality checks passed!")
    
    report_lines.append("\n" + "=" * 80)
      # Write the report
    if output_file is None:
        output_file = os.path.join(results_dir, f"dq_report_{latest_timestamp}.txt")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"Clean text report generated: {output_file}")
    return output_file

if __name__ == "__main__":
    # Default to the dq-results directory
    results_dir = "n:/Projects/task2/docker-iceberg/dq-results"
    
    if len(sys.argv) > 1:
        results_dir = sys.argv[1]
    
    generate_clean_text_report(results_dir)
