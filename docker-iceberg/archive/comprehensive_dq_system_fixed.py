#!/usr/bin/env python3
"""
Comprehensive Data Quality System - Fixed Version
Generates detailed reports for ALL columns across ALL 8 Home Credit tables
"""

import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, stddev, desc, length, abs as spark_abs
from pyspark.sql.types import *
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveDQSystem:
    """Comprehensive Data Quality System with detailed column analysis"""
    
    def __init__(self):
        """Initialize the comprehensive DQ system"""
        self.spark = self.init_spark()
        self.results_dir = "dq-results"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # All 8 Home Credit tables
        self.tables = [
            "application_train_daily", "application_test_daily", 
            "bureau_daily", "bureau_balance_daily",
            "credit_card_balance_daily", "installments_payments_daily", 
            "POS_CASH_balance_daily", "previous_application_daily"
        ]
        
        logger.info("🚀 Comprehensive DQ System initialized for detailed column analysis")

    def init_spark(self):
        """Initialize Spark session"""
        spark = SparkSession.builder \
            .appName("ComprehensiveDQSystem") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        return spark

    def get_comprehensive_table_analysis(self, table_name):
        """Get comprehensive analysis for a single table"""
        logger.info(f"🔍 Analyzing table: {table_name}")
        
        try:
            # Read the table data from HDFS
            table_path = f"hdfs://namenode:9000/warehouse/{table_name}"
            df = self.spark.read.parquet(table_path)
            
            # Get basic info
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Analyze each column
            column_analyses = []
            
            for col_name in df.columns:
                # Skip metadata columns
                if col_name in ['ingestion_date', 'ingestion_timestamp', 'data_source', 'batch_id', 'simulation_id']:
                    continue
                    
                col_analysis = self.analyze_column(df, col_name, total_rows)
                column_analyses.append(col_analysis)
            
            # Get schema info
            schema_info = []
            for field in df.schema.fields:
                if field.name not in ['ingestion_date', 'ingestion_timestamp', 'data_source', 'batch_id', 'simulation_id']:
                    schema_info.append({
                        'column_name': field.name,
                        'data_type': str(field.dataType),
                        'nullable': field.nullable
                    })
            
            return {
                'table_name': table_name,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'business_columns': len(column_analyses),
                'schema_info': schema_info,
                'column_analyses': column_analyses,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'error': str(e),
                'analysis_timestamp': datetime.now().isoformat()
            }

    def analyze_column(self, df, col_name, total_rows):
        """Detailed analysis of a single column"""
        try:
            # Basic stats
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            non_null_count = total_rows - null_count
            
            # Get column data type
            col_type = str(df.schema[col_name].dataType)
            
            # Initialize analysis result
            analysis = {
                'column_name': col_name,
                'data_type': col_type,
                'total_rows': total_rows,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'non_null_count': non_null_count,
                'unique_values': None,
                'min_value': None,
                'max_value': None,
                'mean_value': None,
                'distinct_count': None,
                'most_frequent_values': [],
                'data_quality_score': None,
                'issues': []
            }
            
            if non_null_count > 0:
                # Get distinct count
                try:
                    distinct_count = df.select(col_name).distinct().count()
                    analysis['distinct_count'] = distinct_count
                    analysis['unique_percentage'] = round((distinct_count / total_rows) * 100, 2)
                except:
                    analysis['distinct_count'] = "Error calculating"
                
                # Type-specific analysis
                if 'int' in col_type.lower() or 'long' in col_type.lower() or 'double' in col_type.lower():
                    # Numeric analysis
                    try:
                        stats = df.select(
                            spark_min(col(col_name)).alias('min_val'),
                            spark_max(col(col_name)).alias('max_val'),
                            avg(col(col_name)).alias('mean_val')
                        ).collect()[0]
                        
                        analysis['min_value'] = stats['min_val']
                        analysis['max_value'] = stats['max_val'] 
                        analysis['mean_value'] = round(stats['mean_val'], 2) if stats['mean_val'] else None
                        
                        # Check for outliers (simple rule: values beyond 3 standard deviations)
                        if stats['mean_val']:
                            std_dev = df.select(stddev(col(col_name))).collect()[0][0]
                            if std_dev:
                                outlier_threshold = 3 * std_dev
                                outlier_count = df.filter(
                                    spark_abs(col(col_name) - stats['mean_val']) > outlier_threshold
                                ).count()
                                if outlier_count > 0:
                                    analysis['issues'].append(f"{outlier_count} potential outliers detected")
                        
                        # Check for negative values where not expected
                        if col_name.startswith('AMT_') or col_name.startswith('CNT_'):
                            negative_count = df.filter(col(col_name) < 0).count()
                            if negative_count > 0:
                                analysis['issues'].append(f"{negative_count} negative values in amount/count field")
                                
                    except Exception as e:
                        analysis['issues'].append(f"Numeric analysis failed: {str(e)}")
                
                elif 'string' in col_type.lower():
                    # String analysis
                    try:
                        # Get most frequent values
                        freq_values = df.groupBy(col_name).count() \
                            .orderBy(desc('count')) \
                            .limit(5) \
                            .collect()
                        
                        analysis['most_frequent_values'] = [
                            {'value': row[col_name], 'count': row['count'], 
                             'percentage': round((row['count'] / total_rows) * 100, 2)}
                            for row in freq_values
                        ]
                        
                        # Check for empty strings
                        empty_count = df.filter((col(col_name) == "") | (col(col_name) == " ")).count()
                        if empty_count > 0:
                            analysis['issues'].append(f"{empty_count} empty/blank string values")
                            
                        # Check string length statistics
                        lengths = df.select(length(col(col_name)).alias('len')).filter(col(col_name).isNotNull())
                        length_stats = lengths.select(
                            spark_min('len').alias('min_len'),
                            spark_max('len').alias('max_len'),
                            avg('len').alias('avg_len')
                        ).collect()[0]
                        
                        analysis['min_length'] = length_stats['min_len']
                        analysis['max_length'] = length_stats['max_len']
                        analysis['avg_length'] = round(length_stats['avg_len'], 1) if length_stats['avg_len'] else None
                        
                    except Exception as e:
                        analysis['issues'].append(f"String analysis failed: {str(e)}")
            
            # Calculate data quality score (0-100)
            quality_score = 100
            
            # Deduct points for nulls
            if null_percentage > 0:
                quality_score -= min(null_percentage, 50)  # Max 50 points deduction for nulls
            
            # Deduct points for issues
            quality_score -= len(analysis['issues']) * 10
            
            # Bonus for high uniqueness in ID fields
            if col_name.startswith('SK_ID_') and analysis.get('unique_percentage', 0) > 95:
                quality_score += 10
                
            analysis['data_quality_score'] = max(0, min(100, round(quality_score, 1)))
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing column {col_name}: {str(e)}")
            return {
                'column_name': col_name,
                'error': str(e),
                'data_quality_score': 0
            }

    def generate_comprehensive_report(self):
        """Generate comprehensive DQ report for all tables"""
        logger.info("🚀 Starting comprehensive data quality analysis for all 8 tables")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Analyze all tables
        all_analyses = []
        for table_name in self.tables:
            analysis = self.get_comprehensive_table_analysis(table_name)
            all_analyses.append(analysis)
        
        # Generate comprehensive HTML report
        html_report = self.generate_html_report(all_analyses, timestamp)
        html_file = f"{self.results_dir}/comprehensive_dq_report_{timestamp}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_report)
        
        # Generate detailed CSV report
        csv_report = self.generate_detailed_csv(all_analyses, timestamp)
        
        # Generate JSON report
        json_file = f"{self.results_dir}/comprehensive_dq_data_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_tables': len(all_analyses),
                    'total_rows': sum(a.get('total_rows', 0) for a in all_analyses),
                    'total_columns': sum(a.get('business_columns', 0) for a in all_analyses)
                },
                'table_analyses': all_analyses
            }, f, indent=2, default=str)
        
        # Generate summary report
        summary_report = self.generate_summary_report(all_analyses, timestamp)
        
        logger.info(f"📊 Comprehensive DQ reports generated:")
        logger.info(f"   📄 HTML Report: {html_file}")
        logger.info(f"   📊 CSV Report: {csv_report}")
        logger.info(f"   📋 JSON Data: {json_file}")
        logger.info(f"   📝 Summary: {summary_report}")
        
        return all_analyses

    def generate_detailed_csv(self, all_analyses, timestamp):
        """Generate detailed CSV with all column information"""
        csv_file = f"{self.results_dir}/detailed_column_analysis_{timestamp}.csv"
        
        rows = []
        for analysis in all_analyses:
            if 'error' in analysis:
                continue
                
            table_name = analysis['table_name']
            for col_analysis in analysis.get('column_analyses', []):
                row = {
                    'table_name': table_name,
                    'column_name': col_analysis['column_name'],
                    'data_type': col_analysis['data_type'],
                    'total_rows': col_analysis['total_rows'],
                    'null_count': col_analysis['null_count'],
                    'null_percentage': col_analysis['null_percentage'],
                    'distinct_count': col_analysis.get('distinct_count', ''),
                    'unique_percentage': col_analysis.get('unique_percentage', ''),
                    'min_value': col_analysis.get('min_value', ''),
                    'max_value': col_analysis.get('max_value', ''),
                    'mean_value': col_analysis.get('mean_value', ''),
                    'data_quality_score': col_analysis['data_quality_score'],
                    'issues': '; '.join(col_analysis.get('issues', [])),
                    'most_frequent_value': col_analysis.get('most_frequent_values', [{}])[0].get('value', '') if col_analysis.get('most_frequent_values') else ''
                }
                rows.append(row)
        
        # Convert to DataFrame and save
        df = pd.DataFrame(rows)
        df.to_csv(csv_file, index=False)
        
        logger.info(f"📊 Detailed CSV saved: {csv_file}")
        return csv_file

    def generate_html_report(self, all_analyses, timestamp):
        """Generate comprehensive HTML report"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Comprehensive Data Quality Report - Home Credit Dataset</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .summary {{ background-color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .table-section {{ background-color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .table-header {{ background-color: #3498db; color: white; padding: 10px; border-radius: 5px; margin-bottom: 10px; }}
                .column-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                .column-table th {{ background-color: #34495e; color: white; padding: 8px; text-align: left; }}
                .column-table td {{ padding: 8px; border-bottom: 1px solid #ddd; }}
                .quality-score {{ font-weight: bold; }}
                .score-excellent {{ color: #27ae60; }}
                .score-good {{ color: #f39c12; }}
                .score-poor {{ color: #e74c3c; }}
                .issues {{ color: #e74c3c; font-size: 0.9em; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 Comprehensive Data Quality Report</h1>
                <h2>All 8 Home Credit Tables - Column-wise Analysis</h2>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        """
        
        # Add summary section
        total_tables = len(all_analyses)
        total_rows = sum(a.get('total_rows', 0) for a in all_analyses)
        total_columns = sum(a.get('business_columns', 0) for a in all_analyses)
        
        html += f"""
            <div class="summary">
                <h2>📊 Overview Summary</h2>
                <p><strong>Total Tables Analyzed:</strong> {total_tables}</p>
                <p><strong>Total Records:</strong> {total_rows:,}</p>
                <p><strong>Total Business Columns:</strong> {total_columns}</p>
                <p><strong>Average Columns per Table:</strong> {total_columns / total_tables:.1f}</p>
            </div>
        """
        
        # Add detailed table sections
        for analysis in all_analyses:
            if 'error' in analysis:
                html += f"""
                    <div class="table-section">
                        <div class="table-header">❌ {analysis['table_name']} - ERROR</div>
                        <p>Error: {analysis['error']}</p>
                    </div>
                """
                continue
            
            html += f"""
                <div class="table-section">
                    <div class="table-header">📊 {analysis['table_name']}</div>
                    <p><strong>Records:</strong> {analysis['total_rows']:,} | <strong>Business Columns:</strong> {analysis['business_columns']}</p>
                    
                    <table class="column-table">
                        <thead>
                            <tr>
                                <th>Column Name</th>
                                <th>Data Type</th>
                                <th>Quality Score</th>
                                <th>Null %</th>
                                <th>Distinct Count</th>
                                <th>Min/Max</th>
                                <th>Issues</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            for col in analysis.get('column_analyses', []):
                score_class = "score-excellent" if col['data_quality_score'] >= 80 else "score-good" if col['data_quality_score'] >= 60 else "score-poor"
                min_max = f"{col.get('min_value', 'N/A')} / {col.get('max_value', 'N/A')}" if col.get('min_value') is not None else "N/A"
                issues_text = "; ".join(col.get('issues', [])) if col.get('issues') else "None"
                
                html += f"""
                            <tr>
                                <td><strong>{col['column_name']}</strong></td>
                                <td>{col['data_type']}</td>
                                <td class="quality-score {score_class}">{col['data_quality_score']:.1f}</td>
                                <td>{col['null_percentage']:.1f}%</td>
                                <td>{col.get('distinct_count', 'N/A')}</td>
                                <td>{min_max}</td>
                                <td class="issues">{issues_text}</td>
                            </tr>
                """
            
            html += """
                        </tbody>
                    </table>
                </div>
            """
        
        html += """
        </body>
        </html>
        """
        
        return html

    def generate_summary_report(self, all_analyses, timestamp):
        """Generate text summary report"""
        summary_file = f"{self.results_dir}/comprehensive_summary_{timestamp}.txt"
        
        with open(summary_file, 'w') as f:
            f.write("🚀 COMPREHENSIVE DATA QUALITY REPORT SUMMARY\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Overall summary
            total_tables = len(all_analyses)
            total_rows = sum(a.get('total_rows', 0) for a in all_analyses)
            total_columns = sum(a.get('business_columns', 0) for a in all_analyses)
            
            f.write("📊 OVERALL SUMMARY:\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Tables: {total_tables}\n")
            f.write(f"Total Records: {total_rows:,}\n")
            f.write(f"Total Business Columns: {total_columns}\n")
            f.write(f"Average Columns per Table: {total_columns / len(all_analyses):.1f}\n\n")
            
            # Table-by-table analysis
            for analysis in all_analyses:
                if 'error' in analysis:
                    f.write(f"❌ {analysis['table_name']}: ERROR - {analysis['error']}\n\n")
                    continue
                
                f.write(f"📊 TABLE: {analysis['table_name']}\n")
                f.write("-" * 60 + "\n")
                f.write(f"Records: {analysis['total_rows']:,}\n")
                f.write(f"Business Columns: {analysis['business_columns']}\n\n")
                
                f.write("COLUMN ANALYSIS:\n")
                for col in analysis.get('column_analyses', []):
                    quality_indicator = "🟢" if col['data_quality_score'] >= 80 else "🟡" if col['data_quality_score'] >= 60 else "🔴"
                    f.write(f"  {quality_indicator} {col['column_name']:<25} | Score: {col['data_quality_score']:>5.1f} | ")
                    f.write(f"Nulls: {col['null_percentage']:>5.1f}% | ")
                    f.write(f"Distinct: {str(col.get('distinct_count', 'N/A')):>8}\n")
                    
                    if col.get('issues'):
                        for issue in col['issues']:
                            f.write(f"    ⚠️ {issue}\n")
                
                f.write("\n")
        
        logger.info(f"📝 Summary report saved: {summary_file}")
        return summary_file

def main():
    """Main execution"""
    print("🚀 Comprehensive Data Quality Analysis for All 8 Home Credit Tables")
    print("=" * 80)
    
    # Initialize and run comprehensive analysis
    dq_system = ComprehensiveDQSystem()
    results = dq_system.generate_comprehensive_report()
    
    print("\n🎉 Comprehensive analysis completed!")
    print("📁 Check dq-results/ directory for detailed reports:")
    print("   📄 HTML Report (comprehensive_dq_report_*.html)")
    print("   📊 CSV Report (detailed_column_analysis_*.csv)")
    print("   📋 JSON Data (comprehensive_dq_data_*.json)")
    print("   📝 Summary (comprehensive_summary_*.txt)")

if __name__ == "__main__":
    main()
