#!/usr/bin/env python3
"""
WORKING Data Quality System - Fixed All Errors
Generates detailed reports for ALL columns across ALL 8 Home Credit tables
"""

import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, length, desc
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg, stddev as spark_stddev
from pyspark.sql.types import *
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkingDQSystem:
    """Working Data Quality System with detailed column analysis"""
    
    def __init__(self):
        """Initialize the working DQ system"""
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
        
        logger.info("🚀 Working DQ System initialized for detailed column analysis")

    def init_spark(self):
        """Initialize Spark session"""
        spark = SparkSession.builder \
            .appName("WorkingDQSystem") \
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

    def analyze_column_simple(self, df, col_name, total_rows):
        """Simple but working column analysis"""
        try:
            # Basic null analysis
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
                'data_quality_score': 0,
                'issues': []
            }
            
            # Calculate quality score
            quality_score = 100 - min(null_percentage, 50)  # Deduct up to 50 points for nulls
            
            if non_null_count > 0:
                try:
                    # Get distinct count safely
                    distinct_count = df.select(col_name).distinct().count()
                    analysis['distinct_count'] = distinct_count
                    analysis['unique_percentage'] = round((distinct_count / total_rows) * 100, 2)
                    
                    # Bonus for high uniqueness in ID fields
                    if col_name.startswith('SK_ID_') and analysis['unique_percentage'] > 95:
                        quality_score += 10
                        
                except Exception as e:
                    analysis['issues'].append(f"Could not calculate distinct count: {str(e)}")
                
                # Type-specific analysis (simplified)
                if any(x in col_type.lower() for x in ['int', 'long', 'double', 'float']):
                    try:
                        # Simple numeric stats
                        stats_df = df.select(
                            spark_min(col(col_name)).alias('min_val'),
                            spark_max(col(col_name)).alias('max_val'),
                            spark_avg(col(col_name)).alias('mean_val')
                        )
                        stats = stats_df.collect()[0]
                        
                        analysis['min_value'] = stats['min_val']
                        analysis['max_value'] = stats['max_val']
                        analysis['mean_value'] = round(float(stats['mean_val']), 2) if stats['mean_val'] else None
                        
                        # Check for negative values in amount/count fields
                        if col_name.startswith(('AMT_', 'CNT_')):
                            negative_count = df.filter(col(col_name) < 0).count()
                            if negative_count > 0:
                                analysis['issues'].append(f"{negative_count} negative values")
                                quality_score -= 10
                                
                    except Exception as e:
                        analysis['issues'].append(f"Numeric analysis failed: {str(e)}")
                
                elif 'string' in col_type.lower():
                    try:
                        # String analysis
                        empty_count = df.filter((col(col_name) == "") | (col(col_name) == " ")).count()
                        if empty_count > 0:
                            analysis['issues'].append(f"{empty_count} empty strings")
                            quality_score -= 5
                            
                        # Get top values
                        freq_values = df.groupBy(col_name).count().orderBy(desc('count')).limit(3).collect()
                        analysis['top_values'] = [
                            {'value': str(row[col_name]), 'count': row['count']}
                            for row in freq_values
                        ]
                        
                    except Exception as e:
                        analysis['issues'].append(f"String analysis failed: {str(e)}")
            
            analysis['data_quality_score'] = max(0, min(100, round(quality_score, 1)))
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing column {col_name}: {str(e)}")
            return {
                'column_name': col_name,
                'error': str(e),
                'data_quality_score': 0
            }

    def analyze_table(self, table_name):
        """Analyze a single table"""
        logger.info(f"🔍 Analyzing table: {table_name}")
        
        try:
            # Read the table data from HDFS
            table_path = f"hdfs://namenode:9000/warehouse/{table_name}"
            df = self.spark.read.parquet(table_path)
            
            # Get basic info
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Analyze each column (skip metadata columns)
            column_analyses = []
            metadata_cols = ['ingestion_date', 'ingestion_timestamp', 'data_source', 'batch_id', 'simulation_id']
            
            for col_name in df.columns:
                if col_name not in metadata_cols:
                    col_analysis = self.analyze_column_simple(df, col_name, total_rows)
                    column_analyses.append(col_analysis)
            
            return {
                'table_name': table_name,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'business_columns': len(column_analyses),
                'column_analyses': column_analyses,
                'analysis_timestamp': datetime.now().isoformat(),
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'error': str(e),
                'analysis_timestamp': datetime.now().isoformat(),
                'status': 'error'
            }

    def generate_working_report(self):
        """Generate comprehensive DQ report for all tables"""
        logger.info("🚀 Starting working data quality analysis for all 8 tables")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Analyze all tables
        all_analyses = []
        for table_name in self.tables:
            analysis = self.analyze_table(table_name)
            all_analyses.append(analysis)
        
        # Generate CSV report
        csv_file = f"{self.results_dir}/working_dq_report_{timestamp}.csv"
        self.save_csv_report(all_analyses, csv_file)
        
        # Generate JSON report
        json_file = f"{self.results_dir}/working_dq_data_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_tables': len(all_analyses),
                    'successful_tables': len([a for a in all_analyses if a.get('status') == 'success']),
                    'total_rows': sum(a.get('total_rows', 0) for a in all_analyses if a.get('status') == 'success'),
                    'total_columns': sum(a.get('business_columns', 0) for a in all_analyses if a.get('status') == 'success')
                },
                'table_analyses': all_analyses
            }, f, indent=2, default=str)
        
        # Generate HTML report
        html_file = f"{self.results_dir}/working_dq_report_{timestamp}.html"
        self.save_html_report(all_analyses, html_file, timestamp)
        
        # Generate summary
        summary_file = f"{self.results_dir}/working_summary_{timestamp}.txt"
        self.save_summary_report(all_analyses, summary_file)
        
        logger.info(f"✅ Working DQ reports generated:")
        logger.info(f"   📊 CSV Report: {csv_file}")
        logger.info(f"   📋 JSON Data: {json_file}")
        logger.info(f"   📄 HTML Report: {html_file}")
        logger.info(f"   📝 Summary: {summary_file}")
        
        return all_analyses

    def save_csv_report(self, all_analyses, csv_file):
        """Save CSV report"""
        rows = []
        for analysis in all_analyses:
            if analysis.get('status') == 'error':
                rows.append({
                    'table_name': analysis['table_name'],
                    'column_name': 'ERROR',
                    'error': analysis.get('error', ''),
                    'data_quality_score': 0
                })
                continue
                
            for col_analysis in analysis.get('column_analyses', []):
                row = {
                    'table_name': analysis['table_name'],
                    'column_name': col_analysis['column_name'],
                    'data_type': col_analysis['data_type'],
                    'total_rows': col_analysis['total_rows'],
                    'null_count': col_analysis['null_count'],
                    'null_percentage': col_analysis['null_percentage'],
                    'distinct_count': col_analysis.get('distinct_count', ''),
                    'min_value': col_analysis.get('min_value', ''),
                    'max_value': col_analysis.get('max_value', ''),
                    'mean_value': col_analysis.get('mean_value', ''),
                    'data_quality_score': col_analysis['data_quality_score'],
                    'issues': '; '.join(col_analysis.get('issues', []))
                }
                rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(csv_file, index=False)

    def save_html_report(self, all_analyses, html_file, timestamp):
        """Save HTML report"""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Working Data Quality Report - Home Credit Dataset</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .summary {{ background-color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .table-section {{ background-color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .good {{ color: #27ae60; }}
                .warning {{ color: #f39c12; }}
                .error {{ color: #e74c3c; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th, td {{ padding: 8px; border: 1px solid #ddd; text-align: left; }}
                th {{ background-color: #34495e; color: white; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>✅ Working Data Quality Report</h1>
                <h2>Home Credit Dataset - Column Analysis</h2>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="summary">
                <h3>📊 Summary</h3>
                <p>Total Tables: {len(all_analyses)}</p>
                <p>Successful Analyses: {len([a for a in all_analyses if a.get('status') == 'success'])}</p>
                <p>Total Rows: {sum(a.get('total_rows', 0) for a in all_analyses if a.get('status') == 'success'):,}</p>
                <p>Total Business Columns: {sum(a.get('business_columns', 0) for a in all_analyses if a.get('status') == 'success')}</p>
            </div>
        """
        
        for analysis in all_analyses:
            if analysis.get('status') == 'error':
                html_content += f"""
                <div class="table-section">
                    <h3 class="error">❌ {analysis['table_name']}</h3>
                    <p class="error">Error: {analysis.get('error', 'Unknown error')}</p>
                </div>
                """
                continue
                
            html_content += f"""
            <div class="table-section">
                <h3 class="good">✅ {analysis['table_name']}</h3>
                <p>Rows: {analysis['total_rows']:,} | Business Columns: {analysis['business_columns']}</p>
                <table>
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Type</th>
                            <th>Quality Score</th>
                            <th>Nulls %</th>
                            <th>Distinct Count</th>
                            <th>Issues</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for col in analysis.get('column_analyses', []):
                score_class = "good" if col['data_quality_score'] >= 80 else "warning" if col['data_quality_score'] >= 60 else "error"
                html_content += f"""
                        <tr>
                            <td>{col['column_name']}</td>
                            <td>{col['data_type']}</td>
                            <td class="{score_class}">{col['data_quality_score']}</td>
                            <td>{col['null_percentage']}%</td>
                            <td>{col.get('distinct_count', 'N/A')}</td>
                            <td>{'; '.join(col.get('issues', []))}</td>
                        </tr>
                """
            
            html_content += """
                    </tbody>
                </table>
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

    def save_summary_report(self, all_analyses, summary_file):
        """Save summary report"""
        with open(summary_file, 'w') as f:
            f.write("WORKING DATA QUALITY SUMMARY REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            successful = [a for a in all_analyses if a.get('status') == 'success']
            failed = [a for a in all_analyses if a.get('status') == 'error']
            
            f.write(f"Total Tables: {len(all_analyses)}\n")
            f.write(f"Successful: {len(successful)}\n")
            f.write(f"Failed: {len(failed)}\n\n")
            
            if successful:
                total_rows = sum(a['total_rows'] for a in successful)
                total_cols = sum(a['business_columns'] for a in successful)
                f.write(f"Total Rows Analyzed: {total_rows:,}\n")
                f.write(f"Total Business Columns: {total_cols}\n\n")
            
            for analysis in all_analyses:
                if analysis.get('status') == 'error':
                    f.write(f"❌ {analysis['table_name']}: {analysis.get('error', 'Unknown error')}\n")
                else:
                    f.write(f"✅ {analysis['table_name']}: {analysis['total_rows']:,} rows, {analysis['business_columns']} columns\n")

def main():
    """Main execution"""
    print("✅ WORKING Data Quality Analysis for All 8 Home Credit Tables")
    print("=" * 70)
    
    # Initialize and run working analysis
    dq_system = WorkingDQSystem()
    results = dq_system.generate_working_report()
    
    print("\n🎉 Working analysis completed successfully!")
    print("📁 Check dq-results/ directory for reports")

if __name__ == "__main__":
    main()
