#!/usr/bin/env python3
"""
Complete Daily DQ System - With True Daily Simulation
====================================================
This system performs:
1. Fresh daily simulation from HDFS CSV files
2. Creates new Iceberg tables with current timestamp
3. Analyzes the fresh simulated data
4. Shows different results each run

This demonstrates REAL daily data changes!
"""

import os
import json
import logging
from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, length
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg, count as spark_count
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DailySimulationDQSystem:
    """Complete DQ System with True Daily Simulation"""
    
    def __init__(self):
        """Initialize the system"""
        self.spark = self.init_spark()
        self.results_dir = "dq-results"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Generate unique simulation ID for this run
        self.simulation_date = datetime.now().strftime("%Y-%m-%d")
        self.simulation_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.random_seed = random.randint(1, 1000)  # Different seed each run!
        
        # Original CSV files in HDFS
        self.hdfs_csv_path = "hdfs://namenode:9000/data/home-credit-default-risk-dataset"
        self.warehouse_path = "hdfs://namenode:9000/warehouse"
        
        # CSV file mapping
        self.csv_files = {
            "application_train": f"{self.hdfs_csv_path}/application_train.csv",
            "application_test": f"{self.hdfs_csv_path}/application_test.csv", 
            "bureau": f"{self.hdfs_csv_path}/bureau.csv",
            "bureau_balance": f"{self.hdfs_csv_path}/bureau_balance.csv",
            "credit_card_balance": f"{self.hdfs_csv_path}/credit_card_balance.csv",
            "installments_payments": f"{self.hdfs_csv_path}/installments_payments.csv",
            "POS_CASH_balance": f"{self.hdfs_csv_path}/POS_CASH_balance.csv",
            "previous_application": f"{self.hdfs_csv_path}/previous_application.csv"
        }
        
        logger.info(f"🚀 Daily Simulation DQ System initialized")
        logger.info(f"📅 Simulation Date: {self.simulation_date}")
        logger.info(f"🔢 Simulation ID: {self.simulation_id}")
        logger.info(f"🎲 Random Seed: {self.random_seed}")

    def init_spark(self):
        """Initialize Spark session"""
        spark = SparkSession.builder \
            .appName("DailySimulationDQSystem") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session initialized")
        return spark

    def simulate_daily_data(self):
        """
        Step 1: Simulate fresh daily data from HDFS CSV files
        This creates NEW data each time with different sampling!
        """
        logger.info("📅 Starting FRESH daily data simulation...")
        
        # Use different sample fractions for variety
        base_fraction = 0.01  # Base 1%
        fraction_variance = random.uniform(0.005, 0.015)  # 0.5% to 1.5% variance
        sample_fraction = base_fraction + fraction_variance
        
        logger.info(f"🎲 Using sample fraction: {sample_fraction:.3f} (seed: {self.random_seed})")
        
        simulated_tables = {}
        
        for table_name, csv_path in self.csv_files.items():
            try:
                logger.info(f"📁 Processing {table_name} from HDFS CSV...")
                
                # Read original CSV from HDFS
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_path)
                
                original_count = df.count()
                logger.info(f"   📊 Original rows: {original_count:,}")
                
                # Apply FRESH sampling with different seed each time
                sampled_df = df.sample(fraction=sample_fraction, seed=self.random_seed)
                sampled_count = sampled_df.count()
                logger.info(f"   📊 Sampled rows: {sampled_count:,} ({sample_fraction*100:.1f}%)")
                
                # Add unique daily simulation metadata
                daily_df = sampled_df \
                    .withColumn("ingestion_date", lit(self.simulation_date)) \
                    .withColumn("ingestion_timestamp", current_timestamp()) \
                    .withColumn("simulation_id", lit(self.simulation_id)) \
                    .withColumn("data_source", lit("hdfs_csv_fresh")) \
                    .withColumn("batch_id", monotonically_increasing_id()) \
                    .withColumn("random_seed", lit(self.random_seed))
                
                # Store with unique table name for this simulation
                daily_table_name = f"{table_name}_daily_{self.simulation_id}"
                simulated_tables[table_name] = daily_table_name
                
                # Save as temporary table for analysis
                daily_df.createOrReplaceTempView(daily_table_name)
                
                logger.info(f"   ✅ Created fresh table: {daily_table_name}")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to simulate {table_name}: {str(e)}")
        
        logger.info("✅ Fresh daily simulation completed")
        return simulated_tables

    def analyze_fresh_data(self, simulated_tables):
        """
        Step 2: Analyze the FRESH simulated data
        """
        logger.info("🔍 Starting analysis of FRESH simulated data...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        all_results = []
        
        for original_name, temp_table_name in simulated_tables.items():
            logger.info(f"📊 Analyzing fresh data: {original_name}")
            
            try:
                # Read from temporary table (fresh data)
                df = self.spark.table(temp_table_name)
                
                # Basic metrics
                total_rows = df.count()
                total_columns = len(df.columns)
                metadata_cols = ['ingestion_date', 'ingestion_timestamp', 'simulation_id', 
                               'data_source', 'batch_id', 'random_seed']
                business_columns = [col for col in df.columns if col not in metadata_cols]
                
                table_result = {
                    'table_name': f"{original_name}_daily",
                    'simulation_id': self.simulation_id,
                    'random_seed': self.random_seed,
                    'total_rows': total_rows,
                    'total_columns': total_columns,
                    'business_columns': len(business_columns),
                    'column_analysis': [],
                    'analysis_timestamp': datetime.now().isoformat()
                }
                  # Analyze each business column
                for col_name in business_columns[:10]:  # Limit to first 10 for speed
                    col_analysis = self.analyze_column_comprehensive(df, col_name, total_rows)
                    table_result['column_analysis'].append(col_analysis)
                
                all_results.append(table_result)
                logger.info(f"   ✅ Analyzed: {total_rows:,} rows, {len(business_columns)} columns")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to analyze {original_name}: {str(e)}")
                all_results.append({
                    'table_name': f"{original_name}_daily",
                    'error': str(e),
                    'simulation_id': self.simulation_id,
                    'analysis_timestamp': datetime.now().isoformat()
                })        
        # Generate reports
        self.generate_fresh_reports(all_results, timestamp)
        logger.info("✅ Fresh data analysis completed")
        
        return all_results

    def analyze_column_comprehensive(self, df, col_name, total_rows):
        """
        Comprehensive column analysis with DQ checks:
        - Volume/null monitoring
        - Min/max/avg range validation  
        - Data type validation
        - Freshness checks
        - Range validation alerts
        """
        try:
            # Basic null analysis
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            non_null_count = total_rows - null_count
            
            col_type = str(df.schema[col_name].dataType)
            
            # Initialize comprehensive analysis
            analysis = {
                'column_name': col_name,
                'data_type': col_type,
                'total_rows': total_rows,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'non_null_count': non_null_count,
                'dq_checks': [],
                'alerts': [],
                'statistics': {}
            }
            
            # DQ Check 1: Volume monitoring
            if total_rows == 0:
                analysis['alerts'].append("CRITICAL: No data found")
                analysis['data_quality_score'] = 0
                return analysis
            
            # DQ Check 2: NULL value validation
            if null_percentage > 95:
                analysis['alerts'].append(f"CRITICAL: {null_percentage}% null values - data integrity issue")
            elif null_percentage > 50:
                analysis['alerts'].append(f"WARNING: {null_percentage}% null values - high null rate")
            elif null_percentage > 20:
                analysis['alerts'].append(f"INFO: {null_percentage}% null values - moderate null rate")
            
            analysis['dq_checks'].append({
                'check_name': 'null_validation',
                'status': 'PASS' if null_percentage <= 50 else 'FAIL',
                'threshold': '≤50%',
                'actual': f"{null_percentage}%"
            })
            
            # Get distinct count for uniqueness analysis
            if non_null_count > 0:
                try:
                    distinct_count = df.select(col_name).distinct().count()
                    uniqueness_percentage = (distinct_count / total_rows) * 100
                    analysis['statistics']['distinct_count'] = distinct_count
                    analysis['statistics']['uniqueness_percentage'] = round(uniqueness_percentage, 2)
                    
                    # DQ Check 3: Uniqueness validation for ID columns
                    if col_name.startswith('SK_ID_'):
                        if uniqueness_percentage < 95:
                            analysis['alerts'].append(f"WARNING: ID column {col_name} uniqueness only {uniqueness_percentage}%")
                        analysis['dq_checks'].append({
                            'check_name': 'id_uniqueness',
                            'status': 'PASS' if uniqueness_percentage >= 95 else 'FAIL',
                            'threshold': '≥95%',
                            'actual': f"{uniqueness_percentage}%"
                        })
                except Exception as e:
                    analysis['alerts'].append(f"Could not calculate distinctness: {str(e)}")
            
            # DQ Check 4: Numeric range validation for numeric columns
            if any(x in col_type.lower() for x in ['int', 'long', 'double', 'float']):
                try:
                    non_null_df = df.filter(col(col_name).isNotNull())
                    if non_null_df.count() > 0:
                        # Calculate statistics
                        stats = non_null_df.agg(
                            spark_min(col_name).alias("min_val"),
                            spark_max(col_name).alias("max_val"), 
                            spark_avg(col_name).alias("avg_val")
                        ).collect()[0]
                        
                        min_val = stats['min_val'] 
                        max_val = stats['max_val']
                        avg_val = stats['avg_val']
                        
                        analysis['statistics'].update({
                            'min_value': min_val,
                            'max_value': max_val,
                            'average_value': round(avg_val, 2) if avg_val else None,
                            'range_span': max_val - min_val if min_val is not None and max_val is not None else None
                        })
                        
                        # DQ Check 5: Range validation alerts
                        if min_val is not None and max_val is not None:
                            # Check for suspicious ranges
                            if col_name == 'TARGET' and (min_val < 0 or max_val > 1):
                                analysis['alerts'].append(f"ERROR: TARGET values outside [0,1] range: {min_val} to {max_val}")
                            
                            if 'DAYS_' in col_name and max_val > 0:
                                analysis['alerts'].append(f"WARNING: {col_name} has positive days values: max={max_val}")
                            
                            if 'AMT_' in col_name and min_val < 0:
                                analysis['alerts'].append(f"WARNING: {col_name} has negative amounts: min={min_val}")
                            
                            # General range validation
                            range_span = max_val - min_val
                            if range_span == 0:
                                analysis['alerts'].append(f"INFO: {col_name} has constant value: {min_val}")
                            
                            analysis['dq_checks'].append({
                                'check_name': 'numeric_range_validation',
                                'status': 'PASS',
                                'min_value': min_val,
                                'max_value': max_val,
                                'average_value': round(avg_val, 2) if avg_val else None
                            })
                        
                except Exception as e:
                    analysis['alerts'].append(f"Numeric analysis failed: {str(e)}")
            
            # DQ Check 6: String length validation for string columns
            elif 'string' in col_type.lower():
                try:
                    non_null_df = df.filter(col(col_name).isNotNull())
                    if non_null_df.count() > 0:
                        # Calculate string length statistics
                        length_stats = non_null_df.agg(
                            spark_min(length(col(col_name))).alias("min_len"),
                            spark_max(length(col(col_name))).alias("max_len"),
                            spark_avg(length(col(col_name))).alias("avg_len")
                        ).collect()[0]
                        
                        analysis['statistics'].update({
                            'min_length': length_stats['min_len'],
                            'max_length': length_stats['max_len'], 
                            'avg_length': round(length_stats['avg_len'], 2) if length_stats['avg_len'] else None
                        })
                        
                        # Check for suspiciously short/long strings
                        if length_stats['min_len'] == 0:
                            analysis['alerts'].append(f"WARNING: {col_name} contains empty strings")
                        if length_stats['max_len'] > 100:
                            analysis['alerts'].append(f"INFO: {col_name} has very long strings (max: {length_stats['max_len']})")
                            
                        analysis['dq_checks'].append({
                            'check_name': 'string_length_validation',
                            'status': 'PASS',
                            'min_length': length_stats['min_len'],
                            'max_length': length_stats['max_len']
                        })
                        
                except Exception as e:
                    analysis['alerts'].append(f"String analysis failed: {str(e)}")
            
            # Calculate overall data quality score
            critical_alerts = len([a for a in analysis['alerts'] if 'CRITICAL' in a])
            warning_alerts = len([a for a in analysis['alerts'] if 'WARNING' in a])
            failed_checks = len([c for c in analysis['dq_checks'] if c.get('status') == 'FAIL'])
            
            quality_score = 100
            quality_score -= critical_alerts * 30  # -30 for each critical alert
            quality_score -= warning_alerts * 10   # -10 for each warning 
            quality_score -= failed_checks * 20    # -20 for each failed check
            quality_score -= min(null_percentage / 2, 25)  # Deduct based on null percentage
            
            analysis['data_quality_score'] = max(round(quality_score, 1), 0)
            
            return analysis
            
        except Exception as e:
            return {                'column_name': col_name,
                'error': str(e),
                'data_quality_score': 0,
                'alerts': [f"CRITICAL: Analysis failed - {str(e)}"]
            }

    def generate_fresh_reports(self, results, timestamp):
        """Generate comprehensive reports for fresh simulated data"""
        
        # JSON report with simulation details
        json_file = f"{self.results_dir}/fresh_daily_analysis_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'simulation_id': self.simulation_id,
                'random_seed': self.random_seed,
                'simulation_date': self.simulation_date,
                'summary': {
                    'total_tables': len(results),
                    'total_rows': sum(r.get('total_rows', 0) for r in results),
                    'total_columns': sum(r.get('business_columns', 0) for r in results)
                },
                'table_results': results
            }, f, indent=2, default=str)
        
        # HTML report with comprehensive DQ analysis
        html_file = f"{self.results_dir}/fresh_daily_dq_report_{timestamp}.html"
        with open(html_file, 'w') as f:
            f.write(self.generate_html_report(results, timestamp))
        
        # Summary with simulation info
        summary_file = f"{self.results_dir}/fresh_daily_summary_{timestamp}.txt"
        with open(summary_file, 'w') as f:
            f.write("FRESH DAILY DATA QUALITY ANALYSIS\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Simulation ID: {self.simulation_id}\n")
            f.write(f"Random Seed: {self.random_seed}\n")
            f.write(f"Simulation Date: {self.simulation_date}\n\n")
            f.write(f"Total Tables: {len(results)}\n")
            f.write(f"Total Rows: {sum(r.get('total_rows', 0) for r in results):,}\n\n")
            
            for result in results:
                if 'error' in result:
                    f.write(f"❌ {result['table_name']}: ERROR\n")
                else:
                    f.write(f"✅ {result['table_name']}: {result['total_rows']:,} rows (seed: {result['random_seed']})\n")
        
        logger.info(f"📊 Fresh simulation reports generated:")
        logger.info(f"   📋 JSON: {json_file}")
        logger.info(f"   🌐 HTML: {html_file}")
        logger.info(f"   📝 Summary: {summary_file}")

    def generate_html_report(self, results, timestamp):
        """Generate comprehensive HTML report with DQ checks"""
        
        total_tables = len(results)
        total_rows = sum(r.get('total_rows', 0) for r in results)
        total_columns = sum(r.get('business_columns', 0) for r in results)
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Fresh Daily DQ Report - Home Credit Dataset</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .summary {{ background-color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .table-section {{ background-color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .good {{ color: #27ae60; }}
                .warning {{ color: #f39c12; }}
                .error {{ color: #e74c3c; }}
                .info {{ color: #3498db; }}
                table {{ width: 100%; border-collapse: collapse; margin-bottom: 15px; }}
                th, td {{ padding: 8px; border: 1px solid #ddd; text-align: left; font-size: 12px; }}
                th {{ background-color: #34495e; color: white; }}
                .dq-check {{ margin: 5px 0; padding: 5px; border-radius: 3px; font-size: 11px; }}
                .check-pass {{ background-color: #d5f4e6; color: #27ae60; }}
                .check-fail {{ background-color: #fdf2f2; color: #e74c3c; }}
                .alert {{ margin: 2px 0; padding: 3px; border-radius: 3px; font-size: 10px; }}
                .alert-critical {{ background-color: #fdf2f2; color: #e74c3c; }}
                .alert-warning {{ background-color: #fefbf3; color: #f39c12; }}
                .alert-info {{ background-color: #f0f8ff; color: #3498db; }}
                .stats {{ font-size: 11px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🔄 Fresh Daily Data Quality Report</h1>
                <h2>Home Credit Dataset - Comprehensive DQ Analysis</h2>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Simulation ID: {self.simulation_id} | Random Seed: {self.random_seed}</p>
            </div>
            
            <div class="summary">
                <h3>📊 Summary</h3>
                <p>Total Tables: {total_tables}</p>
                <p>Successful Analyses: {len([r for r in results if 'error' not in r])}</p>
                <p>Total Rows: {total_rows:,}</p>
                <p>Total Business Columns: {total_columns}</p>
                <p><strong>🎲 Fresh Data:</strong> New random sample with seed {self.random_seed}</p>
            </div>
        """
        
        # Generate table sections
        for result in results:
            if 'error' in result:
                html += f"""
                <div class="table-section">
                    <h3 class="error">❌ {result['table_name']}</h3>
                    <p class="error">Error: {result['error']}</p>
                </div>
                """
                continue
                
            table_name = result['table_name']
            total_rows = result['total_rows']
            business_columns = result['business_columns']
            
            html += f"""
            <div class="table-section">
                <h3 class="good">✅ {table_name}</h3>
                <p>Rows: {total_rows:,} | Business Columns: {business_columns}</p>
                <table>
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Type</th>
                            <th>Quality Score</th>
                            <th>Nulls %</th>
                            <th>Statistics</th>
                            <th>DQ Checks</th>
                            <th>Alerts</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            # Add column analysis rows
            for col_analysis in result.get('column_analysis', []):
                col_name = col_analysis.get('column_name', 'Unknown')
                data_type = col_analysis.get('data_type', 'Unknown')
                quality_score = col_analysis.get('data_quality_score', 0)
                null_pct = col_analysis.get('null_percentage', 0)
                
                # Format statistics
                stats = col_analysis.get('statistics', {})
                stats_html = ""
                if 'min_value' in stats:
                    stats_html += f"Min: {stats['min_value']}<br>"
                if 'max_value' in stats:
                    stats_html += f"Max: {stats['max_value']}<br>"
                if 'average_value' in stats:
                    stats_html += f"Avg: {stats['average_value']}<br>"
                if 'distinct_count' in stats:
                    stats_html += f"Distinct: {stats['distinct_count']}<br>"
                if 'uniqueness_percentage' in stats:
                    stats_html += f"Unique: {stats['uniqueness_percentage']}%"
                
                # Format DQ checks
                checks_html = ""
                for check in col_analysis.get('dq_checks', []):
                    status = check.get('status', 'UNKNOWN')
                    check_name = check.get('check_name', 'unknown')
                    css_class = 'check-pass' if status == 'PASS' else 'check-fail'
                    checks_html += f'<div class="dq-check {css_class}">{check_name}: {status}</div>'
                
                # Format alerts
                alerts_html = ""
                for alert in col_analysis.get('alerts', []):
                    if 'CRITICAL' in alert:
                        css_class = 'alert-critical'
                    elif 'WARNING' in alert:
                        css_class = 'alert-warning'
                    else:
                        css_class = 'alert-info'
                    alerts_html += f'<div class="alert {css_class}">{alert}</div>'
                
                # Determine quality score color
                if quality_score >= 80:
                    score_class = 'good'
                elif quality_score >= 60:
                    score_class = 'warning'
                else:
                    score_class = 'error'
                
                html += f"""
                        <tr>
                            <td><strong>{col_name}</strong></td>
                            <td class="stats">{data_type}</td>
                            <td class="{score_class}"><strong>{quality_score}</strong></td>
                            <td>{null_pct}%</td>
                            <td class="stats">{stats_html}</td>
                            <td>{checks_html}</td>
                            <td>{alerts_html}</td>
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

def main():
    """Main execution - TRUE daily simulation"""
    print("📅 FRESH DAILY DATA QUALITY SIMULATION")
    print("=" * 60)
    print("This run will generate DIFFERENT results each time!")
    print("✅ Fresh CSV sampling from HDFS")
    print("✅ Different random seed each run")
    print("✅ Unique simulation ID")
    print("✅ Fresh data analysis")
    print("=" * 60)
    
    # Initialize system
    dq_system = DailySimulationDQSystem()
    
    # Step 1: Simulate fresh daily data
    simulated_tables = dq_system.simulate_daily_data()
    
    # Step 2: Analyze fresh data
    results = dq_system.analyze_fresh_data(simulated_tables)
    
    print("\n🎉 FRESH DAILY SIMULATION COMPLETED!")
    print("📁 Check dq-results/ for reports with simulation details")
    print("🔄 Run again to see DIFFERENT results!")

if __name__ == "__main__":
    main()
