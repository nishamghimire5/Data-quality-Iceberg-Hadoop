import json
import sys
import os
import glob
from typing import Dict, Any

def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_table_map(analysis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {t['table_name']: t for t in analysis['table_results']}

def get_column_map(table: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c['column_name']: c for c in table['column_analysis']}

def calc_percent_change(old, new):
    if old == 0:
        return None if new == 0 else float('inf')
    return ((new - old) / old) * 100

def extract_numeric_fields(col: Dict[str, Any]) -> Dict[str, float]:
    # Extract all numeric fields from both top-level and 'statistics' if present
    numeric = {}
    for k, v in col.items():
        if isinstance(v, (int, float)):
            numeric[k] = v
    if 'statistics' in col and isinstance(col['statistics'], dict):
        for k, v in col['statistics'].items():
            if isinstance(v, (int, float)):
                numeric[k] = v
    return numeric

def compare_columns(col_old, col_new):
    drift = {}
    old_stats = extract_numeric_fields(col_old)
    new_stats = extract_numeric_fields(col_new)
    for stat in old_stats:
        if stat in new_stats:
            drift[stat] = {
                'old': old_stats[stat],
                'new': new_stats[stat],
                'percent_change': calc_percent_change(old_stats[stat], new_stats[stat])
            }
    return drift

def compare_tables(table_old, table_new):
    drift = {}
    cols_old = get_column_map(table_old)
    cols_new = get_column_map(table_new)
    for col_name in cols_old:
        if col_name in cols_new:
            col_drift = compare_columns(cols_old[col_name], cols_new[col_name])
            if col_drift:
                drift[col_name] = col_drift
    return drift

def generate_html_report(drift_report, html_path):
    html = [
        '<html><head><title>Data Drift Report</title>',
        '<style>body{font-family:sans-serif;}table{border-collapse:collapse;}th,td{border:1px solid #ccc;padding:4px;}th{background:#eee;} .neg{color:red;} .pos{color:green;}</style>',
        '</head><body>',
        '<h1>Data Drift Report</h1>'
    ]
    if 'message' in drift_report:
        html.append(f'<p>{drift_report["message"]}</p>')
    else:
        for table, columns in drift_report.items():
            html.append(f'<h2>Table: {table}</h2>')
            for col, stats in columns.items():
                html.append(f'<h3>Column: {col}</h3>')
                html.append('<table><tr><th>Statistic</th><th>Old</th><th>New</th><th>% Change</th></tr>')
                for stat, vals in stats.items():
                    pc = vals.get('percent_change')
                    pc_str = ''
                    if pc is not None:
                        pc_str = f'<span class="neg">{pc:.2f}%</span>' if pc < 0 else f'<span class="pos">{pc:.2f}%</span>'
                    html.append(f'<tr><td>{stat}</td><td>{vals.get("old")}</td><td>{vals.get("new")}</td><td>{pc_str}</td></tr>')
                html.append('</table>')
    html.append('</body></html>')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html))
    print(f"HTML report written to {html_path}")

def calculate_drift(old_path, new_path, output_path):
    old_data = load_json(old_path)
    new_data = load_json(new_path)
    tables_old = get_table_map(old_data)
    tables_new = get_table_map(new_data)
    drift_report = {}
    for table_name in tables_old:
        if table_name in tables_new:
            table_drift = compare_tables(tables_old[table_name], tables_new[table_name])
            if table_drift:
                drift_report[table_name] = table_drift
    if not drift_report:
        drift_report = {"message": "No drift detected or no comparable columns found."}
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(drift_report, f, indent=2)
    print(f"Drift report written to {output_path}")
    # Generate HTML
    html_path = output_path.replace('.json', '.html')
    generate_html_report(drift_report, html_path)

def extract_datetime_from_filename(filename):
    # Expects fresh_daily_analysis_YYYYMMDD_HHMMSS.json
    base = os.path.basename(filename)
    parts = base.replace('.json','').split('_')
    if len(parts) >= 5:
        return parts[3] + '_' + parts[4]
    return base

def batch_drift(input_dir):
    pattern = os.path.join(input_dir, 'fresh_daily_analysis_*.json')
    files = sorted(glob.glob(pattern), key=lambda x: extract_datetime_from_filename(x))
    if len(files) < 2:
        print("Not enough files to compare.")
        return
    output_dir = os.path.join(input_dir, 'data_drift')
    os.makedirs(output_dir, exist_ok=True)
    for i in range(len(files) - 1):
        old_path = files[i]
        new_path = files[i+1]
        date1 = extract_datetime_from_filename(old_path)
        date2 = extract_datetime_from_filename(new_path)
        output_path = os.path.join(output_dir, f'data_drift_{date1}_vs_{date2}.json')
        print(f"Comparing {old_path} -> {new_path}")
        calculate_drift(old_path, new_path, output_path)

def main():
    if len(sys.argv) == 2 and sys.argv[1] in ('--batch', '--all'):
        batch_drift('docker-iceberg/dq-results')
        return
    if len(sys.argv) != 4:
        print("Usage: python calculate_data_drift.py <old_json> <new_json> <output_json>")
        print("   or: python calculate_data_drift.py --batch")
        sys.exit(1)
    old_path, new_path, output_path = sys.argv[1:4]
    if not os.path.exists(old_path) or not os.path.exists(new_path):
        print("Input file(s) not found.")
        sys.exit(1)
    calculate_drift(old_path, new_path, output_path)

if __name__ == "__main__":
    main() 