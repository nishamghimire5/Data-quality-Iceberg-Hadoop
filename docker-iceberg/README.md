# Home Credit Data Quality System

**Complete Documentation**: See the root [`README.md`](../README.md) for comprehensive user guide and technical documentation.

---

## Quick Start

```bash
# Start environment
docker-compose up -d

# Run analysis
docker exec -it spark-iceberg python /opt/spark/daily_simulation_dq_system.py

# Export Parquet files from HDFS to local warehouse (for DQOps)
docker cp export_hdfs_to_local.py spark-iceberg:/opt/spark/
docker exec -it spark-iceberg python /opt/spark/export_hdfs_to_local.py

# Copy reports
docker cp spark-iceberg:/opt/spark/dq-results/. ./dq-results/
```

---

**For complete instructions, troubleshooting, sample output, and technical details:**

**[Main README](../README.md)** - Complete system documentation

## Data Drift Detection

> **Note:** All commands below should be run from the project root directory (`N:\Projects\task2`) to ensure paths work correctly.

You can detect and visualize data drift between daily analysis results using the `calculate_data_drift.py` script.

### Batch Mode (Recommended)

To compare all consecutive daily analysis files:

```sh
python docker-iceberg/calculate_data_drift.py --batch
```

- Compares all consecutive `fresh_daily_analysis_*.json` files in `docker-iceberg/dq-results/`.
- Outputs drift reports as both `.json` and `.html` in `docker-iceberg/dq-results/data_drift/`.
- The HTML report provides a human-friendly summary of all detected drifts.

### Single Comparison

To compare two specific files:

```sh
python docker-iceberg/calculate_data_drift.py <old_json> <new_json> <output_json>
```

Example:

```sh
python docker-iceberg/calculate_data_drift.py docker-iceberg/dq-results/fresh_daily_analysis_20250618_051929.json docker-iceberg/dq-results/fresh_daily_analysis_20250627_074741.json docker-iceberg/dq-results/data_drift/data_drift_20250618_051929_vs_20250627_074741.json
```

- Also generates an HTML report with the same base name as the JSON output.

### Viewing Results

Open the generated HTML files in your browser to easily review and share data drift findings.
