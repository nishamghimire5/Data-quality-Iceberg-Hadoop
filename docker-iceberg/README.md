# Home Credit Data Quality System

**Complete Documentation**: See the root [`README.md`](../README.md) for comprehensive user guide and technical documentation.

---

## Quick Start

```bash
# Start environment
docker-compose up -d

# Run analysis
docker cp daily_simulation_dq_system.py spark-iceberg:/opt/spark/
docker exec -it spark-iceberg python /opt/spark/daily_simulation_dq_system.py

# Copy reports
docker cp spark-iceberg:/opt/spark/dq-results/. ./dq-results/
```

---

**For complete instructions, troubleshooting, sample output, and technical details:**

**[Main README](../README.md)** - Complete system documentation
