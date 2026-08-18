# Airflow ETL Orchestration Platform

Apache Airflow-based data orchestration system for T24 banking data pipeline with multi-stage processing (sync → parse → model).

## 📋 Overview

**Purpose:** Automated daily/hourly data import from T24 banking system via SFTP → Processing (parse/transform) → Publication to BI/Reporting

**Tech Stack:**
- Apache Airflow 2.x (orchestration)
- Apache Spark 3.5.1 (distributed file sync)
- dbt (data transformation, modeling)
- PostgreSQL (ETL logging & state management)
- MinIO (object storage for intermediate data)
- Apache Iceberg (OLAP table format)
- Kubernetes (Spark execution)

## 🏗️ Architecture

### Active Pipelines (5)

| Pipeline | Trigger | Data Source | Processing | Output |
|----------|---------|-------------|-----------|--------|
| **Credit** | Daily manual | SFTP `/FromT24/CREDIT/` | Spark parse + dbt model | Iceberg + Gold reports |
| **Treasury Remittance** | Daily manual | SFTP `/FromT24/TREASURY/` (CSV) | No-op parser + dbt model | Remittance reports |
| **Treasury Liquidity** | Daily manual | Accounting model output | dbt model only (depends on accounting) | Liquidity analysis |
| **AML V2** | Manual trigger OR hourly | SFTP `/DW.EXPORT/AML/` | Local Python parsers (5 report types) + publish to PBIRS | PBIRS dashboards |
| **CRB Deposits** | Manual trigger | MinIO `raw/crb/` (CRB fixed-width) | Spark parse → Iceberg bronze | `hive.bronze.crb_deposits` |

### Data Flow Pattern (All Pipelines)
```
[Extract Date Range] → [Sync SFTP→MinIO] → [Detect Candidates] → [Insert Logs] → [Parse/Model] → [Update Logs]
```

## 🚀 Quick Start

### Manual Pipeline Trigger

```bash
# Trigger with date range (JSON parameters)
airflow dags trigger credit_pipeline_parser_dag \
  --conf '{"START_DATE":"2026-04-01","END_DATE":"2026-04-01"}'

# Trigger AML V2 for specific dates
airflow dags trigger aml_v2_pipeline \
  --conf '{"START_DATE":"2026-04-01","END_DATE":"2026-04-03"}'

# Trigger without params (uses auto-detect: execution_date or current date)
airflow dags trigger treasury_remittance_pipeline_dag
```

## 📁 Project Structure

```
.
├── dags/                              # Airflow DAG definitions
│   ├── credit_pipeline_parser_dag.py
│   ├── treasury_remittance_pipeline_dag.py
│   ├── treasury_remittance_model_dag.py
│   ├── treasury_liquidity_model_dag.py
│   └── aml_v2_pipeline_dag.py
│
├── scripts/sync/                      # Spark sync scripts (SFTP → MinIO)
│   ├── credit_sync_spark.py
│   ├── aml_v2_sync_spark.py
│   ├── treasury_remittance_sync_spark.py
│   └── (others)
│
├── scripts/parser/                    # Spark parser scripts (raw → Iceberg bronze)
│   ├── accounting_parser_spark.py
│   ├── crb_deposits_parser.py         # CRB GL Balance Details → hive.bronze.crb_deposits
│   └── (others)
│
├── spark-app/sync/                    # Spark job YAML configs (sync jobs)
│   ├── credit_sync_spark.yaml
│   ├── aml_v2_sync_spark.yaml
│   └── (others)
│
├── spark-app/parser/                  # Spark job YAML configs (parser jobs)
│   ├── accounting_parser_spark.yaml
│   ├── crb_deposits_parser_spark.yaml # CRB deposits parser job
│   └── (others)
│
├── ACCOUNTING_REPORTS/                # dbt project (accounting)
├── CREDIT_REPORTS/                    # dbt project (credit)
├── TREASURY_REPORTS/                  # dbt project (treasury)
├── AML_REPORTS_V2/                    # Python parsers (5 report types)
│   ├── parser_9k.py
│   ├── parser_aml03.py
│   ├── parser_aml05.py
│   ├── parser_aot.py
│   ├── parser_fcm.py
│   └── push_to_pbirs.py              # Publish to BI system
│
├── DATABASE_SCHEMA.md                 # PostgreSQL DDL for all tracking tables
├── DATA_FLOW_GUIDE.md                 # Detailed data flow per pipeline
├── SFTP_FOLDER_STRUCTURE.md           # SFTP path conventions
├── SFTP_STRUCTURE_EXAMPLES.md         # Real data examples
└── PIPELINE_IMPLEMENTATION_GUIDE.md   # Architecture patterns & best practices
```

## ⚙️ Configuration

### Required Airflow Variables

```bash
# SFTP Configuration
SFTP_HOST="t24-server.company.com"
SFTP_USER="etl_user"
SFTP_PASS="secure_password"

# MinIO Configuration
MINIO_ENDPOINT="10.0.40.121:9000"        # ⚠️ HOST:PORT ONLY (no protocol/path)
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_SECURE="False"

# Kubernetes
SPARK_NAMESPACE="spark-jobs"

# PBIRS (Power BI Reporting Services) - AML V2 only
PBIRS_BASE_URL="http://10.0.40.121/reports"
PBIRS_USERNAME="pbirs_user"
PBIRS_PASSWORD="secure_password"
```

### Required PostgreSQL Tables

Run all `CREATE TABLE` statements from `DATABASE_SCHEMA.md`:
```bash
psql -U airflow_user -d airflow_db -f /path/to/DATABASE_SCHEMA.md
```

Tables include:
- `etl_parsed_logs_*` (sync/parse status per pipeline)
- `etl_model_logs_*` (dbt model build status per pipeline)

## 📊 Data Flow Examples

### Credit Pipeline (Daily)
```
SFTP: /FromT24/CREDIT/2026-04-01/*.csv
  ↓ (sync via Spark)
MinIO: s3a://raw/credit/2026-04-01/ + _SUCCESS marker
  ↓ (detect → parse)
Iceberg: hive.bronze.credit__* (3 tables)
  ↓ (dbt model)
Iceberg: hive.silver.credit__* + hive.gold.credit_reports
  ↓ (update logs)
PostgreSQL: etl_parsed_logs_credit, etl_model_logs_credit
```

### CRB Deposits Parser
```
MinIO: s3a://raw/crb/{filename}   ← upload file CRB thủ công per branch
  ↓ (Spark parse)
Iceberg: hive.bronze.crb_deposits (partitioned by load_date)
  - deposit_type: demand / time / passbook
  - gl_line: 4310, 4320, 4340, 4410, 4450–4520
  - account_number, branch_code, branch_name, product_code, product_name
  - local_ccy_amt, int_rate, value_date, mat_date
  - 2-tier reconciliation: per GL section vs TOTAL FOR + group totals
```

**Required Airflow Variables (CRB):**
```
SPARK_CRB_PARSER_DRIVER_CORES, SPARK_CRB_PARSER_DRIVER_CORE_REQUEST
SPARK_CRB_PARSER_DRIVER_CORE_LIMIT, SPARK_CRB_PARSER_DRIVER_MEMORY
SPARK_CRB_PARSER_EXECUTOR_CORES, SPARK_CRB_PARSER_EXECUTOR_CORE_REQUEST
SPARK_CRB_PARSER_EXECUTOR_CORE_LIMIT, SPARK_CRB_PARSER_EXECUTOR_INSTANCES
SPARK_CRB_PARSER_EXECUTOR_MEMORY
```

### AML V2 Pipeline (Manual Trigger)
```
Manual trigger: airflow dags trigger aml_v2_pipeline --conf '{"START_DATE":"...", "END_DATE":"..."}'
  ↓ (sync via Spark)
SFTP: /DW.EXPORT/AML/2026-04-01/{9k,aml03,aml05,aot,fcm}.xlsx
  ↓
MinIO: s3a://raw/aml_input/2026-04-01/ + _SUCCESS marker
  ↓ (detect → local Python parser)
5 report types (9k, aml03, aml05, aot, fcm) → MinIO: s3a://raw/aml_output/2026-04-01/
  ↓ (push to PBIRS)
PBIRS Dashboards (/reports/powerbi/...)
  ↓ (update logs)
PostgreSQL: etl_parse_logs_aml_v2, etl_publish_logs_aml_v2
```

## 📖 Documentation

- **[libs/t24_parser/README.md](libs/t24_parser/README.md)** ⭐ **T24 XMLRECORD Parser Tool (kiến trúc v1.4 — DB-only sourcing)**
  - Parser metadata-driven cho MỌI bảng T24 (RECID+XMLRECORD → Bronze Iceberg typed/arrays)
  - Dùng chung cho CDC streaming (Debezium→Kafka→Spark) và JDBC pull; e2e 20/20 + verify 8K so sánh
  - ⚠️ Các pipeline SFTP bên dưới thuộc kiến trúc CŨ (v1.2/v1.3) — đang được thay thế dần theo `arch/BNCTL_DWH_System_Architecture.md`

- **[PIPELINE_IMPLEMENTATION_GUIDE.md](PIPELINE_IMPLEMENTATION_GUIDE.md)** ← START HERE
  - Architecture patterns, Airflow 2.x best practices, date range filtering, error handling
  
- **[DATA_FLOW_GUIDE.md](DATA_FLOW_GUIDE.md)**
  - Detailed flow per pipeline with real examples
  
- **[SFTP_FOLDER_STRUCTURE.md](SFTP_FOLDER_STRUCTURE.md)**
  - SFTP directory conventions and file naming
  
- **[DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)**
  - All PostgreSQL DDL for ETL logging tables

## 🔍 Debugging

### Check DAG Status
```bash
airflow dags list
airflow dags test credit_pipeline_parser_dag 2026-04-01
```

### View Task Logs
```bash
airflow tasks logs credit_pipeline_parser_dag detect_parse_candidates 2026-04-01T12:00:00
```

### Query ETL Logs
```sql
-- Check which dates have been parsed
SELECT cob_date, is_parsed, error_message FROM etl_parsed_logs_credit ORDER BY cob_date DESC LIMIT 10;

-- Check model build status
SELECT cob_date, is_built, dbt_run_id FROM etl_model_logs_credit WHERE is_built = false;
```

## 📝 License & Support

Internal project maintained by Data Engineering team.
