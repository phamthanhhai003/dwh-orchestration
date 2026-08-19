# DWH — Orchestration & Transformation

Pipeline dữ liệu **COB (Close of Business)** cho core banking Temenos T24: đưa dữ liệu từ MSSQL và
SFTP vào Hive Iceberg, gate theo chu kỳ COB thật của T24, rồi build lên Silver data mart và Gold
report phục vụ 5 nghiệp vụ — Accounting, Credit, AML, Operational, Treasury.

> 📐 **[Tài liệu kiến trúc đầy đủ → `arch/DWH_System_Architecture.md`](arch/DWH_System_Architecture.md)**
>
> 🏗️ Tầng hạ tầng K8s (Kafka/Strimzi, MinIO, Dremio, Hive Metastore, Airflow, Jenkins, ELK) ở repo
> riêng: **[dwh-deployments](https://github.com/phamthanhhai003/dwh-deployments)**

---

## Bài toán

T24 chạy COB mỗi đêm. Trong lúc COB chạy, bảng nghiệp vụ vẫn biến động và CDC stream vẫn nhận
event — nếu dbt model đọc Bronze ở thời điểm tuỳ ý thì báo cáo ngày D có thể lẫn giao dịch ngày
D+1, và chạy lại 2 lần ra 2 kết quả khác nhau.

Pipeline này giải quyết bằng 3 nguyên tắc:

| Nguyên tắc | Cách làm |
|---|---|
| **`business_date` từ marker thật, không từ lịch** | Đọc record `BNK/COB.INITIALISE` trong `hive.bronze.t24_batch`; `process_status = 0` ⟹ COB xong. Một hàm `cob_marker.cob_done()` dùng chung mọi DAG. |
| **Seal trước, đọc sau** | Nguồn nào xong cho ngày D thì `sealed` vào Postgres `etl_control`. Model chỉ chạy khi *đúng source-set của nó* sealed — không chờ thừa, không chạy sớm. |
| **Ghim snapshot thay vì khoá bảng** | Seal luồng CDC kèm `iceberg_snapshot_id`; dbt đọc `AT SNAPSHOT '{{ var("snap_x") }}'`. Kết quả tái lập được, CDC vẫn ghi tiếp bình thường. |

---

## Kiến trúc

```
T24 MSSQL ──┬─ CDC (25 bảng)  Debezium → Kafka → Spark Streaming ──┐
            └─ PULL (9 bảng)  bcp → MinIO → Spark batch parse ─────┤
SFTP HOLD ───  5 report CRF/CRB/CRC, manifest-driven fetch ────────┤
                                                                   ▼
                                        BRONZE  hive.bronze.*  (Iceberg / MinIO)
                                                   │  cob_gate: seal + pin snapshot
                                                   ▼
                                        SILVER  hive.silver.t24_*   11 ENQ mart
                                                   ▼
                                        GOLD    hive.gold.*         41 report models
                                                   ▼
                                             Dremio → BI
```

| | Con số |
|---|---|
| Airflow DAG | 14 objects từ 9 file (3 ingest · 1 silver · 6 gold factory · 4 ops/benchmark) |
| Bảng T24 ingest | 25 CDC · 9 pull · 5 report SFTP |
| dbt model | 11 silver ENQ + 41 gold model, 6 project |
| Bảng control | 4 (`etl_control`, `etl_model_runs`, `etl_stream_progress`, `etl_ingest_logs`) |

---

## Stack

Apache Airflow 3.0.2 · Spark 3.5.1 (Structured Streaming + batch, Spark Operator trên K8s) ·
Apache Iceberg trên MinIO S3A với Hive Metastore catalog · Dremio · dbt (`dbt-dremio`) ·
Debezium + Kafka (Strimzi KRaft) · PostgreSQL · Jenkins + Kaniko

---

## Cấu trúc repo

```
dags/                        Airflow DAG
├── cob_gate_dag.py            INGEST · sensor COB-done → caught-up 25 bảng CDC → pin + seal
├── pull_cob_dag.py            INGEST · bcp extract 9 bảng reference → parse → seal
├── sftp_hold_dag.py           INGEST · manifest-driven fetch 5 report CRF/CRB/CRC → seal
├── build_mart_dag.py          BUILD  · fan-out 11 nhánh → hive.silver.t24_*
├── dbt_model_cob_dag.py       BUILD  · factory sinh 6 DAG gold từ DOMAIN_CONFIGS
├── pull_bulk_dag.py           OPS    · BCP bulk 36 bảng CDC (Day-1 init / benchmark)
├── pull_bulk_fresh_dag.py     OPS    · BCP full snapshot 22 bảng reference
├── pull_jdbc_dag.py           OPS    · đối chứng Spark JDBC vs BCP
├── t24_pull_pipeline_dag.py   OPS    · JDBC pull thủ công, initial load / recovery
└── lib/
    ├── cob_marker.py          nguồn chân lý DUY NHẤT cho business_date
    ├── etl_control.py         API gate: open_pending / seal / all_sealed / all_enqs_built
    ├── enq_sources.py         map ENQ → source-set; derive cdc_union() và pull_union()
    ├── kafka_lag.py           lag nhánh B: kafka_end_offset − spark_committed_offset
    └── dremio.py              Dremio REST: login / query / refresh / latest_snapshot_id

libs/t24_parser/             Parser XMLRECORD metadata-driven (21 unit test)
├── metadata.py                STANDARD.SELECTION → TableSpec
├── core.py                    parse(df, spec) — pure transform, dùng chung CDC + batch
└── sinks.py                   latest_per_recid / split_deletes / write_bronze (idempotent)

jobs/                        Entrypoint chạy trong Spark image
├── t24_streaming_cdc.py       Kafka → foreachBatch(parse) → MERGE Bronze → etl_stream_progress
├── t24_batch_parse.py         Parquet raw → parse → Bronze
└── t24_bcp_extract.py         bcp queryout → pyarrow → Parquet → MinIO

T24_SILVER/                  dbt · 11 ENQ data mart  → hive.silver
T24_ACCOUNTING/              dbt · 22 model (+10 seed CSV)  ┐
T24_CREDIT/                  dbt · 7 model                  │
T24_OPERATIONAL/             dbt · 6 model                  ├─ → hive.gold
T24_AML/                     dbt · 4 model                  │
T24_TREASURY/                dbt · 2 model                  ┘
COB_TEST/                    dbt · demo E2E (mix cả 3 luồng CDC + pull + sftp)

spark-app/                   SparkApplication YAML (extract / parser / sync)
spark/                       Dockerfile.t24 + manifest streaming CDC
kafka/                       Strimzi cluster + Kafka Connect + Debezium MSSQL connector
scripts/cob_pipeline/        DDL bảng control Postgres (idempotent)
arch/                        Tài liệu kiến trúc + báo cáo hiệu năng
dev.Jenkinsfile              CI/CD: dbt lint → Kaniko build → rollout Airflow
```

---

## Thêm nghiệp vụ mới — config-driven

Không phải sửa DAG code:

| Muốn thêm | Sửa đúng 1 chỗ |
|---|---|
| Silver ENQ mới | 1 entry `enq_sources.ENQ_SOURCES` + 1 file `T24_SILVER/models/<enq>.sql` |
| Gold report / division mới | 1 dict `dbt_model_cob_dag.DOMAIN_CONFIGS` + dbt project |
| Bảng pull mới | append vào `pull_cob.TABLES` |
| Bảng CDC mới | append vào `cob_gate.CDC_SOURCES` |

`enq_sources.py` tự derive `cdc_union()` (25 bảng cob_gate phải gate + pin) và `pull_union()`
(9 bảng pull_cob phải extract) từ khai báo ENQ — không maintain 2 list song song. Có
`_validate()` chống lệch classification CDC/pull:

```bash
python dags/lib/enq_sources.py       # VALIDATION: OK + in ra union
```

Hướng dẫn từng bước: [`arch/ADD_DBT_MODEL_TO_COB.md`](arch/ADD_DBT_MODEL_TO_COB.md)

---

## Vận hành

Mọi DAG đều `schedule=None` và neo theo marker COB (production gắn cron cho `cob_gate`).

```bash
# Chạy COB bình thường — cob_gate tự phát hiện D từ marker T24
airflow dags trigger cob_gate

# Replay / rebuild 1 ngày cụ thể: bỏ qua marker + bỏ idempotent guard
airflow dags trigger dbt_aml -c '{"business_date":"2026-06-25"}'

# Kiểm tra gate: nguồn nào đã sealed cho D
psql -d cob_control -c "
  SELECT source_table, flow, status, iceberg_snapshot_id
  FROM etl_control WHERE business_date = '2026-06-25' ORDER BY flow, source_table;"

# Kiểm tra silver/gold đã build chưa
psql -d cob_control -c "
  SELECT domain, enq_name, layer, status, models_built
  FROM etl_model_runs WHERE business_date = '2026-06-25';"
```

Chạy dbt tay:

```bash
cd T24_SILVER
dbt run --select t24_daily_txn_9k \
  --vars '{"business_date":"2026-06-25","snap_account":"7215384021"}' --target dev
```

Test parser:

```bash
cd libs/t24_parser && pip install -e '.[dev]' && pytest -m 'not spark'
```

Runbook E2E: [`arch/COB_E2E_TEST_RUNBOOK.md`](arch/COB_E2E_TEST_RUNBOOK.md) ·
Replay: [`arch/REPLAY_RERUN_GUIDE.html`](arch/REPLAY_RERUN_GUIDE.html)

---

## Hiệu năng (đo thật)

**BCP vs Spark JDBC — extract 100K rows:** `~25s` vs `~68s`, và BCP dùng **0 executor pod**
(1 pod 1 core / 2GB) thay vì 2. Đánh đổi: BCP giữ shared lock khi scan.
→ [`arch/BCP_vs_JDBC_deep_dive.html`](arch/BCP_vs_JDBC_deep_dive.html)

**`pull_bulk` — 36 bảng CDC, DEV (cap 100K rows/bảng):** bottleneck là parse chứ không phải
extract, chi phối bởi XML size/row.

| Bảng | Rows | Size est | Extract | Parse |
|---|---:|---:|---:|---:|
| AAFBNK_AA010 (AA.ACTIVITY.HISTORY) | 100,000 | 4,361 MB | 360s | 4,517s |
| FBNK_ACCOUNT | 100,000 | 1,849 MB | 331s | 3,744s |
| FBNK_CUSTOMER | 100,000 | 496 MB | 24s | 1,492s |

**UAT (full data):** `FBNK_AA_PROCESS_DETAILS` 22.8M rows / 46 GB extract ~93 phút;
`FBNK_CUSTOMER` 619K rows / 3 GB parse ~1h38m.

**`pull_bulk_fresh` — 22 bảng reference, DEV:** 21/22 thành công, wall time **~10m24s**. Spark
submit overhead chiếm ~60–70s/bảng ngay cả với bảng 0 rows.

Chi tiết: [`arch/BCP_PIPELINE_REPORT.html`](arch/BCP_PIPELINE_REPORT.html) ·
[`arch/BCP_FRESH_PIPELINE_REPORT.html`](arch/BCP_FRESH_PIPELINE_REPORT.html) ·
[`arch/CDC_TABLE_CLASSIFICATION.md`](arch/CDC_TABLE_CLASSIFICATION.md) ·
[`arch/DAY1_INIT_LOAD_PLANNING.md`](arch/DAY1_INIT_LOAD_PLANNING.md)

---

## Cấu hình

**Airflow Connections:** `cob_control_conn` (Postgres control plane) · `minio_conn`

**Airflow Variables:** `dremio_host` / `dremio_user` / `dremio_password` · `DBT_TARGET` ·
`SPARK_NAMESPACE` · `kafka_bootstrap` · `cob_checkpoint_prefix` · `SFTP_HOST` / `SFTP_USER` /
`SFTP_PASS` / `REMOTE_DIR` · `BUCKET_NAME`

**K8s Secrets:** `mssql-credentials` · `minio-credentials`

Khởi tạo bảng control:

```bash
psql -d cob_control -f scripts/cob_pipeline/01_postgres_control_tables.sql
```

> 🔐 Không có credential nào hardcode trong repo. Tất cả qua Airflow Variables/Connections hoặc
> K8s Secret. File `kafka/*.example.yaml` là template, secret thật không commit.

---

## Known gaps

Ghi thẳng để người đọc code không mất thời gian đi tìm — chi tiết ở
[mục Known gaps của doc kiến trúc](arch/DWH_System_Architecture.md#known-gaps):

- `scripts/sync/sftp_sync_spark.py` chưa có trong repo (chỉ tồn tại trong image đã build) → build
  image từ repo sạch thì `sftp_hold` không chạy được.
- `F_PL_CLOSE_DATES` thiếu schema trong `ss_full.json` → 1/22 bảng reference parse fail.
- `T24_SILVER/models/` có 15 file nhưng `ENQ_SOURCES` chỉ khai 11 → 4 model orphan.
- `dags/lib/t24_sources.py::CDC_TABLES` stale (19 bảng) — authoritative là `enq_sources.py`.
