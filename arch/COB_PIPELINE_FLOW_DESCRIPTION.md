# COB Pipeline — Mô tả Flow End-to-End (dùng để vẽ diagram)

## Tổng quan

Pipeline chạy hàng ngày theo chu kỳ COB (Close of Business). Mỗi ngày D, toàn bộ dữ liệu T24 của ngày đó được ingest vào Bronze, xây dựng thành Silver (data mart), rồi nhặt vào Gold (báo cáo cuối).

---
hay
## Các thành phần chính

### Nguồn dữ liệu (2 nguồn vật lý)
- **T24 MSSQL** (UAT) — dữ liệu bảng nghiệp vụ + marker COB. 2 luồng:
  - **CDC** (in-scope **25 bảng** = `enq_sources.cdc_union()`; xlsx tổng 37): Debezium → Kafka → Spark Streaming → Bronze Iceberg (chạy 24/7)
  - **Pull** (in-scope **9 bảng** = `enq_sources.pull_union()`; xlsx tổng 22): JDBC MSSQL → MinIO raw → Spark parse → Bronze Iceberg (chạy sau COB)
- **SFTP Windows Server — folder HOLD** — file CRB/CRF do T24 COB sinh ra.
  - **SFTP** (CRB): MSSQL view `HOLD_CONSOL` cho **danh sách RECID = tên file** (WHERE `DATE.TIME` = D)
    → fetch đúng các file đó từ folder HOLD → MinIO raw → Spark parse → Bronze Iceberg (chạy sau COB)
  - ⚠️ Luồng SFTP dùng **CẢ MSSQL** (lấy list RECID theo ngày D) **LẪN folder HOLD** (lấy file vật lý).

### Bảng control (Postgres)
| Bảng | Vai trò |
|---|---|
| `etl_stream_progress` | CDC Streaming ghi max_lsn mỗi micro-batch |
| `etl_ingest_logs` | Tracking 2 nhịp sync + parse cho luồng Pull và SFTP |
| `etl_control` | Gate: nguồn ngày D đã sealed (đóng băng) chưa? |
| `etl_model_runs` | Gate: silver ENQ / gold division ngày D xong chưa? |

### Tầng dữ liệu (Dremio / Iceberg)
```
hive.bronze.*  →  hive.silver.*  →  hive.gold.*
  (T24 raw)       (ENQ data mart)   (báo cáo cuối)
```

---

## Flow chi tiết theo thứ tự thời gian

### NHỊP 0 — Luồng CDC (chạy liên tục, không phụ thuộc COB)

```
T24 MSSQL (CDC enabled)
  → Debezium connector (Kafka Connect)
  → Kafka topic (1 topic/bảng)
  → Spark Structured Streaming job
  → write hive.bronze.t24_<table> (Iceberg)
  → upsert etl_stream_progress (table_name, max_lsn)
      [cập nhật liên tục, không liên quan đến ngày D]
```

**25 bảng CDC in-scope** (union các ENQ cần): account, customer, stmt_entry, funds_transfer, category_entry, account_his, ... (`enq_sources.cdc_union()` là nguồn chân lý).

---

### NHỊP 1 — Phát hiện COB xong + pin snapshot CDC (cob_gate_dag)

**Trigger**: DAG `cob_gate_dag` chạy theo cron (PROD: vd "0 1 * * *"). 4 task tuần tự:
`detect_cob → open_pending → wait_caught_up → pin_and_seal`.

```
[Sensor: detect_cob]  (mode=reschedule, poke 2 phút, timeout 6h)
  cob_marker.cob_done() đọc marker COB.INITIALISE qua Dremio:
    process_status = 0  → COB xong (T24: 0=done, 1=running, 2=batch success)
  Trả về (D, L_cob)  — L_cob = LSN mốc COB
  Idempotent: nếu D đã sealed cdc rồi → bỏ qua (COB cũ), chờ COB mới
  Output xcom: business_date=D, l_cob=L_cob
```

```
[Task: open_pending]
  Với mỗi bảng CDC trong CDC_SOURCES:
    ctl.open_pending(D, src, flow='cdc', l_cob=L_cob)
    → etl_control: (D, src, cdc, status='pending', l_cob)
```

```
[Sensor: wait_caught_up]  (mode=reschedule, poke 1 phút, timeout 3h)
  Đọc etl_stream_progress: kiểm tra max_lsn của TỪNG bảng CDC >= L_cob.
  Ý nghĩa: đảm bảo Spark Streaming đã nuốt hết event tới mốc COB
           TRƯỚC khi pin snapshot (nếu không snapshot sẽ thiếu data ngày D).
  Dừng khi tất cả bảng caught_up.
```

```
[Task: pin_and_seal]
  Với mỗi bảng CDC:
    1. snap = dremio.latest_snapshot_id(src)  — snapshot Iceberg tại thời điểm này
    2. ctl.seal(D, src, flow='cdc', snapshot_id=snap)
       → etl_control: (D, src, cdc, status='sealed', iceberg_snapshot_id=snap)
  
  Ý nghĩa: "đóng băng" điểm đọc CDC tại snapshot này cho ngày D.
  Downstream dùng AT SNAPSHOT '<snap>' để đọc đúng dữ liệu ngày D.
```

**Sau pin_and_seal**: `cob_gate_dag` kết thúc. Không trigger downstream trực tiếp — các DAG downstream tự poll `cob_marker` + `etl_control`.

---

### NHỊP 2 — Pull (pull_cob_dag, tự poll cob_marker — chạy song song độc lập)

**Trigger**: Giống SFTP — `get_business_date` (PythonSensor) tự poll `cob_marker` lấy D, không nhận trigger từ `cob_gate_dag`. Idempotent: nếu D đã sealed hết bronze pull → bỏ qua.

Với mỗi bảng pull (in-scope **9 bảng** = `pull_union`; lọc từ `t24_sources.PULL_FULL` để tránh double-ingest), chạy tuần tự:

```
[Sensor: get_business_date]  (poll cob_marker → D, open_pending N bảng)

[Task: sync]
  JDBC kết nối T24 MSSQL
  SELECT * FROM dbo.FBNK_<TABLE>  (full, không watermark)
  → ghi file Parquet vào MinIO: s3://spark-scripts/raw/t24_<table>/D/
  
  Sau sync xong:
  → ctl.mark_sync_done(source_name='hive.bronze.t24_account', flow='pull', date=D)
    INSERT etl_ingest_logs (source_name, flow='pull', D, is_sync=true, synced_at=now())

[Task: parse]
  Spark job đọc MinIO raw/t24_account/D/
  → parse, cast types, thêm cột business_date=D
  → write hive.bronze.t24_account (partition by business_date)
  
  Sau parse xong:
  → ctl.mark_parse_done(source_name='hive.bronze.t24_account', flow='pull', date=D)
    UPDATE etl_ingest_logs SET is_parsed=true, parsed_at=now()
  
  → ctl.seal(D, 'hive.bronze.t24_account', flow='pull', expected=N, actual=N)
    INSERT etl_control (D, source_table, flow='pull', status='sealed', actual_count=N)
```

---

### NHỊP 2b — SFTP/CRB (sftp_crb_dag, tự poll như pull — không nhận trigger)

**Trigger**: Giống `pull_cob_dag` — `detect_sftp` tự poll `cob_marker` lấy D, không nhận trigger từ `cob_gate_dag`. Chạy song song độc lập.

**Ý tưởng chính**: "lấy file nào" do **MSSQL `HOLD_CONSOL` quyết định** (list RECID theo ngày D), folder HOLD trên SFTP chỉ là kho chứa file vật lý. RECID = tên file.

```
[Sensor: detect_sftp]  (mode=reschedule, poke 2 phút, timeout 6h)
  cob_marker.cob_done() → (D, l_cob) khi process_status=0
  Nếu COB chưa xong → return False (reschedule)
  Idempotent: get_ingest_log(pg,'crb','sftp',D) — nếu is_parsed=True → return False
  push xcom business_date=D; ctl.open_pending(pg, D, BRONZE, flow='sftp')

[Task: get_recids]   (mới)
  Query MSSQL view T24:
    SELECT RECID FROM HOLD_CONSOL WHERE "DATE.TIME" = D
  → ra danh sách RECID (= tên file cần lấy). Push xcom: recids=[...]
  Nếu list rỗng → không có file CRB cho D (xử lý: seal 0 hoặc skip tùy nghiệp vụ)

[Task: sync]
  Vào SFTP folder HOLD, nhặt ĐÚNG các file có tên khớp list RECID
    → copy về MinIO s3://spark-scripts/raw/crb/D/
  → ctl.mark_sync_done('crb', 'sftp', D)
    INSERT etl_ingest_logs (crb, sftp, D, is_sync=true)

[Task: parse]
  Spark đọc MinIO raw/crb/D/ → parse CRB file format
  → write hive.bronze.crb_deposits (partition by load_date=D)

[Task: reconcile_seal]
  Đếm rows trong hive.bronze.crb_deposits WHERE load_date=D
  Đối chiếu số file lấy được vs số RECID kỳ vọng (từ get_recids)
  → ctl.mark_parse_done('crb', 'sftp', D)
    UPDATE etl_ingest_logs SET is_parsed=true
  → ctl.seal(D, 'hive.bronze.crb_deposits', flow='sftp', expected=len(recids), actual=N)
    INSERT etl_control (D, crb_deposits, sftp, sealed)
```

> Khác bản cũ: KHÔNG còn "list folder tìm folder ngày D" — D + danh sách file đến từ
> MSSQL `HOLD_CONSOL`. Nên SFTP cần CẢ MSSQL (list RECID) LẪN folder HOLD (file).

---

### NHỊP 3 — Build Silver / ENQ Data Mart (build_mart_dag — 1 DAG fan-out)

**Mô hình hiện tại**: **1 DAG `build_mart`** (mirror `pull_cob`): 1 sensor COB+gate chung →
**fan-out per-ENQ** (`prepare_vars_<enq> → dbt_run_<enq> → finish_<enq>`), `max_active_tasks=4`.
Build **10 ENQ in-scope** (`enq_sources.ENQ_SOURCES`) trong 1 project **`T24_SILVER`** (chứa 14
model: 10 in-scope + 4 orphan KHÔNG được `--select`). Mỗi nhánh ghi `etl_model_runs` riêng +
skip-if-built riêng (granularity per-ENQ giữ nguyên, chỉ gom về 1 DAG cho gọn).

> 📌 *Design cũ (rollback)*: từng là factory **mỗi ENQ 1 DAG độc lập (14 DAG)** — mỗi DAG poll
> source-set riêng. Đổi sang 1-DAG-fan-out để bớt clutter Airflow; gate-per-ENQ vẫn còn ở tầng task.

**Trigger**: 1 PythonSensor (`_detect_and_gate`) = COB-done + `all_sealed(ALL_SOURCES=25 cdc+9 pull)`.
Idempotent: mọi ENQ đã built cho D → chờ COB sau.

Ví dụ nhánh ENQ `t24_daily_txn_9k` (cần `t24_stmt_entry` CDC):

```
[Sensor: _detect_and_gate]  (chung cho cả DAG)
  cob_marker.cob_done() → D; ctl.all_sealed(pg, D, ALL_SOURCES) → đủ 25 cdc + 9 pull sealed

[Task: prepare_vars_<enq>]
  domain = ENQ_SOURCES[enq]["division"]
  snaps = ctl.sealed_snapshots(pg, D, cdc_dim_của_ENQ)   # chỉ dim cần AT SNAPSHOT
  Build --vars JSON (snap_<dim> + business_date=D + target_date=D)
  ctl.start_enq_run(pg, domain, enq, D)  → etl_model_runs (domain, enq, silver, D, running)

[Task: dbt_run_<enq>]  dbt run --project T24_SILVER --select t24_daily_txn_9k --vars ...
  SQL trong model:
    FROM hive.bronze.t24_stmt_entry AT SNAPSHOT 'snap_xyz'  (CDC dim → AT SNAPSHOT)
    [WHERE <datecol> = business_date]                       (CDC event/transaction → WHERE D)
    [JOIN bảng pull ... WHERE business_date = D]            (pull → WHERE D)
  → write hive.silver.t24_daily_txn_9k  (+ stamp business_date=D đồng nhất)

[Task: finish_<enq>]
  ctl.finish_enq_run(pg, domain, enq, D, status='success')
    → etl_model_runs (domain, enq, silver, D, success)
```

**Quy ước đọc nguồn trong model SQL (theo loại bảng):**
- CDC **dim/current-state** (account, customer) → `FROM ... AT SNAPSHOT '{{ var("snap_<x>") }}'` (pin ở NHỊP 1).
- CDC **event/transaction** (stmt_entry) → `WHERE <datecol> = business_date` (chọn đúng ngày COB).
- Bảng **pull/sftp** → `WHERE business_date = date '{{ var("business_date") }}'`.
- **10 ENQ in-scope**: acct_cust, ac_gic_close, ft_inremit, inactive_account_report,
  no_legal_doc_cus_report, daily_txn_9k, e_account_open, cris_report, find_arrangement_al_bnctl,
  aa_wof_loans_report. (4 model orphan trong T24_SILVER — ac_locked_amt, categ_ent_book,
  cui_stmt01, stmt_ent_book_cui — chưa gắn ENQ, KHÔNG build.)

---

### NHỊP 4 — Build Gold / Báo cáo cuối (dbt_model_cob_dag)

**Trigger**: PythonSensor tự poll etl_model_runs (không cần trigger từ nhịp trước)

```
[Sensor: wait_enqs_built]
  Poll etl_model_runs mỗi 1 phút:
    ctl.all_enqs_built(pg, 'aml', [
      't24_inactive_account_report',
      't24_no_legal_doc_cus_report',
      't24_e_account_open',
      't24_daily_txn_9k',
    ], D)
  Dừng khi tất cả 4 ENQ = success

[Task: skip_if_built]  ShortCircuit
  ctl.is_model_built(pg, 'aml', D) → True = skip

[Task: dbt_run]  dbt run T24_AML project
  ctl.start_model_run(pg, 'aml', D)
    → etl_model_runs: (aml, '', gold, D, running)
  
  dbt run --project-dir T24_AML --vars '{business_date: D}'
  
  SQL trong mỗi model (ví dụ rpt_aml03):
    SELECT branch_name, account_number, ...
    FROM hive.silver.t24_inactive_account_report
    WHERE business_date = date 'D'
  → write hive.gold.rpt_aml03_inactive_accounts
  → write hive.gold.rpt_aml05_no_legal_doc
  → write hive.gold.rpt_aot_accounts_opened
  → write hive.gold.rpt_9k_txn
  
  ctl.finish_model_run(pg, 'aml', D, status='success')
    → etl_model_runs: (aml, '', gold, D, success)
```

---

## Trạng thái các bảng log sau khi ngày D hoàn tất

### etl_stream_progress (cập nhật liên tục)
```
table_name              max_lsn         updated_at
t24_account             0x00A1B2...     2026-06-22 18:45:00
t24_customer            0x00A1B3...     2026-06-22 18:45:01
...
```

### etl_ingest_logs
```
source_name                      flow   date        is_sync  is_parsed
hive.bronze.t24_account          pull   2026-06-22  true     true
hive.bronze.t24_customer         pull   2026-06-22  true     true
...9 pull tables (pull_union)...
crb                              sftp   2026-06-22  true     true
```

### etl_control
```
business_date  source_table                    flow  status  iceberg_snapshot_id  actual_count
2026-06-22     hive.bronze.t24_account         cdc   sealed  snap_abc123          null
2026-06-22     hive.bronze.t24_customer        cdc   sealed  snap_def456          null
...25 CDC rows (cdc_union)...
2026-06-22     hive.bronze.t24_company         pull  sealed  null                 121439
2026-06-22     hive.bronze.t24_aa_product      pull  sealed  null                 0
...9 pull rows (pull_union)...
2026-06-22     hive.bronze.crb_deposits        sftp  sealed  null                 450
```

### etl_model_runs
```
domain  enq_name                        layer   business_date  status
aml     t24_inactive_account_report     silver  2026-06-22     success
aml     t24_no_legal_doc_cus_report     silver  2026-06-22     success
aml     t24_e_account_open              silver  2026-06-22     success
aml     t24_daily_txn_9k               silver  2026-06-22     success
aml     (empty string)                  gold    2026-06-22     success
```

---

## Sơ đồ phụ thuộc giữa các DAG

```
T24 MSSQL ─[CDC 24/7]─► bronze + marker t24_batch + etl_stream_progress
   │
   ├─ cob_gate_dag   (poll t24_batch → wait_caught_up đọc etl_stream_progress → pin+seal cdc)
   ├─ pull_cob_dag   (poll t24_batch → sync→parse→seal pull)
   └─ sftp_crb_dag   (poll t24_batch → get_recids từ MSSQL HOLD_CONSOL → sync file HOLD → parse→seal)
        ▲ 3 DAG độc lập, KHÔNG trigger nhau, cùng poll marker t24_batch
        │
        ▼  (đều seal vào etl_control)
   bronze.*
        │
        ▼
   build_mart      ── 1 DAG fan-out 10 ENQ (sensor: cob_done + all_sealed 25 cdc+9 pull)
   (silver / ENQ)    ── dbt T24_SILVER --select per-ENQ ──► hive.silver.*  + finish_enq_run → etl_model_runs
        │
        ▼
   dbt_model_cob_dag ── per division (sensor etl_model_runs: all_enqs_built các ENQ division cần)
   (gold / reports)   ── dbt T24_AML ──────► hive.gold.*    + finish_model_run → etl_model_runs
```

**Lưu ý 2 bảng dễ nhầm** (cùng do CDC sinh ra nhưng vai trò khác):

| Bảng | Ai đọc | Dùng để |
|---|---|---|
| `t24_batch` (marker COB) | **cả 3 DAG ingest** | "COB ngày D xong chưa?" → lấy D |
| `etl_stream_progress` (max_lsn) | **chỉ `cob_gate`** (`wait_caught_up`) | "CDC stream nuốt tới L_cob chưa?" trước khi pin snapshot |

→ pull/sftp **không** đụng `etl_stream_progress`; chúng tự sync/parse data nên không cần chờ stream caught_up.

---

## Ghi chú cho người vẽ diagram

- **Màu sắc gợi ý**: CDC=xanh lá, Pull=xanh dương, SFTP=cam, Gate/Sensor=vàng, dbt=tím
- **Mỗi bảng Postgres** nên vẽ như 1 datastore ở giữa (hình trụ), các DAG đọc/ghi vào
- **Sensor** nên vẽ dạng vòng lặp poll (mũi tên cong quay lại)
- **Thứ tự thời gian** trong ngày: CDC (liên tục) → COB detect (~18:00) → Pull + SFTP song song (~18:05, đều poll cùng marker) → Silver (sau khi source-set từng ENQ sealed) → Gold (sau khi đủ ENQ silver của division)
- **Snapshot** là điểm mấu chốt của CDC: pin tại thời điểm COB xong để đọc đúng "dữ liệu ngày D"

---

## ✅ ACTION ITEMS — trạng thái (cập nhật 2026-06-24, sau E2E cluster operational)

### Điểm thiết kế — đã chốt

**1. Domain-keying cho ENQ dùng chung** — ✅ giải bằng *gán mỗi ENQ đúng 1 division*.
`enq_sources.ENQ_SOURCES[enq]["division"]` cố định domain; silver ghi `etl_model_runs` theo division đó,
gold gate `all_enqs_built(domain, enq_names, D)` khớp. Hiện KHÔNG có ENQ shared 2 division (acct_cust→operational).
⚠️ *Nếu sau này 1 ENQ phục vụ ≥2 division* → quay lại phương án domain-agnostic (`domain=''`, gate theo enq_name).

**2. `CDC_SOURCES` = HỢP mọi bảng CDC mà ENQ cần** — ✅ DONE.
`CDC_SOURCES = enq_sources.cdc_union()` = **25 bảng** (derive tự động từ ENQ_SOURCES, không hardcode).

**3. `wait_caught_up` nhánh A + B** — ✅ DONE (wire vào DAG 2026-06-24).
`cob_gate_dag.py::_caught_up` = A (`max_lsn≥L_cob`) **OR** B (`global≥L_cob AND lag[topic]==0`).
Nhánh B dùng `lib.kafka_lag` (kafka-python `end_offsets` − checkpoint MinIO `offsets/<maxBatchId>` line2).
`kafka-python` cài trong image (Dockerfile). E2E cluster: lag=0 cho cả 25 topic → bảng im không treo.

### Việc làm — đã xong
- [x] `build_mart_dag.py` — **1 DAG fan-out 10 ENQ** (không phải 14-DAG; xem NHỊP 3)
- [x] Apply DDL `scripts/cob_pipeline/01_postgres_control_tables.sql` lên Postgres `cob_control`
- [x] Mapping ENQ → source-set: `dags/lib/enq_sources.py` (cdc_dim / cdc_event / pull per ENQ)

### Còn treo thật
- [ ] `sftp_crb_dag.py` theo flow RECID (task `get_recids` query MSSQL `HOLD_CONSOL` WHERE `DATE.TIME`=D) — chưa wire vào COB gate (CRB/CRF chạy luồng SFTP riêng)
- [ ] Vendored `dbt_packages` cho T24_AML (hiện `dbt deps` runtime cần egress hub.getdbt.com)
- [ ] Test scenario S3/S8/S9/S10/S11 (idempotent, catch-up D+1, snapshot isolation, nhánh B cô lập, silver-fail chặn gold)
