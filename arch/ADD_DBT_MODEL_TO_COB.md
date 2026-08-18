---
date: 2026-06-17
type: guide
tags: [cob, dbt, onboarding]
---

# Tích hợp dbt model division vào COB pipeline

Đi theo đúng ví dụ **`dbt_aml`** (đã làm xong) — division khác copy y hệt, đổi tên.

---

## Bước 1 — Tạo dbt project cho division

Folder riêng dưới repo root + `dbt_project.yml` + `profiles.yml`. AML làm:

```
AML_COB/
├── dbt_project.yml        # name: aml_cob; materialized mặc định tuỳ bạn
├── profiles.yml           # type: dremio, host/user/pass qua env (copy y từ AML_COB)
└── models/
    ├── rpt_9k_large_cash.sql
    ├── rpt_aml05_no_legal_doc.sql
    ├── rpt_aml03_inactive_accounts.sql
    └── rpt_aot_accounts_opened.sql
```
→ Copy thẳng `AML_COB/dbt_project.yml` + `profiles.yml`, đổi `name` thành `<division>_cob`.

## Bước 2 — Viết model: nguồn đọc theo luồng

Ví dụ thật rút gọn từ `AML_COB/models/rpt_9k_large_cash.sql` — đủ 2 kiểu nguồn:

```sql
{{ config(schema='gold') }}

with entry as (          -- CDC: đọc AT SNAPSHOT, var snap_<bảng> do gate cấp
    select recid, account_number, amount_lcy, currency
    from hive.bronze.t24_stmt_entry at snapshot '{{ var("snap_stmt_entry") }}'
),
txn as (                 -- PULL: lọc WHERE business_date = D
    select recid as transaction_code, short_desc
    from hive.bronze.t24_transaction
    where business_date = date '{{ var("business_date") }}'
)
select e.account_number, e.amount_lcy, t.short_desc
from entry e
left join txn t on e.transaction_code = t.transaction_code
```

Quy tắc nguồn (3 luồng):
| Luồng | Cách đọc | Var dùng |
|---|---|---|
| CDC (`t24_account`, `t24_stmt_entry`...) | `... AT SNAPSHOT '{{ var("snap_<x>") }}'` | `snap_<đuôi sau t24_>`, vd `t24_account`→`snap_account` |
| PULL (`t24_company`, `t24_category`...) | `... WHERE business_date = date '{{ var("business_date") }}'` | `business_date` |
| SFTP (`crb_deposits`...) | `... WHERE load_date = date '{{ var("business_date") }}'` | `business_date` |

(Gate tự bơm các var này lúc chạy — bạn chỉ cần dùng đúng tên.)

## Bước 3 — Ốp vào pipeline: thêm 1 dict vào `dags/dbt_model_cob_dag.py`

AML đã thêm đúng cái này (`DOMAIN_CONFIGS`):

```python
{
    "domain": "aml", "dag_id": "dbt_aml",
    "tags": ["dbt", "aml", "gate", "v1.4"],
    "dbt_dir": "AML_COB",
    "dbt_select": "rpt_aml05_no_legal_doc rpt_aot_accounts_opened "
                  "rpt_aml03_inactive_accounts rpt_9k_large_cash",
    "models_built": 4,
    "cdc_sources":  ["hive.bronze.t24_account", "hive.bronze.t24_customer", "hive.bronze.t24_stmt_entry"],
    "pull_sources": ["hive.bronze.t24_company", "hive.bronze.t24_eb_lookup", "hive.bronze.t24_transaction"],
    "sftp_sources": [],
}
```
→ Factory tự sinh DAG `dbt_aml`: chờ đúng 6 nguồn này sealed → cấp `business_date` + `snap_*` → `dbt run --select ...`.
`cdc/pull/sftp_sources` = liệt kê **đúng** các bảng model bạn đọc, theo luồng.

## Bước 4 — Test E2E thủ công (đúng quy trình đã chạy cho `dbt_aml`)

Pipeline chạy bằng **sensor**: DAG được trigger trước, sensor chờ tới khi data tới thì tự đi tiếp. Nên trình tự là **trigger trước → seed sau → quan sát → verify**.

### 4.1. Trigger sẵn các DAG để khởi động sensor
```bash
for d in cob_gate pull_cob dbt_aml; do        # + sftp_crb nếu domain có sftp_sources
  airflow dags trigger $d; done
```
3 sensor (`cob_gate.detect_cob`, `pull_cob.get_business_date`, `dbt_aml.detect_d`) vào `up_for_reschedule` — chờ data của ngày D.

### 4.2. Seed MSSQL — marker + bảng CDC (PULL không cần seed)
Chọn D (vd `2026-06-21`, `Dymd=20260621`).

- **F_BATCH (marker COB-done):** set `<c13>`=`Dymd` (last_run_date), `<c12>`=`0` (job_status). Đây là cái cob_gate dò để biết "COB ngày D xong".
- **Bảng CDC** (`FBNK_ACCOUNT`/`FBNK_CUSTOMER`/`FBNK_STMT_ENTRY`...): **clone bản ghi THẬT từ data sample** (`arch/tool_parser/FBNK_*.csv`) — giữ NGUYÊN cấu trúc `<cN>`, chỉ đổi `recid` + vài field cần để bản ghi "áng" vào report bạn test.
  - ⚠️ **KHÔNG gõ XMLRECORD mới / seed bừa** — sai vị trí `<cN>` là parse ra rác hoặc 0 row.
  - cN của field nào → chạy `build_spec("<TABLE>", ss)` (`libs/t24_parser`) trên `s3a://spark-scripts/t24/ss_full.json`. Vd đã pin: ACCOUNT `opening_date=c78`, `inactiv_marker=c22`, `customer=c1`; STMT.ENTRY `amount_lcy=c3`, `currency=c12`, `account_number=c1`; CUSTOMER `legal_id=c34`.
- **PULL KHÔNG seed:** full-mode — mỗi ngày `pull_cob` đọc thẳng MSSQL → ghi bronze partition `business_date=D`. Đã là 1 partition riêng theo COB, không cần đụng.
- ⚠️ **Thứ tự:** seed `F_BATCH` TRƯỚC, rồi mới tới các bảng CDC.

### 4.3. Quan sát flow tự chạy (sensor poke ~60–120s)
```
cob_gate : detect_cob → open_pending(cdc) → wait_caught_up → pin_and_seal   (ghim snapshot mỗi bảng CDC, seal vào etl_control)
pull_cob: get_business_date → (extract→parse→seal) × N bảng pull           (full-mode, seal theo count)
dbt_<dom>: detect_d → wait_all_sealed → prepare_vars → dbt run → finish     (chờ ĐỦ nguồn rồi mới build)
```
Theo dõi qua `etl_control` (nguồn nào sealed chưa):
```sql
SELECT source_table, flow, status FROM etl_control WHERE business_date='<D>' ORDER BY flow;
```

### 4.4. Reconcile / verify — điều kiện XANH
- `etl_control` (D): mọi `cdc_sources` = `sealed` (có `iceberg_snapshot_id`); mọi `pull_sources` = `sealed` (reconcile `expected=actual` count, 0-tolerant); sftp nếu có.
- `etl_stream_progress`: mỗi CDC table `max_lsn ≥ L_cob` (`= t24_batch.__lsn`) → đã caught-up.
- `etl_model_runs (domain, D)` = `success`.
- Gold `hive.gold.rpt_*` ra row đúng như data đã shape. *(AML chạy thật: 9k=288, aot=1, aml03=1, aml05=1.)*

> 🤖 **Tự động hoá:** skill `cob-e2e-test` `TARGET=dbt_<domain>` làm trọn 4.1→4.4 (trigger + seed + monitor + verify) — dùng khi đã quen tay.

---

**Lưu ý nguồn:** các bảng bạn liệt kê phải đã được ingest seal. Account/customer/stmt_entry (CDC) + mọi bảng trong `dags/lib/t24_sources.py` `PULL_FULL` là **có sẵn**. Cần bảng CDC mới → báo platform thêm vào `cob_gate.CDC_SOURCES` + stream; bảng pull mới → thêm vào `PULL_FULL`. Không thì gate chờ mãi.
