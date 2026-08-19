---
name: cob-e2e-test
description: Chạy/verify E2E test COB pipeline v1.4 cho 1 business_date trên cluster dev — LINH ĐỘNG theo từng dbt model (factory DOMAIN_CONFIGS, gate chỉ chờ source-set của model; demo=3 luồng CDC+Pull+SFTP, aml=2 luồng CDC+Pull). Dùng khi user yêu cầu "test COB", seed 1 ngày COB mới, test 1 dbt model qua gate, verify pipeline, hoặc thêm/chạy 1 kịch bản test. Bao gồm runbook chuẩn + profiles per-model + catalog kịch bản.
---

# COB E2E Test — Runbook

Test pipeline COB v1.4: ingest chung (`cob_gate` CDC + `pull_cob` pull + `sftp_crb` sftp) seal vào
`etl_control`; tầng BUILD = **factory `dags/dbt_model_cob_dag.py`** sinh 1 DAG gate/model (`DOMAIN_CONFIGS`).
Thiết kế: `arch/PIPELINE_ORCHESTRATION_DESIGN.md`. Kết quả demo: `arch/E2E_DEMO_RESULT.md`.

> ⭐ **Skill LINH ĐỘNG theo từng dbt model.** Mỗi domain trong `DOMAIN_CONFIGS` khai báo source-set của nó
> (`cdc_sources`/`pull_sources`/`sftp_sources`); build-gate CHỈ chờ đúng bấy nhiêu luồng → **model ít luồng = gate dễ hơn**.
> Chọn `TARGET` (= domain) rồi chạy theo profile bên dưới. Demo (3 luồng) là **case khó nhất**, giữ làm chuẩn.

### Profiles per-model (chọn 1 làm `TARGET`)
| domain | DAG build | dbt_dir | luồng (sources gate chờ) | seed thêm | Gold verify |
|---|---|---|---|---|---|
| `demo` | `dbt_demo` | `COB_TEST` | **3**: CDC account/customer · pull sector · sftp crb | account/customer event + SFTP folder | `hive.gold.demo_deposit_by_sector` (sector 1005) |
| `aml` | `dbt_aml` | `AML_COB` | **2**: CDC account/customer/**stmt_entry** · pull company/eb_lookup/transaction | account/customer/**stmt_entry** event (data để 4 report ra row) | `hive.gold.rpt_{aml05,aot,aml03,9k}_*` |

> **Ingest seal HỢP mọi domain** (KHÔNG per-domain): `cob_gate.CDC_SOURCES = account+customer+stmt_entry`;
> `pull_cob` đã pull cả 35 bảng PULL_FULL (gồm company/eb_lookup/transaction) → AML pull-sources **sẵn sealed**.
> ⚠️ Vì cob_gate gate cả `stmt_entry` → **mọi test (kể cả demo) seed PHẢI bump stmt_entry** (xem B3) kẻo `wait_caught_up` chờ vô hạn.
> Pull full-mode đọc thẳng MSSQL → KHÔNG cần seed bảng pull. Thêm model = thêm 1 dict `DOMAIN_CONFIGS` + (nếu cần CDC mới) append `cob_gate.CDC_SOURCES`.

> 🔐 **KHÔNG hardcode cred.** Lấy từ Airflow Variables/Connections + k8s Secret. Export ra env trước:
> ```bash
> NS=airflow-development-ns; SPK=spark2-development-ns; KAF=kafka-development-ns
> SCHED=$(kubectl -n $NS get pods -o name | grep scheduler | grep -v prepull | head -1 | cut -d/ -f2)
> MSSQL=$(kubectl -n $KAF get pods -o name | grep mssql | head -1 | cut -d/ -f2)
> MOCK=$(kubectl -n $NS get pods -o name | grep mock-sftp | head -1 | cut -d/ -f2)
> export SA_PWD=$(kubectl -n $SPK get secret mssql-credentials -o jsonpath='{.data.MSSQL_PASSWORD}' | base64 -d)
> export DREMIO_PWD=$(kubectl -n $NS exec $SCHED -c scheduler -- airflow variables get dremio_password 2>/dev/null | grep -v Vault)
> # Tunnel cần: localhost:19047 (Dremio) localhost:19000 (MinIO) — xem memory service_tunnels
> ```

## Tham số test
- `D` = business_date cần test (YYYY-MM-DD), vd `2026-06-19`. `Dymd` = YYYYMMDD (vd `20260619`).
- Mọi nguồn PHẢI cùng D (CDC marker = pull = sftp folder = D) để dbt gate đủ-sealed.

---

## ⚙️ Quy trình CHUẨN (happy-path full-auto)

### B0. Health check (BẮT BUỘC trước khi test)
```bash
kubectl get --raw '/livez?verbose' 2>/dev/null | grep -i etcd        # [+]etcd ok
kubectl -n $SPK get sparkapplication t24-cob-streaming                # RUNNING (marker chỉ vào Bronze qua stream)
kubectl -n $NS get deploy airflow-scheduler -o jsonpath='{.spec.template.spec.containers[0].image}'  # build ổn định?
```
⚠️ **KHÔNG trigger DAG giữa lúc CICD rollout** (task chạy code cũ → reset etl_control). Chờ build đứng yên.

### B1. Trigger DAG TRƯỚC (sensor vào trạng thái chờ) — theo TARGET
```bash
export TARGET=dbt_aml          # hoặc dbt_demo (case 3-luồng). DAG build của domain.
# ingest: cob_gate + pull_cob luôn cần; sftp_crb CHỈ khi domain có sftp_sources (demo có, aml KHÔNG).
DAGS="cob_gate pull_cob $TARGET"; [ "$TARGET" = "dbt_demo" ] && DAGS="$DAGS sftp_crb"
for d in $DAGS; do kubectl -n $NS exec $SCHED -c scheduler -- airflow dags trigger $d; done
```

### B2. Verify sensor `up_for_reschedule` (idempotent — không vồ COB cũ đã xong)
```bash
# Lấy run_id mới nhất mỗi DAG rồi check task đầu:
kubectl -n $NS exec $SCHED -c scheduler -- airflow tasks states-for-dag-run cob_gate <run_id> | grep detect_cob
# kỳ vọng: up_for_reschedule (cob_gate.detect_cob, pull.get_business_date, $TARGET.detect_d, [sftp.detect_sftp nếu demo])
```

### B3. SEED DATA — thứ tự BẮT BUỘC: (1) F_BATCH → (2) CDC events account/customer/**stmt_entry** → (3) SFTP folder (chỉ demo)

> ⚠️ **Schema rule:** `F_BATCH`/`FBNK_ACCOUNT`/`FBNK_CUSTOMER`/`FBNK_STMT_ENTRY` = bảng `(RECID, XMLRECORD)`; parser bung
> `XMLRECORD` (`<cN>` theo vị trí) thành cột bronze. **KHÔNG tự gõ XMLRECORD mới** → **CLONE bản ghi có sẵn +
> `REPLACE`** RECID (+ field cần đổi). `<c1>` của ACCOUNT = CUSTOMER (chain tới customer→sector).
>
> ⚠️ **Thứ tự LSN:** marker tạo event mang LSN = **L_cob**; account/customer/**stmt_entry** phải sinh event SAU → LSN
> **> L_cob** → `wait_caught_up` pass. Chèn ngược = caught_up chờ vô hạn.
>
> ⚠️ **cob_gate gate cả `stmt_entry`** (union mọi domain) → **MỌI test (kể cả demo) PHẢI seed stmt_entry** (block bên dưới),
> kẻo `wait_caught_up` kẹt ở `t24_stmt_entry`.

**(1)+(2) MSSQL** — sửa `D=...`, `Dymd=...`, suffix recid theo D rồi chạy:
```bash
cat > /tmp/cob_seed.sql <<SQLEOF
USE testdb;
GO
-- COB-done marker = BNK/COB.INITIALISE, PROCESS.STATUS (c3)=0 + LAST.RUN.DATE (c13)=D.
-- (rule mới, xem dags/lib/cob_marker.py; trước đây neo BNK/DW.EXTRACT.EOD + job_status c12)
UPDATE dbo.F_BATCH SET XMLRECORD = N'<row xml:space="preserve" id="BNK/COB.INITIALISE"><c1>A000</c1><c3>0</c3><c4>F</c4><c6>COB.EXECUTE.API</c6><c6 m="2">COB.CHECK.EB.EOD.ERROR</c6><c6 m="3">EB.MANAGE.SERVICES</c6><c7 /><c7 m="2">COB.EXECUTE.API</c7><c8>D</c8><c8 m="2">D</c8><c8 m="3">D</c8><c12>0</c12><c12 m="2">0</c12><c12 m="3">0</c12><c13>${Dymd}</c13><c13 m="2">${Dymd}</c13><c13 m="3">${Dymd}</c13><c19>0</c19><c28>2</c28><c29>1_R23m</c29><c30>2510191707</c30><c31>44659_INPUTTER_OFS_T24.GENERIC.UPLOAD</c31><c32>TL0010001</c32><c33>1</c33><c36>APPLICATION</c36><c37>A-START</c37><c38>A000</c38></row>'
WHERE RECID = N'BNK/COB.INITIALISE';
GO
DECLARE @ax NVARCHAR(MAX)=(SELECT TOP 1 XMLRECORD FROM dbo.FBNK_ACCOUNT WHERE RECID=N'00000002227944');
DELETE FROM dbo.FBNK_ACCOUNT WHERE RECID IN (N'${Dymd}A1',N'${Dymd}A2');
INSERT dbo.FBNK_ACCOUNT(RECID,XMLRECORD) VALUES
 (N'${Dymd}A1', REPLACE(@ax,N'00000002227944',N'${Dymd}A1')),
 (N'${Dymd}A2', REPLACE(@ax,N'00000002227944',N'${Dymd}A2'));
DECLARE @cx NVARCHAR(MAX)=(SELECT TOP 1 XMLRECORD FROM dbo.FBNK_CUSTOMER);
DELETE FROM dbo.FBNK_CUSTOMER WHERE RECID IN (N'${Dymd}C1',N'${Dymd}C2');
INSERT dbo.FBNK_CUSTOMER(RECID,XMLRECORD) VALUES (N'${Dymd}C1',@cx),(N'${Dymd}C2',@cx);
GO
-- stmt_entry: BẮT BUỘC bump LSN (cob_gate gate t24_stmt_entry). Clone + REPLACE recid là đủ cho caught_up.
DECLARE @sx NVARCHAR(MAX)=(SELECT TOP 1 XMLRECORD FROM dbo.FBNK_STMT_ENTRY);
DELETE FROM dbo.FBNK_STMT_ENTRY WHERE RECID IN (N'${Dymd}S1',N'${Dymd}S2');
INSERT dbo.FBNK_STMT_ENTRY(RECID,XMLRECORD) VALUES (N'${Dymd}S1',@sx),(N'${Dymd}S2',@sx);
GO
SQLEOF
kubectl -n $KAF exec -i $MSSQL -- bash -c "cat > /tmp/s.sql; /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P \"$SA_PWD\" -C -W -i /tmp/s.sql" < /tmp/cob_seed.sql
```

> 🎯 **Data-shaping cho 4 report AML (`TARGET=dbt_aml`)** — clone-bump ở trên CHỈ đủ qua gate; để report RA ROW cần
> nắn data theo `<cN>` (vị trí lấy từ SS layout `ss_full.json` / `libs/t24_parser`, đối chiếu cột bronze ở `build_report/AML_BUILD_SPEC.md`):
> - **9K**: 1 `stmt_entry` có `account_number`=account seeded, `amount_lcy` ≥9000, `currency`=`USD`.
> - **AOT**: account có `opening_date`=`${Dymd}` (mở đúng ngày D) + ≥1 stmt_entry credit dương (first deposit).
> - **AML05**: 1 customer `legal_id` rỗng HOẶC `legal_exp_date` ≤ D, có account.
> - **AML03**: account `inactiv_marker`=`Y` + `date_last_*_cust` trong 30 ngày trước D.
>
> Chưa nắn thì 4 report vẫn build SẠCH nhưng 0 row (đã verify SQL trên Dremio). Pull-sources AML (company/eb_lookup/transaction) KHÔNG cần seed (full-mode).

**(3) SFTP folder** (CRB — độc lập marker; D = tên folder). Reuse file CRB sẵn có:
```bash
kubectl -n $NS exec $MOCK -- sh -c "mkdir -p /home/airflow/FromT24/CRB/${D} && cp /home/airflow/FromT24/CRB/2026-06-16/CRB_original /home/airflow/FromT24/CRB/${D}/ && chmod -R a+rX /home/airflow/FromT24"
```

> 💡 **Để Gold có ROW** cần ≥1 account có RECID = account_number trong file CRB **và** `<c1>`(customer) chain tới
> sector. Demo đã align sẵn 5 account (one-time, COB#1) → persist trong bronze → mọi COB sau tự có row sector 1005.
> Account `${Dymd}A1/A2` ở trên chỉ để đẩy LSN (không cần chain).

### B4. Monitor auto-flow (sensor tự detect ở poke kế ≤120s → ~210s ra Gold)
```bash
for d in $DAGS; do
  echo "$d: $(kubectl -n $NS exec $SCHED -c scheduler -- airflow dags state $d <run_id>)"; done
# kỳ vọng tuần tự: cob_gate → pull[/sftp] → $TARGET = success
```

### B5. VERIFY — Gold + bảng log (xem mục Helper)
- `etl_control` (ingest, chung): cdc **3 row** account/customer/**stmt_entry** (snapshot_id) · pull `t24_sector`/`t24_industry`(+35 PULL_FULL gồm company/eb_lookup/transaction) count · sftp `crb_deposits` (nếu demo).
- `etl_stream_progress`: account/customer/**stmt_entry** `max_lsn ≥ L_cob` (= t24_batch max_lsn).
- `etl_model_runs`: `($DOMAIN, D)` success (`DOMAIN`=demo|aml).
- **Gold theo TARGET:**
  - `demo` → `hive.gold.demo_deposit_by_sector` row D (sector 1005, 209M, 5 acct, 1 cust).
  - `aml` → `hive.gold.rpt_aml05_no_legal_doc` / `rpt_aot_accounts_opened` / `rpt_aml03_inactive_accounts` / `rpt_9k_large_cash`
    (row > 0 NẾU đã data-shape ở B3; chưa shape = 0 row nhưng build success). ⚠️ `materialized='table'` → chỉ giữ D mới nhất.
  - (demo) `etl_parsed_logs`: `(crb, D)` is_sync=T, is_parsed=T.

### B6. Cleanup (tuỳ chọn)
Xoá test record (`${Dymd}A*/C*/S*`), folder SFTP; KHÔNG xoá 5 aligned account (cần cho row demo). Reset marker nếu muốn.

---

## 🔧 Helper — verify (cred từ env, không hardcode)

**Dump control tables (Postgres, qua conn — không cần cred):**
```bash
kubectl -n $NS exec $SCHED -c scheduler -- python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
pg=PostgresHook(postgres_conn_id='cob_control_conn'); D='${D}'
for t,q in [('control',\"SELECT source_table,flow,status,expected_count,iceberg_snapshot_id FROM etl_control WHERE business_date='%s' ORDER BY flow\"),('parsed',\"SELECT * FROM etl_parsed_logs WHERE report_date='%s'\"),('runs',\"SELECT domain,status FROM etl_model_runs WHERE business_date='%s'\")]:
    print('###',t); [print(r) for r in pg.get_records(q%D)]"
```

**Query Dremio (Gold/bronze/stream_progress) qua tunnel localhost:19047:**
```python
# login: POST /apiv2/login {userName:'user', password: $DREMIO_PWD} → token '_dremio<tok>'
# query: POST /api/v3/sql {sql} → id → poll /api/v3/job/<id> → GET /api/v3/job/<id>/results
# Gold:   SELECT * FROM hive.gold.demo_deposit_by_sector
# Marker: SELECT recid,last_run_date,process_status FROM hive.bronze.t24_batch WHERE recid='BNK/COB.INITIALISE'  (process_status=0 ⇒ COB xong)
# Dremio cache: ALTER TABLE <t> REFRESH METADATA (hoặc FORGET METADATA nếu kẹt sau drop/recreate)
```

**Log task Airflow fail:** remote ở `s3://airflow-logs` (MinIO) — sigv4 GET, key URL-encode `safe="/"`.

---

## 📚 Catalog kịch bản (mở rộng tại đây)

| # | Kịch bản | Cách tạo | Kỳ vọng | Trạng thái |
|---|---|---|---|---|
| S1 | **Happy full-auto** | quy trình chuẩn B0–B5 | 4 success, Gold có row D | ✅ verified 16/17/18 |
| S2 | **Idempotent (chờ COB mới)** | trigger 4 DAG khi marker còn D-cũ đã sealed | cả 4 `up_for_reschedule`, KHÔNG vồ D-cũ | ✅ verified |
| S3 | **Resume sync (granular)** | seed + chạy tới sau `sync` (is_sync=T) → ép `parse` fail → re-run sftp_crb | `branch_need_sync`→`skip_sync` (bỏ sync), vào thẳng parse | ⏳ template |
| S4 | **COB chưa xong** | marker `process_status=1` (c3=1, đang running) hoặc `2` (batch success) | detect_cob/get_business_date return False → chờ; gate KHÔNG fire | ⏳ template |
| S5 | **Thiếu 1 nguồn** | KHÔNG tạo folder SFTP cho D | cob_gate+pull seal; `dbt_demo.wait_all_sealed` chờ (completeness) | ⏳ template |
| S6 | **Gold rỗng (join lệch)** | seed D KHÔNG có aligned account khớp CRB | pipeline success nhưng Gold 0 row | ✅ thấy ở COB#1 |
| S7 | **Schema drift bảng pull** | bảng `t24_*` bị ghi thiếu/thừa cột (vd `t24_industry` 16-cột KHÔNG business_date do legacy `t24_pull_pipeline` ghi) | pull parse `INSERT_COLUMN_ARITY_MISMATCH` → drop bằng util Spark/Iceberg (KHÔNG Dremio) + re-run | ✅ gặp thật (sector 18, industry 17) |
| S8 | **caught_up WAIT** | chèn account/customer TRƯỚC marker (LSN < L_cob) | `wait_caught_up` chờ tới khi có event mới ≥ L_cob | ⏳ template |
| S9 | **Catch-up nhiều COB** | bump marker qua nhiều ngày trước khi trigger | mỗi lần fire xử lý D mới nhất; cần loop để đóng từng D | ⏳ template (design: so etl_control) |

**Thêm kịch bản mới:** thêm 1 dòng bảng trên + (nếu cần) 1 mục con mô tả: *seed gì khác chuẩn → kỳ vọng → cách verify*.
Nguyên tắc giữ: cùng schema-clone, đúng thứ tự LSN, mỗi nguồn cùng D, cred từ env.

---

## ⚠️ Gotchas (gặp khi build/test)
- **Cluster shared**: dev khác push → build churn; trigger giữa rollout → reset etl_control. Reset bẩn: `UPDATE etl_control SET status='sealed'...` + fail run lỡ qua ORM `DagRun.set_state(State.FAILED)`.
- **GAP double-ingest**: 1 bảng bị 2 nguồn ghi → schema drift. Đã gặp THẬT: legacy `t24_pull_pipeline` (manual, KHÔNG `--business-date`) tạo `t24_sector`/`t24_industry` thiếu cột → pull_cob COB ghi fail. Guardrail 1-bảng-1-nguồn chưa làm → **đừng chạy `t24_pull_pipeline` cho bảng COB pull đang sở hữu**.
- **Fan-out pull + worker-02 chật RAM**: pin worker-02 (~96% RAM) chỉ chứa ~1 spark job 2g/2g cùng lúc → nhiều parse song song = executor `Insufficient memory`→Pending→deadlock. Parse pull đã right-size **768m/1-core** (bảng reference 10 row). Thêm bảng pull lớn → có thể cần serialize hoặc tăng size.
- **Đổi schema Iceberg (drop+recreate)**: ⚠️ **Dremio `DROP TABLE` = NO-OP trên hive external source** (báo OK nhưng bảng vẫn còn — verified 2026-06-17 với `t24_industry`). Phải drop từ **Spark/Iceberg**: util `s3a://spark-scripts/util/drop_iceberg_table.py` (args = table names → `DROP TABLE IF EXISTS .. PURGE`) chạy qua temp SparkApplication (image v4 + hive catalog conf) → xoá cả bảng main + `*__sv` spillover → re-run parse tạo lại partitioned. Apply→monitor COMPLETED→delete CR.
- **Re-align data**: sửa MSSQL → re-stream → **re-run cob_gate** (snapshot mới) → **clear `etl_model_runs`** (nếu không skip-if-built bỏ qua).
- **DAG paused mặc định** → `airflow dags unpause <dag>` trước khi chạy.
- **Image spark**: job demo pin `worker-02` (nơi có sẵn `spark-t24:v4` + `spark-3.5.1:v2`). Image mới phải load tay lên node (DNS node hỏng pull dockerhub) — xem memory.
