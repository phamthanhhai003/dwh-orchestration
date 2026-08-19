# Guide: Thêm 1 bảng CDC vào pipeline streaming (T24 → Bronze)

Hướng dẫn cho dev thêm 1 bảng T24 vào luồng CDC (Debezium → Kafka → Spark streaming → Bronze Iceberg).

> Namespace: Spark = `spark2-development-ns`, Kafka/MSSQL = `kafka-development-ns`.
> File interface dev sửa: `spark/manifests/cdc-tables.yaml`.

## Điều kiện
- Bảng có trong `ss_full.json` (F_STANDARD_SELECTION, ~3749 bảng) → parser tự dựng schema. Bảng T24 chuẩn gần như chắc có.
- SQL Server Agent đang chạy (bắt buộc cho CDC).

---

## Flow chuẩn — bảng mới, đổ data SAU (sạch nhất, khỏi snapshot/touch)

Làm đúng thứ tự: bật CDC + connector + streaming TRƯỚC, **insert data sau cùng** → mỗi row tự thành CDC `create` event chảy vào Bronze.

### 1. MSSQL — tạo bảng rỗng
```sql
CREATE TABLE dbo.FBNK_X (
  RECID     nvarchar(255) NOT NULL PRIMARY KEY,
  XMLRECORD nvarchar(max) NULL
);
```

### 2. MSSQL — bật CDC trên bảng
```sql
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'FBNK_X', @role_name=NULL;
```

### 3. Connector — thêm bảng vào `table.include.list`
Sửa `kafka/03-mssql-connector.yaml`, thêm `dbo.FBNK_X` vào `table.include.list`, rồi:
```bash
kubectl apply -f kafka/03-mssql-connector.yaml      # connector restart → bắt đầu capture bảng mới
```

### 4. Streaming — thêm bảng vào danh sách
Thêm 1 dòng vào `data.tables.txt` trong `spark/manifests/cdc-tables.yaml`:
```
X=t24.testdb.dbo.FBNK_X:hive.bronze.t24_x
```
Định dạng: `NAME=t24.<db>.<schema>.<TABLE>:hive.bronze.t24_<ten>`. Rồi:
```bash
kubectl apply -f spark/manifests/cdc-tables.yaml
kubectl -n spark2-development-ns delete sparkapplication t24-cob-streaming
kubectl apply -f spark/manifests/t24-cob-streaming.yaml      # resume từ checkpoint
```

### 5. MSSQL — insert data (LÀM SAU CÙNG)
```sql
INSERT INTO dbo.FBNK_X (RECID, XMLRECORD) VALUES (N'...', N'<row ...>...</row>');
```
→ Mỗi row sinh CDC `create` → Debezium → streaming → parse → `hive.bronze.t24_x` (op=c).

### 6. Verify (Dremio)
```sql
ALTER TABLE hive.bronze.t24_x REFRESH METADATA;   -- Dremio cache metadata, phải refresh
SELECT COUNT(*) FROM hive.bronze.t24_x;
SELECT * FROM hive.bronze.t24_x LIMIT 5;           -- check cột typed + multi-value = array
```
+ kiểm tra `hive.bronze.etl_stream_progress` có row `t24_x` (max_lsn).

---

## Bảng ĐÃ CÓ SẴN data (data có trước khi bật CDC → không tự vào)

Làm bước 1–4 như trên (data đã có). Để lấy lại history, chọn 1 trong 2:

- **Nhanh (dev) — touch all rows:** ép sinh CDC bằng update thật
  ```sql
  UPDATE dbo.FBNK_X SET XMLRECORD = XMLRECORD + N' ';   -- thêm space cuối, XML vẫn parse (op=u)
  ```
  ⚠️ No-op `SET col=col` **KHÔNG** sinh CDC (SQL Server skip) → phải đổi giá trị thật.

- **Chuẩn (prod) — Debezium incremental snapshot** *(⚠️ CHƯA verify trên cụm này — theo doc Debezium)*: bật signal channel (`signal.enabled.channels` + `signal.data.collection`) rồi gửi signal `execute-snapshot` cho bảng đó → Debezium đọc thẳng bảng nguồn, lấy đủ history (op=r), không đụng bảng khác.

> Lưu ý: `snapshot.mode=initial` chỉ snapshot 1 lần ở lần start ĐẦU của connector → **không** re-snapshot bảng thêm sau.

---

## PULL — kéo bảng theo lần (yaml-only, không streaming)

Dùng khi lấy bảng theo batch (JDBC) thay vì CDC realtime. Mỗi bảng = 2 job nối tiếp:
**extract** (JDBC → raw parquet) rồi **parse** (raw → Bronze). Template:
`spark/manifests/t24-extract-template-pull.yaml` + `t24-parse-template-pull.yaml` (ví dụ FBNK_FUNDS_TRANSFER).

### Thêm 1 bảng pull
1. **Copy** 2 template thành file riêng cho bảng (vd `t24-extract-account.yaml`, `t24-parse-account.yaml`).
2. **Sửa extract**: `metadata.name`, `--dbtable` (vd `dbo.FBNK_ACCOUNT`), `--output` (`s3a://raw/t24/FBNK_ACCOUNT/<date>/`), `--mode`. Bản yaml-only này chạy `full`. Mode `incremental` đường chuẩn = đọc/ghi watermark từ bảng `etl_pull_watermark` (COB-driven, KHÔNG gõ `--last-value` tay) — thuộc phần DAG đã gác; `--last-value` chỉ để chạy tay tạm khi debug.
3. **Sửa parse**: `metadata.name`, `--table` (tên trong SS, vd `ACCOUNT`), `--input` (= `--output` của extract), `--target` (bronze, vd `hive.bronze.t24_account_pull`).
4. **Chạy** (parse SAU khi extract COMPLETED):
   ```bash
   kubectl apply -f spark/manifests/t24-extract-account.yaml
   kubectl -n spark2-development-ns wait --for=jsonpath='{.status.applicationState.state}'=COMPLETED sparkapplication/t24-extract-account --timeout=900s
   kubectl apply -f spark/manifests/t24-parse-account.yaml
   ```
5. **Verify**: như mục Verify (Dremio `REFRESH METADATA` + count bronze).

> JDBC password: tự lấy từ Secret `mssql-credentials` (env `MSSQL_PASSWORD`) — KHÔNG điền vào yaml.
> ⚠️ GÁC LẠI (chưa làm): COB gate cho pull, reconcile tự động (COUNT bronze==source),
> `etl_pull_watermark`, fan-out Airflow DAG. Bản này = chạy tay từng bảng.

## Ghi chú
- `keep-lsn` (giữ cột `__lsn` cho gate COB): là arg `--keep-lsn-tables` trong `t24-cob-streaming.yaml`, chỉ bảng gate (`BATCH`) cần, set 1 lần.
- `ss_full.json` đã đầy đủ → không cần đụng khi thêm bảng.
- Load CSV → MSSQL: sinh file `.sql` dùng `N'...'` (escape `'` thành `''`) rồi `kubectl cp` + `sqlcmd -i` (tránh BULK INSERT codepage trên Linux SQL Server).
