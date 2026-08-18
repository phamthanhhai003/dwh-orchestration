# Day 1 Init Load — Planning Document
**Mục tiêu:** Full snapshot toàn bộ data T24 từ MSSQL vào DWH (Hive Iceberg Bronze) trước khi bật CDC streaming.

---

## 1. Phạm vi & Quy mô

### Bảng ưu tiên cao (giao dịch lớn)

| Bảng | Ước tính rows (prod) | Ước tính size |
|---|---|---|
| FBNK_STMT_ENTRY | ~50–80M | ~168 GB |
| FBNK_STMT_ENTRY_DETAIL | ~60–100M | ~173 GB |
| FBNK_CATEG_ENTRY | ~50–80M | ~145 GB |
| FBNK_CATEG_ENTRY_DETAIL | ~50–80M | ~133 GB |
| FBNK_AA_PROCESS_DETAILS | ~10–20M | ~35 GB |

### Bảng ưu tiên trung bình (master/reference)

| Bảng | Ước tính rows (prod) | Ghi chú |
|---|---|---|
| FBNK_ACCOUNT | ~5–10M | |
| FBNK_ACCOUNT_HIS | ~20–50M | lớn, append-only |
| FBNK_CUSTOMER | ~1–3M | |
| FBNK_AA_ARR_ACCOUNT | ~5–10M | |
| FBNK_FUNDS_TRANSFER | ~10–30M | |
| + ~25 bảng còn lại | ~tổng 20–50M | CDC tables hiện có |

**Tổng ước tính: 300–500M rows, ~700GB–1TB raw XML**

---

## 2. Các Solution

### Option A: BCP Bulk Export ✅ RECOMMENDED

```
MSSQL
  └─ bcp queryout (1 sequential scan)
        └─ TSV stream → pyarrow chunks (500K rows/chunk)
              └─ parquet → MinIO s3a://raw/t24/<TABLE>/<DATE>/
                    └─ Spark parse (t24_batch_parse.py)
                          └─ Hive Iceberg Bronze
```

**Cơ chế:**
- `bcp` là native tool SQL Server, stream thẳng stdout → không buffer RAM
- pyarrow đọc stream TSV theo chunk → ghi parquet lên MinIO trực tiếp
- Không cần disk ephemeral trên pod

**Pros:**
- DB chỉ bị hit **1 lần duy nhất** per table — sequential scan, sau đó disconnect
- DB load thấp nhất trong tất cả options
- Memory footprint thấp (~250MB/chunk ở 500K rows)
- Stable: không bị ảnh hưởng bởi INSERT trong lúc dump (snapshot tại thời điểm T)
- Infrastructure đã sẵn: `pull_bulk_dag.py`, `t24_bcp_extract.py`, `t24_bulk_parse.yaml`

**Cons:**
- Cần `mssql-tools` (bcp CLI) trong Docker image → cần rebuild `spark-t24:v5`
- Single-threaded per table → chậm hơn JDBC parallel về throughput
- Cần network throughput MSSQL → MinIO ổn định

**Blocker hiện tại:** Image `spark-t24:v4` thiếu `bcp` binary → cần build `v5`

---

### Option B: JDBC Keyset Pagination

```
Spark JDBC
  └─ WHERE RECID > '{last_checkpoint}' ORDER BY RECID TOP 500K
        └─ lặp N batch cho đến hết
              └─ Hive Iceberg Bronze
```

**Pros:**
- Không cần build image mới
- Checkpoint rõ ràng (last RECID), resume được nếu fail giữa chừng
- Stable với concurrent INSERT (RECID T24 luôn tăng dần theo thời gian)

**Cons:**
- DB bị hit **N lần** (N = total_rows / batch_size) — ví dụ 80M rows / 500K = **160 queries**
- 4/5 bảng lớn **không có index trên RECID** → mỗi batch = full scan đến checkpoint → chậm dần
- Cần tạo index RECID trước: `CREATE INDEX IX_RECID ON FBNK_CATEG_ENTRY (RECID)`
- Nếu không có index: keyset không tốt hơn OFFSET nhiều

---

### Option C: JDBC OFFSET/FETCH ❌ KHÔNG DÙNG

```sql
SELECT ... ORDER BY RECID OFFSET N ROWS FETCH NEXT 500000 ROWS ONLY
```

**Vấn đề:**
- CDC đang write liên tục → OFFSET shift → **miss hoặc duplicate rows**
- OFFSET càng lớn càng chậm (scan toàn bộ từ đầu đến offset)
- Không reliable cho production init load

---

### Option D: RECID List Batch

```
SELECT RECID → lưu list → loop WHERE RECID IN (batch_1000)
```

**Cons:**
- Đọc DB 2 lần (1 lấy RECID, N lần pull data)
- Lưu hàng triệu RECID ở đâu?
- IN clause N=1000 → hàng trăm nghìn round trips cho bảng lớn
- Không scale

---

## 3. So Sánh Tổng Hợp

| Tiêu chí | BCP | Keyset JDBC | OFFSET | RECID List |
|---|---|---|---|---|
| DB hits | **1/table** | N batch | N batch | 1 + N×M |
| DB load | **Thấp nhất** | Trung bình | Cao | Cao |
| Stable với CDC inserts | ✓ | ✓ | ✗ | ✓ |
| Cần index | Không | **Có** | Không | Không |
| Resume nếu fail | Partial (theo chunk) | **Tốt nhất** (RECID checkpoint) | Không | Không |
| Throughput | Trung bình | Thấp–Trung bình | Chậm dần | Thấp |
| Phức tạp impl | Thấp (đã có) | Trung bình | Thấp | Cao |
| Cần infra mới | Rebuild image | Không | Không | Không |

---

## 4. Recommended Plan: BCP + CDC Handoff

### Phase 0 — Chuẩn bị (trước Day 1)

```
1. Rebuild image spark-t24:v5 (thêm mssql-tools + t24_bcp_extract.py)
2. Test BCP trên testdb với 1 bảng nhỏ
3. Verify Debezium connector config: snapshot.mode = schema_only
4. Xác nhận MinIO có đủ dung lượng (~1TB free)
```

### Phase 1 — Ghi LSN Checkpoint

```sql
-- Chạy trên MSSQL trước khi BCP bắt đầu
-- Lưu lại LSN này để Debezium bắt đầu CDC từ đây
SELECT sys.fn_cdc_get_max_lsn() AS snapshot_lsn;
```

### Phase 2 — BCP Init Load (tuần tự)

```
Thứ tự ưu tiên (bảng nhỏ trước để test, bảng lớn sau):
  1. FBNK_AA_PROCESS_DETAILS  (~35GB)   → ~30-45 phút
  2. FBNK_CATEG_ENTRY         (~145GB)  → ~2-3 giờ
  3. FBNK_CATEG_ENTRY_DETAIL  (~133GB)  → ~2-3 giờ
  4. FBNK_STMT_ENTRY          (~168GB)  → ~2-3 giờ
  5. FBNK_STMT_ENTRY_DETAIL   (~173GB)  → ~2-3 giờ
  + 25 bảng còn lại           (~200GB)  → ~3-4 giờ

Tổng estimate: 12-18 giờ (chạy qua đêm)
```

**Chạy tuần tự** (không song song) vì cluster đang overcommit RAM:
- worker-01: limit 163% RAM
- worker-02: 144%, worker-03: 147%
- Spark job mỗi bảng: driver 2GB + 2 executor × 4GB = ~10GB

### Phase 3 — Verify Bronze

```sql
-- Sau khi parse xong từng bảng, check row count khớp
SELECT COUNT(*) FROM hive.bronze.t24_categ_entry;
-- So với MSSQL:
SELECT COUNT(*) FROM FBNK_CATEG_ENTRY;
```

### Phase 4 — Bật CDC từ LSN đã note

```yaml
# Cập nhật connector config
snapshot.mode: schema_only           # bỏ qua snapshot, dùng data từ BCP
# Debezium sẽ stream changes từ LSN đã ghi ở Phase 1
```

---

## 5. Cluster Constraints

| Node | CPU | RAM | RAM limit% | Ghi chú |
|---|---|---|---|---|
| worker-00 | 18c | 24GB | 113% | control-plane + worker |
| worker-01 | 18c | 20GB | **163%** | overcommit cao |
| worker-02 | 18c | 20GB | **144%** | overcommit cao |
| worker-03 | 18c | 20GB | 147% | RAM actual thấp nhất (39%) → target BCP jobs |
| master-02 | 8c | 16GB | OK | SchedulingDisabled |

**Khuyến nghị:** Pin Spark BCP jobs vào worker-03, chạy ngoài giờ hành chính.

---

## 6. Risk & Mitigation

| Risk | Mức độ | Mitigation |
|---|---|---|
| OOMKill Spark pod | Cao | Chạy tuần tự, pin worker-03, giảm executor memory nếu cần |
| BCP bị ngắt giữa chừng | Trung bình | Re-trigger DAG cho bảng đó (idempotent: overwrite parquet) |
| MinIO hết dung lượng | Trung bình | Check free space trước, xóa raw sau khi parse xong |
| LSN gap (miss changes) | Thấp | Ghi LSN trước khi BCP bắt đầu bảng đầu tiên |
| CDC duplicate với BCP data | Thấp | Debezium `schema_only` + start from snapshot LSN |

---

## 7. Action Items

- [ ] Build image `spark-t24:v5` với `mssql-tools`
- [ ] Test BCP pipeline trên testdb (1 bảng)
- [ ] Confirm MinIO free capacity ≥ 1TB
- [ ] Update Debezium connector: `snapshot.mode: schema_only`
- [ ] Schedule maintenance window cho Day 1 (chạy đêm ~12-18h)
- [ ] Script ghi LSN trước khi bắt đầu
