# BCP Pull Pipeline — Performance Tracking

## Metadata

| Field | Value |
|---|---|
| Tracking updated | 2026-06-25 |
| Pipeline | `pull_bulk` DAG |
| Source | testdb (MSSQL) → MinIO → Hive Iceberg |

---

## DEV Environment

**DAG Run ID:** `manual__2026-06-24T18:41:02.355043+00:00_paDr4vX1`  
**Start:** 2026-06-24 18:41 UTC  
**Row cap:** 100,000 rows/table (nếu bảng < 100K → lấy số thực)  
**Size est:** tỉ lệ tuyến tính theo row cap từ dữ liệu testdb

### Spark Config (DEV)

| Parameter | Value |
|---|---|
| Executor instances | 2 |
| Executor cores | 2 |
| Executor memory | 2g |
| Driver memory | 1g |
| shuffle.partitions | 4 |
| Extract pool | t24_pulling = 4 slots |
| Parse pool | t24_parsing = 5 slots |
| Seal pool | t24_sealing = 7 slots |

### Table Performance (DEV)

> `*` = bảng < 100K rows → rows và size là số thực  
> `†` = bảng 0 rows → skip extract, write empty placeholder, seal = 0

| MSSQL Table | Dev Rows | Dev Size Est (MB) | Extract (s) | Parse (s) | Seal (s) |
|---|---:|---:|---:|---:|---:|
| FBNK_ACCOUNT | 100,000 | 360 | 331 | *(running)* | |
| AAFBNK_AA010 | 100,000 | 1,092 | 360 | *(running)* | |
| FBNK_AA_ACCOUNT_DETAILS | 100,000 | 328 | 177 | 1,281 | 11 |
| FBNK_PV_ASSET_DETAIL | 100,000 | 285 | 101 | 1,228 | 10 |
| FBNK_CUSTOMER | 100,000 | 218 | 24 | *(running)* | |
| FBNK_EM_LO_APPLICATION `*` | 98,355 | 338 | 98 | 577 | 10 |
| FBNK_AA_ARRANGEMENT | 100,000 | 131 | 19 | 462 | 10 |
| AAFBNK_AA093 | 100,000 | 130 | 18 | 420 | 10 |
| FBNK_AA_BILL_DETAILS | 100,000 | 86 | 18 | 377 | 10 |
| FBNK_AA_ARR_TERM_AMOUNT | 100,000 | 52 | 14 | 388 | 10 |
| FBNK_ACCOUNT_HIS | 100,000 | 64 | 14 | 433 | 10 |
| FBNK_CATEG_ENTRY_DETAIL | 100,000 | 43 | 14 | 314 | 10 |
| FBNK_AA_ARR_CUSTOMER | 100,000 | 43 | 12 | 181 | 10 |
| FBNK_AA_ARR_ACCOUNT | 100,000 | 65 | 14 | 290 | 10 |
| FBNK_STMT_ENTRY_DETAIL | 100,000 | 64 | 15 | 297 | 10 |
| FBNK_ACCT_ACTIVITY | 100,000 | 32 | 15 | 277 | 10 |
| AAFBNK_AA063 | 100,000 | 63 | 19 | *(running)* | |
| FBNK_AA_PROCESS_DETAILS | 100,000 | 21 | 13 | 216 | 11 |
| FBNK_ACCOUNT_STATEMENT | 100,000 | 25 | 13 | 232 | 11 |
| FBNK_STMT_ENTRY | 100,000 | 50 | 15 | 229 | 11 |
| FBNK_ACCOUNT_CLOSED | 100,000 | 7 | 11 | 241 | 10 |
| FBNK_ACCT_STMT_PRINT | 100,000 | 8 | 13 | 190 | 10 |
| FBNK_STMT_PRINTED | 100,000 | 36 | 14 | 109 | 10 |
| FBNK_CUSTOMER_ACCOUNT | 100,000 | 2 | 11 | 130 | 10 |
| FBNK_AC_LOCKED_EVENTS | 100,000 | 25 | 13 | *(sched)* | |
| ACFBNK_ST002 | 100,000 | 11 | 10 | 114 | 10 |
| ACFBNK_CA002 | 100,000 | 9 | 13 | 107 | 10 |
| FBNK_FUNDS_TRANSFER `*` | 10,158 | 9 | 11 | 1,166 | 10 |
| F_AA_CUSTOMER_ROLE `*` | 51 | <1 | 13 | 86 | 10 |
| FBNK_AC_SUB_ACCOUNT `*` | 6 | <1 | 11 | 105 | 11 |
| FBNK_TELLER `*` | 6 | <1 | 11 | 90 | 10 |
| FBNK_CATEG_ENT_TODAY `*` | 3 | <1 | 11 | *(running)* | |
| ACFBNK_AC009 `†` | 0 | 0 | 13 | 105 | 10 |
| FBNK_ACCT_ENT_TODAY `†` | 0 | 0 | 11 | 84 | 10 |
| FBNK_EM_DL_APPLICATION `†` | 0 | 0 | 11 | 91 | 10 |
| FBNK_ACCOUNT_HIS | 100,000 | 64 | 14 | 433 | 10 |

**Bottleneck notes:**
- Extract chậm nhất: `FBNK_ACCOUNT` (331s) và `AAFBNK_AA010` (360s) — XML size/row rất lớn
- Parse chậm nhất: `FBNK_ACCOUNT` + `AAFBNK_AA010` (>40 phút, vẫn đang chạy)
- `FBNK_FUNDS_TRANSFER`: 10K rows nhưng parse 1,166s → XML record phức tạp

---

## UAT Environment

**Tables:** FBNK_ACCOUNT, FBNK_CUSTOMER  
**Rows & size:** số thực (full table, không cap)  
**Status:** chưa chạy xong → để trống

### Spark Config (UAT)

| Parameter | Value |
|---|---|
| Executor instances | 2 |
| shuffle.partitions | 16 |

### Table Performance (UAT)

| MSSQL Table | Actual Rows | Actual Size (MB) | Extract (s) | Parse (s) | Seal (s) |
|---|---:|---:|---:|---:|---:|
| FBNK_ACCOUNT | *(TBD)* | *(TBD)* | | | |
| FBNK_CUSTOMER | *(TBD)* | *(TBD)* | | | |
