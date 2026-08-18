# CDC Table Classification — by Dev Size Est (at 100K rows cap)

**Thresholds:**
- Type 1 (Small)  : < 20 MB
- Type 2 (Medium) : 20 – 200 MB
- Type 3 (Large)  : > 200 MB

> Size est = tỉ lệ tuyến tính từ testdb: `testdb_size_MB × (100K / testdb_rows)`.  
> Bảng có actual rows < 100K → dùng size thực.

---

### Type 3 — Large (> 200 MB) — 6 bảng

| Table | Dev Size Est (MB) |
|---|---:|
| AAFBNK_AA010 (AA.ACTIVITY.HISTORY) | 1,092 |
| FBNK_ACCOUNT | 360 |
| FBNK_EM_LO_APPLICATION | 338 |
| FBNK_AA_ACCOUNT_DETAILS | 328 |
| FBNK_PV_ASSET_DETAIL | 285 |
| FBNK_CUSTOMER | 218 |

---

### Type 2 — Medium (20–200 MB) — 17 bảng

| Table | Dev Size Est (MB) |
|---|---:|
| FBNK_AA_ARRANGEMENT | 131 |
| AAFBNK_AA093 (AA.ARR.PAYMENT.SCHEDULE) | 130 |
| FBNK_AA_BILL_DETAILS | 86 |
| FBNK_AA_ARR_ACCOUNT | 65 |
| FBNK_ACCOUNT_HIS | 64 |
| FBNK_STMT_ENTRY_DETAIL | 64 |
| AAFBNK_AA063 (AA.ARR.BALANCE.MAINTENANCE) | 63 |
| FBNK_AA_ARR_TERM_AMOUNT | 52 |
| FBNK_STMT_ENTRY | 50 |
| FBNK_AA_ARR_CUSTOMER | 43 |
| FBNK_CATEG_ENTRY_DETAIL | 43 |
| FBNK_CATEG_ENTRY | 37 |
| FBNK_STMT_PRINTED | 36 |
| FBNK_ACCT_ACTIVITY | 32 |
| FBNK_AC_LOCKED_EVENTS | 25 |
| FBNK_ACCOUNT_STATEMENT | 25 |
| FBNK_AA_PROCESS_DETAILS | 21 |

---

### Type 1 — Small (< 20 MB) — 13 bảng

| Table | Dev Size Est (MB) | Note |
|---|---:|---|
| FBNK_ACCT_STMT_PRINT | 8 | |
| FBNK_ACCOUNT_CLOSED | 7 | |
| ACFBNK_CA002 (CATEG.ENTRY.DETAIL.XREF) | 9 | |
| FBNK_FUNDS_TRANSFER | 9 | actual rows (10,158) |
| ACFBNK_ST002 (STMT.ENTRY.DETAIL.XREF) | 11 | |
| FBNK_CUSTOMER_ACCOUNT | 2 | |
| FBNK_AA_CUSTOMER_ROLE | <1 | actual rows (51) |
| FBNK_AC_SUB_ACCOUNT | <1 | actual rows (6) |
| FBNK_TELLER | <1 | actual rows (6) |
| FBNK_CATEG_ENT_TODAY | <1 | actual rows (3) |
| ACFBNK_AC009 (AC.ACCOUNT.SWEEP.HIST) | 0 | empty table |
| FBNK_ACCT_ENT_TODAY | 0 | empty table |
| FBNK_EM_DL_APPLICATION | 0 | empty table |
