"""T24 sourcing config — chốt từ arch/T24 Source Sizing & Sourcing Classification.md (2026-06-15).

Scope = 60 resolved / 65 logical. Bỏ EXCLUDE (CUSTOMER$NAU) + 5 UNRESOLVED.
Quyết định (anh Cường, 2026-06-17): **full hết, KHÔNG incremental/watermark** (field date nằm
trong XMLRECORD, không WHERE được trên (RECID,XMLRECORD)). pull.daily → pull.full. pull.once/weekly
chạy yaml standalone ngoài COB DAG. CDC 19 trên dev triển luôn.

resolve(physical) → (ss_key, bronze):
  - ss_key = T24 logical name (RECID trong ss_full.json; parser tra `--table` == RECID).
  - bronze = hive.bronze.t24_<logical lower, '.'→'_'>.
  - auto-derive cho prefix FBNK_/F_ (suffix == logical, '_'→'.').
  - ⚠️ Bảng suffix CODE (AAFBNK_AA093, ACFBNK_ST002...) → code ≠ logical → BẮT BUỘC _OVERRIDE.
"""
from __future__ import annotations

_PREFIXES = ("FBNK_", "F_")

# physical → (ss_key logical, bronze) cho bảng prefix lạ / suffix-code (auto-derive SAI).
_OVERRIDE: dict[str, tuple[str, str]] = {
    # AA (Arrangement Architecture) — physical = mã AAxxx, logical khác hẳn
    "AAFBNK_AA093": ("AA.ARR.PAYMENT.SCHEDULE",     "hive.bronze.t24_aa_arr_payment_schedule"),
    "AAFBNK_AA010": ("AA.ACTIVITY.HISTORY",         "hive.bronze.t24_aa_activity_history"),
    "AAFBNK_AA063": ("AA.ARR.BALANCE.MAINTENANCE",  "hive.bronze.t24_aa_arr_balance_maintenance"),
    "AAFBNK_AA508": ("AA.SIMULATION.RUNNER",        "hive.bronze.t24_aa_simulation_runner"),
    # AC xref — physical = mã, logical khác
    "ACFBNK_ST002": ("STMT.ENTRY.DETAIL.XREF",      "hive.bronze.t24_stmt_entry_detail_xref"),
    "ACFBNK_CA002": ("CATEG.ENTRY.DETAIL.XREF",     "hive.bronze.t24_categ_entry_detail_xref"),
    "ACFBNK_AC009": ("AC.ACCOUNT.SWEEP.HIST",       "hive.bronze.t24_ac_account_sweep_hist"),
    # $HIS — physical MSSQL dùng '_' không phải '#'. ss_key = ACCOUNT (dùng chung layout bảng live;
    # "ACCOUNT$HIS" KHÔNG có RECID riêng trong ss_full.json).
    "FBNK_ACCOUNT_HIS": ("ACCOUNT",                 "hive.bronze.t24_account_his"),
}


def resolve(physical: str) -> tuple[str, str]:
    """(ss_key, bronze) từ tên bảng MSSQL. Override trước, else strip prefix."""
    if physical in _OVERRIDE:
        return _OVERRIDE[physical]
    for p in _PREFIXES:
        if physical.startswith(p):
            suffix = physical[len(p):]
            break
    else:
        suffix = physical
    return suffix.replace("_", "."), f"hive.bronze.t24_{suffix.lower()}"


# ── (a) PULL.FULL daily — vào COB pull DAG (37 PULL.FULL resolved + 2 PULL.DAILY→full) ──────────
#    35 bảng. SECTOR/INDUSTRY = demo reference (NGOÀI 60-list) — giữ cho Gold demo, xem note dưới.
PULL_FULL = [
    # FBNK_/F_ prefix — auto-derive OK
    "FBNK_MCB_GROUP", "FBNK_AA_PRODUCT", "F_AC_STMT_PARAMETER",
    "F_ARCHIVE", "F_BNCTL_CUS_DIST", "F_BNCTL_CUS_SUBDIST", "F_BNCTL_CUS_VILL",
    "F_CATEGORY", "F_COMPANY", "F_COMPANY_SMS_GROUP", "FBNK_CURRENCY",
    "F_DEPT_ACCT_OFFICER", "F_EB_LOOKUP", "F_EB_SYSTEM_ID",
    "F_GIC_ID", "F_MNEMONIC_COMPANY", "F_POSTING_RESTRICT", "FBNK_TRANSACTION", "F_USER",
    # PULL.DAILY → full (bỏ watermark)
    "F_COMPANY_CONSOL",
    # coded suffix — qua _OVERRIDE
    "AAFBNK_AA508",   # AA.SIMULATION.RUNNER
]

# LOẠI (preflight #2, 2026-06-17) — không parse được qua cN-positional:
#   F_ACCOUNT_CONCAT  : concat index 1 field, KHÔNG có SS RECID → parser không map cột.
#   F_PL_CLOSE_DATES  : XMLRECORD = chuỗi thô "20251231CL" (KHÔNG phải <row><cN> XML).
# 4 bảng rỗng (0 row) VẪN giữ trong list — reconcile_seal nay 0-tolerant (seal exp=act=0):
#   ACFBNK_AC009 · FBNK_ACCT_ENT_TODAY · FBNK_EM_DL_APPLICATION · F_COMPANY_SMS_GROUP.

# Demo reference (giữ cho Gold demo_deposit_by_sector; KHÔNG trong 60-list classification).
PULL_FULL_DEMO = ["FBNK_SECTOR", "FBNK_INDUSTRY"]

# ── (b) CDC target (35) — vào spark/manifests/cdc-tables.yaml + connector include.list ──────────
#    Synced DWH-Daily-Tracking 2026-06-21. Verified: is_tracked_by_cdc=1 + ss_full.json.
#    Skip: F_ACCOUNT_CONCAT (ss_full MISSING + tracking=recheck).
#    FBNK_ACCOUNT_HIS: enabled via :ss=ACCOUNT override trong cdc-tables.yaml (streaming script hỗ trợ).
CDC_TABLES = [
    # batch 1 — original 19
    "FBNK_STMT_ENTRY", "FBNK_STMT_ENTRY_DETAIL", "FBNK_CATEG_ENTRY", "FBNK_CATEG_ENTRY_DETAIL",
    "FBNK_AA_PROCESS_DETAILS", "FBNK_ACCT_ACTIVITY", "FBNK_ACCOUNT", "AAFBNK_AA010",
    "FBNK_AA_ARR_ACCOUNT", "FBNK_AA_BILL_DETAILS", "FBNK_CUSTOMER", "FBNK_AA_ACCOUNT_DETAILS",
    "FBNK_PV_ASSET_DETAIL", "FBNK_ACCOUNT_STATEMENT", "FBNK_AA_ARRANGEMENT", "AAFBNK_AA063",
    "FBNK_AC_LOCKED_EVENTS", "FBNK_FUNDS_TRANSFER", "FBNK_CATEG_ENT_TODAY",
    # batch 2 — 2026-06-21 sync tracking (CDC enabled + ss_full verified)
    "ACFBNK_ST002", "ACFBNK_CA002", "ACFBNK_AC009", "AAFBNK_AA093",
    "FBNK_CUSTOMER_ACCOUNT", "FBNK_STMT_PRINTED", "FBNK_AA_ARR_CUSTOMER",
    "FBNK_AA_ARR_TERM_AMOUNT", "FBNK_ACCOUNT_CLOSED", "FBNK_ACCT_STMT_PRINT",
    "FBNK_EM_LO_APPLICATION", "F_AA_CUSTOMER_ROLE", "FBNK_AC_SUB_ACCOUNT",
    "FBNK_TELLER", "FBNK_ACCT_ENT_TODAY", "FBNK_EM_DL_APPLICATION",
    "FBNK_ACCOUNT_HIS",
]

# ── (c) Standalone (yaml ngoài DAG, full, chạy tay) — 4 ONCE + 1 WEEKLY ─────────────────────────
PULL_STANDALONE = [
    #"FBNK_ACCOUNT_HIS",   # ACCOUNT$HIS  ~21GB  (once seed) — moved to CDC_TABLES (CDC=1 confirmed)
]

# Bỏ: EXCLUDE FBNK_CUSTOMER#NAU; UNRESOLVED AA.ARRANGEMENT.ACTIVITY.HISTORY /
#      AC.ACCOUNT.PARAMETER / AU.COMPANY.DECOMMISSION / ECB / TODAY.TXN.ACC (chưa có physical name).
