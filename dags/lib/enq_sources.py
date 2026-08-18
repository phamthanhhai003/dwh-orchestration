"""Source-set mapping cho 14 silver ENQ → flow (cdc/pull) + dim/event.

AUTHORITATIVE classification CDC/pull = arch/list_table.xlsx (37 CDC + 22 PULL + 1 EXCLUDE).
KHÔNG dùng dags/lib/t24_sources.py (CDC=19, stale) làm nguồn phân loại — xem
memory reference_t24_cdc_pull_classification.

Quyết định 1 (anh Cường, 2026-06-24): silver đọc nguồn theo LOẠI bảng —
  * cdc_dim   (current-state): FROM hive.bronze.t24_x AT SNAPSHOT '{{ var("snap_x") }}'
  * cdc_event (transaction)  : WHERE <date_col> = date '{{ var("business_date") }}'
  * pull                     : WHERE business_date = date '{{ var("business_date") }}'

Mỗi ENQ khai 3 nhóm: cdc_dim=[...], cdc_event=[(table,date_col),...], pull=[...].
Gate (build_mart_dag) chờ all_sealed(cdc_dim ∪ cdc_event-tables ∪ pull) cho D.
prepare_vars bơm snapshot_id cho từng cdc_dim; cdc_event + pull chỉ cần business_date.

Tên bảng = phần sau 'hive.bronze.t24_' (vd 'account', 'stmt_entry').
"""

# ── Authoritative classification (arch/list_table.xlsx, cột method) ──────────────
# 22 PULL.FRESH (seal business_date). Còn lại trong scope = CDC (pin snapshot / date-filter).
PULL_TABLES = {
    "company", "company_consol", "company_sms_group", "currency", "category",
    "transaction", "posting_restrict", "archive", "user", "gic_id", "eb_lookup",
    "eb_system_id", "dept_acct_officer", "mnemonic_company", "mcb_group",
    "aa_product", "aa_simulation_runner", "ac_stmt_parameter",
    "bnctl_cus_dist", "bnctl_cus_subdist", "bnctl_cus_vill", "pl_close_dates",
}

# ── 9 ENQ IN-SCOPE (AML/Credit/Operational/Treasury). Accounting + 5 ENQ orphan đã bỏ ──
#   division: ENQ thuộc gold division nào (9 ENQ in-scope KHÔNG chia sẻ chéo division).
#   ⚠️ ft_inremit.funds_transfer date_col = TODO confirm anh Cường (xem note cuối file).
ENQ_SOURCES = {
    # ── OPERATIONAL ──────────────────────────────────────────────────────────
    "t24_acct_cust": {
        "division": "operational",
        "cdc_dim": ["account", "customer"],
        "cdc_event": [],
        "pull": [],
    },
    "t24_ac_gic_close": {
        "division": "operational",
        # account_closed: filter cumulative WHERE acct_close_date <= D (đọc snapshot, KHÔNG event)
        "cdc_dim": ["account_closed", "account_his"],
        "cdc_event": [],
        "pull": ["posting_restrict"],
    },
    # ── TREASURY ─────────────────────────────────────────────────────────────
    "t24_ft_inremit": {
        "division": "treasury",
        # funds_transfer = SNAPSHOT (logic.md: filter bắt buộc DUY NHẤT = TRANSACTION.TYPE='IT',
        # KHÔNG period window/date WHERE; "Today" implicit từ live FT file). Date slicing để ở
        # gold (remittance daily=value/processing date=D · quarterly=range) đọc chung silver.
        "cdc_dim": ["account", "funds_transfer"],
        "cdc_event": [],
        "pull": [],
    },
    # ── AML ──────────────────────────────────────────────────────────────────
    "t24_inactive_account_report": {
        "division": "aml",
        # account: cumulative last_txn >= D-30 (snapshot; thay CURRENT_DATE bằng business_date)
        "cdc_dim": ["account", "customer"],
        "cdc_event": [],
        "pull": ["company"],
    },
    "t24_no_legal_doc_cus_report": {
        "division": "aml",
        "cdc_dim": ["account", "customer", "customer_account"],
        "cdc_event": [],
        "pull": ["company", "eb_lookup", "gic_id"],
    },
    "t24_daily_txn_9k": {
        "division": "aml",
        "cdc_dim": ["account", "customer", "acct_activity", "teller"],
        "cdc_event": [("stmt_entry", "booking_date")],  # confirmed (model comment + report single-day)
        "pull": ["transaction", "eb_lookup", "gic_id", "company"],  # +company (fresh branch 8640b44)
    },
    "t24_e_account_open": {   # AOT — Accounts Opened Today (rpt_aot_t24). In-scope từ branch fresh.
        "division": "aml",
        # account driver WHERE opening_date=D (snapshot); stmt_entry* cho first-deposit (snapshot)
        "cdc_dim": ["account", "customer", "stmt_entry", "stmt_entry_detail",
                    "stmt_entry_detail_xref"],
        "cdc_event": [],
        "pull": ["company", "eb_lookup"],
    },
    # ── CREDIT ───────────────────────────────────────────────────────────────
    "t24_cris_report": {
        "division": "credit",
        "cdc_dim": ["account", "customer", "aa_account_details",
                    "aa_arr_payment_schedule", "pv_asset_detail",
                    "aa_arr_term_amount"],  # +aa_arr_term_amount (fresh branch 8640b44)
        "cdc_event": [],
        "pull": ["category", "dept_acct_officer"],
    },
    "t24_find_arrangement_al_bnctl": {
        "division": "credit",
        "cdc_dim": ["customer", "aa_arrangement", "aa_account_details",
                    "aa_arr_customer", "aa_arr_term_amount", "aa_bill_details",
                    "aa_customer_role", "em_dl_application", "em_lo_application"],
        "cdc_event": [],
        "pull": ["aa_product", "mcb_group"],
    },
    "t24_aa_wof_loans_report": {
        "division": "credit",
        "cdc_dim": ["customer", "account_his", "aa_arrangement", "aa_arr_account",
                    "aa_arr_customer", "aa_arr_balance_maintenance",
                    "aa_activity_history", "aa_process_details"],
        "cdc_event": [],
        "pull": ["aa_product", "mcb_group"],
    },
    # ── ACCOUNTING ───────────────────────────────────────────────────────────
    # Nguồn SFTP native T24 (CRF GL/PL), seal flow='sftp' bởi sftp_hold (KHÔNG cdc/pull).
    # silver trial_balance_detail (T24_SILVER) đọc crf_bnctlgl(GL)+crf_bnctlpl(PL) theo business_date.
    # Gold T24_ACCOUNTING (dbt_accounting) còn cần cris (credit silver) + crb_bnctlgl (sftp) —
    # khai ở DOMAIN_CONFIGS (extra_enqs + direct_bronze_sources), KHÔNG ở đây.
    "trial_balance_detail": {
        "division": "accounting",
        "cdc_dim": [],
        "cdc_event": [],
        "pull": [],
        "sftp": ["crf_bnctlgl", "crf_bnctlpl"],   # bronze name KHÔNG có prefix t24_
    },
}

# ── Derived helpers ─────────────────────────────────────────────────────────────
BRONZE = "hive.bronze.t24_"
SFTP_BRONZE = "hive.bronze."          # sftp bronze (crf/crb/crc) không mang prefix t24_


def _all_tables(spec):
    """Bảng cdc+pull (sau prefix t24_) 1 ENQ cần. KHÔNG gồm sftp (prefix khác)."""
    return spec["cdc_dim"] + [t for t, _ in spec["cdc_event"]] + spec["pull"]


def source_set(enq):
    """Source-set (fully-qualified bronze) để gate chờ all_sealed cho 1 ENQ.
    cdc/pull dùng prefix hive.bronze.t24_; sftp (crf/crb) dùng hive.bronze. (không t24_)."""
    spec = ENQ_SOURCES[enq]
    return ([BRONZE + t for t in _all_tables(spec)]
            + [SFTP_BRONZE + t for t in spec.get("sftp", [])])


def cdc_union(enqs=None):
    """Union bảng CDC (dim + event) mọi ENQ in-scope cần → cob_gate CDC_SOURCES."""
    enqs = enqs or ENQ_SOURCES.keys()
    out = set()
    for e in enqs:
        s = ENQ_SOURCES[e]
        out |= set(s["cdc_dim"]) | {t for t, _ in s["cdc_event"]}
    return sorted(out)


def pull_union(enqs=None):
    """Union bảng pull mọi ENQ in-scope cần → pull_cob TABLES."""
    enqs = enqs or ENQ_SOURCES.keys()
    return sorted({t for e in enqs for t in ENQ_SOURCES[e]["pull"]})


def enqs_for_division(domain):
    return [e for e, s in ENQ_SOURCES.items() if s["division"] == domain]


def _validate():
    """cdc_* phải KHÔNG nằm trong PULL_TABLES và ngược lại (chống lệch classification)."""
    bad = []
    for e, s in ENQ_SOURCES.items():
        for t in s["cdc_dim"] + [x for x, _ in s["cdc_event"]]:
            if t in PULL_TABLES:
                bad.append(f"{e}: '{t}' khai CDC nhưng PULL_TABLES có nó")
        for t in s["pull"]:
            if t not in PULL_TABLES:
                bad.append(f"{e}: '{t}' khai pull nhưng KHÔNG trong PULL_TABLES")
    return bad


if __name__ == "__main__":
    errs = _validate()
    print("VALIDATION:", "OK" if not errs else errs)
    print(f"\nCDC union ({len(cdc_union())}):", cdc_union())
    print(f"\nPULL union ({len(pull_union())}):", pull_union())
    for dom in ("aml", "credit", "operational", "treasury"):
        print(f"\n{dom}: {enqs_for_division(dom)}")
