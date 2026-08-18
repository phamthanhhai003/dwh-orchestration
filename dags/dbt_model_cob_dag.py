"""dbt build-gate factory (COB v1.4) — per-domain gate tầng BUILD, config-driven.

Mỗi domain trong DOMAIN_CONFIGS sinh 1 DAG. Hỗ trợ 3 chế độ gate:

  [silver mode]   enq_names ≠ []  → chờ silver ENQ built (all_enqs_built)
                  + direct_bronze_sources ≠ [] → chờ thêm bronze sealed (sftp/pull thẳng vào gold)
  [bronze mode]   enq_names = []  → legacy: chờ bronze sealed trực tiếp (all_sealed)
                  dùng cho AML_COB / demo (read thẳng từ bronze, AT SNAPSHOT cho CDC)

Flow chung:
  detect_d → wait_gate → skip_if_built → prepare_vars → dbt run → finish

snap var = "snap_<đuôi sau t24_>" (vd t24_account → snap_account) — chỉ dùng bronze mode.
Thêm model mới = thêm 1 dict vào DOMAIN_CONFIGS.
"""
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor

from lib import cob_marker
from lib import dremio
from lib import etl_control as ctl

CONN_ID     = "cob_control_conn"
DBT_TARGET  = Variable.get("DBT_TARGET", default_var="dev")
_REPO_DIR   = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# Fetch Dremio creds 1 lần ở module-level (tránh Variable.get per-DAG × N → DagBag timeout).
_DREMIO_ENV = {
    "DREMIO_HOST":     Variable.get("dremio_host", default_var=""),
    "DREMIO_USER":     Variable.get("dremio_user", default_var=""),
    "DREMIO_PASSWORD": Variable.get("dremio_password", default_var=""),
}

# ─── Domain configs — mỗi dict 1 DAG ────────────────────────────────────────
#
# Silver mode (enq_names ≠ []):
#   enq_names          : silver ENQ cần built trước (all_enqs_built)
#   direct_bronze_sources: bronze đọc thẳng vào gold (không qua silver) — chỉ cần sealed
#                          Ví dụ: CRB/CRF SFTP; để [] nếu không có
#
# Bronze mode legacy (enq_names = []):
#   cdc_sources  : bronze CDC — gate + prepare_vars sinh snap_<x>
#   pull_sources : bronze pull — chỉ cần sealed
#   sftp_sources : bronze sftp — chỉ cần sealed
#
DOMAIN_CONFIGS = [
    {   # DEMO — bronze mode (mix CDC + pull + sftp). Giữ làm chuẩn test E2E.
        "domain": "demo",
        "dag_id": "dbt_demo",
        "tags": ["dbt", "demo", "gate", "v1.4"],
        "dbt_dir": "COB_TEST",
        "dbt_select": "demo_deposit_by_sector",
        "models_built": 1,
        "enq_names": [],
        "direct_bronze_sources": [],
        "cdc_sources":  ["hive.bronze.t24_account", "hive.bronze.t24_customer"],
        "pull_sources": ["hive.bronze.t24_sector"],
        "sftp_sources": ["hive.bronze.crb_deposits"],
    },
    # ── 4 division gold (silver mode) — gold đọc hive.silver.t24_* do build_mart_dag build ──
    # enq_names = silver ENQ division cần (khớp enq_sources.enqs_for_division). Gate chờ
    # all_enqs_built(domain, enq_names, D). domain = division (silver ENQ ghi cùng domain).
    {
        "domain": "operational",
        "dag_id": "dbt_operational",
        "tags": ["dbt", "operational", "gold", "v1.4"],
        "dbt_dir": "T24_OPERATIONAL",
        "dbt_select": ("final_closed_account_report final_hnf_by_age_report "
                       "final_hnf_by_gender_report final_list_of_deposits_report "
                       "final_opening_account_report final_sms_alert_report"),
        "models_built": 6,
        "enq_names": ["t24_acct_cust", "t24_ac_gic_close"],
        "direct_bronze_sources": [],
        "cdc_sources": [], "pull_sources": [], "sftp_sources": [],
    },
    {
        "domain": "aml",
        "dag_id": "dbt_aml",
        "tags": ["dbt", "aml", "gold", "v1.4"],
        "dbt_dir": "T24_AML",
        "dbt_select": "rpt_aml03_t24 rpt_aml05_t24 rpt_9k_transaction_t24 rpt_aot_t24",
        "models_built": 4,
        "enq_names": ["t24_inactive_account_report", "t24_no_legal_doc_cus_report",
                      "t24_daily_txn_9k", "t24_e_account_open"],
        "direct_bronze_sources": [],
        "cdc_sources": [], "pull_sources": [], "sftp_sources": [],
    },
    {
        "domain": "credit",
        "dag_id": "dbt_credit",
        "tags": ["dbt", "credit", "gold", "v1.4"],
        "dbt_dir": "T24_CREDIT",
        "dbt_select": ("final_disbursement_consolidation final_extra_accountable_report "
                       "final_loan_sector_daily_report final_loan_sector_yearly_report "
                       "final_monthly_disbursement_by_branch final_provision_report "
                       "final_repayment_report"),
        "models_built": 7,
        "enq_names": ["t24_cris_report", "t24_find_arrangement_al_bnctl",
                      "t24_aa_wof_loans_report"],
        "direct_bronze_sources": [],
        "cdc_sources": [], "pull_sources": [], "sftp_sources": [],
    },
    {
        "domain": "treasury",
        "dag_id": "dbt_treasury",
        "tags": ["dbt", "treasury", "gold", "v1.4"],
        "dbt_dir": "T24_TREASURY",
        "dbt_select": "rpt_remittance_daily rpt_remittance_quarterly",
        "models_built": 2,
        "enq_names": ["t24_ft_inremit"],
        "direct_bronze_sources": [],
        "cdc_sources": [], "pull_sources": [], "sftp_sources": [],
    },
    {   # ACCOUNTING — gold đọc silver.trial_balance_detail (acct) + silver.t24_cris_report (credit,
        # cross-division qua extra_enqs) + bronze.crb_bnctlgl (sftp, direct). Build CẢ project
        # T24_ACCOUNTING (acct_count_* → stg → 12 final report). dbt_select = package selector.
        "domain": "accounting",
        "dag_id": "dbt_accounting",
        "tags": ["dbt", "accounting", "gold", "v1.4"],
        "dbt_dir": "T24_ACCOUNTING",
        "dbt_select": "t24_accounting",          # cả package (mọi model trong project)
        "models_built": 22,
        "enq_names": ["trial_balance_detail"],
        "extra_enqs": [("credit", "t24_cris_report")],   # cần cris silver cho acct_count_assets
        "direct_bronze_sources": ["hive.bronze.crb_bnctlgl"],   # acct_count_liabilities đọc thẳng
        "cdc_sources": [], "pull_sources": [], "sftp_sources": [],
    },
]


def _snap_var(src: str) -> str:
    """bronze CDC source → tên var snapshot trong model (hive.bronze.t24_account → snap_account)."""
    return "snap_" + src.rsplit("t24_", 1)[-1]


def _build_dag(cfg: dict) -> DAG:
    domain              = cfg["domain"]
    enq_names           = cfg.get("enq_names", [])
    extra_enqs          = cfg.get("extra_enqs", [])   # [(domain, enq)] silver ENQ CHÉO division
    direct_bronze       = cfg.get("direct_bronze_sources", [])
    cdc_sources         = cfg.get("cdc_sources", [])
    silver_mode         = bool(enq_names)

    # bronze mode: gate chờ tất cả bronze sources
    all_bronze_sources  = cdc_sources + cfg.get("pull_sources", []) + cfg.get("sftp_sources", [])
    dbt_dir             = os.path.join(_REPO_DIR, cfg["dbt_dir"])

    def _detect_d(**ctx) -> bool:
        # Override thủ công (replay/debug): conf {"business_date"} → bỏ marker + ép build lại.
        d, _ = cob_marker.conf_override(ctx)
        forced = d is not None
        if not forced:
            base, tok = dremio.login()
            res = cob_marker.cob_done(base, tok)
            if not res:
                return False
            d, _ = res
        if not forced and ctl.is_model_built(ctl.hook(CONN_ID), domain, d):
            return False
        ctx["ti"].xcom_push(key="business_date", value=d)
        return True

    def _gate_ready(**ctx) -> bool:
        """Gate linh hoạt:
        silver mode : all_enqs_built(enq_names) + all_sealed(direct_bronze_sources)
        bronze mode : all_sealed(cdc+pull+sftp sources)
        """
        d = ctx["ti"].xcom_pull(task_ids="detect_d", key="business_date")
        if not d:
            return False
        pg = ctl.hook(CONN_ID)

        if silver_mode:
            if not ctl.all_enqs_built(pg, domain, enq_names, d):
                return False
            # extra_enqs: silver ENQ thuộc division KHÁC (vd accounting cần t24_cris_report của
            # credit) — all_enqs_built filter theo domain nên phải check riêng từng (domain, enq).
            for xdom, xenq in extra_enqs:
                if not ctl.all_enqs_built(pg, xdom, [xenq], d):
                    return False
            if direct_bronze and not ctl.all_sealed(pg, d, direct_bronze):
                return False
            return True

        # bronze mode (legacy)
        return ctl.all_sealed(pg, d, all_bronze_sources)

    with DAG(
        dag_id=cfg["dag_id"],
        schedule=None,
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=cfg["tags"],
    ) as dag:

        detect_d = PythonSensor(task_id="detect_d", python_callable=_detect_d,
                                mode="reschedule", poke_interval=120, timeout=6 * 3600)

        wait_gate = PythonSensor(task_id="wait_gate", python_callable=_gate_ready,
                                 mode="reschedule", poke_interval=60, timeout=4 * 3600)

        @task
        def skip_if_built(**ctx):
            d = ctx["ti"].xcom_pull(task_ids="detect_d", key="business_date")
            # forced (replay conf) → bỏ guard idempotent thứ 2 để build lại (khớp detect_d).
            forced = cob_marker.conf_override(ctx)[0] is not None
            if not forced and ctl.is_model_built(ctl.hook(CONN_ID), domain, d):
                raise AirflowSkipException(f"{domain} đã build cho {d} → skip")
            ctl.start_model_run(ctl.hook(CONN_ID), domain, d)
            return d

        @task
        def prepare_vars(**ctx):
            d = ctx["ti"].xcom_pull(task_ids="detect_d", key="business_date")
            # target_date = business_date: gold/legacy model dùng var target_date làm ngày report
            v: dict = {"business_date": d, "target_date": d}
            if not silver_mode and cdc_sources:
                # bronze mode: inject snapshot_id per CDC source (AT SNAPSHOT)
                snaps = ctl.sealed_snapshots(ctl.hook(CONN_ID), d, cdc_sources)
                for src, snap in snaps.items():
                    v[_snap_var(src)] = snap
            return json.dumps(v)

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                f"set -e; export WORK_DIR=/tmp/{domain}_run; rm -rf $WORK_DIR && mkdir -p $WORK_DIR; "
                f"cp -R {dbt_dir}/* $WORK_DIR/; cd $WORK_DIR; export DBT_PROFILES_DIR=.; "
                # dbt deps nếu project có packages.yml (vd T24_AML dùng dbt_utils.generate_surrogate_key).
                # Project không có packages.yml (operational/credit/treasury) → bỏ qua, vô hại.
                "if [ -f packages.yml ]; then dbt deps; fi; "
                # Seeds (reference maps tĩnh, vd T24_ACCOUNTING map_*): best-effort (|| true) —
                # dbt-dremio KHÔNG drop seed khi --full-refresh → env mới tạo seed thiếu, env đã có
                # thì bỏ qua lỗi 'already exists'. Đổi NỘI DUNG seed = phải drop tay rồi reseed.
                f"if ls seeds/*.csv >/dev/null 2>&1; then dbt seed --target {DBT_TARGET} || true; fi; "
                # --full-refresh: gold = snapshot ngày D (current-state, KHÔNG tích luỹ history)
                # → rebuild full mỗi COB; tránh incremental MERGE hỏng (sms_alert: account_id
                #   không có trong DBT_INTERNAL_SOURCE temp — bug config incremental của model).
                f"dbt run --full-refresh --select {cfg['dbt_select']} "
                "--vars '{{ ti.xcom_pull(task_ids=\"prepare_vars\") }}' "
                f"--target {DBT_TARGET}"
            ),
            append_env=True,
            env=_DREMIO_ENV,
            do_xcom_push=False,
        )

        @task
        def finish(**ctx):
            d = ctx["ti"].xcom_pull(task_ids="detect_d", key="business_date")
            ctl.finish_model_run(ctl.hook(CONN_ID), domain, d,
                                 status="success", models_built=cfg["models_built"])

        s = skip_if_built()
        detect_d >> wait_gate >> s
        s >> prepare_vars() >> dbt_run >> finish()

    return dag


for _cfg in DOMAIN_CONFIGS:
    globals()[_cfg["dag_id"]] = _build_dag(_cfg)
