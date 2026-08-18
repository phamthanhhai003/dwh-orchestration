"""build_mart — 1 DAG fan-out build 10 silver ENQ (NHỊP 3, COB v2).

Mirror pattern pull_cob: 1 sensor COB+gate chung → fan-out per-ENQ
(prepare_vars → dbt_run → finish). Gọn hơn 10-DAG-factory cũ, vẫn giữ
granularity per-ENQ (mỗi nhánh ghi etl_model_runs riêng + skip-if-built riêng).

  detect_and_gate (cob_done D + all_sealed(mọi source-set in-scope))
   ├─ t24_acct_cust   : prepare_vars → dbt_run(--select) → finish_enq_run
   ├─ t24_ac_gic_close: prepare_vars → dbt_run → finish
   └─ ... (10 ENQ)

prepare_vars bơm snap_<dim> (snapshot ghim cob_gate) + business_date/target_date.
Silver model dùng AT SNAPSHOT (dim) · WHERE booking_date=D (event) · WHERE business_date=D (pull).
Build → hive.silver.t24_<enq> (project T24_SILVER, config materialized='table' schema='silver').
Thêm ENQ = thêm entry enq_sources.ENQ_SOURCES (KHÔNG đụng file này).
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
from lib import enq_sources
from lib import etl_control as ctl

CONN_ID    = "cob_control_conn"
DBT_TARGET = Variable.get("DBT_TARGET", default_var="dev")
_REPO_DIR  = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
_SILVER_DIR = os.path.join(_REPO_DIR, "T24_SILVER")   # 1 project chứa 14 silver ENQ

# Fetch Dremio creds 1 lần module-level (tránh Variable.get per-task → DagBag timeout).
_DREMIO_ENV = {
    "DREMIO_HOST":     Variable.get("dremio_host", default_var=""),
    "DREMIO_USER":     Variable.get("dremio_user", default_var=""),
    "DREMIO_PASSWORD": Variable.get("dremio_password", default_var=""),
}

ALL_ENQS    = list(enq_sources.ENQ_SOURCES.keys())                         # 10 in-scope
ALL_SOURCES = sorted({s for e in ALL_ENQS for s in enq_sources.source_set(e)})  # 25 cdc + 9 pull


def _snap_var(src: str) -> str:
    return "snap_" + src.rsplit("t24_", 1)[-1]


def _detect_and_gate(**ctx) -> bool:
    """1 sensor chung: COB-done(D) + MỌI source-set in-scope sealed cho D.
    Idempotent: MỌI ENQ đã built cho D → COB cũ, chờ tiếp."""
    # Override thủ công (replay/debug): conf {"business_date"} → bỏ marker + bỏ idempotent
    # all_enqs_built (ép build lại). GATE all_sealed(ALL_SOURCES) VẪN GIỮ (dependency upstream thật).
    d, _ = cob_marker.conf_override(ctx)
    forced = d is not None
    if not forced:
        base, tok = dremio.login()
        res = cob_marker.cob_done(base, tok)
        if not res:
            return False
        d, _ = res
    pg = ctl.hook(CONN_ID)
    if not forced and all(ctl.all_enqs_built(pg, enq_sources.ENQ_SOURCES[e]["division"], [e], d)
                          for e in ALL_ENQS):
        return False                       # mọi ENQ xong cho D → chờ COB sau
    if not ctl.all_sealed(pg, d, ALL_SOURCES):
        return False                       # nguồn (cdc+pull) chưa seal đủ → reschedule
    ctx["ti"].xcom_push(key="business_date", value=d)
    return True


with DAG(
    dag_id="build_mart",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,                     # throttle: ≤4 dbt build silver song song (Dremio)
    tags=["build_mart", "silver", "v2"],
) as dag:

    gate = PythonSensor(
        task_id="detect_and_gate", python_callable=_detect_and_gate,
        mode="reschedule", poke_interval=120, timeout=6 * 3600)

    @task
    def prepare_vars(enq: str, **ctx):
        """skip-if-built (per ENQ) + start_enq_run + build --vars (snap_<dim> + dates)."""
        d = ctx["ti"].xcom_pull(task_ids="detect_and_gate", key="business_date")
        pg = ctl.hook(CONN_ID)
        domain = enq_sources.ENQ_SOURCES[enq]["division"]
        # forced (replay conf) → bỏ guard idempotent thứ 2 để build lại (khớp detect_and_gate).
        forced = cob_marker.conf_override(ctx)[0] is not None
        if not forced and ctl.all_enqs_built(pg, domain, [enq], d):
            raise AirflowSkipException(f"silver {enq} đã build cho {d} → skip")
        ctl.start_enq_run(pg, domain, enq, d)
        v: dict = {"business_date": d, "target_date": d}     # gold/silver dùng cả 2
        cdc_dim = [enq_sources.BRONZE + t for t in enq_sources.ENQ_SOURCES[enq]["cdc_dim"]]
        for src, snap in ctl.sealed_snapshots(pg, d, cdc_dim).items():
            v[_snap_var(src)] = snap
        return json.dumps(v)

    @task
    def finish(enq: str, **ctx):
        d = ctx["ti"].xcom_pull(task_ids="detect_and_gate", key="business_date")
        ctl.finish_enq_run(ctl.hook(CONN_ID), enq_sources.ENQ_SOURCES[enq]["division"],
                           enq, d, status="success", models_built=1)

    for _enq in ALL_ENQS:
        pv = prepare_vars.override(task_id=f"prepare_vars_{_enq}")(enq=_enq)

        dbt_run = BashOperator(
            task_id=f"dbt_run_{_enq}",
            bash_command=(
                f"set -e; export WORK_DIR=/tmp/build_mart_{_enq}; "
                f"rm -rf $WORK_DIR && mkdir -p $WORK_DIR; "
                f"cp -R {_SILVER_DIR}/* $WORK_DIR/; cd $WORK_DIR; export DBT_PROFILES_DIR=.; "
                f"dbt run --select {_enq} "
                f"--vars '{{{{ ti.xcom_pull(task_ids=\"prepare_vars_{_enq}\") }}}}' "
                f"--target {DBT_TARGET}"
            ),
            append_env=True, env=_DREMIO_ENV, do_xcom_push=False,
        )

        fn = finish.override(task_id=f"finish_{_enq}")(enq=_enq)

        gate >> pv >> dbt_run >> fn
