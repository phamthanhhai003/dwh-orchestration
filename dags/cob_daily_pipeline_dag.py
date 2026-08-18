"""COB-driven CDC pipeline — DAG E2E mẫu (dev proof).

Chuỗi: COB-done gate (F_BATCH) → catch-up (etl_stream_progress) → pin snapshot_id
→ dbt model đọc AT SNAPSHOT → ghi etl_model_runs. Xem arch/BNCTL_DWH_Daily_Pipeline_Design.md.

- Dremio REST (Variables dremio_host/user/password) đọc bronze + progress + snapshot.
- Postgres (postgres_conn) giữ etl_control + etl_model_runs (tự tạo nếu chưa có).
- dbt project COB_TEST chạy qua BashOperator (image airflow-dbt có sẵn dbt-dremio).
- KHÔNG named tag (Dremio không đọc tag trên hive catalog) — ghim bằng snapshot_id.
"""
from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator

POSTGRES_CONN_ID = "postgres_conn"
DBT_TARGET = Variable.get("DBT_TARGET", default_var="dev")
_REPO_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DBT_DIR = os.path.join(_REPO_DIR, "COB_TEST")

COB_RECID = "BNK/COB.INITIALISE"  # COB-done = process_status (c3)==0; xem lib/cob_marker.py
BATCH_TABLE = "hive.bronze.t24_batch"
PROGRESS_TABLE = "hive.bronze.etl_stream_progress"
NEEDED = ["hive.bronze.t24_batch"]   # bảng CDC model test cần (gate + pin theo đây)
DOMAIN = "cob_test"


# ─── Dremio REST helpers ──────────────────────────────────────────────────
def _dremio_login():
    host = Variable.get("dremio_host")
    base = f"http://{host}:9047"
    r = urllib.request.urlopen(urllib.request.Request(
        base + "/apiv2/login",
        data=json.dumps({"userName": Variable.get("dremio_user"),
                         "password": Variable.get("dremio_password")}).encode(),
        headers={"Content-Type": "application/json"}))
    return base, json.load(r)["token"]


def _dremio_q(base, tok, sql):
    jid = json.load(urllib.request.urlopen(urllib.request.Request(
        base + "/api/v3/sql", data=json.dumps({"sql": sql}).encode(),
        headers={"Authorization": "_dremio" + tok,
                 "Content-Type": "application/json"})))["id"]
    st = {}
    for _ in range(40):
        time.sleep(2)
        st = json.load(urllib.request.urlopen(urllib.request.Request(
            base + f"/api/v3/job/{jid}",
            headers={"Authorization": "_dremio" + tok})))
        if st["jobState"] in ("COMPLETED", "FAILED", "CANCELED"):
            break
    if st.get("jobState") != "COMPLETED":
        raise RuntimeError(f"dremio {st.get('jobState')}: {st.get('errorMessage')}")
    return json.load(urllib.request.urlopen(urllib.request.Request(
        base + f"/api/v3/job/{jid}/results?offset=0&limit=100",
        headers={"Authorization": "_dremio" + tok}))).get("rows", [])


def _refresh(base, tok, table):
    try:
        _dremio_q(base, tok, f"ALTER TABLE {table} REFRESH METADATA")
    except Exception:
        pass


def _first(v):
    return v[0] if isinstance(v, list) and v else v


with DAG(
    dag_id="cob_daily_pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["cob", "cdc", "e2e"],
) as dag:

    @task
    def init_and_gate():
        """Tạo bảng control + đợi COB-done (F_BATCH) → D, L_cob."""
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg.run("""CREATE TABLE IF NOT EXISTS etl_control (
                    business_date date NOT NULL, source_table text NOT NULL,
                    flow text NOT NULL, status text NOT NULL DEFAULT 'pending',
                    l_cob text, iceberg_snapshot_id text, sealed_at timestamptz,
                    PRIMARY KEY (business_date, source_table));""")
        pg.run("""CREATE TABLE IF NOT EXISTS etl_model_runs (
                    domain text NOT NULL, business_date date NOT NULL,
                    status text NOT NULL DEFAULT 'running', completed_at timestamptz,
                    PRIMARY KEY (domain, business_date));""")
        base, tok = _dremio_login()
        _refresh(base, tok, BATCH_TABLE)
        rows = _dremio_q(base, tok,
            f"SELECT recid, last_run_date, process_status, \"__lsn\" AS lcob "
            f"FROM {BATCH_TABLE} WHERE recid = '{COB_RECID}'")
        rows = [r for r in rows if str(_first(r["process_status"])) == "0"]
        if not rows:
            raise RuntimeError("COB-done (COB.INITIALISE process_status=0) chưa thấy trong t24_batch")
        d = str(_first(rows[0]["last_run_date"]))[:10]
        lcob = rows[0]["lcob"]
        pg.run("""INSERT INTO etl_control (business_date, source_table, flow, l_cob, status)
                  VALUES (%s,%s,'cdc',%s,'pending')
                  ON CONFLICT (business_date, source_table)
                  DO UPDATE SET l_cob=EXCLUDED.l_cob, status='pending'""",
               parameters=(d, BATCH_TABLE, lcob))
        return {"business_date": d, "l_cob": lcob}

    @task
    def wait_caught_up(gate):
        """Gate 2 nhánh (§5):
            A: max_lsn[T] >= L_cob                      (bảng bận)
            B: stream_lsn >= L_cob AND lag[T] == 0       (bảng im)
        ⚠️ `lag` hiện CHƯA được luồng stream điền (PySpark cluster-mode không
        expose lastProgress/listener cho Python; worker image không có Kafka
        client). Logic B verified ngoài luồng ở scripts/cob_pipeline/scenario_branch_b.py.
        Để bật B live: bake `confluent-kafka` vào worker image rồi tính lag tại
        đây (Kafka end-offset − Spark committed-offset từ MinIO checkpoint), HOẶC
        cho stream publish lag qua py4j Kafka consumer. Tới đó: bảng IM rơi vào B
        sẽ WAIT (an toàn, không sai) cho tới khi lag được điền.
        """
        base, tok = _dremio_login()
        _refresh(base, tok, PROGRESS_TABLE)
        prog = {r["table_name"]: r for r in _dremio_q(base, tok, f"SELECT * FROM {PROGRESS_TABLE}")}
        lcob = gate["l_cob"]
        stream_lsn = (prog.get("__global__") or {}).get("max_lsn")
        for t in NEEDED:
            r = prog.get(t) or {}
            mx, lag = r.get("max_lsn"), r.get("lag")
            A = mx is not None and mx >= lcob
            B = (stream_lsn is not None and stream_lsn >= lcob) and lag == 0
            if not (A or B):
                raise RuntimeError(
                    f"caught_up chưa đạt {t}: A(max_lsn={mx})={A} "
                    f"B(lag={lag})={B} vs L_cob={lcob}")
        return gate

    @task
    def pin_snapshots(gate):
        """Ghim snapshot_id hiện tại (per table) → etl_control; trả vars cho dbt."""
        base, tok = _dremio_login()
        _refresh(base, tok, BATCH_TABLE)
        rows = _dremio_q(base, tok,
            f"SELECT snapshot_id FROM TABLE(table_snapshot('{BATCH_TABLE}')) "
            f"ORDER BY committed_at DESC LIMIT 1")
        snap = str(rows[0]["snapshot_id"])
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(
            """UPDATE etl_control SET iceberg_snapshot_id=%s, status='sealed', sealed_at=now()
               WHERE business_date=%s AND source_table=%s""",
            parameters=(snap, gate["business_date"], BATCH_TABLE))
        return json.dumps({"business_date": gate["business_date"], "snap_batch": snap})

    @task
    def record_success(gate):
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(
            """INSERT INTO etl_model_runs (domain, business_date, status, completed_at)
               VALUES (%s,%s,'success',now())
               ON CONFLICT (domain,business_date)
               DO UPDATE SET status='success', completed_at=now()""",
            parameters=(DOMAIN, gate["business_date"]))

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -e; export WORK_DIR=/tmp/cob_test_run; "
            "rm -rf $WORK_DIR && mkdir -p $WORK_DIR; "
            f"cp -R {DBT_DIR}/* $WORK_DIR/; cd $WORK_DIR; export DBT_PROFILES_DIR=.; "
            "dbt run --select gold_cob_test "
            "--vars '{{ ti.xcom_pull(task_ids=\"pin_snapshots\") }}' "
            f"--target {DBT_TARGET}"
        ),
        append_env=True,
        env={"DREMIO_HOST": Variable.get("dremio_host"),
             "DREMIO_USER": Variable.get("dremio_user"),
             "DREMIO_PASSWORD": Variable.get("dremio_password")},
        do_xcom_push=False,
    )

    g = init_and_gate()
    checked = wait_caught_up(g)
    pinned = pin_snapshots(checked)
    pinned >> dbt_run >> record_success(g)
