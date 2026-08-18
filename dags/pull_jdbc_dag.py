"""pull_jdbc — JDBC bulk extract cho 5 bảng giao dịch lớn, ghi vào hive.test.*.

Logic COB y hệt pull_cob_dag.py từng chữ. Khác duy nhất:
  - TABLES: 5 bảng lớn hardcode
  - _derive: bronze → hive.test.* (thay hive.bronze.*)
  - Raw output: raw/test_jdbc/ (thay raw/t24/)
  - YAML: t24_jdbc_extract / t24_jdbc_parse
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.sensors.python import PythonSensor

from lib import cob_marker
from lib import dremio
from lib import etl_control as ctl
from lib import t24_sources

CONN_ID     = "cob_control_conn"
SPARK_NS    = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")

TABLES = [
    "FBNK_CATEG_ENTRY",
    "FBNK_STMT_ENTRY_DETAIL",
    "FBNK_STMT_ENTRY",
    "FBNK_CATEG_ENTRY_DETAIL",
    "FBNK_AA_PROCESS_DETAILS",
]

_DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
_AIRFLOW_HOME = os.path.dirname(_DAG_DIR)


def _derive(name: str) -> tuple[str, str, str]:
    ss, bronze = t24_sources.resolve(name)
    bronze = bronze.replace("hive.bronze.", "hive.test.")
    slug = bronze.split(".")[-1].removeprefix("t24_").replace("_", "-")
    return ss, bronze, slug


BRONZE_TABLES = [_derive(t)[1] for t in TABLES]


def _wait_cob(**ctx) -> bool:
    # Override thủ công (replay/debug): conf {"business_date"} → bỏ marker + ép chạy lại.
    d, _ = cob_marker.conf_override(ctx)
    forced = d is not None
    if not forced:
        base, tok = dremio.login()
        res = cob_marker.cob_done(base, tok)
        if not res:
            return False
        d, _ = res
    pg = ctl.hook(CONN_ID)
    if not forced and ctl.all_sealed(pg, d, BRONZE_TABLES):
        return False
    ctx["ti"].xcom_push(key="business_date", value=d)
    for bronze in BRONZE_TABLES:
        ctl.open_pending(pg, d, bronze, flow="pull")
    return True


with DAG(
    dag_id="pull_jdbc",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(_AIRFLOW_HOME, "spark-app", "extract"),
        os.path.join(_AIRFLOW_HOME, "spark-app", "parser"),
    ],
    tags=["t24", "pull", "jdbc", "test"],
) as dag:

    get_business_date = PythonSensor(
        task_id="get_business_date", python_callable=_wait_cob,
        mode="reschedule", poke_interval=120, timeout=6 * 3600)

    @task
    def reconcile_seal(bronze: str, **ctx):
        d = ctx["ti"].xcom_pull(task_ids="get_business_date", key="business_date")
        base, tok = dremio.login()
        dremio.refresh(base, tok, bronze)
        rows = dremio.query(base, tok,
            f"SELECT count(*) AS n FROM {bronze} WHERE business_date = DATE '{d}'")
        n = int(dremio.first(rows[0]["n"])) if rows else 0
        ctl.seal(ctl.hook(CONN_ID), d, bronze, flow="pull", expected=n, actual=n)
        return n

    for name in TABLES:
        ss, bronze, slug = _derive(name)
        spark_params = {"mssql": name, "ss": ss, "bronze": bronze, "slug": slug}

        extract = SparkKubernetesOperator(
            task_id=f"extract_{slug}",
            namespace=SPARK_NS,
            application_file="t24_jdbc_extract.yaml",
            params=spark_params,
            kubernetes_conn_id="k8s", do_xcom_push=False, delete_on_termination=False,
            pool="t24_pulling", pool_slots=1,
            startup_timeout_seconds=1800,
        )

        parse = SparkKubernetesOperator(
            task_id=f"parse_{slug}",
            namespace=SPARK_NS,
            application_file="t24_jdbc_parse.yaml",
            params=spark_params,
            kubernetes_conn_id="k8s", do_xcom_push=False, delete_on_termination=False,
            pool="t24_parsing", pool_slots=1,
            startup_timeout_seconds=1800,
        )

        seal = reconcile_seal.override(task_id=f"seal_{slug}")(bronze=bronze)

        get_business_date >> extract >> parse >> seal
