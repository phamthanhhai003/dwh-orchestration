"""pull_bulk — BCP bulk extract CDC tables (hàng chục triệu dòng).

Logic COB y hệt pull_cob_dag.py (sensor → open_pending → extract → parse → seal).
Khác pull_cob ở extract: dùng bcp (TDS Bulk Copy) thay JDBC — throughput cao hơn ~5-10x.
  - extract : KubernetesPodOperator (plain pod, no Spark overhead) → t24_bcp_extract.py
  - parse   : t24_bulk_parse.yaml   (SparkApp, input = s3a://raw/test_bulk/)
Output test path: s3a://raw/test_bulk/<TABLE>/<D>/ (không ảnh hưởng prod path raw/t24/).
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.sensors.python import PythonSensor
from kubernetes.client import (
    V1ConfigMapVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1SecretKeySelector,
    V1Volume,
    V1VolumeMount,
)

from lib import cob_marker
from lib import dremio
from lib import etl_control as ctl
from lib import t24_sources

CONN_ID   = "cob_control_conn"
SPARK_NS  = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")
_DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
_AIRFLOW_HOME = os.path.dirname(_DAG_DIR)

_BCP_SERVER   = "mssql.bnctl-kafka-development-ns.svc.cluster.local"
_BCP_DATABASE = "testdb"

_BCP_ENV = [
    V1EnvVar(name="MSSQL_PASSWORD",
             value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name="mssql-credentials", key="MSSQL_PASSWORD"))),
    V1EnvVar(name="AWS_ACCESS_KEY_ID",
             value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name="minio-credentials", key="AWS_ACCESS_KEY_ID"))),
    V1EnvVar(name="AWS_SECRET_ACCESS_KEY",
             value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name="minio-credentials", key="AWS_SECRET_ACCESS_KEY"))),
    V1EnvVar(name="AWS_REGION", value="us-east-1"),
    V1EnvVar(name="MINIO_ENDPOINT", value="http://minio.bnctl-minio-development-ns.svc.cluster.local:9000"),
]

_BCP_VOLUMES = [V1Volume(name="bcp-script", config_map=V1ConfigMapVolumeSource(name="t24-bcp-extract-script"))]
_BCP_MOUNTS  = [V1VolumeMount(name="bcp-script", mount_path="/opt/jobs/t24_bcp_extract.py", sub_path="t24_bcp_extract.py", read_only=True)]

TABLES = [
    "AAFBNK_AA508",
    "FBNK_MCB_GROUP",
    "F_EB_LOOKUP",
    "F_CATEGORY",
    "FBNK_TRANSACTION",
    "F_DEPT_ACCT_OFFICER",
    "F_USER",
    "F_BNCTL_CUS_VILL",
    "FBNK_AA_PRODUCT",
    "F_EB_SYSTEM_ID",
    "F_BNCTL_CUS_SUBDIST",
    "F_ARCHIVE",
    "F_POSTING_RESTRICT",
    "F_COMPANY",
    "F_GIC_ID",
    "F_MNEMONIC_COMPANY",
    "F_BNCTL_CUS_DIST",
    "FBNK_CURRENCY",
    "F_COMPANY_CONSOL",
    "F_AC_STMT_PARAMETER",
    "F_PL_CLOSE_DATES",
    "F_COMPANY_SMS_GROUP",
]


def _derive(name: str) -> tuple[str, str, str]:
    """(ss_key, bronze, slug) — qua t24_sources.resolve (y hệt pull_cob_dag)."""
    ss, bronze = t24_sources.resolve(name)
    slug = bronze.split(".")[-1].removeprefix("t24_").replace("_", "-")
    return ss, bronze, slug


TEST_TABLES = [_derive(t)[1].replace("hive.bronze.", "hive.test.") for t in TABLES]


def _wait_cob(**ctx) -> bool:
    import traceback, logging
    log = logging.getLogger(__name__)
    try:
        base, tok = dremio.login()
        res = cob_marker.cob_done(base, tok)
        if not res:
            return False
        d, _ = res
        pg = ctl.hook(CONN_ID)
        if ctl.all_sealed(pg, d, TEST_TABLES):
            return False
        ctx["ti"].xcom_push(key="business_date", value=d)
        for tbl in TEST_TABLES:
            ctl.open_pending(pg, d, tbl, flow="pull")
        return True
    except Exception as e:
        log.error("_wait_cob FAILED: %s\n%s", e, traceback.format_exc())
        raise


with DAG(
    dag_id="pull_bulk_fresh",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(_AIRFLOW_HOME, "spark-app", "extract"),
        os.path.join(_AIRFLOW_HOME, "spark-app", "parser"),
    ],
    tags=["t24", "pull", "bulk", "bcp", "fresh"],
) as dag:

    get_business_date = PythonSensor(
        task_id="get_business_date",
        python_callable=_wait_cob,
        mode="reschedule",
        poke_interval=120,
        timeout=6 * 3600,
    )

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
        test_tbl = bronze.replace("hive.bronze.", "hive.test.")
        spark_params = {"mssql": name, "ss": ss, "bronze": bronze, "slug": slug}

        extract = KubernetesPodOperator(
            task_id=f"extract_{slug}",
            namespace=SPARK_NS,
            image="congtvjits/spark-t24:build-156",
            image_pull_policy="IfNotPresent",
            cmds=["python3", "/opt/jobs/t24_bcp_extract.py"],
            arguments=[
                "--server",   _BCP_SERVER,
                "--database", _BCP_DATABASE,
                "--table",    f"dbo.{name}",
                "--output",   f"s3a://raw/test_fresh/{name}/"
                              "{{ ti.xcom_pull(task_ids='get_business_date', key='business_date') }}/",
            ],
            env_vars=_BCP_ENV,
            volumes=_BCP_VOLUMES,
            volume_mounts=_BCP_MOUNTS,
            service_account_name="spark-sa",
            kubernetes_conn_id="k8s",
            do_xcom_push=False,
            is_delete_operator_pod=False,
            get_logs=True,
            pool="t24_pulling", pool_slots=1,
            startup_timeout_seconds=600,
        )

        parse = SparkKubernetesOperator(
            task_id=f"parse_{slug}",
            namespace=SPARK_NS,
            application_file="t24_bulk_fresh_parse.yaml",
            params={**spark_params, "bronze": test_tbl},
            kubernetes_conn_id="k8s", do_xcom_push=False, delete_on_termination=False,
            pool="t24_parsing", pool_slots=1,
            startup_timeout_seconds=1800,
        )

        seal = reconcile_seal.override(task_id=f"seal_{slug}", pool="t24_sealing", pool_slots=1)(bronze=test_tbl)

        get_business_date >> extract >> parse >> seal
