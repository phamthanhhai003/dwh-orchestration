"""pull_cob — luồng PULL cho COB pipeline: N bảng BCP → bronze t24_<x> (theo D) → seal.

Đa-bảng: 1 sensor COB chung, fan-out per-bảng (extract → parse → reconcile_seal).
Thêm bảng = append vào TABLES (cần: bảng có trong testdb + SS key có trong ss_full.json).

EXTRACT = BCP bulk copy (KubernetesPodOperator chạy t24_bcp_extract.py, ~5-10x JDBC, KHÔNG
Spark/KHÔNG pin worker-02) → raw parquet RECID/XMLRECORD ở s3a://raw/t24/<TABLE>/<D>/.
PARSE   = t24_pull_parse.yaml (đọc đúng raw path đó) → hive.bronze.t24_<x> stamp business_date.
(Đổi từ JDBC-Spark extract 2026-06-25: pull_bulk_dag của dev đã proven; cần ConfigMap
`t24-bcp-extract-script` trên cluster.)

  get_business_date (Dremio marker F_BATCH → D, open_pending N bảng)
   ├─ FBNK_SECTOR   : extract → parse(--business-date=D) → reconcile_seal(t24_sector)
   ├─ FBNK_INDUSTRY : extract → parse(--business-date=D) → reconcile_seal(t24_industry)
   └─ ...

Neo theo COB-done (đọc D từ marker F_BATCH, cùng nguồn với cob_gate — KHÔNG suy lịch).
Idempotent: sensor chỉ fire COB MỚI = khi CHƯA đủ MỌI bảng pull sealed cho D.
File-yaml (như CRB/legacy) — D + thông số bảng nhúng qua xcom/params trong yaml. KHÔNG dùng make_app.
Demo: trigger tay SAU khi cob_gate có D (PROD: sensor COB-done riêng).

⚠️ Double-ingest: `t24_pull_pipeline` (manual recovery) cũng list FBNK_SECTOR/FBNK_INDUSTRY.
   ĐỪNG chạy nó cho các bảng COB pull đang sở hữu ở đây (1-bảng-1-nguồn).
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
from lib import enq_sources
from lib import etl_control as ctl
from lib import t24_sources

CONN_ID     = "cob_control_conn"
SPARK_NS    = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")

# ── BCP extract (mirror pull_bulk_dag): plain pod chạy t24_bcp_extract.py (TDS bulk copy,
# ~5-10x JDBC, KHÔNG Spark, KHÔNG pin worker-02). Output parquet RECID/XMLRECORD về RAW path
# mà t24_pull_parse.yaml đọc (s3a://raw/t24/<TABLE>/<D>/) → parse→hive.bronze GIỮ NGUYÊN.
_BCP_IMAGE    = "congtvjits/spark-t24:build-156"
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

# Nguồn pull = CHỈ bảng PULL thật mà 9 ENQ in-scope cần (enq_sources.pull_union, 9 bảng).
# ⚠️ KHÔNG dùng t24_sources.PULL_FULL trực tiếp: nó (stale CDC=19) liệt nhiều bảng GIỜ là
# CDC (teller/account_closed/em_*_application...) → pull chúng = DOUBLE-INGEST với cob_gate.
# Lọc PULL_FULL theo classification mới (37CDC/22pull) qua enq_sources. Physical-name +
# resolve() vẫn lấy từ t24_sources (map MSSQL→bronze). Demo sector/industry bỏ (ngoài scope).
_PULL_INSCOPE = set(enq_sources.pull_union())
TABLES = [t for t in t24_sources.PULL_FULL
          if t24_sources.resolve(t)[1].rsplit("t24_", 1)[-1] in _PULL_INSCOPE]

_DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
_AIRFLOW_HOME = os.path.dirname(_DAG_DIR)


def _derive(name: str) -> tuple[str, str, str]:
    """(ss_key, bronze_table, slug) từ tên bảng MSSQL — qua t24_sources.resolve
    (xử override suffix-code + prefix lạ). slug = bronze bỏ 't24_' (DNS-safe cho yaml name)."""
    ss, bronze = t24_sources.resolve(name)
    slug = bronze.split(".")[-1].removeprefix("t24_").replace("_", "-")
    return ss, bronze, slug


BRONZE_TABLES = [_derive(t)[1] for t in TABLES]


def _wait_cob(**ctx) -> bool:
    """COB-done sensor (neo marker COB.INITIALISE qua lib.cob_marker, cùng nguồn cob_gate).
    Idempotent: chỉ fire COB MỚI — nếu MỌI bảng pull đã sealed cho D rồi thì là COB cũ, chờ tiếp.
    True → push D + open_pending(pull) cho TẤT CẢ bảng trong TABLES."""
    # Override thủ công (replay/debug): conf {"business_date"} → bỏ marker + ép chạy lại.
    d, _ = cob_marker.conf_override(ctx)
    forced = d is not None
    if not forced:
        base, tok = dremio.login()
        res = cob_marker.cob_done(base, tok)
        if not res:
            return False  # COB chưa xong → reschedule
        d, _ = res
    pg = ctl.hook(CONN_ID)
    if not forced and ctl.all_sealed(pg, d, BRONZE_TABLES):
        return False  # D đã pull đủ MỌI bảng → chờ COB sau
    ctx["ti"].xcom_push(key="business_date", value=d)
    for bronze in BRONZE_TABLES:
        ctl.open_pending(pg, d, bronze, flow="pull")
    return True


with DAG(
    dag_id="pull_cob",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(_AIRFLOW_HOME, "spark-app", "extract"),
        os.path.join(_AIRFLOW_HOME, "spark-app", "parser"),
    ],
    tags=["t24", "pull", "demo", "v1.4"],
) as dag:

    get_business_date = PythonSensor(
        task_id="get_business_date", python_callable=_wait_cob,
        mode="reschedule", poke_interval=120, timeout=6 * 3600)

    @task
    def reconcile_seal(bronze: str, **ctx):
        """Count bronze WHERE business_date=D → seal(flow='pull') cho RIÊNG bảng này."""
        d = ctx["ti"].xcom_pull(task_ids="get_business_date", key="business_date")
        base, tok = dremio.login()
        dremio.refresh(base, tok, bronze)
        rows = dremio.query(base, tok,
            f"SELECT count(*) AS n FROM {bronze} WHERE business_date = DATE '{d}'")
        n = int(dremio.first(rows[0]["n"])) if rows else 0
        # 0-tolerant (preflight #2): bảng reference rỗng (0 row nguồn) → bronze 0 row HỢP LỆ,
        # vẫn seal (KHÔNG raise) → all_sealed không kẹt. Bronze KHÔNG tồn tại = count query throw
        # ở trên (parse fail thật) → task fail đúng. Full-mode → expected=actual=n.
        ctl.seal(ctl.hook(CONN_ID), d, bronze, flow="pull", expected=n, actual=n)
        return n

    for name in TABLES:
        ss, bronze, slug = _derive(name)
        spark_params = {"mssql": name, "ss": ss, "bronze": bronze, "slug": slug}

        extract = KubernetesPodOperator(
            task_id=f"extract_{slug}",
            namespace=SPARK_NS,
            image=_BCP_IMAGE,
            image_pull_policy="IfNotPresent",
            cmds=["python3", "/opt/jobs/t24_bcp_extract.py"],  # image build-156 chỉ có python3 (python MISSING)
            arguments=[
                "--server",   _BCP_SERVER,
                "--database", _BCP_DATABASE,
                "--table",    f"dbo.{name}",
                "--output",   f"s3a://raw/t24/{name}/"
                              "{{ ti.xcom_pull(task_ids='get_business_date', key='business_date') }}/",
            ],
            env_vars=_BCP_ENV,
            volumes=_BCP_VOLUMES,
            volume_mounts=_BCP_MOUNTS,
            service_account_name="spark-sa",
            kubernetes_conn_id="k8s", do_xcom_push=False, is_delete_operator_pod=False,
            get_logs=True,
            pool="t24_pulling", pool_slots=1,   # throttle submit (chung pool với t24_pull_pipeline)
            startup_timeout_seconds=600,
        )

        parse = SparkKubernetesOperator(
            task_id=f"parse_{slug}",
            namespace=SPARK_NS,
            application_file="t24_pull_parse.yaml",
            params=spark_params,
            kubernetes_conn_id="k8s", do_xcom_push=False, delete_on_termination=False,
            pool="t24_parsing", pool_slots=1,
            startup_timeout_seconds=1800,
        )

        seal = reconcile_seal.override(task_id=f"seal_{slug}")(bronze=bronze)

        get_business_date >> extract >> parse >> seal
