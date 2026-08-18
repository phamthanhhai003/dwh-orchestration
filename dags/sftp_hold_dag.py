"""sftp_hold — luồng SFTP manifest-driven cho CRF/CRB/CRC (thay sftp_crb cũ).

Ý tưởng: bảng HOLD_CONTROL (T24, về bronze qua CDC) = manifest RECID→(report, branch, COB).
Query manifest → fetch ĐÚNG file theo RECID từ thư mục HOLD phẳng (KHÔNG quét folder rác).
Doc: arch/HOLD_SFTP_MANIFEST_DESIGN.md.

Trigger = COB-done (pull-style, cob_marker.cob_done → D). COB xong ⟹ T24 đã export hết file ra HOLD
⟹ KHÔNG sensor file-arrival / KHÔNG wait. Thiếu file (manifest có, HOLD vắng = purge) → fetch FAIL.

  detect (COB-done → D; idempotent: skip nếu 5 bronze đã sealed cho D)
  caught_up (đợi bronze.hold_control sealed cho D → manifest đủ; cần HOLD_CONTROL trong CDC set cob_gate)
   ├─ CRF.BNCTLGL : manifest → fetch → parse(accounting) → seal
   ├─ CRF.BNCTLPL : manifest → fetch → parse(accounting) → seal
   ├─ CRB.BNCTLGL : manifest → fetch → parse(crb_deposits) → seal
   ├─ CRC.BNCTLGL : manifest → fetch → parse(accounting) → seal
   └─ CRC.BNCTLPL : manifest → fetch → parse(accounting) → seal

Fan-out per report (song song, cô lập lỗi, seal RIÊNG mỗi bronze) — khớp pull_cob/cob_gate.
Cột bronze.hold_control (sanitize SS): recid, report_name, company_id, date_created (YYYYMMDD).
"""
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.sensors.python import PythonSensor

from lib import cob_marker
from lib import dremio
from lib import etl_control as ctl
from lib import kafka_lag

CONN_ID         = "cob_control_conn"
MINIO_CONN_ID   = "minio_conn"
SPARK_NS        = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")
HOLD_CONTROL    = "hive.bronze.hold_control"      # bronze (physical MSSQL = F_HOLD_CONTROL)
HOLD_TOPIC      = "t24.testdb.dbo.F_HOLD_CONTROL"  # Debezium topic của manifest table
MANIFEST_BUCKET = Variable.get("SFTP_MANIFEST_BUCKET", default_var="spark-scripts")
KAFKA_BOOTSTRAP = Variable.get(
    "kafka_bootstrap",
    default_var="cdc-kafka-bootstrap.bnctl-kafka-development-ns.svc.cluster.local:9092")
CKPT_PREFIX     = Variable.get("cob_checkpoint_prefix", default_var="t24-cob-v3")

# report → (bronze table, parser yaml). CRF/CRC dùng accounting parser (GL/PL text, CRF per-branch /
# CRC consolidated); CRB pattern KHÁC → crb_deposits parser riêng.
REPORTS = [
    {"report": "CRF.BNCTLGL", "bronze": "hive.bronze.crf_bnctlgl", "parser": "hold_accounting_parser_spark.yaml"},
    {"report": "CRF.BNCTLPL", "bronze": "hive.bronze.crf_bnctlpl", "parser": "hold_accounting_parser_spark.yaml"},
    {"report": "CRB.BNCTLGL", "bronze": "hive.bronze.crb_bnctlgl", "parser": "hold_crb_parser_spark.yaml"},
    {"report": "CRC.BNCTLGL", "bronze": "hive.bronze.crc_bnctlgl", "parser": "hold_accounting_parser_spark.yaml"},
    {"report": "CRC.BNCTLPL", "bronze": "hive.bronze.crc_bnctlpl", "parser": "hold_accounting_parser_spark.yaml"},
]
BRONZE_TABLES = [r["bronze"] for r in REPORTS]

_DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
_AIRFLOW_HOME = os.path.dirname(_DAG_DIR)


def _task_slug(report: str) -> str:   # task_id-safe: CRF.BNCTLGL -> crf_bnctlgl
    return report.lower().replace(".", "_")


def _dns_slug(report: str) -> str:    # k8s name-safe: CRF.BNCTLGL -> crf-bnctlgl
    return report.lower().replace(".", "-")


def _detect(**ctx) -> bool:
    """COB-done → D. Idempotent: chỉ fire COB MỚI = khi CHƯA đủ MỌI bronze sealed cho D.
    Override thủ công (replay/debug): conf {"business_date":"YYYY-MM-DD"} → bỏ marker COB shared
    + ép chạy lại (bỏ idempotent all_sealed). cob_marker.conf_override dùng chung cho cả 3 luồng."""
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
        return False  # D đã sftp đủ 5 report → chờ COB sau
    ctx["ti"].xcom_push(key="business_date", value=d)
    return True


def _caught_up(**ctx) -> bool:
    """Đảm bảo CDC đã consume hết event HOLD_CONTROL trước khi query manifest (tránh fetch thiếu).
    lag=0 (lib.kafka_lag nhánh B) ⟹ Spark stream đã cạn topic ⟹ mọi row của D đã land bronze.
    Topic chưa tồn tại / dev seed thẳng bronze ⟹ lag 0 ⟹ pass ngay (manifest đọc bronze seeded)."""
    lag = kafka_lag.lag_per_topic(KAFKA_BOOTSTRAP, CKPT_PREFIX, [HOLD_TOPIC])
    return lag.get(HOLD_TOPIC, 1) == 0


with DAG(
    dag_id="sftp_hold",
    schedule=None,                       # trigger từ cob_gate_dag (như pull_cob)
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(_AIRFLOW_HOME, "spark-app", "sync"),
        os.path.join(_AIRFLOW_HOME, "spark-app", "parser"),
    ],
    tags=["t24", "sftp", "hold", "crf", "crb", "crc", "v1.4"],
) as dag:

    detect = PythonSensor(task_id="detect", python_callable=_detect,
                          mode="reschedule", poke_interval=120, timeout=6 * 3600)

    caught_up = PythonSensor(task_id="caught_up", python_callable=_caught_up,
                             mode="reschedule", poke_interval=120, timeout=6 * 3600)

    @task
    def build_manifest(report: str, bronze: str, **ctx):
        """Query bronze.hold_control cho report+D → ghi manifest JSON lên MinIO; open_pending(sftp)."""
        d = ctx["ti"].xcom_pull(task_ids="detect", key="business_date")   # YYYY-MM-DD
        base, tok = dremio.login()
        dremio.refresh(base, tok, HOLD_CONTROL)
        # date_created = kiểu DATE (parser cast T24 date) → so bằng DATE literal, KHÔNG phải YYYYMMDD.
        rows = dremio.query(base, tok,
            f"SELECT recid, company_id FROM {HOLD_CONTROL} "
            f"WHERE report_name = '{report}' AND date_created = DATE '{d}'")
        manifest = [{"recid": str(r["recid"]), "company": str(r.get("company_id") or "NA")}
                    for r in rows]

        key = f"manifests/sftp_hold/{report}/{d}.json"
        S3Hook(aws_conn_id=MINIO_CONN_ID).load_string(
            json.dumps(manifest), key=key, bucket_name=MANIFEST_BUCKET, replace=True)

        ctx["ti"].xcom_push(key="manifest_key", value=key)
        ctx["ti"].xcom_push(key="expected", value=len(manifest))
        ctl.open_pending(ctl.hook(CONN_ID), d, bronze, flow="sftp")
        return len(manifest)

    @task
    def reconcile_seal(report: str, bronze: str, **ctx):
        """Count bronze WHERE business_date=D → seal(flow='sftp') với expected=count manifest."""
        d = ctx["ti"].xcom_pull(task_ids="detect", key="business_date")
        expected = ctx["ti"].xcom_pull(task_ids=f"manifest_{_task_slug(report)}", key="expected") or 0
        base, tok = dremio.login()
        dremio.refresh(base, tok, bronze)
        rows = dremio.query(base, tok,
            f"SELECT count(*) AS n FROM {bronze} WHERE business_date = DATE '{d}'")
        n = int(dremio.first(rows[0]["n"])) if rows else 0
        ctl.seal(ctl.hook(CONN_ID), d, bronze, flow="sftp", expected=expected, actual=n)
        return n

    for r in REPORTS:
        report, bronze, parser_yaml = r["report"], r["bronze"], r["parser"]
        tslug, dslug = _task_slug(report), _dns_slug(report)

        manifest = build_manifest.override(task_id=f"manifest_{tslug}")(
            report=report, bronze=bronze)

        fetch = SparkKubernetesOperator(
            task_id=f"fetch_{tslug}",
            namespace=SPARK_NS,
            application_file="sftp_sync_spark.yaml",
            params={"report_name": report, "slug": dslug,
                    "manifest_task_id": f"manifest_{tslug}"},
            kubernetes_conn_id="k8s", do_xcom_push=False, delete_on_termination=False,
            startup_timeout_seconds=600,
        )

        parse = SparkKubernetesOperator(
            task_id=f"parse_{tslug}",
            namespace=SPARK_NS,
            application_file=parser_yaml,
            params={"report_name": report, "slug": dslug, "bronze": bronze},
            kubernetes_conn_id="k8s", do_xcom_push=False, delete_on_termination=False,
            startup_timeout_seconds=1800,
        )

        seal = reconcile_seal.override(task_id=f"seal_{tslug}")(report=report, bronze=bronze)

        detect >> caught_up >> manifest >> fetch >> parse >> seal
