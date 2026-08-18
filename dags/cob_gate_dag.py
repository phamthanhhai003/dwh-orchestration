"""cob_gate — DAG global tầng INGEST (sensor #1 COB-done + caught_up CDC + pin + seal).

Xem arch/PIPELINE_ORCHESTRATION_DESIGN.md §2b, §4b. Đây là "nguồn D duy nhất":
  detect_cob (sensor reschedule, F_BATCH marker) → D, L_cob
  → open_pending(cdc) vào etl_control
  → wait_caught_up (sensor reschedule): mỗi bảng CDC caught_up = nhánh A (max_lsn>=L_cob)
      OR nhánh B (stream_lsn>=L_cob AND lag=0) → bảng IM/rỗng không treo (lib.kafka_lag)
  → pin_and_seal: ghim snapshot_id từng bảng → seal(flow='cdc')

Downstream (pull / sftp / dbt per-domain) tự đọc etl_control, KHÔNG bị DAG này gọi thẳng.
DDL bảng control: scripts/cob_pipeline/01_postgres_control_tables.sql (DB cob_control).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.standard.sensors.python import PythonSensor

from lib import cob_marker
from lib import dremio
from lib import enq_sources
from lib import etl_control as ctl
from lib import kafka_lag
from lib import t24_sources

CONN_ID = "cob_control_conn"
PROGRESS_TABLE = "hive.bronze.etl_stream_progress"
# Nhánh B (im table): cấu hình Kafka + checkpoint Spark trên MinIO.
KAFKA_BOOTSTRAP = Variable.get(
    "kafka_bootstrap",
    default_var="cdc-kafka-bootstrap.bnctl-kafka-development-ns.svc.cluster.local:9092")
CKPT_PREFIX = Variable.get("cob_checkpoint_prefix", default_var="t24-cob-v3")
# Bảng CDC mà downstream cần gate+pin = HỢP của mọi domain (batch chỉ là marker).
#   demo cần: account+customer. aml cần thêm: stmt_entry (9k/aot).
#   ⚠️ thêm bảng ở đây = wait_caught_up CHỜ bảng đó caught_up → seed PHẢI bump LSN bảng đó
#     (nếu không sẽ chờ vô hạn). Mỗi domain build-gate vẫn chỉ chờ subset riêng của nó.
# Union bảng CDC mà ENQ in-scope cần (derive từ enq_sources; 25 bảng).
# wait_caught_up = nhánh A (max_lsn) OR nhánh B (lag=0) → bảng IM/rỗng KHÔNG còn treo.
CDC_SOURCES = [enq_sources.BRONZE + t for t in enq_sources.cdc_union()]

# TABLE_TOPIC: bronze fully-qualified → Kafka topic Debezium (t24.testdb.dbo.<physical>).
# physical reverse-resolve từ t24_sources; account_his testdb dùng '_' không phải '#'.
def _topic_map() -> dict:
    rev = {}
    for p in (t24_sources.CDC_TABLES + t24_sources.PULL_FULL + t24_sources.PULL_STANDALONE):
        try:
            rev[t24_sources.resolve(p)[1]] = p   # bronze fq -> physical
        except Exception:
            pass
    out = {}
    for src in CDC_SOURCES:
        phys = rev.get(src)
        if phys:
            phys = phys.replace("FBNK_ACCOUNT#HIS", "FBNK_ACCOUNT_HIS")
            out[src] = f"t24.testdb.dbo.{phys}"
    return out

TABLE_TOPIC = _topic_map()


# ─── sensor #1: COB-done (đọc marker COB.INITIALISE qua lib.cob_marker) ───────
def _detect_cob(**ctx) -> bool:
    # Override thủ công (replay/debug): conf {"business_date","l_cob"} → bỏ marker + ép chạy lại.
    d, lcob = cob_marker.conf_override(ctx)
    forced = d is not None
    if not forced:
        base, tok = dremio.login()
        res = cob_marker.cob_done(base, tok)  # (D, l_cob) khi process_status=0, else None
        if not res:
            return False  # COB chưa xong (running/2) hoặc chưa khởi tạo → reschedule
        d, lcob = res
    # Design (idempotent): chỉ fire COB MỚI — nếu D đã sealed cdc rồi thì đây là COB cũ
    # (số-0 tồn), tiếp tục chờ COB sau (KHÔNG vồ lại marker đã xử lý). forced → bỏ qua (ép replay).
    if not forced and ctl.all_sealed(ctl.hook(CONN_ID), d, CDC_SOURCES):
        return False
    ti = ctx["ti"]
    ti.xcom_push(key="business_date", value=d)
    ti.xcom_push(key="l_cob", value=lcob)
    return True


# ─── sensor #2: caught_up (stream nuốt tới L_cob chưa) ────────────────────────
def _caught_up(**ctx) -> bool:
    """caught_up(T) = A OR B:
      A (bảng bận): max_lsn[T] >= L_cob
      B (bảng im) : stream_lsn(__global__) >= L_cob  AND  lag[topic_T] == 0
        lag = kafka_end_offset − spark_committed_offset (lib.kafka_lag). lag=0 ⇒ Spark đã
        consume hết event topic T (im/cạn). Bảng rỗng (topic chưa có/đã cạn) ⇒ lag 0 ⇒ pass."""
    ti = ctx["ti"]
    lcob = ti.xcom_pull(task_ids="detect_cob", key="l_cob")
    base, tok = dremio.login()
    dremio.refresh(base, tok, PROGRESS_TABLE)
    prog = {r["table_name"]: r for r in dremio.query(base, tok, f"SELECT * FROM {PROGRESS_TABLE}")}

    sg = (prog.get("__global__") or {}).get("max_lsn")
    global_caught = sg is not None and str(sg) >= str(lcob)

    # bảng chưa qua nhánh A → ứng viên nhánh B (cần lag). Chỉ gọi Kafka khi global đã qua mốc
    # (tránh query Kafka mỗi poke khi stream chưa tới L_cob).
    b_candidates = [t for t in CDC_SOURCES
                    if not (str((prog.get(t) or {}).get("max_lsn")) >= str(lcob)
                            and (prog.get(t) or {}).get("max_lsn") is not None)]
    lag = {}
    if global_caught and b_candidates:
        topics = [TABLE_TOPIC[t] for t in b_candidates if t in TABLE_TOPIC]
        try:
            lag = kafka_lag.lag_per_topic(KAFKA_BOOTSTRAP, CKPT_PREFIX, topics)
        except Exception as exc:               # Kafka/MinIO lỗi → coi như chưa caught (reschedule)
            print(f"[caught_up] lag tính lỗi → reschedule: {exc}")
            return False

    for t in CDC_SOURCES:
        mx = (prog.get(t) or {}).get("max_lsn")
        if mx is not None and str(mx) >= str(lcob):
            continue                           # A
        topic = TABLE_TOPIC.get(t)
        if global_caught and topic is not None and lag.get(topic, 1) == 0:
            continue                           # B
        return False                           # chưa caught_up → reschedule
    return True


with DAG(
    dag_id="cob_gate",
    schedule=None,            # PROD: cron (vd "0 1 * * *"); demo trigger tay
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cob", "gate", "ingest", "v1.4"],
) as dag:

    # ⚠️ TODO liveness: hiện chỉ timeout (COB không tới sau 6h → fail+alert).
    # Cần thêm check stream còn sống (etl_stream_progress.__global__ tiến) kẻo chờ stream chết.
    detect_cob = PythonSensor(
        task_id="detect_cob",
        python_callable=_detect_cob,
        mode="reschedule",
        poke_interval=120,
        timeout=6 * 3600,
    )

    @task
    def open_pending(**ctx):
        ti = ctx["ti"]
        d = ti.xcom_pull(task_ids="detect_cob", key="business_date")
        lcob = ti.xcom_pull(task_ids="detect_cob", key="l_cob")
        pg = ctl.hook(CONN_ID)
        for src in CDC_SOURCES:
            ctl.open_pending(pg, d, src, flow="cdc", l_cob=lcob)
        return d

    wait_caught_up = PythonSensor(
        task_id="wait_caught_up",
        python_callable=_caught_up,
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 3600,
    )

    @task
    def pin_and_seal(**ctx):
        ti = ctx["ti"]
        d = ti.xcom_pull(task_ids="detect_cob", key="business_date")
        base, tok = dremio.login()
        pg = ctl.hook(CONN_ID)
        for src in CDC_SOURCES:
            snap = dremio.latest_snapshot_id(base, tok, src)
            ctl.seal(pg, d, src, flow="cdc", snapshot_id=snap)
        return d

    detect_cob >> open_pending() >> wait_caught_up >> pin_and_seal()
