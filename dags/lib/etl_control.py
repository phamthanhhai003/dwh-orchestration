"""Helper dùng chung cho orchestration COB pipeline — đọc/ghi bảng control Postgres.

Hợp đồng readiness 3 nhịp:
  Nhịp 1 — Bronze: luồng ingest (cdc/pull/sftp) SEAL (source_table, D) → etl_control.
  Nhịp 2 — Silver: BUILD_REPORT dbt chạy từng ENQ, ghi (domain, enq_name, layer='silver', D).
  Nhịp 3 — Gold:   Division dbt chờ đủ ENQ silver → ghi (domain, enq_name='', layer='gold', D).
Xem arch/PIPELINE_ORCHESTRATION_DESIGN.md.

Bảng (DDL: scripts/cob_pipeline/01_postgres_control_tables.sql):
  etl_control(business_date, source_table, flow, status, expected_count,
              actual_count, l_cob, iceberg_snapshot_id, sealed_at)
  etl_model_runs(domain, enq_name, layer, business_date, status,
                 dbt_invocation_id, models_built, started_at, completed_at)
    layer='silver': enq_name = tên ENQ; layer='gold': enq_name = '' (sentinel).
  etl_ingest_logs(source_name, flow, business_date, is_sync, is_parsed,
                  synced_at, parsed_at, error_message)
    flow='pull': source_name = bronze table; flow='sftp': source_name = report_type.

Convention: hàm nhận PostgresHook (`pg`). Lấy hook bằng hook() hoặc tự tạo.
"""
from __future__ import annotations

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Conn trỏ DB `cob_control` (tách riêng cho design v1.4, KHÔNG lẫn DB etl_control legacy).
# Tạo Airflow connection `cob_control_conn` → host postgres primary, dbname=cob_control.
DEFAULT_CONN_ID = "cob_control_conn"


def hook(conn_id: str = DEFAULT_CONN_ID) -> PostgresHook:
    return PostgresHook(postgres_conn_id=conn_id)


# Nguồn DDL DUY NHẤT = file SQL (xem PIPELINE_ORCHESTRATION_DESIGN §3). Bootstrap
# bằng cách chạy file này qua PostgresHook (KHÔNG re-declare CREATE TABLE inline).
DEFAULT_DDL_PATH = "/opt/airflow/dags/repo/scripts/cob_pipeline/01_postgres_control_tables.sql"


def apply_control_ddl(pg: PostgresHook, sql_path: str = DEFAULT_DDL_PATH) -> None:
    """Apply DDL control (idempotent CREATE TABLE IF NOT EXISTS + ALTER). Gọi 1 lần khi
    bootstrap. sql_path mặc định trỏ repo trong pod; override nếu khác."""
    with open(sql_path, "r", encoding="utf-8") as f:
        pg.run(f.read())


# ── etl_control ──────────────────────────────────────────────────────────────
def open_pending(pg: PostgresHook, business_date: str, source_table: str,
                 flow: str, l_cob: str | None = None) -> None:
    """Mở 1 row pending cho (D, source_table). Idempotent (ON CONFLICT)."""
    pg.run(
        """INSERT INTO etl_control (business_date, source_table, flow, l_cob, status)
           VALUES (%s, %s, %s, %s, 'pending')
           ON CONFLICT (business_date, source_table)
           DO UPDATE SET flow = EXCLUDED.flow,
                         l_cob = COALESCE(EXCLUDED.l_cob, etl_control.l_cob),
                         status = 'pending'""",
        parameters=(business_date, source_table, flow, l_cob),
    )


def seal(pg: PostgresHook, business_date: str, source_table: str, flow: str,
         snapshot_id: str | None = None,
         expected: int | None = None, actual: int | None = None) -> None:
    """Đóng dấu (D, source_table) = sealed (đủ + đông cứng). Upsert idempotent.

    flow='cdc'  → kèm snapshot_id (đọc AT SNAPSHOT).
    flow='pull'/'sftp' → kèm expected/actual count (đọc WHERE business_date=D).
    """
    pg.run(
        """INSERT INTO etl_control
               (business_date, source_table, flow, status,
                iceberg_snapshot_id, expected_count, actual_count, sealed_at)
           VALUES (%s, %s, %s, 'sealed', %s, %s, %s, now())
           ON CONFLICT (business_date, source_table)
           DO UPDATE SET flow = EXCLUDED.flow,
                         status = 'sealed',
                         iceberg_snapshot_id = COALESCE(EXCLUDED.iceberg_snapshot_id,
                                                        etl_control.iceberg_snapshot_id),
                         expected_count = COALESCE(EXCLUDED.expected_count, etl_control.expected_count),
                         actual_count   = COALESCE(EXCLUDED.actual_count, etl_control.actual_count),
                         sealed_at = now()""",
        parameters=(business_date, source_table, flow, snapshot_id, expected, actual),
    )


def mark_failed(pg: PostgresHook, business_date: str, source_table: str) -> None:
    pg.run(
        """UPDATE etl_control SET status='failed'
           WHERE business_date=%s AND source_table=%s""",
        parameters=(business_date, source_table),
    )


def all_sealed(pg: PostgresHook, business_date: str, sources: list[str]) -> bool:
    """True khi MỌI source trong list đã status='sealed' cho D. Gate per-domain."""
    if not sources:
        return True
    row = pg.get_first(
        """SELECT count(*) FROM etl_control
           WHERE business_date=%s AND source_table = ANY(%s) AND status='sealed'""",
        parameters=(business_date, list(sources)),
    )
    return (row[0] if row else 0) == len(set(sources))


def sealed_snapshots(pg: PostgresHook, business_date: str,
                     sources: list[str]) -> dict[str, str]:
    """{source_table: iceberg_snapshot_id} cho các source CDC đã sealed (snapshot != NULL).
    Truyền sang dbt làm var để đọc AT SNAPSHOT."""
    rows = pg.get_records(
        """SELECT source_table, iceberg_snapshot_id FROM etl_control
           WHERE business_date=%s AND source_table = ANY(%s)
             AND status='sealed' AND iceberg_snapshot_id IS NOT NULL""",
        parameters=(business_date, list(sources)),
    )
    return {r[0]: r[1] for r in rows}


# ── etl_model_runs — silver (ENQ) ────────────────────────────────────────────
def start_enq_run(pg: PostgresHook, domain: str, enq_name: str,
                  business_date: str) -> None:
    """Ghi row running cho 1 ENQ silver của domain. Idempotent."""
    pg.run(
        """INSERT INTO etl_model_runs (domain, enq_name, layer, business_date, status, started_at)
           VALUES (%s, %s, 'silver', %s, 'running', now())
           ON CONFLICT (domain, enq_name, layer, business_date)
           DO UPDATE SET status='running', started_at=now(), completed_at=NULL""",
        parameters=(domain, enq_name, business_date),
    )


def finish_enq_run(pg: PostgresHook, domain: str, enq_name: str,
                   business_date: str, status: str = "success",
                   models_built: int | None = None,
                   dbt_invocation_id: str | None = None) -> None:
    """Đánh dấu ENQ silver done. status='success'|'failed'."""
    pg.run(
        """INSERT INTO etl_model_runs
               (domain, enq_name, layer, business_date, status,
                models_built, dbt_invocation_id, completed_at)
           VALUES (%s, %s, 'silver', %s, %s, %s, %s, now())
           ON CONFLICT (domain, enq_name, layer, business_date)
           DO UPDATE SET status=EXCLUDED.status,
                         models_built=COALESCE(EXCLUDED.models_built, etl_model_runs.models_built),
                         dbt_invocation_id=COALESCE(EXCLUDED.dbt_invocation_id, etl_model_runs.dbt_invocation_id),
                         completed_at=now()""",
        parameters=(domain, enq_name, business_date, status, models_built, dbt_invocation_id),
    )


def all_enqs_built(pg: PostgresHook, domain: str, enq_names: list[str],
                   business_date: str) -> bool:
    """True khi TẤT CẢ ENQ silver của domain đã success cho D.
    Division gate gọi hàm này trước khi chạy gold."""
    if not enq_names:
        return True
    row = pg.get_first(
        """SELECT count(*) FROM etl_model_runs
           WHERE domain=%s AND layer='silver' AND business_date=%s
             AND enq_name = ANY(%s) AND status='success'""",
        parameters=(domain, business_date, list(enq_names)),
    )
    return (row[0] if row else 0) == len(set(enq_names))


# ── etl_model_runs — gold (division) ─────────────────────────────────────────
def is_model_built(pg: PostgresHook, domain: str, business_date: str) -> bool:
    """True nếu gold division đã build success cho D (skip-if-built)."""
    row = pg.get_first(
        """SELECT 1 FROM etl_model_runs
           WHERE domain=%s AND enq_name='' AND layer='gold'
             AND business_date=%s AND status='success'""",
        parameters=(domain, business_date),
    )
    return row is not None


def start_model_run(pg: PostgresHook, domain: str, business_date: str) -> None:
    pg.run(
        """INSERT INTO etl_model_runs (domain, enq_name, layer, business_date, status, started_at)
           VALUES (%s, '', 'gold', %s, 'running', now())
           ON CONFLICT (domain, enq_name, layer, business_date)
           DO UPDATE SET status='running', started_at=now(), completed_at=NULL""",
        parameters=(domain, business_date),
    )


def finish_model_run(pg: PostgresHook, domain: str, business_date: str,
                     status: str = "success", models_built: int | None = None,
                     dbt_invocation_id: str | None = None) -> None:
    pg.run(
        """INSERT INTO etl_model_runs
               (domain, enq_name, layer, business_date, status,
                models_built, dbt_invocation_id, completed_at)
           VALUES (%s, '', 'gold', %s, %s, %s, %s, now())
           ON CONFLICT (domain, enq_name, layer, business_date)
           DO UPDATE SET status=EXCLUDED.status,
                         models_built=COALESCE(EXCLUDED.models_built, etl_model_runs.models_built),
                         dbt_invocation_id=COALESCE(EXCLUDED.dbt_invocation_id, etl_model_runs.dbt_invocation_id),
                         completed_at=now()""",
        parameters=(domain, business_date, status, models_built, dbt_invocation_id),
    )


# ── etl_ingest_logs (pull + sftp) ────────────────────────────────────────────
def get_ingest_log(pg: PostgresHook, source_name: str, flow: str,
                   business_date: str) -> dict | None:
    """Trạng thái sync/parse của (source_name, flow, D). None nếu chưa có dòng nào."""
    row = pg.get_first(
        """SELECT is_sync, is_parsed FROM etl_ingest_logs
           WHERE source_name=%s AND flow=%s AND business_date=%s""",
        parameters=(source_name, flow, business_date),
    )
    if row is None:
        return None
    return {"is_sync": bool(row[0]), "is_parsed": bool(row[1])}


def mark_sync_done(pg: PostgresHook, source_name: str, flow: str,
                   business_date: str) -> None:
    """Nhịp 1 xong (MSSQL/SFTP → MinIO raw) → is_sync=true. Idempotent."""
    pg.run(
        """INSERT INTO etl_ingest_logs (source_name, flow, business_date, is_sync, synced_at)
           VALUES (%s, %s, %s, true, now())
           ON CONFLICT (source_name, flow, business_date)
           DO UPDATE SET is_sync=true, synced_at=now()""",
        parameters=(source_name, flow, business_date),
    )


def mark_parse_done(pg: PostgresHook, source_name: str, flow: str,
                    business_date: str) -> None:
    """Nhịp 2 xong (raw → Bronze Iceberg) → is_parsed=true. Idempotent.
    Gọi trước seal() để đảm bảo thứ tự: parse done → seal etl_control."""
    pg.run(
        """INSERT INTO etl_ingest_logs
               (source_name, flow, business_date, is_sync, is_parsed, synced_at, parsed_at)
           VALUES (%s, %s, %s, true, true, now(), now())
           ON CONFLICT (source_name, flow, business_date)
           DO UPDATE SET is_parsed=true, parsed_at=now()""",
        parameters=(source_name, flow, business_date),
    )
