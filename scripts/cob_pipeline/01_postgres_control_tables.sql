-- COB-driven CDC pipeline — bảng control trên Postgres (conn: cob_control_conn).
-- Xem arch/PIPELINE_ORCHESTRATION_DESIGN.md.
-- Chạy: psql ... -f 01_postgres_control_tables.sql  (hoặc PostgresHook.run — idempotent)

-- 1) etl_stream_progress: Spark foreachBatch upsert mỗi micro-batch.
--    1 dòng per bảng (max_lsn[T]) + 1 dòng table='__global__' (stream_lsn).
--    committed_offsets: JSON endOffset từ StreamingQueryListener (cho lag nhánh B).
CREATE TABLE IF NOT EXISTS etl_stream_progress (
    table_name        text PRIMARY KEY,
    max_lsn           text,                         -- SQL Server LSN dạng chuỗi hex; so lexicographic
    committed_offsets jsonb,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- 2) etl_control: gate "nguồn cho D đã sealed chưa?" (per source_table).
--    flow='cdc'        → seal kèm iceberg_snapshot_id (đọc AT SNAPSHOT).
--    flow='pull'/'sftp'→ seal kèm expected/actual count (đọc WHERE business_date=D).
CREATE TABLE IF NOT EXISTS etl_control (
    business_date        date        NOT NULL,
    source_table         text        NOT NULL,
    flow                 text        NOT NULL CHECK (flow IN ('cdc','pull','sftp')),
    status               text        NOT NULL DEFAULT 'pending',  -- pending|sealed|failed
    expected_count       bigint,
    actual_count         bigint,
    l_cob                text,                                    -- LSN mốc COB (chuỗi)
    iceberg_snapshot_id  text,                                    -- snapshot_id (chỉ flow=cdc)
    sealed_at            timestamptz,
    PRIMARY KEY (business_date, source_table)
);
DO $$ BEGIN
    ALTER TABLE etl_control DROP CONSTRAINT IF EXISTS etl_control_flow_check;
    ALTER TABLE etl_control ADD  CONSTRAINT etl_control_flow_check
        CHECK (flow IN ('cdc','pull','sftp'));
EXCEPTION WHEN others THEN NULL; END $$;

-- 3) etl_model_runs: gate "silver ENQ / gold division cho D xong chưa?".
--    layer='silver': enq_name = tên ENQ (vd 't24_inactive_account_report').
--    layer='gold':   enq_name = '' (sentinel), domain = tên division (vd 'aml').
--    Division gate: chờ tất cả rows (domain=X, layer='silver', D) đều success.
CREATE TABLE IF NOT EXISTS etl_model_runs (
    domain            text        NOT NULL,           -- division: 'aml', 'credit', ...
    enq_name          text        NOT NULL DEFAULT '', -- ENQ silver: 't24_inactive_...'; gold: ''
    layer             text        NOT NULL DEFAULT 'gold'
                          CHECK (layer IN ('silver','gold')),
    business_date     date        NOT NULL,
    status            text        NOT NULL DEFAULT 'running',  -- running|success|failed
    dbt_invocation_id text,
    models_built      int,
    started_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    PRIMARY KEY (domain, enq_name, layer, business_date)
);
DO $$ BEGIN
    ALTER TABLE etl_model_runs ADD COLUMN enq_name text NOT NULL DEFAULT '';
    ALTER TABLE etl_model_runs ADD COLUMN layer text NOT NULL DEFAULT 'gold'
        CHECK (layer IN ('silver','gold'));
    ALTER TABLE etl_model_runs DROP CONSTRAINT etl_model_runs_pkey;
    ALTER TABLE etl_model_runs ADD PRIMARY KEY (domain, enq_name, layer, business_date);
EXCEPTION WHEN others THEN NULL; END $$;

-- 4) etl_ingest_logs: tracking 2 nhịp sync+parse cho luồng pull và sftp.
--    Nhịp 1 — sync: pull (MSSQL→MinIO raw) / sftp (SFTP→MinIO raw) → is_sync=true.
--    Nhịp 2 — parse: Spark (raw→Bronze Iceberg) → is_parsed=true → rồi seal etl_control.
--    CDC không có ở đây vì streaming (không có sync step riêng).
CREATE TABLE IF NOT EXISTS etl_ingest_logs (
    source_name   text        NOT NULL,  -- pull: 'hive.bronze.t24_account'; sftp: 'crb','crf'
    flow          text        NOT NULL CHECK (flow IN ('pull','sftp')),
    business_date date        NOT NULL,
    is_sync       boolean     NOT NULL DEFAULT false,
    is_parsed     boolean     NOT NULL DEFAULT false,
    synced_at     timestamptz,
    parsed_at     timestamptz,
    error_message text,
    PRIMARY KEY (source_name, flow, business_date)
);
-- Migrate từ etl_parsed_logs (sftp) nếu tồn tại:
DO $$ BEGIN
    INSERT INTO etl_ingest_logs (source_name, flow, business_date,
                                  is_sync, is_parsed, synced_at, parsed_at, error_message)
    SELECT report_type, 'sftp', report_date,
           is_sync, is_parsed, synced_at, parsed_at, error_message
    FROM etl_parsed_logs
    ON CONFLICT DO NOTHING;
EXCEPTION WHEN undefined_table THEN NULL; END $$;
