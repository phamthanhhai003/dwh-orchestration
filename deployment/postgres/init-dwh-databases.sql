-- ════════════════════════════════════════════════════════════════
-- DWH-critical databases + roles
-- ════════════════════════════════════════════════════════════════
-- Postgres Helm chart (postgres/values-full.yaml) chỉ init các DB phụ
-- (airbyte/temporal/n8n/dolphin/kong). Các DB lõi của DWH dưới đây được
-- tạo ngoài chart → phải chạy script này SAU khi Postgres ready, TRƯỚC
-- khi deploy Hive Metastore và Airflow.
--
-- Chạy:
--   kubectl exec -n bnctl-postgres-development-ns cluster-postgresql-primary-0 \
--     -c postgresql -- sh -c 'PGPASSWORD=$(cat $POSTGRES_PASSWORD_FILE) psql -U postgres' \
--     < deployment/postgres/init-dwh-databases.sql
--
-- ⚠️ Đổi password bên dưới cho khớp:
--   - airflow   → airflow-metadata secret (connection string)
--   - hive_user → hive metastore-site.xml (ConnectionPassword)
-- ════════════════════════════════════════════════════════════════

-- ── Airflow metadata DB ──────────────────────────────────────────
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow_secure_password';
  END IF;
END $$;
SELECT 'CREATE DATABASE airflow OWNER airflow'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- ── Hive Metastore DB ────────────────────────────────────────────
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hive_user') THEN
    CREATE ROLE hive_user LOGIN PASSWORD 'hive_password';
  END IF;
END $$;
SELECT 'CREATE DATABASE hive_metastore OWNER hive_user'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hive_metastore')\gexec

-- ── ETL state tracking DB (parser/model DAGs) ────────────────────
SELECT 'CREATE DATABASE etl_control OWNER postgres'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_control')\gexec

-- ── COB control DB ───────────────────────────────────────────────
SELECT 'CREATE DATABASE cob_control OWNER postgres'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'cob_control')\gexec
