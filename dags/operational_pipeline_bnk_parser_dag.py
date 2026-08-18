"""
Operational Pipeline - BNK Daily Branch
==========================================
Xử lý dữ liệu T24 Banking (BNK_ACCOUNT, BNK_CUSTOMER, BNK_TXN_ENTRY).

Pattern B (T24 auto-export flat folder):
  - T24 dump file vào 1 folder gốc SFTP, không subfolder
  - COB date được detect từ content (cột MIS_DATE) của từng file CSV
  - `dag_run.conf.START_DATE/END_DATE` = mtime filter window, KHÔNG phải COB
  - 1 batch có thể chứa nhiều COB → sync split vào nhiều folder MinIO
  - Sync YAML tự fallback `ds` nếu không có conf (cho scheduled run)

Flow:
  1. Sync SFTP BNK → MinIO raw/bnk/{cob}/ (per-file COB detection)
  2. detect_new_cob: scan MinIO folder có _SUCCESS marker
  3. validate_completeness: kiểm tra đủ 3 category file per COB
  4. Spark BNK parser expand trên từng COB → 3 bảng hive.bronze.aml_*
  5. Update etl_parsed_logs_operational_bnk

Trigger example (manual, backfill):
  airflow dags trigger operational_bnk_parse_pipeline \\
    --conf '{"START_DATE":"2026-04-10","END_DATE":"2026-04-12"}'

Trigger example (scheduled): không cần conf — sync YAML fallback `ds`.

Dependency: Độc lập, không đợi source khác.
Chi tiết: xem dags/PARSER_PIPELINES.md section 6.3.
"""

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os

from lib.sftp_validators import BnkValidator

# --- CẤU HÌNH HỆ THỐNG ---
DB_CONN_ID = 'postgres_conn'
MINIO_CONN_ID = 'minio_conn'
BUCKET_NAME = 'raw'
SOURCE_PREFIX = 'bnk/'  # Nơi bnk_sync_spark.py upload

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_HOME = os.path.dirname(DAG_FOLDER)
SPARK_NAMESPACE = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- LOGIC CÁC TASK ---

EXPECTED_CATEGORIES = ['ACCOUNT', 'CUSTOMER', 'TXN_ENTRY']


def validate_sftp_logic(**context):
    """
    Pre-sync validation cho BNK (Pattern B — T24 flat folder).
    Check 3 category file + MIS_DATE column trong ACCOUNT/CUSTOMER header.
    """
    result = BnkValidator().run()
    if not result.passed:
        raise AirflowException(
            f"BNK SFTP validation failed: {result.errors}"
        )


def detect_new_cob_logic():
    """
    Phát hiện các ngày BNK mới trong MinIO chưa được parse.

    Chỉ pick folder có _SUCCESS marker (sync đã hoàn tất cho COB đó).
    Cấu trúc: raw/bnk/{YYYY-MM-DD}/{files...}, _SUCCESS
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX) or []

    # Chỉ pick folder có _SUCCESS marker
    ready_on_minio = set()
    for key in all_keys:
        if key.endswith('/_SUCCESS'):
            parts = key.replace(SOURCE_PREFIX, '').split('/')
            if parts and len(parts[0]) == 10 and parts[0][4] == '-' and parts[0][7] == '-':
                ready_on_minio.add(parts[0])

    parsed_records = pg_hook.get_records(
        "SELECT report_date::text FROM etl_parsed_logs_operational_bnk WHERE is_parsed = true"
    )
    parsed_cobs = {row[0] for row in parsed_records}

    cobs_to_run = sorted(ready_on_minio - parsed_cobs)
    logging.info(f"BNK COB ready (có _SUCCESS, chưa parse): {cobs_to_run}")
    return cobs_to_run


def validate_cob_completeness_logic(**context):
    """
    Với mỗi COB detect được, verify đủ 3 category file (ACCOUNT, CUSTOMER, TXN_ENTRY).
    COB thiếu file → loại khỏi danh sách + log WARNING (sẽ được pick up lần sau khi
    đầy đủ). Nếu không có COB nào hoàn chỉnh → raise để DAG fail rõ ràng.
    """
    ti = context['ti']
    candidates = ti.xcom_pull(task_ids='detect_new_cob') or []
    if not candidates:
        logging.info("[validate] No candidate COBs — nothing to validate.")
        return []

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    complete_cobs = []
    incomplete = []

    for cob in candidates:
        keys = s3_hook.list_keys(
            bucket_name=BUCKET_NAME,
            prefix=f"{SOURCE_PREFIX}{cob}/"
        ) or []
        files = [k.split('/')[-1].upper() for k in keys
                 if not k.endswith('_SUCCESS')]

        missing = [
            cat for cat in EXPECTED_CATEGORIES
            if not any(cat in f for f in files)
        ]

        if missing:
            logging.warning(
                f"[validate] COB {cob} missing categories: {missing}. "
                f"Files found: {files}. Skip for now — will retry next run."
            )
            incomplete.append({'cob': cob, 'missing': missing})
        else:
            complete_cobs.append(cob)
            logging.info(f"[validate] COB {cob} complete: {files}")

    if incomplete:
        logging.warning(f"[validate] Incomplete COBs skipped: {incomplete}")

    if not complete_cobs:
        raise ValueError(
            f"No complete COB found to process. "
            f"Incomplete: {incomplete}. "
            "Check SFTP source and re-run sync when T24 EOD batch is complete."
        )

    logging.info(f"[validate] Complete COBs to process: {complete_cobs}")
    return complete_cobs


def insert_etl_parsed_logs_logic(ti):
    """Chèn log trước khi parse. Chỉ xử lý COB đã pass validate_cob_completeness."""
    new_cobs = ti.xcom_pull(task_ids='validate_cob_completeness')
    if not new_cobs:
        return []

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    for cob in new_cobs:
        pg_hook.run(
            "INSERT INTO etl_parsed_logs_operational_bnk (report_date, is_parsed) "
            "VALUES (%s, false) "
            "ON CONFLICT (report_date) DO UPDATE SET is_parsed = false, parsed_at = null",
            parameters=(cob,)
        )
    return [{"cob": cob} for cob in new_cobs]


def update_parsed_status_logic(cob, **kwargs):
    """Cập nhật status sau khi parser chạy thành công."""
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    pg_hook.run(
        "UPDATE etl_parsed_logs_operational_bnk "
        "SET is_parsed = true, parsed_at = now(), error_message = null "
        "WHERE report_date = %s",
        parameters=(cob,)
    )


# --- KHAI BÁO DAG ---

with DAG(
    dag_id='operational_bnk_parse_pipeline',
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "sync"),
        os.path.join(AIRFLOW_HOME, "spark-app", "parser")
    ],
    default_args=default_args,
    schedule=None,  # Manual trigger — khi bấm chạy lúc nào, execution_date = lúc đó
    catchup=False,
    tags=['operational', 'bnk', 'parse', 'daily'],
) as dag:

    # Pre-sync validation — check flat folder có đủ 3 category file + MIS_DATE
    t0_validate = PythonOperator(
        task_id='validate_sftp',
        python_callable=validate_sftp_logic,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # Spark BNK Sync — SFTP → MinIO
    # YAML dùng `{{ dag_run.conf.get('START_DATE', ds) }}` fallback để hỗ trợ cả
    # manual trigger (với conf) và scheduled trigger (không conf, fallback ds).
    t0_sync = SparkKubernetesOperator(
        task_id='bnk_sftp_to_minio_sync',
        namespace=SPARK_NAMESPACE,
        application_file='bnk_sync_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False,
    )
    
    t0_detect = PythonOperator(
        task_id='detect_new_cob',
        python_callable=detect_new_cob_logic
    )

    t1_validate = PythonOperator(
        task_id='validate_cob_completeness',
        python_callable=validate_cob_completeness_logic
    )

    t2_insert_logs = PythonOperator(
        task_id='insert_etl_parsed_logs',
        python_callable=insert_etl_parsed_logs_logic
    )

    # Spark BNK Parser — expand trên từng COB hoàn chỉnh
    t3_parser = SparkKubernetesOperator.partial(
        task_id='submit_bnk_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='bnk_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t2_insert_logs.output.map(lambda cob_dict: {'PROCESS_DATE': cob_dict['cob']})
    )

    t4_update_status = PythonOperator.partial(
        task_id='update_etl_parsed_logs',
        python_callable=update_parsed_status_logic
    ).expand(
        op_kwargs=t2_insert_logs.output.map(lambda cob_dict: {"cob": cob_dict['cob']})
    )

    t0_validate >> t0_sync >> t0_detect >> t1_validate >> t2_insert_logs >> t3_parser >> t4_update_status
