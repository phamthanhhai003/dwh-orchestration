"""
Treasury Remittance Pipeline - Parser Layer
=============================================
Xử lý dữ liệu Treasury Incoming Remittance (file Excel).

Pattern A (vendor-organized folder):
  - Phòng ban upload file Excel theo cấu trúc SFTP {REMOTE}/{YYYY-MM-DD}/<files>
  - `dag_run.conf.START_DATE/END_DATE` = COB date trực tiếp (semantic 1-1)
  - Sync mirror folder structure: SFTP {YYYY-MM-DD}/ → MinIO raw/treasury/{YYYY-MM-DD}/
  - Parser chạy 1 Spark job per COB qua .expand(), đọc Excel từ folder đó

Flow:
  1. extract_dates: lấy START_DATE/END_DATE từ conf hoặc fallback data_interval_start
  2. Sync SFTP treasury → MinIO raw/treasury/{YYYY-MM-DD}/ + _SUCCESS marker
  3. detect_new_cob: scan folder có _SUCCESS, loại COB đã parsed
  4. insert_etl_parsed_logs: ghi row pending cho mỗi COB
  5. Spark parser .expand() trên từng COB → hive.bronze.treasury_remittance
  6. update_etl_parsed_logs: mark is_parsed=true

Trigger example (manual):
  airflow dags trigger treasury_remittance_pipeline \\
    --conf '{"START_DATE":"2026-03-15","END_DATE":"2026-03-15"}'

Trigger example (scheduled): không cần conf — YAML fallback `ds`.

Dependency: Độc lập, không đợi source khác.
Chi tiết: xem dags/PARSER_PIPELINES.md section 6.5.
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
import re

from lib.sftp_validators import TreasuryValidator

# --- CẤU HÌNH HỆ THỐNG ---
DB_CONN_ID = 'postgres_conn'
MINIO_CONN_ID = 'minio_conn'
BUCKET_NAME = 'raw'
SOURCE_PREFIX = 'treasury/'

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_HOME = os.path.dirname(DAG_FOLDER)
SPARK_NAMESPACE = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- LOGIC CÁC TASK ---

def extract_date_range_logic(**context):
    """
    Extract START_DATE/END_DATE theo thứ tự ưu tiên:
    1. dag_run.conf (JSON passed at trigger via --conf) — manual/backfill
    2. data_interval_start — scheduled run, COB = start của data interval
    """
    dag_run = context.get('dag_run')
    start_date = None
    end_date = None
    source = None

    # 1. dag_run.conf (manual trigger)
    if dag_run and dag_run.conf:
        start_date = dag_run.conf.get('START_DATE')
        end_date = dag_run.conf.get('END_DATE')
        if start_date or end_date:
            source = 'dag_run.conf'

    # 2. data_interval_start (scheduled trigger)
    if not start_date or not end_date:
        di_start = context.get('data_interval_start')
        if di_start:
            date_str = di_start.strftime('%Y-%m-%d')
            start_date = start_date or date_str
            end_date = end_date or date_str
            source = source or 'data_interval_start'

    if not start_date or not end_date:
        raise ValueError(
            "Cannot determine START_DATE/END_DATE. "
            "Provide via --conf or ensure DAG has data_interval_start."
        )

    for d in [start_date, end_date]:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', d):
            raise ValueError(f"Invalid date format: {d}. Expected YYYY-MM-DD")

    logging.info(f"[extract_dates] {start_date} → {end_date} (source: {source})")
    return {'start_date': start_date, 'end_date': end_date}


def detect_new_cob_logic(**context):
    """
    Phát hiện các ngày treasury mới trong MinIO chưa được parse.
    Chỉ pick folder có _SUCCESS marker (sync đã hoàn tất).
    Cấu trúc: raw/treasury/{YYYY-MM-DD}/<files>, _SUCCESS
    Lọc theo date range từ upstream extract_dates task via XCOM.
    """
    ti = context['ti']
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    upstream_output = ti.xcom_pull(task_ids='extract_dates')
    if not upstream_output:
        logging.error("[detect_new_cob] No upstream output from extract_dates")
        return []

    start_date_str = upstream_output.get('start_date')
    end_date_str = upstream_output.get('end_date')

    start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX) or []

    # Chỉ pick folder có /_SUCCESS marker + nằm trong date range
    ready_on_minio = set()
    for key in all_keys:
        if not key.endswith('/_SUCCESS'):
            continue
        parts = key.replace(SOURCE_PREFIX, '').split('/')
        if not parts:
            continue
        folder = parts[0]
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', folder):
            continue
        try:
            folder_dt = datetime.strptime(folder, '%Y-%m-%d')
            if start_dt <= folder_dt <= end_dt:
                ready_on_minio.add(folder)
        except ValueError:
            continue

    parsed_records = pg_hook.get_records(
        "SELECT report_date::text FROM etl_parsed_logs_treasury_remittance WHERE is_parsed = true"
    )
    parsed_cobs = {row[0] for row in parsed_records}

    cobs_to_run = sorted(ready_on_minio - parsed_cobs)
    logging.info(
        f"[detect_new_cob] Treasury COB ready (có _SUCCESS) trong "
        f"[{start_date_str} → {end_date_str}] chưa parse: {cobs_to_run}"
    )
    return cobs_to_run


def insert_etl_parsed_logs_logic(**context):
    """Chèn log trước khi parse. Reset state nếu COB đã tồn tại để cho phép re-parse."""
    ti = context['ti']
    new_cobs = ti.xcom_pull(task_ids='detect_new_cob')
    if not new_cobs:
        return []

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    for cob in new_cobs:
        pg_hook.run(
            "INSERT INTO etl_parsed_logs_treasury_remittance (report_date, is_parsed) "
            "VALUES (%s, false) "
            "ON CONFLICT (report_date) DO UPDATE SET is_parsed = false, parsed_at = null, error_message = null",
            parameters=(cob,)
        )
    return [{"cob": cob} for cob in new_cobs]


def validate_sftp_logic(**context):
    """
    Pre-sync validation cho treasury (Pattern A).
    Validate từng COB trong date range có file xlsx đúng format + schema cơ bản.
    """
    ti = context['ti']
    upstream = ti.xcom_pull(task_ids='extract_dates')
    start_date = upstream.get('start_date')
    end_date = upstream.get('end_date')

    from datetime import timedelta as td
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    errors_all = []
    cob = start_dt
    while cob <= end_dt:
        cob_str = cob.strftime('%Y-%m-%d')
        result = TreasuryValidator(cob=cob_str).run()
        if not result.passed:
            errors_all.append(f"COB {cob_str}: {result.errors}")
        cob += td(days=1)

    if errors_all:
        raise AirflowException(
            f"Treasury SFTP validation failed: {errors_all}"
        )


def update_parsed_status_logic(cob, **kwargs):
    """Cập nhật trạng thái parse thành công sau khi Spark parser hoàn thành."""
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    pg_hook.run(
        "UPDATE etl_parsed_logs_treasury_remittance "
        "SET is_parsed = true, parsed_at = now(), error_message = null "
        "WHERE report_date = %s",
        parameters=(cob,)
    )
    logging.info(f"✅ Treasury remittance COB {cob} marked as parsed successfully.")


# --- KHAI BÁO DAG ---

with DAG(
    dag_id='treasury_remittance_pipeline',
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "sync"),
        os.path.join(AIRFLOW_HOME, "spark-app", "parser")
    ],
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['treasury', 'remittance', 'parse', 'bnctl', 'spark']
) as dag:

    # Bước 0: Extract date range từ 3 sources
    t0_extract = PythonOperator(
        task_id='extract_dates',
        python_callable=extract_date_range_logic
    )

    # Pre-sync validation
    t0_validate = PythonOperator(
        task_id='validate_sftp',
        python_callable=validate_sftp_logic,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # Bước 1: Sync SFTP → MinIO (raw/treasury/{YYYY-MM-DD}/...)
    t0_sync = SparkKubernetesOperator(
        task_id='submit_sync_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='treasury_remittance_sync_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    )

    t1_detect = PythonOperator(
        task_id='detect_new_cob',
        python_callable=detect_new_cob_logic
    )

    t2_insert_logs = PythonOperator(
        task_id='insert_etl_parsed_logs',
        python_callable=insert_etl_parsed_logs_logic
    )

    # Bước 3: Spark Parser - Convert Treasury Excel → Iceberg with Spark
    # 1 Spark job PER COB (expand pattern)
    # Each job process specific PROCESS_DATE folder
    t3_parser_spark = SparkKubernetesOperator.partial(
        task_id='submit_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='treasury_remittance_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t2_insert_logs.output.map(lambda cob_dict: {'PROCESS_DATE': cob_dict['cob']})
    )

    t4_update_logs = PythonOperator.partial(
        task_id='update_etl_parsed_logs',
        python_callable=update_parsed_status_logic
    ).expand(
        op_kwargs=t2_insert_logs.output.map(lambda cob_dict: {"cob": cob_dict['cob']})
    )

    t0_extract >> t0_validate >> t0_sync >> t1_detect >> t2_insert_logs >> t3_parser_spark >> t4_update_logs
