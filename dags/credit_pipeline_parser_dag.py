"""
Credit Pipeline - Parser Layer
===============================
Xử lý dữ liệu Credit hàng ngày.

Pattern A (vendor-organized folder):
  - Phòng ban upload theo cấu trúc SFTP {REMOTE}/{YYYY-MM-DD}/<files>
  - `dag_run.conf.START_DATE/END_DATE` = COB date trực tiếp (semantic 1-1)
  - Sync mirror folder structure: SFTP {YYYY-MM-DD}/ → MinIO raw/credit/{YYYY-MM-DD}/
  - Parser chạy 1 Spark job per COB qua .expand(), mỗi job có --date riêng

Flow:
  1. extract_dates: lấy START_DATE/END_DATE từ conf hoặc fallback data_interval_start
  2. Sync SFTP Credit → MinIO raw/credit/{YYYY-MM-DD}/ + _SUCCESS marker
  3. detect_new_cob: scan folder có _SUCCESS, loại COB đã parsed
  4. insert_etl_parsed_logs: ghi row pending cho mỗi COB
  5. Spark parser .expand() trên từng COB → 3 bảng hive.bronze.credit__*
  6. update_etl_parsed_logs: mark is_parsed=true

Trigger example (manual):
  airflow dags trigger credit_parse_pipeline \\
    --conf '{"START_DATE":"2026-03-15","END_DATE":"2026-03-15"}'

Trigger example (scheduled): không cần conf — YAML fallback `ds` = COB của data_interval_start.

Dependency: Độc lập, không đợi source khác.
Chi tiết: xem dags/PARSER_PIPELINES.md section 6.1.
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

from lib.sftp_validators import CreditValidator

# --- CẤU HÌNH HỆ THỐNG ---
DB_CONN_ID = 'postgres_conn'
MINIO_CONN_ID = 'minio_conn'
BUCKET_NAME = 'raw'
SOURCE_PREFIX = 'credit/'

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_HOME = os.path.dirname(DAG_FOLDER)
SPARK_NAMESPACE = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- LOGIC CÁC TASK ---

def extract_date_range_logic(**context):
    """
    Extract START_DATE, END_DATE theo thứ tự ưu tiên:
    1. dag_run.conf (JSON truyền lúc trigger) — manual/backfill
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

    # Validate format YYYY-MM-DD
    for d in [start_date, end_date]:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', d):
            raise ValueError(f"Invalid date format: {d}. Expected YYYY-MM-DD")

    logging.info(f"[DAG] Extracted dates: {start_date} → {end_date} (source: {source})")
    return {'start_date': start_date, 'end_date': end_date}


def detect_new_cob_logic(**context):
    """
    Phát hiện các ngày Credit mới trong MinIO chưa được parse.
    Filter trong khoảng [start_date, end_date] từ extract task.
    Chỉ pick folder có _SUCCESS marker (sync đã hoàn tất).
    Cấu trúc: raw/credit/{YYYY-MM-DD}/<files>, _SUCCESS
    """
    ti = context['ti']

    upstream_output = ti.xcom_pull(task_ids='extract_dates')
    if not upstream_output:
        logging.error("[DAG] extract_dates task không return data")
        return []

    start_date_str = upstream_output.get('start_date')
    end_date_str = upstream_output.get('end_date')

    start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX) or []

    # Chỉ pick folder có _SUCCESS marker + nằm trong date range
    ready_on_minio = set()
    for key in all_keys:
        if not key.endswith('/_SUCCESS'):
            continue
        parts = key.replace(SOURCE_PREFIX, '').split('/')
        if not parts:
            continue
        folder = parts[0]
        if len(folder) != 10 or folder[4] != '-' or folder[7] != '-':
            continue
        try:
            folder_dt = datetime.strptime(folder, '%Y-%m-%d')
            if start_dt <= folder_dt <= end_dt:
                ready_on_minio.add(folder)
        except ValueError:
            continue

    parsed_records = pg_hook.get_records(
        "SELECT report_date::text FROM etl_parsed_logs_credit WHERE is_parsed = true"
    )
    parsed_cobs = {row[0] for row in parsed_records}

    cobs_to_run = sorted(ready_on_minio - parsed_cobs)
    logging.info(
        f"[DAG] Credit COB ready (có _SUCCESS) trong [{start_date_str} → {end_date_str}] "
        f"chưa parse: {cobs_to_run}"
    )
    return cobs_to_run


def insert_etl_parsed_logs_logic(**context):
    """Chèn log trước khi parse."""
    ti = context['ti']
    new_cobs = ti.xcom_pull(task_ids='detect_new_cob')
    if not new_cobs:
        return []

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    for cob in new_cobs:
        pg_hook.run(
            "INSERT INTO etl_parsed_logs_credit (report_date, is_parsed) "
            "VALUES (%s, false) "
            "ON CONFLICT (report_date) DO UPDATE SET is_parsed = false, parsed_at = null",
            parameters=(cob,)
        )
    return [{"cob": cob} for cob in new_cobs]


def validate_sftp_logic(**context):
    """
    Pre-sync validation: check SFTP có đủ folder + file + schema.
    Fail sẽ block sync — tránh waste compute parser trên data invalid.
    """
    ti = context['ti']
    upstream = ti.xcom_pull(task_ids='extract_dates')
    start_date = upstream.get('start_date')
    end_date = upstream.get('end_date')

    # Validate theo từng COB trong range
    from datetime import timedelta as td
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    errors_all = []
    cob = start_dt
    while cob <= end_dt:
        cob_str = cob.strftime('%Y-%m-%d')
        result = CreditValidator(cob=cob_str).run()
        if not result.passed:
            errors_all.append(f"COB {cob_str}: {result.errors}")
        cob += td(days=1)

    if errors_all:
        raise AirflowException(
            f"Credit SFTP validation failed: {errors_all}"
        )


def update_parsed_status_logic(cob, **kwargs):
    """Cập nhật status sau khi parser chạy thành công."""
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    pg_hook.run(
        "UPDATE etl_parsed_logs_credit "
        "SET is_parsed = true, parsed_at = now(), error_message = null "
        "WHERE report_date = %s",
        parameters=(cob,)
    )


# --- KHAI BÁO DAG ---

with DAG(
    dag_id='credit_parse_pipeline',
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "sync"),
        os.path.join(AIRFLOW_HOME, "spark-app", "parser")
    ],
    default_args=default_args,
    schedule=None,  # Manual trigger với START_DATE và END_DATE
    catchup=False,
    tags=['credit', 'parse', 'bnctl', 'spark'],
) as dag:
    
    # Extract date range từ dag_run.conf
    t0_extract = PythonOperator(
        task_id='extract_dates',
        python_callable=extract_date_range_logic
    )

    # Pre-sync validation — check SFTP folder structure, file count, schema
    t0_validate = PythonOperator(
        task_id='validate_sftp',
        python_callable=validate_sftp_logic,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # Spark Credit Sync — SFTP → MinIO (1 job with date range)
    t0_sync = SparkKubernetesOperator(
        task_id='credit_sftp_to_minio_sync',
        namespace=SPARK_NAMESPACE,
        application_file='credit_sync_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False,
    )
    
    t0_detect = PythonOperator(
        task_id='detect_new_cob',
        python_callable=detect_new_cob_logic
    )

    t1_insert_logs = PythonOperator(
        task_id='insert_etl_parsed_logs',
        python_callable=insert_etl_parsed_logs_logic
    )

    # Spark Credit Parser — expand trên từng COB
    t2_parser = SparkKubernetesOperator.partial(
        task_id='submit_credit_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='credit_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t1_insert_logs.output.map(lambda cob_dict: {'PROCESS_DATE': cob_dict['cob']})
    )

    t3_update_status = PythonOperator.partial(
        task_id='update_etl_parsed_logs',
        python_callable=update_parsed_status_logic
    ).expand(
        op_kwargs=t1_insert_logs.output.map(lambda cob_dict: {"cob": cob_dict['cob']})
    )

    # Dependencies: extract → validate → sync → detect → insert → parser → update
    t0_extract >> t0_validate >> t0_sync >> t0_detect >> t1_insert_logs >> t2_parser >> t3_update_status
