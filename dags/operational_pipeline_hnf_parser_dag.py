"""
Operational Pipeline - HNF Monthly Branch
===========================================
Xử lý dữ liệu Operational (HNF, Agent Banking, Digital Service) HÀNG THÁNG.

Pattern A (vendor-organized folder, monthly grain):
  - Phòng ban upload theo cấu trúc SFTP {REMOTE}/{YYYY-MM}/{hnf|agent_banking|digital_service}/
  - `dag_run.conf.PROCESS_MONTH_OPERATIONAL` = tháng cần process (format YYYY-MM)
  - Sync mirror folder structure: SFTP {YYYY-MM}/ → MinIO raw/operation/{YYYY-MM}/
  - Parser đọc toàn bộ 3 subfolder → 8 bảng hive.bronze.*

Flow:
  1. extract_process_month: lấy PROCESS_MONTH từ conf hoặc fallback data_interval_start
  2. Sync SFTP operational → MinIO raw/operation/{YYYY-MM}/ + _SUCCESS marker
  3. detect_new_month: check MinIO folder có _SUCCESS, loại tháng đã parsed
  4. insert_etl_parsed_logs: ghi row pending (report_date = YYYY-MM-01)
  5. Spark parser expand trên từng tháng → 8 bảng bronze
  6. update_etl_parsed_logs: mark is_parsed=true

Trigger example (manual):
  airflow dags trigger operational_hnf_parse_pipeline \\
    --conf '{"PROCESS_MONTH_OPERATIONAL":"2026-03"}'

Trigger example (scheduled monthly cron):
  schedule='0 3 5 * *'  # 3h sáng ngày 5 mỗi tháng
  Không cần conf — fallback data_interval_start.strftime('%Y-%m').

Dependency: Độc lập với BNK nhánh daily.
Chi tiết: xem dags/PARSER_PIPELINES.md section 6.4.
"""

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os
import re

from lib.sftp_validators import OperationalHnfValidator

# --- CẤU HÌNH HỆ THỐNG ---
DB_CONN_ID = 'postgres_conn'
MINIO_CONN_ID = 'minio_conn'
BUCKET_NAME = 'raw'
SOURCE_PREFIX = 'operation/'  # Nơi operational_sync_spark.py upload

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

def extract_process_month_logic(**context):
    """
    Extract PROCESS_MONTH theo thứ tự ưu tiên:
    1. dag_run.conf.PROCESS_MONTH_OPERATIONAL — manual/backfill
    2. data_interval_start.strftime('%Y-%m') — scheduled monthly cron
    """
    dag_run = context.get('dag_run')
    process_month = None
    source = None

    if dag_run and dag_run.conf:
        process_month = dag_run.conf.get('PROCESS_MONTH_OPERATIONAL')
        if process_month:
            source = 'dag_run.conf'

    if not process_month:
        di_start = context.get('data_interval_start')
        if di_start:
            process_month = di_start.strftime('%Y-%m')
            source = 'data_interval_start'

    if not process_month or not re.match(r'^\d{4}-\d{2}$', process_month):
        raise ValueError(
            f"Invalid or missing PROCESS_MONTH: {process_month!r}. "
            f"Expected format YYYY-MM. "
            f"Usage: airflow dags trigger operational_hnf_parse_pipeline "
            f"--conf '{{\"PROCESS_MONTH_OPERATIONAL\": \"2026-04\"}}'"
        )

    logging.info(f"[DAG] PROCESS_MONTH={process_month} (source: {source})")
    return {'month': process_month}


def validate_sftp_logic(**context):
    """
    Pre-sync validation cho HNF (Pattern A monthly).
    Check folder {month}/ có 3 subfolder + 8 file required + Sheet4 trong CRB.
    """
    ti = context['ti']
    upstream = ti.xcom_pull(task_ids='extract_process_month')
    month = upstream.get('month')
    if not month:
        raise AirflowException("validate_sftp: no month from extract_process_month")

    result = OperationalHnfValidator(month=month).run()
    if not result.passed:
        raise AirflowException(
            f"HNF SFTP validation failed for month {month}: {result.errors}"
        )


def detect_new_month_logic(**context):
    """
    Dùng PROCESS_MONTH từ extract task.
    Check MinIO có _SUCCESS marker + chưa parsed trong log table.
    Skip rõ ràng (AirflowSkipException) nếu marker thiếu hoặc đã parsed.
    """
    ti = context['ti']

    upstream_output = ti.xcom_pull(task_ids='extract_process_month')
    if not upstream_output:
        raise ValueError("[DAG] extract_process_month task không return data")

    process_month = upstream_output.get('month')
    if not process_month or not re.match(r'^\d{4}-\d{2}$', process_month):
        raise ValueError(f"[DAG] Invalid month format: {process_month!r}. Expected YYYY-MM")

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    # Check _SUCCESS marker trên MinIO
    success_key = f"operation/{process_month}/_SUCCESS"
    try:
        s3_hook.head_object(bucket_name=BUCKET_NAME, key=success_key)
        logging.info(f"[DAG] Found marker: {success_key}")
    except Exception:
        raise AirflowSkipException(
            f"_SUCCESS marker not found at {success_key}. "
            "Sync chưa hoàn tất hoặc không có data cho tháng này."
        )

    # Check already parsed
    parsed_records = pg_hook.get_records(
        "SELECT 1 FROM etl_parsed_logs_operational_hnf "
        "WHERE to_char(report_date, 'YYYY-MM') = %s AND is_parsed = true",
        parameters=(process_month,)
    )
    if parsed_records:
        raise AirflowSkipException(
            f"Month {process_month} already parsed. "
            "Xoá row trong etl_parsed_logs_operational_hnf để reprocess."
        )

    logging.info(f"[DAG] Month to process: {process_month}")
    return [process_month]


def insert_etl_parsed_logs_logic(**context):
    """Chèn log trước khi parse."""
    ti = context['ti']
    new_months = ti.xcom_pull(task_ids='detect_new_month')
    if not new_months:
        return []

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    for month in new_months:
        pg_hook.run(
            "INSERT INTO etl_parsed_logs_operational_hnf (report_date, is_parsed) "
            "VALUES (%s::date, false) "
            "ON CONFLICT (report_date) DO UPDATE SET is_parsed = false, parsed_at = null",
            parameters=(f"{month}-01",)  # Convert YYYY-MM to YYYY-MM-01 for date column
        )
    return [{"month": month} for month in new_months]


def update_parsed_status_logic(month, **kwargs):
    """Cập nhật status sau khi parser chạy thành công."""
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    pg_hook.run(
        "UPDATE etl_parsed_logs_operational_hnf "
        "SET is_parsed = true, parsed_at = now(), error_message = null "
        "WHERE date_trunc('month', report_date)::date = %s::date",
        parameters=(f"{month}-01",)
    )


# --- KHAI BÁO DAG ---

with DAG(
    dag_id='operational_hnf_parse_pipeline',
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "sync"),
        os.path.join(AIRFLOW_HOME, "spark-app", "parser")
    ],
    default_args=default_args,
    schedule=None,  # Manual trigger — sẽ đổi sang cron '0 3 5 * *' ở Wave 7
    catchup=False,
    tags=['operational', 'hnf', 'parse', 'monthly'],
) as dag:

    # Extract PROCESS_MONTH từ dag_run.conf hoặc fallback data_interval_start
    t0_extract = PythonOperator(
        task_id='extract_process_month',
        python_callable=extract_process_month_logic
    )

    # Pre-sync validation
    t0_validate = PythonOperator(
        task_id='validate_sftp',
        python_callable=validate_sftp_logic,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # Spark Sync — SFTP → MinIO
    # YAML dùng `{{ dag_run.conf.get('PROCESS_MONTH_OPERATIONAL', data_interval_start.strftime('%Y-%m')) }}`
    t0_sync = SparkKubernetesOperator(
        task_id='hnf_sftp_to_minio_sync',
        namespace=SPARK_NAMESPACE,
        application_file='operational_sync_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False,
    )
    
    t0_detect = PythonOperator(
        task_id='detect_new_month',
        python_callable=detect_new_month_logic
    )

    t1_insert_logs = PythonOperator(
        task_id='insert_etl_parsed_logs',
        python_callable=insert_etl_parsed_logs_logic
    )

    # Spark HNF Parser — expand trên từng tháng từ detect task output
    t2_parser = SparkKubernetesOperator.partial(
        task_id='submit_hnf_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='operational_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t1_insert_logs.output.map(lambda month_dict: {'PROCESS_MONTH': month_dict['month']})
    )

    t3_update_status = PythonOperator.partial(
        task_id='update_etl_parsed_logs',
        python_callable=update_parsed_status_logic
    ).expand(
        op_kwargs=t1_insert_logs.output.map(lambda month_dict: {"month": month_dict['month']})
    )

    t0_extract >> t0_validate >> t0_sync >> t0_detect >> t1_insert_logs >> t2_parser >> t3_update_status
