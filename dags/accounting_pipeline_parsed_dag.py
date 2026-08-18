"""
Accounting Pipeline - Parser Layer
===================================
Xử lý dữ liệu T24 General Ledger CRF reports.

Pattern B (T24 auto-export flat folder):
  - T24 dump file CRF vào 1 folder gốc SFTP, không subfolder
  - COB date detect từ header "AS AT CLOSE OF dd MMM yyyy" trong content file
  - `dag_run.conf.START_DATE/END_DATE` = mtime filter window, KHÔNG phải COB
  - 1 batch có thể chứa nhiều COB → sync split vào nhiều folder MinIO
  - Sync YAML fallback `ds` nếu không có conf (cho scheduled run)

Flow:
  1. Sync SFTP accounting → MinIO raw/accounting/{biz_date}/
     (biz_date detect từ content, không từ mtime)
  2. detect_new_cob: scan MinIO folder có _SUCCESS marker
  3. insert_etl_parsed_logs: ghi row pending cho mỗi COB
  4. Spark parser .expand() trên từng COB → hive.bronze.accounting_parser
  5. update_etl_parsed_logs: mark is_parsed=true

Trigger example (manual, backfill):
  airflow dags trigger accounting_parse_pipeline \\
    --conf '{"START_DATE":"2026-03-15","END_DATE":"2026-03-17"}'

Trigger example (scheduled): không cần conf — YAML fallback `ds`.

Dependency: Độc lập, không đợi source khác.
Chi tiết: xem dags/PARSER_PIPELINES.md section 6.2.
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

from lib.sftp_validators import AccountingValidator

# --- CẤU HÌNH HỆ THỐNG ---
DB_CONN_ID = 'postgres_conn'
MINIO_CONN_ID = 'minio_conn'
BUCKET_NAME = 'raw'
SOURCE_PREFIX = 'accounting/' 

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

def detect_new_cob_logic():
    """
    Phát hiện các ngày accounting mới trong MinIO chưa được parse.
    Chỉ pick folder có _SUCCESS marker (sync đã hoàn tất cho COB đó).
    Cấu trúc: raw/accounting/{YYYY-MM-DD}/{files...}, _SUCCESS
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX) or []

    # Chỉ pick folder có /_SUCCESS marker + format YYYY-MM-DD
    ready_on_minio = set()
    for key in all_keys:
        if not key.endswith('/_SUCCESS'):
            continue
        parts = key.replace(SOURCE_PREFIX, '').split('/')
        if parts and len(parts[0]) == 10 and parts[0][4] == '-' and parts[0][7] == '-':
            ready_on_minio.add(parts[0])

    parsed_records = pg_hook.get_records(
        "SELECT report_date::text FROM etl_parsed_logs_accounting WHERE is_parsed = true"
    )
    parsed_cobs = {row[0] for row in parsed_records}

    cobs_to_run = sorted(ready_on_minio - parsed_cobs)
    logging.info(f"Accounting COB ready (có _SUCCESS, chưa parse): {cobs_to_run}")
    return cobs_to_run

def insert_etl_parsed_logs_logic(ti):
    """Chèn log trước khi parse. Reset state nếu COB đã tồn tại để cho phép re-parse."""
    new_cobs = ti.xcom_pull(task_ids='detect_new_cob')
    if not new_cobs:
        return []

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    for cob in new_cobs:
        pg_hook.run(
            "INSERT INTO etl_parsed_logs_accounting (report_date, is_parsed) "
            "VALUES (%s, false) "
            "ON CONFLICT (report_date) DO UPDATE SET is_parsed = false, parsed_at = null, error_message = null",
            parameters=(cob,)
        )
    return [{"cob": cob} for cob in new_cobs]


def validate_sftp_logic(**context):
    """
    Pre-sync validation cho accounting (Pattern B — T24 flat folder).
    Check file count ≥65 + sample file có CRF format markers.
    """
    result = AccountingValidator().run()
    if not result.passed:
        raise AirflowException(
            f"Accounting SFTP validation failed: {result.errors}"
        )


def update_parsed_status_logic(cob, **kwargs):
    """Cập nhật status sau khi parser chạy thành công."""
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    pg_hook.run(
        "UPDATE etl_parsed_logs_accounting "
        "SET is_parsed = true, parsed_at = now(), error_message = null "
        "WHERE report_date = %s",
        parameters=(cob,)
    )

# --- KHAI BÁO DAG ---

with DAG(
    dag_id='accounting_parse_pipeline',
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "sync"),
        os.path.join(AIRFLOW_HOME, "spark-app", "parser")
    ],
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['accounting', 'parse', 'bnctl', 'spark']
) as dag:
    t0_validate = PythonOperator(
        task_id='validate_sftp',
        python_callable=validate_sftp_logic,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    t1 = SparkKubernetesOperator(
        task_id='submit_sync_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='accounting_sync_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    )

    # T2 & T3: Các task xử lý logic DB & Check file
    t2 = PythonOperator(task_id='detect_new_cob', python_callable=detect_new_cob_logic)
    t3 = PythonOperator(task_id='insert_etl_parsed_logs', python_callable=insert_etl_parsed_logs_logic)

    t4 = SparkKubernetesOperator.partial(
        task_id='submit_accounting_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='accounting_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t3.output.map(lambda cob_dict: {'PROCESS_DATE': cob_dict['cob']})
    )

    t5 = PythonOperator.partial(
        task_id='update_etl_parsed_logs',
        python_callable=update_parsed_status_logic
    ).expand(
        op_kwargs=t3.output.map(lambda cob_dict: {"cob": cob_dict['cob']})
    )

    t0_validate >> t1 >> t2 >> t3 >> t4 >> t5