"""
Treasury External Pipeline - Parser Layer
==========================================
Xử lý dữ liệu Treasury External Liquidity (file Excel từ MinIO).

Flow:
  1. extract_dates     : lấy START_DATE/END_DATE từ conf hoặc data_interval_start
  2. detect_new_cob    : scan raw/treasury_external/ có _SUCCESS, lọc chưa parse
  3. insert_etl_parsed_logs : ghi row pending cho mỗi COB
  4. submit_external_parser_spark   : Spark job treasury_external_parser_spark.yaml
                                      → hive.bronze.treasury_external
  5. submit_liquid_mock_parser_spark: Spark job treasury_liquid_mock_external_parser_spark.yaml
                                      → chạy sau external parser, trước model
  6. update_etl_parsed_logs : mark is_parsed=true

Trigger example:
  airflow dags trigger treasury_external_pipeline \\
    --conf '{"START_DATE":"2026-04-01","END_DATE":"2026-04-01"}'

Dependency: MinIO raw/treasury_external/{YYYY-MM-DD}/ phải có _SUCCESS marker.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os
import re

DB_CONN_ID = 'postgres_conn'
MINIO_CONN_ID = 'minio_conn'
BUCKET_NAME = 'raw'
SOURCE_PREFIX = 'treasury_external/'
ETL_LOG_TABLE = 'etl_parsed_logs_treasury_external'

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


def extract_date_range_logic(**context):
    dag_run = context.get('dag_run')
    start_date = end_date = source = None

    if dag_run and dag_run.conf:
        start_date = dag_run.conf.get('START_DATE')
        end_date = dag_run.conf.get('END_DATE')
        if start_date or end_date:
            source = 'dag_run.conf'

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
    ti = context['ti']
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    upstream = ti.xcom_pull(task_ids='extract_dates')
    if not upstream:
        logging.error("[detect_new_cob] No upstream output from extract_dates")
        return []

    start_dt = datetime.strptime(upstream['start_date'], '%Y-%m-%d')
    end_dt = datetime.strptime(upstream['end_date'], '%Y-%m-%d')

    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX) or []

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
        f"SELECT report_date::text FROM {ETL_LOG_TABLE} WHERE is_parsed = true"
    )
    parsed_cobs = {row[0] for row in parsed_records}

    cobs_to_run = sorted(ready_on_minio - parsed_cobs)
    logging.info(
        f"[detect_new_cob] Treasury external COB ready trong "
        f"[{upstream['start_date']} → {upstream['end_date']}] chưa parse: {cobs_to_run}"
    )
    return cobs_to_run


def insert_etl_parsed_logs_logic(**context):
    ti = context['ti']
    new_cobs = ti.xcom_pull(task_ids='detect_new_cob')
    if not new_cobs:
        return []

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    for cob in new_cobs:
        pg_hook.run(
            f"INSERT INTO {ETL_LOG_TABLE} (report_date, is_parsed) "
            "VALUES (%s, false) "
            "ON CONFLICT (report_date) DO UPDATE SET is_parsed = false, parsed_at = null, error_message = null",
            parameters=(cob,)
        )
    return [{"cob": cob} for cob in new_cobs]


def update_parsed_status_logic(cob, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    pg_hook.run(
        f"UPDATE {ETL_LOG_TABLE} "
        "SET is_parsed = true, parsed_at = now(), error_message = null "
        "WHERE report_date = %s",
        parameters=(cob,)
    )
    logging.info(f"[update_logs] Treasury external COB {cob} marked as parsed.")


with DAG(
    dag_id='treasury_external_pipeline',
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "parser")
    ],
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['treasury', 'external', 'liquidity', 'parse', 'spark']
) as dag:

    t0_extract = PythonOperator(
        task_id='extract_dates',
        python_callable=extract_date_range_logic
    )

    t1_detect = PythonOperator(
        task_id='detect_new_cob',
        python_callable=detect_new_cob_logic
    )

    t2_insert_logs = PythonOperator(
        task_id='insert_etl_parsed_logs',
        python_callable=insert_etl_parsed_logs_logic
    )

    # Spark job 1: parse raw Excel → hive.bronze.treasury_external
    t3_external_parser = SparkKubernetesOperator.partial(
        task_id='submit_external_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='treasury_external_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t2_insert_logs.output.map(lambda cob_dict: {'PROCESS_DATE': cob_dict['cob']})
    )

    # Spark job 2: liquid mock external — chạy sau external parser, trước model
    t4_liquid_mock_parser = SparkKubernetesOperator.partial(
        task_id='submit_liquid_mock_external_parser_spark_app',
        namespace=SPARK_NAMESPACE,
        application_file='treasury_liquid_mock_external_parser_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False
    ).expand(
        params=t2_insert_logs.output.map(lambda cob_dict: {'PROCESS_DATE': cob_dict['cob']})
    )

    t5_update_logs = PythonOperator.partial(
        task_id='update_etl_parsed_logs',
        python_callable=update_parsed_status_logic
    ).expand(
        op_kwargs=t2_insert_logs.output.map(lambda cob_dict: {"cob": cob_dict['cob']})
    )

    t0_extract >> t1_detect >> t2_insert_logs >> t3_external_parser >> t4_liquid_mock_parser >> t5_update_logs
