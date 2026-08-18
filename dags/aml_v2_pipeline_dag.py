"""
AML V2 Pipeline
===============
Xử lý báo cáo AML (9k, aml03, aml05, aot, fcm) từ file Excel, publish lên PBIRS.

Pattern A (vendor-organized folder, daily, special architecture):
  - Phòng ban upload file Excel theo SFTP {REMOTE}/{YYYY-MM-DD}/<reports>.xlsx
  - `dag_run.conf.START_DATE/END_DATE` = COB date range
  - Parser là Python subprocess (không Spark) — chạy trong Airflow worker
  - Grain = (report_date, report_name) — composite key, mỗi report độc lập
  - 2 log tables: etl_parse_logs_aml_v2 + etl_publish_logs_aml_v2

Flow:
  1. extract_date_range: lấy date range từ conf hoặc fallback data_interval_start
  2. Sync SFTP → MinIO raw/aml_input/{YYYY-MM-DD}/
  3. detect_parse_candidates: scan MinIO, filter theo date range, loại report đã parsed
  4. prepare_parse_logs: insert row pending (report_date, report_name)
  5. run_single_parser.expand(item): chạy Python parser riêng cho từng report
     → success: mark status=success, is_parsed=true
     → failure: mark status=failed + error_message, RAISE AirflowException
  6. prepare_publish_logs: insert publish log rows cho report parsed_ok
     (trigger_rule=ALL_DONE — chạy cả khi 1 số parser fail, publish các cái OK)
  7. push_to_pbirs_for_report.expand(item): push từng (date, name) pair lên PBIRS

Trigger example (manual):
  airflow dags trigger aml_v2_pipeline \\
    --conf '{"START_DATE":"2026-03-15","END_DATE":"2026-03-15"}'

Dependency: Độc lập.
Chi tiết: xem dags/PARSER_PIPELINES.md section 6.6.
"""

import os
import subprocess
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.sftp_validators import AmlV2Validator

DB_CONN_ID = "postgres_conn"
MINIO_CONN_ID = "minio_conn"
BUCKET_NAME = "raw"
SOURCE_PREFIX = "aml_input/"

CURRENT_DAG_DIR = os.path.dirname(os.path.realpath(__file__))  # /path/to/dags
REPO_ROOT = os.path.dirname(CURRENT_DAG_DIR)  # /path/to/airflow_orchestration
AML_V2_DIR = os.path.join(REPO_ROOT, "AML_REPORTS_V2")
AIRFLOW_HOME = REPO_ROOT
SPARK_NAMESPACE = Variable.get("SPARK_NAMESPACE", default_var="bnctl-spark2-development-ns")

REPORT_CONFIG = {
    "9k": {
        "script": "parser_9k.py",
        "output": "9k_final.xlsx",
    },
    "aml03": {
        "script": "parser_aml03.py",
        "output": "AML03_final.xlsx",
    },
    "aml05": {
        "script": "parser_aml05.py",
        "output": "AML05_final.xlsx",
    },
    "aot": {
        "script": "parser_aot.py",
        "output": "AOT_final.xlsx",
    },
    "fcm": {
        "script": "parser_fcm.py",
        "output": "FCM_final.xlsx",
    },
}


def detect_report_from_key(object_key):
    file_name = object_key.split("/")[-1].lower()
    if file_name.startswith("9k") and file_name.endswith(".xlsx"):
        return "9k"
    if "aml03" in file_name and file_name.endswith(".xlsx"):
        return "aml03"
    if "aml05" in file_name and file_name.endswith(".xlsx"):
        return "aml05"
    if "aot" in file_name and file_name.endswith(".xlsx"):
        return "aot"
    if "fcm" in file_name and file_name.endswith(".xlsx"):
        return "fcm"
    return None


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="aml_v2_pipeline",
    template_searchpath=[
        os.path.join(AIRFLOW_HOME, "spark-app", "sync"),
    ],
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=10,
    tags=["aml", "aml_v2", "parse", "publish"],
) as dag:

    @task
    def extract_date_range(**context):
        """
        Extract START_DATE/END_DATE theo thứ tự ưu tiên:
        1. dag_run.conf (JSON passed at trigger via --conf) — manual/backfill
        2. data_interval_start — scheduled run, COB = start của data interval
        """
        dag_run = context.get('dag_run')
        start_date = None
        end_date = None
        source = None

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

        logging.info(f"[extract_dates] {start_date} → {end_date} (source: {source})")
        return {'start_date': start_date, 'end_date': end_date}

    @task(retries=3, retry_delay=timedelta(minutes=10))
    def validate_sftp(dates):
        """
        Pre-sync validation cho AML v2: check folder + 5 report files.
        Validate từng COB trong date range.
        """
        from datetime import timedelta as td
        start_date = dates['start_date']
        end_date = dates['end_date']

        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')

        errors_all = []
        cob = start_dt
        while cob <= end_dt:
            cob_str = cob.strftime('%Y-%m-%d')
            result = AmlV2Validator(cob=cob_str).run()
            if not result.passed:
                errors_all.append(f"COB {cob_str}: {result.errors}")
            cob += td(days=1)

        if errors_all:
            raise AirflowException(
                f"AML v2 SFTP validation failed: {errors_all}"
            )

    @task
    def detect_parse_candidates(**context):
        """
        Phát hiện các file AML input trong date range cần parse.
        Lấy date range từ upstream extract_date task.
        """
        ti = context['ti']
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

        # Get date range from upstream task
        upstream_output = ti.xcom_pull(task_ids='extract_date_range')
        if not upstream_output:
            logging.error("[detect_parse_candidates] No upstream output from extract_dates")
            return []
        
        start_date_str = upstream_output.get('start_date')
        end_date_str = upstream_output.get('end_date')
        
        from datetime import datetime
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

        all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX) or []

        discovered = []
        for key in all_keys:
            report_name = detect_report_from_key(key)
            if not report_name:
                continue

            parts = key.split("/")
            if len(parts) < 3:
                continue

            report_date = parts[1]
            
            # Filter by date range
            try:
                report_dt = datetime.strptime(report_date, '%Y-%m-%d')
                if not (start_dt <= report_dt <= end_dt):
                    continue
            except ValueError:
                continue

            script_name = REPORT_CONFIG[report_name]["script"]
            output_file = REPORT_CONFIG[report_name]["output"]

            discovered.append(
                {
                    "report_date": report_date,
                    "report_name": report_name,
                    "script_name": script_name,
                    "input_key": key,
                    "output_key": f"aml_output/{report_date}/{output_file}",
                }
            )

        if not discovered:
            logging.info("[detect_parse_candidates] Không có file AML trong date range")
            return []

        parsed_rows = pg_hook.get_records(
            """
            SELECT report_date::text, report_name
            FROM etl_parse_logs_aml_v2
            WHERE is_parsed = true
            """
        )
        parsed_set = {(row[0], row[1]) for row in parsed_rows}

        candidates = [
            item
            for item in discovered
            if (item["report_date"], item["report_name"]) not in parsed_set
        ]

        logging.info(f"[detect_parse_candidates] AML v2 parse candidates: {len(candidates)}")
        return candidates

    @task
    def prepare_parse_logs(candidates):
        if not candidates:
            return []

        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        for item in candidates:
            pg_hook.run(
                """
                INSERT INTO etl_parse_logs_aml_v2
                    (report_date, report_name, input_key, output_key, is_parsed, status, error_message, parsed_at)
                VALUES
                    (%s::date, %s, %s, %s, false, 'pending', null, null)
                ON CONFLICT (report_date, report_name)
                DO UPDATE SET
                    input_key = EXCLUDED.input_key,
                    output_key = EXCLUDED.output_key,
                    is_parsed = false,
                    status = 'pending',
                    error_message = null,
                    parsed_at = null,
                    updated_at = now()
                """,
                parameters=(
                    item["report_date"],
                    item["report_name"],
                    item["input_key"],
                    item["output_key"],
                ),
            )

        return candidates

    @task
    def run_single_parser(item):
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

        report_date = item["report_date"]
        report_name = item["report_name"]
        script_path = os.path.join(AML_V2_DIR, item["script_name"])

        pg_hook.run(
            """
            UPDATE etl_parse_logs_aml_v2
            SET status = 'running', error_message = null, updated_at = now()
            WHERE report_date = %s::date AND report_name = %s
            """,
            parameters=(report_date, report_name),
        )

        env = os.environ.copy()
        env.update(
            {
                "DATE_STR": report_date,
                "MINIO_ENDPOINT": str(Variable.get("MINIO_ENDPOINT")),
                "MINIO_ACCESS_KEY": str(Variable.get("MINIO_ACCESS_KEY")),
                "MINIO_SECRET_KEY": str(Variable.get("MINIO_SECRET_KEY")),
                "MINIO_SECURE": str(Variable.get("MINIO_SECURE", default_var="False")),
            }
        )

        result = subprocess.run(
            ["python", script_path],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            pg_hook.run(
                """
                UPDATE etl_parse_logs_aml_v2
                SET is_parsed = true,
                    status = 'success',
                    parsed_at = now(),
                    error_message = null,
                    updated_at = now()
                WHERE report_date = %s::date AND report_name = %s
                """,
                parameters=(report_date, report_name),
            )
            return {
                "report_date": report_date,
                "report_name": report_name,
                "parsed_ok": True,
                "output_key": item["output_key"],
            }

        error_text = (result.stderr or result.stdout or "Parser failed")[:4000]
        pg_hook.run(
            """
            UPDATE etl_parse_logs_aml_v2
            SET is_parsed = false,
                status = 'failed',
                error_message = %s,
                updated_at = now()
            WHERE report_date = %s::date AND report_name = %s
            """,
            parameters=(error_text, report_date, report_name),
        )

        logging.error(
            "Parser failed for %s %s. stderr/stdout: %s",
            report_date,
            report_name,
            error_text,
        )
        # Raise để Airflow UI mark task failed. State + error_message đã persist
        # trong etl_parse_logs_aml_v2, downstream trigger_rule=ALL_DONE sẽ tiếp tục.
        raise AirflowException(
            f"Parser failed for {report_date} {report_name}: {error_text[:500]}"
        )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def prepare_publish_logs(parse_results):
        """
        Insert publish log row cho mỗi (date, name) đã parse success.
        Trigger rule ALL_DONE: chạy ngay cả khi 1 số run_single_parser fail, để
        publish các report OK (composite grain — mỗi report độc lập).
        """
        if not parse_results:
            logging.info("[prepare_publish_logs] Không có parse result nào.")
            return []

        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        publish_items = []

        for r in parse_results:
            if not r or not r.get("parsed_ok"):
                continue

            report_date = r["report_date"]
            report_name = r["report_name"]
            output_key  = r["output_key"]

            pg_hook.run(
                """
                INSERT INTO etl_publish_logs_aml_v2
                    (report_date, report_name, source_key, pbirs_path, is_published, status, error_message, published_at)
                VALUES
                    (%s::date, %s, %s, %s, false, 'pending', null, null)
                ON CONFLICT (report_date, report_name)
                DO UPDATE SET
                    source_key = EXCLUDED.source_key,
                    pbirs_path = EXCLUDED.pbirs_path,
                    is_published = false,
                    status = 'pending',
                    error_message = null,
                    published_at = null,
                    updated_at = now()
                """,
                parameters=(
                    report_date,
                    report_name,
                    output_key,
                    f"/AML_V2/{report_date}",
                ),
            )
            publish_items.append({
                "report_date": report_date,
                "report_name": report_name,
                "output_key":  output_key,
            })

        logging.info(
            f"[prepare_publish_logs] {len(publish_items)} (date,name) ready to publish: "
            f"{[(i['report_date'], i['report_name']) for i in publish_items]}"
        )
        return publish_items

    def _ensure_requests_ntlm():
        """
        TODO: bake requests-ntlm vào Airflow worker image rồi xoá hàm này.
        Tạm thời runtime-install vì image hiện tại chưa có package này.
        Warning: runtime pip install là anti-pattern — chậm, fail nếu không có net,
        leak state qua worker. Chỉ chấp nhận ở giai đoạn dev/test.
        """
        try:
            import requests_ntlm  # noqa: F401
            return
        except ImportError:
            pass

        import sys
        logging.warning(
            "⚠️  requests-ntlm not in image — installing at runtime (anti-pattern). "
            "TODO: bake into Airflow worker image."
        )
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "requests-ntlm", "-q"]
        )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def push_to_pbirs_for_report(item):
        """
        Push 1 file AML output lên PBIRS theo composite key (date, name).
        Filter UPDATE log bằng CẢ report_date VÀ report_name để tránh touch
        nhầm record của report fail trước đó.
        """
        _ensure_requests_ntlm()

        report_date = item["report_date"]
        report_name = item["report_name"]
        output_key  = item["output_key"]

        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

        pg_hook.run(
            """
            UPDATE etl_publish_logs_aml_v2
            SET status = 'running', error_message = null, updated_at = now()
            WHERE report_date = %s::date AND report_name = %s
            """,
            parameters=(report_date, report_name),
        )

        script_path = os.path.join(AML_V2_DIR, "push_to_pbirs.py")
        env = os.environ.copy()
        env.update({
            "DATE_STR":         report_date,
            "OUTPUT_KEY":       output_key,
            "MINIO_ENDPOINT":   str(Variable.get("MINIO_ENDPOINT")),
            "MINIO_ACCESS_KEY": str(Variable.get("MINIO_ACCESS_KEY")),
            "MINIO_SECRET_KEY": str(Variable.get("MINIO_SECRET_KEY")),
            "MINIO_SECURE":     str(Variable.get("MINIO_SECURE", default_var="False")),
            "PBIRS_BASE_URL":   str(Variable.get("PBIRS_BASE_URL")),
            "PBIRS_USERNAME":   str(Variable.get("PBIRS_USERNAME")),
            "PBIRS_PASSWORD":   str(Variable.get("PBIRS_PASSWORD")),
        })

        result = subprocess.run(
            ["python", script_path],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            pg_hook.run(
                """
                UPDATE etl_publish_logs_aml_v2
                SET is_published = true,
                    status = 'success',
                    published_at = now(),
                    error_message = null,
                    updated_at = now()
                WHERE report_date = %s::date AND report_name = %s
                """,
                parameters=(report_date, report_name),
            )
            return {"report_date": report_date, "report_name": report_name, "published_ok": True}

        # Failure path — persist error_message + raise để Airflow UI mark failed
        error_text = (result.stderr or result.stdout or "Publish failed")[:4000]
        pg_hook.run(
            """
            UPDATE etl_publish_logs_aml_v2
            SET is_published = false,
                status = 'failed',
                error_message = %s,
                updated_at = now()
            WHERE report_date = %s::date AND report_name = %s
            """,
            parameters=(error_text, report_date, report_name),
        )
        logging.error("PBIRS push failed for %s %s: %s", report_date, report_name, error_text)
        raise AirflowException(
            f"PBIRS push failed for {report_date} {report_name}: {error_text[:500]}"
        )

    # Bước 0: Extract date range
    dates = extract_date_range()

    # Pre-sync validation
    validation = validate_sftp(dates)

    # Bước 1: Sync SFTP → MinIO
    sync_task = SparkKubernetesOperator(
        task_id='sync_aml_v2_input',
        namespace=SPARK_NAMESPACE,
        application_file='aml_v2_sync_spark.yaml',
        kubernetes_conn_id='k8s',
        do_xcom_push=False,
        delete_on_termination=False,
    )

    # Bước 2: Detect parse candidates (tuples of (date, report))
    parse_candidates = detect_parse_candidates()

    # Bước 3: Insert pending rows vào etl_parse_logs_aml_v2
    parse_ready = prepare_parse_logs(parse_candidates)

    # Bước 4: Run Python parser per (date, report) — fail loud nếu parser fail
    parse_results = run_single_parser.expand(item=parse_ready)

    # Bước 5: Insert publish log rows cho các (date, name) parsed_ok
    publish_items = prepare_publish_logs(parse_results)

    # Bước 6: Push từng (date, name) lên PBIRS — expand per item
    push_to_pbirs = push_to_pbirs_for_report.expand(item=publish_items)

    # DAG flow
    dates >> validation >> sync_task >> parse_candidates >> parse_ready >> parse_results >> publish_items >> push_to_pbirs
