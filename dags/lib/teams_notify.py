"""
Teams webhook notification for pipeline_model DAGs — Airflow 3.x compatible.

Design: notify_teams takes NO XCom inputs so it can never be blocked by
upstream failures or unresolved dynamic-task XCom. Dependencies are wired
with >> (order only). Results are read from the etl_model_logs_* table.

Usage inside each model DAG (with DAG(...) block):

    from lib.teams_notify import send_model_notification

    LOG_TABLE = "etl_model_logs_credit"   # domain-specific
    S3_LOG_BUCKET = "airflow-logs"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_teams(**context):
        try:
            send_model_notification(context, LOG_TABLE, POSTGRES_CONN_ID, S3_LOG_BUCKET)
        except Exception as exc:
            logging.warning("[teams_notify] Notification failed (non-fatal): %s", exc)

    # Wiring — dependency only, no XCom passed:
    ready_cobs = detect_ready_cobs()
    ...
    update = update_etl_model_logs(dbt_tasks.output)
    notify = notify_teams()
    ready_cobs >> notify
    update >> notify
"""

import logging
from datetime import datetime, timezone

import requests

TEAMS_WEBHOOK_URL = (
    "https://jitsinnovationlabs.webhook.office.com/webhookb2/"
    "5bf2407b-7cf3-40a3-8253-ab4d9ca0bfb5@c8114483-2f2b-41ec-9624-7a8f896b4ed8/"
    "IncomingWebhook/428ee9f1d8ed4b88a0da38167c3afdad/"
    "acfa638b-fce6-4021-83ae-2e6d1476cab6/"
    "V2tVaMkLKCssKWecqPEh_IDlkIG7c08kRd8HTn-83MgrA1"
)

_COLOR = {
    "success": "00b300",
    "partial": "ff8c00",
    "failed":  "cc0000",
    "skipped": "808080",
}


def _fetch_dbt_summary(dag_id: str, run_id: str, bucket: str) -> str:
    """
    Read dbt_run task logs from S3/Minio.
    Returns one clean line per COB, e.g.:
      2026-01-23 → PASS=21 WARN=0 ERROR=0 SKIP=0
    """
    try:
        import re
        import boto3
        from botocore.exceptions import ClientError
        from airflow.models import Connection

        conn = Connection.get_connection_from_secrets("minio_conn")
        extra = conn.extra_dejson
        endpoint = (
            extra.get("endpoint_url")
            or extra.get("host")
            or "http://minio-svc.bnctl-minio-development-ns.svc.cluster.local:9000"
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
        )

        lines_out = []
        for map_index in range(50):
            key = (
                f"dag_id={dag_id}/run_id={run_id}"
                f"/task_id=dbt_run/map_index={map_index}/attempt=1.log"
            )
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                content = obj["Body"].read().decode("utf-8", errors="replace")

                cob_label = f"map{map_index}"
                done_stats = ""
                for line in content.splitlines():
                    # Extract COB date from echo line
                    m = re.search(r"COB:\s*(\d{4}-\d{2}-\d{2})", line)
                    if m:
                        cob_label = m.group(1)
                    # Extract stats from "Done. PASS=N WARN=N ERROR=N SKIP=N" line
                    if "Done. PASS=" in line and "s3://" not in line:
                        raw = line.split("INFO - ")[-1].strip()
                        raw = re.sub(r"^\d{2}:\d{2}:\d{2}\s+", "", raw)
                        m2 = re.search(r"PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)\s+SKIP=(\d+)", raw)
                        if m2:
                            done_stats = f"PASS={m2.group(1)} WARN={m2.group(2)} ERROR={m2.group(3)} SKIP={m2.group(4)}"
                        else:
                            done_stats = raw

                if done_stats:
                    log_path = (
                        f"s3://{bucket}/dag_id={dag_id}/run_id={run_id}"
                        f"/task_id=dbt_run/map_index={map_index}/attempt=1.log"
                    )
                    lines_out.append(f"{cob_label} → {done_stats}\n{log_path}")
            except ClientError as e:
                if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                    break
                raise

        return "\n\n".join(lines_out) if lines_out else ""
    except Exception as exc:
        logging.warning("[teams_notify] Could not fetch dbt summary: %s", exc)
        return ""


def send_model_notification(
    context: dict,
    log_table: str,
    postgres_conn_id: str,
    s3_log_bucket: str = "airflow-logs",
) -> None:
    """
    Query etl_model_logs_* and send a Teams card.

    Scoping logic:
      - If dag_run.conf has START_DATE/END_DATE  -> use those dates explicitly.
      - Otherwise -> use dag_run.start_date to find COBs touched in this run.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    dag_run = context["dag_run"]
    dag_id: str = dag_run.dag_id
    run_id: str = dag_run.run_id
    conf: dict = dag_run.conf or {}

    run_start = dag_run.start_date
    if run_start and getattr(run_start, "tzinfo", None) is None:
        run_start = run_start.replace(tzinfo=timezone.utc)

    pg = PostgresHook(postgres_conn_id=postgres_conn_id)

    start_date = conf.get("START_DATE")
    end_date = conf.get("END_DATE")

    if start_date and end_date:
        rows = pg.get_records(
            f"""
            SELECT report_date::TEXT, is_built
            FROM {log_table}
            WHERE report_date BETWEEN %s AND %s
            ORDER BY report_date
            """,
            parameters=(start_date, end_date),
        )
    elif run_start:
        rows = pg.get_records(
            f"""
            SELECT report_date::TEXT, is_built
            FROM {log_table}
            WHERE built_at >= %s
               OR (is_built = false AND error_message IS NOT NULL AND built_at IS NULL)
            ORDER BY report_date
            """,
            parameters=(run_start,),
        )
    else:
        rows = []

    success_cobs = [r[0] for r in rows if r[1]]
    failed_cobs  = [r[0] for r in rows if not r[1]]

    if not rows:
        state = "skipped"
    elif not failed_cobs:
        state = "success"
    elif not success_cobs:
        state = "failed"
    else:
        state = "partial"

    facts: list[dict] = []
    if success_cobs:
        facts.append({"name": "Success COBs", "value": ", ".join(success_cobs)})
    if failed_cobs:
        facts.append({"name": "Failed COBs", "value": ", ".join(failed_cobs)})
    if not rows:
        facts.append({"name": "Note", "value": "No COBs were processed in this run"})

    dbt_summary = _fetch_dbt_summary(dag_id, run_id, s3_log_bucket)
    if dbt_summary:
        facts.append({"name": "dbt run summary", "value": dbt_summary})

    if run_start:
        duration = str(datetime.now(timezone.utc) - run_start).split(".")[0]
        facts.append({"name": "Duration", "value": duration})

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": _COLOR.get(state, "ff8c00"),
        "summary": f"[{dag_id}] {state.upper()}",
        "sections": [
            {
                "activityTitle": f"**{dag_id}** — {state.upper()}",
                "activitySubtitle": f"`{run_id}`",
                "facts": facts,
            }
        ],
    }

    resp = requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=15)
    resp.raise_for_status()
    logging.info("[teams_notify] Sent for %s (%s)", dag_id, state)


def make_notify_task(log_table: str, postgres_conn_id: str):
    """
    Factory — returns a @task(trigger_rule=ALL_DONE) function bound to log_table.
    Call the returned function inside a with DAG(...) block to wire it up.

    Example:
        notify_teams = make_notify_task("etl_model_logs_credit", POSTGRES_CONN_ID)
        with DAG(...) as dag:
            ...
            notify = notify_teams()
            ready_cobs >> notify
            update >> notify
    """
    from airflow.decorators import task
    from airflow.utils.trigger_rule import TriggerRule

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_teams(**context):
        try:
            send_model_notification(context, log_table, postgres_conn_id)
        except Exception as exc:
            logging.warning("[teams_notify] Notification failed (non-fatal): %s", exc)

    return notify_teams
