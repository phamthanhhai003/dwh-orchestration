"""
dbt Model Pipeline Factory
===========================
Single file replacing 6 individual model DAGs:
  - accounting_model_pipeline
  - credit_model_pipeline
  - operational_model_bnk_pipeline
  - operational_model_hnf_pipeline
  - treasury_liquidity_model_pipeline
  - treasury_remittance_model_pipeline

All behavior controlled by DOMAIN_CONFIGS. Each config dict drives one DAG.

Config keys:
  dag_id          str   — Airflow dag_id
  tags            list  — Airflow UI tags
  start_date      datetime
  dbt_dir         str   — subdirectory under repo root (e.g. "ACCOUNTING_REPORTS")
  work_prefix     str   — /tmp/{work_prefix}_run_{cob} scratch dir
  dbt_select      str   — space-separated models/tags passed to `dbt run --select`
  model_log_table str   — etl_model_logs_{domain}
  detect_from     str   — "parsed": join etl_parsed_logs_* (standard)
                          "model":  join another etl_model_logs_* (treasury_liquidity)
  parsed_log_table str  — required when detect_from="parsed"
  dep_model_table  str  — required when detect_from="model"
  has_refresh     bool  — True adds refresh_metadata step before dbt_run (accounting only)
  monthly_cob     bool  — True → date_trunc('month', ...) grouping instead of daily (hnf only)
  date_cast       str   — extra PG cast on report_date param, e.g. "::date" (hnf), "" otherwise
  update_mode     str   — "bulk":    single update_etl_model_logs(dbt_outputs) with fail tracking
                          "per_cob": update_etl_model_logs.expand(cob=...) one row per COB
  no_partial_parse bool — adds --no-partial-parse to dbt run (bnk, hnf)
"""

import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.teams_notify import make_notify_task

_DAG_DIR = os.path.dirname(os.path.realpath(__file__))
_REPO_DIR = os.path.dirname(_DAG_DIR)
POSTGRES_CONN_ID = "postgres_conn"
DBT_TARGET = Variable.get("DBT_TARGET", default_var="dev")

# ─── Domain configurations ────────────────────────────────────────────────────

DOMAIN_CONFIGS = [
    {
        "dag_id": "accounting_model_pipeline",
        "tags": ["accounting", "model"],
        "start_date": datetime(2026, 1, 1),
        "dbt_dir": "ACCOUNTING_REPORTS",
        "work_prefix": "accounting",
        "dbt_select": "staging reporting",
        "model_log_table": "etl_model_logs_accounting",
        "detect_from": "parsed",
        "parsed_log_table": "etl_parsed_logs_accounting",
        "has_refresh": True,
        "monthly_cob": False,
        "date_cast": "",
        "update_mode": "bulk",
        "no_partial_parse": False,
    },
    {
        "dag_id": "credit_model_pipeline",
        "tags": ["credit", "model"],
        "start_date": datetime(2026, 1, 1),
        "dbt_dir": "CREDIT_REPORTS",
        "work_prefix": "credit",
        "dbt_select": "staging reporting",
        "model_log_table": "etl_model_logs_credit",
        "detect_from": "parsed",
        "parsed_log_table": "etl_parsed_logs_credit",
        "has_refresh": False,
        "monthly_cob": False,
        "date_cast": "",
        "update_mode": "bulk",
        "no_partial_parse": False,
    },
    {
        "dag_id": "operational_model_bnk_pipeline",
        "tags": ["operational", "model", "bnk"],
        "start_date": datetime(2026, 1, 1),
        "dbt_dir": "OPERATIONAL_REPORT",
        "work_prefix": "operational_bnk",
        "dbt_select": (
            "stg__opening_account stg__closed_account stg__list_of_deposits "
            "final_opening_account_report final_closed_account_report final_list_of_deposits_report"
        ),
        "model_log_table": "etl_model_logs_operational_bnk",
        "detect_from": "parsed",
        "parsed_log_table": "etl_parsed_logs_operational_bnk",
        "has_refresh": False,
        "monthly_cob": False,
        "date_cast": "",
        "update_mode": "bulk",
        "no_partial_parse": True,
    },
    {
        "dag_id": "operational_model_hnf_pipeline",
        "tags": ["operational", "model", "hnf"],
        "start_date": datetime(2026, 1, 1),
        "dbt_dir": "OPERATIONAL_REPORT",
        "work_prefix": "operational_hnf",
        "dbt_select": (
            "stg__hnf_account_detail "
            "final_hnf_by_age_report final_hnf_by_gender_report "
            "final_digital_service_list final_digital_service_transaction "
            "final_bank_agents_branches final_bank_agents_transaction"
        ),
        "model_log_table": "etl_model_logs_operational_hnf",
        "detect_from": "parsed",
        "parsed_log_table": "etl_parsed_logs_operational_hnf",
        "has_refresh": False,
        "monthly_cob": True,
        "date_cast": "::date",
        "update_mode": "bulk",
        "no_partial_parse": True,
    },
    {
        "dag_id": "treasury_liquidity_model_pipeline",
        "tags": ["treasury", "liquidity", "model"],
        "start_date": datetime(2026, 4, 7),
        "dbt_dir": "TREASURY_REPORTS",
        "work_prefix": "treasury_liquidity",
        "dbt_select": "tag:liquidity",
        "model_log_table": "etl_model_logs_treasury_liquidity",
        "detect_from": "model",
        "dep_model_table": "etl_model_logs_accounting",
        "has_refresh": False,
        "monthly_cob": False,
        "date_cast": "",
        "update_mode": "per_cob",
        "no_partial_parse": False,
    },
    {
        "dag_id": "treasury_remittance_model_pipeline",
        "tags": ["treasury", "remittance", "model"],
        "start_date": datetime(2026, 4, 7),
        "dbt_dir": "TREASURY_REPORTS",
        "work_prefix": "treasury_remittance",
        "dbt_select": "tag:remittance",
        "model_log_table": "etl_model_logs_treasury_remittance",
        "detect_from": "parsed",
        "parsed_log_table": "etl_parsed_logs_treasury_remittance",
        "has_refresh": False,
        "monthly_cob": False,
        "date_cast": "",
        "update_mode": "per_cob",
        "no_partial_parse": False,
    },
]


# ─── Factory ──────────────────────────────────────────────────────────────────

def make_model_dag(cfg: dict) -> DAG:
    model_log_table = cfg["model_log_table"]
    dbt_source_dir = os.path.join(_REPO_DIR, cfg["dbt_dir"])
    work_prefix = cfg["work_prefix"]
    dbt_select = cfg["dbt_select"]
    detect_from = cfg["detect_from"]
    monthly_cob = cfg["monthly_cob"]
    date_cast = cfg["date_cast"]
    update_mode = cfg["update_mode"]
    has_refresh = cfg["has_refresh"]
    no_partial_parse = cfg.get("no_partial_parse", False)

    notify_teams = make_notify_task(model_log_table, POSTGRES_CONN_ID)

    with DAG(
        dag_id=cfg["dag_id"],
        start_date=cfg["start_date"],
        schedule=None,
        catchup=False,
        max_active_tasks=1,
        tags=cfg["tags"],
    ) as dag:

        @task
        def detect_ready_cobs(**context):
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            dag_run = context.get("dag_run")
            start_date = end_date = None
            force_rerun = False
            if dag_run and dag_run.conf:
                start_date = dag_run.conf.get("START_DATE")
                end_date = dag_run.conf.get("END_DATE")
                force_rerun = dag_run.conf.get("FORCE_RERUN", False)

            if force_rerun and not (start_date and end_date):
                raise AirflowException(
                    "FORCE_RERUN=true requires START_DATE and END_DATE. "
                    "Refusing to force-rerun all built COBs without a date scope."
                )

            if detect_from == "parsed":
                parsed_table = cfg["parsed_log_table"]
                if monthly_cob:
                    sel = "date_trunc('month', p.report_date)::date::TEXT"
                    join_on = "date_trunc('month', p.report_date)::date = b.report_date"
                    group_by = "GROUP BY date_trunc('month', p.report_date)"
                    distinct = "DISTINCT "
                else:
                    sel = "p.report_date::TEXT"
                    join_on = "p.report_date = b.report_date"
                    group_by = ""
                    distinct = ""

                if start_date and end_date:
                    if force_rerun:
                        logging.info(f"[detect] FORCE_RERUN: {start_date} → {end_date}")
                        sql = f"""
                            SELECT {distinct}{sel}
                            FROM {parsed_table} p
                            WHERE p.is_parsed = true
                              AND p.report_date BETWEEN %s AND %s
                            ORDER BY 1;
                        """
                    else:
                        logging.info(f"[detect] Scoped: {start_date} → {end_date}")
                        sql = f"""
                            SELECT {sel}
                            FROM {parsed_table} p
                            LEFT JOIN {model_log_table} b ON {join_on}
                            WHERE p.is_parsed = true
                              AND (b.is_built IS NULL OR b.is_built = false)
                              AND p.report_date BETWEEN %s AND %s
                            {group_by}
                            ORDER BY 1;
                        """
                    records = pg_hook.get_records(sql, parameters=(start_date, end_date))
                else:
                    logging.info("[detect] No conf — querying ALL pending COBs")
                    sql = f"""
                        SELECT {sel}
                        FROM {parsed_table} p
                        LEFT JOIN {model_log_table} b ON {join_on}
                        WHERE p.is_parsed = true
                          AND (b.is_built IS NULL OR b.is_built = false)
                        {group_by}
                        ORDER BY 1;
                    """
                    records = pg_hook.get_records(sql)

            else:  # detect_from == "model" — treasury_liquidity depends on accounting model
                dep_table = cfg["dep_model_table"]
                if start_date and end_date:
                    if force_rerun:
                        logging.info(f"[detect] FORCE_RERUN: {start_date} → {end_date}")
                        sql = f"""
                            SELECT a.report_date::TEXT
                            FROM {dep_table} a
                            WHERE a.is_built = true
                              AND a.report_date BETWEEN %s AND %s
                            ORDER BY a.report_date;
                        """
                    else:
                        logging.info(f"[detect] Scoped: {start_date} → {end_date}")
                        sql = f"""
                            SELECT a.report_date::TEXT
                            FROM {dep_table} a
                            LEFT JOIN {model_log_table} t ON a.report_date = t.report_date
                            WHERE a.is_built = true
                              AND (t.is_built IS NULL OR t.is_built = false)
                              AND a.report_date BETWEEN %s AND %s
                            ORDER BY a.report_date;
                        """
                    records = pg_hook.get_records(sql, parameters=(start_date, end_date))
                else:
                    logging.info("[detect] No conf — querying ALL pending COBs")
                    sql = f"""
                        SELECT a.report_date::TEXT
                        FROM {dep_table} a
                        LEFT JOIN {model_log_table} t ON a.report_date = t.report_date
                        WHERE a.is_built = true
                          AND (t.is_built IS NULL OR t.is_built = false)
                        ORDER BY a.report_date;
                    """
                    records = pg_hook.get_records(sql)

            cobs = [r[0] for r in records] if records else []
            logging.info(f"[detect] Ready COBs: {cobs}")
            return cobs

        @task
        def insert_etl_model_logs(cob_list):
            if not cob_list:
                return []
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            for cob in cob_list:
                pg_hook.run(
                    f"INSERT INTO {model_log_table} (report_date, is_built) VALUES (%s{date_cast}, false) "
                    f"ON CONFLICT (report_date) DO UPDATE SET is_built = false, built_at = null",
                    parameters=(cob,),
                )
            return [{"cob": cob} for cob in cob_list]

        # ─── dbt run command ──────────────────────────────────────────────────

        _no_partial = "--no-partial-parse " if no_partial_parse else ""
        DBT_RUN_COMMAND = (
            "set -e; "
            f"export WORK_DIR=/tmp/{work_prefix}_run_{{{{ params.cob }}}}; "
            "echo '>>> STAGING PROJECT TO: $WORK_DIR'; "
            "rm -rf $WORK_DIR && mkdir -p $WORK_DIR; "
            f"cp -R {dbt_source_dir}/* $WORK_DIR/; "
            "cd $WORK_DIR; "
            "export DBT_PROFILES_DIR=.; "
            "echo '>>> INSTALLING DEPENDENCIES...'; "
            "dbt deps && "
            f"echo '>>> RUNNING {work_prefix.upper()} DBT FOR COB: {{{{ params.cob }}}}'; "
            "dbt run "
            f"{_no_partial}"
            f"--select {dbt_select} "
            "--vars '{\"target_date\": \"{{ params.cob }}\"}' "
            f"--target {DBT_TARGET}; "
            "echo '{{ params.cob }}'"
        )

        _dbt_env = {
            "DREMIO_HOST": Variable.get("dremio_host"),
            "DREMIO_USER": Variable.get("dremio_user"),
            "DREMIO_PASSWORD": Variable.get("dremio_password"),
        }

        if has_refresh:
            REFRESH_COMMAND = (
                "set -e; "
                f"export WORK_DIR=/tmp/{work_prefix}_refresh; "
                "echo '>>> STAGING PROJECT TO: $WORK_DIR'; "
                "rm -rf $WORK_DIR && mkdir -p $WORK_DIR; "
                f"cp -R {dbt_source_dir}/* $WORK_DIR/; "
                "cd $WORK_DIR; "
                "export DBT_PROFILES_DIR=.; "
                "echo '>>> INSTALLING DEPENDENCIES FOR REFRESH...'; "
                "dbt deps && "
                "echo '>>> REFRESHING DREMIO METADATA'; "
                f"dbt run-operation refresh_bronze --target {DBT_TARGET}"
            )
            refresh_metadata = BashOperator(
                task_id="refresh_metadata",
                bash_command=REFRESH_COMMAND,
                append_env=True,
                env=_dbt_env,
            )

        dbt_run = BashOperator.partial(
            task_id="dbt_run",
            bash_command=DBT_RUN_COMMAND,
            append_env=True,
            env=_dbt_env,
            do_xcom_push=True,
        )

        # ─── update task — two modes ──────────────────────────────────────────

        if update_mode == "bulk":
            @task(trigger_rule=TriggerRule.ALL_DONE)
            def update_etl_model_logs(dbt_outputs):
                if not dbt_outputs:
                    raise AirflowException(
                        "No dbt outputs received. All dbt tasks may have failed or "
                        "no COBs were detected. Check upstream tasks."
                    )
                success_cobs = [c for c in dbt_outputs if c and str(c).strip()]
                failed_count = len(dbt_outputs) - len(success_cobs)
                pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                for cob in success_cobs:
                    cob = str(cob).strip()
                    logging.info(f"[update] SUCCESS: {cob}")
                    pg_hook.run(
                        f"UPDATE {model_log_table} "
                        f"SET is_built = true, built_at = NOW(), error_message = null "
                        f"WHERE report_date = %s{date_cast}",
                        parameters=(cob,),
                    )
                if failed_count > 0:
                    logging.warning(f"[update] {failed_count} dbt task(s) failed out of {len(dbt_outputs)}")
                    pg_hook.run(
                        f"UPDATE {model_log_table} "
                        f"SET error_message = 'dbt run failed — check Airflow task log for details' "
                        f"WHERE is_built = false AND error_message IS NULL",
                    )
                if not success_cobs:
                    raise AirflowException(
                        f"ALL {len(dbt_outputs)} dbt tasks failed. "
                        "No COBs were built. Check dbt_run task logs for errors."
                    )
                logging.info(f"[update] Done: {len(success_cobs)} success, {failed_count} failed")

        else:  # per_cob
            @task(trigger_rule=TriggerRule.ALL_DONE)
            def update_etl_model_logs(cob):
                pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                logging.info(f"[update] SUCCESS: {cob}")
                pg_hook.run(
                    f"UPDATE {model_log_table} SET is_built = true, built_at = NOW() WHERE report_date = %s",
                    parameters=(cob,),
                )

        # ─── Wire flow ────────────────────────────────────────────────────────

        ready_cobs = detect_ready_cobs()
        formatted_cobs = insert_etl_model_logs(ready_cobs)
        dbt_tasks = dbt_run.expand(params=formatted_cobs)
        notify = notify_teams()
        ready_cobs >> notify

        if update_mode == "bulk":
            if has_refresh:
                formatted_cobs >> refresh_metadata >> dbt_tasks
            else:
                formatted_cobs >> dbt_tasks
            update = update_etl_model_logs(dbt_tasks.output)
            update >> notify
        else:  # per_cob
            formatted_cobs >> dbt_tasks
            update_tasks = update_etl_model_logs.expand(cob=formatted_cobs.map(lambda x: x["cob"]))
            dbt_tasks >> update_tasks >> notify

    return dag


# ─── Register all DAGs at module level (required for Airflow DagBag) ─────────

for _cfg in DOMAIN_CONFIGS:
    globals()[_cfg["dag_id"]] = make_model_dag(_cfg)
