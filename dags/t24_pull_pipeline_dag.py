"""T24 JDBC Pull Pipeline — full snapshot per table, dynamic từ TABLES.

Thêm bảng: append vào TABLES, tự ra 2 task extract + parse.
Điều kiện: bảng tồn tại trong testdb + SS key có trong ss_full.json.
Manual trigger only — dùng cho initial load / recovery.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

SPARK_NS  = Variable.get("SPARK_NAMESPACE", default_var="spark2-development-ns")
SPARK_IMG = "congtvjits/spark-t24:v3"
MSSQL_URL = Variable.get("T24_MSSQL_URL", default_var="jdbc:sqlserver://mssql.kafka-development-ns.svc.cluster.local:1433;databaseName=testdb;encrypt=false")
SS_PATH   = "s3a://spark-scripts/t24/ss_full.json"

# ── THÊM BẢNG Ở ĐÂY ──────────────────────────────────────────────────────────
TABLES = [
    "FBNK_INDUSTRY",
    "FBNK_SECTOR",
    "FBNK_AA_ARR_TERM_AMOUNT",      # AA.ARR.TERM.AMOUNT
    "FBNK_EM_LO_APPLICATION",       # EM.LO.APPLICATION
    "FBNK_AA_ARR_CUSTOMER",         # AA.ARR.CUSTOMER
    "FBNK_ACCT_STMT_PRINT",         # ACCT.STMT.PRINT
    "AAFBNK_AA508",                 # AA.SIMULATION.RUNNER
    "FBNK_ACCOUNT_CLOSED",          # ACCOUNT.CLOSED
    "FBNK_CUSTOMER_ACCOUNT",        # CUSTOMER.ACCOUNT
    "F_ACCOUNT_CONCAT",             # ACCOUNT.CONCAT
    "FBNK_MCB_GROUP",               # MCB.GROUP
    "FBNK_TELLER",                  # TELLER
    "F_AA_CUSTOMER_ROLE",           # AA.CUSTOMER.ROLE
    "FBNK_AA_PRODUCT",              # AA.PRODUCT
    "ACFBNK_AC009",                 # AC.ACCOUNT.SWEEP.HIST
    "F_AC_STMT_PARAMETER",          # AC.STMT.PARAMETER
    "FBNK_AC_SUB_ACCOUNT",          # AC.SUB.ACCOUNT
    "FBNK_ACCT_ENT_TODAY",          # ACCT.ENT.TODAY
    "F_ARCHIVE",                    # ARCHIVE
    "F_BNCTL_CUS_DIST",             # BNCTL.CUS.DIST
    "F_BNCTL_CUS_SUBDIST",          # BNCTL.CUS.SUBDIST
    "F_BNCTL_CUS_VILL",             # BNCTL.CUS.VILL
    "F_CATEGORY",                   # CATEGORY
    "F_COMPANY",                    # COMPANY
    "F_COMPANY_SMS_GROUP",          # COMPANY.SMS.GROUP
    "FBNK_CURRENCY",                # CURRENCY
    "F_DEPT_ACCT_OFFICER",          # DEPT.ACCT.OFFICER
    "F_EB_LOOKUP",                  # EB.LOOKUP
    "F_EB_SYSTEM_ID",               # EB.SYSTEM.ID
    "FBNK_EM_DL_APPLICATION",       # EM.DL.APPLICATION
    "F_GIC_ID",                     # GIC.ID
    "F_MNEMONIC_COMPANY",           # MNEMONIC.COMPANY
    "F_POSTING_RESTRICT",           # POSTING.RESTRICT
    "FBNK_TRANSACTION",             # TRANSACTION
    "F_USER", 
    # "FBNK_CATEGORY",
    # "FBNK_FUNDS_TRANSFER",
]
# ─────────────────────────────────────────────────────────────────────────────


def _derive(name: str) -> tuple[str, str]:
    """(ss_key, bronze_table) from MSSQL table name. Strip FBNK_/F_ prefix."""
    for prefix in ("FBNK_", "F_"):
        if name.startswith(prefix):
            suffix = name[len(prefix):]
            break
    else:
        suffix = name
    ss     = suffix.replace("_", ".")
    bronze = f"hive.bronze.t24_{suffix.lower()}"
    return ss, bronze

_MSSQL_ENV = """
          - name: MSSQL_PASSWORD
            valueFrom:
              secretKeyRef:
                name: mssql-sa-credentials
                key: MSSQL_PASSWORD"""

_MINIO_ENV = """
          - name: AWS_ACCESS_KEY_ID
            valueFrom:
              secretKeyRef:
                name: minio-credentials
                key: AWS_ACCESS_KEY_ID
          - name: AWS_SECRET_ACCESS_KEY
            valueFrom:
              secretKeyRef:
                name: minio-credentials
                key: AWS_SECRET_ACCESS_KEY
          - name: AWS_REGION
            valueFrom:
              secretKeyRef:
                name: minio-credentials
                key: AWS_REGION"""

_SPARK_CONF = """
    "spark.driver.extraClassPath": "/opt/spark/jars/*"
    "spark.executor.extraClassPath": "/opt/spark/jars/*"
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.hive": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.hive.type": "hive"
    "spark.sql.catalog.hive.uri": "{{ var.value.HIVE_METASTORE_URI }}"
    "spark.sql.catalog.hive.warehouse": "s3a://hive-warehouse"
    "spark.sql.defaultCatalog": "hive"
    "spark.sql.catalog.hive.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    "spark.sql.catalog.hive.s3.endpoint": "{{ conn.minio_conn.extra_dejson.endpoint_url }}"
    "spark.sql.catalog.hive.s3.path-style-access": "true"
    "spark.sql.catalog.hive.s3.access-key-id": "{{ conn.minio_conn.login }}"
    "spark.sql.catalog.hive.s3.secret-access-key": "{{ conn.minio_conn.password }}"
    "spark.hadoop.fs.s3a.endpoint": "{{ conn.minio_conn.extra_dejson.endpoint_url }}"
    "spark.hadoop.fs.s3a.access.key": "{{ conn.minio_conn.login }}"
    "spark.hadoop.fs.s3a.secret.key": "{{ conn.minio_conn.password }}"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.sql.shuffle.partitions": "4" """


def _make_app(job_name: str, script: str, args: list[str],
              driver_mem: str = "1g", exec_mem: str = "1g",
              exec_cores: int = 1, driver_env: str = "") -> str:
    args_yaml = "\n".join(f'    - "{a}"' for a in args)
    return f"""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: {job_name}-{{{{ dag_run.run_id.split('_')[-1] | lower }}}}
  namespace: {SPARK_NS}
spec:
  type: Python
  mode: cluster
  image: "{SPARK_IMG}"
  imagePullPolicy: IfNotPresent
  pythonVersion: "3"
  mainApplicationFile: "local:///opt/jobs/{script}"
  arguments:
{args_yaml}
  sparkVersion: "3.5.1"
  sparkConf:
{_SPARK_CONF}
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "{driver_mem}"
    serviceAccount: spark-sa
    env:{driver_env}
  executor:
    cores: {exec_cores}
    instances: 1
    memory: "{exec_mem}"
    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-credentials
            key: AWS_ACCESS_KEY_ID
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-credentials
            key: AWS_SECRET_ACCESS_KEY
      - name: AWS_REGION
        valueFrom:
          secretKeyRef:
            name: minio-credentials
            key: AWS_REGION
  restartPolicy:
    type: Never
"""


def _extract_app(name: str) -> str:
    slug = name.lower().replace("_", "-")
    raw  = f"s3a://raw/t24/{name}/{{{{ dag_run.run_after.strftime('%Y-%m-%d') }}}}/"
    return _make_app(
        job_name=f"t24-extract-{slug}",
        script="t24_jdbc_extract.py",
        args=[
            "--url",     MSSQL_URL,
            "--dbtable", f"dbo.{name}",
            "--user",    Variable.get("T24_MSSQL_USER", default_var="sa"),
            "--output",  raw,
            "--mode",    "full",
        ],
        driver_env=_MSSQL_ENV,
    )


def _parse_app(name: str) -> str:
    slug   = name.lower().replace("_", "-")
    ss, bronze = _derive(name)
    raw  = f"s3a://raw/t24/{name}/{{{{ dag_run.run_after.strftime('%Y-%m-%d') }}}}/"
    dlq  = f"s3a://checkpoint/t24-dlq/{name}/{{{{ dag_run.run_after.strftime('%Y-%m-%d') }}}}/"
    return _make_app(
        job_name=f"t24-parse-{slug}",
        script="t24_batch_parse.py",
        args=[
            "--table",  ss,
            "--input",  raw,
            "--ss",     SS_PATH,
            "--target", bronze,
            "--dlq",    dlq,
        ],
        driver_mem="2g",
        exec_mem="2g",
        exec_cores=2,
        driver_env=_MINIO_ENV,
    )


with DAG(
    dag_id="t24_pull_pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["t24", "pull", "jdbc"],
) as dag:

    trigger = EmptyOperator(task_id="jdbc_pull_data")

    for name in TABLES:
        extract = SparkKubernetesOperator(
            task_id=f"extract_{name}",
            namespace=SPARK_NS,
            application_file=_extract_app(name),
            kubernetes_conn_id="k8s",
            do_xcom_push=False,
            delete_on_termination=False,
            pool="t24_pulling",
            pool_slots=1,
        )
        parse = SparkKubernetesOperator(
            task_id=f"parse_{name}",
            namespace=SPARK_NS,
            application_file=_parse_app(name),
            kubernetes_conn_id="k8s",
            do_xcom_push=False,
            delete_on_termination=False,
            pool="t24_parsing",
            pool_slots=1,
        )
        trigger >> extract >> parse
