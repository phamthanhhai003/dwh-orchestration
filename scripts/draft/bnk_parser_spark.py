from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType
from datetime import date
import os

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

spark = (
    SparkSession.builder
    .appName("BNK Parser")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

RAW_BASE    = "s3a://raw/bnk"
BRONZE_BASE = "s3a://bronze/bnk"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_ingestion_date() -> str:
    """Tính lúc runtime, không phải lúc import."""
    return str(date.today())


def read_bnk_csv(path: str):
    return (
        spark.read
        .option("encoding", "UTF-16")
        .option("sep", "~")
        .option("header", "true")
        .option("inferSchema", "false")
        .format("csv")
        .load(path)
    )


def clean_columns(df):
    renamed = {c: c.strip().lstrip("@").strip("\ufeff\r\x00\ufffd") for c in df.columns}
    for old, new in renamed.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.trim(F.regexp_replace(F.col(f"`{col_name}`"), r"[\ufeff\r\x00]", ""))
        )
    df = df.replace("", None)
    df = df.dropna(how="all")
    return df


def add_metadata(df, source_file: str):
    return (
        df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_date", F.lit(get_ingestion_date()).cast(DateType()))
    )


def cast_yyyymmdd(df, *cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_date(F.col(c), "yyyyMMdd"))
    return df


def cast_decimal(df, *cols, precision=20, scale=4):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(DecimalType(precision, scale)))
    return df


def write_bronze_csv(df, source_name: str):
    """
    Ghi lên MinIO bằng Spark native — không kéo data về driver.
    Output: s3a://bronze/bnk/{source_name}_parsed.csv/part-*.csv
    """
    out_path = f"{BRONZE_BASE}/{source_name}_parsed.csv"
    n = df.count()
    print(f"\n>>> Writing {n} rows → {out_path}")

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(out_path)
    )
    print(f">>> Done: {out_path}")


def show_demo(df, table_name: str, demo_cols: list, n: int = 5):
    print(f"\n{'='*70}")
    print(f"  DEMO OUTPUT — {table_name}  ({df.count()} rows total)")
    print(f"{'='*70}")
    df.select(*demo_cols).show(n, truncate=40)


# ---------------------------------------------------------------------------
# BNK_ACCOUNT
# ---------------------------------------------------------------------------

def parse_bnk_account():
    df = read_bnk_csv(f"{RAW_BASE}/BNK_ACCOUNT.csv")
    df = clean_columns(df)
    df = df.filter(F.col("ID").isNotNull() & F.col("ID").rlike(r"^\d+$"))
    df = cast_yyyymmdd(df,
        "MIS_DATE", "OPENING_DATE", "VALUE_DATE", "CLOSURE_DATE",
        "DATE_LAST_CR_CUST", "DATE_LAST_CR_AUTO", "DATE_LAST_CR_BANK",
        "DATE_LAST_DR_CUST", "DATE_LAST_DR_AUTO", "DATE_LAST_DR_BANK",
        "CAP_DATE_CHARGE", "CAP_DATE_CR_INT", "CAP_DATE_DR_INT",
        "FROM_DATE", "AVAILABLE_DATE", "DATE_LAST_UPDATE", "NEXT_STMT_DATE",
    )
    df = cast_decimal(df,
        "OPEN_ACTUAL_BAL", "OPEN_CLEARED_BAL",
        "ONLINE_ACTUAL_BAL", "ONLINE_CLEARED_BAL", "WORKING_BALANCE",
        "AMNT_LAST_CR_CUST", "AMNT_LAST_DR_CUST",
        "START_YEAR_BAL", "CREDIT_MOVEMENT", "DEBIT_MOVEMENT",
        "VALUE_DATED_BAL", "OPEN_AVAILABLE_BAL", "AVAILABLE_BAL",
    )
    df = add_metadata(df, "BNK_ACCOUNT.csv")
    show_demo(df, "BNK_ACCOUNT", [
        "ID", "MIS_DATE", "CUSTOMER", "ACCOUNT_TITLE_1",
        "CURRENCY", "OPENING_DATE", "WORKING_BALANCE",
        "ONLINE_ACTUAL_BAL", "CREDIT_MOVEMENT", "DEBIT_MOVEMENT",
        "_ingestion_date",
    ])
    write_bronze_csv(df, "BNK_ACCOUNT")


# ---------------------------------------------------------------------------
# BNK_CUSTOMER
# ---------------------------------------------------------------------------

def parse_bnk_customer():
    df = read_bnk_csv(f"{RAW_BASE}/BNK_CUSTOMER.csv")
    df = clean_columns(df)
    df = df.filter(F.col("ID").isNotNull() & F.col("ID").rlike(r"^\d+$"))
    df = cast_yyyymmdd(df,
        "MIS_DATE", "DATE_OF_BIRTH", "BIRTH_INCORP_DATE",
        "LEGAL_ISS_DATE", "LEGAL_EXP_DATE", "CONTACT_DATE",
        "EMPLOYMENT_START", "CUSTOMER_SINCE", "DATE_LAST_VERIFIED",
        "LAST_KYC_REVIEW_DATE", "LAST_AML_RESULT_DATE", "EXIT_DATE",
    )
    df = cast_decimal(df,
        "SALARY", "ANNUAL_BONUS",
        "NET_MONTHLY_IN", "NET_MONTHLY_OUT",
        "RESIDENCE_VALUE", "MORTGAGE_AMT",
    )
    df = add_metadata(df, "BNK_CUSTOMER.csv")
    show_demo(df, "BNK_CUSTOMER", [
        "ID", "MIS_DATE", "SHORT_NAME", "NAME_1",
        "GENDER", "DATE_OF_BIRTH", "NATIONALITY",
        "CUSTOMER_STATUS", "EMPLOYMENT_STATUS", "SALARY",
        "_ingestion_date",
    ])
    write_bronze_csv(df, "BNK_CUSTOMER")


# ---------------------------------------------------------------------------
# BNK_AML_TXN_ENTRY
# ---------------------------------------------------------------------------

def parse_bnk_aml_txn_entry():
    df = read_bnk_csv(f"{RAW_BASE}/BNK_AML_TXN_ENTRY.csv")
    df = clean_columns(df)
    df = df.filter(F.col("transactionID").isNotNull())
    df = cast_yyyymmdd(df, "operationDate", "valueDate")
    df = cast_decimal(df, "amountCurrency", "amountRefCurrency")
    df = add_metadata(df, "BNK_AML_TXN_ENTRY.csv")
    show_demo(df, "BNK_AML_TXN_ENTRY", [
        "branch", "transactionID", "operationDate",
        "currency", "amountCurrency", "amountRefCurrency",
        "accountID", "debitCreditInd", "customerName",
        "valueDate", "_ingestion_date",
    ])
    write_bronze_csv(df, "BNK_AML_TXN_ENTRY")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== [1/3] BNK_ACCOUNT ===")
    parse_bnk_account()

    print("\n=== [2/3] BNK_CUSTOMER ===")
    parse_bnk_customer()

    print("\n=== [3/3] BNK_AML_TXN_ENTRY ===")
    parse_bnk_aml_txn_entry()

    spark.stop()
    print("\n✓ All BNK files processed successfully.")