"""
PySpark JDBC script — pull FBNK_ACCOUNT from MSSQL (testdb) → Iceberg on MinIO
"""

import os

from pyspark.sql import SparkSession

MSSQL_HOST = "127.0.0.1"
MSSQL_PORT = 1433
MSSQL_DB   = "testdb"
MSSQL_USER = "sa"
MSSQL_PASS = os.environ["MSSQL_PASS"]
TABLE      = "dbo.FBNK_ACCOUNT"

MINIO_ENDPOINT   = "https://bnctl-dwh-api-minio.digitaltransformationdemo.com"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

HIVE_METASTORE   = "thrift://10.0.40.120:30083"
ICEBERG_TABLE    = "hive.bronze.fbnk_account"

JDBC_URL = (
    f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};"
    f"databaseName={MSSQL_DB};"
    f"encrypt=false;"
    f"trustServerCertificate=true"
)


def main():
    spark = (
        SparkSession.builder
        .appName("fbnk_account_jdbc_to_iceberg")
        .master("local[*]")
        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Hive catalog
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive.type", "hive")
        .config("spark.sql.catalog.hive.uri", HIVE_METASTORE)
        .config("spark.sql.catalog.hive.warehouse", "s3a://raw")
        .config("spark.sql.catalog.hive.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.catalog.hive.client.region", "us-east-1")
        # S3A hadoop
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TABLE)
        .option("user", MSSQL_USER)
        .option("password", MSSQL_PASS)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    print(f"[INFO] Loaded {df.count()} rows from {TABLE}")
    df.printSchema()

    spark.sql("CREATE DATABASE IF NOT EXISTS hive.bronze")
    spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_TABLE}")

    df.writeTo(ICEBERG_TABLE) \
        .using("iceberg") \
        .tableProperty("location", "s3a://raw/fbnk_account") \
        .create()
    print(f"[INFO] Written to Iceberg table {ICEBERG_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()
