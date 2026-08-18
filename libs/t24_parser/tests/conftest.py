import csv
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def ss():
    with open(FIXTURES / "ss_account_customer.json") as f:
        return {r["RECID"]: r["XMLRECORD"] for r in json.load(f)}


def _rows(name):
    with open(FIXTURES / name, newline="") as f:
        return list(csv.DictReader(f))


@pytest.fixture(scope="session")
def account_rows():
    return _rows("account_20.csv")


@pytest.fixture(scope="session")
def customer_rows():
    return _rows("customer_20.csv")


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]")
         .appName("t24-parser-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    yield s
    s.stop()
