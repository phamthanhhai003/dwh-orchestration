"""Spark integration tests - run inside the Spark image (locally needs java+pyspark).

    pytest -m spark
"""

import datetime
import decimal

import pytest

from t24_parser.metadata import build_spec

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def account_parsed(spark, ss, account_rows):
    from t24_parser.core import parse
    spec = build_spec("ACCOUNT", ss)
    df = spark.createDataFrame(
        [(r["RECID"], r["XMLRECORD"]) for r in account_rows],
        "RECID string, XMLRECORD string")
    return parse(df, spec), spec


def test_main_known_record(account_parsed):
    frames, _ = account_parsed
    row = frames.main.filter("recid = '00000002227508'").collect()[0]
    # values cross-checked by hand against the raw XMLRECORD sample
    assert row.customer == "30163001"                       # c1
    assert row.category == "3004"                           # c2
    assert row.opening_date == datetime.date(2019, 7, 11)   # c78, IN2D
    assert row.online_actual_bal == decimal.Decimal("-1159.8000")  # c25, IN2AMT
    assert row.currency == "USD"                            # c8


def test_main_one_row_per_recid(account_parsed, account_rows):
    frames, _ = account_parsed
    assert frames.main.count() == len(account_rows)
    assert frames.main.select("recid").distinct().count() == len(account_rows)


def test_trailing_padding_trimmed(account_parsed):
    frames, _ = account_parsed
    row = frames.main.filter("recid = '00000002227508'").collect()[0]
    # c42 = '861   ...' heavily right-padded in the raw record
    vals = [v for v in row.asDict().values() if isinstance(v, str)]
    assert not any(v != v.strip() for v in vals if v)


def test_mv_array_dense(account_parsed):
    frames, _ = account_parsed
    row = frames.main.filter("recid = '00000002227508'").collect()[0]
    # c99 = ALT.ACCT.TYPE, m=1..4 -> dense array in order
    assert row.alt_acct_type == ["LEGACY", "T24.IBAN", "PREV.IBAN", "WOF.LOAN"]
    # ACCOUNT.TITLE.1 (declared M, single in data) -> 1-element array
    assert row.account_title_1 == ["Business Loan"]
    assert row.short_title == ["Business Loan"]


def test_mv_array_padding(spark, ss):
    """Sparse mv set -> null-padded dense array (index = mv_no - 1)."""
    from t24_parser.core import parse
    spec = build_spec("ACCOUNT", ss)
    df = spark.createDataFrame(
        [("R1", '<row id="R1"><c3 m="2">B</c3><c3 m="4">D</c3></row>')],
        "RECID string, XMLRECORD string")
    row = parse(df, spec).main.collect()[0]
    assert row.account_title_1 == [None, "B", None, "D"]


def test_locals_in_main(spark, ss, customer_rows):
    from t24_parser.core import parse
    spec = build_spec("CUSTOMER", ss)
    df = spark.createDataFrame(
        [(r["RECID"], r["XMLRECORD"]) for r in customer_rows],
        "RECID string, XMLRECORD string")
    row = parse(df, spec).main.filter("recid = '10000001'").collect()[0]
    ini = getattr(row, "initials", None) or getattr(row, "lr_initials", None)
    assert ini == "XF"  # LOCAL.REF<1,2> = c179 m=2


def test_sv_spillover(spark, ss, customer_rows):
    """sv>1 cells (mostly inside LOCAL.REF) land in sv_long - lossless."""
    from t24_parser.core import parse
    spec = build_spec("CUSTOMER", ss)
    df = spark.createDataFrame(
        [(r["RECID"], r["XMLRECORD"]) for r in customer_rows],
        "RECID string, XMLRECORD string")
    sv = parse(df, spec).sv_long
    assert set(sv.columns) >= {"recid", "field", "pos", "mv_no", "sv_no", "value"}
    assert sv.filter("sv_no > 1").count() == sv.count()


def test_malformed_xml_goes_to_errors(spark, ss):
    from t24_parser.core import parse
    spec = build_spec("ACCOUNT", ss)
    df = spark.createDataFrame(
        [("GOOD1", '<row id="GOOD1"><c1>X</c1></row>'),
         ("BAD1", "<row><c1>unclosed")],
        "RECID string, XMLRECORD string")
    frames = parse(df, spec)
    assert frames.errors.count() == 1
    assert frames.errors.collect()[0].recid == "BAD1"
    assert frames.main.count() == 1


def test_cdc_passthrough(spark, ss):
    from t24_parser.core import parse
    spec = build_spec("ACCOUNT", ss)
    df = spark.createDataFrame(
        [("R1", '<row id="R1"><c1>C1</c1></row>', "u", "0000abcd")],
        "RECID string, XMLRECORD string, __op string, __lsn string")
    frames = parse(df, spec, passthrough=["__op", "__lsn"])
    row = frames.main.collect()[0]
    assert row.__op == "u" and row.__lsn == "0000abcd"
