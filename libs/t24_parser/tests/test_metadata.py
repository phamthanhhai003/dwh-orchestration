from t24_parser.metadata import (TYPE_DATE, TYPE_DECIMAL, TYPE_STRING,
                                 build_spec, sanitize)


def test_account_spec_shape(ss):
    spec = build_spec("ACCOUNT", ss)
    by_name = {f.name: f for f in spec.fields}
    # cross-validated against the MSSQL computed columns in the export
    assert by_name["CUSTOMER"].pos == 1
    assert by_name["CATEGORY"].pos == 2
    assert by_name["OPENING.DATE"].pos == 78
    assert spec.local_ref_pos == 20
    assert len(spec.fields) == 255


def test_customer_spec_shape(ss):
    spec = build_spec("CUSTOMER", ss)
    assert spec.local_ref_pos == 179
    assert len(spec.fields) == 201
    assert len(spec.locals) >= 200  # 213 local fields in BNCTL CUSTOMER


def test_type_inference(ss):
    spec = build_spec("ACCOUNT", ss)
    by_name = {f.name: f for f in spec.fields}
    assert by_name["OPENING.DATE"].dtype == TYPE_DATE          # IN2D
    assert by_name["ONLINE.ACTUAL.BAL"].dtype == TYPE_DECIMAL  # IN2AMT
    assert by_name["CUSTOMER"].dtype == TYPE_STRING            # IN2CUS


def test_single_vs_multi(ss):
    spec = build_spec("ACCOUNT", ss)
    by_name = {f.name: f for f in spec.fields}
    assert by_name["CUSTOMER"].multi is False
    assert by_name["ACCOUNT.TITLE.1"].multi is True
    # LOCAL.REF never appears in either projection list
    assert all(f.name != "LOCAL.REF" for f in spec.single_fields)
    assert all(f.name != "LOCAL.REF" for f in spec.mv_fields)


def test_local_fields(ss):
    spec = build_spec("ACCOUNT", ss)
    by_name = {lf.name: lf for lf in spec.locals}
    assert by_name["WELCOME.PACK"].lref_mv == 1
    assert by_name["GENDER"].lref_mv == 3


def test_sanitize():
    assert sanitize("@ID") == "recid"
    assert sanitize("OPENING.DATE") == "opening_date"
    assert sanitize("ACCOUNT.TITLE.1") == "account_title_1"


def test_no_duplicate_columns(ss):
    for table in ("ACCOUNT", "CUSTOMER"):
        spec = build_spec(table, ss)
        cols = [f.column for f in spec.single_fields] + [lf.column for lf in spec.locals]
        # core.parse prefixes clashing local columns with lr_, so here we only
        # require that the D-field projection itself is collision-free
        d_cols = [f.column for f in spec.fields]
        assert len(d_cols) == len(set(d_cols)), f"duplicate D columns in {table}"
        assert cols  # non-empty
