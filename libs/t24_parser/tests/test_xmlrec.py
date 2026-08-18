import pytest
from xml.etree.ElementTree import ParseError

from t24_parser.xmlrec import Entry, iter_entries, to_dict

XML = ('<row xml:space="preserve" id="X1">'
       '<c1>A</c1>'
       '<c2 m="2">B</c2>'
       '<c3 m="4" s="2">C</c3>'
       '<c4 m="3"/>'
       '<c5>  padded  </c5>'
       '</row>')


def test_defaults_and_attrs():
    entries = list(iter_entries(XML))
    assert Entry(1, 1, 1, "A") in entries
    assert Entry(2, 2, 1, "B") in entries
    assert Entry(3, 4, 2, "C") in entries


def test_empty_element_is_empty_string():
    assert to_dict(XML)[(4, 3, 1)] == ""


def test_padding_preserved():
    # trimming is the typing layer's job, not the tokenizer's
    assert to_dict(XML)[(5, 1, 1)] == "  padded  "


def test_malformed_raises():
    with pytest.raises(ParseError):
        list(iter_entries("<row><c1>unclosed</row>"))


def test_real_records_tokenize(account_rows, customer_rows):
    for row in account_rows + customer_rows:
        entries = list(iter_entries(row["XMLRECORD"]))
        assert entries, f"no entries for {row['RECID']}"
        ids = [(e.pos, e.mv, e.sv) for e in entries]
        assert len(ids) == len(set(ids)), f"duplicate cells in {row['RECID']}"
