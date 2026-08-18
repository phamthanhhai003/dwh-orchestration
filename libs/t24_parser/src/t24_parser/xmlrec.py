"""Pure-python T24 XMLRECORD tokenizer. No Spark imports here.

Dialect (validated against 2,000 real BNCTL records, 2026-06-11):

    <row xml:space="preserve" id="RECID">
      <c1>value</c1>                      field position 1, mv=1, sv=1
      <c99 m="2">value</c99>              multi-value index 2
      <c179 m="210" s="2">value</c179>    multi-value 210, sub-value 2
      <c100 m="4"/>                       empty value
    </row>

Missing m/s attributes default to 1. Values keep their padding
(xml:space="preserve") - trimming is a *typing* concern, not a tokenizing
concern, so it happens in metadata/core, not here.
"""

from __future__ import annotations

import re
from typing import Iterator, NamedTuple
from xml.etree import ElementTree as ET

_C_TAG = re.compile(r"^c(\d+)$")


class Entry(NamedTuple):
    """One value cell of an XMLRECORD: field position + mv/sv indexes."""

    pos: int
    mv: int
    sv: int
    value: str  # raw, un-trimmed ('' for empty elements)


def iter_entries(xmlrecord: str) -> Iterator[Entry]:
    """Tokenize one XMLRECORD string into Entry tuples.

    Raises xml.etree.ElementTree.ParseError on malformed XML - callers on
    the hot path (Spark UDF) catch it and route the record to DLQ.
    """
    root = ET.fromstring(xmlrecord)
    for el in root:
        m = _C_TAG.match(el.tag)
        if not m:
            continue  # ignore non-cN elements defensively
        yield Entry(
            pos=int(m.group(1)),
            mv=int(el.get("m", 1)),
            sv=int(el.get("s", 1)),
            value=el.text or "",
        )


def to_dict(xmlrecord: str) -> dict[tuple[int, int, int], str]:
    """Convenience: {(pos, mv, sv): value}. Used by metadata.py and tests."""
    return {(e.pos, e.mv, e.sv): e.value for e in iter_entries(xmlrecord)}
