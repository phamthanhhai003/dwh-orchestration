"""STANDARD.SELECTION -> TableSpec. Pure python, runs on the driver.

STANDARD.SELECTION is itself stored as XMLRECORD in the same dialect.
Each mv index of the record describes one field of the target table.

Empirically decoded layout (validated 2026-06-11 against BNCTL export):

    c1  SYS.FIELD.NAME   field name ('@ID' = RECID)
    c2  SYS.TYPE         'D' = real data field (parse it);
                         'I'/'C'/... = computed/join expression (skip)
    c3  SYS.FIELD.NO     position N -> <cN> in data XMLRECORD; '0' = @ID
    c4  SYS.VAL.PROG     IN2 conversion -> type inference:
                         IN2D = date yyyyMMdd, IN2AMT = decimal amount,
                         anything else = string (v0.1 - numeric TODO)
    c10 SYS.SINGLE.MULT  'S' single / 'M' multi-value
    --- USR (local/custom field) block, own mv numbering ---
    c15 USR.FIELD.NAME   e.g. 'WELCOME.PACK'
    c16 USR.TYPE         'I'
    c17 USR expression   'LOCAL.REF<1,N>' -> value lives at LOCAL.REF mv=N
    c18 USR.VAL.PROG     IN2 conversion for the local field
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

from .xmlrec import to_dict

_LOCAL_REF_EXPR = re.compile(r"^LOCAL\.REF<1,(\d+)>$")

# IN2 conversion prefix -> logical type. v0.1 keeps everything else string;
# numeric detection (plain IN2/IN2R masks) is a deliberate later step.
TYPE_DATE = "date"
TYPE_DECIMAL = "decimal"
TYPE_STRING = "string"


def in2_to_type(conv: str) -> str:
    head = conv.split("&", 1)[0] if conv else ""
    if head == "IN2D":
        return TYPE_DATE
    if head == "IN2AMT":
        return TYPE_DECIMAL
    return TYPE_STRING


def sanitize(name: str) -> str:
    """T24 field name -> Bronze column name. 'OPENING.DATE' -> 'opening_date'."""
    if name == "@ID":
        return "recid"
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


@dataclass(frozen=True)
class FieldDef:
    name: str          # T24 name, e.g. 'OPENING.DATE'
    pos: int           # -> <cN>
    dtype: str         # TYPE_DATE / TYPE_DECIMAL / TYPE_STRING
    multi: bool        # c10 == 'M'

    @property
    def column(self) -> str:
        return sanitize(self.name)


@dataclass(frozen=True)
class LocalFieldDef:
    name: str          # e.g. 'WELCOME.PACK'
    lref_mv: int       # N in LOCAL.REF<1,N> -> mv index inside the LOCAL.REF field
    dtype: str

    @property
    def column(self) -> str:
        return sanitize(self.name)


@dataclass
class TableSpec:
    table: str                          # SS RECID, e.g. 'ACCOUNT'
    fields: list[FieldDef] = field(default_factory=list)
    locals: list[LocalFieldDef] = field(default_factory=list)

    @property
    def single_fields(self) -> list[FieldDef]:
        return [f for f in self.fields if not f.multi and f.name != "LOCAL.REF"]

    @property
    def mv_fields(self) -> list[FieldDef]:
        return [f for f in self.fields if f.multi and f.name != "LOCAL.REF"]

    @property
    def local_ref_pos(self) -> Optional[int]:
        for f in self.fields:
            if f.name == "LOCAL.REF":
                return f.pos
        return None

    def pos_to_field(self) -> dict[int, FieldDef]:
        return {f.pos: f for f in self.fields}


def load_standard_selection(path: Union[str, Path]) -> dict[str, str]:
    """Load the SS export (JSON array of {RECID, XMLRECORD}) -> {table: xml}."""
    with open(path, encoding="utf-8") as f:
        return {r["RECID"]: r["XMLRECORD"] for r in json.load(f)}


def build_spec(table: str, ss: dict[str, str]) -> TableSpec:
    """Compile the SS record of `table` into a TableSpec.

    Only SYS type-'D' fields with a numeric position >= 1 become FieldDefs -
    those are the cells that physically exist in the data XMLRECORD.
    """
    if table not in ss:
        raise KeyError(f"table {table!r} not found in STANDARD.SELECTION "
                       f"({len(ss)} tables loaded)")
    rec = to_dict(ss[table])

    def cell(pos: int, mv: int) -> str:
        return rec.get((pos, mv, 1), "")

    spec = TableSpec(table=table)

    sys_mvs = sorted({mv for (pos, mv, _sv) in rec if pos == 1})
    seen_cols: dict[str, int] = {}
    for mv in sys_mvs:
        name, typ, pos_s = cell(1, mv), cell(2, mv), cell(3, mv)
        if typ != "D" or not pos_s.isdigit() or int(pos_s) < 1:
            continue
        fd = FieldDef(
            name=name,
            pos=int(pos_s),
            dtype=in2_to_type(cell(4, mv)),
            multi=cell(10, mv) == "M",
        )
        # Duplicate positions exist in SS (aliases). First definition wins.
        if fd.pos in {f.pos for f in spec.fields}:
            continue
        if fd.column in seen_cols:  # name collision after sanitize -> suffix pos
            fd = FieldDef(f"{name}.P{fd.pos}", fd.pos, fd.dtype, fd.multi)
        seen_cols[fd.column] = fd.pos
        spec.fields.append(fd)

    usr_mvs = sorted({mv for (pos, mv, _sv) in rec if pos == 15})
    seen_locals: set[str] = set()
    for mv in usr_mvs:
        name, expr = cell(15, mv), cell(17, mv)
        m = _LOCAL_REF_EXPR.match(expr or "")
        if not name or not m:
            continue
        lf = LocalFieldDef(name=name, lref_mv=int(m.group(1)),
                           dtype=in2_to_type(cell(18, mv)))
        if lf.column in seen_locals:
            continue
        seen_locals.add(lf.column)
        spec.locals.append(lf)

    return spec
