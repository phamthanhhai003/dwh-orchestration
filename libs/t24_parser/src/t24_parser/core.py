"""Spark parse core: DataFrame(RECID, XMLRECORD) -> ONE wide typed frame.

Pure transform - no I/O, no streaming/batch awareness. The exact same
function runs in both paths:

    batch:      parse(spark.read..., spec)
    streaming:  stream.writeStream.foreachBatch(lambda df, _: sink(parse(df, spec)))

Output (ParsedFrames):
    main     1 row per RECID:
               - recid + passthrough columns
               - single-value fields  -> typed scalar columns
               - LOCAL.REF fields     -> typed scalar columns (named per SS)
               - multi-value fields   -> typed ARRAY columns, DENSE by mv_no
                 (element i = value at mv = i+1; null where the mv index is
                 absent - T24 associated multi-values align by index, and
                 sparse mv sets are common, so padding is required)
    sv_long  rare sub-value cells (sv > 1, mostly inside LOCAL.REF):
               (recid, field, pos, mv_no, sv_no, value) - lossless spillover
    errors   records whose XMLRECORD failed to tokenize (route to DLQ)

Everything is built with Spark higher-order functions on the tokenized
entry array - no explode/groupBy shuffle on the main path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
from xml.etree.ElementTree import ParseError

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import (ArrayType, IntegerType, StringType, StructField,
                               StructType)

from .metadata import TYPE_DATE, TYPE_DECIMAL, FieldDef, TableSpec
from .xmlrec import iter_entries

ENTRY_SCHEMA = ArrayType(StructType([
    StructField("pos", IntegerType(), False),
    StructField("mv", IntegerType(), False),
    StructField("sv", IntegerType(), False),
    StructField("value", StringType(), True),
]))

DECIMAL_TYPE = "decimal(20,4)"
DATE_FMT = "yyyyMMdd"
_SQL_TYPE = {TYPE_DATE: "date", TYPE_DECIMAL: DECIMAL_TYPE, "string": "string"}


@F.udf(returnType=ENTRY_SCHEMA)
def _tokenize(xml: str):
    """XMLRECORD -> [(pos, mv, sv, value)]; None signals a malformed record."""
    if xml is None:
        return None
    try:
        return [tuple(e) for e in iter_entries(xml)]
    except ParseError:
        return None


@dataclass
class ParsedFrames:
    main: DataFrame
    sv_long: DataFrame
    errors: DataFrame


def _typed_sql(v: str, dtype: str) -> str:
    """SQL snippet: trim + ''->null + cast according to IN2-derived type."""
    base = f"nullif(trim({v}), '')"
    if dtype == TYPE_DATE:
        return f"to_date({base}, '{DATE_FMT}')"
    if dtype == TYPE_DECIMAL:
        return f"cast({base} as {DECIMAL_TYPE})"
    return base


def _scalar_sql(pos: int, mv: int, dtype: str) -> str:
    """First matching cell (pos, mv, sv=1) -> typed scalar."""
    cell = (f"element_at(filter(__entries, "
            f"e -> e.pos = {pos} AND e.mv = {mv} AND e.sv = 1), 1).value")
    return _typed_sql(cell, dtype)


def _array_sql(pos: int, dtype: str) -> str:
    """All sv=1 cells of a multi-value field -> dense typed array by mv_no."""
    m = (f"map_from_entries(transform(filter(__entries, "
         f"e -> e.pos = {pos} AND e.sv = 1), e -> struct(e.mv, e.value)))")
    elem = _typed_sql(f"{m}[i]", dtype)
    return (f"CASE WHEN size({m}) = 0 THEN cast(null as array<{_SQL_TYPE[dtype]}>) "
            f"ELSE transform(sequence(1, array_max(map_keys({m}))), i -> {elem}) END")


def _name_case(pos_col: Column, fields: Sequence[FieldDef], attr: str) -> Column:
    expr = F.lit(None).cast(StringType())
    for f in fields:
        expr = F.when(pos_col == f.pos, F.lit(getattr(f, attr))).otherwise(expr)
    return expr


def parse(df: DataFrame, spec: TableSpec,
          recid_col: str = "RECID", xml_col: str = "XMLRECORD",
          passthrough: Sequence[str] = ()) -> ParsedFrames:
    """Parse a static DataFrame of (RECID, XMLRECORD [, passthrough...])."""
    pt = list(passthrough)
    base = df.select(
        F.col(recid_col).alias("recid"),
        _tokenize(F.col(xml_col)).alias("__entries"),
        *[F.col(c) for c in pt],
    )
    errors = base.filter(F.col("__entries").isNull()).drop("__entries")
    ok = base.filter(F.col("__entries").isNotNull())

    lref = spec.local_ref_pos

    cols: list[Column] = [F.col("recid"), *[F.col(c) for c in pt]]
    used = {"recid", *pt}
    for f in spec.single_fields:
        cols.append(F.expr(_scalar_sql(f.pos, 1, f.dtype)).alias(f.column))
        used.add(f.column)
    if lref is not None:
        for lf in spec.locals:
            col = lf.column if lf.column not in used else f"lr_{lf.column}"
            used.add(col)
            cols.append(F.expr(_scalar_sql(lref, lf.lref_mv, lf.dtype)).alias(col))
    for f in spec.mv_fields:
        col = f.column if f.column not in used else f"{f.column}_mv"
        used.add(col)
        cols.append(F.expr(_array_sql(f.pos, f.dtype)).alias(col))
    main = ok.select(*cols)

    # --- sub-value spillover (rare; keeps the parse lossless) ---
    sv = (ok.select("recid", *pt,
                    F.explode(F.expr("filter(__entries, e -> e.sv > 1)")).alias("e"))
            .select("recid", *pt,
                    _name_case(F.col("e.pos"),
                               spec.fields, "name").alias("field"),
                    F.col("e.pos").alias("pos"),
                    F.col("e.mv").alias("mv_no"), F.col("e.sv").alias("sv_no"),
                    F.nullif(F.trim("e.value"), F.lit("")).alias("value")))

    return ParsedFrames(main=main, sv_long=sv, errors=errors)
