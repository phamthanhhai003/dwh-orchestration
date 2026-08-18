"""Ad-hoc CLI - works WITHOUT Spark for metadata commands.

    python -m t24_parser tables  --ss F_STANDARD_SELECTION_.json
    python -m t24_parser inspect ACCOUNT --ss ...   # field/type/MV summary
    python -m t24_parser schema  ACCOUNT --ss ...   # Bronze main schema

Spark-needing commands (parse a sample) live in jobs/, not here.
"""

from __future__ import annotations

import argparse
import sys

from .metadata import build_spec, load_standard_selection


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="t24_parser")
    p.add_argument("--ss", required=True, help="path to STANDARD.SELECTION JSON export")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("tables")
    for c in ("inspect", "schema"):
        sp = sub.add_parser(c)
        sp.add_argument("table")
    args = p.parse_args(argv)

    ss = load_standard_selection(args.ss)

    if args.cmd == "tables":
        for t in sorted(ss):
            print(t)
        return 0

    spec = build_spec(args.table, ss)
    if args.cmd == "inspect":
        print(f"table {spec.table}: {len(spec.fields)} D-fields "
              f"({len(spec.single_fields)} single, {len(spec.mv_fields)} multi), "
              f"LOCAL.REF pos={spec.local_ref_pos}, {len(spec.locals)} local fields")
        print("\n-- single-value --")
        for f in spec.single_fields:
            print(f"  c{f.pos:<4} {f.name:<35} {f.dtype}")
        print("\n-- multi-value (-> array column in main) --")
        for f in spec.mv_fields:
            print(f"  c{f.pos:<4} {f.name:<35} {f.dtype}")
        print("\n-- local fields (LOCAL.REF) --")
        for lf in spec.locals:
            print(f"  mv{lf.lref_mv:<4} {lf.name:<35} {lf.dtype}")
    elif args.cmd == "schema":
        # mirrors core.parse() column order and collision rules
        used = {"recid"}
        print("recid string")
        for f in spec.single_fields:
            print(f"{f.column} {f.dtype}")
            used.add(f.column)
        if spec.local_ref_pos is not None:
            for lf in spec.locals:
                col = lf.column if lf.column not in used else f"lr_{lf.column}"
                used.add(col)
                print(f"{col} {lf.dtype}")
        for f in spec.mv_fields:
            col = f.column if f.column not in used else f"{f.column}_mv"
            used.add(col)
            print(f"{col} array<{f.dtype}>")
    return 0

