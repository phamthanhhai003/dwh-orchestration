"""t24_parser - metadata-driven T24 XMLRECORD parser.

Shared core for both sourcing paths of the DWH v1.4 architecture:
  - CDC streaming (Debezium -> Kafka -> Spark Structured Streaming, foreachBatch)
  - JDBC pull batch (Raw zone -> Bronze)

Layering:
  xmlrec.py    pure-python XMLRECORD tokenizer (no Spark) - unit-testable anywhere
  metadata.py  STANDARD.SELECTION -> TableSpec (pure python, driver side)
  core.py      Spark parse: DataFrame(RECID, XMLRECORD) -> main / mv / locals frames
  sources.py   input adapters (Kafka+Debezium envelope, JDBC, Raw zone)
  sinks.py     output adapters (Iceberg Bronze MERGE)
"""

__version__ = "0.1.0"

from .metadata import TableSpec, FieldDef, LocalFieldDef, load_standard_selection, build_spec  # noqa: F401
