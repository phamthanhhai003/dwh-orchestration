"""BCP bulk extract: MSSQL table → parquet chunks → MinIO (no local disk).

Stream pipeline:
    bcp stdout (TSV) → pyarrow chunked CSV reader → parquet BytesIO → MinIO upload

Memory per chunk ≈ CHUNK_ROWS × avg_row_size (~500 B) ≈ 250 MB at 500K rows.
No ephemeral disk needed — output goes straight to MinIO via boto3.

Image requirements (congtvjits/bcp-t24:v1):
    mssql-tools (bcp), python3, pyarrow, boto3

Usage:
    t24_bcp_extract.py \
        --server mssql.bnctl-kafka-development-ns.svc.cluster.local \
        --database testdb \
        --table dbo.FBNK_CATEG_ENTRY \
        --output s3a://raw/t24/FBNK_CATEG_ENTRY/2026-06-23/

Env:
    MSSQL_PASSWORD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    MINIO_ENDPOINT  (default: http://minio.bnctl-minio-development-ns.svc.cluster.local:9000)
    BCP_CHUNK_ROWS  (default: 500000)
"""
from __future__ import annotations

import subprocess
import sys

# Bootstrap pyarrow if missing (image may not have it until next rebuild)
try:
    import pyarrow  # noqa: F401
except ImportError:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "--target=/tmp/pyarrow_pkg", "pyarrow==17.0.0"],
        check=True,
    )
    sys.path.insert(0, "/tmp/pyarrow_pkg")

import argparse
import io
import os

import boto3
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

CHUNK_ROWS = int(os.environ.get("BCP_CHUNK_ROWS", "500000"))
_MINIO_ENDPOINT = os.environ.get(
    "MINIO_ENDPOINT",
    "http://minio.bnctl-minio-development-ns.svc.cluster.local:9000",
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_MINIO_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _parse_s3_url(url: str) -> tuple[str, str]:
    """s3a://bucket/prefix/ → (bucket, 'prefix/')"""
    for prefix in ("s3a://", "s3://"):
        if url.startswith(prefix):
            url = url[len(prefix):]
            break
    path = url
    bucket, _, prefix = path.partition("/")
    return bucket, prefix.rstrip("/") + "/"


def _upload_chunk(s3, bucket: str, prefix: str, idx: int,
                  batches: list[pa.RecordBatch]) -> int:
    table = pa.Table.from_batches(batches)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    key = f"{prefix}part-{idx:04d}.parquet"
    s3.upload_fileobj(buf, bucket, key)
    print(f"  uploaded s3://{bucket}/{key}  rows={table.num_rows}", flush=True)
    return table.num_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server",   required=True, help="MSSQL host[:port]")
    ap.add_argument("--database", required=True)
    ap.add_argument("--table",    required=True, help="schema.table e.g. dbo.FBNK_CATEG_ENTRY")
    ap.add_argument("--output",   required=True, help="s3a://raw/t24/TABLE/DATE/")
    args = ap.parse_args()

    password = os.environ.get("MSSQL_PASSWORD")
    if not password:
        raise SystemExit("missing env MSSQL_PASSWORD")

    bucket, prefix = _parse_s3_url(args.output)
    s3 = _s3_client()

    # bcp: character mode, tab field separator, newline row separator.
    # XMLRECORD is cN-XML (<row><c1>...</c1></row>) — no tabs inside → safe.
    query = f"SELECT RECID, XMLRECORD FROM {args.table}"
    cmd = [
        "bcp", query, "queryout", "/dev/stdout",
        "-S", args.server,
        "-d", args.database,
        "-U", "sa",
        "-P", password,
        "-c",       # character (text) output
        "-t\t",     # field terminator = tab
        "-r\n",     # row terminator = newline
    ]

    print(f"BCP START {args.table} → {args.output}", flush=True)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)
    # bcp flushes status lines ("Starting copy...", "N rows copied.") to stdout
    # interleaved with data. Filter: only pass lines containing a tab (= data rows).
    grep_proc = subprocess.Popen(
        ["grep", "-aP", "\\t"],
        stdin=proc.stdout,
        stdout=subprocess.PIPE,
    )
    proc.stdout.close()

    read_opts = pcsv.ReadOptions(
        column_names=["RECID", "XMLRECORD"],
        block_size=64 * 1024 * 1024,  # 64 MB read buffer per chunk
    )
    parse_opts   = pcsv.ParseOptions(delimiter="\t")
    convert_opts = pcsv.ConvertOptions(
        column_types={"RECID": pa.string(), "XMLRECORD": pa.string()}
    )

    try:
        reader = pcsv.open_csv(
            grep_proc.stdout,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )
    except pa.lib.ArrowInvalid as e:
        grep_proc.wait()
        proc.wait()
        if "Empty CSV" in str(e):
            print(f"BCP SKIP {args.table} — 0 rows, writing empty placeholder", flush=True)
            empty = pa.table({"RECID": pa.array([], type=pa.string()), "XMLRECORD": pa.array([], type=pa.string())})
            buf = io.BytesIO()
            pq.write_table(empty, buf, compression="snappy")
            buf.seek(0)
            s3.upload_fileobj(buf, bucket, f"{prefix}part-0000.parquet")
            print(f"  uploaded s3://{bucket}/{prefix}part-0000.parquet  rows=0", flush=True)
            sys.exit(0)
        raise

    chunk_idx    = 0
    accumulated: list[pa.RecordBatch] = []
    accum_rows   = 0
    total_rows   = 0
    files_written = 0

    for batch in reader:
        accumulated.append(batch)
        accum_rows  += batch.num_rows
        total_rows  += batch.num_rows

        if accum_rows >= CHUNK_ROWS:
            _upload_chunk(s3, bucket, prefix, chunk_idx, accumulated)
            chunk_idx    += 1
            files_written += 1
            accumulated   = []
            accum_rows    = 0

    if accumulated:
        _upload_chunk(s3, bucket, prefix, chunk_idx, accumulated)
        files_written += 1

    grep_proc.wait()
    proc.wait()
    if proc.returncode != 0:
        raise SystemExit(f"bcp exited with code {proc.returncode}")

    print(f"BCP DONE {args.table}  total_rows={total_rows}  files={files_written}", flush=True)


if __name__ == "__main__":
    main()
