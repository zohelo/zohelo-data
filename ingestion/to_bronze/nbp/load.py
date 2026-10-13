#!/usr/bin/env python3
"""NBP bronze.load: Landing table-quarters → bronze.nbp_observations.

Loads the newest Landing version of every entity (``A_2024Q1`` …). One Parquet file per
Landing file, ``02_bronze/nbp/observations/<entity>/<raw_sha256>.parquet``, partitioned by
``table_code``; the table's file list is the checkpoint. Values stay as the source text
(JSON numbers are read as text, never through floats). A superseded version of an open
quarter stays in Bronze until Silver picks the newest per entity (raw_sha256 + source_version).
Rejections: bronze.nbp_rejected_files.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.nbp.extract import landed

TABLE_ID = ("bronze", "nbp_observations")
REJECTED_ID = ("bronze", "nbp_rejected_files")
OUTPUT_PREFIX = "02_bronze/nbp/observations/"
COLUMNS = [
    ("table_code", "VARCHAR"), ("entity", "VARCHAR"), ("table_no", "VARCHAR"),
    ("effective_date", "VARCHAR"), ("trading_date", "VARCHAR"), ("currency_name", "VARCHAR"),
    ("currency_code", "VARCHAR"), ("mid", "VARCHAR"), ("bid", "VARCHAR"), ("ask", "VARCHAR"),
    ("gold_price", "VARCHAR"), ("raw_sha256", "VARCHAR"), ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"), ("source_row_number", "BIGINT"),
]
ARROW = pa.schema([(n, pa.timestamp("us", tz="UTC") if k == "TIMESTAMPTZ" else pa.int64() if k == "BIGINT"
                    else pa.string()) for n, k in COLUMNS])
COMMIT_EVERY = 50


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    """One row per currency quote (tables A/B/C) or per gold price; values as source text."""
    data = json.loads(body, parse_float=str, parse_int=str)
    if not isinstance(data, list) or not data:
        raise ValueError("NBP response is not a non-empty JSON array")
    table = receipt["table"]
    base = {"table_code": table, "entity": receipt["entity"], "raw_sha256": receipt["sha256"],
            "source_version": receipt["version_token"],
            "ingested_at": datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)}
    rows, number = [], 0
    for record in data:
        if table == "GOLD":
            number += 1
            rows.append({**base, "effective_date": record["data"], "gold_price": record["cena"],
                         "source_row_number": number})
            continue
        if record.get("table") != table:
            raise ValueError(f"table {record.get('table')!r} in a {table} file")
        for rate in record["rates"]:
            number += 1
            rows.append({**base, "table_no": record["no"], "effective_date": record["effectiveDate"],
                         "trading_date": record.get("tradingDate"), "currency_name": rate.get("currency"),
                         "currency_code": rate["code"], "mid": rate.get("mid"), "bid": rate.get("bid"),
                         "ask": rate.get("ask"), "source_row_number": number})
    return pa.Table.from_pylist(rows, schema=ARROW)


def read_raw(client, bucket: str, receipt: dict[str, Any]) -> bytes:
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key("nbp", receipt["sha256"], key)
    body = b"".join(raw_chunks(client, bucket, key))
    if sha256(body).hexdigest() != receipt["sha256"] or len(body) != receipt["size_bytes"]:
        raise RuntimeError(f"NBP raw bytes do not match their receipt: {key}")
    return body


def load(budget_minutes: float) -> int:
    deadline = time.monotonic() + budget_minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    table = bf.retry(lambda: bf.ensure_table(cat, TABLE_ID, COLUMNS, "table_code"), "load_table")
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, REJECTED_ID)
    newest = [rows[-1] for rows in landed(client, landing_bucket).values()]
    todo = sorted((r for r in newest if r["sha256"] not in done), key=lambda r: r["entity"])
    totals = {"candidates": len(todo), "appended_files": 0, "rows_added": 0, "rejected": 0}
    pending: list[str] = []
    rejected: list[dict[str, Any]] = []
    code_sha = os.environ.get("GITHUB_SHA", "")
    with tempfile.TemporaryDirectory(prefix="zohelo-nbp-bronze-") as temporary:
        for receipt in todo:
            if time.monotonic() > deadline:
                break
            body = read_raw(client, landing_bucket, receipt)
            try:
                rows = decode(body, receipt)
            except (ValueError, KeyError, TypeError) as exc:
                rejected.append({"entity": receipt["entity"], "raw_sha256": receipt["sha256"],
                                 "landing_key": receipt["object"]["key"], "error": f"{type(exc).__name__}: {exc}"[:1000],
                                 "code_sha": code_sha})
                continue
            path = Path(temporary) / f"{receipt['sha256']}.parquet"
            pq.write_table(rows, path, compression="zstd")
            key = f"{OUTPUT_PREFIX}{receipt['entity']}/{receipt['sha256']}.parquet"
            pending.append(bf.upload(client, lake, key, path, receipt["sha256"]))
            totals["rows_added"] += rows.num_rows
            path.unlink()
            if len(pending) >= COMMIT_EVERY:
                bf.commit(cat, TABLE_ID, pending, ["table_code", "entity", "raw_sha256"], "nbp-bronze-load")
                totals["appended_files"] += len(pending)
                pending = []
        if pending:
            bf.commit(cat, TABLE_ID, pending, ["table_code", "entity", "raw_sha256"], "nbp-bronze-load")
            totals["appended_files"] += len(pending)
    bf.record_rejections(cat, REJECTED_ID, rejected)
    totals["rejected"] = len(rejected)
    totals["remaining"] = totals["candidates"] - totals["appended_files"] - totals["rejected"]
    result = "complete" if totals["remaining"] == 0 else "incomplete_time_budget"
    annotate("notice" if result == "complete" else "warning", "nbp Bronze from Landing", {"result": result, **totals})
    step_summary(f"## nbp · bronze load: {result}\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0 if result == "complete" else 2
