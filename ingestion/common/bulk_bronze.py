"""Shared bronze.load for small bulk_file sources: newest Landing copy of every declared file → one Bronze table.

For sources whose files are small enough to decode in memory (NBP interest rates, OECD SDMX selections).
Every declared entity's newest Landing copy that the table and the rejections table do not yet hold is
read (Landing or Archive, checked against its receipt), decoded by the source's ``decode(body, receipt)``
into an Arrow table with the given columns, and written as one Parquet file per Landing file,
``02_bronze/<source>/<folder>/<entity>/<raw_sha256>.parquet``, partitioned by ``entity``; the table's file
list is the checkpoint. A decoder error (ValueError, KeyError) is recorded in the rejections table.
``landing_root``, ``namespace`` and ``lake_root`` exist for Dev trials (zz_dev_ scratch locations).

Accumulating sources (news feeds, attention signals: each copy holds items the next one may not) pass
``all_versions=True``: every landed copy not yet held is loaded, not only the newest; such a source does not
declare ``bronze_latest_only``. ``specs`` replaces the declared files for sources whose files are built from
a seed (``to_landing/<source>/``).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

PROVENANCE = [("raw_sha256", "VARCHAR"), ("source_version", "VARCHAR"), ("ingested_at", "TIMESTAMPTZ")]


def arrow_schema(columns: list[tuple[str, str]]) -> pa.Schema:
    kinds = {"TIMESTAMPTZ": pa.timestamp("us", tz="UTC"), "BIGINT": pa.int64(), "VARCHAR": pa.string()}
    return pa.schema([(name, kinds[kind]) for name, kind in columns])


def provenance(receipt: dict[str, Any]) -> dict[str, Any]:
    """The provenance columns of every row decoded from one Landing file."""
    return {
        "entity": receipt["entity"],
        "raw_sha256": receipt["sha256"],
        "source_version": receipt["version_token"],
        "ingested_at": datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC),
    }


def read_raw(client, bucket: str, source: str, receipt: dict[str, Any]) -> bytes:
    """The Landing file's exact bytes (Landing, else Archive), verified against its receipt."""
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key(source, receipt["sha256"], key)
    body = b"".join(raw_chunks(client, bucket, key))
    if sha256(body).hexdigest() != receipt["sha256"] or len(body) != receipt["size_bytes"]:
        raise RuntimeError(f"{source} raw bytes do not match their receipt: {key}")
    return body


def load(
    source: str,
    table_id: tuple[str, str],
    rejected_id: tuple[str, str],
    columns: list[tuple[str, str]],
    decode: Callable[[bytes, dict[str, Any]], pa.Table],
    folder: str,
    landing_root: str = "01_landing",
    lake_root: str = "02_bronze",
    all_versions: bool = False,
    specs: list[dict[str, Any]] | None = None,
) -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    table = bf.retry(lambda: bf.ensure_table(cat, table_id, columns, "entity"), "load_table")
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, rejected_id)
    receipts = []
    for spec in bulk_file.declaration(source) if specs is None else specs:
        held = bulk_file.landed(client, landing_bucket, bulk_file.prefix(source, spec["entity"], landing_root))
        if held:
            receipts += held if all_versions else [held[-1]]
    todo = [r for r in receipts if r["sha256"] not in done]
    totals: dict[str, Any] = {"entities_landed": len(receipts), "candidates": len(todo), "appended_files": 0}
    totals.update({"rows_added": 0, "rejected": 0})
    uris, rejected = [], []
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{source}-bronze-") as temporary:
        for receipt in todo:
            body = read_raw(client, landing_bucket, source, receipt)
            try:
                rows = decode(body, receipt)
            except (ValueError, KeyError, TypeError, ET.ParseError, UnicodeDecodeError) as exc:
                rejected.append(
                    {
                        "entity": receipt["entity"],
                        "raw_sha256": receipt["sha256"],
                        "landing_key": receipt["object"]["key"],
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                        "code_sha": os.environ.get("GITHUB_SHA", ""),
                    }
                )
                continue
            path = Path(temporary) / f"{receipt['sha256']}.parquet"
            pq.write_table(rows, path, compression="zstd")
            key = f"{lake_root}/{source}/{folder}/{receipt['entity']}/{receipt['sha256']}.parquet"
            uris.append(bf.upload(client, lake, key, path, receipt["sha256"]))
            totals["rows_added"] += rows.num_rows
    if uris:
        bf.commit(cat, table_id, uris, ["entity", "raw_sha256"], f"{source}-bronze-load")
        totals["appended_files"] = len(uris)
    bf.record_rejections(cat, rejected_id, rejected)
    totals["rejected"] = len(rejected)
    annotate("notice", f"{source} Bronze from Landing", {"result": "complete", "table": ".".join(table_id), **totals})
    step_summary(f"## {source} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
