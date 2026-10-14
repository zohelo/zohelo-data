#!/usr/bin/env python3
"""GUS BDL metadata → Bronze: variables per subgroup and the territorial unit list.

Reads the API pages that ``landing.extract`` lands under
``01_landing/gus_bdl/snapshot/metadata/`` (one file per page) and writes one Parquet file
per page:

  bronze.bdl_metadata_variables  one row per variable: subgroup, variable id, n1…n5
                                 (dimension labels), level, measure unit
  bronze.bdl_metadata_units      one row per unit: unit id (12-character KTS-based), name,
                                 parent id, level, kind

Values stay as published (text); the table's file list is the checkpoint. Every landed
page is loaded (pages of older versions too); Silver keeps the newest version per subgroup.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, get_with_backoff, raw_chunks, req, s3

METADATA = "01_landing/gus_bdl/snapshot/metadata/"
VARIABLES_ID = ("bronze", "bdl_metadata_variables")
UNITS_ID = ("bronze", "bdl_metadata_units")
REJECTED_ID = ("bronze", "bdl_metadata_rejected_files")
PROVENANCE = [
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("part_index", "BIGINT"),
    ("source_row_number", "BIGINT"),
]
VARIABLE_COLUMNS = (
    [("subgroup_id", "VARCHAR"), ("variable_id", "VARCHAR")]
    + [(f"n{i}", "VARCHAR") for i in range(1, 6)]
    + [("level", "VARCHAR"), ("measure_unit_id", "VARCHAR"), ("measure_unit_name", "VARCHAR")]
    + PROVENANCE
)
UNIT_COLUMNS = [
    ("entity", "VARCHAR"),
    ("unit_id", "VARCHAR"),
    ("name", "VARCHAR"),
    ("parent_id", "VARCHAR"),
    ("level", "VARCHAR"),
    ("kind", "VARCHAR"),
    ("has_description", "VARCHAR"),
    *PROVENANCE,
]


def arrow_schema(columns: list[tuple[str, str]]) -> pa.Schema:
    kinds = {"VARCHAR": pa.string(), "BIGINT": pa.int64(), "TIMESTAMPTZ": pa.timestamp("us", tz="UTC")}
    return pa.schema([(name, kinds[kind]) for name, kind in columns])


def text(value: Any) -> str | None:
    return None if value is None else str(value)


def rows_for(receipt: dict[str, Any], body: bytes) -> list[dict[str, Any]]:
    results = json.loads(body).get("results")
    if not isinstance(results, list):
        raise ValueError("BDL page without a results list")
    base = {
        "raw_sha256": receipt["sha256"],
        "source_version": receipt["version_token"],
        "ingested_at": datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC),
        "part_index": receipt["part"]["index"],
    }
    rows = []
    for number, item in enumerate(results, start=1):
        if receipt["metadata_kind"] == "variables":
            rows.append(
                {
                    **base,
                    "subgroup_id": receipt["entity"],
                    "variable_id": text(item.get("id")),
                    **{f"n{i}": text(item.get(f"n{i}")) for i in range(1, 6)},
                    "level": text(item.get("level")),
                    "measure_unit_id": text(item.get("measureUnitId")),
                    "measure_unit_name": text(item.get("measureUnitName")),
                    "source_row_number": number,
                }
            )
        else:
            rows.append(
                {
                    **base,
                    "entity": receipt["entity"],
                    "unit_id": text(item.get("id")),
                    "name": text(item.get("name")),
                    "parent_id": text(item.get("parentId")),
                    "level": text(item.get("level")),
                    "kind": text(item.get("kind")),
                    "has_description": text(item.get("hasDescription")),
                    "source_row_number": number,
                }
            )
    return rows


def load() -> dict[str, Any]:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    tables = {"variables": (VARIABLES_ID, VARIABLE_COLUMNS, "subgroup_id"), "units": (UNITS_ID, UNIT_COLUMNS, "entity")}
    done: set[str] = bf.rejected_hashes(cat, REJECTED_ID)
    for table_id, columns, partition in tables.values():
        done |= bf.loaded_hashes(
            bf.retry(lambda t=table_id, c=columns, p=partition: bf.ensure_table(cat, t, c, p), "table")
        )
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing, Prefix=METADATA):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(lambda k: json.loads(get_with_backoff(client, landing, k)), keys))
    todo = [r for r in receipts if r["sha256"] not in done and r.get("metadata_kind") in tables]
    uris: dict[str, list[str]] = {"variables": [], "units": []}
    rejected, counts = [], {"variables": 0, "units": 0}
    with tempfile.TemporaryDirectory(prefix="zohelo-bdl-metadata-") as temporary:
        for receipt in todo:
            kind = receipt["metadata_kind"]
            table_id, columns, partition = tables[kind]
            key = receipt["object"]["key"]
            try:
                client.head_object(Bucket=landing, Key=key)
            except client.exceptions.ClientError:
                key = archive_key("gus_bdl", receipt["sha256"], key)
            body = b"".join(raw_chunks(client, landing, key))
            if sha256(body).hexdigest() != receipt["sha256"]:
                raise RuntimeError(f"BDL metadata bytes do not match their receipt: {key}")
            try:
                rows = rows_for(receipt, body)
            except (ValueError, KeyError, TypeError) as exc:
                rejected.append(
                    {
                        "entity": receipt["entity"],
                        "raw_sha256": receipt["sha256"],
                        "landing_key": key,
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                        "code_sha": os.environ.get("GITHUB_SHA", ""),
                    }
                )
                continue
            path = Path(temporary) / f"{receipt['sha256']}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=arrow_schema(columns)), path, compression="zstd")
            out_key = f"02_bronze/gus_bdl/metadata_{kind}/{receipt['entity']}/{receipt['sha256']}.parquet"
            uris[kind].append(bf.upload(client, lake, out_key, path, receipt["sha256"]))
            counts[kind] += len(rows)
            path.unlink()
            if len(uris[kind]) >= 200:
                bf.commit(cat, table_id, uris[kind], [partition, "raw_sha256"], "bdl-metadata-bronze")
                uris[kind] = []
    for kind, pending in uris.items():
        if pending:
            table_id, _, partition = tables[kind]
            bf.commit(cat, table_id, pending, [partition, "raw_sha256"], "bdl-metadata-bronze")
    bf.record_rejections(cat, REJECTED_ID, rejected)
    return {
        "metadata_pages": len(todo),
        "variable_rows": counts["variables"],
        "unit_rows": counts["units"],
        "rejected_pages": len(rejected),
    }
