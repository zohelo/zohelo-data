#!/usr/bin/env python3
"""TED notices (Poland) bronze.load: newest Landing copy of every page entity → bronze.ted_notices.

Each Landing file is one TED Search API answer: one page of the notices of Polish buyers published on one day
(ingestion/to_landing/ted_notices/). One Bronze row per notice: the publication number and every requested field
as text (a string value as published, any other value as its JSON text, keys sorted), plus the notice's position
in the page and the provenance columns. The API adds a ``links`` object (addresses of the notice in every
language and format) to each notice: it is not stored, the publication number gives the address. An answer
without ``notices``, a notice without a publication number, or a notice with a key that is neither requested nor
``links`` rejects the file (bronze.ted_notices_rejected_files). A day without notices gives an empty file.

Candidates are found from Landing key names in one listing: the newest file of each entity whose data file is
still in Landing (a file already moved to the Archive was proven in Bronze first), so a scheduled run reads only
the receipts of new files.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.ted_notices import extract

SOURCE = "ted_notices"
TABLE = "ted_notices"
REJECTED = "ted_notices_rejected_files"
IGNORED = {"links"}
FIELD_COLUMNS = [(f, f.replace("-", "_")) for f in extract.FIELDS if f != "publication-number"]
COLUMNS = [
    ("entity", "VARCHAR"),
    ("publication_number", "VARCHAR"),
    *[(column, "VARCHAR") for _, column in FIELD_COLUMNS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the notice in the page, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def as_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    answer = json.loads(body)
    if not isinstance(answer, dict) or not isinstance(answer.get("notices"), list):
        raise ValueError(f"not a TED search answer: {body[:200]!r}")
    known = set(extract.FIELDS) | IGNORED
    columns: dict[str, list[Any]] = {name: [] for name, _ in COLUMNS}
    base = bulk_bronze.provenance(receipt)
    for number, notice in enumerate(answer["notices"], start=1):
        unknown = sorted(set(notice) - known)
        if unknown:
            raise ValueError(f"notice {number}: fields not requested {unknown[:5]}")
        publication = notice.get("publication-number")
        if not isinstance(publication, str) or not publication:
            raise ValueError(f"notice {number}: no publication number")
        columns["publication_number"].append(publication)
        for field, column in FIELD_COLUMNS:
            columns[column].append(as_text(notice.get(field)))
        columns["source_row_number"].append(number)
        for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
            columns[name].append(base[name])
    return pa.Table.from_pydict(columns, schema=ARROW)


def candidates(client, bucket: str, landing_root: str) -> list[dict[str, Any]]:
    """Receipts of the newest file of each entity whose data file is still in Landing."""
    receipts, data = extract.listing(client, bucket, landing_root)
    keys = []
    for rows in receipts.values():
        receipt_key = rows[-1][1]
        if receipt_key[: -len(".receipt.json")] + ".json" in data:
            keys.append(receipt_key)
    return extract.read_receipts(client, bucket, keys)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a day's pages are a few hundred kilobytes."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    table_id, rejected_id = (namespace, TABLE), (namespace, REJECTED)
    table = bf.retry(lambda: bf.ensure_table(cat, table_id, COLUMNS, "entity"), "load_table")
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, rejected_id)
    receipts = candidates(client, landing_bucket, landing_root)
    todo = [r for r in receipts if r["sha256"] not in done]
    totals: dict[str, Any] = {"candidates_in_landing": len(receipts), "new": len(todo), "appended_files": 0}
    totals.update({"rows_added": 0, "empty_files": 0, "rejected": 0})
    uris, rejected = [], []
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{SOURCE}-bronze-") as temporary:
        for receipt in todo:
            body = bulk_bronze.read_raw(client, landing_bucket, SOURCE, receipt)
            try:
                rows = decode(body, receipt)
            except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
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
            key = f"{lake_root}/{SOURCE}/notices/{receipt['entity']}/{receipt['sha256']}.parquet"
            uris.append(bf.upload(client, lake, key, path, receipt["sha256"]))
            totals["rows_added"] += rows.num_rows
            totals["empty_files"] += rows.num_rows == 0
    if uris:
        bf.commit(cat, table_id, uris, ["entity", "raw_sha256"], f"{SOURCE}-bronze-load")
        totals["appended_files"] = len(uris)
    bf.record_rejections(cat, rejected_id, rejected)
    totals["rejected"] = len(rejected)
    totals["rejected_sample"] = [{k: r[k] for k in ("entity", "error")} for r in rejected[:3]]
    annotate("notice", f"{SOURCE} Bronze from Landing", {"result": "complete", "table": ".".join(table_id), **totals})
    step_summary(f"## {SOURCE} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
