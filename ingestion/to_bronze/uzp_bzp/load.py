#!/usr/bin/env python3
"""e-Zamówienia BZP bronze.load: newest Landing copy of every (notice type, publication day) entity →
bronze.uzp_bzp_notices.

Each Landing file is the reduced answer of the BZP read API for one notice type on one UTC publication day
(ingestion/to_landing/uzp_bzp/): ``{notice_type, publication_day, stats_count, notices: [...]}``. One Bronze row per
notice: every kept API field as text (a string as published, a boolean or number as its JSON text), the reduced
``contractors`` (province and country of each contractor) and the form's ``values`` lines as JSON text, the day's
``notice/stats`` count, the notice's position in the file and the provenance columns. A file whose wrapper or a
notice whose field set differs from the reduced shape is rejected (bronze.uzp_bzp_rejected_files), never guessed.
A day without notices gives one marker row (every notice field null, source_row_number 0, stats_count 0), so the
landed days are known downstream (silver.uzp_bzp_days) and a month with a day missing stays out of Gold.

Candidates are found from Landing key names in one listing: the newest file of each entity whose data file is
still in Landing (a file already moved to the Archive was proven in Bronze first), so a scheduled run reads only
the receipts of new files.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.uzp_bzp import extract

SOURCE = "uzp_bzp"
TABLE = "uzp_bzp_notices"
REJECTED = "uzp_bzp_rejected_files"
COMMIT_EVERY = 50  # files per catalog commit (a first load is about 640 files)
WRAPPER = {"notice_type", "publication_day", "stats_count", "notices"}
FIELDS = [*sorted(extract.KEPT), "contractors", "values"]


def snake(name: str) -> str:
    """objectId → object_id, isTenderAmountBelowEU → is_tender_amount_below_eu."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


FIELD_COLUMNS = [(f, "form_values" if f == "values" else snake(f)) for f in FIELDS]  # `values` is an SQL keyword
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(column, "VARCHAR") for _, column in FIELD_COLUMNS],
    ("stats_count", "BIGINT"),  # notice/stats count of the type on the day (the file's completeness reference)
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the notice in the file (ascending objectId), from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def as_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    document = json.loads(body)
    if not isinstance(document, dict) or set(document) != WRAPPER or not isinstance(document["notices"], list):
        raise ValueError(f"not a reduced BZP answer: {body[:200]!r}")
    if document["notice_type"] != receipt.get("notice_type") or type(document["stats_count"]) is not int:
        raise ValueError(f"wrapper does not match the receipt: {document['notice_type']} {document['stats_count']}")
    columns: dict[str, list[Any]] = {name: [] for name, _ in COLUMNS}
    base = bulk_bronze.provenance(receipt)
    for number, notice in enumerate(document["notices"], start=1):
        if not isinstance(notice, dict) or set(notice) != set(FIELDS):
            raise ValueError(f"notice {number}: fields differ from the reduced shape")
        if not isinstance(notice.get("objectId"), str) or not notice["objectId"]:
            raise ValueError(f"notice {number}: no objectId")
        for field, column in FIELD_COLUMNS:
            columns[column].append(as_text(notice.get(field)))
        columns["stats_count"].append(document["stats_count"])
        columns["source_row_number"].append(number)
        for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
            columns[name].append(base[name])
    if not document["notices"]:
        if document["stats_count"] != 0:
            raise ValueError(f"no notices but notice/stats says {document['stats_count']}")
        for _, column in FIELD_COLUMNS:  # the day marker: the day was landed and had no notices of the type
            columns[column].append(None)
        columns["stats_count"].append(0)
        columns["source_row_number"].append(0)
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
    """New Landing files, oldest entity first, committed COMMIT_EVERY files at a time so a long first load keeps its
    progress; stops at the time budget (exit 2, the next run continues: loaded files are skipped by SHA-256)."""
    deadline = time.monotonic() + budget_minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    table_id, rejected_id = (namespace, TABLE), (namespace, REJECTED)
    table = bf.retry(lambda: bf.ensure_table(cat, table_id, COLUMNS, "entity"), "load_table")
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, rejected_id)
    receipts = candidates(client, landing_bucket, landing_root)
    todo = sorted((r for r in receipts if r["sha256"] not in done), key=lambda r: r["entity"])
    totals: dict[str, Any] = {"candidates_in_landing": len(receipts), "new": len(todo), "appended_files": 0}
    totals.update({"rows_added": 0, "empty_files": 0, "rejected": 0})
    pending: list[str] = []
    rejected: list[dict[str, Any]] = []

    def flush() -> None:
        if pending:
            bf.commit(cat, table_id, pending, ["entity", "raw_sha256"], f"{SOURCE}-bronze-load")
            totals["appended_files"] += len(pending)
            pending.clear()

    with tempfile.TemporaryDirectory(prefix=f"zohelo-{SOURCE}-bronze-") as temporary:
        for receipt in todo:
            if time.monotonic() > deadline:
                break
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
            pending.append(bf.upload(client, lake, key, path, receipt["sha256"]))
            path.unlink()
            empty = rows.column("object_id").null_count == rows.num_rows
            totals["rows_added"] += rows.num_rows
            totals["empty_files"] += empty
            if len(pending) >= COMMIT_EVERY:
                flush()
        flush()
    bf.record_rejections(cat, rejected_id, rejected)
    totals["rejected"] = len(rejected)
    totals["rejected_sample"] = [{k: r[k] for k in ("entity", "error")} for r in rejected[:3]]
    totals["remaining"] = totals["new"] - totals["appended_files"] - totals["rejected"]
    result = "complete" if totals["remaining"] == 0 else "incomplete_time_budget"
    annotate("notice", f"{SOURCE} Bronze from Landing", {"result": result, "table": ".".join(table_id), **totals})
    step_summary(f"## {SOURCE} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0 if result == "complete" else 2
