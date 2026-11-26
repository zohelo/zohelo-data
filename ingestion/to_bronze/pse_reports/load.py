#!/usr/bin/env python3
"""PSE Reports API v2 bronze.load: newest Landing copy of every (report, business day) entity → one Bronze table per
report (bronze.pse_reports_rce_pln, bronze.pse_reports_his_wlk_cal).

Each Landing file is the exact answer of the API for one report on one business day (ingestion/to_landing/pse_reports/):
``{"value": [...]}``, one object per 15-minute period. One Bronze row per period: every field as the text it has in
the file (numbers keep their JSON text, read with ``parse_float=str``; null stays null), the row's position in the
file and the provenance columns. A file whose wrapper differs, whose rows carry another field set than FIELDS of
the report (a field added, renamed or dropped by PSE), a non-scalar value or another business day than its entity
is rejected (bronze.pse_reports_rejected_files) with the reason, never guessed.

Candidates are found from Landing key names in one listing: the newest file of each entity whose data file is still
in Landing (a file already moved to the Archive was proven in Bronze first), so a scheduled run reads only the
receipts of new files.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.pse_reports import extract

SOURCE = "pse_reports"
REJECTED = "pse_reports_rejected_files"
COMMIT_EVERY = 100  # files per catalog commit (a first load is about 1,800 files)
TIMES = ["business_date", "period", "period_utc", "dtime", "dtime_utc"]
STAMPS = ["publication_ts", "publication_ts_utc"]
# The field set of each report as API v2 publishes it (Dev probe 45790352597, 26 Nov 2026), value fields in name order.
VALUES = {
    "rce_pln": ["rce_pln"],
    "his_wlk_cal": [
        "demand", "jg", "jga", "jgm", "jgm1", "jgm2", "jgo", "jgw1", "jgw2", "jgz1", "jgz2", "jgz3", "jnwrb", "pv",
        "swm_np", "swm_p", "wi",
    ],
}  # fmt: skip
FIELDS = {report: [*TIMES, *values, *STAMPS] for report, values in VALUES.items()}


def table_of(report: str) -> str:
    return f"{SOURCE}_{report}"


def columns_of(report: str) -> list[tuple[str, str]]:
    return [
        ("entity", "VARCHAR"),
        *[(f, "VARCHAR") for f in FIELDS[report]],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),  # position of the row in the file, from 1
    ]


def as_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    raise ValueError(f"non-scalar value {str(value)[:80]}")


def decode(body: bytes, receipt: dict[str, Any]) -> tuple[str, pa.Table]:
    """(report, the Bronze rows of one Landing file); ValueError for any layout that is not the known one."""
    parsed = extract.parse_entity(receipt["entity"])
    if parsed is None:
        raise ValueError(f"unknown entity {receipt['entity']}")
    report, day = parsed
    document = json.loads(body, parse_float=str, parse_int=str)
    if (
        not isinstance(document, dict)
        or not isinstance(document.get("value"), list)
        or set(document) - {"value", "nextLink"}
    ):
        raise ValueError(f"not a PSE API v2 answer: {body[:200]!r}")
    fields = FIELDS[report]
    columns: dict[str, list[Any]] = {name: [] for name, _ in columns_of(report)}
    base = bulk_bronze.provenance(receipt)
    for number, row in enumerate(document["value"], start=1):
        if not isinstance(row, dict) or set(row) != set(fields):
            found = sorted(row) if isinstance(row, dict) else type(row).__name__
            added, missing = sorted(set(found) - set(fields)), sorted(set(fields) - set(found))
            raise ValueError(f"row {number}: field set differs from {report}: added {added}, missing {missing}")
        if row["business_date"] != f"{day}":
            raise ValueError(f"row {number}: business_date {row['business_date']} in the file of {day}")
        for field in fields:
            columns[field].append(as_text(row[field]))
        columns["source_row_number"].append(number)
        for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
            columns[name].append(base[name])
    if not document["value"]:
        raise ValueError("no rows (Landing writes only complete days)")
    return report, pa.Table.from_pydict(columns, schema=bulk_bronze.arrow_schema(columns_of(report)))


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
    """New Landing files, oldest entity first, committed COMMIT_EVERY files per table at a time so a long first load
    keeps its progress; stops at the time budget (exit 2, the next run continues: loaded files are skipped by
    SHA-256)."""
    deadline = time.monotonic() + budget_minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    rejected_id = (namespace, REJECTED)
    tables = {
        r: bf.retry(lambda r=r: bf.ensure_table(cat, (namespace, table_of(r)), columns_of(r), "entity"), "load_table")
        for r in FIELDS
    }
    done = set().union(*(bf.loaded_hashes(t) for t in tables.values())) | bf.rejected_hashes(cat, rejected_id)
    receipts = candidates(client, landing_bucket, landing_root)
    todo = sorted((r for r in receipts if r["sha256"] not in done), key=lambda r: r["entity"])
    totals: dict[str, Any] = {"candidates_in_landing": len(receipts), "new": len(todo), "appended_files": 0}
    totals.update({"rows_added": {r: 0 for r in FIELDS}, "rejected": 0})
    pending: dict[str, list[str]] = {r: [] for r in FIELDS}
    rejected: list[dict[str, Any]] = []

    def flush(report: str) -> None:
        if pending[report]:
            bf.commit(
                cat, (namespace, table_of(report)), pending[report], ["entity", "raw_sha256"], f"{SOURCE}-bronze-load"
            )
            totals["appended_files"] += len(pending[report])
            pending[report] = []

    with tempfile.TemporaryDirectory(prefix=f"zohelo-{SOURCE}-bronze-") as temporary:
        for receipt in todo:
            if time.monotonic() > deadline:
                break
            body = bulk_bronze.read_raw(client, landing_bucket, SOURCE, receipt)
            try:
                report, rows = decode(body, receipt)
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
            key = f"{lake_root}/{SOURCE}/{report}/{receipt['entity']}/{receipt['sha256']}.parquet"
            pending[report].append(bf.upload(client, lake, key, path, receipt["sha256"]))
            path.unlink()
            totals["rows_added"][report] += rows.num_rows
            if len(pending[report]) >= COMMIT_EVERY:
                flush(report)
        for report in FIELDS:
            flush(report)
    bf.record_rejections(cat, rejected_id, rejected)
    totals["rejected"] = len(rejected)
    totals["rejected_sample"] = [{k: r[k] for k in ("entity", "error")} for r in rejected[:3]]
    totals["remaining"] = totals["new"] - totals["appended_files"] - totals["rejected"]
    result = "complete" if totals["remaining"] == 0 else "incomplete_time_budget"
    annotate("notice" if not rejected else "warning", f"{SOURCE} Bronze from Landing", {"result": result, **totals})
    step_summary(f"## {SOURCE} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0 if result == "complete" else 2
