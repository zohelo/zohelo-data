#!/usr/bin/env python3
"""Senate (register PLG-025) bronze.load: newest Landing copy of every page → four Bronze tables.

  sittings_page_<nn> → bronze.senat_sittings     one row per sitting block of a list page
  day_<id>_<day>     → bronze.senat_day_votes    one row per vote listed on a sitting day (a day without votes: one
                                                 row with empty vote columns)
  vote_<id>          → bronze.senat_vote_results one row per vote print page: totals as printed, mark tallies
  senators           → bronze.senat_senators     one row per senator of term XI (id, name, annotation)
Values as printed (text), parsed by the strict parsers of ingestion/to_landing/senat/pages.py: a page whose layout
differs is recorded in bronze.senat_rejected_files with the reason, never guessed. One Parquet file per Landing
file, ``02_bronze/senat/<folder>/<entity>/<raw_sha256>.parquet``, partitioned by entity; the table's file list is
the checkpoint, per (entity, SHA-256): two votes whose print pages are byte-identical (the page does not carry the
vote id) are both loaded. Bounded by the run's time budget; the next run continues.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
import os
from pathlib import Path, PurePosixPath
import tempfile
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing import senat as landing
from ingestion.to_landing.senat import pages

SOURCE = "senat"
PROVENANCE = [("entity", "VARCHAR"), *bulk_bronze.PROVENANCE]
TABLES: dict[str, dict[str, Any]] = {
    "sittings": {
        "table": "senat_sittings",
        "folder": "sittings",
        "columns": [
            *PROVENANCE,
            ("page_number", "VARCHAR"),
            ("sitting_id", "VARCHAR"),
            ("sitting_label", "VARCHAR"),
            ("sitting_number", "VARCHAR"),
            ("date_text", "VARCHAR"),
            ("status_text", "VARCHAR"),
            ("day_numbers", "VARCHAR"),  # day numbers with a votes page, '|' separated; empty for a planned sitting
            ("source_row_number", "BIGINT"),
        ],
    },
    "day": {
        "table": "senat_day_votes",
        "folder": "day_votes",
        "columns": [
            *PROVENANCE,
            ("sitting_id", "VARCHAR"),
            ("day_number", "VARCHAR"),
            ("sitting_number", "VARCHAR"),
            ("date_text", "VARCHAR"),
            ("status_text", "VARCHAR"),
            ("vote_number", "VARCHAR"),
            ("subject", "VARCHAR"),
            ("subtitle", "VARCHAR"),
            ("vote_id", "VARCHAR"),
            ("source_row_number", "BIGINT"),
        ],
    },
    "vote": {
        "table": "senat_vote_results",
        "folder": "vote_results",
        "columns": [
            *PROVENANCE,
            ("vote_id", "VARCHAR"),
            ("title", "VARCHAR"),
            ("subtitle", "VARCHAR"),
            ("voted", "VARCHAR"),  # głosowało
            ("votes_for", "VARCHAR"),  # za
            ("votes_against", "VARCHAR"),  # przeciw
            ("abstained", "VARCHAR"),  # wstrzymało się
            ("senators_listed", "VARCHAR"),
            ("mark_counts", "VARCHAR"),  # senators per printed mark, e.g. 'nieob.=5|przec.=62|wstrz.=1|za=32'
        ],
    },
    "senators": {
        "table": "senat_senators",
        "folder": "senators",
        "columns": [
            *PROVENANCE,
            ("senator_id", "VARCHAR"),
            ("term", "VARCHAR"),
            ("slug", "VARCHAR"),
            ("name", "VARCHAR"),
            ("annotation", "VARCHAR"),  # as printed, e.g. 'mandat wygasł 17.11.2024 r.'
            ("source_row_number", "BIGINT"),
        ],
    },
}
REJECTED = "senat_rejected_files"
COMMIT_EVERY = 500


def kind_of(entity: str) -> str:
    if entity.startswith("sittings_page_"):
        return "sittings"
    if entity == "senators":
        return "senators"
    if entity.startswith("day_"):
        return "day"
    if entity.startswith("vote_"):
        return "vote"
    raise ValueError(f"unknown Senate entity {entity!r}")


def rows_of(body: bytes, receipt: dict[str, Any]) -> list[dict[str, Any]]:
    """Bronze rows of one Landing file; ValueError when the page layout is not the known one."""
    entity = receipt["entity"]
    kind = kind_of(entity)
    if kind == "sittings":
        found = [{**s, "page_number": str(int(entity.rsplit("_", 1)[1]))} for s in pages.sittings(body)["sittings"]]
    elif kind == "day":
        _, sitting_id, day = entity.split("_")
        page = pages.day_votes(body, sitting_id, day)
        common = {k: page[k] for k in ("day_number", "sitting_number", "date_text", "status_text")}
        found = [{**common, "sitting_id": sitting_id, **v} for v in page["votes"]]
        if not found:  # a day without votes: one row with empty vote columns (no file without rows)
            found = [{**common, "sitting_id": sitting_id}]
    elif kind == "vote":
        found = [{**pages.vote_result(body), "vote_id": entity.split("_", 1)[1]}]
    else:
        found = pages.senators(body)
    base = bulk_bronze.provenance(receipt)
    names = [c for c, _ in TABLES[kind]["columns"]]
    out = []
    for n, item in enumerate(found, start=1):
        row = {c: None for c in names} | base | {k: v for k, v in item.items() if k in names}
        if "source_row_number" in names:
            row["source_row_number"] = n
        out.append(row)
    return out


def newest_receipts(client, bucket: str, landing_root: str) -> list[dict[str, Any]]:
    """Newest receipt of every landed entity (one listing, receipts read in parallel)."""
    keys = landing.held_receipts(client, bucket, landing_root)
    with ThreadPoolExecutor(max_workers=8) as pool:
        return list(pool.map(lambda k: json.loads(get_with_backoff(client, bucket, k)), sorted(keys.values())))


def loaded_pairs(table) -> set[tuple[str, str]]:
    """(entity, raw SHA-256) of every file the table holds (files are named <entity>/<sha256>.parquet)."""
    if table.current_snapshot() is None:
        return set()
    out = set()
    for path in table.inspect.data_files().column("file_path").to_pylist():
        p = PurePosixPath(path)
        if bf.SHA_RE.fullmatch(p.stem):
            out.add((p.parent.name, p.stem))
        else:  # not written by this loader (compaction is off, #875): its rows' own values
            columns = bf.read_columns(table, path, ["entity", "raw_sha256"]).to_pylist()
            out |= {(r["entity"], r["raw_sha256"]) for r in columns}
    return out


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    deadline = time.monotonic() + max(budget_minutes, 5) * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    tables = {
        kind: bf.retry(lambda s=spec: bf.ensure_table(cat, (namespace, s["table"]), s["columns"], "entity"), "load")
        for kind, spec in TABLES.items()
    }
    done = set().union(*(loaded_pairs(t) for t in tables.values()))
    rejected_before = bf.rejected_hashes(cat, (namespace, REJECTED))
    receipts = newest_receipts(client, landing_bucket, landing_root)
    todo = [r for r in receipts if (r["entity"], r["sha256"]) not in done and r["sha256"] not in rejected_before]
    totals: dict[str, Any] = {"entities_landed": len(receipts), "candidates": len(todo), "rows_added": {}}
    uris: dict[str, list[str]] = {kind: [] for kind in TABLES}
    rejected: list[dict[str, Any]] = []
    stopped = 0

    def one(receipt: dict[str, Any], temporary: str) -> tuple[str, str | None, int, dict | None]:
        kind = kind_of(receipt["entity"])
        body = bulk_bronze.read_raw(client, landing_bucket, SOURCE, receipt)
        try:
            rows = rows_of(body, receipt)
        except (ValueError, KeyError, UnicodeDecodeError) as exc:
            return (
                kind,
                None,
                0,
                {
                    "entity": receipt["entity"],
                    "raw_sha256": receipt["sha256"],
                    "landing_key": receipt["object"]["key"],
                    "error": f"{type(exc).__name__}: {exc}"[:1000],
                    "code_sha": os.environ.get("GITHUB_SHA", ""),
                },
            )
        spec = TABLES[kind]
        path = Path(temporary) / f"{receipt['entity']}.{receipt['sha256']}.parquet"
        pq.write_table(
            pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(spec["columns"])), path, compression="zstd"
        )
        key = f"{lake_root}/{SOURCE}/{spec['folder']}/{receipt['entity']}/{receipt['sha256']}.parquet"
        uri = bf.upload(client, lake, key, path, receipt["sha256"])
        path.unlink()
        return kind, uri, len(rows), None

    def flush() -> None:
        for kind, found in uris.items():
            if found:
                bf.commit(cat, (namespace, TABLES[kind]["table"]), found, ["entity", "raw_sha256"], "senat-bronze-load")
                totals.setdefault("appended_files", {}).setdefault(kind, 0)
                totals["appended_files"][kind] += len(found)
                uris[kind] = []

    with tempfile.TemporaryDirectory(prefix="zohelo-senat-bronze-") as temporary:
        for start in range(0, len(todo), COMMIT_EVERY):
            if time.monotonic() > deadline:
                stopped = len(todo) - start
                break
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda r: one(r, temporary), todo[start : start + COMMIT_EVERY]))
            for kind, uri, count, problem in results:
                if problem:
                    rejected.append(problem)
                    continue
                uris[kind].append(uri)
                totals["rows_added"][kind] = totals["rows_added"].get(kind, 0) + count
            flush()
    bf.record_rejections(cat, (namespace, REJECTED), rejected)
    totals.update({"rejected": len(rejected), "rejected_sample": [r["error"][:160] for r in rejected[:3]]})
    totals.update({"left_for_next_run": stopped, "finished_at": datetime.now(UTC).isoformat(timespec="seconds")})
    annotate("notice", f"{SOURCE} Bronze from Landing", {"result": "complete" if not stopped else "partial", **totals})
    step_summary(f"## {SOURCE} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
