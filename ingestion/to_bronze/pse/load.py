#!/usr/bin/env python3
"""PSE bronze.load: the newest Landing copy of every report-day -> one Bronze table per report.

Each Landing file is one API answer ``{"value": [...]}`` for one report and business day (entity
``rce_pln_YYYYMMDD`` or ``his_wlk_cal_YYYYMMDD``), one row per 15-minute period. Bronze keeps one row per answer
row, every field as text (JSON null stays null: the API's only "no value" marker; numbers as Python writes them),
plus provenance. A row whose field set differs from the report's declared fields, a row of another business day or
an empty answer rejects the file into ``bronze.pse_rejected_files``. One Parquet file per Landing file, partitioned
by entity; the table's file list is the checkpoint. Superseded copies of a day are removed after the load
(``bronze_latest_only``). Receipts are read from one listing of the source's Landing folder (about 1,800 entities),
not one listing per entity.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
from ingestion.to_landing import pse as landing

SOURCE = "pse"
REJECTED = "pse_rejected_files"
TIME_FIELDS = ["business_date", "dtime", "dtime_utc", "period", "period_utc", "publication_ts", "publication_ts_utc"]
# Value fields per report as the API publishes them (Dev probe 43284836734, all business days probed alike).
VALUE_FIELDS = {
    "rce_pln": ["rce_pln"],
    "his_wlk_cal": [
        "demand", "jg", "jga", "jgm", "jgm1", "jgm2", "jgo", "jgw1", "jgw2", "jgz1", "jgz2", "jgz3", "jnwrb", "pv",
        "swm_np", "swm_p", "wi",
    ],
}  # fmt: skip
TABLES = {"rce_pln": "pse_rce_pln", "his_wlk_cal": "pse_his_wlk_cal"}
WORKERS = 4


def columns(report: str) -> list[tuple[str, str]]:
    fields = TIME_FIELDS + VALUE_FIELDS[report]
    return [
        ("entity", "VARCHAR"),
        *[(f, "VARCHAR") for f in fields],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def report_of(entity: str) -> str:
    return entity.rsplit("_", 1)[0]


def text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool | dict | list):
        raise ValueError(f"unexpected value {value!r}")
    return str(value)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    report = report_of(receipt["entity"])
    fields = TIME_FIELDS + VALUE_FIELDS[report]
    day = receipt["entity"].rsplit("_", 1)[1]
    items = json.loads(body)["value"]
    if not items:
        raise ValueError("no rows")
    base = bulk_bronze.provenance(receipt)
    rows = []
    for n, item in enumerate(items, start=1):
        if set(item) != set(fields):
            extra, absent = sorted(set(item) - set(fields)), sorted(set(fields) - set(item))
            raise ValueError(f"row {n}: unknown fields {extra}, missing fields {absent}")
        if str(item["business_date"]).replace("-", "") != day:
            raise ValueError(f"row {n}: business_date {item['business_date']} is not the entity's day")
        rows.append({**base, **{f: text(item[f]) for f in fields}, "source_row_number": n})
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(columns(report)))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: the history is about 1,800 files of 25-50 KB, a day adds two. Files are read,
    decoded and written by WORKERS threads (R2 round trips, not CPU, bound the history load), each R2 request
    waiting out throttling (landing.r2_call: R2 answered 503 "Reduce your rate of simultaneous reads" in Dev trial
    43297605676); one commit per table."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    rejected_id = (namespace, REJECTED)
    receipts = landing.newest_receipts(client, landing_bucket, landing_root)
    rejected_before = bf.rejected_hashes(cat, rejected_id)
    totals: dict[str, Any] = {"entities_landed": len(receipts)}
    rejected: list[dict[str, Any]] = []
    for report, table_name in TABLES.items():
        table_id = (namespace, table_name)
        table = bf.retry(
            lambda table_id=table_id, r=report: bf.ensure_table(cat, table_id, columns(r), "entity"), "load"
        )
        done = bf.loaded_hashes(table) | rejected_before
        todo = [r for r in receipts if report_of(r["entity"]) == report and r["sha256"] not in done]
        with tempfile.TemporaryDirectory(prefix=f"zohelo-{SOURCE}-bronze-") as temporary:

            def one(receipt: dict[str, Any], report: str = report, temporary: str = temporary) -> dict[str, Any]:
                body = landing.r2_call(lambda: bulk_bronze.read_raw(client, landing_bucket, SOURCE, receipt))
                try:
                    rows = decode(body, receipt)
                except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
                    return {
                        "rejected": {
                            "entity": receipt["entity"],
                            "raw_sha256": receipt["sha256"],
                            "landing_key": receipt["object"]["key"],
                            "error": f"{type(exc).__name__}: {exc}"[:1000],
                            "code_sha": os.environ.get("GITHUB_SHA", ""),
                        }
                    }
                path = Path(temporary) / f"{receipt['sha256']}.parquet"
                pq.write_table(rows, path, compression="zstd")
                key = f"{lake_root}/{SOURCE}/{report}/{receipt['entity']}/{receipt['sha256']}.parquet"
                uri = landing.r2_call(lambda: bf.upload(client, lake, key, path, receipt["sha256"]))
                path.unlink()
                return {"uri": uri, "rows": rows.num_rows}

            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                results = list(pool.map(one, todo))
        uris = [r["uri"] for r in results if "uri" in r]
        rejected += [r["rejected"] for r in results if "rejected" in r]
        if uris:
            bf.commit(cat, table_id, uris, ["entity", "raw_sha256"], f"{SOURCE}-bronze-load")
        rows_added = sum(r.get("rows", 0) for r in results)
        totals[table_name] = {"candidates": len(todo), "appended_files": len(uris), "rows_added": rows_added}
    bf.record_rejections(cat, rejected_id, rejected)
    totals["rejected"] = len(rejected)
    totals["rejected_sample"] = [{k: r[k] for k in ("entity", "error")} for r in rejected[:3]]
    annotate("notice", f"{SOURCE} Bronze from Landing", {"result": "complete", **totals})
    step_summary(f"## {SOURCE} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
