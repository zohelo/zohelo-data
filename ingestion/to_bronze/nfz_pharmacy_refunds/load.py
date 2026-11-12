#!/usr/bin/env python3
"""NFZ pharmacy refunds bronze.load: every page of the newest complete download of each entity →
bronze.nfz_ra_monthly_provisions.

Each Landing file is one page of ``/monthly-provisions`` for one area x ATC selection
(ingestion/to_landing/nfz_pharmacy_refunds/). One Bronze row per record: the selection as requested (area code
``PL`` or the NFZ voivodeship code, ATC group code or ``_T`` for no filter, and the group name sent), year, month
and the five values as the exact JSON number text (``null`` stays NULL), plus the page, ``meta.count`` and the
record's position. An answer that is not a page of the asked selection, a page number other than the receipt's
part, or a record with other fields rejects the file (bronze.nfz_pharmacy_refunds_rejected_files). Entities whose
newest download is incomplete (a run stopped between parts) are left for the next load.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3
from ingestion.to_landing.nfz_pharmacy_refunds import extract

SOURCE = "nfz_pharmacy_refunds"
TABLE = "nfz_ra_monthly_provisions"
REJECTED = "nfz_pharmacy_refunds_rejected_files"
VALUES = [
    ("quantity", "quantity"),
    ("refund", "refund"),
    ("donation", "donation"),
    ("contribution-of-patient", "contribution_of_patient"),
    ("value", "value"),
]
COLUMNS = [
    ("entity", "VARCHAR"),
    ("area_code", "VARCHAR"),  # PL (no province filter) or NFZ voivodeship code 01..16
    ("atc_code", "VARCHAR"),  # ATC main group (A ... V) or _T (no ATC filter)
    ("atc_name", "VARCHAR"),  # the group name sent as filter (NULL for _T)
    ("year", "VARCHAR"),
    ("month", "VARCHAR"),
    *[(column, "VARCHAR") for _, column in VALUES],
    ("meta_count", "BIGINT"),  # rows the API announced for the selection (all pages)
    ("page", "BIGINT"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the record in the page, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def text(value: Any) -> str | None:
    return None if value is None else str(value)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    page = json.loads(body, parse_float=str, parse_int=str)  # keep the number text exactly as published
    if not isinstance(page, dict) or not isinstance(page.get("data"), list) or "meta" not in page:
        raise ValueError(f"not a monthly-provisions page: {str(page)[:200]}")
    meta, part = page["meta"], receipt["part"]
    if str(meta.get("page")) != str(part["index"]) or (not page["data"] and int(meta["count"])):
        raise ValueError(f"page {meta.get('page')} with {len(page['data'])} rows, receipt part {part}")
    echoed = {k: v[0] for k, v in parse_qs(urlsplit((page.get("links") or {}).get("self") or "").query).items()}
    asked = (None if receipt["area_code"] == "PL" else receipt["area_code"], receipt["atc_name"] is not None)
    if (echoed.get("province"), bool(echoed.get("atc"))) != asked:
        raise ValueError(f"answer for province {echoed.get('province')} atc {echoed.get('atc')}, asked {asked}")
    columns: dict[str, list[Any]] = {n: [] for n, _ in COLUMNS}
    for number, record in enumerate(page["data"], start=1):
        if set(record) != extract.FIELDS:
            raise ValueError(f"record {number}: fields {sorted(record)}")
        for key, column in [("year", "year"), ("month", "month"), *VALUES]:
            columns[column].append(text(record[key]))
        columns["source_row_number"].append(number)
    count = len(columns["year"])
    base = bulk_bronze.provenance(receipt)
    fixed = {
        **base,
        "area_code": receipt["area_code"],
        "atc_code": receipt["atc_code"],
        "atc_name": receipt["atc_name"],
        "meta_count": int(meta["count"]),
        "page": part["index"],
    }
    for name, value in fixed.items():
        columns[name] = [value] * count
    return pa.Table.from_pydict(columns, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 1,000 pages of a few kB."""
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)
    complete = sorted(
        e
        for e, rows in held.items()
        if extract.newest_complete(rows) and rows[-1]["ingested_at"] == extract.newest_complete(rows)[0]["ingested_at"]
    )
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "monthly_provisions",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in complete],
    )
