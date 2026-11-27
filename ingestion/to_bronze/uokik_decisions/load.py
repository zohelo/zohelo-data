#!/usr/bin/env python3
"""UOKiK decisions bronze.load: the newest landed copy of every category → bronze.uokik_decisions_index.

A Landing file is one category of the view ``WWW-wszystkie`` of https://decyzje.uokik.gov.pl/bp/dec_prez.nsf,
reduced before Landing to the index fields (ingestion/to_landing/uokik_decisions/extract.py): one Bronze row per
decision, every value as text, with the category's stated number of entries (``descendants``) and provenance. A
document of another shape (source, fields, category entity, an empty @unid or number) is rejected into
bronze.uokik_decisions_rejected_files. Shared loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.uokik_decisions import extract

SOURCE = extract.SOURCE
TABLE = "uokik_decisions_index"
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(field, "VARCHAR") for field in extract.FIELDS],
    ("descendants", "BIGINT"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    doc = json.loads(body)
    if doc.get("source") != SOURCE or doc.get("fields") != extract.FIELDS:
        raise ValueError(f"not a {SOURCE} category document")
    if f"category_{extract.slug(str(doc.get('category', '')))}" != receipt["entity"]:
        raise ValueError(f"category {doc.get('category')!r} is not the entity {receipt['entity']}")
    if not doc.get("rows"):
        raise ValueError("category document without decisions")
    base = bulk_bronze.provenance(receipt)
    rows = []
    for n, row in enumerate(doc["rows"], start=1):
        if sorted(row) != sorted(extract.FIELDS) or not row["unid"] or not row["decision_number"]:
            raise ValueError(f"row {n}: fields {sorted(row)}, or an empty @unid or decision number")
        if row["category"] != doc["category"]:
            raise ValueError(f"row {n}: another category")
        rows.append({**base, **row, "descendants": doc.get("descendants"), "source_row_number": n})
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(COLUMNS))


def load(landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, f"{SOURCE}_rejected_files"),
        COLUMNS,
        decode,
        "decisions",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=extract.landed_specs(landing_root),
    )
