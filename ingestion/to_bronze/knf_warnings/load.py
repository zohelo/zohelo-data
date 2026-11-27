#!/usr/bin/env python3
"""KNF public warning list bronze.load: every landed copy of the list → bronze.knf_warnings_entries.

A Landing file is the list page reduced before Landing (ingestion/to_landing/knf_warnings/extract.py): one Bronze row
per entry, values as text, with the section's title, position and stated number of entries, the page's stated total
and provenance. The list is an accumulating snapshot (the page shows only the current list, history starts with the
first copy), so every landed copy is loaded (``all_versions``), not only the newest. A document of another shape
(source, fields, a section without its entries, a closed value outside its list) is rejected into
bronze.knf_warnings_rejected_files. Shared loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.knf_warnings import extract

SOURCE = extract.SOURCE
TABLE = "knf_warnings_entries"
COLUMNS = [
    ("entity", "VARCHAR"),
    ("section_position", "BIGINT"),
    ("section_title", "VARCHAR"),
    ("section_entries", "BIGINT"),
    *[(field, "VARCHAR") for field in extract.FIELDS],
    ("stated_total", "BIGINT"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    doc = json.loads(body)
    if doc.get("source") != SOURCE or doc.get("fields") != extract.FIELDS or receipt["entity"] != extract.ENTITY:
        raise ValueError(f"not a {SOURCE} list document")
    sections = {s["key"]: s for s in doc.get("sections", [])}
    if not doc.get("rows") or len(sections) != len(doc["sections"]):
        raise ValueError("list document without entries, or two sections with one key")
    if len(doc["rows"]) != doc.get("stated_total"):
        raise ValueError(f"{len(doc['rows'])} entries, the page stated {doc.get('stated_total')}")
    base = bulk_bronze.provenance(receipt)
    rows = []
    for n, row in enumerate(doc["rows"], start=1):
        if sorted(row) != sorted(extract.FIELDS) or row["section_key"] not in sections:
            raise ValueError(f"row {n}: fields {sorted(row)}, or an unknown section")
        kind, ruling = row["identifier_kind"], row["final_ruling_mentioned"]
        if kind not in extract.IDENTIFIER_KINDS or ruling not in ("true", "false"):
            raise ValueError(f"row {n}: identifier kind or ruling flag outside its closed list")
        section = sections[row["section_key"]]
        rows.append({**base, **row, "section_position": section["position"], "section_title": section["title"],
                     "section_entries": section["entries"], "stated_total": doc["stated_total"],
                     "source_row_number": n})  # fmt: skip
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(COLUMNS))


def load(landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, f"{SOURCE}_rejected_files"),
        COLUMNS,
        decode,
        "entries",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        all_versions=True,
        specs=[{"entity": extract.ENTITY}],
    )
