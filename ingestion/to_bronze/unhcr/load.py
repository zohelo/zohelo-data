#!/usr/bin/env python3
"""UNHCR bronze.load: newest Landing copy of every declared API selection → bronze.unhcr_population.

Each file is one JSON response of the Refugee Statistics API ``/population/v1/population/`` for one fixed selection
(ingestion/sources/unhcr.yaml): ``{"page", "maxPages", "total", "items": [{"year", "coo_id", "coo_name", "coo",
"coo_iso", "coa_id", "coa_name", "coa", "coa_iso", "refugees", ...}, ...]}``. One row per item, every value as the
text the API sends (numbers as JSON prints them, "-" kept). The selection must arrive in one page: a response with
``maxPages`` other than 1 is rejected, so a truncated selection never loads. An item with other fields, a response
that is not such an object, or one without items is rejected (bronze.unhcr_rejected_files). The standard JSON
parser reads it (about 8,000 items, as the ICP loader does). Code shared with other small bulk sources:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "unhcr"
TABLE_ID = ("bronze", "unhcr_population")
REJECTED_ID = ("bronze", "unhcr_rejected_files")
FIELDS = [
    "year",
    "coo_id",
    "coo_name",
    "coo",
    "coo_iso",
    "coa_id",
    "coa_name",
    "coa",
    "coa_iso",
    "refugees",
    "asylum_seekers",
    "returned_refugees",
    "idps",
    "returned_idps",
    "stateless",
    "ooc",
    "oip",
    "hst",
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in FIELDS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    page = json.loads(body.decode("utf-8"), parse_int=str, parse_float=str)  # numbers kept as published text
    if not isinstance(page, dict) or not isinstance(page.get("items"), list):
        raise ValueError(f"not a UNHCR population response: {str(page)[:200]}")
    if str(page.get("page")) != "1" or str(page.get("maxPages")) != "1":
        raise ValueError(f"selection not in one page: page {page.get('page')} of {page.get('maxPages')}")
    items = page["items"]
    if not items:
        raise ValueError("no items")
    columns: dict[str, list[Any]] = {name: [] for name in FIELDS}
    for number, item in enumerate(items, start=1):
        unknown = sorted(set(item) - set(FIELDS))
        if unknown:
            raise ValueError(f"item {number}: unknown fields {unknown}")
        for name in FIELDS:
            value = item.get(name)
            columns[name].append(None if value is None else str(value))
    count = len(items)
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    data["source_row_number"] = list(range(1, count + 1))
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two JSON responses of about 2 MB and 0.5 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "population",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
