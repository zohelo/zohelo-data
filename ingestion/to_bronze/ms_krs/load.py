#!/usr/bin/env python3
"""Open KRS API bronze.load: newest Landing copy of every part → bronze.ms_krs_extracts.

Each file holds one JSON line per KRS number: the entity-level record that ingestion/to_landing/ms_krs/extract.py
kept of the current extract (no natural persons, no addresses), or only ``http_status`` 204 or 404 for a number
without a current extract. One row per line, every value as text (the main-activity codes as a JSON array). A
line with a field outside the allowlist, without a ten-digit KRS number or repeating a number rejects the file
(bronze.ms_krs_rejected_files). Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import ms_krs as landing
from ingestion.to_landing.ms_krs.extract import FIELDS

SOURCE = "ms_krs"
TABLE_ID = ("bronze", "ms_krs_extracts")
REJECTED_ID = ("bronze", "ms_krs_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in FIELDS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))  # true/false, numbers, the code list


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    lines = body.decode("utf-8").splitlines()
    if not lines:
        raise ValueError("no records")
    columns: dict[str, list[Any]] = {name: [] for name in FIELDS}
    seen = set()
    for number, line in enumerate(lines, start=1):
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ValueError(f"line {number}: not an object")
        unknown = sorted(set(record) - set(FIELDS))
        if unknown:
            raise ValueError(f"line {number}: fields outside the allowlist {unknown}")
        krs = record.get("krs_number")
        if not (isinstance(krs, str) and len(krs) == 10 and krs.isascii() and krs.isdigit()) or krs in seen:
            raise ValueError(f"line {number}: KRS number missing, malformed or repeated")
        seen.add(krs)
        for name in FIELDS:
            columns[name].append(text(record.get(name)))
    count = len(lines)
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    data["source_row_number"] = list(range(1, count + 1))
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: 16 files of about 2 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "extracts",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=landing.specs(),
    )
