#!/usr/bin/env python3
"""World Bank GEM bronze.load: newest Landing copy of every declared series → bronze.gem_observations.

Each Landing file is the World Bank API v2 answer for one Global Economic Monitor series (source 15) for the EU27
members and every period (ingestion/to_landing/world_bank_gem/). One Bronze row per record with a value: the codes
and labels of its three concepts (Country, Series, Time) and the value as the exact JSON number text. Records with a
null value are absent observations and are not stored. A page that is not a source-15 answer, an answer over more
than one page, or a record with another concept rejects the file (bronze.gem_rejected_files). Entities are the
series folders found in Landing (``series_<id>``).
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3
from ingestion.to_landing.world_bank_gem import extract

SOURCE = "world_bank_gem"
TABLE = "gem_observations"
REJECTED = "gem_rejected_files"
CONCEPTS = {"Country": "country", "Series": "series", "Time": "time"}
COLUMNS = [
    ("entity", "VARCHAR"),
    ("country_code", "VARCHAR"),  # ISO 3166-1 alpha-3
    ("country_name", "VARCHAR"),
    ("series_code", "VARCHAR"),  # e.g. CPTOTSAXNZGY
    ("series_name", "VARCHAR"),
    ("time_code", "VARCHAR"),  # 2025, 2025Q3, 2025M07
    ("time_value", "VARCHAR"),
    ("value_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the record in the page, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
NAMES = [n for n, _ in COLUMNS[1:7]]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    page = json.loads(body, parse_float=str, parse_int=str)  # keep the number text exactly as published
    if not isinstance(page, dict) or str((page.get("source") or {}).get("id")) != str(extract.SOURCE_ID):
        raise ValueError(f"not a source {extract.SOURCE_ID} answer: {str(page)[:200]}")
    if str(page.get("pages")) != "1":
        raise ValueError(f"answer over {page.get('pages')} pages")
    columns: dict[str, list[Any]] = {n: [] for n in (*NAMES, "value_text", "source_row_number")}
    for number, record in enumerate(page["source"]["data"], start=1):
        value = record.get("value")
        if value is None:
            continue
        found = {}
        for variable in record["variable"]:
            concept = CONCEPTS.get(variable["concept"])
            if concept is None:
                raise ValueError(f"record {number}: unknown concept {variable['concept']!r}")
            found[f"{concept}_code"], found[f"{concept}_name"] = variable["id"], variable["value"]
        found["time_value"] = found.pop("time_name", None)
        if set(found) != set(NAMES):
            raise ValueError(f"record {number}: concepts {sorted(found)}")
        for n in NAMES:
            columns[n].append(found[n])
        columns["value_text"].append(str(value))
        columns["source_row_number"].append(number)
    count = len(columns["value_text"])
    if not count:
        raise ValueError("no observations")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: eight answers of about 4 MB."""
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)
    entities = sorted({r["entity"] for rows in held.values() for r in rows})
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in entities],
    )
