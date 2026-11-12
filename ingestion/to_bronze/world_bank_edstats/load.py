#!/usr/bin/env python3
"""World Bank EdStats bronze.load: newest Landing copy of every declared PISA selection → bronze.edstats_observations.

Each file is one World Bank API v2 answer (ingestion/sources/world_bank_edstats.yaml) for the PISA mean-score
indicators of one subject in the Education Statistics database (source 12), every economy, 2000 onwards:
``[{"page", "pages", "per_page", "total", "sourceid", "lastupdated"}, [{"indicator": {"id", "value"}, "country":
{"id", "value"}, "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, ...]]``. One Bronze row per
record with a value (a null value is an absent observation), every field as text, the value as the exact JSON
number text. An answer over more than one page, an error message, or a record with other fields is rejected
(bronze.edstats_rejected_files). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "world_bank_edstats"
TABLE_ID = ("bronze", "edstats_observations")
REJECTED_ID = ("bronze", "edstats_rejected_files")
FIELDS = {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}
COLUMNS = [
    ("entity", "VARCHAR"),
    ("indicator_id", "VARCHAR"),  # LO.PISA.MAT, LO.PISA.MAT.FE, LO.PISA.MAT.MA, ...
    ("indicator_name", "VARCHAR"),
    ("country_id", "VARCHAR"),  # World Bank two-character code
    ("country_name", "VARCHAR"),
    ("country_iso3", "VARCHAR"),  # countryiso3code (aggregates have World Bank codes such as OED)
    ("year", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("obs_status", "VARCHAR"),
    ("source_last_updated", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    answer = json.loads(body, parse_float=str, parse_int=str)  # keep the number text exactly as published
    if not (isinstance(answer, list) and len(answer) == 2 and isinstance(answer[0], dict)):
        raise ValueError(f"not a World Bank API data answer: {str(answer)[:200]}")
    meta, records = answer
    if "message" in meta:
        raise ValueError(f"World Bank API error: {json.dumps(meta)[:300]}")
    if str(meta.get("pages")) != "1":
        raise ValueError(f"answer over {meta.get('pages')} pages")
    rows: dict[str, list[Any]] = {name: [] for name, _ in COLUMNS[1:11]}
    rows["source_row_number"] = []
    for number, record in enumerate(records or [], start=1):
        unknown = sorted(set(record) - FIELDS)
        if unknown:
            raise ValueError(f"record {number}: unknown fields {unknown}")
        if record.get("value") is None:
            continue
        rows["indicator_id"].append(record["indicator"]["id"])
        rows["indicator_name"].append(record["indicator"]["value"])
        rows["country_id"].append(record["country"]["id"])
        rows["country_name"].append(record["country"]["value"])
        rows["country_iso3"].append(record.get("countryiso3code") or None)
        rows["year"].append(str(record["date"]))
        rows["value_text"].append(str(record["value"]))
        rows["obs_status"].append(record.get("obs_status") or None)
        rows["source_last_updated"].append(meta.get("lastupdated"))
        rows["source_row_number"].append(number)
    count = len(rows["value_text"])
    if not count:
        raise ValueError("no observations")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(rows)
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: three answers of a few MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
