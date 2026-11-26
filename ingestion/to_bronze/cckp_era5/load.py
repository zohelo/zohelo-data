#!/usr/bin/env python3
"""CCKP ERA5 bronze.load: newest Landing copy of the ERA5 selection → bronze.cckp_era5_records.

The Landing file (ingestion/to_landing/cckp_era5/) is a ZIP of exact answers ``<var>_<agg>_<area>.json`` for tas
and pr, monthly and annual, ``all_countries`` and ``POL_adm1``. One Bronze row per geocode x period of an answer:
variable, aggregation and the dataset id's period from the member name and the receipt, the value as its JSON text
(null kept as null).

A member name outside these patterns, a missing member, an answer that is not ``{"metadata": {"status":
"success"}, "data": {geocode: {period: value}}}``, an empty answer, a period key that is not ``YYYY-MM`` or a value
that is an object rejects the file into bronze.cckp_era5_rejected_files (never guessed).
"""

from __future__ import annotations

import io
import json
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.cckp_era5 import extract

SOURCE = "cckp_era5"
TABLE = "cckp_era5_records"
REJECTED = "cckp_era5_rejected_files"
VALUE_COLUMNS = ["variable", "aggregation", "dataset_period", "geocode", "period", "value", "member"]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in VALUE_COLUMNS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the row in its member, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
DATA = re.compile(r"(tas|pr)_(monthly|annual)_(all_countries|POL_adm1)\.json")
PERIOD = re.compile(r"\d{4}-\d{4}")
KEY = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
EXPECTED = {
    extract.member_name(v, a, area)
    for v in extract.VARIABLES
    for a in extract.AGGREGATIONS
    for area in ("all_countries", f"{extract.COUNTRY}_adm1")
}


def text(value: Any) -> str | None:
    """A published value as text: numbers as their JSON text, strings unchanged, null as null."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"value is not a number or text: {str(value)[:80]}")
    return json.dumps(value)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    period = str(receipt["version_token"]).split("_")[0]
    if not PERIOD.fullmatch(period):
        raise ValueError(f"version {receipt['version_token']} does not start with a period")
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = {name: json.loads(archive.read(name)) for name in archive.namelist()}
    unknown = set(members) - EXPECTED
    missing = EXPECTED - set(members)
    if unknown or missing:
        raise ValueError(f"members unknown {sorted(unknown)}, missing {sorted(missing)}")
    rows: list[dict[str, Any]] = []
    for name in sorted(EXPECTED):
        variable, aggregation, _ = DATA.fullmatch(name).groups()
        answer = members[name]
        if not isinstance(answer, dict) or (answer.get("metadata") or {}).get("status") != "success":
            raise ValueError(f"{name}: not a success answer")
        data = answer.get("data")
        if not isinstance(data, dict) or not data:
            raise ValueError(f"{name}: empty or not an object")
        n = 0
        for geocode, series in data.items():
            if not isinstance(series, dict):
                raise ValueError(f"{name}: {geocode} is not an object")
            for key, value in series.items():
                if not KEY.fullmatch(key):
                    raise ValueError(f"{name}: period key {key!r}")
                n += 1
                rows.append(
                    {
                        "variable": variable,
                        "aggregation": aggregation,
                        "dataset_period": period,
                        "geocode": geocode,
                        "period": key,
                        "value": text(value),
                        "member": name,
                        "source_row_number": n,
                    }
                )
    base = bulk_bronze.provenance(receipt)
    data = {c: [base[c]] * len(rows) for c in ("entity", "raw_sha256", "source_version", "ingested_at")}
    for column in (*VALUE_COLUMNS, "source_row_number"):
        data[column] = [r.get(column) for r in rows]
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one file of a few MB."""
    del budget_minutes
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "records",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": extract.ENTITY}],
    )
