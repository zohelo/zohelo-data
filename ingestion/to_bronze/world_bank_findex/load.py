#!/usr/bin/env python3
"""World Bank Global Findex bronze.load: newest Landing copy of the country-level CSV → bronze.findex_observations.

The file is wide: one row per economy (or aggregate) × survey year × population group, then one column per
indicator (shares 0–1, ``NA`` when not asked or not published). Bronze keeps it long, one row per row ×
indicator with a value: the row's key columns (economy name and code, year, adult population, region, income
group, group, group2) and the indicator id with its value as text. ``NA`` and empty cells are absent
observations and are not stored. The key columns are found by name prefix (the edition suffix of
``regionwb24_hi`` / ``incomegroupwb24`` changes with each edition); a file without them is rejected
(bronze.findex_rejected_files). The CSV is served as ISO-8859-2: decoded as UTF-8 when valid, else ISO-8859-2.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "world_bank_findex"
TABLE = "findex_observations"
REJECTED = "findex_rejected_files"
COLUMNS = [
    ("entity", "VARCHAR"),
    ("country_name", "VARCHAR"),
    ("country_code", "VARCHAR"),
    ("year", "VARCHAR"),
    ("pop_adult", "VARCHAR"),
    ("region", "VARCHAR"),
    ("income_group", "VARCHAR"),
    ("group_name", "VARCHAR"),  # source column `group`: all, gender, age_cat, income, education, laborforce, urbanicity
    ("group_value", "VARCHAR"),  # source column `group2`: all, women, men, ages 15-24, poorest 40% …
    ("indicator", "VARCHAR"),  # source column name, e.g. account_t_d
    ("value_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
# Bronze column → test on the header name (exact name or prefix).
KEYS = {
    "country_name": lambda h: h.startswith("countrynew") or h == "economy",
    "country_code": lambda h: h in ("codewb", "economycode"),
    "year": lambda h: h == "year",
    "pop_adult": lambda h: h == "pop_adult",
    "region": lambda h: h.startswith("regionwb"),
    "income_group": lambda h: h.startswith("incomegroupwb"),
    "group_name": lambda h: h == "group",
    "group_value": lambda h: h == "group2",
}
MISSING = ("", "NA")


def text_of(body: bytes) -> str:
    try:
        return body.decode("utf-8-sig")
    except UnicodeDecodeError:
        return body.decode("iso-8859-2")


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    reader = csv.reader(io.StringIO(text_of(body), newline=""))
    header = [h.strip() for h in next(reader, [])]
    at: dict[str, int] = {}
    for i, h in enumerate(header):
        for name, test in KEYS.items():
            if test(h):
                if name in at:
                    raise ValueError(f"two columns for {name}: {header[at[name]]}, {h}")
                at[name] = i
    if set(at) != set(KEYS):
        raise ValueError(f"key columns missing: {sorted(set(KEYS) - set(at))} in {header[:10]}")
    indicators = [(i, h) for i, h in enumerate(header) if i not in at.values()]
    if not indicators:
        raise ValueError("no indicator columns")
    keys = list(KEYS)
    columns: dict[str, list[Any]] = {n: [] for n in (*keys, "indicator", "value_text", "source_row_number")}
    for number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        values = [row[at[n]] for n in keys]
        for i, indicator in indicators:
            if row[i] not in MISSING:
                for n, v in zip(keys, values, strict=True):
                    columns[n].append(v)
                columns["indicator"].append(indicator)
                columns["value_text"].append(row[i])
                columns["source_row_number"].append(number)
    count = len(columns["indicator"])
    if not count:
        raise ValueError("no observations")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one CSV of about 18 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
