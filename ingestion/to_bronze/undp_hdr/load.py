#!/usr/bin/env python3
"""UNDP HDR bronze.load: newest Landing copy of the composite indices time series → bronze.undp_hdr_values.

The Human Development Data Center publishes one wide CSV per report (``HDR25_Composite_indices_complete_
time_series.csv``): the id columns iso3, country, hdicode, region, then one column per index component and
year (``hdi_1990`` … ``hdi_2023``, ``le_2023``, ``gii_rank_2023`` …). Bronze keeps every cell as published,
one row per country row × value column (empty cells skipped): the id columns, the source column name and the
value as text. A file whose header does not start with the id columns, or a row of another width, is rejected
(bronze.undp_hdr_rejected_files). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "undp_hdr"
TABLE_ID = ("bronze", "undp_hdr_values")
REJECTED_ID = ("bronze", "undp_hdr_rejected_files")
ID_COLUMNS = ["iso3", "country", "hdicode", "region"]
COLUMNS = [
    ("entity", "VARCHAR"),
    ("iso3", "VARCHAR"),  # ISO 3166-1 alpha-3, or a ZZ… aggregate (regions, HDI groups, world)
    ("country", "VARCHAR"),
    ("hdicode", "VARCHAR"),  # Very High / High / Medium / Low
    ("region", "VARCHAR"),  # UNDP region code (aggregates and some countries: empty)
    ("column_name", "VARCHAR"),  # hdi_2023, le_f_2023, gii_rank_2023 …
    ("value", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = body.decode("cp1252")  # earlier releases were not UTF-8
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if not header or header[:4] != ID_COLUMNS or len(header) < 5:
        raise ValueError(f"not an HDR composite indices header: {header[:6] if header else header}")
    if len(set(header)) != len(header):
        raise ValueError("duplicate column names")
    base = bulk_bronze.provenance(receipt)
    data: dict[str, list[Any]] = {name: [] for name, _ in COLUMNS}
    for number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        for name, value in zip(header[4:], row[4:], strict=True):
            if value == "":
                continue
            for key, item in zip(ID_COLUMNS, row[:4], strict=True):
                data[key].append(item)
            data["column_name"].append(name)
            data["value"].append(value)
            data["source_row_number"].append(number)
    count = len(data["value"])
    if not count:
        raise ValueError("no values")
    for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
        data[name] = [base[name]] * count
    return pa.Table.from_pydict(data, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one file of about 2 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "values",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
