#!/usr/bin/env python3
"""UNdata bronze.load: newest Landing copy of every Statistical Yearbook table → bronze.undata_syb_values.

The UN Statistical Yearbook (SYB) tables on data.un.org are CSV files with a title line (table code and
title, e.g. ``T02,"Population, density and surface area"``), the header
``Region/Country/Area,,Year,Series,Value,Footnotes,Source`` and one row per area, year and series. Bronze keeps
every row as published: the table code and title, the area code (UN M49) and name, year, series, value (text,
thousands separators as published), footnotes and source. A file without that title line and header, or a
row of another width, is rejected (bronze.undata_rejected_files). Code shared with other small bulk sources:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "undata"
TABLE_ID = ("bronze", "undata_syb_values")
REJECTED_ID = ("bronze", "undata_rejected_files")
HEADER = ["Region/Country/Area", "", "Year", "Series", "Value", "Footnotes", "Source"]
FIELDS = ["area_code", "area_name", "year", "series", "value", "footnotes", "source_text"]
COLUMNS = [
    ("entity", "VARCHAR"),
    ("table_code", "VARCHAR"),  # T02
    ("table_title", "VARCHAR"),  # Population, density and surface area
    ("area_code", "VARCHAR"),  # UN M49 (1 = world, 616 = Poland)
    ("area_name", "VARCHAR"),
    ("year", "VARCHAR"),
    ("series", "VARCHAR"),  # Population mid-year estimates (millions)
    ("value", "VARCHAR"),  # "7,021.73" as published
    ("footnotes", "VARCHAR"),
    ("source_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = body.decode("cp1252")
    reader = csv.reader(io.StringIO(text, newline=""))
    title, header = next(reader, None), next(reader, None)
    if not title or not title[0].startswith("T") or len(title) < 2:
        raise ValueError(f"not a Statistical Yearbook title line: {title[:3] if title else title}")
    if not header or [h.strip() for h in header[:7]] != HEADER or any(h.strip() for h in header[7:]):
        raise ValueError(f"not a Statistical Yearbook header: {header[:8] if header else header}")
    base = bulk_bronze.provenance(receipt)
    data: dict[str, list[Any]] = {name: [] for name, _ in COLUMNS}
    for number, row in enumerate(reader, start=3):
        if not row or not any(row):
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        for name, value in zip(FIELDS, row[:7], strict=True):
            data[name].append(value)
        data["source_row_number"].append(number)
    count = len(data["area_code"])
    if not count:
        raise ValueError("no rows")
    data["table_code"], data["table_title"] = [title[0]] * count, [title[1]] * count
    for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
        data[name] = [base[name]] * count
    return pa.Table.from_pydict(data, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about thirty files of a few MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "syb_values",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
