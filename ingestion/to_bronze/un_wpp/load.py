#!/usr/bin/env python3
"""UN WPP bronze.load: newest Landing copy of every declared CSV (gzip) → bronze.un_wpp_rows.

Generic CSV rows as published: one header row per file (``source_row_number`` 1, ``field_names`` = the header,
comma-separated, ``field_values`` null) and one row per data row (``field_values`` = the row's values,
``|``-separated, ``field_names`` null, so the header is stored once per file, not on 1.76 million rows). A revision
that adds or reorders columns still loads; Silver joins each row to its file's header and picks fields by name.
Values stay source text. A file that is not gzip, has no LocID/Time header, a row of another width than the
header, or a value containing ``|`` is rejected (bronze.un_wpp_rejected_files). The demographic indicators file
is about 84,000 rows, the population by five-year age group file about 1.76 million (190 MB uncompressed): both
decode in memory. Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import gzip
import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "un_wpp"
TABLE_ID = ("bronze", "un_wpp_rows")
REJECTED_ID = ("bronze", "un_wpp_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("field_names", "VARCHAR"),  # header row only: SortOrder,LocID,Notes,ISO3_code,ISO2_code,…,Time,TPopulation1Jan,…
    ("field_values", "VARCHAR"),  # data rows only: 189|616||POL|PL|…|2023|38813.949|…
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
REQUIRED = ("LocID", "Location", "Time")


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        text = gzip.decompress(body).decode("utf-8-sig")
    except (OSError, EOFError) as exc:
        raise ValueError(f"not a gzip file: {exc}") from exc
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if not header or any(name not in header for name in REQUIRED):
        raise ValueError(f"not a WPP CSV header: {header[:8] if header else header}")
    if any("," in name or "|" in name for name in header):
        raise ValueError("a column name contains a separator")
    names: list[str | None] = [",".join(header)]
    values: list[str | None] = [None]
    numbers: list[int] = [1]
    for number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        joined = "|".join(row)
        if joined.count("|") != len(header) - 1:
            raise ValueError(f"line {number}: a value contains '|'")
        names.append(None)
        values.append(joined)
        numbers.append(number)
    count = len(values)
    if count == 1:
        raise ValueError("no rows")
    base = bulk_bronze.provenance(receipt)
    data = {name: [base[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update({"field_names": names, "field_values": values, "source_row_number": numbers})
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two files, the larger about 30 MB of gzip."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "rows",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
