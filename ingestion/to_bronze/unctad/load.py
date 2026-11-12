#!/usr/bin/env python3
"""UNCTAD bronze.load: newest Landing copy of every declared Data Hub table (7z of one CSV) → bronze.unctad_rows.

Generic CSV rows, one per data row of the table's CSV as published: ``field_names`` holds the header and
``field_values`` the row's values, both ``|``-separated (UNCTAD column names contain commas, e.g. "Growth rate,
year-on-year"), so a table that adds or reorders columns still loads; Silver picks fields by name. Values stay
source text. The 7z archive is extracted with the runner's 7-Zip tool (the Landing check uses the same tool). An
archive that does not hold exactly one CSV, a header without Year/Period/Quarter first, a row of another width
than the header, or a name or value containing ``|`` is rejected (bronze.unctad_rejected_files). The largest table
(services trade by category) is about 808,000 rows, 147 MB uncompressed. Code shared with other small bulk
sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "unctad"
TABLE_ID = ("bronze", "unctad_rows")
REJECTED_ID = ("bronze", "unctad_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("field_names", "VARCHAR"),  # Year|Economy|Economy Label|Flow|Flow Label|Millions of US$ at current prices|…
    ("field_values", "VARCHAR"),  # 2024|616|Poland|02|Exports|382844.666|…
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
TIME_FIELDS = ("Year", "Period", "Quarter")


def extract(body: bytes) -> bytes:
    """The single CSV member of a 7z archive, as bytes."""
    tool = shutil.which("7z") or shutil.which("7za")
    if not tool:
        raise RuntimeError("7z is not installed")
    with tempfile.TemporaryDirectory(prefix="zohelo-unctad-") as temporary:
        archive = Path(temporary) / "table.7z"
        archive.write_bytes(body)
        listing = subprocess.run([tool, "l", "-slt", str(archive)], capture_output=True, text=True, timeout=600)
        members = [ln[7:] for ln in listing.stdout.splitlines() if ln.startswith("Path = ")][1:]
        if listing.returncode != 0 or len(members) != 1 or not members[0].lower().endswith(".csv"):
            raise ValueError(f"not a 7z archive of one CSV: {members[:4]} {listing.stderr[-200:]}")
        out = subprocess.run([tool, "x", "-so", str(archive)], capture_output=True, timeout=1800)
        if out.returncode != 0:
            raise ValueError(f"7z extraction failed: {out.stderr[-200:]!r}")
        return out.stdout


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    reader = csv.reader(io.StringIO(extract(body).decode("utf-8-sig"), newline=""))
    header = next(reader, None)
    if not header or header[0] not in TIME_FIELDS:
        raise ValueError(f"not a Data Hub table header: {header[:6] if header else header}")
    if any("|" in name for name in header):
        raise ValueError("a column name contains '|'")
    values: list[str] = []
    numbers: list[int] = []
    for number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        joined = "|".join(row)
        if joined.count("|") != len(header) - 1:
            raise ValueError(f"line {number}: a value contains '|'")
        values.append(joined)
        numbers.append(number)
    count = len(values)
    if not count:
        raise ValueError("no rows")
    base = {**bulk_bronze.provenance(receipt), "field_names": "|".join(header)}
    data = {
        name: [base[name]] * count for name in ("entity", "field_names", "raw_sha256", "source_version", "ingested_at")
    }
    data.update({"field_values": values, "source_row_number": numbers})
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: nine archives, 14 MB of 7z in all."""
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
