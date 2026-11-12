#!/usr/bin/env python3
"""FAOSTAT bronze.load: newest Landing copy of every declared domain zip → bronze.faostat_observations.

Each FAOSTAT bulk zip ("<Domain>_E_All_Data_(Normalized).zip") holds one data CSV,
``<Domain>_E_All_Data_(Normalized).csv``, with the columns Area Code, Area Code (M49), Area, Item Code, Item,
Element Code, Element, Year Code, Year, [Source Code, Source,] Unit, Value, Flag, Note, plus small code lists
(AreaCodes, Elements, Flags, ItemCodes, Sources) that repeat the labels of the data file and are not loaded.
One row per published row, every value as source text (``Area Code (M49)`` keeps FAO's leading apostrophe);
``source_code`` and ``source`` are null in domains without them. DuckDB reads the CSV (no per-cell Python).
A file that is not a zip, has no data CSV, or whose header differs from these columns is rejected
(bronze.faostat_rejected_files). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
from pathlib import Path
import tempfile
from typing import Any
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "faostat"
TABLE_ID = ("bronze", "faostat_observations")
REJECTED_ID = ("bronze", "faostat_rejected_files")
# Bronze column ← FAOSTAT header; the two Source columns exist only in some domains (e.g. GT).
HEADER = {
    "area_code": "Area Code",
    "area_code_m49": "Area Code (M49)",
    "area": "Area",
    "item_code": "Item Code",
    "item": "Item",
    "element_code": "Element Code",
    "element": "Element",
    "year_code": "Year Code",
    "year": "Year",
    "source_code": "Source Code",
    "source": "Source",
    "unit": "Unit",
    "value": "Value",
    "flag": "Flag",
    "note": "Note",
}
OPTIONAL = {"Source Code", "Source"}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in HEADER],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a zip file: {exc}") from exc
    data = [n for n in archive.namelist() if n.endswith("_All_Data_(Normalized).csv")]
    if len(data) != 1:
        raise ValueError(f"expected one *_All_Data_(Normalized).csv member, found {data}")
    with tempfile.TemporaryDirectory(prefix="zohelo-faostat-") as temporary:
        path = Path(temporary) / "data.csv"
        with archive.open(data[0]) as source, path.open("wb") as target:
            while chunk := source.read(1 << 22):
                target.write(chunk)
        with path.open(encoding="utf-8-sig", errors="strict") as f:
            header = f.readline().rstrip("\r\n")
        names = [h.strip('"') for h in header.split(",")]
        expected = [h for h in HEADER.values() if h not in OPTIONAL or h in names]
        if names != expected:
            raise ValueError(f"unknown FAOSTAT header: {names}")
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        selects = [
            f'"{HEADER[name]}" as {name}' if HEADER[name] in names else f"cast(null as varchar) as {name}"
            for name in HEADER
        ]
        prov = bulk_bronze.provenance(receipt)
        table = con.sql(
            f"""
            select
                ? as entity,
                {", ".join(selects)},
                ? as raw_sha256,
                ? as source_version,
                cast(? as timestamptz) as ingested_at,
                cast(row_number() over () + 1 as bigint) as source_row_number
            from read_csv(?, header = true, all_varchar = true, quote = '"', escape = '"', delim = ',',
                          strict_mode = true, null_padding = false, encoding = 'utf-8')
            """,
            params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
        ).to_arrow_table()
        con.close()
    if not table.num_rows:
        raise ValueError("no observations")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two domain zips (20 MB and 3 MB)."""
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
