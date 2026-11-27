#!/usr/bin/env python3
"""WTO Stats bronze.load: newest Landing copy of the OECD-WTO BaTIS BPM6 ZIP → two Bronze tables.

The ZIP (``OECD-WTO_BATIS_data_BPM6-1.zip``, 534 MB) holds ``Bulk download/OECD-WTO_BATIS_BPM6_<Month><Year>_bulk.csv``
(3.1 GB, 52.1 million rows: every reporter × partner × flow × service item × year of the balanced matrix), the codes
workbook ``…_codes.xlsx`` and the methodology PDF.

- ``bronze.wto_stats_batis``: the CSV rows of the selection, every value as source text, column names lower-cased,
  plus the CSV member name (it carries the release, e.g. November2026). Selection (register GL-020, modelling notes):
  rows with Poland as reporter or as partner, and rows whose reporter is a country group (World, EU, EU extra-trade);
  about 1 % of the file. The other bilateral rows are not stored (52 M rows of no use to a Poland-first platform).
  DuckDB reads the CSV from disk (no per-cell Python) and counts every row, so the annotation states rows read and
  rows kept.
- ``bronze.wto_stats_batis_codes``: every non-empty cell of the codes workbook (sheets notes, economies, flow, service
  items, type, methodology codes) as source text (ingestion/common/xlsx_cells.py); Silver takes labels from it.

A ZIP without exactly one ``…_bulk.csv`` and one ``…_codes.xlsx``, a CSV header other than the twelve published
columns, a malformed row, or a workbook whose economies, service items or methodology codes header differs is
rejected (bronze.wto_stats_rejected_files), never guessed. Code shared with other bulk sources:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
from pathlib import Path
import re
import tempfile
from typing import Any
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells
from ingestion.common.report import annotate

SOURCE = "wto_stats"
ENTITY = "batis_bpm6"
TABLE = "wto_stats_batis"
CODES = "wto_stats_batis_codes"
REJECTED = "wto_stats_rejected_files"
HEADER = [
    "Reporter",
    "type_Reporter",
    "Partner",
    "type_Partner",
    "Flow",
    "Item_code",
    "type_Item",
    "Year",
    "Reported_value",
    "Final_value",
    "Final_value_methodology",
    "Balanced_value",
]
SELECTION = "\"Reporter\" = 'PL' or \"Partner\" = 'PL' or \"type_Reporter\" = 'g'"
CSV_MEMBER = re.compile(r"(^|/)OECD-WTO_BATIS_BPM6_[A-Za-z]+[0-9]{4}_bulk\.csv$")
XLSX_MEMBER = re.compile(r"(^|/)OECD-WTO_BATIS_BPM6_[A-Za-z]+[0-9]{4}_codes\.xlsx$")
CODE_HEADERS = {  # sheet → the first cells of its row 1, as published (December 2025 and November 2026 workbooks)
    "economies": ["Code", "Code description", "Type"],
    "service items": ["Item_code", "Description", "Type"],
    "methodology codes": ["Methodology", "Description"],
}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name.lower(), "VARCHAR") for name in HEADER],
    ("csv_member", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
]
CODE_COLUMNS = [
    ("entity", "VARCHAR"),
    ("workbook", "VARCHAR"),
    ("sheet_name", "VARCHAR"),
    ("row_number", "BIGINT"),
    ("column_number", "BIGINT"),
    ("value_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
CODE_ARROW = bulk_bronze.arrow_schema(CODE_COLUMNS)
COUNTS: dict[str, dict[str, int]] = {}  # raw_sha256 → rows read and kept, for the load annotation


def member(archive: zipfile.ZipFile, pattern: re.Pattern) -> str:
    names = [n for n in archive.namelist() if pattern.search(n)]
    if len(names) != 1:
        raise ValueError(f"expected one member like {pattern.pattern}, found {names} in {archive.namelist()[:8]}")
    return names[0]


def open_zip(body: bytes) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    """The selected CSV rows as text (DuckDB on the extracted CSV)."""
    archive = open_zip(body)
    name = member(archive, CSV_MEMBER)
    member(archive, XLSX_MEMBER)
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-wto-stats-") as temporary:
        path = Path(temporary) / "batis.csv"
        with archive.open(name) as source, path.open("wb") as target:
            while chunk := source.read(16 << 20):
                target.write(chunk)
        with path.open("rb") as handle:
            first = handle.readline().decode("utf-8-sig").rstrip("\r\n")
        names = [h.strip().strip('"') for h in first.split(",")]
        if names != HEADER:
            raise ValueError(f"unknown BaTIS CSV header: {names}")
        con = duckdb.connect()
        con.execute(f"set temp_directory = '{temporary}'")
        source_csv = (
            "read_csv(?, header = true, all_varchar = true, quote = '\"', escape = '\"', delim = ',', "
            "strict_mode = true, null_padding = false)"
        )
        try:
            total = con.execute(f"select count(*) from {source_csv}", [str(path)]).fetchone()[0]
            table = con.execute(
                f"""
                select
                    ? as entity,
                    {", ".join(f'"{h}" as {h.lower()}' for h in HEADER)},
                    ? as csv_member,
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at
                from {source_csv}
                where {SELECTION}
                """,
                [prov["entity"], name, prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"CSV not readable: {exc}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows of the selection")
    COUNTS[receipt["sha256"]] = {"csv_rows": total, "kept_rows": table.num_rows}
    return table.cast(ARROW)


def decode_codes(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    """Every non-empty cell of the codes workbook, with a header check of the sheets Silver reads."""
    archive = open_zip(body)
    name = member(archive, XLSX_MEMBER)
    cells = list(xlsx_cells.cells(archive.read(name)))
    for sheet, expected in CODE_HEADERS.items():
        found = [
            c["value_text"]
            for c in sorted(cells, key=lambda c: c["column_number"])
            if c["sheet_name"] == sheet and c["row_number"] == 1
        ][: len(expected)]
        if found != expected:
            raise ValueError(f"codes workbook sheet {sheet!r}: header {found}, expected {expected}")
    prov = bulk_bronze.provenance(receipt)
    data = {
        "entity": [prov["entity"]] * len(cells),
        "workbook": [name] * len(cells),
        "sheet_name": [c["sheet_name"] for c in cells],
        "row_number": [c["row_number"] for c in cells],
        "column_number": [c["column_number"] for c in cells],
        "value_text": [c["value_text"] for c in cells],
        **{k: [prov[k]] * len(cells) for k in ("raw_sha256", "source_version", "ingested_at")},
    }
    return pa.Table.from_pydict(data, schema=CODE_ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one ZIP, read once per table (a new BaTIS release a year, revisions in between)."""
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    common = {"landing_root": landing_root, "lake_root": lake_root, "specs": [{"entity": ENTITY}]}
    status = bulk_bronze.load(SOURCE, (namespace, TABLE), (namespace, REJECTED), COLUMNS, decode, "batis", **common)
    if COUNTS:
        annotate("notice", f"{SOURCE} BaTIS rows read and kept", {"selection": SELECTION, "files": COUNTS})
    return max(
        status,
        bulk_bronze.load(
            SOURCE, (namespace, CODES), (namespace, REJECTED), CODE_COLUMNS, decode_codes, "batis_codes", **common
        ),
    )
