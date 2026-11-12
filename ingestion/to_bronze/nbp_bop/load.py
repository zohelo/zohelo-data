#!/usr/bin/env python3
"""NBP balance of payments bronze.load: newest Landing copy of each workbook → bronze.nbp_bop_cells.

<<<<<<< HEAD
The three workbooks (bop_m, bop_q, iip_q) have one sheet per currency (PLN, EUR, USD; "IIP PLN" …), items
in rows and periods in columns: a header row of years (each year written once over its months or quarters)
and below it a row of period labels (I … XII, "I kw / I Q" … "IV kw / IV Q"). One Bronze row per filled
value cell, as source text, with the item's row label and its structural context: blank rows separate
blocks of items; the first row of a block is the block's head (its total). An account or total heading
standing alone ("Rachunek bieżący / Current account", "Ogółem aktywa / Assets", "Międzynarodowa pozycja
inwestycyjna netto") opens a section, so the same label under two heads or sections (equity under direct
investment assets and liabilities) stays distinguishable.

A workbook without the year and period header rows, or a sheet name the decoder does not know, rejects
the file (bronze.nbp_bop_rejected_files): a change of the NBP layout is never loaded silently. Columns to
the right of the period grid whose header is no period (none in the Dev probe) are not values of a
period and are counted, not loaded. Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
=======
One row per non-empty cell (sheet, row, column, source text, number format), decoded by the shared
ingestion/common/xlsx_cells.py; Silver (silver.nbp_bop_observations) reads the item rows and the year and
period header. A workbook whose sheets are not exactly the declared currency sheets (PLN, EUR, USD; the IIP
workbook "IIP PLN", "IIP EUR", "IIP USD") is rejected (bronze.nbp_bop_rejected_files), so a missing or
renamed currency is never loaded silently. Loader: ingestion/common/bulk_bronze.py.
>>>>>>> feat/2754-nbp-bop
"""

from __future__ import annotations

<<<<<<< HEAD
import io
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "nbp_bop"
TABLE_ID = ("bronze", "nbp_bop_cells")
REJECTED_ID = ("bronze", "nbp_bop_rejected_files")
SHEETS = {"PLN", "EUR", "USD", "IIP PLN", "IIP EUR", "IIP USD"}
YEAR = re.compile(r"^(19|20)\d{2}$")
SECTION = re.compile(r"^(rachunek|ogółem|międzynarodowa pozycja)", re.IGNORECASE)  # account and total headings
COLUMNS = [
    ("entity", "VARCHAR"),
    ("sheet", "VARCHAR"),
    ("source_row_number", "BIGINT"),  # Excel row of the item
    ("column_number", "BIGINT"),  # Excel column of the value (1 = A)
    ("section_label", "VARCHAR"),  # the latest account or total heading standing alone above the item
    ("block_label", "VARCHAR"),  # the first row of the item's block
    ("item_label", "VARCHAR"),
    ("year_text", "VARCHAR"),
    ("period_text", "VARCHAR"),
    ("value_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _header(rows: list[tuple]) -> tuple[int, int]:
    """Index of the year row and of the first period column."""
    for i, row in enumerate(rows[:30]):
        years = [j for j, cell in enumerate(row) if YEAR.match(_text(cell))]
        if len(years) >= 3:
            return i, years[0]
    raise ValueError("no header row of years")


def decode_sheet(name: str, rows: list[tuple], base: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    """Value cells of one sheet and the number of filled cells under a non-period header; pure."""
    year_row, first = _header(rows)
    if year_row + 1 >= len(rows):
        raise ValueError(f"sheet {name!r}: no period row under the years")
    years, periods = rows[year_row], rows[year_row + 1]
    width = max(len(r) for r in rows)
    grid: dict[int, tuple[str, str]] = {}
    year = ""
    for j in range(first, width):
        cell = _text(years[j]) if j < len(years) else ""
        if cell:
            year = cell if YEAR.match(cell) else ""
        period = _text(periods[j]) if j < len(periods) else ""
        if year and period:
            grid[j] = (year, period)
    if not grid:
        raise ValueError(f"sheet {name!r}: no period columns")
    out: list[dict[str, Any]] = []
    skipped = 0
    section = block = None
    block_rows: list[tuple[int, str]] = []

    def flush() -> None:
        nonlocal section, block
        if len(block_rows) == 1 and SECTION.match(block_rows[0][1]):
            section = block_rows[0][1]
        head = block_rows[0][1] if block_rows else None
        for index, label in block_rows:
            row = rows[index]
            for j in range(first, len(row)):
                value = _text(row[j])
                if not value:
                    continue
                if j not in grid:
                    continue
                out.append(
                    {
                        **base,
                        "sheet": name,
                        "source_row_number": index + 1,
                        "column_number": j + 1,
                        "section_label": section,
                        "block_label": head,
                        "item_label": label,
                        "year_text": grid[j][0],
                        "period_text": grid[j][1],
                        "value_text": value,
                    }
                )
        block_rows.clear()

    for index in range(year_row + 2, len(rows)):
        row = rows[index]
        label = " ".join(_text(c) for c in row[:first] if _text(c))
        if not label:
            if block_rows:
                flush()
            continue
        skipped += sum(1 for j in range(first, len(row)) if j not in grid and _text(row[j]))
        block_rows.append((index, label))
    if block_rows:
        flush()
    return out, skipped


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    import openpyxl

    workbook = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    unknown = set(workbook.sheetnames) - SHEETS
    if unknown:
        raise ValueError(f"unknown sheets {sorted(unknown)}")
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    for sheet in workbook.worksheets:
        cells, _ = decode_sheet(sheet.title, list(sheet.iter_rows(values_only=True)), base)
        rows += cells
    if not rows:
        raise ValueError("no value cells")
    return pa.Table.from_pylist(rows, schema=ARROW)
=======
from typing import Any

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "nbp_bop"
TABLE = "nbp_bop_cells"
REJECTED = "nbp_bop_rejected_files"
SHEETS = {
    "bop_m": {"PLN", "EUR", "USD"},
    "bop_q": {"PLN", "EUR", "USD"},
    "iip_q": {"IIP PLN", "IIP EUR", "IIP USD"},
}


def decode(body: bytes, receipt: dict[str, Any]):
    """xlsx_cells.decode, then the workbook's sheets must be exactly its declared currency sheets."""
    table = xlsx_cells.decode(body, receipt)
    found = set(table.column("sheet_name").to_pylist())
    expected = SHEETS.get(receipt["entity"])
    if expected is None:
        raise ValueError(f"undeclared entity {receipt['entity']!r}")
    if found != expected:
        raise ValueError(f"sheets {sorted(found)}, expected {sorted(expected)}")
    return table
>>>>>>> feat/2754-nbp-bop


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: three workbooks of a few hundred kB."""
    return bulk_bronze.load(
        SOURCE,
<<<<<<< HEAD
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
=======
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
>>>>>>> feat/2754-nbp-bop
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
