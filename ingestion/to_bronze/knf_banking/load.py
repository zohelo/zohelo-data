#!/usr/bin/env python3
"""knf_banking bronze.load: newest Landing copy of each declared workbook → bronze.knf_banking_cells.

One row per non-empty cell (sheet, row, column, source text, number format), decoded by the shared
ingestion/common/xlsx_cells.py; Silver picks the cells it models. Loader: ingestion/common/bulk_bronze.py.

Known layouts (Dev trial 43285080215), checked before a file is loaded:

  current  sheets Info, Struktura, Bilans, Należności_1 to Należności_5, Zobowiązania, Pozabilans, RZiS, Hipoteki,
           Płynność, Adekwatność; every table sheet prints its unit in A3:A4 ("Dane w" / "zł" or "sztukach") and has
           its first header row (month or quarter ends from column B on) at row 9
  archive  sheets SPIS TABEL, Tabela 01 to Tabela 11 and Tabela 07a; first header row at row 10

A file that is not an XLSX workbook, has another set of sheets, or a table sheet without that header is recorded in
bronze.knf_banking_rejected_files with the reason, never guessed.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "knf_banking"
TABLE = "knf_banking_cells"
REJECTED = "knf_banking_rejected_files"
CURRENT_TABLES = [
    "Struktura",
    "Bilans",
    *(f"Należności_{n}" for n in range(1, 6)),
    "Zobowiązania",
    "Pozabilans",
    "RZiS",
    "Hipoteki",
    "Płynność",
    "Adekwatność",
]
ARCHIVE_TABLES = [*(f"Tabela {n:02d}" for n in range(1, 12)), "Tabela 07a"]
LAYOUTS = {
    "current": {"index": "Info", "tables": CURRENT_TABLES, "header_row": 9, "units": {"zł", "sztukach"}},
    "archive": {"index": "SPIS TABEL", "tables": ARCHIVE_TABLES, "header_row": 10, "units": None},
}
MIN_DATES = 3  # a header row holds at least three date cells (Silver uses the same rule)


def _is_date(cell: dict[str, Any]) -> bool:
    fmt = cell["number_format"] or ""
    return cell["cell_type"] == "n" and "y" in fmt.lower() and "#" not in fmt


def check_layout(entity: str, rows: list[dict[str, Any]]) -> None:
    """Raise ValueError when the workbook is not the known layout of its entity."""
    layout = LAYOUTS.get(entity)
    if layout is None:
        raise ValueError(f"unknown entity {entity!r}: no known layout")
    expected = {layout["index"], *layout["tables"]}
    found = {r["sheet_name"] for r in rows}
    if found != expected:
        raise ValueError(
            f"{entity}: unknown sheet layout: missing {sorted(expected - found)}, unexpected {sorted(found - expected)}"
        )
    problems = []
    texts = {(r["sheet_name"], r["cell_ref"]): (r["value_text"] or "").strip() for r in rows if r["column_number"] == 1}
    for sheet in layout["tables"]:
        header = [
            r
            for r in rows
            if r["sheet_name"] == sheet and r["row_number"] == layout["header_row"] and r["column_number"] >= 2
        ]
        dates = sum(1 for r in header if _is_date(r))
        if dates < MIN_DATES:
            problems.append(f"{sheet}: {dates} date cells in header row {layout['header_row']}")
        if layout["units"] is not None:
            a3, a4 = texts.get((sheet, "A3")), texts.get((sheet, "A4"))
            if a3 != "Dane w" or a4 not in layout["units"]:
                problems.append(f"{sheet}: unit cells A3:A4 = {a3!r} / {a4!r}")
    if problems:
        raise ValueError(f"{entity}: unknown header layout: {'; '.join(problems)[:800]}")


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    table = xlsx_cells.decode(body, receipt)
    columns = ["sheet_name", "row_number", "column_number", "cell_ref", "cell_type", "value_text", "number_format"]
    check_layout(receipt["entity"], table.select(columns).to_pylist())
    return table


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two workbooks of a few MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
