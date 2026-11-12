#!/usr/bin/env python3
"""NBP balance of payments bronze.load: newest Landing copy of each workbook → bronze.nbp_bop_cells.

One row per non-empty cell (sheet, row, column, source text, number format), decoded by the shared
ingestion/common/xlsx_cells.py; Silver (silver.nbp_bop_observations) reads the item rows and the year and
period header. A workbook whose sheets are not exactly the declared currency sheets (PLN, EUR, USD; the IIP
workbook "IIP PLN", "IIP EUR", "IIP USD") is rejected (bronze.nbp_bop_rejected_files), so a missing or
renamed currency is never loaded silently. Loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

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


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: three workbooks of a few hundred kB."""
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
