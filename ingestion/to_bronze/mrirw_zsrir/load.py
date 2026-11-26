#!/usr/bin/env python3
"""MRiRW ZSRIR bronze.load: newest Landing copy of each market bulletin → bronze.mrirw_zsrir_cells.

One row per non-empty cell of the workbook (ingestion/common/xlsx_cells.py: value as stored, sheet, row, column, number
format); Silver picks the monthly sheet of each market (SHEETS). The bulletins are reports of about thirty sheets whose
names and order changed between 2024 and 2026, so the modelled sheet is found by its name and checked: exactly one sheet
whose name (trimmed) matches the market's pattern, and in its column B a cell whose text is the header of the monthly
grid. A workbook that misses either, or a file that is not an XLSX package, is rejected
(bronze.mrirw_zsrir_rejected_files), never guessed. Keep SHEETS equal to the CTE layouts of
warehouse/models/silver/mrirw_zsrir/mrirw_zsrir_prices.sql.
"""

from __future__ import annotations

import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "mrirw_zsrir"
TABLE = "mrirw_zsrir_cells"
REJECTED = "mrirw_zsrir_rejected_files"
# market: (sheet-name pattern, header text in column B of the monthly grid)
SHEETS = {
    "cereals": (r"ZAKUP WYKRESY MCE", "MC/ROK"),  # rows = months (Excel dates), columns = grains
    "milk": (r"MIESIĘCZNE CENY SKUPU 2004-20\d\d", "ROK"),  # rows = years, columns = months I-XII
    "pork": (r"ZAKUP ŻYWIEC WYKRES MSC", "MIESIĄC"),  # rows = months I-XII, columns = years
}


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    market = receipt["entity"]
    if market not in SHEETS:
        raise ValueError(f"entity {market!r} is not a declared market")
    pattern, header = SHEETS[market]
    table = xlsx_cells.decode(body, receipt)
    names = sorted({n for n in table.column("sheet_name").to_pylist() if re.fullmatch(pattern, n.strip())})
    if len(names) != 1:
        raise ValueError(f"unknown layout: {len(names)} sheets match {pattern!r} ({market}), one expected")
    found = any(
        sheet == names[0] and column == 2 and " ".join(str(text).split()) == header
        for sheet, column, text in zip(
            table.column("sheet_name").to_pylist(),
            table.column("column_number").to_pylist(),
            table.column("value_text").to_pylist(),
            strict=True,
        )
    )
    if not found:
        raise ValueError(f"unknown layout: no {header!r} header in column B of sheet {names[0]!r} ({market})")
    return table


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: three workbooks of 1-3 MB."""
    from ingestion.to_landing.mrirw_zsrir import MARKETS

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": m} for m in MARKETS],
    )
