#!/usr/bin/env python3
"""ZUS statistical portal bronze.load: newest Landing copy of each family × period → bronze.zus_psz_cells.

One row per non-empty cell (ingestion/common/xlsx_cells.py: value as stored, sheet, row, column, number format) of
each landed workbook; Silver picks the cells it models. The family's header row must be there: column A of some row
starts with the family's anchor text (ingestion/to_landing/zus_psz FAMILIES, whitespace collapsed), and every cell is on
one worksheet (Silver joins cells by row and column). A workbook without the anchor, with cells on more than one
worksheet, or a file that is not an XLSX package, is rejected (bronze.zus_psz_rejected_files), never guessed.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "zus_psz"
TABLE = "zus_psz_cells"
REJECTED = "zus_psz_rejected_files"


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    from ingestion.to_landing.zus_psz import FAMILIES, family_of

    family = family_of(receipt["entity"])
    if family not in FAMILIES:
        raise ValueError(f"entity {receipt['entity']!r} belongs to no declared family")
    table = xlsx_cells.decode(body, receipt)
    sheets = sorted(set(table.column("sheet_name").to_pylist()))
    if len(sheets) != 1:
        raise ValueError(f"unknown layout: {len(sheets)} worksheets with cells {sheets[:5]}, one expected ({family})")
    anchor = FAMILIES[family]["anchor"]
    texts = (
        " ".join(str(v).split())
        for v, col in zip(
            table.column("value_text").to_pylist(), table.column("column_number").to_pylist(), strict=True
        )
        if col == 1
    )
    if not any(t.startswith(anchor) for t in texts):
        raise ValueError(f"unknown layout: no header row starting {anchor!r} in column A ({family})")
    return table


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about fifty workbooks of 10-40 kB."""
    from ingestion.to_landing import zus_psz as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held_entities(landing_root))],
    )
