#!/usr/bin/env python3
"""knf_banking bronze.load: newest Landing copy of each declared workbook → bronze.knf_banking_cells.

One row per non-empty cell (sheet, row, column, source text, number format), decoded by the shared
ingestion/common/xlsx_cells.py; Silver picks the cells it models. A file that is not an XLSX workbook is
recorded in bronze.knf_banking_rejected_files, never loaded silently. Loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "knf_banking"
TABLE = "knf_banking_cells"
REJECTED = "knf_banking_rejected_files"


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two workbooks of a few MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        xlsx_cells.decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
