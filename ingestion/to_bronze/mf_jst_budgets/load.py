#!/usr/bin/env python3
"""MF local-government budget summaries bronze.load: newest copy of each summary workbook → bronze.mf_jst_budgets_cells.

One row per non-empty cell of the workbook (ingestion/common/xlsx_cells.py: value as stored, sheet, row, column,
number format), partitioned by entity (``<type>_<dw|zn>_<year>_k<quarter>``). A file that is not an XLSX workbook,
or holds no cell, is rejected (bronze.mf_jst_budgets_rejected_files).
"""

from __future__ import annotations

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "mf_jst_budgets"
TABLE_ID = ("bronze", "mf_jst_budgets_cells")
REJECTED_ID = ("bronze", "mf_jst_budgets_rejected_files")


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 20 workbooks of 30 kB a quarter."""
    from ingestion.to_landing import mf_jst_budgets as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        xlsx_cells.COLUMNS,
        xlsx_cells.decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held(landing_root))],
    )
