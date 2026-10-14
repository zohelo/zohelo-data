"""Eurostat bronze.load: incremental Landing → bronze.eurostat_full_observations.

Batches of batch.py (decode.py per file); see ingestion/common/bronze_load.py. Decoder
rejections are recorded in bronze.eurostat_rejected_files and skipped, never retried
silently (ops/retry_rejected clears them). One file can take ~1 h (AVIA_TF_ACA: 1.48B
cells), so no batch starts in the last hour of the budget. Then the landed structure
metadata → bronze.eurostat_structure_items (structure.py) within the remaining budget.
"""
from __future__ import annotations

import time

from ingestion.common.bronze_load import run_batches
from ingestion.common.report import annotate


def load(budget_minutes: float) -> int:
    started = time.monotonic()
    status = run_batches("eurostat", "ingestion.to_bronze.eurostat.batch", "eurostat-bronze-batch",
                         budget_minutes, batch_size=256, batch_seconds=2700, margin_seconds=3600)
    left = budget_minutes - (time.monotonic() - started) / 60 - 15
    if status in (0, 2) and left > 10:
        from ingestion.to_bronze.eurostat import structure
        annotate("notice", "eurostat structure Bronze", structure.load(left))
    return status
