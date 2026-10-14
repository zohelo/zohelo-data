"""Eurostat bronze.load: incremental Landing → bronze.eurostat_full_observations.

Batches of batch.py (decode.py per file); see ingestion/common/bronze_load.py. Decoder
rejections are recorded in bronze.eurostat_rejected_files and skipped, never retried
silently (ops/retry_rejected clears them). One file can take ~1 h (AVIA_TF_ACA: 1.48B
cells), so no batch starts in the last hour of the budget. Before the data batches, the
bulk structure metadata → bronze.eurostat_structure_items (structure.py).
"""
from __future__ import annotations

import time

from ingestion.common.bronze_load import run_batches
from ingestion.common.report import annotate


def load(budget_minutes: float) -> int:
    # Structure metadata first: four bulk messages, minutes; the data batches can use the
    # whole budget on a few very large files.
    started = time.monotonic()
    from ingestion.to_bronze.eurostat import structure
    annotate("notice", "eurostat structure Bronze", structure.load(min(60.0, budget_minutes / 4)))
    left = budget_minutes - (time.monotonic() - started) / 60
    return run_batches("eurostat", "ingestion.to_bronze.eurostat.batch", "eurostat-bronze-batch",
                       left, batch_size=256, batch_seconds=2700, margin_seconds=3600)
