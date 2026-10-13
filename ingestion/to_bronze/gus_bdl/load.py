"""GUS BDL bronze.load: Landing subgroup exports → bronze.bdl_subgroup_observations.

Batches of batch.py (decode.py per file); see ingestion/common/bronze_load.py. Rejections
are recorded in bronze.bdl_rejected_files (ops/retry_rejected clears them).
"""
from __future__ import annotations

from ingestion.common.bronze_load import run_batches


def load(budget_minutes: float) -> int:
    return run_batches("gus_bdl", "ingestion.to_bronze.gus_bdl.batch", "bdl-bronze-batch",
                       budget_minutes, batch_size=512, batch_seconds=2700, margin_seconds=900)
