"""GUS BDL bronze.load: Landing subgroup exports → bronze.bdl_subgroup_observations, then
the landed API metadata → bronze.bdl_metadata_variables / bronze.bdl_metadata_units.

Batches of batch.py (decode.py per file); see ingestion/common/bronze_load.py. Rejections
are recorded in bronze.bdl_rejected_files and bronze.bdl_metadata_rejected_files
(ops/retry_rejected clears them).
"""
from __future__ import annotations

from ingestion.common.bronze_load import run_batches
from ingestion.common.report import annotate


def load(budget_minutes: float) -> int:
    status = run_batches("gus_bdl", "ingestion.to_bronze.gus_bdl.batch", "bdl-bronze-batch",
                         budget_minutes, batch_size=512, batch_seconds=2700, margin_seconds=900)
    if status in (0, 2):
        from ingestion.to_bronze.gus_bdl import metadata
        annotate("notice", "gus_bdl metadata Bronze", metadata.load())
    return status
