"""World Bank WDI activities.

bronze.verify     pipeline/common/lineage.py --source world_bank_wdi
landing.archive   pipeline/common/archive.py --source world_bank_wdi

The API campaign adapter (api_adapter.py, run by pipeline/common/run_api_campaign.py)
is paused and not wired as an activity.
"""
from __future__ import annotations


def _bronze_verify(p) -> int:
    from pipeline.common import lineage
    return lineage.main(["--source", "world_bank_wdi"])


def _archive(p) -> int:
    from pipeline.common import archive
    return archive.main(["--source", "world_bank_wdi"])


ACTIVITIES = {
    "bronze.verify": _bronze_verify,
    "landing.archive": _archive,
}
