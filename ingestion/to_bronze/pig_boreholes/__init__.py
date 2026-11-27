"""PIG-PIB CBDG boreholes Landing → Bronze activities (register PL-ENV-014).

Landing is ingestion/to_landing/pig_boreholes/ (the WFS layer in GEOJSON pages).

bronze.load       load.py → bronze.pig_boreholes_features (one Parquet file per Landing page);
                  rejections → bronze.pig_boreholes_rejected_files
bronze.verify     ingestion/common/lineage.py --source pig_boreholes
landing.archive   ingestion/common/archive.py --source pig_boreholes (move to Archive)
archive.prune     ingestion/common/retention.py --source pig_boreholes (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "pig_boreholes"


def _load(p) -> int:
    from ingestion.to_bronze.pig_boreholes import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", SOURCE])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
