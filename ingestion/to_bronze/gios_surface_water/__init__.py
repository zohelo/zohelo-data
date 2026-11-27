"""GIOŚ surface-water status assessment 2019-2024: Landing → Bronze activities (register PL-ENV-011, #3312).

Landing is ingestion/to_landing/gios_surface_water/ (pages of the ArcGIS status layers, bulk_file pattern).

bronze.load       load.py → bronze.gios_surface_water_features (one Parquet file per Landing file); rejections →
                  bronze.gios_surface_water_rejected_files
bronze.verify     ingestion/common/lineage.py --source gios_surface_water
landing.archive   ingestion/common/archive.py --source gios_surface_water (move to Archive)
archive.prune     ingestion/common/retention.py --source gios_surface_water (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gios_surface_water"


def _load(p) -> int:
    from ingestion.to_bronze.gios_surface_water import load

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
