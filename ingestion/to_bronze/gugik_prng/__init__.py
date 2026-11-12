"""GUGiK PRNG Landing → Bronze activities (register PL-ENV-002).

Landing is the shared bulk_file pattern (two XLSX packages of the national part).

bronze.load       load.py → bronze.prng_names (one Parquet file per Landing file);
                  rejections → bronze.prng_rejected_files
bronze.verify     ingestion/common/lineage.py --source gugik_prng
landing.archive   ingestion/common/archive.py --source gugik_prng (move to Archive)
archive.prune     ingestion/common/retention.py --source gugik_prng (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gugik_prng"


def _load(p) -> int:
    from ingestion.to_bronze.gugik_prng import load

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
