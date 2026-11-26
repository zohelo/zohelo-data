"""IMGW public data API Landing → Bronze activities (accumulating snapshots).

bronze.load       load.py → bronze.imgw_api_synop, bronze.imgw_api_meteo (every landed snapshot, one Parquet file each)
bronze.verify     every landed snapshot is in Bronze or rejected with a reason. The shared check
                  (ingestion/common/lineage.py) applies the latest-only gate to sources outside its accumulating list,
                  so this source checks membership itself with the same reads (lineage.bronze_hashes)
landing.archive   ingestion/common/archive.py --source imgw_api (move to Archive)
archive.prune     ingestion/common/retention.py --source imgw_api (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "imgw_api"


def _load(p) -> int:
    from ingestion.to_bronze.imgw_api import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage
    from ingestion.common.r2 import landing_objects, req, s3
    from ingestion.common.report import annotate

    meta = landing_objects(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SOURCE)
    held, rejected = lineage.bronze_hashes(SOURCE)
    loaded = set().union(*held.values()) if held else set()
    raw = {d for d, item in meta.items() if item.get("kind") == "raw"}
    missing = raw - loaded - rejected
    summary = {
        "result": "fail" if missing else "pass",
        "policy": "accumulating: every landed snapshot is kept in Bronze",
        "landing_snapshots": len(raw),
        "bronze_files": {name: len(hashes) for name, hashes in held.items()},
        "rejected_with_reason": len(raw & rejected),
        "unexplained": len(missing),
        "unexplained_sample": sorted(meta[d]["key"] for d in missing)[:10],
    }
    annotate("error" if missing else "notice", f"{SOURCE} bronze.verify", summary)
    return 1 if missing else 0


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
