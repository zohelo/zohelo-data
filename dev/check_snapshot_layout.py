"""Dev check (deleted before merge): run the new Eurostat code read-only against production.

landing.verify with control output redirected to a zz_dev_ scratch prefix (removed at the
end), the Bronze candidate selection, and the lineage report.
"""
import json

from ingestion.common.report import annotate
from ingestion.common.r2 import req, s3
import ingestion.to_landing.eurostat.extract as extract
import ingestion.to_bronze.eurostat.batch as batch
from ingestion.common import lineage

SCRATCH = "06_control/zz_dev_eurostat_bulk_landing/"


def main() -> int:
    extract.CONTROL_PREFIX = SCRATCH
    rc_verify = extract.main(["--mode", "verify"])
    client = s3()
    cands = batch.landing_candidates(client, req("R2_LANDING_BUCKET"))
    existing = batch.existing_raw_hashes()
    failed = batch.recorded_failures(client, req("R2_LAKEHOUSE_BUCKET"))
    missing = [c for c in cands if c["raw_sha256"] not in existing and c["raw_sha256"] not in failed]
    annotate("notice", "dev bronze candidates", {
        "latest_datasets": len(cands), "in_bronze": sum(c["raw_sha256"] in existing for c in cands),
        "rejected_with_reason": sum(c["raw_sha256"] in failed for c in cands), "to_load": len(missing),
        "to_load_bytes": sum(c["size"] for c in missing),
        "to_load_sample": [c["dataset_id"] for c in sorted(missing, key=lambda c: -c["size"])[:15]]})
    rc_lineage = lineage.main(["--source", "eurostat"])
    removed = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=req("R2_LAKEHOUSE_BUCKET"), Prefix=SCRATCH):
        for obj in page.get("Contents", []):
            client.delete_object(Bucket=req("R2_LAKEHOUSE_BUCKET"), Key=obj["Key"])
            removed += 1
    annotate("notice", "dev cleanup", {"scratch_objects_removed": removed, "verify_rc": rc_verify, "lineage_rc": rc_lineage})
    return rc_verify or rc_lineage


if __name__ == "__main__":
    raise SystemExit(main())
