"""Dev trial for #2387 (not for main): inspect, load, inspect again and verify the world_bank_icp extractor
into the zz_dev_world_bank_icp/ Landing root, report the landed files, then delete every object under it."""

from __future__ import annotations

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.world_bank_icp import extract

ROOT = "zz_dev_world_bank_icp"


def objects(client, bucket: str) -> list[dict]:
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{ROOT}/"):
        found += page.get("Contents", [])
    return found


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    codes = {}
    left = -1
    try:
        for mode in ("inspect", "load", "inspect", "verify"):
            codes[f"{len(codes)}_{mode}"] = extract.main(["--mode", mode, "--landing-root", ROOT, "--budget-minutes", "60"])
        data = [o for o in objects(client, bucket) if not o["Key"].endswith(".receipt.json")]
        annotate("notice", "world_bank_icp trial landed", {
            "codes": codes, "files": len(data), "bytes": sum(o["Size"] for o in data),
            "largest": sorted(({"key": o["Key"].split("/")[-2], "bytes": o["Size"]} for o in data),
                              key=lambda x: -x["bytes"])[:5],
        })
    finally:
        keys = [o["Key"] for o in objects(client, bucket)]
        for start in range(0, len(keys), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start : start + 1000]]})
        left = len(objects(client, bucket))
        annotate("notice" if left == 0 else "error", "world_bank_icp trial clean-up", {"deleted": len(keys), "left": left})
    return 0 if codes and all(c == 0 for c in codes.values()) and left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
