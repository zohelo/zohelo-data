"""Dev trial for #2468 (KRUS) (not for main): inspect, load and verify the krus declaration into the
zz_dev_krus/ Landing prefix, report the landed files, then delete every object under it."""

from __future__ import annotations

import os
from pathlib import Path

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

ROOT = "zz_dev_krus"


def objects(client, bucket: str) -> list[dict]:
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{ROOT}/"):
        found += page.get("Contents", [])
    return found


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    codes = {}
    try:
        for mode in ("inspect", "load", "verify"):
            codes[mode] = bulk_file.main("krus", mode, landing_root=ROOT)
        landed = [
            {"key": o["Key"], "bytes": o["Size"]}
            for o in objects(client, bucket)
            if not o["Key"].endswith(".receipt.json")
        ]
        out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
        out.mkdir(parents=True, exist_ok=True)
        for item in landed:
            body = client.get_object(Bucket=bucket, Key=item["key"])["Body"].read()
            (out / Path(item["key"]).name).with_suffix(".xlsx").write_bytes(body)
        annotate("notice", "krus trial landed", {"codes": codes, "entities": len(landed), "files": landed})
    finally:
        keys = [o["Key"] for o in objects(client, bucket)]
        for start in range(0, len(keys), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start : start + 1000]]})
        left = len(objects(client, bucket))
        annotate("notice" if left == 0 else "error", "krus trial clean-up", {"deleted": len(keys), "left": left})
    return 0 if codes and all(c == 0 for c in codes.values()) and left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
