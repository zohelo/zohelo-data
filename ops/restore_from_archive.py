#!/usr/bin/env python3
"""Put archived raw files back into Landing so they can be reprocessed.

When landing.archive moves a Landing data file, its receipt stays in Landing and points
to the Archive copy (05_archive/<source>/raw/<sha256>.bin, or .zst for a zstd-compressed
copy of an uncompressed format). This finds receipts whose data object is missing and,
with --apply, writes the original bytes back to the receipt's key and verifies the
SHA-256 by reading them back. Never overwrites.
Args: --source eurostat [--dataset CODE_OR_GLOB ...] [--apply]
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import fnmatch
import hashlib
import json
import tempfile

from botocore.exceptions import ClientError

from ingestion.common.r2 import archive_key, raw_chunks, req, s3
from ingestion.common.report import annotate


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--dataset", nargs="*", default=[])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    prefix = f"01_landing/{args.source}/snapshot/"
    patterns = [p.lower() for p in args.dataset]

    present, receipts = set(), []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            entity = obj["Key"][len(prefix) :].split("/")[1] if obj["Key"].count("/") > 4 else ""
            if patterns and not any(fnmatch.fnmatch(entity.lower(), p) for p in patterns):
                continue
            if obj["Key"].endswith(".receipt.json"):
                receipts.append(obj["Key"])
            else:
                present.add(obj["Key"])

    def inspect(key: str) -> dict | None:
        receipt = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        target = (receipt.get("object") or {}).get("key")
        if not target or target in present:
            return None
        return {
            "receipt": key,
            "target": target,
            "sha256": receipt["sha256"],
            "size": receipt["size_bytes"],
            "archive": archive_key(args.source, receipt["sha256"], target),
        }

    with ThreadPoolExecutor(max_workers=16) as pool:
        missing = [m for m in pool.map(inspect, receipts) if m]

    def restore(item: dict) -> str:
        try:
            head = client.head_object(Bucket=bucket, Key=item["archive"])
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) in ("404", "NoSuchKey", "NotFound"):
                return "not_in_archive"
            raise
        if item["archive"].endswith(".bin") and int(head["ContentLength"]) != int(item["size"]):
            return "archive_size_mismatch"
        if not args.apply:
            return "restorable"
        meta = {"sha256": item["sha256"], "restored-from": item["archive"]}
        if item["archive"].endswith(".bin"):
            client.copy_object(
                Bucket=bucket,
                Key=item["target"],
                MetadataDirective="REPLACE",
                Metadata=meta,
                CopySource={"Bucket": bucket, "Key": item["archive"]},
            )
        else:
            with tempfile.TemporaryFile() as handle:
                for block in raw_chunks(client, bucket, item["archive"]):
                    handle.write(block)
                handle.seek(0)
                client.upload_fileobj(handle, bucket, item["target"], ExtraArgs={"Metadata": meta})
        digest = hashlib.sha256()
        for block in raw_chunks(client, bucket, item["target"]):
            digest.update(block)
        if digest.hexdigest() != item["sha256"]:
            client.delete_object(Bucket=bucket, Key=item["target"])
            return "readback_mismatch"
        return "restored"

    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = Counter(pool.map(restore, missing))
    annotate(
        "notice",
        f"{args.source} restore from archive",
        {
            "receipts": len(receipts),
            "data_missing": len(missing),
            **outcomes,
            "applied": args.apply,
            "examples": [m["target"] for m in missing[:5]],
        },
    )
    return 1 if outcomes["readback_mismatch"] or outcomes["archive_size_mismatch"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
