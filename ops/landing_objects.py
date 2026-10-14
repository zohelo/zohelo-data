#!/usr/bin/env python3
"""Look into the Landing bucket (Landing and Archive folders) without downloading whole files.

Use it when a source changes format or a Bronze load rejects files: it shows what is
actually stored. A prefix argument (ending in "/") lists object counts, size and key
groups under it; with --peek it also shows the first bytes and the most frequent XML
tags of up to --limit objects. A key argument peeks that one object.
Args: PREFIX_OR_KEY ... [--peek] [--limit 3]
"""
from __future__ import annotations

import argparse
from collections import Counter
import re
import zlib

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

PEEK_BYTES = 3_000_000


def summarize(client, bucket: str, prefix: str) -> tuple[dict, list[str]]:
    """Count, size and group the objects under a prefix; return the summary and the keys."""
    groups: Counter[str] = Counter()
    keys, size = [], 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            size += obj["Size"]
            rest = obj["Key"][len(prefix):].split("/")
            groups["/".join(rest[:2]) if len(rest) > 2 else rest[0][:40]] += 1
    return {"prefix": prefix, "objects": len(keys), "mb": round(size / 1e6, 1),
            "groups": groups.most_common(25), "sample": keys[:8]}, keys


def peek(client, bucket: str, key: str) -> dict:
    """First bytes of one object (gzip decoded when possible) and its most frequent XML tags."""
    obj = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{PEEK_BYTES}")
    body = obj["Body"].read()
    if body[:2] == b"\x1f\x8b":
        # A ranged read cuts the gzip stream; decode what arrived.
        body = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(body)
    text = body.decode("utf-8", "replace")
    return {"key": key, "range": obj.get("ContentRange"), "head": text[:2500],
            "tags": Counter(re.findall(r"<([A-Za-z_:]+)[ >]", text)).most_common(30)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--peek", action="store_true")
    parser.add_argument("--limit", type=int, default=3)
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    for path in args.paths:
        if path.endswith("/"):
            summary, keys = summarize(client, bucket, path)
            annotate("notice", f"objects {path}", summary)
            targets = [k for k in keys if not k.endswith(".receipt.json")][:args.limit] if args.peek else []
        else:
            targets = [path]
        for key in targets:
            annotate("notice", f"peek {key[-60:]}", peek(client, bucket, key))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
