"""Dev trial for #2036: write latest.json under zz_dev_published/v1/ from prod Gold, then delete zz_dev_published/."""

from __future__ import annotations

import sys

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from warehouse import publish

PREFIX = "zz_dev_published/"


def cleanup() -> int:
    client = s3()
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    keys = [
        obj["Key"]
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX)
        for obj in page.get("Contents", [])
    ]
    for key in keys:
        client.delete_object(Bucket=bucket, Key=key)
    return len(keys)


def main(argv: list[str]) -> int:
    try:
        result = publish.main(prefix=PREFIX + "v1/")
        client = s3()
        bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
        pl = {}
        for kpi_key in result["written"]:
            body = client.get_object(Bucket=bucket, Key=f"{PREFIX}v1/kpi/{kpi_key}/latest.json")["Body"].read()
            pl[kpi_key] = '"geography_key":"PL"' in body.decode("utf-8")
        annotate("notice", "latest files with PL", pl)
    finally:
        annotate("notice", f"deleted {cleanup()}", {"prefix": PREFIX})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
