"""Dev trial for #2119: write manifest.json under zz_dev_published/v1/, read it back, delete the prefix."""

import json

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from warehouse.publish import manifest_json

PREFIX = "zz_dev_published/"


def main() -> int:
    client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    n = manifest_json(client, PREFIX + "v1/")
    body = client.get_object(Bucket=bucket, Key=PREFIX + "v1/manifest.json")["Body"].read()
    obj = json.loads(body)
    kpis = [m.get("kpi_key") for m in obj["metrics"]]
    annotate("notice", "manifest.json", {"metrics": n, "kpis": kpis, "bytes": len(body)})
    keys = [
        o["Key"]
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX)
        for o in p.get("Contents", [])
    ]
    if keys:
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True})
    annotate("notice", f"deleted {len(keys)}", {"keys": keys})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
