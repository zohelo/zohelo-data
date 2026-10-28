"""Dev trial for #2126: geo files under zz_dev_published/v1/, bounds per file, then delete the prefix."""

import json

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse.publish import geo

PREFIX = "zz_dev_published/"


def points(coordinates):
    if coordinates and isinstance(coordinates[0], (int, float)):
        yield coordinates
        return
    for c in coordinates:
        yield from points(c)


def main() -> int:
    client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    try:
        counts = geo(catalog(), client, PREFIX + "v1/")
        result = {"counts": counts}
        for level in counts:
            body = client.get_object(Bucket=bucket, Key=PREFIX + f"v1/geo/{level}.geojson")["Body"].read()
            features = json.loads(body)["features"]
            xy = [p for f in features for p in points(f["geometry"]["coordinates"])]
            lons, lats = [p[0] for p in xy], [p[1] for p in xy]
            result[level] = [len(features), len(body), min(lons), max(lons), min(lats), max(lats)]
        annotate("notice", "geo", result)
    except Exception:
        import traceback

        annotate("error", "geo trial failed", {"traceback": traceback.format_exc()[-3000:]})
        raise
    finally:
        keys = [
            o["Key"]
            for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX)
            for o in p.get("Contents", [])
        ]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True})
        annotate("notice", f"deleted {len(keys)}", {})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
