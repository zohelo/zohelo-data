"""Dev trial for #2366: write geo/<level>.geojson under zz_dev_published/v1/, report, delete."""

import json

from ingestion.common.r2 import catalog, req, s3
from warehouse.publish import geo

PREFIX = "zz_dev_published/v1/"

client = s3()
bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
geo(catalog(), client, PREFIX)
report = {}
for level in ("country", "voivodeship", "powiat", "gmina"):
    body = client.get_object(Bucket=bucket, Key=f"{PREFIX}geo/{level}.geojson")["Body"].read()
    feats = json.loads(body)["features"]
    report[level] = [len(feats), len(body), feats[0]["properties"].get("parent_key")]
print(f"::notice title=geo::{json.dumps(report)}")
keys = [
    obj["Key"]
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="zz_dev_published/")
    for obj in page.get("Contents", [])
]
for key in keys:
    client.delete_object(Bucket=bucket, Key=key)
print(f"::notice title=deleted::deleted {len(keys)}")
