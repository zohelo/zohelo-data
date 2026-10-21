"""Delete the scratch prefix zz_dev_bdl_web/ left by a crashed trial (dev writes only)."""

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

client, bucket = s3(), req("R2_LANDING_BUCKET")
keys = [
    o["Key"]
    for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="zz_dev_bdl_web/")
    for o in p.get("Contents", [])
]
for k in keys:
    client.delete_object(Bucket=bucket, Key=k)
annotate("notice", "bdl scratch cleanup", {"deleted": keys})
