"""Trial of the BDL website export on 2 missing subgroups, written to a scratch prefix
(zz_dev_bdl_web/) and deleted afterwards. Production Landing is not touched."""

import subprocess
import sys
import traceback

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright==1.56.0"], check=True)

from ingestion.common.r2 import req, s3  # noqa: E402
from ingestion.common.report import annotate  # noqa: E402
from ingestion.to_landing.gus_bdl import web_export  # noqa: E402

SCRATCH = "zz_dev_bdl_web/"
try:
    web_export.DATA = SCRATCH
    web_export.PAUSE_SECONDS = 5
    code = web_export.main(limit=2, budget_minutes=25)
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    keys = [
        o["Key"]
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SCRATCH)
        for o in p.get("Contents", [])
    ]
    sizes = {k: client.head_object(Bucket=bucket, Key=k)["ContentLength"] for k in keys}
    for k in keys:
        client.delete_object(Bucket=bucket, Key=k)
    annotate("notice", "bdl web export trial", {"exit": code, "scratch_objects": sizes, "deleted": len(keys)})
except Exception:
    annotate("error", "bdl web export trial", {"traceback": traceback.format_exc()[-2500:]})
    raise
