"""Trial of the BDL website export on 2 missing subgroups, written to a scratch prefix
(zz_dev_bdl_web/) and deleted afterwards. Production Landing is not touched."""

from collections import Counter
import csv
import io
import subprocess
import sys
import traceback
import zipfile

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright==1.56.0"], check=True)

from ingestion.common.r2 import req, s3  # noqa: E402
from ingestion.common.report import annotate  # noqa: E402
from ingestion.to_landing.gus_bdl import web_export  # noqa: E402

SCRATCH = "zz_dev_bdl_web/"
try:
    web_export.DATA = SCRATCH
    web_export.PAUSE_SECONDS = 5
    only = sys.argv[1:] or None
    code = web_export.main(limit=2, budget_minutes=45, only=only)
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    keys = [
        o["Key"]
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SCRATCH)
        for o in p.get("Contents", [])
    ]
    sizes = {k: client.head_object(Bucket=bucket, Key=k)["ContentLength"] for k in keys}
    contents = {}
    for k in keys:
        if k.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(client.get_object(Bucket=bucket, Key=k)["Body"].read())) as z:
                for m in z.infolist():
                    rows = list(
                        csv.reader(io.StringIO(z.read(m).decode("utf-8-sig", "replace"), newline=""), delimiter=";")
                    )
                    units = {r[0] for r in rows[1:] if r}
                    contents[m.filename] = {
                        "rows": len(rows) - 1,
                        "units": len(units),
                        "units_by_code_length": dict(Counter(len(u) for u in units)),
                        "header": rows[0][:12] if rows else [],
                    }
    for k in keys:
        client.delete_object(Bucket=bucket, Key=k)
    annotate(
        "notice",
        "bdl web export trial",
        {"exit": code, "scratch_objects": sizes, "contents": contents, "deleted": len(keys)},
    )
except Exception:
    annotate("error", "bdl web export trial", {"traceback": traceback.format_exc()[-2500:]})
    raise
