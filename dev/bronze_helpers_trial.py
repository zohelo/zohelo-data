"""Trial of the shared Bronze helpers on real R2 (no table writes):
download + verify one Eurostat and one BDL Landing file, upload a scratch Parquet to a
zz_trial/ prefix (verified), then delete it; run both batch loaders with --max-files 1."""
import json
import tempfile
import traceback
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import get_with_backoff, req, s3
from ingestion.common.report import annotate

out = {}
try:
    client, landing, lake = s3(), req("R2_LANDING_BUCKET"), req("R2_LAKEHOUSE_BUCKET")
    with tempfile.TemporaryDirectory() as tmp:
        for source, prefix in (("eurostat", "01_landing/eurostat/snapshot/data/AACT_ALI01/"),
                               ("gus_bdl", "01_landing/gus_bdl/snapshot/data/P1312/")):
            page = client.list_objects_v2(Bucket=landing, Prefix=prefix)
            key = next(o["Key"] for o in page["Contents"] if o["Key"].endswith(".receipt.json"))
            r = json.loads(get_with_backoff(client, landing, key))
            dest = Path(tmp) / f"{source}.raw"
            bf.download(client, landing, source, r["object"]["key"], r["sha256"], r.get("size_bytes"), dest)
            out[f"{source}_download_ok"] = dest.stat().st_size
            bad = Path(tmp) / f"{source}.bad"
            try:
                bf.download(client, landing, source, r["object"]["key"], "0" * 64, r.get("size_bytes"), bad)
                out[f"{source}_bad_sha_detected"] = False
            except RuntimeError:
                out[f"{source}_bad_sha_detected"] = True
        path = Path(tmp) / "t.parquet"
        pq.write_table(pa.table({"a": [1, 2]}), path)
        uri = bf.upload(client, lake, "zz_trial/bronze_helpers/t.parquet", path, "f" * 64)
        uri2 = bf.upload(client, lake, "zz_trial/bronze_helpers/t.parquet", path, "f" * 64)
        out["upload"] = uri == uri2
        client.delete_object(Bucket=lake, Key="zz_trial/bronze_helpers/t.parquet")
    from ingestion.to_bronze.eurostat import batch as eb
    from ingestion.to_bronze.gus_bdl import batch as bb
    out["eurostat_batch_exit"] = eb.main(["--max-files", "1", "--session-seconds", "600"])
    out["bdl_batch_exit"] = bb.main(["--max-files", "1", "--session-seconds", "600"])
except Exception:
    out["error"] = traceback.format_exc()[-1500:]
annotate("error" if "error" in out else "notice", "bronze helpers trial", out)
