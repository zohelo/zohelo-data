"""Dry run of the Eurostat structure decoder on real Landing files (no writes)."""
import collections
import json
import random
import traceback

from ingestion.common.r2 import get_with_backoff, raw_chunks, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat.structure import METADATA, rows_for

client, landing = s3(), req("R2_LANDING_BUCKET")
keys = []
for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing, Prefix=METADATA):
    keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".structure.xml")]
random.seed(7)
sample = sorted(random.sample(keys, 12)) + [k for k in keys if "/NAMA_10_GDP/" in k.upper()][:1]
out = {"structure_files": len(keys), "files": []}
for key in sample:
    receipt = json.loads(get_with_backoff(client, landing, key[: -len(".structure.xml")] + ".receipt.json"))
    try:
        rows = rows_for(receipt, b"".join(raw_chunks(client, landing, key)))
        kinds = collections.Counter(r["artefact"] for r in rows)
        dims = [(r["item_id"], r["position"], json.loads(r["references_json"] or "{}").get("Enumeration", {}).get("id"))
                for r in rows if r["artefact"] == "dimension"]
        out["files"].append({"dataset": receipt["dataset_code"], "rows": len(rows), "kinds": dict(kinds), "dims": dims[:8]})
    except Exception:
        out["files"].append({"key": key[-60:], "error": traceback.format_exc()[-600:]})
for i in range(0, len(out["files"]), 3):
    annotate("notice", f"structure trial {i}", {"structure_files": out["structure_files"], "files": out["files"][i:i + 3]})
