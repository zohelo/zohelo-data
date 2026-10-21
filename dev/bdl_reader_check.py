"""Read-only: the stricter part rule (indexes exactly 1..total of one plan) against the
current Landing receipts: which subgroups would the old and the new readers take?"""

from collections import defaultdict

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_bdl import entities
from ingestion.to_landing.gus_bdl import inspect

client, bucket = s3(), req("R2_LANDING_BUCKET")
new_versions = inspect.landing_versions(client, bucket)
new_parts = entities.candidates(client, bucket)
new_keys = {(r["subgroup_id"], r["version_token"]) for r in new_parts}


def old_rule(receipts):
    by_part = defaultdict(dict)
    for r in receipts:
        if r.get("kind") == "gus_bdl_landing" and r.get("area") == "data":
            index = int((r.get("part") or {}).get("index", 1))
            held = by_part[(r["subgroup_id"], r["version_token"])].get(index)
            if held is None or (r.get("ingested_at") or "") > (held.get("ingested_at") or ""):
                by_part[(r["subgroup_id"], r["version_token"])][index] = r
    newest = {}
    for (subgroup, version), parts in by_part.items():
        parts = list(parts.values())
        total = int((parts[0].get("part") or {}).get("total", 1))
        if len({(p.get("part") or {}).get("index", 1) for p in parts}) < total:
            continue
        if subgroup not in newest or version > newest[subgroup][0]:
            newest[subgroup] = (version, parts)
    return newest


import json  # noqa: E402

keys = [
    o["Key"]
    for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=inspect.DATA)
    for o in p.get("Contents", [])
    if o["Key"].endswith(".receipt.json")
]
receipts = [json.loads(client.get_object(Bucket=bucket, Key=k)["Body"].read()) for k in keys]
old = old_rule(receipts)
old_files = sum(len(p) for _, p in old.values())
annotate(
    "notice",
    "bdl reader check",
    {
        "old_subgroups": len(old),
        "new_subgroups": len({k[0] for k in new_keys}),
        "new_landing_versions": len(new_versions),
        "old_files": old_files,
        "new_files": len(new_parts),
        "only_old": sorted(set(old) - {k[0] for k in new_keys})[:30],
        "version_differs": sorted(s for s, (v, _) in old.items() if new_versions.get(s) not in (None, v))[:30],
    },
)
