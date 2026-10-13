#!/usr/bin/env python3
"""Copy the September BDL web exports into the agreed Landing layout (dev, deleted before merge).

Source: 01_landing/gus_bdl/web_bulk/P####/<sha>/<name>.zip with the Drive-era plans
web_bulk/_control/web-parts-P####-v1.json (a tree of export selections; leaves are the
exported parts). A subgroup is complete when every leaf is landed.

Target per complete subgroup, one file + receipt per part:
  01_landing/gus_bdl/snapshot/data/<P####>/<version>__<ingested>[__part-<n>].zip
  version  = the subgroup's BDL lastUpdate if it is not later than the export, else
             19700101T000000Z (unknown → treated as stale by landing.inspect)
  ingested = the part's export time (UTC)

--mode plan : counts only (calls the BDL API once per subgroup for lastUpdate).
--mode copy : copies, verifies SHA-256 by reading back, writes receipts. Never deletes;
              the old layout is retired separately after verification.
Partial and failed subgroups are reported and left where they are.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import threading

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.gus_bdl.api import Bdl, token

OLD = "01_landing/gus_bdl/web_bulk/"
NEW = "01_landing/gus_bdl/snapshot/data/"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "."))


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["plan", "copy"], default="plan")
    parser.add_argument("--max-subgroups", type=int, default=0)
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LANDING_BUCKET")

    objects = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=OLD):
        for o in page.get("Contents", []):
            objects[o["Key"]] = o["Size"]
    plan_keys = sorted(k for k in objects if k.startswith(OLD + "_control/web-parts-") and k.endswith("-v1.json"))
    if args.max_subgroups:
        plan_keys = plan_keys[: args.max_subgroups]

    def read_plan(key: str) -> dict:
        plan = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        nodes = plan.get("nodes", {})
        leaves = [n for n in nodes.values() if not n.get("children")]
        sub = plan.get("subgroup_id")
        parts, missing_files = [], 0
        for n in leaves:
            receipt = n.get("receipt") or {}
            archive = receipt.get("archive_object") or {}
            if n.get("status") != "landed" or not archive.get("sha256"):
                continue
            old_key = f"{OLD}{sub}/{archive['sha256']}/{archive['name']}"
            if old_key not in objects:
                missing_files += 1
                continue
            parts.append({"old_key": old_key, "sha256": archive["sha256"], "size": int(archive["size"]),
                          "name": archive["name"], "completed": receipt.get("completed_at_utc"),
                          "scope": n.get("scope")})
        complete = bool(leaves) and len(parts) == len(leaves) and missing_files == 0
        return {"subgroup": sub, "leaves": len(leaves), "parts": parts, "complete": complete,
                "missing_files": missing_files}

    with ThreadPoolExecutor(max_workers=8) as pool:
        plans = list(pool.map(read_plan, plan_keys))
    complete = [p for p in plans if p["complete"]]

    api = Bdl()
    versions: Counter = Counter()
    for p in complete:
        last = api.last_update(p["subgroup"])
        first_export = min(utc(x["completed"]) for x in p["parts"])
        if last is not None and last <= first_export:
            p["version"], state = token(last), "current_at_export"
        else:
            p["version"], state = token(None), "changed_since_export" if last else "no_last_update"
        p["last_update"] = last.isoformat() if last else None
        versions[state] += 1

    outcomes: Counter = Counter()
    lock = threading.Lock()

    def copy(p: dict, part: dict, index: int) -> None:
        suffix = f"__part-{index:03d}" if len(p["parts"]) > 1 else ""
        stem = f"{NEW}{p['subgroup']}/{p['version']}__{token(utc(part['completed']))}{suffix}"
        key, receipt_key = stem + ".zip", stem + ".receipt.json"
        try:
            client.head_object(Bucket=bucket, Key=receipt_key)
            with lock:
                outcomes["already_copied"] += 1
            return
        except client.exceptions.ClientError:
            pass
        client.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": part["old_key"]},
                           MetadataDirective="REPLACE", Metadata={"sha256": part["sha256"]},
                           ContentType="application/zip")
        digest, size = sha256(), 0
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        for block in iter(lambda: body.read(8 << 20), b""):
            digest.update(block)
            size += len(block)
        if digest.hexdigest() != part["sha256"] or size != part["size"]:
            client.delete_object(Bucket=bucket, Key=key)
            raise RuntimeError(f"copy mismatch: {part['old_key']}")
        receipt = {"schema_version": 2, "kind": "gus_bdl_landing", "source_id": "gus_bdl", "area": "data",
                   "channel": "web_export", "subgroup_id": p["subgroup"], "sha256": part["sha256"],
                   "size_bytes": part["size"], "object": {"bucket": bucket, "key": key},
                   "source_version": p["last_update"] if p["version"] != token(None) else None,
                   "version_token": p["version"], "ingested_at": part["completed"],
                   "export_file_name": part["name"],
                   "part": {"index": index, "total": len(p["parts"]), "selection": part["scope"]},
                   "migrated_from": part["old_key"], "migrated_at": datetime.now(timezone.utc).isoformat()}
        client.put_object(Bucket=bucket, Key=receipt_key, ContentType="application/json",
                          Body=json.dumps(receipt, indent=2, ensure_ascii=False).encode())
        with lock:
            outcomes["copied"] += 1

    if args.mode == "copy":
        jobs = [(p, part, i) for p in complete for i, part in
                enumerate(sorted(p["parts"], key=lambda x: x["completed"]), start=1)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda job: copy(*job), jobs))

    summary = {
        "mode": args.mode, "plans": len(plans), "complete": len(complete),
        "incomplete": sum(1 for p in plans if not p["complete"]),
        "incomplete_with_parts": sum(1 for p in plans if not p["complete"] and p["parts"]),
        "parts_in_complete": sum(len(p["parts"]) for p in complete),
        "complete_gb": round(sum(x["size"] for p in complete for x in p["parts"]) / 1e9, 3),
        "missing_files": sum(p["missing_files"] for p in plans),
        "versions": dict(versions), "api_requests": api.requests, **outcomes,
        "multi_part_subgroups": sum(1 for p in complete if len(p["parts"]) > 1)}
    (OUT / "bdl-migration.json").write_text(json.dumps({"summary": summary, "subgroups": [
        {k: v for k, v in p.items() if k != "parts"} | {"parts": len(p["parts"])} for p in plans]}, indent=2))
    annotate("notice", "bdl landing migration", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
