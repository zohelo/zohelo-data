#!/usr/bin/env python3
"""archive.prune: apply a source's Archive retention policy.

Policy in ingestion/sources/<source>.yaml, e.g.::

    archive_policy:
      retention: {keep_latest: 1, keep_days: 90}
      prune: report          # report | delete

Snapshot sources: an Archive file is kept when it is one of the ``keep_latest`` newest
versions of its entity, when it was ingested within ``keep_days``, when Bronze still
holds it, when it is still in Landing, or when nothing tells its version (kept and
reported). Everything else is deleted together with the receipts that point to it
(``prune: delete``); ``prune: report`` only reports. Sources without ``retention`` keep
everything (delta / append-only raw is never pruned by default).
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
from typing import Any

from ingestion.common.r2 import archived, landing_objects, req, s3, sources
from ingestion.common.report import annotate, step_summary


def bronze_hashes(source: str) -> set[str]:
    if source == "eurostat":
        from ingestion.common.r2 import catalog
        from ingestion.to_bronze.eurostat.batch import TABLE_ID, loaded_hashes
        return loaded_hashes(catalog().load_table(TABLE_ID))
    return set()


def old_receipts(client, bucket: str, source: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts of retired Landing layouts kept in the Archive (05_archive/<s>/landing_receipts_*/)."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"05_archive/{source}/landing_receipts_"):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json")]

    def read(key: str):
        body = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        digest = body.get("sha256") or body.get("raw_sha256")
        entity = body.get("dataset_code") or body.get("dataset_id") or key.split("/")[-2]
        version = str(body.get("version_token") or body.get("eurostat_version") or key.rsplit("/", 1)[1].split(".")[0])
        ingested = str(body.get("ingested_at") or body.get("retrieved_at_utc") or body.get("downloaded_at") or "")
        return digest, {"receipt": key, "entity": entity, "version": version, "ingested": ingested}

    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with ThreadPoolExecutor(max_workers=16) as pool:
        for digest, info in pool.map(read, keys):
            if digest:
                out[digest].append(info)
    return out


def ingested_time(value: str) -> datetime | None:
    for fmt in ("%Y%m%dT%H%M%SZ",):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    args = parser.parse_args(argv)
    policy = (sources().get(args.source) or {}).get("archive_policy") or {}
    retention = policy.get("retention")
    if not retention:
        annotate("notice", f"{args.source} archive retention", {"result": "no retention policy; nothing pruned"})
        return 0
    keep_latest, keep_days = int(retention.get("keep_latest", 1)), float(retention.get("keep_days", 90))
    apply = policy.get("prune") == "delete"
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    now = datetime.now(timezone.utc)

    meta = landing_objects(client, bucket, args.source)
    stored = archived(client, bucket, args.source)
    legacy = old_receipts(client, bucket, args.source)
    in_bronze = bronze_hashes(args.source)

    # Every known version: (area, entity) -> [(version, ingested, digest)]
    versions: dict[tuple[str, str], list[tuple[str, str, str]]] = defaultdict(list)
    receipts_of: dict[str, list[str]] = defaultdict(list)
    for digest, item in meta.items():
        for path in item.get("snapshot_paths", []):
            versions[(path["area"], path["entity"])].append((path["version"], path["ingested"], digest))
            if path.get("receipt"):
                receipts_of[digest].append(path["receipt"])
    for digest, infos in legacy.items():
        for info in infos:
            versions[("data", info["entity"])].append((info["version"], info["ingested"], digest))
            receipts_of[digest].append(info["receipt"])

    latest: set[str] = set()
    for rows in versions.values():
        newest = sorted(set(rows), key=lambda r: (r[0], r[1]), reverse=True)
        kept_versions = []
        for version, _ingested, digest in newest:
            if version not in kept_versions:
                kept_versions.append(version)
            if len(kept_versions) > keep_latest:
                break
            latest.add(digest)
    newest_ingest: dict[str, datetime] = {}
    for rows in versions.values():
        for _version, ingested, digest in rows:
            when = ingested_time(ingested)
            if when and (digest not in newest_ingest or when > newest_ingest[digest]):
                newest_ingest[digest] = when

    reasons: Counter = Counter()
    doomed = []
    for digest, obj in stored.items():
        item = meta.get(digest) or {}
        if digest in latest:
            reasons["kept_latest_version"] += 1
        elif digest in in_bronze:
            reasons["kept_in_bronze"] += 1
        elif item.get("present"):
            reasons["kept_still_in_landing"] += 1
        elif digest not in newest_ingest:
            reasons["kept_version_unknown"] += 1
        elif newest_ingest[digest] >= now - timedelta(days=keep_days):
            reasons["kept_within_days"] += 1
        else:
            reasons["to_prune"] += 1
            doomed.append((digest, obj))

    deleted_objects = 0
    if apply and doomed:
        keys = []
        for digest, obj in doomed:
            keys.append(obj["key"])
            for receipt in receipts_of.get(digest, []):
                keys.append(receipt)
        for start in range(0, len(keys), 1000):
            response = client.delete_objects(Bucket=bucket, Delete={
                "Objects": [{"Key": k} for k in keys[start:start + 1000]], "Quiet": True})
            if response.get("Errors"):
                raise RuntimeError(f"delete failed: {response['Errors'][:3]}")
            deleted_objects += len(keys[start:start + 1000])
    summary = {"source": args.source, "mode": "delete" if apply else "report",
               "keep_latest": keep_latest, "keep_days": keep_days,
               "archive_files": len(stored), "archive_gb": round(sum(o["size"] for o in stored.values()) / 1e9, 2),
               **reasons, "prune_gb": round(sum(o["size"] for _, o in doomed) / 1e9, 2),
               "deleted_objects": deleted_objects,
               "prune_examples": [o["key"] for _, o in doomed[:5]]}
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("notice", f"{args.source} archive retention", summary)
    step_summary(f"## {args.source} · archive retention\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
