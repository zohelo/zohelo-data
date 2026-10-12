#!/usr/bin/env python3
"""One-off: copy Eurostat Landing files from the two earlier layouts into the agreed one.

Earlier layouts (01_landing/eurostat/bulk/):
  legacy   raw-<sha256>.bin, identified only through the old campaign checkpoint
  current  data/<CODE>/<version>.tsv.gz + <version>.receipt.json (written 11-12 Oct 2026)
Agreed layout (docs/landing.md):
  01_landing/eurostat/snapshot/<data|metadata>/<CODE>/<version>__<ingested_at>[__part-<id>].<ext>
  + .receipt.json

Modes:
  plan   read-only: what would be copied, by layout and kind; nothing is written
  copy   server-side copy + full SHA-256 readback + receipt; idempotent; originals untouched

Originals are retired (archived and deleted) in a later step, after the Eurostat code
reads only the agreed layout. Remove this script in the pull request that does that.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import threading
import time

from ingestion.common.report import annotate, step_summary
from ingestion.sources.eurostat.extract import R2, version_token

OLD_PREFIX = "01_landing/eurostat/bulk/"
CURRENT_PREFIX = OLD_PREFIX + "data/"
NEW_PREFIX = "01_landing/eurostat/snapshot/"
LEGACY_CAMPAIGN = "eurostat_bulk"
# kind -> (area, file stem suffix, expected content); the extension is confirmed from the
# file's first bytes at copy time (gzip, zip, xml or text).
KIND_TARGET = {
    "eurostat_tsv_gzip": ("data", "", "tsv"),
    "eurostat_structure_sdmx": ("metadata", ".structure", "xml"),
    "eurostat_metadata_zip": ("metadata", ".sdmx", "zip"),
    "eurostat_codelist_tsv": ("metadata", ".codelist", "tsv"),
    "eurostat_inventory": ("metadata", "", "tsv"),
    "eurostat_inventory_codelist": ("metadata", "", "tsv"),
    "eurostat_inventory_metadata": ("metadata", "", "tsv"),
}
CONTENT_TYPES = {"gz": "application/gzip", "zip": "application/zip", "xml": "application/xml",
                 "tsv": "text/tab-separated-values"}
INVENTORY_ENTITY = {"eurostat_inventory": "inventory-data",
                    "eurostat_inventory_codelist": "inventory-codelist",
                    "eurostat_inventory_metadata": "inventory-metadata"}


def extension(stem_suffix: str, content: str, head: bytes) -> str:
    """Native extension from the first bytes; refuse content that contradicts the kind."""
    if head[:2] == b"\x1f\x8b":
        return f"{stem_suffix}.{content}.gz"
    if head[:2] == b"PK":
        if content != "zip":
            raise RuntimeError("unexpected ZIP content")
        return f"{stem_suffix}.zip"
    if content == "zip":
        raise RuntimeError("expected ZIP content")
    if content == "xml" and not head.lstrip().startswith(b"<"):
        raise RuntimeError("expected XML content")
    return f"{stem_suffix}.{content}"


def compact(timestamp: str) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def target(area: str, code: str, token: str, ingested: str, ext: str, part: str | None):
    base = f"{NEW_PREFIX}{area}/{code}/{token}__{ingested}" + (f"__part-{part}" if part else "")
    return base + ext, base + ".receipt.json"


def current_items(r2: R2) -> list[dict]:
    keys = [k for k in r2.list_keys(r2.landing, CURRENT_PREFIX) if k.endswith(".receipt.json")]

    def read(key: str) -> dict:
        receipt = json.loads(r2.client.get_object(Bucket=r2.landing, Key=key)["Body"].read())
        code = receipt["dataset_code"]
        data_key = receipt.get("object", {}).get("key") or key[: -len(".receipt.json")] + ".tsv.gz"
        token = receipt.get("version_token") or version_token(receipt.get("eurostat_version"))
        data, rec = target("data", code, token, compact(receipt["retrieved_at"]), ".tsv.gz", None)
        return {"layout": "current", "kind": "eurostat_tsv_gzip", "code": code, "part": None,
                "area": "data", "stem_suffix": "", "content": "tsv", "base": data[: -len(".tsv.gz")],
                "version": receipt.get("eurostat_version"), "token": token,
                "ingested_at": receipt["retrieved_at"], "sha256": receipt["sha256"],
                "size": int(receipt["size_bytes"]), "source_url": receipt.get("source_url"),
                "from_key": data_key, "from_receipt": key, "to_key": data, "to_receipt": rec}

    with ThreadPoolExecutor(max_workers=16) as pool:
        return list(pool.map(read, keys))


def legacy_items(r2: R2) -> tuple[list[dict], list[dict]]:
    from ingestion.legacy.engine.source_campaign_store import R2CampaignStore, _R2ObjectStore

    state = R2CampaignStore(LEGACY_CAMPAIGN, publication_only=True).load() or {}
    resolver = _R2ObjectStore(r2.client, landing_bucket=r2.landing, lakehouse_bucket=r2.lakehouse)
    entries = [(task_id, e) for task_id, e in (state.get("completed") or {}).items()
               if isinstance(e, dict) and isinstance(e.get("raw"), dict)]
    skipped: list[dict] = []

    def read(pair) -> dict | None:
        task_id, entry = pair
        raw = entry["raw"]
        try:
            bucket, key = resolver._parse(raw["id"])
            rbucket, rkey = resolver._parse(entry["receipt"]["id"])
            receipt = json.loads(r2.client.get_object(Bucket=rbucket, Key=rkey)["Body"].read())
        except Exception as exc:
            skipped.append({"task": task_id, "reason": f"unresolved: {type(exc).__name__}"})
            return None
        dist = receipt.get("distribution") or {}
        kind = dist.get("kind", "unknown")
        dataset_id = entry.get("dataset_id") or dist.get("dataset_id") or ""
        code = entry.get("original_dataset_id") or dataset_id.split("::partition::")[0]
        part = dataset_id.split("::partition::")[1] if "::partition::" in dataset_id else None
        part = re.sub(r"[^A-Za-z0-9._-]+", "-", part)[:80] if part else None
        if bucket != r2.landing or not key.startswith(OLD_PREFIX):
            skipped.append({"task": task_id, "reason": "outside_landing"})
            return None
        if kind not in KIND_TARGET:
            skipped.append({"task": task_id, "reason": f"kind:{kind}",
                            "sample": {"dataset_id": dataset_id, "original": entry.get("original_dataset_id"),
                                       "distribution": {k: v for k, v in dist.items() if k != "kind"},
                                       "size": raw.get("size_bytes")}})
            return None
        area, stem_suffix, content = KIND_TARGET[kind]
        code = INVENTORY_ENTITY.get(kind) or code.split("::")[0]
        token = version_token(dist.get("version"))
        ingested = receipt.get("retrieved_at_utc")
        if not ingested:
            skipped.append({"task": task_id, "reason": "no_retrieved_at"})
            return None
        data, rec = target(area, code, token, compact(ingested), stem_suffix + "." + content, part)
        return {"layout": "legacy", "kind": kind, "code": code, "part": part, "area": area,
                "stem_suffix": stem_suffix, "content": content,
                "base": data[: -len(stem_suffix + "." + content)],
                "version": dist.get("version"), "token": token, "ingested_at": ingested,
                "sha256": raw["sha256"], "size": int(raw["size_bytes"]),
                "source_url": dist.get("url") or dist.get("download_url"),
                "from_key": key, "from_receipt": f"r2://{rbucket}/{rkey}",
                "to_key": data, "to_receipt": rec}

    with ThreadPoolExecutor(max_workers=16) as pool:
        items = [i for i in pool.map(read, entries) if i]
    return items, skipped


def stream_sha256(r2: R2, key: str) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    body = r2.client.get_object(Bucket=r2.landing, Key=key)["Body"]
    for chunk in iter(lambda: body.read(8 * 1024 * 1024), b""):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def copy_one(r2: R2, item: dict, run_id: str) -> str:
    head_bytes = r2.client.get_object(Bucket=r2.landing, Key=item["from_key"], Range="bytes=0-63")["Body"].read()
    item["to_key"] = item["base"] + extension(item["stem_suffix"], item["content"], head_bytes)
    existing = r2.head(r2.landing, item["to_receipt"])
    if existing is not None:
        receipt = json.loads(r2.client.get_object(Bucket=r2.landing, Key=item["to_receipt"])["Body"].read())
        if receipt.get("sha256") != item["sha256"]:
            raise RuntimeError(f"receipt conflict at {item['to_receipt']}")
        return "already_copied"
    head = r2.head(r2.landing, item["to_key"])
    if head is None or head.get("Metadata", {}).get("sha256") != item["sha256"]:
        if head is not None:
            raise RuntimeError(f"different object already at {item['to_key']}")
        r2.client.copy({"Bucket": r2.landing, "Key": item["from_key"]}, r2.landing, item["to_key"],
                       ExtraArgs={"MetadataDirective": "REPLACE",
                                  "ContentType": CONTENT_TYPES[item["to_key"].rsplit(".", 1)[1]],
                                  "Metadata": {"sha256": item["sha256"], "size": str(item["size"]),
                                               "retrieved-at": item["ingested_at"],
                                               "eurostat-version": item["version"] or "",
                                               "migrated-from": item["from_key"][:512]}},
                       Config=r2.transfer)
    sha, size = stream_sha256(r2, item["to_key"])
    if (sha, size) != (item["sha256"], item["size"]):
        raise RuntimeError(f"readback mismatch for {item['to_key']}")
    receipt = {
        "schema_version": 2, "kind": "eurostat_landing", "source_id": "eurostat",
        "dataset_code": item["code"], "part": item["part"], "content": "snapshot",
        "area": item["area"], "source_kind": item["kind"],
        "eurostat_version": item["version"], "version_token": item["token"],
        "ingested_at": item["ingested_at"], "sha256": sha, "size_bytes": size,
        "source_url": item["source_url"],
        "object": {"bucket": r2.landing, "key": item["to_key"]},
        "migrated_from": {"layout": item["layout"], "key": item["from_key"],
                          "receipt": item["from_receipt"]},
        "migrated_at": datetime.now(timezone.utc).isoformat(), "migration_run": run_id,
    }
    r2.put_new(r2.landing, item["to_receipt"],
               json.dumps(receipt, indent=2, sort_keys=True).encode(), "application/json")
    return "copied"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("plan", "copy"), required=True)
    parser.add_argument("--max-items", type=int, default=0, help="copy at most this many (0 = all)")
    parser.add_argument("--budget-minutes", type=float, default=320)
    args = parser.parse_args(argv)
    started = time.monotonic()
    r2 = R2()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + os.environ.get("GITHUB_RUN_ID", "local")

    current = current_items(r2)
    legacy, skipped = legacy_items(r2)
    items = sorted(legacy + current, key=lambda i: (i["code"], i["token"], i["layout"]))
    targets = Counter(i["to_key"] for i in items)
    clashes = [k for k, n in targets.items() if n > 1]
    referenced = {i["from_key"] for i in items}
    unreferenced = [k for k in r2.list_keys(r2.landing, OLD_PREFIX)
                    if k not in referenced and not k.startswith(CURRENT_PREFIX)]
    plan = {
        "items": len(items), "bytes": sum(i["size"] for i in items),
        "by_layout": dict(Counter(i["layout"] for i in items)),
        "partition_files": sum(i["part"] is not None for i in items),
        "datasets": len({i["code"] for i in items}),
        "skipped_legacy": dict(Counter(s["reason"] for s in skipped)),
        "target_clashes": len(clashes),
        "unreferenced_old_objects": len(unreferenced),
        "unreferenced_examples": unreferenced[:5],
        "examples": [{k: i[k] for k in ("layout", "from_key", "to_key")} for i in items[:2] + legacy[:1]],
    }
    annotate("notice", "eurostat landing migration plan", plan)
    seen: set = set()
    for item in skipped:
        if "sample" in item and item["reason"] not in seen and len(seen) < 8:
            seen.add(item["reason"])
            annotate("notice", f"eurostat migration sample {item['reason']}", item["sample"])
    if clashes:
        annotate("error", "eurostat landing migration clashes", clashes[:20])
        return 1
    if args.mode == "plan":
        return 0

    deadline = started + args.budget_minutes * 60
    queue = items[: args.max_items] if args.max_items else items
    outcomes: Counter = Counter()
    failures: list[dict] = []
    lock = threading.Lock()

    def work(item: dict) -> None:
        if time.monotonic() > deadline:
            outcome, failure = "not_started_time_budget", None
        else:
            try:
                outcome, failure = copy_one(r2, item, run_id), None
            except Exception as exc:
                outcome, failure = "failed", {"key": item["from_key"], "error": str(exc)[:200]}
        with lock:
            outcomes[outcome] += 1
            if failure:
                failures.append(failure)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(work, queue))
    result = {"queued": len(queue), **outcomes,
              "elapsed_minutes": round((time.monotonic() - started) / 60, 1)}
    annotate("notice", "eurostat landing migration copy", result)
    step_summary(f"## eurostat Landing migration (copy)\n\n```json\n{json.dumps(result, indent=1)}\n```\n")
    if failures:
        annotate("error", "eurostat landing migration failures", failures[:15])
        return 1
    return 2 if outcomes["not_started_time_budget"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
