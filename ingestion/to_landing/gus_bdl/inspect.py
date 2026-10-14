"""landing.inspect for GUS BDL: every subgroup's lastUpdate (BDL API) vs Landing.

Entity = subgroup (P####). A Landing version is complete when it has every part its
receipts announce (part.total). Classification per subgroup:
  current  newest complete version ≥ the subgroup's lastUpdate
  stale    lastUpdate is later (or the version is unknown: 19700101T000000Z)
  missing  no complete version in Landing
No downloads and no Landing writes; about one API request per subgroup.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import fnmatch
import json
import time

from ingestion.common.r2 import get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gus_bdl.api import Bdl, token

DATA = "01_landing/gus_bdl/snapshot/data/"


def landing_versions(client, bucket: str) -> dict[str, str]:
    """Newest complete version token per subgroup."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=DATA):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str):
        return json.loads(get_with_backoff(client, bucket, key))

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(read, keys))
    parts: dict[tuple[str, str], set] = defaultdict(set)
    totals: dict[tuple[str, str], int] = {}
    for r in receipts:
        ident = (r["subgroup_id"], r["version_token"])
        parts[ident].add((r.get("part") or {}).get("index", 1))
        totals[ident] = int((r.get("part") or {}).get("total", 1))
    newest: dict[str, str] = {}
    for (subgroup, version), seen in parts.items():
        if len(seen) >= totals[(subgroup, version)] and version > newest.get(subgroup, ""):
            newest[subgroup] = version
    return newest


def main(patterns: list[str] | None = None, budget_minutes: float = 50) -> int:
    """Stops checking after budget_minutes (the Pipeline job has 60) and exits 2."""
    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    held = landing_versions(client, bucket)
    api = Bdl()
    subgroups = [
        s for s in api.subgroups() if not patterns or any(fnmatch.fnmatch(s["id"].lower(), p.lower()) for p in patterns)
    ]
    status: Counter = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    for subject in subgroups:
        if time.monotonic() > deadline:
            status["not_checked_time_budget"] += 1
            continue
        sid = subject["id"]
        version = held.get(sid)
        if version is None:
            state = "missing"
        else:
            last = token(api.last_update(sid, subject))
            state = "stale" if version == token(None) or last > version else "current"
        status[state] += 1
        if len(examples[state]) < 15:
            examples[state].append(sid)
    summary = {
        "subgroups_in_bdl": len(subgroups),
        **status,
        "landing_subgroups": len(held),
        "api_requests": api.requests,
        "examples": dict(examples),
    }
    summary["listing_has_last_update"] = any("lastUpdate" in s for s in subgroups[:5])
    annotate("notice", "gus_bdl landing inspect", summary)
    step_summary("## gus_bdl · landing inspect\n\n" + "\n".join(f"- {k}: {v}" for k, v in status.items()) + "\n")
    return 2 if status["not_checked_time_budget"] else 0
