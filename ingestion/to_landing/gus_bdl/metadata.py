#!/usr/bin/env python3
"""GUS BDL Landing metadata: variables per subgroup and the territorial units.

Bronze holds the website exports, whose rows carry dimension labels but no variable id.
Silver maps them through the API's variable list of each subgroup (id, n1…n5, measure
unit), and maps BDL unit ids to TERYT through the unit list. Both are landed as the exact
API responses, one file per page:

  01_landing/gus_bdl/snapshot/metadata/<P####>/<version>__<ingested>__part-NNN.variables.json
  01_landing/gus_bdl/snapshot/metadata/units/<ingested>__<ingested>__part-NNN.units.json

Version of a subgroup's variables = the version of its newest complete data in Landing
(the variables describe that export); subgroups without data are skipped. A subgroup whose
variables are already landed for that version is skipped, so runs resume. The unit list
has no version in the API; it is landed when --units is given (yearly is enough).
Polite: the shared Bdl client keeps below the key's limits; a time budget ends a run
(exit 2, the next run continues).
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import fnmatch
from hashlib import sha256
import json
import os
import time
from typing import Any

from ingestion.common.r2 import get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gus_bdl.api import Bdl, BudgetSpent
from ingestion.to_landing.gus_bdl.inspect import landing_versions

METADATA = "01_landing/gus_bdl/snapshot/metadata/"


def landed_variable_versions(client, bucket: str) -> dict[str, set[str]]:
    """Versions with a complete set of variable pages, per subgroup."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=METADATA):
        keys += [
            o["Key"]
            for o in page.get("Contents", [])
            if o["Key"].endswith(".receipt.json") and "/units/" not in o["Key"]
        ]

    def read(key: str) -> dict[str, Any]:
        return json.loads(get_with_backoff(client, bucket, key))

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(read, keys))
    seen: dict[tuple[str, str], set[int]] = {}
    totals: dict[tuple[str, str], int] = {}
    for r in receipts:
        ident = (r["entity"], r["version_token"])
        seen.setdefault(ident, set()).add(r["part"]["index"])
        totals[ident] = r["part"]["total"]
    out: dict[str, set[str]] = {}
    for ident, parts in seen.items():
        if len(parts) >= totals[ident]:
            out.setdefault(ident[0], set()).add(ident[1])
    return out


def write_pages(
    client, bucket: str, entity: str, version: str, kind: str, pages: list[tuple[str, bytes]], extra: dict[str, Any]
) -> None:
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for index, (url, body) in enumerate(pages, start=1):
        key = f"{METADATA}{entity}/{version}__{ingested}__part-{index:03d}.{kind}.json"
        digest = sha256(body).hexdigest()
        client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json", Metadata={"sha256": digest}
        )
        receipt = {
            "schema_version": 2,
            "kind": "gus_bdl_landing",
            "source_id": "gus_bdl",
            "area": "metadata",
            "channel": "api",
            "entity": entity,
            "metadata_kind": kind,
            "version_token": version,
            "ingested_at": ingested,
            "source_url": url,
            "sha256": digest,
            "size_bytes": len(body),
            "object": {"bucket": bucket, "key": key},
            "part": {"index": index, "total": len(pages)},
            "code_sha": os.environ.get("GITHUB_SHA", ""),
            **extra,
        }
        client.put_object(
            Bucket=bucket,
            Key=key[: -len(".json")] + ".receipt.json",
            ContentType="application/json",
            Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
        )


def fetch_pages(api: Bdl, path: str, **params: Any) -> list[tuple[str, bytes]]:
    """Every page of a listing as exact response bytes."""
    pages, page = [], 0
    while True:
        url, body, parsed = api.get_raw(path, page=page, **{"page-size": 100, **params})
        pages.append((url, body))
        if not (parsed.get("links") or {}).get("next"):
            return pages
        page += 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--filter", default="", help="subgroup globs, e.g. P1*,P2*")
    parser.add_argument("--units", action="store_true", help="also land the territorial unit list")
    parser.add_argument("--dry-run", action="store_true", help="fetch and count only; no writes")
    parser.add_argument("--budget-minutes", type=float, default=300)
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    patterns = [p.strip().lower() for p in args.filter.replace(",", " ").split() if p.strip()]
    data_versions = landing_versions(client, bucket)
    held = {} if args.dry_run else landed_variable_versions(client, bucket)
    todo = sorted(
        s
        for s, v in data_versions.items()
        if (not patterns or any(fnmatch.fnmatch(s.lower(), p) for p in patterns)) and v not in held.get(s, set())
    )
    api, outcomes = Bdl.shared(), Counter()
    api.deadline = deadline
    if args.units:
        unit_pages = []
        for level in range(0, 7):
            unit_pages += fetch_pages(api, "/units", level=level, lang="pl")
        if not args.dry_run:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            write_pages(client, bucket, "units", stamp, "units", unit_pages, {})
        outcomes["unit_pages"] = len(unit_pages)
    for subgroup in todo:
        if time.monotonic() > deadline:
            outcomes["not_started_time_budget"] += 1
            continue
        try:
            pages = fetch_pages(api, "/variables", **{"subject-id": subgroup, "lang": "pl"})
        except BudgetSpent:
            outcomes["not_started_time_budget"] += 1
            continue
        variables = sum(len(json.loads(body).get("results") or []) for _, body in pages)
        if not args.dry_run:
            write_pages(
                client,
                bucket,
                subgroup,
                data_versions[subgroup],
                "variables",
                pages,
                {"subgroup_id": subgroup, "variables": variables},
            )
        outcomes["subgroups_landed"] += 1
        outcomes["variable_pages"] += len(pages)
        outcomes["variables"] += variables
    summary = {
        "dry_run": args.dry_run,
        "subgroups_with_data": len(data_versions),
        "selected": len(todo),
        "api_requests": api.requests,
        **outcomes,
    }
    print(json.dumps(summary, sort_keys=True))
    annotate("notice", "gus_bdl landing metadata", summary)
    step_summary(f"## gus_bdl · landing metadata\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return 2 if outcomes["not_started_time_budget"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
