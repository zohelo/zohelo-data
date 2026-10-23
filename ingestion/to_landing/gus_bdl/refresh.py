#!/usr/bin/env python3
"""GUS BDL landing.extract: API refresh of changed subgroups (owner decision 13 Oct: the website
export is the initial load, the API refreshes subgroups that changed).

Daily, a third of the subgroups held in Landing is checked against the API (``/subjects/{id}``
lastUpdate; each subgroup is checked every 3 days, ~800 requests). A subgroup whose lastUpdate is
newer than its newest complete Landing version is refreshed whole: every variable × unit level the
subject publishes, page by page (``/data/by-variable``), landed as the exact responses:

  01_landing/gus_bdl/snapshot/data/<P####>/<lastUpdate>__<ingested>__part-NNN.api.json  (+ receipt)

A subgroup's pages are fetched into memory first; a run stopped by its budget stores nothing of
that subgroup. Receipts are written only after every page is in R2, so a run that dies while
storing leaves no partial version (its unreceipted pages stay as unreferenced objects, never read).
Each receipt carries what Bronze needs to read its page alone: the variable's dimension labels
(n1…n5), measure unit and the subject's dimension names. Subgroups never exported are not loaded
here (initial load = website export). The shared Bdl client keeps below the key's limits; a time
budget ends a run (exit 2) and the next run continues.

--initial with --only: the listed subgroups are loaded through the API even when never exported
(owner decision #889, 22 Oct: the ~100 subgroups whose whole API load costs at most ~1,000
requests; everything else comes from the website).
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any

import requests

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gus_bdl.api import Bdl, BudgetSpent, token
from ingestion.to_landing.gus_bdl.inspect import landing_versions

DATA = "01_landing/gus_bdl/snapshot/data/"
CHECK_SHARE = 3  # each subgroup is checked every CHECK_SHARE days


def todays_share(subgroups: list[str], day: int | None = None) -> list[str]:
    """The subgroups checked today: a stable third by hash, rotating daily."""
    day = datetime.now(UTC).timetuple().tm_yday if day is None else day
    return [s for s in subgroups if int(sha256(s.encode()).hexdigest(), 16) % CHECK_SHARE == day % CHECK_SHARE]


def variable_descriptor(v: dict[str, Any]) -> dict[str, Any]:
    labels = [v.get(f"n{i}") for i in range(1, 6)]
    return {
        "id": v.get("id"),
        "labels": [x for x in labels if x is not None],
        "measure_unit": v.get("measureUnitName"),
        "level": v.get("level"),
    }


def fetch_subgroup(api: Bdl, subgroup: str, subject: dict[str, Any]) -> list[dict[str, Any]]:
    """Every data page of the subgroup with its descriptor: [{url, body, variable}]."""
    variables = list(api.pages("/variables", **{"subject-id": subgroup, "lang": "pl"}))
    levels = sorted({int(x) for x in subject.get("levels") or range(0, 7)})
    pages = []
    for v in variables:
        descriptor = variable_descriptor(v)
        for level in levels:
            page = 0
            while True:
                url, body, parsed = api.get_raw(
                    f"/data/by-variable/{v['id']}", **{"unit-level": level, "page": page, "page-size": 100}
                )
                if parsed.get("results"):
                    pages.append({"url": url, "body": body, "variable": descriptor, "unit_level": level})
                if not (parsed.get("links") or {}).get("next"):
                    break
                page += 1
    return pages


def land(client, bucket: str, subgroup: str, version: str, subject: dict[str, Any], pages: list[dict]) -> int:
    """Data pages first, receipts after all pages are stored (no partial version)."""
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    stored = []
    for index, page in enumerate(pages, start=1):
        key = f"{DATA}{subgroup}/{version}__{ingested}__part-{index:04d}.api.json"
        digest = sha256(page["body"]).hexdigest()
        client.put_object(
            Bucket=bucket, Key=key, Body=page["body"], ContentType="application/json", Metadata={"sha256": digest}
        )
        stored.append((index, key, digest, page))
    for index, key, digest, page in stored:
        receipt = {
            "schema_version": 2,
            "kind": "gus_bdl_landing",
            "source_id": "gus_bdl",
            "area": "data",
            "channel": "api",
            "subgroup_id": subgroup,
            "version_token": version,
            "ingested_at": ingested,
            "source_url": page["url"],
            "sha256": digest,
            "size_bytes": len(page["body"]),
            "object": {"bucket": bucket, "key": key},
            "part": {"index": index, "total": len(stored), "unit_level": page["unit_level"]},
            "variable": page["variable"],
            "dimension_names": [d.get("name") if isinstance(d, dict) else d for d in subject.get("dimensions") or []],
            "code_sha": os.environ.get("GITHUB_SHA", ""),
        }
        client.put_object(
            Bucket=bucket,
            Key=key[: -len(".json")] + ".receipt.json",
            ContentType="application/json",
            Body=json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True).encode(),
        )
    return len(stored)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="*", help="subgroups to check (all of them, not today's share)")
    parser.add_argument("--budget-minutes", type=float, default=300)
    parser.add_argument("--dry-run", action="store_true", help="check only; no downloads, no writes")
    parser.add_argument(
        "--initial", action="store_true", help="also load listed subgroups never exported (owner decision #889)"
    )
    parser.add_argument(
        "--max-requests", type=int, default=0, help="stop starting subgroups after this many API requests (0: no cap)"
    )
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.budget_minutes * 60
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    held = landing_versions(client, bucket)
    check = sorted(args.only) if args.only else todays_share(sorted(held))
    api, outcomes, refreshed, stale, without_data, gone = Bdl.shared(), Counter(), [], [], [], []
    api.deadline = deadline
    for position, subgroup in enumerate(check):
        if time.monotonic() > deadline:
            outcomes["not_checked_time_budget"] += 1
            continue
        if subgroup not in held and not args.initial:
            outcomes["not_in_landing_skipped"] += 1  # initial load is the website export
            continue
        try:
            subject = api.get(f"/subjects/{subgroup}", lang="pl")
            version = token(api.last_update(subgroup, subject))
        except BudgetSpent:
            outcomes["not_checked_time_budget"] += len(check) - position
            break
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 404:
                raise
            outcomes["gone_at_source"] += 1  # the API no longer has this subgroup (P1342, 22 Oct)
            gone.append(subgroup)
            continue
        if subgroup in held and version <= held[subgroup] and held[subgroup] != token(None):
            outcomes["current"] += 1
            continue
        stale.append(subgroup)
        if args.dry_run:
            continue
        # a cheap subgroup costs up to ~1,000 requests: start one only if it fits under the cap
        if args.max_requests and api.requests + 1000 > args.max_requests:
            outcomes["stale_not_started_request_cap"] += 1
            continue
        if deadline - time.monotonic() < 20 * 60:  # a large subgroup can take most of an hour
            outcomes["stale_not_started_time_budget"] += 1
            continue
        try:
            pages = fetch_subgroup(api, subgroup, subject)
        except BudgetSpent:  # nothing of this subgroup is stored yet (pages are kept in memory)
            outcomes["stale_not_started_time_budget"] += 1
            break
        if not pages:
            outcomes["stale_without_data"] += 1
            without_data.append(subgroup)
            continue
        land(client, bucket, subgroup, version, subject, pages)
        refreshed.append({"subgroup": subgroup, "version": version, "pages": len(pages)})
        outcomes["refreshed"] += 1
    summary = {
        "checked": len(check),
        "stale": len(stale),
        "stale_sample": stale[:15],
        "refreshed": refreshed[:20],
        "without_data": without_data,
        "gone_at_source": gone,
        "api_requests": api.requests,
        "dry_run": args.dry_run,
        **outcomes,
    }
    print(json.dumps(summary, sort_keys=True))
    annotate("notice", "gus_bdl landing refresh (API)", summary)
    step_summary(f"## gus_bdl · API refresh\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    left = outcomes["not_checked_time_budget"] + outcomes["stale_not_started_time_budget"]
    return 2 if left else 0


if __name__ == "__main__":
    raise SystemExit(main())
