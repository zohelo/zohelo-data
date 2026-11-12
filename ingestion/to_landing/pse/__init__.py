"""PSE Reports API v2 Landing activities (register PL-ENV-019).

PSE (Polskie Sieci Elektroenergetyczne) publishes its power-system and market reports through the public API
https://api.raporty.pse.pl/api/<report>?$filter=business_date eq '<YYYY-MM-DD>' (no key, JSON ``{"value": [...]}``,
one row per 15-minute period, 92/96/100 rows a day). Two reports are built (REPORTS): ``rce-pln`` (the market price
of electricity RCE, PLN/MWh, published the day before the business day) and ``his-wlk-cal`` (power-system values:
demand, wind ``wi``, photovoltaics ``pv``, generating-unit groups and cross-border balances, MW, published after
the day and re-published later). Entity = report x business day (``rce_pln_YYYYMMDD``,
``his_wlk_cal_YYYYMMDD``); version = the newest ``publication_ts_utc`` of its rows (the API sends no Last-Modified
or ETag; Dev probe 43284836734); a copy lands only when its SHA-256 differs from the newest landed copy (repeated
requests return identical bytes). An empty answer (a day not published yet) is not landed.

landing.inspect  the business days FIRST_DAY .. yesterday (Europe/Warsaw) vs Landing per report (missing / held),
                 and yesterday's answer of each report against its newest copy (current / stale); no writes
landing.extract  missing days first (oldest first), then the newest REREAD_DAYS days again (PSE re-publishes
                 his-wlk-cal days after the day); one request at a time, at most one started a second; stops starting new
                 requests at the run's time budget, the next run continues (restart unit: one report-day)
landing.verify   every receipt has its file in Landing or the Archive
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
import os
import time
from typing import Any
from urllib.parse import quote

SOURCE = "pse"
API = "https://api.raporty.pse.pl/api"
REPORTS = {"rce_pln": "rce-pln", "his_wlk_cal": "his-wlk-cal"}  # entity prefix -> API report
FIRST_DAY = date(2024, 6, 14)  # first business day of API v2 (rce-pln: none for 2024-06-13)
REREAD_DAYS = 7  # newest business days requested again on every run (corrections)
PAUSE = 1.0  # seconds between the starts of two requests: at most one a second


def yesterday(now: datetime | None = None) -> date:
    """The last complete business day: yesterday in Europe/Warsaw."""
    from zoneinfo import ZoneInfo

    return (now or datetime.now(UTC)).astimezone(ZoneInfo("Europe/Warsaw")).date() - timedelta(days=1)


def entity(report: str, day: date) -> str:
    return f"{report}_{day:%Y%m%d}"


def url(report: str, day: date) -> str:
    return f"{API}/{REPORTS[report]}?$filter=" + quote(f"business_date eq '{day:%Y-%m-%d}'")


def days(first: date, last: date) -> list[date]:
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def listing(client: Any, bucket: str, landing_root: str) -> tuple[dict[str, list[str]], set[str]]:
    """One listing of the source's Landing data: receipt keys per entity (oldest landed first) and data keys."""
    root = f"{landing_root}/{SOURCE}/snapshot/data/"
    receipts: dict[str, list[str]] = {}
    data: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=root):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith(".receipt.json"):
                receipts.setdefault(key[len(root) :].split("/", 1)[0], []).append(key)
            else:
                data.add(key)
    for keys in receipts.values():  # <version>__<ingested>.receipt.json: order by the ingest stamp
        keys.sort(key=lambda k: k.rsplit("/", 1)[-1].split("__", 1)[-1])
    return receipts, data


def read_receipts(client: Any, bucket: str, keys: list[str]) -> list[dict[str, Any]]:
    """The receipts behind these keys (8 reads at a time; R2 throttles bursts)."""
    from concurrent.futures import ThreadPoolExecutor

    from ingestion.common.r2 import get_with_backoff

    def one(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    with ThreadPoolExecutor(max_workers=8) as pool:
        return list(pool.map(one, keys))


def newest_receipts(client: Any, bucket: str, landing_root: str = "01_landing") -> list[dict[str, Any]]:
    """Receipt of the newest landed copy of every entity (used by bronze.load)."""
    receipts, _ = listing(client, bucket, landing_root)
    return read_receipts(client, bucket, [keys[-1] for keys in receipts.values()])


def version_of(rows: list[dict[str, Any]]) -> str:
    """Newest publication_ts_utc of the rows as a layout timestamp (YYYYMMDDTHHMMSSZ)."""
    stamps = [str(r.get("publication_ts_utc") or "") for r in rows]
    newest = max(stamps)
    moment = datetime.strptime(newest[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    return moment.strftime("%Y%m%dT%H%M%SZ")


def land_day(
    client: Any, bucket: str, report: str, day: date, landing_root: str, held: dict[str, Any] | None
) -> dict[str, Any]:
    """Request one report-day and land it with a receipt unless it is empty or its bytes are unchanged."""
    from pathlib import Path
    import tempfile

    from ingestion.to_landing.patterns import bulk_file

    name = entity(report, day)
    source_url = url(report, day)
    with tempfile.TemporaryDirectory(prefix="zohelo-pse-") as temporary:
        path = Path(temporary) / "file.json"
        digest, size, headers, final_url, _ = bulk_file.download(source_url, path, "raw", read_timeout=120, wait=30)
        if held and held["sha256"] == digest:
            return {"entity": name, "outcome": "unchanged"}
        rows = json.loads(path.read_bytes())["value"]
        if not rows:
            return {"entity": name, "outcome": "empty, not landed"}
        dates = {str(r.get("business_date")) for r in rows}
        if dates != {f"{day:%Y-%m-%d}"}:
            raise RuntimeError(f"{name}: answer holds business dates {sorted(dates)[:5]}")
        version, ingested = version_of(rows), bulk_file.stamp(datetime.now(UTC))
        key = f"{bulk_file.prefix(SOURCE, name, landing_root)}{version}__{ingested}.json"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": "application/json", "Metadata": {"sha256": digest}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": name,
        "content": "snapshot",
        "area": "data",
        "version_token": version,
        "version_basis": "publication_ts_utc (newest of the rows)",
        "ingested_at": ingested,
        "source_url": source_url,
        "final_url": final_url,
        "sha256": digest,
        "size_bytes": size,
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "pse",
            "format": "json",
            "report": REPORTS[report],
            "business_date": f"{day:%Y-%m-%d}",
            "rows": len(rows),
            "http": {"content_type": headers.get("Content-Type")},
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {"entity": name, "outcome": "landed_changed" if held else "landed_new", "rows": len(rows), "bytes": size}


def plan(receipts: dict[str, list[str]], last: date) -> list[tuple[str, date]]:
    """Missing report-days (oldest first), then the newest REREAD_DAYS days that are held."""
    missing = [(r, d) for r in REPORTS for d in days(FIRST_DAY, last) if entity(r, d) not in receipts]
    window = days(max(FIRST_DAY, last - timedelta(days=REREAD_DAYS - 1)), last)
    reread = [(r, d) for d in window for r in REPORTS if entity(r, d) in receipts]
    return sorted(missing, key=lambda x: (x[1], x[0])) + reread


def extract(budget_minutes: float, landing_root: str = "01_landing", last: date | None = None) -> int:
    """landing.extract; ``last`` (default yesterday) and ``landing_root`` exist for Dev trials."""
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    started = time.monotonic()
    last = last or yesterday()
    receipts, _ = listing(client, bucket, landing_root)
    todo = plan(receipts, last)
    held = {
        r["entity"]: r
        for r in read_receipts(
            client, bucket, [receipts[entity(rp, d)][-1] for rp, d in todo if entity(rp, d) in receipts]
        )
    }
    outcomes: dict[str, int] = {}
    landed_bytes, errors, stopped, previous = 0, [], 0, 0.0
    for n, (report, day) in enumerate(todo):
        if time.monotonic() - started > budget_minutes * 60:
            stopped = len(todo) - n
            break
        time.sleep(max(0.0, previous + PAUSE - time.monotonic()))  # request starts at least PAUSE seconds apart
        previous = time.monotonic()
        try:
            result = land_day(client, bucket, report, day, landing_root, held.get(entity(report, day)))
        except (bulk_file.PermanentError, RuntimeError, ValueError, KeyError, OSError) as exc:  # OSError: requests
            errors.append({"entity": entity(report, day), "error": str(exc)[:200]})
            if len(errors) >= 10:  # the API or the layout changed: stop instead of hammering it
                break
            continue
        outcomes[result["outcome"]] = outcomes.get(result["outcome"], 0) + 1
        landed_bytes += result.get("bytes", 0)
    summary = {
        "source": SOURCE,
        "mode": "load",
        "last_business_day": str(last),
        "planned": len(todo),
        "outcomes": outcomes,
        "landed_bytes": landed_bytes,
        "not_started_budget": stopped,
        "errors": errors,
        "minutes": round((time.monotonic() - started) / 60, 1),
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    status = 1 if errors else 0
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing extract", summary)
    step_summary(f"## {SOURCE} · landing extract\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


def inspect(landing_root: str = "01_landing", last: date | None = None) -> int:
    """landing.inspect: days expected vs held per report; yesterday's answer vs its newest copy; no writes."""
    from hashlib import sha256

    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or yesterday()
    receipts, _ = listing(client, bucket, landing_root)
    status, reports = 0, {}
    for n, report in enumerate(REPORTS):
        expected = days(FIRST_DAY, last)
        held = [d for d in expected if entity(report, d) in receipts]
        row: dict[str, Any] = {
            "expected_days": len(expected),
            "held": len(held),
            "missing": len(expected) - len(held),
            "missing_sample": [str(d) for d in expected if entity(report, d) not in receipts][:5],
            "first_held": str(held[0]) if held else None,
            "last_held": str(held[-1]) if held else None,
        }
        if n:
            time.sleep(PAUSE)
        try:
            answer = requests.get(url(report, last), headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 120))
            answer.raise_for_status()
            rows = len(answer.json()["value"])
        except (requests.RequestException, ValueError, KeyError) as exc:
            row["yesterday"] = {"state": "unreachable", "error": str(exc)[:200]}
            status = 1
        else:
            newest = receipts.get(entity(report, last))
            state = "missing"
            if newest:
                held_sha = read_receipts(client, bucket, [newest[-1]])[0]["sha256"]
                state = "current" if held_sha == sha256(answer.content).hexdigest() else "stale"
            row["yesterday"] = {"state": state, "rows": rows}
        reports[REPORTS[report]] = row
    summary = {"source": SOURCE, "mode": "inspect", "last_business_day": str(last), "reports": reports}
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing inspect", summary)
    return status


def verify(landing_root: str = "01_landing") -> int:
    """landing.verify: every receipt has its data file in Landing or the Archive."""
    from ingestion.common.r2 import archive_key, archived, req, s3
    from ingestion.common.report import annotate

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    receipts, data = listing(client, bucket, landing_root)
    stored = archived(client, bucket, SOURCE)
    rows = read_receipts(client, bucket, [k for keys in receipts.values() for k in keys])
    missing = [
        r["object"]["key"]
        for r in rows
        if r["object"]["key"] not in data
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    summary = {
        "source": SOURCE,
        "mode": "verify",
        "entities": len(receipts),
        "receipts": len(rows),
        "in_landing": sum(r["object"]["key"] in data for r in rows),
        "missing_data": len(missing),
        "missing_sample": missing[:5],
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if not missing else "error", f"{SOURCE} landing verify", summary)
    return 1 if missing else 0


ACTIVITIES = {
    "landing.inspect": lambda p: inspect(),
    "landing.extract": lambda p: extract(p.budget_minutes),
    "landing.verify": lambda p: verify(),
}
