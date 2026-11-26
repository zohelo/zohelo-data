"""dane.gov.pl catalogue via data.europa.eu: every dataset record of the harvested catalogue, reduced before Landing.

Route (Dev probes 45813541234 and 45815120866, 26 Nov 2026): dane.gov.pl does not answer GitHub runners, so the
catalogue is read from the official EU republisher, the search API of data.europa.eu (no key, no published limit):
``https://data.europa.eu/api/hub/search/search?filter=dataset&facets={"catalog":["dane-gov-pl"]}``. Plain paging
(``page``, ``limit``) is unusable: the index serves at most 10,000 records that way and its default order is not
stable (probe 45813541234: 10,000 rows read, 7,751 distinct). The scroll is: ``scroll=true`` on the first request
returns a ``scrollId``, ``/api/hub/search/scroll?scrollId=<id>`` the next page of the same snapshot of the index,
until a page comes back empty (probe 45815120866: 10,001 of 10,001 records in 12 pages of 1,000). ``includes``
limits each record to the fields named, which cuts the transfer to about a third (3.0 GB for the full records).

Reduction: each record keeps the catalogue fields of ``reduce`` (identifier, Polish title, publisher name, type and
dane.gov.pl institution link, issued and modified, periodicity, categories, high-value flag, the harvest record's
times, and per distribution its id, format, media type, size, times, status, resource link and Polish title). Not
kept, so never written: descriptions in 24 languages, keywords, the publisher's and contact points' e-mail
addresses, translations' metadata. The distributions' files are not read (they are on api.dane.gov.pl).

Completeness: the catalogue lands only when the distinct dataset ids equal the ``count`` of the first answer;
a record repeated with the same content is counted, one repeated with other content stops the run. A failed request
restarts the scroll once (a lost answer would otherwise skip a page). Landing: one entity ``catalogue``, one JSON line
per dataset sorted by id with sorted keys, so an unchanged catalogue gives the same bytes and is not landed again
(receipt after the file). Version = download time (the API gives no version of the harvest).
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any

import requests

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.dane_gov_pl import ENTITY, SOURCE
from ingestion.to_landing.patterns import bulk_file

API = "https://data.europa.eu/api/hub/search"
CATALOGUE = "dane-gov-pl"
FACETS = json.dumps({"catalog": [CATALOGUE]}, separators=(",", ":"))
PAGE_SIZE = 1000  # accepted by the API (probe 45813541234); 12 pages for the catalogue
PAUSE_SECONDS = 1.0  # between requests: one request at a time, a second apart (register PLG-030)
MAX_PAGES = 100  # a guard against a scroll that never ends
PASSES = 2  # a failed request restarts the scroll once
RETRY_WAITS = (15, 45, 120)  # seconds before the second, third and fourth attempt of the first request
RETRY_STATUSES = {408, 429, 500, 502, 503, 504}
INCLUDES = ",".join(
    (
        "id", "identifier", "landing_page", "title.pl", "publisher.name", "publisher.type", "publisher.resource",
        "modified", "issued", "accrual_periodicity.resource", "categories.id", "categories.resource", "is_hvd",
        "catalog_record", "country.id", "distributions.id", "distributions.format.id",
        "distributions.format.resource", "distributions.media_type", "distributions.byte_size",
        "distributions.modified", "distributions.issued", "distributions.access_url", "distributions.status.label",
        "distributions.title.pl",
    )
)  # fmt: skip


class Stop(RuntimeError):
    """The API refused, kept failing or served an incomplete catalogue: nothing is landed."""


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _first(value: Any, key: str | None = None) -> str | None:
    """The first string of a list (or of the ``key`` of its first object)."""
    if isinstance(value, list) and value:
        item = value[0]
        return _text(item.get(key)) if key and isinstance(item, dict) else _text(item)
    return None


def _get(value: Any, *path: str) -> Any:
    for key in path:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def reduce_distribution(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict) or not _text(item.get("id")):
        raise ValueError("distribution without an id")
    size = item.get("byte_size")
    return {
        "id": item["id"],
        "format": _text(_get(item, "format", "id")),
        "format_resource": _text(_get(item, "format", "resource")),
        "media_type": _text(item.get("media_type")),
        "byte_size": size if isinstance(size, int) and not isinstance(size, bool) else None,
        "modified": _text(item.get("modified")),
        "issued": _text(item.get("issued")),
        "access_url": _first(item.get("access_url")),
        "status": _text(_get(item, "status", "label")),
        "title_pl": _text(_get(item, "title", "pl")),
    }


def reduce(record: Any) -> dict[str, Any]:
    """The allowlisted dataset record; raises ValueError when the id is missing."""
    if not isinstance(record, dict) or not _text(record.get("id")):
        raise ValueError("dataset without an id")
    categories = record.get("categories") if isinstance(record.get("categories"), list) else []
    distributions = record.get("distributions") if isinstance(record.get("distributions"), list) else []
    hvd = record.get("is_hvd")
    return {
        "id": record["id"],
        "identifier": _first(record.get("identifier")),
        "landing_page": _first(record.get("landing_page"), "resource"),
        "title_pl": _text(_get(record, "title", "pl")),
        "publisher_name": _text(_get(record, "publisher", "name")),
        "publisher_type": _text(_get(record, "publisher", "type")),
        "publisher_resource": _text(_get(record, "publisher", "resource")),
        "modified": _text(record.get("modified")),
        "issued": _text(record.get("issued")),
        "accrual_periodicity": _text(_get(record, "accrual_periodicity", "resource")),
        "categories": [
            {"id": _text(c.get("id")), "resource": _text(c.get("resource"))} for c in categories if isinstance(c, dict)
        ],
        "is_hvd": hvd if isinstance(hvd, bool) else None,
        "catalog_record_issued": _text(_get(record, "catalog_record", "issued")),
        "catalog_record_modified": _text(_get(record, "catalog_record", "modified")),
        "country": _text(_get(record, "country", "id")),
        "distributions": sorted((reduce_distribution(d) for d in distributions), key=lambda d: d["id"]),
    }


def encode(records: dict[str, dict[str, Any]]) -> bytes:
    """One JSON line per dataset, sorted by id and by key: the same records give the same bytes."""
    lines = [json.dumps(records[k], ensure_ascii=False, sort_keys=True, separators=(",", ":")) for k in sorted(records)]
    return ("\n".join(lines) + "\n").encode("utf-8") if lines else b""


class Client:
    def __init__(self, pause_seconds: float = PAUSE_SECONDS) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"})
        self.pause, self.requests, self.bytes = pause_seconds, 0, 0

    def call(self, path: str, params: dict[str, Any], waits: tuple[int, ...] = RETRY_WAITS) -> dict[str, Any]:
        """One answer's ``result``; retried only where a repeat cannot skip data (``waits``)."""
        last = ""
        for wait in (0, *waits):
            time.sleep(self.pause + wait)
            self.requests += 1
            try:
                answer = self.session.get(f"{API}{path}", params=params, timeout=(20, 300))
            except requests.RequestException as exc:
                last = f"{type(exc).__name__}: {str(exc)[:160]}"
                continue
            if answer.status_code in RETRY_STATUSES:
                last = f"HTTP {answer.status_code}"
                continue
            if answer.status_code != 200:
                raise Stop(f"{path}: HTTP {answer.status_code} {answer.text[:160]}")
            self.bytes += len(answer.content)
            try:
                result = answer.json().get("result")
            except (ValueError, AttributeError) as exc:
                raise Stop(f"{path}: not a JSON object answer") from exc
            if not isinstance(result, dict) or not isinstance(result.get("results"), list):
                raise Stop(
                    f"{path}: unknown answer layout {sorted(result) if isinstance(result, dict) else type(result)}"
                )
            return result
        raise Stop(f"{path}: no answer after {len(waits) + 1} attempts ({last})")

    def count(self) -> int | None:
        result = self.call("/search", {"filter": "dataset", "facets": FACETS, "limit": 1, "includes": "id"})
        return result.get("count") if isinstance(result.get("count"), int) else None

    def scroll(self, deadline: float) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        """Every dataset of the catalogue, reduced, from one scroll; with the facts for the receipt."""
        first = {"filter": "dataset", "facets": FACETS, "limit": PAGE_SIZE, "scroll": "true", "includes": INCLUDES}
        result = self.call("/search", first)
        count, records, pages, served = result.get("count"), {}, 1, 0
        while True:
            for raw in result["results"]:
                record = reduce(raw)
                served += 1
                held = records.get(record["id"])
                if held is not None and held != record:
                    raise Stop(f"{record['id']}: served twice with other content")
                records[record["id"]] = record
            scroll_id = result.get("scrollId")
            if not result["results"] or not scroll_id:
                break
            if time.monotonic() > deadline:
                raise Stop(f"time budget ended after {pages} pages")
            if pages >= MAX_PAGES:
                raise Stop(f"more than {MAX_PAGES} pages")
            result = self.call("/scroll", {"scrollId": scroll_id}, waits=())  # a repeat could skip a page
            pages += 1
        if not isinstance(count, int) or len(records) != count:
            raise Stop(f"{len(records)} distinct datasets, the API reports count {count}")
        distributions = sum(len(r["distributions"]) for r in records.values())
        facts = {"pages": pages, "count": count, "repeated": served - len(records), "distributions": distributions}
        return records, facts


def catalogue(client: Client, deadline: float) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """The catalogue from a complete scroll; a failed scroll is started again once (PASSES)."""
    for attempt in range(PASSES):
        try:
            records, facts = client.scroll(deadline)
            return records, {**facts, "scroll_passes": attempt + 1}
        except Stop as exc:
            if attempt == PASSES - 1 or time.monotonic() > deadline:
                raise
            print(json.dumps({"status": "scroll restarted", "error": str(exc)[:300]}), flush=True)
    raise Stop("unreachable")


def newest_modified(records: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The newest dataset and harvest-record times (the harvest lag is measured against them)."""
    modified = [r["modified"] for r in records.values() if r["modified"]]
    harvested = [r["catalog_record_modified"] for r in records.values() if r["catalog_record_modified"]]
    return {"newest_modified": max(modified, default=None), "newest_harvest_record": max(harvested, default=None)}


def _land(client: Any, bucket: str, records: dict, facts: dict, held: list, landing_root: str) -> bool:
    """Write the catalogue as a new version unless the bytes are those of its newest copy."""
    body = encode(records)
    digest = sha256(body).hexdigest()
    if not body or (held and held[-1]["sha256"] == digest):
        return False
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{bulk_file.prefix(SOURCE, ENTITY, landing_root)}{ingested}__{ingested}.jsonl"
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson", Metadata={"sha256": digest}
    )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": ENTITY,
        "content": "snapshot",
        "area": "data",
        "version_token": ingested,
        "version_basis": "download time",
        "ingested_at": ingested,
        "source_url": f"{API}/search?filter=dataset&facets={FACETS}&limit={PAGE_SIZE}&scroll=true&includes=<fields>",
        "final_url": f"{API}/scroll",
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": SOURCE,
            "format": "jsonl",
            "reduced": "allowlist of catalogue fields (ingestion/to_landing/dane_gov_pl/extract.py)",
            "records": len(records),
            **facts,
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return True


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    started = time.monotonic()
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    api = Client()
    summary: dict[str, Any] = {"source": SOURCE, "mode": "extract", "entity": ENTITY}
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, landing_root))
    try:
        records, facts = catalogue(api, started + budget_minutes * 60)
    except (Stop, ValueError) as exc:
        summary.update({"outcome": "failed", "error": str(exc)[:300]})
    else:
        landed = _land(client, bucket, records, facts, held, landing_root)
        summary.update({"outcome": "landed" if landed else "unchanged", "records": len(records), **facts})
        summary.update(newest_modified(records))
    summary.update(
        {"requests": api.requests, "mb_read": round(api.bytes / 1e6), "seconds": round(time.monotonic() - started)}
    )
    status = 1 if summary["outcome"] == "failed" else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing extract", summary)
    step_summary(f"## {SOURCE} · landing extract\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


def inspect(landing_root: str = "01_landing") -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, landing_root))
    newest = held[-1] if held else None
    errors = []
    try:
        reported = Client().count()
    except Stop as exc:
        reported = None
        errors.append(str(exc)[:200])
    summary = {
        "source": SOURCE,
        "mode": "inspect",
        "files": [
            {
                "entity": ENTITY,
                "landed_versions": len(held),
                "newest": newest["version_token"] if newest else None,
                "newest_records": (newest.get("detail") or {}).get("records") if newest else None,
                "api_count": reported,
                "state": "missing" if not held else "unknown",  # the records are compared when extract runs
            }
        ],
        "errors": errors,
    }
    print(json.dumps(summary, sort_keys=True))
    annotate("error" if errors else "notice", f"{SOURCE} landing inspect", summary)
    return 1 if errors else 0
