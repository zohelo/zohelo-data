#!/usr/bin/env python3
"""OpenAlex Landing: counts of works with an author affiliated in Poland and the Polish institutions, as two
bundles of exact API answers (register CD-028, #2996).

Route (docs/sources/openalex.md). The bulk snapshot (about 750 GB per format) is far beyond this selection and the
free storage tier, so the documented REST API ``https://api.openalex.org`` is read with aggregate requests only:

- entity ``works_pl``: ``/works?filter=institutions.country_code:PL&group_by=publication_year`` (every work with at
  least one authorship affiliated with an institution in Poland, counted once, by publication year), the same with
  ``open_access.is_oa:true``, the list of fields (``/fields``, the 26 fields of the OpenAlex topic classification)
  and, per field, ``...,primary_topic.field.id:<id>&group_by=publication_year``. Counts only: no work, title or
  author name is requested.
- entity ``institutions_pl``: ``/institutions?filter=country_code:PL`` with the root fields ``select``ed (id, ROR,
  name, type, geo, works and citation counts, counts by year, lineage, updated date); organisations, no persons.

Every request pages by cursor (``cursor=*``, ``per_page=200``) until the answer gives no ``next_cursor``; one
member per page. A ZIP of the exact answer bodies with fixed member dates is the Landing file of each entity.

Version = the time the read started (the API gives no update time for an aggregate). The answers carry timing
fields (``meta.db_response_time_ms``) and cursors that differ on every read, so whether a read changed is decided
on its content: the members' items, sorted, and the counts of ``meta`` (``content_sha256`` in the receipt). A read
whose content equals the newest landed copy's writes nothing. A read that fails part-way lands nothing.

Polite access (help.openalex.org/access/pricing/, register CD-028): no key; the keyless budget is shared per IP
address, a few tens of requests a week is a small part of it. One request at a time, PAUSE_SECONDS apart, an
identifying User-Agent, timeouts, bounded retries with backoff on 429, 5xx, timeouts and refused connections
(Retry-After honoured, at most RETRY_AFTER_MAX). A refusal that persists fails the read; the next step is then a
free OpenAlex key (an owner question), never a retry pattern.

Layout (docs/landing.md): ``<root>/openalex/snapshot/data/<entity>/<version>__<ingested>.zip`` + receipt.
Modes: inspect (newest copy held, no requests), load (read and land when changed), verify (every receipt has its
data file, in Landing or the Archive).
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
import re
import time
from typing import Any
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "openalex"
API = "https://api.openalex.org"
COUNTRY = "PL"
WORKS_FILTER = f"institutions.country_code:{COUNTRY}"
ENTITIES = ("works_pl", "institutions_pl")
INSTITUTION_FIELDS = (
    "id,ror,display_name,type,country_code,geo,works_count,cited_by_count,counts_by_year,lineage,updated_date"
)
FIELD_ID = re.compile(r"https://openalex\.org/fields/(\d+)")
PER_PAGE = 200
MAX_PAGES = 50  # per request; the institutions of Poland fit in about 15
PAUSE_SECONDS = 1.0
TIMEOUT = (30, 120)
RETRY_DELAYS = (10, 30, 90, 180)
RETRY_AFTER_MAX = 300
MARGIN_MINUTES = 3


def fetch(session: requests.Session, path: str, params: dict[str, Any], deadline: float) -> tuple[bytes, Any]:
    """GET one answer: (exact body, parsed JSON). 429, 5xx and network errors are retried with backoff; any other
    status fails at once."""
    last = ""
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if time.monotonic() + delay > deadline:
            break
        time.sleep(delay + PAUSE_SECONDS)
        try:
            response = session.get(API + path, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last = type(exc).__name__
            continue
        if response.status_code == 200:
            try:
                parsed = response.json()
            except ValueError as exc:
                raise RuntimeError(f"OpenAlex answered no JSON for {path}: {response.content[:200]!r}") from exc
            if not isinstance(parsed, dict) or not isinstance(parsed.get("meta"), dict):
                raise RuntimeError(f"OpenAlex answer without meta for {path}: {response.content[:200]!r}")
            return response.content, parsed
        last = f"HTTP {response.status_code} {response.text[:200]}"
        if response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"OpenAlex {last} for {path} {params}")
        retry_after = response.headers.get("Retry-After", "")
        if retry_after.isdigit() and attempt < len(RETRY_DELAYS):
            time.sleep(min(RETRY_AFTER_MAX, int(retry_after)))
    raise RuntimeError(f"OpenAlex kept failing ({last}) for {path} {params}")


def pages(
    session: requests.Session, name: str, path: str, params: dict[str, Any], deadline: float
) -> list[tuple[str, bytes, Any]]:
    """Every page of one request by cursor: [(member name, exact body, parsed)]."""
    out: list[tuple[str, bytes, Any]] = []
    cursor = "*"
    items_key = "group_by" if "group_by" in params else "results"
    for n in range(1, MAX_PAGES + 1):
        body, parsed = fetch(session, path, {**params, "per_page": PER_PAGE, "cursor": cursor}, deadline)
        out.append((f"{name}__p{n:03d}.json", body, parsed))
        cursor = parsed["meta"].get("next_cursor")
        if not cursor or not parsed.get(items_key):
            return out
    raise RuntimeError(f"{name}: more than {MAX_PAGES} pages")


def field_ids(members: list[tuple[str, bytes, Any]]) -> list[str]:
    """The numeric ids of the fields list (``https://openalex.org/fields/17`` → ``17``), each once, sorted."""
    ids = set()
    for _, _, parsed in members:
        for item in parsed.get("results") or []:
            match = FIELD_ID.fullmatch(str(item.get("id")))
            if not match:
                raise RuntimeError(f"fields: unexpected id {item.get('id')!r}")
            ids.add(match.group(1))
    if not ids:
        raise RuntimeError("fields: the list is empty")
    return sorted(ids, key=int)


def read(session: requests.Session, entity: str, deadline: float) -> list[tuple[str, bytes, Any]]:
    """Every answer of one entity, in read order."""
    if entity == "institutions_pl":
        return pages(
            session,
            "institutions",
            "/institutions",
            {"filter": f"country_code:{COUNTRY}", "select": INSTITUTION_FIELDS},
            deadline,
        )
    members = pages(
        session, "works_by_year", "/works", {"filter": WORKS_FILTER, "group_by": "publication_year"}, deadline
    )
    members += pages(
        session,
        "works_oa_by_year",
        "/works",
        {"filter": f"{WORKS_FILTER},open_access.is_oa:true", "group_by": "publication_year"},
        deadline,
    )
    fields = pages(session, "fields", "/fields", {"select": "id,display_name,domain"}, deadline)
    members += fields
    for field in field_ids(fields):
        members += pages(
            session,
            f"works_by_year_field_{field}",
            "/works",
            {"filter": f"{WORKS_FILTER},primary_topic.field.id:{field}", "group_by": "publication_year"},
            deadline,
        )
    return members


def content_sha256(members: list[tuple[str, bytes, Any]]) -> str:
    """Digest of what a read says, without what changes on every read (response times, cursors, page cuts): per
    request, its items sorted by key or id, and the counts of its first page's meta."""
    requests_: dict[str, dict[str, Any]] = {}
    for name, _, parsed in members:
        request = name.rsplit("__p", 1)[0]
        entry = requests_.setdefault(
            request, {"meta": {k: parsed["meta"].get(k) for k in ("count", "groups_count")}, "items": []}
        )
        entry["items"] += parsed.get("group_by") or parsed.get("results") or []
    for entry in requests_.values():
        entry["items"].sort(key=lambda i: str(i.get("key", i.get("id"))))
    return sha256(json.dumps(requests_, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def bundle(members: list[tuple[str, bytes, Any]]) -> bytes:
    """A ZIP of the exact answer bodies; fixed member dates so equal answers give equal bytes."""
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body, _ in members:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body)
    return out.getvalue()


def landed(client, bucket: str, root: str, entity: str) -> list[dict[str, Any]]:
    """Receipts of the entity, oldest landed first."""
    return bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, root))


def land(client, bucket: str, root: str, entity: str, held: list[dict[str, Any]], deadline: float) -> dict[str, Any]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    members = read(session, entity, deadline)
    content = content_sha256(members)
    counts: dict[str, Any] = {"answers": len(members), "content_sha256": content}
    if held and (held[-1].get("detail") or {}).get("content_sha256") == content:
        return {**counts, "outcome": "unchanged"}
    body = bundle(members)
    digest = sha256(body).hexdigest()
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{bulk_file.prefix(SOURCE, entity, root)}{version}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
    receipt = {
        "schema_version": 2,
        "kind": "openalex_landing",
        "source_id": SOURCE,
        "entity": entity,
        "content": "snapshot",
        "area": "data",
        "source_kind": "openalex_api_json_answers_zip",
        "version_token": version,
        "version_basis": "read started (the API gives no update time for aggregates)",
        "ingested_at": ingested,
        "source_url": f"{API}/works?filter={WORKS_FILTER}" if entity == "works_pl" else f"{API}/institutions",
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "detail": counts,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    put_new(
        client,
        bucket,
        key[: -len(".zip")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return {**counts, "outcome": "landed", "key": key, "size_bytes": len(body)}


def run_verify(client, bucket: str, root: str, entity: str, held: list[dict[str, Any]]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=bulk_file.prefix(SOURCE, entity, root)
    ):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE) if any(r["object"]["key"] not in present for r in held) else {}
    problems = [
        r["object"]["key"]
        for r in held
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    return {"receipts": len(held), "missing_data": len(problems), "missing_sample": problems[:5]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--budget-minutes", type=float, default=60)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    deadline = time.monotonic() + (args.budget_minutes - MARGIN_MINUTES) * 60
    status = 0
    summary: dict[str, Any] = {"mode": args.mode, "entities": {}}
    for entity in ENTITIES:
        held = landed(client, bucket, args.landing_root, entity)
        one: dict[str, Any] = {"copies_held": len(held), "newest": held[-1]["version_token"] if held else None}
        if args.mode == "verify":
            one.update(run_verify(client, bucket, args.landing_root, entity, held))
            status |= 1 if one["missing_data"] else 0
        elif args.mode == "load":
            try:
                one.update(land(client, bucket, args.landing_root, entity, held, deadline))
            except (RuntimeError, requests.RequestException, KeyError, ValueError, TypeError) as exc:
                one.update({"outcome": "failed", "error": str(exc)[:300]})
                status = 1
        summary["entities"][entity] = one
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing {args.mode}", summary)
    step_summary(f"## {SOURCE} · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
