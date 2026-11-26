#!/usr/bin/env python3
"""e-Zamówienia BZP Landing: contract and result notices of the BZP read API, one publication day and type at a time.

Route (docs/sources/uzp_bzp.md, register PLG-022). The BZP read API of the e-Zamówienia platform
(``GET https://ezamowienia.gov.pl/mo-board/api/v1/notice``, free, no application; instruction "Załącznik 3"
of https://ezamowienia.gov.pl/pl/integracja/) answers one notice type per request (``NoticeType``), at most 500
notices a page, with ``SearchAfter`` = the ``objectId`` of the last notice for the next page (ascending
``objectId``). ``notice/stats`` gives the number of notices per type for a publication day. Stated limit: 30
requests a minute (the instruction ZIP does not give one); requests go one at a time, PAUSE_SECONDS apart.

Measured from Actions (Dev probes 43797263601, 43797627130, 43797993730, 16 Nov 2026): ``PublicationDateFrom`` is
a UTC instant, ``PublicationDateTo`` reaches 24 hours beyond the time it names (To = 3 Nov 10:00 returned notices
up to 4 Nov 09:58), so a day D is asked as From = To = D 00:00:00 and every answer is still filtered to the UTC
day D; the day's notices must then equal the ``notice/stats`` count of D for the type, or the day is not written.
About 600 contract and 700 result notices a working day, a handful on Saturdays and Sundays; a repeated request
gives identical answers.

Selection (the smallest valuable one, #2981): contract notices (``ContractNotice``, ogłoszenie o zamówieniu) and
result notices (``TenderResultNotice``, ogłoszenie o wyniku postępowania) published from WINDOW_START. Left out:
the other 13 notice types (contract performance, notice and contract changes, intentions, competitions,
concessions, notices below the Act's thresholds), and notices before WINDOW_START (the platform has them from 2021).

Reduced before Landing (as news feeds, O33, and the ``reduce:`` of the bulk_file pattern): a notice's ``htmlBody``
(the whole form, about 95% of the bytes) names contact persons, e-mail addresses and telephone numbers and the
winning contractors with their addresses, which this platform must not store. Each notice keeps the API's own
fields except: an e-mail address in a kept free-text field (FREE_TEXT) becomes ``[e-mail]``; ``htmlBody`` is
replaced by ``values`` (the template-labelled value lines of VALUE_FIELDS with their
part number: estimated value, value per part, outcome per part, offers received, price of the chosen offer,
contract date, contract value); ``contractors`` keeps only each contractor's province and country (a contractor
may be a natural person: no name, city or national id); a subsidised contracting authority (client type ``3``,
which may be a natural person) keeps neither name, city, national id nor platform id. A new or unknown field fails
the day instead of landing unchecked. The receipt keeps the SHA-256 and size of every original answer page.

Entity = one notice type on one publication day, ``<contract|result>_<YYYYMMDD>``; version = the publication day.
A day is a closed set once over: each is fetched once (after the day ended, UTC) and the newest RECHECK_DAYS days
are asked again; an answer whose reduced bytes equal its newest landed copy is not written again. Days without
notices land their (empty) answer once.

Layout (docs/landing.md): ``<root>/uzp_bzp/snapshot/data/<entity>/<YYYYMMDD>T000000Z__<ingested>.json`` + receipt.
Modes: inspect (window days vs Landing), load, verify.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import json
import os
import re
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

SOURCE = "uzp_bzp"
API = "https://ezamowienia.gov.pl/mo-board/api/v1/notice"
STATS = f"{API}/stats"
TYPES = {"contract": "ContractNotice", "result": "TenderResultNotice"}
WINDOW_START = date(2026, 1, 1)
RECHECK_DAYS = 3
PAGE_SIZE = 500
MAX_PAGES = 20  # a day has about 700 notices of a type (2 pages); far more means the date filter changed
PAUSE_SECONDS = 2.5  # stated limit: 30 requests a minute
TIMEOUT_SECONDS = (20, 180)
RETRY_DELAYS = (30, 60, 120, 240)
ENTITY_RE = re.compile(r"(contract|result)_(\d{8})")

# The fields of a notice (NoticeDto of bzp.external.API.yaml) and what Landing keeps of each.
KEPT = {
    "objectId", "noticeType", "noticeNumber", "bzpNumber", "tenderId", "publicationDate", "clientType", "orderType",
    "tenderType", "isTenderAmountBelowEU", "orderObject", "cpvCode", "submittingOffersDate", "procedureResult",
    "organizationName", "organizationCity", "organizationProvince", "organizationCountry", "organizationNationalId",
    "organizationId",
}  # fmt: skip
REPLACED = {"htmlBody", "contractors"}
AUTHORITY_PERSONAL = ("organizationName", "organizationCity", "organizationNationalId", "organizationId")
CONTRACTOR_KEPT = ("contractorProvince", "contractorCountry")
CONTRACTOR_KNOWN = {"contractorName", "contractorCity", "contractorNationalId", *CONTRACTOR_KEPT}
# Value lines of the notice form kept from htmlBody: (number, label prefix) -> field. Template labels of the BZP
# forms of 2021 (Pzp); the value is the text of the line's <span class="normal">.
VALUE_FIELDS = {
    ("4.1.5.)", "Wartość zamówienia"): "estimated_value",
    ("4.1.5.)", "Łączna wartość poszczególnych części zamówienia"): "estimated_value",
    ("4.2.5.)", "Wartość części"): "part_estimated_value",
    ("4.3.)", "Wartość zamówienia"): "estimated_value",
    ("4.3.)", "Łączna wartość poszczególnych części zamówienia"): "estimated_value",
    ("4.5.5.)", "Wartość części"): "part_estimated_value",
    ("5.1.)", "Postępowanie zakończyło się"): "part_outcome",
    ("6.1.)", "Liczba otrzymanych ofert"): "offers_received",
    ("6.4.)", "Cena lub koszt oferty wykonawcy, któremu udzielono zamówienia"): "price_chosen_offer",
    ("8.1.)", "Data zawarcia umowy"): "contract_date",
    ("8.2.)", "Wartość umowy"): "contract_value",
}
LINE_RE = re.compile(
    r"<h3[^>]*>\s*(\d+(?:\.\d+)*\.\))\s*([^<:]{1,200}?)\s*:\s*<span[^>]*>\s*([^<]{0,200}?)\s*</span>", re.S
)
PART_RE = re.compile(r">\s*Część\s+(\d+)\s*<|\(dla części\s+(\d+)\)")
# Free-text fields kept: an e-mail address written into them (a contact person's) is replaced before Landing. Dev
# trials 43842705235 and 45767597789 found "@" in 33 notice subjects, none of them in the shape of an address.
FREE_TEXT = ("orderObject", "organizationName", "organizationCity")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
EMAIL_MARK = "[e-mail]"


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def entity_of(kind: str, day: date) -> str:
    return f"{kind}_{day:%Y%m%d}"


def parse_entity(entity: str) -> tuple[str, date] | None:
    match = ENTITY_RE.fullmatch(entity)
    return (match.group(1), datetime.strptime(match.group(2), "%Y%m%d").date()) if match else None


def last_day(now: datetime | None = None) -> date:
    """The newest publication day asked for: yesterday (UTC), so a day is complete when it is first fetched."""
    return (now or datetime.now(UTC)).date() - timedelta(days=1)


def window(last: date) -> list[date]:
    return [WINDOW_START + timedelta(days=i) for i in range((last - WINDOW_START).days + 1)]


def listing(client, bucket: str, root: str) -> tuple[dict[str, list[tuple[str, str]]], set[str]]:
    """Per entity: (ingested stamp, receipt key) oldest first; and the data keys present. Key names only, no reads."""
    receipts: dict[str, list[tuple[str, str]]] = {}
    data: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        for o in page.get("Contents", []):
            key = o["Key"]
            entity, name = key[len(prefix(root)) :].split("/", 1)
            if key.endswith(".receipt.json"):
                receipts.setdefault(entity, []).append((name.split("__", 1)[1].split(".", 1)[0], key))
            else:
                data.add(key)
    for rows in receipts.values():
        rows.sort()
    return receipts, data


def read_receipts(client, bucket: str, keys: list[str]) -> list[dict[str, Any]]:
    def read(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    with ThreadPoolExecutor(max_workers=4) as pool:  # R2 throttles bursts of reads
        return list(pool.map(read, keys))


def held_entities(receipts: dict[str, list]) -> set[tuple[str, date]]:
    return {parsed for entity in receipts if (parsed := parse_entity(entity))}


def get(session: requests.Session, url: str, params: dict[str, Any]) -> bytes:
    """GET with bounded retries: network errors, 403 (possibly the answer above the rate limit), 429 and 5xx back
    off; any other status fails at once."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        time.sleep(PAUSE_SECONDS)
        try:
            response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            response = None  # also an answer cut short (Dev trial 43840995051: IncompleteRead of a 10 MB page)
        if response is not None and response.status_code == 200:
            return response.content
        if response is not None and response.status_code not in (403, 429, 500, 502, 503, 504):
            raise RuntimeError(f"BZP API {response.status_code} for {params}: {response.text[:300]}")
        if attempt == len(RETRY_DELAYS):
            break
        time.sleep(RETRY_DELAYS[attempt])
    raise RuntimeError(f"BZP API kept failing for {params}")


def stats_of(session: requests.Session, day: date) -> dict[str, int]:
    """Notices per type published on the UTC day (notice/stats counts whole days from From to To)."""
    body = get(session, STATS, {"PublicationDateFrom": f"{day}T00:00:00", "PublicationDateTo": f"{day}T23:59:59"})
    answer = json.loads(body)
    if not isinstance(answer, list) or not all(isinstance(a, dict) for a in answer):
        raise RuntimeError(f"{day}: notice/stats answer is not a list: {body[:200]!r}")
    counts = {str(a.get("noticeType")): a.get("numberOfNotices") for a in answer}
    for kind in TYPES.values():
        if type(counts.get(kind)) is not int:
            raise RuntimeError(f"{day}: notice/stats has no count for {kind}")
    return counts


def html_values(body: str) -> list[dict[str, Any]]:
    """The VALUE_FIELDS lines of a notice form, in form order, with the part number they follow (None before the
    first part heading)."""
    parts = [(m.start(), int(m.group(1) or m.group(2))) for m in PART_RE.finditer(body)]
    out = []
    for m in LINE_RE.finditer(body):
        number, label, value = m.group(1), re.sub(r"\s+", " ", m.group(2)), re.sub(r"\s+", " ", m.group(3))
        field = next((f for (n, p), f in VALUE_FIELDS.items() if n == number and label.startswith(p)), None)
        if field is None:
            continue
        part = None
        for position, number_of_part in parts:
            if position > m.start():
                break
            part = number_of_part
        out.append({"field": field, "part": part, "value": value[:100]})
    return out


def reduce_notice(item: dict[str, Any]) -> dict[str, Any]:
    """The kept part of one notice (module docstring); an unknown field fails loudly."""
    unknown = sorted(set(item) - KEPT - REPLACED)
    if unknown:
        raise RuntimeError(f"notice {item.get('noticeNumber')}: unknown fields {unknown[:5]}")
    out = {k: item.get(k) for k in sorted(KEPT)}
    if str(item.get("clientType") or "").split(".", 1)[0] == "3":
        for k in AUTHORITY_PERSONAL:
            out[k] = None
    for k in FREE_TEXT:
        if isinstance(out[k], str):
            out[k] = EMAIL_RE.sub(EMAIL_MARK, out[k])
    contractors = []
    for c in item.get("contractors") or []:
        if not isinstance(c, dict) or set(c) - CONTRACTOR_KNOWN:
            raise RuntimeError(f"notice {item.get('noticeNumber')}: unknown contractor fields {sorted(c)[:6]}")
        contractors.append({k: c.get(k) for k in CONTRACTOR_KEPT})
    out["contractors"] = contractors
    out["values"] = html_values(item.get("htmlBody") or "")
    return out


def fetch_day(session: requests.Session, kind: str, day: date, expected: int) -> tuple[bytes, list[dict[str, Any]]]:
    """One type on one UTC day: every page (SearchAfter), filtered to the day, reduced and checked against the
    notice/stats count; returns the Landing bytes and the original pages (sha256, size, notices)."""
    params: dict[str, Any] = {
        "NoticeType": TYPES[kind],
        "PublicationDateFrom": f"{day}T00:00:00",
        "PublicationDateTo": f"{day}T00:00:00",  # the API reaches 24 hours beyond To (module docstring)
        "PageSize": PAGE_SIZE,
    }
    kept: dict[str, dict[str, Any]] = {}
    pages, others = [], Counter()
    after = None
    for _ in range(MAX_PAGES):
        body = get(session, API, {**params, **({"SearchAfter": after} if after else {})})
        try:
            items = json.loads(body)
        except ValueError as exc:
            raise RuntimeError(f"{kind} {day}: BZP API returned no JSON: {body[:200]!r}") from exc
        if not isinstance(items, list):
            raise RuntimeError(f"{kind} {day}: BZP API answer is not a list: {body[:300]!r}")
        pages.append({"sha256": sha256(body).hexdigest(), "size_bytes": len(body), "notices": len(items)})
        for item in items:
            if item.get("noticeType") != TYPES[kind]:
                raise RuntimeError(f"{kind} {day}: notice of type {item.get('noticeType')} in the answer")
            published = str(item.get("publicationDate") or "")
            if not published.endswith("Z") or published[:10] != f"{day}":
                others[published[:10]] += 1
                continue
            object_id = item.get("objectId")
            if not object_id or object_id in kept:
                raise RuntimeError(f"{kind} {day}: objectId missing or repeated: {object_id}")
            kept[object_id] = reduce_notice(item)
        if len(items) < PAGE_SIZE:
            break
        after = items[-1].get("objectId")
    else:
        raise RuntimeError(f"{kind} {day}: more than {MAX_PAGES} pages; the date filter may have changed")
    if len(kept) != expected:
        raise RuntimeError(
            f"{kind} {day}: {len(kept)} notices of the day on {len(pages)} pages, notice/stats says {expected}"
            f" (other days in the answers: {dict(others.most_common(3))})"
        )
    notices = [kept[k] for k in sorted(kept)]
    document = {
        "notice_type": TYPES[kind],
        "publication_day": f"{day}",
        "stats_count": expected,
        "notices": notices,
    }
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(), pages


def put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    """Never overwrite Landing: a key that exists is an error unless the bytes are equal."""
    digest = sha256(body).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") == digest:
            return
        raise RuntimeError(f"Landing key exists with different bytes: {key}")
    except client.exceptions.ClientError as exc:  # only "not found" means free to write
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, Metadata={"sha256": digest})
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")


def land_day(client, bucket: str, root: str, session, day: date, newest: dict[str, dict]) -> Counter:
    """Both types of one day: stats, every page, checked and reduced; each answer that differs from its newest
    copy is written with its receipt."""
    outcomes: Counter = Counter()
    counts = stats_of(session, day)
    token = f"{day:%Y%m%d}T000000Z"
    for kind in TYPES:
        body, pages = fetch_day(session, kind, day, counts[TYPES[kind]])
        entity = entity_of(kind, day)
        digest = sha256(body).hexdigest()
        before = newest.get(entity)
        outcomes[f"{kind}_notices"] += counts[TYPES[kind]]
        if before and before["sha256"] == digest:
            outcomes["files_unchanged"] += 1
            continue
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix(root)}{entity}/{token}__{ingested}.json"
        put_new(client, bucket, key, body, "application/json")
        receipt = {
            "schema_version": 3,
            "kind": "uzp_bzp_landing",
            "source_id": SOURCE,
            "entity": entity,
            "area": "data",
            "content": "snapshot",
            "source_kind": "bzp_read_api_v1_json_reduced",
            "version_token": token,
            "version_basis": "publication_day",
            "ingested_at": ingested,
            "source_url": API,
            "notice_type": TYPES[kind],
            "publication_day": f"{day}",
            "stats_count": counts[TYPES[kind]],
            "original_pages": pages,
            "detail": {"reduced": "uzp_bzp_notice"},
            "sha256": digest,
            "size_bytes": len(body),
            "object": {"bucket": bucket, "key": key},
            "replaces_sha256": before["sha256"] if before else None,
            "code_sha": os.environ.get("GITHUB_SHA", ""),
        }
        put_new(
            client,
            bucket,
            key[: -len(".json")] + ".receipt.json",
            json.dumps(receipt, indent=2, sort_keys=True).encode(),
            "application/json",
        )
        outcomes["files_landed_changed" if before else "files_landed_new"] += 1
        outcomes["bytes_landed"] += len(body)
        outcomes["bytes_original"] += sum(p["size_bytes"] for p in pages)
    return outcomes


def run_verify(client, bucket: str, root: str) -> dict[str, Any]:
    receipts, data = listing(client, bucket, root)
    stored = archived(client, bucket, SOURCE)
    problems = []
    held = read_receipts(client, bucket, [k for rows in receipts.values() for _, k in rows])
    for r in held:
        key = r["object"]["key"]
        if key not in data and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], key):
            problems.append(key)
    days = {day for _, day in held_entities(receipts)}
    return {
        "receipts": len(held),
        "entities": len(receipts),
        "days": len(days),
        "first_last_day": [str(min(days)) if days else None, str(max(days)) if days else None],
        "missing_data": len(problems),
        "missing_sample": problems[:10],
    }


def day_complete(held: set[tuple[str, date]], day: date) -> bool:
    return all((kind, day) in held for kind in TYPES)


def extract(budget_minutes: float, landing_root: str = "01_landing", last: date | None = None) -> int:
    """landing.extract: missing days oldest first, then the newest RECHECK_DAYS days again (within the budget)."""
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or last_day()
    receipts, _ = listing(client, bucket, landing_root)
    held = held_entities(receipts)
    days = window(last)
    missing = [d for d in days if not day_complete(held, d)]
    recheck = [d for d in days[-RECHECK_DAYS:] if day_complete(held, d)]
    wanted = {entity_of(k, d) for d in recheck for k in TYPES}
    newest = {r["entity"]: r for r in read_receipts(client, bucket, [receipts[e][-1][1] for e in wanted])}
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    deadline = time.monotonic() + budget_minutes * 60
    outcomes: Counter = Counter()
    failed: list[dict[str, str]] = []
    for day in missing + recheck:
        if time.monotonic() > deadline:
            outcomes["days_not_started_time_budget"] += 1
            continue
        try:
            outcomes.update(land_day(client, bucket, landing_root, session, day, newest))
            outcomes["days_done"] += 1
        except RuntimeError as exc:  # one day failing must not stop the others; recorded, run ends red
            failed.append({"day": str(day), "error": str(exc)[:300]})
    summary = {
        "mode": "load",
        "window": [str(WINDOW_START), str(last)],
        "days_in_window": len(days),
        "days_missing_before": len(missing),
        "days_rechecked": len(recheck),
        **outcomes,
        "failed_days": len(failed),
        "failed_sample": failed[:5],
    }
    status = 1 if failed else 2 if outcomes["days_not_started_time_budget"] else 0
    report("load", summary, status)
    return status


def inspect(landing_root: str = "01_landing", last: date | None = None) -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or last_day()
    receipts, data = listing(client, bucket, landing_root)
    held = held_entities(receipts)
    days = window(last)
    summary = {
        "mode": "inspect",
        "window": [str(WINDOW_START), str(last)],
        "days_in_window": len(days),
        "days_current": len([d for d in days if day_complete(held, d)]),
        "days_missing": len([d for d in days if not day_complete(held, d)]),
        "entities": len(receipts),
        "data_files_in_landing": len(data),
        "newest_day_held": str(max(d for _, d in held)) if held else None,
    }
    report("inspect", summary, 0)
    return 0


def verify(landing_root: str = "01_landing") -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    summary = {"mode": "verify", **run_verify(client, bucket, landing_root)}
    status = 1 if summary["missing_data"] else 0
    report("verify", summary, status)
    return status


def report(mode: str, summary: dict[str, Any], status: int) -> None:
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"uzp_bzp landing {mode}", summary)
    step_summary(f"## uzp_bzp · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--budget-minutes", type=float, default=150)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    if args.mode == "inspect":
        return inspect(args.landing_root)
    if args.mode == "verify":
        return verify(args.landing_root)
    return extract(args.budget_minutes, args.landing_root)


if __name__ == "__main__":
    raise SystemExit(main())
