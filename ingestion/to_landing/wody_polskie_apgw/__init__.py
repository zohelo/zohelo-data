"""Wody Polskie aPGW planning geodatabase: Landing activities (register PL-ENV-013, #3313).

Państwowe Gospodarstwo Wodne Wody Polskie publish the spatial database of the update of the river basin management
plans (aPGW) on dane.gov.pl, dataset 599 "Baza danych przestrzennych aktualizacji planów gospodarowania wodami"
(CC BY 4.0): one File Geodatabase ZIP per plan cycle, today ``Geobaza_2aPGW_ver_20230915.gdb.zip`` (the 2nd update,
2023 edition; 307,606,332 bytes, Last-Modified 20 Dec 2023, resource dated 2023-12-20).

The resource list is read once per run from the portal's public API (``/1.4/datasets/599/resources``, no key).
Entity = the plan cycle's geodatabase, ``geobaza_<n>apgw`` from the file name (``Geobaza_2aPGW_…`` →
``geobaza_2apgw``); version = the file's Last-Modified (the receipt keeps the ETag and the URL). A new plan update
arrives as a new resource and is a new entity; a re-issued file of the same cycle is a new version of its entity.
The 307 MB file is downloaded only when it is not landed yet or its URL, ETag or Last-Modified differ from the newest
receipt: a weekly run reads the API and HEADs the file, nothing more. Restart unit: one file. Two resources for one
entity are an error (a choice between files is never made by position); a resource that is no ``.gdb.zip`` is listed,
not landed.

landing.inspect  the dataset's geodatabases vs Landing (missing / stale / current); no writes
landing.extract  every geodatabase that is missing or stale, one download at a time, PAUSE apart, time-budgeted
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

SOURCE = "wody_polskie_apgw"
DATASET = 599
API = f"https://api.dane.gov.pl/1.4/datasets/{DATASET}/resources"
PAUSE = 3.0
PAGES = 5  # resources are read 100 a page; the dataset held 1 geodatabase on 26 Nov 2026
FILE = re.compile(r"/Geobaza_(\d+)aPGW[^/]*\.gdb\.zip$", re.IGNORECASE)
EDITION = re.compile(r"_ver_(\d{4})(\d{2})(\d{2})", re.IGNORECASE)


def resources(get=None) -> list[dict[str, Any]]:
    """Every resource of the dataset (id, title, file URL, dates), page by page; bounded retries with backoff."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    def fetch(url: str) -> dict[str, Any]:
        error = ""
        for attempt in range(4):
            try:
                r = requests.get(
                    url, headers={"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"}, timeout=(30, 150)
                )
                if r.status_code == 200:
                    return r.json()
                if 400 <= r.status_code < 500 and r.status_code not in (408, 429):
                    raise bulk_file.PermanentError(f"{url}: HTTP {r.status_code}")
                error = f"HTTP {r.status_code}"
            except (requests.RequestException, ValueError) as exc:
                error = f"{type(exc).__name__}: {str(exc)[:120]}"
            print(json.dumps({"status": "retry", "url": url, "error": error}), flush=True)
            time.sleep(20 * (attempt + 1))
        raise RuntimeError(f"{url}: no answer after 4 attempts ({error})")

    out: list[dict[str, Any]] = []
    url: str | None = f"{API}?per_page=100&page=1"
    for _ in range(PAGES):
        if not url:
            break
        body = (get or fetch)(url)
        for item in body.get("data", []):
            a = item.get("attributes") or {}
            out.append(
                {
                    "id": str(item.get("id")),
                    "title": (a.get("title") or "").strip(),
                    "file_url": a.get("file_url"),
                    "created": a.get("created"),
                    "data_date": a.get("data_date"),
                }
            )
        url = (body.get("links") or {}).get("next")
        if url:
            time.sleep(PAUSE)
    return out


def entity_of(url: str | None) -> str | None:
    """``geobaza_<n>apgw`` from a geodatabase file URL, else None."""
    m = FILE.search(url or "")
    return f"geobaza_{int(m.group(1))}apgw" if m else None


def edition_year(url: str) -> int | None:
    """The year of the geodatabase version in its file name (``…_ver_20230915.gdb.zip`` → 2023), else None."""
    m = EDITION.search(url)
    return int(m.group(1)) if m else None


def discover(listed: list[dict[str, Any]] | None = None) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """The geodatabases of the dataset as bulk_file specs; two resources for one entity are an error."""
    listed = resources() if listed is None else listed
    found: dict[str, dict[str, Any]] = {}
    other: list[str] = []
    for r in listed:
        entity = entity_of(r.get("file_url"))
        if entity is None:
            other.append(f"{r['id']}: {r['title'][:80]}")
            continue
        if entity in found:
            raise ValueError(f"{SOURCE}: two resources for {entity}: {found[entity]['resource']} and {r['id']}")
        found[entity] = {
            "entity": entity,
            "url": r["file_url"],
            "format": "zip",
            "version": "last_modified",
            "read_timeout": 600,
            "resource": r["id"],
            "resource_created": r.get("created"),
            "edition_year": edition_year(r["file_url"]),
        }
    summary = {
        "resources_listed": len(listed),
        "geodatabases": {e: {"resource": s["resource"], "url": s["url"]} for e, s in sorted(found.items())},
        "not_geodatabases": other,
    }
    return found, summary


def held(landing_root: str = "01_landing") -> dict[str, list[dict[str, Any]]]:
    """{entity: receipts, oldest first}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, list[dict[str, Any]]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], []).append(receipt)
    return out


def state(spec: dict[str, Any], receipts: list[dict[str, Any]], head: dict[str, str | None]) -> str:
    """'missing' (nothing landed), 'current' (the newest receipt has the same URL and the server's ETag and
    Last-Modified) or 'stale' (anything else, including a server that sends neither validator)."""
    if not receipts:
        return "missing"
    newest = receipts[-1]
    http = (newest.get("detail") or {}).get("http") or {}
    same_url = newest.get("source_url") == spec["url"]
    validators = head.get("etag") or head.get("last_modified")
    same = http.get("etag") == head.get("etag") and http.get("last_modified") == head.get("last_modified")
    return "current" if same_url and validators and same else "stale"


def headers_of(url: str) -> tuple[dict[str, str | None], str]:
    """The server's validators for the file (HEAD, with the pattern's retries), and the HTTP status."""
    from ingestion.to_landing.patterns import bulk_file

    response, http = bulk_file.probe(url)
    h = response.headers if response is not None else {}
    return {
        "etag": h.get("ETag"),
        "last_modified": h.get("Last-Modified"),
        "content_length": h.get("Content-Length"),
    }, http


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    landed = held()
    rows, level = [], "notice" if found else "error"
    for entity, spec in sorted(found.items()):
        head, http = headers_of(spec["url"])
        row = {"entity": entity, "http": http, **head, "landed_versions": len(landed.get(entity, []))}
        row["state"] = state(spec, landed.get(entity, []), head) if http == "200" else "unreachable"
        level = "error" if http != "200" else level
        rows.append(row)
    annotate(level, f"{SOURCE} landing inspect", {**summary, "files": rows})
    return 1 if level == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    landed = held(landing_root)
    results, refused, current, deferred = [], [], [], 0
    for n, (entity, spec) in enumerate(sorted(found.items())):
        if time.monotonic() > deadline:
            deferred = len(found) - n
            break
        time.sleep(PAUSE)
        head, http = headers_of(spec["url"])
        if http != "200":
            refused.append({"entity": entity, "error": f"HEAD {http}"})
            continue
        if state(spec, landed.get(entity, []), head) == "current":
            current.append(entity)
            continue
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": entity, "error": str(exc)[:120]})
    summary.update(
        {
            "current_not_downloaded": current,
            "landed": [r for r in results if r["outcome"] != "unchanged"],
            "downloaded_unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    level = "error" if not found else ("warning" if refused else "notice")
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if level == "error" or (refused and not results and not current) else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
