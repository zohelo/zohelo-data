"""RSPO school lists: Landing activities (register PLS-021).

The Ministry of National Education publishes the list of schools and educational institutions of the school
register (RSPO, data of the SIO module "Placówki") once a year on dane.gov.pl, dataset 839 "Wykaz szkół i placówek
oświatowych": one XLSX resource per reference date 30 September, titled "Wykaz szkół i placówek oświatowych wg
stanu bazy SIO na 30.09.<year>" (2020-2025 on 26 Nov 2026, one sheet "Arkusz1", 58 columns, about 55,600 rows).
The resource list is read once per run from the portal's public API (``/1.4/datasets/839/resources``, no key).
Entity = one reference year (``school_list_<year>``); version = the file's Last-Modified. A file is downloaded
only when its URL is not landed yet: the portal stores every upload under a new dated path
(``/media/resources/<yyyymmdd>/...``), so a corrected file is a new URL. Restart unit: one file.
Lists before 2020 (2017, 2018: another layout; 2013-2015: one XLS per voivodeship) are not landed.

landing.inspect  the dataset's lists vs Landing (to land / held); no writes
landing.extract  every list whose file URL is not landed yet, one download at a time, PAUSE apart, time-budgeted
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

SOURCE = "rspo"
API = "https://api.dane.gov.pl/1.4/datasets/839/resources"
FIRST_YEAR = 2020  # the first list in the 58-column layout
PAUSE = 3.0
PAGES = 5  # resources are read 100 a page; the dataset held 75 on 26 Nov 2026
TITLE = re.compile(r"wg stanu bazy SIO na 30\.09\.(\d{4})\s*$")


def resources(get=None) -> list[dict[str, Any]]:
    """Every resource of the dataset (id, title, file URL), page by page; bounded retries with backoff."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    def fetch(url: str) -> dict[str, Any]:
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
                {"id": str(item.get("id")), "title": (a.get("title") or "").strip(), "file_url": a.get("file_url")}
            )
        url = (body.get("links") or {}).get("next")
        if url:
            time.sleep(PAUSE)
    return out


def discover(listed: list[dict[str, Any]] | None = None) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """The yearly lists of the dataset as bulk_file specs; two resources for one year are an error (a choice between
    files is never made by position)."""
    listed = resources() if listed is None else listed
    found: dict[str, dict[str, Any]] = {}
    for r in listed:
        m = TITLE.search(r["title"])
        if not m or int(m.group(1)) < FIRST_YEAR:
            continue
        if not (r.get("file_url") or "").lower().endswith(".xlsx"):
            raise ValueError(f"{SOURCE}: resource {r['id']} ({r['title']}) has no XLSX file: {r.get('file_url')!r}")
        entity = f"school_list_{m.group(1)}"
        if entity in found:
            raise ValueError(f"{SOURCE}: two resources for {entity}: {found[entity]['resource']} and {r['id']}")
        found[entity] = {
            "entity": entity,
            "url": r["file_url"],
            "format": "raw",
            "version": "last_modified",
            "read_timeout": 600,
            "resource": r["id"],
        }
    years = sorted(int(e[-4:]) for e in found)
    summary = {
        "resources_listed": len(listed),
        "lists": {e: s["resource"] for e, s in sorted(found.items())},
        "years_missing": [y for y in range(FIRST_YEAR, (years[-1] if years else FIRST_YEAR) + 1) if y not in years],
    }
    return found, summary


def held(landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: file URLs landed}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, set[str]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], set()).add(receipt["source_url"])
    return out


def plan(found: dict[str, dict[str, Any]], landed: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Lists whose file URL is not landed yet, oldest year first."""
    return [spec for entity, spec in sorted(found.items()) if spec["url"] not in landed.get(entity, set())]


def level_of(found: dict[str, dict[str, Any]], summary: dict[str, Any]) -> str:
    """'error' when no list is found or a year between FIRST_YEAR and the newest list is missing, else 'notice'."""
    return "error" if not found or summary["years_missing"] else "notice"


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    todo = plan(found, held())
    level = level_of(found, summary)
    annotate(level, f"{SOURCE} landing inspect", {**summary, "to_land": [s["entity"] for s in todo]})
    return 1 if level == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    todo = plan(found, held(landing_root))
    results, refused, deferred = [], [], 0
    for n, spec in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary.update(
        {
            "planned": len(todo),
            "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    level = level_of(found, summary)
    if refused and level == "notice":
        level = "warning"
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if level == "error" or (refused and not results) else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
