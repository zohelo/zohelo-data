"""GIOŚ groundwater monitoring results: Landing activities (register PL-ENV-012, #2870).

GIOŚ publishes the yearly results of the groundwater quality monitoring of the State Environmental Monitoring (PMŚ)
on dane.gov.pl, dataset 1405 "Państwowy Monitoring Środowiska - monitoring jakości wód podziemnych" (CC BY 4.0,
update frequency "Rocznie"): one file per year and indicator group, titled "Wyniki badań w <year>r. wskaźników
fizykochemicznych NIEORGANICZNYCH|ORGANICZNYCH - monitoring jakości wód podziemnych - monitoring diagnostyczny|
operacyjny" (2019 and 2020 as CSV, 2021 on as XLSX; 12 files 2019-2024 on 26 Nov 2026, Dev probe 45810873298). The
producer's own portal https://mjwp.gios.gov.pl/ answered every request, robots.txt included, with HTTP 403 and a
maintenance page on 26 Nov 2026 (Dev probes 45809388367, 45809752001); it is not used
(docs/sources/gios_groundwater.md).

The resource list is read once per run from the portal's public API (``/1.4/datasets/1405/resources``, no key).
Entity = year x indicator group x monitoring kind (``<year>_<inorganic|organic>_<diagnostic|operational>``);
version = the file's Last-Modified. A file is downloaded only when its URL is not landed yet: the portal stores every
upload under a new dated path (``/media/resources/<yyyymmdd>/...``), so a re-issued file is a new URL and replaces
the entity's version. Restart unit: one file. A resource of the dataset whose title names no year and group (the
WMS and ATOM services) is not a result file and is listed, not landed.

landing.inspect  the dataset's result files vs Landing (to land / held); no writes
landing.extract  every result file whose URL is not landed yet, one download at a time, PAUSE apart, time-budgeted
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

SOURCE = "gios_groundwater"
API = "https://api.dane.gov.pl/1.4/datasets/1405/resources"
PAUSE = 3.0
PAGES = 5  # resources are read 100 a page; the dataset held 14 on 26 Nov 2026
TITLE = re.compile(
    r"Wyniki badań w (\d{4})\s*r\.?\s+wskaźników fizykochemicznych (NIEORGANICZNYCH|ORGANICZNYCH)"
    r"\s+-\s+monitoring jakości wód podziemnych\s+-\s+monitoring (diagnostyczny|operacyjny)\s*$",
    re.IGNORECASE,
)
GROUPS = {"nieorganicznych": "inorganic", "organicznych": "organic"}
KINDS = {"diagnostyczny": "diagnostic", "operacyjny": "operational"}
FORMATS = (".xlsx", ".csv")


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


def entity_of(title: str) -> str | None:
    """``<year>_<inorganic|organic>_<diagnostic|operational>`` from a resource title, else None."""
    m = TITLE.search(" ".join(title.split()))
    if not m:
        return None
    return f"{m.group(1)}_{GROUPS[m.group(2).lower()]}_{KINDS[m.group(3).lower()]}"


def discover(listed: list[dict[str, Any]] | None = None) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """The result files of the dataset as bulk_file specs; two resources for one entity are an error (a choice between
    files is never made by position), and so is a result file that is neither XLSX nor CSV."""
    listed = resources() if listed is None else listed
    found: dict[str, dict[str, Any]] = {}
    other: list[str] = []
    for r in listed:
        entity = entity_of(r["title"])
        if entity is None:
            other.append(f"{r['id']}: {r['title'][:80]}")
            continue
        url = r.get("file_url") or ""
        if not url.lower().endswith(FORMATS):
            raise ValueError(f"{SOURCE}: resource {r['id']} ({r['title']}) is not XLSX or CSV: {url!r}")
        if entity in found:
            raise ValueError(f"{SOURCE}: two resources for {entity}: {found[entity]['resource']} and {r['id']}")
        found[entity] = {
            "entity": entity,
            "url": url,
            "format": "raw",
            "version": "last_modified",
            "read_timeout": 300,
            "resource": r["id"],
        }
    years = sorted({int(e[:4]) for e in found})
    summary = {
        "resources_listed": len(listed),
        "result_files": {e: s["resource"] for e, s in sorted(found.items())},
        "years": years,
        "years_missing": [y for y in range(years[0], years[-1] + 1) if y not in years] if years else [],
        "not_result_files": other,
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
    """Result files whose URL is not landed yet, oldest year first."""
    return [spec for entity, spec in sorted(found.items()) if spec["url"] not in landed.get(entity, set())]


def level_of(found: dict[str, dict[str, Any]], summary: dict[str, Any]) -> str:
    """'error' when no result file is found or a year inside the listed range is missing, else 'notice'."""
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
