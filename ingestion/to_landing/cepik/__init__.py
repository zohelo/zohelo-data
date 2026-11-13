"""CEPiK vehicle statistics Landing activities (register PL-ENV-024).

The Ministry of Digital Affairs publishes the statistics of the Central Register of Vehicles and Drivers (CEPiK) on
gov.pl: https://www.gov.pl/web/cepik/statystyki lists one page per topic and year, e.g.
/web/cepik/pojazdy-zarejestrowane-w-2026-roku, with XLSX and PDF attachments (XLSX from 2016; 2015 is PDF only).
Landed: the XLSX workbooks of the TOPICS below (vehicles registered by voivodeship and by vehicle type), years from
FIRST_YEAR. Entity = topic and year (``<topic>_<year>``); version = the attachment's Last-Modified (download time
when it is missing). The current year's workbook is replaced every month under a new attachment URL; it is
downloaded on every run (unchanged bytes are not written), a closed year only when its page links an attachment URL
not landed yet (a correction). Restart unit: one file.

landing.inspect  the workbooks the year pages list vs Landing (to land / held); no writes
landing.extract  every listed workbook not landed yet plus the current year's, one download at a time, PAUSE apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from datetime import UTC, datetime
import re
import time
from typing import Any

SOURCE = "cepik"
BASE = "https://www.gov.pl/web/cepik/"
FIRST_YEAR = 2016
PAUSE = 2.0
# topic -> (page slug with {year}, regex on the attachment's link text that picks the workbook)
TOPICS: dict[str, tuple[str, re.Pattern[str]]] = {
    "vehicles_voivodeship": ("pojazdy-zarejestrowane-w-{year}-roku", re.compile(r"wojew", re.IGNORECASE)),
    "vehicles_type": ("pojazdy-zarejestrowane-w-{year}-roku", re.compile(r"rodzaj", re.IGNORECASE)),
}
XLSX = re.compile(r"\.xlsx?\b|\bxlsx?\b", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("\u200b", "")) for href, text in links(url)]


def pick(topic: str, year: int, links: list[tuple[str, str]]) -> dict[str, Any] | None:
    """The topic's XLSX attachment on the year page; None when the page lists none. Two different matching
    attachments are an error (a choice between files is never made by position)."""
    _slug, text = TOPICS[topic]
    found = [(h, t) for h, t in links if "/attachment/" in h and XLSX.search(t) and text.search(t)]
    if not found:
        return None
    if len({h for h, _ in found}) > 1:
        raise ValueError(f"{SOURCE} {topic} {year}: more than one matching attachment: {found}")
    return {"entity": f"{topic}_{year}", "url": found[0][0], "format": "raw", "version": "last_modified"}


def discover(today: datetime | None = None) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    year_now = (today or datetime.now(UTC)).year
    found: dict[str, dict[str, Any]] = {}
    without: list[str] = []
    pages: dict[str, list[tuple[str, str]]] = {}
    for year in range(FIRST_YEAR, year_now + 1):
        for topic, (slug, _text) in TOPICS.items():
            url = BASE + slug.format(year=year)
            if url not in pages:
                time.sleep(PAUSE)
                try:
                    pages[url] = _links(url)
                except Exception:  # noqa: BLE001 - a page not (yet) published: reported as without
                    pages[url] = []
            spec = pick(topic, year, pages[url])
            if spec:
                found[spec["entity"]] = spec
            else:
                without.append(f"{topic}_{year}")
    return found, {"years": [FIRST_YEAR, year_now], "without_workbook": without, "pages_read": len(pages)}


def held(landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: attachment URLs landed}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, set[str]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], set()).add(receipt["source_url"])
    return out


def plan(found: dict[str, dict[str, Any]], landed: dict[str, set[str]], year_now: int) -> list[dict[str, Any]]:
    """Workbooks whose attachment URL is not landed yet, and the current year's (its bytes decide)."""
    return [
        spec
        for entity, spec in sorted(found.items())
        if spec["url"] not in landed.get(entity, set()) or int(entity[-4:]) == year_now
    ]


def coverage_level(found: dict[str, dict[str, Any]], summary: dict[str, Any]) -> str:
    """'error' when nothing is found or a year before the current one lacks a workbook (a renamed link must not be
    skipped silently); 'warning' when only the current year lacks one (its page can go up in January without data);
    else 'notice'."""
    last = summary["years"][1]
    missing = summary["without_workbook"]
    if not found or any(int(entity[-4:]) < last for entity in missing):
        return "error"
    return "warning" if missing else "notice"


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    todo = plan(found, held(), summary["years"][1])
    level = coverage_level(found, summary)
    annotate(level, f"{SOURCE} landing inspect", {**summary, "to_land": [s["entity"] for s in todo]})
    return 1 if level == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    todo = plan(found, held(landing_root), summary["years"][1])
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
    level = coverage_level(found, summary)
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
