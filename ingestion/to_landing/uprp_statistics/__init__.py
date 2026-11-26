"""UPRP industrial-property statistics: Landing activities (register EX-001).

The Patent Office of the Republic of Poland links its annual reports on https://uprp.gov.pl/pl/publikacje/raport-roczny-uprp
(30 PDF files for 2003 to 2025; from 2019 a Polish and an English edition, link texts "2025 (wersja polska)" and
"2025 (wersja angielska)", before that one file a year, link text "2018"). The file addresses follow no pattern
("Raport%20roczny%202024_0.pdf", "raport_roczny_2018.pdf", a dated folder for 2025), so the page is the entry. Only
the Polish reports from FIRST_YEAR are landed: they end with the statistics tables Bronze reads
(ingestion/to_bronze/uprp_statistics/); the 2003 to 2018 reports have other layouts and the English editions repeat
the Polish figures (docs/sources/uprp_statistics.md). Entity = one report (``raport_<year>``), version = the file's
Last-Modified. A file whose URL is landed and whose Last-Modified equals the landed version is not downloaded again;
any other listed file is downloaded and lands only when its SHA-256 differs from the newest landed copy (bulk_file).
Guards, never a silent pick: a year with two different Polish files, a page without any, or a gap between FIRST_YEAR
and the newest year listed stops the run. One request at a time, PAUSE seconds apart, the platform's User-Agent;
the statistics host statystyka.pue.uprp.gov.pl (robots.txt "Disallow: /") is never requested. Restart unit: one
file.

landing.inspect  the reports the page lists vs Landing (to download / held); no writes
landing.extract  every listed report not held at its Last-Modified
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urlparse

SOURCE = "uprp_statistics"
PAGE = "https://uprp.gov.pl/pl/publikacje/raport-roczny-uprp"
HOSTS = {"uprp.gov.pl", "www.uprp.gov.pl"}  # never statystyka.pue.uprp.gov.pl (robots.txt Disallow: /)
FIRST_YEAR = 2019  # first report with the five-year statistics tables Bronze reads
PAUSE = 3.0
POLISH = re.compile(r"^((?:19|20)\d{2})(?:\s*\(wersja polska\))?$", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return links(url)


def reports(links: list[tuple[str, str]]) -> dict[str, dict[str, Any]]:
    """{entity: bulk_file spec} of the Polish annual reports from FIRST_YEAR; ValueError on two files for one year,
    none at all or a gap between FIRST_YEAR and the newest year."""
    by_year: dict[int, set[str]] = {}
    for href, text in links:
        match = POLISH.match(text.strip())
        if not match or not href.lower().endswith(".pdf") or urlparse(href).hostname not in HOSTS:
            continue
        by_year.setdefault(int(match.group(1)), set()).add(href)
    by_year = {y: h for y, h in by_year.items() if y >= FIRST_YEAR}
    if not by_year:
        raise ValueError(f"{SOURCE}: no Polish annual report from {FIRST_YEAR} linked on {PAGE}")
    double = {y: sorted(h) for y, h in by_year.items() if len(h) > 1}
    if double:
        raise ValueError(f"{SOURCE}: more than one Polish report file for a year: {double}")
    gaps = sorted(set(range(FIRST_YEAR, max(by_year) + 1)) - set(by_year))
    if gaps:
        raise ValueError(f"{SOURCE}: no Polish report linked for {gaps} (renamed link text?)")
    return {
        f"raport_{y}": {"entity": f"raport_{y}", "url": next(iter(h)), "format": "pdf", "version": "last_modified"}
        for y, h in sorted(by_year.items())
    }


def discover() -> dict[str, dict[str, Any]]:
    return reports(_links(PAGE))


def held(landing_root: str = "01_landing") -> dict[str, list[dict[str, Any]]]:
    """{entity: receipts, oldest landed first}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, list[dict[str, Any]]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], []).append(receipt)
    return out


def current(spec: dict[str, Any], receipts: list[dict[str, Any]]) -> bool:
    """True when the newest landed copy came from this URL and the server's Last-Modified (HEAD) is its version."""
    from ingestion.to_landing.patterns import bulk_file

    if not receipts or receipts[-1]["source_url"] != spec["url"]:
        return False
    head, http = bulk_file.probe(spec["url"])
    version = bulk_file.server_version(dict(head.headers)) if head is not None and http == "200" else None
    return version is not None and version == receipts[-1]["version_token"]


def plan(found: dict[str, dict[str, Any]], landed: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    todo = []
    for n, (entity, spec) in enumerate(sorted(found.items())):
        if entity in landed:
            if n:
                time.sleep(PAUSE)
            if current(spec, landed[entity]):
                continue
        todo.append(spec)
    return todo


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found = discover()
    todo = plan(found, held())
    annotate(
        "notice",
        f"{SOURCE} landing inspect",
        {"reports_listed": sorted(found), "to_download": [s["entity"] for s in todo]},
    )
    return 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found = discover()
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
    summary = {
        "reports_listed": sorted(found),
        "planned": len(todo),
        "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
        "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
        "megabytes": round(sum(r.get("size_mb", 0) for r in results), 1),
        "deferred_to_next_run": deferred,
        "refused": refused,
    }
    # a listed report the server refuses fails the run after the others have landed (as kgpsp_interventions)
    annotate("error" if refused else "notice", f"{SOURCE} landing extract", summary)
    return 1 if refused else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
