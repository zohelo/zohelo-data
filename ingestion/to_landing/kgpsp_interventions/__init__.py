"""KG PSP fire-service intervention statistics: Landing activities (register EX-003).

The Headquarters of the State Fire Service publishes on https://www.gov.pl/web/kgpsp/interwencje-psp, per year since
2016, two ZIP attachments (``https://www.gov.pl/attachment/<uuid>``): the standard summary tables ("tabele stałych
zestawień") and the data from incident reports ("zestawienie danych z informacji ze zdarzeń"). Only the standard
tables are landed; the incident-report archives are left out (docs/sources/kgpsp_interventions.md).

The attachment uuids change when a file is replaced, so the page is the entry. The standard-tables links carry only
the year and the file name as text ("2025 2025_.zip 7.96MB", under the heading "Tabele stałych Zestawień-ST SWD PSP"),
the incident-report links name their series ("2025 - zestawienie danych z informacji ze zdarzeń ..."): every
attachment link whose text starts with a year and a ``.zip`` file name and does not name the incident reports is one
entity (``tabele_<year>``). Guards, never a silent first-link pick: a year with two different table links, a page
without any, or a gap between the first and the newest year listed stops the run.
Version = Last-Modified when gov.pl sends it, else download time; an attachment URL already landed is not downloaded
again, a new URL (a replaced file) lands as a new version when its bytes differ. Restart unit: one file. One request
at a time, PAUSE seconds apart.

landing.inspect  the table links the page lists vs Landing (to land / held); no writes
landing.extract  every listed file whose URL is not landed yet
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "kgpsp_interventions"
PAGE = "https://www.gov.pl/web/kgpsp/interwencje-psp"
FIRST_YEAR = 2016
PAUSE = 2.0
TABLES_TEXT = re.compile(r"^(20[0-9]{2})\s+\S*\.zip\b", re.IGNORECASE)
INCIDENTS_TEXT = re.compile(r"informacji\s+ze\s+zdarze", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("​", "")) for href, text in links(url)]


def tables(links: list[tuple[str, str]]) -> dict[str, dict[str, Any]]:
    """{entity: spec} of the standard-tables attachments of the page; ValueError on an ambiguous or missing year."""
    by_year: dict[int, set[str]] = {}
    for href, text in links:
        match = TABLES_TEXT.match(text.strip())
        if "/attachment/" not in href or not match or INCIDENTS_TEXT.search(text):
            continue
        by_year.setdefault(int(match.group(1)), set()).add(href)
    by_year = {y: h for y, h in by_year.items() if y >= FIRST_YEAR}
    if not by_year:
        raise ValueError(f"{SOURCE}: no standard-tables link on {PAGE}")
    double = {y: sorted(h) for y, h in by_year.items() if len(h) > 1}
    if double:
        raise ValueError(f"{SOURCE}: more than one standard-tables link for a year: {double}")
    gaps = sorted(set(range(FIRST_YEAR, max(by_year) + 1)) - set(by_year))
    if gaps:
        raise ValueError(f"{SOURCE}: no standard-tables link for {gaps} (renamed link text?)")
    return {
        f"tabele_{y}": {"entity": f"tabele_{y}", "url": next(iter(h)), "format": "zip", "version": "last_modified"}
        for y, h in sorted(by_year.items())
    }


def discover() -> dict[str, dict[str, Any]]:
    return tables(_links(PAGE))


def held(landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: attachment URLs landed}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, set[str]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], set()).add(receipt["source_url"])
    return out


def plan(found: dict[str, dict[str, Any]], landed: dict[str, set[str]]) -> list[dict[str, Any]]:
    return [spec for entity, spec in sorted(found.items()) if spec["url"] not in landed.get(entity, set())]


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found = discover()
    todo = plan(found, held())
    annotate(
        "notice",
        f"{SOURCE} landing inspect",
        {"years_listed": sorted(found), "to_land": [s["entity"] for s in todo]},
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
        "years_listed": sorted(found),
        "planned": len(todo),
        "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
        "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
        "deferred_to_next_run": deferred,
        "refused": refused,
    }
    annotate("warning" if refused else "notice", f"{SOURCE} landing extract", summary)
    return 1 if refused and not results else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
