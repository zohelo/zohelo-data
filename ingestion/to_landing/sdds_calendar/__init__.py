"""SDDS advance release calendar Landing activities (register TP-012 NBP, TP-013 Ministry of Finance).

GUS, the SDDS national coordinator, links the year's calendar workbook from PAGE. The file name carries the calendar
year and the update date (``sdds_kalendarz_upowszechniania_danych_w_2026_r_25_11_2026.xlsx``, Dev probe 45963679231),
so the link is read from the page on every run: entity ``calendar_<year>``, so a year keeps its files when the next
year's workbook replaces it on the page. Two links of one year on the page, or none at all, fail inspect and extract
(never a silent first pick). Version = download time (no Last-Modified or ETag); a copy lands only when its SHA-256
changed (bulk_file pattern). Two requests per run, PAUSE seconds apart; robots.txt of stat.gov.pl allows all.

landing.inspect  the page's workbooks against Landing; no writes
landing.extract  every listed year's workbook, unchanged bytes skipped
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import unquote

SOURCE = "sdds_calendar"
PAGE = "https://stat.gov.pl/banki-i-bazy-danych/sdds/kalendarz-upowszechniania-danych"
FILE = re.compile(r"/sdds_kalendarz_upowszechniania_danych_w_(20\d\d)_r_[0-9_]*\.xlsx$")
PAUSE = 2.0


def match(links: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    """Specs of the calendar workbooks on the page (one per year) and, per year listed twice, its links."""
    by_year: dict[str, list[str]] = {}
    for url in links:
        m = FILE.search(unquote(url))
        if m:
            by_year.setdefault(m.group(1), []).append(url)
    specs = {
        f"calendar_{year}": {"entity": f"calendar_{year}", "url": urls[0], "format": "raw", "version": "ingested"}
        for year, urls in by_year.items()
        if len(set(urls)) == 1
    }
    return specs, {year: urls for year, urls in by_year.items() if len(set(urls)) > 1}


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    from ingestion.to_landing.patterns import bulk_file

    links = bulk_file.page_links({"url_from": {"page": PAGE, "pattern": r"\.xlsx$"}})
    found, ambiguous = match(links)
    return found, {"workbooks_listed": len(links), "calendars": sorted(found), "ambiguous": ambiguous}


def held(landing_root: str = "01_landing") -> set[str]:
    """Entities with a landed copy."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    return {r["entity"] for r in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/")}


def _problem(summary: dict[str, Any]) -> bool:
    return bool(summary["ambiguous"]) or not summary["calendars"]


def inspect(landing_root: str = "01_landing") -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    landed = held(landing_root)
    summary.update({"held": sorted(landed), "missing": sorted(set(found) - landed)})
    level = "error" if _problem(summary) else "notice"
    annotate(level, f"{SOURCE} landing inspect", summary)
    return 1 if level == "error" else 0


def extract(landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    results, refused = [], []
    for entity in sorted(found):
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, found[entity], landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": entity, "error": str(exc)[:120]})
    summary.update(
        {
            "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "refused": refused,
        }
    )
    level = "error" if _problem(summary) or refused else "notice"
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if level == "error" else 0


def _inspect(p) -> int:
    return inspect()


def _extract(p) -> int:
    return extract()


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
