"""ARiMR direct-payment statistics Landing activities (register PLS-034).

ARiMR publishes no spreadsheet or open-data file of its programme statistics: the gov.pl page "Płatności bezpośrednie"
(https://www.gov.pl/web/arimr/platnosci-bezposrednie2, linked from "Wdrażane programy i działania - dane liczbowe")
lists one PDF report a month, "Dane wg stanu na <dd.mm.yyyy> r." (84 attachments, December 2017 to the newest
month; Dev probes 43343134150 and 43345651745). Each report restates the cumulative figures of every campaign by
voivodeship as of its date: applications for area payments, decisions granting them and the amount paid. The newest
report holds the amounts of every campaign since 2004; a year-end report (31 December) also holds applications and
decisions since 2004. So the newest report and every year-end report are landed (nine year-end reports, December
2017 to December 2025, about 0.3 MB each): when Bronze rejects one, the next older one holds the same campaigns.
Entity = one report
(``report_<yyyymmdd>``, the "stanu na" date); version = download time (gov.pl sends no version); an attachment URL
already landed is not downloaded again, a new URL for the same date (a replaced file) lands as a new version.
Restart unit: one file. Older monthly reports are not landed (each later report restates them).

landing.inspect  the reports the page lists vs Landing (to land / held); no writes
landing.extract  the newest and every year-end report not yet landed, one download at a time, PAUSE apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from datetime import date
import re
import time
from typing import Any

SOURCE = "arimr_payments"
PAGE = "https://www.gov.pl/web/arimr/platnosci-bezposrednie2"
PAUSE = 2.0
# "Dane wg stanu na 30.09.2026 r. ...", also "Data wg stanu" and "Dane wg.stanu" in older link texts
AS_OF = re.compile(r"wg\.?\s*stanu\s+na\s+(\d{2})\.(\d{2})\.(\d{4})", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("​", "")) for href, text in links(url)]


def listed(links: list[tuple[str, str]]) -> dict[date, str]:
    """{report date: attachment URL} of the PDF reports on the page. Two different files for one date are a choice
    between entities, never resolved silently: ValueError."""
    found: dict[date, set[str]] = {}
    for href, text in links:
        match = AS_OF.search(text)
        if "/attachment/" not in href or not match or ".pdf" not in text.lower():
            continue
        day, month, year = (int(g) for g in match.groups())
        found.setdefault(date(year, month, day), set()).add(href)
    double = {d.isoformat(): sorted(u) for d, u in found.items() if len(u) > 1}
    if double:
        raise ValueError(f"{SOURCE}: more than one attachment for one report date: {double}")
    return {d: next(iter(u)) for d, u in found.items()}


def wanted(reports: dict[date, str]) -> dict[str, dict[str, Any]]:
    """The newest report and every year-end (31 December) report, as bulk_file specs by entity. Every year-end
    report, not only the newest: Bronze rejects a whole file whose table it cannot check (the 31.12.2025 report
    prints "Zachodniopomorskie" twice and no "Wielkopolskie" in its 2004-2016 applications table, Dev run
    45747874758), and Silver then takes each campaign from the next older report that holds it."""
    picks = {d for d in reports if (d.month, d.day) == (12, 31)}
    if reports:
        picks.add(max(reports))
    specs = [{"entity": f"report_{d:%Y%m%d}", "url": reports[d], "format": "raw", "version": "ingested"} for d in picks]
    return {spec["entity"]: spec for spec in sorted(specs, key=lambda s: s["entity"])}


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    reports = listed(_links(PAGE))
    found = wanted(reports)
    summary = {
        "reports_listed": len(reports),
        "first": min(reports).isoformat() if reports else None,
        "newest": max(reports).isoformat() if reports else None,
        "wanted": sorted(found),
    }
    return found, summary


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


def coverage_level(found: dict[str, dict[str, Any]], summary: dict[str, Any]) -> str:
    """'error' when the page lists no report or no year-end report (a renamed link text must not pass silently)."""
    year_end = [e for e in found if e.endswith("1231")]
    return "error" if not found or not year_end else "notice"


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    todo = plan(found, held())
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
