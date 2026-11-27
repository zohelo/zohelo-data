"""KOWR weekly grain market reports: Landing activities (register PLS-033).

KOWR publishes its weekly "Raport o sytuacji na rynku zbóż w okresie <Monday> – <Sunday>" only as a PDF attachment of
two gov.pl pages: https://www.gov.pl/web/kowr/raporty-tygodniowe (the latest four weeks of grain, meat, dairy and the
combined report) and the archive https://www.gov.pl/web/kowr/Rynek-zboz (175 grain reports, March 2023 to about five
weeks back; Dev probe 45895465874). Entity = one report week (``grain_<yyyymmdd>``, the Monday the link text names);
version = the server's Last-Modified. A week not yet landed is downloaded once; the weeks of raporty-tygodniowe are
read again on every run, so a replaced file lands as a new version (unchanged bytes are skipped). Restart unit: one
file. One request at a time, PAUSE seconds apart (gov.pl attachments), time-budgeted; the rest waits for the next run.

landing.inspect  the weeks the two pages list vs Landing (to land / held); no writes
landing.extract  every week not landed yet, then the latest weeks again, one download at a time, PAUSE apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise
import re
import time
from typing import Any

SOURCE = "kowr_markets"
LATEST = "https://www.gov.pl/web/kowr/raporty-tygodniowe"
ARCHIVE = "https://www.gov.pl/web/kowr/Rynek-zboz"
PAUSE = 5.0
# "Raport o sytuacji na rynku zbóż w okresie 12.10.2026 r. – 18.10.2026 r. ..." ("Raporty o ..." in 2023)
WEEK = re.compile(r"sytuacji na rynku zbóż w okresie (\d{2})\.(\d{2})\.(\d{4})", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("​", "")) for href, text in links(url)]


def listed(links: list[tuple[str, str]]) -> dict[date, str]:
    """{report Monday: attachment URL} of the grain reports one page links. Two different files for one week on one
    page, or a week that does not start on a Monday, are never resolved silently: ValueError."""
    found: dict[date, set[str]] = {}
    for href, text in links:
        match = WEEK.search(text)
        if "/attachment/" not in href or not match:
            continue
        day, month, year = (int(g) for g in match.groups())
        monday = date(year, month, day)
        if monday.weekday() != 0:
            raise ValueError(f"{SOURCE}: report week does not start on a Monday: {text[:100]!r}")
        found.setdefault(monday, set()).add(href)
    double = {d.isoformat(): sorted(u) for d, u in found.items() if len(u) > 1}
    if double:
        raise ValueError(f"{SOURCE}: more than one attachment for one report week: {double}")
    return {d: next(iter(u)) for d, u in found.items()}


def entity(monday: date) -> str:
    return f"grain_{monday:%Y%m%d}"


def spec(monday: date, url: str) -> dict[str, Any]:
    return {"entity": entity(monday), "url": url, "format": "pdf", "version": "last_modified"}


def discover() -> tuple[dict[str, dict[str, Any]], set[str], dict[str, Any]]:
    """(specs by entity, entities of the latest page, summary). The latest page's file wins for a week on both
    pages: it is the one KOWR published last."""
    archive = listed(_links(ARCHIVE))
    time.sleep(2)
    latest = listed(_links(LATEST))
    weeks = {**archive, **latest}
    found = {entity(d): spec(d, u) for d, u in sorted(weeks.items())}
    summary = {
        "archive_weeks": len(archive),
        "latest_weeks": len(latest),
        "weeks": len(weeks),
        "first": min(weeks).isoformat() if weeks else None,
        "newest": max(weeks).isoformat() if weeks else None,
        "gap_weeks": gaps(sorted(weeks)),
    }
    return found, {entity(d) for d in latest}, summary


def gaps(mondays: list[date]) -> list[str]:
    """Mondays between the first and the newest report without one (holiday weeks; information only)."""
    out = []
    for a, b in pairwise(mondays):
        day = a + timedelta(7)
        while day < b:
            out.append(day.isoformat())
            day += timedelta(7)
    return out


def held(landing_root: str = "01_landing") -> set[str]:
    """Entities with at least one landed copy."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    return {r["entity"] for r in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/")}


def plan(found: dict[str, dict[str, Any]], latest: set[str], landed: set[str]) -> list[dict[str, Any]]:
    """Weeks not landed yet (oldest first), then the latest page's weeks already landed (checked for a new file)."""
    new = [s for e, s in sorted(found.items()) if e not in landed]
    again = [s for e, s in sorted(found.items()) if e in landed and e in latest]
    return new + again


def coverage_level(found: dict[str, dict[str, Any]], latest: set[str]) -> str:
    """'error' when either page lists no grain report (a renamed link text must not pass silently)."""
    return "error" if not found or not latest or len(found) == len(latest) else "notice"


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, latest, summary = discover()
    landed = held()
    level = coverage_level(found, latest)
    todo = [e for e in sorted(found) if e not in landed]
    annotate(
        level,
        f"{SOURCE} landing inspect",
        {**summary, "held": len(landed), "to_land": len(todo), "to_land_first": todo[:5]},
    )
    return 1 if level == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, latest, summary = discover()
    todo = plan(found, latest, held(landing_root))
    results, refused, deferred = [], [], 0
    for n, item in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, item, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": item["entity"], "error": str(exc)[:120]})
    landed = [r["entity"] for r in results if r["outcome"] != "unchanged"]
    summary.update(
        {
            "planned": len(todo),
            "landed": len(landed),
            "landed_last": landed[-5:],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "bytes": sum(round(r.get("size_mb", 0) * 1e6) for r in results if r["outcome"] != "unchanged"),
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    level = coverage_level(found, latest)
    if (refused or deferred) and level == "notice":
        level = "warning"
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if level == "error" or (refused and not results) else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
