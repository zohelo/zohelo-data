"""MRPiPS social assistance statistics Landing activities (register PLS-031).

The Ministry of Family, Labour and Social Policy publishes, per year, a page "Statystyka za rok <year>" (linked from
https://www.gov.pl/web/rodzina/statystyki-pomocy-spolecznej, years 1999 onwards) with the annual reports as old
Excel (.xls) attachments: MRPiPS-03 (social assistance benefits, beneficiaries and reasons, by voivodeship),
MRPiPS-05 and MRPiPS-06. Only MRPiPS-03 is landed (the form was named MPiPS-03 and MRiPS-03 in some years); a
"korekta" (corrected) attachment replaces the original. Entity = one year (``mrpips03_<year>``); version = download
time (gov.pl sends no Last-Modified); an attachment URL already landed is not downloaded again, a new URL (a
correction) lands as a new version. Restart unit: one file.

landing.inspect  the reports the year pages list vs Landing (to land / held); no writes
landing.extract  every listed report not yet landed, one download at a time, PAUSE seconds apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "mrpips_social_assistance"
LISTING = "https://www.gov.pl/web/rodzina/statystyki-pomocy-spolecznej"
FIRST_YEAR = 2019
PAUSE = 2.0
YEAR_TEXT = re.compile(r"^Staty?s?t?y?ka za (?:rok )?(\d{4})$", re.IGNORECASE)
REPORT_TEXT = re.compile(r"M[A-Z]*PS[ _-]*03(?![0-9])", re.IGNORECASE)
CORRECTION = re.compile(r"korekt", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("​", "")) for href, text in links(url)]


def reports(year: int, links: list[tuple[str, str]]) -> dict[str, Any] | None:
    """The year's MRPiPS-03 attachment (a correction if there is one); None when the page lists none."""
    found = [(h, t) for h, t in links if "/attachment/" in h and REPORT_TEXT.search(t) and ".xls" in t.lower()]
    # .xls covers .xlsx too: 2021 was published as an XLSX workbook
    corrected = [(h, t) for h, t in found if CORRECTION.search(t)]
    pick = corrected or found
    if not pick:
        return None
    if len({h for h, _ in pick}) > 1:
        raise ValueError(f"{SOURCE} {year}: more than one MRPiPS-03 attachment: {pick}")
    return {"entity": f"mrpips03_{year}", "url": pick[0][0], "format": "raw", "version": "ingested"}


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    years: dict[int, str] = {}
    for href, text in _links(LISTING):
        match = YEAR_TEXT.match(text.strip())
        if match and int(match.group(1)) >= FIRST_YEAR:
            years.setdefault(int(match.group(1)), href)
    found, without = {}, []
    for year, href in sorted(years.items()):
        time.sleep(PAUSE)
        spec = reports(year, _links(href))
        if spec:
            found[spec["entity"]] = spec
        else:
            without.append(year)
    return found, {"years_listed": sorted(years), "without_report": without, "pages_read": 1 + len(years)}


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

    found, summary = discover()
    todo = plan(found, held())
    annotate(
        "notice" if found else "error", f"{SOURCE} landing inspect", {**summary, "to_land": [s["entity"] for s in todo]}
    )
    return 0 if found else 1


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
    annotate("notice" if not refused else "warning", f"{SOURCE} landing extract", summary)
    return 1 if refused and not results else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
