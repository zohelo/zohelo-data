"""GDDKiA rest areas (MOP table) Landing activities (register PL-ENV-023).

GDDKiA links on https://www.gov.pl/web/gddkia/wykaz-parkingow-i-mop one workbook "Tabela MOP istniejące", the
existing rest areas (miejsca obsługi podróżnych) and parkings on national roads, as an attachment
(``https://www.gov.pl/attachment/<uuid>``) whose link names its edition month ("Tabela MOP - wrzesień 2026 r.").
The uuid changes per edition, so the page is the entry. Entity = one edition (``mop_<yyyy>_<mm>``, from the month in
the link text); version = Last-Modified; a new attachment URL lands only when its bytes differ (bulk_file.load_file).
Guards, never a silent first-link pick: a page without such a link, a link whose month cannot be read, or two
different links for one month stop the run. Restart unit: one file. One request at a time, PAUSE seconds apart.

landing.inspect  the editions the page links vs Landing; no writes
landing.extract  every linked edition whose URL is not landed yet
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "gddkia_mop"
PAGE = "https://www.gov.pl/web/gddkia/wykaz-parkingow-i-mop"
PAUSE = 2.0
TABLE_TEXT = re.compile(r"^Tabela\s+MOP\b.*istniej", re.IGNORECASE)
MONTH_TEXT = re.compile(r"\b([a-ząćęłńóśźż]+)\s+(20[0-9]{2})\b", re.IGNORECASE)
# Polish month names, nominative or genitive, by their stem ("wrzesień", "września")
MONTHS = (
    ("stycz", 1),
    ("lut", 2),
    ("mar", 3),
    ("kwie", 4),
    ("maj", 5),
    ("czerw", 6),
    ("lip", 7),
    ("sierp", 8),
    ("wrze", 9),
    ("październik", 10),
    ("pazdziernik", 10),
    ("listopad", 11),
    ("grud", 12),
)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("​", "")) for href, text in links(url)]


def edition(text: str) -> str | None:
    """``mop_<yyyy>_<mm>`` of a link text's first "<month name> <year>", None when no month name is found."""
    for word, year in MONTH_TEXT.findall(text):
        month = next((n for stem, n in MONTHS if word.lower().startswith(stem)), None)
        if month:
            return f"mop_{year}_{month:02d}"
    return None


def tables(links: list[tuple[str, str]]) -> dict[str, dict[str, Any]]:
    """{entity: spec} of the MOP-table attachments of the page; ValueError on a missing, unreadable or doubled one."""
    by_edition: dict[str, set[str]] = {}
    for href, text in links:
        if "/attachment/" not in href or not TABLE_TEXT.search(text.strip()):
            continue
        entity = edition(text)
        if entity is None:
            raise ValueError(f"{SOURCE}: no edition month in the link text {text.strip()[:120]!r}")
        by_edition.setdefault(entity, set()).add(href)
    if not by_edition:
        raise ValueError(f"{SOURCE}: no 'Tabela MOP istniejące' link on {PAGE}")
    double = {e: sorted(h) for e, h in by_edition.items() if len(h) > 1}
    if double:
        raise ValueError(f"{SOURCE}: more than one MOP-table link for an edition: {double}")
    return {
        e: {"entity": e, "url": next(iter(h)), "format": "raw", "version": "last_modified"}
        for e, h in sorted(by_edition.items())
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
        {"editions_listed": sorted(found), "to_land": [s["entity"] for s in todo]},
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
        "editions_listed": sorted(found),
        "planned": len(todo),
        "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
        "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
        "deferred_to_next_run": deferred,
        "refused": refused,
    }
    annotate("error" if refused else "notice", f"{SOURCE} landing extract", summary)
    return 1 if refused else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
