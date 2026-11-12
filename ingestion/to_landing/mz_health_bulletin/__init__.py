"""MZ / Centrum e-Zdrowia statistical bulletin: Landing activities (register PLS-017, #2957).

The bulletin page https://ezdrowie.gov.pl/portal/home/badania-i-dane/biuletyn-statystyczny lists every edition as a
plain HTML attachment link (no script, no API): "Biuletyn Statystyczny <year>" (PDF, 2000-2025; 2000, 2004 and 2005
as RAR) and, from the 2019 edition, "Biuletyn Statystyczny <year> tablice" (XLSX), under /downloadFile/<id> (2022 on)
or /pobierz/biuletyn-statystyczny-<year>-tablice (2019-2021). Only the XLSX tables are landed (PDF table extraction is
a separate Task); entity = one edition (``tablice_<year>``, the edition year, not the reference year); version =
download time (the server sends no Last-Modified), written only when the SHA-256 changed. A link not landed yet is
downloaded; the newest edition is downloaded again on every run (a correction can keep its link). Restart unit: one
file.

landing.inspect  the XLSX editions the page lists vs Landing (to land / held); PDF editions counted only; no writes
landing.extract  every listed edition not landed yet, plus the newest edition again, one download at a time
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "mz_health_bulletin"
PAGE = "https://ezdrowie.gov.pl/portal/home/badania-i-dane/biuletyn-statystyczny"
FIRST_YEAR = 2019
PAUSE = 2.0
# link text as rendered: "Biuletyn Statystyczny 2025 tablice xlsx rozmiar 0,29 M 2025-10-31"
TABLES = re.compile(r"^biuletyn statystyczny[ _]?(\d{4}) tablice xlsx\b", re.IGNORECASE)
EDITION = re.compile(r"^biuletyn statystyczny[ _]?(\d{4}) (pdf|rar)\b", re.IGNORECASE)
POSTED = re.compile(r"(\d{4}-\d{2}-\d{2})\s*$")
FILE_LINK = re.compile(r"^https://ezdrowie\.gov\.pl/(downloadFile/\d+|pobierz/[a-z0-9-]+)$")


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return links(url)


def editions(links: list[tuple[str, str]]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """The XLSX table files the page lists, one spec per edition from FIRST_YEAR, and a summary (PDF editions left
    out, posting dates). Two different links for one edition's tables raise: the page changed and must be read
    again, never guessed."""
    found: dict[str, dict[str, Any]] = {}
    pdf, posted = [], {}
    for href, text in links:
        text = " ".join(text.split())
        tables, edition = TABLES.match(text), EDITION.match(text)
        if edition:
            pdf.append(int(edition.group(1)))
        if not tables or int(tables.group(1)) < FIRST_YEAR:
            continue
        if not FILE_LINK.match(href):
            raise ValueError(f"{SOURCE}: unexpected link for {text!r}: {href}")
        entity = f"tablice_{tables.group(1)}"
        if entity in found and found[entity]["url"] != href:
            raise ValueError(f"{SOURCE}: two links for {entity}: {found[entity]['url']}, {href}")
        found[entity] = {"entity": entity, "url": href, "format": "raw", "version": "ingested"}
        if POSTED.search(text):
            posted[entity] = POSTED.search(text).group(1)
    summary = {
        "xlsx_editions": sorted(found),
        "posted": posted,
        "pdf_editions_left_out": f"{min(pdf)}-{max(pdf)} ({len(set(pdf))})" if pdf else None,
    }
    return found, summary


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    return editions(_links(PAGE))


def held(landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: links landed}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, set[str]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], set()).add(receipt["source_url"])
    return out


def plan(found: dict[str, dict[str, Any]], landed: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Editions whose link is not landed, plus the newest edition (re-checked by SHA-256 on every run)."""
    newest = max(found) if found else None
    return [s for e, s in sorted(found.items()) if s["url"] not in landed.get(e, set()) or e == newest]


def coverage_level(found: dict[str, dict[str, Any]]) -> str:
    """'error' when no edition is found or a year between FIRST_YEAR and the newest listed edition has no tables
    file (a renamed link must not be skipped silently); else 'notice'."""
    years = sorted(int(e.rsplit("_", 1)[1]) for e in found)
    if not years or years != list(range(FIRST_YEAR, years[-1] + 1)):
        return "error"
    return "notice"


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    landed = held()
    level = coverage_level(found)
    todo = [s["entity"] for s in found.values() if s["url"] not in landed.get(s["entity"], set())]
    annotate(level, f"{SOURCE} landing inspect", {**summary, "to_land": sorted(todo), "held": sorted(landed)})
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
    level = coverage_level(found)
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
