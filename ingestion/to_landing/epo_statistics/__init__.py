"""EPO patent statistics Landing activities (register EX-004, European Patent Office).

The EPO's statistics data-download page (PAGE) links the workbooks of the current EPO Technology Dashboard edition on
link.epo.org; every file name ends with ``-epo-technology-dashboard-<edition>.xlsx`` (Dev probe 45924424161, 27 Nov
2026: 36 workbooks of the 2025 edition, 18 to 173 KB, no Last-Modified or ETag). A curated selection (SELECTION) names
the landed workbooks: entity = ``<code>_<edition>``, found by a regex on the file name without that ending, so each
edition is its own entity and an edition stays in Landing, Bronze and Silver when the next one replaces it on the page
(Gold takes the newest edition per year). A selected code that matches no link fails inspect and extract (a renamed
file is never skipped silently); two editions listed at once land both. Version = download time (the server sends no
validator); every selected workbook is downloaded on every run (seven small files) and lands only when its SHA-256
changed (bulk_file pattern). Restart unit: one file. One request at a time, PAUSE seconds apart; the page and
link.epo.org allow it (robots.txt of www.epo.org disallows other paths; link.epo.org has none).

landing.inspect  the selection against the page's links and Landing; no writes
landing.extract  every selected workbook, unchanged bytes skipped
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import unquote

SOURCE = "epo_statistics"
PAGE = "https://www.epo.org/en/about-us/statistics/data-download"
EDITION = re.compile(r"/([^/]+)-epo-technology-dashboard-(20\d\d)\.xlsx$")
PAUSE = 2.0
# code: regex (re.fullmatch) on the file name before "-epo-technology-dashboard-<edition>.xlsx". The year span in a name
# moves with each edition (the 2025 edition names the field x country applications file "2016-2026"), so no pattern
# names it. Left out: the dashboard summaries, top-applicant lists (company names), top-10 tables, the other 19 country
# workbooks and the national-office files (docs/sources/epo_statistics.md).
SELECTION: dict[str, str] = {
    "applications_by_country": r"european-patent-applications-\d{4}-\d{4}-per-country-of-residence-of-the-applicant",
    "grants_by_country": r"granted-patents-\d{4}-\d{4}-per-country-of-residence-of-the-applicant",
    "applications_by_field": r"european-patent-applications-\d{4}-\d{4}-per-field-of-technology",
    "grants_by_field": r"granted-patents-\d{4}-\d{4}-per-field-of-technology",
    "applications_by_field_country": (
        r"european-patent-applications-per-field-of-technology-and-per-country-of-residence-of-the-applicant"
        r"-for-each-individual-year-\d{4}-\d{4}"
    ),
    "grants_by_field_country": (
        r"granted-patents-per-field-of-technology-and-per-country-of-residence-for-each-individual-year-\d{4}-\d{4}"
    ),
    "poland": r"poland",
}


def match(links: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    """Specs of the selected workbooks (one per code and edition listed) and, per code that matches no link or two
    links of one edition, the file names it matched."""
    found: dict[str, dict[str, Any]] = {}
    hits: dict[str, list[str]] = {code: [] for code in SELECTION}
    for url in links:
        m = EDITION.search(unquote(url))
        if not m:
            continue
        stem, edition = m.groups()
        for code, pattern in SELECTION.items():
            if re.fullmatch(pattern, stem):
                hits[code].append(f"{stem} ({edition})")
                entity = f"{code}_{edition}"
                if entity in found and found[entity]["url"] != url:
                    found[entity]["ambiguous"] = True
                found.setdefault(entity, {"entity": entity, "url": url, "format": "raw", "version": "ingested"})
    problems = {code: names for code, names in hits.items() if not names}
    problems.update({e: hits[e.rsplit("_", 1)[0]] for e, spec in found.items() if spec.get("ambiguous")})
    return {e: s for e, s in found.items() if not s.get("ambiguous")}, problems


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    from ingestion.to_landing.patterns import bulk_file

    links = bulk_file.page_links({"url_from": {"page": PAGE, "pattern": r"\.xlsx$"}})
    found, problems = match(links)
    summary = {
        "workbooks_listed": len(links),
        "editions_listed": sorted({EDITION.search(unquote(u)).group(2) for u in links if EDITION.search(unquote(u))}),
        "selected": len(found),
        "not_matched_once": problems,
    }
    return found, summary


def held(landing_root: str = "01_landing") -> set[str]:
    """Entities with a landed copy."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    return {r["entity"] for r in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/")}


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    landed = held()
    summary.update({"held": sorted(landed), "missing": sorted(set(found) - landed)})
    level = "error" if summary["not_matched_once"] else "notice"
    annotate(level, f"{SOURCE} landing inspect", summary)
    return 1 if level == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    results, refused, deferred = [], [], 0
    specs = [found[e] for e in sorted(found)]
    for n, spec in enumerate(specs):
        if time.monotonic() > deadline:
            deferred = len(specs) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary.update(
        {
            "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    level = "error" if summary["not_matched_once"] or (refused and not results) else "warning" if refused else "notice"
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if level == "error" else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
