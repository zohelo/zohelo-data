"""ISWS justice statistics Landing activities (register PLG-031, Ministry of Justice).

The Ministry of Justice's statistical information service (Informator Statystyczny Wymiaru Sprawiedliwości) lists
its multi-year workbooks ("Opracowania wieloletnie") on one page, HUB: every workbook is a link
``download,2853,<n>.html`` whose text is the workbook's title; ``<n>`` is the position in the list, so the URL is not
an identifier. A curated selection (SELECTION) names each landed workbook: entity = a short code of ours, found by a
regex on the link text (the title), which must match exactly one link, so a renamed or removed title fails Landing
instead of being skipped. Version = download time (the server sends no Last-Modified); every selected workbook is
downloaded on every run (a few small files) and lands only when its SHA-256 changed (bulk_file pattern), so the
yearly extension or a correction lands as a new version. Restart unit: one file. One request at a time, PAUSE
seconds apart.

landing.inspect  the selection against the hub's titles and Landing; no writes
landing.extract  every selected workbook, unchanged bytes skipped
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "ms_isws"
HUB = "https://isws.ms.gov.pl/pl/baza-statystyczna/opracowania-wieloletnie/"
LINK = re.compile(r"/download,2853,\d+\.html$")
PAUSE = 2.0
# entity: regex (case-insensitive, re.search) on the workbook title as the hub prints it (whitespace collapsed).
SELECTION: dict[str, str] = {}


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return links(url)


def match(links: list[tuple[str, str]]) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    """Specs of the selected workbooks and, per entity that does not match exactly one workbook link, the titles it
    matched (none or several)."""
    workbooks = [(href, " ".join(text.split())) for href, text in links if LINK.search(href)]
    found, problems = {}, {}
    for entity, pattern in SELECTION.items():
        hits = list(dict.fromkeys((h, t) for h, t in workbooks if re.search(pattern, t, re.IGNORECASE)))
        if len({h for h, _ in hits}) == 1:
            found[entity] = {"entity": entity, "url": hits[0][0], "format": "raw", "version": "ingested"}
        else:
            problems[entity] = [t for _, t in hits]
    return found, problems


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    links = _links(HUB)
    found, problems = match(links)
    summary = {
        "workbooks_listed": len({h for h, _ in links if LINK.search(h)}),
        "selected": len(SELECTION),
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
