"""Barometr Zawodów national report and poster: Landing activities (register PLS-030).

Both files of every forecast year are declared in ingestion/sources/barometr_zawodow.yaml with
``years: {from: 2016, open: 2}`` and found on the year's national publication page (``url_from``); Landing, receipts
and versions are the shared bulk_file pattern's. This module adds what the declaration cannot say:

- the year in the link pattern (``{year}``): a page that links another year's file is not taken;
- format ``pdf``: an HTML answer with status 200 is not landed (the declaration allows only raw for PDFs);
- the next year's page as optional: the edition for year N is published in December of N-1 or January of N, while
  ``expand_years`` stops at the current year; a page without the files yet is a notice, not a failure;
- one request at a time, PAUSE seconds apart; inspect leaves out closed years, extract reads a closed year only
  while Landing holds no copy of it.

landing.inspect  open and missing years vs Landing (one page read and one HEAD per file); no writes
landing.extract  open and missing years, written only when the SHA-256 changed
landing.verify   every receipt has its file (Landing or Archive)
"""

from __future__ import annotations

from datetime import datetime
import time
from typing import Any

SOURCE = "barometr_zawodow"
PAUSE_SECONDS = 3.0


def specs(today: datetime | None = None) -> list[dict[str, Any]]:
    """Declared files per year, plus the next year's (optional), with the year in the link pattern."""
    from ingestion.to_landing.patterns import bulk_file

    declared = bulk_file.declaration(SOURCE, today)
    out = []
    for spec in declared:
        out.append(_with_year(spec, spec["year"]))
    for entity in {s["entity"].rsplit("_", 1)[0] for s in declared}:
        newest = max((s for s in declared if s["entity"].startswith(f"{entity}_")), key=lambda s: s["year"])
        year = newest["year"] + 1
        nxt = {**newest, "entity": f"{entity}_{year}", "year": year, "optional": True, "closed": False}
        page = newest["url_from"]["page"].replace(f"year={newest['year']}", f"year={year}")
        nxt["url_from"] = {**newest["url_from"], "page": page}
        out.append(_with_year(nxt, year))
    return sorted(out, key=lambda s: (s["year"], s["entity"]))


def _with_year(spec: dict[str, Any], year: int) -> dict[str, Any]:
    url_from = {**spec["url_from"], "pattern": spec["url_from"]["pattern"].replace("{year}", str(year))}
    return {**spec, "url_from": url_from, "format": "pdf"}


def _run(mode: str, landing_root: str = "01_landing", today: datetime | None = None) -> int:
    from ingestion.to_landing.patterns import bulk_file

    chosen = specs(today)
    if mode == "inspect":  # the shared inspect does not pause: one file per call, PAUSE seconds apart
        status = 0
        for n, spec in enumerate(s for s in chosen if not s.get("closed")):
            if n:
                time.sleep(PAUSE_SECONDS)
            status = max(status, bulk_file.main(SOURCE, mode, landing_root=landing_root, specs=[spec]))
        return status
    # load: bulk_file skips a closed year that Landing holds (no request) and downloads one that is missing
    pause = PAUSE_SECONDS if mode == "load" else 0.0
    return bulk_file.main(SOURCE, mode, landing_root=landing_root, specs=chosen, pause_seconds=pause)


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect"),
    "landing.extract": lambda p: _run("load"),
    "landing.verify": lambda p: _run("verify"),
}
