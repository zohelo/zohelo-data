"""RPWDL register files: Landing activities (register PLS-020).

The 18 ZIPs are declared in ingestion/sources/rpwdl.yaml and land through the shared bulk_file pattern. This module
adds a pause between the downloads (the endpoint builds each ZIP on request; one request at a time, 5 s apart) and
keeps one register state per year (#3139): each ZIP is declared with ``years: {from: 2026, open: 1}``, so its entity
is ``<register>_<state>_<year>`` (the current year, Europe/Warsaw). Every run downloads the current year's 18 ZIPs
and writes a copy only when its SHA-256 differs from the newest copy of that year; a past year is closed and never
downloaded again, so its newest copy (the last load of that year) stays as the year-end state. A past year without a
copy stays empty: today's register is not that year's state, so it is never downloaded into a past year. The first
run of a year always lands its 18 ZIPs, even when the bytes equal last year's (a new entity holds no copy yet).

The current year's files are required: the shared pattern treats the current year of a per-year file as optional
(a page that does not list the file yet in January), but these URLs do not depend on the year, so a refused
download fails the run instead of becoming a notice. Inspect and extract leave out the closed years (no request).

landing.inspect  current-year files vs Landing (HEAD per file); no writes
landing.extract  every current-year ZIP, written only when its SHA-256 changed
landing.verify   every receipt of every year has its file (Landing or Archive)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

SOURCE = "rpwdl"
PAUSE_SECONDS = 5.0


def specs(today: datetime | None = None) -> list[dict[str, Any]]:
    """The declared files, one entity per register type, ledger state and year, none optional."""
    from ingestion.to_landing.patterns import bulk_file

    return [{**spec, "optional": False} for spec in bulk_file.declaration(SOURCE, today)]


def _run(mode: str, landing_root: str = "01_landing", today: datetime | None = None) -> int:
    from ingestion.to_landing.patterns import bulk_file

    chosen = specs(today)
    if mode in {"inspect", "load"}:
        chosen = [s for s in chosen if not s.get("closed")]
    pause = PAUSE_SECONDS if mode == "load" else 0.0
    return bulk_file.main(SOURCE, mode, landing_root=landing_root, specs=chosen, pause_seconds=pause)


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect"),
    "landing.extract": lambda p: _run("load"),
    "landing.verify": lambda p: _run("verify"),
}
