"""GUS DBW Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   website inventory of bulk ZIP files vs Landing, no downloads
landing.extract   extract.py --mode load      missing and changed files (unchanged bytes skipped)
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.gus_dbw import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(min(p.budget_minutes, 330))])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
