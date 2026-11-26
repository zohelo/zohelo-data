"""NFZ contracts Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   years and branches in scope vs Landing, no downloads but the year range
landing.extract   extract.py --mode load      missing years and branches, then the open and stale ones
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.nfz_contracts import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
