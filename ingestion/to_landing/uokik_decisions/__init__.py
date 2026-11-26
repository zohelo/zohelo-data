"""UOKiK decisions (PLG-020) and concentration decisions (PLG-021) Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   categories of the view vs Landing (one request), no writes
landing.extract   extract.py --mode load      every category of the view's index, reduced to the index fields
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.uokik_decisions import extract

    return extract.main(["--mode", mode, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
