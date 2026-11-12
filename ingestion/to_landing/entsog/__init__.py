"""ENTSOG Transparency Platform Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   gas months of the window vs Landing (missing / open / closed), no requests
landing.extract   extract.py --mode load      missing months newest first, then open months; unchanged answers are not written
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.entsog import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
