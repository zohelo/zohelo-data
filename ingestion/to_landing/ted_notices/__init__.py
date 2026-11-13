"""TED notices (Poland) Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   publication days of the window vs Landing, no downloads
landing.extract   extract.py --mode load      missing days (oldest first), then the newest days again
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.ted_notices import extract

    return extract.main(["--mode", mode, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
