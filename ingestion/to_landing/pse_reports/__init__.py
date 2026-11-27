"""PSE Reports API v2 (rce-pln, his-wlk-cal) Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   business days of the window vs Landing, no downloads
landing.extract   extract.py --mode load      missing days (oldest first), then the newest 7 days again
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.pse_reports import extract

    return extract.main(["--mode", mode, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
