"""NFZ pharmacy refunds Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   255 area x ATC entities vs Landing (one request: /available-periods)
landing.extract   extract.py --mode load      missing entities, then entities older than the source's end-date
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.nfz_pharmacy_refunds import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
