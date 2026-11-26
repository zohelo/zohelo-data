"""NFZ waiting times Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   this month's 32 entities (case x voivodeship) vs Landing, no requests
landing.extract   extract.py --mode load      this month's missing entities, every page of each
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.nfz_waiting_times import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
