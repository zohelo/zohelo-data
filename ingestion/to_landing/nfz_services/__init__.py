"""NFZ statistics API, services (JGP) Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   dictionaries vs Landing, the canary benefits' index; no writes
landing.extract   extract.py --mode load      dictionaries, missing benefits, then the benefits checked in this run
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.nfz_services import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
