"""IRENASTAT Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   IRENASTAT tables listed vs Landing (one listing request)
landing.extract   extract.py --mode load      read a new table release in parts and land it as one ZIP
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.irena_stat import extract

    return extract.main(["--mode", mode, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
