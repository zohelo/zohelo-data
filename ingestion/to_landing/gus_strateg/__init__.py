"""GUS STRATEG Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   the newest copy of the SOR selection held, no requests
landing.extract   extract.py --mode load      read the SOR selection (about 190 answers) and land it when changed
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.gus_strateg import extract

    return extract.main(["--mode", mode, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
