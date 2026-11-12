"""World Bank GEM Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   declared series of source 15 vs Landing, no downloads
landing.extract   extract.py --mode load      missing series, then series older than the source's lastupdated
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.world_bank_gem import extract

    return extract.main(["--mode", mode, "--filter", p.filter, "--budget-minutes", str(p.budget_minutes)])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
