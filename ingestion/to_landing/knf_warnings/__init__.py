"""KNF public warning list (PLG-019) Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   one GET of the list page, parsed and checked, no writes
landing.extract   extract.py --mode load      the list, reduced to the kept fields, written when its SHA-256 changed
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.knf_warnings import extract

    return extract.main(["--mode", mode])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
