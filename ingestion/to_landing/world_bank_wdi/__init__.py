"""World Bank WDI Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   Landing state vs the server's Last-Modified (HEAD)
landing.extract   extract.py --mode load      download the bulk ZIP; write only when the SHA-256 changed
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.world_bank_wdi import extract

    return extract.main(["--mode", mode])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
