"""OpenData.org Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   newest full snapshot in the public bucket vs Landing
landing.extract   extract.py --mode load      lands it when newer (~21 GB, MD5 and SHA-256 checked)
landing.verify    extract.py --mode verify    every receipt has its data file (Landing or Archive)
"""

from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.opendata_org import extract

    return extract.main(["--mode", mode])


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
