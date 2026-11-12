"""URE price indicators Landing activities: the shared bulk_file pattern with one source rule.

landing.inspect  bulk_file pattern (run.py)
landing.extract  bulk_file load of the declared pages, 2 seconds apart; a download whose page content (title,
                 publication and modification date, every table cell; to_bronze/ure/load.py ``parse``) equals the
                 newest landed copy is not landed again: www.ure.gov.pl sends a different page on every request
                 (same size, new SHA-256: a per-request token; Dev trial 43286298849), so the SHA-256 alone would
                 land every page on every run
landing.verify   bulk_file pattern (run.py)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

SOURCE = "ure"


def same_page(held: dict[str, Any], path: Path) -> bool:
    """True when the downloaded page carries the same content as the newest landed copy (``held``, its receipt)."""
    from ingestion.common.bulk_bronze import read_raw
    from ingestion.common.r2 import s3
    from ingestion.to_bronze.ure.load import parse

    old = read_raw(s3(), held["object"]["bucket"], SOURCE, held)
    try:
        return parse(old) == parse(path.read_bytes())
    except (ValueError, UnicodeDecodeError):
        return False


def _extract(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", pause_seconds=2, same=same_page)


ACTIVITIES = {"landing.extract": _extract}
