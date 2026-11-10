"""Seeded files: bulk_file entities built from a seed instead of a fixed list (topic signals, docs/topics.md).

The topics seed warehouse/seeds/topic_signal.csv names, per topic, the GDELT queries and Wikipedia articles to
follow; the sources ``gdelt`` and ``wikimedia_pageviews`` turn each row into one bulk_file entity whose URL is
built from the row (``to_landing/<source>/``). Landing, receipts, versions and the Bronze load are the shared
bulk_file pattern's. Imports stay light: ``run.py schedule`` imports the source packages.
"""

from __future__ import annotations

import csv
from pathlib import Path
import re
import unicodedata

ROOT = Path(__file__).resolve().parents[3]
SEED = ROOT / "warehouse" / "seeds" / "topic_signal.csv"


def rows(signal: str) -> list[dict[str, str]]:
    """The seed rows of one signal kind (gdelt_query | wiki_article)."""
    with SEED.open(newline="", encoding="utf-8") as handle:
        return [r for r in csv.DictReader(handle) if r["signal"] == signal]


def slug(text: str) -> str:
    """Lower-case ASCII path segment (ł → l, other letters by Unicode decomposition)."""
    text = unicodedata.normalize("NFKD", text.replace("ł", "l").replace("Ł", "L"))
    return re.sub(r"[^a-z0-9]+", "_", text.encode("ascii", "ignore").decode().lower()).strip("_")


def unique(specs: list[dict]) -> list[dict]:
    """The specs, after checking that no two share an entity."""
    seen: dict[str, str] = {}
    for spec in specs:
        if spec["entity"] in seen:
            raise ValueError(f"two seed rows give the entity {spec['entity']!r}")
        seen[spec["entity"]] = spec["url"]
    return specs


def held(source: str, entity: str) -> bool:
    """True when the entity has a landed copy (the first landing fetches a longer history)."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    return bool(bulk_file.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(source, entity)))
