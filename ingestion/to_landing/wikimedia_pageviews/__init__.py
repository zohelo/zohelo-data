"""Wikimedia pageviews Landing activities: daily views of the Wikipedia articles listed per topic.

landing.extract  one bulk_file entity per wiki_article row of warehouse/seeds/topic_signal.csv (value
                 ``<lang>:<Title>``): the REST API's per-article daily user views, all access, up to yesterday
                 (UTC); 455 days on the first landing, 14 days afterwards (Silver keeps the newest value per day)
landing.verify   every receipt has its file (bulk_file pattern)
A renamed or deleted article (HTTP 404) is a warning, not a failure (gold.topic_quality lists topics without
signals).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from ingestion.to_landing.patterns import seeded

SOURCE = "wikimedia_pageviews"
API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
FIRST_DAYS, NEXT_DAYS = 455, 14


def entity(value: str) -> str:
    lang, title = value.split(":", 1)
    return f"{lang}_{seeded.slug(title)}"


def specs(check_held: bool = True) -> list[dict]:
    """One file per wiki_article row; the window depends on whether the article has landed before."""
    end = datetime.now(UTC).date() - timedelta(days=1)
    out = []
    for row in seeded.rows("wiki_article"):
        lang, title = row["value"].split(":", 1)
        name = entity(row["value"])
        days = NEXT_DAYS if check_held and seeded.held(SOURCE, name) else FIRST_DAYS
        start = end - timedelta(days=days - 1)
        path = quote(title.replace(" ", "_"), safe="")
        url = f"{API}/{lang}.wikipedia.org/all-access/user/{path}/daily/{start:%Y%m%d}/{end:%Y%m%d}"
        out.append({"entity": name, "url": url, "format": "raw", "version": "ingested", "article": row["value"]})
    return seeded.unique(out)


def _extract(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", specs=specs(), pause_seconds=0.2, missing_ok=True)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs(check_held=False))


ACTIVITIES = {"landing.extract": _extract, "landing.verify": _verify}
