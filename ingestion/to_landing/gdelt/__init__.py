"""GDELT DOC 2.0 API Landing activities: daily news volume and tone per topic query (CD-030).

landing.extract  two bulk_file entities per gdelt_query row of warehouse/seeds/topic_signal.csv (value = the
                 DOC API query): ``<topic>__volraw`` (mode timelinevolraw: matching articles per day and all
                 monitored articles, ``norm``) and ``<topic>__tone`` (mode timelinetone: average tone); 3 months
                 (the API's window) on the first landing, 2 weeks afterwards (Silver keeps the newest value per
                 day). GDELT asks for at most one request every 5 seconds: 6 seconds between requests, and its
                 HTTP 429 is retried with back-off by the bulk_file pattern.
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from urllib.parse import quote

from ingestion.to_landing.patterns import seeded

SOURCE = "gdelt"
API = "https://api.gdeltproject.org/api/v2/doc/doc"
MODES = {"volraw": "timelinevolraw", "tone": "timelinetone"}
PAUSE_SECONDS = 6


def specs(check_held: bool = True) -> list[dict]:
    """Two files per gdelt_query row (volume, tone)."""
    out = []
    for row in seeded.rows("gdelt_query"):
        for suffix, mode in MODES.items():
            name = f"{row['topic_key']}__{suffix}"
            span = "2w" if check_held and seeded.held(SOURCE, name) else "3m"
            url = f"{API}?query={quote(row['value'], safe='')}&mode={mode}&format=json&timespan={span}"
            out.append(
                {"entity": name, "url": url, "format": "raw", "version": "ingested", "topic_key": row["topic_key"]}
            )
    return seeded.unique(out)


def _extract(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", specs=specs(), pause_seconds=PAUSE_SECONDS, missing_ok=True)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs(check_held=False))


ACTIVITIES = {"landing.extract": _extract, "landing.verify": _verify}
