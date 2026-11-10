"""GDELT DOC 2.0 API Landing activities: daily news volume and tone per topic query (CD-030).

landing.extract  two bulk_file entities per gdelt_query row of warehouse/seeds/topic_signal.csv (value = the
                 DOC API query): ``<topic>__volraw`` (mode timelinevolraw: matching articles per day and all
                 monitored articles, ``norm``) and ``<topic>__tone`` (mode timelinetone: average tone); 3 months
                 (the API's window) on the first landing, 2 weeks afterwards (Silver keeps the newest value per
                 day). GDELT asks for at most one request every 5 seconds: 6 seconds between requests, and its
                 HTTP 429 is retried with back-off by the bulk_file pattern. Shared runner addresses are often
                 refused for long stretches (Dev probe 42785760450), so the run is best effort: it starts at a
                 different topic each day, stops after 3 refused files in a row or 40 minutes, and reports the
                 rest as deferred (gold.topic_quality shows topics whose volume is stale).
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from urllib.parse import quote

from ingestion.to_landing.patterns import seeded

SOURCE = "gdelt"
API = "https://api.gdeltproject.org/api/v2/doc/doc"
MODES = {"volraw": "timelinevolraw", "tone": "timelinetone"}
PAUSE_SECONDS = 6
BUDGET_SECONDS = 40 * 60
STOP_AFTER_REFUSED = 3


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
    from datetime import UTC, datetime
    import time

    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    todo = specs()
    start = datetime.now(UTC).toordinal() % max(len(todo), 1)
    todo = todo[start:] + todo[:start]  # a different first topic each day
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    deadline, refused_in_row = time.monotonic() + BUDGET_SECONDS, 0
    results, refused, deferred = [], [], []
    for n, spec in enumerate(todo):
        if refused_in_row >= STOP_AFTER_REFUSED or time.monotonic() > deadline:
            deferred = [s["entity"] for s in todo[n:]]
            break
        if n:
            time.sleep(PAUSE_SECONDS)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, "01_landing"))
            refused_in_row = 0
        except (bulk_file.PermanentError, RuntimeError, requests.RequestException) as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
            refused_in_row += 1
    summary = {
        "landed": sum(r["outcome"] != "unchanged" for r in results),
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "refused": refused[:20],
        "deferred": len(deferred),
        "deferred_sample": deferred[:10],
    }
    annotate("notice" if not refused and not deferred else "warning", f"{SOURCE} landing load (best effort)", summary)
    return 0


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs(check_held=False))


ACTIVITIES = {"landing.extract": _extract, "landing.verify": _verify}
