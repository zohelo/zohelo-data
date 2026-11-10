"""Official release calendars Landing activities (topics, O32): Eurostat euro indicators, GUS, ECB.

landing.extract  one bulk_file entity per calendar page or window, built here from the run date:
                 eurostat_euro_indicators  the release calendar's JSON feed (``/eurostat/o/calendars/eventsJson``,
                                           euro indicators only), 14 days back to 90 days ahead
                 gus_day_<YYYYMMDD>        GUS ``kalendarium`` day pages (stat.gov.pl/kalendarium/DD-MM-YYYY.html),
                                           7 days back to 45 days ahead; GUS has no machine-readable calendar
                 ecb_statistical           ECB statistical calendar (HTML list of forthcoming releases)
                 ecb_meetings              ECB Governing Council meeting calendar (HTML)
                 One request per page and run, 1 second apart (ECB robots.txt asks for a 5-second crawl delay:
                 its two pages come last, 5 seconds apart). A page that is unchanged is not written again.
landing.verify   every receipt has its file (bulk_file pattern)
NBP (MPC meetings, publication calendar) and the Ministry of Finance are parked: nbp.pl answers automated clients
with a bot-protection page, www.gov.pl refuses connections from GitHub runners (Dev probe 42784902820); the seed
release_calendar.csv stays the manual fallback.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

SOURCE = "release_calendars"
EUROSTAT = (
    "https://ec.europa.eu/eurostat/o/calendars/eventsJson?start={start}T00%3A00%3A00Z&end={end}T00%3A00%3A00Z"
    "&theme=&category=&keywords=&isEuroindicator=true&authorInclude=&authorExclude="
)
GUS_DAY = "https://stat.gov.pl/kalendarium/{d:%d-%m-%Y}.html"
ECB = {
    "ecb_statistical": "https://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
    "ecb_meetings": "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html",
}
GUS_BACK, GUS_AHEAD = 7, 45
EUROSTAT_BACK, EUROSTAT_AHEAD = 14, 90


def specs(today=None) -> list[dict]:
    """The calendar files of a run (dates relative to today, UTC)."""
    today = today or datetime.now(UTC).date()
    raw = {"format": "raw", "version": "ingested"}
    start, end = today - timedelta(days=EUROSTAT_BACK), today + timedelta(days=EUROSTAT_AHEAD)
    out = [{"entity": "eurostat_euro_indicators", "url": EUROSTAT.format(start=start, end=end), **raw}]
    for n in range(-GUS_BACK, GUS_AHEAD + 1):
        day = today + timedelta(days=n)
        out.append({"entity": f"gus_day_{day:%Y%m%d}", "url": GUS_DAY.format(d=day), **raw})
    out += [{"entity": name, "url": url, "pause": 5, **raw} for name, url in ECB.items()]
    return out


def _extract(p) -> int:
    import time

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    results, refused = [], []
    for n, spec in enumerate(specs()):
        if n:
            time.sleep(spec.get("pause", 1))
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, "01_landing"))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary = {
        "landed": sum(r["outcome"] != "unchanged" for r in results),
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "refused": refused,
    }
    annotate("notice" if not refused else "warning", f"{SOURCE} landing load", summary)
    return 1 if len(refused) == len(results) + len(refused) else 0


def landed_specs(landing_root: str = "01_landing") -> list[dict]:
    """Every entity with a landed copy (GUS day pages of past runs included), for verify and the Bronze load."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    entities = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        entities |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return [{"entity": e} for e in sorted(entities)]


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=landed_specs())


ACTIVITIES = {"landing.extract": _extract, "landing.verify": _verify}
