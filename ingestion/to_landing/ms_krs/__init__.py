"""Open KRS API (Ministry of Justice) Landing activities: current extracts of the KRS numbers GLEIF lists.

landing.inspect  Landing per part (held copies, records) and one request for yesterday's bulletin of changed
                 KRS numbers, to see that the API answers; no extract is requested
landing.extract  extract.py: first the held numbers that the daily bulletins list since their last request,
                 then selected numbers not held yet, one request at a time with a pause, within the time
                 budget; each answer is reduced to the entity-level allowlist before it lands
                 (docs/sources/ms_krs.md); filter ``upto=<KRS number>`` limits the selection
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

SOURCE = "ms_krs"
PARTS = 16


def specs() -> list[dict]:
    """The fixed Landing entities: the selection split by KRS number modulo PARTS."""
    return [{"entity": f"extracts_{part:02d}", "format": "raw", "version": "ingested"} for part in range(PARTS)]


def _inspect(p) -> int:
    from ingestion.to_landing.ms_krs import extract

    return extract.inspect()


def _extract(p) -> int:
    from ingestion.to_landing.ms_krs import extract

    upto = None
    for item in filter(None, (p.filter or "").split()):
        key, _, value = item.partition("=")
        if key != "upto" or not (value.isdigit() and len(value) == 10):
            raise SystemExit(f"{SOURCE}: unknown filter {item!r} (upto=<10-digit KRS number>)")
        upto = value
    return extract.extract(p.budget_minutes, upto=upto)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs())


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
