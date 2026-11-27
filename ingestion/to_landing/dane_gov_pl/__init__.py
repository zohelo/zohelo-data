"""dane.gov.pl catalogue (register PLG-030) Landing activities: catalogue metadata harvested by data.europa.eu.

landing.inspect  Landing (held copies, records of the newest) and one small request for the number of datasets the
                 search API reports for the catalogue `dane-gov-pl`; nothing is landed
landing.extract  extract.py: every dataset record of the catalogue through the search API's scroll, one request at a
                 time with a pause, each record reduced to an allowlist of catalogue fields before it lands
                 (docs/sources/dane_gov_pl.md); the catalogue lands only when its records changed
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

SOURCE = "dane_gov_pl"
ENTITY = "catalogue"


def specs() -> list[dict]:
    """The one Landing entity: the whole catalogue as JSON lines."""
    return [{"entity": ENTITY, "format": "raw", "version": "ingested"}]


def _inspect(p) -> int:
    from ingestion.to_landing.dane_gov_pl import extract

    return extract.inspect()


def _extract(p) -> int:
    from ingestion.to_landing.dane_gov_pl import extract

    return extract.extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs())


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
