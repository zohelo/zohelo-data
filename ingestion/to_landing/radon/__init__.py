"""RAD-on (OPI PIB) Landing activities: POL-on institutions and study programmes from the RAD-on open-data API.

landing.inspect  Landing per resource (held copies, records) and one small request per resource for the count the
                 API reports (``pagination.maxCount``); nothing is landed
landing.extract  extract.py: every page of each resource, one request at a time with a pause, each record reduced
                 to the resource's allowlist of non-person fields before it lands (docs/sources/radon.md); a
                 resource lands only when its records changed
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

SOURCE = "radon"
# Landing entity -> API resource under https://radon.nauka.gov.pl/opendata/polon/
RESOURCES = {"institutions": "institutions", "courses": "courses"}


def specs() -> list[dict]:
    """The fixed Landing entities, one per resource."""
    return [{"entity": entity, "format": "raw", "version": "ingested"} for entity in RESOURCES]


def _inspect(p) -> int:
    from ingestion.to_landing.radon import extract

    return extract.inspect()


def _extract(p) -> int:
    from ingestion.to_landing.radon import extract

    return extract.extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs())


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
