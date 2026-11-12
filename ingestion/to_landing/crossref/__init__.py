"""Crossref Landing activities: aggregate counts of the Polish-affiliated selection and the Polish funders (CD-029).

No work-level records are landed: every works request has ``rows=0`` and returns only ``total-results`` and facet
counts, so no titles, abstracts, author names or ORCIDs reach Landing. The selection is the works whose deposited
author affiliation text matches ``Poland`` or ``Polska`` (``query.affiliation``; a field query, so an affiliation
in another country that names Poland matches too, and works whose publisher deposits no affiliations are not
counted).

landing.inspect  Landing per entity and one ``rows=0`` request: HTTP status and the rate-limit headers it sends
landing.extract  through the shared bulk_file pattern (exact JSON bytes, parsed before landing, receipt after the
                 file, unchanged bytes skipped), one request at a time with PAUSE_SECONDS between them:
                 ``works_pl_by_year``  the whole selection: total and counts by publication year (``published``)
                 ``works_pl_<year>``   one year FIRST_YEAR .. current year: counts by type, publisher and funder
                 ``funders_pl``        the Funder Registry entries located in Poland (183 on 12 Nov 2026)
landing.verify   every receipt has its file (bulk_file pattern)

Rate limits measured on 12 Nov 2026 (Dev probe 43326689827, public pool): list queries of /works send
``x-rate-limit-limit: 1`` per ``1s`` (pool ``public-array``), /funders 5 per 1s; the 2 s pause stays under both,
and bulk_file retries 429 and 5xx with a growing wait. No contact address is sent (the polite pool is optional).
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
from urllib.parse import urlencode

SOURCE = "crossref"
API = "https://api.crossref.org"
AFFILIATION = "Poland Polska"
FIRST_YEAR = 2000  # yearly detail from here; works_pl_by_year holds every year
PAUSE_SECONDS = 2.0
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
YEAR_FACETS = "type-name:*,publisher-name:*,funder-doi:*"


def _spec(entity: str, path: str, params: dict) -> dict:
    return {
        "entity": entity,
        "url": f"{API}{path}?{urlencode(params)}",
        "format": "json",  # parsed before landing: a cut-off answer is retried, never landed
        "version": "ingested",  # the API sends no Last-Modified
        "user_agent": USER_AGENT,
        "read_timeout": 120,
        "retries": 4,
        "wait": 30,
    }


def specs(today: datetime | None = None) -> list[dict]:
    year = (today or datetime.now(UTC)).year
    works = {"query.affiliation": AFFILIATION, "rows": 0}
    return [
        _spec("works_pl_by_year", "/works", {**works, "facet": "published:*"}),
        *[
            _spec(f"works_pl_{y}", "/works",
                  {**works, "filter": f"from-pub-date:{y},until-pub-date:{y}", "facet": YEAR_FACETS})
            for y in range(FIRST_YEAR, year + 1)
        ],
        _spec("funders_pl", "/funders", {"filter": "location:Poland", "rows": 1000}),
    ]  # fmt: skip


def _inspect(p) -> int:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = []
    for spec in specs():
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"]))
        files.append({"entity": spec["entity"], "landed_versions": len(held),
                      "newest": held[-1]["version_token"] if held else None})  # fmt: skip
    try:
        answer = requests.get(f"{API}/works", params={"rows": 0}, headers={"User-Agent": USER_AGENT}, timeout=(30, 60))
        endpoint = answer.status_code
        limits = {k: v for k, v in answer.headers.items() if k.lower().startswith(("x-rate-limit", "x-api-pool"))}
    except requests.RequestException as exc:
        endpoint, limits = type(exc).__name__, {}
    summary = {"source": SOURCE, "mode": "inspect", "endpoint": endpoint, "rate_limit": limits, "files": files}
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if endpoint == 200 else "error", f"{SOURCE} landing inspect", summary)
    return 0 if endpoint == 200 else 1


def _extract(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", specs=specs(), pause_seconds=PAUSE_SECONDS)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=specs())


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
