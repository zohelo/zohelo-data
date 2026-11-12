"""Wikidata Landing activities: Polish territorial units by TERC code (P1653) from the Wikidata Query Service.

landing.inspect  Landing per entity (held copies, newest) and one tiny ASK query that the endpoint answers; the
                 selections themselves are not run (no validator to compare: load compares the SHA-256)
landing.extract  one SPARQL query per entity of queries.rq (units, population, area, coordinates, website, names),
                 one at a time with a pause, through the shared bulk_file pattern (exact JSON bytes, receipt after
                 the file, unchanged bytes skipped); each answer is about 1-2.5 MB and takes under 10 s
landing.verify   every receipt has its file (bulk_file pattern)
The user agent names the project and its website, as Wikimedia's User-Agent policy asks.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from urllib.parse import urlencode

SOURCE = "wikidata"
ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "zohelo-data/1.0 (https://zohelo.com; public statistics platform)"
PAUSE_SECONDS = 5.0


def queries() -> dict[str, str]:
    """Entity → SPARQL text, from queries.rq (a line ``#> <entity>`` starts each query)."""
    text = (Path(__file__).parent / "queries.rq").read_text(encoding="utf-8")
    parts = re.split(r"^#> (\w+)\n", text, flags=re.M)[1:]
    return {entity: body.strip() for entity, body in zip(parts[::2], parts[1::2], strict=True)}


def specs() -> list[dict]:
    return [
        {
            "entity": entity,
            "url": f"{ENDPOINT}?{urlencode({'query': query, 'format': 'json'})}",
            "format": "raw",
            "version": "ingested",  # the endpoint sends no Last-Modified
            "user_agent": USER_AGENT,
            "read_timeout": 120,
            "retries": 3,
        }
        for entity, query in queries().items()
    ]


def _inspect(p) -> int:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = []
    for spec in specs():
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"]))
        newest = held[-1]["version_token"] if held else None
        files.append(
            {
                "entity": spec["entity"],
                "landed_versions": len(held),
                "newest": newest,
                "state": "unknown" if held else "missing",
            }
        )
    try:
        answer = requests.get(
            ENDPOINT, params={"query": "ASK {}", "format": "json"}, headers={"User-Agent": USER_AGENT}, timeout=60
        )
        endpoint = answer.status_code
    except requests.RequestException as exc:
        endpoint = type(exc).__name__
    summary = {"source": SOURCE, "mode": "inspect", "endpoint": endpoint, "files": files}
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
