"""NIZP PZH-PIB EPIMELD annual bulletins: Landing activities (register PLS-019).

The EPIMELD index https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html (robots.txt 404) links the annual bulletins
"Choroby zakaźne i zatrucia w Polsce w <year> roku" as ``<year>/Ch_<year>.pdf`` (1999 on) and, until 2013, partial
updates ``Ch_<year>a.pdf``. Entity = one bulletin from FIRST_EDITION on, ``ch_<year>``; version = the file's
Last-Modified, a new copy only when the SHA-256 changed. The updates are not landed: they reprint single changed
rows without the column headers, so they do not carry the bulletin's table (docs/sources/pzh_epimeld.md); the inspect
annotation lists them. Restart unit: one bulletin.

landing.inspect  bulletins the index links vs Landing (bulk_file inspect: missing / stale / current); no writes
landing.extract  missing bulletins, and held ones whose Last-Modified is newer than the landed copy (one HEAD each),
                 one request at a time, PAUSE seconds apart (bulk_file load: exact bytes, receipt after the file)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "pzh_epimeld"
INDEX = "https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html"
FIRST_EDITION = 2006  # the Bronze parser is checked on the bulletins 2006 on
PAUSE = 5.0
BULLETIN = re.compile(r"/Ch_(\d{4})(a?)\.pdf$", re.IGNORECASE)


def discover() -> tuple[dict[str, str], list[str]]:
    """({entity: PDF URL} of the bulletins the index links, update file names not landed)."""
    from ingestion.to_landing.patterns import bulk_file

    links = bulk_file.page_links({"url_from": {"page": INDEX, "pattern": BULLETIN.pattern}})
    found: dict[str, str] = {}
    updates: list[str] = []
    for url in links:
        year, update = BULLETIN.search(url).groups()
        if int(year) < FIRST_EDITION:
            continue
        if update:
            updates.append(url.rsplit("/", 1)[-1])
        elif f"ch_{year}" in found:
            raise RuntimeError(f"the index links two files for {year}: {found[f'ch_{year}']} and {url}")
        else:
            found[f"ch_{year}"] = url
    return dict(sorted(found.items())), sorted(updates)


def specs(found: dict[str, str]) -> list[dict[str, Any]]:
    return [{"entity": e, "url": u, "format": "pdf", "version": "last_modified"} for e, u in found.items()]


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Bulletins with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def plan(found: dict[str, str], landing_root: str = "01_landing") -> list[dict[str, Any]]:
    """Every bulletin; a held one is ``closed`` (skipped by bulk_file load) unless the server's Last-Modified is
    newer than the landed copy's version or unknown."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), []
    for spec in specs(found):
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root))
        if held:
            time.sleep(PAUSE)
            head, http = bulk_file.probe(spec["url"])
            server = bulk_file.server_version(dict(head.headers)) if head is not None and http == "200" else None
            spec["closed"] = server is not None and server <= held[-1]["version_token"]
        out.append(spec)
    return out


def _inspect(p) -> int:
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    found, updates = discover()
    annotate("notice", f"{SOURCE} landing inspect: index", {"bulletins": len(found), "updates_not_landed": updates})
    return bulk_file.main(SOURCE, "inspect", specs=specs(found))


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    """budget_minutes is unused: about 20 PDFs of 1-2.5 MB, one changes a year."""
    from ingestion.to_landing.patterns import bulk_file

    found, _ = discover()
    if not found:
        raise RuntimeError(f"no annual bulletin linked on {INDEX}")
    return bulk_file.main(SOURCE, "load", landing_root, specs=plan(found, landing_root), pause_seconds=PAUSE)


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held_entities())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
