"""NID register of immovable monuments: Landing activities (register PLS-026).

NID publishes the register of immovable monuments (register book A) as one PDF list per voivodeship, Warsaw as a
list of its own, on https://nid.pl/zasoby/rejestr-zabytkow-zasoby/ ("Wykaz zabytków nieruchomych wpisanych do
rejestru zabytków (księga A) - stan na <date>"); the file names carry the date of the list
(``<CODE>-rej_<dd.mm.yyyy>.pdf``), so each file is found by its code on that page. One entity per list.

landing.inspect  the page's link of every list, its Last-Modified against Landing (bulk_file pattern)
landing.extract  every list through the shared bulk_file pattern (exact bytes, receipt after the file, unchanged
                 bytes skipped), one at a time, 3 s apart; the page is read once per run
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from typing import Any

SOURCE = "nid_monuments"
PAGE = "https://nid.pl/zasoby/rejestr-zabytkow-zasoby/"
PAUSE_SECONDS = 3.0
# File code on the page → (entity, TERYT voivodeship code). MAZ is the voivodeship without Warsaw, MAZ-War Warsaw.
LISTS = {
    "DLN": ("dln", "02"),
    "KUJ": ("kuj", "04"),
    "LBL": ("lbl", "06"),
    "LBS": ("lbs", "08"),
    "LDZ": ("ldz", "10"),
    "MAL": ("mal", "12"),
    "MAZ": ("maz", "14"),
    "MAZ-War": ("maz_war", "14"),
    "OPO": ("opo", "16"),
    "PDK": ("pdk", "18"),
    "PDL": ("pdl", "20"),
    "POM": ("pom", "22"),
    "SLS": ("sls", "24"),
    "SWT": ("swt", "26"),
    "WAR": ("war", "28"),
    "WLK": ("wlk", "30"),
    "ZPO": ("zpo", "32"),
}


def pattern(code: str) -> str:
    return rf"/{code}-rej_[^/]*\.pdf$"


def specs(links: list[str] | None = None) -> list[dict[str, Any]]:
    """One spec per list with the URL found on the page (read once); a list without a link fails the run."""
    import re

    from ingestion.to_landing.patterns import bulk_file

    if links is None:
        links = bulk_file.page_links({"url_from": {"page": PAGE, "pattern": r"-rej_[^/]*\.pdf$"}})
    out = []
    for code, (entity, _) in LISTS.items():
        found = [u for u in links if re.search(pattern(code), u)]
        if len(found) != 1:
            raise bulk_file.PermanentError(f"{code}: {len(found)} links match {pattern(code)!r} on {PAGE}")
        out.append({"entity": entity, "url": found[0], "format": "raw", "version": "last_modified"})
    return out


def _extract(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", specs=specs(), pause_seconds=PAUSE_SECONDS)


def _inspect(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "inspect", specs=specs())


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e, _ in LISTS.values()])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
