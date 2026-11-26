"""KNF pension statistics Landing activities (register PLG-018): the quarterly PPK information.

KNF publishes every statistics release as its own article of the listing
https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne (filtered by market with
``?categories=/publikacje_i_opracowania/dane_statystyczne/PPK``), so the file URL and the article change with every
release. The quarterly PPK release
("Wartość aktywów netto funduszy zdefiniowanej daty (PPK) wg stanu na koniec <quarter> kwartału <year>") links one
XLSX that holds the whole series since the first quarter of 2020 (rolling), so only the newest release is landed.
Entity = ``ppk_quarterly``; version = the file's Last-Modified (download time when absent: KNF sends none), written
only when the SHA-256 changed. robots.txt of www.knf.gov.pl allows every agent (Dev probe 45758367857). Restart unit:
the file.

landing.inspect  the newest PPK article and its XLSX link vs Landing; no writes
landing.extract  the newest release's XLSX (the PPK-filtered listing first, the main listing when it has none),
                 one request at a time, PAUSE seconds apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import html
import re
import time
from typing import Any

SOURCE = "knf_pensions"
ENTITY = "ppk_quarterly"
BASE = "https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne"
# The listing filtered to the PPK category (the form's "categories" filter; lists the PPK releases newest first, ten
# a page: Dev probe 45764209192), then the unfiltered listing (newest releases of every market) as a fallback.
LISTINGS = [BASE + "?categories=/publikacje_i_opracowania/dane_statystyczne/PPK", BASE]
PAUSE = 4.0
ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4}
ANCHOR = re.compile(r'<a\s[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<label>.*?)</a>', re.S | re.I)
TITLE = re.compile(
    r"aktywów netto funduszy zdefiniowanej daty.*?koniec\s+(?P<q>IV|III|II|I)\s+kwarta\w*\s+(?P<year>20\d\d)",
    re.I | re.S,
)
XLSX = re.compile(r"/komponenty/img/[^/]+\.xlsx$", re.I)


def _get(url: str) -> Any:
    """One page, with retries like bulk_file (4xx other than 408/429 fail at once)."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"page {response.status_code}: {url}")
            response.raise_for_status()
            return response
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def articles(page_html: str, page_url: str) -> list[dict[str, Any]]:
    """PPK release articles linked on a listing page: {quarter: (year, q), url, title}, newest first."""
    from urllib.parse import urljoin

    found: dict[str, dict[str, Any]] = {}
    for m in ANCHOR.finditer(page_html):
        url = urljoin(page_url, html.unescape(m["href"]))
        title = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", m["label"])).split())
        t = TITLE.search(title)
        if "articleId=" in url and t:
            found.setdefault(url, {"quarter": (int(t["year"]), ROMAN[t["q"].upper()]), "url": url, "title": title})
    return sorted(found.values(), key=lambda a: a["quarter"], reverse=True)


def discover() -> dict[str, Any] | None:
    """The newest PPK release: {quarter, url (article), title, file (XLSX URL)}, or None when no listing has one."""
    from ingestion.to_landing.patterns import bulk_file

    newest, read = None, []
    for listing in LISTINGS:
        if read:
            time.sleep(PAUSE)
        try:
            response = _get(listing)
        except bulk_file.PermanentError as exc:  # a refused filter: try the main listing
            read.append({"listing": listing, "error": str(exc)[:80]})
            continue
        found = articles(response.text, response.url)
        read.append({"listing": listing, "ppk_articles": len(found)})
        if found:
            newest = found[0]
            break
    print({"listings": read, "newest": newest and newest["title"]}, flush=True)
    if not newest:
        return None
    time.sleep(PAUSE)
    response = _get(newest["url"])
    files = [u for u in bulk_file.links_matching(response.text, response.url, XLSX.pattern)]
    if len(files) != 1:  # one XLSX per release; anything else is a layout change, not a pick
        raise bulk_file.PermanentError(f"{len(files)} XLSX links on {newest['url']}: {files[:4]}")
    return {**newest, "file": files[0]}


def spec_of(release: dict[str, Any]) -> dict[str, Any]:
    return {"entity": ENTITY, "url": release["file"], "format": "raw", "version": "last_modified"}


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    release = discover()
    summary = {"release": release and release["title"], "file": release and release["file"]}
    annotate("notice" if release else "error", f"{SOURCE} landing inspect", summary)
    return 0 if release else 1


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    """budget_minutes is unused: one file of about 130 kB."""
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    release = discover()
    if not release:
        annotate("error", f"{SOURCE} landing extract", {"error": "no PPK release on the listing pages"})
        return 1
    time.sleep(PAUSE)
    result = bulk_file.load_file(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SOURCE, spec_of(release), landing_root)
    annotate("notice", f"{SOURCE} landing extract", {"release": release["title"], "outcome": result["outcome"]})
    return 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": ENTITY}])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
