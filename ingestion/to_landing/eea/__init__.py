"""EEA Datahub Landing activities: the newest version of the greenhouse gas inventory (GovReg) as one CSV ZIP.

The EEA publishes every version of a dataset as its own catalogue record with its own file share, so the file URL
changes per release. ``resolve()`` finds it at run time, read-only and in a handful of small requests:

1. the series record's siblings (``/catalogue/srv/api/records/<series>/related?type=siblings``, JSON) whose title
   starts with "GovReg: National emissions reported to the UNFCCC"; the newest year in their titles;
2. for each version record of that year, its download page ``https://sdi.eea.europa.eu/data/<record>`` (the public
   share's token in the page) and the share's file list (public WebDAV ``PROPFIND``, depth 1);
3. the ``UNFCCC_<n>_csv.zip`` of the version whose ZIP is newest (Last-Modified); its public download link
   ``/datashare/s/<token>/download?path=/&files=<name>``.

The file itself is landed by the shared bulk_file pattern (exact bytes, receipt after the file, never overwritten,
an unchanged SHA-256 is skipped; the share's link sends no Last-Modified, so version = download time).

landing.inspect  resolve + bulk_file inspect (missing / stale / current)
landing.extract  resolve + bulk_file load
landing.verify   every receipt has its file (bulk_file pattern; no requests to the EEA)
"""

from __future__ import annotations

from email.utils import parsedate_to_datetime
import re
import time
from urllib.parse import quote

import requests

SOURCE = "eea"
ENTITY = "govreg_ghg_inventory"
SERIES = "3b7fe76c-524a-439a-bfd2-a6e4046302a2"  # GovReg national emissions reported to the UNFCCC, all versions
CATALOGUE = "https://sdi.eea.europa.eu/catalogue/srv/api/records"
DATA = "https://sdi.eea.europa.eu/data"
SHARE = "https://sdi.eea.europa.eu/datashare"
TITLE = re.compile(r"^GovReg: National emissions reported to the UNFCCC")
ZIP = re.compile(r"^UNFCCC_\d+_csv\.zip$")
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
TIMEOUT, RETRIES, PAUSE = 120, 4, 1.0


def _request(session: requests.Session, method: str, url: str, **kw) -> requests.Response:
    """Bounded retries with backoff on network errors, 429 and 5xx; other statuses fail at once."""
    for attempt in range(RETRIES):
        time.sleep(PAUSE)
        try:
            response = session.request(method, url, timeout=TIMEOUT, **kw)
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code in (200, 207):
            return response
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"EEA {method} {url}: HTTP {response.status_code}")
        time.sleep(min(240, 15 * 2**attempt))
    raise RuntimeError(f"EEA {method} {url}: kept failing")


def versions(session: requests.Session) -> list[dict]:
    """GovReg version records of the newest year named in their titles."""
    related = _request(
        session, "GET", f"{CATALOGUE}/{SERIES}/related?type=siblings", headers={"Accept": "application/json"}
    ).json()
    found = {}
    for item in related.get("siblings") or []:
        title = (item.get("title") or {}).get("eng", "")
        year = re.search(r"\b(19|20)\d{2}\b", title)
        if TITLE.match(title) and year:
            found[item["id"]] = {"record": item["id"], "title": title, "year": int(year.group(0))}
    if not found:
        raise RuntimeError(f"EEA series {SERIES}: no GovReg version among its siblings")
    newest = max(v["year"] for v in found.values())
    return [v for v in found.values() if v["year"] == newest]


def share_zip(session: requests.Session, record: str) -> dict | None:
    """The CSV ZIP of one version record's share: name, Last-Modified, size and public download URL."""
    page = _request(session, "GET", f"{DATA}/{record}").text
    tokens = re.findall(r"datashare/s/([A-Za-z0-9]+)", page)
    if not tokens:
        return None
    token = tokens[0]
    listing = _request(
        session, "PROPFIND", f"{SHARE}/public.php/webdav/", auth=(token, ""), headers={"Depth": "1"}
    ).text
    for block in re.findall(r"<d:response>(.*?)</d:response>", listing, re.S):
        name = re.search(r"<d:href>[^<]*/([^/<]+)</d:href>", block)
        modified = re.search(r"<d:getlastmodified>([^<]+)</d:getlastmodified>", block)
        size = re.search(r"<d:getcontentlength>(\d+)</d:getcontentlength>", block)
        if name and ZIP.match(name.group(1)) and modified:
            return {
                "file": name.group(1),
                "modified": parsedate_to_datetime(modified.group(1)),
                "size": int(size.group(1)) if size else None,
                "url": f"{SHARE}/s/{token}/download?path=%2F&files={quote(name.group(1))}",
            }
    return None


def resolve() -> list[dict]:
    """The bulk_file spec of the newest GovReg CSV ZIP."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    best = None
    for version in versions(session):
        found = share_zip(session, version["record"])
        if found and (best is None or found["modified"] > best["modified"]):
            best = {**found, **version}
    if best is None:
        raise RuntimeError("EEA GovReg: no version share offers a UNFCCC_<n>_csv.zip")
    print(f"eea resolved: {best['title']} ({best['record']}), {best['file']}, {best['size']} bytes", flush=True)
    return [
        {"entity": ENTITY, "url": best["url"], "format": "zip", "version": "last_modified", "record": best["record"]}
    ]


def _run(mode: str, p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    specs = [{"entity": ENTITY}] if mode == "verify" else resolve()
    return bulk_file.main(SOURCE, mode, specs=specs)


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect", p),
    "landing.extract": lambda p: _run("load", p),
    "landing.verify": lambda p: _run("verify", p),
}
