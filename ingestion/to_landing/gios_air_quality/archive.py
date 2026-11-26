"""GIOŚ yearly archive of verified measurements → Landing (#3119; docs/sources/gios_air_quality.md).

The archive page https://powietrze.gios.gov.pl/pjp/archives (no robots.txt: HTTP 404; Dev probe 45746079470) links
each file as ``/pjp/archives/downloadFile/<id>``: "Wyniki pomiarów z <year> roku" (a ZIP of workbooks per indicator
and averaging time, 2000 to the last verified year; 2.3 MB for 2000, 50 MB for 2025) and "Metadane oraz kody stacji
i stanowisk pomiarowych" (one workbook: stations with their old codes, voivodeship and coordinates). GIOŚ replaces a
file under a new id (2016 and 2018 have ids 602 and 603, their neighbours 262 and 322), so the id is the version:
an entity is landed when the page lists an id that its newest receipt does not name (``detail.file_id``).

Entities: ``archive_<year>`` (.zip) and ``archive_stations`` (.xlsx); version = download time (the server sends no
Last-Modified or ETag, only Content-Length; Dev probe 45747293591). One request at a time, PAUSE_SECONDS apart, bounded retries; the station file first, then missing
years newest first, then replaced ones, until the time budget ends (the next run continues). Unchanged bytes under a
new id write nothing (the next run downloads that file again).
"""

from __future__ import annotations

import io
import re
import time
from typing import Any
import zipfile

import requests

PAGE = "https://powietrze.gios.gov.pl/pjp/archives"
DOWNLOAD = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/{id}"
PAUSE_SECONDS = 5.0
RETRIES = 4
ANCHOR = re.compile(r'downloadFile/(\d+)"[^>]*>(.*?)</a>', re.S)
YEAR = re.compile(r"Wyniki pomiarów z (\d{4}) roku")
STATIONS_TITLE = "Metadane oraz kody stacji i stanowisk pomiarowych"
STATIONS = "archive_stations"


def is_archive(entity: str) -> bool:
    return entity.startswith("archive_")


def session() -> requests.Session:
    from ingestion.to_landing.gios_air_quality import USER_AGENT

    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def get(s: requests.Session, url: str) -> bytes:
    """One GET with bounded retries: network errors, a body shorter than its Content-Length (the server cut a 75 MB
    download short in Dev probe 45747293591), 429 and 5xx back off; any other status raises."""
    for attempt in range(RETRIES):
        status, body = None, b""
        try:
            response = s.get(url, timeout=(30, 600))
            status, body = response.status_code, response.content
            length = response.headers.get("Content-Length")
            if status == 200 and body and (not length or int(length) == len(body)):
                return body
        except requests.RequestException:
            pass
        if status is not None and 400 <= status < 500 and status != 429:
            raise RuntimeError(f"{status} for {url}")
        time.sleep(min(30 * 2**attempt, 300))
    raise RuntimeError(f"GIOŚ archive kept failing: {url}")


def listing(s: requests.Session) -> dict[str, dict[str, Any]]:
    """Entity → {"id", "title", "url", "ext"} of every file the archive page lists that this source loads."""
    html = get(s, PAGE).decode("utf-8")
    out: dict[str, dict[str, Any]] = {}
    for match in ANCHOR.finditer(html):
        title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", match.group(2))).strip()
        year = YEAR.fullmatch(title)
        entity = f"archive_{year.group(1)}" if year else STATIONS if title == STATIONS_TITLE else None
        if entity is None:
            continue  # e.g. "Statystyki z lat 2000-2025": not loaded
        if entity in out:
            raise ValueError(f"archive page lists {title!r} twice")
        ext = "xlsx" if entity == STATIONS else "zip"
        out[entity] = {"id": int(match.group(1)), "title": title, "url": DOWNLOAD.format(id=match.group(1)), "ext": ext}
    if STATIONS not in out or not any(e != STATIONS for e in out):
        raise ValueError(f"archive page layout changed: found {sorted(out)}")
    return out


def states(listed: dict[str, dict[str, Any]], held: dict[str, list]) -> dict[str, str]:
    """Entity → missing / stale (a new file id) / current."""
    out = {}
    for entity, item in listed.items():
        copies = held.get(entity) or []
        if not copies:
            out[entity] = "missing"
        else:
            out[entity] = "current" if copies[-1].get("detail", {}).get("file_id") == item["id"] else "stale"
    return out


def order(state: dict[str, str]) -> list[str]:
    """Station file first, then missing years newest first, then stale ones newest first."""
    def key(entity: str) -> tuple[bool, bool, int]:
        year = 0 if entity == STATIONS else int(entity.rsplit("_", 1)[1])
        return entity != STATIONS, state[entity] != "missing", -year

    return sorted((e for e, s in state.items() if s != "current"), key=key)


def run(client, bucket: str, root: str, held: dict[str, list], deadline: float) -> tuple[dict[str, Any], int]:
    """Land every missing or replaced archive file within the time budget."""
    from ingestion.to_landing.gios_air_quality import land

    s = session()
    listed = listing(s)
    state = states(listed, held)
    summary: dict[str, Any] = {"listed": len(listed), "current": sum(v == "current" for v in state.values())}
    summary.update({"landed": [], "unchanged": [], "left_for_next_run": []})
    for entity in order(state):
        if time.monotonic() > deadline:
            summary["left_for_next_run"].append(entity)
            continue
        item = listed[entity]
        time.sleep(PAUSE_SECONDS)
        body = get(s, item["url"])
        try:
            zipfile.ZipFile(io.BytesIO(body)).namelist()  # a ZIP, and the workbook is one too
        except zipfile.BadZipFile as exc:
            raise ValueError(f"{entity}: {item['url']} is not a ZIP file: {exc}") from exc
        detail = {"file_id": item["id"], "title": item["title"], "pattern": "gios_air_archive", "format": item["ext"]}
        result = land(client, bucket, root, entity, body, held.get(entity) or [], detail, ext=item["ext"], url=item["url"])
        summary["unchanged" if result == "unchanged" else "landed"].append(entity)
        summary.setdefault("bytes", 0)
        summary["bytes"] += len(body) if result != "unchanged" else 0
    return summary, 0
