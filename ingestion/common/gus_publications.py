"""GUS publication tables: annual (or periodic) stat.gov.pl publications whose tables are an XLSX workbook or a ZIP
of XLSX workbooks, one file per edition (register PLS-027 culture, PLS-028 cultural participation, PLS-035 tourism).

Landing. A series page on stat.gov.pl (e.g. ``.../kultura-turystyka-sport/kultura/``) links the newest edition
("Kultura i dziedzictwo narodowe w 2025 r."); every edition page links the series' other editions and its own tables
file ("... Tablice w formacie XLSX [w pliku ZIP]", ``/download/gfx/portalinformacyjny/...``). Entity = one edition
(``<prefix>_<year>``, the year in the edition title = the reference year); version = download time (stat.gov.pl
sends no Last-Modified), written only when the SHA-256 changed, so a corrected file lands as a new version. Editions
not yet landed are downloaded; the newest edition is downloaded again on every run (unchanged bytes are skipped);
older landed editions are not read again. Restart unit: one edition. One request at a time, PAUSE seconds apart.

Bronze. One row per non-empty cell (ingestion/common/xlsx_cells.py) of every XLSX workbook of the file (the file
itself, or each ``.xlsx`` member of the ZIP), with the member name (empty for a plain workbook). A file that is
neither, or a ZIP without any workbook, is rejected (``bronze.<source>_rejected_files``), never guessed.
"""

from __future__ import annotations

from dataclasses import dataclass
import io
import re
import time
from typing import Any
import zipfile

PAUSE = 2.0


@dataclass(frozen=True)
class Series:
    source: str
    series_page: str  # the stat.gov.pl subject page that links the newest edition
    edition_text: str  # regex on an edition link's text; group 1 = the edition year
    tables_text: str  # regex on the tables link's text on an edition page
    prefix: str  # entity = <prefix>_<year>
    first_year: int


def _links(url: str) -> list[tuple[str, str]]:
    """(absolute href, link text with whitespace collapsed) of every <a> on a page, with bounded retries."""
    from html.parser import HTMLParser
    from urllib.parse import urljoin

    import requests

    from ingestion.to_landing.patterns import bulk_file

    class Anchors(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.out: list[list[str]] = []
            self.open = False

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag == "a":
                self.out.append([urljoin(url, (dict(attrs).get("href") or "").strip()), ""])
                self.open = True

        def handle_data(self, data: str) -> None:
            if self.open:
                self.out[-1][1] += data

        def handle_endtag(self, tag: str) -> None:
            if tag == "a":
                self.open = False

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"page {response.status_code}: {url}")
            response.raise_for_status()
            parser = Anchors()
            parser.feed(response.text)
            return [(href, " ".join(text.split())) for href, text in parser.out]
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def editions(series: Series, links: list[tuple[str, str]]) -> dict[int, str]:
    """{year: edition page} of the edition links among a page's links, from first_year on (archive anchors left out)."""
    out: dict[int, str] = {}
    for href, text in links:
        match = re.match(series.edition_text, text)
        if match and "#" not in href and int(match.group(1)) >= series.first_year:
            out.setdefault(int(match.group(1)), href)
    return out


def tables_link(series: Series, links: list[tuple[str, str]]) -> str | None:
    """The edition's tables file: the one link whose text matches tables_text and whose file is .zip or .xlsx."""
    found = [h for h, t in links if re.search(series.tables_text, t) and re.search(r"\.(zip|xlsx)$", h, re.I)]
    found = list(dict.fromkeys(found))
    if len(found) > 1:
        raise ValueError(f"{series.source}: more than one tables link: {found}")
    return found[0] if found else None


def held(series: Series, landing_root: str = "01_landing") -> set[str]:
    """Editions with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{series.source}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def discover(series: Series, landed: set[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Specs of the editions to download (not landed yet, plus the newest) and a summary of what the pages list."""
    found = editions(series, _links(series.series_page))
    if not found:
        raise RuntimeError(f"{series.source}: no edition link on {series.series_page}")
    newest_year = max(found)
    time.sleep(PAUSE)
    pages = {newest_year: _links(found[newest_year])}
    found = {**editions(series, pages[newest_year]), **found}
    specs, without_tables = [], []
    for year in sorted(found):
        entity = f"{series.prefix}_{year}"
        if entity in landed and year != newest_year:
            continue
        if year not in pages:
            time.sleep(PAUSE)
            pages[year] = _links(found[year])
        url = tables_link(series, pages[year])
        if url is None:
            without_tables.append(year)
            continue
        fmt = "zip" if url.lower().endswith(".zip") else "raw"
        specs.append({"entity": entity, "url": url, "format": fmt, "version": "ingested"})
    summary = {"editions_listed": sorted(found), "newest": newest_year, "without_tables_file": without_tables}
    summary["pages_read"] = 1 + len(pages)
    return specs, summary


def activities(series: Series) -> dict[str, Any]:
    """landing.inspect / landing.extract / landing.verify of one series."""

    def _inspect(p) -> int:
        from ingestion.common.report import annotate

        landed = held(series)
        specs, summary = discover(series, landed)
        summary.update({"held": sorted(landed), "to_download": [s["entity"] for s in specs]})
        annotate("notice", f"{series.source} landing inspect", summary)
        return 0

    def _extract(p) -> int:
        return extract(series, p.budget_minutes)

    def _verify(p) -> int:
        from ingestion.to_landing.patterns import bulk_file

        return bulk_file.main(series.source, "verify", specs=[{"entity": e} for e in sorted(held(series))])

    return {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}


def extract(series: Series, budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    specs, summary = discover(series, held(series, landing_root))
    results, refused, deferred = [], [], 0
    for n, spec in enumerate(specs):
        if time.monotonic() > deadline:
            deferred = len(specs) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, series.source, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary.update(
        {
            "planned": len(specs),
            "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    annotate("notice" if not refused else "warning", f"{series.source} landing extract", summary)
    return 1 if refused and not results else 0


def bronze_columns() -> list[tuple[str, str]]:
    """The shared workbook-cell shape (xlsx_cells.COLUMNS) with the ZIP member after the entity."""
    from ingestion.common import xlsx_cells

    return [*xlsx_cells.COLUMNS[:1], ("member", "VARCHAR"), *xlsx_cells.COLUMNS[1:]]


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the cells of the workbook, or of every .xlsx member of the ZIP."""
    from ingestion.common import bulk_bronze, xlsx_cells

    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"neither an XLSX workbook nor a ZIP: {exc}") from exc
    if "xl/workbook.xml" in archive.namelist():
        workbooks = [("", body)]
    else:
        names = sorted(i.filename for i in archive.infolist() if i.filename.lower().endswith(".xlsx"))
        workbooks = [(name, archive.read(name)) for name in names]
    if not workbooks:
        raise ValueError(f"ZIP without an XLSX workbook: {archive.namelist()[:20]}")
    rows, base = [], bulk_bronze.provenance(receipt)
    for member, data in workbooks:
        try:
            cells = list(xlsx_cells.cells(data))
        except (zipfile.BadZipFile, KeyError) as exc:
            raise ValueError(f"member {member!r} is not an XLSX workbook: {exc}") from exc
        rows += [{**base, "member": member, **c} for c in cells]
    if not rows:
        raise ValueError("workbooks without cells")
    rows = [{**r, "source_row_number": i} for i, r in enumerate(rows, 1)]
    return bulk_bronze.pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(bronze_columns()))


def bronze_load(source: str, entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """bronze.load: the newest copy of every landed edition → bronze.<source>_cells."""
    from ingestion.common import bulk_bronze

    return bulk_bronze.load(
        source,
        (namespace, f"{source}_cells"),
        (namespace, f"{source}_rejected_files"),
        bronze_columns(),
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(entities)],
    )


def bronze_activities(source: str, entities) -> dict[str, Any]:
    """bronze.load / bronze.verify / landing.archive / archive.prune of a source whose entities() lists its editions."""

    def _load(p) -> int:
        return bronze_load(source, entities())

    def _verify(p) -> int:
        from ingestion.common import lineage

        return lineage.main(["--source", source])

    def _archive(p) -> int:
        from ingestion.common import archive

        return archive.main(["--source", source])

    def _prune(p) -> int:
        from ingestion.common import retention

        return retention.main(["--source", source])

    return {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
