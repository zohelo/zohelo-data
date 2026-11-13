"""Read-only probe of ISWS multi-year workbooks (register PLG-031): listing links and every workbook's cells.

Prints to the job log only (no R2 writes): the listing's download links with their text and the text before them,
then for each workbook its Content-Disposition, size, magic bytes, sheets and cells (row by row, non-empty only).
One request at a time, PAUSE seconds apart.
"""

from __future__ import annotations

from html.parser import HTMLParser
import io
import re
import sys
import time
from urllib.parse import urljoin

import requests

LISTING = "https://isws.ms.gov.pl/pl/baza-statystyczna/opracowania-wieloletnie/"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 2.0
MAX_ROWS = 400


class Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: list[list[str]] = []  # [href, link text, text before]
        self.text: list[str] = []
        self.open = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href") or ""
            self.items.append([href, "", " ".join("".join(self.text[-40:]).split())[-200:]])
            self.open = True

    def handle_data(self, data):
        self.text.append(data)
        if self.open:
            self.items[-1][1] += data

    def handle_endtag(self, tag):
        if tag == "a":
            self.open = False


def get(url: str) -> requests.Response:
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
            if r.status_code == 200:
                return r
            print(f"HTTP {r.status_code} {url}", flush=True)
        except requests.RequestException as exc:
            print(f"ERR {type(exc).__name__} {url}", flush=True)
        time.sleep(20 * (attempt + 1))
    raise RuntimeError(url)


def dump_xls(body: bytes) -> None:
    import xlrd

    book = xlrd.open_workbook(file_contents=body, formatting_info=False)
    for sheet in book.sheets():
        print(f"  SHEET {sheet.name!r} rows={sheet.nrows} cols={sheet.ncols} merged={len(sheet.merged_cells)}")
        for r in range(min(sheet.nrows, MAX_ROWS)):
            parts = []
            for c in range(sheet.ncols):
                cell = sheet.cell(r, c)
                if cell.ctype in (0, 6) or cell.value == "":
                    continue
                v = cell.value
                if cell.ctype == 5:
                    v = "ERR:" + xlrd.error_text_from_code.get(v, "?")
                elif isinstance(v, float):
                    v = str(int(v)) if v.is_integer() else repr(v)
                v = " ".join(str(v).split())[:70]
                parts.append(f"{xlrd.formula.colname(c)}={v}")
            if parts:
                print(f"  R{r + 1}: " + " | ".join(parts))


def dump_xlsx(body: bytes) -> None:
    from ingestion.common import xlsx_cells

    rows: dict[tuple[str, int], list[str]] = {}
    for c in xlsx_cells.cells(body):
        rows.setdefault((c["sheet_name"], c["row_number"]), []).append(f"{c['cell_ref']}={c['value_text'][:70]}")
    for (sheet, r), parts in rows.items():
        if r <= MAX_ROWS:
            print(f"  [{sheet}] R{r}: " + " | ".join(parts))


def main(argv: list[str]) -> int:
    page = get(LISTING)
    parser = Page()
    parser.feed(page.text)
    links = []
    for href, text, before in parser.items:
        url = urljoin(page.url, href.strip())
        if "download" in url:
            links.append(url)
            print(f"LINK {url} | text={' '.join(text.split())!r} | before={before!r}")
    print(f"LINKS {len(links)}", flush=True)
    only = {int(a) for a in argv} if argv else None
    for n, url in enumerate(links, 1):
        if only and n not in only:
            continue
        time.sleep(PAUSE)
        try:
            r = get(url)
        except RuntimeError:
            print(f"FILE {n} FAILED {url}")
            continue
        body = r.content
        print(
            f"FILE {n} {url} cd={r.headers.get('Content-Disposition')!r} ct={r.headers.get('Content-Type')!r} "
            f"lm={r.headers.get('Last-Modified')!r} etag={r.headers.get('ETag')!r} bytes={len(body)} magic={body[:4].hex()}",
            flush=True,
        )
        try:
            if body[:2] == b"PK":
                dump_xlsx(body)
            elif body[:4] == bytes.fromhex("d0cf11e0"):
                dump_xls(body)
            else:
                print("  NOT A WORKBOOK: " + body[:200].decode("latin-1"))
        except Exception as exc:  # noqa: BLE001 - a probe reports and goes on
            print(f"  DECODE ERROR {type(exc).__name__}: {exc}")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
