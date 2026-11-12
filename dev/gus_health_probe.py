"""Read-only probe: GUS 'Zdrowie i ochrona zdrowia' publication (register PLS-004). No writes anywhere.

Reads the stat.gov.pl health subject pages, lists the edition links, opens the newest and the oldest edition pages,
lists their file links, downloads the tables file of each and prints, per workbook and sheet, the title rows; for
sheets whose title matches `show=<regex>` (newest edition only) the first rows of the grid. One request at a time,
2 s apart, bounded retries.
"""

from __future__ import annotations

import io
import re
import sys
import time
import zipfile

from ingestion.common import xlsx_cells
from ingestion.common.gus_publications import _links

import requests

from ingestion.to_landing.patterns import bulk_file

PAGES = [
    "https://stat.gov.pl/obszary-tematyczne/zdrowie/",
    "https://stat.gov.pl/obszary-tematyczne/zdrowie/zdrowie/",
]
EDITION = re.compile(r"^Zdrowie i ochrona zdrowia w (\d{4})(?: r\.| roku)?$")


def get(url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=120)
            print(f"  GET {url} -> {r.status_code} {len(r.content)} bytes {r.headers.get('Content-Type')}")
            if r.status_code >= 400:
                return None
            return r.content
        except requests.RequestException as exc:
            print("  error", exc)
            time.sleep(10 * (attempt + 1))
    return None


def workbooks(body: bytes) -> list[tuple[str, bytes]]:
    z = zipfile.ZipFile(io.BytesIO(body))
    if "xl/workbook.xml" in z.namelist():
        return [("", body)]
    print("  members:", [(i.filename, i.file_size) for i in z.infolist()][:80])
    return [(i.filename, z.read(i)) for i in z.infolist() if i.filename.lower().endswith(".xlsx")]


def grid(data: bytes):
    out: dict[str, dict[int, dict[int, str]]] = {}
    for c in xlsx_cells.cells(data):
        out.setdefault(c["sheet_name"], {}).setdefault(int(c["row_number"]), {})[int(c["column_number"])] = str(
            c["value_text"]
        )
    return out


def show_book(label: str, body: bytes, show: str | None, rows: int) -> None:
    for member, data in workbooks(body):
        sheets = grid(data)
        print(f"  == {label} member={member!r} sheets={len(sheets)}")
        for name, g in sheets.items():
            title = " ".join(" ".join(v for _, v in sorted(g.get(r, {}).items())) for r in (1, 2, 3, 4))
            print(f"    [{name}] rows={len(g)} :: {' '.join(title.split())[:230]}")
            if show and re.search(show, title, re.I):
                for r in sorted(g)[:rows]:
                    cells = " | ".join(f"{k}:{' '.join(v.split())[:38]}" for k, v in sorted(g[r].items())[:18])
                    print(f"      r{r}: {cells}")


def main(argv: list[str]) -> int:
    show, rows = None, 45
    for arg in argv:
        if arg.startswith("show="):
            show = arg[5:]
        elif arg.startswith("rows="):
            rows = int(arg[5:])
    editions: dict[int, str] = {}
    for page in PAGES:
        print(f"\n===== {page}")
        try:
            links = _links(page)
        except Exception as exc:  # noqa: BLE001 - probe reports and continues
            print("  page error", type(exc).__name__, exc)
            continue
        for href, text in links:
            if re.search(r"zdrow", href + " " + text, re.I) and "#" not in href:
                print(f"  {href} | {text[:110]}")
            m = EDITION.match(text)
            if m and "#" not in href:
                editions.setdefault(int(m.group(1)), href)
        time.sleep(2)
    if not editions:
        print("no edition link found")
        return 0
    newest = max(editions)
    print(f"\n===== edition page {newest}: {editions[newest]}")
    links = _links(editions[newest])
    for href, text in links:
        m = EDITION.match(text)
        if m and "#" not in href:
            editions.setdefault(int(m.group(1)), href)
    print("editions:", {y: editions[y] for y in sorted(editions)})
    for year in sorted({newest, min(editions)}, reverse=True):
        time.sleep(2)
        page_links = links if year == newest else _links(editions[year])
        print(f"\n===== files of {year}")
        files = [(h, t) for h, t in page_links if re.search(r"\.(zip|xlsx|xls|pdf)$", h, re.I)]
        for h, t in files:
            print(f"  {h} | {t[:120]}")
        tables = [h for h, t in files if re.search(r"tablic|tabel", t, re.I) and re.search(r"\.(zip|xlsx)$", h, re.I)]
        tables = list(dict.fromkeys(tables))
        print("  tables links:", tables)
        for url in tables[:2]:
            time.sleep(2)
            body = get(url)
            if body:
                try:
                    show_book(str(year), body, show if year == newest else None, rows)
                except Exception as exc:  # noqa: BLE001
                    print("  workbook error", type(exc).__name__, exc)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
