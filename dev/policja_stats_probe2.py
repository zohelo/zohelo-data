"""Read-only probe 2 of statystyka.policja.pl for PLG-033 (12 Nov 2026): the by-administrative-unit workbooks.

robots.txt (Disallow: /ftp/ only) honoured; one request at a time, 2 seconds apart; GET only, no redirects off the
host. For each category page: the XLSX links, then each by-administrative-unit workbook downloaded and printed cell
by cell to the job log (sheet, cell, type, format, value) so its layout can be read. No writes to R2.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from urllib.parse import urljoin

import requests

from ingestion.common import xlsx_cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://statystyka.policja.pl/st/przestepstwa-ogolem"
PAGES = [
    "",
    "/przestepstwa-kryminalne",
    "/przestepstwa-kryminalne/7-wybranych-kategorii-p",
    "/przestepstwa-kryminalne/zabojstwo",
    "/przestepstwa-kryminalne/zgwalcenie",
    "/przestepstwa-kryminalne/ustawa-o-przeciwdzialan",
    "/przestepstwa-gospodarcz",
    "/przestepstwa-gospodarcz/przestepstwa-korupcyjne",
    "/przestepstwa-drogowe",
    "/przestepstwa-drogowe/prowadzenie-pojazdu-w-s",
]
LAST = [0.0]


def get(url: str) -> requests.Response:
    wait = 2.0 - (time.time() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    try:
        return requests.get(url, headers={"User-Agent": UA}, timeout=(20, 60), allow_redirects=False)
    finally:
        LAST[0] = time.time()


def main() -> None:
    for path in PAGES:
        page = BASE + path
        r = get(page)
        print("PAGE", r.status_code, page, r.headers.get("Last-Modified"), flush=True)
        if r.status_code != 200:
            continue
        hrefs = re.findall(r'href="([^"]+\.xlsx)"', r.text)
        links = []
        for h in hrefs:
            u = urljoin(page, h)
            if u not in links:
                links.append(u)
        print("LINKS", json.dumps(links), flush=True)
        for url in links:
            if "postepowania" in url:
                continue
            f = get(url)
            body = f.content
            print(
                "FILE",
                json.dumps(
                    {
                        "url": url,
                        "status": f.status_code,
                        "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "last_modified": f.headers.get("Last-Modified"),
                        "etag": f.headers.get("ETag"),
                        "type": f.headers.get("Content-Type"),
                    }
                ),
                flush=True,
            )
            if f.status_code != 200:
                continue
            try:
                for c in xlsx_cells.cells(body):
                    print(
                        "CELL",
                        url.rsplit("/", 1)[-1],
                        c["sheet_index"],
                        json.dumps(c["sheet_name"], ensure_ascii=False),
                        c["cell_ref"],
                        c["cell_type"],
                        json.dumps(c["number_format"], ensure_ascii=False),
                        json.dumps(c["value_text"], ensure_ascii=False),
                        flush=False,
                    )
            except Exception as exc:  # report and go on: a probe
                print("DECODE_ERROR", url, repr(exc)[:300], flush=True)


if __name__ == "__main__":
    main()
