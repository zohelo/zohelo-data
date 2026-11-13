"""Read-only probe of statystyka.policja.pl for PLG-033 (13 Nov 2026): category pages and their workbooks.

robots.txt is read first and honoured (it disallows /ftp/ only); GET only, one request at a time, 2 seconds apart,
on statystyka.policja.pl only. Walks the /st/przestepstwa-ogolem tree (at most 70 pages), prints every XLSX link
with its link text and the page title, then downloads the workbooks (at most 40) and prints each sheet as rows of
cells (first 70 rows) to the job log, so the layout can be read. No writes to R2.
"""

from __future__ import annotations

import hashlib
import html as htmllib
import json
import re
import time
import urllib.robotparser
from urllib.parse import urljoin, urlsplit

import requests

from ingestion.common import xlsx_cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "statystyka.policja.pl"
START = "https://statystyka.policja.pl/st/przestepstwa-ogolem"
LAST = [0.0]
MAX_PAGES = 70
MAX_FILES = 40
MAX_ROWS = 70


def get(url: str) -> requests.Response:
    wait = 2.0 - (time.time() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    try:
        for attempt in range(3):
            try:
                return requests.get(url, headers={"User-Agent": UA}, timeout=(20, 60), allow_redirects=False)
            except requests.RequestException as exc:
                print("RETRY", url, repr(exc)[:200], flush=True)
                time.sleep(10 * (attempt + 1))
        raise RuntimeError(f"failed: {url}")
    finally:
        LAST[0] = time.time()


def main() -> None:
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(get(f"https://{HOST}/robots.txt").text.splitlines())
    seen: set[str] = set()
    queue = [START]
    files: dict[str, tuple[str, str]] = {}
    while queue and len(seen) < MAX_PAGES:
        url = queue.pop(0)
        if url in seen or not rp.can_fetch(UA, url):
            continue
        seen.add(url)
        r = get(url)
        title = re.search(r"<title>(.*?)</title>", r.text, re.S | re.I) if r.status_code == 200 else None
        print("PAGE", r.status_code, url, json.dumps(htmllib.unescape(title.group(1).strip()) if title else None,
                                                     ensure_ascii=False), flush=True)
        if r.status_code != 200:
            continue
        for href, text in re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', r.text, flags=re.S | re.I):
            absolute = urljoin(url, htmllib.unescape(href.strip())).split("#")[0]
            parts = urlsplit(absolute)
            if parts.netloc != HOST:
                continue
            label = htmllib.unescape(re.sub(r"<[^>]+>|\s+", " ", text)).strip()
            if re.search(r"\.xlsx?$", parts.path, re.I):
                print("XLSX", url, absolute, json.dumps(label, ensure_ascii=False), flush=True)
                files.setdefault(absolute, (url, label))
            elif parts.path.startswith("/st/przestepstwa-ogolem") and absolute not in seen and absolute not in queue:
                queue.append(absolute)
    print("PAGES", len(seen), "FILES", len(files), flush=True)

    for n, (url, (page, label)) in enumerate(files.items()):
        if n >= MAX_FILES or not rp.can_fetch(UA, url):
            print("SKIP", url, flush=True)
            continue
        f = get(url)
        body = f.content
        print("FILE", json.dumps({"url": url, "page": page, "status": f.status_code, "bytes": len(body),
                                  "sha256": hashlib.sha256(body).hexdigest(),
                                  "last_modified": f.headers.get("Last-Modified"), "etag": f.headers.get("ETag"),
                                  "type": f.headers.get("Content-Type")}, ensure_ascii=False), flush=True)
        if f.status_code != 200:
            continue
        name = url.rsplit("/", 1)[-1]
        try:
            grid: dict[tuple[str, int], list[str]] = {}
            fmts: dict[str, set[str]] = {}
            for c in xlsx_cells.cells(body):
                key = (c["sheet_name"], c["row_number"])
                if c["row_number"] <= MAX_ROWS:
                    grid.setdefault(key, []).append(f'{c["cell_ref"]}={c["value_text"][:60]}')
                fmts.setdefault(c["sheet_name"], set()).add(f'{c["cell_type"]}:{c["number_format"]}')
            for (sheet, row), cells in grid.items():
                print("ROW", name, json.dumps(sheet, ensure_ascii=False), row, " | ".join(cells)[:900], flush=False)
            for sheet, f_set in fmts.items():
                print("FMT", name, json.dumps(sheet, ensure_ascii=False), sorted(f_set)[:12], flush=True)
        except Exception as exc:  # a probe: report and go on
            print("DECODE_ERROR", url, repr(exc)[:300], flush=True)


if __name__ == "__main__":
    main()
