"""Read-only probe of statystyka.policja.pl for PLG-033 (12 Nov 2026).

robots.txt is read first and honoured; one request at a time, 2 seconds apart; GET only. Lists the /st/ category
pages linked from the portal (two levels), the XLSX links on each, and downloads the XLSX files into DEV_OUTPUT_DIR
(kept as the run's artifact) so their layout can be read offline. No writes to R2.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time
import urllib.robotparser
from urllib.parse import urljoin, urlsplit

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://statystyka.policja.pl/"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
LAST = [0.0]
MAX_PAGES = 120
MAX_FILES = 80


def get(url: str) -> requests.Response:
    wait = 2.0 - (time.time() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    try:
        return requests.get(url, headers={"User-Agent": UA}, timeout=(20, 60))
    finally:
        LAST[0] = time.time()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    robots_text = get(BASE + "robots.txt").text
    print("ROBOTS", json.dumps(robots_text[:1500]))
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots_text.splitlines())

    seen, queue, pages, files = set(), [BASE], {}, {}
    while queue and len(seen) < MAX_PAGES:
        url = queue.pop(0)
        if url in seen or not rp.can_fetch(UA, url):
            continue
        seen.add(url)
        r = get(url)
        print("PAGE", r.status_code, url, flush=True)
        if r.status_code != 200 or "html" not in r.headers.get("content-type", ""):
            continue
        html = r.text
        (OUT / "pages").mkdir(exist_ok=True)
        (OUT / "pages" / (re.sub(r"[^a-z0-9]+", "_", urlsplit(url).path.lower()).strip("_") or "root")).with_suffix(
            ".html"
        ).write_text(html)
        links = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.S | re.I)
        xlsx = []
        for href, text in links:
            absolute = urljoin(url, href.strip())
            label = re.sub(r"<[^>]+>|\s+", " ", text).strip()
            if urlsplit(absolute).netloc != "statystyka.policja.pl":
                continue
            if re.search(r"\.xlsx?($|\?)", absolute, re.I):
                xlsx.append((absolute, label))
                files.setdefault(absolute, (url, label))
            elif urlsplit(absolute).path.startswith("/st/") and absolute.split("#")[0] not in seen:
                depth = urlsplit(absolute).path.strip("/").count("/")
                if depth <= 3:
                    queue.append(absolute.split("#")[0])
        pages[url] = xlsx
        if xlsx:
            print("XLSX", url, json.dumps(xlsx, ensure_ascii=False), flush=True)

    print("FILES", len(files), flush=True)
    (OUT / "files").mkdir(exist_ok=True)
    manifest = []
    for n, (url, (page, label)) in enumerate(files.items()):
        if n >= MAX_FILES or not rp.can_fetch(UA, url):
            continue
        r = get(url)
        name = urlsplit(url).path.strip("/").replace("/", "__")
        (OUT / "files" / name).write_bytes(r.content)
        row = {
            "url": url,
            "page": page,
            "label": label,
            "status": r.status_code,
            "bytes": len(r.content),
            "last_modified": r.headers.get("Last-Modified"),
            "etag": r.headers.get("ETag"),
            "content_type": r.headers.get("Content-Type"),
        }
        manifest.append(row)
        print("FILE", json.dumps(row, ensure_ascii=False), flush=True)
    (OUT / "manifest.json").write_text(json.dumps({"pages": pages, "files": manifest}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
