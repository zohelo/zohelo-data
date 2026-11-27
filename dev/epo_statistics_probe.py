"""Read-only probe of the EPO statistics workbooks (register EX-004, #3320). Writes nothing to R2.

Reads robots.txt of www.epo.org and link.epo.org, the data-download page, lists every XLSX link with its text,
downloads each workbook once (one request at a time, 2 s apart) and prints its headers and every non-empty cell
(zlib + base64 JSON) to the job log; one compact annotation with the counts. Deleted before merge.
"""

from __future__ import annotations

import base64
from hashlib import sha256
from html.parser import HTMLParser
import json
import re
import sys
import time
from urllib.parse import urljoin

import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

PAGE = "https://www.epo.org/en/about-us/statistics/data-download"


class Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[list[str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href = dict(attrs).get("href")
            self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.links.append([self._href, " ".join("".join(self._text).split())])
            self._href = None


def get(session: requests.Session, url: str) -> requests.Response:
    for attempt in range(3):
        try:
            r = session.get(url, timeout=60)
            if r.status_code < 500 and r.status_code not in (408, 429):
                return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
        time.sleep(5 * (attempt + 1))
    return session.get(url, timeout=60)


def main(argv: list[str]) -> int:
    del argv
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    out: dict = {"robots": {}}
    for host in ("https://www.epo.org/robots.txt", "https://link.epo.org/robots.txt"):
        r = get(s, host)
        out["robots"][host] = r.status_code
        print("ROBOTS", host, r.status_code, r.text[:3000], flush=True)
        time.sleep(2)
    r = get(s, PAGE)
    out["page"] = [r.status_code, len(r.text)]
    p = Links()
    p.feed(r.text)
    files = []
    for href, text in p.links:
        if href and re.search(r"\.xlsx?(\?|$)", href, re.I):
            files.append([urljoin(PAGE, href), text])
    # headings around the links (section titles) for the page structure
    print("PAGEHTML", base64.b64encode(__import__("zlib").compress(r.text.encode(), 9)).decode(), flush=True)
    print("LINKS", json.dumps(files, ensure_ascii=False), flush=True)
    out["xlsx_links"] = len(files)
    summary = []
    for url, text in files:
        time.sleep(2)
        r = get(s, url)
        body = r.content
        info = {"u": url.rsplit("/", 1)[-1][:90], "st": r.status_code, "len": len(body),
                "lm": r.headers.get("Last-Modified"), "etag": r.headers.get("ETag"),
                "ct": r.headers.get("Content-Type")}
        if r.status_code == 200:
            try:
                cells = list(xlsx_cells.cells(body))
                sheets = sorted({c["sheet_name"] for c in cells})
                info.update({"cells": len(cells), "sheets": sheets[:12], "sha": sha256(body).hexdigest()[:12]})
                enc = base64.b64encode(__import__("zlib").compress(json.dumps(
                    {"url": url, "text": text, "headers": dict(r.headers), "cells": cells}, ensure_ascii=False).encode(), 9)).decode()
                print("CELLS", enc, flush=True)
            except Exception as exc:  # noqa: BLE001
                info["error"] = f"{type(exc).__name__}: {exc}"[:200]
        print("FILE", json.dumps(info, ensure_ascii=False), flush=True)
        summary.append(info)
    out["status"] = sorted({i["st"] for i in summary})
    out["bytes"] = sum(i["len"] for i in summary)
    out["cells"] = sum(i.get("cells", 0) for i in summary)
    out["errors"] = [i for i in summary if i.get("error") or i["st"] != 200][:5]
    out["lm_seen"] = sorted({i["lm"] for i in summary if i["lm"]})[:5]
    annotate("notice", "epo_statistics probe", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
