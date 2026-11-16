"""Read-only probe for uprp_reports (EX-001, #559): the annual-report page's PDF links with their text, then the
Polish reports for 2019-2025 one at a time (5 s apart): size, Last-Modified, page count and the text layer of every
page (pypdf, plain), printed to the job log. Nothing is written to R2. Deleted before merge."""

from __future__ import annotations

from html.parser import HTMLParser
import io
import json
import re
import sys
import time
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

PAGE = "https://uprp.gov.pl/pl/publikacje/raport-roczny-uprp"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


class Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[list[str]] = []
        self.cur: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.cur = [dict(attrs).get("href") or "", ""]

    def handle_data(self, data):
        if self.cur is not None:
            self.cur[1] += data

    def handle_endtag(self, tag):
        if tag == "a" and self.cur is not None:
            self.out.append([self.cur[0], " ".join(self.cur[1].split())])
            self.cur = None


def get(url: str, **kw) -> requests.Response:
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 300), **kw)
            if r.status_code in (403, 401, 404):
                return r
            if r.status_code == 200:
                return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
        time.sleep(20 * (attempt + 1))
    raise RuntimeError(f"failed: {url}")


def main(argv) -> int:
    from pypdf import PdfReader

    robots = get("https://uprp.gov.pl/robots.txt")
    print("ROBOTS", robots.status_code, robots.text[:3000], flush=True)
    page = get(PAGE)
    print("PAGE", page.status_code, len(page.content), page.headers.get("Content-Type"), flush=True)
    if page.status_code != 200:
        annotate("error", "uprp probe: page refused", {"status": page.status_code, "body": page.text[:500]})
        return 1
    p = Links()
    p.feed(page.text)
    pdfs = [[urljoin(page.url, h), t] for h, t in p.out if ".pdf" in h.lower()]
    for u, t in pdfs:
        print("LINK", json.dumps([u, t], ensure_ascii=False), flush=True)
    # context around each pdf link (headings, language labels)
    for m in re.finditer(r'href="([^"]+\.pdf)"', page.text, flags=re.I):
        s = page.text[max(0, m.start() - 400): m.end() + 200]
        print("CTX", json.dumps(re.sub(r"\s+", " ", s), ensure_ascii=False), flush=True)
    summary = {"links": len(pdfs)}
    picked = []
    for u, t in pdfs:
        text = (u + " " + t).lower()
        years = [int(y) for y in re.findall(r"20[12]\d", text) if 2019 <= int(y) <= 2025]
        english = any(w in text for w in ("annual report", "annual%20report", "_en", "-en", "english", "eng"))
        if years and not english:
            picked.append((years[0], u, t))
    print("PICKED", json.dumps(picked, ensure_ascii=False), flush=True)
    files = []
    for n, (year, u, t) in enumerate(picked):
        if n:
            time.sleep(5)
        r = get(u)
        info = {"year": year, "url": u, "status": r.status_code, "bytes": len(r.content),
                "last_modified": r.headers.get("Last-Modified"), "type": r.headers.get("Content-Type")}
        if r.status_code != 200:
            files.append(info)
            continue
        reader = PdfReader(io.BytesIO(r.content))
        info["pages"] = len(reader.pages)
        info["meta"] = {k: str(v)[:80] for k, v in (reader.metadata or {}).items()}
        files.append(info)
        print("FILE", json.dumps(info, ensure_ascii=False), flush=True)
        for i, pg in enumerate(reader.pages, start=1):
            try:
                body = pg.extract_text() or ""
            except Exception as exc:  # noqa: BLE001
                body = f"<extract failed {exc}>"
            print(f"===PAGE {year} {i}===", flush=True)
            print(body, flush=True)
    summary["files"] = files
    annotate("notice", "uprp probe", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
