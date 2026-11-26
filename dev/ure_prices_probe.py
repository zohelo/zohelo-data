"""Read-only Dev probe for ure_prices (#2870): robots.txt, the indicator list pages and the indicator pages.

One request at a time, 4 s apart, the platform's User-Agent; nothing is written to R2. Reports per page: status,
bytes, SHA-256 (each indicator page fetched twice to see whether the bytes are stable), 'Data modyfikacji', and each
<table>'s header cells, row count and first/last rows; plus the links of the list pages and any terms text. The
exact page bytes are printed to the job log (base64) for offline parser work. Deleted before merge.
"""

from __future__ import annotations

import base64
from hashlib import sha256
from html.parser import HTMLParser
import re
import time
from urllib import robotparser
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

BASE = "https://www.ure.gov.pl"
LISTS = [f"{BASE}/pl/energia-elektryczna/ceny-wskazniki", f"{BASE}/pl/paliwa-gazowe/ceny-wskazniki"]
PAUSE = 4.0


class Tables(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self.links: list[tuple[str, str]] = []
        self.depth = 0
        self.row: list[str] | None = None
        self.cell: list[str] | None = None
        self.href: str | None = None
        self.text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.depth += 1
            self.tables.append([])
        elif tag == "tr" and self.depth:
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.cell = []
        elif tag == "a":
            self.href = dict(attrs).get("href")
            self.text = []

    def handle_endtag(self, tag):
        if tag == "table" and self.depth:
            self.depth -= 1
        elif tag == "tr" and self.row is not None:
            self.tables[-1].append(self.row)
            self.row = None
        elif tag in ("td", "th") and self.cell is not None and self.row is not None:
            self.row.append(re.sub(r"\s+", " ", "".join(self.cell)).strip())
            self.cell = None
        elif tag == "a" and self.href:
            self.links.append((self.href, re.sub(r"\s+", " ", "".join(self.text)).strip()))
            self.href = None

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)
        if self.href is not None:
            self.text.append(data)


def get(session: requests.Session, url: str) -> requests.Response:
    time.sleep(PAUSE)
    return session.get(url, timeout=(30, 60), allow_redirects=True)


def dump(name: str, body: bytes) -> None:
    print(f"=====BEGIN {name}=====")
    print(base64.b64encode(body).decode())
    print(f"=====END {name}=====", flush=True)


def page_text(html: str) -> str:
    text = re.sub(r"(?s)<(script|style)[^>]*>.*?</\1>", " ", html)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))


def main() -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    robots = session.get(f"{BASE}/robots.txt", timeout=(30, 60))
    rp = robotparser.RobotFileParser()
    rp.parse(robots.text.splitlines())
    annotate("notice", "ure probe · robots.txt", {"status": robots.status_code, "text": robots.text[:1500]})
    pages: dict[str, str] = {}
    list_texts: list[str] = []
    terms_links: list[tuple[str, str]] = []
    for url in LISTS:
        if not rp.can_fetch(USER_AGENT, url):
            annotate("warning", "ure probe · robots disallows", url)
            continue
        r = get(session, url)
        list_texts.append(r.text)
        p = Tables()
        p.feed(r.text)
        found = [(urljoin(r.url, h), t) for h, t in p.links if re.search(r"/ceny-wskazniki/\d+", h)]
        terms_links += [(urljoin(r.url, h), t) for h, t in p.links
                        if re.search(r"regulamin|prawa|copyright|licenc|wolnytekst|warunki|polityka|dostepnosc", h + t, re.I)]
        for link, title in found:
            pages.setdefault(re.search(r"/ceny-wskazniki/(\d+)", link).group(1), link)
        annotate("notice", f"ure probe · list {url.rsplit('/', 2)[-2]}", {
            "status": r.status_code, "bytes": len(r.content), "final_url": r.url,
            "indicator_links": [[re.search(r"/(\d+),", u + ",").group(1) if re.search(r"/(\d+),", u) else u, t[:90]] for u, t in found][:30]})
        dump(f"list_{url.rsplit('/', 2)[-2]}", r.content)
    stability, report = {}, {}
    for pid in ["7852", "7853", "7859"] + [k for k in pages if k not in ("7852", "7853", "7859")]:
        url = pages.get(pid)
        if not url:
            report[pid] = "no link on the list pages"
            continue
        if not rp.can_fetch(USER_AGENT, url):
            report[pid] = "robots disallows"
            continue
        r1 = get(session, url)
        mod = re.search(r"Data modyfikacji[^0-9]{0,80}(\d{2}\.\d{2}\.\d{4}[^<]{0,12})", r1.text)
        pub = re.search(r"Data (?:publikacji|utworzenia)[^0-9]{0,80}(\d{2}\.\d{2}\.\d{4}[^<]{0,12})", r1.text)
        p = Tables()
        p.feed(r1.text)
        tables = [{"rows": len(t), "cols": sorted({len(x) for x in t}), "head": t[:3], "tail": t[-2:]} for t in p.tables]
        attach = [(urljoin(r1.url, h), t[:60]) for h, t in p.links if re.search(r"\.(xlsx?|csv|pdf|zip)\b|download|plik", h, re.I)]
        entry = {"url": url, "status": r1.status_code, "bytes": len(r1.content), "sha": sha256(r1.content).hexdigest()[:16],
                 "ctype": r1.headers.get("Content-Type"), "last_modified": r1.headers.get("Last-Modified"),
                 "etag": r1.headers.get("ETag"), "modified": mod.group(1) if mod else None,
                 "published": pub.group(1) if pub else None, "tables": tables, "attachments": attach[:8]}
        if pid in ("7852", "7853", "7859"):
            dump(f"page_{pid}", r1.content)
            r2 = get(session, url)
            stability[pid] = {"same_bytes": r1.content == r2.content, "sizes": [len(r1.content), len(r2.content)]}
            if r1.content != r2.content:
                a, b = r1.content, r2.content
                i = next((k for k in range(min(len(a), len(b))) if a[k] != b[k]), min(len(a), len(b)))
                stability[pid]["first_diff"] = [a[max(0, i - 80): i + 80].decode("utf-8", "replace"),
                                                b[max(0, i - 80): i + 80].decode("utf-8", "replace")]
            annotate("notice", f"ure probe · page {pid}", entry)
        else:
            report[pid] = {k: entry[k] for k in ("url", "status", "bytes", "modified")} | {
                "tables": [{"rows": t["rows"], "cols": t["cols"], "head": t["head"][:2]} for t in tables]}
    annotate("notice", "ure probe · byte stability (two GETs)", stability)
    annotate("notice", "ure probe · other indicator pages", report)
    terms = {}
    text = page_text(" ".join(list_texts))
    terms["list_page_hits"] = [text[max(0, m.start() - 120): m.start() + 200] for m in
                               re.finditer(r"licenc|prawa autorsk|Creative Commons|ponowne wykorzyst|wykorzystywani|copyright", text, re.I)][:5]
    seen = set()
    for link, title in terms_links:
        if link in seen or not link.startswith(BASE) or len(seen) >= 4 or not rp.can_fetch(USER_AGENT, link):
            continue
        seen.add(link)
        r = get(session, link)
        t = page_text(r.text)
        terms[link] = {"title": title[:80], "status": r.status_code, "hits": [t[max(0, m.start() - 150): m.start() + 300] for m in
                       re.finditer(r"licenc|prawa autorsk|Creative Commons|ponowne wykorzyst|wykorzystywani|copyright", t, re.I)][:4]}
    terms["links_seen"] = sorted({l for l, _ in terms_links})[:20]
    annotate("notice", "ure probe · terms", terms)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
