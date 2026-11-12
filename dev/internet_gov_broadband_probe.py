"""Read-only Dev probe of internet.gov.pl broadband reports (PL-ENV-034, #2963): find the release pages of every
edition on gov.pl (news list with paging, site search, links between release pages), list their attachments, and
read the national PDF's text layer with pypdf (plain and layout modes). One request at a time, 2 s pauses, bounded
retries. Writes nothing to R2. Deleted before merge."""

from __future__ import annotations

from datetime import date
from html.parser import HTMLParser
import io
import json
import re
import sys
import time
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://www.gov.pl/web/cyfryzacja"
KNOWN = [
    f"{BASE}/ponad-8-mln-punktow-adresowych-w-zasiegu-internetu-stacjonarnego",
    f"{BASE}/coraz-wiecej-punktow-adresowych-w-zasiegu-szybkiego-internetu-stacjonarnego",
    f"{BASE}/polska-w-zasiegu--pierwszy-raport-z-danymi-z-internetgovpl",
]
MATCH = re.compile(r"zasi[eę]g|internet|punkt\w* adresow|szerokopasmow|światłowod|gigabit", re.I)
NOTES = 0


def note(title, data, level="notice"):
    global NOTES
    text = json.dumps(data, ensure_ascii=False, default=str)
    print(f"### {title}: {text}", flush=True)
    if NOTES < 9:
        NOTES += 1
        annotate(level, title, json.loads(text) if len(text) < 3800 else {"cut": text[:3700]})


def get(url):
    for attempt in range(3):
        time.sleep(2)
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 120))
            print(f"GET {url} -> {r.status_code} {len(r.content)} bytes", flush=True)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(30 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            print(f"GET {url} error {exc}", flush=True)
            if attempt == 2:
                return None
            time.sleep(20 * (attempt + 1))
    return None


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self._href, self._text = [], None, []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href, self._text = dict(attrs).get("href"), []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.links.append((self._href, " ".join(" ".join(self._text).split())))
            self._href = None


def links(html, page):
    p = Links()
    p.feed(html)
    return [(urljoin(page, h), t) for h, t in p.links]


def dates(html):
    out = []
    for d, m, y in re.findall(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", html):
        try:
            out.append(date(int(y), int(m), int(d)))
        except ValueError:
            pass
    return out


def news_list(limit):
    found, paging = {}, set()
    for n in range(1, limit + 1):
        url = f"{BASE}/wiadomosci" + (f"?page={n}" if n > 1 else "")
        r = get(url)
        if r is None or r.status_code != 200:
            note("news list stopped", {"page": n, "status": None if r is None else r.status_code})
            break
        ls = links(r.text, r.url)
        if n == 1:
            paging = sorted({h for h, _ in ls if "page" in h})[:20]
            print("PAGING", json.dumps(paging), flush=True)
        for h, t in ls:
            if h.startswith(BASE + "/") and MATCH.search(t + " " + h):
                found.setdefault(h, t)
        ds = [d for d in dates(r.text) if d.year > 2000]
        print(f"PAGE {n} dates {min(ds) if ds else None}..{max(ds) if ds else None}", flush=True)
        if ds and max(ds) < date(2024, 6, 1):
            break
    return found, paging


def release(url):
    r = get(url)
    if r is None or r.status_code != 200:
        return {"url": url, "status": None if r is None else r.status_code}
    ls = links(r.text, r.url)
    att = [(h, t) for h, t in ls if "/attachment/" in h]
    text = re.sub(r"(?s)<[^>]+>", " ", r.text)
    stan = sorted(set(re.findall(r"stan(?:u)? na (\d{1,2}[ .]\w+[ .]\d{4})", text)))[:5]
    others = [(h, t) for h, t in ls if h.startswith(BASE + "/") and MATCH.search(t + " " + h) and h != url]
    return {"url": url, "status": r.status_code, "published": [str(d) for d in sorted(set(dates(r.text)))[-3:]],
            "stan_na": stan, "attachments": att, "related": others[:15], "title": re.findall(r"<title>(.*?)</title>", r.text, re.S)[:1]}


def pdf_text(url):
    from pypdf import PdfReader

    r = get(url)
    if r is None or r.status_code != 200:
        return None
    body = r.content
    reader = PdfReader(io.BytesIO(body))
    print(f"PDF {url} bytes={len(body)} pages={len(reader.pages)} head={body[:8]!r} meta={reader.metadata}", flush=True)
    for i, page in enumerate(reader.pages):
        print(f"=== PAGE {i + 1} PLAIN ===", flush=True)
        print(page.extract_text(), flush=True)
        print(f"=== PAGE {i + 1} LAYOUT ===", flush=True)
        try:
            print(page.extract_text(extraction_mode="layout"), flush=True)
        except Exception as exc:  # noqa: BLE001 - probe: report any extractor failure
            print(f"layout failed: {exc!r}", flush=True)
    return {"bytes": len(body), "pages": len(reader.pages), "last_modified": r.headers.get("Last-Modified")}


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 150
    found, paging = news_list(limit)
    note("news list matches", {"count": len(found), "paging": paging[:8], "items": list(found.items())[:40]})
    print("ALL MATCHES", json.dumps(found, ensure_ascii=False), flush=True)
    for q in ("Polska w zasięgu", "zasięgu stacjonarnego dostępu do internetu"):
        for url in (f"https://www.gov.pl/web/cyfryzacja/szukaj?query={q}", f"https://www.gov.pl/szukaj?query={q}"):
            r = get(url)
            if r is not None and r.status_code == 200:
                hits = [(h, t) for h, t in links(r.text, r.url) if MATCH.search(t + " " + h) and "/web/" in h]
                print("SEARCH", url, json.dumps(hits[:30], ensure_ascii=False), flush=True)
    candidates = list(dict.fromkeys(KNOWN + [h for h in found if re.search(r"zasi[eę]g", h + found[h], re.I)]))
    pages, seen = [], set()
    while candidates and len(seen) < 40:
        url = candidates.pop(0)
        if url in seen:
            continue
        seen.add(url)
        info = release(url)
        pages.append(info)
        print("RELEASE", json.dumps(info, ensure_ascii=False), flush=True)
        for h, t in info.get("related", []):
            if re.search(r"zasi[eę]g", h + t, re.I) and h not in seen:
                candidates.append(h)
    reports = [p for p in pages if len(p.get("attachments", [])) >= 10]
    note("release pages with attachments", [{"url": p["url"], "published": p["published"], "stan_na": p["stan_na"],
                                              "n": len(p["attachments"]),
                                              "national": [a for a in p["attachments"] if "olsk" in a[1]][:2]}
                                             for p in reports])
    for p in reports:
        national = [a for a in p["attachments"] if re.search(r"Polska", a[1])]
        if national:
            note("national pdf", {"page": p["url"], "file": national[0], **(pdf_text(national[0][0]) or {})})


if __name__ == "__main__":
    main()
