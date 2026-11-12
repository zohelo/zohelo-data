"""Read-only route probe for the ZUS social insurance yearbook (PLS-010). No writes anywhere. Deleted before merge.

1. zus.pl yearbook page(s): every document link (pdf/xls/xlsx/zip) and its HEAD (size, type, Last-Modified).
2. psz.zus.pl home and publication-like pages: links mentioning "rocznik".
3. dane.gov.pl API: ZUS institution, its datasets mentioning "rocznik", "emeryt", "ubezpiecz", "skład", and the
   resources (format, link) of datasets whose title mentions "rocznik".
4. robots.txt of the hosts.
5. For the first XLSX resource found per matching dataset: sheet names and the first rows of the first sheet.
"""

from __future__ import annotations

from html.parser import HTMLParser
import json
import re
import sys
import time
from urllib.parse import unquote_plus, urljoin

import requests

from ingestion.common import xlsx_cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 1.5


class Anchors(HTMLParser):
    def __init__(self, base):
        super().__init__(convert_charrefs=True)
        self.base, self.out, self.open = base, [], False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.out.append([urljoin(self.base, (dict(attrs).get("href") or "").strip()), ""])
            self.open = True

    def handle_data(self, data):
        if self.open:
            self.out[-1][1] += data

    def handle_endtag(self, tag):
        if tag == "a":
            self.open = False


def get(url, **kw):
    time.sleep(PAUSE)
    for attempt in range(3):
        try:
            return requests.get(url, headers={"User-Agent": UA}, timeout=60, **kw)
        except requests.RequestException as exc:
            print("  error", type(exc).__name__, str(exc)[:200])
            time.sleep(10 * (attempt + 1))
    return None


def anchors(url):
    r = get(url)
    if r is None:
        return []
    print(f"  GET {url} status={r.status_code} final={r.url} bytes={len(r.content)}")
    p = Anchors(r.url)
    p.feed(r.text)
    return [(h, " ".join(t.split())) for h, t in p.out]


def yearbook_pages():
    print("\n##### 1. zus.pl yearbook page")
    base = "https://www.zus.pl/baza-wiedzy/statystyka/rocznik-statystyczny-ubezpieczen-spolecznych"
    pages = [base]
    seen_docs = {}
    for url in pages[:6]:
        for h, t in anchors(url):
            if "rocznik-statystyczny-ubezpieczen-spolecznych" in h and ("cur=" in h or "page" in h.lower()):
                h2 = h.split("#")[0]
                if h2 not in pages and len(pages) < 6:
                    pages.append(h2)
            if re.search(r"/documents/", h) or re.search(r"\.(pdf|xlsx?|zip)\b", h, re.I):
                seen_docs.setdefault(h.split("#")[0], t)
    print(f"  pages read: {pages}")
    for h, t in seen_docs.items():
        print(f"  DOC {unquote_plus(h)} | {t[:80]}")
    print(f"  documents: {len(seen_docs)}")
    for h in list(seen_docs)[:30]:
        time.sleep(PAUSE)
        try:
            r = requests.head(h, headers={"User-Agent": UA}, timeout=60, allow_redirects=True)
            print(f"  HEAD {r.status_code} {r.headers.get('Content-Type')} len={r.headers.get('Content-Length')} "
                  f"lm={r.headers.get('Last-Modified')} {unquote_plus(h)[-90:]}")
        except requests.RequestException as exc:
            print("  HEAD error", exc)
    print("\n  -- zus.pl statistics hub links mentioning rocznik/xls")
    for h, t in anchors("https://www.zus.pl/baza-wiedzy/statystyka"):
        if re.search(r"rocznik|xlsx?|tablic", h + " " + t, re.I):
            print(f"  L {h} | {t[:80]}")


def psz():
    print("\n##### 2. psz.zus.pl")
    for url in ("https://psz.zus.pl/", "https://psz.zus.pl/publikacje", "https://psz.zus.pl/kategorie/publikacje"):
        for h, t in anchors(url):
            if re.search(r"rocznik|publikac", h + " " + t, re.I):
                print(f"  L {h} | {t[:100]}")


def dane():
    print("\n##### 3. dane.gov.pl")
    api = "https://api.dane.gov.pl/1.4"
    r = get(f"{api}/institutions", params={"q": "Zakład Ubezpieczeń Społecznych", "per_page": 20})
    inst = []
    if r is not None:
        print("  institutions status", r.status_code)
        try:
            for d in r.json().get("data", []):
                a = d.get("attributes", {})
                print(f"  INST {d.get('id')} | {a.get('title')} | datasets={a.get('datasets_count')}")
                if "Ubezpiecze" in (a.get("title") or "") and "Rolnicz" not in (a.get("title") or ""):
                    inst.append(d.get("id"))
        except ValueError:
            print("  not json", r.text[:300])
    datasets = []
    for iid in inst[:2]:
        for page in range(1, 6):
            r = get(f"{api}/institutions/{iid}/datasets", params={"per_page": 100, "page": page})
            if r is None or r.status_code != 200:
                print("  datasets status", None if r is None else r.status_code)
                break
            data = r.json().get("data", [])
            for d in data:
                a = d.get("attributes", {})
                datasets.append((d.get("id"), a.get("title"), a.get("modified"), a.get("license_name") or a.get("license_chosen")))
            if len(data) < 100:
                break
    print(f"  ZUS datasets: {len(datasets)}")
    for i, t, m, lic in datasets:
        print(f"  DS {i} | {t} | modified={m} | licence={lic}")
    r = get(f"{api}/search", params={"q": "rocznik statystyczny ubezpieczeń społecznych", "per_page": 20})
    if r is not None and r.status_code == 200:
        for d in r.json().get("data", []):
            a = d.get("attributes", {})
            print(f"  SEARCH {d.get('type')} {d.get('id')} | {a.get('title')}")
    picks = [d for d in datasets if re.search(r"rocznik", d[1] or "", re.I)]
    print(f"  datasets with 'rocznik': {len(picks)}")
    for i, t, _, _ in picks[:5]:
        r = get(f"{api}/datasets/{i}/resources", params={"per_page": 100})
        if r is None or r.status_code != 200:
            continue
        res = r.json().get("data", [])
        print(f"  == {i} {t}: {len(res)} resources")
        first_xlsx = None
        for x in res:
            a = x.get("attributes", {})
            print(f"    RES {x.get('id')} | {a.get('title')} | fmt={a.get('format')} | {a.get('link') or a.get('download_url')}")
            if first_xlsx is None and (a.get("format") or "").lower() in ("xlsx", "xls"):
                first_xlsx = a.get("download_url") or a.get("link")
        if first_xlsx:
            sheets(first_xlsx)
    r = get(f"{api}/datasets", params={"q": "rocznik ZUS", "per_page": 20})
    if r is not None and r.status_code == 200:
        for d in r.json().get("data", []):
            a = d.get("attributes", {})
            print(f"  DSQ {d.get('id')} | {a.get('title')} | inst={a.get('institution', {}) if isinstance(a.get('institution'), str) else ''}")


def sheets(url):
    r = get(url)
    if r is None:
        return
    print(f"    XLSX status={r.status_code} bytes={len(r.content)} lm={r.headers.get('Last-Modified')}")
    if r.content[:2] != b"PK":
        print("    not xlsx", r.content[:8])
        return
    by = {}
    for c in xlsx_cells.cells(r.content):
        by.setdefault(c["sheet_name"], {}).setdefault(c["row_number"], []).append(c)
    print(f"    sheets ({len(by)}): {list(by)[:80]}")
    for name in list(by)[:2]:
        rows = by[name]
        for k in sorted(rows)[:12]:
            cells = sorted(rows[k], key=lambda c: c["column_number"])
            print(f"      {name[:20]} r{k}: " + " | ".join(f"{c['column_number']}:{c['value_text'][:50]}" for c in cells[:10]))


def robots():
    print("\n##### 4. robots.txt")
    for host in ("https://www.zus.pl", "https://psz.zus.pl", "https://dane.gov.pl", "https://api.dane.gov.pl"):
        r = get(host + "/robots.txt")
        if r is not None:
            print(f"  {host} {r.status_code}: " + " / ".join(r.text.splitlines()[:25])[:900])


def main(argv) -> int:
    for step in (yearbook_pages, psz, dane, robots):
        try:
            step()
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("  probe error", step.__name__, type(exc).__name__, exc)
    extra = [a[5:] for a in argv if a.startswith("xlsx=")]
    for u in extra:
        sheets(u)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
