"""Read-only probe of psz.zus.pl (PLS-009). No writes anywhere.

Args (space separated):
  cats            the home page's /kategorie/ links, then each category page's /kategorie/ links
  links=<url>     every XLSX/XLS link on a page: decoded file name | link text
  xlsx=<url>      sheets and rows of one workbook (rows=<n> first rows, all=1 every row of the first sheet)
  rows=<n>        preview rows (default 40)
"""

from __future__ import annotations

from html.parser import HTMLParser
import io
import re
import sys
import time
from urllib.parse import unquote_plus, urljoin

import requests

from ingestion.common import xlsx_cells

GREP = None
WIDTH = 140
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


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


def get(url):
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
            return r
        except requests.RequestException as exc:
            print("  error", exc)
            time.sleep(10 * (attempt + 1))
    return None


def anchors(url):
    r = get(url)
    if r is None:
        return []
    print(f"  status={r.status_code} final={r.url} bytes={len(r.content)}")
    p = Anchors(r.url)
    p.feed(r.text)
    return [(h, " ".join(t.split())) for h, t in p.out]


def cats():
    home = anchors("https://psz.zus.pl/")
    tops = sorted({h.split("?")[0] for h, _ in home if "/kategorie/" in h})
    print("TOP", len(tops))
    for h in tops:
        print("  ", h)
    for top in tops:
        time.sleep(1.5)
        print(f"\n== {top}")
        subs = sorted({h.split("?")[0] for h, _ in anchors(top) if "/kategorie/" in h})
        for s in subs:
            print("   sub", s)


def links(url):
    seen = set()
    n = 0
    for h, t in anchors(url):
        if not re.search(r"\.xlsx?(/|$|\?)", h, re.I) or h in seen:
            continue
        seen.add(h)
        n += 1
        m = re.search(r"/documents/(\d+)/(\d+)/([^/]+)", h)
        name = unquote_plus(m.group(3)) if m else h
        if GREP and not re.search(GREP, name + " " + t, re.I):
            continue
        print(f"  L {m.group(2) if m else '-'} | {name} | {t[:90]} | {h}")
    print(f"  xlsx links: {n}")
    # also year/tab structure hints
    for h, t in anchors(url) if False else []:
        pass


def xlsx(url, rows, full):
    r = get(url)
    if r is None:
        return
    print(f"  status={r.status_code} bytes={len(r.content)} lm={r.headers.get('Last-Modified')} ct={r.headers.get('Content-Type')}")
    body = r.content
    if body[:2] != b"PK":
        print("  not xlsx; magic", body[:8])
        return
    by_sheet = {}
    for c in xlsx_cells.cells(body):
        by_sheet.setdefault((0, c["sheet_name"]), {}).setdefault(c["row_number"], []).append(c)
    for (i, name), rws in by_sheet.items():
        print(f"  --- sheet {i} {name!r} rows={len(rws)} max_row={max(rws)}")
        keys = sorted(rws)
        show = keys if full else keys[:rows] + (keys[-4:] if len(keys) > rows else [])
        for k in show:
            cells = sorted(rws[k], key=lambda c: c["column_number"])
            print(f"    r{k}: " + " | ".join(f"{c['column_number']}:{c['value_text'][:WIDTH].replace(chr(10), ' ')}" for c in cells[:18]))


def main(argv):
    rows, full = 40, False
    for a in argv:
        if a.startswith("rows="):
            rows = int(a[5:])
        elif a.startswith("grep="):
            global GREP
            GREP = a[5:]
        elif a == "all=1":
            full = True
    for a in argv:
        try:
            if a == "cats":
                cats()
            elif a.startswith("links="):
                print(f"\n===== links {a[6:]}")
                links(a[6:])
            elif a.startswith("text="):
                print(f"\n===== text {a[5:]}")
                r = get(a[5:])
                if r is not None:
                    body = re.sub(r"(?is)<(script|style).*?</\\1>", " ", r.text)
                    body = re.sub(r"<[^>]+>", " ", body)
                    import html as _h
                    words = " ".join(_h.unescape(body).split())
                    print(f"  status={r.status_code} chars={len(words)}")
                    for m in re.finditer(GREP or ".", words, re.I):
                        print("  ..." + words[max(0, m.start() - 300): m.end() + 500] + "...")
                        if not GREP:
                            break
            elif a.startswith("xlsx="):
                print(f"\n===== xlsx {a[5:]}")
                xlsx(a[5:], rows, full)
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("  probe error", type(exc).__name__, exc)
        time.sleep(1.5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
