"""Read-only probe of the MZ / CeZ statistical bulletin page (register PLS-017, #2957). No writes anywhere.

mode page:   robots.txt, the bulletin page, its downloadFile links with their texts, script/API hints.
mode sheets: download the XLSX editions (ids given as args id=<n>) and annotate the sheet names and titles.
mode dump:   rows 1..N of sheets matching a regex in given ids (args id=<n> sheet=<regex> rows=<n>).
Deleted before merge."""

from __future__ import annotations

import re
import sys
import time

import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAGE = "https://ezdrowie.gov.pl/portal/home/badania-i-dane/biuletyn-statystyczny"
BASE = "https://ezdrowie.gov.pl"
COUNTS = {"notice": 0, "warning": 0, "error": 0}


def note(title, data):
    for level in ("notice", "warning", "error"):
        if COUNTS[level] < 10:
            COUNTS[level] += 1
            annotate(level, title, data)
            return
    print(title, data)


def get(url):
    time.sleep(1.5)
    return requests.get(url, headers={"User-Agent": UA}, timeout=60)


def chunks(items, size=3700):
    out, cur = [], ""
    for it in items:
        if len(cur) + len(it) + 3 > size:
            out.append(cur)
            cur = ""
        cur += it + " ¦ "
    if cur:
        out.append(cur)
    return out


def page():
    r = get(BASE + "/robots.txt")
    note("robots", {"status": r.status_code, "text": r.text[:1500]})
    r = get(PAGE)
    html = r.text
    links = re.findall(r'<a[^>]+href="([^"]*downloadFile/(\d+)[^"]*)"[^>]*>(.*?)</a>', html, flags=re.S)
    rows = []
    for href, fid, inner in links:
        text = re.sub(r"<[^>]+>", " ", inner)
        text = re.sub(r"\s+", " ", text).strip()
        rows.append(f"{fid}:{text[:90]}")
    hints = sorted(set(re.findall(r'["\'](/[^"\']*(?:api|json|resource|portlet|download)[^"\']*)["\']', html, flags=re.I)))[:60]
    note("page", {"status": r.status_code, "bytes": len(html), "content_type": r.headers.get("Content-Type"),
                  "download_links": len(links), "hints": hints, "title": re.findall(r"<title>(.*?)</title>", html, re.S)[:1]})
    for i, c in enumerate(chunks(rows)):
        note(f"links {i}", c)
    # context around the first downloadFile occurrence (in case links are not plain <a>)
    m = re.search(r"downloadFile", html)
    if m and not links:
        note("context", html[max(0, m.start() - 1500): m.start() + 1500])
    if not m:
        note("no downloadFile in html", {"head": html[:1800], "scripts": re.findall(r'<script[^>]+src="([^"]+)"', html)[:40]})


def sheets(ids):
    for fid in ids:
        r = get(f"{BASE}/downloadFile/{fid}" if fid.isdigit() else f"{BASE}/pobierz/{fid}")
        info = {"id": fid, "status": r.status_code, "bytes": len(r.content), "type": r.headers.get("Content-Type"),
                "disposition": r.headers.get("Content-Disposition"), "last_modified": r.headers.get("Last-Modified"),
                "magic": r.content[:4].hex()}
        if r.status_code != 200 or r.content[:2] != b"PK":
            note(f"file {fid}", info)
            continue
        titles: dict[str, list[str]] = {}
        maxrow: dict[str, int] = {}
        for c in xlsx_cells.cells(r.content):
            name = c["sheet_name"]
            maxrow[name] = max(maxrow.get(name, 0), c["row_number"])
            if c["row_number"] <= 4 and len(" ".join(titles.get(name, []))) < 110:
                titles.setdefault(name, []).append(re.sub(r"\s+", " ", c["value_text"]).strip())
        info["sheets"] = len(maxrow)
        items = [f"{n}[{maxrow[n]}]:{' '.join(t)[:110]}" for n, t in titles.items()]
        parts = chunks(items, 3500)
        note(f"file {fid}", {**info, "parts": len(parts)})
        for i, p in enumerate(parts):
            note(f"sheets {fid} {i}", p)


def dump(ids, sheet, rows):
    for fid in ids:
        r = get(f"{BASE}/downloadFile/{fid}" if fid.isdigit() else f"{BASE}/pobierz/{fid}")
        lines: dict[tuple[str, int], list[str]] = {}
        for c in xlsx_cells.cells(r.content):
            if c["row_number"] <= rows and re.search(sheet, c["sheet_name"]):
                lines.setdefault((c["sheet_name"], c["row_number"]), []).append(
                    f"{c['column_number']}:{re.sub(chr(10), ' ', c['value_text'])[:40]}")
        items = [f"{s}#{n} " + "|".join(v) for (s, n), v in sorted(lines.items(), key=lambda x: (x[0][0], x[0][1]))]
        for i, p in enumerate(chunks(items, 3800)):
            note(f"dump {fid} {i}", p)


def html(argv):
    url = next((a[4:] for a in argv if a.startswith("url=")), PAGE)
    r = get(url)
    t = r.text
    note("html meta", {"url": url, "status": r.status_code, "bytes": len(t)})
    hrefs = sorted(set(re.findall(r'href="([^"#]+)"', t)))
    note("hrefs", " ".join(h for h in hrefs if "badania" in h or "biuletyn" in h.lower() or "page" in h.lower() or "?" in h)[:3800])
    for word in ("2021", "2019", "2018", "wi\u0119cej", "Archiwum", "cur=", "delta", "pagination", "15038"):
        hits = [m.start() for m in re.finditer(word, t)]
        if hits:
            i = hits[0]
            note(f"ctx {word} ({len(hits)})", re.sub(r"\s+", " ", t[max(0, i - 900): i + 900]))


def main(argv):
    mode = argv[0] if argv else "page"
    ids = [a[3:] for a in argv if a.startswith("id=")]
    if mode == "page":
        page()
    elif mode == "sheets":
        sheets(ids)
    elif mode == "html":
        html(argv)
    elif mode == "dump":
        sheet = next((a[6:] for a in argv if a.startswith("sheet=")), ".")
        rows = int(next((a[5:] for a in argv if a.startswith("rows=")), "12"))
        dump(ids, sheet, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
