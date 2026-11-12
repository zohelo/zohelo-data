"""GIOŚ groundwater (PL-ENV-012) read-only probe, phase 1 of the trial (#2870).

GET only, one request at a time with pauses, robots.txt honoured. No R2 writes. Lists the yearly result pages
of mjwp.gios.gov.pl, every file link on each page (href + link text), downloads each XLSX once and prints the
sheet names, sizes, the first rows of every sheet and per-column non-numeric value strings (job log), with a
compact summary per year as annotations.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from html.parser import HTMLParser
import json
import re
import time
from urllib.parse import urljoin
import urllib.robotparser
import zipfile

import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://mjwp.gios.gov.pl/"
START = BASE + "wyniki-badan/wyniki-badan-2025.html"
PAUSE = 2.0
NUM = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")


class Anchors(HTMLParser):
    def __init__(self, url):
        super().__init__(convert_charrefs=True)
        self.url, self.out, self.open = url, [], False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.out.append([urljoin(self.url, (dict(attrs).get("href") or "").strip()), ""])
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
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 120), **kw)
            return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
            time.sleep(20 * (attempt + 1))
    raise RuntimeError(f"unreachable {url}")


def anchors(url):
    r = get(url)
    p = Anchors(r.url)
    p.feed(r.text)
    return r, [(h, " ".join(t.split())) for h, t in p.out], r.text


def profile(body, label):
    by_sheet = defaultdict(list)
    for c in xlsx_cells.cells(body):
        by_sheet[c["sheet_name"]].append(c)
    out = []
    for name, cs in by_sheet.items():
        rows = defaultdict(dict)
        for c in cs:
            rows[c["row_number"]][c["column_number"]] = c["value_text"]
        maxr, maxc = max(rows), max(c["column_number"] for c in cs)
        print(f"\n=== {label} | sheet {name!r} | rows {len(rows)} max_row {maxr} max_col {maxc} cells {len(cs)}")
        for rn in sorted(rows)[:14]:
            vals = rows[rn]
            line = " | ".join(f"{k}:{str(vals[k])[:60]}" for k in sorted(vals))
            print(f"  r{rn}: {line[:1500]}")
        print("  ... last rows:")
        for rn in sorted(rows)[-3:]:
            vals = rows[rn]
            print(f"  r{rn}: " + " | ".join(f"{k}:{str(vals[k])[:40]}" for k in sorted(vals))[:800])
        nonnum = defaultdict(Counter)
        fmts = Counter()
        for c in cs:
            if c["row_number"] > 8:
                v = c["value_text"].strip()
                if not NUM.match(v):
                    nonnum[c["column_number"]][v[:30]] += 1
                fmts[(c["cell_type"], c["number_format"])] += 1
        print("  formats:", dict(fmts.most_common(8)))
        for col in sorted(nonnum):
            cnt = nonnum[col]
            if len(cnt) <= 40:
                print(f"  col {col} non-numeric: {dict(cnt.most_common(25))}")
            else:
                print(f"  col {col} non-numeric: {len(cnt)} distinct, top {dict(cnt.most_common(8))}")
        out.append({"sheet": name, "rows": len(rows), "max_col": maxc, "cells": len(cs)})
    return out


def main():
    rp = urllib.robotparser.RobotFileParser()
    rr = get(BASE + "robots.txt")
    print("robots", rr.status_code, rr.text[:500])
    rp.parse(rr.text.splitlines() if rr.status_code == 200 and "<html" not in rr.text[:300].lower() else [])
    r, links, html = anchors(START)
    print("start", r.status_code, r.url, len(html))
    pages = sorted({h for h, _ in links if re.search(r"/wyniki-badan/wyniki-badan-[0-9-]+\.html$", h)})
    for h, t in links:
        if "wyniki" in h:
            print("nav", h, "|", t)
    pages = sorted(set(pages) | {START})
    print("pages", len(pages), pages)
    summary = {}
    for page in pages:
        if not rp.can_fetch(UA, page):
            summary[page] = "robots disallow"
            continue
        r, links, html = anchors(page)
        year = page.rsplit("wyniki-badan-", 1)[-1].removesuffix(".html")
        ext = r"\.(xlsx?|zip|csv|pdf|jpe?g|png|docx?)$"
        files = [(h, t) for h, t in links if re.search(ext, h, re.I) or "/g2/" in h]
        print(f"\n##### page {year} status {r.status_code} bytes {len(html)} links {len(links)} files {len(files)}")
        for h, t in files:
            print(f"  file {h} | {t}")
        if not files:  # how are results shown? print the visible headings and tables count
            print("  tables:", html.count("<table"), "iframes:", html.count("<iframe"))
            body = re.sub(r"<script.*?</script>", "", html, flags=re.S)
            text = re.sub(r"<[^>]+>", " ", body)
            print("  text:", " ".join(text.split())[:1500])
        ys = {"status": r.status_code, "files": []}
        for h, t in files:
            if not re.search(r"\.xlsx?$", h, re.I):
                ys["files"].append({"t": t[:60], "ext": h.rsplit(".", 1)[-1][:5]})
                continue
            f = get(h)
            info = {
                "t": t[:80],
                "u": h.rsplit("/", 2)[-2] + "/" + h.rsplit("/", 1)[-1][:12],
                "st": f.status_code,
                "len": len(f.content),
                "lm": f.headers.get("Last-Modified"),
            }
            if f.status_code == 200 and h.lower().endswith(".xlsx"):
                try:
                    info["sheets"] = profile(f.content, f"{year} {t[:60]}")
                except (zipfile.BadZipFile, KeyError, ValueError) as exc:
                    info["err"] = str(exc)[:100]
            ys["files"].append(info)
        summary[year] = ys
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    items = list(summary.items())
    # annotations: compact per-year file list, at most 10 per level
    for level, i in enumerate(range(0, len(items), 3)):
        part = dict(items[i : i + 3])
        annotate("notice" if level % 2 == 0 else "warning", f"gios_groundwater probe {i // 3}", part)


if __name__ == "__main__":
    main()
