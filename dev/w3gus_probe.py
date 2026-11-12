"""Read-only probe for wave-2 Polish sources: fetch pages/files, print links and file previews. No writes.

Args (space separated): `re=<regex>` link filter for following pages, `rows=<n>` preview rows,
`sheets=<n>` sheets previewed, otherwise a URL. HTML → status, headers and matching links (href | text);
XLSX → sheets and first rows; ZIP → members; JSON/CSV/text → first characters.
"""

from __future__ import annotations

from html.parser import HTMLParser
import io
import json
import re
import sys
import time
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
import zipfile

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
RNS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.cur = [], None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.cur = [dict(attrs).get("href") or "", ""]

    def handle_data(self, data):
        if self.cur is not None:
            self.cur[1] += data

    def handle_endtag(self, tag):
        if tag == "a" and self.cur is not None:
            self.out.append((self.cur[0], " ".join(self.cur[1].split())))
            self.cur = None


def col(ref):
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group(0):
        n = n * 26 + ord(ch) - 64
    return n - 1


ONLY = None


def xlsx_preview(body, rows, sheets):
    z = zipfile.ZipFile(io.BytesIO(body))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % NS["m"])))
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    target = {r.attrib["Id"]: r.attrib["Target"] for r in rels}
    names = [(s.attrib["name"], target[s.attrib[RNS]]) for s in wb.find("m:sheets", NS)]
    print("  sheets:", [n for n, _ in names])
    for name, tgt in names[:sheets]:
        if ONLY and not re.search(ONLY, name):
            continue
        path = "xl/" + tgt.lstrip("/").removeprefix("xl/")
        root = ET.fromstring(z.read(path))
        dim = root.find("m:dimension", NS)
        allrows = root.findall("m:sheetData/m:row", NS)
        print(f"  --- sheet {name!r} dim={dim.attrib.get('ref') if dim is not None else None} rows={len(allrows)}")
        for r in allrows[:rows] + ([] if len(allrows) <= rows else allrows[-3:]):
            vals = {}
            for c in r.findall("m:c", NS):
                v = c.find("m:v", NS)
                t = c.attrib.get("t")
                if t == "s" and v is not None:
                    val = shared[int(v.text)]
                elif t == "inlineStr":
                    val = "".join(x.text or "" for x in c.iter("{%s}t" % NS["m"]))
                else:
                    val = v.text if v is not None else None
                if val not in (None, ""):
                    vals[col(c.attrib["r"])] = val
            if vals:
                print(f"    r{r.attrib.get('r')}: " + " | ".join(f"{k}:{str(v)[:60]}" for k, v in sorted(vals.items())[:40]))


def fetch(url):
    for attempt in range(3):
        try:
            return requests.get(url, headers={"User-Agent": UA}, timeout=60, allow_redirects=True)
        except requests.RequestException as exc:
            print("  error", exc)
            time.sleep(10)
    return None


def main(argv):
    pattern, rows, sheets, grep, member = r".", 12, 2, None, None
    for arg in argv:
        if arg.startswith("only="):
            global ONLY
            ONLY = arg[5:] or None
            continue
        if arg.startswith("member="):
            member = arg[7:]
            continue
        if arg.startswith("grep="):
            grep = arg[5:]
            continue
        if arg.startswith("re="):
            pattern = arg[3:]
            continue
        if arg.startswith("rows="):
            rows = int(arg[5:])
            continue
        if arg.startswith("sheets="):
            sheets = int(arg[7:])
            continue
        print(f"\n===== {arg}")
        try:
            probe(arg, pattern, rows, sheets, grep, member)
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("  probe error", type(exc).__name__, exc)
        time.sleep(1)
    return 0


def probe(arg, pattern, rows, sheets, grep, member):
    if True:
        r = fetch(arg)
        if r is None:
            return
        h = {k: r.headers.get(k) for k in ("Content-Type", "Content-Length", "Last-Modified", "Content-Disposition")}
        print(f"  status={r.status_code} final={r.url} bytes={len(r.content)} {json.dumps(h, ensure_ascii=False)}")
        body, ctype = r.content, (r.headers.get("Content-Type") or "").lower()
        if body[:2] == b"PK":
            try:
                z = zipfile.ZipFile(io.BytesIO(body))
                if "xl/workbook.xml" in z.namelist():
                    xlsx_preview(body, rows, sheets)
                else:
                    for i in z.infolist()[:60]:
                        print(f"  member {i.filename} {i.file_size}")
                    print(f"  members total {len(z.infolist())}")
                    for i in z.infolist():
                        if member and re.search(member, i.filename) and i.filename.endswith(".xlsx"):
                            print(f"  >>> member preview {i.filename}")
                            xlsx_preview(z.read(i), rows, sheets)
            except Exception as exc:  # noqa: BLE001 - probe
                print("  zip error", exc)
        elif body[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
            print("  OLE2 (xls/doc) file")
        elif "html" in ctype:
            if grep:
                for m in list(re.finditer(grep, r.text))[:80]:
                    print("  grep:", r.text[max(0, m.start() - 80): m.end() + 120].replace("\n", " "))
            p = Links()
            p.feed(r.text)
            n = 0
            for href, text in p.out:
                full = urljoin(r.url, href)
                if re.search(pattern, full) or re.search(pattern, text):
                    print(f"  {full} | {text[:100]}")
                    n += 1
                    if n >= 400:
                        break
            print(f"  links shown {n} of {len(p.out)}")
        else:
            print("  head:", body[:3000].decode("utf-8", "replace"))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
