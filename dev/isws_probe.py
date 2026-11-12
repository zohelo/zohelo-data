"""Read-only probe of the ISWS multi-year workbooks (register PLG-031): lists the hub's download links with their
titles, downloads every workbook one at a time (pause 1.5 s) into memory only and prints its HTTP headers, format
and cells (rows capped). Writes nothing to R2. Deleted before merge.

args: [max rows per sheet] [comma-separated link numbers to download; default all]"""

from __future__ import annotations

from hashlib import sha256
from html.parser import HTMLParser
import sys
import time
from urllib.parse import urljoin

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HUB = "https://isws.ms.gov.pl/pl/baza-statystyczna/opracowania-wieloletnie/"


class Anchors(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.open = [], False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            a = dict(attrs)
            self.out.append([a.get("href") or "", "", a.get("title") or ""])
            self.open = True

    def handle_data(self, data):
        if self.open:
            self.out[-1][1] += data

    def handle_endtag(self, tag):
        if tag == "a":
            self.open = False


def cells(body):
    if body[:2] == b"PK":
        from ingestion.common import xlsx_cells

        for c in xlsx_cells.cells(body):
            yield c["sheet_name"], c["row_number"], c["column_number"], c["value_text"], c["cell_type"]
        return
    import xlrd

    book = xlrd.open_workbook(file_contents=body, on_demand=True)
    for name in book.sheet_names():
        sh = book.sheet_by_name(name)
        for r in range(sh.nrows):
            for c in range(sh.ncols):
                cell = sh.cell(r, c)
                if cell.ctype in (0, 6) or cell.value in ("", None):
                    continue
                v = cell.value
                if cell.ctype == 5:
                    v = xlrd.error_text_from_code.get(v, "#ERR")
                elif isinstance(v, float):
                    v = str(int(v)) if v.is_integer() else repr(v)
                yield name, r + 1, c + 1, str(v), cell.ctype


def main(argv):
    maxrows = int(argv[0]) if argv else 80
    only = set(argv[1].split(",")) if len(argv) > 1 else None
    s = requests.Session()
    s.headers["User-Agent"] = UA
    r = s.get(HUB, timeout=60)
    print("HUB", r.status_code, len(r.text), r.headers.get("Content-Type"), flush=True)
    p = Anchors()
    p.feed(r.text)
    links = [(urljoin(HUB, h), " ".join(t.split()), ti) for h, t, ti in p.out if "download" in h]
    print("LINKS", len(links))
    for i, (h, t, ti) in enumerate(links, 1):
        print(f"L{i:03d}\t{h}\t{t}\t{ti}")
    k = r.text.find("download,2853")
    print("CONTEXT", r.text[max(0, k - 1500) : k + 800].replace("\n", " ")[:2300])
    for i, (h, t, _ti) in enumerate(links, 1):
        if only and str(i) not in only:
            continue
        time.sleep(1.5)
        try:
            d = s.get(h, timeout=(30, 120))
        except requests.RequestException as exc:
            print(f"\n##### W{i:03d} ERROR {exc}")
            continue
        body = d.content
        print(
            f"\n##### W{i:03d} {t} | {d.status_code} | {len(body)} B | sha {sha256(body).hexdigest()[:12]} | "
            f"CT={d.headers.get('Content-Type')} | CD={d.headers.get('Content-Disposition')} | "
            f"LM={d.headers.get('Last-Modified')} | ET={d.headers.get('ETag')} | magic={body[:8].hex()}",
            flush=True,
        )
        try:
            rows: dict = {}
            for sheet, rn, cn, v, _ct in cells(body):
                rows.setdefault((sheet, rn), []).append(f"{cn}:{v.replace(chr(10), ' ')[:60]}")
            sheets: dict = {}
            for sheet, rn in rows:
                sheets[sheet] = max(sheets.get(sheet, 0), rn)
            print("SHEETS", sheets)
            for (sheet, rn), vals in rows.items():
                if rn <= maxrows:
                    print(f"{sheet[:20]}|{rn}| " + " | ".join(vals)[:900])
        except Exception as exc:  # noqa: BLE001
            print("DECODE ERROR", type(exc).__name__, exc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
