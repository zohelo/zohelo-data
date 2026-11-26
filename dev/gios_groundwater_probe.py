"""Read-only probe of mjwp.gios.gov.pl yearly result pages and their workbooks (PL-ENV-012, #2870).

No R2 access, no writes outside the runner's temp folder. One request at a time, 2 s apart.
Prints, per year page, every link that looks like a file (with its text), and per workbook its sheets:
dimensions, the first rows, the last rows and the non-numeric strings of the data area.
"""

from __future__ import annotations

from collections import Counter
from html.parser import HTMLParser
import io
import json
import re
import sys
import time
from urllib.parse import urljoin

import requests

from ingestion.common import xlsx_cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://mjwp.gios.gov.pl/wyniki-badan/wyniki-badan-{year}.html"
FILE = re.compile(r"\.(xlsx|xls|zip|csv|pdf|ods|7z|rar|docx?)(\?|$)", re.I)


def get(url: str, stream: bool = False) -> requests.Response:
    time.sleep(2)
    for attempt in range(3):
        try:
            return requests.get(url, headers={"User-Agent": UA}, timeout=(30, 120))
        except requests.RequestException as exc:
            print(json.dumps({"retry": url, "error": str(exc)[:150]}), flush=True)
            time.sleep(10 * (attempt + 1))
    raise RuntimeError(url)


class Anchors(HTMLParser):
    def __init__(self, base: str) -> None:
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


def anchors(html: str, base: str) -> list[tuple[str, str]]:
    p = Anchors(base)
    p.feed(html)
    return [(h, " ".join(t.split())) for h, t in p.out]


def grid(body: bytes, name: str) -> dict:
    sheets: dict[str, dict] = {}
    if name.endswith(".xls"):
        import xlrd

        book = xlrd.open_workbook(file_contents=body)
        for sh in book.sheets():
            rows = {}
            for r in range(sh.nrows):
                vals = {c + 1: str(sh.cell_value(r, c)) for c in range(sh.ncols) if str(sh.cell_value(r, c)) != ""}
                if vals:
                    rows[r + 1] = vals
            sheets[sh.name] = {"rows": rows, "fmt": {}}
        return sheets
    for c in xlsx_cells.cells(body):
        s = sheets.setdefault(c["sheet_name"], {"rows": {}, "fmt": Counter()})
        s["rows"].setdefault(c["row_number"], {})[c["column_number"]] = c["value_text"]
        if c["cell_type"] == "n" and c["number_format"] not in ("General", None):
            s["fmt"][c["number_format"]] += 1
    return sheets


def num(text: str) -> bool:
    try:
        float(text.replace(",", "."))
        return True
    except ValueError:
        return False


def describe(body: bytes, name: str) -> None:
    try:
        sheets = grid(body, name)
    except Exception as exc:  # noqa: BLE001 - a probe reports any decoder failure
        print(json.dumps({"decode_error": f"{type(exc).__name__}: {exc}"[:300]}), flush=True)
        return
    for sheet, s in sheets.items():
        rows = s["rows"]
        if not rows:
            print(json.dumps({"sheet": sheet, "empty": True}))
            continue
        keys = sorted(rows)
        maxcol = max(max(r) for r in rows.values())
        print(json.dumps({"sheet": sheet, "rows": len(keys), "first_row": keys[0], "last_row": keys[-1],
                          "max_col": maxcol, "number_formats": dict(Counter(s["fmt"]).most_common(6))},
                         ensure_ascii=False), flush=True)
        for r in keys[:14] + keys[-3:]:
            cells = rows[r]
            line = [str(cells.get(c, ""))[:38] for c in range(1, min(maxcol, 80) + 1)]
            while line and line[-1] == "":
                line.pop()
            print(f"  r{r}: " + " | ".join(line), flush=True)
        texts = Counter()
        cols_text = Counter()
        for r in keys[6:]:
            for c, v in rows[r].items():
                if not num(v):
                    texts[v[:30]] += 1
                    cols_text[c] += 1
        print("  non-numeric (rows 7+):", json.dumps(texts.most_common(45), ensure_ascii=False), flush=True)
        lt = Counter(re.sub(r"[0-9.,]+", "N", v)[:12] for r in keys for v in rows[r].values() if re.match(r"^\s*[<>]", v))
        print("  qualifier shapes:", json.dumps(lt.most_common(10), ensure_ascii=False), flush=True)


def main() -> int:
    years = [int(y) for y in sys.argv[1:]] or list(range(2025, 2003, -1))
    for index in ("https://mjwp.gios.gov.pl/wyniki-badan.html", "https://mjwp.gios.gov.pl/wyniki-badan/"):
        r = get(index)
        links = [(h, t) for h, t in anchors(r.text, r.url) if "wyniki-badan" in h]
        print(json.dumps({"index": index, "http": r.status_code, "final": r.url, "links": links[:60]},
                         ensure_ascii=False), flush=True)
    for year in years:
        url = BASE.format(year=year)
        r = get(url)
        links = anchors(r.text, r.url)
        files = [(h, t) for h, t in links if FILE.search(h) or "/g2/" in h or "download" in h.lower()]
        print("=" * 100)
        print(json.dumps({"year": year, "http": r.status_code, "bytes": len(r.content), "final": r.url,
                          "links_total": len(links), "file_links": files}, ensure_ascii=False), flush=True)
        if year in (2024, 2015, 2010) or not files:
            body = re.sub(r"\s+", " ", re.sub(r"<script.*?</script>|<style.*?</style>", "", r.text, flags=re.S))
            m = body.find("Wyniki")
            print("  content:", body[max(0, m - 200): m + 3500], flush=True)
        for href, text in files:
            if not re.search(r"\.(xlsx|xls|zip|csv)(\?|$)", href, re.I):
                continue
            f = get(href)
            print("-" * 80)
            print(json.dumps({"file": href, "text": text, "http": f.status_code, "bytes": len(f.content),
                              "type": f.headers.get("Content-Type"), "last_modified": f.headers.get("Last-Modified"),
                              "etag": f.headers.get("ETag"), "magic": f.content[:4].hex()}, ensure_ascii=False),
                  flush=True)
            if f.status_code == 200 and len(f.content) < 30_000_000:
                name = href.lower().split("?")[0]
                if name.endswith(".zip"):
                    import zipfile

                    with zipfile.ZipFile(io.BytesIO(f.content)) as z:
                        print("  zip members:", [(i.filename, i.file_size) for i in z.infolist()][:30], flush=True)
                        for i in z.infolist():
                            if i.filename.lower().endswith((".xlsx", ".xls")):
                                print("  member", i.filename, flush=True)
                                describe(z.read(i), i.filename.lower())
                else:
                    describe(f.content, name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
