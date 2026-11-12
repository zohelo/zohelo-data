"""Dev probe for #2970 (KG PSP interventions; test branch only, never on main). Read-only, no R2 writes.

Reads https://www.gov.pl/web/kgpsp/interwencje-psp, lists every link with its text, downloads the standard-tables
ZIP of the newest and the oldest year and one incident-report ZIP, and prints the members, the sheet names and the
first rows of each sheet (standard tables) or only the column headers and row counts (incident data: no record
content is printed). One request at a time, pauses between them.
"""

from __future__ import annotations

from html.parser import HTMLParser
import io
import json
import re
import sys
import time
from urllib.parse import urljoin
import zipfile

import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

PAGE = "https://www.gov.pl/web/kgpsp/interwencje-psp"


class Anchors(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[list[str]] = []
        self.depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.out.append([urljoin(PAGE, (dict(attrs).get("href") or "").strip()), ""])
            self.depth = 1

    def handle_data(self, data):
        if self.depth:
            self.out[-1][1] += data

    def handle_endtag(self, tag):
        if tag == "a":
            self.depth = 0


def get(url: str) -> requests.Response:
    time.sleep(2)
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 300))
    print(f"GET {url} -> {r.status_code} {len(r.content)} bytes {r.headers.get('Content-Type')} "
          f"LM={r.headers.get('Last-Modified')} CD={r.headers.get('Content-Disposition')}", flush=True)
    return r


def show_xlsx(name: str, body: bytes, max_rows: int = 14) -> dict:
    out = {}
    by_sheet: dict[str, dict[int, list]] = {}
    for c in xlsx_cells.cells(body):
        by_sheet.setdefault(c["sheet_name"], {}).setdefault(c["row_number"], []).append(
            (c["column_number"], c["value_text"])
        )
    for sheet, rows in by_sheet.items():
        out[sheet] = {"rows": len(rows), "max_row": max(rows), "max_col": max(c for r in rows.values() for c, _ in r)}
        print(f"  --- {name} :: sheet {sheet!r} rows={len(rows)} max_row={max(rows)}", flush=True)
        for rn in sorted(rows)[:max_rows]:
            print(f"    r{rn}: " + " | ".join(f"c{c}={v[:60]!r}" for c, v in sorted(rows[rn])[:14]), flush=True)
        # label column values (first column) of all rows, to find voivodeships
        firsts = [sorted(rows[rn])[0][1][:40] for rn in sorted(rows)]
        print(f"    first-col labels: {firsts[:60]}", flush=True)
    return out


def standard(url: str, label: str) -> dict:
    r = get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    members = [(i.filename, i.file_size) for i in z.infolist()]
    print(f"== standard tables {label}: {len(members)} members", flush=True)
    for m in members:
        print(f"  member {m}", flush=True)
    summary = {"bytes": len(r.content), "members": len(members), "names": [m[0] for m in members][:40], "sheets": {}}
    for name, _ in members:
        low = name.lower()
        data = z.read(name)
        if low.endswith(".xlsx"):
            try:
                summary["sheets"][name] = show_xlsx(name, data)
            except Exception as exc:  # noqa: BLE001
                print(f"  xlsx error {name}: {exc}", flush=True)
        elif low.endswith(".zip"):
            inner = zipfile.ZipFile(io.BytesIO(data))
            print(f"  inner zip {name}: {[(i.filename, i.file_size) for i in inner.infolist()][:50]}", flush=True)
            for i in inner.infolist()[:3]:
                if i.filename.lower().endswith(".xlsx"):
                    show_xlsx(i.filename, inner.read(i.filename))
        else:
            print(f"  other member {name}: first bytes {data[:16]!r}", flush=True)
    return summary


def incident(url: str, label: str) -> dict:
    r = get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    members = [(i.filename, i.file_size) for i in z.infolist()]
    print(f"== incident data {label}: {members}", flush=True)
    out = {"bytes": len(r.content), "members": members[:20], "headers": {}}
    for name, size in members:
        data = z.read(name)
        low = name.lower()
        if low.endswith((".csv", ".txt")):
            for enc in ("utf-8-sig", "cp1250"):
                try:
                    text = data.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            lines = text.splitlines()
            out["headers"][name] = {"encoding": enc, "lines": len(lines), "header": lines[0][:1500]}
            print(f"  {name}: {len(lines)} lines enc={enc}\n  header: {lines[0][:3000]}", flush=True)
        elif low.endswith(".xlsx"):
            hdr = {}
            for c in xlsx_cells.cells(data):
                if c["row_number"] <= 3:
                    hdr.setdefault(c["sheet_name"], []).append((c["row_number"], c["column_number"], c["value_text"][:50]))
                elif c["row_number"] > 3 and len(hdr) > 3:
                    break
            out["headers"][name] = {s: v[:80] for s, v in hdr.items()}
            print(f"  {name}: header cells {json.dumps(hdr, ensure_ascii=False)[:4000]}", flush=True)
        else:
            print(f"  other member {name} size={size} first bytes {data[:16]!r}", flush=True)
    return out


def main() -> int:
    r = get(PAGE)
    p = Anchors()
    p.feed(r.text)
    links = [(h, " ".join(t.split())) for h, t in p.out]
    att = [(h, t) for h, t in links if "/attachment/" in h]
    print(f"page links {len(links)}, attachment links {len(att)}", flush=True)
    for h, t in links:
        if "/attachment/" in h or re.search(r"(?i)zestawie|tabel|interwenc|ofiar", t):
            print(f"  LINK {t!r} -> {h}", flush=True)
    # headings near links: print page text around 'zestawie'
    text = re.sub(r"<[^>]+>", " ", r.text)
    text = " ".join(text.split())
    for m in re.finditer(r"(?i)licencj|tabele stałych|informacji ze zdarze", text):
        print(f"  CTX ...{text[max(0, m.start() - 200): m.end() + 300]}...", flush=True)
        break
    annotate("notice", "kgpsp probe page", {"http": r.status_code, "links": len(links),
                                            "attachments": [[t[:90], h[-40:]] for h, t in att][:24]})
    tables = [(h, t) for h, t in att if re.search(r"(?i)tabel", t)]
    events = [(h, t) for h, t in att if re.search(r"(?i)informacji ze zdarze|zdarzeń", t)]
    res = {}
    if tables:
        res["newest"] = standard(tables[0][0], tables[0][1])
        res["oldest"] = standard(tables[-1][0], tables[-1][1])
    if events and (len(sys.argv) < 2 or sys.argv[1] != "noevents"):
        res["incident"] = incident(events[0][0], events[0][1])
    annotate("notice", "kgpsp probe zips", json.loads(json.dumps(res, default=str))
             if len(json.dumps(res, default=str)) < 3800 else {"names": {k: v.get("names") or v.get("members") for k, v in res.items()}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
