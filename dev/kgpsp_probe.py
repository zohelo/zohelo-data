"""Dev probe for #2970 (KG PSP interventions; test branch only, never on main). Read-only, no R2 writes.

Reads https://www.gov.pl/web/kgpsp/interwencje-psp, lists the attachment links with their text, downloads the
standard-tables ZIP of the newest and the oldest year and prints the members, file types, sheet names and the first
rows of each sheet. Downloads one incident-report ZIP and prints only member names, sizes and column headers (no
record content). One request at a time, pauses between them.
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
    time.sleep(3)
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 300))
    print(f"GET {url} -> {r.status_code} {len(r.content)} bytes {r.headers.get('Content-Type')} "
          f"LM={r.headers.get('Last-Modified')} ETag={r.headers.get('ETag')} CD={r.headers.get('Content-Disposition')}",
          flush=True)
    r.raise_for_status()
    return r


def show_xlsx(name: str, body: bytes, max_rows: int, max_cols: int = 18) -> dict:
    by_sheet: dict[str, dict[int, list]] = {}
    for c in xlsx_cells.cells(body):
        by_sheet.setdefault(c["sheet_name"], {}).setdefault(c["row_number"], []).append(
            (c["column_number"], c["value_text"], c["number_format"])
        )
    out = {}
    for sheet, rows in by_sheet.items():
        out[sheet] = [len(rows), max(rows), max(c for r in rows.values() for c, _, _ in r)]
        print(f"  --- {name} :: sheet {sheet!r} rows={len(rows)} max_row={max(rows)} max_col={out[sheet][2]}", flush=True)
        for rn in sorted(rows)[:max_rows]:
            print(f"    r{rn}: " + " | ".join(f"c{c}={v[:50]!r}" for c, v, _ in sorted(rows[rn])[:max_cols]), flush=True)
        if max_rows:
            firsts = [sorted(rows[rn])[0][1][:30] for rn in sorted(rows)]
            print(f"    first-col labels: {firsts[:80]}", flush=True)
    return out


def show_xls(name: str, body: bytes, max_rows: int, max_cols: int = 18) -> dict:
    import xlrd

    book = xlrd.open_workbook(file_contents=body, on_demand=True)
    out = {}
    for sname in book.sheet_names():
        sh = book.sheet_by_name(sname)
        out[sname] = [sh.nrows, sh.ncols]
        print(f"  --- {name} :: xls sheet {sname!r} rows={sh.nrows} cols={sh.ncols}", flush=True)
        for r in range(min(sh.nrows, max_rows)):
            vals = [(c + 1, sh.cell_value(r, c)) for c in range(min(sh.ncols, max_cols)) if sh.cell_value(r, c) not in ("", None)]
            print(f"    r{r + 1}: " + " | ".join(f"c{c}={str(v)[:50]!r}" for c, v in vals), flush=True)
        if max_rows:
            firsts = [str(next((v for v in sh.row_values(r) if v not in ("", None)), ""))[:30] for r in range(sh.nrows)]
            print(f"    first-col labels: {firsts[:80]}", flush=True)
        book.unload_sheet(sname)
    return out


def walk(z: zipfile.ZipFile, prefix: str, max_rows: int, detail: int) -> dict:
    out = {}
    shown = 0
    for info in z.infolist():
        name, low = info.filename, info.filename.lower()
        if info.is_dir():
            continue
        data = z.read(name)
        key = prefix + name
        if low.endswith(".zip"):
            print(f"  inner zip {key} ({len(data)} bytes)", flush=True)
            out.update(walk(zipfile.ZipFile(io.BytesIO(data)), key + "!", max_rows, detail))
            continue
        kind = "xlsx" if data[:2] == b"PK" else "ole2(xls/doc)" if data[:4] == b"\xd0\xcf\x11\xe0" else repr(data[:8])
        print(f"  member {key} size={info.file_size} kind={kind}", flush=True)
        if kind == "xlsx" and low.endswith((".xlsx", ".xlsm")):
            try:
                out[key] = show_xlsx(key, data, max_rows if shown < detail else 0)
                shown += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  xlsx error {key}: {exc}", flush=True)
                out[key] = f"error {exc}"[:80]
        elif kind.startswith("ole2") and low.endswith(".xls"):
            try:
                out[key] = show_xls(key, data, max_rows if shown < detail else 0)
                shown += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  xls error {key}: {exc}", flush=True)
                out[key] = f"error {exc}"[:80]
        else:
            out[key] = kind
    return out


def incident(url: str) -> dict:
    """Member names, sizes and the header line or header cells only; never record content."""
    r = get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    out = {}
    for info in z.infolist():
        name, low = info.filename, info.filename.lower()
        data = z.read(name)
        print(f"  incident member {name} size={info.file_size} first={data[:4]!r}", flush=True)
        if low.endswith((".csv", ".txt")):
            for enc in ("utf-8-sig", "cp1250"):
                try:
                    text = data.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            lines = text.splitlines()
            out[name] = {"lines": len(lines), "header": lines[0][:1800]}
            print(f"    lines={len(lines)} enc={enc} header: {lines[0][:4000]}", flush=True)
        elif data[:2] == b"PK" and low.endswith(".xlsx"):
            hdr: dict[str, list] = {}
            nrows: dict[str, int] = {}
            for c in xlsx_cells.cells(data):
                nrows[c["sheet_name"]] = max(nrows.get(c["sheet_name"], 0), c["row_number"])
                if c["row_number"] <= 2:
                    hdr.setdefault(c["sheet_name"], []).append(c["value_text"][:40])
            out[name] = {s: {"rows": nrows[s], "header": v[:120]} for s, v in hdr.items()}
            print(f"    sheets/rows {nrows}; header cells {json.dumps(hdr, ensure_ascii=False)[:6000]}", flush=True)
        else:
            out[name] = f"size {info.file_size}"
    return out


def main() -> int:
    r = get(PAGE)
    p = Anchors()
    p.feed(r.text)
    links = [(h, " ".join(t.replace("​", "").split())) for h, t in p.out]
    att = [(h, t) for h, t in links if "/attachment/" in h]
    print(f"page links {len(links)}, attachment links {len(att)}", flush=True)
    for h, t in att:
        print(f"  ATT {t!r} -> {h}", flush=True)
    text = " ".join(re.sub(r"<[^>]+>", " ", r.text).split())
    for m in re.finditer(r"(?i)tabele sta|zestawienie danych|licencj", text):
        print(f"  CTX ...{text[max(0, m.start() - 150): m.end() + 250]}...", flush=True)
    annotate("notice", "kgpsp probe page", {"http": r.status_code, "links": len(links),
                                            "attachments": [[t[:70], h[-36:]] for h, t in att][:24]})
    tables = [(h, t) for h, t in att if re.match(r"^20[0-9]{2} \S*\.zip", t) and not re.search(r"(?i)zdarze", t)]
    events = [(h, t) for h, t in att if re.search(r"(?i)informacji ze zdarze|zdarze", t) and (h, t) not in tables]
    res: dict = {}
    for n, (h, t) in enumerate(tables):
        label = "newest" if n == 0 else "oldest" if n == len(tables) - 1 else f"t{n}"
        z = zipfile.ZipFile(io.BytesIO(get(h).content))
        print(f"== standard tables {label} {t!r}: {len(z.infolist())} entries", flush=True)
        rows, detail = (45, 80) if label in ("newest", "oldest") else (0, 0)
        members = walk(z, "", rows, detail)
        if label in ("newest", "oldest"):
            res[label] = {"link": t[:60], "members": members}
        else:
            res.setdefault("other_years", {})[t[:40]] = sorted(members)[:60]
    if events and "noevents" not in sys.argv[1:]:
        print(f"== incident data {events[0][1]!r}", flush=True)
        res["incident"] = incident(events[0][0])
    for label, body in res.items():
        s = json.dumps(body, ensure_ascii=False, default=str)
        annotate("notice", f"kgpsp probe {label}", {"chars": len(s), "head": s[:3500]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
