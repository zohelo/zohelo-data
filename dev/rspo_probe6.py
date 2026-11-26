"""Read-only probe 6 (PLS-021, #3031): the MEN school lists on dane.gov.pl by resource id: dataset terms, file layout.

GET only, one request at a time, 2 s apart, at most 60 requests. Prints dataset and resource metadata (titles,
formats, sizes, dates, licence fields) and, for each file read, its layout: format, sheet names, header row and
per column the number of non-empty values, distinct count and maximum length. Sample values only for columns whose
header names a code, type or date (never names, addresses, phone numbers, e-mails or web addresses).
"""

from __future__ import annotations

import csv
import io
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.dane.gov.pl/1.4"
BUDGET = [60]
NOTES: list[str] = []
SAFE = re.compile(r"(typ|rodzaj|kod|teryt|data|status|publiczn|kategori|regon|numer rspo|rspo|identyfikator|organ|jednostk|"
                  r"wojew|powiat|gmina|symbol|uczni|oddzia|zawod|nauczy|specyfik|miejsce w strukturze|liczba)", re.I)
UNSAFE = re.compile(r"(nazwa|imi|nazwisk|dyrektor|telefon|faks|fax|mail|www|strona|ulica|nr domu|numer domu|lokal|adres|"
                    r"kod poczt|poczta|miejscowo)", re.I)


def note(section: str, item) -> None:
    line = f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)}"
    print(line[:40000], flush=True)
    NOTES.append(line[:400])


def get(url: str, accept: str = "application/json", limit: int = 5_000_000, timeout: int = 150):
    if BUDGET[0] <= 0:
        return None, {"status": "budget used"}
    BUDGET[0] -= 1
    time.sleep(2.0)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(limit + 1)
            hdr = {k.lower(): v[:200] for k, v in r.headers.items()
                   if k.lower() in ("content-type", "content-length", "last-modified", "content-disposition", "etag")}
            return body, {"status": r.status, "final": r.geturl(), **hdr, "read": len(body), "truncated": len(body) > limit}
    except urllib.error.HTTPError as e:
        return None, {"status": e.code}
    except Exception as e:  # noqa: BLE001
        return None, {"status": f"{type(e).__name__}: {str(e)[:160]}"}


def jget(url: str):
    b, i = get(url)
    if not b:
        return None, i
    try:
        return json.loads(b), i
    except ValueError:
        return None, {**i, "head": b[:200].decode("utf-8", "replace")}


def profile_rows(rows: list[list[str]], label: str) -> None:
    if not rows:
        note("layout", {"file": label, "rows": 0})
        return
    # header = first row with at least 5 non-empty cells
    h = next((i for i, r in enumerate(rows[:30]) if sum(1 for c in r if str(c).strip()) >= 5), 0)
    header = [str(c).strip() for c in rows[h]]
    body = rows[h + 1:]
    cols = []
    for j, name in enumerate(header):
        vals = [str(r[j]).strip() for r in body if j < len(r) and r[j] is not None and str(r[j]).strip()]
        rec = {"col": j, "name": name, "filled": len(vals), "distinct": len(set(vals)), "maxlen": max((len(v) for v in vals), default=0)}
        if SAFE.search(name) and not UNSAFE.search(name):
            counts: dict[str, int] = {}
            for v in vals:
                counts[v] = counts.get(v, 0) + 1
            top = sorted(counts.items(), key=lambda kv: -kv[1])
            rec["top"] = top[:40] if len(counts) <= 120 else top[:5]
        cols.append(rec)
    note("layout", {"file": label, "header_row": h, "pre_header": [[str(c)[:60] for c in r if str(c).strip()] for r in rows[:h]],
                    "rows": len(body), "columns": len(header)})
    for c in cols:
        note("column", {"file": label, **c})


def profile_file(body: bytes, label: str) -> None:
    head = body[:8]
    if head.startswith(b"PK"):
        zf = zipfile.ZipFile(io.BytesIO(body))
        names = zf.namelist()
        note("zip", {"file": label, "members": names[:30]})
        if any(n.startswith("xl/") for n in names):
            from ingestion.common.xlsx_cells import cells  # noqa: PLC0415
            grid: dict[str, dict[int, dict[int, str]]] = {}
            for c in cells(body):
                grid.setdefault(c["sheet_name"], {}).setdefault(c["row_number"], {})[c["column_number"]] = c["value_text"] or ""
            note("xlsx", {"file": label, "sheets": list(grid)})
            for sheet, rows_by_n in list(grid.items())[:3]:
                width = max((max(r) for r in rows_by_n.values() if r), default=0)
                rows = [[rows_by_n[n].get(j, "") for j in range(1, width + 1)] for n in sorted(rows_by_n)]
                profile_rows(rows, f"{label}#{sheet}")
            return
        for n in names[:3]:
            if n.lower().endswith((".csv", ".txt")):
                profile_file(zf.read(n), f"{label}!{n}")
        return
    if head.startswith(b"\xd0\xcf\x11\xe0"):
        import xlrd  # noqa: PLC0415
        wb = xlrd.open_workbook(file_contents=body)
        note("xls", {"file": label, "sheets": wb.sheet_names()})
        for sh in wb.sheets()[:3]:
            rows = [sh.row_values(i) for i in range(sh.nrows)]
            profile_rows(rows, f"{label}#{sh.name}")
        return
    for enc in ("utf-8-sig", "cp1250"):
        try:
            text = body.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        note("layout", {"file": label, "error": "not utf-8 or cp1250"})
        return
    first = text.splitlines()[0] if text else ""
    delim = max([";", ",", "\t", "|"], key=first.count)
    note("csv", {"file": label, "encoding": enc, "delimiter": delim, "bytes": len(body), "lines": text.count("\n")})
    profile_rows(list(csv.reader(io.StringIO(text), delimiter=delim)), label)


def main(argv: list[str]) -> int:
    rids = ["1254769", "65342", "59471", "59468", "59467", "57400", "51150", "3017"]
    d, i = jget(f"{API}/search?" + urllib.parse.urlencode({"q": "Wykaz szkół i placówek 2019", "per_page": 30}))
    note("search2019", {**i, "items": [[x.get("id"), (x.get("attributes") or {}).get("model"),
                                         re.sub(r"</?mark>", "", (x.get("attributes") or {}).get("title") or "")]
                                        for x in (d or {}).get("data", [])]})
    datasets: set[str] = set()
    files: dict[str, str] = {}
    for rid in rids:
        r, i = jget(f"{API}/resources/{rid}")
        data = (r or {}).get("data", {})
        rel = data.get("relationships", {}).get("dataset", {})
        did = str((rel.get("data") or {}).get("id") or "")
        a = data.get("attributes", {})
        note("resource", {"id": rid, "dataset": did, "title": a.get("title"), "data_date": a.get("data_date"),
                          "file_url": a.get("file_url"), "file_size": a.get("file_size"), "link": a.get("link")})
        if did:
            datasets.add(did)
        if a.get("file_url"):
            files[rid] = a["file_url"]
    for did in sorted(datasets):
        d, i = jget(f"{API}/datasets/{did}")
        a = (d or {}).get("data", {}).get("attributes", {})
        note("dataset", {"id": did, **i, "title": a.get("title"), "modified": a.get("modified"),
                         "frequency": a.get("update_frequency"), "notes": re.sub(r"<[^>]+>", " ", a.get("notes") or "")[:600],
                         "licence": {k: v for k, v in a.items() if re.search(r"licen|condition|source|custom|terms", k)},
                         "institution": (d or {}).get("data", {}).get("relationships", {}).get("institution", {}).get("data")})
        rs, i = jget(f"{API}/datasets/{did}/resources?per_page=50")
        note("dataset_resources", {"dataset": did, **i, "resources": [
            [x.get("id"), (x.get("attributes") or {}).get("title"), (x.get("attributes") or {}).get("format"),
             (x.get("attributes") or {}).get("file_size"), (x.get("attributes") or {}).get("data_date")]
            for x in (rs or {}).get("data", [])]})
    for rid in ("1254769", "3017", "51150", "57400"):
        if rid not in files:
            continue
        b, i = get(files[rid], accept="*/*", limit=60_000_000, timeout=300)
        note("file", {"resource": rid, **i})
        if b and not i.get("truncated"):
            try:
                profile_file(b, f"{rid}.xlsx")
            except Exception as e:  # noqa: BLE001
                note("layout", {"file": rid, "error": f"{type(e).__name__}: {e}"})
    annotate("notice", "rspo probe 6", [n[:300] for n in NOTES if n.startswith(("[dataset]", "[file]", "[layout]"))][:14])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
