"""Read-only probe 5 (PLS-021, #3031): the MEN school lists on dane.gov.pl, metadata, terms and file layout.

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
            import openpyxl  # noqa: PLC0415
            wb = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
            note("xlsx", {"file": label, "sheets": wb.sheetnames})
            for ws in wb.worksheets[:3]:
                rows = [["" if c is None else c for c in r] for r in ws.iter_rows(values_only=True)]
                profile_rows(rows, f"{label}#{ws.title}")
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
    ids: dict[str, str] = {}
    for q in ("Wykaz szkół i placówek oświatowych wg stanu bazy SIO", "Wykaz szkół i placówek oświatowych według stanu na dzień"):
        d, i = jget(f"{API}/search?" + urllib.parse.urlencode({"q": q, "per_page": 50}))
        items = []
        for x in (d or {}).get("data", []):
            a = x.get("attributes") or {}
            t = re.sub(r"</?mark>", "", a.get("title") or "")
            items.append([x.get("id"), a.get("model"), t, a.get("modified")])
            if t.startswith("Wykaz szkół i placówek oświatowych") and ("stanu" in t or "RSPO" in t):
                ids[str(x.get("id"))] = a.get("model") or "dataset"
        note("search", {"q": q, **i, "items": items})
    ids.setdefault("51521", "dataset")
    for did, model in sorted(ids.items()):
        if model != "dataset":
            r, i = jget(f"{API}/resources/{did}")
            note("resource_hit", {"id": did, "model": model, **i, "attrs": (r or {}).get("data", {}).get("attributes")})
            continue
        d, i = jget(f"{API}/datasets/{did}")
        a = (d or {}).get("data", {}).get("attributes", {})
        note("dataset", {"id": did, **i, "title": a.get("title"), "slug": a.get("slug"), "modified": a.get("modified"),
                         "created": a.get("created"), "frequency": a.get("update_frequency"),
                         "notes": re.sub(r"<[^>]+>", " ", a.get("notes") or "")[:800],
                         "licence": {k: v for k, v in a.items() if re.search(r"licen|condition|source|custom", k)},
                         "org": (d or {}).get("data", {}).get("relationships", {}).get("institution", {}).get("links")})
        rs, i = jget(f"{API}/datasets/{did}/resources?per_page=50")
        res = []
        for x in (rs or {}).get("data", []):
            ra = x.get("attributes") or {}
            res.append({"id": x.get("id"), "title": ra.get("title"), "format": ra.get("format"), "size": ra.get("file_size"),
                        "url": ra.get("download_url") or ra.get("file_url") or ra.get("link"), "data_date": ra.get("data_date"),
                        "modified": ra.get("modified"), "created": ra.get("created"), "openness": ra.get("openness_score"),
                        "type": ra.get("type"), "media_type": ra.get("media_type")})
        note("resources", {"dataset": did, **i, "resources": res})
        ids[did] = json.dumps(res, ensure_ascii=False)
    # layout of one file per dataset, newest snapshot first, files up to 60 MB
    files = []
    for did, res in ids.items():
        try:
            for r in json.loads(res):
                if r.get("url") and (r.get("format") or "").lower() in ("csv", "xlsx", "xls", "zip"):
                    files.append((did, r))
        except (ValueError, TypeError):
            pass
    seen_ds: set[str] = set()
    for did, r in sorted(files, key=lambda t: -int(t[0])):
        if did in seen_ds:
            continue
        seen_ds.add(did)
        b, i = get(r["url"], accept="*/*", limit=60_000_000, timeout=300)
        note("file", {"dataset": did, "resource": r["id"], "format": r.get("format"), **i})
        if b and not i.get("truncated"):
            try:
                profile_file(b, f"{did}/{r['id']}.{r.get('format')}")
            except Exception as e:  # noqa: BLE001
                note("layout", {"file": r["id"], "error": f"{type(e).__name__}: {e}"})
    annotate("notice", "rspo probe 5", [n[:300] for n in NOTES if n.startswith(("[dataset]", "[file]", "[layout]"))][:14])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
