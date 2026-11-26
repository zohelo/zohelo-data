"""Read-only probe of the GIOŚ yearly archive (#3119): robots.txt, the archive page, HEAD of every file, the
newest, the oldest and one middle yearly ZIP downloaded to the runner's disk (no R2 writes): members, sizes, sheet
names and the first rows of each workbook. One request at a time, 3 s apart."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate

BASE = "https://powietrze.gios.gov.pl"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA


def get(url: str, **kw) -> requests.Response:
    time.sleep(3)
    return S.get(url, timeout=(30, 600), **kw)


def head(url: str) -> dict:
    time.sleep(3)
    r = S.head(url, timeout=(30, 120), allow_redirects=True)
    out = {"status": r.status_code, **{k: r.headers.get(k) for k in ("content-type", "content-length", "last-modified", "content-disposition", "etag")}}
    if r.status_code != 200 or not r.headers.get("content-length"):
        time.sleep(3)
        g = S.get(url, timeout=(30, 120), stream=True)
        out["get"] = {"status": g.status_code, **{k: g.headers.get(k) for k in ("content-type", "content-length", "last-modified", "content-disposition")}}
        g.close()
    return out


def first_rows(con, path: Path, sheet: str, rng: str = "A1:H9") -> list:
    try:
        return con.execute(
            f"select * from read_xlsx('{path}', sheet = '{sheet}', header = false, all_varchar = true, range = '{rng}')"
        ).fetchall()
    except duckdb.Error as exc:
        return [f"read_xlsx failed: {str(exc)[:200]}"]


def workbook(con, data: bytes, name: str, tmp: Path, full: bool) -> dict:
    from ingestion.common import xlsx_cells

    out: dict = {}
    with zipfile.ZipFile(io.BytesIO(data)) as wb:
        sheets = xlsx_cells.sheets(wb)
        out["sheets"] = [s for s, _ in sheets]
        for s, p in sheets[:3]:
            head_xml = wb.open(p).read(4000).decode("utf-8", "replace")
            m = re.search(r'<dimension ref="([^"]+)"', head_xml)
            out.setdefault("dimension", {})[s] = m.group(1) if m else None
    path = tmp / "w.xlsx"
    path.write_bytes(data)
    for s in out["sheets"][:2]:
        out.setdefault("rows", {})[s] = first_rows(con, path, s)
        out.setdefault("last_col_rows", {})[s] = first_rows(con, path, s, "A1:ZZ7")[:7]
        if full:
            t = time.monotonic()
            try:
                n = con.execute(f"select count(*) from read_xlsx('{path}', sheet = '{s}', header = false, all_varchar = true)").fetchone()[0]
                out.setdefault("full_rows", {})[s] = [n, round(time.monotonic() - t, 1)]
            except duckdb.Error as exc:
                out.setdefault("full_rows", {})[s] = str(exc)[:200]
    return out


def main() -> int:
    report: dict = {}
    r = get(f"{BASE}/robots.txt")
    report["robots"] = {"status": r.status_code, "text": r.text[:1500]}
    print("ROBOTS", r.status_code, r.text[:3000])
    page = get(f"{BASE}/pjp/archives")
    report["archives_page"] = {"status": page.status_code, "bytes": len(page.content)}
    html = page.text
    links = []
    for m in re.finditer(r'href="([^"]*downloadFile/(\d+))"', html):
        start = max(0, m.start() - 400)
        context = re.sub(r"<[^>]+>", " ", html[start : m.end() + 400])
        context = re.sub(r"\s+", " ", context)
        links.append({"id": int(m.group(2)), "href": m.group(1), "context": context[-500:]})
    print("LINKS", len(links))
    for link in links:
        print("LINK", json.dumps(link, ensure_ascii=False))
    # titles: text of the anchor or nearby
    for m in re.finditer(r'downloadFile/(\d+)"[^>]*>(.*?)</a>', html, re.S):
        print("ANCHOR", m.group(1), re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(2))).strip()[:200])
    Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp")).mkdir(parents=True, exist_ok=True)
    (Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp")) / "archives.html").write_text(html)
    heads = {}
    for link in links:
        url = link["href"] if link["href"].startswith("http") else BASE + link["href"]
        heads[link["id"]] = head(url)
        print("HEAD", link["id"], json.dumps(heads[link["id"]], ensure_ascii=False))
    report["heads"] = len(heads)
    # pick years from anchors
    years = {}
    for m in re.finditer(r'downloadFile/(\d+)"[^>]*>(.*?)</a>', html, re.S):
        text = re.sub(r"<[^>]+>", " ", m.group(2))
        y = re.search(r"Wyniki pomiar\w+ z (\d{4}) roku", text)
        if y:
            years[int(y.group(1))] = int(m.group(1))
    if not years:
        for link in links:
            y = re.search(r"Wyniki pomiar\w+ z (\d{4}) roku", link["context"])
            if y:
                years.setdefault(int(y.group(1)), link["id"])
    print("YEARS", json.dumps(years))
    report["years"] = years
    picks = []
    if years:
        ys = sorted(years)
        picks = sorted({ys[-1], ys[0], ys[len(ys) // 2]})
    pick_args = [int(a) for a in sys.argv[1:]]
    if pick_args:
        picks = pick_args
    con = duckdb.connect()
    con.execute("INSTALL excel; LOAD excel; SET memory_limit='8GB'")
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        for year in picks:
            fid = years[year]
            started = time.monotonic()
            resp = get(f"{BASE}/pjp/archives/downloadFile/{fid}")
            body = resp.content
            info = {"year": year, "id": fid, "status": resp.status_code, "bytes": len(body), "s": round(time.monotonic() - started, 1),
                    "type": resp.headers.get("content-type"), "disposition": resp.headers.get("content-disposition")}
            print("DOWNLOAD", json.dumps(info, ensure_ascii=False))
            try:
                z = zipfile.ZipFile(io.BytesIO(body))
            except zipfile.BadZipFile:
                print("NOT A ZIP", body[:300])
                continue
            members = [(i.filename, i.file_size, i.compress_size) for i in z.infolist()]
            info["members"] = len(members)
            info["uncompressed"] = sum(m[1] for m in members)
            print("MEMBERS", year, len(members))
            for m in members:
                print("MEMBER", year, json.dumps(m, ensure_ascii=False))
            biggest = max(members, key=lambda m: m[1])[0] if members else None
            for name, size, _ in members:
                if not name.lower().endswith((".xlsx", ".xls", ".csv", ".txt")):
                    continue
                data = z.read(name)
                if name.lower().endswith(".xlsx"):
                    try:
                        w = workbook(con, data, name, tmp, full=(name == biggest))
                    except Exception as exc:  # probe: record and go on
                        w = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
                    print("BOOK", year, name, json.dumps(w, ensure_ascii=False, default=str)[:6000])
                else:
                    print("TEXT", year, name, data[:800])
            report[f"y{year}"] = info
    annotate("notice", "gios archive probe", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
