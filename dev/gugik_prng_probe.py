"""Read-only probe of the GUGiK PRNG downloads (PL-ENV-002, Task #2971).

GET and HEAD only, one request at a time, one second apart, robots.txt honoured, a request and byte budget.
Reports the PRNG page's download links and terms sentences, the gov.pl free-data page's sentences that name
PRNG, each package's members, notice files, and for each table (XLSX via DuckDB's excel extension, GML/SHP via
DuckDB spatial) its columns, row count, sample rows and the distinct values of short-list columns. Writes
nothing to R2. Everything goes to the log; a short summary to annotations.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import urllib.parse
import urllib.robotparser
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BUDGET = {"requests": 80, "bytes": 1_500_000_000}
ROBOTS: dict[str, urllib.robotparser.RobotFileParser | None] = {}
OUT: dict[str, list] = {}
PAGE = "https://www.geoportal.gov.pl/pl/dane/panstwowy-rejestr-nazw-geograficznych-prng/"
GOV = "https://www.gov.pl/web/gugik/dane-udostepniane-bez-platnie-do-pobrania-z-serwisu-wwwgeoportalgovpl"
WFS = ("https://mapy.geoportal.gov.pl/wss/service/PZGiK/PRNG/WFS/GeographicalNames"
       "?SERVICE=WFS&REQUEST=GetCapabilities")


def note(section: str, item) -> None:
    OUT.setdefault(section, []).append(item)
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:30000]}", flush=True)


def robots_ok(url: str) -> bool:
    parts = urllib.parse.urlsplit(url)
    host = f"{parts.scheme}://{parts.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=30)
            if r.status_code == 200:
                rp.parse(r.text.splitlines())
                note("robots", {"host": host, "status": 200, "text": r.text[:800]})
            else:
                rp.parse([])
                note("robots", {"host": host, "status": r.status_code})
        except requests.RequestException as e:
            note("robots", {"host": host, "error": f"{type(e).__name__}: {str(e)[:120]}"})
            rp = None
        ROBOTS[host] = rp
    rp = ROBOTS[host]
    return rp is None or rp.can_fetch(UA, url)


def fetch(url: str, method: str = "GET", path: Path | None = None, limit: int = 5_000_000):
    if BUDGET["requests"] <= 0:
        return None, {"status": "request budget used"}
    if not robots_ok(url):
        return None, {"status": "robots disallow"}
    BUDGET["requests"] -= 1
    time.sleep(1.0)
    try:
        with requests.request(method, url, headers={"User-Agent": UA, "Accept-Encoding": "identity"},
                              timeout=(30, 300), stream=True, allow_redirects=True) as r:
            hdr = {k.lower(): v[:160] for k, v in r.headers.items() if k.lower() in (
                "content-type", "content-length", "last-modified", "etag", "content-disposition", "server")}
            info = {"status": r.status_code, "final": r.url, **hdr}
            if method == "HEAD" or r.status_code != 200:
                return None, info
            size = 0
            if path is not None:
                with path.open("wb") as fh:
                    for block in r.iter_content(8 << 20):
                        fh.write(block)
                        size += len(block)
                        if size > BUDGET["bytes"]:
                            return None, {**info, "status": "byte budget used"}
                BUDGET["bytes"] -= size
                return path, {**info, "read": size}
            body = b""
            for block in r.iter_content(1 << 20):
                body += block
                if len(body) >= limit:
                    break
            return body, {**info, "read": len(body)}
    except requests.RequestException as e:
        return None, {"status": f"{type(e).__name__}: {str(e)[:200]}"}


def text_of(html: bytes | None) -> str:
    return (html or b"").decode("utf-8", "replace")


def plain(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def links(html: str, base: str) -> list[tuple[str, str]]:
    out = []
    for m in re.finditer(r"<a\b[^>]*href\s*=\s*[\"']([^\"'#]+)[\"'][^>]*>(.*?)</a>", html, re.I | re.S):
        out.append((urllib.parse.urljoin(base, m.group(1).strip()), plain(m.group(2)).strip()[:120]))
    return list(dict.fromkeys(out))


def snippets(text: str, pattern: str, width: int = 300, n: int = 20) -> list[str]:
    return [text[max(0, m.start() - width): m.end() + width] for m in re.finditer(pattern, text, re.I)][:n]


def table_profile(con, rel: str, label: str) -> None:
    cols = con.execute(f"describe select * from {rel}").fetchall()
    names = [c[0] for c in cols]
    rows = con.execute(f"select count(*) from {rel}").fetchone()[0]
    sample = con.execute(f"select * from {rel} limit 3").fetchall()
    short = {}
    for c in names:
        if c.lower() in ("geom", "geometry", "wkb_geometry"):
            continue
        q = f'"{c}"'
        d = con.execute(f"select count(distinct {q}), count(*) filter (where {q} is null) from {rel}").fetchone()
        entry = {"distinct": d[0], "nulls": d[1]}
        if d[0] <= 80:
            entry["values"] = con.execute(
                f"select {q}::varchar, count(*) from {rel} group by 1 order by 2 desc limit 80").fetchall()
        short[c] = entry
    note("table", {"label": label, "rows": rows, "columns": [(c[0], c[1]) for c in cols],
                   "sample": [[str(v)[:80] for v in r] for r in sample], "profile": short})


def inspect_package(path: Path, url: str, work: Path) -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit = '10GB'")
    if not zipfile.is_zipfile(path):
        note("package", {"url": url, "not_zip_head": path.read_bytes()[:300].decode("utf-8", "replace")})
        targets = [path]
    else:
        folder = work / re.sub(r"[^A-Za-z0-9]+", "_", url)[-60:]
        with zipfile.ZipFile(path) as z:
            members = [(i.filename, i.file_size, i.compress_size) for i in z.infolist()]
            note("package", {"url": url, "zip_bytes": path.stat().st_size, "members": members[:200]})
            for i in z.infolist():
                low = i.filename.lower()
                if low.endswith((".txt", ".xml", ".html", ".htm", ".md", ".pdf")) and i.file_size < 3_000_000 \
                        and not low.endswith(".gml"):
                    raw = z.read(i)
                    note("notice", {"member": i.filename, "bytes": len(raw),
                                    "text": raw[:4000].decode("utf-8", "replace") if not low.endswith(".pdf")
                                    else "(pdf)"})
            z.extractall(folder)
        targets = sorted(p for p in folder.rglob("*") if p.is_file())
    for t in targets:
        low = t.name.lower()
        try:
            if low.endswith((".xlsx", ".xls")):
                con.execute("INSTALL excel; LOAD excel;")
                with zipfile.ZipFile(t) as wb:
                    from ingestion.common.xlsx_cells import sheets
                    names = [s[0] for s in sheets(wb)]
                note("workbook", {"file": t.name, "bytes": t.stat().st_size, "sheets": names})
                for s in names:
                    rel = f"read_xlsx('{t}', sheet='{s}', all_varchar=true)"
                    table_profile(con, rel, f"{t.name}#{s}")
            elif low.endswith(".csv"):
                table_profile(con, f"read_csv('{t}', all_varchar=true)", t.name)
            elif low.endswith((".gml", ".shp")):
                con.execute("INSTALL spatial; LOAD spatial;")
                if low.endswith(".gml"):
                    note("gml_head", {"file": t.name, "bytes": t.stat().st_size,
                                      "head": t.open("rb").read(3000).decode("utf-8", "replace")})
                table_profile(con, f"ST_Read('{t}')", t.name)
            elif low.endswith((".prj", ".cpg")):
                note("aux", {"file": t.name, "text": t.read_text(errors="replace")[:600]})
        except Exception as e:  # noqa: BLE001
            note("error", {"file": t.name, "error": f"{type(e).__name__}: {str(e)[:400]}"})


def main(argv: list[str]) -> int:
    wms = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/PRNG/WMS/DaneDoPobrania"
    body, info = fetch(wms + "?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0", limit=400_000)
    caps = text_of(body)
    layers = re.findall(r"<Layer[^>]*>\s*<Name>([^<]+)</Name>\s*<Title>([^<]*)</Title>", caps)
    fmts = re.findall(r"<Format>([^<]+)</Format>", caps)
    note("wms_caps", {**info, "layers": layers, "formats": sorted(set(fmts)), "head": caps[:1500]})
    names = [n for n, _t in layers] or ["0"]
    hrefs: list[str] = []
    # points in Poland (EPSG:4326, lat/lon axis order of WMS 1.3.0): Warsaw, Krakow, Gdansk
    for lat, lon in ((52.23, 21.01), (50.06, 19.94), (54.35, 18.65)):
        bbox = f"{lat - 0.05},{lon - 0.05},{lat + 0.05},{lon + 0.05}"
        for fmt in ("text/html", "application/vnd.ogc.gml", "text/plain"):
            q = urllib.parse.urlencode({
                "SERVICE": "WMS", "VERSION": "1.3.0", "REQUEST": "GetFeatureInfo", "LAYERS": ",".join(names),
                "QUERY_LAYERS": ",".join(names), "STYLES": "", "CRS": "EPSG:4326", "BBOX": bbox, "WIDTH": "101",
                "HEIGHT": "101", "I": "50", "J": "50", "INFO_FORMAT": fmt, "FEATURE_COUNT": "50", "FORMAT": "image/png"})
            body, info = fetch(f"{wms}?{q}", limit=400_000)
            t = text_of(body)
            found = re.findall(r"https?://[^\"'\s<>]+", t)
            hrefs += found
            note("featureinfo", {"point": [lat, lon], "format": fmt, **info, "urls": sorted(set(found))[:60],
                                 "text": t[:3000]})
            if found:
                break
        if hrefs:
            break
    body, winfo = fetch(WFS, limit=300_000)
    wcaps = text_of(body)
    note("wfs_types", {"types": re.findall(r"<FeatureType[^>]*>\s*<Name>([^<]+)</Name>\s*<Title>([^<]*)</Title>", wcaps),
                       "output": sorted(set(re.findall(r"<ows:Value>([^<]+)</ows:Value>", wcaps)))[:60]})
    cands = [u for u in dict.fromkeys(hrefs) if re.search(r"\.(zip|xlsx|xls|csv|gml|7z)(\?|$)", u, re.I)]
    note("candidates", cands)
    heads = []
    for u in cands[:30]:
        _b, h = fetch(u, method="HEAD")
        heads.append({"url": u, **h})
        note("head", heads[-1])
    work = Path(tempfile.mkdtemp(prefix="prng_"))

    def rank(h):
        u = h["url"].lower()
        return (0 if "xls" in u else 1 if "csv" in u else 2 if "gml" in u else 3, int(h.get("content-length") or 0))
    done = 0
    for h in sorted([h for h in heads if h.get("status") == 200], key=rank):
        if rank(h)[0] >= 2 and done:
            continue  # GML/SHP only when no XLSX or CSV package exists
        if int(h.get("content-length") or 0) > 600_000_000:
            note("skipped", {"url": h["url"], "reason": "over 600 MB"})
            continue
        target = work / f"f{done}.bin"
        started = time.monotonic()
        p, i = fetch(h["url"], path=target)
        note("download", {"url": h["url"], **i, "seconds": round(time.monotonic() - started)})
        if p:
            inspect_package(p, h["url"], work)
            p.unlink()
            done += 1
    out_dir = os.environ.get("DEV_OUTPUT_DIR")
    if out_dir:
        with open(os.path.join(out_dir, "gugik_prng_probe.json"), "w", encoding="utf-8") as fh:
            json.dump(OUT, fh, ensure_ascii=False, indent=1, default=str)
    annotate("notice", "prng probe summary", {
        "candidates": cands, "heads": [{k: h.get(k) for k in ("url", "status", "content-length", "last-modified",
                                                               "etag")} for h in heads],
        "downloads": [{k: d.get(k) for k in ("url", "status", "read", "seconds")} for d in OUT.get("download", [])],
        "tables": [{k: t[k] for k in ("label", "rows")} for t in OUT.get("table", [])],
        "errors": OUT.get("error", [])[:5]})
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
