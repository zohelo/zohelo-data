"""Read-only probe of the PIG-PIB MIDAS WFS (register PL-ENV-015, part of #2870).

GET only, one request at a time, two seconds apart, robots.txt honoured, a request budget. Reports: the MIDAS
rules page text (zrodla-danych.html), the WFS capabilities (feature types, output formats, paging constraints),
DescribeFeatureType, resultType=hits per layer, then one full GetFeature per layer (streamed to a temp file, never
to R2): bytes, seconds, numberMatched/numberReturned, and a DuckDB spatial profile of the file (columns, nulls,
distinct counts, closed lists, key uniqueness, coordinate ranges). Also the ArcGIS REST layer list (field aliases).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
import time
import urllib.parse
import urllib.robotparser
import xml.etree.ElementTree as ET

import duckdb
import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
WFS = "https://cbdgmapa.pgi.gov.pl/arcgis/services/midas/MapServer/WFSServer"
REST = "https://cbdgmapa.pgi.gov.pl/arcgis/rest/services/midas/MapServer"
PAGES = ["https://www.pgi.gov.pl/midas/zrodla-danych.html", "https://www.pgi.gov.pl/midas.html"]
BUDGET = {"requests": 40}
PARAMS: dict = {}
ROBOTS: dict[str, urllib.robotparser.RobotFileParser | None] = {}
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", tempfile.gettempdir()))


def note(section: str, item) -> None:
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:30000]}", flush=True)


def robots_ok(url: str) -> bool:
    parts = urllib.parse.urlsplit(url)
    host = f"{parts.scheme}://{parts.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=30)
            rp.parse(r.text.splitlines() if r.status_code == 200 else [])
            note("robots", {"host": host, "status": r.status_code, "text": r.text[:600] if r.ok else ""})
        except requests.RequestException as e:
            note("robots", {"host": host, "error": f"{type(e).__name__}: {str(e)[:120]}"})
            rp = None
        ROBOTS[host] = rp
    rp = ROBOTS[host]
    return rp is None or rp.can_fetch(UA, url)


def fetch(url: str, to: Path | None = None, limit: int = 5_000_000):
    if BUDGET["requests"] <= 0:
        return None, {"status": "request budget used"}
    if not robots_ok(url):
        return None, {"status": "robots disallow"}
    BUDGET["requests"] -= 1
    time.sleep(2.0)
    t0 = time.monotonic()
    try:
        with requests.get(url, headers={"User-Agent": UA}, timeout=(30, 600), stream=True) as r:
            hdr = {k.lower(): v[:160] for k, v in r.headers.items() if k.lower() in (
                "content-type", "content-length", "last-modified", "etag", "content-encoding")}
            info = {"status": r.status_code, **hdr}
            if to is not None:
                n = 0
                with to.open("wb") as f:
                    for block in r.iter_content(1 << 20):
                        f.write(block)
                        n += len(block)
                return None, {**info, "bytes": n, "seconds": round(time.monotonic() - t0, 1)}
            body = b""
            for block in r.iter_content(1 << 20):
                body += block
                if len(body) >= limit:
                    break
            return body, {**info, "read": len(body), "seconds": round(time.monotonic() - t0, 1)}
    except requests.RequestException as e:
        return None, {"status": f"{type(e).__name__}: {str(e)[:200]}"}


def plain(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def pages() -> None:
    for url in PAGES:
        body, info = fetch(url)
        note("page", {"url": url, **info})
        if not body:
            continue
        text = plain(body.decode("utf-8", "replace"))
        start = text.find("Źród")
        note("page_text", {"url": url, "text": text[max(0, start - 200): start + 6000] if start >= 0 else text[:6000]})
        links = sorted(set(re.findall(r'href="([^"]+)"', body.decode("utf-8", "replace"))))
        note("page_links", [x for x in links if re.search(r"zasad|licenc|warunk|isp|regulamin|pdf", x, re.I)][:40])


def capabilities() -> list[dict]:
    body, info = fetch(f"{WFS}?service=WFS&request=GetCapabilities&version=2.0.0")
    note("capabilities", info)
    if not body:
        return []
    (OUT / "capabilities.xml").write_bytes(body)
    root = ET.fromstring(body)
    ns = {"wfs": "http://www.opengis.net/wfs/2.0", "ows": "http://www.opengis.net/ows/1.1"}
    types = []
    for ft in root.iter("{http://www.opengis.net/wfs/2.0}FeatureType"):
        rec = {child.tag.split("}")[1]: (child.text or "").strip() for child in ft if len(child) == 0}
        rec["formats"] = [e.text for e in ft.iter("{http://www.opengis.net/wfs/2.0}Format")]
        types.append(rec)
    note("feature_types", types)
    constraints = {c.get("name"): (c.findtext("ows:DefaultValue", namespaces=ns) or "") for c in root.iter(
        "{http://www.opengis.net/ows/1.1}Constraint")}
    note("constraints", constraints)
    for op in root.iter("{http://www.opengis.net/ows/1.1}Operation"):
        if op.get("name") == "GetFeature":
            params = {p.get("name"): [v.text for v in p.iter("{http://www.opengis.net/ows/1.1}Value")]
                      for p in op.iter("{http://www.opengis.net/ows/1.1}Parameter")}
            PARAMS.update(params)
            note("getfeature_params", params)
    text = body.decode("utf-8", "replace")
    note("capabilities_terms", re.findall(r"<ows:(?:Fees|AccessConstraints)>(.*?)</ows:", text, re.S)[:4])
    return types


def describe(names: list[str]) -> None:
    body, info = fetch(f"{WFS}?service=WFS&request=DescribeFeatureType&version=2.0.0&typeNames={','.join(names)}")
    note("describe", info)
    if body:
        (OUT / "describe.xsd").write_bytes(body)
        root = ET.fromstring(body)
        for ct in root.iter("{http://www.w3.org/2001/XMLSchema}complexType"):
            els = [(e.get("name"), e.get("type"), e.get("minOccurs")) for e in ct.iter(
                "{http://www.w3.org/2001/XMLSchema}element")]
            note("describe_type", {"type": ct.get("name"), "elements": els})


def hits(name: str) -> None:
    body, info = fetch(f"{WFS}?service=WFS&request=GetFeature&version=2.0.0&typeNames={name}&resultType=hits")
    m = re.search(rb'numberMatched="(\d+|unknown)"', body or b"")
    note("hits", {"type": name, **info, "numberMatched": m.group(1).decode() if m else (body or b"")[:300]})


def profile(path: Path, name: str) -> None:
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        cols = con.execute("DESCRIBE SELECT * FROM ST_Read(?)", [str(path)]).fetchall()
        note("columns", {"type": name, "columns": [(c[0], c[1]) for c in cols]})
        n = con.execute("SELECT count(*) FROM ST_Read(?)", [str(path)]).fetchone()[0]
        note("rows", {"type": name, "rows": n})
        con.execute("CREATE TABLE t AS SELECT * FROM ST_Read(?)", [str(path)])
        geom = [c[0] for c in cols if c[1].upper().startswith("GEOMETRY")]
        for c, typ, *_ in cols:
            if c in geom:
                g = con.execute(f'SELECT min(st_xmin("{c}")), max(st_xmax("{c}")), min(st_ymin("{c}")), '
                                f'max(st_ymax("{c}")), count(*) FILTER (WHERE "{c}" IS NULL), '
                                f'any_value(st_geometrytype("{c}")), avg(st_npoints("{c}")) FROM t').fetchone()
                note("geometry", {"type": name, "col": c, "range": g})
                continue
            d, nulls = con.execute(f'SELECT count(DISTINCT "{c}"), count(*) FILTER (WHERE "{c}" IS NULL) FROM t').fetchone()
            rec = {"type": name, "col": c, "dtype": typ, "distinct": d, "nulls": nulls}
            if d <= 40:
                rec["values"] = con.execute(f'SELECT "{c}"::VARCHAR, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC').fetchall()
            else:
                rec["sample"] = [r[0] for r in con.execute(f'SELECT DISTINCT "{c}"::VARCHAR FROM t LIMIT 6').fetchall()]
                rec["minmax"] = con.execute(f'SELECT min("{c}")::VARCHAR, max("{c}")::VARCHAR FROM t').fetchone()
            note("col", rec)
    except duckdb.Error as e:
        note("profile_error", {"type": name, "error": str(e)[:400]})
    finally:
        con.close()


def full(name: str, fmt: str | None) -> None:
    suffix = ".json" if fmt and "json" in fmt.lower() else ".gml"
    path = Path(tempfile.gettempdir()) / (name.replace(":", "_") + suffix)
    url = f"{WFS}?service=WFS&request=GetFeature&version=2.0.0&typeNames={name}"
    if fmt:
        url += "&outputFormat=" + urllib.parse.quote(fmt)
    _, info = fetch(url, to=path)
    head = path.read_bytes()[:600] if path.exists() else b""
    tail = path.read_bytes()[-300:] if path.exists() else b""
    m = re.search(rb'numberMatched="(\w+)" numberReturned="(\w+)"', head)
    note("full", {"type": name, "format": fmt, **info, "matched_returned": m.groups() if m else None,
                  "head": head[:400].decode("utf-8", "replace"), "tail": tail.decode("utf-8", "replace")})
    if path.exists() and path.stat().st_size and str(info.get("status")) == "200":
        profile(path, name)


def rest() -> None:
    body, info = fetch(f"{REST}?f=json")
    note("rest", info)
    if body:
        try:
            j = json.loads(body)
            note("rest_layers", {"maxRecordCount": j.get("maxRecordCount"), "layers": [
                (x.get("id"), x.get("name")) for x in j.get("layers", [])],
                "copyright": j.get("copyrightText"), "spatialReference": j.get("spatialReference")})
        except ValueError:
            note("rest_body", body[:300].decode("utf-8", "replace"))


def main() -> None:
    pages()
    types = capabilities()
    names = [t.get("Name") for t in types if t.get("Name")]
    if names:
        describe(names)
    for name in names:
        hits(name)
    rest()
    fmt = None
    allfmt = [f for t in types for f in t.get("formats", [])] + (PARAMS.get("outputFormat") or [])
    for cand in allfmt:
        if cand and "json" in cand.lower():
            fmt = cand
            break
    note("chosen_format", fmt)
    for name in names:
        full(name, fmt)
    note("budget_left", BUDGET)


if __name__ == "__main__":
    main()
