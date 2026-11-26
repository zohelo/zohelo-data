"""Read-only probe of the Lasy Państwowe BDL GeoServer WFS (register PL-ENV-018, Task #3301).

GET only, one request at a time, 3 s apart, the platform User-Agent, a request budget, nothing written to R2.
Reports: capabilities (feature types, CRS, output formats, constraints), DescribeFeatureType of the forest
district / RDLP / forest range types, resultType=hits per type, then one full GetFeature (application/json) per
type, streamed to a temp file: bytes, seconds, and a DuckDB profile (property keys and types, nulls, distinct
counts, sample rows, geometry type and coordinate range). Type names are percent-encoded (UTF-8).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
WFS = "https://wfs.bdl.lasy.gov.pl/geoserver/BDL/ows"
BUDGET = {"requests": 30}
LEFT = {"n": 10}
TMP = Path(tempfile.gettempdir())


def note(title: str, data) -> None:
    print(f"[{title}] {json.dumps(data, ensure_ascii=False, default=str)}", flush=True)
    if LEFT["n"] > 0:
        LEFT["n"] -= 1
        annotate("notice", title, data)


def url(**params: str) -> str:
    return WFS + "?" + urllib.parse.urlencode({"service": "WFS", "version": "2.0.0", **params}, safe=":,/")


def fetch(target: str, to: Path | None = None, limit: int = 20_000_000):
    if BUDGET["requests"] <= 0:
        return None, {"status": "budget used"}
    BUDGET["requests"] -= 1
    time.sleep(3)
    t0 = time.monotonic()
    try:
        with requests.get(target, headers={"User-Agent": UA}, timeout=(30, 900), stream=True) as r:
            info = {"status": r.status_code, **{k.lower(): v[:120] for k, v in r.headers.items() if k.lower() in (
                "content-type", "content-length", "last-modified", "etag", "content-encoding")}}
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


def capabilities() -> list[dict]:
    body, info = fetch(url(request="GetCapabilities"))
    if not body:
        note("bdl capabilities failed", info)
        return []
    root = ET.fromstring(body)
    w = "{http://www.opengis.net/wfs/2.0}"
    types = []
    for ft in root.iter(w + "FeatureType"):
        types.append({"name": ft.findtext(w + "Name"), "title": (ft.findtext(w + "Title") or "")[:60],
                      "crs": ft.findtext(w + "DefaultCRS")})
    ows = "{http://www.opengis.net/ows/1.1}"
    constraints = {c.get("name"): c.findtext(ows + "DefaultValue") for c in root.iter(ows + "Constraint")}
    formats = []
    for op in root.iter(ows + "Operation"):
        if op.get("name") == "GetFeature":
            for p in op.iter(ows + "Parameter"):
                if p.get("name") == "outputFormat":
                    formats = [v.text for v in p.iter(ows + "Value")]
    text = body.decode("utf-8", "replace")
    terms = [x[:300] for x in re.findall(r"<ows:(?:Fees|AccessConstraints)>(.*?)</ows:", text, re.S)]
    note("bdl capabilities", {**info, "types": types, "constraints": constraints, "formats": formats, "terms": terms})
    return types


def describe(names: list[str]) -> None:
    body, info = fetch(url(request="DescribeFeatureType", typeNames=",".join(names)))
    out = {"info": info, "types": []}
    if body:
        try:
            root = ET.fromstring(body)
            x = "{http://www.w3.org/2001/XMLSchema}"
            for ct in root.iter(x + "complexType"):
                out["types"].append({"type": ct.get("name"), "elements": [
                    f"{e.get('name')}:{(e.get('type') or '').split(':')[-1]}:{e.get('nillable')}"
                    for e in ct.iter(x + "element")]})
        except ET.ParseError as e:
            out["error"] = str(e)[:200]
            out["body"] = body[:400].decode("utf-8", "replace")
    note("bdl describe", out)


def hits(name: str) -> int | None:
    body, info = fetch(url(request="GetFeature", typeNames=name, resultType="hits"))
    m = re.search(rb'numberMatched="(\d+)"', body or b"")
    print("[hits]", name, info, (body or b"")[:300], flush=True)
    return int(m.group(1)) if m else None


def profile(path: Path, name: str, info: dict) -> None:
    con = duckdb.connect()
    out: dict = {"type": name, **info}
    try:
        con.execute(f"CREATE TABLE doc AS SELECT content::JSON AS j FROM read_text('{path}')")
        out["head"] = con.execute("SELECT j->>'type', j->>'totalFeatures', j->>'numberMatched', "
                                  "j->>'numberReturned', (j->'crs')::VARCHAR FROM doc").fetchone()
        con.execute("CREATE TABLE f AS SELECT row_number() OVER () n, x f FROM "
                    "(SELECT unnest(from_json(j->'features', '[\"JSON\"]')) x FROM doc)")
        out["features"] = con.execute("SELECT count(*), count(DISTINCT f->>'id') FROM f").fetchone()
        keys = [r[0] for r in con.execute("SELECT DISTINCT unnest(json_keys(f->'properties')) k FROM f ORDER BY 1").fetchall()]
        cols = []
        for k in keys:
            e = "'" + k.replace("'", "''") + "'"
            d, nulls, typ, lo, hi = con.execute(
                f"SELECT count(DISTINCT f->'properties'->>{e}), count(*) FILTER (WHERE f->'properties'->>{e} IS NULL), "
                f"string_agg(DISTINCT json_type(f->'properties'->{e}), '/'), min(f->'properties'->>{e}), "
                f"max(f->'properties'->>{e}) FROM f").fetchone()
            rec = [k, typ, d, nulls]
            if d <= 20:
                rec.append(con.execute(f"SELECT list(DISTINCT f->'properties'->>{e}) FROM f").fetchone()[0])
            else:
                rec.append([str(lo)[:40], str(hi)[:40]])
            cols.append(rec)
        out["columns"] = cols
        out["sample"] = [r[0] for r in con.execute("SELECT (f->'properties')::VARCHAR FROM f ORDER BY n LIMIT 3").fetchall()]
        out["ids"] = con.execute("SELECT min(f->>'id'), max(f->>'id') FROM f").fetchone()
        out["geometry"] = con.execute(
            "SELECT string_agg(DISTINCT f->'geometry'->>'type', '/'), count(*) FILTER (WHERE f->'geometry' IS NULL "
            "OR f->>'geometry' = 'null'), substr((f->'geometry'->'coordinates')::VARCHAR, 1, 60) FROM f GROUP BY 3 "
            "ORDER BY 3 LIMIT 1").fetchone()
        out["geometry_name"] = con.execute("SELECT any_value(f->>'geometry_name') FROM f").fetchone()[0]
    except duckdb.Error as e:
        out["error"] = str(e)[:400]
        out["start"] = path.read_bytes()[:300].decode("utf-8", "replace")
    finally:
        con.close()
    note("bdl layer " + name, out)


def main() -> None:
    types = capabilities()
    wanted = [t["name"] for t in types if t["name"] and re.search(r"nadle|rdlp|le[sś]nictw|dyrekc", t["name"], re.I)]
    print("[wanted]", wanted, flush=True)
    if wanted:
        describe(wanted)
    counts = {n: hits(n) for n in wanted}
    note("bdl hits", counts)
    for n in wanted:
        if counts.get(n) is not None and counts[n] <= 20000:
            path = TMP / (re.sub(r"\W", "_", n) + ".json")
            _, info = fetch(url(request="GetFeature", typeNames=n, outputFormat="application/json"), to=path)
            if str(info.get("status")) == "200" and path.exists():
                profile(path, n, info)
            else:
                note("bdl layer failed " + n, info)
    print("[budget_left]", BUDGET, flush=True)


if __name__ == "__main__":
    main()
