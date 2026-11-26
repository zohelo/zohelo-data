"""Second read-only probe of the PIG-PIB MIDAS WFS (PL-ENV-015, part of #2870).

Probe 45817073166 got 200 for capabilities, DescribeFeatureType and resultType=hits, then an immediate 503 ("No
server is available to handle this request") for every full GetFeature and the ArcGIS REST root. This probe asks
slowly: 15 s between requests, a 503 retried after 60, 120 and 240 s (Retry-After honoured), and pages of `count`
features (ImplementsResultPaging TRUE) in GEOJSON, native CRS EPSG:2180. Per layer: pages, bytes, seconds, features,
then a DuckDB profile of the joined pages (keys, nulls, closed lists, dates, geometry). Also the opening text of the
MIDAS rules page. Nothing is written to R2. Annotations only (the job log is not readable from the session).
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import tempfile
import time
import urllib.parse

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
WFS = "https://cbdgmapa.pgi.gov.pl/arcgis/services/midas/MapServer/WFSServer"
LAYERS = [
    "MIDAS_-_udokumentowane_złoża_kopalin",
    "MIDAS_-_obszary_górnicze",
    "MIDAS_-_tereny_górnicze",
    "MIDAS___złoża_wybilansowane__wyselekcjonowane_dane_archiwalne_",
    "MIDAS_-_archiwalne_obszary_górnicze__wyselekcjonowane_dane_archiwalne_",
    "MIDAS_-_archiwalne_tereny_górnicze__wyselekcjonowane_dane_archiwalne_",
]
PAUSE = 15
LEFT = {"n": 10, "requests": 120}
LOG: list = []


def emit(title: str, data) -> None:
    print(f"[{title}] {json.dumps(data, ensure_ascii=False, default=str)}", flush=True)
    if LEFT["n"] > 0:
        LEFT["n"] -= 1
        annotate("notice", title, data)


def get(url: str):
    """GET with the pause, a 503/429/5xx retried after 60, 120, 240 s; returns (response bytes or None, info)."""
    waits = [60, 120, 240]
    while True:
        if LEFT["requests"] <= 0:
            return None, {"status": "budget"}
        LEFT["requests"] -= 1
        time.sleep(PAUSE)
        t0 = time.monotonic()
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 600))
            info = {"status": r.status_code, "bytes": len(r.content), "s": round(time.monotonic() - t0, 1),
                    "type": r.headers.get("content-type", "")[:40]}
            if r.status_code in (429, 500, 502, 503, 504) and waits:
                wait = int(r.headers.get("Retry-After", "0") or 0) or waits[0]
                LOG.append({"retry": info["status"], "wait": wait, "url": url[-80:]})
                waits.pop(0)
                time.sleep(wait)
                continue
            return (r.content if r.ok else None), info
        except requests.RequestException as e:
            info = {"status": f"{type(e).__name__}: {str(e)[:120]}"}
            if waits:
                LOG.append({"retry": info["status"][:40], "wait": waits[0]})
                time.sleep(waits.pop(0))
                continue
            return None, info


def rules_page() -> None:
    body, info = get("https://www.pgi.gov.pl/midas/zrodla-danych.html")
    text = ""
    if body:
        html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", body.decode("utf-8", "replace"), flags=re.S | re.I)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).replace("&nbsp;", " ")
        i = text.find("stanowią informację sektora publicznego")
        text = text[max(0, i - 1500): i + 400] if i >= 0 else text[:1500]
    emit("midas rules page zrodla-danych", {**info, "text": text})


def profile(path: Path, layer: str, pages: list) -> dict:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    out: dict = {"pages": pages}
    try:
        con.execute("CREATE TABLE t AS SELECT * FROM ST_Read(?)", [str(path)])
        cols = con.execute("DESCRIBE t").fetchall()
        out["rows"] = con.execute("SELECT count(*) FROM t").fetchone()[0]
        prof = []
        for name, typ, *_ in cols:
            if typ.upper().startswith("GEOMETRY"):
                g = con.execute(
                    f'SELECT round(min(st_xmin("{name}"))), round(max(st_xmax("{name}"))), round(min(st_ymin("{name}"))),'
                    f' round(max(st_ymax("{name}"))), count(*) FILTER (WHERE "{name}" IS NULL OR st_isempty("{name}")),'
                    f' count(*) FILTER (WHERE NOT st_isvalid("{name}")), round(avg(st_npoints("{name}"))),'
                    f' round(sum(st_area("{name}")) / 1e6) FROM t').fetchone()
                prof.append([name, "geom", list(g)])
                continue
            d, nulls = con.execute(f'SELECT count(DISTINCT "{name}"), count(*) FILTER (WHERE "{name}" IS NULL '
                                   f'OR trim("{name}"::VARCHAR) = \'\') FROM t').fetchone()
            rec = [name[:28], typ[:7], d, nulls]
            if d <= 10:
                rec.append([[str(v)[:24], k] for v, k in con.execute(
                    f'SELECT "{name}"::VARCHAR, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC').fetchall()])
            else:
                rec.append([str(x)[:22] if x is not None else None for x in con.execute(
                    f'SELECT min("{name}"::VARCHAR), max("{name}"::VARCHAR) FROM t').fetchone()])
            prof.append(rec)
        out["cols"] = prof
        names = [c[0] for c in cols]
        for key in ("OBJECTID", "Numer_złoża", "ID_konturu", "Numer_w_rejestrze", "Kod"):
            if key in names:
                out[f"dup_{key}"] = con.execute(
                    f'SELECT count(*) FROM (SELECT "{key}" FROM t GROUP BY 1 HAVING count(*) > 1)').fetchone()[0]
        if "Numer_złoża" in names and "Pole_złoża" in names:
            out["dup_numer_pole"] = con.execute('SELECT count(*) FROM (SELECT "Numer_złoża", "Pole_złoża" FROM t '
                                                'GROUP BY ALL HAVING count(*) > 1)').fetchone()[0]
        if "Lokalizacja" in names:
            out["lokalizacja_sample"] = [r[0][:90] for r in con.execute(
                'SELECT "Lokalizacja" FROM t WHERE "Lokalizacja" IS NOT NULL USING SAMPLE 3 ROWS').fetchall()]
        for c in [n for n in names if n.startswith("Data_")][:1]:
            out[f"shapes_{c}"] = con.execute(
                f'SELECT regexp_replace("{c}"::VARCHAR, \'[0-9]\', \'9\', \'g\'), count(*) FROM t GROUP BY 1 '
                f'ORDER BY 2 DESC LIMIT 4').fetchall()
    except duckdb.Error as e:
        out["error"] = str(e)[:500]
    finally:
        con.close()
    return out


def layer(name: str, count: int) -> None:
    features, pages, crs = [], [], None
    start = 0
    while True:
        url = (f"{WFS}?service=WFS&request=GetFeature&version=2.0.0&typeNames=midas:{urllib.parse.quote(name)}"
               f"&outputFormat=GEOJSON&count={count}&startIndex={start}")
        body, info = get(url)
        if body is None:
            pages.append(info)
            break
        try:
            doc = json.loads(body)
        except ValueError:
            pages.append({**info, "not_json": body[:120].decode("utf-8", "replace")})
            break
        got = doc.get("features") or []
        crs = crs or doc.get("crs")
        pages.append([start, info["bytes"], info["s"], len(got)])
        features += got
        if len(got) < count:
            break
        start += count
    if features and len(LOG) < 50:
        first = features[0]
        LOG.append({"layer": name[:30], "sample_props": {k: str(v)[:30] for k, v in first.get("properties", {}).items()},
                    "geom_type": (first.get("geometry") or {}).get("type"), "crs": crs})
    path = Path(tempfile.gettempdir()) / f"midas_{abs(hash(name))}.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "crs": crs, "features": features}))
    emit(f"midas {name[:60]}", profile(path, name, pages) if features else {"pages": pages})


def main() -> int:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    rules_page()
    for name in LAYERS:
        layer(name, count)
    emit("midas probe log", LOG[:30])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
