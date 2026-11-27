"""Read-only Dev probe for wody_polskie_apgw (#3313): the dane.gov.pl record of dataset 599 and its resources, a HEAD
of the aPGW 2023 geodatabase ZIP, then the ZIP downloaded to the runner's temp folder (never to R2) and every layer of
the File Geodatabase listed with DuckDB spatial: feature count, fields, geometry type, CRS, and per field the non-null
count, distinct count and up to 25 values when it has few. Full listing in the job log; compact annotations. Never
merged."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

API = "https://api.dane.gov.pl/1.4/datasets/599"
UA = {"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"}


def get(url: str) -> requests.Response:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=(30, 120))
            if r.status_code == 200 or 400 <= r.status_code < 500:
                return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
        time.sleep(15 * (attempt + 1))
    raise RuntimeError(url)


def main(argv) -> int:
    rec = get(API)
    a = rec.json()["data"]["attributes"]
    record = {k: a.get(k) for k in ("title", "license_name", "license_condition_responsibilities", "update_frequency",
                                     "created", "modified", "last_modified_resource", "verified", "resources_count")}
    print("RECORD", json.dumps(a, ensure_ascii=False, default=str)[:6000], flush=True)
    time.sleep(2)
    res = get(f"{API}/resources?per_page=100&page=1").json()
    resources = []
    for item in res.get("data", []):
        r = item.get("attributes") or {}
        print("RESOURCE", item.get("id"), json.dumps(r, ensure_ascii=False, default=str)[:3000], flush=True)
        resources.append({"id": item.get("id"), "title": r.get("title"), "format": r.get("format"),
                          "file_url": r.get("file_url"), "file_size": r.get("file_size"), "created": r.get("created"),
                          "modified": r.get("modified"), "data_date": r.get("data_date"), "type": r.get("type")})
    annotate("notice", "apgw probe dataset 599", {"record": record, "resources": resources,
                                                   "links": res.get("links"), "meta": res.get("meta")})
    zips = [r for r in resources if (r["file_url"] or "").lower().endswith(".gdb.zip")]
    if len(zips) != 1:
        annotate("error", "apgw probe resource", {"gdb_zips": zips})
        return 1
    url = zips[0]["file_url"]
    head, http = bulk_file.probe(url)
    headers = {k: head.headers.get(k) for k in ("Content-Length", "Last-Modified", "ETag", "Content-Type")} if head else {}
    with tempfile.TemporaryDirectory(prefix="apgw-probe-") as tmp:
        path = Path(tmp) / "apgw.zip"
        started = time.monotonic()
        digest, size, got, final, detail = bulk_file.download(url, path, "zip", read_timeout=600)
        names = zipfile.ZipFile(path).namelist()
        folders = sorted({n.split("/")[0] for n in names})
        gdbs = sorted({n.split(".gdb/")[0] + ".gdb" for n in names if ".gdb/" in n})
        annotate("notice", "apgw probe download", {"url": url, "head_http": http, "head": headers, "sha256": digest,
                                                    "bytes": size, "seconds": round(time.monotonic() - started),
                                                    "zip_members": len(names), "top": folders[:10], "gdbs": gdbs,
                                                    "sample_members": names[:8]})
        con = duckdb.connect()
        con.execute("SET memory_limit = '8GB'")
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except duckdb.Error as exc:
            annotate("error", "apgw probe spatial", {"error": str(exc)[:500], "duckdb": duckdb.__version__})
            return 1
        gdb = f"/vsizip/{path}/{gdbs[0]}" if gdbs else str(path)
        try:
            meta = con.execute("SELECT * FROM ST_Read_Meta(?)", [gdb]).fetchall()
            cols = [d[0] for d in con.description]
        except duckdb.Error as exc:
            print("vsizip failed", exc, flush=True)
            zipfile.ZipFile(path).extractall(Path(tmp) / "x")
            gdb = str(Path(tmp) / "x" / gdbs[0])
            meta = con.execute("SELECT * FROM ST_Read_Meta(?)", [gdb]).fetchall()
            cols = [d[0] for d in con.description]
        info = dict(zip(cols, meta[0]))
        print("DRIVER", info.get("driver_short_name"), flush=True)
        layers = info["layers"]
        compact = []
        for layer in layers:
            name = layer["name"]
            geoms = [{"name": g.get("name"), "type": g.get("type"),
                      "crs": (g.get("crs") or {}).get("auth_name", "") + ":" + str((g.get("crs") or {}).get("auth_code"))}
                     for g in layer.get("geometry_fields") or []]
            fields = [f["name"] for f in layer.get("fields") or []]
            types = {f["name"]: f["type"] for f in layer.get("fields") or []}
            print("LAYER", name, layer.get("feature_count"), json.dumps(geoms), json.dumps(types), flush=True)
            prof = {}
            try:
                q = ", ".join(f'count("{f}") AS "n_{f}", count(DISTINCT "{f}") AS "d_{f}"' for f in fields)
                row = con.execute(f"SELECT count(*) AS rows{', ' + q if q else ''} FROM ST_Read(?, layer = ?)",
                                  [gdb, name]).fetchone()
                rows = row[0]
                for i, f in enumerate(fields):
                    nn, dd = row[1 + 2 * i], row[2 + 2 * i]
                    prof[f] = {"nonnull": nn, "distinct": dd}
                    if dd <= 25:
                        vals = con.execute(f'SELECT CAST("{f}" AS VARCHAR) v, count(*) FROM ST_Read(?, layer = ?) '
                                           "GROUP BY 1 ORDER BY 2 DESC", [gdb, name]).fetchall()
                        prof[f]["values"] = vals
                    else:
                        ex = con.execute(f'SELECT CAST("{f}" AS VARCHAR) FROM ST_Read(?, layer = ?) '
                                         f'WHERE "{f}" IS NOT NULL LIMIT 3', [gdb, name]).fetchall()
                        prof[f]["examples"] = [e[0] for e in ex]
                print("PROFILE", name, rows, json.dumps(prof, ensure_ascii=False, default=str), flush=True)
            except duckdb.Error as exc:
                rows = None
                print("PROFILE-ERROR", name, exc, flush=True)
            compact.append({"layer": name, "n": layer.get("feature_count"), "rows": rows,
                            "geom": [g["type"] + "/" + g["crs"] for g in geoms], "fields": fields})
        annotate("notice", "apgw probe layers", {"driver": info.get("driver_short_name"), "count": len(layers),
                                                  "layers": [[c["layer"], c["rows"], c["geom"]] for c in compact]})
        chunk, part = [], 1
        for c in compact:
            chunk.append([c["layer"], c["fields"]])
            if len(json.dumps(chunk)) > 3300:
                annotate("notice", f"apgw probe fields {part}", chunk[:-1])
                chunk, part = chunk[-1:], part + 1
                if part > 5:
                    break
        if chunk and part <= 5:
            annotate("notice", f"apgw probe fields {part}", chunk)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
