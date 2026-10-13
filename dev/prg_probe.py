"""Probe PRG layers with DuckDB spatial: fields, rows, CRS, geometry types (no R2 writes)."""
import tempfile
import time
import zipfile
from pathlib import Path

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URL = "https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip"
out = {}
with tempfile.TemporaryDirectory() as t:
    zpath = Path(t) / "prg.zip"
    started = time.monotonic()
    with requests.get(URL, headers={"User-Agent": UA}, stream=True, timeout=900) as r, zpath.open("wb") as f:
        for block in r.iter_content(8 << 20):
            f.write(block)
    out["download_s"] = round(time.monotonic() - started)
    zipfile.ZipFile(zpath).extractall(t)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    for shp in sorted(Path(t).glob("*.shp")):
        started = time.monotonic()
        cols = con.execute("DESCRIBE SELECT * FROM ST_Read(?)", [str(shp)]).fetchall()
        n, types = con.execute("SELECT count(*), list(DISTINCT ST_GeometryType(geom)) FROM ST_Read(?)", [str(shp)]).fetchone()
        out[shp.stem] = {"rows": n, "geom_types": [str(x) for x in types],
                         "fields": [f"{c[0]}:{c[1]}" for c in cols if c[0] != "geom"],
                         "read_s": round(time.monotonic() - started)}
    out["prj"] = next(Path(t).glob("A01*.prj")).read_text()[:120]
annotate("notice", "prg probe", out)
