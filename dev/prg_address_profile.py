"""Dev probe 2 (PL-ENV-007, #3106): GUGiK PRG address points, read-only profile of the address layer.

Downloads the national SHP package once (0.56 GB) to the runner's temporary disk, unpacks only the address-point
layer PRG_PunktyAdresowe_POLSKA.* with 7z (its members are Deflate64, which Python's zipfile cannot read), and
profiles it with DuckDB spatial (attributes, row count, nulls, key shapes, closed lists, coordinates), then writes a
trial Parquet of every attribute as text plus WKB to measure size, time and memory. Writes nothing to R2.
"""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import resource
import shutil
import subprocess
import tempfile
import time

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

URL = "https://opendata.geoportal.gov.pl/prg/adresy/PunktyAdresowe/POLSKA.zip"
LEFT = {"n": 9}


def note(title, data, level="notice"):
    print(f"[{title}] {json.dumps(data, default=str)}", flush=True)
    if LEFT["n"] > 0:
        LEFT["n"] -= 1
        annotate(level, title, data)


def rss_mb() -> int:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def main() -> int:
    timing = {}
    with tempfile.TemporaryDirectory(prefix="prg-addr-") as tmp:
        work = Path(tmp)
        disk = shutil.disk_usage(tmp)
        started = time.monotonic()
        raw = work / "package.zip"
        digest, size = sha256(), 0
        with requests.get(URL, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}, stream=True,
                          timeout=(30, 300)) as r:
            r.raise_for_status()
            headers = {k: r.headers.get(k) for k in ("Content-Length", "Last-Modified", "ETag")}
            with raw.open("wb") as handle:
                for block in r.iter_content(8 << 20):
                    handle.write(block)
                    digest.update(block)
                    size += len(block)
        timing["download_s"] = round(time.monotonic() - started)
        started = time.monotonic()
        out = work / "unz"
        done = subprocess.run(["7z", "x", "-bd", "-y", f"-o{out}", str(raw), "PRG_PunktyAdresowe_POLSKA.*"],
                              capture_output=True, text=True, timeout=1800)
        timing["unpack_s"] = round(time.monotonic() - started)
        files = {p.name: p.stat().st_size for p in out.iterdir()} if out.exists() else {}
        note("prg address download and unpack", {
            "headers": headers, "bytes": size, "sha256": digest.hexdigest()[:16], "timing": timing,
            "disk_free_gb_before": round(disk.free / 1e9, 1), "unpack_rc": done.returncode,
            "unpack_tail": (done.stdout + done.stderr)[-300:], "files": files,
            "disk_free_gb_after": round(shutil.disk_usage(tmp).free / 1e9, 1)})
        shp = str(out / "PRG_PunktyAdresowe_POLSKA.shp")
        con = duckdb.connect()
        con.execute("SET memory_limit = '8GB'")
        con.execute(f"SET temp_directory = '{work}'")
        con.execute("INSTALL spatial; LOAD spatial;")
        cols = q(con, f"DESCRIBE SELECT * FROM ST_Read('{shp}')")
        names = [c[0] for c in cols if c[0] != "geom"]
        started = time.monotonic()
        con.execute(f"CREATE TABLE a AS SELECT * EXCLUDE (geom), ST_AsWKB(geom) AS wkb, ST_X(geom) AS x, "
                    f"ST_Y(geom) AS y, ST_GeometryType(geom)::VARCHAR AS gt FROM ST_Read('{shp}')")
        timing["read_s"] = round(time.monotonic() - started)
        n = con.execute("SELECT count(*) FROM a").fetchone()[0]
        note("prg address layer attributes", {"columns": cols, "rows": n, "timing": timing, "rss_mb": rss_mb()})
        prof = {}
        for c in names:
            row = con.execute(
                f'SELECT count(*) FILTER (WHERE "{c}" IS NULL), count(*) FILTER (WHERE trim(CAST("{c}" AS VARCHAR)) '
                f"= ''), approx_count_distinct(\"{c}\"), max(length(CAST(\"{c}\" AS VARCHAR))) FROM a").fetchone()
            prof[c] = list(row)
        note("prg address column profile (null, empty, approx distinct, max length)", prof)
        low = [c for c in names if prof[c][2] < 300]
        note("prg address closed lists", {c: q(con, f'SELECT CAST("{c}" AS VARCHAR), count(*) FROM a GROUP BY 1 '
                                                 "ORDER BY 2 DESC LIMIT 25") for c in low})
        shapes = {}
        for c in names:
            if c not in low:
                shapes[c] = q(con, f"SELECT regexp_replace(regexp_replace(CAST(\"{c}\" AS VARCHAR), '[0-9]', '9', "
                                   f"'g'), '[A-Za-zÀ-ž]', 'a', 'g') s, count(*) FROM a GROUP BY 1 ORDER BY 2 DESC "
                                   "LIMIT 6")
        note("prg address value shapes (digits 9, letters a)", shapes)
        dup = {}
        for c in names:
            if prof[c][2] > n * 0.5:
                dup[c] = con.execute(f'SELECT count(*), sum(k) FROM (SELECT "{c}", count(*) k FROM a GROUP BY 1 '
                                     "HAVING count(*) > 1)").fetchone()
        note("prg address key candidates and geometry", {
            "repeated_values (keys, rows)": {c: list(v) for c, v in dup.items()},
            "geometry_types": q(con, "SELECT gt, count(*) FROM a GROUP BY 1"),
            "xy_range": q(con, "SELECT min(x), max(x), min(y), max(y), count(*) FILTER (WHERE x IS NULL) FROM a"),
            "sample": q(con, "SELECT * EXCLUDE (wkb) FROM a USING SAMPLE 3 ROWS"),
            "rss_mb": rss_mb()})
        started = time.monotonic()
        target = work / "bronze.parquet"
        text = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in names)
        con.execute(f"COPY (SELECT {text}, wkb FROM a) TO '{target}' (FORMAT parquet, COMPRESSION zstd, "
                    "ROW_GROUP_SIZE 100000)")
        timing["parquet_s"] = round(time.monotonic() - started)
        sizes = q(con, f"SELECT path_in_schema, sum(total_compressed_size) FROM parquet_metadata('{target}') "
                       "GROUP BY 1 ORDER BY 2 DESC")
        note("prg address trial parquet", {"bytes": target.stat().st_size, "by_column": sizes, "timing": timing,
                                          "rss_mb": rss_mb()})
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
