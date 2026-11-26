"""Read-only probe 2 of the GIOŚ yearly archive (#3119): every yearly ZIP downloaded to the runner's disk (no R2
writes), decoded by ingestion/to_bronze/gios_air_quality/archive.py and passed through the Silver SQL locally;
profiles per year. Plus the station metadata file and the headers of one download. One request at a time, 5 s
apart."""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_bronze.gios_air_quality import archive
from warehouse import entity

BASE = "https://powietrze.gios.gov.pl"
S = requests.Session()
S.headers["User-Agent"] = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
FIVE = ("PM10", "PM2.5", "NO2", "O3", "SO2")


def get(url: str, **kw) -> requests.Response:
    time.sleep(5)
    return S.get(url, timeout=(30, 600), **kw)


def profile(con, bronze: Path, silver: Path) -> dict:
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    b, s = f"read_parquet('{bronze}')", f"read_parquet('{silver}')"
    out = {
        "averaging": q(f"select averaging_time, count(*) from {b} group by 1 order by 2 desc"),
        "units": q(f"select coalesce(unit,'<none>'), count(*) from {b} group by 1 order by 2 desc"),
        "indicators": [r[0] for r in q(f"select distinct indicator from {b} order by 1")],
        "non_numeric": q(f"select value, count(*) from {b} where try_cast(value as double) is null group by 1 order by 2 desc limit 15"),
        "negative": q(f"select indicator, count(*), min(try_cast(value as double)) from {b} where try_cast(value as double) < 0 group by 1 limit 10"),
        "zeros": q(f"select count(*) from {b} where try_cast(value as double) = 0")[0][0],
        "dup_key": q(f"select count(*) from (select 1 from {s} group by station_code, indicator, averaging_time, measured_at having count(*) > 1)")[0][0],
        "dup_key_sensor": q(f"select count(*) from (select 1 from {s} group by station_code, indicator, averaging_time, sensor_code, measured_at having count(*) > 1)")[0][0],
        "dup_sample": q(f"select station_code, indicator, averaging_time, list(distinct member), list(distinct sensor_code), count(*) from {s} group by station_code, indicator, averaging_time, measured_at having count(*) > 1 limit 3"),
        "range": q(f"select averaging_time, min(measured_at), max(measured_at), min(measured_on), max(measured_on) from {s} group by 1"),
        "off_hour": q(f"select count(*) from {s} where averaging_time = '1g' and (minute(measured_at) <> 0 or second(measured_at) <> 0)")[0][0],
        "off_midnight": q(f"select averaging_time, count(*) from {s} where averaging_time <> '1g' and cast(measured_at as time) <> time '00:00' group by 1")[:5],
        "five_1g_stations": q(f"select indicator, count(distinct station_code), count(*) from {s} where averaging_time = '1g' and indicator in {FIVE} group by 1 order by 1"),
        "five_units": q(f"select indicator, coalesce(unit, '<none>'), count(*) from {s} where indicator in {FIVE} group by 1, 2 order by 1"),
        "max_five": q(f"select indicator, averaging_time, max(value) from {s} where indicator in {FIVE} group by 1, 2 order by 1, 2"),
        "null_keys": q(f"select count(*) filter (where station_code is null), count(*) filter (where indicator is null), count(*) filter (where averaging_time is null), count(*) filter (where measured_at is null) from {s}")[0],
    }
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    page = get(f"{BASE}/pjp/archives")
    html = page.text
    i = html.find("downloadFile")
    print("HREF_SNIPPET", re.sub(r"\s+", " ", html[max(0, i - 300) : i + 200]))
    years = {}
    for m in re.finditer(r'downloadFile/(\d+)"[^>]*>(.*?)</a>', html, re.S):
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(2))).strip()
        y = re.search(r"Wyniki pomiarów z (\d{4}) roku", text)
        if y:
            years[int(y.group(1))] = int(m.group(1))
        elif "Metadane" in text:
            years["meta"] = int(m.group(1))
    time.sleep(5)
    h = S.head(f"{BASE}/pjp/archives/downloadFile/{years[2025]}", timeout=(30, 120), allow_redirects=True)
    print("HEAD", h.status_code, json.dumps(dict(h.headers)))
    con = duckdb.connect()
    con.execute("install excel; load excel")
    # station metadata
    meta = get(f"{BASE}/pjp/archives/downloadFile/{years['meta']}")
    print("META", meta.status_code, len(meta.content), meta.headers.get("content-type"), meta.headers.get("content-disposition"), meta.content[:4])
    with tempfile.TemporaryDirectory() as t:
        p = Path(t) / "meta.xlsx"
        p.write_bytes(meta.content)
        try:
            import zipfile, io  # noqa: E401
            from ingestion.common import xlsx_cells
            sheets = xlsx_cells.sheets(zipfile.ZipFile(io.BytesIO(meta.content)))
            print("META_SHEETS", sheets)
            for name, _ in sheets:
                rows = con.execute(f"select * from read_xlsx('{p}', sheet='{name}', header=false, all_varchar=true) limit 4").fetchall()
                n = con.execute(f"select count(*) from read_xlsx('{p}', sheet='{name}', header=false, all_varchar=true)").fetchone()[0]
                print("META_SHEET", name, n, json.dumps(rows, ensure_ascii=False)[:3000])
        except Exception as exc:
            print("META_FAIL", type(exc).__name__, str(exc)[:300])
    picks = [int(a) for a in sys.argv[1:]] or sorted(y for y in years if y != "meta")
    summary = {}
    for year in picks:
        started = time.monotonic()
        resp = get(f"{BASE}/pjp/archives/downloadFile/{years[year]}")
        receipt = {"entity": f"archive_{year}", "sha256": "probe", "version_token": "probe", "ingested_at": "20261126T000000Z"}
        info = {"bytes": len(resp.content), "download_s": round(time.monotonic() - started, 1)}
        with tempfile.TemporaryDirectory() as t:
            bronze, silver = Path(t) / "b.parquet", Path(t) / "s.parquet"
            try:
                t0 = time.monotonic()
                rep = archive.decode(resp.content, receipt, bronze)
                info["decode_s"] = round(time.monotonic() - t0, 1)
                info.update({k: rep[k] for k in ("members", "rows", "skipped", "other_sheets", "layouts")})
                info["layouts"] = {k: len(v) for k, v in rep["layouts"].items()}
                t0 = time.monotonic()
                try:
                    res = entity.apply("gios_air_archive_values", str(bronze), silver, same_rows=True, memory_limit="8GB")
                    info["silver_rows"] = res["rows"]
                    info["silver_bytes"], info["bronze_bytes"] = res["bytes"], bronze.stat().st_size
                    info["silver_s"] = round(time.monotonic() - t0, 1)
                    info["profile"] = profile(con, bronze, silver)
                except Exception as exc:
                    info["silver_error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
            except Exception as exc:
                info["decode_error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
        print("YEAR", year, json.dumps(info, default=str, ensure_ascii=False))
        summary[year] = {k: info.get(k) for k in ("bytes", "rows", "silver_rows", "decode_error", "silver_error")}
        summary[year]["dup"] = (info.get("profile") or {}).get("dup_key")
        (OUT / f"year_{year}.json").write_text(json.dumps(info, default=str, ensure_ascii=False, indent=1))
    annotate("notice", "gios archive probe 2", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
