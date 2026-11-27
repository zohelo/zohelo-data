"""Read-only probe of the WTO bulk files (GL-020, Task #3317), 27 Nov 2026, second pass. GET only, one request.

Downloads the BaTIS BPM6 data ZIP to the runner's disk only (nothing written to R2), and profiles its CSV member
(3.1 GB) and the codes workbook with DuckDB and xlsx_cells. Deleted before merge (never on the PR branch).
"""

from collections import defaultdict
import json
from pathlib import Path
import sys
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URL = "https://www.wto.org/english/res_e/statis_e/daily_update_e/OECD-WTO_BATIS_data_BPM6-1.zip"


def note(title, data):
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, data if len(text) <= 3800 else {"truncated": text[:3700]})


def q(con, sql):
    return [[str(v) for v in r] for r in con.sql(sql).fetchall()]


def main(argv):
    work = Path(tempfile.mkdtemp())
    path = work / "data.zip"
    t = time.monotonic()
    with requests.get(URL, headers={"User-Agent": UA}, timeout=(30, 900), stream=True) as resp:
        resp.raise_for_status()
        with path.open("wb") as fh:
            for chunk in resp.iter_content(8 << 20):
                fh.write(chunk)
    seconds = round(time.monotonic() - t)
    with zipfile.ZipFile(path) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        xlsx = [n for n in z.namelist() if n.lower().endswith(".xlsx")]
        with z.open(csvs[0]) as fh:
            head = fh.read(2500).decode("utf-8", "replace")
        z.extract(csvs[0], work)
        book = z.read(xlsx[0])
    note("data head", {"download_s": seconds, "csv": csvs, "head": head})
    sheets = defaultdict(list)
    for c in xlsx_cells.cells(book):
        sheets[c["sheet_name"]].append(c)
    summary = {}
    for name, cells in sheets.items():
        rows = defaultdict(dict)
        for c in cells:
            rows[c["row_number"]][c["column_number"]] = c["value_text"]
        keys = sorted(rows)
        summary[name] = {"rows": len(keys), "first": [rows[k] for k in keys[:6]]}
    note("codes workbook sheets", {k: v["rows"] for k, v in summary.items()})
    note("codes workbook heads", summary)
    eu = {}
    for name, cells in sheets.items():
        hits = [c["value_text"] for c in cells if any(s in str(c["value_text"]) for s in ("EU", "Euro", "Poland", "World"))]
        eu[name] = hits[:30]
    note("codes workbook EU/Poland/World", eu)
    csv = work / csvs[0]
    con = duckdb.connect(str(work / "p.duckdb"))
    con.sql("set preserve_insertion_order = false")
    con.sql(f"create table d as select * from read_csv('{csv}', all_varchar=true, header=true)")
    cols = [c[0] for c in con.sql("describe d").fetchall()]
    prof = {"columns": cols, "rows": q(con, "select count(*) from d")[0][0]}
    for c in cols:
        prof[f"distinct:{c}"] = q(con, f'select approx_count_distinct("{c}") from d')[0][0]
        prof[f"nulls:{c}"] = q(con, f'select count(*) from d where "{c}" is null')[0][0]
    note("data profile 1", prof)
    p2 = {}
    for c in cols:
        if int(prof[f"distinct:{c}"]) <= 30:
            p2[c] = q(con, f'select "{c}", count(*) from d group by 1 order by 2 desc')
        else:
            p2[c] = q(con, f'select min("{c}"), max("{c}") from d')
    note("data profile 2", p2)
    hits = {c: q(con, f"select \"{c}\", count(*) from d where \"{c}\" in ('PL','POL','616','Poland') group by 1")
            for c in cols}
    note("data poland columns", hits)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        annotate("error", "probe failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
        sys.exit(1)
