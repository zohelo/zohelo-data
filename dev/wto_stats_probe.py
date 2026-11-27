"""Read-only probe of the WTO bulk files (GL-020, Task #3317), 27 Nov 2026. GET only, one request at a time.

Downloads to the runner's disk only (nothing written to R2): robots.txt, the bulk page, the BaTIS BPM6 codes and data
ZIPs; lists members and profiles the data CSV with DuckDB. Deleted before merge (never on the PR branch).
"""

import json
from pathlib import Path
import re
import sys
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAGE = "https://www.wto.org/english/res_e/statis_e/trade_datasets_e.htm"
S = requests.Session()
S.headers["User-Agent"] = UA


def get(url, stream=False):
    time.sleep(2)
    return S.get(url, timeout=(30, 900), stream=stream)


def note(title, data):
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, data if len(text) <= 3800 else {"truncated": text[:3700]})


def main(argv):
    out = {}
    r = get("https://www.wto.org/robots.txt")
    out["robots"] = {"status": r.status_code, "text": r.text[:1000]}
    r = get(PAGE)
    links = sorted(set(re.findall(r'href="([^"]+\.(?:zip|csv|xlsx|pdf)[^"]*)"', r.text, re.I)))
    out["page"] = {"status": r.status_code, "links": links[:40]}
    note("wto page and robots", out)
    work = Path(tempfile.mkdtemp())
    codes = [x for x in links if "BATIS_codes_BPM6" in x]
    data = [x for x in links if "BATIS_data_BPM6" in x]
    info = {}
    for label, link in (("codes", codes[0]), ("data", data[0])):
        url = requests.compat.urljoin(PAGE, link)
        t = time.monotonic()
        path = work / f"{label}.zip"
        with get(url, stream=True) as resp:
            with path.open("wb") as fh:
                for chunk in resp.iter_content(1 << 20):
                    fh.write(chunk)
            info[label] = {"url": url, "status": resp.status_code, "bytes": path.stat().st_size,
                           "lm": resp.headers.get("last-modified"), "etag": resp.headers.get("etag"),
                           "ct": resp.headers.get("content-type"), "s": round(time.monotonic() - t)}
        with zipfile.ZipFile(path) as z:
            info[label]["members"] = [(i.filename, i.file_size, i.compress_size) for i in z.infolist()][:20]
    note("wto batis downloads", info)
    with zipfile.ZipFile(work / "codes.zip") as z:
        for name in z.namelist()[:6]:
            body = z.read(name)
            if name.lower().endswith((".csv", ".txt")):
                note(f"codes member {name}", {"head": body[:3200].decode("utf-8", "replace")})
            else:
                note(f"codes member {name}", {"bytes": len(body), "magic": body[:8].hex()})
    with zipfile.ZipFile(work / "data.zip") as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            head = fh.read(3000).decode("utf-8", "replace")
        note("data head", {"member": name, "head": head})
        z.extract(name, work)
    csv = work / name
    con = duckdb.connect()
    con.sql(f"create table d as select * from read_csv('{csv}', all_varchar=true, header=true)")
    cols = [c[0] for c in con.sql("describe d").fetchall()]
    prof = {"columns": cols, "rows": con.sql("select count(*) from d").fetchone()[0]}
    for c in cols:
        prof[f"distinct:{c}"] = con.sql(f'select count(distinct "{c}") from d').fetchone()[0]
    note("data profile 1", prof)
    p2 = {}
    for c in cols:
        if prof[f"distinct:{c}"] <= 40:
            p2[c] = con.sql(f'select "{c}", count(*) from d group by 1 order by 2 desc').fetchall()
        else:
            p2[c] = con.sql(f'select min("{c}"), max("{c}") from d').fetchall()
    note("data profile 2", p2)
    pl = " or ".join(f"\"{c}\" in ('PL','POL','616')" for c in cols)
    p3 = {c: con.sql(f"select count(*) from d where \"{c}\" in ('PL','POL','616')").fetchone()[0] for c in cols}
    p3["sample_pl"] = con.sql(f"select * from d where {pl} limit 6").fetchall()
    p3["pl_any"] = con.sql(f"select count(*) from d where {pl}").fetchone()[0]
    note("data poland", p3)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        annotate("error", "probe failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
        sys.exit(1)
