"""Dev probe of WIPO IP Statistics (register GL-032, #1262): read-only, writes nothing to R2.

Reads https://www.wipo.int/en/web/ip-statistics once, lists the ZIP/XLSX links, downloads the patent, trademark and
design indicator ZIPs one at a time (10 s apart) and prints the shape of every member: CSV header, first lines, row
count, per-column distinct counts and the most frequent values of low-cardinality columns, a few Poland rows; XLSX
sheets and their first rows. Output goes to the job log only (no files are kept). Deleted before merge.
"""

from __future__ import annotations

import io
import json
import re
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import duckdb
import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT, links_matching

PAGE = "https://www.wipo.int/en/web/ip-statistics"
WANT = [r"wipo-data-patent-indicators\.zip", r"wipo-data-trademark-indicators\.zip", r"design-indicators\.zip"]


def log(title, data):
    print(f"### {title}: {json.dumps(data, default=str, ensure_ascii=False)}", flush=True)


def profile_csv(name: str, body: bytes, summary: dict) -> None:
    text_head = body[:3000].decode("utf-8", errors="replace").splitlines()[:12]
    log(f"{name} head", text_head)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "m.csv"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            sniff = con.execute(f"select Delimiter, Quote, HasHeader, SkipRows, Columns from sniff_csv('{path}', sample_size=-1)").fetchall()
            log(f"{name} sniff", sniff)
            con.execute(f"create table t as select * from read_csv('{path}', all_varchar=true, sample_size=-1)")
        except duckdb.Error as exc:
            log(f"{name} csv error", str(exc)[:500])
            return
        cols = [r[0] for r in con.execute("describe t").fetchall()]
        n = con.execute("select count(*) from t").fetchone()[0]
        prof = {}
        for c in cols:
            d = con.execute(f'select count(distinct "{c}"), count(*) filter (where "{c}" is null) from t').fetchone()
            entry = {"distinct": d[0], "nulls": d[1]}
            if d[0] <= 120:
                entry["top"] = con.execute(f'select "{c}", count(*) from t group by 1 order by 2 desc limit 40').fetchall()
            else:
                entry["sample"] = [r[0] for r in con.execute(f'select distinct "{c}" from t limit 8').fetchall()]
            prof[c] = entry
        log(f"{name} rows={n} columns", prof)
        cond = " or ".join(f"\"{c}\" in ('Poland','PL','POL','Polish Patent Office')" for c in cols)
        log(f"{name} poland rows", con.execute(f"select * from t where {cond} limit 25").fetchall())
        log(f"{name} poland count", con.execute(f"select count(*) from t where {cond}").fetchone()[0])
        summary[name] = {"rows": n, "columns": cols[:30]}


def profile_xlsx(name: str, body: bytes, summary: dict) -> None:
    sheets: dict = {}
    for c in xlsx_cells.cells(body):
        s = sheets.setdefault(c["sheet_name"], {"cells": 0, "max_row": 0, "max_col": 0, "first": []})
        s["cells"] += 1
        s["max_row"] = max(s["max_row"], c["row_number"])
        s["max_col"] = max(s["max_col"], c["column_number"])
        if c["row_number"] <= 12 and len(s["first"]) < 80:
            s["first"].append([c["cell_ref"], c["value_text"][:60]])
    log(f"{name} xlsx", sheets)
    summary[name] = {k: {kk: v[kk] for kk in ("cells", "max_row", "max_col")} for k, v in sheets.items()}


def main(argv: list[str]) -> int:
    del argv
    out: dict = {}
    r = requests.get(PAGE, headers={"User-Agent": USER_AGENT}, timeout=60)
    log("page", {"status": r.status_code, "url": r.url, "bytes": len(r.content)})
    links = links_matching(r.text, r.url, r"\.(zip|xlsx|csv)(\?|$)")
    log("links", links)
    out["links"] = links
    for pat in WANT:
        match = [u for u in links if re.search(pat, u)]
        if not match:
            out[pat] = "no link"
            continue
        url = match[0]
        time.sleep(10)
        t0 = time.monotonic()
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}, timeout=(30, 300))
        except requests.RequestException as exc:
            out[pat] = f"error {type(exc).__name__}: {str(exc)[:200]}"
            log(pat, out[pat])
            continue
        info = {"url": url, "final": resp.url, "status": resp.status_code, "bytes": len(resp.content),
                "seconds": round(time.monotonic() - t0, 1),
                "headers": {k: resp.headers.get(k) for k in ("Last-Modified", "ETag", "Content-Type", "Content-Length")}}
        log(pat, info)
        summary: dict = {}
        if resp.status_code == 200:
            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    members = [(i.filename, i.file_size, str(i.date_time)) for i in z.infolist()]
                    log(f"{pat} members", members)
                    info["members"] = members
                    for m in z.infolist():
                        body = z.read(m)
                        low = m.filename.lower()
                        if low.endswith(".csv") or low.endswith(".txt"):
                            profile_csv(m.filename, body, summary)
                        elif low.endswith(".xlsx"):
                            profile_xlsx(m.filename, body, summary)
            except zipfile.BadZipFile as exc:
                log(f"{pat} not a zip", {"error": str(exc), "first": resp.content[:300].decode("latin-1")})
        info["summary"] = summary
        out[pat] = info
    annotate("notice", "wipo probe", {k: (v if k == "links" else {kk: vv for kk, vv in v.items() if kk != "summary"} if isinstance(v, dict) else v) for k, v in out.items()})
    for pat in WANT:
        if isinstance(out.get(pat), dict):
            annotate("notice", f"wipo probe {pat}", out[pat].get("summary", {}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
