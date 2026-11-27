"""Second read-only probe for GL-018 (#3316): the order of the bulk CSV links on the database archive page, and the
current release's CSV ZIP (2026_Q3.1_AllData_After_20260930_CSV.zip) downloaded to the runner's disk (not R2) and
profiled with DuckDB. GET to unstats.un.org only, one request at a time, pauses, identifying User-Agent.
Everything is printed to the job log; compact notices. Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

import json
import re
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://unstats.un.org"
S = requests.Session()
S.headers.update({"User-Agent": UA})
EU = ("('40','56','100','191','196','203','208','233','246','250','276','300','348','372','380','428','440','442',"
      "'470','528','616','620','642','703','705','724','752')")
URL = f"{HOST}/sdgs/indicators/database/archive/2026_Q3.1_AllData_After_20260930_CSV.zip"


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, data if len(text) < 3800 else {"truncated": text[:3800]})


def main() -> int:
    page = S.get(f"{HOST}/sdgs/indicators/database/archive", timeout=(20, 120), headers={"Accept": "text/html"})
    order = re.findall(r'href="([^"]+_CSV\.zip)"', page.text)
    note("sdg probe3 archive order", {"first5": order[:5], "last5": order[-5:], "n": len(order),
                                      "after_files_in_page_order": [x.rsplit("/", 1)[-1] for x in order
                                                                    if "_AllData_After_" in x][:6]})
    time.sleep(3)
    start = time.monotonic()
    path = "/tmp/sdg_newest.zip"
    with S.get(URL, timeout=(20, 900), stream=True) as r, open(path, "wb") as f:
        info = {"st": r.status_code, "headers": {k: r.headers.get(k) for k in ("content-length", "last-modified", "etag")}}
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    info["minutes"] = round((time.monotonic() - start) / 60, 2)
    with zipfile.ZipFile(path) as z:
        info["members"] = [(m.filename, m.file_size, m.date_time) for m in z.infolist()]
        member = max(z.infolist(), key=lambda m: m.file_size)
        z.extract(member, "/tmp/sdg")
        csv = f"/tmp/sdg/{member.filename}"
    raw = open(csv, "rb").read(3000)
    info["bom"] = raw[:3].hex()
    info["head"] = raw[:1500].decode("utf-8", "replace")
    try:
        raw.decode("utf-8")
        info["utf8_head"] = True
    except UnicodeDecodeError as exc:
        info["utf8_head"] = str(exc)
    note("sdg probe3 file", info)
    con = duckdb.connect()
    try:
        con.execute(f"create table t as select * from read_csv('{csv}', all_varchar=true, header=true)")
        enc = "utf-8"
    except duckdb.Error as exc:
        note("sdg probe3 utf8 read failed", {"error": str(exc)[:1500]})
        con.execute(f"create table t as select * from read_csv('{csv}', all_varchar=true, header=true, encoding='latin-1')")
        enc = "latin-1"
    cols = [r[0] for r in con.execute("describe t").fetchall()]

    def q(s):
        return con.execute(s).fetchall()

    prof = {"encoding": enc, "columns": cols, "rows": q("select count(*) from t")[0][0]}
    prof["areas"] = q('select count(distinct "GeoAreaCode") from t')[0][0]
    prof["series"] = q('select count(distinct "SeriesCode") from t')[0][0]
    prof["releases"] = q('select "Release", count(*) from t group by 1') if "Release" in cols else None
    con.execute(f'create table e as select * from t where "GeoAreaCode" in {EU}')
    prof["rows_eu27"] = q("select count(*) from e")[0][0]
    prof["series_eu27"] = q('select count(distinct "SeriesCode") from e')[0][0]
    prof["rows_pl"] = q("""select count(*) from e where "GeoAreaCode" = '616'""")[0][0]
    note("sdg probe3 counts", prof)
    cols_prof = {c: q(f'select count(distinct "{c}"), count(*) filter (where "{c}" is null), '
                      f'count(*) filter (where "{c}" = \'NA\') from e')[0] for c in cols}
    note("sdg probe3 columns eu (distinct, null, NA)", cols_prof)
    dims = [c for c in cols if c.startswith("[")]
    keycols = ['"SeriesCode"', '"GeoAreaCode"', '"TimePeriod"', *[f'"{c}"' for c in dims
                                                                   if c not in ("[Nature]", "[Units]")]]
    small = {}
    for c in ("[Nature]", "[Units]", "[Reporting Type]", "[Observation Status]", "Time_Detail", "TimeCoverage",
              "BasePeriod", "Status", "Release"):
        if c in cols:
            small[c] = q(f'select "{c}", count(*) from e group by 1 order by 2 desc limit 25')
    small["value_non_numeric"] = q("""select "Value", count(*) from e where not regexp_full_match(coalesce("Value", ''),
        '-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?') group by 1 order by 2 desc limit 25""")
    small["timeperiod_shape"] = q("""select regexp_replace("TimePeriod", '[0-9]', '9', 'g'), count(*) from e group by 1""")
    small["dup_key_rows"] = q(f"select count(*), sum(n) from (select count(*) n from e group by {', '.join(keycols)} having n > 1)")
    small["dup_key_with_indicator"] = q(f'select count(*) from (select count(*) n from e group by {", ".join(keycols)}, "Indicator" having n > 1)')
    small["dup_key_distinct_values"] = q(f'select count(*) from (select count(distinct "Value") n from e group by {", ".join(keycols)} having n > 1)')
    small["dup_key_distinct_values_sample"] = q(f'select "SeriesCode", "GeoAreaCode", "TimePeriod", list(distinct "Value"), list(distinct "Indicator"), list(distinct "Time_Detail") from e group by {", ".join(keycols)} having count(distinct "Value") > 1 limit 8')
    small["units_per_series_max"] = q('select max(n) from (select "SeriesCode", count(distinct "[Units]") n from e group by 1)')
    note("sdg probe3 eu profile", small)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
