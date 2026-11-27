"""Read-only probe for GL-010 World Bank Enterprise Surveys (#3315): is there a Data360 bulk CSV for WB_ES
(data360files.worldbank.org/data360-data/data/WB_ES/WB_ES.csv), as for ITU_DH (GL-031)? GET/HEAD only, one
request at a time, >= 1.2 s apart; profiled with DuckDB in the runner's /tmp; no writes to R2."""

import json
import os
import time
import urllib.error
import urllib.request

import duckdb

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://data360files.worldbank.org"
BASE = f"{HOST}/data360-data/data/WB_ES"
API = "https://data360api.worldbank.org/data360"
OUT = os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output")
LAST = [0.0]
NOTES = []


def note(title, obj):
    text = json.dumps(obj, default=str)
    NOTES.append((title, text))
    print(f"::notice title={title}::" + text[:3900], flush=True)


def call(url, method="GET", tries=3, dest=None):
    for attempt in range(tries):
        wait = 1.2 - (time.time() - LAST[0])
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA}, method=method)
            with urllib.request.urlopen(req, timeout=300) as r:
                if method != "GET":
                    return r.status, dict(r.headers), b""
                if dest:
                    with open(dest, "wb") as f:
                        while chunk := r.read(1 << 20):
                            f.write(chunk)
                    return r.status, dict(r.headers), b""
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                time.sleep(20 * (attempt + 1))
                continue
            return e.code, dict(e.headers or {}), e.read()[:500]
        except Exception as e:  # noqa: BLE001
            if attempt < tries - 1:
                time.sleep(20)
                continue
            return 0, {}, str(e).encode()
        finally:
            LAST[0] = time.time()


def main():
    os.makedirs(OUT, exist_ok=True)
    heads = {}
    for url in (f"{BASE}/WB_ES.csv", f"{BASE}/WB_ES.zip", f"{BASE}/WB_ES_T_CORR4.csv"):
        st, h, _ = call(url, "HEAD")
        heads[url.rsplit("/", 1)[1]] = [st, h.get("Content-Length"), h.get("Last-Modified"), h.get("ETag"),
                                        h.get("Content-Type")]
    st, h, b = call(f"{API}/indicators?datasetId=WB_ES")
    inds = json.loads(b) if st == 200 else []
    st2, h2, b2 = call(f"{API}/data?DATABASE_ID=WB_ES&REF_AREA=POL&top=3&skip=0")
    api = json.loads(b2) if st2 == 200 else {}
    note("wb_es probe heads", {"heads": heads, "indicators": len(inds), "ind_sample": inds[:8],
                               "api_pol": {"st": st2, "count": api.get("count"),
                                           "row": (api.get("value") or [None])[0]}})
    if heads["WB_ES.csv"][0] != 200:
        return
    path = "/tmp/wb_es.csv"
    st, h, _ = call(f"{BASE}/WB_ES.csv", dest=path)
    size = os.path.getsize(path)
    con = duckdb.connect()
    con.execute(f"create table t as select * from read_csv('{path}', all_varchar=true, header=true)")
    cols = [r[0] for r in con.execute("describe t").fetchall()]
    n = con.execute("select count(*) from t").fetchone()[0]
    with open(path, "rb") as f:
        head = f.read(1500)
    note("wb_es probe file", {"st": st, "bytes": size, "lm": h.get("Last-Modified"), "rows": n, "cols": cols,
                              "head": head.decode("utf-8", "replace")})

    def q(sql):
        return con.execute(sql).fetchall()

    prof = {}
    for c in ("FREQ", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "OBS_CONF", "DECIMALS", "SEX", "AGE",
              "URBANISATION", "DATABASE_ID", "LATEST_DATA", "COMP_BREAKDOWN_1", "COMP_BREAKDOWN_2",
              "COMP_BREAKDOWN_3", "COMMENT_OBS"):
        if c in cols:
            prof[c] = [q(f'select count(distinct "{c}") from t')[0][0],
                       q(f'select "{c}", count(*) from t group by 1 order by 2 desc limit 12')]
    note("wb_es probe codes", prof)
    keys = [c for c in ("INDICATOR", "REF_AREA", "SEX", "AGE", "URBANISATION", "COMP_BREAKDOWN_1",
                        "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3", "UNIT_MEASURE", "TIME_PERIOD", "FREQ") if c in cols]
    k = ", ".join(f'"{c}"' for c in keys)
    dups = q(f"select count(*), sum(n) from (select {k}, count(*) n from t group by all having count(*) > 1)")
    dup_sample = q(f"select {k}, count(*) n from t group by all having count(*) > 1 limit 3")
    dup_diff = q(f"select count(*) from (select {k} from t group by all having count(*) > 1 "
                 "and count(distinct coalesce(OBS_VALUE, '~')) > 1)")
    nonnum = q("select OBS_VALUE, count(*) from t where try_cast(OBS_VALUE as double) is null "
               "group by 1 order by 2 desc limit 15")
    zeros = q("select count(*) from t where try_cast(OBS_VALUE as double) = 0")
    note("wb_es probe grain", {
        "keys": keys, "dup_keys": dups, "dup_sample": dup_sample, "dup_with_diff_values": dup_diff,
        "nonnum": nonnum, "zeros": zeros,
        "indicators": q("select count(distinct INDICATOR) from t"),
        "areas": q("select count(distinct REF_AREA) from t"),
        "period": q("select min(TIME_PERIOD), max(TIME_PERIOD), count(distinct TIME_PERIOD) from t"),
        "top_ind": q("select INDICATOR, count(*) from t group by 1 order by 2 desc limit 6"),
        "max_years_per_area_ind": q("select max(n) from (select INDICATOR, REF_AREA, count(distinct TIME_PERIOD) n "
                                    "from t group by all)"),
    })
    lab = [c for c in cols if c.endswith("_LABEL")]
    bd = {}
    for c in ("COMP_BREAKDOWN_1", "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3"):
        if c in cols:
            lc = c + "_LABEL"
            sel = f'"{c}", ' + (f'any_value("{lc}"), ' if lc in cols else "")
            bd[c] = q(f'select {sel} count(*) from t group by "{c}" order by count(*) desc limit 40')
    note("wb_es probe breakdowns", {"label_cols": lab, "bd": bd})
    pol = q("select INDICATOR, TIME_PERIOD, COMP_BREAKDOWN_1, UNIT_MEASURE, OBS_VALUE, OBS_STATUS, OBS_CONF "
            "from t where REF_AREA = 'POL' and COMP_BREAKDOWN_1 in ('_T', '_Z') order by 1, 2 limit 45")
    note("wb_es probe poland", {"pol_rows": q("select count(*), min(TIME_PERIOD), max(TIME_PERIOD) from t "
                                               "where REF_AREA = 'POL'"), "sample": pol,
                                "years": q("select TIME_PERIOD, count(*) from t where REF_AREA = 'POL' "
                                           "group by 1 order by 1")})
    ulab = "any_value(UNIT_MEASURE_LABEL), " if "UNIT_MEASURE_LABEL" in cols else ""
    units = q(f"select UNIT_MEASURE, {ulab}count(distinct INDICATOR), count(*), min(try_cast(OBS_VALUE as double)), "
              "max(try_cast(OBS_VALUE as double)) from t group by 1 order by 4 desc")
    ind_units = q("select count(*) from (select INDICATOR from t group by 1 having count(distinct UNIT_MEASURE) > 1)")
    ilabc = "any_value(INDICATOR_LABEL), " if "INDICATOR_LABEL" in cols else ""
    ilab = q(f"select INDICATOR, {ilabc}count(*) from t group by 1 order by 1")
    note("wb_es probe units", {"units": units, "indicators_multi_unit": ind_units})
    with open(os.path.join(OUT, "wb_es_probe.json"), "w") as f:
        json.dump({"notes": NOTES, "indicators": ilab, "api_indicators": inds}, f, default=str)
    note("wb_es probe indicators", [r[:2] for r in ilab][:300])


if __name__ == "__main__":
    main()
