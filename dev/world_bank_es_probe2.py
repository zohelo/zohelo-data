"""Read-only probe 2 for GL-010 (#3315): how the breakdowns of WB_ES.csv combine, which rows make a Gold total, and
Poland's values of the candidate KPI indicators. One GET of the bulk file (as probe 1); no writes to R2."""

import json
import os
import urllib.request

import duckdb

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URL = "https://data360files.worldbank.org/data360-data/data/WB_ES/WB_ES.csv"


def note(title, obj):
    print(f"::notice title={title}::" + json.dumps(obj, default=str)[:3900], flush=True)


def main():
    path = "/tmp/wb_es.csv"
    req = urllib.request.Request(URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=300) as r, open(path, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    con = duckdb.connect()
    con.execute(f"create table t as select * from read_csv('{path}', all_varchar=true, header=true)")
    con.execute("create view v as select *, left(INDICATOR, 7) as pfx, "
                "case when COMP_BREAKDOWN_1 = '_T' then 'T' when COMP_BREAKDOWN_1 like '%MAJOR_SECTOR%' then 'MAJ' "
                "when COMP_BREAKDOWN_1 like 'WB_ES_M_SECTOR_1' or COMP_BREAKDOWN_1 = 'WB_ES_S_SECTOR_1' then 'ALL' "
                "else 'SEC' end as c1, "
                "case when COMP_BREAKDOWN_2 in ('_T', '_Z') then COMP_BREAKDOWN_2 else 'X' end as c2, "
                "case when COMP_BREAKDOWN_3 in ('_T', '_Z') then COMP_BREAKDOWN_3 else 'X' end as c3 from t")

    def q(sql):
        return con.execute(sql).fetchall()

    note("wb_es probe2 combos", {
        "prefix_c1_c2_c3": q("select pfx, c1, c2, c3, count(*), count(distinct INDICATOR) from v group by all order by 1, 2, 3, 4"),
        "time_formats": q("select regexp_replace(TIME_PERIOD, '[0-9]', '9', 'g'), count(*) from t group by 1"),
        "zeros_by_unit": q("select UNIT_MEASURE, count(*) from t where try_cast(OBS_VALUE as double) = 0 group by 1 order by 2 desc"),
        "empty_values": q("select count(*) from t where OBS_VALUE is null or trim(OBS_VALUE) = ''"),
    })
    note("wb_es probe2 indicators per prefix", {
        "ind_by_prefix": q("select pfx, count(distinct INDICATOR), count(*) from v group by 1"),
        "ind_without_total_row": q("select pfx, count(*) from (select pfx, INDICATOR from v group by all "
                                   "having count(*) filter (where c1 = 'T' and c2 <> 'X' and c3 <> 'X') = 0) group by 1"),
        "ind_without_T_or_ALL": q("select pfx, count(*) from (select pfx, INDICATOR from v group by all "
                                  "having count(*) filter (where c1 in ('T', 'ALL') and c2 <> 'X' and c3 <> 'X') = 0) group by 1"),
        "M_cb1": q("select COMP_BREAKDOWN_1, count(*) from v where pfx = 'WB_ES_M' group by 1 order by 2 desc limit 8"),
        "S_cb1": q("select COMP_BREAKDOWN_1, count(*) from v where pfx = 'WB_ES_S' group by 1 order by 2 desc limit 8"),
        "total_rows_T": q("select count(*) from v where c1 = 'T' and c2 <> 'X' and c3 <> 'X'"),
        "total_rows_T_or_ALL": q("select count(*) from v where c1 in ('T', 'ALL') and c2 <> 'X' and c3 <> 'X'"),
        "rows_c2c3_total_any_c1": q("select count(*) from v where c2 <> 'X' and c3 <> 'X'"),
        "dup_total_keys": q("select count(*) from (select INDICATOR, REF_AREA, TIME_PERIOD from v where c1 in ('T', 'ALL') "
                            "and c2 <> 'X' and c3 <> 'X' group by all having count(*) > 1)"),
        "dup_total_sample": q("select INDICATOR, REF_AREA, TIME_PERIOD, string_agg(COMP_BREAKDOWN_1 || '/' || COMP_BREAKDOWN_2 || '/' "
                              "|| COMP_BREAKDOWN_3 || '=' || OBS_VALUE, ' ; ') from v where c1 in ('T', 'ALL') and c2 <> 'X' and c3 <> 'X' "
                              "group by all having count(*) > 1 limit 4"),
    })
    kw = ("bribe", "gift", "access to finance", "female top manager", "operating license", "website", "corruption",
          "informal", "female ownership", "major constraint")
    cond = " or ".join(f"lower(INDICATOR_LABEL) like '%{k}%'" for k in kw)
    note("wb_es probe2 kpi candidates", q(
        f"select INDICATOR, any_value(INDICATOR_LABEL), any_value(UNIT_MEASURE), count(distinct REF_AREA) from t "
        f"where INDICATOR like 'WB_ES_T_%' and ({cond}) group by 1 order by 1 limit 60"))
    note("wb_es probe2 poland totals", q(
        f"select INDICATOR, TIME_PERIOD, COMP_BREAKDOWN_2, COMP_BREAKDOWN_3, OBS_VALUE from t where REF_AREA = 'POL' "
        f"and COMP_BREAKDOWN_1 = '_T' and COMP_BREAKDOWN_2 in ('_T', '_Z') and COMP_BREAKDOWN_3 in ('_T', '_Z') "
        f"and INDICATOR like 'WB_ES_T_%' and ({cond}) order by 1, 2 limit 80"))
    note("wb_es probe2 areas", {
        "areas": q("select REF_AREA, any_value(REF_AREA_LABEL) from t group by 1 order by 1"),
        "years_2026": q("select REF_AREA, count(*) from t where TIME_PERIOD like '2026%' group by 1"),
    })
    os.makedirs(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"), exist_ok=True)


if __name__ == "__main__":
    main()
