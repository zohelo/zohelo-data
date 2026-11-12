"""Read-only: why metrics_value_range finds zeros (GUS BDL P2361, GUS DBW 1038_16_389_3 and
1038_16_92_4), and the publishers' own values behind the new bounds. No writes."""

import json
import urllib.request

import duckdb

from ingestion.common.r2 import req
from ingestion.common.report import annotate


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute(f"CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req('STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN')}')")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(
        f"CREATE SECRET r2_files (TYPE s3, KEY_ID '{req('STORAGE_WRITE_ACCESS_KEY')}', "
        f"SECRET '{req('STORAGE_WRITE_SECRET_KEY')}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"
    )
    con.execute(
        f"ATTACH '{req('STORAGE_LAKEHOUSE_CATALOG_NAME')}' AS r2 "
        f"(TYPE iceberg, SECRET r2_catalog, ENDPOINT '{req('STORAGE_LAKEHOUSE_CATALOG_URL')}', READ_ONLY)"
    )
    return con


def q(con, sql):
    try:
        return [list(map(str, r)) for r in con.execute(sql).fetchall()]
    except Exception as e:  # report, keep going
        return f"error: {str(e)[:300]}"


def fetch(url):
    try:
        req_ = urllib.request.Request(url, headers={"User-Agent": "zohelo-dev"})
        with urllib.request.urlopen(req_, timeout=60) as r:
            return json.loads(r.read())
    except Exception as e:
        return f"error: {str(e)[:200]}"


def main() -> int:
    con = connect()
    out = {}
    out["bdl_p2361"] = q(
        con,
        """select value = 0 as zero, attribute, value_text, count(*) from r2.silver.bdl_observations
           where subgroup_id = 'P2361' group by all order by 4 desc limit 12""",
    )
    out["bdl_zero_attr_all"] = q(
        con,
        """select attribute, value_text, count(*) from r2.silver.bdl_observations
           where value = 0 group by all order by 3 desc limit 8""",
    )
    out["bdl_attr_all"] = q(
        con, "select attribute, count(*) from r2.silver.bdl_observations group by all order by 2 desc limit 10"
    )
    out["dbw_zero"] = q(
        con,
        """select regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') b, value = 0 zero, missing_reason_id,
                  flag_id, confidentiality_id, count(*), min(year), max(year)
           from r2.silver.dbw_observations
           where entity_id like '1038_16_389_3%' or entity_id like '1038_16_92_4%'
              or entity_id like '1514_1360_95_3%' or entity_id like '1038_16_105_4%'
           group by all order by all""",
    )
    out["dbw_zero_all"] = q(
        con,
        """select missing_reason_id, flag_id, confidentiality_id, count(*) from r2.silver.dbw_observations
           where value = 0 group by all order by 4 desc limit 10""",
    )
    out["gold_zero_bdl_p2361"] = q(
        con,
        """select geography_key, date_key, value from r2.gold.metrics_unemployment_rate
           where source_system='gus_bdl' and value=0 order by 1, 2 limit 6""",
    )
    wb = fetch("https://api.worldbank.org/v2/country/IR;RS/indicator/NY.GDP.MKTP.CN?format=json&date=1990:2025&per_page=200")
    if isinstance(wb, list) and len(wb) > 1:
        vals = {(r["country"]["id"], r["date"]): r["value"] for r in wb[1] or []}
        out["wb_lcu"] = {f"{k[0]}{k[1]}": v for k, v in vals.items() if k in {("IR", "2025"), ("IR", "2024"), ("RS", "1990"), ("RS", "1995")}}
    else:
        out["wb_lcu"] = wb
    pop = fetch("https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL?format=json&date=2025")
    out["wb_wld_pop_2025"] = pop[1][0]["value"] if isinstance(pop, list) and pop[1] else pop
    es = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    hu = fetch(es + "nama_10_gdp?geo=HU&unit=CP_MNAC&na_item=B1GQ&time=2025")
    out["es_hu_mnac_2025"] = list(hu["value"].values()) if isinstance(hu, dict) else hu
    eea = fetch(es + "demo_pjan?geo=EEA31&age=TOTAL&sex=T&time=2020")
    out["es_eea31_pop_2020"] = list(eea["value"].values()) if isinstance(eea, dict) else eea
    annotate("notice", "metrics_value_range zeros and evidence", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
