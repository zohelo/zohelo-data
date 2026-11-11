"""Read-only probe for the Gold activation of the statistical sources (Eurostat, GUS BDL, GUS DBW, NBP).
Prints findings to the job log (read with gh api .../jobs/<id>/logs). Writes nothing. Deleted before merge.
Usage: python -m dev.gus_probe <section> [<section> ...]; sections: coverage, eurostat, bdl, dbw, nbp."""

from __future__ import annotations

import re
import sys

import duckdb

from ingestion.common.r2 import catalog

CAT = None


def cat():
    global CAT
    if CAT is None:
        CAT = catalog()
    return CAT


def show(title, rel, limit=400):
    print(f"\n=== {title}", flush=True)
    try:
        rows = rel.fetchall()
        cols = [d[0] for d in rel.description]
    except Exception as exc:  # report and continue
        print(f"ERROR {type(exc).__name__}: {str(exc)[:500]}")
        return
    print("\t".join(cols))
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:160] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def scan(table, row_filter=None, fields=("*",)):
    from pyiceberg.expressions import AlwaysTrue

    t = cat().load_table(table)
    return t.scan(row_filter=row_filter or AlwaysTrue(), selected_fields=fields).to_arrow()


def coverage():
    cov = scan(("gold", "coverage"))
    con = duckdb.connect()
    con.register("cov", cov)
    show("coverage status by source", con.sql(
        "select source, status, count(*) n, sum(silver_rows) rows from cov "
        "where source in ('eurostat','gus_bdl','gus_dbw','nbp','nbp_interest_rates') group by all order by all"))
    show("eurostat loaded", con.sql(
        "select upper(block_id) id, silver_rows, block_name from cov where source='eurostat' and in_silver "
        "order by 1"), limit=20000)
    show("bdl loaded", con.sql(
        "select block_id, silver_rows, block_name from cov where source='gus_bdl' and in_silver order by 1"),
        limit=20000)
    show("dbw loaded (blocks, files collapsed)", con.sql(
        "select regexp_replace(block_id, '(_[0-9]{4})?_pl$', '') blk, count(*) files, sum(silver_rows) rows, "
        "any_value(regexp_replace(block_name, ' \\([0-9]{4}\\)$', '')) name from cov "
        "where source='gus_dbw' and in_silver group by 1 order by 1"), limit=20000)


EUROSTAT = ["ERT_BIL_EUR_M", "PRC_HICP_MINR", "EARN_MW_CUR", "EXT_ST_EU27_2020SITC", "ISOC_CI_IN_H", "ISOC_CI_IT_H",
            "ISOC_R_BROAD_H", "EI_BSCO_M"]


def eurostat(extra):
    from pyiceberg.expressions import In

    ids = EUROSTAT + extra
    tbl = scan(("silver", "eurostat_observations"), In("dataset_id", [*ids, *(i.lower() for i in ids)]),
               ("dataset_id", "freq_code", "unit_code", "geo_code", "time_period", "breakdown_dimensions",
                "breakdown_codes", "value"))
    con = duckdb.connect()
    con.register("o", tbl)
    show("eurostat datasets: rows, freq, dims", con.sql(
        "select upper(dataset_id) d, freq_code, breakdown_dimensions, count(*) n, count(distinct unit_code) units, "
        "min(time_period) first, max(time_period) last, count(*) filter (where geo_code='PL') pl_rows "
        "from o group by all order by 1"), limit=2000)
    show("eurostat units", con.sql(
        "select upper(dataset_id) d, unit_code, count(*) n from o group by all order by 1,2"), limit=3000)
    show("eurostat PL series (codes, unit)", con.sql(
        "select upper(dataset_id) d, freq_code, unit_code, breakdown_codes, count(*) n, min(time_period) f, "
        "max(time_period) l from o where geo_code='PL' group by all order by 1,2,3,4"), limit=6000)
    show("EXT_ST geos", con.sql(
        "select upper(dataset_id) d, geo_code, count(*) from o where upper(dataset_id) like 'EXT_ST%' group by all "
        "order by all"), limit=500)
    show("HICP coicop18 codes with X (PL)", con.sql(
        "select distinct breakdown_codes from o where upper(dataset_id)='PRC_HICP_MINR' and geo_code='PL' "
        "and unit_code='RCH_A' and breakdown_codes like '%X%' order by 1"), limit=200)


BDL = ["P2656", "P2721", "P2692", "P2713", "P3908", "P2837", "P4610", "P3788", "P3794", "P1869", "P2861"]


def bdl(extra):
    from pyiceberg.expressions import In

    subs = BDL + extra
    con = duckdb.connect()
    var = scan(("silver", "bdl_variables"), In("subgroup_id", subs))
    con.register("v", var)
    show("bdl variables", con.sql(
        "select subgroup_id, variable_id, n1, n2, n3, n4, n5, measure_unit_name, level from v order by 1, 2"),
        limit=4000)
    obs = scan(("silver", "bdl_observations"), In("subgroup_id", subs),
               ("subgroup_id", "variable_id", "unit_code", "year", "value", "measure_unit", "channel"))
    con.register("o", obs)
    show("bdl observations by subgroup and unit-code shape", con.sql(
        "select subgroup_id, case when unit_code='0000000' or unit_code like '000000000000' then 'PL' "
        "when length(unit_code)=12 then 'bdl' || substr(unit_code,1,2) || '-lvl' "
        "else 'terc' || length(unit_code) end k, "
        "count(*) n, count(distinct variable_id) vars, min(year) y0, max(year) y1 from o group by all order by 1,2"),
        limit=500)
    show("bdl rows per subgroup (total, from 2010, unit levels <=2 approx)", con.sql(
        "select subgroup_id, count(*) n, count(*) filter (where year>=2010) n2010, count(distinct unit_code) units, "
        "string_agg(distinct measure_unit, '|') mu, string_agg(distinct channel, '|') ch from o group by 1 order by 1"))


DBW = ["1603_1339_116_4", "184_960_267_1", "304_1698_99_4", "305_1698_5_1", "359_39_94_4", "1618_1335_95_4",
       "1714_1575_243_1", "309_485_180_3", "507_870_93_1"]


def dbw(extra):
    from pyiceberg.expressions import Or, StartsWith

    blocks = DBW + extra
    flt = Or(*[StartsWith("entity_id", f"{b}_") for b in blocks])
    con = duckdb.connect()
    obs = scan(("silver", "dbw_observations"), flt)
    con.register("o", obs)
    flt2 = Or(*[StartsWith("entity_id", f"{b}_") for b in blocks])
    dic = scan(("bronze", "dbw_dictionary_items"), flt2,
               ("entity_id", "nazwa_kolumny", "nazwa_slownika", "id_elementu", "opis"))
    con.register("d", dic)
    con.sql("create table dd as select regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') blk, lower(nazwa_kolumny) col, "
            "id_elementu el, any_value(opis) lbl from d group by all")
    show("dbw rows per block", con.sql(
        "select regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') blk, count(*) n, count(distinct entity_id) files, "
        "min(year) y0, max(year) y1, count(distinct presentation_id) pres, count(distinct period_id) periods "
        "from o group by 1 order by 1"))
    show("dbw dims and position counts", con.sql(
        "with x as (select regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') blk, unnest(["
        + ",".join(f"struct_pack(k:={k}, dim:=dimension_{k}_id, pos:=position_{k}_id)" for k in range(1, 10))
        + "]) p from o) select blk, p.k, p.dim, (select lbl from dd where dd.blk=x.blk and col='id_wymiar_'||p.k "
        "and el=cast(p.dim as varchar)) dim_name, count(distinct p.pos) positions from x where p.pos is not null "
        "group by all order by 1,2"), limit=500)
    show("dbw small-dimension positions (<=40 positions)", con.sql(
        "with x as (select distinct regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') blk, unnest(["
        + ",".join(f"struct_pack(k:={k}, pos:=position_{k}_id)" for k in range(1, 10))
        + "]) p from o), c as (select blk, p.k k, count(distinct p.pos) np from x group by all) "
        "select x.blk, x.p.k, x.p.pos, dd.lbl from x join c on c.blk=x.blk and c.k=x.p.k and c.np<=40 "
        "left join dd on dd.blk=x.blk and dd.col='id_pozycja_'||x.p.k and dd.el=cast(x.p.pos as varchar) "
        "where x.p.pos is not null order by 1,2,3"), limit=3000)
    show("dbw presentations, periods, indicators", con.sql(
        "select blk, col, el, lbl from dd where col in ('id_sposob_prezentacji_miara','id_okres','id_zmienna') "
        "order by 1,2,3"), limit=3000)


def nbp():
    import requests

    pages = [
        "https://nbp.pl/statystyka-i-sprawozdawczosc/statystyka-bilansu-platniczego/",
        "https://nbp.pl/statystyka-i-sprawozdawczosc/statystyka-bilansu-platniczego/bilans-platniczy/",
        "https://nbp.pl/statystyka-i-sprawozdawczosc/statystyka-bilansu-platniczego/miedzynarodowa-pozycja-inwestycyjna/",
        "https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/",
        "https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/balance-of-payments/",
        "https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/international-investment-position/",
    ]
    links = set()
    for url in pages:
        try:
            r = requests.get(url, timeout=60, headers={"User-Agent": "zohelo-data probe"})
            found = set(re.findall(r'href="([^"]+\.(?:xlsx|xls|csv|zip))"', r.text, re.I))
            print(f"PAGE {url} -> {r.status_code}, {len(r.text)} bytes, {len(found)} files")
            for f in sorted(found):
                print("  LINK", f)
            links |= found
            for sub in sorted(set(re.findall(r'href="(https://nbp\.pl/[^"]*(?:bilans|pozycj|balance|investment)[^"]*)"', r.text)))[:40]:
                print("  SUBPAGE", sub)
        except Exception as exc:
            print(f"PAGE {url} -> {type(exc).__name__}: {exc}")
    base = "https://static.nbp.pl/dane/bilans-platniczy/"
    for name in ["bop_m.xlsx", "bop_q.xlsx", "bop_kw.xlsx", "bopa.xlsx", "bop_m_en.xlsx", "bop_kw_en.xlsx",
                 "mip_q.xlsx", "mip_kw.xlsx", "iip_q.xlsx", "mpi_kw.xlsx", "mpi.xlsx"]:
        try:
            r = requests.head(base + name, timeout=60, allow_redirects=True)
            print(f"HEAD {base + name} -> {r.status_code} {r.headers.get('Content-Length')} {r.headers.get('Last-Modified')}")
            if r.status_code == 200:
                links.add(base + name)
        except Exception as exc:
            print(f"HEAD {name} -> {type(exc).__name__}")
    import subprocess

    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "openpyxl==3.1.5"], check=True)  # dev only
    import io

    import openpyxl

    for url in sorted(links):
        if not url.startswith("http") or not url.lower().endswith("xlsx"):
            continue
        if not any(k in url.lower() for k in ("bop", "mip", "iip", "mpi", "bilans", "pozyc")):
            continue
        try:
            data = requests.get(url, timeout=120).content
            wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
            print(f"\n##### {url} {len(data)} bytes sheets={wb.sheetnames}")
            for ws in wb.worksheets[:6]:
                print(f"--- sheet {ws.title} dims={ws.dimensions}")
                for i, row in enumerate(ws.iter_rows(values_only=True)):
                    if i >= 70:
                        break
                    cells = ["" if c is None else str(c)[:28] for c in row[:14]]
                    if any(cells):
                        print(f"{i + 1}: " + " | ".join(cells))
        except Exception as exc:
            print(f"XLSX {url} -> {type(exc).__name__}: {str(exc)[:200]}")


def main(argv):
    sections = argv or ["coverage"]
    extra = [a.split("=", 1)[1].split(",") for a in sections if a.startswith("+")]
    for s in sections:
        if s == "coverage":
            coverage()
        elif s.startswith("eurostat"):
            eurostat([x for x in s.split(":")[1:] if x])
        elif s.startswith("bdl"):
            bdl([x for x in s.split(":")[1:] if x])
        elif s.startswith("dbw"):
            dbw([x for x in s.split(":")[1:] if x])
        elif s == "nbp":
            nbp()
    del extra
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
