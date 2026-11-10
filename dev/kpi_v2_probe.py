"""Dev probe (read-only) for the zohelo-bi first slice: which candidate blocks are in Silver, and
whether the planned Eurostat filters find rows for PL and EU27_2020. Deleted before merge."""

import json

import duckdb
from pyiceberg.expressions import And, In

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

HAS = "list_contains(string_split(breakdown_codes, ','), '{}')"

# dataset -> planned filter (as poc_src_eurostat.py writes them)
PLANNED = {
    "TRNG_LFSE_01": f"unit_code = 'PC' and {HAS.format('Y25-64')}",
    "ILC_PEPS01N": f"unit_code = 'PC' and ({HAS.format('TOTAL')} or {HAS.format('Y_LT18')})",
    "ISOC_SK_DSKL_I21": f"unit_code = 'PC_IND' and {HAS.format('I_DSK2_BAB')}",
    "IRT_LT_MCBY_M": "true",
    "ISOC_R_BROAD_H": "true",
    "ISOC_CI_IT_H": f"unit_code = 'PC_HH' and {HAS.format('H_BBFIX')}",
    "NAMA_10_PC": f"unit_code = 'VI_PPS_EU27_2020_HAB' and {HAS.format('B1GQ')}",
    "NAMA_10R_2GDP": "unit_code = 'PPS_HAB_EU27_2020'",
    "GOV_10DD_EDPT1": f"unit_code = 'PC_GDP' and {HAS.format('S13')} and ({HAS.format('GD')} or {HAS.format('B9')})",
    "ENV_AIR_GGE": f"unit_code = 'THS_T' and {HAS.format('GHG')}",
    "RD_E_GERDTOT": f"unit_code = 'PC_GDP' and {HAS.format('TOTAL')}",
    "DEMO_R_MLIFEXP": HAS.format("Y_LT1"),
    "DEMO_MLEXPEC": HAS.format("Y_LT1"),
    "LFSI_NEET_A": HAS.format("Y15-29"),
    "EDAT_LFSE_20": HAS.format("Y15-29"),
    "TIPSII10": "true",
    "DEMO_PJANIND": HAS.format("OLDDEP1"),
    "DEMO_R_PJANIND2": HAS.format("OLDDEP1"),
    "CEI_WM011": "true",
    "ERT_EFF_IC_M": "true",
    "EDAT_LFSE_03": f"{HAS.format('Y25-34')} and {HAS.format('ED5-8')}",
    "DEMO_FIND": HAS.format("TOTFERRT"),
    "DEMO_R_FIND2": HAS.format("TOTFERRT"),
    "LFSI_EMP_A": f"unit_code = 'PC_POP' and {HAS.format('Y20-64')} and {HAS.format('ACT')}",
}

# other candidates: coverage status only
OTHER = [
    "LFSQ_ARGAN", "LFST_R_LFP2ACTRT", "LFSI_EMP_Q", "LFSQ_URGAN", "UNE_RT_Q", "LFST_R_LFU3RT", "BOP_GDP6_Q",
    "BOP_C6_Q", "TIPSBP20", "EI_BSCO_M", "EI_BSIN_M_R2", "EI_BSSI_M_R2", "EI_BSIN_Q_R2", "TEIBS070",
    "ERT_BIL_EUR_M", "EXT_ST_EU27_2020SITC", "BOP_FDI6_FLOW", "NAMQ_10_GDP", "NAMA_10_GDP", "NAMA_10_A10",
    "NAMA_10_AN6", "ILC_DI12", "EARN_GR_GPGR2", "PRC_HICP_MINR", "PRC_HICP_MANR", "STS_INPR_M", "STS_COPR_M",
    "STS_INPP_M", "STS_TRTU_M", "JVS_Q_NACE2", "NAMQ_10_LP_ULC", "UNE_LTU_A", "LFSA_LTUE", "EARN_MW_CUR",
    "EARN_SES18_47", "EARN_SES_PUB2S", "PRC_HPI_Q", "SPR_EXP_SUM", "TPS00100", "YTH_EMPL_090", "EI_LMJV_Q_R2",
    "NAMA_10R_2HHINC", "NAMA_10R_2GFCF", "ILC_LI02", "ILC_MDDD11", "EI_MFIR_M", "IRT_ST_M", "TEINA225",
    "TIPSUN20", "DEMO_PJAN", "DEMO_R_PJANAGGR3", "UNE_RT_M", "UNE_RT_A", "SPR_PNS_FTPS", "TEC00118",
    "NAMQ_10_A10", "EI_BSRT_M_R2", "TEIBS010", "PRC_PPP_IND", "DEMO_R_D2JAN", "NAMA_10R_2GVAGR", "SDG_08_10",
    "TEC00115", "TIPSNA70", "NAMA_10_LP_ULC", "TEC00001",
]

BDL_WORDS = [
    "aktywno", "cen towarów i usług", "wskaźniki cen", "internet", "szerokopasm", "budowlano", "dochód",
    "dochody", "rodzinne", "produkt krajowy", "nakłady inwestycyjne", "trwanie życia", "produkcja sprzedana",
    "obciążenia", "emeryt", "renci", "lokali mieszkalnych", "1 m2", "bezrobotni", "dzietno", "płodno",
    "wynagrodzen", "pracujący", "pomoc społeczna", "nieruchomo",
]


def chunks(title: str, data: dict) -> None:
    """Annotate a dict in pieces under the 4 KB cap."""
    part, n = {}, 1
    for key, value in data.items():
        part[key] = value
        if len(json.dumps(part, default=str)) > 3700:
            last = part.pop(key)
            annotate("notice", f"{title} {n}", part)
            part, n = {key: last}, n + 1
    if part:
        annotate("notice", f"{title} {n}", part)


def main() -> int:
    import sys

    cat = catalog()
    con = duckdb.connect()
    if "silver" in sys.argv[1:]:
        return silver(cat, con)
    if "verify" in sys.argv[1:]:
        return verify(cat, con)
    cov = cat.load_table(("gold", "coverage")).scan(
        selected_fields=("source", "block_id", "block_name", "status", "silver_rows")
    ).to_arrow()
    con.register("cov", cov)
    ids = sorted({*PLANNED, *OTHER})
    rows = con.execute(
        "select upper(block_id), status, silver_rows from cov where source = 'eurostat' and upper(block_id) in "
        "(select unnest(?))", [ids]
    ).fetchall()
    found = {r[0]: f"{r[1]}:{r[2]}" for r in rows}
    chunks("eurostat coverage", {i: found.get(i, "absent") for i in ids})
    bdl = con.execute(
        "select block_id, status, left(block_name, 48) from cov where source = 'gus_bdl' and ("
        + " or ".join("lower(block_name) like ?" for _ in BDL_WORDS) + ") order by block_id",
        [f"%{w}%" for w in BDL_WORDS],
    ).fetchall()
    chunks("bdl coverage", {r[0]: f"{r[1]}|{r[2]}" for r in bdl})
    nbp = con.execute("select block_id, status from cov where source in ('nbp', 'ecb') order by 1").fetchall()
    annotate("notice", "nbp ecb coverage", nbp[:150])
    stat = con.execute("select source, status, count(*) from cov group by all order by 1, 2").fetchall()
    annotate("notice", "coverage summary", stat)
    return 0


def silver(cat, con) -> int:

    spelled = [*PLANNED, *(d.lower() for d in PLANNED)]
    obs = cat.load_table(("silver", "eurostat_observations")).scan(
        row_filter=And(In("dataset_id", spelled), In("geo_code", ["PL", "EU27_2020", "PL22", "PL91"])),
        selected_fields=("dataset_id", "freq_code", "unit_code", "geo_code", "time_period", "value",
                         "breakdown_dimensions", "breakdown_codes"),
    ).to_arrow()
    con.register("obs", obs)
    con.execute("create table o as select *, upper(dataset_id) as ds from obs where geo_code in ('PL', 'EU27_2020', 'PL22', 'PL91')")
    out = {}
    for ds, rule in PLANNED.items():
        base = con.execute(
            "select list(distinct freq_code), list(distinct unit_code)[1:10], any_value(breakdown_dimensions) "
            "from o where ds = ?", [ds]).fetchone()
        dims = (base[2] or "").split(",")
        codes = {}
        for i, d in enumerate(dims):
            if not d:
                continue
            vals = con.execute(
                f"select list(distinct string_split(breakdown_codes, ',')[{i + 1}])[1:10] from o where ds = ? and geo_code = 'PL'",
                [ds]).fetchone()[0]
            codes[d] = vals
        hit = con.execute(
            f"select geo_code, count(*), max(time_period), count(distinct breakdown_codes || unit_code) from o "
            f"where ds = ? and value is not null and ({rule}) group by 1 order by 1", [ds]).fetchall()
        series = con.execute(
            f"select list(distinct unit_code || '|' || breakdown_codes)[1:8] from o "
            f"where ds = ? and geo_code = 'PL' and value is not null and ({rule})", [ds]).fetchone()[0]
        out[ds] = {"f": base[0], "u": base[1], "dims": codes, "hit": hit, "pl": series}
    chunks("silver", out)
    return 0


CHECKS = {
    "ghg_tot": "select list(distinct split_part(breakdown_codes, ',', 2)) from o where ds = 'ENV_AIR_GGE' "
    "and geo_code = 'PL' and unit_code = 'THS_T' and breakdown_codes like 'GHG,TOT%'",
    "ghg_dims": "select any_value(breakdown_dimensions) from o where ds = 'ENV_AIR_GGE'",
    "dskl": "select geo_code, count(*), max(time_period) from o where ds = 'ISOC_SK_DSKL_I21' and unit_code = 'PC_IND' "
    "and breakdown_codes in ('IND_TOTAL,I_DSK2_BAB', 'I_DSK2_BAB,IND_TOTAL') group by 1",
    "dskl_dims": "select any_value(breakdown_dimensions) from o where ds = 'ISOC_SK_DSKL_I21'",
    "reer": "select unit_code, geo_code, count(*), min(time_period), max(time_period) from o where ds = 'ERT_EFF_IC_M' "
    "and breakdown_codes = 'REER_IC42_CPI' and geo_code in ('PL', 'EU27_2020', 'DE') group by 1, 2",
    "r2gdp_geos": "select length(geo_code), count(distinct geo_code), max(time_period) from o where ds = 'NAMA_10R_2GDP' "
    "and unit_code = 'PPS_HAB_EU27_2020' group by 1 order by 1",
    "r2gdp_eu": "select list(distinct geo_code) from o where ds = 'NAMA_10R_2GDP' and geo_code like 'EU%'",
    "mlexpec_geos": "select count(distinct geo_code), list(distinct geo_code) filter (where length(geo_code) > 2) "
    "from o where ds = 'DEMO_MLEXPEC' and breakdown_codes in ('T,Y_LT1', 'Y_LT1,T')",
    "mlexpec_dims": "select any_value(breakdown_dimensions) from o where ds = 'DEMO_MLEXPEC'",
    "find_geos": "select count(distinct geo_code), list(distinct geo_code) filter (where length(geo_code) > 2) "
    "from o where ds = 'DEMO_FIND' and breakdown_codes = 'TOTFERRT'",
    "pjanind_geos": "select count(distinct geo_code), max(time_period) from o where ds = 'DEMO_PJANIND' "
    "and breakdown_codes = 'OLDDEP1'",
    "r_find2_pl": "select list(distinct geo_code) from o where ds = 'DEMO_R_FIND2' and geo_code like 'PL%'",
    "neet_eu": "select count(distinct geo_code) from o where ds = 'LFSI_NEET_A' and unit_code = 'PC_POP'",
    "tipsii_eu": "select count(distinct geo_code), list(distinct geo_code) filter (where length(geo_code) > 2) "
    "from o where ds = 'TIPSII10' and unit_code = 'PC_GDP'",
    "mcby_geos": "select count(distinct geo_code), max(time_period) from o where ds = 'IRT_LT_MCBY_M'",
    "peps_dims": "select any_value(breakdown_dimensions) from o where ds = 'ILC_PEPS01N'",
    "edat_dims": "select any_value(breakdown_dimensions) from o where ds = 'EDAT_LFSE_03'",
    "trng_dims": "select any_value(breakdown_dimensions) from o where ds = 'TRNG_LFSE_01'",
    "gov_dims": "select any_value(breakdown_dimensions) from o where ds = 'GOV_10DD_EDPT1'",
    "gerd_dims": "select any_value(breakdown_dimensions) from o where ds = 'RD_E_GERDTOT'",
    "emp_dims": "select any_value(breakdown_dimensions) from o where ds = 'LFSI_EMP_A'",
    "neet_dims": "select any_value(breakdown_dimensions) from o where ds = 'LFSI_NEET_A'",
    "mlifexp_dims": "select any_value(breakdown_dimensions) from o where ds = 'DEMO_R_MLIFEXP'",
    "eurostat_ids_case": "select list(distinct dataset_id) from o",
}


def verify(cat, con) -> int:
    ids = ["ENV_AIR_GGE", "ISOC_SK_DSKL_I21", "ERT_EFF_IC_M", "NAMA_10R_2GDP", "DEMO_MLEXPEC", "DEMO_FIND",
           "DEMO_PJANIND", "DEMO_R_FIND2", "LFSI_NEET_A", "TIPSII10", "IRT_LT_MCBY_M", "ILC_PEPS01N",
           "EDAT_LFSE_03", "TRNG_LFSE_01", "GOV_10DD_EDPT1", "RD_E_GERDTOT", "LFSI_EMP_A", "DEMO_R_MLIFEXP"]
    obs = cat.load_table(("silver", "eurostat_observations")).scan(
        row_filter=In("dataset_id", [*ids, *(i.lower() for i in ids)]),
        selected_fields=("dataset_id", "unit_code", "geo_code", "time_period", "breakdown_dimensions",
                         "breakdown_codes"),
    ).to_arrow()
    con.register("obs", obs)
    con.execute("create table o as select *, upper(dataset_id) as ds from obs")
    out = {}
    for name, sql in CHECKS.items():
        try:
            out[name] = con.execute(sql).fetchall()
        except Exception as e:  # report and go on
            out[name] = f"error: {e}"[:300]
    chunks("verify", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
