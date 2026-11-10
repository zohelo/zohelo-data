"""Gold PoC extraction stage: the activated Eurostat datasets' rows, read from only their
Silver files. silver.eurostat_observations holds one file per dataset (design #875); the
Iceberg metadata names those files, so the planner (pyiceberg) picks the activated ones and
DuckDB streams just them. The dbt-duckdb SQL path scanned every file (trial 1 and 2).
In v1 this is the per-block builder. Without a catalog (local harness) it reads the source table.

Keep the dataset list in step with macros/poc_blocks.sql (poc_eurostat_blocks)."""

# dataset -> (frequency, the sample filter of the PoC, pushed down so the extraction stays small)
HAS = "list_contains(string_split(breakdown_codes, ','), '{}')"
COUNTRIES = "(length(geo_code) = 2 or geo_code = 'EU27_2020')"  # countries and the EU aggregate
PL_REGIONS = "(geo_code like 'PL%' and length(geo_code) between 3 and 4)"  # Polish NUTS 1 and NUTS 2


def all_of(*codes, unit=None, any_of=(), geo=COUNTRIES):
    """Sample filter: every code in the series key, one of `any_of`, the unit, the geographies."""
    parts = [HAS.format(c) for c in codes]
    if any_of:
        parts.append("(" + " or ".join(HAS.format(c) for c in any_of) + ")")
    if unit:
        parts.append(f"unit_code = '{unit}'")
    return " and ".join([*parts, geo])


DATASETS = {
    "DEMO_PJAN": ("A", HAS.format("TOTAL")),
    "DEMO_R_PJANAGGR3": ("A", HAS.format("TOTAL") + " and length(geo_code) <= 4"),
    "DEMO_GIND": ("A", "true"),
    # zohelo-bi first slice, Polska w skrócie (seeds/kpi_v2_map.csv, #2584)
    "TRNG_LFSE_01": ("A", f"unit_code = 'PC' and {HAS.format('Y25-64')} and {COUNTRIES}"),
    "ILC_PEPS01N": ("A", f"unit_code = 'PC' and ({HAS.format('TOTAL')} or {HAS.format('Y_LT18')}) and {COUNTRIES}"),
    "ISOC_SK_DSKL_I21": (
        "A",
        f"unit_code = 'PC_IND' and {HAS.format('I_DSK2_BAB')} and {HAS.format('IND_TOTAL')} and {COUNTRIES}",
    ),
    "IRT_LT_MCBY_M": ("M", COUNTRIES),
    "NAMA_10R_2GDP": ("A", f"unit_code = 'PPS_HAB_EU27_2020' and ({COUNTRIES} or {PL_REGIONS})"),
    "GOV_10DD_EDPT1": (
        "A",
        f"unit_code = 'PC_GDP' and {HAS.format('S13')} and ({HAS.format('GD')} or {HAS.format('B9')}) and {COUNTRIES}",
    ),
    "ENV_AIR_GGE": ("A", f"unit_code = 'THS_T' and {HAS.format('GHG')} and {HAS.format('TOTX4_MEMO')} and {COUNTRIES}"),
    "RD_E_GERDTOT": ("A", f"unit_code = 'PC_GDP' and {HAS.format('TOTAL')} and {COUNTRIES}"),
    "DEMO_MLEXPEC": ("A", f"unit_code = 'YR' and {HAS.format('Y_LT1')} and {COUNTRIES}"),
    "DEMO_R_MLIFEXP": ("A", f"unit_code = 'YR' and {HAS.format('Y_LT1')} and {PL_REGIONS}"),
    "LFSI_NEET_A": ("A", f"unit_code = 'PC_POP' and {HAS.format('Y15-29')} and {COUNTRIES}"),
    "TIPSII10": ("A", f"unit_code = 'PC_GDP' and {COUNTRIES}"),
    "DEMO_PJANIND": ("A", f"{HAS.format('OLDDEP1')} and {COUNTRIES}"),
    "DEMO_R_PJANIND2": ("A", f"{HAS.format('OLDDEP1')} and {PL_REGIONS}"),
    "CEI_WM011": ("A", f"unit_code = 'PC' and {HAS.format('RCY')} and {COUNTRIES}"),
    "ERT_EFF_IC_M": ("M", f"unit_code = 'I20' and {HAS.format('REER_IC42_CPI')} and {COUNTRIES}"),
    "EDAT_LFSE_03": ("A", f"unit_code = 'PC' and {HAS.format('Y25-34')} and {HAS.format('ED5-8')} and {COUNTRIES}"),
    "DEMO_FIND": ("A", f"{HAS.format('TOTFERRT')} and {COUNTRIES}"),
    "DEMO_R_FIND2": ("A", f"{HAS.format('TOTFERRT')} and {PL_REGIONS}"),
    "NAMA_10_GDP": (
        "A",
        f"(unit_code in ('CP_MEUR', 'CLV_PCH_PRE', 'CP_MNAC') and {HAS.format('B1GQ')})"
        f" or (unit_code = 'PC_GDP' and {HAS.format('P51G')})",
    ),
    "NAMA_10_PC": ("A", "unit_code = 'CP_EUR_HAB' and " + HAS.format("B1GQ")),
    "UNE_RT_A": ("A", "unit_code = 'PC_ACT' and " + HAS.format("Y15-74")),
    "UNE_RT_M": ("M", all_of("SA", unit="PC_ACT", any_of=("TOTAL", "Y_LT25"))),
    "PRC_HICP_MINR": ("M", "unit_code = 'RCH_A' and " + HAS.format("TOTAL")),
    "LFSI_EMP_A": ("A", "unit_code = 'PC_POP' and " + HAS.format("Y20-64")),
    # zohelo-bi first slice, every source per KPI (O31, #2592); codes checked in Silver (Dev probe)
    "EI_BSIN_M_R2": ("M", all_of("BS-ICI", "SA", unit="BAL")),
    "EI_BSIN_Q_R2": ("Q", all_of("BS-ICU-PC", "SA")),
    "EI_BSCO_M": ("M", all_of("BS-CSMCI", "SA", unit="BAL")),
    "EI_BSSI_M_R2": ("M", all_of("BS-ESI-I", "SA")),
    "STS_INPR_M": ("M", all_of("PRD", "B-D", "CA", unit="PCH_SM")),
    "STS_COPR_M": ("M", all_of("PRD", "F", "CA", unit="PCH_SM")),
    "STS_INPP_M": ("M", all_of("PRC_PRR", "B-E36", "NSA", unit="PCH_SM")),
    "STS_TRTU_M": ("M", all_of("VOL_SLS", "G47", "CA", unit="PCH_SM")),
    "BOP_GDP6_Q": ("Q", all_of("CA", "BAL", "WRL_REST", "NSA", unit="PC_GDP")),
    "BOP_C6_Q": ("Q", all_of("MIO_EUR", "BAL", "WRL_REST", "S1", any_of=("CA", "G", "S", "IN1", "IN2"))),
    "BOP_FDI6_FLOW": ("A", all_of("MIO_EUR", "FDI", "NI", "TOTAL", "DI__D__F", "WRL_REST")),
    "NAMQ_10_GDP": (
        "Q",
        f"{HAS.format('SCA')} and {COUNTRIES} and ("
        f"(unit_code in ('CLV_PCH_SM', 'CLV_PCH_PRE') and ({HAS.format('B1GQ')} or {HAS.format('P51G')}))"
        f" or (unit_code = 'CON_PPCH_SM' and ({' or '.join(HAS.format(c) for c in ('P3', 'P5G', 'P6', 'P7'))})))",
    ),
    "NAMQ_10_LP_ULC": ("Q", all_of("SCA", unit="PCH_SM", any_of=("RLPR_PER", "NULC_PER"))),
    "JVS_Q_NACE2": ("Q", all_of("NSA", "B-S", "TOTAL", "JVR")),
    "LFSI_EMP_Q": ("Q", all_of("SA", "Y20-64", unit="PC_POP", any_of=("EMP_LFS", "ACT"))),
    "UNE_RT_Q": ("Q", all_of("SA", unit="PC_ACT", any_of=("Y15-74", "Y15-24"))),
    "LFST_R_LFU3RT": ("A", all_of("TOTAL", "T", "Y15-74", unit="PC", geo=f"({COUNTRIES} or {PL_REGIONS})")),
    "UNE_LTU_A": ("A", all_of("LTU", "Y15-74", unit="PC_ACT")),
    "EARN_GR_GPGR2": ("A", all_of("B-S_X_O", unit="PC")),
    "NAMA_10_AN6": ("A", all_of("N11G", unit="CP_MNAC")),
    "NAMA_10_A10": ("A", all_of("TOTAL", "B1G", unit="CP_MNAC")),
    "ILC_DI12": ("A", all_of("GINI_HND", "TOTAL")),
    "NAMA_10R_2GVAGR": ("A", all_of("B1GQ", unit="PCH_PRE", geo=f"({COUNTRIES} or {PL_REGIONS})")),
    "GOV_10A_EXP": ("A", all_of("S13", "GF10", "TE", unit="PC_GDP")),
}
COLUMNS = (
    "dataset_id, freq_code, unit_code, geo_code, time_period, value, status_code, "
    "breakdown_dimensions, breakdown_codes, raw_sha256, source_version"
)


def planned_files(table_name, row_filter):
    """Data files of the Iceberg table that may hold rows matching the filter (metadata only)."""
    import os

    from pyiceberg.catalog.rest import RestCatalog

    cat = RestCatalog(
        name="zohelo",
        warehouse=os.environ["STORAGE_LAKEHOUSE_CATALOG_NAME"],
        uri=os.environ["STORAGE_LAKEHOUSE_CATALOG_URL"],
        token=os.environ["STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN"],
    )
    scan = cat.load_table(table_name).scan(row_filter=row_filter)
    tasks = list(scan.plan_files())
    if any(task.delete_files for task in tasks):  # the files are read directly: deletes would be ignored
        raise RuntimeError(f"{table_name}: delete files present; read through the catalog instead")
    return [task.file.file_path for task in tasks]


def model(dbt, con):
    import os

    dbt.config(materialized="table", alias="src_eurostat")
    dbt.source("silver_entities", "eurostat_observations")  # declares the input (changed-only build)
    rule = " or ".join(f"(upper(dataset_id) = '{d}' and freq_code = '{f}' and ({k}))" for d, (f, k) in DATASETS.items())
    where = f"({rule}) and value is not null"
    if "STORAGE_LAKEHOUSE_CATALOG_URL" not in os.environ:  # local harness: the fake source table
        return con.query(f"select {COLUMNS} from r2.silver.eurostat_observations where {where}")
    from pyiceberg.expressions import In

    ids = [*DATASETS, *(d.lower() for d in DATASETS)]  # the id as Eurostat spells it, whichever case
    files = planned_files(("silver", "eurostat_observations"), In("dataset_id", ids))
    if not files:
        return con.query(f"select {COLUMNS} from r2.silver.eurostat_observations where false")
    paths = ", ".join("'" + p.replace("'", "''") + "'" for p in files)
    return con.query(f"select {COLUMNS} from read_parquet([{paths}]) where {where}")
