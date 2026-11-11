"""Dev trial (GL-016 ECB, GL-039 UN WPP, GL-021 UNCTAD): Landing → Bronze → Silver → Gold build SQL into scratch only.

Per source: the bulk_file Landing load into the Landing prefix zz_dev_src_intl/ (twice: the second must be
unchanged), the source's Bronze load into Iceberg namespace zz_dev_src_intl and lake prefix zz_dev_src_intl/
(twice: the second appends nothing), then the Silver and Gold build SQL (rendered from warehouse/models) in
local DuckDB over that Bronze table, with checks as queries printed to the log and annotations. Everything
written is removed in `finally`. Never writes production namespaces or prefixes.
"""

from __future__ import annotations

import importlib
from pathlib import Path
import re
import sys

import duckdb

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_src_intl_" + (sys.argv[1] if len(sys.argv) > 1 else "all")  # one scratch per parallel trial
EU = "AT,BE,BG,CY,CZ,DE,DK,EE,ES,FI,FR,GR,HR,HU,IE,IT,LT,LU,LV,MT,NL,PL,PT,RO,SE,SI,SK,GB,US,CH,NO".split(",")
GEO_SQL = (
    "create table geo as select * from (select unnest(?) as geography_key, 'country' as level_name) union all "
    "select * from (values ('EUROSTAT:EA','aggregate'),('EUROSTAT:EA19','aggregate'),('EUROSTAT:EA20','aggregate'),"
    "('EUROSTAT:EU','aggregate'),('EUROSTAT:EU27_2020','aggregate'),('WB:WLD','aggregate'),('_Z','special'),"
    "('_X','special')) t(geography_key, level_name)"
)


def render(path: str, subs: dict[str, str]) -> str:
    sql = Path(path).read_text()
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*config\([^)]*\)\s*\}\}", "", sql)
    for pattern, value in subs.items():
        sql = re.sub(pattern, lambda _m, v=value: v, sql)
    return sql


def macro(name: str) -> str:
    text = Path("warehouse/macros/poc_blocks.sql").read_text()
    return re.search(r"macro " + name + r"\(\) %\}(.*?)\{% endmacro", text, re.S).group(1)


def show(con, title: str, query: str, limit: int = 60) -> list:
    rows = [list(map(str, r)) for r in con.execute(query).fetchall()][:limit]
    print(f"### {title}: {rows}", flush=True)
    return rows


TRIALS = {
    "ecb": {
        "bronze": ["ecb_observations"],
        "silver": {"ecb_observations": "warehouse/models/silver/ecb/ecb_observations.sql"},
        "gold": ("warehouse/models/gold/poc_build_ecb.sql", "poc_ecb_blocks", {"ecb_observations": "ecb_observations"}),
        "checks": [
            ("rows per entity", "select entity, dataflow, count(*), count(distinct series_key), min(time_period), max(time_period), count(*) filter (where obs_value is null) from ecb_observations group by all order by 1"),
            ("unique", "select count(*) from (select dataflow, series_key, time_period from ecb_observations group by all having count(*) > 1)"),
            ("null period_start", "select count(*) from ecb_observations where period_start is null"),
            ("freqs", "select freq, count(*) from ecb_observations group by 1"),
            ("hicp areas", "select ref_area, count(*), min(time_period), max(time_period) from ecb_observations where entity = 'hicp_annual_rate' group by 1 order by 1"),
            ("PL latest", "select entity, series_key, time_period, obs_value from ecb_observations where ref_area = 'PL' or currency = 'PLN' qualify row_number() over (partition by entity order by time_period desc) <= 2"),
            ("units", "select entity, unit, unit_mult, count(*) from ecb_observations group by all order by 1"),
            ("aggregate titles", "select distinct entity, ref_area, left(title_compl, 90) from ecb_observations where ref_area in ('B0','B6','D0','I8','I9','I10','U2','V5') order by 1, 2"),
            ("irs series per area", "select ref_area, series_key, count(*), min(time_period), max(time_period) from ecb_observations where entity = 'irs_long_term' and ref_area in (select ref_area from ecb_observations where entity = 'irs_long_term' group by 1 having count(distinct series_key) > 1) group by all order by all"),
        ],
    },
    "un_wpp": {
        "bronze": ["un_wpp_rows"],
        "silver": {
            "un_wpp_demographic_indicators": "warehouse/models/silver/un_wpp/un_wpp_demographic_indicators.sql",
            "un_wpp_population_age_groups": "warehouse/models/silver/un_wpp/un_wpp_population_age_groups.sql",
        },
        "gold": ("warehouse/models/gold/poc_build_un_wpp.sql", "poc_un_wpp_blocks", {
            "un_wpp_demographic_indicators": "un_wpp_demographic_indicators",
            "un_wpp_population_age_groups": "un_wpp_population_age_groups",
        }),
        "checks": [
            ("bronze rows", "select entity, count(*) from bronze_un_wpp_rows group by 1"),
            ("dem rows", "select count(*), count(distinct loc_id), min(year), max(year) from un_wpp_demographic_indicators"),
            ("dem unique", "select count(*) from (select loc_id, year from un_wpp_demographic_indicators group by all having count(*) > 1)"),
            ("age unique", "select count(*) from (select loc_id, year from un_wpp_population_age_groups group by all having count(*) > 1)"),
            ("age groups not 21", "select count(*) from un_wpp_population_age_groups where age_groups <> 21"),
            ("PL and EU27", "select d.loc_id, d.year, d.tpopulation1july, d.tfr, d.lex, a.old_age_dependency_ratio, a.pop_total from un_wpp_demographic_indicators d join un_wpp_population_age_groups a using (loc_id, year) where d.loc_id in (616, 1551, 900) and d.year in (1950, 2000, 2023, 2024, 2050, 2100) order by 1, 2"),
            ("pop check", "select count(*) filter (where abs(d.tpopulation1july - a.pop_total) > 0.01 * a.pop_total) from un_wpp_demographic_indicators d join un_wpp_population_age_groups a using (loc_id, year)"),
        ],
    },
    "unctad": {
        "bronze": ["unctad_rows"],
        "silver": {
            "unctad_fdi_flows_stock": "warehouse/models/silver/unctad/unctad_fdi_flows_stock.sql",
            "unctad_merchandise_trade": "warehouse/models/silver/unctad/unctad_merchandise_trade.sql",
            "unctad_services_trade": "warehouse/models/silver/unctad/unctad_services_trade.sql",
            "unctad_digital_services_trade": "warehouse/models/silver/unctad/unctad_digital_services_trade.sql",
            "unctad_commodity_prices": "warehouse/models/silver/unctad/unctad_commodity_prices.sql",
            "unctad_liner_shipping_connectivity": "warehouse/models/silver/unctad/unctad_liner_shipping_connectivity.sql",
            "unctad_merchant_fleet": "warehouse/models/silver/unctad/unctad_merchant_fleet.sql",
            "unctad_seaborne_trade": "warehouse/models/silver/unctad/unctad_seaborne_trade.sql",
            "unctad_port_calls": "warehouse/models/silver/unctad/unctad_port_calls.sql",
        },
        "gold": ("warehouse/models/gold/poc_build_unctad.sql", "poc_unctad_blocks", {m: m for m in ["unctad_fdi_flows_stock", "unctad_merchandise_trade", "unctad_services_trade"]}),
        "checks": [
            ("bronze rows", "select entity, count(*) from bronze_unctad_rows group by 1 order by 1"),
            ("unctad_fdi_flows_stock unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_fdi_flows_stock group by all having count(*) > 1)), min(period), max(period) from unctad_fdi_flows_stock"),
            ("unctad_merchandise_trade unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_merchandise_trade group by all having count(*) > 1)), min(period), max(period) from unctad_merchandise_trade"),
            ("unctad_services_trade unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_services_trade group by all having count(*) > 1)), min(period), max(period) from unctad_services_trade"),
            ("unctad_digital_services_trade unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_digital_services_trade group by all having count(*) > 1)), min(period), max(period) from unctad_digital_services_trade"),
            ("unctad_commodity_prices unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_commodity_prices group by all having count(*) > 1)), min(period), max(period) from unctad_commodity_prices"),
            ("unctad_liner_shipping_connectivity unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_liner_shipping_connectivity group by all having count(*) > 1)), min(period), max(period) from unctad_liner_shipping_connectivity"),
            ("unctad_merchant_fleet unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_merchant_fleet group by all having count(*) > 1)), min(period), max(period) from unctad_merchant_fleet"),
            ("unctad_seaborne_trade unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_seaborne_trade group by all having count(*) > 1)), min(period), max(period) from unctad_seaborne_trade"),
            ("unctad_port_calls unique", "select count(*), (select count(*) from (select period, economy, dimensions from unctad_port_calls group by all having count(*) > 1)), min(period), max(period) from unctad_port_calls"),
            ("EU economies", "select distinct economy, economy_label from unctad_fdi_flows_stock where economy in ('040','056','100','191','196','203','208','233','246','250','251','276','300','348','372','380','428','440','442','470','528','616','620','642','703','705','724','752','3412','0000') order by 1"),
            ("PL fdi", "select period, dimensions, measure_values, measure_missing from unctad_fdi_flows_stock where economy = '616' and period >= '2022' order by 1, 2"),
        ],
    },
}


def trial(source: str) -> None:
    spec = TRIALS[source]
    for attempt in (1, 2):
        annotate("notice", f"{source} landing load {attempt}", {"exit": bulk_file.main(source, "load", landing_root=SCRATCH)})
    loader = importlib.import_module(f"ingestion.to_bronze.{source}.load")
    for attempt in (1, 2):
        annotate("notice", f"{source} bronze load {attempt}", {"exit": loader.load(0, landing_root=SCRATCH, namespace=SCRATCH)})
    con = duckdb.connect()
    for table in spec["bronze"]:
        con.register(f"bronze_{table}", catalog().load_table((SCRATCH, table)).scan().to_arrow())
    rejected = [t for t in catalog().list_tables(SCRATCH) if t[1].endswith("rejected_files")]
    for t in rejected:
        show(con, f"{source} rejected {t[1]}", "select 1") if False else None
        arrow = catalog().load_table(t).scan().to_arrow()
        print(f"### {source} rejected {t[1]}: {arrow.to_pylist()[:5]}", flush=True)
    for name, path in spec["silver"].items():
        sql = render(path, {r"\{\{\s*source\('[a-z_]+',\s*'([a-z_]+)'\)\s*\}\}": None} if False else {})
        if "unctad_table(" in sql:
            from jinja2 import Environment

            text = Path("warehouse/macros/unctad.sql").read_text() + sql
            sql = Environment().from_string(text).render(source=lambda _s, t: f"bronze_{t}")
        sql = re.sub(r"\{\{\s*source\('[a-z_]+',\s*'([a-z_0-9]+)'\)\s*\}\}", lambda m: f"bronze_{m.group(1)}", sql)
        sql = re.sub(r"\{\{\s*ref\('([a-z_0-9]+)'\)\s*\}\}", lambda m: m.group(1), sql)
        con.execute(f"create table {name} as {sql}")
    results = {name: con.execute(f"select count(*) from {name}").fetchone()[0] for name in spec["silver"]}
    for title, query in spec["checks"]:
        results[title] = show(con, f"{source} {title}", query)
    con.execute(GEO_SQL, [EU])
    path, blocks, refs = spec["gold"]
    sql = render(path, {r"\{\{\s*" + blocks + r"\(\)\s*\}\}": macro(blocks)})
    if source == "unctad":
        sql = sql.replace("{{ poc_unctad_areas() }}", macro("poc_unctad_areas"))
    sql = sql.replace("{{ ref('poc_dim_geography') }}", "geo")
    for ref, table in refs.items():
        sql = sql.replace("{{ ref('" + ref + "') }}", table)
    con.execute(f"create table build as {sql}")
    results["gold rows"] = show(con, f"{source} gold", "select block_id, measure_key, geography_key, frequency, count(*), min(date_key), max(date_key) from build group by all order by all", 200)
    show(con, f"{source} gold grain detail", "select measure_key, geography_key, breakdown_json, count(*) from build group by measure_key, geography_key, coalesce(geography_source_code,''), breakdown_json, date_key having count(*) > 1 limit 20")
    results["gold grain"] = show(con, f"{source} gold grain", "select count(*) from (select measure_key, geography_key, coalesce(geography_source_code,''), breakdown_json, date_key from build group by all having count(*) > 1)")
    results["gold unmatched"] = show(con, f"{source} gold unmatched", "select geography_source_code, count(*) from build where geography_key = '_X' group by 1 order by 2 desc")
    results["gold PL latest"] = show(con, f"{source} gold PL", "select measure_key, date_key, value, observation_status_key from build where geography_key in ('PL', '_Z') qualify row_number() over (partition by measure_key order by date_key desc) <= 2 order by 1, 2")
    annotate("notice", f"{source} trial (scratch)", results)


def cleanup() -> None:
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for bucket_var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    left = sum(len(p.get("Contents", [])) for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME") for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/"))
    removed["objects_left"] = left
    annotate("notice", "scratch removed", removed)


def main() -> int:
    sources = sys.argv[1:] or list(TRIALS)
    failed = 0
    try:
        for source in sources:
            try:
                trial(source)
            except Exception as exc:
                failed += 1
                annotate("error", f"{source} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
