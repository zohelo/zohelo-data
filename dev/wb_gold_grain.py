"""Read-only Dev check (#2762 review): exact fact_grain key (series_key x date_key) of the World Bank Gold builds.

Reads production bronze.wgi_*, bronze.findex_observations, bronze.icp_observations (ICP: the series landed so far),
silver.wdi_countries and gold.dim_geography; renders the Silver models and the Gold build SQL in local DuckDB with
the World Bank mapping of poc_map_geography (WDI's mapping for every WDI economy code). Writes nothing.
"""

from __future__ import annotations

import json
from pathlib import Path
import re

import duckdb

from ingestion.common.r2 import catalog

BLOCKS = Path("warehouse/macros/poc_blocks.sql").read_text()
EU = "AUT BEL BGR HRV CYP CZE DNK EST FIN FRA DEU GRC HUN IRL ITA LVA LTU LUX MLT NLD POL PRT ROU SVK SVN ESP SWE"
EUQ = ",".join(f"'{c}'" for c in EU.split())


def clean(sql: str) -> str:
    sql = re.sub(r"\{\{\s*config\([^)]*\)\s*\}\}", "", sql)
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*source\([^,]*,\s*'([^']+)'\)\s*\}\}", lambda m: f"bronze_{m.group(1)}", sql)
    sql = re.sub(
        r"\{\{\s*poc_(\w+)_blocks\(\)\s*\}\}",
        lambda m: re.search(r"\{% macro poc_" + m.group(1) + r"_blocks\(\) %\}(.*?)\{% endmacro %\}", BLOCKS, re.S).group(1),
        sql,
    )
    return re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", lambda m: m.group(1), sql)


def main() -> int:
    cat = catalog()
    con = duckdb.connect()
    con.register("geo", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
    try:
        con.register("wdi_countries", cat.load_table(("silver", "wdi_countries")).scan().to_arrow())
        print("GRAIN | wdi_countries | silver table")
    except Exception as exc:  # noqa: BLE001  (silver.wdi_countries missing: rebuild it from Bronze, read-only)
        print(f"GRAIN | wdi_countries | silver missing ({type(exc).__name__}); built from bronze.wdi_bulk_metadata")
        con.register("bronze_wdi_bulk_metadata", cat.load_table(("bronze", "wdi_bulk_metadata")).scan().to_arrow())
        con.execute(f"create table wdi_countries as {clean(Path('warehouse/models/silver/world_bank_wdi/wdi_countries.sql').read_text())}")
    for t in ("wgi_observations", "wgi_metadata", "findex_observations", "icp_observations"):
        con.register(f"bronze_{t}", cat.load_table(("bronze", t)).scan().to_arrow())
    for m in ("world_bank_wgi/wgi_indicators", "world_bank_wgi/wgi_observations", "world_bank_findex/findex_observations", "world_bank_icp/icp_observations"):
        con.execute(f"create table {m.split('/')[1]} as {clean(Path(f'warehouse/models/silver/{m}.sql').read_text())}")
    con.execute("""create table wdi_map as
        select w.economy_code as source_code, coalesce(g.geography_key, g_agg.geography_key, '_X') as geography_key
        from wdi_countries w
        left join geo g on g.level_name = 'country' and w.iso2_code = g.iso2
        left join geo g_agg on g_agg.geography_key = 'WB:' || w.economy_code
        qualify row_number() over (partition by w.economy_code order by g.geography_key nulls last) = 1""")
    con.execute("""create table poc_map_geography as
        select s.source_system, m.source_code, m.geography_key from wdi_map m
        cross join (values ('world_bank_wgi'), ('world_bank_findex'), ('world_bank_icp')) s(source_system)""")
    out: dict = {"wdi iso2 sharing a key": con.execute(
        "select geography_key, list(source_code) from wdi_map where geography_key <> '_X' group by 1 having count(*) > 1").fetchall()}
    for src in ("wgi", "findex", "icp"):
        con.execute(f"create table b_{src} as {clean(Path(f'warehouse/models/gold/poc_build_{src}.sql').read_text())}")
        sk = "measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json"
        out[f"{src} rows"] = con.execute(f"select count(*), count(distinct measure_key), count(distinct geography_key), min(date_key), max(date_key) from b_{src}").fetchall()
        out[f"{src} fact_grain dupes (series_key x date_key)"] = con.execute(f"select count(*) from (select {sk}, date_key from b_{src} group by all having count(*) > 1)").fetchall()
        out[f"{src} keys with >1 source code"] = con.execute(f"select geography_key, list(distinct geography_source_code) from b_{src} where geography_key <> '_X' group by 1 having count(distinct geography_source_code) > 1").fetchall()
        out[f"{src} matched codes vs keys"] = con.execute(f"select count(distinct geography_source_code) filter (where geography_key <> '_X'), count(distinct geography_key) filter (where geography_key <> '_X') from b_{src}").fetchall()
        out[f"{src} unmatched"] = con.execute(f"select string_agg(distinct geography_source_code, ' ' order by geography_source_code) from b_{src} where geography_key = '_X'").fetchall()
        out[f"{src} PL latest"] = con.execute(f"select measure_key, date_key, value from b_{src} where geography_key = 'PL' qualify row_number() over (partition by measure_key order by date_key desc) = 1").fetchall()
        out[f"{src} EU countries"] = con.execute(f"select measure_key, count(distinct geography_key) from b_{src} where geography_source_code in ({EUQ}) and geography_key <> '_X' group by 1").fetchall()
    for k, v in out.items():
        print(f"GRAIN | {k} | {json.dumps(v, default=str, ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
