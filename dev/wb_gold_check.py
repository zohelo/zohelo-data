"""Dev check (#2736 Gold): World Bank WGI, Findex, ICP Gold build SQL over scratch Silver.

Lands and loads the three sources into scratch (zz_dev_wbg), builds their Silver tables in local DuckDB, then
runs the Gold build stage SQL (warehouse/models/gold/poc_build_<src>.sql, blocks from the macros) with the
geography mapping of poc_map_geography against read-only copies of production gold.dim_geography and
silver.wdi_countries. Prints row counts, PL/EU27 coverage, unmatched codes, grain duplicates and Poland's
latest values. Scratch is removed in `finally`; production is only read.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import re

import duckdb

from dev import wb_trial
from ingestion.common.r2 import catalog

wb_trial.SCRATCH = "zz_dev_wbg"
EU = wb_trial.EUQ
BLOCKS = Path("warehouse/macros/poc_blocks.sql").read_text()


def macro(name: str) -> str:
    return re.search(r"\{% macro " + name + r"\(\) %\}(.*?)\{% endmacro %\}", BLOCKS, re.S).group(1)


def render_gold(src: str) -> str:
    sql = Path(f"warehouse/models/gold/poc_build_{src}.sql").read_text()
    sql = re.sub(r"\{\{\s*config\([^)]*\)\s*\}\}", "", sql)
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*poc_(\w+)_blocks\(\)\s*\}\}", lambda m: macro(f"poc_{m.group(1)}_blocks"), sql)
    return re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", lambda m: m.group(1), sql)


def mapping_sql() -> str:
    text = Path("warehouse/models/gold/poc_map_geography.sql").read_text()
    parts = []
    for cte in ("wb_wgi", "wb_findex", "wb_icp"):
        body = re.search(cte + r" as \((.*?)\n\)", text, re.S).group(1)
        parts.append(re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", lambda m: m.group(1), body))
    return "create table poc_map_geography as with geo as (select * from poc_dim_geography) " + " union all ".join(parts)


def main() -> int:
    cat = catalog()
    con = duckdb.connect()
    out: dict = {}
    try:
        con.register("poc_dim_geography", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
        con.register("wdi_countries", cat.load_table(("silver", "wdi_countries")).scan().to_arrow())
        for source in ("world_bank_wgi", "world_bank_findex", "world_bank_icp"):
            plan = wb_trial.PLAN[source]
            wb_trial.land(source)
            importlib.import_module(f"ingestion.to_bronze.{source}.load").load(
                0, landing_root=wb_trial.SCRATCH, namespace=wb_trial.SCRATCH
            )
            for table in plan["tables"]:
                con.register(f"bronze_{table}", cat.load_table((wb_trial.SCRATCH, table)).scan().to_arrow())
            for model in plan["models"]:
                name = model.split("/")[-1]
                con.execute(f"create table {name} as {wb_trial.render(f'{wb_trial.SILVER}/{model}.sql', {})}")
        con.execute(mapping_sql())
        out["map"] = con.execute(
            "select source_system, count(*), count(*) filter (where geography_key = '_X') from poc_map_geography group by 1"
        ).fetchall()
        out["unmatched"] = con.execute(
            "select source_system, string_agg(source_code, ' ' order by source_code) from poc_map_geography where geography_key = '_X' group by 1"
        ).fetchall()
        out["EU27 key in geo"] = con.execute(
            "select geography_key, level_name from poc_dim_geography where geography_key in ('WB:EUU', 'EUROSTAT:EU27_2020', 'PL')"
        ).fetchall()
        for src in ("wgi", "findex", "icp"):
            con.execute(f"create table build_{src} as {render_gold(src)}")
            out[f"{src} rows"] = con.execute(
                f"select count(*), count(distinct measure_key), count(distinct geography_key), min(date_key), max(date_key) from build_{src}"
            ).fetchall()
            out[f"{src} grain dupes"] = con.execute(
                f"select count(*) from (select measure_key, geography_key, geography_source_code, date_key from build_{src} group by all having count(*) > 1)"
            ).fetchall()
            out[f"{src} PL latest"] = con.execute(
                f"select measure_key, date_key, value, unit_code from build_{src} where geography_key = 'PL' qualify row_number() over (partition by measure_key order by date_key desc) = 1"
            ).fetchall()
            out[f"{src} EU countries"] = con.execute(
                f"select measure_key, count(distinct b.geography_key) from build_{src} b join poc_dim_geography g using (geography_key) where g.level_name = 'country' and geography_source_code in ({EU}) group by 1"
            ).fetchall()
            out[f"{src} value range"] = con.execute(f"select measure_key, min(value), max(value) from build_{src} group by 1").fetchall()
        for k, v in out.items():
            print(f"GOLD | {k} | {json.dumps(v, default=str, ensure_ascii=False)}")
    finally:
        wb_trial.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
