"""Dev trial of Our World in Data (CD-031, #2982) on the branch's code: Landing (bulk_file, scratch prefix
zz_dev_owid/01_landing) twice, landing verify and inspect, Bronze (ingestion/to_bronze/owid) into namespace
zz_dev_owid twice, a Bronze profile, the branch's Silver SQL and tests and the Gold build stage rendered with Jinja
and run in DuckDB (geography from gold.dim_geography, read only), Poland spot values, value bounds, and the origin
licences of each chart read from the indicator metadata. Everything written is removed in finally. Deleted before
merge."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import requests

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.owid import load as owid_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "owid"
SCRATCH = "zz_dev_owid"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/owid.sql").read_text()
BOUNDS = {  # measure chart → (min, max), as in warehouse/tests/metrics_value_range.sql
    "consumption_co2_per_capita": (0, 100),
    "electoral_democracy_index": (0, 1),
    "gdp_per_capita_maddison": (100, 300000),
}
UA = bulk_file.USER_AGENT


def quiet(level, title, data):  # the shared code's own annotations go to the log (10 per step)
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


bulk_bronze.annotate = quiet
bulk_file.annotate = quiet


def render(path_or_text, tables: dict[str, str]) -> str:
    text = path_or_text.read_text() if isinstance(path_or_text, Path) else path_or_text
    text = re.sub(r"\{\{ config\(.*?\) \}\}", "", text, flags=re.S)
    tpl = jinja2.Environment().from_string(MACROS + text)
    return tpl.render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def one(con, sql):
    return con.sql(sql).fetchall()[0][0]


def rows(con, sql):
    rel = con.sql(sql)
    names = [d[0] for d in rel.description]
    return [dict(zip(names, r, strict=True)) for r in rel.fetchall()]


def show(con, title, sql, limit=80):
    print(f"\n=== {title}", flush=True)
    out = rows(con, sql)
    for r in out[:limit]:
        print(r, flush=True)
    if len(out) > limit:
        print(f"... {len(out)} rows")


def landing(client, bucket) -> dict:
    specs = bulk_file.declaration(SOURCE)
    runs = []
    for attempt in (1, 2):
        outcomes = {}
        for n, spec in enumerate(specs):
            if n:
                time.sleep(2)
            try:
                res = bulk_file.load_file(client, bucket, SOURCE, spec, LAND)
                outcomes[spec["entity"]] = res["outcome"]
            except bulk_file.PermanentError as exc:  # e.g. 403 "Data is non-redistributable": recorded, not retried
                outcomes[spec["entity"]] = f"refused: {str(exc)[:120]}"
        runs.append(outcomes)
    held = {}
    for spec in specs:
        receipts = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], LAND))
        held[spec["entity"]] = {"copies": len(receipts), "bytes": receipts[-1]["size_bytes"] if receipts else None}
    verify = bulk_file.main(SOURCE, "verify", landing_root=LAND)
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=LAND)
    return {"run1": runs[0], "run2": runs[1], "held": held, "verify_status": verify, "inspect_status": inspect}


def bronze(cat) -> tuple[dict, dict]:
    counts = []
    for attempt in (1, 2):
        owid_load.load(0, landing_root=LAND, namespace=SCRATCH)
        state = {}
        for name in ("owid_values", "owid_charts"):
            table = cat.load_table((SCRATCH, name))
            files = table.inspect.data_files()
            state[name] = {"rows": table.scan().to_arrow().num_rows, "files": files.num_rows}
        counts.append(state)
    try:
        rejected = cat.load_table((SCRATCH, "owid_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001 - no rejections table when nothing was rejected
        rejected = []
    tables = {
        "values": cat.load_table((SCRATCH, "owid_values")).scan().to_arrow(),
        "charts": cat.load_table((SCRATCH, "owid_charts")).scan().to_arrow(),
    }
    return (
        {
            "load1": counts[0],
            "load2": counts[1],
            "second_load_changed": counts[0] != counts[1],
            "rejected": [(r.get("entity"), (r.get("error") or "")[:160]) for r in rejected],
        },
        tables,
    )


def licences() -> list[dict]:
    """origins[].license of each chart's value column, from the indicator metadata (read only)."""
    out = []
    for slug in (
        "consumption-co2-per-capita",
        "electoral-democracy-index",
        "gdp-per-capita-maddison-project-database",
    ):
        time.sleep(2)
        meta = requests.get(f"https://ourworldindata.org/grapher/{slug}.metadata.json", headers={"User-Agent": UA},
                            timeout=60).json()
        for title, col in meta["columns"].items():
            if col.get("shortName") in ("owid_region",):
                continue
            url = col.get("fullMetadata") or (
                f"https://api.ourworldindata.org/v1/indicators/{col['owidVariableId']}.metadata.json"
                if col.get("owidVariableId") else None)
            row = {"chart": slug, "column": col.get("shortName"), "url": url, "citation": meta["chart"].get("citation")}
            if url:
                time.sleep(2)
                r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
                row["http"] = r.status_code
                if r.status_code == 200:
                    j = r.json()
                    print(json.dumps(j, indent=1)[:15000], flush=True)
                    row["license"] = j.get("license")
                    row["origins"] = [
                        {"producer": o.get("producer"), "title": o.get("title"), "license": o.get("license"),
                         "attribution": o.get("attribution"), "urlMain": o.get("urlMain"),
                         "citation": (o.get("citationFull") or "")[:300]}
                        for o in j.get("origins", [])
                    ]
            out.append(row)
    return out


def main(argv) -> int:
    client = s3()
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    try:
        land = landing(client, landing_bucket)
        print("LANDING", land, flush=True)
        annotate("notice", "owid trial landing", land)

        bres, tables = bronze(cat)
        con.register("bronze_values", tables["values"])
        con.register("bronze_charts", tables["charts"])
        profile = rows(con, """
            select entity, count(*) as cells, count(distinct entity_name || '|' || year) as entity_years,
                count(distinct entity_name) as entities, count(distinct nullif(code, '')) as codes,
                count(distinct case when code = '' then entity_name end) as entities_without_code,
                min(try_cast(year as integer)) as min_year, max(try_cast(year as integer)) as max_year,
                count(*) filter (where try_cast(year as integer) is null) as bad_years,
                string_agg(distinct column_name, ',') as columns
            from bronze_values group by entity order by entity""")
        values = rows(con, """
            select entity, column_name, count(*) as cells,
                count(*) filter (where try_cast(value as double) is null) as non_numeric,
                count(*) filter (where try_cast(value as double) = 0) as zeros,
                min(try_cast(value as double)) as min_v, max(try_cast(value as double)) as max_v
            from bronze_values group by all order by all""")
        nonnum = rows(con, """
            select entity, column_name, value, count(*) as n from bronze_values
            where try_cast(value as double) is null and column_name not in ('owid_region', 'gdp_per_capita__annotations')
            group by all order by n desc limit 20""")
        attrs = rows(con, """
            select column_name, value, count(*) as n from bronze_values
            where column_name in ('owid_region', 'gdp_per_capita__annotations') group by all order by n desc limit 15""")
        bres.update({"profile": profile})
        print("BRONZE", bres, values, nonnum, attrs, flush=True)
        annotate("notice", "owid trial bronze", bres)
        annotate("notice", "owid trial bronze values", {"columns": values, "non_numeric_values": nonnum,
                                                        "attribute_values": attrs})
        show(con, "charts metadata", "select * exclude (raw_sha256, ingested_at) from bronze_charts")

        con.sql("create table owid_observations as " + render(
            ROOT / "warehouse/models/silver/owid/owid_observations.sql", {"owid_values": "bronze_values"}))
        con.sql("create table owid_charts as " + render(
            ROOT / "warehouse/models/silver/owid/owid_charts.sql", {"owid_charts": "bronze_charts"}))
        silver_tests = {
            "obs_dup_grain": one(con, "select count(*) from (select 1 from owid_observations group by chart, entity_name, year having count(*) > 1)"),
            "obs_null_keys": one(con, "select count(*) from owid_observations where chart is null or entity_name is null or year is null or indicator is null or value is null or raw_sha256 is null"),
            "obs_bad_chart": one(con, "select count(*) from owid_observations where chart not in ('consumption_co2_per_capita', 'electoral_democracy_index', 'gdp_per_capita_maddison') or indicator not in ('consumption_emissions_per_capita', 'electdem_vdem__estimate_best', 'gdp_per_capita')"),
            "charts_dup_grain": one(con, "select count(*) from (select 1 from owid_charts group by chart, short_name having count(*) > 1)"),
            "charts_null_keys": one(con, "select count(*) from owid_charts where chart is null or short_name is null or raw_sha256 is null"),
        }
        for name in ("owid_observations_charts", "owid_charts_declared"):
            failing = rows(con, render(ROOT / f"warehouse/tests/{name}.sql",
                                       {"owid_observations": "owid_observations", "owid_charts": "owid_charts"}))
            silver_tests[name] = failing
        silver = {
            "rows": rows(con, """select chart, count(*) as n, min(year) as min_year, max(year) as max_year,
                count(distinct entity_name) as entities, count(*) filter (where entity_code is null) as no_code_rows,
                count(*) filter (where value = 0) as zeros, count(*) filter (where annotation is not null) as annotated
                from owid_observations group by chart order by chart"""),
            "total": one(con, "select count(*) from owid_observations"),
            "charts_rows": one(con, "select count(*) from owid_charts"),
            "tests": silver_tests,
            "code_year_dups": rows(con, """select chart, entity_code, year, string_agg(entity_name, ' | ') as names
                from owid_observations where entity_code is not null group by all having count(*) > 1 limit 10"""),
            "versions": rows(con, "select chart, short_name, unit, last_updated, next_update from owid_charts order by all"),
        }
        print("SILVER", silver, flush=True)
        annotate("notice", "owid trial silver", silver)

        geo = cat.load_table(("gold", "dim_geography")).scan().to_arrow()
        con.register("dim_geography", geo)
        con.sql("create table geo_map as with geo as (select * from dim_geography), "
                + render("{{ poc_geography__owid() }}", {}) + " select * from owid")
        con.sql("create table build_owid as " + render(
            ROOT / "warehouse/models/gold/poc_build_owid.sql",
            {"poc_map_geography": "geo_map", "owid_observations": "owid_observations", "owid_charts": "owid_charts"}))
        bounds = " union all ".join(f"select '{k}' as chart, {lo} as lo, {hi} as hi" for k, (lo, hi) in BOUNDS.items())
        gold = {
            "rows": rows(con, """select block_id, count(*) as n, count(distinct geography_key) as areas,
                min(date_key) as first, max(date_key) as last, any_value(measure_key) as measure_key,
                any_value(source_version) as source_version from build_owid group by block_id order by all"""),
            "total": one(con, "select count(*) from build_owid"),
            "dup_series_date": one(con, """select count(*) from (select 1 from build_owid
                group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)"""),
            "map_dup_codes": one(con, "select count(*) from (select source_code from geo_map group by 1 having count(*) > 1)"),
            "null_keys": one(con, "select count(*) from build_owid where measure_key is null or geography_key is null or date_key is null or value is null"),
            "out_of_bounds": rows(con, f"""select b.block_id, count(*) as n, min(value) as min_v, max(value) as max_v
                from build_owid as b join ({bounds}) as x on b.block_id = x.chart
                where b.value < x.lo or b.value > x.hi group by all"""),
            "silver_rows_not_built": rows(con, """select o.chart, count(*) as n,
                count(*) filter (where o.entity_code is null) as no_code,
                count(*) filter (where o.entity_code like 'OWID_%') as owid_codes,
                count(*) filter (where o.entity_code is not null and o.entity_code not like 'OWID_%') as iso_codes
                from owid_observations as o left join geo_map as g on o.entity_code = g.source_code
                where g.source_code is null group by all order by all"""),
            "codes_not_mapped": one(con, """select string_agg(distinct o.entity_code || '=' || o.entity_name, '; ')
                from owid_observations as o left join geo_map as g on o.entity_code = g.source_code
                where g.source_code is null and o.entity_code is not null"""),
            "no_code_entities": one(con, """select string_agg(distinct entity_name, '; ') from owid_observations
                where entity_code is null"""),
        }
        print("GOLD", gold, flush=True)
        annotate("notice", "owid trial gold", gold)
        poland = rows(con, """select block_id, period_label, value, unit_code, source_version from build_owid
            where geography_key = 'PL' qualify row_number() over (partition by block_id order by date_key desc) <= 3
            order by block_id, period_label""")
        first_pl = rows(con, """select block_id, min(period_label) as first, max(period_label) as last, count(*) as n
            from build_owid where geography_key = 'PL' group by all order by all""")
        world = rows(con, """select block_id, max(period_label) as last, arg_max(value, date_key) as value
            from build_owid where geography_key = 'WB:WLD' group by all order by all""")
        annotate("notice", "owid trial Poland", {"newest": poland, "range": first_pl, "world_newest": world})

        lic = licences()
        print("LICENCES", lic, flush=True)
        annotate("notice", "owid trial origin licences", lic)
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        annotate("notice", "owid trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
