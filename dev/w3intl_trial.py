"""Dev trial (wave 3, international sources): Landing → Bronze → Silver → Gold into scratch, each step twice.

Per source: the bulk_file Landing load into the Landing prefix zz_dev_w3intl/ (twice: the second must write
nothing), the source's Bronze load into Iceberg namespace zz_dev_w3intl and lake prefix zz_dev_w3intl/ (twice: the
second appends nothing), then the Silver model, the source's singular tests and the Gold build stage (rendered with
Jinja from warehouse/models and warehouse/macros) in local DuckDB over that Bronze table, each built twice (same
counts). One or two compact annotations per source. Everything written is removed in `finally`; never writes a
production namespace or prefix.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import re
import sys

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_w3intl"
EU = {
    "AT": "AUT", "BE": "BEL", "BG": "BGR", "CY": "CYP", "CZ": "CZE", "DE": "DEU", "DK": "DNK", "EE": "EST",
    "ES": "ESP", "FI": "FIN", "FR": "FRA", "GR": "GRC", "HR": "HRV", "HU": "HUN", "IE": "IRL", "IT": "ITA",
    "LT": "LTU", "LU": "LUX", "LV": "LVA", "MT": "MLT", "NL": "NLD", "PL": "POL", "PT": "PRT", "RO": "ROU",
    "SE": "SWE", "SI": "SVN", "SK": "SVK", "GB": "GBR", "US": "USA", "NO": "NOR", "CH": "CHE", "UA": "UKR",
}
MACROS = "".join(Path(p).read_text() for p in ("warehouse/macros/poc_blocks.sql", "warehouse/macros/m49.sql"))

TRIALS = {
    "eea": {
        "bronze": "eea_ghg_inventory",
        "silver": "eea_ghg_inventory",
        "grain": "country_code, format_name, pollutant_name, sector_code, year",
        "checks": {
            "per entity": "select count(*), count(distinct country_code), min(year), max(year), count(*) filter (where value is null), count(distinct pollutant_name), count(distinct sector_code) from eea_ghg_inventory",
            "countries": "select string_agg(distinct country_code || '/' || coalesce(country_code_3, '') , ',' order by country_code || '/' || coalesce(country_code_3, '')) from eea_ghg_inventory",
            "formats": "select format_name, count(*) from eea_ghg_inventory group by 1",
            "pollutants": "select pollutant_name, unit, count(*) from eea_ghg_inventory group by all order by 3 desc limit 25",
            "top sectors": "select sector_code, sector_name, parent_sector_code, count(*) from eea_ghg_inventory where parent_sector_code like 'Sectors/Totals%' or sector_code not like '%.%' group by all order by 1 limit 40",
            "notations": "select notation, count(*), count(*) filter (where value is not null) from eea_ghg_inventory group by 1 order by 2 desc limit 15",
            "PL total": "select sector_code, sector_name, year, value, unit from eea_ghg_inventory where country_code = 'PL' and pollutant_name = 'All greenhouse gases - (CO2 equivalent)' and year = (select max(year) from eea_ghg_inventory) and sector_code not like '%.%' order by 1 limit 30",
        },
    },
    "unhcr": {
        "bronze": "unhcr_population",
        "silver": "unhcr_population",
        "grain": "entity, year, coa, coo",
        "tests": ["unhcr_selection"],
        "checks": {
            "per entity": "select entity, count(*), count(distinct coa), count(distinct coo), min(year), max(year) from unhcr_population group by 1 order by 1",
            "nulls": "select entity, count(*) filter (where refugees is null), count(*) filter (where asylum_seekers is null), count(*) filter (where oip is null) from unhcr_population group by 1",
            "iso dup": "select entity, year, coa_iso, coo_iso, count(*) from unhcr_population group by all having count(*) > 1 limit 5",
            "PL": "select year, refugees, asylum_seekers, stateless, oip from unhcr_population where entity = 'population_by_asylum' and coa_iso = 'POL' order by year desc limit 4",
            "PL top origins": "select coo_iso, coo_name, refugees from unhcr_population where entity = 'poland_by_origin' and year = (select max(year) from unhcr_population) order by refugees desc nulls last limit 5",
            "max": "select max(refugees), max(asylum_seekers), arg_max(coa_iso, refugees) from unhcr_population where entity = 'population_by_asylum'",
        },
    },
}


def show(con, title: str, query: str, limit: int = 80) -> list:
    rows = [[str(v) for v in r] for r in con.execute(query).fetchall()][:limit]
    print(f"### {title}: {json.dumps(rows)}", flush=True)
    return rows


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    text = MACROS + Path(path).read_text()
    return Environment(extensions=["jinja2.ext.do"]).from_string(text).render(
        config=lambda **_k: "",
        ref=lambda name: refs.get(name, name),
        source=lambda _s, table: sources[table],
    )


def render_activated() -> str:
    return Environment(extensions=["jinja2.ext.do"]).from_string(MACROS + "{{ poc_activated_blocks() }}").render(ref=lambda n: n, config=lambda **_k: "")


def build_geo(con) -> None:
    rows = [(iso2, iso3, "country") for iso2, iso3 in EU.items()]
    rows += [("EUROSTAT:EU27_2020", None, "aggregate"), ("WB:WLD", None, "aggregate"), ("_X", None, "special")]
    con.execute("create or replace table geo (geography_key varchar, iso2 varchar, iso3 varchar, level_name varchar)")
    con.executemany("insert into geo values (?, ?, ?, ?)", [(k, k if lv == "country" else None, i3, lv) for k, i3, lv in rows])


def trial(source: str) -> dict:
    spec = TRIALS[source]
    result: dict = {}
    specs = None
    if source == "eea":
        from ingestion.to_landing import eea

        specs = eea.resolve()
        result["resolved"] = {k: specs[0][k] for k in ("url", "record")}
    result["landing"] = [bulk_file.main(source, "load", landing_root=SCRATCH, specs=specs) for _ in (1, 2)]
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = {}
    for f in specs or bulk_file.declaration(source):
        copies = bulk_file.landed(client, bucket, bulk_file.prefix(source, f["entity"], SCRATCH))
        held[f["entity"]] = [len(copies), copies[-1]["size_bytes"] if copies else 0]
    result["landed_copies_bytes"] = held  # 1 copy each: the second load wrote nothing
    loader = importlib.import_module(f"ingestion.to_bronze.{source}.load")
    result["bronze_exit"] = [loader.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    bronze = cat.load_table((SCRATCH, spec["bronze"]))
    files = [t.file.file_path for t in bronze.scan().plan_files()]
    arrow = bronze.scan().to_arrow()
    result["bronze"] = {"files": len(files), "rows": arrow.num_rows}
    try:
        rejected = cat.load_table((SCRATCH, f"{source}_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    result["rejected"] = [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:5]
    con = duckdb.connect()
    con.register("bronze_obs", arrow)
    model(con, source, result)
    annotate("notice", f"{source} trial L/B/S (scratch {SCRATCH})", {k: result.get(k) for k in ("resolved", "landing", "landed_copies_bytes", "bronze_exit", "bronze", "rejected", "silver_rows_two_builds", "silver_duplicate_keys", "silver_null_keys", "tests", "per entity", "PL", "max")})
    annotate("notice", f"{source} trial Gold (scratch)", {k: result.get(k) for k in ("gold_rows_two_builds", "gold_dup_series_date", "gold measures", "gold PL latest", "build_check", "grain_check", "PL top origins")})
    return result


def model(con, source: str, result: dict) -> None:
    spec = TRIALS[source]
    silver = spec["silver"]
    sql = render(f"warehouse/models/silver/{source}/{silver}.sql", {}, {spec["bronze"]: "bronze_obs"})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table {silver} as {sql}")
        counts.append(con.execute(f"select count(*) from {silver}").fetchone()[0])
    result["silver_rows_two_builds"] = counts
    result["silver_duplicate_keys"] = con.execute(
        f"select count(*) from (select {spec['grain']} from {silver} group by all having count(*) > 1)"
    ).fetchone()[0]
    keys = " or ".join(f"{k.strip()} is null" for k in spec["grain"].split(",")) + " or raw_sha256 is null"
    result["silver_null_keys"] = con.execute(f"select count(*) from {silver} where {keys}").fetchone()[0]
    result["tests"] = {
        t: con.execute(f"select count(*) from ({render(f'warehouse/tests/{t}.sql', {silver: silver}, {})})").fetchone()[0]
        for t in spec.get("tests", [])
    }
    for title, query in spec["checks"].items():
        result[title] = show(con, f"{source} {title}", query)
    if source == "eea":
        annotate("notice", "eea silver 1/2", {k: result[k] for k in ("countries", "formats", "notations", "pollutants")})
        annotate("notice", "eea silver 2/2", {k: result[k] for k in ("top sectors", "PL total")})
    if not Path(f"warehouse/models/gold/poc_build_{source}.sql").exists():
        return
    build_geo(con)
    gmap_sql = render("warehouse/models/gold/poc_map_geography.sql", {}, {})
    cte = re.search(rf"\n{source} as \((.*?)\n\),?\n(?:\n|select)", gmap_sql, re.S).group(1)
    con.execute(f"create or replace table gmap as {cte}")
    con.execute(f"create or replace view gmap_all as select '{source}' as source_system, source_code, geography_key from gmap")
    build_sql = render(f"warehouse/models/gold/poc_build_{source}.sql", {silver: silver, "poc_map_geography": "gmap_all"}, {})
    con.execute(f"create or replace table activated as select * from ({render_activated()}) where source_system = '{source}'")
    gold = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        gold.append(con.execute("select count(*) from build").fetchone()[0])
    result["gold_rows_two_builds"] = gold
    result["gold_dup_series_date"] = con.execute(
        "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as sk, date_key "
        "from build group by all having count(*) > 1)"
    ).fetchone()[0]
    result["gold measures"] = show(con, f"{source} gold measures", "select measure_key, frequency, count(distinct geography_key), count(*), min(date_key), max(date_key), max(value) from build group by all order by 1")
    result["gold PL latest"] = show(con, f"{source} gold PL", "select measure_key, breakdown_json, date_key, value from build where geography_key = 'PL' and breakdown_json = '{}' qualify row_number() over (partition by measure_key, breakdown_json order by date_key desc) = 1 order by 1", 12)
    result["build_check"] = show(con, f"{source} build_check", "select b.block_id, count(o.block_id) as row_count, count(distinct o.measure_key || '|' || o.geography_key || '|' || o.breakdown_json) as series, count(*) filter (where o.geography_key = '_X') as unmatched, count(*) filter (where o.is_total) as total_rows, min(o.date_key), max(o.date_key) from activated as b left join build as o on o.source_system = b.source_system and o.block_id = b.block_id group by all order by 1")
    result["grain_check"] = show(con, f"{source} grain_check", "select block_id, count(*) from (select block_id, measure_key || '|' || geography_key || '|' || breakdown_json as series_key, date_key from build group by all having count(*) > 1) group by all")


def cleanup() -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for bucket_var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    annotate("notice", "scratch removed", removed)


def main() -> int:
    failed = 0
    try:
        for source in sys.argv[1:] or list(TRIALS):
            try:
                trial(source)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                annotate("error", f"{source} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
