"""Dev trial (GL-022 FAOSTAT, GL-025 WHO data.who.int, GL-026 UNESCO UIS): Landing → Bronze → Silver → Gold into scratch.

Per source: the bulk_file Landing load into the Landing prefix zz_dev_w2fao/ (twice: the second must write
nothing), the source's Bronze load into Iceberg namespace zz_dev_w2fao and lake prefix zz_dev_w2fao/ (twice: the
second appends nothing), then the Silver models and the Gold build stage (rendered with Jinja from
warehouse/models and warehouse/macros) in local DuckDB over that Bronze table, each built twice (same counts),
with checks printed to the log and one compact annotation per source. Everything written is removed in `finally`.
Never writes production namespaces or prefixes.
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

SCRATCH = "zz_dev_w2fao"
EU = {
    "AT": "AUT", "BE": "BEL", "BG": "BGR", "CY": "CYP", "CZ": "CZE", "DE": "DEU", "DK": "DNK", "EE": "EST",
    "ES": "ESP", "FI": "FIN", "FR": "FRA", "GR": "GRC", "HR": "HRV", "HU": "HUN", "IE": "IRL", "IT": "ITA",
    "LT": "LTU", "LU": "LUX", "LV": "LVA", "MT": "MLT", "NL": "NLD", "PL": "POL", "PT": "PRT", "RO": "ROU",
    "SE": "SWE", "SI": "SVN", "SK": "SVK", "GB": "GBR", "US": "USA", "NO": "NOR", "CH": "CHE",
}
MACROS = "".join(Path(p).read_text() for p in ("warehouse/macros/poc_blocks.sql", "warehouse/macros/m49.sql"))

TRIALS = {
    "faostat": {
        "silver": "faostat_observations",
        "grain": "entity, area_code, item_code, element_code, year, source_code",
        "checks": {
            "per entity": "select entity, count(*), count(distinct area_code), min(year), max(year), count(*) filter (where value is null), count(*) filter (where year is null or area_code is null) from faostat_observations group by 1 order by 1",
            "units": "select entity, unit, count(*) from faostat_observations group by all order by 1, 3 desc limit 12",
            "aggregates": "select distinct area_code, area_code_m49, area from faostat_observations where area_code in (5000, 5707) or area ilike 'European Union%' order by 1",
            "ghg sources": "select source_code, source, count(*) from faostat_observations where item_code = '6829' and element_code = '723113' group by all",
            "PL ghg land": "select entity, item_code, element_code, source_code, year, value, unit, flag from faostat_observations where area_code = 173 and ((item_code = '6829' and element_code = '723113') or (item_code = '6601' and element_code = '5110')) qualify row_number() over (partition by entity, source_code order by year desc) <= 2",
            "projections": "select entity, item_code, source_code, min(year), max(year), count(*) from faostat_observations where is_projection group by all order by 1, 2 limit 30",
            "flags in gold items": "select flag, count(*) from faostat_observations where (item_code = '6829' and element_code = '723113') or (item_code = '6601' and element_code = '5110') group by 1 order by 2 desc",
            "max ghg land": "select item_code, max(value) from faostat_observations where (item_code = '6829' and element_code = '723113') or (item_code = '6601' and element_code = '5110') group by 1",
        },
    },
    "who": {
        "silver": "who_observations",
        "grain": "entity, geo_type, geo_code_m49, year, sex",
        "checks": {
            "per entity": "select entity, count(*), count(distinct geo_code_m49), min(year), max(year), count(*) filter (where value is null) from who_observations group by 1",
            "sex, types": "select sex, geo_type, time_type, publish_state, count(*) from who_observations group by all order by all",
            "PL": "select entity, sex, year, value from who_observations where geo_type = 'COUNTRY' and geo_code_m49 = '616' qualify row_number() over (partition by entity, sex order by year desc) <= 1",
            "World": "select entity, sex, year, value from who_observations where geo_type = 'GLOBAL' qualify row_number() over (partition by entity, sex order by year desc) <= 1",
            "range": "select entity, min(value), max(value) from who_observations group by 1",
            "sex and state": "select sex, publish_state, count(*) from who_observations group by all order by all",
        },
    },
    "unesco_uis": {
        "silver": "unesco_uis_observations",
        "grain": "entity, indicator_id, geo_unit, year",
        "checks": {
            "per indicator": "select entity, indicator_id, count(*), count(distinct geo_unit), min(year), max(year), count(*) filter (where value is null) from unesco_uis_observations group by all order by 1, 2",
            "regions": "select geo_unit, count(*) from unesco_uis_observations where not is_country_code group by 1 order by 1 limit 60",
            "qualifiers": "select magnitude, qualifier, count(*) from unesco_uis_observations group by all",
            "PL": "select indicator_id, year, value, qualifier from unesco_uis_observations where geo_unit = 'POL' qualify row_number() over (partition by indicator_id order by year desc) <= 2 order by 1, 2",
            "range": "select indicator_id, min(value), max(value) from unesco_uis_observations group by 1",
        },
    },
}


def show(con, title: str, query: str, limit: int = 80) -> list:
    rows = [[str(v) for v in r] for r in con.execute(query).fetchall()][:limit]
    print(f"### {title}: {json.dumps(rows)}", flush=True)
    return rows


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    env = Environment()
    text = MACROS + Path(path).read_text()
    return env.from_string(text).render(
        config=lambda **_k: "",
        ref=lambda name: refs.get(name, name),
        source=lambda _s, table: sources[table],
    )


def render_activated() -> str:
    return Environment().from_string(MACROS + "{{ poc_activated_blocks() }}").render(ref=lambda n: n, config=lambda **_k: "")


def build_geo(con) -> None:
    rows = [(iso2, iso3, "country") for iso2, iso3 in EU.items()]
    rows += [("EUROSTAT:EU27_2020", None, "aggregate"), ("WB:WLD", None, "aggregate"), ("_X", None, "special")]
    con.execute("create or replace table geo (geography_key varchar, iso3 varchar, level_name varchar)")
    con.executemany("insert into geo values (?, ?, ?)", [(k, i3, lv) for k, i3, lv in rows])
    con.execute("create or replace view geo_v as select geography_key, geography_key as iso2, iso3, level_name from geo")


def trial(source: str) -> dict:
    spec = TRIALS[source]
    result: dict = {}
    result["landing"] = [bulk_file.main(source, "load", landing_root=SCRATCH) for _ in (1, 2)]
    held = {
        f["entity"]: len(bulk_file.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(source, f["entity"], SCRATCH)))
        for f in bulk_file.declaration(source)
    }
    result["landed_copies_per_entity"] = held  # 1 each: the second load wrote nothing
    loader = importlib.import_module(f"ingestion.to_bronze.{source}.load")
    result["bronze_exit"] = [loader.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    bronze = cat.load_table((SCRATCH, f"{source}_observations"))
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
    annotate("notice", f"{source} trial (scratch {SCRATCH})", {k: result[k] for k in ("landing", "landed_copies_per_entity", "bronze_exit", "bronze", "rejected", "silver_rows_two_builds", "silver_duplicate_keys", "silver_null_sha", "gold_rows_two_builds", "gold_dup_series_date")})
    annotate("notice", f"{source} gold (scratch)", {"measures": result["gold measures"], "PL": result["gold PL latest"], "build_check": result["build_check"], "grain_check": result["grain_check"]})
    return result


def model(con, source: str, result: dict) -> None:
    spec = TRIALS[source]
    silver = spec["silver"]
    sql = render(f"warehouse/models/silver/{source}/{silver}.sql", {}, {f"{source}_observations": "bronze_obs"})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table {silver} as {sql}")
        counts.append(con.execute(f"select count(*) from {silver}").fetchone()[0])
    result["silver_rows_two_builds"] = counts
    result["silver_duplicate_keys"] = con.execute(
        f"select count(*) from (select {spec['grain']} from {silver} group by all having count(*) > 1)"
    ).fetchone()[0]
    result["silver_null_sha"] = con.execute(f"select count(*) from {silver} where raw_sha256 is null").fetchone()[0]
    for title, query in spec["checks"].items():
        result[title] = show(con, f"{source} {title}", query)
    build_geo(con)
    gmap_sql = render(
        "warehouse/models/gold/poc_map_geography.sql",
        {"poc_dim_geography": "geo_v", "eurostat_codes": "empty_codes", "teryt_terc": "x", "bdl_units": "x",
         "wdi_countries": "x", "faostat_observations": "faostat_observations", "who_observations": "who_observations",
         "unesco_uis_observations": "unesco_uis_observations"},
        {},
    )
    cte = re.search(rf"\n{source} as \((.*?)\n\),?\n(?:\n|select)", gmap_sql, re.S).group(1)
    con.execute(f"create or replace table gmap as {cte}")
    result["geo map"] = show(con, f"{source} geo map", "select geography_key = '_X', count(*) from gmap group by 1")
    build_sql = render(f"warehouse/models/gold/poc_build_{source}.sql", {silver: silver, "poc_map_geography": "gmap_all"}, {})
    con.execute(f"create or replace view gmap_all as select '{source}' as source_system, source_code, geography_key from gmap")
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
    result["gold measures"] = show(con, f"{source} gold measures", "select measure_key, count(distinct geography_key), count(*), min(date_key), max(date_key), min(value), max(value) from build group by 1 order by 1")
    result["gold PL latest"] = show(con, f"{source} gold PL", "select measure_key, breakdown_json, date_key, value, observation_status_key from build where geography_key = 'PL' qualify row_number() over (partition by measure_key, breakdown_json order by date_key desc) = 1 order by 1, 2")
    result["build_check"] = show(con, f"{source} build_check", "select b.source_system, b.block_id, count(o.block_id) as row_count, count(distinct o.measure_key || '|' || o.geography_key || '|' || o.breakdown_json) as series, count(*) filter (where o.geography_key = '_X') as rows_geography_unmatched, count(*) filter (where o.is_total) as rows_total_series, count(*) filter (where o.frequency = 'X') as rows_period_unplaced, min(o.date_key), max(o.date_key), list(distinct o.observation_status_key) from activated as b left join build as o on o.source_system = b.source_system and o.block_id = b.block_id group by all order by 2")
    result["grain_check"] = show(con, f"{source} grain_check", "select source_system, block_id, count(*) as duplicate_keys from (select source_system, block_id, measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key from build group by all having count(*) > 1) group by all")
    result["gold areas"] = show(con, f"{source} gold areas", "select string_agg(distinct geography_key, ',' order by geography_key) from build")


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


def who_pages() -> None:
    import html as h
    import urllib.request

    out = {}
    for uid, url in (("C64284D", "https://data.who.int/indicators/i/48D9B0C/C64284D"), ("90E2E48", "https://data.who.int/indicators/i/A21CFC2/90E2E48")):
        req = urllib.request.Request(url, headers={"User-Agent": "zohelo-data/1.0 (+https://zohelo.com; terms check)"})
        with urllib.request.urlopen(req, timeout=60) as r:
            text = h.unescape(re.sub(r"(?s)<[^>]+>", " ", r.read().decode("utf-8", "replace")))
        text = re.sub(r"\s+", " ", text)
        hits = [text[max(0, m.start() - 150): m.start() + 450] for m in re.finditer(r"licen|Creative Commons|terms and conditions|Terms of use|Citation|copyright|third.party", text, re.I)]
        print(f"### who page {uid} {url}: {json.dumps(hits)}", flush=True)
        out[uid] = {"status": r.status, "licence_mentions": [x[:400] for x in hits if re.search(r"licen|Creative|copyright|third", x, re.I)][:4], "citation": [x[:400] for x in hits if "Citation" in x][:1]}
    annotate("notice", "who indicator pages (terms check)", out)


def main() -> int:
    sources = sys.argv[1:] or list(TRIALS)
    failed = 0
    try:
        try:
            who_pages()
        except Exception as exc:  # noqa: BLE001
            annotate("error", "who pages failed", {"error": f"{type(exc).__name__}: {str(exc)[:500]}"})
        for source in sources:
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
