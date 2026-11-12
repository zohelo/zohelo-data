"""Dev trial (CD-026 Wikidata, #2843): Landing → Bronze → Silver → Gold into scratch.

Landing load of the six SPARQL selections into the Landing prefix zz_dev_wikidata/ (twice: the second must write
nothing unless Wikidata was edited in between), Bronze load into Iceberg namespace zz_dev_wikidata and lake prefix
zz_dev_wikidata/ (twice: the second appends nothing), then the Silver models and the Gold entity table rendered with
Jinja from warehouse/models in local DuckDB, each built twice, with the dbt tests' checks. TERYT and dim_geography
are read (read-only) from production silver.teryt_terc and gold.dim_geography. Everything written is removed in
`finally`. At most 10 compact annotations.
"""

from __future__ import annotations

import json
from pathlib import Path
import re

import duckdb
from jinja2 import Environment
import requests

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.wikidata import load as bronze_load
from ingestion.to_landing import wikidata as landing
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_wikidata"


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def licence_page() -> None:
    url = "https://www.wikidata.org/wiki/Wikidata:Copyright"
    r = requests.get(url, headers={"User-Agent": landing.USER_AGENT}, timeout=60)
    text = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", r.text))
    hits = [text[max(0, m.start() - 200): m.start() + 300] for m in re.finditer(r"CC0|Creative Commons", text)][:3]
    annotate("notice", "wikidata licence page", {"status": r.status_code, "hits": hits})


def trial() -> None:
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    runs = []
    for _ in (1, 2):
        runs.append(bulk_file.main(landing.SOURCE, "load", landing_root=SCRATCH, specs=landing.specs(),
                                   pause_seconds=landing.PAUSE_SECONDS))
    held = {}
    for spec in landing.specs():
        rec = bulk_file.landed(s3(), bucket, bulk_file.prefix(landing.SOURCE, spec["entity"], SCRATCH))
        held[spec["entity"]] = [len(rec), rec[-1]["size_bytes"] if rec else 0]
    verify = bulk_file.main(landing.SOURCE, "verify", landing_root=SCRATCH, specs=landing.specs())
    bronze_exit = [bronze_load.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    table = cat.load_table((SCRATCH, "wikidata_bindings"))
    files = len(list(table.scan().plan_files()))
    arrow = table.scan().to_arrow()
    try:
        rejected = cat.load_table((SCRATCH, "wikidata_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    con = duckdb.connect()
    con.register("bronze_b", arrow)
    con.register("teryt_a", cat.load_table(("silver", "teryt_terc")).scan().to_arrow())
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
    con.execute("create table teryt as select * from teryt_a")
    con.execute("create table geo as select geography_key from geo_a")
    annotate("notice", "wikidata landing+bronze (scratch)", {
        "landing_exit_two_runs": runs, "verify_exit": verify, "copies_and_bytes": held, "bronze_exit": bronze_exit,
        "bronze_files": files, "bronze_rows": arrow.num_rows,
        "rows_per_entity": q(con, "select entity, count(*) from bronze_b group by 1 order by 1"),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    stm = render("warehouse/models/silver/wikidata/wikidata_statements.sql", {}, {"wikidata_bindings": "bronze_b"})
    att = render("warehouse/models/silver/wikidata/wikidata_terc_attributes.sql",
                 {"teryt_terc": "teryt", "wikidata_statements": "wikidata_statements"}, {})
    gold = render("warehouse/models/gold/geo_unit_wikidata.sql", {"wikidata_terc_attributes": "att"}, {})
    counts = {"statements": [], "att": [], "gold": []}
    for _ in (1, 2):
        con.execute(f"create or replace table wikidata_statements as {stm}")
        con.execute(f"create or replace table att as {att}")
        con.execute(f"create or replace table gold as {gold}")
        for t in counts:
            tbl = {"statements": "wikidata_statements", "att": "att", "gold": "gold"}[t]
            counts[t].append(con.execute(f"select count(*) from {tbl}").fetchone()[0])
    annotate("notice", "wikidata silver statements", {
        "rows_two_builds": counts["statements"],
        "dup_statement_id": q(con, "select count(*) from (select statement_id from wikidata_statements group by 1 having count(*) > 1)"),
        "null_keys": q(con, "select count(*) filter (where statement_id is null), count(*) filter (where qid is null), count(*) filter (where raw_sha256 is null), count(*) filter (where rank is null) from wikidata_statements"),
        "per_property_rank": q(con, "select property, rank, count(*), count(distinct qid), max(solutions) from wikidata_statements group by all order by all"),
        "population_time": q(con, "select min(point_in_time), max(point_in_time), count(*) filter (where point_in_time is null) from wikidata_statements where property = 'P1082'"),
        "area_units": q(con, "select unit_qid, count(*) from wikidata_statements where property = 'P2046' group by 1"),
        "coords_unparsed": q(con, "select count(*) from wikidata_statements where property = 'P625' and (lon is null or lat is null)")})
    shapes = """select case when regexp_matches(value_text, '^[0-9]{2}$') then 'woj' when regexp_matches(value_text, '^[0-9]{4}$') then 'pow'
        when regexp_matches(value_text, '^[0-9]{7}$') then 'gmi' else 'other' end as shape,
        count(*) as statements, count(*) filter (where rank = 'deprecated' or end_time is not null or dissolved is not null) as not_current,
        count(distinct value_text) filter (where value_text in (select terc_code from teryt)) as codes_in_teryt,
        count(distinct value_text) filter (where value_text not in (select terc_code from teryt) and rank <> 'deprecated' and end_time is null and dissolved is null) as current_codes_not_in_teryt
        from wikidata_statements where property = 'P1653' group by 1 order by 1"""
    annotate("notice", "wikidata TERC coverage", {
        "shapes": q(con, shapes),
        "teryt_levels_vs_wikidata": q(con, "select t.unit_level, count(*), count(a.terc_code) from teryt as t left join (select distinct terc_code from att) as a using (terc_code) group by 1 order by 1"),
        "teryt_gmina_types_missing": q(con, "select t.gmina_type_code, count(*) from teryt as t where t.terc_code not in (select terc_code from att) group by 1 order by 1"),
        "not_in_teryt_sample": q(con, "select value_text, qid from wikidata_statements where property = 'P1653' and regexp_matches(value_text, '^([0-9]{2}|[0-9]{4}|[0-9]{7})$') and value_text not in (select terc_code from teryt) and rank <> 'deprecated' and dissolved is null limit 8"),
        "codes_with_several_items": q(con, "select terc_code, value_text, candidates from att where property = 'qid' and candidates > 1"),
        "items_with_several_codes": q(con, "select count(*) from (select qid from att where property = 'qid' group by 1 having count(*) > 1)")})
    annotate("notice", "wikidata silver attributes", {
        "rows_two_builds": counts["att"],
        "dup_terc_property": q(con, "select count(*) from (select terc_code, property from att group by all having count(*) > 1)"),
        "null_keys": q(con, "select count(*) filter (where terc_code is null), count(*) filter (where qid is null), count(*) filter (where value_text is null), count(*) filter (where raw_sha256 is null) from att"),
        "per_property": q(con, "select property, count(*), count(*) filter (where candidates > 1), sum(candidates - 1), count(*) filter (where tied > 1), count(*) filter (where rank = 'preferred') from att group by 1 order by 1"),
        "population_dates": q(con, "select year(point_in_time), count(*) from att where property = 'population' group by 1 order by 2 desc limit 6"),
        "bad_property": q(con, "select count(*) from att where property not in ('qid','label_pl','label_en','wikipedia_pl','wikipedia_en','population','area','coordinates','website')")})
    annotate("notice", "wikidata gold geo_unit_wikidata", {
        "rows_two_builds": counts["gold"],
        "dup_geography_key": q(con, "select count(*) from (select geography_key from gold group by 1 having count(*) > 1)"),
        "not_in_dim_geography": q(con, "select count(*), list(geography_key)[1:5] from gold where geography_key not in (select geography_key from geo)"),
        "per_level": q(con, "select level_name, count(*), count(population), count(area), count(lat), count(website), count(wikipedia_pl), count(wikipedia_en), count(label_en) from gold group by 1 order by 1"),
        "spot": q(con, "select geography_key, wikidata_qid, label_pl, population, population_date, area, area_unit, lat, lon, website from gold where terc_code in ('14', '1465011', '1261011', '3262011') order by 1"),
        "ranges": q(con, "select level_name, min(population), max(population), min(area), max(area), min(lat), max(lat), min(lon), max(lon) from gold group by 1 order by 1")})


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
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
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
    from ingestion.common import bulk_bronze

    bulk_file.annotate = bulk_bronze.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    failed = 0
    try:
        try:
            licence_page()
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"licence_page_error": str(exc)[:300]}))
        try:
            trial()
        except Exception as exc:  # noqa: BLE001
            failed = 1
            annotate("error", "wikidata trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
