"""Dev trial (PL-ENV-014, Task #3300): PIG-PIB CBDG boreholes, source pig_boreholes, Landing → Gold on the final code.

Landing extract of the WFS layer (GEOJSON pages) into zz_dev_pig_boreholes/ twice (the second must write nothing:
every page unchanged), then inspect and verify; Bronze load into the Iceberg namespace zz_dev_pig_boreholes and the lake
prefix zz_dev_pig_boreholes/ twice (the second appends nothing); a profile of the Bronze rows; the Silver model, its
dbt tests and completeness test and the Gold build stage rendered with Jinja from warehouse/ and run in local DuckDB,
each twice, with the grain check by series_key x date_key and spot values for Poland. silver.prg_units (gminas) and
gold.dim_geography are read (read-only) from production. Everything written is removed in `finally`. At most 10
annotations.
"""

from __future__ import annotations

from pathlib import Path
import re
import time
from types import SimpleNamespace

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.pig_boreholes import load as bh_load
from ingestion.to_landing import pig_boreholes as bh_landing
from ingestion.to_landing.patterns import bulk_file

SOURCE = "pig_boreholes"
SCRATCH = "zz_dev_pig_boreholes"
NOTES = {"left": 9}
CAPTURED: list = []
ANNOTATE = report.annotate
MACROS = Path("warehouse/macros/sources/pig_boreholes.sql").read_text()
SILVER = "warehouse/models/silver/pig_boreholes/pig_boreholes_boreholes.sql"


def note(title: str, data, level: str = "notice") -> None:
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        ANNOTATE(level, title, data)
    print(f"[{title}] {data}", flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(MACROS + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def trial(cat) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    bh_landing.LANDING_ROOT = SCRATCH
    p = SimpleNamespace(budget_minutes=150)
    runs = []
    for attempt in (1, 2):
        started = time.monotonic()
        code = bh_landing._run("load", p)
        runs.append([attempt, code, round(time.monotonic() - started), CAPTURED[-1][1] if CAPTURED else None])
    inspect = bh_landing._run("inspect", p)
    inspect_row = CAPTURED[-1][1]
    verify = bh_landing._run("verify", p)
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, "boreholes", SCRATCH))
    note("boreholes landing (scratch)", {
        "extract_two_runs": runs, "inspect_exit": inspect, "inspect": inspect_row, "verify_exit": verify,
        "verify": str(CAPTURED[-1][1])[:300],
        "receipts": len(held), "bytes": sum(r["size_bytes"] for r in held),
        "versions": sorted({r["ingested_at"] for r in held}),
        "parts": [[r["part"]["index"], r["size_bytes"], r["sha256"][:10]] for r in held][:16]})

    timings = []
    for _ in (1, 2):
        started = time.monotonic()
        code = bh_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "pig_boreholes_features"))
        files = table.inspect.files()
        timings.append({"exit": code, "seconds": round(time.monotonic() - started), "files": files.num_rows,
                        "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "pig_boreholes_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("boreholes bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    con.execute("SET memory_limit = '10GB'; INSTALL spatial; LOAD spatial;")
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    texts = [c for c in bh_load.NAMES]
    num = ["glebokosc_m", "rzedna_m_npm", "x_pl1992", "y_pl1992", "rok_wiercenia", "id_cbdg", "point_lon", "point_lat"]
    note("boreholes bronze profile", {
        "rows": one(con, "select count(*) from bronze_b"),
        "distinct": q(con, "select count(distinct objectid), count(distinct id_cbdg), count(distinct raw_sha256), "
                           "count(distinct page), min(objectid::bigint), max(objectid::bigint) from bronze_b"),
        "id_cbdg_repeats": q(con, "select id_cbdg, count(*), list(objectid)[1:3] from bronze_b group by 1 "
                                  "having count(*) > 1 order by 2 desc limit 5"),
        "json_null": {c: n for c, n in zip(texts, con.execute(
            "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in texts) + " from bronze_b").fetchone())
            if n},
        "literal_null_or_empty": {c: n for c, n in zip(texts, con.execute(
            "select " + ", ".join(f"count(*) filter (where trim({c}) in ('null', ''))" for c in texts)
            + " from bronze_b").fetchone()) if n},
        "not_numeric": {c: q(con, f"select {c}, count(*) from bronze_b where trim({c}) not in ('null', '') and "
                                  f"try_cast(trim({c}) as double) is null group by 1 limit 3") for c in num},
        "cel_wiercenia": q(con, "select cel_wiercenia, drilling_purpose, count(*) from bronze_b group by all "
                                "order by 3 desc"),
        "wojewodztwo": q(con, "select wojewodztwo, count(*) from bronze_b group by 1 order by 2 desc"),
        "flags": q(con, "select geofizyka, analizy, count(*) from bronze_b group by all order by 3 desc"),
        "year": q(con, "select min(rok_wiercenia::int), max(rok_wiercenia::int), count(*) filter (where "
                       "rok_wiercenia::int > 2026 or rok_wiercenia::int < 1800) from bronze_b where "
                       "rok_wiercenia is not null"),
        "elevation_low": q(con, "select rzedna_m_npm, count(*) from bronze_b where try_cast(rzedna_m_npm as double) "
                                "< -500 group by 1 order by 2 desc limit 4"),
        "depth": q(con, "select min(glebokosc_m::double), max(glebokosc_m::double), count(*) filter (where "
                        "glebokosc_m::double <= 0) from bronze_b"),
        "lon_lat_range": q(con, "select min(point_lon::double), max(point_lon::double), min(point_lat::double), "
                                "max(point_lat::double) from bronze_b")})

    silver = render(SILVER, {}, {"pig_boreholes_features": "bronze_b"})
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver_t as {silver}")
        rows.append(one(con, "select count(*) from silver_t"))
    spec = yaml.safe_load(Path("warehouse/models/silver/pig_boreholes/_pig_boreholes.yml").read_text())["models"][0]
    nn, av = [], {}
    for c in spec["columns"]:
        for t in c.get("tests", []):
            if t == "not_null":
                nn.append(c["name"])
            elif isinstance(t, dict) and "accepted_values" in t:
                av[c["name"]] = t["accepted_values"]["arguments"]["values"]
    tests = {
        "dup_id_cbdg": one(con, "select count(*) from (select id_cbdg from silver_t group by 1 having count(*) > 1)"),
        "dup_objectid": one(con, "select count(*) from (select objectid from silver_t group by 1 having count(*) > 1)"),
        "not_null_fails": {c: n for c, n in zip(nn, con.execute(
            "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in nn) + " from silver_t").fetchone())},
        "accepted_values_fails": {c: q(con, f"select {c}, count(*) from silver_t where {c} is not null and {c} not in "
                                            f"({', '.join(repr(v) for v in vals)}) group by 1") for c, vals in av.items()},
        "complete_fails": q(con, Environment().from_string(
            Path("warehouse/tests/pig_boreholes_boreholes_complete.sql").read_text()).render(ref=lambda n: "silver_t")),
    }
    note("boreholes silver", {
        "rows_two_builds": rows, "tests": tests,
        "profile": q(con, "select min(snapshot_date), count(distinct source_version), count(distinct raw_sha256), "
                          "min(drilling_year), max(drilling_year), count(drilling_year), count(depth_m), "
                          "count(elevation_m_asl), count(drilling_purpose), count(voivodeship_name) from silver_t"),
        "spot_objectid_1": q(con, "select id_cbdg, objectid, depth_m, drilling_purpose, drilling_year, point_lon, "
                                  "point_lat, voivodeship_name from silver_t where objectid = 1")})

    geo_cols = ("geography_key", "level_name", "name_pl", "terc_code")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    from pyiceberg.expressions import EqualTo

    con.register("prg_a", cat.load_table(("silver", "prg_units")).scan(
        row_filter=EqualTo("unit_level", "gmina"),
        selected_fields=("unit_level", "teryt_code", "name_pl", "geometry_wkb")).to_arrow())
    con.execute("create table prg_t as select * from prg_a")
    cte = Environment().from_string(MACROS + "{{ poc_geography__pig_boreholes() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from pig_boreholes")
    gold_sql = render("warehouse/models/gold/poc_build_pig_boreholes.sql", {
        "poc_map_geography": "map_t", "prg_units": "prg_t", "pig_boreholes_boreholes": "silver_t"}, {})
    plan = " ".join(r[1] for r in con.execute("explain " + gold_sql).fetchall())
    ops = sorted(set(re.findall(r"(SPATIAL_JOIN|NESTED_LOOP_JOIN|BLOCKWISE_NL_JOIN|PIECEWISE_MERGE_JOIN|IE_JOIN|"
                                r"HASH_JOIN|CROSS_PRODUCT)", plan)))
    gold_rows = []
    for _ in (1, 2):
        started = time.monotonic()
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append([one(con, "select count(*) from gold_t"), round(time.monotonic() - started)])
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    note("boreholes gold build_pig_boreholes", {
        "rows_two_builds_seconds": gold_rows, "join_ops": ops, "prg_gminas": one(con, "select count(*) from prg_t"),
        "map_rows": one(con, "select count(*) from map_t"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_level": q(con, "select case when geography_key = 'PL' then 'PL' when length(geography_source_code) = 2 "
                           "then 'woj' else 'powiat' end, count(*), count(distinct geography_key), sum(is_total::int) "
                           "from gold_t group by 1 order by 1"),
        "measure_date": q(con, "select measure_key, date_key, frequency, count(*) from gold_t group by all"),
        "unmatched": q(con, "select geography_source_code, count(*) from gold_t where geography_key = '_X' group by 1"),
        "null_or_negative": one(con, "select count(*) from gold_t where value is null or value <= 0"),
        "breakdown_sum_vs_total": one(con, "select count(*) from (select geography_key, sum(value) filter (where not "
                                           "is_total) s, max(value) filter (where is_total) t from gold_t group by 1) "
                                           "where s <> t"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    con.execute("create table placed_t as select b.id_cbdg, b.voivodeship_name, b.powiat_name, "
                "list(distinct substr(g.teryt_code, 1, 2)) w, list(distinct substr(g.teryt_code, 1, 4)) p "
                "from silver_t b join (select teryt_code, st_geomfromwkb(geometry_wkb) geom from prg_t) g "
                "on st_intersects(g.geom, st_point(b.point_lon, b.point_lat)) group by all")
    note("boreholes gold spot values", {
        "poland": q(con, "select breakdown_json, value from gold_t where geography_key = 'PL' order by value desc"),
        "poland_vs_silver": one(con, "select count(*) from silver_t"),
        "placement": q(con, "select (select count(*) from silver_t) - count(*) as outside_every_gmina, "
                            "count(*) filter (where len(p) > 1) two_powiats, count(*) filter (where len(w) > 1) "
                            "two_voivodeships from placed_t"),
        "outside_sample": q(con, "select id_cbdg, point_lon, point_lat, voivodeship_name from silver_t s where not "
                                 "exists (select 1 from placed_t p where p.id_cbdg = s.id_cbdg) limit 4"),
        "name_crosscheck_woj": q(con, "select count(*) filter (where lower(g.name_pl) = lower(p.voivodeship_name)), "
                                      "count(*) filter (where p.voivodeship_name is not null and lower(g.name_pl) <> "
                                      "lower(p.voivodeship_name)), count(*) filter (where p.voivodeship_name is null) from "
                                      "placed_t p join geo g on g.level_name = 'wojewodztwo' and g.terc_code = p.w[1] "
                                      "where len(p.w) = 1"),
        "woj_sum_vs_poland": q(con, "select sum(value) filter (where length(geography_source_code) = 2), sum(value) "
                                    "filter (where length(geography_source_code) = 4), max(value) filter (where "
                                    "geography_key = 'PL') from gold_t where is_total"),
        "voivodeships_top": q(con, "select g.name_pl, b.value from gold_t as b join geo as g using (geography_key) "
                                   "where b.is_total and length(b.geography_source_code) = 2 order by 2 desc limit 5"),
        "powiats_top": q(con, "select g.name_pl, b.value from gold_t as b join geo as g using (geography_key) "
                              "where b.is_total and length(b.geography_source_code) = 4 order by 2 desc limit 3")})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("boreholes licence (catalogue rows)", {
        "source": source_row, "licence": [r for r in seed_rows.rows("licence") if r["licence_key"] in lic],
        "unit_map": [r for r in seed_rows.rows("unit_map") if r["source_system"] == SOURCE]})


def cleanup(cat) -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
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
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
                       ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
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
    NOTES["left"] += 1
    note("scratch removed", removed)


def main() -> int:
    capture = lambda _l, title, data: CAPTURED.append([title, data])  # noqa: E731
    report.annotate = capture  # landing _run imports it at call time
    bulk_file.annotate = capture
    bh_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("boreholes trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
