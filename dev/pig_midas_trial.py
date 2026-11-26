"""Dev trial (PL-ENV-015, part of #2870): PIG-PIB MIDAS, source pig_midas, Landing → Gold on the final code.

Landing extract of the six declared WFS layers into zz_dev_pig_midas/ twice (the second must write nothing: same
SHA-256), 10 s between requests, inspect and verify, and resultType=hits per layer to compare with the features landed;
Bronze load into the Iceberg namespace zz_dev_pig_midas and the lake prefix zz_dev_pig_midas/ twice (the second appends
nothing); a profile of the Bronze rows; the three Silver models, their completeness tests and the Gold build stage
rendered with Jinja from warehouse/ and run in local DuckDB, each twice, with the checks of their dbt tests, the grain
check by series_key x date_key and spot values for Poland. silver.prg_units (voivodeships) and gold.dim_geography are
read (read-only) from production. Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

from pathlib import Path
import re
import time
import urllib.parse

import duckdb
from jinja2 import Environment
import requests

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.pig_midas import load as midas_load
from ingestion.to_landing.patterns import bulk_file

SOURCE = "pig_midas"
SCRATCH = "zz_dev_pig_midas"
NOTES = {"left": 9}
CAPTURED: list = []
MACROS = Path("warehouse/macros/sources/pig_midas.sql").read_text()


def note(title: str, data, level: str = "notice") -> None:
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        annotate(level, title, data)
    print(f"[{title}] {data}", flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(MACROS + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def hits(spec: dict) -> int | None:
    url = spec["url"].replace("&outputFormat=GEOJSON", "&resultType=hits")
    parts = urllib.parse.urlsplit(url)
    url = urllib.parse.urlunsplit(parts._replace(query=urllib.parse.quote(parts.query, safe="=&:")))
    time.sleep(10)
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
    m = re.search(r'numberMatched="(\d+)"', r.text)
    return int(m.group(1)) if m else None


def trial(cat) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    specs = bulk_file.declaration(SOURCE)
    runs, matched = [], {}
    for attempt in (1, 2):
        for spec in specs:
            if attempt == 1:
                matched[spec["entity"]] = hits(spec)
            time.sleep(10)
            started = time.monotonic()
            result = bulk_file.load_file(client, bucket, SOURCE, spec, SCRATCH)
            runs.append([attempt, spec["entity"], result.get("outcome"), result.get("size_mb"),
                         round(time.monotonic() - started)])
    held = {s["entity"]: bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, s["entity"], SCRATCH)) for s in specs}
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH, specs=specs)
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=specs)
    note("midas landing (scratch)", {
        "extract_two_runs": runs, "inspect_exit": inspect, "verify_exit": verify, "hits": matched,
        "inspect_rows": [[r.get(k) for k in ("entity", "state", "http", "server_version")] for t, d in CAPTURED
                         for r in (d.get("files") or d.get("results") or d.get("rows") or [] if isinstance(d, dict)
                                   else d if isinstance(d, list) else []) if isinstance(r, dict)][:12],
        "captured_titles": [[t, str(d)[:300]] for t, d in CAPTURED][:4],
        "receipts": {e: [[r["size_bytes"], r["version_token"], r["sha256"][:12]] for r in h] for e, h in held.items()}})

    timings = []
    for _ in (1, 2):
        started = time.monotonic()
        code = midas_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "pig_midas_features"))
        files = table.inspect.files()
        timings.append({"exit": code, "seconds": round(time.monotonic() - started), "files": files.num_rows,
                        "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "pig_midas_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("midas bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; INSTALL spatial; LOAD spatial;")
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    by_entity = q(con, "select entity, count(*), count(distinct objectid), count(distinct id_konturu), "
                       "count(distinct numer_zloza), min(source_version), count(distinct raw_sha256) from bronze_b "
                       "group by 1 order by 1")
    texts = [c for c in midas_load.NAMES if c != "objectid"]
    note("midas bronze profile", {
        "rows_by_entity": by_entity,
        "features_vs_hits": {r[0]: [int(r[1]), matched.get(r[0])] for r in by_entity},
        "null_geometry": one(con, "select count(*) from bronze_b where geometry_geojson is null or "
                                  "geometry_geojson = 'null'"),
        "literal_null_text": {c: n for c, n in zip(texts, con.execute(
            "select " + ", ".join(f"count(*) filter (where trim({c}) = 'null')" for c in texts)
            + " from bronze_b").fetchone()) if n},
        "date_shapes": q(con, "select regexp_replace(coalesce(data_ustanowienia, '~'), '[0-9]', '9', 'g'), "
                              "regexp_replace(coalesce(data_waznosci, '~'), '[0-9]', '9', 'g'), "
                              "regexp_replace(coalesce(data_zniesienia, '~'), '[0-9]', '9', 'g'), count(*) "
                              "from bronze_b where id_konturu is not null group by all order by 4 desc limit 6"),
        "number_shapes": q(con, "select entity, regexp_replace(numer_zloza, '[0-9]+', '9', 'g'), count(*) from bronze_b "
                                "group by all order by 1, 3 desc"),
        "status": q(con, "select entity, status, count(*) from bronze_b group by all order by 1"),
        "kod_vs_kopalina": one(con, "select count(*) from (select kod from bronze_b where kod is not null group by 1 "
                                    "having count(distinct kopalina) > 1)"),
        "kod_two_names_in_deposits": q(con, "select kod, list(distinct kopalina) from bronze_b where entity = 'deposits' "
                                            "group by 1 having count(distinct kopalina) > 1 limit 5")})

    models = {m: render(f"warehouse/models/silver/pig_midas/{m}.sql", {}, {"pig_midas_features": "bronze_b"})
              for m in ("pig_midas_deposits", "pig_midas_mining_areas", "pig_midas_mining_terrains")}
    rows = []
    for _ in (1, 2):
        for m, sql in models.items():
            con.execute(f"create or replace table {m} as {sql}")
        rows.append([one(con, f"select count(*) from {m}") for m in models])
    keys = {"pig_midas_deposits": "objectid", "pig_midas_mining_areas": "contour_id",
            "pig_midas_mining_terrains": "contour_id"}
    tests = {}
    for m, key in keys.items():
        nn = ["layer", key, "point_lon", "point_lat", "snapshot_date", "raw_sha256"] + (
            ["deposit_number", "mineral_code", "mineral"] if m == "pig_midas_deposits" else ["status", "register_number"])
        tests[m] = {
            "dup_grain": one(con, f"select count(*) from (select layer, {key} from {m} group by all having count(*) > 1)"),
            "nulls": {c: n for c, n in zip(nn, con.execute(
                "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in nn) + f" from {m}").fetchone())
                if n},
            "complete_fails": q(con, Environment().from_string(Path(f"warehouse/tests/{m}_complete.sql").read_text())
                                .render(ref=lambda n: n)),
        }
        if m != "pig_midas_deposits":
            tests[m]["status_by_layer"] = q(con, f"select layer, status, count(*) from {m} group by all order by 1")
    note("midas silver", {
        "rows_two_builds": rows, "tests": tests,
        "deposits": q(con, "select layer, count(*), count(distinct deposit_number), count(distinct mineral_code), "
                           "min(snapshot_date), round(min(point_lon), 2), round(max(point_lon), 2), "
                           "round(min(point_lat), 2), round(max(point_lat), 2) from pig_midas_deposits group by 1"),
        "areas": q(con, "select layer, count(*), count(legal_area_m2), round(sum(legal_area_m2) / 1e6), "
                        "min(established_on), max(established_on), max(valid_until), max(abolished_on) "
                        "from pig_midas_mining_areas group by 1"),
        "terrains": q(con, "select layer, count(*), count(legal_area_m2), min(established_on), max(abolished_on) "
                           "from pig_midas_mining_terrains group by 1"),
        "top_minerals": q(con, "select mineral_code, mineral, count(distinct deposit_number) from pig_midas_deposits "
                               "where layer = 'deposits' group by all order by 3 desc limit 8")})

    geo_cols = ("geography_key", "level_name", "name_pl", "terc_code")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    from pyiceberg.expressions import EqualTo

    con.register("prg_a", cat.load_table(("silver", "prg_units")).scan(
        row_filter=EqualTo("unit_level", "gmina"),
        selected_fields=("unit_level", "teryt_code", "geometry_wkb")).to_arrow())
    con.execute("create table prg_t as select * from prg_a")
    con.execute("create table prg_g as select teryt_code, g as geom, st_xmin(g) x0, st_xmax(g) x1, st_ymin(g) y0, "
                "st_ymax(g) y1 from (select teryt_code, st_geomfromwkb(geometry_wkb) g from prg_t)")
    cte = Environment().from_string(MACROS + "{{ poc_geography__pig_midas() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from pig_midas")
    gold_sql = render("warehouse/models/gold/poc_build_pig_midas.sql", {
        "poc_map_geography": "map_t", "prg_units": "prg_t", "pig_midas_deposits": "pig_midas_deposits",
        "pig_midas_mining_areas": "pig_midas_mining_areas"}, {})
    plan = " ".join(r[1] for r in con.execute("explain " + gold_sql).fetchall())
    ops = sorted(set(re.findall(r"(SPATIAL_JOIN|NESTED_LOOP_JOIN|BLOCKWISE_NL_JOIN|PIECEWISE_MERGE_JOIN|IE_JOIN|"
                                r"HASH_JOIN|CROSS_PRODUCT)", plan)))
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    note("midas gold build_pig_midas", {
        "rows_two_builds": gold_rows, "join_ops": ops, "prg_gminas": one(con, "select count(*) from prg_t"),
        "map_rows": one(con, "select count(*) from map_t"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_measure": q(con, "select measure_key, date_key, frequency, count(*), sum(is_total::int), "
                             "count(distinct geography_key) from gold_t group by all order by 1"),
        "unmatched": q(con, "select geography_source_code, count(*) from gold_t where geography_key = '_X' group by 1"),
        "null_or_negative": one(con, "select count(*) from gold_t where value is null or value < 0"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    note("midas gold spot values", {
        "poland": q(con, "select measure_key, value from gold_t where geography_key = 'PL' and is_total order by 1"),
        "poland_vs_silver": q(con, "select (select count(distinct deposit_number) from pig_midas_deposits where layer = "
                                   "'deposits'), (select count(*) from pig_midas_mining_areas where layer = "
                                   "'mining_areas'), (select round(sum(legal_area_m2)) from pig_midas_mining_areas "
                                   "where layer = 'mining_areas')"),
        "voivodeship_sum_vs_poland": q(con, "select measure_key, sum(value) filter (where geography_key <> 'PL'), "
                                            "max(value) filter (where geography_key = 'PL') from gold_t where is_total "
                                            "group by 1 order by 1"),
        "voivodeships_deposits": q(con, "select g.name_pl, b.value from gold_t as b join geo as g using "
                                        "(geography_key) where b.measure_key = 'pig_midas:deposits:deposits:documented' "
                                        "and b.is_total order by 2 desc limit 5"),
        "poland_minerals": q(con, "select breakdown_label_json, value from gold_t where geography_key = 'PL' and "
                                  "not is_total order by value desc limit 6"),
        "deposit_contours_outside_voivodeships": q(con, "select count(*), round(min(point_lat), 2), "
                                                        "round(max(point_lat), 2) from pig_midas_deposits d where "
                                                        "layer = 'deposits' and not exists (select 1 from prg_g v where "
                                                        "d.point_lon between v.x0 and v.x1 and d.point_lat between "
                                                        "v.y0 and v.y1 and st_contains(v.geom, "
                                                        "st_point(d.point_lon, d.point_lat)))"),
        "spot_deposit_324": q(con, "select layer, deposit_number, deposit_name, mineral_code, point_lon, point_lat, "
                                   "shape_area_m2 from pig_midas_deposits where deposit_number = 324")})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("midas licence (catalogue rows)", {
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
    bulk_file.annotate = lambda _l, title, data: CAPTURED.append([title, data])  # into the landing note (cap 10)
    midas_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("midas trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
