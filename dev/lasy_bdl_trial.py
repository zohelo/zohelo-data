"""Dev trial (PL-ENV-018, #3301): Lasy Państwowe BDL, source lasy_bdl, Landing → Gold on the final code.

Landing extract of the two declared WFS layers into zz_dev_lasy_bdl/ twice (the second must write nothing: same
SHA-256), 10 s between requests, inspect and verify, and resultType=hits per layer to compare with the features landed;
Bronze load into the Iceberg namespace zz_dev_lasy_bdl and the lake prefix zz_dev_lasy_bdl/ twice (the second appends
nothing); a profile of the Bronze rows; the two Silver models, their completeness tests and the Gold build stage
rendered with Jinja from warehouse/ and run in local DuckDB, each twice, with the checks of their dbt tests, the grain
check by series_key x date_key and spot values for Poland. silver.prg_units (gminas) and gold.dim_geography are read
(read-only) from production. Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment
import requests

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.lasy_bdl import load as bdl_load
from ingestion.to_landing.patterns import bulk_file

SOURCE = "lasy_bdl"
SCRATCH = "zz_dev_lasy_bdl"
NOTES = {"left": 9}
CAPTURED: list = []
MACROS = Path("warehouse/macros/sources/lasy_bdl.sql").read_text()
CODES = [f"{i:02d}" for i in range(1, 18)]


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
    url = spec["url"].replace("&outputFormat=application/json", "&resultType=hits")
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
    note("bdl landing (scratch)", {
        "extract_two_runs": runs, "inspect_exit": inspect, "verify_exit": verify, "hits": matched,
        "captured": [[t, str(d)[:400]] for t, d in CAPTURED][:4],
        "receipts": {e: [[r["size_bytes"], r["version_token"], r["sha256"][:12]] for r in h] for e, h in held.items()}})

    timings = []
    for _ in (1, 2):
        started = time.monotonic()
        code = bdl_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "lasy_bdl_features"))
        files = table.inspect.files()
        timings.append({"exit": code, "seconds": round(time.monotonic() - started), "files": files.num_rows,
                        "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "lasy_bdl_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("bdl bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; INSTALL spatial; LOAD spatial;")
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    cols = [c for c, _ in bdl_load.COLUMNS if c not in ("entity", "ingested_at", "source_row_number")]
    by_entity = q(con, "select entity, count(*), count(distinct feature_id), count(distinct region_cd), "
                       "count(distinct region_cd || '-' || coalesce(inspectorate_cd, '')), min(source_version), "
                       "count(distinct raw_sha256) from bronze_b group by 1 order by 1")
    note("bdl bronze profile", {
        "rows_by_entity": by_entity,
        "features_vs_hits": {r[0]: [int(r[1]), matched.get(r[0])] for r in by_entity},
        "nulls_by_entity": q(con, "select entity, " + ", ".join(f"count(*) - count({c})" for c in cols)
                             + " from bronze_b group by 1"),
        "null_columns_order": cols,
        "a_year": q(con, "select entity, a_year, count(*) from bronze_b group by all order by 1"),
        "address_shapes": q(con, "select regexp_replace(adress_forest, '[0-9]', '9', 'g'), count(*) from bronze_b "
                                 "where entity = 'districts' group by 1"),
        "text_values_blank": one(con, "select count(*) from bronze_b where trim(coalesce(inspectorate_name, "
                                      "region_name, 'x')) = ''"),
        "computed_ranges": q(con, "select entity, min(area_m2::double), max(area_m2::double), "
                                  "round(sum(area_m2::double) / 1e6), min(point_lon::double), max(point_lon::double), "
                                  "min(point_lat::double), max(point_lat::double), "
                                  "max(abs(centroid_lon::double - point_lon::double)) from bronze_b group by 1"),
        "repeated_names": q(con, "select inspectorate_name, list(region_cd || '-' || inspectorate_cd) from bronze_b "
                                 "where entity = 'districts' group by 1 having count(*) > 1")})

    models = {m: render(f"warehouse/models/silver/lasy_bdl/{m}.sql", {}, {"lasy_bdl_features": "bronze_b"})
              for m in ("lasy_bdl_districts", "lasy_bdl_regions")}
    rows = []
    for _ in (1, 2):
        for m, sql in models.items():
            con.execute(f"create or replace table {m} as {sql}")
        rows.append([one(con, f"select count(*) from {m}") for m in models])
    keys = {"lasy_bdl_districts": "district_code", "lasy_bdl_regions": "region_code"}
    nn = {"lasy_bdl_districts": ["district_code", "region_code", "inspectorate_code", "forest_address", "name",
                                 "data_year", "area_m2", "point_lon", "point_lat", "snapshot_date", "raw_sha256"],
          "lasy_bdl_regions": ["region_code", "name", "data_year", "area_m2", "point_lon", "point_lat",
                               "snapshot_date", "raw_sha256"]}
    codes = ", ".join(f"'{c}'" for c in CODES)
    tests = {}
    for m, key in keys.items():
        tests[m] = {
            "dup_grain": one(con, f"select count(*) from (select {key} from {m} group by all having count(*) > 1)"),
            "nulls": {c: n for c, n in zip(nn[m], con.execute(
                "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in nn[m]) + f" from {m}").fetchone())
                if n},
            "region_not_accepted": one(con, f"select count(*) from {m} where region_code not in ({codes})"),
            "complete_fails": q(con, Environment().from_string(Path(f"warehouse/tests/{m}_complete.sql").read_text())
                                .render(ref=lambda n: n)),
        }
    note("bdl silver", {
        "rows_two_builds": rows, "tests": tests,
        "districts_region_missing": one(con, "select count(*) from lasy_bdl_districts d where not exists "
                                             "(select 1 from lasy_bdl_regions r where r.region_code = d.region_code)"),
        "regions": q(con, "select r.region_code, r.name, round(r.area_m2 / 1e6), count(d.district_code), "
                          "round(sum(d.area_m2) / 1e6) from lasy_bdl_regions r left join lasy_bdl_districts d "
                          "using (region_code) group by all order by 1"),
        "totals_km2": q(con, "select (select round(sum(area_m2) / 1e6, 1) from lasy_bdl_districts), "
                             "(select round(sum(area_m2) / 1e6, 1) from lasy_bdl_regions)"),
        "district_area_km2": q(con, "select round(min(area_m2) / 1e6, 1), round(median(area_m2) / 1e6, 1), "
                                    "round(max(area_m2) / 1e6, 1) from lasy_bdl_districts"),
        "snapshot": q(con, "select min(snapshot_date), max(snapshot_date), min(data_year), max(data_year) "
                           "from lasy_bdl_districts"),
        "spot_kosciany": q(con, "select district_code, name, forest_address, round(area_m2 / 1e6, 2), point_lon, "
                                "point_lat from lasy_bdl_districts where name in ('Kościan', 'Białowieża')")})

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
    cte = Environment().from_string(MACROS + "{{ poc_geography__lasy_bdl() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from lasy_bdl")
    gold_sql = render("warehouse/models/gold/poc_build_lasy_bdl.sql", {
        "poc_map_geography": "map_t", "prg_units": "prg_t", "lasy_bdl_districts": "lasy_bdl_districts",
        "lasy_bdl_regions": "lasy_bdl_regions"}, {})
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    note("bdl gold build_lasy_bdl", {
        "rows_two_builds": gold_rows, "prg_gminas": one(con, "select count(*) from prg_t"),
        "map_rows": one(con, "select count(*) from map_t"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_measure": q(con, "select measure_key, date_key, frequency, count(*), sum(is_total::int), "
                             "count(distinct geography_key) from gold_t group by all order by 1"),
        "unmatched": q(con, "select geography_source_code, count(*) from gold_t where geography_key = '_X' group by 1"),
        "null_or_nonpositive": one(con, "select count(*) from gold_t where value is null or value <= 0"),
        "null_labels": one(con, "select count(*) from gold_t where not is_total and (breakdown_label_json is null "
                                "or breakdown_label_json like '%null%')"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    note("bdl gold spot values", {
        "poland": q(con, "select measure_key, value from gold_t where geography_key = 'PL' and is_total order by 1"),
        "voivodeship_sum_vs_poland": q(con, "select measure_key, sum(value) filter (where geography_key <> 'PL'), "
                                            "max(value) filter (where geography_key = 'PL') from gold_t where is_total "
                                            "group by 1 order by 1"),
        "rdlp_sum_vs_poland": q(con, "select measure_key, sum(value) from gold_t where not is_total group by 1"),
        "districts_by_voivodeship": q(con, "select g.name_pl, b.value from gold_t as b join geo as g using "
                                           "(geography_key) where b.measure_key = 'lasy_bdl:districts:districts:count' "
                                           "and b.is_total order by 2 desc"),
        "unplaced_districts": q(con, "select district_code, name, point_lon, point_lat from lasy_bdl_districts d "
                                     "where not exists (select 1 from prg_g v where d.point_lon between v.x0 and v.x1 "
                                     "and d.point_lat between v.y0 and v.y1 and st_contains(v.geom, "
                                     "st_point(d.point_lon, d.point_lat)))"),
        "rdlp_rows": q(con, "select breakdown_label_json, value from gold_t where measure_key = "
                            "'lasy_bdl:districts:districts:count' and not is_total order by 2 desc limit 5"),
        "spot_district": q(con, "select breakdown_json, breakdown_label_json, value from gold_t where measure_key = "
                                "'lasy_bdl:districts:m2:district_area' order by value desc limit 3")})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("bdl licence (catalogue rows)", {
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
    bdl_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("bdl trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
