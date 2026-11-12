"""Dev trial (PL-ENV-002, #2971): GUGiK PRNG, source gugik_prng, Landing → Gold on the final code.

Landing extract of both declared packages into the Landing prefix zz_dev_gugik_prng/ twice (the second must write
nothing: same SHA-256), inspect and verify; Bronze load into the Iceberg namespace zz_dev_gugik_prng and the lake prefix
zz_dev_gugik_prng/ twice (the second appends nothing); a profile of the Bronze rows; the Silver models, the completeness
tests and the Gold build stage rendered with Jinja from warehouse/ and run in local DuckDB, each twice, with the checks
of their dbt tests, the grain check by series_key × date_key, the metrics view's rows and value bound, and spot values
for Poland. dim_geography is read (read-only) from production. Everything written is removed in `finally`. At most 10
annotations.
"""

from __future__ import annotations

import csv
from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_prng import load as prng_load
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gugik_prng"
SCRATCH = "zz_dev_gugik_prng"
NOTES = {"left": 9}


def note(title: str, data, level: str = "notice") -> None:
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        annotate(level, title, data)
    print(f"[{title}] {data}", flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path: str, refs: dict[str, str], sources: dict[str, str], prefix: str = "") -> str:
    return Environment().from_string(prefix + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def trial(cat) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    specs = bulk_file.declaration(SOURCE)
    runs = []
    for attempt in (1, 2):
        for spec in specs:
            started = time.monotonic()
            result = bulk_file.load_file(client, bucket, SOURCE, spec, SCRATCH)
            runs.append({"run": attempt, "entity": spec["entity"], "outcome": result.get("outcome"),
                         "size_mb": result.get("size_mb"), "version": result.get("version"), "seconds": round(time.monotonic() - started)})
    held = {s["entity"]: bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, s["entity"], SCRATCH)) for s in specs}
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH, specs=specs)
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=specs)
    note("prng landing (scratch)", {
        "extract_two_runs": runs, "inspect_exit": inspect, "verify_exit": verify,
        "receipts": {e: [{"size": r["size_bytes"], "version": r["version_token"], "basis": r.get("version_basis"),
                          "sha": r["sha256"][:12], "members": r["detail"].get("members")} for r in h]
                     for e, h in held.items()}})

    timings = []
    for _ in (1, 2):
        started = time.monotonic()
        code = prng_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "prng_names"))
        files = table.inspect.files()
        timings.append({"exit": code, "seconds": round(time.monotonic() - started), "files": files.num_rows,
                        "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "prng_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("prng bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'")
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    texts = [c for c in prng_load.ATTRIBUTES.values()]
    note("prng bronze profile", {
        "rows_by_entity": q(con, "select entity, count(*), count(distinct identyfikator_prng), min(export_date), "
                                 "max(export_date), count(distinct raw_sha256) from bronze_b group by 1 order by 1"),
        "prng_id_repeated_across": one(con, "select count(*) from (select identyfikator_prng from bronze_b group by 1 "
                                            "having count(*) > 1)"),
        "prng_id_not_digits": one(con, "select count(*) from bronze_b where not regexp_full_match("
                                       "coalesce(identyfikator_prng, ''), '[0-9]+')"),
        "null_or_empty": {c: n for c, n in zip(texts, con.execute(
            "select " + ", ".join(f"count(*) filter (where {c} is null or trim({c}) = '')" for c in texts)
            + " from bronze_b").fetchone()) if n},
        "literal_NULL": {c: n for c, n in zip(texts, con.execute(
            "select " + ", ".join(f"count(*) filter (where trim({c}) = 'NULL')" for c in texts)
            + " from bronze_b").fetchone()) if n},
        "gmina_code_shapes": q(con, "select entity, regexp_replace(identyfikator_gminy, '[0-9]', '9', 'g') s, count(*) "
                                    "from bronze_b group by all order by 3 desc limit 8"),
        "multi_gmina_sample": q(con, "select identyfikator_gminy, gmina from bronze_b where identyfikator_gminy like "
                                     "'%;%' limit 2"),
        "serials_not_numeric": q(con, "select count(*) filter (where not regexp_full_match(trim(wersja_obiektu), "
                                      "'[0-9]+(\\.[0-9]+)?')), count(*) filter (where not regexp_full_match("
                                      "trim(poczatek_wersji_obiektu), '[0-9]+(\\.[0-9]+)?')), count(*) filter (where "
                                      "not regexp_full_match(trim(wazna_od), '[0-9]+(\\.0+)?')) from bronze_b"),
        "wgs_shapes": q(con, "select rodzaj_reprezentacji, regexp_replace(wspolrzedne_geograficzne, '[0-9]', '9', 'g'), "
                             "count(*) from bronze_b group by all order by 3 desc limit 5"),
        "xy_warszawa": q(con, "select nazwa_glowna, rodzaj_obiektu, wspolrzedne_geograficzne, wspolrzedne_prostokatne_xy "
                              "from bronze_b where nazwa_glowna = 'Warszawa' and rodzaj_obiektu = 'miasto'"),
        "kinds": q(con, "select entity, count(distinct rodzaj_obiektu), count(distinct kategoria_obiektu) from bronze_b "
                        "group by 1")})

    silver_sql = render("warehouse/models/silver/gugik_prng/prng_objects.sql", {}, {"prng_names": "bronze_b"})
    bridge_sql = render("warehouse/models/silver/gugik_prng/prng_object_gminas.sql", {"prng_objects": "silver_o"}, {})
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver_o as {silver_sql}")
        con.execute(f"create or replace table silver_g as {bridge_sql}")
        rows.append([one(con, "select count(*) from silver_o"), one(con, "select count(*) from silver_g")])
    accepted = {
        "entity": ("miejscowosci", "obiekty_fizjograficzne"),
        "name_status": ("urzędowa", "zestandaryzowana", "niestandaryzowana"),
        "object_category": ("miejscowość", "pokrycie terenu", "ukształtowanie terenu", "hydrografia", "transport",
                            "budowle", "inne"),
        "representation_kind": ("punkt główny", "punkt początkowy obiektu; punkt końcowy obiektu"),
    }
    tests = {
        "unique_prng_id": one(con, "select count(*) from (select prng_id from silver_o group by 1 having count(*) > 1)"),
        "unique_iip_id": one(con, "select count(*) from (select iip_id from silver_o group by 1 having count(*) > 1)"),
        "not_null_objects": q(con, "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in (
            "prng_id", "entity", "iip_id", "name", "name_status", "object_category", "object_kind",
            "representation_kind", "export_date", "raw_sha256")) + " from silver_o"),
        "accepted_values_fails": {c: q(con, f"select {c}, count(*) from silver_o where {c} not in "
                                            f"({', '.join(repr(v) for v in vals)}) group by 1")
                                  for c, vals in accepted.items()},
        "unique_bridge": one(con, "select count(*) from (select prng_id, gmina_code from silver_g group by all having "
                                  "count(*) > 1)"),
        "not_null_bridge": q(con, "select count(*) filter (where prng_id is null), count(*) filter (where gmina_code is "
                                  "null), count(*) filter (where raw_sha256 is null) from silver_g"),
        "complete_objects": q(con, render("warehouse/tests/gugik_prng_objects_complete.sql",
                                          {"prng_objects": "silver_o"}, {})),
        "complete_bridge": q(con, render("warehouse/tests/gugik_prng_object_gminas_complete.sql",
                                         {"prng_object_gminas": "silver_g"}, {})),
    }
    note("prng silver", {
        "rows_two_builds_objects_bridge": rows, "tests": tests,
        "by_entity": q(con, "select entity, count(*), count(simc_id), count(latitude), min(latitude), max(latitude), "
                            "min(longitude), max(longitude), min(version_at), max(version_at), min(valid_from), "
                            "max(valid_from), sum(gmina_count = 0), max(gmina_count) from silver_o group by 1"),
        "name_status": q(con, "select entity, name_status, count(*) from silver_o group by all order by 1, 2"),
        "categories": q(con, "select object_category, count(*) from silver_o group by 1 order by 2 desc"),
        "no_gmina": q(con, "select object_category, object_kind, count(*) from silver_o where gmina_count = 0 group by "
                           "all order by 3 desc limit 5"),
        "bridge_levels": q(con, "select entity, count(distinct gmina_code), count(distinct powiat_code), "
                                "count(distinct voivodeship_code) from silver_g group by 1")})

    geo_cols = ("geography_key", "level_name", "name_pl", "terc_code")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    macro = Path("warehouse/macros/sources/gugik_prng.sql").read_text()
    cte = Environment().from_string(macro + "{{ poc_geography__gugik_prng() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from gugik_prng")
    gold_sql = render("warehouse/models/gold/poc_build_gugik_prng.sql",
                      {"poc_map_geography": "map_t", "prng_objects": "silver_o", "prng_object_gminas": "silver_g"}, {})
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    note("prng gold build_gugik_prng", {
        "rows_two_builds": gold_rows,
        "map_rows": q(con, "select count(*), count(distinct source_code) from map_t"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_block": q(con, "select block_id, date_key, frequency, count(*), count(distinct measure_key), "
                           "sum(is_total::int) from gold_t group by all order by 1"),
        "levels": q(con, "select b.block_id, coalesce(g.level_name, b.geography_key), count(*) from gold_t as b left "
                         "join geo as g using (geography_key) group by all order by 1, 2"),
        "unmatched": q(con, "select block_id, geography_source_code, sum(value) filter (where is_total) from gold_t "
                            "where geography_key = '_X' group by all order by 3 desc limit 12"),
        "null_or_nonpositive": one(con, "select count(*) from gold_t where value is null or value <= 0"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    metrics = con.execute(
        "select geography_key, date_key, count(*) n, max(value) v from gold_t where measure_key = "
        "'gugik_prng:miejscowosci:named_objects:count' and is_total and geography_key <> '_X' group by all").fetchall()
    note("prng gold spot values and metrics", {
        "poland": q(con, "select block_id, breakdown_json, value from gold_t where geography_key = 'PL' order by 1, 3 "
                         "desc limit 14"),
        "poland_totals_vs_silver": q(con, "select (select value from gold_t where geography_key = 'PL' and block_id = "
                                          "'miejscowosci' and is_total), (select count(*) from silver_o where entity = "
                                          "'miejscowosci'), (select value from gold_t where geography_key = 'PL' and "
                                          "block_id = 'obiekty_fizjograficzne' and is_total), (select count(*) from "
                                          "silver_o where entity = 'obiekty_fizjograficzne')"),
        "voivodeships_localities": q(con, "select g.name_pl, b.value from gold_t as b join geo as g using "
                                          "(geography_key) where g.level_name = 'wojewodztwo' and b.block_id = "
                                          "'miejscowosci' and b.is_total order by 2 desc limit 4"),
        "gmina_sum_vs_pl_localities": q(con, "select sum(b.value) from gold_t as b join geo as g using (geography_key) "
                                             "where g.level_name = 'gmina' and b.block_id = 'miejscowosci' and "
                                             "b.is_total"),
        "spot_gminas": q(con, "select geography_key, g.name_pl, block_id, value from gold_t as b left join geo as g "
                              "using (geography_key) where geography_key in ('TERC:1465011', 'TERC:1261011', "
                              "'TERC:0264011') and is_total order by 1, 3"),
        "metrics_rows": len(metrics), "metrics_max_rows_per_area_period": max((r[2] for r in metrics), default=0),
        "metrics_out_of_bound_0_5e5": sum(1 for r in metrics if not (0 <= r[3] <= 5e5)),
        "metrics_max": max((r[3] for r in metrics), default=None)})

    seeds = Path("warehouse/seeds")
    source_row = [r for r in csv.DictReader((seeds / "source.csv").open()) if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("prng licence (catalogue rows)", {
        "source_csv": source_row,
        "licence_csv": [r for r in csv.DictReader((seeds / "licence.csv").open()) if r["licence_key"] in lic],
        "unit_map": [r for r in csv.DictReader((seeds / "unit_map.csv").open()) if r["source_system"] == SOURCE]})


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
    bulk_file.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    prng_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("prng trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
